package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/google/uuid"
)

// TaskExecution is a consistent snapshot of the task and its current runtime
// owner. A terminal task does not need an active lease.
type TaskExecution struct {
	Task    kernelstore.Task
	Attempt *kernelstore.Attempt
	Lease   *kernelstore.Lease
}

// TaskRepository is the existing durable task execution boundary, augmented
// with a read-only snapshot for service supervision.
type TaskRepository interface {
	CreateTask(context.Context, kernelstore.CreateTaskInput) (kernelstore.CreateTaskResult, error)
	GetTask(context.Context, string, uuid.UUID) (kernelstore.Task, error)
	RequestTaskCancellation(context.Context, string, uuid.UUID, int64) (kernelstore.Task, error)
	GetAgentVersionByRef(context.Context, string, string) (kernelstore.AgentVersion, error)
	GetServiceTaskExecution(context.Context, string, uuid.UUID) (TaskExecution, error)
}

// TaskSpawner submits service instances to normal admission and scheduling.
// It never launches an ungoverned process inside the control plane.
type TaskSpawner struct {
	repository TaskRepository
	clock      func() time.Time
}

func NewTaskSpawner(repository TaskRepository) *TaskSpawner {
	return &TaskSpawner{repository: repository, clock: func() time.Time { return time.Now().UTC() }}
}

func (p *TaskSpawner) Rebind(store Store) InstanceSpawner {
	repository, ok := store.(TaskRepository)
	if !ok {
		return &TaskSpawner{clock: p.clock}
	}
	return &TaskSpawner{repository: repository, clock: p.clock}
}

func serviceVersion(svc *Service) string {
	if svc.Spec.AgentVersionRef != "" {
		return svc.Spec.AgentVersionRef
	}
	if svc.Spec.AgentVersion != "" {
		return svc.Spec.AgentVersion
	}
	if strings.Contains(svc.AgentID, "@") {
		return svc.AgentID
	}
	return ""
}

// ValidateService freezes a canonical launch declaration before any instance
// can be created. Admission remains authoritative for policy and budgets.
func (p *TaskSpawner) ValidateService(ctx context.Context, svc *Service) error {
	if p.repository == nil {
		return fmt.Errorf("%w: task repository is not configured", ErrInvalidServiceSpec)
	}
	if svc.Spec.AgentVersionRef != "" && svc.Spec.AgentVersion != "" && svc.Spec.AgentVersionRef != svc.Spec.AgentVersion {
		return fmt.Errorf("%w: agentVersion and agentVersionRef disagree", ErrInvalidServiceSpec)
	}
	ref := serviceVersion(svc)
	namespace, name, version, err := agentversion.ParseRef(ref)
	if err != nil || namespace != svc.Namespace {
		return fmt.Errorf("%w: a published agentVersionRef in the service namespace is required", ErrInvalidServiceSpec)
	}
	ref = agentversion.FormatRef(namespace, name, version)
	if _, err := p.repository.GetAgentVersionByRef(ctx, svc.TenantID, ref); err != nil {
		return fmt.Errorf("%w: resolve agentVersionRef: %v", ErrInvalidServiceSpec, err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(svc.Spec.WorkloadSpec, &document); err != nil || document == nil {
		return fmt.Errorf("%w: workloadSpec must be a task specification object", ErrInvalidServiceSpec)
	}
	if _, ok := document["budget"]; !ok {
		return fmt.Errorf("%w: workloadSpec.budget is required", ErrInvalidServiceSpec)
	}
	spec, err := workload.Decode(svc.Spec.WorkloadSpec)
	if err != nil || len(spec.Placement.RuntimeClasses) == 0 || strings.TrimSpace(spec.Placement.Region) == "" {
		return fmt.Errorf("%w: workloadSpec requires placement.runtimeClasses and placement.region", ErrInvalidServiceSpec)
	}
	if svc.Spec.RuntimeClass != "" {
		if !slices.Contains(spec.Placement.RuntimeClasses, svc.Spec.RuntimeClass) {
			return fmt.Errorf("%w: runtimeClass must be allowed by workloadSpec placement", ErrInvalidServiceSpec)
		}
		var placement map[string]json.RawMessage
		if err := json.Unmarshal(document["placement"], &placement); err != nil {
			return fmt.Errorf("%w: invalid placement", ErrInvalidServiceSpec)
		}
		placement["runtimeClasses"], _ = json.Marshal([]string{svc.Spec.RuntimeClass})
		placement["preferredClass"], _ = json.Marshal(svc.Spec.RuntimeClass)
		document["placement"], _ = json.Marshal(placement)
	}
	svc.Spec.WorkloadSpec, err = json.Marshal(document)
	if err != nil {
		return err
	}
	svc.Spec.AgentVersionRef, svc.Spec.AgentVersion = ref, ref
	return nil
}

// PrepareInstance pins the launch inputs to this restart generation. Its
// deterministic task ID also recovers a crash between task and instance writes.
func (p *TaskSpawner) PrepareInstance(svc *Service, inst *Instance) {
	inst.AgentVersion = serviceVersion(svc)
	inst.RuntimeClass = svc.Spec.RuntimeClass
	inst.LaunchSpec = append([]byte(nil), svc.Spec.WorkloadSpec...)
	key := fmt.Sprintf("%s/%s/%s/%d", svc.TenantID, svc.ID, inst.ID, inst.RestartCount)
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("fenced/service/"+key))
	inst.TaskID = &id
	inst.FencingToken = 0
	inst.LastHeartbeat = time.Time{}
	inst.Phase = InstanceStarting
}

func (p *TaskSpawner) SpawnInstance(ctx context.Context, svc *Service, inst *Instance) error {
	if p.repository == nil || inst.TaskID == nil || len(inst.LaunchSpec) == 0 {
		return fmt.Errorf("service instance has no pinned task launch")
	}
	key := fmt.Sprintf("service/%s/instance/%s/generation/%d", svc.ID, inst.ID, inst.RestartCount)
	_, err := p.repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: *inst.TaskID, TenantID: svc.TenantID, Namespace: svc.Namespace,
		AgentVersionRef: inst.AgentVersion, Spec: inst.LaunchSpec,
		Goal:           "Run service " + svc.ID + " instance " + inst.ID,
		IdempotencyKey: key,
	})
	return err
}

func (p *TaskSpawner) StopInstance(ctx context.Context, svc *Service, inst *Instance) error {
	if p.repository == nil {
		return fmt.Errorf("task repository is not configured")
	}
	if inst.TaskID == nil {
		if inst.Phase == InstanceStopping || inst.Phase == InstanceDraining {
			inst.Phase = InstanceStopped
			now := p.clock()
			inst.TerminatedAt = &now
			return nil
		}
		return nil // Legacy state-only instances never launched a task.
	}
	for attempt := 0; attempt < 6; attempt++ {
		task, err := p.repository.GetTask(ctx, svc.TenantID, *inst.TaskID)
		if errors.Is(err, kernelstore.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if task.Phase.Terminal() || task.CancelRequestedAt != nil {
			return nil
		}
		_, err = p.repository.RequestTaskCancellation(ctx, svc.TenantID, task.ID, task.ResourceVersion)
		if errors.Is(err, kernelstore.ErrVersionConflict) || kernelstore.IsRetryableTransaction(err) {
			continue
		}
		return err
	}
	return kernelstore.ErrVersionConflict
}

// RefreshInstance derives liveness from the currently fenced runtime lease.
// Stopping and draining cannot be reversed by a late heartbeat.
func (p *TaskSpawner) RefreshInstance(ctx context.Context, svc *Service, inst *Instance) error {
	if inst.TaskID == nil {
		if inst.Phase == InstanceStopping || inst.Phase == InstanceDraining {
			inst.Phase = InstanceStopped
			now := p.clock()
			inst.TerminatedAt = &now
			return nil
		}
		if err := p.ValidateService(ctx, svc); err != nil {
			return err
		}
		p.PrepareInstance(svc, inst)
		return p.SpawnInstance(ctx, svc, inst)
	}
	execution, err := p.repository.GetServiceTaskExecution(ctx, svc.TenantID, *inst.TaskID)
	if errors.Is(err, kernelstore.ErrNotFound) {
		if inst.Phase == InstanceStopping || inst.Phase == InstanceDraining {
			inst.Phase = InstanceStopped
			now := p.clock()
			inst.TerminatedAt = &now
			return nil
		}
		return p.SpawnInstance(ctx, svc, inst)
	}
	if err != nil {
		return err
	}
	task := execution.Task
	if task.Phase.Terminal() {
		now := p.clock()
		inst.TerminatedAt = &now
		inst.NextRestartAt = nil
		switch task.Phase {
		case domain.TaskSucceeded:
			inst.Phase, inst.ExitCode = InstanceStopped, 0
		case domain.TaskCancelled:
			if inst.ExitCode != 0 {
				inst.Phase = InstanceFailed
			} else {
				inst.Phase = InstanceStopped
			}
		default:
			inst.Phase, inst.ExitCode = InstanceFailed, 1
		}
		if inst.ExitReason == "" {
			inst.ExitReason = "task " + string(task.Phase)
			if execution.Attempt != nil && execution.Attempt.FailureMessage != "" {
				inst.ExitReason += ": " + execution.Attempt.FailureMessage
			}
		}
		return nil
	}
	if inst.Phase == InstanceStopping || inst.Phase == InstanceDraining {
		return nil
	}
	inst.Phase = InstanceStarting
	inst.LastHeartbeat = time.Time{}
	if execution.Attempt == nil || execution.Lease == nil {
		return nil
	}
	inst.FencingToken = uint64(execution.Attempt.FencingToken)
	inst.RuntimeClass = execution.Attempt.RuntimeClass
	inst.LastHeartbeat = execution.Lease.HeartbeatAt
	if !execution.Lease.ExpiresAt.After(p.clock()) {
		return nil
	}
	switch execution.Attempt.Phase {
	case domain.AttemptRunning, domain.AttemptWaitingTool, domain.AttemptWaitingAgent,
		domain.AttemptWaitingApproval, domain.AttemptCheckpointing:
		inst.Phase = InstanceRunning
		if p.clock().Sub(execution.Lease.HeartbeatAt) <= svc.Spec.Health.HeartbeatTTL {
			inst.ConsecutiveFailures = 0
		}
	}
	return nil
}
