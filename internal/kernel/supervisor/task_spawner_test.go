package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// taskSpawnerRepository keeps cancellation requests separate from terminal
// acknowledgement, as the real Task worker does.
type taskSpawnerRepository struct {
	tasks              map[uuid.UUID]TaskExecution
	created            []kernelstore.CreateTaskInput
	cancellationErrors []error
	cancellationCalls  int
	clock              func() time.Time
}

func (r *taskSpawnerRepository) CreateTask(_ context.Context, in kernelstore.CreateTaskInput) (kernelstore.CreateTaskResult, error) {
	if execution, ok := r.tasks[in.ID]; ok {
		return kernelstore.CreateTaskResult{Task: execution.Task, Existing: true}, nil
	}
	task := kernelstore.Task{ID: in.ID, TenantID: in.TenantID, Namespace: in.Namespace,
		AgentVersionRef: in.AgentVersionRef, Goal: in.Goal, Spec: append(json.RawMessage(nil), in.Spec...),
		IdempotencyKey: in.IdempotencyKey, Phase: domain.TaskQueued, ResourceVersion: 1}
	r.tasks[in.ID] = TaskExecution{Task: task}
	r.created = append(r.created, in)
	return kernelstore.CreateTaskResult{Task: task}, nil
}

func (r *taskSpawnerRepository) GetTask(_ context.Context, tenant string, id uuid.UUID) (kernelstore.Task, error) {
	execution, ok := r.tasks[id]
	if !ok || execution.Task.TenantID != tenant {
		return kernelstore.Task{}, kernelstore.ErrNotFound
	}
	return execution.Task, nil
}

func (r *taskSpawnerRepository) RequestTaskCancellation(_ context.Context, tenant string, id uuid.UUID, expectedVersion int64) (kernelstore.Task, error) {
	r.cancellationCalls++
	if len(r.cancellationErrors) > 0 {
		err := r.cancellationErrors[0]
		r.cancellationErrors = r.cancellationErrors[1:]
		if err != nil {
			return kernelstore.Task{}, err
		}
	}
	execution, ok := r.tasks[id]
	if !ok || execution.Task.TenantID != tenant {
		return kernelstore.Task{}, kernelstore.ErrNotFound
	}
	if execution.Task.ResourceVersion != expectedVersion {
		return kernelstore.Task{}, kernelstore.ErrVersionConflict
	}
	now := r.clock()
	execution.Task.CancelRequestedAt = &now
	execution.Task.ResourceVersion++
	r.tasks[id] = execution
	return execution.Task, nil
}

func (r *taskSpawnerRepository) GetAgentVersionByRef(_ context.Context, tenant, ref string) (kernelstore.AgentVersion, error) {
	if ref != "worker@1.0.0" && ref != "worker@2.0.0" {
		return kernelstore.AgentVersion{}, kernelstore.ErrNotFound
	}
	return kernelstore.AgentVersion{TenantID: tenant, Namespace: "default", Name: "worker"}, nil
}

func (r *taskSpawnerRepository) GetServiceTaskExecution(_ context.Context, tenant string, id uuid.UUID) (TaskExecution, error) {
	execution, ok := r.tasks[id]
	if !ok || execution.Task.TenantID != tenant {
		return TaskExecution{}, kernelstore.ErrNotFound
	}
	return execution, nil
}

type taskSpawnerFixture struct {
	t     *testing.T
	ctx   context.Context
	now   time.Time
	store *MemoryStore
	repo  *taskSpawnerRepository
	sup   *Supervisor
	svc   *Service
}

func newTaskSpawnerFixture(t *testing.T, replicas int, policy RestartPolicy, maxRetries int) *taskSpawnerFixture {
	t.Helper()
	f := &taskSpawnerFixture{t: t, ctx: context.Background(), now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	clock := func() time.Time { return f.now }
	f.store = NewMemoryStoreWithClock(clock)
	f.repo = &taskSpawnerRepository{tasks: make(map[uuid.UUID]TaskExecution), clock: clock}
	f.sup = NewSupervisor(f.store, WithSpawner(NewTaskSpawner(f.repo)), WithClock(clock))
	svc := &Service{ID: "svc-task-test", TenantID: "tenant-a", Namespace: "default", Name: "worker-service", AgentID: "worker",
		Spec: ServiceSpec{Replicas: replicas, AgentVersionRef: "worker@1.0.0", RuntimeClass: "remote", RestartPolicy: policy,
			Backoff: BackoffConfig{InitialInterval: time.Second, MaxInterval: time.Second, Factor: 1, MaxRetries: maxRetries},
			Health:  HealthConfig{HeartbeatTTL: 10 * time.Second, UnhealthyThreshold: 3, CheckInterval: time.Second},
			Rollout: RolloutConfig{MaxUnavailable: 1, MaxSurge: 1}, DrainTimeout: time.Second,
			WorkloadSpec: json.RawMessage(`{"budget":{"tokens":100,"costUsd":1,"toolCalls":1,"wallSeconds":60},"placement":{"runtimeClasses":["remote"],"region":"cn-east","cpuMillis":100,"memoryMiB":64,"llmConcurrency":1}}`)}}
	var err error
	f.svc, err = f.sup.CreateService(f.ctx, svc)
	if err != nil {
		t.Fatalf("create task-backed service: %v", err)
	}
	return f
}

func (f *taskSpawnerFixture) instances() []*Instance {
	f.t.Helper()
	instances, err := f.store.ListInstances(f.ctx, f.svc.TenantID, f.svc.ID)
	if err != nil {
		f.t.Fatalf("list instances: %v", err)
	}
	return instances
}

func (f *taskSpawnerFixture) instance(id string) *Instance {
	f.t.Helper()
	inst, err := f.store.GetInstance(f.ctx, f.svc.TenantID, f.svc.ID, id)
	if err != nil {
		f.t.Fatalf("get instance: %v", err)
	}
	return inst
}

func (f *taskSpawnerFixture) reconcile() {
	f.t.Helper()
	if err := f.sup.ReconcileService(f.ctx, f.svc); err != nil {
		f.t.Fatalf("reconcile: %v", err)
	}
}

func (f *taskSpawnerFixture) run(inst *Instance) {
	f.t.Helper()
	if inst.TaskID == nil {
		f.t.Fatal("task-backed instance has no task ID")
	}
	execution := f.repo.tasks[*inst.TaskID]
	execution.Task.Phase = domain.TaskRunning
	execution.Attempt = &kernelstore.Attempt{ID: uuid.New(), TenantID: inst.TenantID, Phase: domain.AttemptRunning,
		RuntimeClass: "remote", FencingToken: 7}
	execution.Lease = &kernelstore.Lease{HeartbeatAt: f.now, ExpiresAt: f.now.Add(time.Minute), FencingToken: 7}
	f.repo.tasks[*inst.TaskID] = execution
}

func (f *taskSpawnerFixture) finish(inst *Instance, phase domain.TaskPhase) {
	f.t.Helper()
	execution := f.repo.tasks[*inst.TaskID]
	execution.Task.Phase = phase
	execution.Task.ResourceVersion++
	execution.Lease = nil
	f.repo.tasks[*inst.TaskID] = execution
}

func TestTaskSpawnerReadinessRequiresRunningAttemptAndLease(t *testing.T) {
	f := newTaskSpawnerFixture(t, 1, RestartAlways, 0)
	inst := f.instances()[0]
	if inst.Phase != InstanceStarting || inst.TaskID == nil {
		t.Fatalf("queued launch became ready: %+v", inst)
	}
	input := f.repo.created[0]
	if input.ID != *inst.TaskID || input.AgentVersionRef != "worker@1.0.0" || input.TenantID != f.svc.TenantID || input.Namespace != f.svc.Namespace || input.IdempotencyKey == "" {
		t.Fatalf("incorrect pinned task submission: %+v", input)
	}
	if err := f.sup.RecordHeartbeat(f.ctx, inst.TenantID, inst.ServiceID, inst.ID); err != nil {
		t.Fatalf("refresh queued heartbeat: %v", err)
	}
	if got := f.instance(inst.ID); got.Phase != InstanceStarting || !got.LastHeartbeat.IsZero() {
		t.Fatalf("external heartbeat manufactured readiness: %+v", got)
	}
	if err := f.sup.ReportInstanceExit(f.ctx, inst.TenantID, inst.ServiceID, inst.ID, 0, "fake exit"); !errors.Is(err, ErrInvalidServiceSpec) {
		t.Fatalf("task-backed exit report must be rejected, got %v", err)
	}
	for _, phase := range []domain.AttemptPhase{domain.AttemptPlaced, domain.AttemptStarting} {
		execution := f.repo.tasks[*inst.TaskID]
		execution.Task.Phase = domain.TaskAdmitted
		execution.Attempt = &kernelstore.Attempt{Phase: phase, RuntimeClass: "remote", FencingToken: 7}
		execution.Lease = &kernelstore.Lease{HeartbeatAt: f.now, ExpiresAt: f.now.Add(time.Minute)}
		f.repo.tasks[*inst.TaskID] = execution
		f.reconcile()
		if got := f.instance(inst.ID); got.Phase != InstanceStarting {
			t.Fatalf("attempt %s became ready: %+v", phase, got)
		}
	}
	f.run(inst)
	f.reconcile()
	if got := f.instance(inst.ID); got.Phase != InstanceRunning || got.FencingToken != 7 || got.RuntimeClass != "remote" {
		t.Fatalf("leased running task did not become ready: %+v", got)
	}
	execution := f.repo.tasks[*inst.TaskID]
	execution.Lease.ExpiresAt = f.now
	f.repo.tasks[*inst.TaskID] = execution
	f.reconcile()
	if got := f.instance(inst.ID); got.Phase == InstanceRunning {
		t.Fatalf("expired runtime lease remained ready: %+v", got)
	}
}

func TestTaskSpawnerInvalidLaunchCannotCreateTasks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Service)
	}{
		{"unpublished version", func(s *Service) { s.Spec.AgentVersionRef, s.Spec.AgentVersion = "worker@9.0.0", "worker@9.0.0" }},
		{"namespace mismatch", func(s *Service) {
			s.Spec.AgentVersionRef, s.Spec.AgentVersion = "other/worker@1.0.0", "other/worker@1.0.0"
		}},
		{"conflicting versions", func(s *Service) { s.Spec.AgentVersion = "worker@2.0.0" }},
		{"missing workload", func(s *Service) { s.Spec.WorkloadSpec = nil }},
		{"non object workload", func(s *Service) { s.Spec.WorkloadSpec = json.RawMessage(`[]`) }},
		{"missing budget", func(s *Service) {
			s.Spec.WorkloadSpec = json.RawMessage(`{"placement":{"runtimeClasses":["remote"],"region":"cn-east"}}`)
		}},
		{"disallowed runtime", func(s *Service) { s.Spec.RuntimeClass = "wasm" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTaskSpawnerFixture(t, 1, RestartNever, 0)
			candidate, err := f.store.GetService(f.ctx, f.svc.TenantID, f.svc.ID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(candidate)
			if _, err := f.sup.UpdateService(f.ctx, candidate); !errors.Is(err, ErrInvalidServiceSpec) {
				t.Fatalf("invalid launch was accepted: %v", err)
			}
			current, err := f.store.GetService(f.ctx, f.svc.TenantID, f.svc.ID)
			if err != nil || current.Spec.AgentVersionRef != "worker@1.0.0" || len(f.repo.created) != 1 {
				t.Fatalf("invalid update changed execution: service=%+v tasks=%d error=%v", current, len(f.repo.created), err)
			}
		})
	}
}

func TestTaskSpawnerRestartWaitsForTerminalAcknowledgement(t *testing.T) {
	f := newTaskSpawnerFixture(t, 1, RestartNever, 1)
	inst := f.instances()[0]
	f.run(inst)
	f.reconcile()
	oldTask := *inst.TaskID
	if err := f.sup.RestartService(f.ctx, inst.TenantID, inst.ServiceID); err != nil {
		t.Fatalf("request restart: %v", err)
	}
	for i := 0; i < 3; i++ {
		f.now = f.now.Add(time.Second)
		f.reconcile()
		got := f.instance(inst.ID)
		if got.Phase != InstanceStopping || got.TerminatedAt != nil || *got.TaskID != oldTask || got.RestartCount != 0 || len(f.repo.created) != 1 {
			t.Fatalf("replacement preceded task cancellation acknowledgement: %+v, tasks=%d", got, len(f.repo.created))
		}
		if err := f.sup.RecordHeartbeat(f.ctx, inst.TenantID, inst.ServiceID, inst.ID); err != nil {
			t.Fatalf("late heartbeat: %v", err)
		}
		if got := f.instance(inst.ID); got.Phase != InstanceStopping {
			t.Fatalf("late heartbeat revived stopping task: %+v", got)
		}
	}
	if f.repo.tasks[oldTask].Task.CancelRequestedAt == nil || f.repo.cancellationCalls != 1 {
		t.Fatalf("cancellation was not idempotently requested: calls=%d", f.repo.cancellationCalls)
	}
	f.finish(inst, domain.TaskCancelled)
	f.reconcile()
	got := f.instance(inst.ID)
	if got.Phase != InstanceStarting || got.RestartCount != 1 || *got.TaskID == oldTask || len(f.repo.created) != 2 {
		t.Fatalf("terminal generation was not replaced: %+v, tasks=%d", got, len(f.repo.created))
	}
	f.reconcile()
	if len(f.repo.created) != 2 || *f.instance(inst.ID).TaskID != *got.TaskID {
		t.Fatal("retired terminal task created a duplicate restart generation")
	}
}

func TestTaskSpawnerCancellationRetries(t *testing.T) {
	t.Run("version conflict retries within stop", func(t *testing.T) {
		f := newTaskSpawnerFixture(t, 1, RestartAlways, 0)
		inst := f.instances()[0]
		f.repo.cancellationErrors = []error{kernelstore.ErrVersionConflict, nil}
		if err := f.sup.StopService(f.ctx, inst.TenantID, inst.ServiceID); err != nil {
			t.Fatalf("stop after conflict: %v", err)
		}
		if f.repo.cancellationCalls != 2 || f.instance(inst.ID).Phase != InstanceStopping {
			t.Fatalf("expected cancellation retry and pending acknowledgement, calls=%d instance=%+v", f.repo.cancellationCalls, f.instance(inst.ID))
		}
	})
	t.Run("failed cancellation retries on reconcile", func(t *testing.T) {
		f := newTaskSpawnerFixture(t, 1, RestartAlways, 0)
		inst := f.instances()[0]
		failure := errors.New("cancellation unavailable")
		f.repo.cancellationErrors = []error{failure}
		if err := f.sup.StopService(f.ctx, inst.TenantID, inst.ServiceID); !errors.Is(err, failure) {
			t.Fatalf("expected cancellation error, got %v", err)
		}
		if got := f.instance(inst.ID); got.Phase != InstanceStopping || got.TerminatedAt != nil {
			t.Fatalf("failed cancellation was marked stopped: %+v", got)
		}
		f.reconcile()
		if f.repo.cancellationCalls != 2 || f.repo.tasks[*inst.TaskID].Task.CancelRequestedAt == nil || len(f.repo.created) != 1 {
			t.Fatalf("reconcile failed to retry cancellation: calls=%d tasks=%d", f.repo.cancellationCalls, len(f.repo.created))
		}
		f.finish(inst, domain.TaskCancelled)
		f.reconcile()
		if got := f.instance(inst.ID); got.Phase != InstanceStopped || got.TerminatedAt == nil || len(f.repo.created) != 1 {
			t.Fatalf("acknowledged stop spawned replacement: %+v", got)
		}
	})
}

func TestTaskSpawnerRestartPolicyAndRetryLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy RestartPolicy
		phase  domain.TaskPhase
	}{
		{"never failed", RestartNever, domain.TaskFailed},
		{"never succeeded", RestartNever, domain.TaskSucceeded},
		{"on failure succeeded", RestartOnFailure, domain.TaskSucceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTaskSpawnerFixture(t, 1, tc.policy, 1)
			inst := f.instances()[0]
			f.finish(inst, tc.phase)
			for i := 0; i < 4; i++ {
				f.now = f.now.Add(time.Minute)
				f.reconcile()
			}
			if len(f.repo.created) != 1 || len(f.instances()) != 1 || !f.instance(inst.ID).IsTerminal() {
				t.Fatalf("replica reconciliation bypassed restart policy: tasks=%d instances=%+v", len(f.repo.created), f.instances())
			}
		})
	}
	for _, policy := range []RestartPolicy{RestartOnFailure, RestartAlways} {
		t.Run(string(policy)+" max retries", func(t *testing.T) {
			f := newTaskSpawnerFixture(t, 1, policy, 1)
			inst := f.instances()[0]
			f.finish(inst, domain.TaskFailed)
			f.reconcile()
			if got := f.instance(inst.ID); got.NextRestartAt == nil || got.RestartCount != 0 || len(f.repo.created) != 1 {
				t.Fatalf("backoff was skipped: %+v", got)
			}
			f.now = f.now.Add(time.Second)
			f.reconcile()
			inst = f.instance(inst.ID)
			if inst.RestartCount != 1 || len(f.repo.created) != 2 {
				t.Fatalf("first failure did not restart: %+v", inst)
			}
			f.finish(inst, domain.TaskFailed)
			for i := 0; i < 4; i++ {
				f.now = f.now.Add(time.Minute)
				f.reconcile()
			}
			if got := f.instance(inst.ID); got.RestartCount != 1 || got.NextRestartAt != nil || len(f.repo.created) != 2 || len(f.instances()) != 1 {
				t.Fatalf("retry limit was bypassed: %+v tasks=%d", got, len(f.repo.created))
			}
		})
	}
}

func TestTaskSpawnerRolloutThreeReplicasOneSurge(t *testing.T) {
	f := newTaskSpawnerFixture(t, 3, RestartNever, 0)
	for _, inst := range f.instances() {
		f.run(inst)
	}
	f.reconcile()
	if _, err := f.sup.RolloutUpgrade(f.ctx, f.svc.TenantID, f.svc.ID, "worker@2.0.0"); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if len(f.repo.created) != 4 {
		t.Fatalf("expected one surge task, got %d tasks", len(f.repo.created))
	}
	for _, inst := range f.instances() {
		if inst.AgentVersion == "worker@1.0.0" && inst.Phase != InstanceRunning {
			t.Fatalf("old instance drained before a replacement ran: %+v", inst)
		}
	}
	for step := 0; step < 20; step++ {
		f.now = f.now.Add(time.Second)
		for _, inst := range f.instances() {
			switch inst.Phase {
			case InstanceStarting, InstanceRunning:
				f.run(inst)
			case InstanceStopping:
				if f.repo.tasks[*inst.TaskID].Task.CancelRequestedAt == nil {
					t.Fatalf("stopping task has no cancellation request: %+v", inst)
				}
				f.finish(inst, domain.TaskCancelled)
			}
		}
		f.reconcile()
		outstanding, newReady, oldLive := 0, 0, 0
		for _, execution := range f.repo.tasks {
			if !execution.Task.Phase.Terminal() {
				outstanding++
			}
		}
		for _, inst := range f.instances() {
			if inst.AgentVersion == "worker@2.0.0" && inst.Phase == InstanceRunning {
				newReady++
			}
			if inst.AgentVersion == "worker@1.0.0" && !inst.IsTerminal() {
				oldLive++
			}
		}
		if outstanding > 4 {
			t.Fatalf("rollout exceeded replicas + maxSurge: %d", outstanding)
		}
		if newReady == 3 && oldLive == 0 {
			if len(f.repo.created) != 6 {
				t.Fatalf("rollout created duplicate replacements: %d tasks", len(f.repo.created))
			}
			return
		}
	}
	t.Fatalf("three-replica rollout stalled: %+v", f.instances())
}

func TestTaskSpawnerRetiredOldVersionDoesNotBlockUpgrade(t *testing.T) {
	for _, tc := range []struct {
		policy RestartPolicy
		phase  domain.TaskPhase
	}{{RestartNever, domain.TaskFailed}, {RestartOnFailure, domain.TaskSucceeded}} {
		t.Run(string(tc.policy), func(t *testing.T) {
			f := newTaskSpawnerFixture(t, 1, tc.policy, 1)
			old := f.instances()[0]
			f.finish(old, tc.phase)
			f.reconcile()
			if _, err := f.sup.RolloutUpgrade(f.ctx, f.svc.TenantID, f.svc.ID, "worker@2.0.0"); err != nil {
				t.Fatalf("upgrade retired version: %v", err)
			}
			if len(f.repo.created) != 2 {
				t.Fatalf("retired old version blocked replacement: tasks=%d", len(f.repo.created))
			}
			for _, inst := range f.instances() {
				if inst.AgentVersion == "worker@2.0.0" {
					if inst.Phase != InstanceStarting || *inst.TaskID == *old.TaskID {
						t.Fatalf("invalid upgraded instance: %+v", inst)
					}
					return
				}
			}
			t.Fatal("upgrade did not create target-version instance")
		})
	}
}

func TestTaskSpawnerStaleHeartbeatReachesFailureThreshold(t *testing.T) {
	f := newTaskSpawnerFixture(t, 1, RestartOnFailure, 1)
	inst := f.instances()[0]
	f.run(inst)
	f.reconcile()
	f.now = f.now.Add(11 * time.Second) // Lease remains valid; service heartbeat is stale.
	for failures := 1; failures <= 3; failures++ {
		f.reconcile()
		got := f.instance(inst.ID)
		if got.ConsecutiveFailures != failures {
			t.Fatalf("refresh erased stale heartbeat failures: want %d, got %+v", failures, got)
		}
		if failures < 3 {
			if got.Phase != InstanceDegraded || f.repo.cancellationCalls != 0 {
				t.Fatalf("premature unhealthy cancellation: %+v", got)
			}
			if err := f.sup.RecordHeartbeat(f.ctx, inst.TenantID, inst.ServiceID, inst.ID); err != nil {
				t.Fatalf("refresh stale heartbeat: %v", err)
			}
			if got := f.instance(inst.ID); got.ConsecutiveFailures != failures {
				t.Fatalf("external heartbeat erased trusted stale failures: %+v", got)
			}
		} else if got.Phase != InstanceStopping || got.TerminatedAt != nil || got.ExitCode == 0 || f.repo.cancellationCalls != 1 {
			t.Fatalf("failure threshold did not request cancellation: %+v calls=%d", got, f.repo.cancellationCalls)
		}
		f.now = f.now.Add(time.Second)
	}
	if len(f.repo.created) != 1 {
		t.Fatal("unhealthy task replaced before cancellation acknowledgement")
	}
	f.finish(inst, domain.TaskCancelled)
	f.reconcile()
	if got := f.instance(inst.ID); got.Phase != InstanceFailed || got.NextRestartAt == nil {
		t.Fatalf("unhealthy cancellation did not retain failure for restart: %+v", got)
	}
	f.now = f.now.Add(time.Second)
	f.reconcile()
	if got := f.instance(inst.ID); got.RestartCount != 1 || got.Phase != InstanceStarting || len(f.repo.created) != 2 {
		t.Fatalf("unhealthy generation failed to restart: %+v", got)
	}
}

func TestTaskSpawnerUpgradeRetiresPendingOldVersionRestarts(t *testing.T) {
	for _, tc := range []struct {
		policy RestartPolicy
		phase  domain.TaskPhase
	}{{RestartAlways, domain.TaskSucceeded}, {RestartOnFailure, domain.TaskFailed}} {
		t.Run(string(tc.policy), func(t *testing.T) {
			f := newTaskSpawnerFixture(t, 3, tc.policy, 0)
			oldInstances := f.instances()
			for _, inst := range oldInstances {
				f.finish(inst, tc.phase)
			}
			f.reconcile()
			for _, inst := range oldInstances {
				if f.instance(inst.ID).NextRestartAt == nil {
					t.Fatal("expected old generation to have a pending restart")
				}
			}
			if _, err := f.sup.RolloutUpgrade(f.ctx, f.svc.TenantID, f.svc.ID, "worker@2.0.0"); err != nil {
				t.Fatalf("upgrade: %v", err)
			}
			for step := 0; step < 4; step++ {
				f.now = f.now.Add(time.Second)
				for _, inst := range f.instances() {
					if inst.AgentVersion == "worker@2.0.0" {
						f.run(inst)
					}
				}
				f.reconcile()
				live := 0
				for _, execution := range f.repo.tasks {
					if !execution.Task.Phase.Terminal() {
						live++
					}
				}
				if live != 3 || len(f.repo.created) != 6 {
					t.Fatalf("old backoff revived surplus tasks: live=%d created=%d", live, len(f.repo.created))
				}
				for _, inst := range oldInstances {
					got := f.instance(inst.ID)
					if !got.IsTerminal() || got.AgentVersion != "worker@1.0.0" || got.NextRestartAt != nil {
						t.Fatalf("old deployment was resurrected: %+v", got)
					}
				}
			}
		})
	}
}

func TestTaskSpawnerRolloutFailureRespectsRestartPolicy(t *testing.T) {
	for _, policy := range []RestartPolicy{RestartNever, RestartOnFailure} {
		t.Run(string(policy), func(t *testing.T) {
			f := newTaskSpawnerFixture(t, 3, policy, 1)
			for _, inst := range f.instances() {
				f.run(inst)
			}
			f.reconcile()
			if _, err := f.sup.RolloutUpgrade(f.ctx, f.svc.TenantID, f.svc.ID, "worker@2.0.0"); err != nil {
				t.Fatal(err)
			}
			var target *Instance
			for _, inst := range f.instances() {
				if inst.AgentVersion == "worker@2.0.0" {
					target = inst
				}
			}
			if target == nil || len(f.repo.created) != 4 {
				t.Fatal("expected one rollout surge task")
			}
			f.finish(target, domain.TaskFailed)
			f.reconcile()
			if len(f.repo.created) != 4 {
				t.Fatal("rollout bypassed backoff by creating a fresh instance")
			}
			wantTasks := 4
			if policy == RestartOnFailure {
				f.now = f.now.Add(time.Second)
				f.reconcile()
				target = f.instance(target.ID)
				if target.RestartCount != 1 || len(f.repo.created) != 5 {
					t.Fatalf("target did not use its allowed restart: %+v tasks=%d", target, len(f.repo.created))
				}
				f.finish(target, domain.TaskFailed)
				wantTasks = 5
			}
			for step := 0; step < 4; step++ {
				f.now = f.now.Add(time.Second)
				for _, inst := range f.instances() {
					if inst.AgentVersion == "worker@1.0.0" {
						f.run(inst)
					}
				}
				f.reconcile()
				if len(f.repo.created) != wantTasks || !f.instance(target.ID).IsTerminal() {
					t.Fatalf("rollout bypassed restart policy: tasks=%d target=%+v", len(f.repo.created), f.instance(target.ID))
				}
				for _, inst := range f.instances() {
					if inst.AgentVersion == "worker@1.0.0" && inst.Phase != InstanceRunning {
						t.Fatalf("failed target drained a healthy old replica: %+v", inst)
					}
				}
			}
		})
	}
}
