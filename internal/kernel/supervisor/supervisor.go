package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/google/uuid"
)

// Option is a functional option for configuring the Supervisor.
type Option func(*Supervisor)

// WithClock sets a custom clock for deterministic testing.
func WithClock(clock func() time.Time) Option {
	return func(s *Supervisor) {
		s.clock = clock
	}
}

// WithSpawner sets the instance spawner implementation.
func WithSpawner(spawner InstanceSpawner) Option {
	return func(s *Supervisor) {
		s.spawner = spawner
	}
}

// WithIPCService attaches the kernel IPC service for mailbox awareness and autowake.
func WithIPCService(ipcSvc *ipc.Service) Option {
	return func(s *Supervisor) {
		s.ipcSvc = ipcSvc
	}
}

// WithMailbox attaches a Mailbox for inspecting pending messages on autowake.
func WithMailbox(mailbox ipc.Mailbox) Option {
	return func(s *Supervisor) {
		s.mailbox = mailbox
	}
}

// Supervisor manages long-running Agent Services, monitors instance health,
// handles crash recovery with exponential backoffs, and balances replica counts.
type Supervisor struct {
	mu      sync.Mutex
	store   Store
	spawner InstanceSpawner
	ipcSvc  *ipc.Service
	mailbox ipc.Mailbox
	clock   func() time.Time
	idGen   func() string
	scoped  bool
}

// NewSupervisor constructs a new Supervisor engine.
func NewSupervisor(store Store, opts ...Option) *Supervisor {
	s := &Supervisor{
		store:   store,
		spawner: NewMockSpawner(),
		clock: func() time.Time {
			return time.Now().UTC()
		},
		idGen: func() string {
			return uuid.New().String()
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	if spawner, ok := s.spawner.(*TaskSpawner); ok {
		spawner.clock = s.clock
	}
	return s
}

// CreateService registers a new AgentService and triggers initial reconciliation.
func (s *Supervisor) createService(ctx context.Context, svc *Service) (*Service, error) {
	if svc == nil {
		return nil, ErrInvalidServiceSpec
	}
	if svc.ID == "" {
		svc.ID = "svc-" + s.idGen()[:8]
	}
	if err := svc.Validate(); err != nil {
		return nil, err
	}
	if err := s.validateLaunch(ctx, svc); err != nil {
		return nil, err
	}

	now := s.clock()
	svc.CreatedAt = now
	svc.UpdatedAt = now
	svc.Status = ServiceStatus{
		Phase:             ServicePending,
		DesiredReplicas:   svc.Spec.Replicas,
		CurrentReplicas:   0,
		ReadyReplicas:     0,
		UpdatedReplicas:   0,
		AvailableReplicas: 0,
		ActiveVersion:     svc.Spec.AgentVersion,
		RestartCount:      0,
		LastTransitionAt:  now,
		Message:           "Service created; waiting for initial reconciliation",
	}

	if err := s.store.CreateService(ctx, svc); err != nil {
		return nil, err
	}

	// Run immediate reconciliation for the newly created service
	if err := s.ReconcileService(ctx, svc); err != nil {
		return nil, err
	}

	return s.store.GetService(ctx, svc.TenantID, svc.ID)
}

// GetService retrieves a service by tenant and ID.
func (s *Supervisor) GetService(ctx context.Context, tenantID, serviceID string) (*Service, error) {
	return s.store.GetService(ctx, tenantID, serviceID)
}

// ListServices lists services for a tenant and namespace.
func (s *Supervisor) ListServices(ctx context.Context, tenantID, namespace string) ([]*Service, error) {
	return s.store.ListServices(ctx, tenantID, namespace)
}

// UpdateService updates service specification and reconciles.
func (s *Supervisor) updateService(ctx context.Context, svc *Service) (*Service, error) {
	if svc == nil {
		return nil, ErrInvalidServiceSpec
	}
	existing, err := s.store.GetService(ctx, svc.TenantID, svc.ID)
	if err != nil {
		return nil, err
	}
	if existing.Status.Phase == ServiceTerminated {
		return nil, ErrServiceTerminated
	}

	if err := svc.Validate(); err != nil {
		return nil, err
	}
	if err := s.validateLaunch(ctx, svc); err != nil {
		return nil, err
	}

	svc.Status = existing.Status
	svc.Status.DesiredReplicas = svc.Spec.Replicas
	if svc.Spec.AgentVersion != "" && svc.Spec.AgentVersion != existing.Spec.AgentVersion {
		svc.Status.PreviousVersion = existing.Spec.AgentVersion
	}
	svc.UpdatedAt = s.clock()

	if err := s.store.UpdateService(ctx, svc); err != nil {
		return nil, err
	}

	if err := s.ReconcileService(ctx, svc); err != nil {
		return nil, err
	}

	return s.store.GetService(ctx, svc.TenantID, svc.ID)
}

// ScaleService changes the desired replica count.
func (s *Supervisor) scaleService(ctx context.Context, tenantID, serviceID string, replicas int) (*Service, error) {
	if replicas < 0 {
		return nil, fmt.Errorf("%w: replicas must be non-negative", ErrInvalidServiceSpec)
	}
	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return nil, err
	}
	if svc.Status.Phase == ServiceTerminated {
		return nil, ErrServiceTerminated
	}

	svc.Spec.Replicas = replicas
	svc.Status.DesiredReplicas = replicas
	svc.UpdatedAt = s.clock()

	if err := s.store.UpdateService(ctx, svc); err != nil {
		return nil, err
	}

	if err := s.ReconcileService(ctx, svc); err != nil {
		return nil, err
	}

	return s.store.GetService(ctx, tenantID, serviceID)
}

// RestartService marks all active instances for termination and spawns replacements.
func (s *Supervisor) restartService(ctx context.Context, tenantID, serviceID string) error {
	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}
	if svc.Status.Phase == ServiceTerminated {
		return ErrServiceTerminated
	}
	instances, err := s.store.ListInstances(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}
	for _, inst := range instances {
		if inst.IsTerminal() {
			if plannedStop(inst.ExitReason) || inst.AgentVersion != serviceVersion(svc) {
				continue
			}
			inst.ExitReason = "operator restart requested"
			inst.NextRestartAt = nil
			if err := s.store.UpdateInstance(ctx, inst); err != nil {
				return err
			}
		} else if err := s.stop(ctx, svc, inst, "operator restart requested"); err != nil {
			return err
		}
	}
	return s.ReconcileService(ctx, svc)
}

// StopService gracefully stops all instances and sets replicas to 0.
func (s *Supervisor) stopService(ctx context.Context, tenantID, serviceID string) error {
	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}
	if svc.Status.Phase == ServiceTerminated {
		return ErrServiceTerminated
	}
	svc.Spec.Replicas = 0
	svc.Spec.AutoWake = false
	svc.Status.DesiredReplicas = 0
	svc.Status.Phase = ServiceSuspended
	svc.Status.Message = "Service stopped by operator"
	if err := s.store.UpdateService(ctx, svc); err != nil {
		return err
	}
	instances, err := s.store.ListInstances(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}
	for _, inst := range instances {
		if !inst.IsTerminal() {
			if err := s.stop(ctx, svc, inst, "service stopped"); err != nil {
				return err
			}
		} else if inst.NextRestartAt != nil {
			inst.NextRestartAt = nil
			if err := s.store.UpdateInstance(ctx, inst); err != nil {
				return err
			}
		}
	}
	return s.ReconcileService(ctx, svc)
}

// DeleteService stops all running instances and marks the service terminated.
func (s *Supervisor) deleteService(ctx context.Context, tenantID, serviceID string) error {
	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}
	svc.Spec.Replicas = 0
	svc.Spec.AutoWake = false
	svc.Status.Phase = ServiceTerminated
	svc.Status.Message = "Service deletion waiting for runtime cancellation"
	if err := s.store.UpdateService(ctx, svc); err != nil {
		return err
	}
	return s.ReconcileService(ctx, svc)
}

// RecordHeartbeat refreshes an instance's liveness timestamp and resets failure counts.
func (s *Supervisor) recordHeartbeat(ctx context.Context, tenantID, serviceID, instanceID string) error {
	inst, err := s.store.GetInstance(ctx, tenantID, serviceID, instanceID)
	if err != nil {
		return err
	}
	if inst.IsTerminal() {
		return ErrInstanceTerminated
	}
	if refresher, ok := s.spawner.(interface {
		RefreshInstance(context.Context, *Service, *Instance) error
	}); ok {
		svc, err := s.store.GetService(ctx, tenantID, serviceID)
		if err != nil {
			return err
		}
		if err := refresher.RefreshInstance(ctx, svc, inst); err != nil {
			return err
		}
		return s.store.UpdateInstance(ctx, inst)
	}

	now := s.clock()
	inst.LastHeartbeat = now
	inst.ConsecutiveFailures = 0
	if inst.Phase == InstanceStarting || inst.Phase == InstanceDegraded {
		inst.Phase = InstanceRunning
	}
	inst.UpdatedAt = now

	return s.store.UpdateInstance(ctx, inst)
}

// ReportInstanceExit records a crash or clean termination of a service instance.
func (s *Supervisor) reportInstanceExit(ctx context.Context, tenantID, serviceID, instanceID string, exitCode int, reason string) error {
	inst, err := s.store.GetInstance(ctx, tenantID, serviceID, instanceID)
	if err != nil {
		return err
	}
	if inst.TaskID != nil {
		return fmt.Errorf("%w: task-backed instance exits are observed from the runtime task", ErrInvalidServiceSpec)
	}

	now := s.clock()
	inst.TerminatedAt = &now
	inst.ExitCode = exitCode
	inst.ExitReason = reason
	if exitCode == 0 {
		inst.Phase = InstanceStopped
	} else {
		inst.Phase = InstanceFailed
	}
	inst.UpdatedAt = now

	if err := s.store.UpdateInstance(ctx, inst); err != nil {
		return err
	}

	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}

	return s.ReconcileService(ctx, svc)
}

// DrainInstance transitions an instance to Draining and sets a deadline for graceful shutdown.
func (s *Supervisor) drainInstance(ctx context.Context, tenantID, serviceID, instanceID string, timeout time.Duration) error {
	inst, err := s.store.GetInstance(ctx, tenantID, serviceID, instanceID)
	if err != nil {
		return err
	}
	if inst.IsTerminal() {
		return ErrInstanceTerminated
	}

	now := s.clock()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := now.Add(timeout)
	inst.Phase = InstanceDraining
	inst.DrainingAt = &now
	inst.DrainDeadline = &deadline
	inst.UpdatedAt = now

	if err := s.store.UpdateInstance(ctx, inst); err != nil {
		return err
	}

	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return err
	}
	return s.ReconcileService(ctx, svc)
}

// RolloutUpgrade updates the service's target version and triggers rolling reconciliation.
func (s *Supervisor) rolloutUpgrade(ctx context.Context, tenantID, serviceID, newVersion string) (*Service, error) {
	if strings.TrimSpace(newVersion) == "" {
		return nil, fmt.Errorf("%w: target agent version cannot be empty", ErrInvalidServiceSpec)
	}

	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return nil, err
	}
	if svc.Status.Phase == ServiceTerminated {
		return nil, ErrServiceTerminated
	}

	if svc.Spec.AgentVersion == newVersion {
		return svc, nil
	}

	svc.Status.PreviousVersion = svc.Spec.AgentVersion
	svc.Spec.AgentVersion = newVersion
	svc.Spec.AgentVersionRef = newVersion
	if err := s.validateLaunch(ctx, svc); err != nil {
		return nil, err
	}
	svc.UpdatedAt = s.clock()

	if err := s.store.UpdateService(ctx, svc); err != nil {
		return nil, err
	}

	if err := s.ReconcileService(ctx, svc); err != nil {
		return nil, err
	}

	return s.store.GetService(ctx, tenantID, serviceID)
}

// RollbackService reverts the service's agent version to the previous recorded version.
func (s *Supervisor) rollbackService(ctx context.Context, tenantID, serviceID string) (*Service, error) {
	svc, err := s.store.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return nil, err
	}
	if svc.Status.Phase == ServiceTerminated {
		return nil, ErrServiceTerminated
	}
	if svc.Status.PreviousVersion == "" {
		return nil, ErrNoPreviousVersion
	}

	prev := svc.Status.PreviousVersion
	curr := svc.Spec.AgentVersion
	svc.Spec.AgentVersion = prev
	svc.Spec.AgentVersionRef = prev
	if err := s.validateLaunch(ctx, svc); err != nil {
		return nil, err
	}
	svc.Status.PreviousVersion = curr
	svc.UpdatedAt = s.clock()

	if err := s.store.UpdateService(ctx, svc); err != nil {
		return nil, err
	}

	if err := s.ReconcileService(ctx, svc); err != nil {
		return nil, err
	}

	return s.store.GetService(ctx, tenantID, serviceID)
}

// Reconcile passes through all services and reconciles their instances.
func (s *Supervisor) Reconcile(ctx context.Context, tenantID string) error {
	var services []*Service
	var err error
	if tenantID == "" {
		all, ok := s.store.(AllServicesStore)
		if !ok {
			return fmt.Errorf("internal service enumeration is not configured")
		}
		services, err = all.ListAllServices(ctx)
	} else {
		services, err = s.store.ListServices(ctx, tenantID, "")
	}
	if err != nil {
		return err
	}

	var reconcileErr error
	for _, svc := range services {
		if err := s.ReconcileService(ctx, svc); err != nil {
			if errors.Is(err, ErrServiceNotFound) {
				continue
			}
			slog.Error("reconcile failed for service", "serviceId", svc.ID, "tenantId", svc.TenantID, "error", err)
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}
	return reconcileErr
}

// ReconcileService reconciles a single AgentService state against desired spec.
func (s *Supervisor) reconcileService(ctx context.Context, svc *Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if svc == nil {
		return nil
	}
	if svc.Status.Phase == ServiceTerminated {
		return s.finishDeletion(ctx, svc)
	}
	if svc.Spec.Replicas > 0 || svc.Spec.AutoWake {
		if err := s.validateLaunch(ctx, svc); err != nil {
			return err
		}
	}

	now := s.clock()
	instances, err := s.store.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil {
		return err
	}

	// Reconstruct instance state from the durable task/lease, including after
	// a controller restart. Do not re-observe a retired generation.
	if refresher, ok := s.spawner.(interface {
		RefreshInstance(context.Context, *Service, *Instance) error
	}); ok {
		for _, inst := range instances {
			if inst.IsActive() || inst.IsDraining() || inst.Phase == InstanceStopping {
				if err := refresher.RefreshInstance(ctx, svc, inst); err != nil {
					return err
				}
				inst.UpdatedAt = now
				if err := s.store.UpdateInstance(ctx, inst); err != nil {
					return err
				}
				if inst.Phase == InstanceStopping {
					if err := s.spawner.StopInstance(ctx, svc, inst); err != nil {
						return err
					}
				}
			}
		}
	}
	// 1. Health inspection: check heartbeats
	for _, inst := range instances {
		if inst.Phase == InstanceRunning || inst.Phase == InstanceStarting || inst.Phase == InstanceDegraded {
			if !inst.LastHeartbeat.IsZero() && now.Sub(inst.LastHeartbeat) > svc.Spec.Health.HeartbeatTTL {
				inst.ConsecutiveFailures++
				if inst.ConsecutiveFailures >= svc.Spec.Health.UnhealthyThreshold {
					inst.ExitCode = -1
					reason := fmt.Sprintf("missed %d consecutive heartbeats (TTL: %v)", inst.ConsecutiveFailures, svc.Spec.Health.HeartbeatTTL)
					if err := s.stop(ctx, svc, inst, reason); err != nil {
						return err
					}
					if inst.Phase == InstanceStopped {
						inst.Phase = InstanceFailed
						if err := s.store.UpdateInstance(ctx, inst); err != nil {
							return err
						}
					}
				} else {
					inst.Phase = InstanceDegraded
					if err := s.store.UpdateInstance(ctx, inst); err != nil {
						return err
					}
				}
			}
		}
	}

	// 2. Draining progression: advance draining instances to stopped when deadline reached or mailbox drained
	instances, err = s.store.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil {
		return err
	}
	for _, inst := range instances {
		if inst.Phase == InstanceDraining {
			deadlineExpired := inst.DrainDeadline != nil && (now.After(*inst.DrainDeadline) || now.Equal(*inst.DrainDeadline))
			mailboxDrained := false
			if s.mailbox != nil {
				msgs, err := s.mailbox.Receive(ctx, inst.Address, 1)
				if err == nil && len(msgs) == 0 {
					mailboxDrained = true
				}
			}
			if deadlineExpired || (s.mailbox != nil && mailboxDrained) {
				inst.ExitCode = 0
				if err := s.stop(ctx, svc, inst, "drained"); err != nil {
					return err
				}
			}
		}
	}

	// 3. Crash recovery & restart policy handling
	instances, err = s.store.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil {
		return err
	}
	targetVersion := serviceVersion(svc)
	for _, inst := range instances {
		// Retired versions do not own a replica in the new deployment. A
		// pending backoff must not resurrect them alongside its replacements.
		if inst.IsTerminal() && inst.AgentVersion != targetVersion {
			if inst.NextRestartAt != nil {
				inst.NextRestartAt = nil
				if err := s.store.UpdateInstance(ctx, inst); err != nil {
					return err
				}
			}
			continue
		}
		if svc.Spec.Replicas > 0 && inst.IsTerminal() && !plannedStop(inst.ExitReason) {
			shouldRestart := false
			switch svc.Spec.RestartPolicy {
			case RestartAlways:
				shouldRestart = true
			case RestartOnFailure:
				if inst.ExitCode != 0 {
					shouldRestart = true
				}
			case RestartNever:
				shouldRestart = false
			}

			manualRestart := inst.ExitReason == "operator restart requested"
			if shouldRestart || manualRestart {
				maxRetries := svc.Spec.Backoff.MaxRetries
				if !manualRestart && maxRetries > 0 && inst.RestartCount >= maxRetries {
					continue
				}

				if inst.NextRestartAt == nil {
					delay := svc.Spec.Backoff.CalculateDelay(inst.RestartCount)
					if manualRestart {
						delay = 0
					}
					next := now.Add(delay)
					inst.NextRestartAt = &next
					if err := s.store.UpdateInstance(ctx, inst); err != nil {
						return err
					}
				}

				if inst.NextRestartAt != nil && (now.After(*inst.NextRestartAt) || now.Equal(*inst.NextRestartAt)) {
					// Time to restart this instance
					inst.RestartCount++
					inst.ConsecutiveFailures = 0
					inst.NextRestartAt = nil
					inst.Phase = InstanceRunning
					inst.LastHeartbeat = now
					inst.ExitCode = 0
					inst.ExitReason = ""
					inst.TerminatedAt = nil
					inst.UpdatedAt = now

					s.prepareLaunch(svc, inst)
					if err := s.store.UpdateInstance(ctx, inst); err != nil {
						return err
					}
					if err := s.spawn(ctx, svc, inst); err != nil {
						return err
					}
					svc.Status.RestartCount++
				}
			}
		}
	}

	// 4. Reload instances and categorize for rolling upgrade and scaling
	instances, err = s.store.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil {
		return err
	}

	var activeInstances []*Instance
	var recoveringInstances []*Instance
	var readyInstances []*Instance
	var updatedActiveInstances []*Instance
	var outdatedActiveInstances []*Instance
	stoppingCount, retiredCount, drainingCount, updatedRecoveringCount := 0, 0, 0, 0

	for _, inst := range instances {
		if inst.IsActive() {
			activeInstances = append(activeInstances, inst)
			if inst.Phase == InstanceRunning {
				readyInstances = append(readyInstances, inst)
			}
			if targetVersion != "" {
				if inst.AgentVersion == targetVersion {
					updatedActiveInstances = append(updatedActiveInstances, inst)
				} else if inst.AgentVersion != "" {
					outdatedActiveInstances = append(outdatedActiveInstances, inst)
				}
			}
		} else if inst.IsRecovering() {
			recoveringInstances = append(recoveringInstances, inst)
			if inst.AgentVersion == targetVersion {
				updatedRecoveringCount++
			}
		} else if inst.Phase == InstanceStopping {
			stoppingCount++
		} else if inst.IsDraining() {
			drainingCount++
		} else if inst.IsTerminal() && !plannedStop(inst.ExitReason) && inst.AgentVersion == targetVersion {
			retiredCount++
		}
	}

	desired := svc.Spec.Replicas

	// AutoWake logic: wake up from 0 replicas when pending messages exist
	if svc.Spec.AutoWake && svc.Spec.Replicas == 0 {
		hasPending := s.checkPendingMessages(ctx, svc)
		if hasPending {
			desired = 1
		}
	}

	// 5. Rolling Update handling
	isRolling := len(outdatedActiveInstances) > 0 && targetVersion != ""
	if isRolling && desired > 0 {
		maxSurge := svc.Spec.Rollout.MaxSurge
		if maxSurge <= 0 {
			maxSurge = 1
		}
		maxUnavailable := svc.Spec.Rollout.MaxUnavailable
		if maxUnavailable <= 0 {
			maxUnavailable = 1
		}

		maxAllowedTotal := desired + maxSurge
		minAllowedHealthy := desired - maxUnavailable
		if minAllowedHealthy < 1 && desired > 0 {
			minAllowedHealthy = 1
		}

		// A. Surge step: spawn target version instances if needed and within maxSurge
		// Backoff and retired target slots still belong to this deployment;
		// creating fresh instances here would bypass its restart policy.
		updatedAllocated := len(updatedActiveInstances) + updatedRecoveringCount + retiredCount
		allocated := len(activeInstances) + len(recoveringInstances) + stoppingCount + drainingCount + retiredCount
		if updatedAllocated < desired && allocated < maxAllowedTotal {
			surgeCount := maxAllowedTotal - allocated
			neededUpdated := desired - updatedAllocated
			toSpawn := surgeCount
			if neededUpdated < toSpawn {
				toSpawn = neededUpdated
			}
			for i := 0; i < toSpawn; i++ {
				instID := fmt.Sprintf("%s-%s", svc.ID, s.idGen()[:6])
				instAddr := ipc.NewInstanceAddress(svc.TenantID, svc.Namespace, svc.AgentID, instID)
				newInstance := &Instance{
					ID:            instID,
					ServiceID:     svc.ID,
					TenantID:      svc.TenantID,
					Namespace:     svc.Namespace,
					AgentID:       svc.AgentID,
					AgentVersion:  targetVersion,
					RuntimeClass:  svc.Spec.RuntimeClass,
					Address:       instAddr,
					Phase:         InstanceRunning,
					LastHeartbeat: now,
					CreatedAt:     now,
					UpdatedAt:     now,
				}
				s.prepareLaunch(svc, newInstance)
				if err := s.store.CreateInstance(ctx, newInstance); err != nil {
					return err
				}
				if err := s.spawn(ctx, svc, newInstance); err != nil {
					return err
				}
				activeInstances = append(activeInstances, newInstance)
				if newInstance.Phase == InstanceRunning {
					readyInstances = append(readyInstances, newInstance)
				}
				updatedActiveInstances = append(updatedActiveInstances, newInstance)
			}
		}

		// B. Drain step: if we meet minimum healthy requirement, drain outdated instances
		healthyCount := len(readyInstances)
		canDrain := healthyCount >= minAllowedHealthy
		if _, managed := s.spawner.(interface {
			RefreshInstance(context.Context, *Service, *Instance) error
		}); managed {
			updatedReady := 0
			for _, inst := range updatedActiveInstances {
				if inst.Phase == InstanceRunning {
					updatedReady++
				}
			}
			canDrain = updatedReady > 0 && healthyCount-1 >= minAllowedHealthy
		}
		if canDrain && len(outdatedActiveInstances) > 0 {
			instToDrain := outdatedActiveInstances[0]
			instToDrain.Phase = InstanceDraining
			instToDrain.DrainingAt = &now
			drainTimeout := svc.Spec.DrainTimeout
			if drainTimeout <= 0 {
				drainTimeout = 30 * time.Second
			}
			deadline := now.Add(drainTimeout)
			instToDrain.DrainDeadline = &deadline
			instToDrain.UpdatedAt = now
			if err := s.store.UpdateInstance(ctx, instToDrain); err != nil {
				return err
			}
		}
	} else {
		// 6. Normal Replica Balancing (Scale Up / Graceful Scale Down)
		allocated := len(activeInstances) + len(recoveringInstances) + stoppingCount + drainingCount + retiredCount
		if allocated < desired {
			needed := desired - allocated
			for i := 0; i < needed; i++ {
				instID := fmt.Sprintf("%s-%s", svc.ID, s.idGen()[:6])
				instAddr := ipc.NewInstanceAddress(svc.TenantID, svc.Namespace, svc.AgentID, instID)
				newInstance := &Instance{
					ID:            instID,
					ServiceID:     svc.ID,
					TenantID:      svc.TenantID,
					Namespace:     svc.Namespace,
					AgentID:       svc.AgentID,
					AgentVersion:  targetVersion,
					RuntimeClass:  svc.Spec.RuntimeClass,
					Address:       instAddr,
					Phase:         InstanceRunning,
					LastHeartbeat: now,
					CreatedAt:     now,
					UpdatedAt:     now,
				}
				s.prepareLaunch(svc, newInstance)
				if err := s.store.CreateInstance(ctx, newInstance); err != nil {
					return err
				}
				if err := s.spawn(ctx, svc, newInstance); err != nil {
					return err
				}
				activeInstances = append(activeInstances, newInstance)
				if newInstance.Phase == InstanceRunning {
					readyInstances = append(readyInstances, newInstance)
				}
			}
		} else if len(activeInstances) > desired {
			excess := len(activeInstances) - desired
			for i := 0; i < excess; i++ {
				instToStop := activeInstances[len(activeInstances)-1-i]
				if svc.Spec.DrainTimeout > 0 {
					instToStop.Phase = InstanceDraining
					instToStop.DrainingAt = &now
					deadline := now.Add(svc.Spec.DrainTimeout)
					instToStop.DrainDeadline = &deadline
					instToStop.UpdatedAt = now
					if err := s.store.UpdateInstance(ctx, instToStop); err != nil {
						return err
					}
				} else {
					if err := s.stop(ctx, svc, instToStop, "scaled down"); err != nil {
						return err
					}
				}
			}
			activeInstances = activeInstances[:desired]
		}
	}

	// 7. Update overall service status
	readyCount := 0
	updatedCount := 0
	for _, inst := range activeInstances {
		if inst.Phase == InstanceRunning {
			readyCount++
			if targetVersion == "" || inst.AgentVersion == targetVersion {
				updatedCount++
			}
		}
	}

	svc.Status.DesiredReplicas = desired
	svc.Status.CurrentReplicas = len(activeInstances)
	svc.Status.ReadyReplicas = readyCount
	svc.Status.UpdatedReplicas = updatedCount
	svc.Status.AvailableReplicas = readyCount
	svc.Status.ActiveVersion = targetVersion

	if desired == 0 {
		if svc.Status.Phase != ServiceSuspended {
			svc.Status.Phase = ServiceSuspended
			svc.Status.LastTransitionAt = now
			svc.Status.Message = "Service has 0 desired replicas (idle/suspended)"
		}
	} else if isRolling {
		if svc.Status.Phase != ServiceDegraded {
			svc.Status.Phase = ServiceDegraded
			svc.Status.LastTransitionAt = now
			svc.Status.Message = fmt.Sprintf("Rolling update to %s in progress (%d/%d updated)", targetVersion, updatedCount, desired)
		}
	} else if readyCount == desired {
		if svc.Status.Phase != ServiceActive {
			svc.Status.Phase = ServiceActive
			svc.Status.LastTransitionAt = now
			svc.Status.Message = fmt.Sprintf("All %d replicas active and healthy", readyCount)
		}
	} else if readyCount > 0 {
		if svc.Status.Phase != ServiceDegraded {
			svc.Status.Phase = ServiceDegraded
			svc.Status.LastTransitionAt = now
			svc.Status.Message = fmt.Sprintf("%d of %d replicas healthy", readyCount, desired)
		}
	} else {
		// readyCount == 0 && desired > 0
		if svc.Status.Phase != ServiceDegraded && svc.Status.Phase != ServicePending {
			svc.Status.Phase = ServiceDegraded
			svc.Status.LastTransitionAt = now
			svc.Status.Message = "No healthy replicas running"
		}
	}

	svc.UpdatedAt = now
	return s.store.UpdateService(ctx, svc)
}

// checkPendingMessages inspects if the agent's mailbox has pending unread messages for AutoWake.
func (s *Supervisor) checkPendingMessages(ctx context.Context, svc *Service) bool {
	if s.mailbox == nil {
		return false
	}
	logicalAddr := svc.LogicalAddress()
	msgs, err := s.mailbox.Receive(ctx, logicalAddr, 1)
	if err == nil && len(msgs) > 0 {
		return true
	}
	return false
}
