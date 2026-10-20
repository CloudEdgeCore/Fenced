package supervisor

import (
	"context"
	"fmt"
	"time"
)

// withService makes an API mutation and its reconciliation one serialized
// database transaction. Task creation and cancellation use that same scope.
func (s *Supervisor) withService(ctx context.Context, tenant, id string, fn func(*Supervisor) error) error {
	locker, ok := s.store.(ServiceLockStore)
	if !ok || s.scoped {
		return fn(s)
	}
	return locker.WithServiceLock(ctx, tenant, id, func(store Store) error {
		spawner := s.spawner
		if binder, ok := spawner.(interface{ Rebind(Store) InstanceSpawner }); ok {
			spawner = binder.Rebind(store)
		}
		scoped := &Supervisor{store: store, spawner: spawner, ipcSvc: s.ipcSvc,
			mailbox: s.mailbox, clock: s.clock, idGen: s.idGen, scoped: true}
		return fn(scoped)
	})
}

func (s *Supervisor) CreateService(ctx context.Context, svc *Service) (*Service, error) {
	if svc == nil {
		return nil, ErrInvalidServiceSpec
	}
	if svc.ID == "" {
		svc.ID = "svc-" + s.idGen()[:8]
	}
	var result *Service
	err := s.withService(ctx, svc.TenantID, svc.ID, func(scoped *Supervisor) (err error) {
		result, err = scoped.createService(ctx, svc)
		return err
	})
	return result, err
}

func (s *Supervisor) UpdateService(ctx context.Context, svc *Service) (*Service, error) {
	if svc == nil {
		return nil, ErrInvalidServiceSpec
	}
	var result *Service
	err := s.withService(ctx, svc.TenantID, svc.ID, func(scoped *Supervisor) (err error) {
		result, err = scoped.updateService(ctx, svc)
		return err
	})
	return result, err
}

func (s *Supervisor) ScaleService(ctx context.Context, tenant, id string, replicas int) (*Service, error) {
	var result *Service
	err := s.withService(ctx, tenant, id, func(scoped *Supervisor) (err error) {
		result, err = scoped.scaleService(ctx, tenant, id, replicas)
		return err
	})
	return result, err
}

func (s *Supervisor) RestartService(ctx context.Context, tenant, id string) error {
	return s.withService(ctx, tenant, id, func(scoped *Supervisor) error { return scoped.restartService(ctx, tenant, id) })
}

func (s *Supervisor) StopService(ctx context.Context, tenant, id string) error {
	return s.withService(ctx, tenant, id, func(scoped *Supervisor) error { return scoped.stopService(ctx, tenant, id) })
}

func (s *Supervisor) DeleteService(ctx context.Context, tenant, id string) error {
	return s.withService(ctx, tenant, id, func(scoped *Supervisor) error { return scoped.deleteService(ctx, tenant, id) })
}

func (s *Supervisor) RecordHeartbeat(ctx context.Context, tenant, service, instance string) error {
	return s.withService(ctx, tenant, service, func(scoped *Supervisor) error { return scoped.recordHeartbeat(ctx, tenant, service, instance) })
}

func (s *Supervisor) ReportInstanceExit(ctx context.Context, tenant, service, instance string, code int, reason string) error {
	return s.withService(ctx, tenant, service, func(scoped *Supervisor) error {
		return scoped.reportInstanceExit(ctx, tenant, service, instance, code, reason)
	})
}

func (s *Supervisor) DrainInstance(ctx context.Context, tenant, service, instance string, timeout time.Duration) error {
	return s.withService(ctx, tenant, service, func(scoped *Supervisor) error { return scoped.drainInstance(ctx, tenant, service, instance, timeout) })
}

func (s *Supervisor) RolloutUpgrade(ctx context.Context, tenant, id, version string) (*Service, error) {
	var result *Service
	err := s.withService(ctx, tenant, id, func(scoped *Supervisor) (err error) {
		result, err = scoped.rolloutUpgrade(ctx, tenant, id, version)
		return err
	})
	return result, err
}

// RolloutUpgradeWithConfig starts a rollout with an explicit configuration.
// The configuration overrides the rollout settings of the service; the frozen
// service contract carries the same override in RolloutUpgradeRequest.config.
func (s *Supervisor) RolloutUpgradeWithConfig(ctx context.Context, tenant, id, version string, config *RolloutConfig) (*Service, error) {
	var result *Service
	err := s.withService(ctx, tenant, id, func(scoped *Supervisor) (err error) {
		result, err = scoped.rolloutUpgradeWithConfig(ctx, tenant, id, version, config)
		return err
	})
	return result, err
}

func (s *Supervisor) RollbackService(ctx context.Context, tenant, id string) (*Service, error) {
	var result *Service
	err := s.withService(ctx, tenant, id, func(scoped *Supervisor) (err error) {
		result, err = scoped.rollbackService(ctx, tenant, id)
		return err
	})
	return result, err
}

func (s *Supervisor) ReconcileService(ctx context.Context, svc *Service) error {
	if svc == nil {
		return nil
	}
	return s.withService(ctx, svc.TenantID, svc.ID, func(scoped *Supervisor) error {
		// The caller's snapshot may predate a scale, stop, or version update.
		current, err := scoped.store.GetService(ctx, svc.TenantID, svc.ID)
		if err != nil {
			return err
		}
		return scoped.reconcileService(ctx, current)
	})
}

func (s *Supervisor) validateLaunch(ctx context.Context, svc *Service) error {
	if validator, ok := s.spawner.(interface {
		ValidateService(context.Context, *Service) error
	}); ok {
		return validator.ValidateService(ctx, svc)
	}
	return nil
}

func (s *Supervisor) prepareLaunch(svc *Service, inst *Instance) {
	if launcher, ok := s.spawner.(interface{ PrepareInstance(*Service, *Instance) }); ok {
		launcher.PrepareInstance(svc, inst)
	}
}

func (s *Supervisor) spawn(ctx context.Context, svc *Service, inst *Instance) error {
	if err := s.spawner.SpawnInstance(ctx, svc, inst); err != nil {
		return fmt.Errorf("launch service instance %s: %w", inst.ID, err)
	}
	return s.store.UpdateInstance(ctx, inst)
}

func (s *Supervisor) stop(ctx context.Context, svc *Service, inst *Instance, reason string) error {
	inst.ExitReason = reason
	if _, managed := s.spawner.(interface {
		RefreshInstance(context.Context, *Service, *Instance) error
	}); managed {
		inst.Phase = InstanceStopping
		inst.TerminatedAt = nil
	} else {
		inst.Phase = InstanceStopped
		now := s.clock()
		inst.TerminatedAt = &now
	}
	inst.UpdatedAt = s.clock()
	if err := s.store.UpdateInstance(ctx, inst); err != nil {
		return err
	}
	if err := s.spawner.StopInstance(ctx, svc, inst); err != nil {
		return err
	}
	return s.store.UpdateInstance(ctx, inst)
}

func (s *Supervisor) finishDeletion(ctx context.Context, svc *Service) error {
	instances, err := s.store.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil {
		return err
	}
	allStopped := true
	for _, inst := range instances {
		if !inst.IsTerminal() && inst.Phase != InstanceStopping {
			if err := s.stop(ctx, svc, inst, "service deleted"); err != nil {
				return err
			}
		}
		if refresher, ok := s.spawner.(interface {
			RefreshInstance(context.Context, *Service, *Instance) error
		}); ok && !inst.IsTerminal() {
			if err := refresher.RefreshInstance(ctx, svc, inst); err != nil {
				return err
			}
		}
		if !inst.IsTerminal() {
			if err := s.stop(ctx, svc, inst, "service deleted"); err != nil {
				return err
			}
		}
		if !inst.IsTerminal() {
			allStopped = false
		}
		if err := s.store.UpdateInstance(ctx, inst); err != nil {
			return err
		}
	}
	if allStopped {
		return s.store.DeleteService(ctx, svc.TenantID, svc.ID)
	}
	return nil
}

func plannedStop(reason string) bool {
	return reason == "scaled down" || reason == "service stopped" || reason == "drained" || reason == "service deleted"
}
