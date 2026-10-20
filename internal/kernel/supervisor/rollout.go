package supervisor

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// The rollout engine moves a service from one agent version to another
// without dropping the service. Two rules hold the line:
//
//  1. A serving instance is only replaced after its replacement has proven
//     itself. The reconciler counts an instance as proven once it has been
//     observed healthy, without interruption, for the configured minimum
//     time. Draining the old instance earlier would trade a working replica
//     for one that has never run.
//
//  2. A rollout that fails does not leave the service on a version that does
//     not work. When the service asked for an automatic rollback, the target
//     version is reverted and the reconciler brings the service back to the
//     version it ran before.
//
// The state of a rollout lives in the durable service status, so a controller
// restart resumes the rollout instead of forgetting it.

// provenReady reports whether the instance has been healthy for the minimum
// time a rollout requires before it may take the place of a serving
// instance.
func provenReady(inst *Instance, minReady time.Duration, now time.Time) bool {
	if inst == nil || inst.Phase != InstanceRunning || inst.ReadyAt == nil {
		return false
	}
	return now.Sub(*inst.ReadyAt) >= minReady
}

// startRollout records the movement between two versions in the durable
// service status.
func startRollout(svc *Service, from, to string, now time.Time) {
	svc.Status.RolloutPhase = RolloutPhaseProgressing
	svc.Status.RolloutFromVersion = from
	svc.Status.RolloutTargetVersion = to
	svc.Status.RolloutStartedAt = now
	svc.Status.RolloutUpdatedAt = now
	svc.Status.RolloutFailureReason = ""
}

// failRollout records a failed rollout. When the service asked for an
// automatic rollback, the target version is reverted here and the reconciler
// rolls the service back; otherwise the service keeps the failed version and
// the operator decides.
func (s *Supervisor) failRollout(ctx context.Context, svc *Service, reason string, now time.Time) error {
	svc.Status.RolloutFailureReason = reason
	svc.Status.RolloutUpdatedAt = now
	failed := serviceVersion(svc)

	previous := strings.TrimSpace(svc.Status.RolloutFromVersion)
	if !svc.Spec.Rollout.AutoRollback || previous == "" {
		svc.Status.RolloutPhase = RolloutPhaseFailed
		svc.Status.Message = fmt.Sprintf("rollout to %s failed: %s", failed, reason)
		return s.store.UpdateService(ctx, svc)
	}

	// Revert the target version. The previous version must still be a
	// launchable one; if it is not, the operator has to intervene.
	failedVersion, failedRef := svc.Spec.AgentVersion, svc.Spec.AgentVersionRef
	svc.Spec.AgentVersion, svc.Spec.AgentVersionRef = previous, previous
	if err := s.validateLaunch(ctx, svc); err != nil {
		svc.Spec.AgentVersion, svc.Spec.AgentVersionRef = failedVersion, failedRef
		svc.Status.RolloutPhase = RolloutPhaseFailed
		svc.Status.Message = fmt.Sprintf("rollout to %s failed: %s; the previous version %s cannot be launched: %v",
			failed, reason, previous, err)
		return s.store.UpdateService(ctx, svc)
	}

	svc.Status.PreviousVersion = failed
	svc.Status.RolloutPhase = RolloutPhaseRollingBack
	svc.Status.Message = fmt.Sprintf("rollout to %s failed: %s; rolling back to %s", failed, reason, previous)
	return s.store.UpdateService(ctx, svc)
}

// advanceRollout inspects the instances of a rollout in progress. A target
// version that fails ends the rollout. The check runs before the crash
// recovery path, so a failed version is not resurrected by the restart
// policy before the rollout is judged.
func (s *Supervisor) advanceRollout(ctx context.Context, svc *Service, instances []*Instance, now time.Time) error {
	if svc.Status.RolloutPhase != RolloutPhaseProgressing {
		return nil
	}
	target := serviceVersion(svc)
	for _, inst := range instances {
		if inst.AgentVersion != target {
			continue
		}
		if inst.Phase == InstanceFailed {
			reason := fmt.Sprintf("instance %s failed", inst.ID)
			if detail := strings.TrimSpace(inst.ExitReason); detail != "" {
				reason += ": " + detail
			}
			return s.failRollout(ctx, svc, reason, now)
		}
	}
	return nil
}

// completeRollout closes a rollout that converged: every replica runs the
// target version and is healthy. The same check closes an automatic and an
// explicit rollback, because both end on the version the service runs now.
func completeRollout(svc *Service, now time.Time) {
	switch svc.Status.RolloutPhase {
	case RolloutPhaseProgressing, RolloutPhaseRollingBack:
		desired := svc.Status.DesiredReplicas
		if desired > 0 &&
			svc.Status.UpdatedReplicas >= desired &&
			svc.Status.ReadyReplicas >= desired {
			if svc.Status.RolloutPhase == RolloutPhaseProgressing {
				svc.Status.RolloutPhase = RolloutPhaseSucceeded
				svc.Status.Message = fmt.Sprintf("Rolled out to %s", svc.Status.RolloutTargetVersion)
			} else {
				svc.Status.RolloutPhase = RolloutPhaseRolledBack
				svc.Status.Message = fmt.Sprintf("Rolled back to %s", svc.Status.RolloutFromVersion)
			}
			svc.Status.RolloutUpdatedAt = now
		}
	}
}
