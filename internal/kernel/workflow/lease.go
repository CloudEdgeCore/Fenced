package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// workflowClaimRenewer is the sub-interface required for lease renewal; it
// isolates LeaseManager from the full WorkflowStore surface.
type workflowClaimRenewer interface {
	RenewWorkflowClaim(context.Context, string, uuid.UUID, string, time.Duration) error
}

// LeaseManager keeps a claimed workflow's lease fresh while the controller
// reconciles it. If renewal fails the work context is cancelled so the
// instance stops processing a claim it no longer owns, allowing a peer to take
// over without waiting for full lease expiry.
type LeaseManager struct {
	renewer workflowClaimRenewer
	owner   string
	lease   time.Duration
}

// NewLeaseManager builds a LeaseManager bound to one controller instance's
// identity. A nil renewer or zero lease makes Guard a no-op.
func NewLeaseManager(renewer workflowClaimRenewer, owner string, lease time.Duration) *LeaseManager {
	return &LeaseManager{renewer: renewer, owner: owner, lease: lease}
}

// Guard wraps ctx so the caller's work is cancelled when the claim lease can
// no longer be renewed. The returned release must be called when the work
// completes; its error is non-nil when renewal failed while the work was
// running, using the same error semantics as the original Controller:
//
//   - If the work errored AND renewal failed → errors.Join(workErr, claimErr)
//   - If the work succeeded AND renewal failed → fmt.Errorf("renew …")
//   - If renewal succeeded → workErr (nil or not)
func (m *LeaseManager) Guard(ctx context.Context, workflow kernelstore.Workflow) (workCtx context.Context, release func() error) {
	if m.renewer == nil || m.lease <= 0 {
		return ctx, func() error { return nil }
	}
	workCtx, cancel := context.WithCancel(ctx)
	renewErr := make(chan error, 1)
	interval := m.lease / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := m.renewer.RenewWorkflowClaim(workCtx, workflow.TenantID, workflow.ID, m.owner, m.lease); err != nil {
					select {
					case renewErr <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	return workCtx, func() error {
		cancel()
		select {
		case claimErr := <-renewErr:
			return claimErr
		default:
			return nil
		}
	}
}

// GuardedProcess runs fn under lease guard and returns the combined error:
// work error + renewal error with the same join semantics as the original
// Controller.processClaimedWorkflow.
func (m *LeaseManager) GuardedProcess(ctx context.Context, workflow kernelstore.Workflow, fn func(context.Context) (bool, error)) (bool, error) {
	workCtx, release := m.Guard(ctx, workflow)
	processed, err := fn(workCtx)
	if claimErr := release(); claimErr != nil {
		if err != nil {
			return processed, errors.Join(err, claimErr)
		}
		return processed, fmt.Errorf("renew workflow claim: %w", claimErr)
	}
	return processed, err
}
