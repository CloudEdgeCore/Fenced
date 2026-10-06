// Package attemptstate helps Runtime Protocol workers converge after an
// optimistic-concurrency conflict without weakening fencing.
package attemptstate

import (
	"context"
	"fmt"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
)

// Snapshot is the current fenced Attempt state returned by the control plane.
type Snapshot struct {
	Version int64
	Phase   domain.AttemptPhase
}

// Settled reports whether another control-plane actor has already made the
// Attempt terminal or requested cancellation. A worker must not overwrite
// either state while converging an older mutation.
func (s Snapshot) Settled() bool {
	return s.Phase == domain.AttemptCancelRequested || s.Phase.Terminal()
}

// Refresh re-reads an Attempt through its fenced identity. The control plane
// validates tenant and fencing-token ownership, so a stale worker cannot use
// this helper to recover ownership after a newer attempt has been installed.
func Refresh(
	ctx context.Context,
	client runtimev1.RuntimeControlServiceClient,
	identity *runtimev1.AttemptIdentity,
	timeout time.Duration,
) (Snapshot, error) {
	var zero Snapshot
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := client.GetAssignment(rpcCtx, &runtimev1.GetAssignmentRequest{Identity: identity})
	if err != nil {
		return zero, fmt.Errorf("refresh fenced runtime assignment: %w", err)
	}
	assignment := response.GetAssignment()
	if assignment == nil || assignment.GetIdentity() == nil || assignment.GetAttemptVersion() <= 0 {
		return zero, fmt.Errorf("refreshed runtime assignment is incomplete")
	}
	if assignment.GetIdentity().GetTenantId() != identity.GetTenantId() ||
		assignment.GetIdentity().GetAttemptId() != identity.GetAttemptId() ||
		assignment.GetIdentity().GetFencingToken() != identity.GetFencingToken() {
		return zero, fmt.Errorf("refreshed runtime assignment identity changed")
	}
	return Snapshot{Version: assignment.GetAttemptVersion(), Phase: domain.AttemptPhase(assignment.GetPhase())}, nil
}
