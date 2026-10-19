// The fenced protocol calls of the adapter worker: the Runtime Control
// service exchanges and the artifact conversions that carry their results.
package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	domain "github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	store "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	attemptstate "github.com/CloudEdgeCore/Fenced/internal/runtime/attemptstate"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

func (w *Worker) heartbeat(ctx context.Context, assignment *runtimev1.Assignment) (*runtimev1.HeartbeatResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	response, err := w.control.Heartbeat(rpcCtx, &runtimev1.HeartbeatRequest{
		Identity: assignment.GetIdentity(), ExpectedLeaseVersion: assignment.GetLeaseVersion(),
		IdempotencyKey: operationKey(assignment, "heartbeat"), RequestedTtlSeconds: int64(w.heartbeatTTL / time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("renew adapter lease: %w", err)
	}
	if response.GetCancelRequested() {
		if err := w.acknowledgeCancellation(ctx, assignment.GetIdentity(), response.GetAttemptVersion()); err != nil {
			return nil, fmt.Errorf("acknowledge adapter cancellation: %w", err)
		}
	}
	return response, nil
}

func (w *Worker) transition(
	ctx context.Context,
	identity *runtimev1.AttemptIdentity,
	version int64,
	phase runtimev1.AttemptPhase,
	failureCode, failureMessage string,
) (int64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	response, err := w.control.TransitionAttempt(rpcCtx, &runtimev1.TransitionAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: identity.GetAttemptId() + ":" + strings.ToLower(phase.String()),
		TargetPhase:    phase, FailureCode: failureCode, FailureMessage: failureMessage,
	})
	if err != nil {
		return 0, fmt.Errorf("transition adapter attempt to %s: %w", phase, err)
	}
	return response.GetAttemptVersion(), nil
}

// advanceAttempt converges a normal lifecycle transition after an optimistic
// concurrency conflict without weakening fencing. A conflict is retried only
// while the fenced Attempt remains in the expected predecessor phase. If the
// requested transition already committed, its refreshed version is accepted;
// cancellation, terminal state, or an unrelated phase always wins and tells
// the caller not to start another execution for the same assignment.
func (w *Worker) advanceAttempt(
	ctx context.Context,
	identity *runtimev1.AttemptIdentity,
	version int64,
	from, to domain.AttemptPhase,
	protoPhase runtimev1.AttemptPhase,
) (int64, bool, error) {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		nextVersion, err := w.transition(ctx, identity, version, protoPhase, "", "")
		if err == nil {
			return nextVersion, true, nil
		}
		// The assignment lease expired or a recovery controller installed a
		// newer fenced owner. This worker no longer owns the Attempt; dropping
		// the assignment is successful convergence, not a process-fatal error.
		if status.Code(err) == codes.PermissionDenied {
			return 0, false, nil
		}
		if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return 0, false, err
		}
		current, refreshErr := attemptstate.Refresh(ctx, w.control, identity, controlRPCTimeout)
		if refreshErr != nil {
			return 0, false, fmt.Errorf("converge adapter attempt to %s: %w", protoPhase, refreshErr)
		}
		switch {
		case current.Phase == domain.AttemptCancelRequested:
			if cancelErr := w.acknowledgeCancellation(ctx, identity, current.Version); cancelErr != nil {
				return 0, false, fmt.Errorf("converge adapter cancellation: %w", cancelErr)
			}
			return current.Version, false, nil
		case current.Phase.Terminal():
			return current.Version, false, nil
		case current.Phase == to:
			return current.Version, true, nil
		case current.Phase == from:
			version = current.Version
		default:
			return current.Version, false, nil
		}
	}
}

// acknowledgeCancellation settles a cancellation observed before or during
// execution. It converges a stale Attempt version through the fenced read API
// so cancellation cannot strand an assignment between placement and the first
// heartbeat, while a terminal or unrelated control-plane state always wins.
func (w *Worker) acknowledgeCancellation(ctx context.Context, identity *runtimev1.AttemptIdentity, version int64) error {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
		_, err := w.control.AcknowledgeCancellation(rpcCtx, &runtimev1.AcknowledgeCancellationRequest{
			Identity: identity, ExpectedAttemptVersion: version,
			IdempotencyKey: identity.GetAttemptId() + ":adapter:cancel",
		})
		cancel()
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return err
		}
		current, refreshErr := attemptstate.Refresh(ctx, w.control, identity, controlRPCTimeout)
		if refreshErr != nil {
			return refreshErr
		}
		if current.Phase.Terminal() || current.Phase != domain.AttemptCancelRequested {
			return nil
		}
		version = current.Version
	}
}

func (w *Worker) fail(
	ctx context.Context,
	identity *runtimev1.AttemptIdentity,
	version int64,
	code string,
	cause error,
) (bool, error) {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		if _, err := w.transition(ctx, identity, version,
			runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED, code, cause.Error()); err == nil {
			return true, nil
		} else if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return false, fmt.Errorf("mark adapter attempt failed (%s): %w", code, err)
		}
		// Approval, checkpoint, cancellation, and deadline controllers may
		// legitimately advance the Attempt while execution is in flight. A
		// definite CAS rejection is safe to resolve by re-reading through the
		// same fenced identity. Cancellation/terminal state always wins.
		current, err := attemptstate.Refresh(ctx, w.control, identity, controlRPCTimeout)
		if err != nil {
			return false, fmt.Errorf("mark adapter attempt failed (%s): %w", code, err)
		}
		if current.Settled() {
			return true, nil
		}
		version = current.Version
	}
}

func operationKey(assignment *runtimev1.Assignment, operation string) string {
	return assignment.GetIdentity().GetAttemptId() + ":adapter:" + operation
}

func artifactProto(reference store.ArtifactReference) *runtimev1.ArtifactReference {
	return &runtimev1.ArtifactReference{
		Uri: reference.URI, Sha256: reference.DigestHex(), SizeBytes: reference.SizeBytes, MediaType: reference.MediaType,
	}
}

func artifactFromProto(reference *runtimev1.ArtifactReference) (store.ArtifactReference, error) {
	var result store.ArtifactReference
	if reference == nil {
		return result, fmt.Errorf("checkpoint artifact reference is required")
	}
	digest, err := hex.DecodeString(reference.GetSha256())
	if err != nil || len(digest) != sha256.Size {
		return result, fmt.Errorf("checkpoint artifact digest is invalid")
	}
	copy(result.SHA256[:], digest)
	result.URI, result.SizeBytes, result.MediaType = reference.GetUri(), reference.GetSizeBytes(), reference.GetMediaType()
	return result, result.Validate()
}

// workflowLineage is the parsed workflow origin of one assignment
// (workflow_id/step_name/version); workflowID is nil for standalone tasks.
