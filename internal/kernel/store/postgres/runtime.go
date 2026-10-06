package postgres

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ kernelstore.RuntimeStore = (*Store)(nil)
var _ kernelstore.TaskCancellationStore = (*Store)(nil)

func (s *Store) RequestTaskCancellation(ctx context.Context, tenantID string, taskID uuid.UUID, expectedVersion int64) (kernelstore.Task, error) {
	var zero kernelstore.Task
	if tenantID == "" || taskID == uuid.Nil || expectedVersion <= 0 {
		return zero, fmt.Errorf("tenant, task, and expected version are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	var activeRunID sql.NullString
	if err := tx.QueryRow(ctx, `SELECT active_run_id::text FROM tasks WHERE tenant_id = $1 AND id = $2`,
		tenantID, taskID.String()).Scan(&activeRunID); err != nil {
		return zero, classify(err)
	}
	now := s.now()
	if !activeRunID.Valid {
		task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks
			WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, taskID.String()))
		if err != nil {
			return zero, classify(err)
		}
		if task.ResourceVersion != expectedVersion {
			return zero, versionConflict("task", task.ID, expectedVersion, task.ResourceVersion)
		}
		if task.Phase == domain.TaskCancelled {
			if err := tx.Commit(ctx); err != nil {
				return zero, classify(err)
			}
			return task, nil
		}
		if err := domain.ValidateTaskTransition(task.Phase, domain.TaskCancelled); err != nil {
			return zero, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
		}
		updated, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'CANCELLED', cancel_requested_at = $1,
			resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
			now, tenantID, task.ID.String(), expectedVersion))
		if err != nil {
			return zero, classifyCAS(err, "task", task.ID, expectedVersion)
		}
		// Quota reservation release (v0.8): an admitted task cancelled before
		// scheduling returns its reserved ceiling.
		if err := s.releaseTenantReservation(ctx, tx, tenantID, task.ID); err != nil {
			return zero, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM task_controller_claims WHERE tenant_id = $1 AND task_id = $2`, tenantID, task.ID.String()); err != nil {
			return zero, classify(err)
		}
		if err := insertEvent(ctx, tx, tenantID, "Task", task.ID, updated.ResourceVersion, "TaskCancelled", map[string]any{
			"taskId": task.ID, "phase": domain.TaskCancelled,
		}, now, s.newID()); err != nil {
			return zero, err
		}
		if err := auditHook(ctx, tx, tenantID, "task.cancelled", "Task", task.ID, map[string]any{
			"phase": string(domain.TaskCancelled),
		}, now); err != nil {
			return zero, err
		}
		if err := tx.Commit(ctx); err != nil {
			return zero, classify(err)
		}
		return updated, nil
	}

	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, activeRunID.String))
	if err != nil {
		return zero, classify(err)
	}
	if run.ActiveAttemptID == nil {
		// A runtime may have acknowledged cancellation after the caller read
		// the task but before this transaction locked the run. Surface the
		// stale snapshot as a normal CAS conflict; an up-to-date replay of an
		// already-cancelled task is idempotent. Returning an invalid lifecycle
		// error here strands workflow cancellation controllers.
		task, taskErr := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks
			WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, taskID.String()))
		if taskErr != nil {
			return zero, classify(taskErr)
		}
		if task.ResourceVersion != expectedVersion {
			return zero, versionConflict("task", task.ID, expectedVersion, task.ResourceVersion)
		}
		if task.Phase == domain.TaskCancelled {
			if err := tx.Commit(ctx); err != nil {
				return zero, classify(err)
			}
			return task, nil
		}
		return zero, fmt.Errorf("%w: running task has no active attempt", kernelstore.ErrInvalidTransition)
	}
	attempt, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, run.ActiveAttemptID.String()))
	if err != nil {
		return zero, classify(err)
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, taskID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if task.ResourceVersion != expectedVersion {
		return zero, versionConflict("task", task.ID, expectedVersion, task.ResourceVersion)
	}
	if task.ActiveRunID == nil || *task.ActiveRunID != run.ID || task.Phase != domain.TaskRunning {
		return zero, fmt.Errorf("%w: task is not running the locked run", kernelstore.ErrInvalidTransition)
	}
	if task.CancelRequestedAt != nil {
		if err := tx.Commit(ctx); err != nil {
			return zero, classify(err)
		}
		return task, nil
	}
	if err := domain.ValidateAttemptTransition(attempt.Phase, domain.AttemptCancelRequested); err != nil {
		return zero, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	updatedAttempt, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'CANCEL_REQUESTED',
		resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+attemptColumns,
		now, tenantID, attempt.ID.String(), attempt.ResourceVersion))
	if err != nil {
		return zero, classifyCAS(err, "attempt", attempt.ID, attempt.ResourceVersion)
	}
	updatedTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET cancel_requested_at = $1,
		resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
		now, tenantID, task.ID.String(), expectedVersion))
	if err != nil {
		return zero, classifyCAS(err, "task", task.ID, expectedVersion)
	}
	if err := insertEvent(ctx, tx, tenantID, "Attempt", attempt.ID, updatedAttempt.ResourceVersion, "AttemptCancelRequested", map[string]any{
		"taskId": task.ID, "runId": run.ID, "attemptId": attempt.ID, "fencingToken": attempt.FencingToken,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := insertEvent(ctx, tx, tenantID, "Task", task.ID, updatedTask.ResourceVersion, "TaskCancelRequested", map[string]any{
		"taskId": task.ID, "runId": run.ID, "attemptId": attempt.ID,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := auditHook(ctx, tx, tenantID, "task.cancellation.requested", "Task", task.ID, map[string]any{
		"runId": run.ID, "attemptId": attempt.ID,
	}, now); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updatedTask, nil
}

func (s *Store) PollRuntimeAssignment(ctx context.Context, tenantID, runtimeInstanceID string) (kernelstore.RuntimeAssignment, error) {
	if tenantID == "" || runtimeInstanceID == "" {
		return kernelstore.RuntimeAssignment{}, fmt.Errorf("tenant and runtime instance are required")
	}
	var attemptID string
	var fencingToken int64
	err := s.pool.QueryRow(ctx, `SELECT a.id::text, a.fencing_token
		FROM attempts a
		JOIN runs r ON r.tenant_id = a.tenant_id AND r.id = a.run_id
		JOIN runtime_leases l ON l.tenant_id = a.tenant_id AND l.attempt_id = a.id
		WHERE a.tenant_id = $1 AND a.runtime_instance_id = $2 AND a.phase IN ('PLACED', 'WAITING_APPROVAL')
		  AND r.active_attempt_id = a.id AND r.current_fencing_token = a.fencing_token
		  AND l.released_at IS NULL AND l.expires_at > $3
		ORDER BY a.created_at, a.id LIMIT 1`, tenantID, runtimeInstanceID, s.now()).Scan(&attemptID, &fencingToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.RuntimeAssignment{}, kernelstore.ErrNoAssignment
	}
	if err != nil {
		return kernelstore.RuntimeAssignment{}, classify(err)
	}
	id, err := uuid.Parse(attemptID)
	if err != nil {
		return kernelstore.RuntimeAssignment{}, fmt.Errorf("parse assignment attempt ID: %w", err)
	}
	return s.GetRuntimeAssignment(ctx, tenantID, id, fencingToken)
}

func (s *Store) GetRuntimeAssignment(ctx context.Context, tenantID string, attemptID uuid.UUID, fencingToken int64) (kernelstore.RuntimeAssignment, error) {
	var result kernelstore.RuntimeAssignment
	if tenantID == "" || attemptID == uuid.Nil || fencingToken <= 0 {
		return result, fmt.Errorf("tenant, attempt, and fencing token are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	result.Attempt, err = scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts
		WHERE tenant_id = $1 AND id = $2`, tenantID, attemptID.String()))
	if err != nil {
		return result, classify(err)
	}
	result.Run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs
		WHERE tenant_id = $1 AND id = $2`, tenantID, result.Attempt.RunID.String()))
	if err != nil {
		return result, classify(err)
	}
	if err := requireCurrent(result.Run, result.Attempt, fencingToken); err != nil {
		return result, err
	}
	result.Task, err = scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks
		WHERE tenant_id = $1 AND id = $2`, tenantID, result.Run.TaskID.String()))
	if err != nil {
		return result, classify(err)
	}
	if result.Task.AgentVersionID != nil {
		version, versionErr := scanAgentVersion(tx.QueryRow(ctx, `SELECT `+agentVersionColumns+`
			FROM agent_versions WHERE tenant_id = $1 AND id = $2`,
			tenantID, result.Task.AgentVersionID.String()))
		if versionErr != nil {
			return result, classify(versionErr)
		}
		result.AgentVersion = &version
	}
	result.Lease, err = scanLease(tx.QueryRow(ctx, `SELECT `+leaseColumns+` FROM runtime_leases
		WHERE tenant_id = $1 AND attempt_id = $2 AND fencing_token = $3 AND released_at IS NULL`,
		tenantID, attemptID.String(), fencingToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return result, fmt.Errorf("%w: active lease not found", kernelstore.ErrFenced)
	}
	if err != nil {
		return result, classify(err)
	}
	if !result.Lease.ExpiresAt.After(s.now()) {
		return result, fmt.Errorf("%w: lease expired at %s", kernelstore.ErrFenced, result.Lease.ExpiresAt.Format(time.RFC3339Nano))
	}
	checkpoint, err := scanCheckpoint(tx.QueryRow(ctx, checkpointSelect+`
		WHERE c.tenant_id = $1 AND c.run_id = $2
		ORDER BY c.ordinal DESC LIMIT 1`, tenantID, result.Run.ID.String()))
	if err == nil {
		result.ResumeCheckpoint = &checkpoint
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return result, classify(err)
	}
	if result.Attempt.Phase == domain.AttemptWaitingApproval {
		var approvalID string
		approvalErr := tx.QueryRow(ctx, `SELECT approval_id::text FROM tool_calls
			WHERE tenant_id = $1 AND attempt_id = $2 AND status = 'REQUIRES_APPROVAL' AND approval_id IS NOT NULL
			ORDER BY created_at DESC LIMIT 1`, tenantID, attemptID.String()).Scan(&approvalID)
		if approvalErr == nil {
			parsed, parseErr := uuid.Parse(approvalID)
			if parseErr != nil {
				return result, fmt.Errorf("parse pending approval ID: %w", parseErr)
			}
			result.PendingApprovalID = &parsed
		} else if !errors.Is(approvalErr, pgx.ErrNoRows) {
			return result, classify(approvalErr)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return result, nil
}

// GetHeartbeatStatus is the narrow lease-renewal read: the task's pending
// cancellation flag and the attempt's current resource version, both fenced
// against the run's current fencing token. Heartbeat uses this instead of
// re-materializing the full assignment on every renewal.
func (s *Store) GetHeartbeatStatus(ctx context.Context, tenantID string, attemptID uuid.UUID, fencingToken int64) (kernelstore.HeartbeatStatus, error) {
	var status kernelstore.HeartbeatStatus
	if tenantID == "" || attemptID == uuid.Nil || fencingToken <= 0 {
		return status, fmt.Errorf("tenant, attempt, and fencing token are required")
	}
	var cancelRequestedAt *time.Time
	err := s.pool.QueryRow(ctx, `SELECT a.resource_version, t.cancel_requested_at
		FROM attempts a
		JOIN runs r ON r.tenant_id = a.tenant_id AND r.id = a.run_id
		JOIN tasks t ON t.tenant_id = r.tenant_id AND t.id = r.task_id
		WHERE a.tenant_id = $1 AND a.id = $2 AND r.current_fencing_token = $3`,
		tenantID, attemptID.String(), fencingToken).Scan(&status.AttemptVersion, &cancelRequestedAt)
	if err != nil {
		return status, classify(err)
	}
	status.CancelRequested = cancelRequestedAt != nil
	return status, nil
}

func (s *Store) CommitCheckpoint(ctx context.Context, input kernelstore.CommitCheckpointInput) (kernelstore.Checkpoint, kernelstore.Attempt, error) {
	var zeroCheckpoint kernelstore.Checkpoint
	var zeroAttempt kernelstore.Attempt
	in, requestHash, err := input.Normalize()
	if err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	defer rollback(ctx, tx)

	checkpoint, storedHash, err := scanCheckpointAndHash(tx.QueryRow(ctx, checkpointSelectWithHash+`
		WHERE c.tenant_id = $1 AND c.attempt_id = $2 AND c.idempotency_key = $3`,
		in.TenantID, in.AttemptID.String(), in.IdempotencyKey))
	if err == nil {
		if subtle.ConstantTimeCompare(storedHash, requestHash[:]) != 1 {
			return zeroCheckpoint, zeroAttempt, kernelstore.ErrIdempotencyConflict
		}
		attempt, scanErr := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts
			WHERE tenant_id = $1 AND id = $2`, in.TenantID, in.AttemptID.String()))
		if scanErr != nil {
			return zeroCheckpoint, zeroAttempt, classify(scanErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return zeroCheckpoint, zeroAttempt, classify(err)
		}
		return checkpoint, attempt, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return zeroCheckpoint, zeroAttempt, classify(err)
	}

	attempt, run, task, lease, err := lockRuntimeOwner(ctx, tx, in.TenantID, in.AttemptID, in.FencingToken)
	if err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	if attempt.ResourceVersion != in.ExpectedAttemptVersion {
		return zeroCheckpoint, zeroAttempt, versionConflict("attempt", attempt.ID, in.ExpectedAttemptVersion, attempt.ResourceVersion)
	}
	if attempt.Phase != domain.AttemptRunning && attempt.Phase != domain.AttemptCheckpointing {
		return zeroCheckpoint, zeroAttempt, fmt.Errorf("%w: cannot checkpoint attempt in phase %s", kernelstore.ErrInvalidTransition, attempt.Phase)
	}
	if task.AgentVersionRef != in.AgentVersionRef || attempt.RuntimeClass == "" {
		return zeroCheckpoint, zeroAttempt, fmt.Errorf("checkpoint compatibility identity does not match assignment")
	}
	resolvedVersionID, err := resolveAgentVersionID(ctx, tx, in.TenantID, in.AgentVersionRef)
	if err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	if resolvedVersionID == nil {
		return zeroCheckpoint, zeroAttempt, fmt.Errorf("%w: checkpoint references unpublished agent version %s", kernelstore.ErrNotFound, in.AgentVersionRef)
	}
	agentVersionID := sql.NullString{String: resolvedVersionID.String(), Valid: true}
	if !lease.ExpiresAt.After(s.now()) {
		return zeroCheckpoint, zeroAttempt, fmt.Errorf("%w: lease expired", kernelstore.ErrFenced)
	}
	artifact, err := ensureArtifact(ctx, tx, in.TenantID, in.State, s.now(), s.newID)
	if err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	var ordinal int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(ordinal), 0) + 1 FROM checkpoints WHERE tenant_id = $1 AND run_id = $2`, input.TenantID, run.ID.String()).Scan(&ordinal); err != nil {
		return zeroCheckpoint, zeroAttempt, classify(err)
	}
	now := s.now()
	_, err = tx.Exec(ctx, checkpointInsert,
		in.CheckpointID.String(), in.TenantID, run.ID.String(), attempt.ID.String(), ordinal,
		in.FencingToken, in.AgentVersionRef, agentVersionID, attempt.RuntimeClass, in.Provider, in.RuntimeABI,
		in.SchemaVersion, artifact.ID.String(), in.ConfirmedReceiptIDs, requestHash[:], in.IdempotencyKey,
		requestHash[:], now)
	if err != nil {
		return zeroCheckpoint, zeroAttempt, classify(err)
	}
	checkpoint, err = scanCheckpoint(tx.QueryRow(ctx, checkpointSelect+` WHERE c.tenant_id = $1 AND c.id = $2`,
		in.TenantID, in.CheckpointID.String()))
	if err != nil {
		return zeroCheckpoint, zeroAttempt, classify(err)
	}
	updatedAttempt, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET resource_version = resource_version + 1,
		updated_at = $1 WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+attemptColumns,
		now, in.TenantID, attempt.ID.String(), in.ExpectedAttemptVersion))
	if err != nil {
		return zeroCheckpoint, zeroAttempt, classifyCAS(err, "attempt", attempt.ID, in.ExpectedAttemptVersion)
	}
	if err := insertEvent(ctx, tx, in.TenantID, "Attempt", attempt.ID, updatedAttempt.ResourceVersion, "CheckpointCommitted", map[string]any{
		"attemptId": attempt.ID, "runId": run.ID, "checkpointId": checkpoint.ID,
		"fencingToken": in.FencingToken, "envelopeSha256": fmt.Sprintf("%x", checkpoint.EnvelopeSHA256),
	}, now, s.newID()); err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "checkpoint.committed", "Attempt", attempt.ID, map[string]any{
		"checkpointId": checkpoint.ID, "runId": run.ID, "schemaVersion": in.SchemaVersion,
		"artifactUri": in.State.URI,
	}, now); err != nil {
		return zeroCheckpoint, zeroAttempt, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zeroCheckpoint, zeroAttempt, classify(err)
	}
	return checkpoint, updatedAttempt, nil
}

func (s *Store) CompleteAttempt(ctx context.Context, in kernelstore.CompleteAttemptInput) (kernelstore.CompleteAttemptResult, error) {
	var result kernelstore.CompleteAttemptResult
	requestHash, err := in.RequestHash()
	if err != nil {
		return result, err
	}
	// Completion finalization is a cross-row invariant (ADR-002): result
	// registration, attempt/run/task terminal states, lease release, outbox
	// events and the idempotency receipt share one SERIALIZABLE transaction.
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	var storedHash []byte
	var responseJSON []byte
	err = tx.QueryRow(ctx, `SELECT request_hash, response FROM runtime_operation_receipts
		WHERE tenant_id = $1 AND attempt_id = $2 AND operation = 'COMPLETE' AND idempotency_key = $3`,
		in.TenantID, in.AttemptID.String(), in.IdempotencyKey).Scan(&storedHash, &responseJSON)
	if err == nil {
		if subtle.ConstantTimeCompare(storedHash, requestHash[:]) != 1 {
			return result, kernelstore.ErrIdempotencyConflict
		}
		result.Attempt, err = scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts WHERE tenant_id = $1 AND id = $2`, in.TenantID, in.AttemptID.String()))
		if err != nil {
			return result, classify(err)
		}
		result.Run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE tenant_id = $1 AND id = $2`, in.TenantID, result.Attempt.RunID.String()))
		if err != nil {
			return result, classify(err)
		}
		result.Task, err = scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE tenant_id = $1 AND id = $2`, in.TenantID, result.Run.TaskID.String()))
		if err != nil {
			return result, classify(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, classify(err)
	}

	attempt, run, task, lease, err := lockRuntimeOwner(ctx, tx, in.TenantID, in.AttemptID, in.FencingToken)
	if err != nil {
		return result, err
	}
	if attempt.ResourceVersion != in.ExpectedAttemptVersion {
		return result, versionConflict("attempt", attempt.ID, in.ExpectedAttemptVersion, attempt.ResourceVersion)
	}
	if err := domain.ValidateAttemptTransition(attempt.Phase, domain.AttemptCompleted); err != nil {
		return result, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	if err := domain.ValidateRunTransition(run.Phase, domain.RunCompleted); err != nil {
		return result, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	if task.Phase != domain.TaskRunning || task.ActiveRunID == nil || *task.ActiveRunID != run.ID {
		return result, fmt.Errorf("%w: task is not running this run", kernelstore.ErrInvalidTransition)
	}
	if !lease.ExpiresAt.After(s.now()) {
		return result, fmt.Errorf("%w: lease expired", kernelstore.ErrFenced)
	}
	artifact, err := ensureArtifact(ctx, tx, in.TenantID, in.Result, s.now(), s.newID)
	if err != nil {
		return result, err
	}
	now := s.now()
	result.Attempt, err = scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'COMPLETED',
		finished_at = $1, resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+attemptColumns,
		now, in.TenantID, attempt.ID.String(), in.ExpectedAttemptVersion))
	if err != nil {
		return result, classifyCAS(err, "attempt", attempt.ID, in.ExpectedAttemptVersion)
	}
	result.Run, err = scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'COMPLETED', result_ref = $1,
		resource_version = resource_version + 1, updated_at = $2, completed_at = $2
		WHERE tenant_id = $3 AND id = $4 AND resource_version = $5 RETURNING `+runColumns,
		artifact.URI, now, in.TenantID, run.ID.String(), run.ResourceVersion))
	if err != nil {
		return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
	}
	result.Task, err = scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'SUCCEEDED', result_ref = $1,
		resource_version = resource_version + 1, updated_at = $2
		WHERE tenant_id = $3 AND id = $4 AND resource_version = $5 RETURNING `+taskColumns,
		artifact.URI, now, in.TenantID, task.ID.String(), task.ResourceVersion))
	if err != nil {
		return result, classifyCAS(err, "task", task.ID, task.ResourceVersion)
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime_leases SET released_at = $1, release_reason = 'COMPLETED'
		WHERE tenant_id = $2 AND id = $3 AND released_at IS NULL`, now, in.TenantID, lease.ID.String()); err != nil {
		return result, classify(err)
	}
	if err := s.releaseTenantReservation(ctx, tx, in.TenantID, task.ID); err != nil {
		return result, err
	}
	if err := s.releaseRuntimeCapacity(ctx, tx, in.TenantID, task.ID, now); err != nil {
		return result, err
	}
	for _, event := range []struct {
		typeName string
		id       uuid.UUID
		version  int64
		event    string
	}{
		{"Attempt", result.Attempt.ID, result.Attempt.ResourceVersion, "AttemptCompleted"},
		{"Run", result.Run.ID, result.Run.ResourceVersion, "RunCompleted"},
		{"Task", result.Task.ID, result.Task.ResourceVersion, "TaskSucceeded"},
	} {
		if err := insertEvent(ctx, tx, in.TenantID, event.typeName, event.id, event.version, event.event, map[string]any{
			"taskId": task.ID, "runId": run.ID, "attemptId": attempt.ID, "resultRef": artifact.URI,
		}, now, s.newID()); err != nil {
			return result, err
		}
	}
	response, err := json.Marshal(map[string]any{
		"attemptVersion": result.Attempt.ResourceVersion, "runVersion": result.Run.ResourceVersion,
		"taskVersion": result.Task.ResourceVersion, "resultRef": artifact.URI,
	})
	if err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO runtime_operation_receipts (
		tenant_id, attempt_id, operation, idempotency_key, request_hash, response, processed_at
	) VALUES ($1, $2, 'COMPLETE', $3, $4, $5, $6)`, in.TenantID, attempt.ID.String(),
		in.IdempotencyKey, requestHash[:], response, now); err != nil {
		return result, classify(err)
	}
	if err := auditHook(ctx, tx, in.TenantID, "attempt.completed", "Attempt", attempt.ID, map[string]any{
		"runId": run.ID, "taskId": task.ID, "resultRef": artifact.URI,
	}, now); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return result, nil
}

func (s *Store) AcknowledgeCancellation(ctx context.Context, in kernelstore.CancelAttemptInput) (kernelstore.CancelAttemptResult, error) {
	var result kernelstore.CancelAttemptResult
	requestHash, err := in.RequestHash()
	if err != nil {
		return result, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	var storedHash []byte
	var responseJSON []byte
	err = tx.QueryRow(ctx, `SELECT request_hash, response FROM runtime_operation_receipts
		WHERE tenant_id = $1 AND attempt_id = $2 AND operation = 'CANCEL' AND idempotency_key = $3`,
		in.TenantID, in.AttemptID.String(), in.IdempotencyKey).Scan(&storedHash, &responseJSON)
	if err == nil {
		if subtle.ConstantTimeCompare(storedHash, requestHash[:]) != 1 {
			return result, kernelstore.ErrIdempotencyConflict
		}
		result.Attempt, err = scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts WHERE tenant_id = $1 AND id = $2`, in.TenantID, in.AttemptID.String()))
		if err != nil {
			return result, classify(err)
		}
		result.Run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE tenant_id = $1 AND id = $2`, in.TenantID, result.Attempt.RunID.String()))
		if err != nil {
			return result, classify(err)
		}
		result.Task, err = scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE tenant_id = $1 AND id = $2`, in.TenantID, result.Run.TaskID.String()))
		if err != nil {
			return result, classify(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, classify(err)
	}

	attempt, run, task, lease, err := lockRuntimeOwner(ctx, tx, in.TenantID, in.AttemptID, in.FencingToken)
	if err != nil {
		return result, err
	}
	if attempt.ResourceVersion != in.ExpectedAttemptVersion {
		return result, versionConflict("attempt", attempt.ID, in.ExpectedAttemptVersion, attempt.ResourceVersion)
	}
	if attempt.Phase != domain.AttemptCancelRequested || task.CancelRequestedAt == nil {
		return result, fmt.Errorf("%w: cancellation was not requested", kernelstore.ErrInvalidTransition)
	}
	if err := domain.ValidateAttemptTransition(attempt.Phase, domain.AttemptCancelled); err != nil {
		return result, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	if err := domain.ValidateRunTransition(run.Phase, domain.RunCancelled); err != nil {
		return result, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	if !lease.ExpiresAt.After(s.now()) {
		return result, fmt.Errorf("%w: lease expired", kernelstore.ErrFenced)
	}
	now := s.now()
	result.Attempt, err = scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'CANCELLED', finished_at = $1,
		resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+attemptColumns,
		now, in.TenantID, attempt.ID.String(), in.ExpectedAttemptVersion))
	if err != nil {
		return result, classifyCAS(err, "attempt", attempt.ID, in.ExpectedAttemptVersion)
	}
	result.Run, err = scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'CANCELLED', active_attempt_id = NULL,
		resource_version = resource_version + 1, updated_at = $1, completed_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+runColumns,
		now, in.TenantID, run.ID.String(), run.ResourceVersion))
	if err != nil {
		return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
	}
	result.Task, err = scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'CANCELLED',
		resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
		now, in.TenantID, task.ID.String(), task.ResourceVersion))
	if err != nil {
		return result, classifyCAS(err, "task", task.ID, task.ResourceVersion)
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime_leases SET released_at = $1, release_reason = 'CANCELLED'
		WHERE tenant_id = $2 AND id = $3 AND released_at IS NULL`, now, in.TenantID, lease.ID.String()); err != nil {
		return result, classify(err)
	}
	// Quota reservation release (v0.8): the task is terminal.
	if err := s.releaseTenantReservation(ctx, tx, in.TenantID, task.ID); err != nil {
		return result, err
	}
	if err := s.releaseRuntimeCapacity(ctx, tx, in.TenantID, task.ID, now); err != nil {
		return result, err
	}
	for _, event := range []struct {
		typeName string
		id       uuid.UUID
		version  int64
		event    string
	}{
		{"Attempt", result.Attempt.ID, result.Attempt.ResourceVersion, "AttemptCancelled"},
		{"Run", result.Run.ID, result.Run.ResourceVersion, "RunCancelled"},
		{"Task", result.Task.ID, result.Task.ResourceVersion, "TaskCancelled"},
	} {
		if err := insertEvent(ctx, tx, in.TenantID, event.typeName, event.id, event.version, event.event, map[string]any{
			"taskId": task.ID, "runId": run.ID, "attemptId": attempt.ID,
		}, now, s.newID()); err != nil {
			return result, err
		}
	}
	response, err := json.Marshal(map[string]any{
		"attemptVersion": result.Attempt.ResourceVersion, "runVersion": result.Run.ResourceVersion,
		"taskVersion": result.Task.ResourceVersion,
	})
	if err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO runtime_operation_receipts (
		tenant_id, attempt_id, operation, idempotency_key, request_hash, response, processed_at
	) VALUES ($1, $2, 'CANCEL', $3, $4, $5, $6)`, in.TenantID, attempt.ID.String(),
		in.IdempotencyKey, requestHash[:], response, now); err != nil {
		return result, classify(err)
	}
	if err := auditHook(ctx, tx, in.TenantID, "cancellation.acknowledged", "Attempt", attempt.ID, map[string]any{
		"runId": run.ID, "taskId": task.ID,
	}, now); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return result, nil
}

func (s *Store) ListExpiredAttempts(ctx context.Context, now time.Time, limit int) ([]kernelstore.RecoveryCandidate, error) {
	if limit <= 0 || limit > 1000 {
		return nil, fmt.Errorf("recovery limit must be between 1 and 1000")
	}
	rows, err := s.pool.Query(ctx, `SELECT a.tenant_id, a.id::text, a.fencing_token, t.spec,
		(t.execution_deadline_at IS NOT NULL AND t.execution_deadline_at <= $1) AS deadline_exceeded
		FROM runtime_leases l
		JOIN attempts a ON a.tenant_id = l.tenant_id AND a.id = l.attempt_id
		JOIN runs r ON r.tenant_id = a.tenant_id AND r.id = a.run_id
		JOIN tasks t ON t.tenant_id = r.tenant_id AND t.id = r.task_id
		WHERE l.released_at IS NULL AND (l.expires_at <= $1 OR
			(t.phase = 'RUNNING' AND t.execution_deadline_at IS NOT NULL AND t.execution_deadline_at <= $1))
		  AND r.active_attempt_id = a.id AND r.current_fencing_token = a.fencing_token
		ORDER BY LEAST(l.expires_at, t.execution_deadline_at), l.id LIMIT $2`, now.UTC(), limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var result []kernelstore.RecoveryCandidate
	for rows.Next() {
		var candidate kernelstore.RecoveryCandidate
		var id string
		if err := rows.Scan(&candidate.TenantID, &id, &candidate.FencingToken, &candidate.TaskSpec, &candidate.DeadlineExceeded); err != nil {
			return nil, classify(err)
		}
		candidate.AttemptID, err = uuid.Parse(id)
		if err != nil {
			return nil, fmt.Errorf("parse expired attempt ID: %w", err)
		}
		result = append(result, candidate)
	}
	return result, classify(rows.Err())
}

func (s *Store) RecoverExpiredAttempt(ctx context.Context, in kernelstore.RecoverExpiredAttemptInput) (kernelstore.RecoveryResult, error) {
	var result kernelstore.RecoveryResult
	if in.TenantID == "" || in.AttemptID == uuid.Nil || in.FencingToken <= 0 || in.LeaseTTL <= 0 || in.MaxAttempts <= 0 {
		return result, fmt.Errorf("valid recovery identity, TTL, and max attempts are required")
	}
	if in.NewAttemptID == uuid.Nil {
		in.NewAttemptID = s.newID()
	}
	if in.NewLeaseID == uuid.Nil {
		in.NewLeaseID = s.newID()
	}
	// Lease-expiry recovery is a cross-row invariant (ADR-002): releasing the
	// old lease, fencing the run, and placing the next attempt are one
	// SERIALIZABLE transaction.
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	attempt, run, task, lease, err := lockRuntimeOwner(ctx, tx, in.TenantID, in.AttemptID, in.FencingToken)
	if err != nil {
		return result, err
	}
	now := s.now()
	if lease.ExpiresAt.After(now) && !in.DeadlineExceeded {
		return result, kernelstore.ErrLeaseNotExpired
	}
	releaseReason := "EXPIRED"
	if in.DeadlineExceeded {
		releaseReason = "EXECUTION_DEADLINE_EXCEEDED"
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime_leases SET released_at = $1, release_reason = $2
		WHERE tenant_id = $3 AND id = $4 AND released_at IS NULL`, now, releaseReason, in.TenantID, lease.ID.String()); err != nil {
		return result, classify(err)
	}
	if in.DeadlineExceeded {
		failedAttempt, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'ATTEMPT_FAILED',
			failure_code = 'WALL_TIME_EXCEEDED', failure_message = 'task execution deadline exceeded',
			finished_at = $1, resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+attemptColumns,
			now, in.TenantID, attempt.ID.String(), attempt.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "attempt", attempt.ID, attempt.ResourceVersion)
		}
		failedRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'FAILED', active_attempt_id = NULL,
			resource_version = resource_version + 1, updated_at = $1, completed_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+runColumns,
			now, in.TenantID, run.ID.String(), run.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
		}
		failedTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'FAILED',
			resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
			now, in.TenantID, task.ID.String(), task.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "task", task.ID, task.ResourceVersion)
		}
		if err := s.releaseTenantReservation(ctx, tx, in.TenantID, task.ID); err != nil {
			return result, err
		}
		if err := s.releaseRuntimeCapacity(ctx, tx, in.TenantID, task.ID, now); err != nil {
			return result, err
		}
		for _, event := range []struct {
			aggregate string
			id        uuid.UUID
			version   int64
			typeName  string
		}{
			{"Attempt", failedAttempt.ID, failedAttempt.ResourceVersion, "AttemptFailed"},
			{"Run", failedRun.ID, failedRun.ResourceVersion, "RunFailed"},
			{"Task", failedTask.ID, failedTask.ResourceVersion, "TaskFailed"},
		} {
			if err := insertEvent(ctx, tx, in.TenantID, event.aggregate, event.id, event.version, event.typeName, map[string]any{
				"taskId": task.ID, "runId": run.ID, "attemptId": attempt.ID, "failureCode": "WALL_TIME_EXCEEDED",
			}, now, s.newID()); err != nil {
				return result, err
			}
		}
		if err := auditHook(ctx, tx, in.TenantID, "task.wall_time.exceeded", "Task", task.ID, map[string]any{
			"runId": run.ID, "attemptId": attempt.ID, "executionDeadline": task.ExecutionDeadlineAt,
		}, now); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return result, nil
	}
	if task.CancelRequestedAt != nil {
		cancelledAttempt, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'CANCELLED',
			finished_at = $1, resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND phase = 'CANCEL_REQUESTED' RETURNING `+attemptColumns,
			now, in.TenantID, attempt.ID.String()))
		if err != nil {
			return result, classify(err)
		}
		cancelledRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'CANCELLED', active_attempt_id = NULL,
			resource_version = resource_version + 1, updated_at = $1, completed_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+runColumns,
			now, in.TenantID, run.ID.String(), run.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
		}
		cancelledTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'CANCELLED',
			resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
			now, in.TenantID, task.ID.String(), task.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "task", task.ID, task.ResourceVersion)
		}
		// Quota reservation release (v0.8): the task is terminal.
		if err := s.releaseTenantReservation(ctx, tx, in.TenantID, task.ID); err != nil {
			return result, err
		}
		if err := s.releaseRuntimeCapacity(ctx, tx, in.TenantID, task.ID, now); err != nil {
			return result, err
		}
		for _, event := range []struct {
			typeName string
			id       uuid.UUID
			version  int64
			event    string
		}{
			{"Attempt", cancelledAttempt.ID, cancelledAttempt.ResourceVersion, "AttemptCancelled"},
			{"Run", cancelledRun.ID, cancelledRun.ResourceVersion, "RunCancelled"},
			{"Task", cancelledTask.ID, cancelledTask.ResourceVersion, "TaskCancelled"},
		} {
			if err := insertEvent(ctx, tx, in.TenantID, event.typeName, event.id, event.version, event.event, map[string]any{
				"taskId": task.ID, "runId": run.ID, "attemptId": attempt.ID, "reason": "LEASE_EXPIRED_AFTER_CANCEL",
			}, now, s.newID()); err != nil {
				return result, err
			}
		}
		if err := auditHook(ctx, tx, in.TenantID, "attempt.recovered", "Attempt", attempt.ID, map[string]any{
			"runId": run.ID, "taskId": task.ID, "reason": "CANCELLED_AFTER_EXPIRY",
		}, now); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return result, nil
	}
	// v1.2: an attempt that never left PLACED was claimed by no runtime —
	// its expiry is a placement queue overflow, not an execution loss.
	// Replacing it must not consume the task retry budget (a burst of
	// dispatches over a small fleet would otherwise burn every task).
	unclaimed := attempt.Phase == domain.AttemptPlaced
	expiryCode := "LEASE_EXPIRED"
	if unclaimed {
		expiryCode = "LEASE_EXPIRED_UNCLAIMED"
	}
	failedAttempt, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'ATTEMPT_FAILED',
		failure_code = $5, failure_message = 'runtime lease expired before completion',
		finished_at = $1, resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+attemptColumns,
		now, in.TenantID, attempt.ID.String(), attempt.ResourceVersion, expiryCode))
	if err != nil {
		return result, classifyCAS(err, "attempt", attempt.ID, attempt.ResourceVersion)
	}
	if err := insertEvent(ctx, tx, in.TenantID, "Attempt", attempt.ID, failedAttempt.ResourceVersion, "AttemptFailed", map[string]any{
		"attemptId": attempt.ID, "runId": run.ID, "failureCode": expiryCode, "fencingToken": in.FencingToken,
	}, now, s.newID()); err != nil {
		return result, err
	}

	if !unclaimed && attempt.Ordinal >= in.MaxAttempts {
		updatedRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'FAILED', active_attempt_id = NULL,
			resource_version = resource_version + 1, updated_at = $1, completed_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+runColumns,
			now, in.TenantID, run.ID.String(), run.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
		}
		updatedTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'FAILED',
			resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
			now, in.TenantID, task.ID.String(), task.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "task", task.ID, task.ResourceVersion)
		}
		// Quota reservation release (v0.8): the task is terminal.
		if err := s.releaseTenantReservation(ctx, tx, in.TenantID, task.ID); err != nil {
			return result, err
		}
		if err := s.releaseRuntimeCapacity(ctx, tx, in.TenantID, task.ID, now); err != nil {
			return result, err
		}
		if err := insertEvent(ctx, tx, in.TenantID, "Run", run.ID, updatedRun.ResourceVersion, "RunFailed", map[string]any{
			"runId": run.ID, "failureCode": "ATTEMPTS_EXHAUSTED",
		}, now, s.newID()); err != nil {
			return result, err
		}
		if err := insertEvent(ctx, tx, in.TenantID, "Task", task.ID, updatedTask.ResourceVersion, "TaskFailed", map[string]any{
			"taskId": task.ID, "runId": run.ID, "failureCode": "ATTEMPTS_EXHAUSTED",
		}, now, s.newID()); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return result, nil
	}

	// Multi-runtime re-placement: an expired attempt whose pool no longer
	// accepts work (operator cordon/drain, readiness lost, or the pool
	// record removed) must NOT be requeued onto the same pool — the requeue
	// would strand the task there forever. Reopen the task for scheduling so
	// the scheduler selects a live pool of the same runtime class.
	poolIneligible, poolErr := runtimePoolIneligible(ctx, tx, attempt.RuntimePoolID)
	if poolErr != nil {
		return result, fmt.Errorf("resolve pool eligibility: %w", poolErr)
	}
	if poolIneligible {
		// Fail the current run (the task keeps its admission) and release
		// the old pool's runtime capacity so the scheduler can reserve the
		// replacement pool. The tenant budget reservation is preserved: the
		// task never became terminal.
		updatedRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'FAILED', active_attempt_id = NULL,
			resource_version = resource_version + 1, updated_at = $1, completed_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+runColumns,
			now, in.TenantID, run.ID.String(), run.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
		}
		updatedTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'ADMITTED', active_run_id = NULL,
			resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 RETURNING `+taskColumns,
			now, in.TenantID, task.ID.String(), task.ResourceVersion))
		if err != nil {
			return result, classifyCAS(err, "task", task.ID, task.ResourceVersion)
		}
		if err := s.releaseRuntimeCapacity(ctx, tx, in.TenantID, task.ID, now); err != nil {
			return result, err
		}
		if err := insertEvent(ctx, tx, in.TenantID, "Run", run.ID, updatedRun.ResourceVersion, "RunFailed", map[string]any{
			"runId": run.ID, "failureCode": "POOL_UNAVAILABLE",
		}, now, s.newID()); err != nil {
			return result, err
		}
		if err := insertEvent(ctx, tx, in.TenantID, "Task", task.ID, updatedTask.ResourceVersion, "TaskReopened", map[string]any{
			"taskId": task.ID, "reason": "POOL_UNAVAILABLE", "poolId": attempt.RuntimePoolID,
		}, now, s.newID()); err != nil {
			return result, err
		}
		if err := auditHook(ctx, tx, in.TenantID, "task.reopened_for_scheduling", "Task", task.ID, map[string]any{
			"runId": run.ID, "reason": "POOL_UNAVAILABLE", "poolId": attempt.RuntimePoolID,
		}, now); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		result.Retried = true
		return result, nil
	}

	newToken := run.CurrentFencingToken + 1
	newAttempt, err := scanAttempt(tx.QueryRow(ctx, `INSERT INTO attempts (
		id, tenant_id, run_id, ordinal, phase, runtime_class, runtime_pool_id,
		runtime_instance_id, fencing_token, resource_version, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 'PLACED', $5, $6, $7, $8, 1, $9, $9) RETURNING `+attemptColumns,
		in.NewAttemptID.String(), in.TenantID, run.ID.String(), attempt.Ordinal+1, attempt.RuntimeClass,
		attempt.RuntimePoolID, attempt.RuntimeInstanceID, newToken, now))
	if err != nil {
		return result, classify(err)
	}
	newLease, err := scanLease(tx.QueryRow(ctx, `INSERT INTO runtime_leases (
		id, tenant_id, run_id, attempt_id, fencing_token, resource_version, acquired_at, heartbeat_at, expires_at
	) VALUES ($1, $2, $3, $4, $5, 1, $6, $6, $7) RETURNING `+leaseColumns,
		in.NewLeaseID.String(), in.TenantID, run.ID.String(), newAttempt.ID.String(), newToken, now, now.Add(in.LeaseTTL)))
	if err != nil {
		return result, classify(err)
	}
	updatedRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET active_attempt_id = $1,
		current_fencing_token = $2, resource_version = resource_version + 1, updated_at = $3
		WHERE tenant_id = $4 AND id = $5 AND resource_version = $6 RETURNING `+runColumns,
		newAttempt.ID.String(), newToken, now, in.TenantID, run.ID.String(), run.ResourceVersion))
	if err != nil {
		return result, classifyCAS(err, "run", run.ID, run.ResourceVersion)
	}
	if err := insertEvent(ctx, tx, in.TenantID, "Attempt", newAttempt.ID, newAttempt.ResourceVersion, "AttemptPlaced", map[string]any{
		"attemptId": newAttempt.ID, "runId": run.ID, "runtimePoolId": newAttempt.RuntimePoolID,
		"runtimeInstanceId": newAttempt.RuntimeInstanceID, "fencingToken": newToken, "recoveredFromAttemptId": attempt.ID,
	}, now, s.newID()); err != nil {
		return result, err
	}
	if err := insertEvent(ctx, tx, in.TenantID, "Run", run.ID, updatedRun.ResourceVersion, "RunAttemptRecovered", map[string]any{
		"runId": run.ID, "attemptId": newAttempt.ID, "previousAttemptId": attempt.ID, "fencingToken": newToken,
	}, now, s.newID()); err != nil {
		return result, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "attempt.recovered", "Attempt", attempt.ID, map[string]any{
		"runId": run.ID, "taskId": task.ID, "newAttemptId": newAttempt.ID,
		"newFencingToken": newToken, "ordinal": attempt.Ordinal + 1,
	}, now); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	result.Retried = true
	result.Lease = kernelstore.AttemptLease{Attempt: newAttempt, Lease: newLease, Run: updatedRun}
	return result, nil
}

// runtimePoolIneligible reports whether a pool should not receive requeued
// work after a lease expiry: cordoned, draining, not ready, or no longer
// registered (operator removed the pool). An empty pool id (never placed)
// keeps the existing requeue behavior.
func runtimePoolIneligible(ctx context.Context, tx pgx.Tx, poolID string) (bool, error) {
	if strings.TrimSpace(poolID) == "" {
		return false, nil
	}
	var status string
	var ready bool
	err := tx.QueryRow(ctx, `SELECT COALESCE(status, 'ACTIVE'), COALESCE(ready, false)
		FROM runtime_pools WHERE id = $1`, poolID).Scan(&status, &ready)
	if errors.Is(err, pgx.ErrNoRows) {
		// Pool not in the registry: fall through to normal requeue. The
		// scheduler will defer the task if the pool truly no longer exists;
		// treating an unknown pool as ineligible would break deployments
		// that manage pools out-of-band (and test setups that truncate and
		// re-register pools between scenarios).
		return false, nil
	}
	if err != nil {
		return false, err
	}
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "CORDONED", "DRAINING":
		return true, nil
	}
	return !ready, nil
}

func lockRuntimeOwner(ctx context.Context, tx pgx.Tx, tenantID string, attemptID uuid.UUID, fencingToken int64) (kernelstore.Attempt, kernelstore.Run, kernelstore.Task, kernelstore.Lease, error) {
	var zeroAttempt kernelstore.Attempt
	var zeroRun kernelstore.Run
	var zeroTask kernelstore.Task
	var zeroLease kernelstore.Lease
	var runID string
	if err := tx.QueryRow(ctx, `SELECT run_id::text FROM attempts WHERE tenant_id = $1 AND id = $2`,
		tenantID, attemptID.String()).Scan(&runID); err != nil {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, classify(err)
	}
	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, runID))
	if err != nil {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, classify(err)
	}
	attempt, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, attemptID.String()))
	if err != nil {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, classify(err)
	}
	if err := requireCurrent(run, attempt, fencingToken); err != nil {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, err
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, run.TaskID.String()))
	if err != nil {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, classify(err)
	}
	lease, err := scanLease(tx.QueryRow(ctx, `SELECT `+leaseColumns+` FROM runtime_leases
		WHERE tenant_id = $1 AND attempt_id = $2 AND fencing_token = $3 AND released_at IS NULL FOR UPDATE`,
		tenantID, attempt.ID.String(), fencingToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, fmt.Errorf("%w: active lease not found", kernelstore.ErrFenced)
	}
	if err != nil {
		return zeroAttempt, zeroRun, zeroTask, zeroLease, classify(err)
	}
	return attempt, run, task, lease, nil
}

func ensureArtifact(ctx context.Context, tx pgx.Tx, tenantID string, input kernelstore.ArtifactReference, now time.Time, newID func() uuid.UUID) (kernelstore.ArtifactReference, error) {
	if err := input.Validate(); err != nil {
		return kernelstore.ArtifactReference{}, err
	}
	if input.ID == uuid.Nil {
		input.ID = newID()
	}
	_, err := tx.Exec(ctx, `INSERT INTO artifacts (id, tenant_id, uri, sha256, size_bytes, media_type, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (tenant_id, sha256) DO NOTHING`,
		input.ID.String(), tenantID, input.URI, input.SHA256[:], input.SizeBytes, input.MediaType, now)
	if err != nil {
		return kernelstore.ArtifactReference{}, classify(err)
	}
	var stored kernelstore.ArtifactReference
	var id string
	var digest []byte
	if err := tx.QueryRow(ctx, `SELECT id::text, uri, sha256, size_bytes, media_type FROM artifacts
		WHERE tenant_id = $1 AND sha256 = $2`, tenantID, input.SHA256[:]).Scan(
		&id, &stored.URI, &digest, &stored.SizeBytes, &stored.MediaType); err != nil {
		return kernelstore.ArtifactReference{}, classify(err)
	}
	stored.ID, err = uuid.Parse(id)
	if err != nil {
		return kernelstore.ArtifactReference{}, fmt.Errorf("parse artifact ID: %w", err)
	}
	if len(digest) != sha256.Size || subtle.ConstantTimeCompare(digest, input.SHA256[:]) != 1 ||
		stored.SizeBytes != input.SizeBytes || stored.URI != input.URI || stored.MediaType != input.MediaType {
		return kernelstore.ArtifactReference{}, fmt.Errorf("artifact digest metadata conflict")
	}
	copy(stored.SHA256[:], digest)
	return stored, nil
}

const checkpointSelect = `SELECT
	c.id::text, c.tenant_id, c.run_id::text, c.attempt_id::text, c.ordinal,
	c.fencing_token, c.agent_version_ref, c.runtime_class, c.provider, c.runtime_abi,
	c.schema_version, a.id::text, a.uri, a.sha256, a.size_bytes, a.media_type,
	c.confirmed_receipt_ids, c.envelope_sha256, c.created_at
	FROM checkpoints c JOIN artifacts a ON a.tenant_id = c.tenant_id AND a.id = c.state_artifact_id `

const checkpointSelectWithHash = `SELECT
	c.id::text, c.tenant_id, c.run_id::text, c.attempt_id::text, c.ordinal,
	c.fencing_token, c.agent_version_ref, c.runtime_class, c.provider, c.runtime_abi,
	c.schema_version, a.id::text, a.uri, a.sha256, a.size_bytes, a.media_type,
	c.confirmed_receipt_ids, c.envelope_sha256, c.created_at, c.request_hash
	FROM checkpoints c JOIN artifacts a ON a.tenant_id = c.tenant_id AND a.id = c.state_artifact_id `

const checkpointInsert = `INSERT INTO checkpoints (
	id, tenant_id, run_id, attempt_id, ordinal, fencing_token, agent_version_ref,
	agent_version_id, runtime_class, provider, runtime_abi, schema_version, state_artifact_id,
	confirmed_receipt_ids, envelope_sha256, idempotency_key, request_hash, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

func scanCheckpoint(row scanner) (kernelstore.Checkpoint, error) {
	checkpoint, _, err := scanCheckpointValues(row, false)
	return checkpoint, err
}

func scanCheckpointAndHash(row scanner) (kernelstore.Checkpoint, []byte, error) {
	return scanCheckpointValues(row, true)
}

func scanCheckpointValues(row scanner, withHash bool) (kernelstore.Checkpoint, []byte, error) {
	var checkpoint kernelstore.Checkpoint
	var checkpointID, runID, attemptID, artifactID string
	var artifactDigest, envelopeDigest, requestHash []byte
	values := []any{
		&checkpointID, &checkpoint.TenantID, &runID, &attemptID, &checkpoint.Ordinal,
		&checkpoint.FencingToken, &checkpoint.AgentVersionRef, &checkpoint.RuntimeClass,
		&checkpoint.Provider, &checkpoint.RuntimeABI, &checkpoint.SchemaVersion,
		&artifactID, &checkpoint.State.URI, &artifactDigest, &checkpoint.State.SizeBytes,
		&checkpoint.State.MediaType, &checkpoint.ConfirmedReceiptIDs, &envelopeDigest, &checkpoint.CreatedAt,
	}
	if withHash {
		values = append(values, &requestHash)
	}
	if err := row.Scan(values...); err != nil {
		return checkpoint, nil, err
	}
	var err error
	if checkpoint.ID, err = uuid.Parse(checkpointID); err != nil {
		return checkpoint, nil, fmt.Errorf("parse checkpoint ID: %w", err)
	}
	if checkpoint.RunID, err = uuid.Parse(runID); err != nil {
		return checkpoint, nil, fmt.Errorf("parse checkpoint run ID: %w", err)
	}
	if checkpoint.AttemptID, err = uuid.Parse(attemptID); err != nil {
		return checkpoint, nil, fmt.Errorf("parse checkpoint attempt ID: %w", err)
	}
	if checkpoint.State.ID, err = uuid.Parse(artifactID); err != nil {
		return checkpoint, nil, fmt.Errorf("parse checkpoint artifact ID: %w", err)
	}
	if len(artifactDigest) != sha256.Size || len(envelopeDigest) != sha256.Size {
		return checkpoint, nil, fmt.Errorf("checkpoint digest length is invalid")
	}
	copy(checkpoint.State.SHA256[:], artifactDigest)
	copy(checkpoint.EnvelopeSHA256[:], envelopeDigest)
	return checkpoint, requestHash, nil
}
