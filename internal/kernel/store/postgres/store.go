// Package postgres implements the Agent OS kernel persistence contract on
// PostgreSQL. Every lifecycle mutation and its outbox event share a transaction.
package postgres

import (
	"context"
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// database permits a store to share a transaction with a service reconciliation.
// Nested lifecycle operations use savepoints instead of acquiring another pool connection.
type database interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type transactionDatabase struct{ pgx.Tx }

func (d transactionDatabase) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return d.Tx.Begin(ctx)
}

type Store struct {
	pool  database
	clock func() time.Time
	newID func() uuid.UUID
}

var _ kernelstore.KernelStore = (*Store)(nil)

func New(pool *pgxpool.Pool) *Store {
	return NewWithClock(pool, func() time.Time { return time.Now().UTC() })
}

// NewWithClock permits deterministic lease-expiry tests. Production callers
// should use New so wall-clock ownership remains internal to the store.
func NewWithClock(pool *pgxpool.Pool, clock func() time.Time) *Store {
	return &Store{pool: pool, clock: clock, newID: uuid.New}
}

func (s *Store) CreateTask(ctx context.Context, in kernelstore.CreateTaskInput) (kernelstore.CreateTaskResult, error) {
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		result, err := s.createTaskOnce(ctx, in)
		if !kernelstore.IsRetryableTransaction(err) {
			return result, err
		}
		lastErr = err
		delay := time.Duration(1<<attempt) * 5 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return kernelstore.CreateTaskResult{}, context.Cause(ctx)
		case <-timer.C:
		}
	}
	return kernelstore.CreateTaskResult{}, fmt.Errorf("create task retries exhausted: %w", lastErr)
}

func (s *Store) createTaskOnce(ctx context.Context, in kernelstore.CreateTaskInput) (kernelstore.CreateTaskResult, error) {
	var result kernelstore.CreateTaskResult
	normalized, requestHash, err := in.ValidateAndHash()
	if err != nil {
		return result, err
	}
	if in.ID == uuid.Nil {
		in.ID = s.newID()
	}
	now := s.now()
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	row := tx.QueryRow(ctx, `
		INSERT INTO tasks (
			id, tenant_id, namespace, agent_version_ref, goal, spec, request_hash,
			idempotency_key, workflow_id, workflow_step_id, workflow_step_name, workflow_attempt, parent_task_id,
			phase, resource_version, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), NULLIF($12, 0), $13, 'QUEUED', 1, $14, $14)
		ON CONFLICT (tenant_id, namespace, idempotency_key) DO NOTHING
		RETURNING `+taskColumns,
		in.ID.String(), in.TenantID, in.Namespace, in.AgentVersionRef, in.Goal,
		normalized, requestHash[:], in.IdempotencyKey, in.WorkflowID, in.WorkflowStepID,
		in.WorkflowStepName, in.WorkflowAttempt, in.ParentTaskID, now,
	)
	created, scanErr := scanTask(row)
	if scanErr == nil {
		// Workflow lineage: the fresh task consumes the task-count slot the
		// step reserved at spawn/creation (the task-insert trigger already
		// counted the task, so the slot moves atomically with it; token and
		// cost reservations transfer later, when admission establishes the
		// task's own budget ledger). Replays take the ON CONFLICT path and
		// never consume a second slot.
		if created.WorkflowStepID != nil {
			command, err := tx.Exec(ctx, `UPDATE workflow_steps SET reserved_tasks = 0
				WHERE tenant_id = $1 AND workflow_id = $2 AND id = $3 AND reserved_tasks > 0`,
				created.TenantID, created.WorkflowID, created.WorkflowStepID)
			if err != nil {
				return result, classify(err)
			}
			if command.RowsAffected() == 1 {
				if _, err := tx.Exec(ctx, `UPDATE workflow_usage_ledgers
					SET step_reserved_tasks = GREATEST(0, step_reserved_tasks - 1), updated_at = $3
					WHERE tenant_id = $1 AND workflow_id = $2`,
					created.TenantID, created.WorkflowID, now); err != nil {
					return result, classify(err)
				}
			}
		}
		if err := insertEvent(ctx, tx, created.TenantID, "Task", created.ID, created.ResourceVersion, "TaskQueued", map[string]any{
			"taskId": created.ID, "namespace": created.Namespace,
		}, now, s.newID()); err != nil {
			return result, err
		}
		if err := auditHook(ctx, tx, created.TenantID, "task.queued", "Task", created.ID, map[string]any{
			"namespace": created.Namespace, "agentVersionRef": created.AgentVersionRef,
		}, now); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return kernelstore.CreateTaskResult{Task: created}, nil
	}
	if !errors.Is(scanErr, pgx.ErrNoRows) {
		return result, classify(scanErr)
	}

	existing, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+`
		FROM tasks WHERE tenant_id = $1 AND namespace = $2 AND idempotency_key = $3
		FOR UPDATE`, in.TenantID, in.Namespace, in.IdempotencyKey))
	if err != nil {
		return result, classify(err)
	}
	if subtle.ConstantTimeCompare(existing.RequestHash[:], requestHash[:]) != 1 {
		return result, fmt.Errorf("%w: tenant=%s namespace=%s key=%s", kernelstore.ErrIdempotencyConflict, in.TenantID, in.Namespace, in.IdempotencyKey)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return kernelstore.CreateTaskResult{Task: existing, Existing: true}, nil
}

func (s *Store) TransitionTask(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, to domain.TaskPhase) (kernelstore.Task, error) {
	var zero kernelstore.Task
	if strings.TrimSpace(tenantID) == "" || id == uuid.Nil || expectedVersion <= 0 {
		return zero, fmt.Errorf("tenant, task id and positive resource version are required")
	}
	if to == domain.TaskSucceeded {
		return zero, fmt.Errorf("%w: task success is only committed by CompleteRun", kernelstore.ErrResultRequired)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	current, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, id.String()))
	if err != nil {
		return zero, classify(err)
	}
	if current.ResourceVersion != expectedVersion {
		return zero, versionConflict("task", id, expectedVersion, current.ResourceVersion)
	}
	if err := domain.ValidateTaskTransition(current.Phase, to); err != nil {
		return zero, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	now := s.now()
	updated, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks
		SET phase = $1, resource_version = resource_version + 1, updated_at = $2
		WHERE tenant_id = $3 AND id = $4 AND resource_version = $5 RETURNING `+taskColumns,
		to, now, tenantID, id.String(), expectedVersion))
	if err != nil {
		return zero, classifyCAS(err, "task", id, expectedVersion)
	}
	// Quota reservation release (v0.8): a terminal task returns its reserved
	// ceiling to the tenant window.
	if to == domain.TaskFailed || to == domain.TaskCancelled {
		if err := s.releaseTenantReservation(ctx, tx, updated.TenantID, updated.ID); err != nil {
			return zero, err
		}
		if err := s.releaseRuntimeCapacity(ctx, tx, updated.TenantID, updated.ID, now); err != nil {
			return zero, err
		}
	}
	if err := insertEvent(ctx, tx, updated.TenantID, "Task", updated.ID, updated.ResourceVersion, "Task"+title(string(to)), map[string]any{
		"taskId": updated.ID, "phase": updated.Phase,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := auditHook(ctx, tx, updated.TenantID, "task.transitioned", "Task", updated.ID, map[string]any{
		"from": string(current.Phase), "to": string(to),
	}, now); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

func (s *Store) CreateRun(ctx context.Context, in kernelstore.CreateRunInput) (kernelstore.Run, error) {
	var zero kernelstore.Run
	if strings.TrimSpace(in.TenantID) == "" || in.TaskID == uuid.Nil || in.ExpectedTaskVersion <= 0 {
		return zero, fmt.Errorf("tenant, task id and positive task version are required")
	}
	if in.ID == uuid.Nil {
		in.ID = s.newID()
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.TaskID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if task.ResourceVersion != in.ExpectedTaskVersion {
		return zero, versionConflict("task", task.ID, in.ExpectedTaskVersion, task.ResourceVersion)
	}
	if err := domain.ValidateTaskTransition(task.Phase, domain.TaskRunning); err != nil {
		return zero, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	var ordinal int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(ordinal), 0) + 1 FROM runs WHERE tenant_id = $1 AND task_id = $2`, in.TenantID, task.ID.String()).Scan(&ordinal); err != nil {
		return zero, classify(err)
	}
	now := s.now()
	executionDeadline := taskExecutionDeadline(task.Spec, now)
	run, err := scanRun(tx.QueryRow(ctx, `INSERT INTO runs (
		id, tenant_id, task_id, ordinal, phase, resource_version, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 'PENDING', 1, $5, $5) RETURNING `+runColumns,
		in.ID.String(), task.TenantID, task.ID.String(), ordinal, now))
	if err != nil {
		return zero, classify(err)
	}
	updatedTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'RUNNING', active_run_id = $1,
		execution_deadline_at = $2, resource_version = resource_version + 1, updated_at = $3
		WHERE tenant_id = $4 AND id = $5 AND resource_version = $6 RETURNING `+taskColumns,
		run.ID.String(), executionDeadline, now, in.TenantID, task.ID.String(), in.ExpectedTaskVersion))
	if err != nil {
		return zero, classifyCAS(err, "task", task.ID, in.ExpectedTaskVersion)
	}
	if err := insertEvent(ctx, tx, task.TenantID, "Run", run.ID, run.ResourceVersion, "RunCreated", map[string]any{
		"runId": run.ID, "taskId": task.ID, "ordinal": run.Ordinal,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := insertEvent(ctx, tx, task.TenantID, "Task", task.ID, updatedTask.ResourceVersion, "TaskRunning", map[string]any{
		"taskId": task.ID, "runId": run.ID,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return run, nil
}

func (s *Store) AcquireAttempt(ctx context.Context, in kernelstore.AcquireAttemptInput) (kernelstore.AttemptLease, error) {
	var zero kernelstore.AttemptLease
	if strings.TrimSpace(in.TenantID) == "" || in.RunID == uuid.Nil || in.ExpectedRunVersion <= 0 ||
		in.TTL <= 0 || strings.TrimSpace(in.RuntimeClass) == "" || strings.TrimSpace(in.RuntimeInstanceID) == "" {
		return zero, fmt.Errorf("tenant, run, positive version, attempt runtime and positive lease TTL are required")
	}
	if in.AttemptID == uuid.Nil {
		in.AttemptID = s.newID()
	}
	if in.LeaseID == uuid.Nil {
		in.LeaseID = s.newID()
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.RunID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if run.ResourceVersion != in.ExpectedRunVersion {
		return zero, versionConflict("run", run.ID, in.ExpectedRunVersion, run.ResourceVersion)
	}
	if run.Phase.Terminal() {
		return zero, fmt.Errorf("%w: run %s is %s", kernelstore.ErrInvalidTransition, run.ID, run.Phase)
	}
	now := s.now()
	activeLease, leaseErr := scanLease(tx.QueryRow(ctx, `SELECT `+leaseColumns+`
		FROM runtime_leases WHERE tenant_id = $1 AND run_id = $2 AND released_at IS NULL FOR UPDATE`, in.TenantID, run.ID.String()))
	if leaseErr == nil {
		if activeLease.ExpiresAt.After(now) {
			return zero, fmt.Errorf("%w: run=%s expires_at=%s", kernelstore.ErrLeaseHeld, run.ID, activeLease.ExpiresAt.Format(time.RFC3339Nano))
		}
		if err := s.expireLeaseAndAttempt(ctx, tx, activeLease, now); err != nil {
			return zero, err
		}
	} else if !errors.Is(leaseErr, pgx.ErrNoRows) {
		return zero, classify(leaseErr)
	}

	var ordinal int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(ordinal), 0) + 1 FROM attempts WHERE tenant_id = $1 AND run_id = $2`, in.TenantID, run.ID.String()).Scan(&ordinal); err != nil {
		return zero, classify(err)
	}
	token := run.CurrentFencingToken + 1
	attempt, err := scanAttempt(tx.QueryRow(ctx, `INSERT INTO attempts (
		id, tenant_id, run_id, ordinal, phase, runtime_class, runtime_instance_id,
		fencing_token, resource_version, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 'PLACED', $5, $6, $7, 1, $8, $8) RETURNING `+attemptColumns,
		in.AttemptID.String(), run.TenantID, run.ID.String(), ordinal, in.RuntimeClass,
		in.RuntimeInstanceID, token, now))
	if err != nil {
		return zero, classify(err)
	}
	lease, err := scanLease(tx.QueryRow(ctx, `INSERT INTO runtime_leases (
		id, tenant_id, run_id, attempt_id, fencing_token, resource_version,
		acquired_at, heartbeat_at, expires_at
	) VALUES ($1, $2, $3, $4, $5, 1, $6, $6, $7) RETURNING `+leaseColumns,
		in.LeaseID.String(), run.TenantID, run.ID.String(), attempt.ID.String(), token, now, now.Add(in.TTL)))
	if err != nil {
		return zero, classify(err)
	}
	updatedRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'RUNNING', active_attempt_id = $1,
		current_fencing_token = $2, resource_version = resource_version + 1, updated_at = $3
		WHERE tenant_id = $4 AND id = $5 AND resource_version = $6 RETURNING `+runColumns,
		attempt.ID.String(), token, now, in.TenantID, run.ID.String(), in.ExpectedRunVersion))
	if err != nil {
		return zero, classifyCAS(err, "run", run.ID, in.ExpectedRunVersion)
	}
	if err := insertEvent(ctx, tx, run.TenantID, "Attempt", attempt.ID, attempt.ResourceVersion, "AttemptPlaced", map[string]any{
		"attemptId": attempt.ID, "runId": run.ID, "fencingToken": token,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := auditHook(ctx, tx, run.TenantID, "attempt.acquired", "Attempt", attempt.ID, map[string]any{
		"runId": run.ID, "fencingToken": token, "runtimeClass": in.RuntimeClass, "runtimeInstanceId": in.RuntimeInstanceID,
	}, now); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return kernelstore.AttemptLease{Attempt: attempt, Lease: lease, Run: updatedRun}, nil
}

func (s *Store) TransitionAttempt(ctx context.Context, in kernelstore.TransitionAttemptInput) (kernelstore.Attempt, error) {
	var zero kernelstore.Attempt
	if strings.TrimSpace(in.TenantID) == "" || in.AttemptID == uuid.Nil || in.FencingToken <= 0 || in.ExpectedAttemptVersion <= 0 {
		return zero, fmt.Errorf("tenant, attempt id, positive fencing token and positive resource version are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	var runID string
	if err := tx.QueryRow(ctx, `SELECT run_id::text FROM attempts WHERE tenant_id = $1 AND id = $2`, in.TenantID, in.AttemptID.String()).Scan(&runID); err != nil {
		return zero, classify(err)
	}
	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, runID))
	if err != nil {
		return zero, classify(err)
	}
	attempt, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.AttemptID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if err := requireCurrent(run, attempt, in.FencingToken); err != nil {
		return zero, err
	}
	if attempt.ResourceVersion != in.ExpectedAttemptVersion {
		return zero, versionConflict("attempt", attempt.ID, in.ExpectedAttemptVersion, attempt.ResourceVersion)
	}
	lease, err := scanLease(tx.QueryRow(ctx, `SELECT `+leaseColumns+` FROM runtime_leases
		WHERE tenant_id = $1 AND attempt_id = $2 AND released_at IS NULL FOR UPDATE`, in.TenantID, attempt.ID.String()))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, fmt.Errorf("%w: attempt %s has no active lease", kernelstore.ErrFenced, attempt.ID)
		}
		return zero, classify(err)
	}
	now := s.now()
	if !lease.ExpiresAt.After(now) {
		return zero, fmt.Errorf("%w: lease expired at %s", kernelstore.ErrFenced, lease.ExpiresAt.Format(time.RFC3339Nano))
	}
	if err := domain.ValidateAttemptTransition(attempt.Phase, in.To); err != nil {
		return zero, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	started := in.To == domain.AttemptStarting || in.To == domain.AttemptRunning
	finished := in.To.Terminal()
	updated, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = $1,
		failure_code = NULLIF($2, ''), failure_message = NULLIF($3, ''),
		started_at = CASE WHEN $4 THEN COALESCE(started_at, $5) ELSE started_at END,
		finished_at = CASE WHEN $6 THEN $5 ELSE finished_at END,
		resource_version = resource_version + 1, updated_at = $5
		WHERE tenant_id = $7 AND id = $8 AND resource_version = $9 RETURNING `+attemptColumns,
		in.To, in.FailureCode, in.FailureMessage, started, now, finished,
		in.TenantID, attempt.ID.String(), in.ExpectedAttemptVersion))
	if err != nil {
		return zero, classifyCAS(err, "attempt", attempt.ID, in.ExpectedAttemptVersion)
	}
	if in.To == domain.AttemptFailed || in.To == domain.AttemptCancelled {
		if _, err := tx.Exec(ctx, `UPDATE runtime_leases SET released_at = $1, release_reason = $2
			WHERE tenant_id = $3 AND id = $4 AND released_at IS NULL`, now, string(in.To), in.TenantID, lease.ID.String()); err != nil {
			return zero, classify(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET active_attempt_id = NULL,
			resource_version = resource_version + 1, updated_at = $1
			WHERE tenant_id = $2 AND id = $3 AND active_attempt_id = $4 AND current_fencing_token = $5`,
			now, in.TenantID, run.ID.String(), attempt.ID.String(), in.FencingToken); err != nil {
			return zero, classify(err)
		}
		// Terminate the run itself: a run left in RUNNING keeps claiming the
		// runs_one_active_per_task partial unique index and blocks every
		// future dispatch of this task (requeue or recovery alike).
		if in.To == domain.AttemptFailed {
			if _, err := tx.Exec(ctx, `UPDATE runs SET phase = $1, completed_at = $2,
				resource_version = resource_version + 1, updated_at = $2
				WHERE tenant_id = $3 AND id = $4 AND phase = $5`,
				domain.RunFailed, now, in.TenantID, run.ID.String(), domain.RunRunning); err != nil {
				return zero, classify(err)
			}
		}
		// v1.2: a cleanly failed attempt finalizes the task unless the
		// workload declared a multi-attempt retryPolicy (those keep the
		// recovery-driven requeue path). Without this, a task whose agent
		// exits FAILED stays RUNNING forever with no active attempt.
		task, taskErr := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE tenant_id = $1 AND id = $2 FOR UPDATE`,
			in.TenantID, run.TaskID.String()))
		if taskErr != nil {
			return zero, classify(taskErr)
		}
		maxAttempts := retryPolicyMaxAttempts(task.Spec)
		// Attempt ordinals are per-run; the retry budget spans every dispatch
		// of the task, so count all attempts ever placed for it.
		var usedAttempts int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM attempts a
			JOIN runs r ON r.tenant_id = a.tenant_id AND r.id = a.run_id
			WHERE r.tenant_id = $1 AND r.task_id = $2`, in.TenantID, task.ID.String()).Scan(&usedAttempts); err != nil {
			return zero, classify(err)
		}
		retriesRemain := in.To == domain.AttemptFailed && usedAttempts < maxAttempts
		switch {
		case retriesRemain:
			// Requeue for a fresh dispatch: the next attempt is a brand-new
			// run created by the standard admission/scheduling pipeline.
			// Without this branch a multi-attempt task would sit RUNNING
			// forever — the recovery controller only scans UNRELEASED
			// leases, and this path just released ours.
			if _, err := tx.Exec(ctx, `UPDATE tasks SET phase = $1, active_run_id = NULL,
				resource_version = resource_version + 1, updated_at = $2 WHERE tenant_id = $3 AND id = $4`,
				domain.TaskQueued, now, in.TenantID, task.ID.String()); err != nil {
				return zero, classify(err)
			}
			if err := s.releaseTenantReservation(ctx, tx, task.TenantID, task.ID); err != nil {
				return zero, err
			}
			if err := s.releaseRuntimeCapacity(ctx, tx, task.TenantID, task.ID, now); err != nil {
				return zero, err
			}
			if err := insertEvent(ctx, tx, task.TenantID, "Task", task.ID, task.ResourceVersion+1,
				"TaskRequeued", map[string]any{
					"taskId": task.ID, "attemptId": attempt.ID, "attemptsUsed": usedAttempts,
					"maxAttempts": maxAttempts, "failureCode": in.FailureCode, "trigger": "attempt." + string(in.To),
				}, now, s.newID()); err != nil {
				return zero, err
			}
		default:
			// Retry budget exhausted (or a cancellation): finalize the task.
			target := domain.TaskFailed
			if in.To == domain.AttemptCancelled {
				target = domain.TaskCancelled
			}
			if err := domain.ValidateTaskTransition(task.Phase, target); err == nil {
				if _, err := tx.Exec(ctx, `UPDATE tasks SET phase = $1,
					resource_version = resource_version + 1, updated_at = $2 WHERE tenant_id = $3 AND id = $4`,
					string(target), now, in.TenantID, task.ID.String()); err != nil {
					return zero, classify(err)
				}
				if err := s.releaseTenantReservation(ctx, tx, task.TenantID, task.ID); err != nil {
					return zero, err
				}
				if err := s.releaseRuntimeCapacity(ctx, tx, task.TenantID, task.ID, now); err != nil {
					return zero, err
				}
				if err := insertEvent(ctx, tx, task.TenantID, "Task", task.ID, task.ResourceVersion+1,
					"Task"+title(string(target)), map[string]any{
						"taskId": task.ID, "attemptId": attempt.ID, "trigger": "attempt." + string(in.To),
					}, now, s.newID()); err != nil {
					return zero, err
				}
			}
		}
	}
	if err := insertEvent(ctx, tx, attempt.TenantID, "Attempt", attempt.ID, updated.ResourceVersion, attemptEventType(in.To), map[string]any{
		"attemptId": attempt.ID, "runId": attempt.RunID, "phase": in.To, "fencingToken": in.FencingToken,
	}, now, s.newID()); err != nil {
		return zero, err
	}
	if err := auditHook(ctx, tx, attempt.TenantID, "attempt.transitioned", "Attempt", attempt.ID, map[string]any{
		"runId": attempt.RunID, "from": string(attempt.Phase), "to": string(in.To),
		"fencingToken": in.FencingToken, "failureCode": in.FailureCode,
	}, now); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

func (s *Store) HeartbeatLease(ctx context.Context, in kernelstore.HeartbeatLeaseInput) (kernelstore.Lease, error) {
	var zero kernelstore.Lease
	if strings.TrimSpace(in.TenantID) == "" || in.AttemptID == uuid.Nil || in.FencingToken <= 0 ||
		in.ExpectedLeaseVersion <= 0 || in.TTL <= 0 {
		return zero, fmt.Errorf("tenant, attempt id, positive fencing token, resource version and lease TTL are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	var runID string
	if err := tx.QueryRow(ctx, `SELECT run_id::text FROM attempts WHERE tenant_id = $1 AND id = $2`, in.TenantID, in.AttemptID.String()).Scan(&runID); err != nil {
		return zero, classify(err)
	}
	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, runID))
	if err != nil {
		return zero, classify(err)
	}
	if run.ActiveAttemptID == nil || *run.ActiveAttemptID != in.AttemptID || run.CurrentFencingToken != in.FencingToken {
		return zero, fmt.Errorf("%w: attempt is not the current run owner", kernelstore.ErrFenced)
	}
	lease, err := scanLease(tx.QueryRow(ctx, `SELECT `+leaseColumns+` FROM runtime_leases
		WHERE tenant_id = $1 AND attempt_id = $2 AND fencing_token = $3 AND released_at IS NULL FOR UPDATE`, in.TenantID, in.AttemptID.String(), in.FencingToken))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, fmt.Errorf("%w: active lease not found", kernelstore.ErrFenced)
		}
		return zero, classify(err)
	}
	if lease.ResourceVersion != in.ExpectedLeaseVersion {
		return zero, versionConflict("lease", lease.ID, in.ExpectedLeaseVersion, lease.ResourceVersion)
	}
	now := s.now()
	if !lease.ExpiresAt.After(now) {
		return zero, fmt.Errorf("%w: lease expired at %s", kernelstore.ErrFenced, lease.ExpiresAt.Format(time.RFC3339Nano))
	}
	updated, err := scanLease(tx.QueryRow(ctx, `UPDATE runtime_leases
		SET heartbeat_at = $1, expires_at = $2, resource_version = resource_version + 1
		WHERE tenant_id = $3 AND id = $4 AND resource_version = $5 AND released_at IS NULL RETURNING `+leaseColumns,
		now, now.Add(in.TTL), in.TenantID, lease.ID.String(), in.ExpectedLeaseVersion))
	if err != nil {
		return zero, classifyCAS(err, "lease", lease.ID, in.ExpectedLeaseVersion)
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

func (s *Store) CompleteRun(ctx context.Context, in kernelstore.CompleteRunInput) (kernelstore.Run, kernelstore.Task, error) {
	var zeroRun kernelstore.Run
	var zeroTask kernelstore.Task
	if strings.TrimSpace(in.TenantID) == "" || in.RunID == uuid.Nil || in.AttemptID == uuid.Nil ||
		in.FencingToken <= 0 || in.ExpectedRunVersion <= 0 {
		return zeroRun, zeroTask, fmt.Errorf("tenant, run, attempt and positive fencing/version values are required")
	}
	if strings.TrimSpace(in.ResultRef) == "" {
		return zeroRun, zeroTask, kernelstore.ErrResultRequired
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zeroRun, zeroTask, err
	}
	defer rollback(ctx, tx)

	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.RunID.String()))
	if err != nil {
		return zeroRun, zeroTask, classify(err)
	}
	if run.ResourceVersion != in.ExpectedRunVersion {
		return zeroRun, zeroTask, versionConflict("run", run.ID, in.ExpectedRunVersion, run.ResourceVersion)
	}
	if run.ActiveAttemptID == nil || *run.ActiveAttemptID != in.AttemptID || run.CurrentFencingToken != in.FencingToken {
		return zeroRun, zeroTask, fmt.Errorf("%w: completed attempt is not the current run owner", kernelstore.ErrFenced)
	}
	if err := domain.ValidateRunTransition(run.Phase, domain.RunCompleted); err != nil {
		return zeroRun, zeroTask, fmt.Errorf("%w: %v", kernelstore.ErrInvalidTransition, err)
	}
	attempt, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.AttemptID.String()))
	if err != nil {
		return zeroRun, zeroTask, classify(err)
	}
	if attempt.FencingToken != in.FencingToken {
		return zeroRun, zeroTask, fmt.Errorf("%w: attempt token is stale", kernelstore.ErrFenced)
	}
	if attempt.Phase != domain.AttemptCompleted {
		return zeroRun, zeroTask, fmt.Errorf("%w: attempt phase is %s", kernelstore.ErrCompletionPending, attempt.Phase)
	}
	now := s.now()
	updatedRun, err := scanRun(tx.QueryRow(ctx, `UPDATE runs SET phase = 'COMPLETED', result_ref = $1,
		resource_version = resource_version + 1, updated_at = $2, completed_at = $2
		WHERE tenant_id = $3 AND id = $4 AND resource_version = $5 RETURNING `+runColumns,
		in.ResultRef, now, in.TenantID, run.ID.String(), in.ExpectedRunVersion))
	if err != nil {
		return zeroRun, zeroTask, classifyCAS(err, "run", run.ID, in.ExpectedRunVersion)
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, run.TaskID.String()))
	if err != nil {
		return zeroRun, zeroTask, classify(err)
	}
	if task.Phase != domain.TaskRunning || task.ActiveRunID == nil || *task.ActiveRunID != run.ID {
		return zeroRun, zeroTask, fmt.Errorf("%w: task is not running this run", kernelstore.ErrInvalidTransition)
	}
	updatedTask, err := scanTask(tx.QueryRow(ctx, `UPDATE tasks SET phase = 'SUCCEEDED', result_ref = $1,
		resource_version = resource_version + 1, updated_at = $2
		WHERE tenant_id = $3 AND id = $4 AND resource_version = $5 RETURNING `+taskColumns,
		in.ResultRef, now, in.TenantID, task.ID.String(), task.ResourceVersion))
	if err != nil {
		return zeroRun, zeroTask, classifyCAS(err, "task", task.ID, task.ResourceVersion)
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime_leases SET released_at = $1, release_reason = 'COMPLETED'
		WHERE tenant_id = $2 AND attempt_id = $3 AND fencing_token = $4 AND released_at IS NULL`, now, in.TenantID, attempt.ID.String(), in.FencingToken); err != nil {
		return zeroRun, zeroTask, classify(err)
	}
	// Quota reservation release (v0.8): the task is terminal, so its reserved
	// ceiling is returned to the tenant window it was admitted in.
	if err := s.releaseTenantReservation(ctx, tx, task.TenantID, task.ID); err != nil {
		return zeroRun, zeroTask, err
	}
	if err := s.releaseRuntimeCapacity(ctx, tx, task.TenantID, task.ID, now); err != nil {
		return zeroRun, zeroTask, err
	}
	if err := insertEvent(ctx, tx, run.TenantID, "Run", run.ID, updatedRun.ResourceVersion, "RunCompleted", map[string]any{
		"runId": run.ID, "attemptId": attempt.ID, "resultRef": in.ResultRef,
	}, now, s.newID()); err != nil {
		return zeroRun, zeroTask, err
	}
	if err := insertEvent(ctx, tx, task.TenantID, "Task", task.ID, updatedTask.ResourceVersion, "TaskSucceeded", map[string]any{
		"taskId": task.ID, "runId": run.ID, "resultRef": in.ResultRef,
	}, now, s.newID()); err != nil {
		return zeroRun, zeroTask, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zeroRun, zeroTask, classify(err)
	}
	return updatedRun, updatedTask, nil
}

func (s *Store) now() time.Time { return s.clock().UTC() }

// begin opens a READ COMMITTED transaction: state machines are expressed
// with resource-version CAS, targeted row locks and unique constraints
// (ADR-002). SERIALIZABLE was measured at ~94% conflict under 16-way
// concurrent completions (PostgreSQL SSI page-level predicates), so it is
// not used for the v0.1 hot paths; every invariant is enforced by row locks
// and constraints instead.
func (s *Store) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, classify(err)
	}
	return tx, nil
}

func rollback(ctx context.Context, tx pgx.Tx) {
	_ = tx.Rollback(context.WithoutCancel(ctx))
}

func (s *Store) expireLeaseAndAttempt(ctx context.Context, tx pgx.Tx, lease kernelstore.Lease, now time.Time) error {
	attempt, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM attempts WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, lease.TenantID, lease.AttemptID.String()))
	if err != nil {
		return classify(err)
	}
	if attempt.Phase == domain.AttemptCompleted {
		return fmt.Errorf("%w: attempt %s completed before its result was committed", kernelstore.ErrCompletionPending, attempt.ID)
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime_leases SET released_at = $1, release_reason = 'EXPIRED'
		WHERE tenant_id = $2 AND id = $3 AND released_at IS NULL`, now, lease.TenantID, lease.ID.String()); err != nil {
		return classify(err)
	}
	updated, err := scanAttempt(tx.QueryRow(ctx, `UPDATE attempts SET phase = 'ATTEMPT_FAILED', failure_code = 'LEASE_EXPIRED',
		failure_message = 'runtime lease expired before completion', finished_at = $1,
		resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND phase NOT IN ('COMPLETED', 'ATTEMPT_FAILED', 'CANCELLED')
		RETURNING `+attemptColumns, now, lease.TenantID, lease.AttemptID.String()))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return classify(err)
	}
	if err := insertEvent(ctx, tx, updated.TenantID, "Attempt", updated.ID, updated.ResourceVersion, "AttemptFailed", map[string]any{
		"attemptId": updated.ID, "runId": updated.RunID, "failureCode": "LEASE_EXPIRED", "fencingToken": updated.FencingToken,
	}, now, s.newID()); err != nil {
		return err
	}
	return nil
}

func requireCurrent(run kernelstore.Run, attempt kernelstore.Attempt, token int64) error {
	if attempt.FencingToken != token || run.CurrentFencingToken != token || run.ActiveAttemptID == nil || *run.ActiveAttemptID != attempt.ID {
		return fmt.Errorf("%w: attempt %s does not own run %s at token %d", kernelstore.ErrFenced, attempt.ID, run.ID, token)
	}
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, tenantID, aggregateType string, aggregateID uuid.UUID, version int64, eventType string, payload any, at time.Time, eventID uuid.UUID) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode outbox payload: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (
		id, tenant_id, aggregate_type, aggregate_id, aggregate_version, event_type, payload, occurred_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, eventID.String(), tenantID, aggregateType, aggregateID.String(), version, eventType, encoded, at)
	if err != nil {
		return classify(err)
	}
	return nil
}

func versionConflict(kind string, id uuid.UUID, expected, actual int64) error {
	return fmt.Errorf("%w: %s=%s expected=%d actual=%d", kernelstore.ErrVersionConflict, kind, id, expected, actual)
}

func classifyCAS(err error, kind string, id uuid.UUID, expected int64) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s=%s expected=%d", kernelstore.ErrVersionConflict, kind, id, expected)
	}
	return classify(err)
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: postgres %s", kernelstore.ErrVersionConflict, pgErr.Code)
		case "40001", "40P01":
			return fmt.Errorf("%w: %w: postgres %s", kernelstore.ErrVersionConflict, kernelstore.ErrRetryableTransaction, pgErr.Code)
		}
	}
	return err
}

func title(value string) string {
	parts := strings.Split(strings.ToLower(value), "_")
	for i := range parts {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

func attemptEventType(phase domain.AttemptPhase) string {
	if phase == domain.AttemptFailed {
		return "AttemptFailed"
	}
	return "Attempt" + title(string(phase))
}

func taskExecutionDeadline(raw json.RawMessage, startedAt time.Time) *time.Time {
	var timing struct {
		Deadline *time.Time `json:"deadline"`
		Budget   struct {
			WallSeconds int64 `json:"wallSeconds"`
		} `json:"budget"`
	}
	if json.Unmarshal(raw, &timing) != nil {
		return nil
	}
	var effective *time.Time
	if timing.Deadline != nil && !timing.Deadline.IsZero() {
		deadline := timing.Deadline.UTC()
		effective = &deadline
	}
	if timing.Budget.WallSeconds > 0 {
		wall := startedAt.UTC().Add(time.Duration(timing.Budget.WallSeconds) * time.Second)
		if effective == nil || wall.Before(*effective) {
			effective = &wall
		}
	}
	return effective
}

const taskColumns = `
	id::text, tenant_id, namespace, agent_version_ref, agent_version_id::text, goal, spec, request_hash,
	idempotency_key, phase, admission_reason_code, admitted_at, cancel_requested_at, active_run_id::text, result_ref,
	workflow_id::text, workflow_step_id::text, workflow_step_name, workflow_attempt, parent_task_id::text, execution_deadline_at,
	next_schedule_attempt_at, schedule_retry_count, last_schedule_rejection,
	resource_version, created_at, updated_at`

const runColumns = `
	id::text, tenant_id, task_id::text, ordinal, phase, active_attempt_id::text,
	current_fencing_token, result_ref, resource_version, created_at, updated_at, completed_at`

const attemptColumns = `
	id::text, tenant_id, run_id::text, ordinal, phase, runtime_class,
	runtime_pool_id, runtime_instance_id, fencing_token, failure_code, failure_message,
	resource_version, created_at, updated_at, started_at, finished_at`

const leaseColumns = `
	id::text, tenant_id, run_id::text, attempt_id::text, fencing_token,
	resource_version, acquired_at, heartbeat_at, expires_at`

type scanner interface{ Scan(...any) error }

func scanTask(row scanner) (kernelstore.Task, error) {
	var task kernelstore.Task
	var id string
	var agentVersionID sql.NullString
	var admissionReason, activeID, result, workflowID, workflowStepID, workflowStepName, parentTaskID sql.NullString
	var workflowAttempt sql.NullInt32
	var admitted, cancel, executionDeadline, nextSchedule sql.NullTime
	var hash, scheduleRejection []byte
	if err := row.Scan(&id, &task.TenantID, &task.Namespace, &task.AgentVersionRef, &agentVersionID,
		&task.Goal, &task.Spec, &hash, &task.IdempotencyKey, &task.Phase, &admissionReason, &admitted, &cancel,
		&activeID, &result, &workflowID, &workflowStepID, &workflowStepName, &workflowAttempt, &parentTaskID, &executionDeadline,
		&nextSchedule, &task.ScheduleRetryCount, &scheduleRejection,
		&task.ResourceVersion, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return task, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return task, fmt.Errorf("parse task id: %w", err)
	}
	task.ID = parsed
	if agentVersionID.Valid {
		parsed, err := uuid.Parse(agentVersionID.String)
		if err != nil {
			return task, fmt.Errorf("parse agent version id: %w", err)
		}
		task.AgentVersionID = &parsed
	}
	if len(hash) != len(task.RequestHash) {
		return task, fmt.Errorf("task request hash has length %d", len(hash))
	}
	copy(task.RequestHash[:], hash)
	if admissionReason.Valid {
		task.AdmissionReasonCode = admissionReason.String
	}
	if admitted.Valid {
		task.AdmittedAt = &admitted.Time
	}
	if cancel.Valid {
		task.CancelRequestedAt = &cancel.Time
	}
	if executionDeadline.Valid {
		task.ExecutionDeadlineAt = &executionDeadline.Time
	}
	if nextSchedule.Valid {
		task.NextScheduleAttemptAt = &nextSchedule.Time
	}
	if scheduleRejection != nil {
		task.LastScheduleRejection = scheduleRejection
	}
	if activeID.Valid {
		parsed, err := uuid.Parse(activeID.String)
		if err != nil {
			return task, fmt.Errorf("parse active run id: %w", err)
		}
		task.ActiveRunID = &parsed
	}
	if result.Valid {
		task.ResultRef = result.String
	}
	if workflowID.Valid {
		parsed, parseErr := uuid.Parse(workflowID.String)
		if parseErr != nil {
			return task, fmt.Errorf("parse workflow id: %w", parseErr)
		}
		task.WorkflowID = &parsed
	}
	if workflowStepID.Valid {
		parsed, parseErr := uuid.Parse(workflowStepID.String)
		if parseErr != nil {
			return task, fmt.Errorf("parse workflow step id: %w", parseErr)
		}
		task.WorkflowStepID = &parsed
	}
	if workflowStepName.Valid {
		task.WorkflowStepName = workflowStepName.String
	}
	if workflowAttempt.Valid {
		task.WorkflowAttempt = int(workflowAttempt.Int32)
	}
	if parentTaskID.Valid {
		parsed, parseErr := uuid.Parse(parentTaskID.String)
		if parseErr != nil {
			return task, fmt.Errorf("parse parent task id: %w", parseErr)
		}
		task.ParentTaskID = &parsed
	}
	return task, nil
}

func scanRun(row scanner) (kernelstore.Run, error) {
	var run kernelstore.Run
	var id, taskID string
	var activeID, result sql.NullString
	var completed sql.NullTime
	if err := row.Scan(&id, &run.TenantID, &taskID, &run.Ordinal, &run.Phase, &activeID,
		&run.CurrentFencingToken, &result, &run.ResourceVersion, &run.CreatedAt,
		&run.UpdatedAt, &completed); err != nil {
		return run, err
	}
	var err error
	if run.ID, err = uuid.Parse(id); err != nil {
		return run, fmt.Errorf("parse run id: %w", err)
	}
	if run.TaskID, err = uuid.Parse(taskID); err != nil {
		return run, fmt.Errorf("parse task id: %w", err)
	}
	if activeID.Valid {
		parsed, err := uuid.Parse(activeID.String)
		if err != nil {
			return run, fmt.Errorf("parse active attempt id: %w", err)
		}
		run.ActiveAttemptID = &parsed
	}
	if result.Valid {
		run.ResultRef = result.String
	}
	if completed.Valid {
		run.CompletedAt = &completed.Time
	}
	return run, nil
}

func scanAttempt(row scanner) (kernelstore.Attempt, error) {
	var attempt kernelstore.Attempt
	var id, runID string
	var runtimePool, failureCode, failureMessage sql.NullString
	var started, finished sql.NullTime
	if err := row.Scan(&id, &attempt.TenantID, &runID, &attempt.Ordinal, &attempt.Phase,
		&attempt.RuntimeClass, &runtimePool, &attempt.RuntimeInstanceID, &attempt.FencingToken,
		&failureCode, &failureMessage, &attempt.ResourceVersion, &attempt.CreatedAt,
		&attempt.UpdatedAt, &started, &finished); err != nil {
		return attempt, err
	}
	var err error
	if attempt.ID, err = uuid.Parse(id); err != nil {
		return attempt, fmt.Errorf("parse attempt id: %w", err)
	}
	if attempt.RunID, err = uuid.Parse(runID); err != nil {
		return attempt, fmt.Errorf("parse run id: %w", err)
	}
	if failureCode.Valid {
		attempt.FailureCode = failureCode.String
	}
	if runtimePool.Valid {
		attempt.RuntimePoolID = runtimePool.String
	}
	if failureMessage.Valid {
		attempt.FailureMessage = failureMessage.String
	}
	if started.Valid {
		attempt.StartedAt = &started.Time
	}
	if finished.Valid {
		attempt.FinishedAt = &finished.Time
	}
	return attempt, nil
}

func scanLease(row scanner) (kernelstore.Lease, error) {
	var lease kernelstore.Lease
	var id, runID, attemptID string
	if err := row.Scan(&id, &lease.TenantID, &runID, &attemptID, &lease.FencingToken,
		&lease.ResourceVersion, &lease.AcquiredAt, &lease.HeartbeatAt, &lease.ExpiresAt); err != nil {
		return lease, err
	}
	var err error
	if lease.ID, err = uuid.Parse(id); err != nil {
		return lease, fmt.Errorf("parse lease id: %w", err)
	}
	if lease.RunID, err = uuid.Parse(runID); err != nil {
		return lease, fmt.Errorf("parse run id: %w", err)
	}
	if lease.AttemptID, err = uuid.Parse(attemptID); err != nil {
		return lease, fmt.Errorf("parse attempt id: %w", err)
	}
	return lease, nil
}

// retryPolicyMaxAttempts reports the declared per-task retry budget: the
// v1.2 default runs exactly one attempt; budgets above one requeue the task
// for a fresh dispatch when an attempt fails cleanly with retries left, and
// finalize it once the budget is exhausted.
func retryPolicyMaxAttempts(spec []byte) int {
	var policy struct {
		RetryPolicy struct {
			MaxAttempts int `json:"maxAttempts"`
		} `json:"retryPolicy"`
	}
	if json.Unmarshal(spec, &policy) != nil {
		return 1
	}
	if policy.RetryPolicy.MaxAttempts < 1 {
		return 1
	}
	return policy.RetryPolicy.MaxAttempts
}
