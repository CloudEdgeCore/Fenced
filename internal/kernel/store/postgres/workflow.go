package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	kernelmoney "github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/errorcode"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const workflowColumns = `id, tenant_id, namespace, idempotency_key, goal, spec, status,
	COALESCE(failure_code, ''), cancel_requested_at,
	COALESCE(budget_max_tasks, 0), COALESCE(budget_max_tokens, 0), COALESCE(budget_max_cost_micro_usd, 0),
	budget_exhausted_at, deadline_at, deadline_exceeded_at, resource_version, created_at, updated_at,
	needs_budget_reconciliation`

const workflowStepColumns = `id, tenant_id, workflow_id, name, ordinal, status, attempt_count, task_id,
	result_summary, COALESCE(failure_code, ''), COALESCE(decided_by, ''), COALESCE(approval_decision, ''), decided_at,
	COALESCE(parent_step_name, ''), spawn_depth, is_dynamic, COALESCE(spawn_key, ''),
	COALESCE(goal, ''), COALESCE(agent_version_ref, ''), spec, COALESCE(max_attempts, 0),
	reserved_tasks, reserved_tokens, reserved_cost_micro_usd,
	resource_version, created_at, updated_at`

func scanWorkflow(row pgx.Row) (kernelstore.Workflow, error) {
	var workflow kernelstore.Workflow
	err := row.Scan(&workflow.ID, &workflow.TenantID, &workflow.Namespace, &workflow.IdempotencyKey,
		&workflow.Goal, &workflow.Spec, &workflow.Status, &workflow.FailureCode, &workflow.CancelRequestedAt,
		&workflow.BudgetMaxTasks, &workflow.BudgetMaxTokens, &workflow.BudgetMaxCostMicroUSD,
		&workflow.BudgetExhaustedAt, &workflow.DeadlineAt, &workflow.DeadlineExceededAt,
		&workflow.ResourceVersion, &workflow.CreatedAt, &workflow.UpdatedAt,
		&workflow.NeedsBudgetReconciliation)
	if err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	return workflow, nil
}

func scanWorkflowStep(row pgx.Row) (kernelstore.WorkflowStep, error) {
	var step kernelstore.WorkflowStep
	var summary []byte
	err := row.Scan(&step.ID, &step.TenantID, &step.WorkflowID, &step.Name, &step.Ordinal, &step.Status,
		&step.AttemptCount, &step.TaskID, &summary, &step.FailureCode, &step.DecidedBy, &step.ApprovalDecision, &step.DecidedAt,
		&step.ParentStepName, &step.SpawnDepth, &step.IsDynamic, &step.SpawnKey,
		&step.Goal, &step.AgentVersionRef, &step.Spec, &step.MaxAttempts,
		&step.ReservedTasks, &step.ReservedTokens, &step.ReservedCostMicroUSD,
		&step.ResourceVersion, &step.CreatedAt, &step.UpdatedAt)
	if summary != nil {
		step.ResultSummary = summary
	}
	if err != nil {
		return kernelstore.WorkflowStep{}, classify(err)
	}
	return step, nil
}

func jsonRawNull(raw []byte) []byte { return raw }

// CreateWorkflow inserts the workflow and its steps atomically. An
// idempotency-key replay returns the existing definition; a conflicting
// definition under the same key is rejected.
func (s *Store) CreateWorkflow(ctx context.Context, in kernelstore.CreateWorkflowInput) (kernelstore.CreateWorkflowResult, error) {
	hash, err := in.RequestHash()
	if err != nil {
		return kernelstore.CreateWorkflowResult{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.CreateWorkflowResult{}, err
	}
	defer rollback(ctx, tx)
	now := s.now()
	created, err := scanWorkflow(tx.QueryRow(ctx, `
		INSERT INTO workflows (id, tenant_id, namespace, idempotency_key, goal, spec, request_hash,
			status, budget_max_tasks, budget_max_tokens, budget_max_cost_micro_usd,
			deadline_at, step_count, resource_version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8, $9, $10, $11, $12, 1, $13, $13)
		ON CONFLICT (tenant_id, namespace, idempotency_key) DO UPDATE SET updated_at = workflows.updated_at
		RETURNING `+workflowColumns,
		in.ID, in.TenantID, in.Namespace, in.IdempotencyKey, in.Goal, in.Spec, hash[:],
		nullableInt64(in.BudgetMaxTasks), nullableInt64(in.BudgetMaxTokens), nullableInt64(int64(in.BudgetMaxCostMicroUSD)),
		in.DeadlineAt, len(in.Steps), now))
	if err != nil {
		return kernelstore.CreateWorkflowResult{}, err
	}
	// Replay: the returned ID differs from the input when the idempotency
	// key already existed; the definition must match canonically (jsonb
	// normalizes key order).
	if created.ID != in.ID {
		if canonicalJSON(created.Spec) != canonicalJSON(in.Spec) || created.Goal != in.Goal {
			return kernelstore.CreateWorkflowResult{}, fmt.Errorf("%w: idempotency key replays a different definition",
				kernelstore.ErrIdempotencyConflict)
		}
		return kernelstore.CreateWorkflowResult{Workflow: created, Existing: true}, nil
	}
	// Declared steps hold their workflow budget reservation from creation:
	// the workflow's promise is complete before any reconcile runs. A
	// declaration that already exceeds a ceiling fails closed here as well
	// (publication validation rejects it first for API callers).
	var reservedTokens, reservedCost int64
	for _, step := range in.Steps {
		tokens, cost := kernelstore.TaskSpecBudgetReservation(step.Spec)
		reservedTokens += tokens
		reservedCost += int64(cost)
	}
	if in.BudgetMaxTasks > 0 && int64(len(in.Steps)) > in.BudgetMaxTasks {
		return kernelstore.CreateWorkflowResult{}, fmt.Errorf("workflow declares %d steps over its budget of %d tasks",
			len(in.Steps), in.BudgetMaxTasks)
	}
	if in.BudgetMaxTokens > 0 && reservedTokens > in.BudgetMaxTokens {
		return kernelstore.CreateWorkflowResult{}, fmt.Errorf("declared step budgets reserve %d tokens over the workflow budget of %d",
			reservedTokens, in.BudgetMaxTokens)
	}
	if in.BudgetMaxCostMicroUSD > 0 && reservedCost > int64(in.BudgetMaxCostMicroUSD) {
		return kernelstore.CreateWorkflowResult{}, fmt.Errorf("declared step budgets reserve %d micro-USD over the workflow budget of %d",
			reservedCost, int64(in.BudgetMaxCostMicroUSD))
	}
	for ordinal, step := range in.Steps {
		stepTokens, stepCost := kernelstore.TaskSpecBudgetReservation(step.Spec)
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow_steps (id, tenant_id, workflow_id, name, ordinal, status, attempt_count,
				reserved_tasks, reserved_tokens, reserved_cost_micro_usd,
				resource_version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 'PENDING', 0, 1, $6, $7, 1, $8, $8)`,
			uuid.New(), in.TenantID, in.ID, step.Name, ordinal, stepTokens, int64(stepCost), now); err != nil {
			return kernelstore.CreateWorkflowResult{}, classify(err)
		}
	}
	// The per-step reservations land on the usage ledger through the
	// workflow_steps insert trigger in the same transaction.
	if err := insertEvent(ctx, tx, in.TenantID, "Workflow", created.ID, created.ResourceVersion, "WorkflowCreated", map[string]any{
		"workflowId": created.ID, "stepCount": len(in.Steps),
	}, now, s.newID()); err != nil {
		return kernelstore.CreateWorkflowResult{}, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "workflow.create", "Workflow", created.ID, map[string]any{
		"stepCount": len(in.Steps), "namespace": in.Namespace,
	}, now); err != nil {
		return kernelstore.CreateWorkflowResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.CreateWorkflowResult{}, classify(err)
	}
	return kernelstore.CreateWorkflowResult{Workflow: created}, nil
}

// GetWorkflow reads one workflow inside the tenant scope.
func (s *Store) GetWorkflow(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.Workflow, error) {
	workflow, err := scanWorkflow(s.pool.QueryRow(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE tenant_id = $1 AND id = $2`, tenantID, id))
	if errors.Is(err, kernelstore.ErrNotFound) {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	return workflow, err
}

// ListActiveWorkflows returns non-terminal workflows oldest-first.
func (s *Store) ListActiveWorkflows(ctx context.Context, batch int) ([]kernelstore.Workflow, error) {
	if batch <= 0 {
		batch = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE status IN ('PENDING','RUNNING')
		ORDER BY created_at ASC LIMIT $1`, batch)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	workflows := []kernelstore.Workflow{}
	for rows.Next() {
		workflow, err := scanWorkflowRow(rows)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, workflow)
	}
	return workflows, rows.Err()
}

func scanWorkflowRow(rows pgx.Rows) (kernelstore.Workflow, error) {
	var workflow kernelstore.Workflow
	var spec []byte
	err := rows.Scan(&workflow.ID, &workflow.TenantID, &workflow.Namespace, &workflow.IdempotencyKey,
		&workflow.Goal, &spec, &workflow.Status, &workflow.FailureCode, &workflow.CancelRequestedAt,
		&workflow.BudgetMaxTasks, &workflow.BudgetMaxTokens, &workflow.BudgetMaxCostMicroUSD,
		&workflow.BudgetExhaustedAt, &workflow.DeadlineAt, &workflow.DeadlineExceededAt,
		&workflow.ResourceVersion, &workflow.CreatedAt, &workflow.UpdatedAt,
		&workflow.NeedsBudgetReconciliation)
	workflow.Spec = spec
	if err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	return workflow, nil
}

// ListWorkflowSteps returns the workflow's steps in declaration order.
func (s *Store) ListWorkflowSteps(ctx context.Context, tenantID string, workflowID uuid.UUID) ([]kernelstore.WorkflowStep, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+workflowStepColumns+` FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2
			ORDER BY ordinal ASC`, tenantID, workflowID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	steps := []kernelstore.WorkflowStep{}
	for rows.Next() {
		var step kernelstore.WorkflowStep
		var summary []byte
		var spec []byte
		if err := rows.Scan(&step.ID, &step.TenantID, &step.WorkflowID, &step.Name, &step.Ordinal,
			&step.Status, &step.AttemptCount, &step.TaskID, &summary, &step.FailureCode, &step.DecidedBy,
			&step.ApprovalDecision, &step.DecidedAt, &step.ParentStepName, &step.SpawnDepth, &step.IsDynamic, &step.SpawnKey,
			&step.Goal, &step.AgentVersionRef, &spec, &step.MaxAttempts,
			&step.ReservedTasks, &step.ReservedTokens, &step.ReservedCostMicroUSD,
			&step.ResourceVersion, &step.CreatedAt, &step.UpdatedAt); err != nil {
			return nil, classify(err)
		}
		step.ResultSummary = summary
		step.Spec = spec
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

// GetWorkflowStep returns one tenant-scoped step without rescanning the
// workflow. The orchestrator uses it to refresh its in-memory snapshot after
// a CAS transition, keeping large DAG reconciliation linear.
func (s *Store) GetWorkflowStep(ctx context.Context, tenantID string, workflowID uuid.UUID, name string) (kernelstore.WorkflowStep, error) {
	return scanWorkflowStep(s.pool.QueryRow(ctx,
		`SELECT `+workflowStepColumns+` FROM workflow_steps
		 WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3`, tenantID, workflowID, name))
}

// TransitionWorkflow CAS-transitions the workflow status.
func (s *Store) TransitionWorkflow(ctx context.Context, in kernelstore.TransitionWorkflowInput) (kernelstore.Workflow, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	defer rollback(ctx, tx)
	current, err := scanWorkflow(tx.QueryRow(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`,
		in.TenantID, in.WorkflowID))
	if errors.Is(err, kernelstore.ErrWorkflowNotFound) {
		return kernelstore.Workflow{}, err
	}
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	if current.ResourceVersion != in.ExpectedVersion {
		return kernelstore.Workflow{}, versionConflict("workflow", in.WorkflowID, in.ExpectedVersion, current.ResourceVersion)
	}
	if !kernelstore.CanTransitionWorkflow(current.Status, in.To) {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow %s -> %s",
			kernelstore.ErrInvalidTransition, current.Status, in.To)
	}
	now := s.now()
	// Write fencing: in claiming mode only the instance holding the live
	// claim may transition the workflow (the row lock above serializes the
	// check against concurrent claim steals).
	if err := requireClaimOwnership(ctx, tx, in.TenantID, in.WorkflowID, in.ExpectedOwner, now); err != nil {
		return kernelstore.Workflow{}, err
	}
	updated, err := scanWorkflow(tx.QueryRow(ctx, `
		UPDATE workflows SET status = $4, failure_code = NULLIF($5, ''), resource_version = resource_version + 1,
			updated_at = $6 WHERE tenant_id = $1 AND id = $2 AND resource_version = $3 RETURNING `+workflowColumns,
		in.TenantID, in.WorkflowID, in.ExpectedVersion, in.To, in.FailureCode, now))
	if err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	if err := insertEvent(ctx, tx, in.TenantID, "Workflow", in.WorkflowID, updated.ResourceVersion,
		"Workflow"+title(string(in.To)), map[string]any{"workflowId": in.WorkflowID, "status": in.To, "failureCode": in.FailureCode}, now, s.newID()); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "workflow.transition", "Workflow", in.WorkflowID,
		map[string]any{"from": current.Status, "to": in.To, "failureCode": in.FailureCode}, now); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	return updated, nil
}

// requireClaimOwnership fences a workflow write to the instance that holds a
// live claim when expectedOwner is non-empty. It locks the workflow row so
// the check cannot race a claim steal; callers must hold or acquire that
// lock themselves (TransitionWorkflow already did, TransitionWorkflowStep
// locks the row here). Empty expectedOwner disables the check.
func requireClaimOwnership(ctx context.Context, tx pgx.Tx, tenantID string, workflowID uuid.UUID, expectedOwner string, now time.Time) error {
	if expectedOwner == "" {
		return nil
	}
	var owner string
	var until time.Time
	if err := tx.QueryRow(ctx,
		`SELECT orchestrator_claim, orchestrator_claim_until FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`,
		tenantID, workflowID.String()).Scan(&owner, &until); err != nil {
		return classify(err)
	}
	if owner != expectedOwner || !until.After(now) {
		return kernelstore.ErrFenced
	}
	return nil
}

// TransitionWorkflowStep CAS-transitions one step, applying the optional
// task id, attempt count, result summary and failure code.
func (s *Store) TransitionWorkflowStep(ctx context.Context, in kernelstore.TransitionWorkflowStepInput) (kernelstore.WorkflowStep, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	defer rollback(ctx, tx)
	current, err := scanWorkflowStep(tx.QueryRow(ctx,
		`SELECT `+workflowStepColumns+` FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3 FOR UPDATE`,
		in.TenantID, in.WorkflowID, in.StepName))
	if err != nil {
		if errors.Is(err, kernelstore.ErrNotFound) {
			return kernelstore.WorkflowStep{}, kernelstore.ErrStepNotFound
		}
		return kernelstore.WorkflowStep{}, err
	}
	if current.ResourceVersion != in.ExpectedVersion {
		return kernelstore.WorkflowStep{}, versionConflict("workflow step", current.ID, in.ExpectedVersion, current.ResourceVersion)
	}
	if !kernelstore.CanTransitionStep(current.Status, in.To) {
		return kernelstore.WorkflowStep{}, fmt.Errorf("%w: step %s -> %s",
			kernelstore.ErrInvalidTransition, current.Status, in.To)
	}
	// Write fencing: a step transition is owned by the orchestrator instance
	// that holds the parent workflow's live claim (see requireClaimOwnership).
	if err := requireClaimOwnership(ctx, tx, in.TenantID, in.WorkflowID, in.ExpectedOwner, s.now()); err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	taskID := current.TaskID
	if in.TaskID != nil {
		taskID = in.TaskID
	}
	attemptCount := current.AttemptCount
	if in.AttemptCount != nil {
		attemptCount = *in.AttemptCount
	}
	summary := current.ResultSummary
	if in.ResultSummary != nil {
		summary = in.ResultSummary
	}
	failureCode := current.FailureCode
	if in.FailureCode != "" {
		failureCode = in.FailureCode
	} else if in.To == kernelstore.StepSucceeded || in.To == kernelstore.StepRunning {
		failureCode = ""
	}
	// Reservation accounting. Terminal transitions release whatever the
	// step still holds (skips and cancellations of undispatched steps; a
	// dispatched step's reservation already transferred to its task). The
	// RUNNING→PENDING retry re-reserves the next attempt's ceiling under
	// the budget guard, exactly like a fresh spawn.
	reserveTasks, reserveTokens, reserveCost := current.ReservedTasks, current.ReservedTokens, current.ReservedCostMicroUSD
	if in.To.Terminal() {
		reserveTasks, reserveTokens, reserveCost = 0, 0, 0
	} else if current.Status == kernelstore.StepRunning && in.To == kernelstore.StepPending {
		reserveTasks, reserveTokens, reserveCost = 1, 0, 0
		reserveTokens, reserveCost = kernelstore.TaskSpecBudgetReservation(current.Spec)
		var budgets struct {
			Tasks  *int64
			Tokens *int64
			Cost   *int64
		}
		if err := tx.QueryRow(ctx,
			`SELECT budget_max_tasks, budget_max_tokens, budget_max_cost_micro_usd
			 FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`,
			in.TenantID, in.WorkflowID).Scan(&budgets.Tasks, &budgets.Tokens, &budgets.Cost); err != nil {
			return kernelstore.WorkflowStep{}, classify(err)
		}
		usage, err := s.workflowTaskUsage(ctx, tx, in.TenantID, in.WorkflowID)
		if err != nil {
			return kernelstore.WorkflowStep{}, err
		}
		if budgets.Tasks != nil && *budgets.Tasks > 0 && usage.CommittedTasks() >= *budgets.Tasks {
			return kernelstore.WorkflowStep{}, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted,
				Message: fmt.Sprintf("retry denied: workflow budget exhausted: %d/%d tasks", usage.CommittedTasks(), *budgets.Tasks)}
		}
		if budgets.Tokens != nil && *budgets.Tokens > 0 && usage.CommittedTokens()+reserveTokens > *budgets.Tokens {
			return kernelstore.WorkflowStep{}, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted,
				Message: fmt.Sprintf("retry denied: workflow budget exhausted: %d/%d tokens", usage.CommittedTokens()+reserveTokens, *budgets.Tokens)}
		}
		if budgets.Cost != nil && *budgets.Cost > 0 && usage.CommittedCostMicroUSD()+reserveCost > kernelmoney.MicroUSD(*budgets.Cost) {
			return kernelstore.WorkflowStep{}, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted,
				Message: fmt.Sprintf("retry denied: workflow budget exhausted: $%.6f/$%.6f",
					usage.CommittedCostMicroUSD().USD()+reserveCost.USD(), kernelmoney.MicroUSD(*budgets.Cost).USD())}
		}
	}
	now := s.now()
	updated, err := scanWorkflowStep(tx.QueryRow(ctx, `
		UPDATE workflow_steps SET status = $4, task_id = $5, attempt_count = $6, result_summary = $7,
			failure_code = NULLIF($8, ''),
			reserved_tasks = $10, reserved_tokens = $11, reserved_cost_micro_usd = $12,
			resource_version = resource_version + 1, updated_at = $9
		WHERE id = $1 AND resource_version = $2 AND tenant_id = $3 RETURNING `+workflowStepColumns,
		current.ID, in.ExpectedVersion, in.TenantID, in.To, taskID, attemptCount, summary, failureCode, now,
		reserveTasks, reserveTokens, int64(reserveCost)))
	if err != nil {
		return kernelstore.WorkflowStep{}, classify(err)
	}
	if deltaTasks, deltaTokens, deltaCost := reserveTasks-current.ReservedTasks,
		reserveTokens-current.ReservedTokens, int64(reserveCost)-int64(current.ReservedCostMicroUSD); deltaTasks != 0 || deltaTokens != 0 || deltaCost != 0 {
		command, err := tx.Exec(ctx, `UPDATE workflow_usage_ledgers SET
			step_reserved_tasks = GREATEST(0, step_reserved_tasks + $3),
			step_reserved_tokens = GREATEST(0, step_reserved_tokens + $4),
			step_reserved_cost_micro_usd = GREATEST(0, step_reserved_cost_micro_usd + $5),
			updated_at = $6
			WHERE tenant_id = $1 AND workflow_id = $2`,
			in.TenantID, in.WorkflowID, deltaTasks, deltaTokens, deltaCost, now)
		if err != nil {
			return kernelstore.WorkflowStep{}, classify(err)
		}
		if command.RowsAffected() != 1 {
			return kernelstore.WorkflowStep{}, fmt.Errorf("workflow usage ledger is missing for workflow %s", in.WorkflowID)
		}
	}
	if err := insertEvent(ctx, tx, in.TenantID, "WorkflowStep", current.ID, updated.ResourceVersion,
		"WorkflowStep"+title(string(in.To)), map[string]any{"workflowId": in.WorkflowID, "step": in.StepName, "status": in.To, "failureCode": failureCode}, now, s.newID()); err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "workflow.step.transition", "WorkflowStep", current.ID,
		map[string]any{"workflowId": in.WorkflowID, "step": in.StepName, "from": current.Status, "to": in.To}, now); err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.WorkflowStep{}, classify(err)
	}
	return updated, nil
}

// DecideWorkflowStepApproval records a human decision (approve/reject) on a
// waiting step; the decided identity is durable audit state.
func (s *Store) DecideWorkflowStepApproval(ctx context.Context, in kernelstore.DecideWorkflowStepApprovalInput) (kernelstore.WorkflowStep, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	defer rollback(ctx, tx)
	current, err := scanWorkflowStep(tx.QueryRow(ctx,
		`SELECT `+workflowStepColumns+` FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3 FOR UPDATE`,
		in.TenantID, in.WorkflowID, in.StepName))
	if err != nil {
		if errors.Is(err, kernelstore.ErrNotFound) {
			return kernelstore.WorkflowStep{}, kernelstore.ErrStepNotFound
		}
		return kernelstore.WorkflowStep{}, err
	}
	if current.ResourceVersion != in.ExpectedVersion {
		return kernelstore.WorkflowStep{}, versionConflict("workflow step", current.ID, in.ExpectedVersion, current.ResourceVersion)
	}
	if current.Status != kernelstore.StepWaitingApproval {
		return kernelstore.WorkflowStep{}, fmt.Errorf("%w: only WAITING_APPROVAL steps can be decided",
			kernelstore.ErrInvalidTransition)
	}
	if current.DecidedBy != "" {
		return kernelstore.WorkflowStep{}, fmt.Errorf("%w: step already decided", kernelstore.ErrInvalidTransition)
	}
	decision := "approved"
	if !in.Approved {
		decision = "rejected"
	}
	now := s.now()
	updated, err := scanWorkflowStep(tx.QueryRow(ctx, `
		UPDATE workflow_steps SET decided_by = $4, approval_decision = $5, decided_at = $6,
			resource_version = resource_version + 1, updated_at = $6
		WHERE id = $1 AND resource_version = $2 AND tenant_id = $3 RETURNING `+workflowStepColumns,
		current.ID, in.ExpectedVersion, in.TenantID, in.DecidedBy, decision, now))
	if err != nil {
		return kernelstore.WorkflowStep{}, classify(err)
	}
	if err := insertEvent(ctx, tx, in.TenantID, "WorkflowStep", current.ID, updated.ResourceVersion,
		"WorkflowStepApprovalDecided", map[string]any{"workflowId": in.WorkflowID, "step": in.StepName, "decision": decision, "decidedBy": in.DecidedBy}, now, s.newID()); err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "workflow.step.approval", "WorkflowStep", current.ID,
		map[string]any{"workflowId": in.WorkflowID, "step": in.StepName, "decision": decision, "decidedBy": in.DecidedBy}, now); err != nil {
		return kernelstore.WorkflowStep{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.WorkflowStep{}, classify(err)
	}
	return updated, nil
}

// RequestWorkflowCancellation records the durable cancel intent.
func (s *Store) RequestWorkflowCancellation(ctx context.Context, tenantID string, workflowID uuid.UUID, expectedVersion int64) (kernelstore.Workflow, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	defer rollback(ctx, tx)
	current, err := scanWorkflow(tx.QueryRow(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, workflowID))
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	if current.ResourceVersion != expectedVersion {
		return kernelstore.Workflow{}, versionConflict("workflow", workflowID, expectedVersion, current.ResourceVersion)
	}
	if current.Status.Terminal() {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow already terminal", kernelstore.ErrInvalidTransition)
	}
	if current.CancelRequestedAt != nil {
		rollback(ctx, tx)
		return current, nil
	}
	now := s.now()
	updated, err := scanWorkflow(tx.QueryRow(ctx, `
		UPDATE workflows SET cancel_requested_at = $4, resource_version = resource_version + 1, updated_at = $4
		WHERE tenant_id = $1 AND id = $2 AND resource_version = $3 RETURNING `+workflowColumns,
		tenantID, workflowID, expectedVersion, now))
	if err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	if err := insertEvent(ctx, tx, tenantID, "Workflow", workflowID, updated.ResourceVersion,
		"WorkflowCancellationRequested", map[string]any{"workflowId": workflowID}, now, s.newID()); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := auditHook(ctx, tx, tenantID, "workflow.cancel.request", "Workflow", workflowID, nil, now); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	return updated, nil
}

// ArtifactMetadata reads the digest, size and media type of one artifact.
func (s *Store) ArtifactMetadata(ctx context.Context, tenantID, uri string) ([]byte, int64, string, error) {
	var digest []byte
	var size int64
	var mediaType string
	err := s.pool.QueryRow(ctx,
		`SELECT sha256, size_bytes, media_type FROM artifacts WHERE tenant_id = $1 AND uri = $2`,
		tenantID, uri).Scan(&digest, &size, &mediaType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, "", kernelstore.ErrNotFound
		}
		return nil, 0, "", classify(err)
	}
	return digest, size, mediaType, nil
}

// canonicalJSON normalizes a JSON document for replay comparison.
func canonicalJSON(raw []byte) string {
	var document any
	if json.Unmarshal(raw, &document) != nil {
		return string(raw)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return string(raw)
	}
	return string(encoded)
}

func nullableInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

// ClaimWorkflows leases active workflows to one orchestrator instance
// Unclaimed or expired-claim workflows are taken round-robin across
// tenants so one tenant's backlog cannot crowd out the others, and a dead
// instance's claims expire for its peers to take over. The claim columns are
// internal and never touch the client-observed resource_version.
func (s *Store) ClaimWorkflows(ctx context.Context, in kernelstore.ClaimWorkflowsInput) ([]kernelstore.Workflow, error) {
	if in.Owner == "" || in.Batch <= 0 || in.Lease <= 0 {
		return nil, fmt.Errorf("owner, positive batch and lease are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	// Rank eligible work within each tenant, then take rank 1 from every
	// tenant before rank 2, and so on. This fills the batch under a small
	// tenant count without allowing one tenant's backlog to crowd out peers.
	// Budgeted workflows stay eligible: the controller must observe and
	// durably drain an exhausted run.
	now := s.now()
	rows, err := tx.Query(ctx, `
		SELECT `+workflowColumns+` FROM (
			SELECT workflows.*,
				ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY
					CASE WHEN orchestrator_claim IS NULL OR orchestrator_claim_until <= $2 THEN 0 ELSE 1 END,
					orchestrator_claim_until ASC NULLS FIRST, created_at ASC, id ASC
				) AS tenant_rank
			FROM workflows
			WHERE status IN ('PENDING','RUNNING')
				AND (orchestrator_claim IS NULL OR orchestrator_claim = $1 OR orchestrator_claim_until <= $2)
		) fair
		ORDER BY tenant_rank ASC, orchestrator_claim_until ASC NULLS FIRST, created_at ASC, tenant_id ASC
		LIMIT $3`, in.Owner, now, in.Batch)
	if err != nil {
		return nil, classify(err)
	}
	candidates := []kernelstore.Workflow{}
	for rows.Next() {
		workflow, err := scanWorkflowRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, workflow)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	claimed := make([]kernelstore.Workflow, 0, len(candidates))
	for _, workflow := range candidates {
		updated, err := scanWorkflow(tx.QueryRow(ctx, `
			UPDATE workflows SET orchestrator_claim = $4, orchestrator_claim_until = $5
			WHERE tenant_id = $1 AND id = $2 AND resource_version = $3
				AND (orchestrator_claim IS NULL OR orchestrator_claim = $4 OR orchestrator_claim_until <= $6)
			RETURNING `+workflowColumns,
			workflow.TenantID, workflow.ID, workflow.ResourceVersion, in.Owner, now.Add(in.Lease), now))
		if err != nil {
			if errors.Is(err, kernelstore.ErrNotFound) {
				continue // lost the claim race; a peer took it
			}
			return nil, classify(err)
		}
		claimed = append(claimed, updated)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, classify(err)
	}
	return claimed, nil
}

// RenewWorkflowClaim extends one live orchestrator lease. A stale owner or an
// already-expired claim is fenced instead of being resurrected.
func (s *Store) RenewWorkflowClaim(ctx context.Context, tenantID string, workflowID uuid.UUID, owner string, lease time.Duration) error {
	if strings.TrimSpace(tenantID) == "" || workflowID == uuid.Nil || strings.TrimSpace(owner) == "" || lease <= 0 {
		return fmt.Errorf("tenant, workflow, owner and positive lease are required")
	}
	now := s.now()
	command, err := s.pool.Exec(ctx, `UPDATE workflows SET orchestrator_claim_until = $1
		WHERE tenant_id = $2 AND id = $3 AND orchestrator_claim = $4 AND orchestrator_claim_until > $5`,
		now.Add(lease), tenantID, workflowID, owner, now)
	if err != nil {
		return classify(err)
	}
	if command.RowsAffected() != 1 {
		return kernelstore.ErrFenced
	}
	return nil
}

// workflowTaskUsage aggregates the tasks carrying one workflow's idempotency
// prefix: their count, settled usage, any unsettled overage, and the
// outstanding step reservations that represent the workflow's future
// commitment.
func (s *Store) workflowTaskUsage(ctx context.Context, tx pgx.Tx, tenantID string, workflowID uuid.UUID) (kernelstore.WorkflowUsage, error) {
	var usage kernelstore.WorkflowUsage
	err := tx.QueryRow(ctx, workflowUsageQuery,
		tenantID, workflowID).Scan(
		&usage.Tasks, &usage.Tokens, &usage.CostMicroUSD, &usage.ReservedTokens, &usage.ReservedCostMicroUSD,
		&usage.StepReservedTasks, &usage.StepReservedTokens, &usage.StepReservedCostMicroUSD, &usage.PendingOverage)
	if err != nil {
		return usage, classify(err)
	}
	return usage, nil
}

// WorkflowUsageSnapshot aggregates a workflow's task count and settled
// usage. Callers without a transaction use this entry point.
func (s *Store) WorkflowUsageSnapshot(ctx context.Context, tenantID string, workflowID uuid.UUID) (kernelstore.WorkflowUsage, error) {
	var usage kernelstore.WorkflowUsage
	err := s.pool.QueryRow(ctx, workflowUsageQuery,
		tenantID, workflowID).Scan(
		&usage.Tasks, &usage.Tokens, &usage.CostMicroUSD, &usage.ReservedTokens, &usage.ReservedCostMicroUSD,
		&usage.StepReservedTasks, &usage.StepReservedTokens, &usage.StepReservedCostMicroUSD, &usage.PendingOverage)
	if err != nil {
		return usage, classify(err)
	}
	var budgets struct {
		Tasks  *int64
		Tokens *int64
		Cost   *int64
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT budget_max_tasks, budget_max_tokens, budget_max_cost_micro_usd FROM workflows WHERE tenant_id = $1 AND id = $2`,
		tenantID, workflowID).Scan(&budgets.Tasks, &budgets.Tokens, &budgets.Cost); err != nil {
		return usage, classify(err)
	}
	if budgets.Tasks != nil {
		usage.BudgetMaxTasks = *budgets.Tasks
	}
	if budgets.Tokens != nil {
		usage.BudgetMaxTokens = *budgets.Tokens
	}
	if budgets.Cost != nil {
		usage.BudgetMaxCostMicroUSD = kernelmoney.MicroUSD(*budgets.Cost)
	}
	return usage, nil
}

const workflowUsageQuery = `
	SELECT task_count,settled_tokens,settled_cost_micro_usd,reserved_tokens,reserved_cost_micro_usd,
		step_reserved_tasks,step_reserved_tokens,step_reserved_cost_micro_usd,pending_tasks>0
	FROM workflow_usage_ledgers WHERE tenant_id=$1 AND workflow_id=$2`

// SpawnWorkflowStep creates one dynamic step with every recursion, fan-out
// and budget guard evaluated inside the same transaction as the insert
// so two racing spawn calls can never both slip past a cap. A
// spawn_key replay returns the existing step unchanged.
func (s *Store) SpawnWorkflowStep(ctx context.Context, in kernelstore.SpawnWorkflowStepInput) (kernelstore.SpawnWorkflowStepResult, error) {
	result := kernelstore.SpawnWorkflowStepResult{}
	if in.WorkflowID == uuid.Nil || strings.TrimSpace(in.TenantID) == "" || in.WorkflowVersion <= 0 ||
		strings.TrimSpace(in.ParentStepName) == "" || strings.TrimSpace(in.Goal) == "" ||
		strings.TrimSpace(in.AgentVersionRef) == "" || strings.TrimSpace(in.IdempotencyKey) == "" {
		return result, fmt.Errorf("workflow, tenant, version, parent, goal, agent version and idempotency key are required")
	}
	if err := kernelstore.ValidateStepName(in.Name); err != nil {
		return result, err
	}
	if len(in.Goal) > 8192 || len(in.IdempotencyKey) > 512 || in.MaxAttempts < 0 || in.MaxAttempts > 10 {
		return result, fmt.Errorf("spawn input exceeds its bound")
	}
	var specObject map[string]json.RawMessage
	if len(in.Spec) == 0 || json.Unmarshal(in.Spec, &specObject) != nil || specObject == nil {
		return result, fmt.Errorf("dynamic step spec must be a JSON object")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	workflow, err := scanWorkflow(tx.QueryRow(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`,
		in.TenantID, in.WorkflowID))
	if err != nil {
		if errors.Is(err, kernelstore.ErrNotFound) {
			return result, kernelstore.ErrWorkflowNotFound
		}
		return result, err
	}
	result.Workflow = workflow
	if !in.Guards.Enabled {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnDisabled, Message: "dynamic spawning is not enabled for this workflow"}
	}
	if workflow.ResourceVersion != in.WorkflowVersion {
		return result, versionConflict("workflow", in.WorkflowID, in.WorkflowVersion, workflow.ResourceVersion)
	}
	if workflow.Status.Terminal() {
		return result, fmt.Errorf("%w: workflow is terminal", kernelstore.ErrInvalidTransition)
	}
	if workflow.CancelRequestedAt != nil {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnCancelled, Message: "workflow cancellation is in progress"}
	}
	if workflow.BudgetExhaustedAt != nil {
		result.WorkflowExhausted = true
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted, Message: "workflow budget is exhausted"}
	}
	if workflow.DeadlineExceededAt != nil || (workflow.DeadlineAt != nil && !s.now().Before(*workflow.DeadlineAt)) {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnDeadlineExceeded, Message: "workflow deadline has been exceeded"}
	}

	spawnKey := in.IdempotencyKey + "-" + kernelstore.SpawnKeyHash(in.Arguments)
	// Replay: the unique partial index makes a racing duplicate impossible.
	if existing, err := scanWorkflowStep(tx.QueryRow(ctx,
		`SELECT `+workflowStepColumns+` FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 AND spawn_key = $3`,
		in.TenantID, in.WorkflowID, spawnKey)); err == nil {
		result.Step = existing
		result.Usage = usageWithBudgets(workflow)
		return result, tx.Commit(ctx)
	} else if !errors.Is(err, kernelstore.ErrNotFound) {
		return result, err
	}
	if _, err := scanWorkflowStep(tx.QueryRow(ctx,
		`SELECT `+workflowStepColumns+` FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3`,
		in.TenantID, in.WorkflowID, in.Name)); err == nil {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnNameConflict, Message: "workflow step name already exists"}
	} else if !errors.Is(err, kernelstore.ErrNotFound) {
		return result, err
	}

	// A stale-low reservation total (P1-05) would let the budget checks below
	// under-count the workflow's real commitment, so new dynamic spawns pause
	// until the controller reconciles the pre-upgrade backlog. Idempotent
	// replays returned above; only genuinely new steps reach this guard.
	if workflow.NeedsBudgetReconciliation {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnReconciliationPending,
			Message: "workflow budget reconciliation is pending; retry once step reservations are reconciled"}
	}

	// Recursion guard: the spawning parent's depth bounds its children.
	var parent kernelstore.WorkflowStep
	var children int64
	if in.ParentStepName != "" {
		parent, err = scanWorkflowStep(tx.QueryRow(ctx,
			`SELECT `+workflowStepColumns+` FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3 FOR UPDATE`,
			in.TenantID, in.WorkflowID, in.ParentStepName))
		if err != nil {
			if errors.Is(err, kernelstore.ErrNotFound) {
				return result, kernelstore.ErrStepNotFound
			}
			return result, err
		}
		if err := tx.QueryRow(ctx,
			`SELECT child_count FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3`,
			in.TenantID, in.WorkflowID, in.ParentStepName).Scan(&children); err != nil {
			return result, classify(err)
		}
	}
	depth := parent.SpawnDepth + 1
	if in.Guards.MaxSpawnDepth > 0 && depth > in.Guards.MaxSpawnDepth {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnDepthExceeded,
			Message: fmt.Sprintf("spawn depth %d exceeds maxSpawnDepth %d", depth, in.Guards.MaxSpawnDepth)}
	}

	// Workflow-level guards.
	var stepCount, dynamicCount int64
	if err := tx.QueryRow(ctx,
		`SELECT step_count, dynamic_step_count FROM workflows WHERE tenant_id = $1 AND id = $2`,
		in.TenantID, in.WorkflowID).Scan(&stepCount, &dynamicCount); err != nil {
		return result, classify(err)
	}
	if in.Guards.MaxWorkflowSteps > 0 && stepCount >= in.Guards.MaxWorkflowSteps {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnTotalStepsExceeded,
			Message: fmt.Sprintf("workflow already has %d steps (maxWorkflowSteps %d)", stepCount, in.Guards.MaxWorkflowSteps)}
	}
	if in.Guards.MaxDynamicSteps > 0 && dynamicCount >= in.Guards.MaxDynamicSteps {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnDynamicExceeded,
			Message: fmt.Sprintf("workflow already has %d dynamic steps (maxDynamicSteps %d)", dynamicCount, in.Guards.MaxDynamicSteps)}
	}

	// Budget guards: tasks/tokens/cost ceilings across the whole run,
	// measured against the workflow's total commitment — created tasks,
	// their settled and reserved usage, and the reservations still held by
	// undispatched steps. Because the reservation for this spawn is added
	// in the same transaction, concurrent spawns serialize on the workflow
	// row and can never collectively promise past a ceiling.
	usage, err := s.workflowTaskUsage(ctx, tx, in.TenantID, in.WorkflowID)
	if err != nil {
		return result, err
	}
	usage.BudgetMaxTasks, usage.BudgetMaxTokens, usage.BudgetMaxCostMicroUSD =
		workflow.BudgetMaxTasks, workflow.BudgetMaxTokens, workflow.BudgetMaxCostMicroUSD
	result.Usage = usage
	reserveTokens, reserveCost := kernelstore.TaskSpecBudgetReservation(in.Spec)
	if workflow.BudgetMaxTasks > 0 && usage.CommittedTasks() >= workflow.BudgetMaxTasks {
		result.WorkflowExhausted = true
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted,
			Message: fmt.Sprintf("workflow budget exhausted: %d/%d tasks", usage.CommittedTasks(), workflow.BudgetMaxTasks)}
	}
	if workflow.BudgetMaxTokens > 0 && usage.CommittedTokens()+reserveTokens > workflow.BudgetMaxTokens {
		result.WorkflowExhausted = true
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted,
			Message: fmt.Sprintf("workflow budget exhausted: %d/%d tokens", usage.CommittedTokens()+reserveTokens, workflow.BudgetMaxTokens)}
	}
	if workflow.BudgetMaxCostMicroUSD > 0 && usage.CommittedCostMicroUSD()+reserveCost > workflow.BudgetMaxCostMicroUSD {
		result.WorkflowExhausted = true
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted,
			Message: fmt.Sprintf("workflow budget exhausted: $%.6f/$%.6f",
				usage.CommittedCostMicroUSD().USD()+reserveCost.USD(), workflow.BudgetMaxCostMicroUSD.USD())}
	}
	if in.Guards.MaxSpawnTasks > 0 && usage.CommittedTasks() >= in.Guards.MaxSpawnTasks {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnTaskLimitExceeded,
			Message: fmt.Sprintf("spawned task limit reached: %d/%d", usage.CommittedTasks(), in.Guards.MaxSpawnTasks)}
	}
	if in.Guards.MaxSpawnTokens > 0 && usage.CommittedTokens()+reserveTokens > in.Guards.MaxSpawnTokens {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnTokenLimitExceeded,
			Message: fmt.Sprintf("spawned token limit reached: %d/%d", usage.CommittedTokens()+reserveTokens, in.Guards.MaxSpawnTokens)}
	}
	if in.Guards.MaxSpawnCostMicroUSD > 0 && usage.CommittedCostMicroUSD()+reserveCost > in.Guards.MaxSpawnCostMicroUSD {
		return result, kernelstore.SpawnDenial{Code: errorcode.SpawnCostLimitExceeded,
			Message: fmt.Sprintf("spawned cost limit reached: $%.6f/$%.6f",
				usage.CommittedCostMicroUSD().USD()+reserveCost.USD(), in.Guards.MaxSpawnCostMicroUSD.USD())}
	}

	// Per-parent fan-out cap.
	if in.ParentStepName != "" {
		if in.Guards.MaxChildrenPerStep > 0 && children >= in.Guards.MaxChildrenPerStep {
			return result, kernelstore.SpawnDenial{Code: errorcode.SpawnFanoutExceeded,
				Message: fmt.Sprintf("parent %q already spawned %d children (maxChildrenPerStep %d)",
					in.ParentStepName, children, in.Guards.MaxChildrenPerStep)}
		}
	}

	ordinal := int(stepCount)
	created, err := scanWorkflowStep(tx.QueryRow(ctx, `
		INSERT INTO workflow_steps (id, tenant_id, workflow_id, name, ordinal, status, attempt_count,
			parent_step_name, spawn_depth, is_dynamic, spawn_key, goal, agent_version_ref, spec, max_attempts,
			reserved_tasks, reserved_tokens, reserved_cost_micro_usd,
			resource_version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 'PENDING', 0, $6, $7, true, $8, $9, $10, $11, $12, 1, $13, $14, 1, $15, $15)
		RETURNING `+workflowStepColumns,
		uuid.New(), in.TenantID, in.WorkflowID, in.Name, ordinal,
		nullableString(in.ParentStepName), depth, spawnKey, in.Goal, in.AgentVersionRef, in.Spec,
		nullableInt(int(in.MaxAttempts)), reserveTokens, int64(reserveCost), s.now()))
	if err != nil {
		return result, classify(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflows SET step_count = step_count + 1, dynamic_step_count = dynamic_step_count + 1
		WHERE tenant_id = $1 AND id = $2`, in.TenantID, in.WorkflowID); err != nil {
		return result, classify(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflow_steps SET child_count = child_count + 1
		WHERE tenant_id = $1 AND workflow_id = $2 AND name = $3`, in.TenantID, in.WorkflowID, in.ParentStepName); err != nil {
		return result, classify(err)
	}
	result.Step = created
	result.Created = true
	if err := insertEvent(ctx, tx, in.TenantID, "WorkflowStep", created.ID, 1, "WorkflowStepSpawned", map[string]any{
		"workflowId": in.WorkflowID, "step": created.Name, "parentStep": in.ParentStepName,
		"spawnDepth": depth, "spawnKey": spawnKey,
	}, s.now(), s.newID()); err != nil {
		return result, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "workflow.step.spawn", "WorkflowStep", created.ID, map[string]any{
		"workflowId": in.WorkflowID, "step": created.Name, "parentStep": in.ParentStepName,
		"spawnDepth": depth, "spawnKey": spawnKey,
	}, s.now()); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return result, nil
}

// MarkWorkflowBudgetExhausted records the durable budget-stop intent; the
// orchestrator observes it like a cancellation and drains the workflow.
func (s *Store) MarkWorkflowBudgetExhausted(ctx context.Context, tenantID string, workflowID uuid.UUID, expectedVersion int64) (kernelstore.Workflow, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	defer rollback(ctx, tx)
	current, err := scanWorkflow(tx.QueryRow(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, workflowID))
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	if current.ResourceVersion != expectedVersion {
		return kernelstore.Workflow{}, versionConflict("workflow", workflowID, expectedVersion, current.ResourceVersion)
	}
	if current.Status.Terminal() {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow already terminal", kernelstore.ErrInvalidTransition)
	}
	if current.BudgetExhaustedAt != nil {
		rollback(ctx, tx)
		return current, nil
	}
	updated, err := scanWorkflow(tx.QueryRow(ctx, `
		UPDATE workflows SET budget_exhausted_at = $4, resource_version = resource_version + 1, updated_at = $4
		WHERE tenant_id = $1 AND id = $2 AND resource_version = $3 RETURNING `+workflowColumns,
		tenantID, workflowID, expectedVersion, s.now()))
	if err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	if err := insertEvent(ctx, tx, tenantID, "Workflow", workflowID, updated.ResourceVersion, "WorkflowBudgetExhausted", map[string]any{
		"workflowId": workflowID,
	}, s.now(), s.newID()); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := auditHook(ctx, tx, tenantID, "workflow.budget.exhausted", "Workflow", workflowID, nil, s.now()); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	return updated, nil
}

// MarkWorkflowDeadlineExceeded records the durable deadline-stop intent; the
// orchestrator then cancels active work and fails the workflow deterministically.
func (s *Store) MarkWorkflowDeadlineExceeded(ctx context.Context, tenantID string, workflowID uuid.UUID, expectedVersion int64) (kernelstore.Workflow, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	defer rollback(ctx, tx)
	current, err := scanWorkflow(tx.QueryRow(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, workflowID))
	if err != nil {
		return kernelstore.Workflow{}, err
	}
	if current.ResourceVersion != expectedVersion {
		return kernelstore.Workflow{}, versionConflict("workflow", workflowID, expectedVersion, current.ResourceVersion)
	}
	if current.Status.Terminal() {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow already terminal", kernelstore.ErrInvalidTransition)
	}
	if current.DeadlineExceededAt != nil {
		rollback(ctx, tx)
		return current, nil
	}
	updated, err := scanWorkflow(tx.QueryRow(ctx, `
		UPDATE workflows SET deadline_exceeded_at = $4, resource_version = resource_version + 1, updated_at = $4
		WHERE tenant_id = $1 AND id = $2 AND resource_version = $3 RETURNING `+workflowColumns,
		tenantID, workflowID, expectedVersion, s.now()))
	if err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	if err := insertEvent(ctx, tx, tenantID, "Workflow", workflowID, updated.ResourceVersion, "WorkflowDeadlineExceeded", map[string]any{
		"workflowId": workflowID, "deadline": current.DeadlineAt,
	}, s.now(), s.newID()); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := auditHook(ctx, tx, tenantID, "workflow.deadline.exceeded", "Workflow", workflowID, map[string]any{
		"deadline": current.DeadlineAt,
	}, s.now()); err != nil {
		return kernelstore.Workflow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return kernelstore.Workflow{}, classify(err)
	}
	return updated, nil
}

func nullableInt(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func usageWithBudgets(workflow kernelstore.Workflow) kernelstore.WorkflowUsage {
	return kernelstore.WorkflowUsage{
		BudgetMaxTasks: workflow.BudgetMaxTasks, BudgetMaxTokens: workflow.BudgetMaxTokens,
		BudgetMaxCostMicroUSD: workflow.BudgetMaxCostMicroUSD,
	}
}

// WorkflowLineage resolves workflow ownership exclusively from explicit task
// lineage columns. Idempotency keys remain opaque and are never parsed.
func (s *Store) WorkflowLineage(ctx context.Context, tenantID string, taskID uuid.UUID) (uuid.UUID, string, int64, bool, error) {
	var workflowID *uuid.UUID
	var stepName string
	var version *int64
	err := s.pool.QueryRow(ctx, `
		SELECT t.workflow_id, COALESCE(t.workflow_step_name, ''), w.resource_version
		FROM tasks t
		LEFT JOIN workflows w ON w.tenant_id = t.tenant_id AND w.id = t.workflow_id
		WHERE t.tenant_id = $1 AND t.id = $2`, tenantID, taskID).Scan(&workflowID, &stepName, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, "", 0, false, kernelstore.ErrNotFound
		}
		return uuid.Nil, "", 0, false, classify(err)
	}
	if workflowID == nil || version == nil || stepName == "" {
		return uuid.Nil, "", 0, false, nil
	}
	return *workflowID, stepName, *version, true, nil
}
