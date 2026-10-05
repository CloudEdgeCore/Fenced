package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ kernelstore.BudgetStore = (*Store)(nil)

func (s *Store) GetTaskBudget(ctx context.Context, tenantID string, taskID uuid.UUID) (kernelstore.TaskBudgetStatus, error) {
	if strings.TrimSpace(tenantID) == "" || taskID == uuid.Nil {
		return kernelstore.TaskBudgetStatus{}, kernelstore.ErrBudgetNotReserved
	}
	// The ledger row carries rolling consumed counters (migration 000009),
	// so a budget read is a single row scan instead of a full re-SUM of the
	// append-only settlement ledger.
	status, err := scanBudgetStatus(s.pool.QueryRow(ctx, `SELECT `+budgetStatusColumns+`
		FROM task_budget_ledgers WHERE tenant_id = $1 AND task_id = $2`, tenantID, taskID.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.TaskBudgetStatus{}, kernelstore.ErrBudgetNotReserved
	}
	if err != nil {
		return kernelstore.TaskBudgetStatus{}, classify(err)
	}
	return status, nil
}

func (s *Store) SettleTaskUsage(ctx context.Context, in kernelstore.SettleTaskUsageInput) (kernelstore.TaskBudgetStatus, error) {
	var zero kernelstore.TaskBudgetStatus
	if !in.Valid() {
		return zero, fmt.Errorf("valid tenant, task, idempotency key, and positive usage are required")
	}
	// Budget settlement is a cross-row invariant (ADR-002): the idempotency
	// replay, exhaustion check and append share one transaction under the
	// ledger row lock.
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)
	status, consumed, err := s.settleUsageInTx(ctx, tx, in.TenantID, in.TaskID, in.IdempotencyKey, in.ReservationKey, in.Usage)
	if err == nil || errors.Is(err, kernelstore.ErrBudgetExceeded) {
		// The hard-stop marker (exhausted = true) must be durable even when
		// the settlement itself is rejected.
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return zero, classify(commitErr)
		}
		if errors.Is(err, kernelstore.ErrBudgetExceeded) {
			agentmetrics.BudgetEvent(ctx, "exhausted", "aggregate")
		} else {
			agentmetrics.BudgetEvent(ctx, "settled", "aggregate")
		}
		status.Consumed = consumed
		return status, err
	}
	return zero, err
}

// settleUsageInTx records one idempotent usage settlement inside the caller's
// transaction: it locks the ledger row, deduplicates by idempotency key,
// enforces the hard stop, appends the settlement, and bumps the rolling
// consumed counters on the ledger row (migration 000009). On
// ErrBudgetExceeded the ledger is marked exhausted and nothing is recorded;
// the caller owns the commit so the marker is durable. ErrBudgetNotReserved
// reports a task without a reservation. The returned consumption is the
// ledger row's counter state (before any new usage on the error paths).
func (s *Store) settleUsageInTx(ctx context.Context, tx pgx.Tx, tenantID string, taskID uuid.UUID, idempotencyKey, reservationKey string, usage kernelstore.TaskBudget) (kernelstore.TaskBudgetStatus, kernelstore.TaskBudget, error) {
	status, err := scanBudgetStatus(tx.QueryRow(ctx, `SELECT `+budgetStatusColumns+`
		FROM task_budget_ledgers WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE`,
		tenantID, taskID.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return status, kernelstore.TaskBudget{}, kernelstore.ErrBudgetNotReserved
	}
	if err != nil {
		return status, kernelstore.TaskBudget{}, classify(err)
	}
	consumed := status.Consumed

	var existing string
	err = tx.QueryRow(ctx, `SELECT id::text FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2 AND idempotency_key = $3`,
		tenantID, taskID.String(), idempotencyKey).Scan(&existing)
	if err == nil {
		// Idempotent replay: the settlement is already recorded; the caller
		// sees the current cumulative consumption.
		return status, consumed, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return status, consumed, classify(err)
	}

	// Hard stop: once a settlement was rejected as over-budget, no further
	// consumption may be recorded even if the numbers would now fit.
	if status.Exhausted {
		return status, consumed, fmt.Errorf("%w: task=%s ledger is exhausted", kernelstore.ErrBudgetExceeded, taskID)
	}
	active, err := s.activeUsageReservations(ctx, tx, tenantID, taskID, reservationKey)
	if err != nil {
		return status, consumed, err
	}
	if exceeded(status.Reserved, consumed, addBudgets(active, usage)) {
		updated, updateErr := scanBudgetStatus(tx.QueryRow(ctx, `UPDATE task_budget_ledgers
			SET exhausted = true, updated_at = $1, resource_version = resource_version + 1
			WHERE tenant_id = $2 AND task_id = $3 RETURNING `+budgetStatusColumns,
			s.now(), tenantID, taskID.String()))
		if updateErr != nil {
			return status, consumed, classify(updateErr)
		}
		return updated, consumed, fmt.Errorf("%w: task=%s reserved=%+v consumed=%+v additional=%+v",
			kernelstore.ErrBudgetExceeded, taskID, status.Reserved, consumed, usage)
	}

	now := s.now()
	command, err := tx.Exec(ctx, `INSERT INTO task_budget_settlements (
		id, tenant_id, task_id, idempotency_key, tokens, cost_micro_usd, tool_calls, wall_seconds, occurred_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (tenant_id, task_id, idempotency_key) DO NOTHING`,
		s.newID().String(), tenantID, taskID.String(), idempotencyKey,
		usage.Tokens, usage.CostMicroUSD, usage.ToolCalls, usage.WallSeconds, now)
	if err != nil {
		return status, consumed, classify(err)
	}
	// Tenant aggregate consumption (v0.6): a newly appended settlement bumps
	// the tenant's current window in the same transaction, so the window
	// counters are exact with the settlement ledger. Replays (rows affected
	// 0) never double-count, and tenants without a configured quota track no
	// windows.
	if command.RowsAffected() == 1 {
		if err := s.bumpTenantWindow(ctx, tx, tenantID, usage, now); err != nil {
			return status, consumed, err
		}
	}
	// Keep the rolling counters exact with the append, under the same row
	// lock that serializes every settlement for this task.
	updated, err := scanBudgetStatus(tx.QueryRow(ctx, `UPDATE task_budget_ledgers
		SET consumed_tokens = consumed_tokens + $1, consumed_cost_micro_usd = consumed_cost_micro_usd + $2,
			consumed_tool_calls = consumed_tool_calls + $3, consumed_wall_seconds = consumed_wall_seconds + $4,
			updated_at = $5, resource_version = resource_version + 1
		WHERE tenant_id = $6 AND task_id = $7 RETURNING `+budgetStatusColumns,
		usage.Tokens, usage.CostMicroUSD, usage.ToolCalls, usage.WallSeconds, now, tenantID, taskID.String()))
	if err != nil {
		return status, consumed, classify(err)
	}
	return updated, updated.Consumed, nil
}

// SettleTaskUsageDelta settles the remainder between a usage family (all
// settlements whose key is FamilyPrefix + ":<suffix>") and a cumulative
// Target, under the ledger row lock so concurrent settlements cannot race the
// delta computation. A family that already settled at least the target
// settles nothing; a task without a reservation is not gated.
func (s *Store) SettleTaskUsageDelta(ctx context.Context, in kernelstore.SettleTaskUsageDeltaInput) (kernelstore.TaskBudgetStatus, error) {
	var zero kernelstore.TaskBudgetStatus
	if !in.Valid() {
		return zero, fmt.Errorf("valid tenant, task, family prefix, idempotency key, and target are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)

	// Take the ledger row lock before reading the family sum: every
	// settlement for this task serializes on the same row, so the delta is
	// computed against a stable snapshot.
	status, err := scanBudgetStatus(tx.QueryRow(ctx, `SELECT `+budgetStatusColumns+`
		FROM task_budget_ledgers WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE`,
		in.TenantID, in.TaskID.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, kernelstore.ErrBudgetNotReserved
	}
	if err != nil {
		return zero, classify(err)
	}

	familySettled, err := s.sumFamilySettlements(ctx, tx, in.TenantID, in.TaskID, in.FamilyPrefix)
	if err != nil {
		return zero, err
	}
	delta := taskBudgetRemainder(in.Target, familySettled)
	if delta.Zero() {
		// The family already reached (or overshot) the target: nothing to
		// settle, including a retried Finish after a crash between the delta
		// settlement and the call row update.
		if err := tx.Commit(ctx); err != nil {
			return zero, classify(err)
		}
		return status, nil
	}
	updated, consumed, err := s.settleUsageInTx(ctx, tx, in.TenantID, in.TaskID, in.IdempotencyKey, in.ReservationKey, delta)
	if err == nil || errors.Is(err, kernelstore.ErrBudgetExceeded) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return zero, classify(commitErr)
		}
		updated.Consumed = consumed
		return updated, err
	}
	return zero, err
}

// sumFamilySettlements sums the usage a family already settled under the
// caller's transaction. The prefix is validated to contain no wildcards, so
// the pattern matches exactly FamilyPrefix + ":<suffix>".
func (s *Store) sumFamilySettlements(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenantID string, taskID uuid.UUID, familyPrefix string) (kernelstore.TaskBudget, error) {
	var settled kernelstore.TaskBudget
	err := q.QueryRow(ctx, `SELECT COALESCE(SUM(tokens), 0), COALESCE(SUM(cost_micro_usd), 0),
		COALESCE(SUM(tool_calls), 0), COALESCE(SUM(wall_seconds), 0)
		FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2 AND idempotency_key LIKE $3`,
		tenantID, taskID.String(), familyPrefix+":%").Scan(
		&settled.Tokens, &settled.CostMicroUSD, &settled.ToolCalls, &settled.WallSeconds)
	if err != nil {
		return settled, classify(err)
	}
	return settled, nil
}

// taskBudgetRemainder returns the usage still owed to reach target, per
// dimension, never negative: a family that overshot settles nothing.
func taskBudgetRemainder(target, settled kernelstore.TaskBudget) kernelstore.TaskBudget {
	cost := target.CostMicroUSD - settled.CostMicroUSD
	if cost < 0 {
		cost = 0
	}
	return kernelstore.TaskBudget{
		Tokens:       max64(0, target.Tokens-settled.Tokens),
		CostMicroUSD: cost,
		ToolCalls:    max64(0, target.ToolCalls-settled.ToolCalls),
		WallSeconds:  max64(0, target.WallSeconds-settled.WallSeconds),
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func addBudgets(a, b kernelstore.TaskBudget) kernelstore.TaskBudget {
	return kernelstore.TaskBudget{
		Tokens: a.Tokens + b.Tokens, CostMicroUSD: a.CostMicroUSD + b.CostMicroUSD,
		ToolCalls: a.ToolCalls + b.ToolCalls, WallSeconds: a.WallSeconds + b.WallSeconds,
	}
}

func (s *Store) activeUsageReservations(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenantID string, taskID uuid.UUID, excludeKey string) (kernelstore.TaskBudget, error) {
	var active kernelstore.TaskBudget
	err := q.QueryRow(ctx, `SELECT COALESCE(SUM(tokens), 0), COALESCE(SUM(cost_micro_usd), 0),
		COALESCE(SUM(tool_calls), 0), COALESCE(SUM(wall_seconds), 0)
		FROM task_usage_reservations
		WHERE tenant_id = $1 AND task_id = $2 AND status = 'ACTIVE' AND expires_at > $3
		  AND reservation_key <> $4`, tenantID, taskID, s.now(), excludeKey).Scan(
		&active.Tokens, &active.CostMicroUSD, &active.ToolCalls, &active.WallSeconds)
	if err != nil {
		return active, classify(err)
	}
	return active, nil
}

// ReserveTaskUsage atomically checks consumed + all live reservations before
// opening a provider-side operation. The task ledger row is the serialization
// point shared with settlement.
func (s *Store) ReserveTaskUsage(ctx context.Context, in kernelstore.ReserveTaskUsageInput) error {
	if !in.Valid() {
		return fmt.Errorf("valid tenant, task, reservation key, amount and expiry are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	status, err := scanBudgetStatus(tx.QueryRow(ctx, `SELECT `+budgetStatusColumns+`
		FROM task_budget_ledgers WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE`, in.TenantID, in.TaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.ErrBudgetNotReserved
	}
	if err != nil {
		return classify(err)
	}
	if status.Exhausted {
		return kernelstore.ErrBudgetExceeded
	}
	if _, err := tx.Exec(ctx, `UPDATE task_usage_reservations SET status = 'EXPIRED', released_at = $1
		WHERE tenant_id = $2 AND task_id = $3 AND status = 'ACTIVE' AND expires_at <= $1`,
		s.now(), in.TenantID, in.TaskID); err != nil {
		return classify(err)
	}
	var existing kernelstore.TaskBudget
	var existingExpiry time.Time
	err = tx.QueryRow(ctx, `SELECT tokens, cost_micro_usd, tool_calls, wall_seconds, expires_at
		FROM task_usage_reservations WHERE tenant_id = $1 AND task_id = $2 AND reservation_key = $3`,
		in.TenantID, in.TaskID, in.ReservationKey).Scan(&existing.Tokens, &existing.CostMicroUSD, &existing.ToolCalls, &existing.WallSeconds, &existingExpiry)
	if err == nil {
		if existing != in.Amount {
			return kernelstore.ErrUsageReservationConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return classify(err)
	}
	active, err := s.activeUsageReservations(ctx, tx, in.TenantID, in.TaskID, "")
	if err != nil {
		return err
	}
	if exceeded(status.Reserved, status.Consumed, addBudgets(active, in.Amount)) {
		return kernelstore.ErrBudgetExceeded
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_usage_reservations
		(id, tenant_id, task_id, reservation_key, tokens, cost_micro_usd, tool_calls, wall_seconds, expires_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, s.newID(), in.TenantID, in.TaskID,
		in.ReservationKey, in.Amount.Tokens, in.Amount.CostMicroUSD, in.Amount.ToolCalls, in.Amount.WallSeconds,
		in.ExpiresAt.UTC(), s.now()); err != nil {
		return classify(err)
	}
	if err := classify(tx.Commit(ctx)); err != nil {
		return err
	}
	agentmetrics.BudgetEvent(ctx, "reserved", "aggregate")
	return nil
}

func (s *Store) ReleaseTaskUsageReservation(ctx context.Context, tenantID string, taskID uuid.UUID, key string) error {
	if strings.TrimSpace(tenantID) == "" || taskID == uuid.Nil || strings.TrimSpace(key) == "" {
		return fmt.Errorf("tenant, task and reservation key are required")
	}
	_, err := s.pool.Exec(ctx, `UPDATE task_usage_reservations SET status = 'RELEASED', released_at = $1
		WHERE tenant_id = $2 AND task_id = $3 AND reservation_key = $4 AND status = 'ACTIVE'`,
		s.now(), tenantID, taskID, key)
	if err := classify(err); err != nil {
		return err
	}
	agentmetrics.BudgetEvent(ctx, "released", "aggregate")
	return nil
}

// exceeded reports whether additional usage would push the task past its
// reserved ceiling on any dimension.
func exceeded(reserved, consumed, additional kernelstore.TaskBudget) bool {
	return (reserved.Tokens > 0 && consumed.Tokens+additional.Tokens > reserved.Tokens) ||
		(reserved.CostMicroUSD > 0 && consumed.CostMicroUSD+additional.CostMicroUSD > reserved.CostMicroUSD) ||
		(reserved.ToolCalls > 0 && consumed.ToolCalls+additional.ToolCalls > reserved.ToolCalls) ||
		(reserved.WallSeconds > 0 && consumed.WallSeconds+additional.WallSeconds > reserved.WallSeconds)
}

const budgetStatusColumns = `
	tenant_id, task_id::text, reserved_tokens, reserved_cost_micro_usd,
	reserved_tool_calls, reserved_wall_seconds, exhausted, resource_version, updated_at,
	consumed_tokens, consumed_cost_micro_usd, consumed_tool_calls, consumed_wall_seconds`

func scanBudgetStatus(row scanner) (kernelstore.TaskBudgetStatus, error) {
	var status kernelstore.TaskBudgetStatus
	var taskID string
	if err := row.Scan(&status.TenantID, &taskID, &status.Reserved.Tokens, &status.Reserved.CostMicroUSD,
		&status.Reserved.ToolCalls, &status.Reserved.WallSeconds, &status.Exhausted,
		&status.ResourceVersion, &status.UpdatedAt,
		&status.Consumed.Tokens, &status.Consumed.CostMicroUSD,
		&status.Consumed.ToolCalls, &status.Consumed.WallSeconds); err != nil {
		return status, err
	}
	parsed, err := uuid.Parse(taskID)
	if err != nil {
		return status, fmt.Errorf("parse task id: %w", err)
	}
	status.TaskID = parsed
	return status, nil
}
