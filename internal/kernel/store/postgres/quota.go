package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ kernelstore.TenantQuotaStore = (*Store)(nil)

func (s *Store) SetTenantQuota(ctx context.Context, in kernelstore.SetTenantQuotaInput) (kernelstore.TenantQuota, error) {
	var zero kernelstore.TenantQuota
	if !in.Valid() {
		return zero, fmt.Errorf("tenant, window seconds >= 60, and non-negative limits are required")
	}
	now := s.now()
	quota, err := scanTenantQuota(s.pool.QueryRow(ctx, `INSERT INTO tenant_quotas (
		tenant_id, window_seconds, tokens, cost_micro_usd, tool_calls, wall_seconds,
		resource_version, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $7)
	ON CONFLICT (tenant_id) DO UPDATE SET
		window_seconds = EXCLUDED.window_seconds,
		tokens = EXCLUDED.tokens,
		cost_micro_usd = EXCLUDED.cost_micro_usd,
		tool_calls = EXCLUDED.tool_calls,
		wall_seconds = EXCLUDED.wall_seconds,
		resource_version = tenant_quotas.resource_version + 1,
		updated_at = EXCLUDED.updated_at
	RETURNING `+tenantQuotaColumns,
		in.TenantID, in.WindowSeconds, in.Limits.Tokens, in.Limits.CostMicroUSD,
		in.Limits.ToolCalls, in.Limits.WallSeconds, now))
	if err != nil {
		return zero, classify(err)
	}
	return quota, nil
}

func (s *Store) GetTenantQuota(ctx context.Context, tenantID string) (kernelstore.TenantQuota, error) {
	quota, err := scanTenantQuota(s.pool.QueryRow(ctx, `SELECT `+tenantQuotaColumns+`
		FROM tenant_quotas WHERE tenant_id = $1`, tenantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.TenantQuota{}, kernelstore.ErrNotFound
	}
	if err != nil {
		return kernelstore.TenantQuota{}, classify(err)
	}
	return quota, nil
}

func (s *Store) DeleteTenantQuota(ctx context.Context, tenantID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM tenant_quotas WHERE tenant_id = $1`, tenantID)
	return classify(err)
}

// GetTenantQuotaUsage returns the settled consumption of the fixed window
// containing at. A tenant without a configured quota reports ErrNotFound; a
// tenant with a quota but no settlements yet reports zero usage.
func (s *Store) GetTenantQuotaUsage(ctx context.Context, tenantID string, at time.Time) (kernelstore.TenantWindowUsage, error) {
	var zero kernelstore.TenantWindowUsage
	var windowSeconds int64
	err := s.pool.QueryRow(ctx, `SELECT window_seconds FROM tenant_quotas WHERE tenant_id = $1`, tenantID).Scan(&windowSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, kernelstore.ErrNotFound
	}
	if err != nil {
		return zero, classify(err)
	}
	windowStart := windowStartAt(at, windowSeconds)
	usage, err := scanTenantWindowUsage(s.pool.QueryRow(ctx, `SELECT `+tenantWindowUsageColumns+`
		FROM tenant_consumption_windows WHERE tenant_id = $1 AND window_start = $2`, tenantID, windowStart))
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.TenantWindowUsage{TenantID: tenantID, WindowStart: windowStart}, nil
	}
	if err != nil {
		return zero, classify(err)
	}
	return usage, nil
}

// enforceTenantQuota is the atomic admission gate for tenant aggregate
// consumption quotas (v0.8: reservation semantics). Inside the DecideAdmission
// transaction it upserts and locks the tenant's current window row and
// rejects the admission when consumed plus the reserved ceilings of in-flight
// tasks plus the task's own ceiling would exceed any configured limit; on
// admission it reserves the ceiling in the same locked row, so concurrent
// admissions serialize and cannot collectively overshoot. The returned
// window start is recorded on the task's ledger so the reservation is
// released against the exact window at task terminal. A tenant without a
// configured quota is never gated. Unbudgeted tasks reserve nothing but are
// still rejected once consumed plus reserved is already over a limit.
func (s *Store) enforceTenantQuota(ctx context.Context, tx pgx.Tx, tenantID string, budget *kernelstore.TaskBudget, now time.Time) (*time.Time, error) {
	var limits kernelstore.TaskBudget
	var windowSeconds int64
	err := tx.QueryRow(ctx, `SELECT window_seconds, tokens, cost_micro_usd, tool_calls, wall_seconds
		FROM tenant_quotas WHERE tenant_id = $1`, tenantID).Scan(
		&windowSeconds, &limits.Tokens, &limits.CostMicroUSD, &limits.ToolCalls, &limits.WallSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(err)
	}
	windowStart := windowStartAt(now, windowSeconds)
	// Create the window row when it does not exist yet, so every admission
	// (even the first for a fresh tenant) serializes on the row lock.
	if _, err := tx.Exec(ctx, `INSERT INTO tenant_consumption_windows (
		tenant_id, window_start, created_at, updated_at
	) VALUES ($1, $2, $3, $3)
	ON CONFLICT (tenant_id, window_start) DO NOTHING`,
		tenantID, windowStart, now); err != nil {
		return nil, classify(err)
	}
	var consumed, reserved kernelstore.TaskBudget
	err = tx.QueryRow(ctx, `SELECT consumed_tokens, consumed_cost_micro_usd, consumed_tool_calls, consumed_wall_seconds,
		reserved_tokens, reserved_cost_micro_usd, reserved_tool_calls, reserved_wall_seconds
		FROM tenant_consumption_windows WHERE tenant_id = $1 AND window_start = $2 FOR UPDATE`,
		tenantID, windowStart).Scan(
		&consumed.Tokens, &consumed.CostMicroUSD, &consumed.ToolCalls, &consumed.WallSeconds,
		&reserved.Tokens, &reserved.CostMicroUSD, &reserved.ToolCalls, &reserved.WallSeconds)
	if err != nil {
		return nil, classify(err)
	}
	var ceiling kernelstore.TaskBudget
	if budget != nil {
		ceiling = *budget
	}
	if kernelstore.QuotaReservationExceeded(limits, consumed, reserved, ceiling) {
		return nil, fmt.Errorf("%w: tenant=%s window_start=%s limits=%+v consumed=%+v reserved=%+v additional=%+v",
			kernelstore.ErrTenantQuotaExceeded, tenantID, windowStart.Format(time.RFC3339), limits, consumed, reserved, ceiling)
	}
	if ceiling.Zero() {
		return &windowStart, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE tenant_consumption_windows SET
		reserved_tokens = reserved_tokens + $1,
		reserved_cost_micro_usd = reserved_cost_micro_usd + $2,
		reserved_tool_calls = reserved_tool_calls + $3,
		reserved_wall_seconds = reserved_wall_seconds + $4,
		resource_version = resource_version + 1, updated_at = $5
		WHERE tenant_id = $6 AND window_start = $7`,
		ceiling.Tokens, ceiling.CostMicroUSD, ceiling.ToolCalls, ceiling.WallSeconds, now, tenantID, windowStart); err != nil {
		return nil, classify(err)
	}
	return &windowStart, nil
}

// releaseTenantReservation releases a task's reserved ceiling from the window
// the task was admitted in, when the task reaches a terminal state (v0.8).
// It runs inside the caller's transaction. Tasks without a ledger (unbudgeted
// or rejected) reserve nothing and are no-ops.
func (s *Store) releaseTenantReservation(ctx context.Context, tx pgx.Tx, tenantID string, taskID uuid.UUID) error {
	var windowStart sql.NullTime
	var ceiling kernelstore.TaskBudget
	err := tx.QueryRow(ctx, `SELECT quota_reserved_window_start,
		reserved_tokens, reserved_cost_micro_usd, reserved_tool_calls, reserved_wall_seconds
		FROM task_budget_ledgers WHERE tenant_id = $1 AND task_id = $2`,
		tenantID, taskID.String()).Scan(
		&windowStart, &ceiling.Tokens, &ceiling.CostMicroUSD, &ceiling.ToolCalls, &ceiling.WallSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return classify(err)
	}
	if !windowStart.Valid || ceiling.Zero() {
		return nil
	}
	_, err = tx.Exec(ctx, `UPDATE tenant_consumption_windows SET
		reserved_tokens = GREATEST(0, reserved_tokens - $1),
		reserved_cost_micro_usd = GREATEST(0, reserved_cost_micro_usd - $2),
		reserved_tool_calls = GREATEST(0, reserved_tool_calls - $3),
		reserved_wall_seconds = GREATEST(0, reserved_wall_seconds - $4),
		resource_version = resource_version + 1, updated_at = $5
		WHERE tenant_id = $6 AND window_start = $7`,
		ceiling.Tokens, ceiling.CostMicroUSD, ceiling.ToolCalls, ceiling.WallSeconds, s.now(), tenantID, windowStart.Time)
	return classify(err)
}

// bumpTenantWindow appends one usage settlement to the tenant's current
// window. It is called only when the settlement row was actually inserted
// (never on idempotent replays), inside the same transaction, so the window
// counters are exact with the settlement ledger. A tenant without a
// configured quota tracks no windows.
func (s *Store) bumpTenantWindow(ctx context.Context, tx pgx.Tx, tenantID string, usage kernelstore.TaskBudget, now time.Time) error {
	var windowSeconds int64
	err := tx.QueryRow(ctx, `SELECT window_seconds FROM tenant_quotas WHERE tenant_id = $1`, tenantID).Scan(&windowSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return classify(err)
	}
	windowStart := windowStartAt(now, windowSeconds)
	_, err = tx.Exec(ctx, `INSERT INTO tenant_consumption_windows (
		tenant_id, window_start, consumed_tokens, consumed_cost_micro_usd,
		consumed_tool_calls, consumed_wall_seconds, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
	ON CONFLICT (tenant_id, window_start) DO UPDATE SET
		consumed_tokens = tenant_consumption_windows.consumed_tokens + EXCLUDED.consumed_tokens,
		consumed_cost_micro_usd = tenant_consumption_windows.consumed_cost_micro_usd + EXCLUDED.consumed_cost_micro_usd,
		consumed_tool_calls = tenant_consumption_windows.consumed_tool_calls + EXCLUDED.consumed_tool_calls,
		consumed_wall_seconds = tenant_consumption_windows.consumed_wall_seconds + EXCLUDED.consumed_wall_seconds,
		resource_version = tenant_consumption_windows.resource_version + 1,
		updated_at = EXCLUDED.updated_at`,
		tenantID, windowStart, usage.Tokens, usage.CostMicroUSD, usage.ToolCalls, usage.WallSeconds, now)
	if err != nil {
		return classify(err)
	}
	return nil
}

// windowStartAt returns the fixed window start containing at, aligned to the
// epoch with the quota's window length. Deterministic for any instant, which
// is what makes the admission gate and the settlement hook agree.
func windowStartAt(at time.Time, windowSeconds int64) time.Time {
	return at.UTC().Truncate(time.Duration(windowSeconds) * time.Second)
}

const tenantQuotaColumns = `
	tenant_id, window_seconds, tokens, cost_micro_usd, tool_calls, wall_seconds,
	resource_version, updated_at`

func scanTenantQuota(row scanner) (kernelstore.TenantQuota, error) {
	var quota kernelstore.TenantQuota
	if err := row.Scan(&quota.TenantID, &quota.WindowSeconds,
		&quota.Limits.Tokens, &quota.Limits.CostMicroUSD, &quota.Limits.ToolCalls, &quota.Limits.WallSeconds,
		&quota.ResourceVersion, &quota.UpdatedAt); err != nil {
		return quota, err
	}
	return quota, nil
}

const tenantWindowUsageColumns = `
	tenant_id, window_start, consumed_tokens, consumed_cost_micro_usd,
	consumed_tool_calls, consumed_wall_seconds,
	reserved_tokens, reserved_cost_micro_usd, reserved_tool_calls, reserved_wall_seconds,
	resource_version, updated_at`

func scanTenantWindowUsage(row scanner) (kernelstore.TenantWindowUsage, error) {
	var usage kernelstore.TenantWindowUsage
	if err := row.Scan(&usage.TenantID, &usage.WindowStart,
		&usage.Consumed.Tokens, &usage.Consumed.CostMicroUSD, &usage.Consumed.ToolCalls, &usage.Consumed.WallSeconds,
		&usage.Reserved.Tokens, &usage.Reserved.CostMicroUSD, &usage.Reserved.ToolCalls, &usage.Reserved.WallSeconds,
		&usage.ResourceVersion, &usage.UpdatedAt); err != nil {
		return usage, err
	}
	return usage, nil
}
