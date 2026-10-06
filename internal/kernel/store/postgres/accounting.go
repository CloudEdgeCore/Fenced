package postgres

import (
	"context"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
)

// AccountingReconciliation reports drift between append-only ledgers and
// their materialized counters. ProviderReceiptGaps are observable but never
// auto-repaired because external provider evidence is required.
type AccountingReconciliation struct {
	TaskLedgerDrift      int64
	QuotaReservedDrift   int64
	ModelLedgerDrift     int64
	ProviderReceiptGaps  int64
	WorkflowCounterDrift int64
	Repaired             bool
}

// ReconcileAccounting audits the budget, quota, and model accounting
// invariants. When repair is true, only counters derivable from immutable
// local ledger rows are corrected in one transaction.
func (s *Store) ReconcileAccounting(ctx context.Context, repair bool) (AccountingReconciliation, error) {
	var report AccountingReconciliation
	tx, err := s.begin(ctx)
	if err != nil {
		return report, err
	}
	defer rollback(ctx, tx)

	queries := []struct {
		target *int64
		sql    string
	}{
		{&report.TaskLedgerDrift, `WITH actual AS (
			SELECT l.tenant_id, l.task_id, COALESCE(SUM(s.tokens),0) tokens,
				COALESCE(SUM(s.cost_micro_usd),0) cost, COALESCE(SUM(s.tool_calls),0) tools,
				COALESCE(SUM(s.wall_seconds),0) wall
			FROM task_budget_ledgers l LEFT JOIN task_budget_settlements s
				ON s.tenant_id=l.tenant_id AND s.task_id=l.task_id
			GROUP BY l.tenant_id,l.task_id)
		SELECT COUNT(*) FROM task_budget_ledgers l JOIN actual a USING (tenant_id,task_id)
		WHERE (l.consumed_tokens,l.consumed_cost_micro_usd,l.consumed_tool_calls,l.consumed_wall_seconds)
			IS DISTINCT FROM (a.tokens,a.cost,a.tools,a.wall)`},
		{&report.QuotaReservedDrift, `WITH expected AS (
			SELECT w.tenant_id,w.window_start,
				COALESCE(SUM(l.reserved_tokens) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) tokens,
				COALESCE(SUM(l.reserved_cost_micro_usd) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) cost,
				COALESCE(SUM(l.reserved_tool_calls) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) tools,
				COALESCE(SUM(l.reserved_wall_seconds) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) wall
			FROM tenant_consumption_windows w LEFT JOIN task_budget_ledgers l
				ON l.tenant_id=w.tenant_id AND l.quota_reserved_window_start=w.window_start
			LEFT JOIN tasks t ON t.tenant_id=l.tenant_id AND t.id=l.task_id
			GROUP BY w.tenant_id,w.window_start)
		SELECT COUNT(*) FROM tenant_consumption_windows w JOIN expected e USING (tenant_id,window_start)
		WHERE (w.reserved_tokens,w.reserved_cost_micro_usd,w.reserved_tool_calls,w.reserved_wall_seconds)
			IS DISTINCT FROM (e.tokens,e.cost,e.tools,e.wall)`},
		{&report.ModelLedgerDrift, `WITH settled AS (
			SELECT m.tenant_id,m.id,m.input_tokens+m.output_tokens tokens,m.cost_micro_usd cost,
				COALESCE(SUM(s.tokens),0) settled_tokens,COALESCE(SUM(s.cost_micro_usd),0) settled_cost
			FROM model_calls m JOIN task_budget_ledgers l ON l.tenant_id=m.tenant_id AND l.task_id=m.task_id
			LEFT JOIN task_budget_settlements s ON s.tenant_id=m.tenant_id AND s.task_id=m.task_id
				AND s.idempotency_key LIKE 'model:' || m.id::text || ':%'
			WHERE m.status IN ('COMPLETED','FAILED','STOPPED')
			GROUP BY m.tenant_id,m.id)
		SELECT COUNT(*) FROM settled WHERE (tokens,cost) IS DISTINCT FROM (settled_tokens,settled_cost)`},
		{&report.ProviderReceiptGaps, `SELECT COUNT(*) FROM model_calls
			WHERE usage_certainty='UNKNOWN_USAGE'
			   OR (status='COMPLETED' AND COALESCE(provider_request_id,'')='')`},
		{&report.WorkflowCounterDrift, `WITH actual AS (
			SELECT w.tenant_id,w.id,COUNT(s.id) steps,COUNT(s.id) FILTER(WHERE s.is_dynamic) dynamic
			FROM workflows w LEFT JOIN workflow_steps s ON s.tenant_id=w.tenant_id AND s.workflow_id=w.id
			GROUP BY w.tenant_id,w.id)
			SELECT COUNT(*) FROM workflows w JOIN actual a ON a.tenant_id=w.tenant_id AND a.id=w.id
			WHERE (w.step_count,w.dynamic_step_count) IS DISTINCT FROM (a.steps,a.dynamic)`},
	}
	for _, query := range queries {
		if err := tx.QueryRow(ctx, query.sql).Scan(query.target); err != nil {
			return report, classify(err)
		}
	}
	agentmetrics.AccountingDrift(ctx, "task_usage", report.TaskLedgerDrift)
	agentmetrics.AccountingDrift(ctx, "quota_reservation", report.QuotaReservedDrift)
	agentmetrics.AccountingDrift(ctx, "model_settlement", report.ModelLedgerDrift)
	agentmetrics.AccountingDrift(ctx, "provider_receipt", report.ProviderReceiptGaps)
	agentmetrics.AccountingDrift(ctx, "workflow_counters", report.WorkflowCounterDrift)
	if repair {
		if _, err := tx.Exec(ctx, `WITH actual AS (
			SELECT l.tenant_id,l.task_id,COALESCE(SUM(s.tokens),0) tokens,
				COALESCE(SUM(s.cost_micro_usd),0) cost,COALESCE(SUM(s.tool_calls),0) tools,
				COALESCE(SUM(s.wall_seconds),0) wall
			FROM task_budget_ledgers l LEFT JOIN task_budget_settlements s
				ON s.tenant_id=l.tenant_id AND s.task_id=l.task_id GROUP BY l.tenant_id,l.task_id)
		UPDATE task_budget_ledgers l SET consumed_tokens=a.tokens,consumed_cost_micro_usd=a.cost,
			consumed_tool_calls=a.tools,consumed_wall_seconds=a.wall,updated_at=$1,resource_version=resource_version+1
		FROM actual a WHERE l.tenant_id=a.tenant_id AND l.task_id=a.task_id AND
			(l.consumed_tokens,l.consumed_cost_micro_usd,l.consumed_tool_calls,l.consumed_wall_seconds)
			IS DISTINCT FROM (a.tokens,a.cost,a.tools,a.wall)`, s.now()); err != nil {
			return report, classify(err)
		}
		if _, err := tx.Exec(ctx, `WITH expected AS (
			SELECT w.tenant_id,w.window_start,
				COALESCE(SUM(l.reserved_tokens) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) tokens,
				COALESCE(SUM(l.reserved_cost_micro_usd) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) cost,
				COALESCE(SUM(l.reserved_tool_calls) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) tools,
				COALESCE(SUM(l.reserved_wall_seconds) FILTER (WHERE t.phase NOT IN ('SUCCEEDED','FAILED','CANCELLED')),0) wall
			FROM tenant_consumption_windows w LEFT JOIN task_budget_ledgers l
				ON l.tenant_id=w.tenant_id AND l.quota_reserved_window_start=w.window_start
			LEFT JOIN tasks t ON t.tenant_id=l.tenant_id AND t.id=l.task_id GROUP BY w.tenant_id,w.window_start)
		UPDATE tenant_consumption_windows w SET reserved_tokens=e.tokens,reserved_cost_micro_usd=e.cost,
			reserved_tool_calls=e.tools,reserved_wall_seconds=e.wall,updated_at=$1,resource_version=resource_version+1
		FROM expected e WHERE w.tenant_id=e.tenant_id AND w.window_start=e.window_start AND
			(w.reserved_tokens,w.reserved_cost_micro_usd,w.reserved_tool_calls,w.reserved_wall_seconds)
			IS DISTINCT FROM (e.tokens,e.cost,e.tools,e.wall)`, s.now()); err != nil {
			return report, classify(err)
		}
		if _, err := tx.Exec(ctx, `WITH actual AS (
			SELECT w.tenant_id,w.id,COUNT(s.id) steps,COUNT(s.id) FILTER(WHERE s.is_dynamic) dynamic
			FROM workflows w LEFT JOIN workflow_steps s ON s.tenant_id=w.tenant_id AND s.workflow_id=w.id
			GROUP BY w.tenant_id,w.id)
		UPDATE workflows w SET step_count=a.steps,dynamic_step_count=a.dynamic,
			resource_version=resource_version+1,updated_at=$1 FROM actual a
		WHERE w.tenant_id=a.tenant_id AND w.id=a.id AND
			(w.step_count,w.dynamic_step_count) IS DISTINCT FROM (a.steps,a.dynamic)`, s.now()); err != nil {
			return report, classify(err)
		}
		report.Repaired = report.TaskLedgerDrift > 0 || report.QuotaReservedDrift > 0 || report.WorkflowCounterDrift > 0
	}
	if err := tx.Commit(ctx); err != nil {
		return report, fmt.Errorf("commit accounting reconciliation: %w", classify(err))
	}
	return report, nil
}
