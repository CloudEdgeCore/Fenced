package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	kernelworkflow "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
	"github.com/google/uuid"
)

// WorkflowReservationReconciliation reports the P1-05 backfill repair. Flagged
// counts workflows still carrying needs_budget_reconciliation at audit time;
// Reconciled and StepsAdjusted count the workflows cleared and the step
// reservation rows corrected during a repair pass.
type WorkflowReservationReconciliation struct {
	Flagged       int64
	Reconciled    int64
	StepsAdjusted int64
	Repaired      bool
}

// ReconcileWorkflowReservations re-derives the token/cost reservation of every
// undispatched step in each flagged workflow and resyncs the usage ledger's
// step_reserved_* aggregate, closing the gap the 000028 upgrade could not fill
// in pure SQL. The token/cost ceiling of a step lives inside its merged task
// spec, so this reuses the exact merge the dispatcher applies — the workflow
// default task spec overlaid with the per-step overlay for a declared step,
// and the stored spec column for a dynamic step — via
// kernelworkflow.WorkflowSpec.StepInputs and kernelstore.TaskSpecBudgetReservation.
// The recomputed reservation is therefore byte-identical to what CreateWorkflow
// and SpawnWorkflowStep committed, never a second, divergent derivation.
//
// Only PENDING and WAITING_APPROVAL steps hold token/cost reservations;
// RUNNING and terminal steps hold none (their reservation transferred to the
// task budget ledger at admission or was released at terminal), so they are
// left untouched — reconciling them would double-count against the task ledger.
//
// Each workflow is reconciled in its own transaction with the standard lock
// order (workflow row, then step rows, then the usage ledger) so it can never
// deadlock against a concurrent spawn, step transition, or terminal release.
func (s *Store) ReconcileWorkflowReservations(ctx context.Context, repair bool) (WorkflowReservationReconciliation, error) {
	var report WorkflowReservationReconciliation
	rows, err := s.pool.Query(ctx,
		`SELECT tenant_id, id::text FROM workflows WHERE needs_budget_reconciliation ORDER BY created_at ASC`)
	if err != nil {
		return report, classify(err)
	}
	type flaggedWorkflow struct {
		tenantID string
		id       string
	}
	var flagged []flaggedWorkflow
	for rows.Next() {
		var wf flaggedWorkflow
		if err := rows.Scan(&wf.tenantID, &wf.id); err != nil {
			rows.Close()
			return report, classify(err)
		}
		flagged = append(flagged, wf)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return report, classify(err)
	}
	report.Flagged = int64(len(flagged))
	agentmetrics.AccountingDrift(ctx, "workflow_reservation", report.Flagged)
	if !repair {
		return report, nil
	}
	for _, wf := range flagged {
		adjusted, reconciled, err := s.reconcileWorkflowReservation(ctx, wf.tenantID, wf.id)
		if err != nil {
			return report, err
		}
		if reconciled {
			report.Reconciled++
			report.StepsAdjusted += adjusted
		}
	}
	report.Repaired = report.Reconciled > 0
	return report, nil
}

// reconcileWorkflowReservation reconciles one flagged workflow in a single
// transaction and clears its flag. It returns the number of step rows whose
// reservation changed and whether the workflow was reconciled (false when a
// concurrent controller already cleared the flag under the row lock).
func (s *Store) reconcileWorkflowReservation(ctx context.Context, tenantID, workflowID string) (int64, bool, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer rollback(ctx, tx)

	wfID, err := uuid.Parse(workflowID)
	if err != nil {
		return 0, false, fmt.Errorf("parse workflow id: %w", err)
	}

	var specRaw []byte
	var stillFlagged bool
	if err := tx.QueryRow(ctx,
		`SELECT spec, needs_budget_reconciliation FROM workflows
		 WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, workflowID).Scan(&specRaw, &stillFlagged); err != nil {
		return 0, false, classify(err)
	}
	if !stillFlagged {
		return 0, false, tx.Commit(ctx)
	}

	stepRows, err := tx.Query(ctx,
		`SELECT id::text, name, is_dynamic, spec, reserved_tokens, reserved_cost_micro_usd
		 FROM workflow_steps
		 WHERE tenant_id = $1 AND workflow_id = $2 AND status IN ('PENDING', 'WAITING_APPROVAL')
		 ORDER BY ordinal ASC FOR UPDATE`, tenantID, workflowID)
	if err != nil {
		return 0, false, classify(err)
	}
	type pendingStep struct {
		id           string
		name         string
		isDynamic    bool
		spec         []byte
		reservedTok  int64
		reservedCost int64
	}
	var steps []pendingStep
	for stepRows.Next() {
		var st pendingStep
		if err := stepRows.Scan(&st.id, &st.name, &st.isDynamic, &st.spec, &st.reservedTok, &st.reservedCost); err != nil {
			stepRows.Close()
			return 0, false, classify(err)
		}
		steps = append(steps, st)
	}
	stepRows.Close()
	if err := stepRows.Err(); err != nil {
		return 0, false, classify(err)
	}

	// Re-derive declared step specs exactly as the dispatcher does (default
	// task spec overlaid with the per-step overlay). Lenient decode: a stored
	// spec must not fail reconcile on strictness it already cleared at
	// publication. A step whose spec is unmergeable is simply absent from the
	// map and keeps its backfilled zero reservation.
	declared := map[string]json.RawMessage{}
	var doc kernelworkflow.WorkflowSpec
	if json.Unmarshal(specRaw, &doc) == nil {
		for _, input := range doc.StepInputs() {
			declared[input.Name] = input.Spec
		}
	}

	var ids []string
	var tokens []int64
	var costs []int64
	for _, st := range steps {
		merged := st.spec // dynamic steps carry their merged spec on the row
		if !st.isDynamic {
			merged = declared[st.name]
		}
		tok, cost := kernelstore.TaskSpecBudgetReservation(merged)
		if tok == st.reservedTok && int64(cost) == st.reservedCost {
			continue
		}
		ids = append(ids, st.id)
		tokens = append(tokens, tok)
		costs = append(costs, int64(cost))
	}

	now := s.now()
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_steps w SET reserved_tokens = v.tokens, reserved_cost_micro_usd = v.cost,
				resource_version = w.resource_version + 1, updated_at = $4
			FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[]) AS v(id, tokens, cost)
			WHERE w.tenant_id = $5 AND w.id = v.id`,
			ids, tokens, costs, now, tenantID); err != nil {
			return 0, false, classify(err)
		}
	}

	// Resync the ledger's step_reserved_* aggregate to the sum of the now
	// corrected reservations across every step. A bare aggregate yields one
	// row even when the workflow has no steps, so the ledger is set to an exact
	// value rather than left stale; the reservation invariant
	// (ledger.step_reserved_* == SUM(workflow_steps.reserved_*)) then holds.
	if _, err := tx.Exec(ctx, `
		UPDATE workflow_usage_ledgers l SET
			step_reserved_tasks = s.tasks, step_reserved_tokens = s.tokens,
			step_reserved_cost_micro_usd = s.cost, updated_at = $3
		FROM (SELECT
				COALESCE(SUM(reserved_tasks), 0) AS tasks,
				COALESCE(SUM(reserved_tokens), 0) AS tokens,
				COALESCE(SUM(reserved_cost_micro_usd), 0) AS cost
			FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2) s
		WHERE l.tenant_id = $1 AND l.workflow_id = $2`, tenantID, workflowID, now); err != nil {
		return 0, false, classify(err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE workflows SET needs_budget_reconciliation = false,
			resource_version = resource_version + 1, updated_at = $3
		WHERE tenant_id = $1 AND id = $2`, tenantID, workflowID, now); err != nil {
		return 0, false, classify(err)
	}

	if err := auditHook(ctx, tx, tenantID, "workflow.reconcileReservations", "Workflow", wfID, map[string]any{
		"stepsAdjusted": len(ids), "pendingSteps": len(steps),
	}, now); err != nil {
		return 0, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, false, classify(err)
	}
	return int64(len(ids)), true, nil
}
