//go:build integration

package postgres_test

import (
	"context"
	"testing"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func TestAccountingReconciliationDetectsAndRepairsDerivedCounters(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	task := createBudgetedTask(t, ctx, repository, "accounting-repair", 100, 10, 60, 1)
	if _, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "accounting-usage",
		Usage: kernelstore.TaskBudget{Tokens: 25},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE task_budget_ledgers SET consumed_tokens=99
		WHERE tenant_id='tenant-a' AND task_id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	report, err := repository.ReconcileAccounting(ctx, false)
	if err != nil || report.TaskLedgerDrift != 1 || report.Repaired {
		t.Fatalf("audit report=%+v err=%v", report, err)
	}
	report, err = repository.ReconcileAccounting(ctx, true)
	if err != nil || !report.Repaired {
		t.Fatalf("repair report=%+v err=%v", report, err)
	}
	status, err := repository.GetTaskBudget(ctx, "tenant-a", task.ID)
	if err != nil || status.Consumed.Tokens != 25 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
}
