//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/google/uuid"
)

func TestTaskBudgetReservationSettlementAndHardStop(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	task := createBudgetedTask(t, ctx, repository, "budget-ledger", 100, 10, 60, 1.0)

	reserved, err := repository.GetTaskBudget(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("read reservation: %v", err)
	}
	if reserved.Reserved.Tokens != 100 || reserved.Reserved.CostMicroUSD != money.MustFromUSD(1.0) ||
		reserved.Reserved.ToolCalls != 10 || reserved.Reserved.WallSeconds != 60 || reserved.Exhausted {
		t.Fatalf("unexpected reservation: %+v", reserved)
	}

	// A settlement within the ceiling is recorded once.
	settled, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "usage-1",
		Usage: kernelstore.TaskBudget{Tokens: 60, CostMicroUSD: money.MustFromUSD(0.5), ToolCalls: 4, WallSeconds: 30},
	})
	if err != nil {
		t.Fatalf("settle usage: %v", err)
	}
	if settled.Consumed.Tokens != 60 || settled.Consumed.CostMicroUSD != money.MustFromUSD(0.5) ||
		settled.Consumed.ToolCalls != 4 || settled.Consumed.WallSeconds != 30 || settled.Exhausted {
		t.Fatalf("unexpected consumption: %+v", settled)
	}

	// Replaying the same idempotency key does not double-count.
	replayed, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "usage-1",
		Usage: kernelstore.TaskBudget{Tokens: 60, CostMicroUSD: money.MustFromUSD(0.5), ToolCalls: 4, WallSeconds: 30},
	})
	if err != nil || replayed.Consumed.Tokens != 60 {
		t.Fatalf("idempotent replay: %+v err=%v", replayed, err)
	}

	// A settlement that would exceed the reservation is rejected, the ledger
	// is marked exhausted, and the settlement is not recorded.
	_, err = repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "usage-2",
		Usage: kernelstore.TaskBudget{Tokens: 50},
	})
	if !errors.Is(err, kernelstore.ErrBudgetExceeded) {
		t.Fatalf("expected budget rejection, got %v", err)
	}
	after, err := repository.GetTaskBudget(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("read exhausted budget: %v", err)
	}
	if after.Consumed.Tokens != 60 || !after.Exhausted {
		t.Fatalf("exhausted ledger state: %+v", after)
	}

	// Further consumption attempts stay rejected.
	if _, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "usage-3",
		Usage: kernelstore.TaskBudget{Tokens: 1},
	}); !errors.Is(err, kernelstore.ErrBudgetExceeded) {
		t.Fatalf("expected continued rejection, got %v", err)
	}

	var settlementRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2`, "tenant-a", task.ID.String()).Scan(&settlementRows); err != nil {
		t.Fatalf("count settlements: %v", err)
	}
	if settlementRows != 1 {
		t.Fatalf("settlements = %d, want only the accepted one", settlementRows)
	}
}

func TestCostDimensionHardStopIsIndependent(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	task := createBudgetedTask(t, ctx, repository, "budget-cost", 0, 0, 0, 1.0)

	// Token dimension is unlimited (0 = no ceiling); cost is enforced.
	if _, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "cost-1",
		Usage: kernelstore.TaskBudget{Tokens: 10_000_000, CostMicroUSD: money.MustFromUSD(0.6)},
	}); err != nil {
		t.Fatalf("settle within cost ceiling: %v", err)
	}
	if _, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "cost-2",
		Usage: kernelstore.TaskBudget{CostMicroUSD: money.MustFromUSD(0.5)},
	}); !errors.Is(err, kernelstore.ErrBudgetExceeded) {
		t.Fatalf("expected cost rejection, got %v", err)
	}
}

func TestConcurrentUsageReservationsProtectHeadroom(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	task := createBudgetedTask(t, ctx, repository, "budget-reservations", 100, 0, 0, 1)
	expiry := clock.Now().Add(time.Minute)
	if err := repository.ReserveTaskUsage(ctx, kernelstore.ReserveTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, ReservationKey: "call-a",
		Amount: kernelstore.TaskBudget{Tokens: 80, CostMicroUSD: money.MustFromUSD(.8)}, ExpiresAt: expiry,
	}); err != nil {
		t.Fatalf("reserve first call: %v", err)
	}
	if err := repository.ReserveTaskUsage(ctx, kernelstore.ReserveTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, ReservationKey: "call-b",
		Amount: kernelstore.TaskBudget{Tokens: 30, CostMicroUSD: money.MustFromUSD(.3)}, ExpiresAt: expiry,
	}); !errors.Is(err, kernelstore.ErrBudgetExceeded) {
		t.Fatalf("second call reused reserved headroom: %v", err)
	}
	if _, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, IdempotencyKey: "call-a:finish", ReservationKey: "call-a",
		Usage: kernelstore.TaskBudget{Tokens: 70, CostMicroUSD: money.MustFromUSD(.7)},
	}); err != nil {
		t.Fatalf("settle reserved call: %v", err)
	}
	if err := repository.ReleaseTaskUsageReservation(ctx, "tenant-a", task.ID, "call-a"); err != nil {
		t.Fatalf("release reservation: %v", err)
	}
	if err := repository.ReserveTaskUsage(ctx, kernelstore.ReserveTaskUsageInput{
		TenantID: "tenant-a", TaskID: task.ID, ReservationKey: "call-b",
		Amount: kernelstore.TaskBudget{Tokens: 30, CostMicroUSD: money.MustFromUSD(.3)}, ExpiresAt: expiry,
	}); err != nil {
		t.Fatalf("reserve remaining headroom: %v", err)
	}
	var active int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_usage_reservations
		WHERE tenant_id = $1 AND task_id = $2 AND status = 'ACTIVE'`, "tenant-a", task.ID).Scan(&active); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	if active != 1 {
		t.Fatalf("active reservations = %d, want 1", active)
	}
}

func TestRejectedAndUnbudgetedTasksHoldNoLedger(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: "rejected budget", Spec: []byte(`{}`), IdempotencyKey: "rejected-budget",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim task: claims=%d err=%v", len(claims), err)
	}
	budget := kernelstore.TaskBudget{Tokens: 100}
	if _, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "admission",
		ClaimFencingToken: claims[0].FencingToken, ExpectedTaskVersion: 1,
		Admit: false, ReasonCode: "POLICY_DENIED", EvaluatorVersion: "test/v1", Budget: &budget,
	}); err != nil {
		t.Fatalf("reject task: %v", err)
	}
	if _, err := repository.GetTaskBudget(ctx, "tenant-a", created.Task.ID); !errors.Is(err, kernelstore.ErrBudgetNotReserved) {
		t.Fatalf("rejected task holds a ledger: %v", err)
	}

	unbudgeted, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: "no budget", Spec: []byte(`{}`), IdempotencyKey: "no-budget",
	})
	if err != nil {
		t.Fatalf("create unbudgeted task: %v", err)
	}
	if _, err := repository.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
		TenantID: "tenant-a", TaskID: unbudgeted.Task.ID, IdempotencyKey: "usage-1",
		Usage: kernelstore.TaskBudget{Tokens: 1},
	}); !errors.Is(err, kernelstore.ErrBudgetNotReserved) {
		t.Fatalf("unbudgeted task accepted a settlement: %v", err)
	}
}

func createBudgetedTask(t *testing.T, ctx context.Context, repository *postgresstore.Store, key string, tokens, toolCalls, wallSeconds int64, costUSD float64) kernelstore.Task {
	t.Helper()
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: key, Spec: []byte(`{}`), IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim task: claims=%d err=%v", len(claims), err)
	}
	budget := kernelstore.TaskBudget{Tokens: tokens, CostMicroUSD: money.MustFromUSD(costUSD), ToolCalls: toolCalls, WallSeconds: wallSeconds}
	admitted, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "admission",
		ClaimFencingToken: claims[0].FencingToken, ExpectedTaskVersion: created.Task.ResourceVersion,
		Admit: true, ReasonCode: "ADMISSION_PASSED", EvaluatorVersion: "test/v1", Budget: &budget,
	})
	if err != nil {
		t.Fatalf("admit task: %v", err)
	}
	return admitted
}
