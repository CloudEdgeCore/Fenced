//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const soakSpec = `{
	"priority":70,"deadline":"2099-08-14T12:00:00Z",
	"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
	"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","dataResidency":"cn","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
}`

func TestSoakSmokePipelineRepeated(t *testing.T) {
	if testing.Short() {
		t.Skip("soak smoke disabled in -short mode")
	}
	ctx := context.Background()
	tasksPerIter := 2000
	iterations := 3
	hasDocker := os.Getenv("FENCED_SOAK_DOCKER") != ""

	pool, repository := newSoakDatabase(t)
	tenant := "tenant-a"
	if _, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: tenant, Namespace: "default", Name: "soak-agent", Version: "1",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`),
	}); err != nil {
		t.Fatalf("publish agent version: %v", err)
	}
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1_000_000, MaxCostMicroUSD: money.MustFromUSD(1_000),
		MaxToolCalls: 100_000, MaxWallSeconds: 86_400, MaxCPU: 64_000,
		MaxMemory: 262_144, MaxLLMConcurrency: 128,
	})
	pools := staticPools{{
		ID: "soak-pool", TenantIDs: []string{tenant}, RuntimeClass: "oci", RuntimeInstanceID: "soak-worker-1",
		Region: "cn-east", DataResidency: "cn", Ready: true,
		AvailableCPU: int64(tasksPerIter * 100), AvailableMemory: int64(tasksPerIter * 128), AvailableLLMSlots: tasksPerIter,
	}}
	if err := repository.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register pools: %v", err)
	}

	for iter := 0; iter < iterations; iter++ {
		t.Logf("=== soak iteration %d/%d (%d tasks) ===", iter+1, iterations, tasksPerIter)

		for i := 0; i < tasksPerIter; i++ {
			if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
				ID: uuid.New(), TenantID: tenant, Namespace: "default", AgentVersionRef: "soak-agent@1",
				Goal: fmt.Sprintf("soak-%d-%d", iter, i), IdempotencyKey: fmt.Sprintf("soak-%d-%d", iter, i),
				Spec: []byte(soakSpec),
			}); err != nil {
				t.Fatalf("iteration %d create task %d: %v", iter, i, err)
			}
		}
		t.Logf("enqueued %d tasks", tasksPerIter)

		admitController := admission.NewController(repository, engine, testPolicyEngine(t), "soak/admission", 50, time.Minute)
		drainPhase(t, ctx, pool, admitController.Reconcile, "ADMITTED", tasksPerIter)

		schedController := scheduler.NewController(repository, pools, "soak/scheduler", 50, time.Minute, 30*time.Minute)
		drainPhase(t, ctx, pool, schedController.Reconcile, "RUNNING", tasksPerIter)
		completeRuns(t, ctx, pool, repository, tasksPerIter)

		remaining := countPhase(t, ctx, pool, tenant, domain.TaskQueued) +
			countPhase(t, ctx, pool, tenant, domain.TaskAdmitted) +
			countPhase(t, ctx, pool, tenant, domain.TaskRunning)
		if remaining > 0 {
			t.Fatalf("iteration %d: %d tasks not terminal", iter, remaining)
		}
		t.Logf("iteration %d: all tasks terminal, Lost=0, Duplicate=0", iter)
	}

	if hasDocker {
		t.Logf("=== PG restart soak ===")
		soakPGrestart(t, ctx, pool, repository, engine, pools, tenant)
	}
}

func soakPGrestart(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repository *postgres.Store,
	engine *admission.Engine, pools scheduler.PoolSource, tenant string) {
	t.Helper()
	restartTasks := 1000
	for i := 0; i < restartTasks; i++ {
		if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: uuid.New(), TenantID: tenant, Namespace: "default", AgentVersionRef: "soak-agent@1",
			Goal: fmt.Sprintf("pg-restart-%d", i), IdempotencyKey: fmt.Sprintf("pg-restart-%d", i),
			Spec: []byte(soakSpec),
		}); err != nil {
			t.Fatalf("pg-restart create %d: %v", i, err)
		}
	}

	out, err := exec.Command("docker", "restart", "fenced-pg").CombinedOutput()
	if err != nil {
		t.Fatalf("docker restart: %v\n%s", err, string(out))
	}
	t.Logf("PG restarted, waiting for readiness...")
	// Poll until the pool reconnects (connections established before the
	// restart are stale; pgxpool discards them and creates fresh ones).
	ready := time.Now().Add(2 * time.Minute)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pingErr := pool.Ping(pingCtx)
		cancel()
		if pingErr == nil {
			break
		}
		if time.Now().After(ready) {
			t.Fatalf("pg-restart: pool did not reconnect: %v", pingErr)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("PG is ready again")

	admitController := admission.NewController(repository, engine, testPolicyEngine(t), "soak/admission-pg", 50, time.Minute)
	drainPhase(t, ctx, pool, admitController.Reconcile, "ADMITTED", restartTasks)
	schedController := scheduler.NewController(repository, pools, "soak/scheduler-pg", 50, time.Minute, 30*time.Minute)
	drainPhase(t, ctx, pool, schedController.Reconcile, "RUNNING", restartTasks)
	completeRuns(t, ctx, pool, repository, restartTasks)

	remaining := countPhase(t, ctx, pool, tenant, domain.TaskQueued) +
		countPhase(t, ctx, pool, tenant, domain.TaskAdmitted) +
		countPhase(t, ctx, pool, tenant, domain.TaskRunning)
	if remaining > 0 {
		t.Fatalf("pg-restart: %d tasks not terminal after recovery", remaining)
	}
	t.Logf("pg-restart: all tasks terminal, Lost=0, Duplicate=0")
}

func countPhase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant string, phase domain.TaskPhase) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE tenant_id=$1 AND phase=$2`, tenant, string(phase)).Scan(&count); err != nil {
		t.Fatalf("count phase %s: %v", phase, err)
	}
	return count
}

func newSoakDatabase(t *testing.T) (*pgxpool.Pool, *postgres.Store) {
	t.Helper()
	url := os.Getenv("FENCED_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open PG: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PG: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE task_usage_reservations,
		runtime_capacity_reservations, runtime_pool_capacities, runtime_pool_tenant_grants, runtime_pools,
		provider_circuit_breakers, workflow_usage_ledgers, workflow_steps, workflows,
		model_calls, model_descriptors, tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		audit_events,
		tenant_consumption_windows, tenant_quotas,
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset DB: %v", err)
	}
	return pool, postgres.NewWithClock(pool, func() time.Time { return time.Now().UTC() })
}
