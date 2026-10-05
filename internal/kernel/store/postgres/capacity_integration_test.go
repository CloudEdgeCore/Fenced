//go:build integration

// Capacity baseline for v0.6: a repeatable control-plane pipeline benchmark
// (enqueue -> admit -> schedule -> complete) over a real PostgreSQL with the
// default controller loop, no runtime workers. The test asserts exactness
// (every task admitted/scheduled/succeeded exactly once, one run/attempt/
// lease each, exactly one outbox event per step) and reports per-phase
// throughput and latency percentiles as the capacity evidence for v0.6+
// middleware decisions (whether the control plane needs Kafka/ClickHouse).
//
// It runs in its own schema so it can truncate kernel tables without racing
// other integration packages, and it never asserts wall-clock thresholds:
// the numbers are reported, the assertions are counts.
package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultCapacityTasks = 1000

// TestControlPlanePipelineCapacityBaseline is the v0.6 capacity baseline: the
// whole kernel pipeline for N tasks on one Postgres, measured per phase.
func TestControlPlanePipelineCapacityBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("capacity baseline disabled in -short mode")
	}
	taskCount := capacityTasks()
	ctx := context.Background()
	pool, store := newCapacityDatabase(t)
	tenant := "tenant-a"
	if _, err := store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: tenant, Namespace: "default", Name: "capacity-agent", Version: "1",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`),
	}); err != nil {
		t.Fatalf("publish agent version: %v", err)
	}

	// Phase 1: enqueue the whole load, bounded concurrency, per-submission
	// latency recorded client-side.
	enqueueWall := time.Now()
	enqueueLatencies := enqueueLoad(t, ctx, store, tenant, taskCount)
	enqueueDuration := time.Since(enqueueWall)

	// Phase 2: admission controller drains the queue to ADMITTED.
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1_000_000, MaxCostMicroUSD: money.MustFromUSD(1_000),
		MaxToolCalls: 100_000, MaxWallSeconds: 86_400, MaxCPU: 64_000,
		MaxMemory: 262_144, MaxLLMConcurrency: 128,
	})
	admissionController := admission.NewController(store, engine, testPolicyEngine(t), "capacity/admission", 50, time.Minute)
	admitWall := time.Now()
	drainPhase(t, ctx, pool, admissionController.Reconcile, "ADMITTED", taskCount)
	admitDuration := time.Since(admitWall)

	// Phase 3: scheduler controller places every task. The lease TTL must
	// outlive the whole pipeline: completion starts only after the last task
	// is scheduled, and the baseline measures pure control-plane throughput
	// (lease renewal by a live worker is exercised by the runtime conformance
	// suite, not here). A short TTL would fence the first tasks while the
	// drain is still running. Scale the TTL with the task count (the drain
	// paces at ~60ms/task; 100ms/task gives headroom for CI runners).
	leaseTTL := time.Duration(taskCount)*100*time.Millisecond + 30*time.Minute
	pools := staticPools{{
		ID: "capacity-pool", TenantIDs: []string{tenant}, RuntimeClass: "oci", RuntimeInstanceID: "capacity-worker-1",
		Region: "cn-east", DataResidency: "cn", Ready: true,
		AvailableCPU: int64(taskCount * 100), AvailableMemory: int64(taskCount * 128), AvailableLLMSlots: taskCount,
	}}
	if err := store.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register capacity fixture pools: %v", err)
	}
	schedulerController := scheduler.NewController(store, pools, "capacity/scheduler", 50, time.Minute, leaseTTL)
	scheduleWall := time.Now()
	drainPhase(t, ctx, pool, schedulerController.Reconcile, "RUNNING", taskCount)
	scheduleDuration := time.Since(scheduleWall)

	// Phase 4: complete every run (attempt lifecycle + result commit).
	completeWall := time.Now()
	completeRuns(t, ctx, pool, store, taskCount)
	completeDuration := time.Since(completeWall)
	pipelineWall := time.Since(enqueueWall)

	// Assertions: exactness, never wall-clock thresholds.
	assertCapacityExactness(t, ctx, pool, tenant, taskCount)
	latencies := capacityPhaseLatencies(t, ctx, pool, tenant)

	t.Logf("CAPACITY REPORT tasks=%d wall=%s throughput=%.0f tasks/s", taskCount,
		pipelineWall.Round(time.Millisecond), float64(taskCount)/pipelineWall.Seconds())
	t.Logf("CAPACITY REPORT enqueue wall=%s throughput=%.0f tasks/s submit p50=%s p95=%s p99=%s",
		enqueueDuration.Round(time.Millisecond), float64(taskCount)/enqueueDuration.Seconds(),
		percentileLabel(enqueueLatencies, 0.50), percentileLabel(enqueueLatencies, 0.95), percentileLabel(enqueueLatencies, 0.99))
	t.Logf("CAPACITY REPORT admit wall=%s throughput=%.0f tasks/s queue->admit p50=%s p95=%s p99=%s",
		admitDuration.Round(time.Millisecond), float64(taskCount)/admitDuration.Seconds(),
		percentileLabel(latencies.admit, 0.50), percentileLabel(latencies.admit, 0.95), percentileLabel(latencies.admit, 0.99))
	t.Logf("CAPACITY REPORT schedule wall=%s throughput=%.0f tasks/s admit->run p50=%s p95=%s p99=%s",
		scheduleDuration.Round(time.Millisecond), float64(taskCount)/scheduleDuration.Seconds(),
		percentileLabel(latencies.schedule, 0.50), percentileLabel(latencies.schedule, 0.95), percentileLabel(latencies.schedule, 0.99))
	t.Logf("CAPACITY REPORT complete wall=%s throughput=%.0f tasks/s run->done p50=%s p95=%s p99=%s",
		completeDuration.Round(time.Millisecond), float64(taskCount)/completeDuration.Seconds(),
		percentileLabel(latencies.run, 0.50), percentileLabel(latencies.run, 0.95), percentileLabel(latencies.run, 0.99))
	t.Logf("CAPACITY REPORT end-to-end p50=%s p95=%s p99=%s",
		percentileLabel(latencies.endToEnd, 0.50), percentileLabel(latencies.endToEnd, 0.95), percentileLabel(latencies.endToEnd, 0.99))
}

func enqueueLoad(t *testing.T, ctx context.Context, store *postgresstore.Store, tenant string, taskCount int) []time.Duration {
	t.Helper()
	latencies := make([]time.Duration, taskCount)
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 100)
	var failures atomic.Int64
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			spec := `{"priority":70,"deadline":"2099-08-14T12:00:00Z",` +
				`"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},` +
				`"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","dataResidency":"cn","artifactRegion":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}}`
			start := time.Now()
			if _, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
				ID: uuid.New(), TenantID: tenant, Namespace: "default", AgentVersionRef: "capacity-agent@1",
				Goal: "capacity task " + strconv.Itoa(i), IdempotencyKey: "capacity-" + strconv.Itoa(i),
				Spec: []byte(spec),
			}); err != nil {
				failures.Add(1)
				t.Errorf("enqueue task %d: %v", i, err)
			}
			latencies[i] = time.Since(start)
		}(i)
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d enqueues failed", failures.Load())
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies
}

// drainPhase reconciles until count tasks reached the phase, with a deadline
// so a stuck pipeline fails the test instead of hanging it. The deadline
// scales with the load (phases must sustain at least 50 tasks/s), so large
// baselines like 100k are not falsely failed by a fixed wall-clock bound.
func drainPhase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reconcile func(context.Context) (int, error), phase string, count int) {
	t.Helper()
	// 60ms/task accommodates CI runners (~4× slower than local): at 19
	// tasks/s the schedule phase for 100k needs ~88 min. The go test
	// timeout and job timeout must both exceed the sum of all phases.
	deadline := time.Now().Add(2*time.Minute + time.Duration(count)*60*time.Millisecond)
	lastDiag := time.Now()
	for {
		if _, err := reconcile(ctx); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		var reached int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE phase = $1`, phase).Scan(&reached); err != nil {
			t.Fatalf("count phase %s: %v", phase, err)
		}
		if reached == count {
			return
		}
		if time.Since(lastDiag) >= 5*time.Second {
			pipelineDiagnostics(t, ctx, pool, phase)
			lastDiag = time.Now()
		}
		if time.Now().After(deadline) {
			pipelineDiagnostics(t, ctx, pool, phase)
			t.Fatalf("pipeline stalled at phase %s: %d of %d", phase, reached, count)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pipelineDiagnostics reports where the control-plane pipeline stands so a
// nightly stall is self-diagnosing: reached counts, deferred tasks with their
// backoff horizon, and the retry-count distribution.
func pipelineDiagnostics(t *testing.T, ctx context.Context, pool *pgxpool.Pool, phase string) {
	t.Helper()
	var total, reached, deferred, deferredSoon int
	var maxRetry int64
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM tasks),
		(SELECT count(*) FROM tasks WHERE phase = $1),
		(SELECT count(*) FROM tasks WHERE phase = 'ADMITTED' AND next_schedule_attempt_at > now()),
		(SELECT count(*) FROM tasks WHERE phase = 'ADMITTED' AND next_schedule_attempt_at IS NOT NULL
			AND next_schedule_attempt_at <= now() + interval '30 seconds'),
		(SELECT COALESCE(max(schedule_retry_count), 0) FROM tasks WHERE phase = 'ADMITTED')`,
		phase).Scan(&total, &reached, &deferred, &deferredSoon, &maxRetry); err != nil {
		t.Logf("PIPELINE diagnostics unavailable: %v", err)
		return
	}
	t.Logf("PIPELINE phase=%s reached=%d/%d deferred=%d deferred<=30s=%d maxRetry=%d",
		phase, reached, total, deferred, deferredSoon, maxRetry)
	if deferred > 0 {
		var retryBuckets string
		if err := pool.QueryRow(ctx, `SELECT string_agg(bucket || ':' || cnt, ' ' ORDER BY bucket) FROM (
			SELECT (schedule_retry_count / 10 * 10)::text AS bucket, count(*) AS cnt
			FROM tasks WHERE phase = 'ADMITTED' AND next_schedule_attempt_at > now()
			GROUP BY 1) t`).Scan(&retryBuckets); err == nil {
			t.Logf("PIPELINE deferred retry-count buckets: %s", retryBuckets)
		}
	}
}

// completeRuns drives every run through the attempt lifecycle and commits
// its result, with bounded worker concurrency.
func completeRuns(t *testing.T, ctx context.Context, pool *pgxpool.Pool, store *postgresstore.Store, taskCount int) {
	t.Helper()
	type runRef struct {
		runID, attemptID uuid.UUID
		tenantID         string
		fencingToken     int64
		runVersion       int64
	}
	rows, err := pool.Query(ctx, `SELECT r.id, a.id, r.tenant_id, a.fencing_token, r.resource_version
		FROM runs r JOIN attempts a ON a.tenant_id = r.tenant_id AND a.run_id = r.id
		WHERE r.phase = 'RUNNING' AND a.phase = 'PLACED'`)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	var refs []runRef
	for rows.Next() {
		var ref runRef
		var runID, attemptID string
		if err := rows.Scan(&runID, &attemptID, &ref.tenantID, &ref.fencingToken, &ref.runVersion); err != nil {
			rows.Close()
			t.Fatalf("scan run: %v", err)
		}
		if ref.runID, err = uuid.Parse(runID); err != nil {
			rows.Close()
			t.Fatalf("parse run id: %v", err)
		}
		if ref.attemptID, err = uuid.Parse(attemptID); err != nil {
			rows.Close()
			t.Fatalf("parse attempt id: %v", err)
		}
		refs = append(refs, ref)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate runs: %v", err)
	}
	if len(refs) != taskCount {
		t.Fatalf("runs to complete = %d, want %d", len(refs), taskCount)
	}

	var wg sync.WaitGroup
	var failures atomic.Int64
	semaphore := make(chan struct{}, 16)
	for _, ref := range refs {
		wg.Add(1)
		go func(ref runRef) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			attemptVersion := int64(1)
			for _, to := range []domain.AttemptPhase{domain.AttemptStarting, domain.AttemptRunning, domain.AttemptCompleted} {
				attempt, err := store.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
					TenantID: ref.tenantID, AttemptID: ref.attemptID, FencingToken: ref.fencingToken,
					ExpectedAttemptVersion: attemptVersion, To: to,
				})
				if err != nil {
					failures.Add(1)
					t.Errorf("transition attempt %s to %s: %v", ref.attemptID, to, err)
					return
				}
				attemptVersion = attempt.ResourceVersion
			}
			if _, _, err := store.CompleteRun(ctx, kernelstore.CompleteRunInput{
				TenantID: ref.tenantID, RunID: ref.runID, AttemptID: ref.attemptID, FencingToken: ref.fencingToken,
				ExpectedRunVersion: ref.runVersion, ResultRef: "cas://sha256/capacity-result",
			}); err != nil {
				failures.Add(1)
				t.Errorf("complete run %s: %v", ref.runID, err)
			}
		}(ref)
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d completions failed", failures.Load())
	}
}

type capacityLatencies struct {
	admit, schedule, run, endToEnd []time.Duration
}

// capacityPhaseLatencies derives per-task phase latencies from store
// timestamps (all written by the same kernel clock), so the numbers are
// repeatable across machines and independent of test-side measurement.
func capacityPhaseLatencies(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant string) capacityLatencies {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT
		EXTRACT(EPOCH FROM (t.admitted_at - t.created_at)),
		EXTRACT(EPOCH FROM (r.created_at - t.admitted_at)),
		EXTRACT(EPOCH FROM (r.completed_at - r.created_at)),
		EXTRACT(EPOCH FROM (r.completed_at - t.created_at))
		FROM tasks t JOIN runs r ON r.task_id = t.id
		WHERE t.tenant_id = $1 AND t.phase = 'SUCCEEDED'`, tenant)
	if err != nil {
		t.Fatalf("query phase latencies: %v", err)
	}
	defer rows.Close()
	var latencies capacityLatencies
	for rows.Next() {
		var admit, schedule, run, endToEnd float64
		if err := rows.Scan(&admit, &schedule, &run, &endToEnd); err != nil {
			t.Fatalf("scan latency: %v", err)
		}
		latencies.admit = append(latencies.admit, secondsDuration(admit))
		latencies.schedule = append(latencies.schedule, secondsDuration(schedule))
		latencies.run = append(latencies.run, secondsDuration(run))
		latencies.endToEnd = append(latencies.endToEnd, secondsDuration(endToEnd))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate latencies: %v", err)
	}
	sortDurations(latencies.admit)
	sortDurations(latencies.schedule)
	sortDurations(latencies.run)
	sortDurations(latencies.endToEnd)
	return latencies
}

func secondsDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func sortDurations(values []time.Duration) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
}

// percentileLabel renders the p-th percentile of sorted durations.
func percentileLabel(durations []time.Duration, percentile float64) string {
	if len(durations) == 0 {
		return "n/a"
	}
	index := int(percentile*float64(len(durations))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(durations) {
		index = len(durations) - 1
	}
	return durations[index].Round(time.Millisecond).String()
}

// assertCapacityExactness verifies the pipeline produced exactly one of each
// artifact per task and no leftovers: the correctness contract the capacity
// numbers must hold.
func assertCapacityExactness(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant string, taskCount int) {
	t.Helper()
	var succeeded, nonTerminal int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE phase = 'SUCCEEDED'),
		count(*) FILTER (WHERE phase NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'REJECTED'))
		FROM tasks WHERE tenant_id = $1`, tenant).Scan(&succeeded, &nonTerminal); err != nil {
		t.Fatalf("count outcomes: %v", err)
	}
	if succeeded != taskCount || nonTerminal != 0 {
		t.Fatalf("outcomes: succeeded=%d nonTerminal=%d of %d", succeeded, nonTerminal, taskCount)
	}
	var runs, attempts, activeLeases int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM runs WHERE tenant_id = $1),
		(SELECT count(*) FROM attempts WHERE tenant_id = $1),
		(SELECT count(*) FROM runtime_leases WHERE tenant_id = $1 AND released_at IS NULL)`,
		tenant).Scan(&runs, &attempts, &activeLeases); err != nil {
		t.Fatalf("count artifacts: %v", err)
	}
	if runs != taskCount || attempts != taskCount || activeLeases != 0 {
		t.Fatalf("artifacts: runs=%d attempts=%d activeLeases=%d, want %d/%d/0", runs, attempts, activeLeases, taskCount, taskCount)
	}
	var decisions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM admission_decisions
		WHERE tenant_id = $1 AND decision = 'ADMIT'`, tenant).Scan(&decisions); err != nil {
		t.Fatalf("count decisions: %v", err)
	}
	if decisions != taskCount {
		t.Fatalf("admit decisions = %d, want %d", decisions, taskCount)
	}
	for _, event := range []struct {
		aggregate string
		eventType string
	}{
		{"Task", "TaskQueued"}, {"Task", "TaskAdmitted"}, {"Task", "TaskRunning"}, {"Task", "TaskSucceeded"},
		{"Run", "RunCreated"}, {"Run", "RunCompleted"},
		{"Attempt", "AttemptPlaced"}, {"Attempt", "AttemptCompleted"},
	} {
		var events int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
			WHERE tenant_id = $1 AND aggregate_type = $2 AND event_type = $3`,
			tenant, event.aggregate, event.eventType).Scan(&events); err != nil {
			t.Fatalf("count event %s/%s: %v", event.aggregate, event.eventType, err)
		}
		if events != taskCount {
			t.Fatalf("outbox %s/%s = %d, want %d (exactly one per task)", event.aggregate, event.eventType, events, taskCount)
		}
	}
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_controller_claims WHERE tenant_id = $1`, tenant).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 0 {
		t.Fatalf("lingering claims = %d, want 0", claims)
	}
}

func capacityTasks() int {
	if value := os.Getenv("FENCED_CAPACITY_TASKS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultCapacityTasks
}

// newCapacityDatabase opens the test database in its own schema so the
// baseline can truncate kernel tables without racing other integration
// packages.
func newCapacityDatabase(t *testing.T) (*pgxpool.Pool, *postgresstore.Store) {
	t.Helper()
	url := os.Getenv("FENCED_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS fenced_capacity`); err != nil {
		admin.Close(ctx)
		t.Fatalf("create capacity schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = "fenced_capacity"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	migrations := filepath.Join("..", "..", "..", "..", "db", "migrations")
	if _, err := migrate.Apply(ctx, pool, migrations); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE task_usage_reservations,
		runtime_capacity_reservations, runtime_pool_capacities, runtime_pool_tenant_grants, runtime_pools,
		provider_circuit_breakers, workflow_usage_ledgers, workflow_steps, workflows,
		model_calls, model_descriptors,
		tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		audit_events,
		tenant_consumption_windows, tenant_quotas,
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	return pool, postgresstore.New(pool)
}
