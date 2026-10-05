//go:build integration

// Load acceptance for v0.1 baseline item 2: a predefined task load keeps
// 1,000+ concurrent Tasks stable, and the run reports task durations, tool
// call ratio and budget accounting. The suite runs in its own PostgreSQL
// schema so it can truncate kernel tables without racing other integration
// packages.
package control_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	runtimecontrol "github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultLoadTasks   = 1000
	defaultLoadWorkers = 8
	toolCallFraction   = 0.30
)

// TestThousandConcurrentTasksStableUnderLoad is the v0.1 acceptance baseline
// item 2: with a predefined load of 1,000+ concurrent tasks the control plane
// and runtimes converge without loss, and the run reports task duration
// percentiles, the concurrent-running peak, and the tool call ratio.
func TestThousandConcurrentTasksStableUnderLoad(t *testing.T) {
	taskCount := loadTasks()
	if testing.Short() {
		t.Skip("load test disabled in -short mode")
	}
	ctx := context.Background()
	pool, store := newLoadDatabase(t)
	tenant := "tenant-a"

	// One immutable AgentVersion, one tool, and a fenced Tool Gateway serve
	// every task in the load.
	if _, err := store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: tenant, Namespace: "default", Name: "load-agent", Version: "1",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci"]},"lifecycle":{"maxAttempts":3}}`),
	}); err != nil {
		t.Fatalf("publish agent version: %v", err)
	}
	if _, err := store.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: tenant, Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
	}); err != nil {
		t.Fatalf("register tool descriptor: %v", err)
	}
	gatewayEngine, err := policy.New(policy.TenantPolicies{tenant: {
		MaxPriority: 100, AllowedTools: []string{"fs.read"}, ApprovalRequiredRisk: "high",
	}})
	if err != nil {
		t.Fatalf("gateway policy engine: %v", err)
	}
	gatewayAddress := serveLoadGateway(t, store, gatewayEngine, tenant)

	// Phase 1: submit the whole load before any controller or worker starts,
	// so the concurrent-running peak is bounded only by capacity.
	submissionStart := time.Now()
	submitLoad(t, ctx, store, tenant, taskCount)
	submissionDuration := time.Since(submissionStart)

	// Phase 2: run admission + scheduling reconciliation and the runtime
	// workers until every task is terminal.
	limits := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1_000_000, MaxCostMicroUSD: money.MustFromUSD(1_000), MaxToolCalls: 100_000,
		MaxWallSeconds: 86_400, MaxCPU: 64_000, MaxMemory: 262_144, MaxLLMConcurrency: 128,
	})
	pools := staticPools{{
		ID: "load-pool", TenantIDs: []string{tenant}, RuntimeClass: "oci", RuntimeInstanceID: "load-worker-1",
		Region: "cn-east", DataResidency: "cn", Ready: true,
		AvailableCPU: int64(taskCount) * 100, AvailableMemory: int64(taskCount) * 128, AvailableLLMSlots: taskCount,
	}}
	admissionController := admission.NewController(store, limits, testPolicyEngine(t), "load/admission", 50, time.Minute)
	// Every assignment is deliberately placed before any worker starts so the
	// test can prove a 1,000-task RUNNING peak. Under the race detector and
	// shared CI CPU, eight worker goroutines can take well over three minutes
	// to consume that artificial backlog. Keep the unclaimed assignment lease
	// beyond the test's ten-minute convergence window; once a worker claims an
	// assignment its normal 30-second heartbeat TTL takes over.
	if err := store.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register load fixture pools: %v", err)
	}
	schedulerController := scheduler.NewController(store, pools, "load/scheduler", 50, time.Minute, 15*time.Minute)

	var peakRunning atomic.Int64
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }
	defer closeStop()
	go reconcileLoop(t, ctx, admissionController, schedulerController, stop)
	go sampleRunningPeak(t, ctx, pool, stop, &peakRunning)

	// Phase 2a: drain the queue into RUNNING before any worker starts, so the
	// concurrent-running peak is exactly the submitted load.
	drainStart := time.Now()
	waitAllRunning(t, ctx, pool, taskCount)
	t.Logf("LOAD PHASE drain to full concurrency took %s", time.Since(drainStart).Round(time.Millisecond))

	// Phase 2b: run the runtime workers until every task is terminal.
	start := time.Now()
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		driveLoadWorkers(t, ctx, store, gatewayAddress, tenant, defaultLoadWorkers, stop)
	}()
	terminal := waitForTerminal(t, ctx, pool, taskCount, closeStop)
	select {
	case <-workersDone:
	case <-time.After(30 * time.Second):
		t.Fatalf("workers did not drain within 30s after convergence")
	}
	elapsed := time.Since(start)

	// Phase 3: assertions and the acceptance report.
	assertLoadOutcomes(t, ctx, pool, tenant, taskCount, terminal, peakRunning.Load())

	succeeded := countLoadByPhase(t, ctx, pool, tenant, "SUCCEEDED")
	var settledToolCalls int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(tool_calls), 0) FROM task_budget_settlements
		WHERE tenant_id = $1`, tenant).Scan(&settledToolCalls); err != nil {
		t.Fatalf("read settlements: %v", err)
	}
	var toolCalls int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tool_calls WHERE tenant_id = $1`, tenant).Scan(&toolCalls); err != nil {
		t.Fatalf("read tool calls: %v", err)
	}
	durations := loadDurations(t, ctx, pool, tenant)
	t.Logf("LOAD REPORT tasks=%d succeeded=%d failed=%d wall=%s submission=%s throughput=%.0f tasks/s",
		taskCount, succeeded, taskCount-succeeded, elapsed.Round(time.Millisecond),
		submissionDuration.Round(time.Millisecond), float64(taskCount)/elapsed.Seconds())
	t.Logf("LOAD REPORT concurrent-running peak=%d (target >= %d)", peakRunning.Load(), taskCount)
	t.Logf("LOAD REPORT task duration p50=%s p95=%s p99=%s max=%s",
		percentileLabel(durations, 0.50), percentileLabel(durations, 0.95),
		percentileLabel(durations, 0.99), percentileLabel(durations, 1.0))
	t.Logf("LOAD REPORT tool calls=%d ratio=%.1f%% settledToolCalls=%d",
		toolCalls, float64(toolCalls)/float64(taskCount)*100, settledToolCalls)
}

func submitLoad(t *testing.T, ctx context.Context, store *postgresstore.Store, tenant string, taskCount int) {
	t.Helper()
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
				`"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}}`
			if i%100 < int(toolCallFraction*100) {
				spec = `{"priority":70,"deadline":"2099-08-14T12:00:00Z",` +
					`"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},` +
					`"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},` +
					fmt.Sprintf(`"tools":[{"name":"fs.read","action":"read","resource":"fs:/tmp","args":{"path":"load-%d.txt"},"idempotencyKey":"load-tool-%d"}]}`, i, i)
			}
			if _, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
				ID: uuid.New(), TenantID: tenant, Namespace: "default", AgentVersionRef: "load-agent@1",
				Goal: "load task " + strconv.Itoa(i), IdempotencyKey: "load-" + strconv.Itoa(i), Spec: []byte(spec),
			}); err != nil {
				failures.Add(1)
				t.Errorf("submit task %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d submissions failed", failures.Load())
	}
}

// driveLoadWorkers runs one reference runtime worker per slot until the load
// converges; it returns when every task is terminal. It must run on a
// goroutine spawned by the test (which owns t.Fatalf).
func driveLoadWorkers(t *testing.T, ctx context.Context, store *postgresstore.Store, gatewayAddress, tenant string, workers int, stop <-chan struct{}) {
	t.Helper()
	artifacts, err := artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Errorf("create artifact store: %v", err)
		return
	}
	runtimeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Errorf("listen for Runtime Protocol: %v", err)
		return
	}
	runtimeServer := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	runtimev1.RegisterRuntimeControlServiceServer(runtimeServer,
		runtimecontrol.NewService(store, tenant, 2*time.Minute))
	go func() { _ = runtimeServer.Serve(runtimeListener) }()
	t.Cleanup(runtimeServer.Stop)
	runtimeConnection, err := grpc.NewClient(runtimeListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("connect to Runtime Protocol: %v", err)
		return
	}
	t.Cleanup(func() { _ = runtimeConnection.Close() })
	gatewayConnection, err := grpc.NewClient(gatewayAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("connect to Tool Gateway: %v", err)
		return
	}
	t.Cleanup(func() { _ = gatewayConnection.Close() })
	var wg sync.WaitGroup
	runTimes := make([]time.Duration, 0, workers*1024)
	var runTimesMu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := reference.NewWorker(runtimev1.NewRuntimeControlServiceClient(runtimeConnection), artifacts,
				tenant, fmt.Sprintf("load-worker-%d", w), 30*time.Second)
			worker.WithToolGateway(gatewayv1.NewToolGatewayServiceClient(gatewayConnection))
			for {
				select {
				case <-stop:
					return
				default:
				}
				runStart := time.Now()
				processed, err := worker.RunOnce(ctx)
				if processed {
					runTimesMu.Lock()
					runTimes = append(runTimes, time.Since(runStart))
					runTimesMu.Unlock()
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("load worker %d: %v", w, err)
				}
				if !processed {
					select {
					case <-stop:
						return
					case <-time.After(5 * time.Millisecond):
					}
				}
			}
		}(w)
	}
	wg.Wait()
	sort.Slice(runTimes, func(i, j int) bool { return runTimes[i] < runTimes[j] })
	if len(runTimes) > 0 {
		t.Logf("LOAD PHASE worker RunOnce p50=%s p95=%s p99=%s count=%d",
			runTimes[len(runTimes)/2].Round(time.Millisecond),
			runTimes[len(runTimes)*95/100].Round(time.Millisecond),
			runTimes[len(runTimes)*99/100].Round(time.Millisecond), len(runTimes))
	}
}

func reconcileLoop(t *testing.T, ctx context.Context, admissionController *admission.Controller, schedulerController *scheduler.Controller, stop <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if _, err := admissionController.Reconcile(ctx); err != nil {
				t.Errorf("admission reconcile: %v", err)
			}
			if _, err := schedulerController.Reconcile(ctx); err != nil {
				t.Errorf("scheduler reconcile: %v", err)
			}
		}
	}
}

func sampleRunningPeak(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stop <-chan struct{}, peak *atomic.Int64) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var running int64
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE phase = 'RUNNING'`).Scan(&running); err != nil {
				t.Errorf("sample running peak: %v", err)
				continue
			}
			for {
				current := peak.Load()
				if running <= current || peak.CompareAndSwap(current, running) {
					break
				}
			}
		}
	}
}

// waitAllRunning blocks until every submitted task has been admitted and
// placed (RUNNING). No worker is running yet, so nothing can complete and the
// concurrent-running peak necessarily reaches the full load.
func waitAllRunning(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskCount int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		var running int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE phase = 'RUNNING'`).Scan(&running); err != nil {
			t.Fatalf("count running tasks: %v", err)
		}
		if running == taskCount {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("load did not reach full concurrency: running=%d of %d", running, taskCount)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForTerminal(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskCount int, closeStop func()) int64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Minute)
	for {
		var terminal int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE phase IN
			('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'REJECTED')`).Scan(&terminal); err != nil {
			t.Fatalf("count terminal tasks: %v", err)
		}
		if terminal == int64(taskCount) {
			closeStop()
			return terminal
		}
		if time.Now().After(deadline) {
			closeStop()
			t.Fatalf("load did not converge: terminal=%d of %d", terminal, taskCount)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func assertLoadOutcomes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant string, taskCount int, terminal, peak int64) {
	t.Helper()
	var succeeded, failed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE phase = 'SUCCEEDED'),
		count(*) FILTER (WHERE phase = 'FAILED') FROM tasks WHERE tenant_id = $1`, tenant).Scan(&succeeded, &failed); err != nil {
		t.Fatalf("count outcomes: %v", err)
	}
	if terminal != int64(taskCount) || succeeded != taskCount || failed != 0 {
		t.Fatalf("load outcomes: terminal=%d succeeded=%d failed=%d of %d", terminal, succeeded, failed, taskCount)
	}
	if peak < int64(taskCount) {
		t.Fatalf("concurrent-running peak = %d, want >= %d (baseline item 2)", peak, taskCount)
	}
}

func loadDurations(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant string) []time.Duration {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT EXTRACT(EPOCH FROM (updated_at - created_at)) FROM tasks
		WHERE tenant_id = $1 AND phase = 'SUCCEEDED'`, tenant)
	if err != nil {
		t.Fatalf("query durations: %v", err)
	}
	defer rows.Close()
	var durations []time.Duration
	for rows.Next() {
		var seconds float64
		if err := rows.Scan(&seconds); err != nil {
			t.Fatalf("scan duration: %v", err)
		}
		durations = append(durations, time.Duration(seconds*float64(time.Second)))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return durations
}

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

func countLoadByPhase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant, phase string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE tenant_id = $1 AND phase = $2`, tenant, phase).Scan(&count); err != nil {
		t.Fatalf("count phase %s: %v", phase, err)
	}
	return count
}

func loadTasks() int {
	if value := os.Getenv("FENCED_LOAD_TASKS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultLoadTasks
}

func serveLoadGateway(t *testing.T, store *postgresstore.Store, engine *policy.Engine, tenant string) string {
	t.Helper()
	decisionGateway := tool.NewGateway(engine, store, store, store,
		&gateway.DevExecutor{MaxOutputBytes: 1 << 20}, &gateway.DevSecretBroker{})
	service := gateway.NewService(decisionGateway, tenant)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Tool Gateway: %v", err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	gatewayv1.RegisterToolGatewayServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func newLoadDatabase(t *testing.T) (*pgxpool.Pool, *postgresstore.Store) {
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
	if _, err := admin.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS fenced_load`); err != nil {
		admin.Close(ctx)
		t.Fatalf("create load schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = "fenced_load"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	migrations := filepath.Join("..", "..", "..", "db", "migrations")
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
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	return pool, postgresstore.New(pool)
}
