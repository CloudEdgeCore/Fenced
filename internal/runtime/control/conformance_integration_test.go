//go:build integration

// Package control_test verifies the fenced Runtime Protocol boundary against a
// real PostgreSQL and, when the Wasmtime provider is built, the v0.1
// acceptance baseline item "the same AgentVersion runs on at least two
// different Runtime Providers".
package control_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelmodel "github.com/CloudEdgeCore/Fenced/internal/kernel/model"
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
	conformanceTenant   = "tenant-a"
	conformanceVersion  = "conformance@1"
	referenceProvider   = "reference-go"
	referenceABI        = "fenced.reference/v1"
	referenceSchema     = "fenced.reference-state/v1"
	wasmtimeProvider    = "wasmtime"
	wasmtimeABI         = "fenced.wasm-component/v1"
	wasmtimeSchema      = "fenced.wasm-logical-state/v1"
	ociProvider         = "oci-gvisor"
	ociABI              = "fenced.oci/v1"
	ociSchema           = "fenced.oci-logical/v1"
	terminalWaitTimeout = 60 * time.Second
)

// lockedBuffer keeps subprocess diagnostics safe to inspect while the child
// is still running. Timeout paths intentionally report stderr before test
// cleanup terminates the process.
type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *lockedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

// scenario describes one provider leg of the conformance run.
type scenario struct {
	key          string
	runtimeClass string
	instanceID   string
	spec         string
	provider     string
	runtimeABI   string
	checkpoint   string
}

type conformanceEnv struct {
	pool       *pgxpool.Pool
	store      *postgresstore.Store
	grpcAddr   string
	tenant     string
	versionRef string
	versionID  uuid.UUID
	taskID     uuid.UUID
}

func TestSameAgentVersionRunsOnBothProviders(t *testing.T) {
	env := newConformanceEnv(t)

	t.Run("reference-go", func(t *testing.T) {
		scenario := scenario{
			key: "conformance-reference", runtimeClass: "oci", instanceID: "worker-reference-1",
			spec:     `{"priority":70,"deadline":"2099-08-14T12:00:00Z","budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":8388608,"llmConcurrency":1}}`,
			provider: referenceProvider, runtimeABI: referenceABI, checkpoint: referenceSchema,
		}
		env.prepareScenario(t, scenario)
		env.driveReferenceWorker(t)
		env.assertConformance(t, scenario)
	})

	t.Run("wasmtime", func(t *testing.T) {
		scenario := scenario{
			key: "conformance-wasmtime", runtimeClass: "wasmtime", instanceID: "worker-wasmtime-1",
			spec:     `{"priority":70,"deadline":"2099-08-14T12:00:00Z","budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},"placement":{"runtimeClasses":["wasmtime"],"preferredClass":"wasmtime","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},"runtime":{"componentPath":"agent.wasm"}}`,
			provider: wasmtimeProvider, runtimeABI: wasmtimeABI, checkpoint: wasmtimeSchema,
		}
		env.prepareScenario(t, scenario)
		env.driveWasmtimeWorker(t)
		env.assertConformance(t, scenario)
	})

	// The OCI/gVisor leg runs only on a host with containerd + runsc: point
	// FENCED_OCI_CONTAINERD_NAMESPACE (and optionally FENCED_OCI_IMAGE,
	// FENCED_OCI_RUNTIME, and FENCED_OCI_RUNTIME_CONFIG) at the sandbox host.
	// CI hosts without the sandbox skip the leg; the provider binary itself is
	// built and unit-tested in CI regardless.
	t.Run("oci", func(t *testing.T) {
		namespace := os.Getenv("FENCED_OCI_CONTAINERD_NAMESPACE")
		if namespace == "" {
			t.Skip("FENCED_OCI_CONTAINERD_NAMESPACE is not set (requires a containerd + runsc host)")
		}
		scenario := scenario{
			key: "conformance-oci", runtimeClass: "oci", instanceID: "worker-oci-1",
			spec:     `{"priority":70,"deadline":"2099-08-14T12:00:00Z","budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":8388608,"llmConcurrency":1},"runtime":{"command":["/bin/sleep","1"]}}`,
			provider: ociProvider, runtimeABI: ociABI, checkpoint: ociSchema,
		}
		env.prepareScenario(t, scenario)
		env.driveOCIWorker(t, namespace)
		env.assertConformance(t, scenario)
	})
}

// prepareScenario resets the kernel tables, publishes the shared immutable
// AgentVersion, submits the task for this provider's runtime class, and runs
// admission plus scheduling. Each provider leg therefore observes the exact
// same published version.
func (env *conformanceEnv) prepareScenario(t *testing.T, scenario scenario) {
	t.Helper()
	ctx := context.Background()
	if _, err := env.pool.Exec(ctx, `TRUNCATE TABLE task_usage_reservations,
		runtime_capacity_reservations, runtime_pool_capacities, runtime_pool_tenant_grants, runtime_pools,
		provider_circuit_breakers, workflow_usage_ledgers, workflow_steps, workflows,
		model_calls, model_descriptors, tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	published, err := env.store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: env.tenant, Namespace: "default",
		Name: "conformance", Version: "1",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci","wasmtime"],"preferred":"oci"},"lifecycle":{"maxAttempts":3}}`),
	})
	if err != nil {
		t.Fatalf("publish conformance agent version: %v", err)
	}
	env.versionID = published.AgentVersion.ID
	created, err := env.store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: env.tenant, Namespace: "default",
		AgentVersionRef: env.versionRef, Goal: "conformance goal " + scenario.key,
		IdempotencyKey: scenario.key, Spec: []byte(scenario.spec),
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	env.taskID = created.Task.ID
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci", "wasmtime"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
		MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	admissionController := admission.NewController(env.store, engine, testPolicyEngine(t), "admission-"+scenario.key, 10, time.Minute)
	if count, err := admissionController.Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("admission reconcile count=%d err=%v", count, err)
	}
	pools := staticPools{{
		ID: "pool-" + scenario.runtimeClass, TenantIDs: []string{env.tenant},
		RuntimeClass: scenario.runtimeClass, RuntimeInstanceID: scenario.instanceID,
		Region: "cn-east", DataResidency: "cn", Ready: true,
		AvailableCPU: 2000, AvailableMemory: 4096, AvailableLLMSlots: 4,
	}}
	if err := env.store.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register fixture pools: %v", err)
	}
	schedulerController := scheduler.NewController(env.store, pools, "scheduler-"+scenario.key, 10, time.Minute, 30*time.Second)
	if count, err := schedulerController.Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("scheduler reconcile count=%d err=%v", count, err)
	}
	task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
	if err != nil || task.Phase != domain.TaskRunning {
		t.Fatalf("task was not scheduled: %+v err=%v", task, err)
	}
	if task.AgentVersionID == nil || *task.AgentVersionID != env.versionID {
		t.Fatalf("task is not bound to the published version: %+v", task)
	}
}

// driveReferenceWorker runs the deterministic Go provider in-process until
// the task reaches a terminal phase.
func (env *conformanceEnv) driveReferenceWorker(t *testing.T) {
	t.Helper()
	artifacts, err := artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatalf("create artifact store: %v", err)
	}
	connection, err := grpc.NewClient(env.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Runtime Protocol: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	worker := reference.NewWorker(
		runtimev1.NewRuntimeControlServiceClient(connection),
		artifacts, env.tenant, "worker-reference-1", 30*time.Second,
	)
	ctx := context.Background()
	deadline := time.Now().Add(terminalWaitTimeout)
	for {
		task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
		if err != nil {
			t.Fatalf("read task: %v", err)
		}
		if task.Phase.Terminal() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reference provider did not finish: phase=%s", task.Phase)
		}
		done, err := worker.RunOnce(ctx)
		if err != nil {
			t.Fatalf("reference worker: %v", err)
		}
		if done {
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// driveWasmtimeWorker spawns the separate Wasmtime provider process. The
// binary and its package root are supplied through the environment so that
// CI can build them in the same job; locally they are built once with the
// pinned toolchain.
func (env *conformanceEnv) driveWasmtimeWorker(t *testing.T) {
	t.Helper()
	runtimeBinary := os.Getenv("FENCED_WASMTIME_RUNTIME")
	packageRoot := os.Getenv("FENCED_WASMTIME_PACKAGE_ROOT")
	if runtimeBinary == "" || packageRoot == "" {
		t.Skip("FENCED_WASMTIME_RUNTIME and FENCED_WASMTIME_PACKAGE_ROOT are not set")
	}
	if _, err := os.Stat(filepath.Join(packageRoot, "agent.wasm")); err != nil {
		t.Skipf("package root has no agent.wasm component: %v", err)
	}
	command := exec.Command(runtimeBinary,
		"--dev-mode", "true",
		"--tenant", env.tenant,
		"--runtime-instance-id", "worker-wasmtime-1",
		"--control-endpoint", env.grpcAddr,
		"--package-root", packageRoot,
		"--artifact-root", t.TempDir(),
	)
	var stderr lockedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start Wasmtime provider: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		// The wait loop may have already consumed the exit value; only wait
		// when the process has not been observed exiting.
		select {
		case <-exited:
		default:
		}
	})

	ctx := context.Background()
	deadline := time.Now().Add(terminalWaitTimeout)
	for {
		select {
		case err := <-exited:
			t.Fatalf("Wasmtime provider exited early: %v\nstderr:\n%s", err, stderr.String())
		default:
		}
		task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
		if err != nil {
			t.Fatalf("read task: %v", err)
		}
		if task.Phase.Terminal() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Wasmtime provider did not finish: phase=%s\nstderr:\n%s", task.Phase, stderr.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// driveOCIWorker spawns the OCI/gVisor provider process against a
// containerd + runsc host. The image reference defaults to the same immutable,
// shellable Alpine image used by CI.
func (env *conformanceEnv) driveOCIWorker(t *testing.T, namespace string) {
	t.Helper()
	imageRef := os.Getenv("FENCED_OCI_IMAGE")
	if imageRef == "" {
		imageRef = "docker.io/library/alpine@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b"
	}
	runtimeName := os.Getenv("FENCED_OCI_RUNTIME")
	if runtimeName == "" {
		runtimeName = "io.containerd.runsc.v1"
	}
	runtimeConfig := os.Getenv("FENCED_OCI_RUNTIME_CONFIG")
	// Snapshotter for sandbox rootfs (v0.7): nested environments (containerd
	// inside a container) cannot mount overlay-on-overlay and must set
	// FENCED_OCI_SNAPSHOTTER=native.
	snapshotter := os.Getenv("FENCED_OCI_SNAPSHOTTER")
	binaryPath, err := exec.LookPath("fenced-runtime-oci")
	if err != nil {
		t.Skipf("fenced-runtime-oci binary not found in PATH (build cmd/fenced-runtime-oci first): %v", err)
	}
	commandArgs := []string{
		"--dev-mode=true",
		"--tenant", env.tenant,
		"--runtime-instance-id", "worker-oci-1",
		"--control-address", env.grpcAddr,
		"--artifact-root", t.TempDir(),
		"--image-ref", imageRef,
		"--containerd-namespace", namespace,
		"--containerd-runtime", runtimeName,
		"--skip-image-pull",
		"--heartbeat-ttl", "30s",
	}
	if runtimeConfig != "" {
		commandArgs = append(commandArgs, "--runtime-config-path", runtimeConfig)
	}
	if snapshotter != "" {
		commandArgs = append(commandArgs, "--containerd-snapshotter", snapshotter)
	}
	command := exec.Command(binaryPath, commandArgs...)
	var stderr lockedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start OCI provider: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		select {
		case <-exited:
		default:
		}
	})

	ctx := context.Background()
	deadline := time.Now().Add(terminalWaitTimeout)
	for {
		select {
		case err := <-exited:
			t.Fatalf("OCI provider exited early: %v\nstderr:\n%s", err, stderr.String())
		default:
		}
		task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
		if err != nil {
			t.Fatalf("read task: %v", err)
		}
		if task.Phase.Terminal() {
			return
		}
		if time.Now().After(deadline) {
			var attemptPhase, failureCode, failureMessage string
			diagnosticErr := env.pool.QueryRow(ctx, `SELECT a.phase, COALESCE(a.failure_code, ''), COALESCE(a.failure_message, '')
				FROM attempts a JOIN runs r ON r.tenant_id = a.tenant_id AND r.id = a.run_id
				WHERE a.tenant_id = $1 AND r.task_id = $2 ORDER BY a.ordinal DESC LIMIT 1`,
				env.tenant, env.taskID).Scan(&attemptPhase, &failureCode, &failureMessage)
			t.Fatalf("OCI provider did not finish: phase=%s attempt=%s failureCode=%s failureMessage=%s diagnosticErr=%v\nstderr:\n%s",
				task.Phase, attemptPhase, failureCode, failureMessage, diagnosticErr, stderr.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertConformance verifies the kernel-visible invariants that both
// providers must satisfy identically for the same published AgentVersion.
func (env *conformanceEnv) assertConformance(t *testing.T, scenario scenario) {
	t.Helper()
	ctx := context.Background()

	task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
	if err != nil {
		t.Fatalf("read final task: %v", err)
	}
	if task.Phase != domain.TaskSucceeded {
		t.Fatalf("task phase = %s, want SUCCEEDED", task.Phase)
	}
	if task.ResultRef == "" {
		t.Fatal("task has no durable result reference")
	}
	if task.AgentVersionID == nil || *task.AgentVersionID != env.versionID {
		t.Fatalf("task version binding = %v, want %s", task.AgentVersionID, env.versionID)
	}

	var runPhase, attemptPhase string
	if err := env.pool.QueryRow(ctx, `SELECT phase FROM runs WHERE task_id = $1`, env.taskID.String()).Scan(&runPhase); err != nil {
		t.Fatalf("read run: %v", err)
	}
	if err := env.pool.QueryRow(ctx, `SELECT a.phase FROM attempts a JOIN runs r ON r.id = a.run_id
		WHERE r.task_id = $1`, env.taskID.String()).Scan(&attemptPhase); err != nil {
		t.Fatalf("read attempt: %v", err)
	}
	if runPhase != string(domain.RunCompleted) || attemptPhase != string(domain.AttemptCompleted) {
		t.Fatalf("lifecycle did not converge: run=%s attempt=%s", runPhase, attemptPhase)
	}

	var checkpointCount int
	var checkpointProvider, checkpointABI, checkpointSchema string
	var checkpointVersionID *string
	if err := env.pool.QueryRow(ctx, `SELECT count(*), min(provider), min(runtime_abi), min(schema_version), min(agent_version_id::text)
		FROM checkpoints c JOIN runs r ON r.id = c.run_id WHERE r.task_id = $1`,
		env.taskID.String()).Scan(&checkpointCount, &checkpointProvider, &checkpointABI, &checkpointSchema, &checkpointVersionID); err != nil {
		t.Fatalf("read checkpoints: %v", err)
	}
	if checkpointCount != 1 || checkpointProvider != scenario.provider ||
		checkpointABI != scenario.runtimeABI || checkpointSchema != scenario.checkpoint {
		t.Fatalf("unexpected checkpoint: count=%d provider=%s abi=%s schema=%s", checkpointCount, checkpointProvider, checkpointABI, checkpointSchema)
	}
	if checkpointVersionID == nil || *checkpointVersionID != env.versionID.String() {
		t.Fatalf("checkpoint version binding = %v, want %s", checkpointVersionID, env.versionID)
	}

	var artifactCount int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM artifacts WHERE tenant_id = $1`, env.tenant).Scan(&artifactCount); err != nil {
		t.Fatalf("read artifacts: %v", err)
	}
	if artifactCount < 2 {
		t.Fatalf("artifacts = %d, want at least checkpoint state and result", artifactCount)
	}

	var succeededEvents int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = $1 AND aggregate_type = 'Task' AND aggregate_id = $2 AND event_type = 'TaskSucceeded'`,
		env.tenant, env.taskID.String()).Scan(&succeededEvents); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if succeededEvents != 1 {
		t.Fatalf("TaskSucceeded outbox events = %d, want 1", succeededEvents)
	}
}

func newConformanceEnv(t *testing.T) *conformanceEnv {
	t.Helper()
	url := os.Getenv("FENCED_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The conformance suite runs in its own PostgreSQL schema so that it can
	// truncate kernel tables without racing the store package's integration
	// tests, which share the same database.
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS fenced_conformance`); err != nil {
		admin.Close(ctx)
		t.Fatalf("create conformance schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = "fenced_conformance"
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
		model_calls, model_descriptors, tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	repository := postgresstore.New(pool)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Runtime Protocol: %v", err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	runtimev1.RegisterRuntimeControlServiceServer(server,
		runtimecontrol.NewService(repository, conformanceTenant, 2*time.Minute))
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	return &conformanceEnv{
		pool: pool, store: repository, grpcAddr: listener.Addr().String(),
		tenant: conformanceTenant, versionRef: conformanceVersion,
	}
}

// TestTaskToolCallFlowThroughGateway drives the full v0.1 tool chain end to
// end: Task → admission → scheduling → reference runtime → fenced Tool
// Gateway (policy, budget settlement, side-effect receipt) → checkpoint with
// confirmed receipts → completion. This is the acceptance evidence for
// baseline items 5 (budget hard-stop execution point) and 6 (tool call with
// complete decision, usage and receipt).
func TestTaskToolCallFlowThroughGateway(t *testing.T) {
	env := newConformanceEnv(t)
	ctx := context.Background()
	scenario := scenario{
		key: "tool-gateway-e2e", runtimeClass: "oci", instanceID: "worker-tool-1",
		spec: `{"priority":70,"deadline":"2099-08-14T12:00:00Z",` +
			`"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},` +
			`"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},` +
			`"tools":[{"name":"fs.read","action":"read","resource":"fs:/tmp","args":{"path":"a.txt"},"idempotencyKey":"e2e-tool-1"}]}`,
		provider: referenceProvider, runtimeABI: referenceABI, checkpoint: referenceSchema,
	}
	env.prepareScenario(t, scenario)

	if _, err := env.store.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: env.tenant, Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
	}); err != nil {
		t.Fatalf("register tool descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{env.tenant: {
		MaxPriority: 100, AllowedTools: []string{"fs.read"}, ApprovalRequiredRisk: "high",
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	gwService := gateway.NewService(tool.NewGateway(engine, env.store, env.store, env.store,
		&gateway.DevExecutor{MaxOutputBytes: 1 << 20}, &gateway.DevSecretBroker{}), env.tenant)
	gwListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Tool Gateway: %v", err)
	}
	gwServer := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	gatewayv1.RegisterToolGatewayServiceServer(gwServer, gwService)
	go func() { _ = gwServer.Serve(gwListener) }()
	t.Cleanup(gwServer.Stop)

	artifacts, err := artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatalf("create artifact store: %v", err)
	}
	connection, err := grpc.NewClient(env.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Runtime Protocol: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	gwConnection, err := grpc.NewClient(gwListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Tool Gateway: %v", err)
	}
	t.Cleanup(func() { _ = gwConnection.Close() })
	worker := reference.NewWorker(
		runtimev1.NewRuntimeControlServiceClient(connection),
		artifacts, env.tenant, "worker-tool-1", 30*time.Second,
	)
	worker.WithToolGateway(gatewayv1.NewToolGatewayServiceClient(gwConnection))

	deadline := time.Now().Add(terminalWaitTimeout)
	for {
		task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
		if err != nil {
			t.Fatalf("read task: %v", err)
		}
		if task.Phase.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task did not finish: phase=%s", task.Phase)
		}
		if _, err := worker.RunOnce(ctx); err != nil {
			t.Fatalf("reference worker: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
	if err != nil || task.Phase != domain.TaskSucceeded {
		t.Fatalf("task phase = %s err=%v, want SUCCEEDED", task.Phase, err)
	}

	var callStatus, callRevision string
	var callCount int
	if err := env.pool.QueryRow(ctx, `SELECT count(*), min(status), min(policy_revision) FROM tool_calls
		WHERE tenant_id = $1 AND task_id = $2`, env.tenant, env.taskID.String()).
		Scan(&callCount, &callStatus, &callRevision); err != nil {
		t.Fatalf("read tool calls: %v", err)
	}
	if callCount != 1 || callStatus != string(kernelstore.ToolCallExecuted) || callRevision == "" {
		t.Fatalf("tool call ledger: count=%d status=%s revision=%s", callCount, callStatus, callRevision)
	}

	var receiptCount int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_operation_receipts r
		JOIN attempts a ON a.id = r.attempt_id AND a.tenant_id = r.tenant_id
		JOIN runs rn ON rn.id = a.run_id
		WHERE r.tenant_id = $1 AND rn.task_id = $2 AND r.operation = 'TOOL:fs.read@1.0.0'`,
		env.tenant, env.taskID.String()).Scan(&receiptCount); err != nil {
		t.Fatalf("read tool receipt: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("tool receipts = %d, want 1", receiptCount)
	}

	var settledToolCalls int64
	if err := env.pool.QueryRow(ctx, `SELECT COALESCE(SUM(tool_calls), 0) FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2`, env.tenant, env.taskID.String()).Scan(&settledToolCalls); err != nil {
		t.Fatalf("read budget settlements: %v", err)
	}
	if settledToolCalls != 1 {
		t.Fatalf("settled tool calls = %d, want 1", settledToolCalls)
	}

	var confirmedReceipts []string
	if err := env.pool.QueryRow(ctx, `SELECT c.confirmed_receipt_ids FROM checkpoints c
		JOIN runs r ON r.id = c.run_id WHERE r.task_id = $1`, env.taskID.String()).Scan(&confirmedReceipts); err != nil {
		t.Fatalf("read checkpoint receipts: %v", err)
	}
	if !slices.Contains(confirmedReceipts, "TOOL:fs.read@1.0.0") {
		t.Fatalf("checkpoint does not confirm the tool receipt: %v", confirmedReceipts)
	}
}

// TestTaskToolApprovalResumeThroughGateway proves the human-in-the-loop
// approval flow end to end: a high-risk tool call parks the attempt in
// WAITING_APPROVAL, the worker keeps the lease alive, a human decision
// through the kernel store resumes execution, and the run completes with the
// receipt confirmed in the checkpoint.
func TestTaskToolApprovalResumeThroughGateway(t *testing.T) {
	env := newConformanceEnv(t)
	ctx := context.Background()
	scenario := scenario{
		key: "tool-approval-e2e", runtimeClass: "oci", instanceID: "worker-approval-1",
		spec: `{"priority":70,"deadline":"2099-08-14T12:00:00Z",` +
			`"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},` +
			`"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},` +
			`"tools":[{"name":"fs.write","action":"write","resource":"fs:/tmp","args":{"path":"a.txt","content":"x"},"idempotencyKey":"approval-tool-1"}]}`,
		provider: referenceProvider, runtimeABI: referenceABI, checkpoint: referenceSchema,
	}
	env.prepareScenario(t, scenario)

	if _, err := env.store.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: env.tenant, Name: "fs.write", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskHigh,
		Actions: []string{"write"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}`),
	}); err != nil {
		t.Fatalf("register tool descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{env.tenant: {
		MaxPriority: 100, AllowedTools: []string{"fs.write"}, ApprovalRequiredRisk: "high",
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	worker := wireWorkerWithGateway(t, env, "worker-approval-1", tool.NewGateway(engine, env.store, env.store, env.store,
		&gateway.DevExecutor{MaxOutputBytes: 1 << 20}, &gateway.DevSecretBroker{}))

	// Phase 1: the high-risk tool parks the attempt waiting for a human.
	driveUntil(t, env, func(task kernelstore.Task) bool {
		return task.Phase.Terminal() || attemptPhase(t, env, task.ID) == "WAITING_APPROVAL"
	}, worker)
	task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
	if err != nil || task.Phase.Terminal() {
		t.Fatalf("task must park before terminal: phase=%s err=%v", task.Phase, err)
	}

	var callStatus, approvalStatus string
	var settlementCount int
	if err := env.pool.QueryRow(ctx, `SELECT c.status, a.status FROM tool_calls c
		JOIN tool_approvals a ON a.call_id = c.id
		WHERE c.task_id = $1`, env.taskID.String()).Scan(&callStatus, &approvalStatus); err != nil {
		t.Fatalf("read parked tool state: %v", err)
	}
	if callStatus != string(kernelstore.ToolCallRequiresApproval) || approvalStatus != string(kernelstore.ToolApprovalPending) {
		t.Fatalf("parked state: call=%s approval=%s", callStatus, approvalStatus)
	}
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM task_budget_settlements WHERE task_id = $1`,
		env.taskID.String()).Scan(&settlementCount); err != nil {
		t.Fatalf("read settlements: %v", err)
	}
	if settlementCount != 0 {
		t.Fatalf("settlements before approval = %d, want 0", settlementCount)
	}

	// Phase 2: a human decides through the kernel store.
	var approvalID uuid.UUID
	if err := env.pool.QueryRow(ctx, `SELECT a.id FROM tool_approvals a JOIN tool_calls c ON c.id = a.call_id
		WHERE c.task_id = $1`, env.taskID.String()).Scan(&approvalID); err != nil {
		t.Fatalf("read approval ID: %v", err)
	}
	decided, err := env.store.DecideToolApproval(ctx, kernelstore.DecideToolApprovalInput{
		TenantID: env.tenant, ApprovalID: approvalID, ExpectedVersion: 1,
		Decision: kernelstore.ToolApprovalApproved, DecidedBy: "human-1", Now: time.Now().UTC(),
	})
	if err != nil || decided.Status != kernelstore.ToolApprovalApproved {
		t.Fatalf("decide approval: %v status=%s", err, decided.Status)
	}

	// Phase 3: the worker resumes and the task completes.
	driveUntil(t, env, func(task kernelstore.Task) bool { return task.Phase.Terminal() }, worker)
	task, err = env.store.GetTask(ctx, env.tenant, env.taskID)
	if err != nil || task.Phase != domain.TaskSucceeded {
		t.Fatalf("task phase = %s err=%v, want SUCCEEDED", task.Phase, err)
	}

	var executedCalls, receipts int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM tool_calls WHERE task_id = $1 AND status = 'EXECUTED'`,
		env.taskID.String()).Scan(&executedCalls); err != nil {
		t.Fatalf("read executed calls: %v", err)
	}
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_operation_receipts r
		JOIN attempts a ON a.id = r.attempt_id JOIN runs rn ON rn.id = a.run_id
		WHERE rn.task_id = $1 AND r.operation = 'TOOL:fs.write@1.0.0'`, env.taskID.String()).Scan(&receipts); err != nil {
		t.Fatalf("read receipts: %v", err)
	}
	var settledToolCalls int64
	if err := env.pool.QueryRow(ctx, `SELECT COALESCE(SUM(tool_calls), 0) FROM task_budget_settlements WHERE task_id = $1`,
		env.taskID.String()).Scan(&settledToolCalls); err != nil {
		t.Fatalf("read settlements: %v", err)
	}
	if executedCalls != 1 || receipts != 1 || settledToolCalls != 1 {
		t.Fatalf("resumed execution: calls=%d receipts=%d settled=%d, want 1/1/1", executedCalls, receipts, settledToolCalls)
	}
}

// wireWorkerWithGateway builds a reference worker connected to an in-process
// fenced Tool Gateway backed by the conformance store.
func wireWorkerWithGateway(t *testing.T, env *conformanceEnv, instanceID string, decisionGateway *tool.Gateway) *reference.Worker {
	t.Helper()
	gatewayService := gateway.NewService(decisionGateway, env.tenant)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Tool Gateway: %v", err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	gatewayv1.RegisterToolGatewayServiceServer(server, gatewayService)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(env.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Runtime Protocol: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	gwConnection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Tool Gateway: %v", err)
	}
	t.Cleanup(func() { _ = gwConnection.Close() })
	artifacts, err := artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatalf("create artifact store: %v", err)
	}
	worker := reference.NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifacts,
		env.tenant, instanceID, 30*time.Second)
	return worker.WithToolGateway(gatewayv1.NewToolGatewayServiceClient(gwConnection))
}

// driveUntil runs the worker until the predicate holds or the deadline passes.
func driveUntil(t *testing.T, env *conformanceEnv, predicate func(kernelstore.Task) bool, worker *reference.Worker) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(terminalWaitTimeout)
	for {
		task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
		if err != nil {
			t.Fatalf("read task: %v", err)
		}
		if predicate(task) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not reach the expected state: phase=%s", task.Phase)
		}
		if _, err := worker.RunOnce(ctx); err != nil {
			t.Fatalf("reference worker: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func attemptPhase(t *testing.T, env *conformanceEnv, taskID uuid.UUID) string {
	t.Helper()
	var phase string
	if err := env.pool.QueryRow(context.Background(), `SELECT a.phase FROM attempts a
		JOIN runs r ON r.id = a.run_id WHERE r.task_id = $1 ORDER BY a.created_at DESC LIMIT 1`,
		taskID.String()).Scan(&phase); err != nil {
		t.Fatalf("read attempt phase: %v", err)
	}
	return phase
}

type staticPools []scheduler.RuntimePool

func (s staticPools) ListRuntimePools(context.Context, string) ([]scheduler.RuntimePool, error) {
	return s, nil
}

func testPolicyEngine(t *testing.T) *policy.Engine {
	t.Helper()
	engine, err := policy.New(policy.TenantPolicies{conformanceTenant: {MaxPriority: 100}})
	if err != nil {
		t.Fatalf("prepare test policy engine: %v", err)
	}
	return engine
}

// TestTaskModelCallFlowThroughGateway proves the Model Gateway decision chain
// end to end through the reference worker: a workload spec with modelCalls
// runs Begin/Finish through the fenced gRPC boundary, the pre-declared usage
// is settled exactly once against the task budget, the call ledger reaches
// COMPLETED with cost computed from the pinned price table, the audit receipt
// records only metadata, and the model results ride in the task result
// document.
func TestTaskModelCallFlowThroughGateway(t *testing.T) {
	env := newConformanceEnv(t)
	ctx := context.Background()
	scenario := scenario{
		key: "model-gateway-e2e", runtimeClass: "oci", instanceID: "worker-model-1",
		spec: `{"priority":70,"deadline":"2099-08-14T12:00:00Z",` +
			`"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},` +
			`"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},` +
			`"modelCalls":[{"modelRef":"openai/gpt-4o","inputTokens":150,"outputTokens":50,"idempotencyKey":"e2e-model-1"}]}`,
		provider: referenceProvider, runtimeABI: referenceABI, checkpoint: referenceSchema,
	}
	env.prepareScenario(t, scenario)

	if _, err := env.store.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: env.tenant, Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(5), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v1",
	}); err != nil {
		t.Fatalf("register model descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{env.tenant: {
		MaxPriority: 100, AllowedModels: []string{"openai/gpt-4o"},
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	modelService := gateway.NewModelService(kernelmodel.NewGateway(engine, env.store, env.store, env.store), env.tenant)
	modelListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Model Gateway: %v", err)
	}
	modelServer := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	modelv1.RegisterModelGatewayServiceServer(modelServer, modelService)
	go func() { _ = modelServer.Serve(modelListener) }()
	t.Cleanup(modelServer.Stop)

	artifacts, err := artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatalf("create artifact store: %v", err)
	}
	connection, err := grpc.NewClient(env.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Runtime Protocol: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	modelConnection, err := grpc.NewClient(modelListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Model Gateway: %v", err)
	}
	t.Cleanup(func() { _ = modelConnection.Close() })
	worker := reference.NewWorker(
		runtimev1.NewRuntimeControlServiceClient(connection),
		artifacts, env.tenant, "worker-model-1", 30*time.Second,
	)
	worker.WithModelGateway(modelv1.NewModelGatewayServiceClient(modelConnection))

	driveUntil(t, env, func(task kernelstore.Task) bool { return task.Phase.Terminal() }, worker)
	task, err := env.store.GetTask(ctx, env.tenant, env.taskID)
	if err != nil || task.Phase != domain.TaskSucceeded {
		t.Fatalf("task phase = %s err=%v, want SUCCEEDED", task.Phase, err)
	}

	var callStatus string
	var callInput, callOutput int64
	var callCost int64
	if err := env.pool.QueryRow(ctx, `SELECT status, input_tokens, output_tokens, cost_micro_usd FROM model_calls
		WHERE tenant_id = $1 AND task_id = $2`, env.tenant, env.taskID.String()).
		Scan(&callStatus, &callInput, &callOutput, &callCost); err != nil {
		t.Fatalf("read model call: %v", err)
	}
	if callStatus != string(kernelstore.ModelCallCompleted) || callInput != 150 || callOutput != 50 {
		t.Fatalf("model call ledger: status=%s input=%d output=%d", callStatus, callInput, callOutput)
	}
	if want := int64(money.MustFromUSD(150.0/1e6*5 + 50.0/1e6*15)); callCost != want {
		t.Fatalf("model call cost = %v, want %v (computed from the pinned price table)", callCost, want)
	}

	var settledTokens int64
	if err := env.pool.QueryRow(ctx, `SELECT COALESCE(SUM(tokens), 0) FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2`, env.tenant, env.taskID.String()).Scan(&settledTokens); err != nil {
		t.Fatalf("read budget settlements: %v", err)
	}
	if settledTokens != 200 {
		t.Fatalf("settled tokens = %d, want exactly 200 (pre-declared usage charged once)", settledTokens)
	}

	var receiptCount int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM runtime_operation_receipts r
		JOIN attempts a ON a.id = r.attempt_id AND a.tenant_id = r.tenant_id
		JOIN runs rn ON rn.id = a.run_id
		WHERE r.tenant_id = $1 AND rn.task_id = $2 AND r.operation = 'MODEL:openai/gpt-4o'`,
		env.tenant, env.taskID.String()).Scan(&receiptCount); err != nil {
		t.Fatalf("read model receipt: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("model receipts = %d, want 1", receiptCount)
	}

	// The task result document carries the model results (metadata only).
	var resultSHA []byte
	var resultSize int64
	var resultMedia string
	if err := env.pool.QueryRow(ctx, `SELECT sha256, size_bytes, media_type FROM artifacts WHERE uri = $1`,
		task.ResultRef).Scan(&resultSHA, &resultSize, &resultMedia); err != nil {
		t.Fatalf("read result artifact metadata: %v", err)
	}
	resultRef := kernelstore.ArtifactReference{URI: task.ResultRef, MediaType: resultMedia, SizeBytes: resultSize}
	copy(resultRef.SHA256[:], resultSHA)
	reader, err := artifacts.Open(ctx, env.tenant, resultRef)
	if err != nil {
		t.Fatalf("open result document: %v", err)
	}
	defer reader.Close()
	var document map[string]any
	if err := json.NewDecoder(io.LimitReader(reader, 1<<20)).Decode(&document); err != nil {
		t.Fatalf("decode result document: %v", err)
	}
	modelResults, ok := document["modelResults"].([]any)
	if !ok || len(modelResults) != 1 {
		t.Fatalf("result document modelResults = %#v, want one entry", document["modelResults"])
	}
	first := modelResults[0].(map[string]any)
	if first["model"] != "openai/gpt-4o" || first["status"] != "COMPLETED" ||
		first["inputTokens"] != float64(150) || first["outputTokens"] != float64(50) {
		t.Fatalf("result document model entry = %#v", first)
	}
}
