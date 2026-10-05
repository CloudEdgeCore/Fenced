//go:build integration

// End-to-end harness for the DevOps reference workload: it assembles the
// real kernel stack (gateway gRPC services, brokered MCP, runtime control,
// admission/scheduling/recovery/workflow controllers, adapter workers) plus
// the example's own surfaces (multi-role DevOps runtime, fake-cluster
// webhook tool endpoint) and drives complete incident workflows against the
// deterministic cluster.
package devops_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	devops "github.com/CloudEdgeCore/Fenced/examples/devops-workflow/runtime"
	devopstools "github.com/CloudEdgeCore/Fenced/examples/devops-workflow/tools/cluster"
	hello "github.com/CloudEdgeCore/Fenced/examples/third-party/hello-agent"
	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	kernelmemory "github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/recovery"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	workflowkernel "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	runtimeadapter "github.com/CloudEdgeCore/Fenced/internal/runtime/adapter"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"slices"
)

const (
	devopsTenant  = "devops-tenant"
	fakeModelRef  = "fake/devops-model"
	settleTimeout = 4 * time.Minute
)

func agentRefs() []string {
	return []string{
		"devops-planner@1.0.0",
		"devops-observer@1.0.0",
		"devops-diagnoser@1.0.0",
		"devops-executor@1.0.0",
		"devops-verifier@1.0.0",
		"devops-rollback@1.0.0",
		"hello-agent@1.0.0",
	}
}

// thirdPartyRefs are the agents the platform treats as opaque third-party
// code (design plan §7).
func thirdPartyRefs() []string {
	return []string{"hello-agent@1.0.0"}
}

type staticPools []scheduler.RuntimePool

func (s staticPools) ListRuntimePools(context.Context, string) ([]scheduler.RuntimePool, error) {
	return s, nil
}

// loggedRuntime surfaces adapter errors for diagnostics.
type loggedRuntime struct {
	inner *devops.Runtime
}

func (l *loggedRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	output, err := l.inner.Run(ctx, request, emit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[devops-e2e] runtime run %s goal=%.120s: %v\n", request.ExecutionID, request.Goal, err)
	}
	return output, err
}

func (l *loggedRuntime) Checkpoint(ctx context.Context, executionID string) (agent.Checkpoint, error) {
	return l.inner.Checkpoint(ctx, executionID)
}

func (l *loggedRuntime) Restore(ctx context.Context, request agent.RestoreRequest) error {
	return l.inner.Restore(ctx, request)
}

type loggedHelloRuntime struct {
	inner *hello.Runtime
}

func (l *loggedHelloRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	output, err := l.inner.Run(ctx, request, emit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[hello-e2e] runtime run %s: %v\n", request.ExecutionID, err)
	}
	return output, err
}

func (l *loggedHelloRuntime) Checkpoint(ctx context.Context, executionID string) (agent.Checkpoint, error) {
	return l.inner.Checkpoint(ctx, executionID)
}

func (l *loggedHelloRuntime) Restore(ctx context.Context, request agent.RestoreRequest) error {
	return l.inner.Restore(ctx, request)
}

// cordonPool marks a runtime pool CORDONED in both the static pool list
// (used by the scheduler) and the durable registry (used by the kernel's
// pool-aware re-placement).
func (h *harness) cordonPool(id string) {
	h.t.Helper()
	for index := range h.pools {
		if h.pools[index].ID == id {
			h.pools[index].Status = "CORDONED"
			break
		}
	}
	if _, err := h.pool.Exec(context.Background(),
		`UPDATE runtime_pools SET status = 'CORDONED', updated_at = now() WHERE id = $1`, id); err != nil {
		h.t.Fatalf("registry cordon %s: %v", id, err)
	}
}

// uncordonPool re-admits a previously cordoned pool.
func (h *harness) uncordonPool(id string) {
	h.t.Helper()
	for index := range h.pools {
		if h.pools[index].ID == id {
			h.pools[index].Status = "ACTIVE"
			break
		}
	}
	if _, err := h.pool.Exec(context.Background(),
		`UPDATE runtime_pools SET status = 'ACTIVE', updated_at = now() WHERE id = $1`, id); err != nil {
		h.t.Fatalf("registry uncordon %s: %v", id, err)
	}
}

type harness struct {
	t          *testing.T
	pool       *pgxpool.Pool
	store      *postgresstore.Store
	cluster    *devopstools.Cluster
	controlURL string
	mcpURL     string
	cancelCtx  context.CancelFunc
	loopWG     sync.WaitGroup
	listener   net.Listener
	loopCtx    context.Context
	workerMu   sync.Mutex
	workers    map[string]context.CancelFunc
	pools      staticPools
	schema     string
	artifacts  *artifact.Filesystem
	bindings   *runtimeadapter.RuntimeBindings
	registry   *mcp.ExecutionRegistry
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// newHarness builds the full in-process stack for one scenario.
func newHarness(t *testing.T, name string, stubborn bool) *harness {
	t.Helper()
	databaseURL := os.Getenv("FENCED_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	normalizedName := strings.Map(func(character rune) rune {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' {
			return character
		}
		return '_'
	}, name)
	schema := "fenced_devops_" + normalizedName
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	_ = admin.Close(ctx)
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ", public"
	config.MaxConns = 24
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "db", "migrations"))
	if err != nil {
		t.Fatalf("migrations path: %v", err)
	}
	if _, err := migrate.Apply(ctx, pool, migrations); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE task_usage_reservations,
		runtime_capacity_reservations, runtime_pool_capacities, runtime_pool_tenant_grants, runtime_pools,
		provider_circuit_breakers, workflow_usage_ledgers,
		model_calls, model_descriptors, tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts, memory_records,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		runtime_leases, attempts, runs, tasks, workflows, workflow_steps RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	store := postgresstore.New(pool)
	h := &harness{t: t, pool: pool, store: store, schema: schema}

	// Fake cluster webhook.
	h.cluster = devopstools.New(stubborn)
	toolListener, toolClient, toolEndpoint, err := devopstools.SelfSignedTLSListener(devopstools.NewServer(h.cluster))
	if err != nil {
		t.Fatalf("cluster listener: %v", err)
	}
	go func() { _ = (&http.Server{Handler: devopstools.NewServer(h.cluster)}).Serve(toolListener) }()
	t.Cleanup(func() { _ = toolListener.Close() })

	policyEngine, err := policy.New(policy.TenantPolicies{devopsTenant: {
		MaxPriority:   100,
		AllowedTools:  []string{"kubernetes.get", "kubernetes.logs", "kubernetes.restart", "kubernetes.scale", "server.exec"},
		AllowedModels: []string{fakeModelRef},
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	webhookExecutor, err := gateway.NewWebhookExecutor(map[string]string{
		"kubernetes.get@1.0.0":     toolEndpoint,
		"kubernetes.logs@1.0.0":    toolEndpoint,
		"kubernetes.restart@1.0.0": toolEndpoint,
		"kubernetes.scale@1.0.0":   toolEndpoint,
		"server.exec@1.0.0":        toolEndpoint,
	}, toolClient)
	if err != nil {
		t.Fatalf("webhook executor: %v", err)
	}
	toolGateway := tool.NewGateway(policyEngine, store, store, store, webhookExecutor, &gateway.DevSecretBroker{})
	modelGateway := model.NewGateway(policyEngine, store, store, store)
	providerRegistry := provider.NewRegistry()
	if _, err := store.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: devopsTenant, Provider: "fake", ModelName: "devops-model", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(1), OutputPriceMicroUSDPerMillion: money.MustFromUSD(2), PriceRevision: "p1",
	}); err != nil {
		t.Fatalf("model descriptor: %v", err)
	}
	authorizer, err := capability.NewAuthorizer(store)
	if err != nil {
		t.Fatalf("authorizer: %v", err)
	}

	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway listen: %v", err)
	}
	gatewayServer := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	gatewayv1.RegisterToolGatewayServiceServer(gatewayServer, gateway.NewService(toolGateway, devopsTenant, authorizer).WithRuntimeFence(store))
	gatewayv1.RegisterMemoryGatewayServiceServer(gatewayServer, gateway.NewMemoryService(
		kernelmemory.NewGateway(kernelmemory.DevEmbedder{}, store), store, devopsTenant, authorizer))
	modelv1.RegisterModelGatewayServiceServer(gatewayServer, gateway.NewModelService(modelGateway, devopsTenant, authorizer))
	modelv1.RegisterModelInvocationServiceServer(gatewayServer, gateway.NewModelInvocationService(
		model.NewInvoker(modelGateway, providerRegistry), devopsTenant, authorizer))
	go func() { _ = gatewayServer.Serve(gatewayListener) }()
	t.Cleanup(gatewayServer.Stop)
	gatewayConn, err := grpc.NewClient(gatewayListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("gateway client: %v", err)
	}
	t.Cleanup(func() { _ = gatewayConn.Close() })

	controlListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control listen: %v", err)
	}
	h.listener = controlListener
	controlServer := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	runtimev1.RegisterRuntimeControlServiceServer(controlServer, control.NewService(store, devopsTenant, 2*time.Minute))
	go func() { _ = controlServer.Serve(controlListener) }()
	t.Cleanup(controlServer.Stop)

	artifacts, err := artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}
	h.artifacts = artifacts

	for _, descriptor := range []struct {
		name    string
		pattern string
		params  string
	}{
		{"kubernetes.get", "k8s:get:*", `{"type":"object","properties":{"namespace":{"type":"string"},"service":{"type":"string"}},"required":["service"]}`},
		{"kubernetes.logs", "k8s:logs:*", `{"type":"object","properties":{"namespace":{"type":"string"},"service":{"type":"string"}},"required":["service"]}`},
		{"kubernetes.restart", "k8s:restart:*", `{"type":"object","properties":{"namespace":{"type":"string"},"service":{"type":"string"}},"required":["service"]}`},
		{"kubernetes.scale", "k8s:scale:*", `{"type":"object","properties":{"namespace":{"type":"string"},"service":{"type":"string"},"replicas":{"type":"integer"}},"required":["service","replicas"]}`},
		{"server.exec", "server:exec:*", `{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`},
		{"hello.echo", "hello:echo:*", `{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}`},
	} {
		if _, err := store.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
			TenantID: devopsTenant, Name: descriptor.name, Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskHigh,
			Actions: []string{"invoke"}, ResourcePatterns: []string{descriptor.pattern},
			ParamsSchema: []byte(descriptor.params),
		}); err != nil {
			t.Fatalf("tool descriptor %s: %v", descriptor.name, err)
		}
	}

	registry := mcp.NewExecutionRegistry()
	h.registry = registry
	toolsAdapter := mcp.NewToolAdapter(reference.NewGrpcToolInvoker(gatewayv1.NewToolGatewayServiceClient(gatewayConn)), registry)
	broker := mcp.NewBroker(toolsAdapter,
		runtimeadapter.NewGrpcModelBroker(modelv1.NewModelInvocationServiceClient(gatewayConn)),
		runtimeadapter.NewGrpcMemoryBroker(gatewayv1.NewMemoryGatewayServiceClient(gatewayConn)),
		nil, nil, registry)
	mcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("mcp listen: %v", err)
	}
	mcpHTTP := &http.Server{Handler: mcp.NewServer("devops-e2e", "v1.0.0", broker)}
	go func() { _ = mcpHTTP.Serve(mcpListener) }()
	t.Cleanup(func() { _ = mcpListener.Close() })
	h.mcpURL = "http://" + mcpListener.Addr().String()

	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("agent listen: %v", err)
	}
	agentHandler, err := agent.NewHost(&loggedRuntime{inner: devops.NewRuntime(h.mcpURL, devops.Models{Fast: fakeModelRef, Reasoning: fakeModelRef})}, agent.HostOptions{Adapter: "devops-e2e", MaxConcurrent: 64})
	if err != nil {
		t.Fatalf("agent host: %v", err)
	}
	go func() { _ = (&http.Server{Handler: agentHandler}).Serve(agentListener) }()
	t.Cleanup(func() { _ = agentListener.Close() })
	agentURL := "http://" + agentListener.Addr().String()

	helloListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hello agent listen: %v", err)
	}
	helloAgentHandler, err := agent.NewHost(&loggedHelloRuntime{inner: hello.NewRuntime(h.mcpURL)}, agent.HostOptions{Adapter: "hello-e2e", MaxConcurrent: 64})
	if err != nil {
		t.Fatalf("hello agent host: %v", err)
	}
	go func() { _ = (&http.Server{Handler: helloAgentHandler}).Serve(helloListener) }()
	t.Cleanup(func() { _ = helloListener.Close() })
	helloAgentURL := "http://" + helloListener.Addr().String()

	bindingEntries := make([]runtimeadapter.RuntimeBinding, 0, len(agentRefs())+4)
	for _, ref := range agentRefs() {
		endpoint := agentURL
		if slices.Contains(thirdPartyRefs(), ref) {
			endpoint = helloAgentURL
		}
		bindingEntries = append(bindingEntries, runtimeadapter.RuntimeBinding{AgentVersionRef: ref, Endpoint: endpoint})
	}
	// Isolation-drill agents (Wasmtime/OCI) are dual-class: when their
	// sandbox pool is cordoned, cross-class placement lands them on an
	// adapter pool, which resolves their entrypoint through this table.
	for _, ref := range []string{"wasm-agent@1.0.0", "wasm-dual-agent@1.0.0", "oci-agent@1.0.0", "oci-dual-agent@1.0.0"} {
		bindingEntries = append(bindingEntries, runtimeadapter.RuntimeBinding{AgentVersionRef: ref, Endpoint: helloAgentURL})
	}
	bindingsFile := filepath.Join(t.TempDir(), "bindings.json")
	encodedBindings, _ := json.Marshal(map[string]any{"bindings": bindingEntries})
	if err := os.WriteFile(bindingsFile, encodedBindings, 0o644); err != nil {
		t.Fatalf("write bindings: %v", err)
	}
	bindings, err := runtimeadapter.LoadRuntimeBindings(bindingsFile)
	if err != nil {
		t.Fatalf("load bindings: %v", err)
	}

	h.bindings = bindings
	h.publishAgents()

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	h.cancelCtx = cancelLoop
	h.loopCtx = loopCtx
	instances := []string{"devops-worker-00", "devops-worker-01", "devops-worker-02", "devops-worker-03", "wasm-worker-1", "oci-worker-1"}
	pools := make(staticPools, 0, len(instances))
	for index, instance := range instances {
		class := "research-reasoning"
		if index == 1 || index == 3 {
			class = "research-network"
		} else if index == 2 {
			class = "research-sandbox"
		} else if index == 4 {
			class = "research-wasm"
		} else if index == 5 {
			class = "oci"
		}
		pools = append(pools, scheduler.RuntimePool{
			ID: fmt.Sprintf("devops-pool-%d", index), TenantIDs: []string{devopsTenant},
			RuntimeClass: class, RuntimeInstanceID: instance, Region: "cn-east", DataResidency: "cn",
			Ready: true, AvailableCPU: 8000, AvailableMemory: 16384, AvailableLLMSlots: 32,
		})
	}
	h.pools = pools
	admissionController := admission.NewController(store, admission.New(admission.Limits{
		RuntimeClasses: []string{"research-reasoning", "research-network", "research-sandbox", "research-wasm", "oci"}, MaxTokens: 1000000,
		MaxCostMicroUSD: money.MustFromUSD(5000), MaxToolCalls: 100000, MaxWallSeconds: 36000,
		MaxCPU: 16000, MaxMemory: 32768, MaxLLMConcurrency: 64,
	}), policyEngine, "devops-admission", 50, time.Minute)
	schedulerController := scheduler.NewController(store, pools, "devops-scheduler", 50, time.Minute, 2*time.Minute)
	if err := store.RegisterRuntimePools(loopCtx, pools); err != nil {
		t.Fatalf("register pools: %v", err)
	}
	workflowController := workflowkernel.NewController(store, store, artifacts, "devops-orchestrator", 100)
	recoveryController := recovery.NewController(store, 128, 30*time.Second)
	h.loopWG.Add(1)
	go func() {
		defer h.loopWG.Done()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				_, _ = admissionController.Reconcile(loopCtx)
				_, _ = schedulerController.Reconcile(loopCtx)
				_, _ = workflowController.Reconcile(loopCtx)
				_, _ = recoveryController.Reconcile(loopCtx)
			}
		}
	}()
	for _, instance := range instances {
		// wasm-worker-* and oci-worker-* pools are served by external
		// subprocesses (Wasmtime/OCI isolation drills), not by adapter
		// workers. Starting an adapter worker here would steal their leases.
		if strings.HasPrefix(instance, "wasm-worker-") || strings.HasPrefix(instance, "oci-worker-") {
			continue
		}
		h.startWorker(loopCtx, instance)
	}
	t.Cleanup(func() { cancelLoop(); h.loopWG.Wait() })

	// Control API HTTP for the approval surface.
	controlAPIHandler := api.NewHandler(store, store, store,
		kernelmemory.NewGateway(kernelmemory.DevEmbedder{}, store), api.WithWorkflowStore(store),
		api.WithMetricsStore(store))
	controlAPIHandler = auth.StaticMiddleware(auth.Principal{Subject: "devops-e2e", TenantID: devopsTenant}, controlAPIHandler)
	controlAPIListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control api listen: %v", err)
	}
	go func() { _ = (&http.Server{Handler: controlAPIHandler}).Serve(controlAPIListener) }()
	t.Cleanup(func() { _ = controlAPIListener.Close() })
	h.controlURL = "http://" + controlAPIListener.Addr().String()

	return h
}

// KillWorker simulates a runtime crash: the poll loop stops dead without
// releasing its leases, so recovery must reclaim them after expiry.
func (h *harness) KillWorker(instance string) {
	h.workerMu.Lock()
	cancel, ok := h.workers[instance]
	h.workerMu.Unlock()
	if ok {
		cancel()
	}
}

func (h *harness) startWorker(loopCtx context.Context, instance string) {
	h.t.Helper()
	controlConn, err := grpc.NewClient(h.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatalf("control client: %v", err)
	}
	worker, err := runtimeadapter.NewWorker(runtimev1.NewRuntimeControlServiceClient(controlConn),
		h.artifacts, "", devopsTenant, instance, 15*time.Second, nil)
	if err != nil {
		h.t.Fatalf("worker %s: %v", instance, err)
	}
	worker = worker.WithExecutionWindow(h.registry).WithRuntimeBindings(h.bindings)
	workerCtx, cancelWorker := context.WithCancel(loopCtx)
	h.workerMu.Lock()
	if h.workers == nil {
		h.workers = map[string]context.CancelFunc{}
	}
	h.workers[instance] = cancelWorker
	h.workerMu.Unlock()
	h.loopWG.Add(1)
	go func() {
		defer h.loopWG.Done()
		defer func() { _ = controlConn.Close() }()
		for {
			processed, err := worker.RunOnce(workerCtx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[devops-e2e] worker %s: %v\n", instance, err)
			}
			if !processed {
				if workerCtx.Err() != nil {
					return
				}
				select {
				case <-workerCtx.Done():
					return
				case <-time.After(15 * time.Millisecond):
				}
			}
		}
	}()
}
