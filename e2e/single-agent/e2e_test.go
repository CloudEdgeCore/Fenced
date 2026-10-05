//go:build integration

// Package e2e_test is the v1.1 acceptance evidence: a real Python agent,
// executing real model calls through the OpenAI-compatible provider layer,
// real MCP-mediated tools and memory, checkpoint/lease-expiry recovery with
// fault injection, and the 1,000-task pipeline with ≥99% success — the
// numbers the development plan's Phase 1 / v1.1 gates require.
//
// Requirements: FENCED_TEST_DATABASE_URL (PostgreSQL), a Python 3
// interpreter (FENCED_E2E_PYTHON, default "python"), and the repository
// checked out (the agent example and SDK are launched from their source
// tree). Counts are tunable: FENCED_E2E_TASKS (default 1000) and
// FENCED_E2E_FAULTS (default 100).
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	kernelmemory "github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	kernelmodel "github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/recovery"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	runtimeadapter "github.com/CloudEdgeCore/Fenced/internal/runtime/adapter"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	e2eTenant         = "tenant-e2e"
	e2eVersionRef     = "real-agent@1"
	e2eProviderKey    = "e2e-provider-secret-key"
	e2eCheckpointKind = "fenced.real-agent/v1"
)

// e2eModelRef is the model under test; the live-model test swaps it before
// building its environment (tests in this package run sequentially).
var e2eModelRef = "fake/agent-model"

// e2eLiveModel, when set, routes the provider registry and the model
// descriptor at a live self-hosted endpoint instead of the scripted fake.
var e2eLiveModel struct {
	provider string
	name     string
	url      string
}

// e2eExposeLiveTools, when set alongside e2eLiveModel, publishes the weather
// tool into the live-model manifest (P1-04) so a real model discovers and
// calls it, and registers a second, never-granted tool to prove capability
// filtering keeps that tool off the model's surface. Default false so the
// plain live-answer test (TestV11RealAgentAgainstLiveModel) is unaffected.
var e2eExposeLiveTools bool

func e2ePython(t *testing.T) string {
	t.Helper()
	name := os.Getenv("FENCED_E2E_PYTHON")
	if name == "" {
		name = "python"
	}
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s interpreter not found (set FENCED_E2E_PYTHON): %v", name, err)
	}
	return path
}

func e2eCount(env string, fallback int) int {
	if raw := os.Getenv(env); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

// fakeOpenAI scripts the two-turn agent loop by message shape: a conversation
// without a tool result gets a weather tool call for the city named in the
// goal; a conversation with a tool result gets the final answer.
type fakeOpenAI struct {
	mu         sync.Mutex
	authorized []string
	requests   int
	latency    time.Duration
}

func (f *fakeOpenAI) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	raw, _ := io.ReadAll(request.Body)
	var body struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(raw, &body)
	f.mu.Lock()
	f.requests++
	f.authorized = append(f.authorized, request.Header.Get("Authorization"))
	f.mu.Unlock()
	if f.latency > 0 {
		time.Sleep(f.latency)
	}
	hasToolResult := false
	goal := ""
	for _, message := range body.Messages {
		if message.Role == "tool" {
			hasToolResult = true
		}
		if message.Role == "user" {
			goal = message.Content
		}
	}
	city := "paris"
	if match := strings.Index(goal, "city:"); match >= 0 {
		rest := goal[match+len("city:"):]
		if end := strings.IndexAny(rest, " \n"); end > 0 {
			rest = rest[:end]
		}
		city = strings.TrimSpace(rest)
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("x-request-id", fmt.Sprintf("req-fake-%d", f.requestCount()))
	writer.WriteHeader(http.StatusOK)
	flush := func() {
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	if !hasToolResult {
		args, _ := json.Marshal(map[string]any{"city": city})
		chunks := []map[string]any{
			{"choices": []map[string]any{{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []map[string]any{
				{"index": 0, "id": "call-1", "type": "function", "function": map[string]any{"name": "weather.lookup", "arguments": string(args)}},
			}}}}},
			{"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}}},
			{"choices": []map[string]any{}, "usage": map[string]any{"prompt_tokens": 40, "completion_tokens": 20}},
		}
		for _, chunk := range chunks {
			encoded, _ := json.Marshal(chunk)
			fmt.Fprintf(writer, "data: %s\n\n", encoded)
			flush()
		}
	} else {
		chunks := []map[string]any{
			{"choices": []map[string]any{{"index": 0, "delta": map[string]any{"content": "weather acquired for " + city}}}},
			{"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}},
			{"choices": []map[string]any{}, "usage": map[string]any{"prompt_tokens": 60, "completion_tokens": 30}},
		}
		for _, chunk := range chunks {
			encoded, _ := json.Marshal(chunk)
			fmt.Fprintf(writer, "data: %s\n\n", encoded)
			flush()
		}
	}
	fmt.Fprint(writer, "data: [DONE]\n\n")
}

func (f *fakeOpenAI) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *fakeOpenAI) authorizations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.authorized...)
}

// weatherWebhook is the real HTTPS tool backend the tool gateway calls.
type weatherWebhook struct {
	mu    sync.Mutex
	calls map[string]int
}

func (w *weatherWebhook) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	raw, _ := io.ReadAll(request.Body)
	var args struct {
		Args struct {
			City string `json:"city"`
		} `json:"args"`
	}
	_ = json.Unmarshal(raw, &args)
	w.mu.Lock()
	w.calls[args.Args.City]++
	w.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(writer, `{"city":%q,"celsius":21,"condition":"clear"}`, args.Args.City)
}

func (w *weatherWebhook) count(city string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[city]
}

type e2eEnv struct {
	pool         *pgxpool.Pool
	store        *postgresstore.Store
	provider     *fakeOpenAI
	webhook      *weatherWebhook
	controlAddr  string
	gatewayAddr  string
	artifacts    *artifact.Filesystem
	policyEngine *policy.Engine
	recovery     *recovery.Controller
	leaseTTL     time.Duration
}

func newE2EEnv(t *testing.T, schema string, providerLatency time.Duration, leaseTTL time.Duration) *e2eEnv {
	t.Helper()
	url := os.Getenv("FENCED_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	migrations := filepath.Join("..", "..", "db", "migrations")
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
		t.Fatalf("reset database: %v", err)
	}
	repository := postgresstore.New(pool)

	env := &e2eEnv{pool: pool, store: repository, leaseTTL: leaseTTL}
	env.provider = &fakeOpenAI{latency: providerLatency}
	providerServer := &http.Server{Handler: env.provider}
	providerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for provider: %v", err)
	}
	go func() { _ = providerServer.Serve(providerListener) }()
	t.Cleanup(func() { _ = providerServer.Close() })

	env.webhook = &weatherWebhook{calls: map[string]int{}}
	webhookServer := httptest.NewTLSServer(env.webhook)
	t.Cleanup(webhookServer.Close)

	env.policyEngine, err = policy.New(policy.TenantPolicies{e2eTenant: {
		MaxPriority:   100,
		AllowedTools:  []string{"weather.lookup"},
		AllowedModels: []string{e2eModelRef},
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}

	// Gateway process surface (in-process, real gRPC): tools, memory, model
	// governance and the real invocation executor.
	webhookExecutor, err := gateway.NewWebhookExecutor(
		map[string]string{"weather.lookup@1.0.0": webhookServer.URL}, webhookServer.Client())
	if err != nil {
		t.Fatalf("webhook executor: %v", err)
	}
	toolGateway := tool.NewGateway(env.policyEngine, repository, repository, repository,
		webhookExecutor, &gateway.DevSecretBroker{})
	modelGateway := kernelmodel.NewGateway(env.policyEngine, repository, repository, repository)
	registry := provider.NewRegistry()
	if e2eLiveModel.url != "" {
		liveConfig := provider.Config{
			Name: e2eLiveModel.provider, BaseURL: e2eLiveModel.url, TimeoutMs: 300000, MaxAttempts: 2,
		}
		// Keyed endpoints (vLLM behind a bearer) exercise credential isolation:
		// the key is held only in the fenced provider config and must never
		// surface in any agent-visible artifact. Keyless endpoints leave it unset.
		if key := os.Getenv("FENCED_REAL_MODEL_KEY"); key != "" {
			liveConfig.APIKey = key
		}
		if err := registry.Register(liveConfig); err != nil {
			t.Fatalf("register live provider: %v", err)
		}
	} else if err := registry.Register(provider.Config{
		Name: "fake", BaseURL: "http://" + providerListener.Addr().String(),
		APIKey: e2eProviderKey, TimeoutMs: 20000, MaxAttempts: 2,
	}); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	authorizer, err := capability.NewAuthorizer(repository)
	if err != nil {
		t.Fatalf("capability authorizer: %v", err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for gateway: %v", err)
	}
	gatewayServer := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	gatewayv1.RegisterToolGatewayServiceServer(gatewayServer, gateway.NewService(toolGateway, e2eTenant, authorizer).WithRuntimeFence(repository))
	gatewayv1.RegisterMemoryGatewayServiceServer(gatewayServer, gateway.NewMemoryService(
		kernelmemory.NewGateway(kernelmemory.DevEmbedder{}, repository), repository, e2eTenant, authorizer))
	modelv1.RegisterModelGatewayServiceServer(gatewayServer, gateway.NewModelService(modelGateway, e2eTenant, authorizer))
	modelv1.RegisterModelInvocationServiceServer(gatewayServer, gateway.NewModelInvocationService(
		kernelmodel.NewInvoker(modelGateway, registry), e2eTenant, authorizer))
	go func() { _ = gatewayServer.Serve(gatewayListener) }()
	t.Cleanup(gatewayServer.Stop)
	env.gatewayAddr = gatewayListener.Addr().String()

	// Runtime Protocol control plane.
	controlListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for runtime control: %v", err)
	}
	controlServer := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	runtimev1.RegisterRuntimeControlServiceServer(controlServer, control.NewService(repository, e2eTenant, 2*time.Minute))
	go func() { _ = controlServer.Serve(controlListener) }()
	t.Cleanup(controlServer.Stop)
	env.controlAddr = controlListener.Addr().String()

	env.artifacts, err = artifact.NewFilesystem(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}
	env.recovery = recovery.NewController(repository, 64, leaseTTL)

	// Seed the tenant registries: the model descriptor with its price table
	// and the weather tool with its schema.
	descriptorProvider, descriptorName := "fake", "agent-model"
	descriptorInput, descriptorOutput := 3.0, 6.0
	if e2eLiveModel.url != "" {
		descriptorProvider, descriptorName = e2eLiveModel.provider, e2eLiveModel.name
		descriptorInput, descriptorOutput = 0, 0
	}
	if _, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: e2eTenant, Provider: descriptorProvider, ModelName: descriptorName, SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(descriptorInput), OutputPriceMicroUSDPerMillion: money.MustFromUSD(descriptorOutput), PriceRevision: "p1",
	}); err != nil {
		t.Fatalf("register model descriptor: %v", err)
	}
	if _, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: e2eTenant, Name: "weather.lookup", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"invoke"}, ResourcePatterns: []string{"weather:*"},
		ParamsSchema: []byte(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
	}); err != nil {
		t.Fatalf("register tool descriptor: %v", err)
	}
	if e2eExposeLiveTools {
		// A second tool the AgentVersion is NOT granted. Capability filtering
		// must keep it out of the model's tools/list surface, and the authorizer
		// must reject it even if a call were somehow attempted (P1-04 权限生效).
		if _, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
			TenantID: e2eTenant, Name: "payments.charge", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskHigh,
			Actions: []string{"invoke"}, ResourcePatterns: []string{"payments:*"},
			ParamsSchema: []byte(`{"type":"object","properties":{"amount":{"type":"number"}},"required":["amount"]}`),
		}); err != nil {
			t.Fatalf("register ungranted tool descriptor: %v", err)
		}
	}
	return env
}

// e2eStack is the shared runtime surface every worker instance uses: one
// real Python agent endpoint and one loopback MCP endpoint whose per-attempt
// execution registry keeps concurrent attempts correctly fenced.
type e2eStack struct {
	agentURL  string
	registry  *mcp.ExecutionRegistry
	python    *exec.Cmd
	pythonOut lockedBuilder
}

type lockedBuilder struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuilder) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *lockedBuilder) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startStack(t *testing.T, env *e2eEnv) *e2eStack {
	t.Helper()
	pythonBin := e2ePython(t)
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root)) // e2e/single-agent -> repository root

	gatewayConnection, err := grpc.NewClient(env.gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to gateway: %v", err)
	}
	t.Cleanup(func() { _ = gatewayConnection.Close() })

	registry := mcp.NewExecutionRegistry()
	tools := mcp.NewToolAdapter(reference.NewGrpcToolInvoker(
		gatewayv1.NewToolGatewayServiceClient(gatewayConnection)), registry)
	broker := mcp.NewBroker(tools,
		runtimeadapter.NewGrpcModelBroker(modelv1.NewModelInvocationServiceClient(gatewayConnection)),
		runtimeadapter.NewGrpcMemoryBroker(gatewayv1.NewMemoryGatewayServiceClient(gatewayConnection)),
		nil, nil, registry)
	mcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for MCP: %v", err)
	}
	mcpServer := &http.Server{Handler: mcp.NewServer("fenced-e2e", "v1.1.0", broker)}
	go func() { _ = mcpServer.Serve(mcpListener) }()
	t.Cleanup(func() { _ = mcpServer.Close() })

	agentPort := freePort(t)
	stack := &e2eStack{
		agentURL: fmt.Sprintf("http://127.0.0.1:%d", agentPort),
		registry: registry,
	}
	stack.python = exec.Command(pythonBin, filepath.Join(root, "examples", "agents", "python_remote", "real_agent.py"),
		"--host", "127.0.0.1", "--port", strconv.Itoa(agentPort))
	stack.python.Env = append(os.Environ(),
		"FENCED_MCP_URL=http://"+mcpListener.Addr().String(),
		"FENCED_MODEL_REF="+e2eModelRef,
		"FENCED_MEMORY_NAMESPACE=runs",
		"PYTHONPATH="+filepath.Join(root, "sdk", "python"),
		"PYTHONUNBUFFERED=1",
	)
	stack.python.Stderr = &stack.pythonOut
	if err := stack.python.Start(); err != nil {
		t.Fatalf("start python agent: %v", err)
	}
	t.Cleanup(func() {
		if stack.python.Process != nil {
			_ = stack.python.Process.Kill()
			_, _ = stack.python.Process.Wait()
		}
	})
	waitForAgent(t, stack)
	return stack
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForAgent(t *testing.T, stack *e2eStack) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(stack.agentURL + "/v1/health")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if stack.python.ProcessState != nil {
			t.Fatalf("python agent exited early: %s", stack.pythonOut.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("python agent did not become healthy: %s", stack.pythonOut.String())
}

// makeWorker builds one adapter worker instance against the shared stack.
func makeWorker(t *testing.T, env *e2eEnv, stack *e2eStack, instanceID string, heartbeatTTL time.Duration) *runtimeadapter.Worker {
	t.Helper()
	controlConnection, err := grpc.NewClient(env.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to control: %v", err)
	}
	t.Cleanup(func() { _ = controlConnection.Close() })
	worker, err := runtimeadapter.NewWorker(runtimev1.NewRuntimeControlServiceClient(controlConnection),
		env.artifacts, stack.agentURL, e2eTenant, instanceID, heartbeatTTL, nil)
	if err != nil {
		t.Fatalf("create adapter worker: %v", err)
	}
	return worker.WithExecutionWindow(stack.registry)
}

// driveWorker runs one worker until ctx ends. Shutdown-phase errors
// (canceled context, closing connection) are not test failures.
func driveWorker(ctx context.Context, t *testing.T, worker *runtimeadapter.Worker, instanceID string) {
	t.Helper()
	for {
		processed, err := worker.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil || status.Code(err) == codes.Canceled {
				return
			}
			t.Logf("worker %s stopping: %v", instanceID, err)
			return
		}
		if !processed {
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

// publishVersion registers the immutable AgentVersion bound to the shared
// agent endpoint.
func publishVersion(ctx context.Context, t *testing.T, env *e2eEnv, entrypoint string) uuid.UUID {
	t.Helper()
	spec := map[string]any{
		"runtimeClassPolicy": map[string]any{"allowed": []string{"adapter"}, "preferred": "adapter"},
		"lifecycle":          map[string]any{"maxAttempts": 4},
		"runtimes": []map[string]any{{
			"class": "adapter", "interface": agentversion.RuntimeInterfaceV1,
			"runtimeABI": "fenced.adapter-http/v1", "entrypoint": []string{entrypoint},
		}},
		"capabilities": map[string]any{
			"tools": e2eManifestTools(), "models": []string{e2eModelRef},
			"memory": []string{"runs"}, "secrets": []string{},
		},
		"checkpoint": map[string]any{
			"mode": "logical", "schemaVersion": e2eCheckpointKind, "intervalSeconds": 1,
		},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("encode spec: %v", err)
	}
	if err := agentversion.ValidateSpec(encoded); err != nil {
		t.Fatalf("spec invalid: %v", err)
	}
	published, err := env.store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: e2eTenant, Namespace: "default",
		Name: "real-agent", Version: "1", Spec: encoded,
	})
	if err != nil {
		t.Fatalf("publish agent version: %v", err)
	}
	return published.AgentVersion.ID
}

// e2eManifestTools decides the tool surface the published AgentVersion grants.
// The scripted fake always exposes the weather tool. A live model exposes it
// only when e2eExposeLiveTools is set (the P1-04 tool-calling test): the plain
// live-answer test leaves it off so the model answers directly and does not
// depend on the deterministic webhook backend.
func e2eManifestTools() []string {
	if e2eLiveModel.url != "" && !e2eExposeLiveTools {
		return []string{}
	}
	return []string{"weather.lookup"}
}

func submitTask(ctx context.Context, t *testing.T, env *e2eEnv, key, goal string) kernelstore.Task {
	t.Helper()
	spec := `{"priority":50,"deadline":"2099-01-01T00:00:00Z",` +
		`"budget":{"tokens":1000,"costUsd":5,"toolCalls":20,"wallSeconds":600},` +
		`"placement":{"runtimeClasses":["adapter"],"preferredClass":"adapter","region":"cn-east",` +
		`"cpuMillis":100,"memoryMiB":128,"workspaceBytes":8388608,"llmConcurrency":2}}`
	created, err := env.store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: e2eTenant, Namespace: "default",
		AgentVersionRef: e2eVersionRef, Goal: goal, IdempotencyKey: key, Spec: []byte(spec),
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	return created.Task
}

type staticPools []scheduler.RuntimePool

func (s staticPools) ListRuntimePools(context.Context, string) ([]scheduler.RuntimePool, error) {
	return s, nil
}

func poolsFor(instances ...string) staticPools {
	pools := make(staticPools, 0, len(instances))
	for _, instance := range instances {
		pools = append(pools, scheduler.RuntimePool{
			ID: "pool-" + instance, TenantIDs: []string{e2eTenant}, RuntimeClass: "adapter",
			RuntimeInstanceID: instance, Region: "cn-east", DataResidency: "cn", Ready: true,
			AvailableCPU: 4000, AvailableMemory: 8192, AvailableLLMSlots: 16,
		})
	}
	return pools
}

// reconcile drives admission + scheduling (+ recovery) once, loudly.
func reconcile(ctx context.Context, t *testing.T, env *e2eEnv, pools staticPools, withRecovery bool) {
	t.Helper()
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"adapter"}, MaxTokens: 100000, MaxCostMicroUSD: money.MustFromUSD(1000),
		MaxToolCalls: 10000, MaxWallSeconds: 36000, MaxCPU: 4000, MaxMemory: 8192, MaxLLMConcurrency: 16,
	})
	admissionController := admission.NewController(env.store, engine, env.policyEngine, "e2e-admission", 100, time.Minute)
	schedulerController := scheduler.NewController(env.store, pools, "e2e-scheduler", 100, time.Minute, 2*time.Minute)
	// Scheduling fails closed without an operator-registered capacity
	// ledger; registering the fixture pools is an idempotent upsert.
	if err := env.store.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register fixture pools: %v", err)
	}
	if _, err := admissionController.Reconcile(ctx); err != nil {
		t.Fatalf("admission reconcile: %v", err)
	}
	if _, err := schedulerController.Reconcile(ctx); err != nil {
		t.Fatalf("scheduler reconcile: %v", err)
	}
	if withRecovery {
		if _, err := env.recovery.Reconcile(ctx); err != nil {
			t.Fatalf("recovery reconcile: %v", err)
		}
	}
}

// reconcileQuiet is the background loop variant.
func reconcileQuiet(ctx context.Context, env *e2eEnv, pools staticPools) {
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"adapter"}, MaxTokens: 1000000, MaxCostMicroUSD: money.MustFromUSD(100000),
		MaxToolCalls: 100000, MaxWallSeconds: 360000, MaxCPU: 4000, MaxMemory: 8192, MaxLLMConcurrency: 64,
	})
	admissionController := admission.NewController(env.store, engine, env.policyEngine, "e2e-admission", 100, time.Minute)
	schedulerController := scheduler.NewController(env.store, pools, "e2e-scheduler", 100, time.Minute, 2*time.Minute)
	_ = env.store.RegisterRuntimePools(ctx, pools)
	_, _ = admissionController.Reconcile(ctx)
	_, _ = schedulerController.Reconcile(ctx)
	_, _ = env.recovery.Reconcile(ctx)
}

func taskPhase(ctx context.Context, t *testing.T, env *e2eEnv, taskID uuid.UUID) kernelstore.Task {
	t.Helper()
	task, err := env.store.GetTask(ctx, e2eTenant, taskID)
	if err != nil {
		t.Fatalf("read task: %v", err)
	}
	return task
}

func waitForTerminal(ctx context.Context, t *testing.T, env *e2eEnv, taskID uuid.UUID, timeout time.Duration) kernelstore.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task := taskPhase(ctx, t, env, taskID)
		if task.Phase.Terminal() {
			return task
		}
		time.Sleep(50 * time.Millisecond)
	}
	task := taskPhase(ctx, t, env, taskID)
	t.Fatalf("task %s did not reach a terminal phase (phase=%s)", taskID, task.Phase)
	return task
}

func queryInt(ctx context.Context, t *testing.T, env *e2eEnv, sql string, args ...any) int64 {
	t.Helper()
	var value int64
	if err := env.pool.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return value
}

// executingInstance reports which runtime instance currently holds the
// task's active attempt.
func executingInstance(ctx context.Context, t *testing.T, env *e2eEnv, taskID uuid.UUID) string {
	t.Helper()
	var instance string
	err := env.pool.QueryRow(ctx, `SELECT a.runtime_instance_id FROM attempts a
		JOIN runs r ON r.id = a.run_id WHERE r.task_id = $1 AND a.phase IN ('PLACED','STARTING','RUNNING')
		ORDER BY a.created_at DESC LIMIT 1`, taskID).Scan(&instance)
	if err != nil {
		return ""
	}
	return instance
}

// confirmedToolCheckpoint reports whether the task's newest checkpoint state
// contains a tool-role message (the tool side effect is confirmed progress).
func confirmedToolCheckpoint(ctx context.Context, t *testing.T, env *e2eEnv, taskID uuid.UUID) bool {
	t.Helper()
	rows, err := env.pool.Query(ctx, `SELECT a.uri, a.sha256, a.size_bytes, a.media_type FROM checkpoints c
		JOIN artifacts a ON a.id = c.state_artifact_id
		JOIN runs r ON r.id = c.run_id
		WHERE r.task_id = $1 ORDER BY c.created_at DESC LIMIT 1`, taskID)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var uri, media string
		var digest []byte
		var size int64
		if err := rows.Scan(&uri, &digest, &size, &media); err != nil {
			return false
		}
		artifactRef := kernelstore.ArtifactReference{URI: uri, MediaType: media, SizeBytes: size}
		copy(artifactRef.SHA256[:], digest)
		reader, err := env.artifacts.Open(ctx, e2eTenant, artifactRef)
		if err != nil {
			return false
		}
		raw, _ := io.ReadAll(io.LimitReader(reader, 2<<20))
		reader.Close()
		var checkpoint struct {
			State struct {
				Messages []struct {
					Role string `json:"role"`
				} `json:"messages"`
			} `json:"state"`
		}
		if json.Unmarshal(raw, &checkpoint) != nil {
			return false
		}
		for _, message := range checkpoint.State.Messages {
			if message.Role == "tool" {
				return true
			}
		}
	}
	return false
}
