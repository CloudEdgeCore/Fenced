//go:build integration

package reference_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestSandboxAgentMcpEntryDrivesRealDecisionChain proves the sandbox Agent
// MCP entry against real PostgreSQL: the worker's identity slot publishes the
// fenced Attempt, the MCP call is forwarded over the Tool Gateway gRPC
// boundary, and the decision chain leaves durable evidence. Outside an
// execution window the endpoint fails closed.
func TestSandboxAgentMcpEntryDrivesRealDecisionChain(t *testing.T) {
	ctx := context.Background()
	pool, store := prepareReferenceDatabase(t)
	assignment := scheduleReferenceTask(t, ctx, store, "sandbox-mcp")

	if _, err := store.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
	}); err != nil {
		t.Fatalf("register tool descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{"tenant-a": {
		MaxPriority: 100, AllowedTools: []string{"fs.read"}, ApprovalRequiredRisk: "high",
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	gwService := gateway.NewService(tool.NewGateway(engine, store, store, store,
		&gateway.DevExecutor{MaxOutputBytes: 1 << 20}, &gateway.DevSecretBroker{}), "tenant-a")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Tool Gateway: %v", err)
	}
	gwServer := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	gatewayv1.RegisterToolGatewayServiceServer(gwServer, gwService)
	go func() { _ = gwServer.Serve(listener) }()
	t.Cleanup(gwServer.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Tool Gateway: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	identitySlot := reference.NewIdentitySlot()
	adapter := mcp.NewToolAdapter(reference.NewGrpcToolInvoker(gatewayv1.NewToolGatewayServiceClient(connection)), identitySlot)
	server := httptest.NewServer(mcp.NewServer("fenced-runtime", "v0.1", adapter))
	t.Cleanup(server.Close)

	// Outside an execution window: fail closed.
	if result := sandboxMcpCall(t, server.URL, "tools/list", map[string]any{}); result["error"] == nil {
		t.Fatalf("tools/list outside a window must fail closed: %+v", result)
	}

	// Publish the real fenced attempt and invoke through MCP.
	identitySlot.Set(mcp.AttemptContext{
		TenantID: "tenant-a", TaskID: assignment.Task.ID, RunID: assignment.Run.ID,
		AttemptID: assignment.Attempt.ID, FencingToken: assignment.Attempt.FencingToken,
		AgentVersionRef: "agent@1",
	})
	result := sandboxMcpCall(t, server.URL, "tools/call", map[string]any{
		"name": "fs.read", "arguments": map[string]any{"path": "a.txt"},
	})
	if result["isError"] != false {
		t.Fatalf("call result: %+v", result)
	}

	var callStatus string
	var settledToolCalls int64
	if err := pool.QueryRow(ctx, `SELECT status FROM tool_calls WHERE tenant_id = $1 AND task_id = $2`,
		"tenant-a", assignment.Task.ID.String()).Scan(&callStatus); err != nil {
		t.Fatalf("read tool call: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(tool_calls), 0) FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2`, "tenant-a", assignment.Task.ID.String()).Scan(&settledToolCalls); err != nil {
		t.Fatalf("read settlements: %v", err)
	}
	var receipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM runtime_operation_receipts
		WHERE tenant_id = $1 AND attempt_id = $2 AND operation = 'TOOL:fs.read@1.0.0'`,
		"tenant-a", assignment.Attempt.ID.String()).Scan(&receipts); err != nil {
		t.Fatalf("read receipts: %v", err)
	}
	if callStatus != string(kernelstore.ToolCallExecuted) || settledToolCalls != 1 || receipts != 1 {
		t.Fatalf("decision chain evidence: call=%s settled=%d receipts=%d", callStatus, settledToolCalls, receipts)
	}

	// Clearing the slot restores default deny.
	identitySlot.Clear()
	if result := sandboxMcpCall(t, server.URL, "tools/call", map[string]any{
		"name": "fs.read", "arguments": map[string]any{"path": "b.txt"},
	}); result["isError"] != true {
		t.Fatalf("call after clear must fail closed: %+v", result)
	}
}

func sandboxMcpCall(t *testing.T, url string, method string, params map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("mcp call: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatalf("decode: %v: %s", err, payload)
	}
	if message["error"] != nil {
		return map[string]any{"error": message["error"]}
	}
	return message["result"].(map[string]any)
}

func prepareReferenceDatabase(t *testing.T) (*pgxpool.Pool, *postgresstore.Store) {
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
	if _, err := admin.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS fenced_reference_mcp`); err != nil {
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
	config.ConnConfig.RuntimeParams["search_path"] = "fenced_reference_mcp"
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
	return pool, postgresstore.New(pool)
}

func scheduleReferenceTask(t *testing.T, ctx context.Context, store *postgresstore.Store, key string) kernelstore.RuntimeAssignment {
	t.Helper()
	if _, err := store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", Name: "agent", Version: "1",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`),
	}); err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1", Goal: key,
		IdempotencyKey: key, Spec: []byte(`{
			"priority":70,"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
			"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},
			"retryPolicy":{"maxAttempts":3}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	limits := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10), MaxToolCalls: 100,
		MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	policyEngine, err := policy.New(policy.TenantPolicies{"tenant-a": {MaxPriority: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := admission.NewController(store, limits, policyEngine, "admission-"+key, 10, time.Minute).Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("admission count=%d err=%v", count, err)
	}
	pools := staticPools{{
		ID: "pool-sandbox", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci", RuntimeInstanceID: "worker-sandbox-1",
		Region: "cn-east", Ready: true, AvailableCPU: 2000, AvailableMemory: 4096, AvailableLLMSlots: 4,
	}}
	if err := store.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register fixture pools: %v", err)
	}
	if count, err := scheduler.NewController(store, pools, "scheduler-"+key, 10, time.Minute, 30*time.Second).Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("scheduler count=%d err=%v", count, err)
	}
	assignment, err := store.PollRuntimeAssignment(ctx, "tenant-a", "worker-sandbox-1")
	if err != nil || assignment.Task.ID != created.Task.ID {
		t.Fatalf("poll assignment=%+v err=%v", assignment, err)
	}
	return assignment
}

type staticPools []scheduler.RuntimePool

func (s staticPools) ListRuntimePools(context.Context, string) ([]scheduler.RuntimePool, error) {
	return s, nil
}
