package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var testIdentity = AttemptContext{
	TenantID: "tenant-a", TaskID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
	RunID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
	AttemptID:    uuid.MustParse("33333333-3333-3333-3333-333333333333"),
	FencingToken: 1, AgentVersionRef: "agent@1",
}

func TestAdapterListsToolsWithSchemas(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{Name: "fs.read", Version: "1.0.0", SideEffectRisk: store.ToolRiskLow, Actions: []string{"read"},
			ResourcePatterns: []string{"fs:/tmp"}, ParamsSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`)},
	}}
	adapter := NewToolAdapter(invoker, StaticIdentity{Context: testIdentity})

	result, rpcErr := adapter.ListTools(context.Background(), nil)
	if rpcErr != nil {
		t.Fatalf("ListTools: %v", rpcErr)
	}
	tools := result.(map[string]any)["tools"].([]map[string]any)
	if len(tools) != 1 || tools[0]["name"] != "fs.read" {
		t.Fatalf("tools: %+v", tools)
	}
	schema, ok := tools[0]["inputSchema"].(json.RawMessage)
	if !ok || string(schema) == "" {
		t.Fatalf("inputSchema missing: %+v", tools[0])
	}
}

func TestAdapterCallsToolWithMappedIdentityAndDeterministicKey(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{Name: "fs.read", Version: "1.0.0", SideEffectRisk: store.ToolRiskLow, Actions: []string{"read"},
			ResourcePatterns: []string{"fs:/tmp"}, ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	adapter := NewToolAdapter(invoker, StaticIdentity{Context: testIdentity})

	params := json.RawMessage(`{"name":"fs.read","arguments":{"path":"a.txt"}}`)
	result, rpcErr := adapter.CallTool(context.Background(), params)
	if rpcErr != nil {
		t.Fatalf("CallTool: %v", rpcErr)
	}
	if invoker.calls != 1 || invoker.input.AttemptID != testIdentity.AttemptID ||
		invoker.input.TenantID != testIdentity.TenantID || invoker.input.Action != "read" || invoker.input.Resource != "fs:/tmp" {
		t.Fatalf("invoker input: %+v calls=%d", invoker.input, invoker.calls)
	}
	if result.(map[string]any)["isError"] != false {
		t.Fatalf("unexpected error result: %+v", result)
	}

	// Identical call replays with the same deterministic idempotency key.
	invoker.replay = true
	if _, rpcErr := adapter.CallTool(context.Background(), params); rpcErr != nil {
		t.Fatalf("replay CallTool: %v", rpcErr)
	}
	if invoker.keys[0] != invoker.keys[1] {
		t.Fatalf("idempotency keys differ across identical calls: %v", invoker.keys)
	}
}

func TestAdapterPreservesOnlyBoundedEndpointHTTPCode(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{{
		Name: "web.fetch", Version: "1.0.0", Actions: []string{"invoke"}, ResourcePatterns: []string{"web:fetch:*"},
	}}}
	adapter := NewToolAdapter(invoker, StaticIdentity{Context: testIdentity})
	params := json.RawMessage(`{"name":"web.fetch","arguments":{"url":"https://example.com"}}`)

	invoker.err = status.Error(codes.Aborted, "TOOL_ENDPOINT_HTTP_503")
	result, rpcErr := adapter.CallTool(context.Background(), params)
	if rpcErr != nil {
		t.Fatalf("CallTool: %v", rpcErr)
	}
	text := result.(map[string]any)["content"].([]map[string]any)[0]["text"].(string)
	if !strings.Contains(text, "TOOL_ENDPOINT_HTTP_503") {
		t.Fatalf("bounded status code lost: %s", text)
	}

	invoker.err = status.Error(codes.Internal, "arbitrary sensitive detail")
	result, rpcErr = adapter.CallTool(context.Background(), params)
	if rpcErr != nil {
		t.Fatalf("CallTool fallback: %v", rpcErr)
	}
	text = result.(map[string]any)["content"].([]map[string]any)[0]["text"].(string)
	if !strings.Contains(text, "TOOL_INVOCATION_FAILED") || strings.Contains(text, "sensitive") {
		t.Fatalf("unbounded status was exposed: %s", text)
	}
}

func TestAdapterSurfacesDeniedAndApprovalOutcomes(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{Name: "fs.write", Version: "1.0.0", SideEffectRisk: store.ToolRiskHigh, Actions: []string{"write"},
			ResourcePatterns: []string{"fs:/tmp"}, ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	adapter := NewToolAdapter(invoker, StaticIdentity{Context: testIdentity})

	invoker.denied = true
	result, rpcErr := adapter.CallTool(context.Background(), json.RawMessage(`{"name":"fs.write","arguments":{}}`))
	if rpcErr != nil {
		t.Fatalf("denied CallTool: %v", rpcErr)
	}
	denied := result.(map[string]any)
	if denied["isError"] != true || !json.Valid([]byte(denied["content"].([]map[string]any)[0]["text"].(string))) {
		t.Fatalf("denied result: %+v", denied)
	}

	invoker.denied = false
	invoker.approvalID = uuid.New()
	result, rpcErr = adapter.CallTool(context.Background(), json.RawMessage(`{"name":"fs.write","arguments":{}}`))
	if rpcErr != nil {
		t.Fatalf("approval CallTool: %v", rpcErr)
	}
	if result.(map[string]any)["isError"] != true {
		t.Fatalf("approval-required result must be an error outcome: %+v", result)
	}
}

func TestAdapterRejectsUnknownToolAndMalformedParams(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{Name: "fs.read", Version: "1.0.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	adapter := NewToolAdapter(invoker, StaticIdentity{Context: testIdentity})

	if _, rpcErr := adapter.CallTool(context.Background(), json.RawMessage(`{"name":"nope","arguments":{}}`)); rpcErr == nil {
		t.Fatal("unknown tool accepted")
	}
	if _, rpcErr := adapter.CallTool(context.Background(), json.RawMessage(`{"name":"fs.read","arguments":{},"extra":1}`)); rpcErr == nil {
		t.Fatal("unknown field accepted")
	}
	if _, rpcErr := adapter.CallTool(context.Background(), json.RawMessage(`{}`)); rpcErr == nil {
		t.Fatal("missing name accepted")
	}
}

func TestLatestDescriptorUsesSemanticVersionOrdering(t *testing.T) {
	descriptors := []store.ToolDescriptor{
		{Name: "versioned", Version: "1.9.0"},
		{Name: "versioned", Version: "1.10.0"},
		{Name: "versioned", Version: "2.0.0"},
		{Name: "versioned", Version: "10.0.0"},
	}
	latest, ok := latestDescriptor(descriptors, "versioned")
	if !ok || latest.Version != "10.0.0" {
		t.Fatalf("latest = %+v, %v; want 10.0.0", latest, ok)
	}
}

func TestToolAdapterResolvesConcurrentExecutionIdentity(t *testing.T) {
	registry := NewExecutionRegistry()
	first, second := testIdentity, testIdentity
	first.AttemptID, second.AttemptID = uuid.New(), uuid.New()
	first.TenantID, second.TenantID = "tenant-a", "tenant-b"
	closeFirst, closeSecond := registry.Open(first), registry.Open(second)
	defer closeFirst()
	defer closeSecond()

	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{{Name: "echo", Version: "1.0.0"}}}
	adapter := NewToolAdapter(invoker, registry)
	params := json.RawMessage(`{"name":"echo","arguments":{}}`)
	for _, identity := range []AttemptContext{first, second} {
		ctx := context.WithValue(context.Background(), executionContextKey{}, identity.AttemptID.String())
		if _, rpcErr := adapter.CallTool(ctx, params); rpcErr != nil {
			t.Fatalf("call for %s: %v", identity.AttemptID, rpcErr)
		}
		if invoker.input.AttemptID != identity.AttemptID || invoker.input.TenantID != identity.TenantID {
			t.Fatalf("identity = %+v; want attempt=%s tenant=%s", invoker.input, identity.AttemptID, identity.TenantID)
		}
	}
	if result, rpcErr := adapter.CallTool(context.Background(), params); rpcErr != nil || result.(map[string]any)["isError"] != true {
		t.Fatalf("production header-less call did not fail closed: result=%+v err=%v", result, rpcErr)
	}
}

// P1-01: the idempotency key must bind the resolved tool version — the same
// arguments against weather@1.0 and weather@2.0 are different side effects
// and must never share replay semantics, while identical calls to the same
// version stay deterministic replays.
func TestMCPIdempotencyKeyBindsToolVersion(t *testing.T) {
	args := json.RawMessage(`{"path":"a.txt"}`)
	keyV1 := mcpIdempotencyKey(testIdentity, "weather", "1.0.0", args)
	keyV1Again := mcpIdempotencyKey(testIdentity, "weather", "1.0.0", args)
	keyV2 := mcpIdempotencyKey(testIdentity, "weather", "2.0.0", args)
	if keyV1 != keyV1Again {
		t.Fatalf("identical calls must share the key: %q vs %q", keyV1, keyV1Again)
	}
	if keyV1 == keyV2 {
		t.Fatalf("different tool versions collapsed onto one idempotency key %q", keyV1)
	}
	// The version participates even under reordered-but-equivalent argument
	// documents, because canonicalization only normalizes formatting.
	keyV2Reordered := mcpIdempotencyKey(testIdentity, "weather", "2.0.0", json.RawMessage(`{"path" : "a.txt"}`))
	if keyV2 != keyV2Reordered {
		t.Fatalf("canonical arguments not stable across formatting: %q vs %q", keyV2, keyV2Reordered)
	}
	if len(keyV1) > 128 {
		t.Fatalf("idempotency key exceeds the 128-byte bound: %d", len(keyV1))
	}
}

// P1-02: tools/list shows exactly one entry per tool name even when several
// versions are granted (a floating "name@*" grant or a publish-time freeze
// over multiple registered versions), and that entry is the same latest
// version a bare-name invocation resolves to.
func TestAdapterListsOneEntryPerToolName(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{Name: "weather", Version: "1.0.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
		{Name: "weather", Version: "2.0.0", ParamsSchema: json.RawMessage(`{"type":"object","properties":{"city":{}}}`)},
		{Name: "weather", Version: "1.10.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
		{Name: "fs.read", Version: "0.9.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	adapter := NewToolAdapter(invoker, StaticIdentity{Context: testIdentity})

	result, rpcErr := adapter.ListTools(context.Background(), nil)
	if rpcErr != nil {
		t.Fatalf("ListTools: %v", rpcErr)
	}
	tools := result.(map[string]any)["tools"].([]map[string]any)
	if len(tools) != 2 {
		t.Fatalf("tools/list returned %d entries, want one per name (2): %+v", len(tools), tools)
	}
	if tools[0]["name"] != "fs.read" {
		t.Fatalf("listing is not in deterministic name order: %+v", tools)
	}
	if tools[1]["name"] != "weather" {
		t.Fatalf("weather entry missing: %+v", tools)
	}
	description := tools[1]["description"].(string)
	if !strings.Contains(description, "version 2.0.0") {
		t.Fatalf("listed entry must be the resolved latest version 2.0.0, got %q", description)
	}

	// A bare-name call resolves to exactly the listed version.
	if _, rpcErr := adapter.CallTool(context.Background(), json.RawMessage(`{"name":"weather","arguments":{"city":"paris"}}`)); rpcErr != nil {
		t.Fatalf("CallTool: %v", rpcErr)
	}
	if invoker.input.ToolVersion != "2.0.0" {
		t.Fatalf("bare-name call resolved %q, want the listed 2.0.0", invoker.input.ToolVersion)
	}
	// An explicit pin still reaches an older granted version.
	if _, rpcErr := adapter.CallTool(context.Background(), json.RawMessage(`{"name":"weather@1.0.0","arguments":{}}`)); rpcErr != nil {
		t.Fatalf("pinned CallTool: %v", rpcErr)
	}
	if invoker.input.ToolVersion != "1.0.0" {
		t.Fatalf("explicit pin resolved %q, want 1.0.0", invoker.input.ToolVersion)
	}
}

type fakeInvoker struct {
	descriptors []store.ToolDescriptor
	input       tool.InvokeInput
	calls       int
	keys        []string
	denied      bool
	approvalID  uuid.UUID
	replay      bool
	err         error
}

func (f *fakeInvoker) ListTools(context.Context, string) ([]store.ToolDescriptor, error) {
	return f.descriptors, nil
}

func (f *fakeInvoker) InvokeTool(_ context.Context, in tool.InvokeInput) (tool.InvokeResult, error) {
	f.calls++
	f.input = in
	f.keys = append(f.keys, in.IdempotencyKey)
	if f.err != nil {
		return tool.InvokeResult{}, f.err
	}
	if f.denied {
		return tool.InvokeResult{Outcome: tool.OutcomeDenied, DenyReasons: []string{"TOOL_NOT_ALLOWED"}}, nil
	}
	if f.approvalID != uuid.Nil {
		return tool.InvokeResult{Outcome: tool.OutcomeRequiresApproval, ApprovalID: &f.approvalID}, nil
	}
	if f.replay {
		return tool.InvokeResult{Outcome: tool.OutcomeReplayed, Result: json.RawMessage(`{"ok":true}`), ReceiptOperation: "TOOL:fs.read@1.0.0"}, nil
	}
	return tool.InvokeResult{Outcome: tool.OutcomeExecuted, Result: json.RawMessage(`{"ok":true}`), ReceiptOperation: "TOOL:fs.read@1.0.0"}, nil
}
