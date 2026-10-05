package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	kernelmodel "github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
)

type fakeModelBroker struct {
	input  kernelmodel.InvokeInput
	output kernelmodel.InvokeOutput
	err    error
}

func (f *fakeModelBroker) InvokeStream(_ context.Context, input kernelmodel.InvokeInput, _ func(string)) (kernelmodel.InvokeOutput, error) {
	f.input = input
	return f.output, f.err
}

type fakeMemoryBroker struct {
	putInput    memory.PutInput
	searchInput memory.SearchInput
	records     []store.MemoryRecord
}

type fakeWorkflowSpawner struct {
	input   SpawnRequest
	output  SpawnOutcome
	invoked int
}

func (f *fakeWorkflowSpawner) Spawn(_ context.Context, input SpawnRequest) (SpawnOutcome, error) {
	f.input = input
	f.invoked++
	return f.output, nil
}

func (f *fakeMemoryBroker) Put(_ context.Context, _ AttemptContext, input memory.PutInput) (store.MemoryRecord, bool, error) {
	f.putInput = input
	return store.MemoryRecord{
		ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), TenantID: input.TenantID,
		Namespace: input.Namespace, Key: input.Key, ResourceVersion: 1,
	}, false, nil
}

func (f *fakeMemoryBroker) Search(_ context.Context, _ AttemptContext, input memory.SearchInput) ([]store.MemoryRecord, error) {
	f.searchInput = input
	if f.records != nil {
		return f.records, nil
	}
	return []store.MemoryRecord{{
		ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), TenantID: input.TenantID,
		Namespace: "runs", Key: "note", Content: "recall me", ResourceVersion: 3,
	}}, nil
}

type fakeToolInvokerForBroker struct {
	listed []store.ToolDescriptor
}

func (f *fakeToolInvokerForBroker) InvokeTool(_ context.Context, input tool.InvokeInput) (tool.InvokeResult, error) {
	return tool.InvokeResult{}, nil
}
func (f *fakeToolInvokerForBroker) ListTools(_ context.Context, _ string) ([]store.ToolDescriptor, error) {
	return f.listed, nil
}

func brokerTestContext() AttemptContext {
	return AttemptContext{
		TenantID: "tenant-1", TaskID: uuid.New(), RunID: uuid.New(), AttemptID: uuid.New(),
		FencingToken: 5, AgentVersionRef: "agent@1",
		AllowedModels:           []string{"fake/agent-model"},
		AllowedMemoryNamespaces: []string{"runs"},
	}
}

func newBrokerForTest(t *testing.T, identity AttemptContext, models ModelBroker, memories MemoryBroker, listed []store.ToolDescriptor) *Broker {
	t.Helper()
	slot := &StaticIdentity{Context: identity}
	tools := NewToolAdapter(&fakeToolInvokerForBroker{listed: listed}, slot)
	return NewBroker(tools, models, memories, nil, nil, slot)
}

func decodeToolJSON(t *testing.T, result any) map[string]any {
	t.Helper()
	document, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result is not an MCP result object: %#v", result)
	}
	content, ok := document["content"].([]map[string]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %#v", document["content"])
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(content[0]["text"].(string)), &parsed); err != nil {
		t.Fatalf("decode tool payload: %v", err)
	}
	return parsed
}

func TestBrokerListsSystemToolsAlongsideTenantTools(t *testing.T) {
	broker := newBrokerForTest(t, brokerTestContext(), &fakeModelBroker{}, &fakeMemoryBroker{},
		[]store.ToolDescriptor{{Name: "echo.dev", Version: "1.0.0", ParamsSchema: []byte(`{"type":"object"}`)}})
	listed, rpcErr := broker.ListTools(context.Background(), json.RawMessage(`{}`))
	if rpcErr != nil {
		t.Fatalf("list: %v", rpcErr)
	}
	names := map[string]bool{}
	for _, tool := range listed.(map[string]any)["tools"].([]map[string]any) {
		names[tool["name"].(string)] = true
	}
	for _, expected := range []string{"echo.dev", SystemModelInvoke, SystemMemoryPut, SystemMemorySearch} {
		if !names[expected] {
			t.Fatalf("tool %q missing from %v", expected, names)
		}
	}
}

func TestBrokerModelInvokeEnforcesGrantAndFencing(t *testing.T) {
	models := &fakeModelBroker{
		output: kernelmodel.InvokeOutput{
			Call: store.ModelCall{ID: uuid.MustParse("00000000-0000-0000-0000-00000000000a"),
				TenantID: "tenant-1", ModelRef: "fake/agent-model", Status: store.ModelCallCompleted,
				InputTokens: 30, OutputTokens: 12, CostMicroUSD: money.MustFromUSD(0.0002), FinishReason: "stop", ProviderRequestID: "req-1"},
			Content: "the answer",
		},
	}
	broker := newBrokerForTest(t, brokerTestContext(), models, nil, nil)

	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemModelInvoke,
		"arguments": map[string]any{"modelRef": "fake/agent-model", "messages": []map[string]any{{"role": "user", "content": "hi"}}},
	}))
	if rpcErr != nil {
		t.Fatalf("call: %v", rpcErr)
	}
	payload := decodeToolJSON(t, result)
	if payload["status"] != "COMPLETED" || payload["content"] != "the answer" {
		t.Fatalf("payload = %#v", payload)
	}
	usage := payload["usage"].(map[string]any)
	if usage["inputTokens"] != float64(30) || usage["outputTokens"] != float64(12) {
		t.Fatalf("usage = %#v", usage)
	}
	// The invocation carries the fenced identity injected by the runtime.
	if models.input.TenantID != "tenant-1" || models.input.FencingToken != 5 || models.input.AgentVersionRef != "agent@1" {
		t.Fatalf("invocation identity = %+v", models.input)
	}
	if !strings.HasPrefix(models.input.IdempotencyKey, "mcp/") {
		t.Fatalf("idempotency key = %q", models.input.IdempotencyKey)
	}

	// A model outside the grant is denied as a tool outcome.
	result, rpcErr = broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemModelInvoke,
		"arguments": map[string]any{"modelRef": "other/model", "messages": []map[string]any{{"role": "user", "content": "hi"}}},
	}))
	if rpcErr != nil {
		t.Fatalf("call: %v", rpcErr)
	}
	payload = decodeToolJSON(t, result)
	if payload["error"] == nil || !strings.Contains(payload["error"].(string), "capability grant") {
		t.Fatalf("expected capability denial, got %#v", payload)
	}
}

func TestBrokerModelInvokeGeneratesToolSchemaAndPinsVersion(t *testing.T) {
	// Two granted versions of one tool with DISTINCT parameter schemas. The
	// agent supplies only tool NAMES; the broker must attach the registry's
	// schema (P0-01: never trust an agent-submitted contract) and honor a
	// pinned "name@version" reference (P1-08).
	descriptors := []store.ToolDescriptor{
		{Name: "weather", Version: "1.0.0", SideEffectRisk: "low", ParamsSchema: []byte(`{"type":"object","properties":{"city":{"type":"string"}}}`)},
		{Name: "weather", Version: "1.2.0", SideEffectRisk: "low", ParamsSchema: []byte(`{"type":"object","properties":{"city":{"type":"string"},"unit":{"type":"string"}}}`)},
	}
	models := &fakeModelBroker{output: kernelmodel.InvokeOutput{
		Call:      store.ModelCall{ID: uuid.New(), TenantID: "tenant-1", ModelRef: "fake/agent-model", Status: store.ModelCallCompleted},
		Content:   "ok",
		ToolCalls: []provider.ToolCall{{ID: "call-1", Name: "weather", Arguments: `{"city":"上海"}`}},
	}}
	broker := newBrokerForTest(t, brokerTestContext(), models, nil, descriptors)

	invoke := func(t *testing.T, tools []string) map[string]any {
		t.Helper()
		result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
			"name": SystemModelInvoke,
			"arguments": map[string]any{
				"modelRef": "fake/agent-model",
				"messages": []map[string]any{{"role": "user", "content": "weather?"}},
				"tools":    tools,
			},
		}))
		if rpcErr != nil {
			t.Fatalf("call: %v", rpcErr)
		}
		return decodeToolJSON(t, result)
	}

	// Bare name resolves the latest granted version; the model sees the
	// platform schema, never a contract this process authored.
	payload := invoke(t, []string{"weather"})
	if len(models.input.Tools) != 1 {
		t.Fatalf("tools attached = %d, want 1", len(models.input.Tools))
	}
	if got := models.input.Tools[0]; got.Name != "weather" || string(got.Parameters) != string(descriptors[1].ParamsSchema) {
		t.Fatalf("bare tool = %+v, want latest-version schema %s", got, descriptors[1].ParamsSchema)
	}
	// The loop closes: the model's tool_calls surface back to the agent.
	calls, ok := payload["toolCalls"].([]any)
	if !ok || len(calls) != 1 || calls[0].(map[string]any)["name"] != "weather" {
		t.Fatalf("tool calls not surfaced to agent: %#v", payload["toolCalls"])
	}

	// Pinned reference resolves the exact older version and advertises the
	// pinned name, so the model's tool_calls refer back to that version.
	invoke(t, []string{"weather@1.0.0"})
	if got := models.input.Tools[0]; got.Name != "weather@1.0.0" || string(got.Parameters) != string(descriptors[0].ParamsSchema) {
		t.Fatalf("pinned tool = %+v, want weather@1.0.0 with its own schema", got)
	}

	// A pinned version that is not registered denies as a tool outcome.
	if payload := invoke(t, []string{"weather@9.9.9"}); payload["error"] == nil || !strings.Contains(payload["error"].(string), "weather@9.9.9") {
		t.Fatalf("expected pinned-miss denial, got %#v", payload)
	}

	// A name the AgentVersion is not granted denies too (fails closed).
	if payload := invoke(t, []string{"secrets.exfiltrate"}); payload["error"] == nil || !strings.Contains(payload["error"].(string), "secrets.exfiltrate") {
		t.Fatalf("expected ungranted denial, got %#v", payload)
	}
}

func TestBrokerModelInvokeReportsProviderFailure(t *testing.T) {
	models := &fakeModelBroker{
		output: kernelmodel.InvokeOutput{Call: store.ModelCall{ID: uuid.New(), Status: store.ModelCallFailed, FinishReason: "provider_rejected"}},
		err:    provider.ErrProviderRejected,
	}
	broker := newBrokerForTest(t, brokerTestContext(), models, nil, nil)
	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemModelInvoke,
		"arguments": map[string]any{"modelRef": "fake/agent-model", "messages": []map[string]any{{"role": "user", "content": "hi"}}},
	}))
	if rpcErr != nil {
		t.Fatalf("call: %v", rpcErr)
	}
	payload := decodeToolJSON(t, result)
	if payload["error"] != "PROVIDER_REJECTED" {
		t.Fatalf("failure payload = %#v", payload)
	}
}

func TestBrokerMemoryToolsEnforceNamespaceGrantAndProvenance(t *testing.T) {
	memories := &fakeMemoryBroker{}
	broker := newBrokerForTest(t, brokerTestContext(), nil, memories, nil)

	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemMemoryPut,
		"arguments": map[string]any{"namespace": "runs", "key": "fact-1", "content": "learned"},
	}))
	if rpcErr != nil {
		t.Fatalf("put: %v", rpcErr)
	}
	payload := decodeToolJSON(t, result)
	if payload["key"] != "fact-1" || payload["replayed"] != false {
		t.Fatalf("put payload = %#v", payload)
	}
	if memories.putInput.Provenance["agentVersionRef"] != "agent@1" || memories.putInput.Provenance["via"] != "mcp:"+SystemMemoryPut {
		t.Fatalf("provenance = %#v", memories.putInput.Provenance)
	}
	if memories.putInput.Provenance["via"] != "mcp:"+SystemMemoryPut {
		t.Fatalf("provenance = %#v", memories.putInput.Provenance)
	}

	// A namespace outside the grant is denied.
	result, _ = broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemMemoryPut,
		"arguments": map[string]any{"namespace": "secret", "key": "x", "content": "y"},
	}))
	payload = decodeToolJSON(t, result)
	if payload["error"] == nil || !strings.Contains(payload["error"].(string), "capability grant") {
		t.Fatalf("expected namespace denial, got %#v", payload)
	}

	result, rpcErr = broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemMemorySearch,
		"arguments": map[string]any{"query": "recall"},
	}))
	if rpcErr != nil {
		t.Fatalf("search: %v", rpcErr)
	}
	payload = decodeToolJSON(t, result)
	records := payload["records"].([]any)
	if len(records) != 1 || records[0].(map[string]any)["content"] != "recall me" {
		t.Fatalf("search payload = %#v", payload)
	}
	if memories.searchInput.TenantID != "" || memories.putInput.TenantID != "" {
		t.Fatal("broker must not pass tenant through agent-controlled input; the kernel adapter derives it from identity")
	}
}

func TestBrokerSpawnEnforcesCapabilityAllowlistAndFencedIdentity(t *testing.T) {
	identity := brokerTestContext()
	identity.WorkflowID = uuid.New()
	identity.ParentStepName = "planner"
	identity.CanSpawnTasks = true
	identity.AllowedChildAgents = []string{"worker@1"}
	spawner := &fakeWorkflowSpawner{output: SpawnOutcome{Code: "created", StepName: "child-1", SpawnDepth: 1}}
	slot := &StaticIdentity{Context: identity}
	broker := NewBroker(NewToolAdapter(&fakeToolInvokerForBroker{}, slot), nil, nil, spawner, nil, slot)

	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemTaskSpawn,
		"arguments": map[string]any{
			"name": "child-1", "goal": "execute the delegated task", "agentVersionRef": "worker@1",
		},
	}))
	if rpcErr != nil {
		t.Fatalf("spawn: %v", rpcErr)
	}
	payload := decodeToolJSON(t, result)
	if payload["outcome"] != "created" || payload["step"] != "child-1" {
		t.Fatalf("spawn payload = %#v", payload)
	}
	if spawner.invoked != 1 || spawner.input.TenantID != identity.TenantID ||
		spawner.input.AttemptID != identity.AttemptID || spawner.input.FencingToken != identity.FencingToken ||
		spawner.input.WorkflowID != identity.WorkflowID || spawner.input.ParentStepName != "planner" {
		t.Fatalf("spawn identity was not fenced: %+v", spawner.input)
	}

	denied := identity
	denied.CanSpawnTasks = false
	deniedSlot := &StaticIdentity{Context: denied}
	deniedBroker := NewBroker(NewToolAdapter(&fakeToolInvokerForBroker{}, deniedSlot), nil, nil, spawner, nil, deniedSlot)
	result, rpcErr = deniedBroker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemTaskSpawn, "arguments": map[string]any{"name": "child-2", "goal": "no", "agentVersionRef": "worker@1"},
	}))
	if rpcErr != nil {
		t.Fatalf("capability denial must be a tool outcome: %v", rpcErr)
	}
	if payload = decodeToolJSON(t, result); !strings.Contains(payload["error"].(string), "not granted") {
		t.Fatalf("spawn capability denial = %#v", payload)
	}

	denied.CanSpawnTasks = true
	denied.AllowedChildAgents = []string{"other@1"}
	deniedSlot = &StaticIdentity{Context: denied}
	deniedBroker = NewBroker(NewToolAdapter(&fakeToolInvokerForBroker{}, deniedSlot), nil, nil, spawner, nil, deniedSlot)
	result, rpcErr = deniedBroker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemTaskSpawn, "arguments": map[string]any{"name": "child-3", "goal": "no", "agentVersionRef": "worker@1"},
	}))
	if rpcErr != nil {
		t.Fatalf("allowlist denial must be a tool outcome: %v", rpcErr)
	}
	if payload = decodeToolJSON(t, result); !strings.Contains(payload["error"].(string), "allowlist") {
		t.Fatalf("child allowlist denial = %#v", payload)
	}
	if spawner.invoked != 1 {
		t.Fatalf("denied spawns reached the kernel: %d invocations", spawner.invoked)
	}
}

// closedWindowResolver simulates an MCP call outside any execution window.
type closedWindowResolver struct{}

func (closedWindowResolver) Resolve(context.Context) (AttemptContext, error) {
	return AttemptContext{}, errors.New("no active execution window")
}

func TestBrokerDeniesWithoutIdentity(t *testing.T) {
	models := &fakeModelBroker{
		output: kernelmodel.InvokeOutput{Call: store.ModelCall{ID: uuid.New(), Status: store.ModelCallCompleted}},
	}
	tools := NewToolAdapter(&fakeToolInvokerForBroker{}, closedWindowResolver{})
	broker := NewBroker(tools, models, &fakeMemoryBroker{}, nil, nil, closedWindowResolver{})
	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemModelInvoke,
		"arguments": map[string]any{"modelRef": "fake/agent-model", "messages": []map[string]any{{"role": "user", "content": "hi"}}},
	}))
	if rpcErr != nil {
		t.Fatalf("call: %v", rpcErr)
	}
	payload := decodeToolJSON(t, result)
	if !strings.Contains(payload["error"].(string), "no fenced attempt identity") {
		t.Fatalf("expected default deny, got %#v", payload)
	}
}

func TestBrokerDelegatesTenantTools(t *testing.T) {
	models := &fakeModelBroker{
		output: kernelmodel.InvokeOutput{Call: store.ModelCall{ID: uuid.New(), Status: store.ModelCallCompleted}},
	}
	broker := newBrokerForTest(t, brokerTestContext(), models, &fakeMemoryBroker{}, nil)
	// The tenant tool path (unknown to the broker) is delegated: it fails in
	// the fake invoker with unknown-tool, proving dispatch reached it.
	_, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": "some.tenant.tool", "arguments": map[string]any{},
	}))
	if rpcErr == nil || !strings.Contains(fmt.Sprintf("%v", rpcErr.Data), "unknown tool") {
		t.Fatalf("expected delegated unknown-tool error, got %v", rpcErr)
	}
	if models.input.ModelRef != "" {
		t.Fatal("model broker must not be invoked for tenant tools")
	}
}

func TestBrokerFailsClosedWithoutBrokers(t *testing.T) {
	identity := brokerTestContext()
	identity.AllowedModels = nil
	tools := NewToolAdapter(&fakeToolInvokerForBroker{}, &StaticIdentity{Context: identity})
	broker := NewBroker(tools, nil, nil, nil, nil, &StaticIdentity{Context: identity})
	_, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name":      SystemModelInvoke,
		"arguments": map[string]any{"modelRef": "fake/agent-model", "messages": []map[string]any{{"role": "user", "content": "hi"}}},
	}))
	if rpcErr == nil {
		t.Fatal("model invocation must fail closed without a broker")
	}
	listed, _ := broker.ListTools(context.Background(), json.RawMessage(`{}`))
	if tools := listed.(map[string]any)["tools"].([]map[string]any); len(tools) != 0 {
		t.Fatalf("system tools must not be listed without brokers: %v", tools)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return encoded
}
