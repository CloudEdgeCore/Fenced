package reference

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/google/uuid"
)

// TestWorkerMcpEndpointForwardsFencedIdentity proves the sandbox Agent MCP
// entry: the worker's identity slot injects the current Attempt's fenced
// context, the MCP adapter forwards calls through the gRPC Tool Gateway
// boundary (with fencing token), and calls outside an execution window are
// denied by default.
func TestWorkerMcpEndpointForwardsFencedIdentity(t *testing.T) {
	repository := newFakeRuntimeStore()
	invoker := &scriptedInvoker{descriptors: []store.ToolDescriptor{
		{Name: "fs.read", Version: "1.0.0", SideEffectRisk: store.ToolRiskLow, Actions: []string{"read"},
			ResourcePatterns: []string{"fs:/tmp"},
			ParamsSchema:     []byte(`{"type":"object","properties":{"path":{"type":"string"}}}`)},
	}}
	worker := newTestWorkerWithGateway(t, repository, "worker-1", invoker)
	slot := NewIdentitySlot()
	worker.WithIdentitySlot(slot)
	adapter := mcp.NewToolAdapter(NewGrpcToolInvoker(worker.toolGateway), slot)
	server := httptest.NewServer(mcp.NewServer("fenced-runtime", "v0.1", adapter))
	t.Cleanup(server.Close)

	// Outside an execution window: default deny (no fenced identity).
	result := mcpCall(t, server.URL, "tools/list", map[string]any{})
	if _, ok := result["error"]; !ok {
		t.Fatalf("tools/list without identity must fail closed: %+v", result)
	}

	// Inside the window: the fenced attempt context is injected and forwarded.
	slot.Set(mcp.AttemptContext{
		TenantID: "tenant-a", TaskID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		RunID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		AttemptID:    uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		FencingToken: 1, AgentVersionRef: "agent@1",
	})
	listed := mcpCall(t, server.URL, "tools/list", map[string]any{})
	tools := listed["tools"].([]any)
	if len(tools) != 1 || tools[0].(map[string]any)["name"] != "fs.read" {
		t.Fatalf("tools: %+v", tools)
	}
	called := mcpCall(t, server.URL, "tools/call", map[string]any{
		"name": "fs.read", "arguments": map[string]any{"path": "a.txt"},
	})
	if called["isError"] != false {
		t.Fatalf("call result: %+v", called)
	}
	if invoker.input.AttemptID.String() != "33333333-3333-3333-3333-333333333333" ||
		invoker.input.FencingToken != 1 || invoker.input.ToolName != "fs.read" {
		t.Fatalf("forwarded input: %+v", invoker.input)
	}
}

// mcpCall performs one JSON-RPC call and returns the raw result document.
func mcpCall(t *testing.T, url string, method string, params map[string]any) map[string]any {
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
