package devops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

// HTTPMCPClient posts JSON-RPC tool calls to the runtime's loopback MCP
// endpoint, carrying the attempt execution id in the fencing header exactly
// like the research reference runtime.
type HTTPMCPClient struct {
	endpoint string
	http     *http.Client
}

const executionHeader = "X-Fenced-Execution"

// NewHTTPMCPClient binds the client to the MCP endpoint URL.
func NewHTTPMCPClient(endpoint string) *HTTPMCPClient {
	return &HTTPMCPClient{endpoint: strings.TrimRight(endpoint, "/"), http: &http.Client{Timeout: 10 * time.Minute}}
}

// CallTool implements MCPClient.
func (c *HTTPMCPClient) CallTool(ctx context.Context, executionID, name string, callArgs any) (json.RawMessage, error) {
	arguments, err := json.Marshal(callArgs)
	if err != nil {
		return nil, fmt.Errorf("encode arguments: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": json.RawMessage(arguments)},
	})
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set(executionHeader, executionID)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mcp http %d: %.200s", response.StatusCode, body)
	}
	var envelope struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode mcp response: %w", err)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("mcp tool %s: %s", name, envelope.Error.Message)
	}
	if len(envelope.Result.Content) == 0 {
		return nil, fmt.Errorf("mcp tool %s returned no content", name)
	}
	return json.RawMessage(envelope.Result.Content[0].Text), nil
}

// Runtime implements agent.Runtime behind the broker.
type Runtime struct {
	mcp    MCPClient
	models Models
	mu     sync.Mutex
	state  map[string]json.RawMessage
}

// NewRuntime builds the multi-role DevOps runtime.
func NewRuntime(mcpEndpoint string, models Models) *Runtime {
	return &Runtime{
		mcp:    NewHTTPMCPClient(mcpEndpoint),
		models: models,
		state:  map[string]json.RawMessage{},
	}
}

// Run implements agent.Runtime.
func (r *Runtime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("devops.started", mustJSON(map[string]any{
		"agentVersionRef": request.AgentVersionRef, "role": RoleFromRef(request.AgentVersionRef),
	}))
	output, err := Run(ctx, Deps{MCP: r.mcp, Models: r.models, ExecutionID: request.ExecutionID},
		request.AgentVersionRef, request.Goal)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.state[request.ExecutionID] = output
	r.mu.Unlock()
	_ = emit("devops.completed", mustJSON(map[string]any{"outputBytes": len(output)}))
	return output, nil
}

// Checkpoint implements agent.Runtime with versioned logical state.
func (r *Runtime) Checkpoint(_ context.Context, executionID string) (agent.Checkpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw := r.state[executionID]
	return agent.Checkpoint{State: raw, SchemaVersion: "devops/v1", CreatedAt: time.Now().UTC()}, nil
}

// Restore implements agent.Runtime; restores are idempotent by contract.
func (r *Runtime) Restore(_ context.Context, request agent.RestoreRequest) error {
	if request.Checkpoint.SchemaVersion != "devops/v1" {
		return fmt.Errorf("incompatible checkpoint schema %q", request.Checkpoint.SchemaVersion)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state[request.ExecutionID] = request.Checkpoint.State
	return nil
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("{}")
	}
	return encoded
}
