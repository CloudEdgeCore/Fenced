package research

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
// like the reference Python SDK.
type HTTPMCPClient struct {
	endpoint string
	http     *http.Client
}

// MCPToolError is the bounded, machine-readable failure returned by an MCP
// tool outcome. Payload preserves the structured document for diagnostics;
// Code is safe to classify without parsing provider or endpoint prose.
type MCPToolError struct {
	Code    string
	Message string
}

func (e *MCPToolError) Error() string {
	if e.Message == "" || e.Message == e.Code {
		return e.Code
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// NewHTTPMCPClient binds the client to the MCP endpoint URL.
func NewHTTPMCPClient(endpoint string) *HTTPMCPClient {
	return &HTTPMCPClient{endpoint: strings.TrimRight(endpoint, "/"), http: &http.Client{Timeout: 10 * time.Minute}}
}

const executionHeader = "X-Fenced-Execution"

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
	var document struct {
		Result *struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, fmt.Errorf("decode mcp response: %w", err)
	}
	if document.Error != nil {
		return nil, fmt.Errorf("mcp error %d: %s", document.Error.Code, document.Error.Message)
	}
	if document.Result == nil || len(document.Result.Content) == 0 {
		return nil, fmt.Errorf("mcp result carried no content")
	}
	text := document.Result.Content[0].Text
	if document.Result.IsError {
		var outcome struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal([]byte(text), &outcome) == nil && outcome.Error != "" {
			return json.RawMessage(text), &MCPToolError{Code: outcome.Error, Message: outcome.Message}
		}
		return json.RawMessage(text), &MCPToolError{Message: text}
	}
	return json.RawMessage(text), nil
}

// Runtime hosts the research roles behind the Fenced Runtime Interface. One
// process serves every agent version; the version reference selects the role
// so a single deployment can host the whole workflow fleet.
type Runtime struct {
	mcp    MCPClient
	models Models

	mu    sync.RWMutex
	state map[string]json.RawMessage // execution id -> logical checkpoint
}

// NewRuntime builds the multi-role runtime.
func NewRuntime(mcpEndpoint string, models Models) *Runtime {
	return &Runtime{
		mcp:    NewHTTPMCPClient(mcpEndpoint),
		models: models,
		state:  map[string]json.RawMessage{},
	}
}

// Run implements agent.Runtime.
func (r *Runtime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("research.started", mustJSON(map[string]any{
		"agentVersionRef": request.AgentVersionRef, "role": RoleFromRef(request.AgentVersionRef),
	}))
	output, err := Run(ctx, Deps{MCP: r.mcp, Models: r.models}, request.ExecutionID, request.AgentVersionRef, request.Goal)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.state[request.ExecutionID] = output
	r.mu.Unlock()
	_ = emit("research.completed", mustJSON(map[string]any{"outputBytes": len(output)}))
	return output, nil
}

// Checkpoint implements agent.Runtime with versioned logical state.
func (r *Runtime) Checkpoint(_ context.Context, executionID string) (agent.Checkpoint, error) {
	r.mu.RLock()
	state, ok := r.state[executionID]
	r.mu.RUnlock()
	if !ok || len(state) == 0 {
		state = json.RawMessage(`{"phase":"running"}`)
	}
	return agent.Checkpoint{SchemaVersion: "research/v1", State: state, CreatedAt: time.Now().UTC()}, nil
}

// Restore implements agent.Runtime; restores are idempotent by contract.
func (r *Runtime) Restore(_ context.Context, request agent.RestoreRequest) error {
	if request.Checkpoint.SchemaVersion != "research/v1" {
		return fmt.Errorf("incompatible checkpoint schema %q", request.Checkpoint.SchemaVersion)
	}
	r.mu.Lock()
	r.state[request.ExecutionID] = request.Checkpoint.State
	r.mu.Unlock()
	return nil
}

func mustJSON(value any) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}
