// Package hello is a minimal third-party agent. Its author wrote ONLY the
// business logic in this file: the platform supplies scheduling, capability,
// budget, isolation, recovery and audit around it. The agent knows nothing
// about leases, fencing tokens, schedulers, runtime pools, or budget ledgers
// (design plan §7 "Third-party developers do not need to understand kernel internals...").
package hello

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

// Runtime implements agent.Runtime behind the brokered MCP surface.
type Runtime struct {
	mcp   *HTTPMCPClient
	mu    sync.Mutex
	state map[string]json.RawMessage
}

// NewRuntime binds the agent to the loopback MCP endpoint.
func NewRuntime(mcpEndpoint string) *Runtime {
	return &Runtime{mcp: NewHTTPMCPClient(mcpEndpoint), state: map[string]json.RawMessage{}}
}

// Run executes one task: it invokes one tenant tool through the platform
// broker and returns the result. This is the whole "agent logic".
func (r *Runtime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("hello.started", mustJSON(map[string]any{"goal": request.Goal}))
	raw, err := r.mcp.CallTool(ctx, request.ExecutionID, "hello.echo@1.0.0", map[string]any{
		"message": request.Goal,
	})
	if err != nil {
		return nil, fmt.Errorf("hello tool: %w", err)
	}
	output, _ := json.Marshal(map[string]any{
		"agent": "hello-agent", "toolResult": json.RawMessage(raw), "goal": request.Goal,
	})
	r.mu.Lock()
	r.state[request.ExecutionID] = output
	r.mu.Unlock()
	_ = emit("hello.completed", mustJSON(map[string]any{"outputBytes": len(output)}))
	return output, nil
}

// Checkpoint implements agent.Runtime.
func (r *Runtime) Checkpoint(_ context.Context, executionID string) (agent.Checkpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw := r.state[executionID]
	if len(raw) == 0 {
		raw = json.RawMessage(`{"phase":"running"}`)
	}
	return agent.Checkpoint{SchemaVersion: "hello/v1", State: raw, CreatedAt: time.Now().UTC()}, nil
}

// Restore implements agent.Runtime.
func (r *Runtime) Restore(_ context.Context, request agent.RestoreRequest) error {
	if request.Checkpoint.SchemaVersion != "hello/v1" {
		return fmt.Errorf("incompatible checkpoint schema %q", request.Checkpoint.SchemaVersion)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state[request.ExecutionID] = request.Checkpoint.State
	return nil
}

// HTTPMCPClient posts JSON-RPC tool calls to the loopback MCP endpoint,
// carrying the attempt execution id in the fencing header.
type HTTPMCPClient struct {
	endpoint string
	http     *http.Client
}

const executionHeader = "X-Fenced-Execution"

func NewHTTPMCPClient(endpoint string) *HTTPMCPClient {
	return &HTTPMCPClient{endpoint: strings.TrimRight(endpoint, "/"), http: &http.Client{Timeout: 10 * time.Minute}}
}

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

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("{}")
	}
	return encoded
}
