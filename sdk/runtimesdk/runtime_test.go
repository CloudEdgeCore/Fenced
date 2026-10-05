package runtimesdk_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/CloudEdgeCore/Fenced/sdk/conformance"
	"github.com/CloudEdgeCore/Fenced/sdk/runtimesdk"
)

type mockRuntimeHandler struct {
	mu         sync.Mutex
	executions map[string]agent.StartRequest
	results    map[string]agent.Result
	events     map[string][]agent.Event
	states     map[string]agent.Checkpoint
}

func newMockHandler() *mockRuntimeHandler {
	return &mockRuntimeHandler{
		executions: make(map[string]agent.StartRequest),
		results:    make(map[string]agent.Result),
		events:     make(map[string][]agent.Event),
		states:     make(map[string]agent.Checkpoint),
	}
}

func (m *mockRuntimeHandler) Health(ctx context.Context) (agent.HealthResponse, error) {
	return agent.HealthResponse{
		Status:           "SERVING",
		ProtocolVersions: []string{agent.ProtocolVersion},
		Adapter:          "runtimesdk-test",
		MaxConcurrent:    10,
		ActiveExecutions: len(m.executions),
	}, nil
}

func (m *mockRuntimeHandler) Start(ctx context.Context, req agent.StartRequest) (agent.StartResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, exists := m.executions[req.ExecutionID]; exists {
		if existing.Goal != req.Goal {
			return agent.StartResponse{}, agent.ErrExecutionConflict
		}
		return agent.StartResponse{
			ExecutionID: req.ExecutionID,
			Status:      agent.StatusRunning,
			Replayed:    true,
		}, nil
	}

	m.executions[req.ExecutionID] = req
	now := time.Now().UTC()
	m.events[req.ExecutionID] = []agent.Event{
		{
			Sequence:   1,
			Type:       "agent.started",
			Payload:    json.RawMessage(`{"status":"running"}`),
			OccurredAt: now,
		},
	}
	status := agent.StatusSucceeded
	if string(req.Input) == `{"blockUntilStopped":true}` {
		status = agent.StatusRunning
	}
	m.results[req.ExecutionID] = agent.Result{
		ExecutionID: req.ExecutionID,
		Status:      status,
		Output:      json.RawMessage(`{"result":"completed"}`),
		CompletedAt: &now,
	}
	m.states[req.ExecutionID] = agent.Checkpoint{
		SchemaVersion: "agent/v1",
		State:         json.RawMessage(`{"step":1}`),
		CreatedAt:     now,
	}

	return agent.StartResponse{
		ExecutionID: req.ExecutionID,
		Status:      agent.StatusAccepted,
		Replayed:    false,
	}, nil
}

func (m *mockRuntimeHandler) Event(ctx context.Context, executionID string, after int64) (agent.EventList, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	evs := m.events[executionID]
	var filtered []agent.Event
	for _, e := range evs {
		if e.Sequence > after {
			filtered = append(filtered, e)
		}
	}
	next := after
	if len(evs) > 0 {
		next = evs[len(evs)-1].Sequence
	}
	return agent.EventList{
		ExecutionID: executionID,
		Events:      filtered,
		NextAfter:   next,
	}, nil
}

func (m *mockRuntimeHandler) Result(ctx context.Context, executionID string) (agent.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	res, ok := m.results[executionID]
	if !ok {
		return agent.Result{}, agent.ErrExecutionNotFound
	}
	return res, nil
}

func (m *mockRuntimeHandler) Checkpoint(ctx context.Context, executionID string) (agent.CheckpointResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, ok := m.states[executionID]
	if !ok {
		return agent.CheckpointResponse{}, agent.ErrExecutionNotFound
	}
	return agent.CheckpointResponse{
		ExecutionID: executionID,
		Checkpoint:  cp,
	}, nil
}

func (m *mockRuntimeHandler) Restore(ctx context.Context, req agent.RestoreRequest) (agent.RestoreResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.states[req.ExecutionID] = req.Checkpoint
	return agent.RestoreResponse{
		ExecutionID: req.ExecutionID,
		Restored:    true,
	}, nil
}

func (m *mockRuntimeHandler) Stop(ctx context.Context, executionID string) (agent.StopResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	m.results[executionID] = agent.Result{
		ExecutionID: executionID,
		Status:      agent.StatusCancelled,
		CompletedAt: &now,
	}
	return agent.StopResponse{
		ExecutionID: executionID,
		Status:      agent.StatusCancelled,
	}, nil
}

func TestRuntimeSDKPassesConformance(t *testing.T) {
	handler := newMockHandler()
	server := httptest.NewServer(runtimesdk.HandlerToHTTP(handler))
	defer server.Close()

	client, err := agent.NewClient(server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	report, err := conformance.Run(context.Background(), client)
	if err != nil {
		t.Fatalf("conformance failed: %v", err)
	}

	if len(report.Checks) != 12 {
		t.Fatalf("expected 12 checks passed, got %d: %+v", len(report.Checks), report.Checks)
	}
}
