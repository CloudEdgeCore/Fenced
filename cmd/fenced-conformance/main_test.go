package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type conformantRuntime struct {
	mu     sync.Mutex
	states map[string]json.RawMessage
}

func (r *conformantRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	if err := emit("agent.started", json.RawMessage(`{}`)); err != nil {
		return nil, err
	}
	if strings.Contains(string(request.Input), "blockUntilStopped") {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	output := json.RawMessage(`{"ok":true}`)
	r.mu.Lock()
	r.states[request.ExecutionID] = output
	r.mu.Unlock()
	return output, nil
}

func (r *conformantRuntime) Checkpoint(_ context.Context, executionID string) (agent.Checkpoint, error) {
	return agent.Checkpoint{SchemaVersion: "test/v1", State: json.RawMessage(`{"ok":true}`), CreatedAt: time.Now().UTC()}, nil
}

func (r *conformantRuntime) Restore(_ context.Context, request agent.RestoreRequest) error {
	r.mu.Lock()
	r.states[request.ExecutionID] = request.Checkpoint.State
	r.mu.Unlock()
	return nil
}

func TestRunEmitsMachineReadableCertification(t *testing.T) {
	host, err := agent.NewHost(&conformantRuntime{states: map[string]json.RawMessage{}}, agent.HostOptions{Adapter: "certified"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"-endpoint", server.URL, "-json"}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	var result certification
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || !result.Passed || result.Schema != "fenced.conformance/v1" || result.Report.Adapter != "certified" || result.Compatible != "PASS" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestRunEmitsHumanReadablePass(t *testing.T) {
	host, err := agent.NewHost(&conformantRuntime{states: map[string]json.RawMessage{}}, agent.HostOptions{Adapter: "certified"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	var output bytes.Buffer
	if err := run([]string{"-endpoint", server.URL}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	outputStr := output.String()
	if !strings.Contains(outputStr, "Fenced Compatible = PASS") {
		t.Fatalf("expected output to contain 'Fenced Compatible = PASS', got:\n%s", outputStr)
	}
}
