package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

// vectorsFile is the shared golden document every SDK implementation and the
// OpenAPI contract are checked against.
const vectorsFile = "fixtures/runtime-interface-v1.json"

// Vectors is the on-disk shape of the golden document.
type Vectors struct {
	Schema      string              `json:"schema"`
	Protocol    string              `json:"protocol"`
	ExecutionID string              `json:"executionId"`
	Enums       map[string][]string `json:"enums"`
	Shapes      map[string][]string `json:"shapes"`
	Endpoints   []VectorEndpoint    `json:"endpoints"`
}

// VectorEndpoint is one request with the response it must produce.
type VectorEndpoint struct {
	Name           string          `json:"name"`
	Method         string          `json:"method"`
	Path           string          `json:"path"`
	Request        json.RawMessage `json:"request"`
	Status         int             `json:"status"`
	RequiredFields []string        `json:"requiredFields"`
	OptionalFields []string        `json:"optionalFields"`
	Values         map[string]any  `json:"values"`
}

func loadVectors(t *testing.T) Vectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(vectorsFile))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vectors Vectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if vectors.Schema != "fenced.runtime-interface.vectors/v1" {
		t.Fatalf("unexpected vector schema %q", vectors.Schema)
	}
	return vectors
}

// vectorRuntime is the deterministic runtime the vectors run against: it
// emits one event, answers a checkpoint and waits for the cancellation, so
// every vector observes a stable execution state.
type vectorRuntime struct{ released chan struct{} }

func (r *vectorRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	if err := emit("vector.started", json.RawMessage(`{"probe":"vector"}`)); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return json.RawMessage(`{"cancelled":true}`), nil
	case <-r.released:
		return json.RawMessage(`{"done":true}`), nil
	}
}

func (r *vectorRuntime) Checkpoint(context.Context, string) (agent.Checkpoint, error) {
	return agent.Checkpoint{
		SchemaVersion: "vector/v1",
		State:         json.RawMessage(`{"step":"vector"}`),
		CreatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (r *vectorRuntime) Restore(context.Context, agent.RestoreRequest) error { return nil }

// TestRuntimeInterfaceVectors runs the shared golden vectors against the Go
// host. The same document is run by the Python SDK and checked against the
// OpenAPI contract, so the three cannot drift apart.
func TestRuntimeInterfaceVectors(t *testing.T) {
	vectors := loadVectors(t)
	runtime := &vectorRuntime{released: make(chan struct{})}
	defer close(runtime.released)

	host, err := agent.NewHost(runtime, agent.HostOptions{Adapter: "vector"})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	server := httptest.NewServer(host)
	defer server.Close()

	for _, vector := range vectors.Endpoints {
		t.Run(vector.Name, func(t *testing.T) {
			path := strings.ReplaceAll(vector.Path, "{executionId}", vectors.ExecutionID)
			var body []byte
			if len(vector.Request) > 0 {
				body = vector.Request
			}
			request, err := http.NewRequest(vector.Method, server.URL+path, bytes.NewReader(body))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if body != nil {
				request.Header.Set("Content-Type", "application/json")
			}
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != vector.Status {
				t.Fatalf("status = %d, want %d", response.StatusCode, vector.Status)
			}
			var document map[string]any
			if err := json.NewDecoder(response.Body).Decode(&document); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			present := map[string]bool{}
			for key := range document {
				present[key] = true
			}
			for _, field := range vector.RequiredFields {
				if !present[field] {
					t.Errorf("required field %q is missing from %v", field, document)
				}
			}
			known := map[string]bool{}
			for _, field := range vector.RequiredFields {
				known[field] = true
			}
			for _, field := range vector.OptionalFields {
				known[field] = true
			}
			for key := range present {
				if !known[key] {
					t.Errorf("unknown field %q in the response %v", key, document)
				}
			}
			for key, want := range vector.Values {
				got, ok := document[key]
				if !ok {
					t.Errorf("field %q is missing from %v", key, document)
					continue
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("field %q = %v, want %v", key, got, want)
				}
			}
		})
	}
}

// TestRuntimeInterfaceVectorEnums pins the status vocabulary of the
// interface. A new status value is a contract change, not an implementation
// detail.
func TestRuntimeInterfaceVectorEnums(t *testing.T) {
	vectors := loadVectors(t)
	statuses := vectors.Enums["executionStatus"]
	if len(statuses) == 0 {
		t.Fatal("the vectors must name the execution status vocabulary")
	}
	declared := map[string]bool{
		agent.StatusAccepted:  true,
		agent.StatusRunning:   true,
		agent.StatusSucceeded: true,
		agent.StatusFailed:    true,
		agent.StatusCancelled: true,
	}
	for _, status := range statuses {
		if !declared[status] {
			t.Errorf("the vectors name the status %q, which the Go SDK does not declare", status)
		}
		delete(declared, status)
	}
	for status := range declared {
		t.Errorf("the Go SDK declares the status %q, which the vectors do not name", status)
	}
}
