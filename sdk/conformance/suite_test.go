package conformance

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type conformantRuntime struct{}

func (conformantRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	if err := emit("conformance.started", json.RawMessage("{\"ok\":true}")); err != nil {
		return nil, err
	}
	var input map[string]any
	_ = json.Unmarshal(request.Input, &input)
	if block, _ := input["blockUntilStopped"].(bool); block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return json.RawMessage("{\"ok\":true}"), nil
}

func (conformantRuntime) Checkpoint(context.Context, string) (agent.Checkpoint, error) {
	return agent.Checkpoint{
		SchemaVersion: "test/v1", State: json.RawMessage("{\"ok\":true}"), CreatedAt: time.Now().UTC(),
	}, nil
}
func (conformantRuntime) Restore(context.Context, agent.RestoreRequest) error { return nil }

func TestRunCertifiesNativeGoAdapter(t *testing.T) {
	host, err := agent.NewHost(conformantRuntime{}, agent.HostOptions{Adapter: "native-go"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	client, err := agent.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := Run(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if report.Adapter != "native-go" || len(report.Checks) < 10 {
		t.Fatalf("incomplete report: %+v", report)
	}
}
