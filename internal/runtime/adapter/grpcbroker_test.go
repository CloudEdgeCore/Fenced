package adapter

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"

	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	kernelmodel "github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type recordingModelService struct {
	modelv1.UnimplementedModelInvocationServiceServer
	mu      sync.Mutex
	request *modelv1.InvokeRequest
}

func (s *recordingModelService) Invoke(request *modelv1.InvokeRequest, stream modelv1.ModelInvocationService_InvokeServer) error {
	s.mu.Lock()
	s.request = request
	s.mu.Unlock()
	return stream.Send(&modelv1.InvokeResponse{Finish: &modelv1.InvokeFinish{
		CallId: uuid.NewString(), Status: string(store.ModelCallCompleted),
		InputTokens: 3, OutputTokens: 5, CostUsd: 0, FinishReason: "stop",
	}})
}

// The broker is the only hop between the agent's offered tool names and the
// Model Gateway's execution surface. Dropping the tool definitions here
// strips the model's tool surface silently — a real-model run answered
// without ever seeing a tool while every fake-provider test stayed green,
// because the scripted provider emits tool calls regardless of what it was
// offered. This pins the mapping.
func TestGrpcModelBrokerForwardsToolDefinitions(t *testing.T) {
	service := &recordingModelService{}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	modelv1.RegisterModelInvocationServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	broker := NewGrpcModelBroker(modelv1.NewModelInvocationServiceClient(connection))

	output, err := broker.InvokeStream(context.Background(), kernelmodel.InvokeInput{
		TenantID: "tenant-a", TaskID: uuid.New(), RunID: uuid.New(),
		AttemptID: uuid.MustParse("33333333-3333-3333-3333-333333333333"), FencingToken: 1,
		AgentVersionRef: "real-agent@1", ModelRef: "deepseek/DeepSeek-V4-Flash-w8a8-mtp", Stream: true,
		Messages: []provider.Message{{Role: "user", Content: "report the weather"}},
		Tools: []provider.ToolDefinition{{
			Name:        "weather.lookup",
			Description: "weather.lookup (version 1.0.0, sideEffectRisk low)",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
		}},
	}, nil)
	if err != nil {
		t.Fatalf("InvokeStream: %v", err)
	}
	if output.Call.Status != store.ModelCallCompleted {
		t.Fatalf("call status = %s, want COMPLETED", output.Call.Status)
	}

	service.mu.Lock()
	defer service.mu.Unlock()
	if service.request == nil {
		t.Fatal("no invocation request reached the gateway service")
	}
	tools := service.request.GetTools()
	if len(tools) != 1 {
		t.Fatalf("gateway received %d tools, want exactly the one offered definition", len(tools))
	}
	if tools[0].GetName() != "weather.lookup" || tools[0].GetDescription() == "" {
		t.Fatalf("tool envelope lost name/description: %+v", tools[0])
	}
	if !json.Valid([]byte(tools[0].GetParametersJson())) || tools[0].GetParametersJson() == "" {
		t.Fatalf("parameters schema not carried verbatim: %q", tools[0].GetParametersJson())
	}
}
