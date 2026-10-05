package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeModelRunner struct {
	input  model.InvokeInput
	deltas []string
	output model.InvokeOutput
	err    error
}

func (f *fakeModelRunner) InvokeStream(_ context.Context, input model.InvokeInput, onDelta func(string)) (model.InvokeOutput, error) {
	f.input = input
	for _, delta := range f.deltas {
		onDelta(delta)
	}
	return f.output, f.err
}

func newInvocationTestClient(t *testing.T, runner ModelRunner, authorizer *capability.Authorizer) modelv1.ModelInvocationServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	modelv1.RegisterModelInvocationServiceServer(server, NewModelInvocationService(runner, "tenant-a", authorizer))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return modelv1.NewModelInvocationServiceClient(connection)
}

func TestModelInvocationStreamsDeltasAndFinish(t *testing.T) {
	call := store.ModelCall{ID: uuid.New(), TenantID: "tenant-a", AttemptID: uuid.New(),
		ModelRef: "fake/model", Status: store.ModelCallCompleted, InputTokens: 90, OutputTokens: 45,
		CostMicroUSD: money.MustFromUSD(0.001), PriceRevision: "p1", FinishReason: "stop", ProviderRequestID: "req-9", ResourceVersion: 2}
	runner := &fakeModelRunner{
		deltas: []string{"Hel", "lo"},
		output: model.InvokeOutput{Call: call, Content: "Hello", ToolCalls: []provider.ToolCall{
			{ID: "call-1", Name: "weather", Arguments: `{"city":"paris"}`},
		}},
	}
	client := newInvocationTestClient(t, runner, nil)

	stream, err := client.Invoke(context.Background(), &modelv1.InvokeRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: call.AttemptID.String(), FencingToken: 3},
		TaskId:   uuid.New().String(), RunId: uuid.New().String(), AgentVersionRef: "agent@1",
		ModelRef: "fake/model", IdempotencyKey: "inv-1", Stream: true,
		Messages: []*modelv1.ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	var deltas []string
	var finish *modelv1.InvokeFinish
	var toolCalls []*modelv1.ChatToolCall
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if chunk.GetDelta() != "" {
			deltas = append(deltas, chunk.GetDelta())
		}
		if chunk.GetToolCall() != nil {
			toolCalls = append(toolCalls, chunk.GetToolCall())
		}
		if chunk.GetFinish() != nil {
			finish = chunk.GetFinish()
		}
	}
	if len(deltas) != 2 || deltas[0] != "Hel" || deltas[1] != "lo" {
		t.Fatalf("deltas = %v", deltas)
	}
	if finish == nil {
		t.Fatal("terminal finish chunk missing")
	}
	if finish.GetCallId() != call.ID.String() || finish.GetStatus() != "COMPLETED" ||
		finish.GetInputTokens() != 90 || finish.GetOutputTokens() != 45 || finish.GetContent() != "Hello" ||
		finish.GetProviderRequestId() != "req-9" || finish.GetCostUsd() != 0.001 {
		t.Fatalf("finish chunk = %+v", finish)
	}
	if len(toolCalls) != 1 || toolCalls[0].GetName() != "weather" || toolCalls[0].GetArgumentsJson() != `{"city":"paris"}` {
		t.Fatalf("tool call chunks = %v", toolCalls)
	}
	if runner.input.Messages[0].Content != "hi" || !runner.input.Stream {
		t.Fatalf("mapped input = %+v", runner.input)
	}
}

func TestModelInvocationReportsTerminalProviderFailureAsChunk(t *testing.T) {
	call := store.ModelCall{ID: uuid.New(), TenantID: "tenant-a", AttemptID: uuid.New(),
		ModelRef: "fake/model", Status: store.ModelCallFailed, FinishReason: "provider_rejected", ResourceVersion: 2}
	runner := &fakeModelRunner{output: model.InvokeOutput{Call: call}, err: provider.ErrProviderRejected}
	client := newInvocationTestClient(t, runner, nil)

	stream, err := client.Invoke(context.Background(), &modelv1.InvokeRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: call.AttemptID.String(), FencingToken: 3},
		TaskId:   uuid.New().String(), RunId: uuid.New().String(), AgentVersionRef: "agent@1",
		ModelRef: "fake/model", IdempotencyKey: "inv-2",
		Messages: []*modelv1.ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	chunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected structured failure chunk, got status %v", err)
	}
	if chunk.GetFailure() == nil || chunk.GetFailure().GetCode() != "PROVIDER_REJECTED" ||
		chunk.GetFailure().GetFinishReason() != "provider_rejected" {
		t.Fatalf("failure chunk = %+v", chunk.GetFailure())
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream must terminate after the failure chunk: %v", err)
	}
}

func TestModelInvocationMapsGovernanceErrors(t *testing.T) {
	runner := &fakeModelRunner{err: model.ErrBudgetExhausted}
	client := newInvocationTestClient(t, runner, nil)
	stream, err := client.Invoke(context.Background(), &modelv1.InvokeRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.New().String(), FencingToken: 3},
		TaskId:   uuid.New().String(), RunId: uuid.New().String(), AgentVersionRef: "agent@1",
		ModelRef: "fake/model", IdempotencyKey: "inv-3",
		Messages: []*modelv1.ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// Streaming status errors surface on the first receive.
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("budget hard stop must map to ResourceExhausted, got %v", err)
	}

	stream, err = client.Invoke(context.Background(), &modelv1.InvokeRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-b", AttemptId: uuid.New().String(), FencingToken: 3},
		TaskId:   uuid.New().String(), RunId: uuid.New().String(),
		ModelRef: "fake/model", IdempotencyKey: "inv-4",
		Messages: []*modelv1.ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign tenant must be denied, got %v", err)
	}
}

func TestModelInvocationEnforcesCapability(t *testing.T) {
	call := store.ModelCall{ID: uuid.New(), TenantID: "tenant-a", AttemptID: uuid.New(),
		ModelRef: "fake/model", Status: store.ModelCallCompleted, ResourceVersion: 2}
	runner := &fakeModelRunner{output: model.InvokeOutput{Call: call}}
	authorizer := testAuthorizer(t, agentversion.Capabilities{Models: []string{"other/model"}})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	modelv1.RegisterModelInvocationServiceServer(server, NewModelInvocationService(runner, "tenant-a", authorizer))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := modelv1.NewModelInvocationServiceClient(connection)

	// The undeclared model is denied before any provider interaction.
	stream, err := client.Invoke(context.Background(), &modelv1.InvokeRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.New().String(), FencingToken: 3},
		TaskId:   uuid.New().String(), RunId: uuid.New().String(), AgentVersionRef: "agent@1",
		ModelRef: "fake/model", IdempotencyKey: "inv-5",
		Messages: []*modelv1.ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, recvErr := stream.Recv(); status.Code(recvErr) != codes.PermissionDenied {
		t.Fatalf("undeclared model must be denied, got %v", recvErr)
	}
	if runner.input.IdempotencyKey != "" {
		t.Fatal("runner must not execute a denied invocation")
	}
}
