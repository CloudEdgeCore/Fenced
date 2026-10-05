package gateway

import (
	"context"
	"net"
	"testing"

	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestModelServiceBeginSettleFinish(t *testing.T) {
	call := store.ModelCall{ID: uuid.New(), TenantID: "tenant-a", AttemptID: uuid.New(),
		ModelRef: "openai/gpt-4o", Status: store.ModelCallStarted, PriceRevision: "v1", ResourceVersion: 1}
	invoker := &fakeModelInvoker{call: call}
	client := newModelTestClient(t, invoker)

	begun, err := client.Begin(context.Background(), &modelv1.BeginRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: call.AttemptID.String(), FencingToken: 1},
		TaskId:   uuid.New().String(), RunId: uuid.New().String(), AgentVersionRef: "agent@1",
		ModelRef: "openai/gpt-4o", IdempotencyKey: "model-1",
	})
	if err != nil || begun.GetCallId() != call.ID.String() {
		t.Fatalf("Begin: %+v err=%v", begun, err)
	}

	if _, err := client.Settle(context.Background(), &modelv1.SettleRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: call.AttemptID.String(), FencingToken: 1},
		CallId:   call.ID.String(), Sequence: 1, OutputTokens: 100,
	}); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	finished, err := client.Finish(context.Background(), &modelv1.FinishRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: call.AttemptID.String(), FencingToken: 1},
		CallId:   call.ID.String(), ExpectedVersion: 1, Status: "COMPLETED",
		InputTokens: 100, OutputTokens: 200, ProviderRequestId: "req-1", FinishReason: "stop",
	})
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if finished.GetStatus() != "COMPLETED" || finished.GetInputTokens() != 100 || finished.GetCostUsd() <= 0 {
		t.Fatalf("finished: %+v", finished)
	}
	if !invoker.finished {
		t.Fatal("invoker was not asked to finish")
	}
}

func TestModelServiceMapsBudgetHardStop(t *testing.T) {
	call := store.ModelCall{ID: uuid.New(), TenantID: "tenant-a", AttemptID: uuid.New(),
		ModelRef: "openai/gpt-4o", Status: store.ModelCallStarted, ResourceVersion: 1}
	client := newModelTestClient(t, &fakeModelInvoker{call: call, hardStop: true})
	_, err := client.Settle(context.Background(), &modelv1.SettleRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: call.AttemptID.String(), FencingToken: 1},
		CallId:   call.ID.String(), Sequence: 1, OutputTokens: 999,
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("hard stop: %v, want ResourceExhausted", err)
	}
}

func TestModelServiceRejectsForeignTenant(t *testing.T) {
	call := store.ModelCall{ID: uuid.New(), TenantID: "tenant-a", AttemptID: uuid.New(), ModelRef: "openai/gpt-4o"}
	client := newModelTestClient(t, &fakeModelInvoker{call: call})
	_, err := client.Begin(context.Background(), &modelv1.BeginRequest{
		Identity: &modelv1.AttemptIdentity{TenantId: "tenant-b", AttemptId: call.AttemptID.String(), FencingToken: 1},
		ModelRef: "openai/gpt-4o",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign tenant: %v, want PermissionDenied", err)
	}
}

type fakeModelInvoker struct {
	call     store.ModelCall
	hardStop bool
	finished bool
}

func (f *fakeModelInvoker) Begin(context.Context, model.BeginInput) (model.BeginResult, error) {
	return model.BeginResult{Call: f.call, PolicyRevision: "r1"}, nil
}

func (f *fakeModelInvoker) GetModelCall(_ context.Context, tenantID string, id uuid.UUID) (store.ModelCall, error) {
	if tenantID != f.call.TenantID || id != f.call.ID {
		return store.ModelCall{}, store.ErrNotFound
	}
	return f.call, nil
}

func (f *fakeModelInvoker) Settle(_ context.Context, _ store.ModelCall, _ int64, _ model.Usage) error {
	if f.hardStop {
		return model.ErrBudgetExhausted
	}
	return nil
}

func (f *fakeModelInvoker) Finish(_ context.Context, call store.ModelCall, in model.FinishInput) (store.ModelCall, error) {
	f.finished = true
	call.Status = in.Status
	call.InputTokens, call.OutputTokens = in.InputTokens, in.OutputTokens
	call.CostMicroUSD = money.MustFromUSD(0.0033)
	call.PriceRevision = "v1"
	call.FinishReason = in.FinishReason
	return call, nil
}

func newModelTestClient(t *testing.T, invoker ModelInvoker) modelv1.ModelGatewayServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	modelv1.RegisterModelGatewayServiceServer(server, NewModelService(invoker, "tenant-a"))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return modelv1.NewModelGatewayServiceClient(connection)
}
