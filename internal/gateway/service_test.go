package gateway

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestInvokeToolExecutedThroughFencedBoundary(t *testing.T) {
	invoker := &fakeInvoker{result: tool.InvokeResult{
		Outcome: tool.OutcomeExecuted, Result: json.RawMessage(`{"ok":true}`),
		ToolCall:         store.ToolCall{ID: uuid.New(), Status: store.ToolCallExecuted},
		ReceiptOperation: "TOOL:fs.read@1.0.0",
	}}
	client := newTestClient(t, invoker)

	response, err := client.InvokeTool(context.Background(), invokeRequest(t, ""))
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	if response.GetOutcome() != "EXECUTED" || string(response.GetResultJson()) != `{"ok":true}` {
		t.Fatalf("unexpected response: %+v", response)
	}
	if response.GetToolCallId() == "" || response.GetReceiptOperation() != "TOOL:fs.read@1.0.0" {
		t.Fatalf("missing call metadata: %+v", response)
	}
	if invoker.input.TenantID != "tenant-a" || invoker.input.AttemptID != attemptID ||
		invoker.input.ToolName != "fs.read" || invoker.input.IdempotencyKey != "invoke-1" {
		t.Fatalf("invoker did not receive the fenced input: %+v", invoker.input)
	}
}

func TestInvokeToolRejectsForeignTenant(t *testing.T) {
	client := newTestClient(t, &fakeInvoker{})
	request := invokeRequest(t, "")
	request.Identity.TenantId = "tenant-b"
	_, err := client.InvokeTool(context.Background(), request)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign tenant: %v, want PermissionDenied", err)
	}
}

func TestInvokeToolMapsHardStopAndExecutionFailures(t *testing.T) {
	client := newTestClient(t, &fakeInvoker{
		err: tool.ErrBudgetExhausted,
	})
	_, err := client.InvokeTool(context.Background(), invokeRequest(t, ""))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("budget hard stop: %v, want ResourceExhausted", err)
	}

	client = newTestClient(t, &fakeInvoker{
		err: &tool.ToolExecutionError{Code: "exit_code_7", Err: tool.ErrToolExecutionFailed},
	})
	_, err = client.InvokeTool(context.Background(), invokeRequest(t, ""))
	if status.Code(err) != codes.Aborted || status.Convert(err).Message() != "exit_code_7" {
		t.Fatalf("execution failure: %v, want Aborted with failure code", err)
	}
}

func TestInvokeToolRejectsMalformedIdentity(t *testing.T) {
	client := newTestClient(t, &fakeInvoker{})
	request := invokeRequest(t, "")
	request.Identity.FencingToken = 0
	if _, err := client.InvokeTool(context.Background(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero fencing token: %v, want InvalidArgument", err)
	}
	request = invokeRequest(t, "")
	request.TaskId = "not-a-uuid"
	if _, err := client.InvokeTool(context.Background(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad task ID: %v, want InvalidArgument", err)
	}
	request = invokeRequest(t, "")
	request.ApprovalId = "not-a-uuid"
	if _, err := client.InvokeTool(context.Background(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad approval ID: %v, want InvalidArgument", err)
	}
}

func TestListToolsScopedToTenant(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: store.ToolRiskLow,
			Actions: []string{"read"}, ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	client := newTestClient(t, invoker)
	response, err := client.ListTools(context.Background(), &gatewayv1.ListToolsRequest{TenantId: "tenant-a"})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(response.GetTools()) != 1 || response.GetTools()[0].GetName() != "fs.read" ||
		response.GetTools()[0].GetSideEffectRisk() != "low" {
		t.Fatalf("unexpected tools: %+v", response)
	}
}

// P1-02: the gRPC listing collapses multi-version descriptors to exactly one
// entry per tool name — the latest granted version, in stable name order — so
// downstream model-facing consumers never see two same-named tools.
func TestListToolsCollapsesDuplicateToolNames(t *testing.T) {
	invoker := &fakeInvoker{descriptors: []store.ToolDescriptor{
		{TenantID: "tenant-a", Name: "weather", Version: "1.10.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
		{TenantID: "tenant-a", Name: "weather", Version: "2.0.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
		{TenantID: "tenant-a", Name: "weather", Version: "1.0.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
		{TenantID: "tenant-a", Name: "fs.read", Version: "0.9.0", ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	client := newTestClient(t, invoker)
	response, err := client.ListTools(context.Background(), &gatewayv1.ListToolsRequest{TenantId: "tenant-a"})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	tools := response.GetTools()
	if len(tools) != 2 {
		t.Fatalf("tools = %d entries, want one per name (2): %+v", len(tools), tools)
	}
	if tools[0].GetName() != "fs.read" || tools[0].GetVersion() != "0.9.0" {
		t.Fatalf("first entry = %s@%s, want fs.read@0.9.0 in name order", tools[0].GetName(), tools[0].GetVersion())
	}
	if tools[1].GetName() != "weather" || tools[1].GetVersion() != "2.0.0" {
		t.Fatalf("second entry = %s@%s, want the latest weather@2.0.0", tools[1].GetName(), tools[1].GetVersion())
	}
}

var attemptID = uuid.MustParse("33333333-3333-3333-3333-333333333333")

func invokeRequest(t *testing.T, approvalID string) *gatewayv1.InvokeToolRequest {
	t.Helper()
	request := &gatewayv1.InvokeToolRequest{
		Identity: &gatewayv1.AttemptIdentity{
			TenantId: "tenant-a", AttemptId: attemptID.String(), FencingToken: 1,
		},
		TaskId:          uuid.MustParse("11111111-1111-1111-1111-111111111111").String(),
		RunId:           uuid.MustParse("22222222-2222-2222-2222-222222222222").String(),
		AgentVersionRef: "agent@1", ToolName: "fs.read", Action: "read", Resource: "fs:/tmp",
		ArgsJson: []byte(`{"path":"a.txt"}`), IdempotencyKey: "invoke-1",
	}
	if approvalID != "" {
		request.ApprovalId = approvalID
	}
	return request
}

type fakeInvoker struct {
	result      tool.InvokeResult
	err         error
	input       tool.InvokeInput
	descriptors []store.ToolDescriptor
}

func (f *fakeInvoker) InvokeTool(_ context.Context, input tool.InvokeInput) (tool.InvokeResult, error) {
	f.input = input
	if f.err != nil {
		return tool.InvokeResult{}, f.err
	}
	return f.result, nil
}

func (f *fakeInvoker) ListTools(context.Context, string) ([]store.ToolDescriptor, error) {
	return f.descriptors, nil
}

func (f *fakeInvoker) GetToolDescriptor(_ context.Context, _, name, version string) (store.ToolDescriptor, error) {
	for _, descriptor := range f.descriptors {
		if descriptor.Name == name && (version == "" || descriptor.Version == version) {
			return descriptor, nil
		}
	}
	// A descriptor the test did not stage still resolves so the freeze can
	// judge it; its zero CreatedAt sits at or before any publication time.
	return store.ToolDescriptor{Name: name, Version: version}, nil
}

func newTestClient(t *testing.T, invoker ToolInvoker) gatewayv1.ToolGatewayServiceClient {
	return newTestClientWithFence(t, invoker, nil)
}

func newTestClientWithFence(t *testing.T, invoker ToolInvoker, fences RuntimeFence) gatewayv1.ToolGatewayServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	service := NewService(invoker, "tenant-a")
	if fences != nil {
		service.WithRuntimeFence(fences)
	}
	gatewayv1.RegisterToolGatewayServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return gatewayv1.NewToolGatewayServiceClient(connection)
}

type fakeToolFence struct {
	assignment store.RuntimeAssignment
	err        error
}

func (f *fakeToolFence) GetRuntimeAssignment(context.Context, string, uuid.UUID, int64) (store.RuntimeAssignment, error) {
	if f.err != nil {
		return store.RuntimeAssignment{}, f.err
	}
	return f.assignment, nil
}

func TestInvokeToolFencedAttemptRejected(t *testing.T) {
	fence := &fakeToolFence{err: store.ErrFenced}
	client := newTestClientWithFence(t, &fakeInvoker{}, fence)
	_, err := client.InvokeTool(context.Background(), invokeRequest(t, ""))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("fenced attempt: %v, want PermissionDenied", err)
	}
}

func TestInvokeToolStaleVersionRejected(t *testing.T) {
	fence := &fakeToolFence{assignment: store.RuntimeAssignment{
		Task: store.Task{AgentVersionRef: "different@2"},
	}}
	client := newTestClientWithFence(t, &fakeInvoker{}, fence)
	_, err := client.InvokeTool(context.Background(), invokeRequest(t, ""))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale version: %v, want PermissionDenied", err)
	}
}
