package syscall

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
)

// ==========================================
// Mocks for Subsystems
// ==========================================

type mockToolInvoker struct {
	invoked tool.InvokeInput
	tools   []store.ToolDescriptor
}

func (m *mockToolInvoker) InvokeTool(ctx context.Context, in tool.InvokeInput) (tool.InvokeResult, error) {
	m.invoked = in
	toolCallID := uuid.New()
	return tool.InvokeResult{
		Outcome:          tool.OutcomeExecuted,
		Result:           json.RawMessage(`{"result":"tool_ok"}`),
		ToolCall:         store.ToolCall{ID: toolCallID},
		ReceiptOperation: "tool.exec",
	}, nil
}

func (m *mockToolInvoker) ListTools(ctx context.Context, tenantID string) ([]store.ToolDescriptor, error) {
	return m.tools, nil
}

func (m *mockToolInvoker) GetToolDescriptor(ctx context.Context, tenantID, name, version string) (store.ToolDescriptor, error) {
	for _, t := range m.tools {
		if t.Name == name {
			return t, nil
		}
	}
	return store.ToolDescriptor{}, fmt.Errorf("tool not found")
}

type mockMemoryInvoker struct {
	mu      sync.Mutex
	records map[string]store.MemoryRecord
}

func newMockMemoryInvoker() *mockMemoryInvoker {
	return &mockMemoryInvoker{
		records: make(map[string]store.MemoryRecord),
	}
}

func (m *mockMemoryInvoker) Put(ctx context.Context, in memory.PutInput) (store.MemoryRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := in.Namespace + "/" + in.Key
	rec := store.MemoryRecord{
		ID:              uuid.New(),
		Namespace:       in.Namespace,
		Key:             in.Key,
		ContentType:     in.ContentType,
		Content:         in.Content,
		Sensitivity:     in.Sensitivity,
		ResourceVersion: int64(len(m.records) + 1),
	}
	m.records[key] = rec
	return rec, false, nil
}

func (m *mockMemoryInvoker) Search(ctx context.Context, in memory.SearchInput) ([]store.MemoryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res []store.MemoryRecord
	for _, rec := range m.records {
		if rec.Namespace == in.Namespace {
			res = append(res, rec)
		}
	}
	return res, nil
}

type mockModelRunner struct{}

func (m *mockModelRunner) InvokeStream(ctx context.Context, in model.InvokeInput, onDelta func(string)) (model.InvokeOutput, error) {
	return model.InvokeOutput{
		Call: store.ModelCall{
			ID:           uuid.New(),
			ModelRef:     in.ModelRef,
			Status:       store.ModelCallCompleted,
			InputTokens:  100,
			OutputTokens: 50,
		},
		Content: "model generated response",
	}, nil
}

type mockModelInvoker struct{}

func (m *mockModelInvoker) Begin(ctx context.Context, in model.BeginInput) (model.BeginResult, error) {
	return model.BeginResult{
		Call: store.ModelCall{
			ID:              uuid.New(),
			ModelRef:        in.ModelRef,
			Status:          store.ModelCallStarted,
			ResourceVersion: 1,
		},
	}, nil
}

func (m *mockModelInvoker) GetModelCall(ctx context.Context, tenantID string, id uuid.UUID) (store.ModelCall, error) {
	return store.ModelCall{
		ID:              id,
		TenantID:        tenantID,
		ResourceVersion: 1,
	}, nil
}

func (m *mockModelInvoker) Settle(ctx context.Context, call store.ModelCall, seq int64, u model.Usage) error {
	return nil
}

func (m *mockModelInvoker) Finish(ctx context.Context, call store.ModelCall, in model.FinishInput) (store.ModelCall, error) {
	call.Status = in.Status
	call.InputTokens = in.InputTokens
	call.OutputTokens = in.OutputTokens
	call.ResourceVersion = 2
	return call, nil
}

type mockRuntimeInvoker struct{}

func (m *mockRuntimeInvoker) CommitCheckpoint(ctx context.Context, in store.CommitCheckpointInput) (store.Checkpoint, store.Attempt, error) {
	return store.Checkpoint{
		ID:           in.CheckpointID,
		TenantID:     in.TenantID,
		AttemptID:    in.AttemptID,
		FencingToken: in.FencingToken,
	}, store.Attempt{ID: in.AttemptID, ResourceVersion: in.ExpectedAttemptVersion + 1}, nil
}

func (m *mockRuntimeInvoker) CompleteAttempt(ctx context.Context, in store.CompleteAttemptInput) (store.CompleteAttemptResult, error) {
	return store.CompleteAttemptResult{
		Attempt: store.Attempt{ID: in.AttemptID, ResourceVersion: in.ExpectedAttemptVersion + 1},
	}, nil
}

// ==========================================
// Tests
// ==========================================

func TestToolSyscalls(t *testing.T) {
	invoker := &mockToolInvoker{
		tools: []store.ToolDescriptor{
			{Name: "web_search", Version: "1.0.0"},
		},
	}
	dispatcher := NewDispatcher()
	handler := NewToolSyscallHandler(invoker, nil)
	dispatcher.Register(SysToolInvoke, handler)
	dispatcher.Register(SysToolList, handler)

	client := NewClient(dispatcher)
	id := AttemptIdentity{
		TenantID:     "tenant-tool",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	// 1. Tool Invoke
	invRes, err := client.InvokeTool(context.Background(), id, ToolInvokePayload{
		ToolName: "web_search",
		Action:   "query",
		ArgsJSON: json.RawMessage(`{"q":"golang"}`),
	})
	if err != nil {
		t.Fatalf("InvokeTool error: %v", err)
	}
	if invRes.Outcome != string(tool.OutcomeExecuted) || string(invRes.ResultJSON) != `{"result":"tool_ok"}` {
		t.Fatalf("unexpected invoke result: %+v", invRes)
	}

	// 2. Tool List
	listRes, err := client.ListTools(context.Background(), id, ToolListPayload{})
	if err != nil {
		t.Fatalf("ListTools error: %v", err)
	}
	if len(listRes.Tools) != 1 || listRes.Tools[0].Name != "web_search" {
		t.Fatalf("unexpected list result: %+v", listRes)
	}
}

func TestMemorySyscalls(t *testing.T) {
	invoker := newMockMemoryInvoker()
	dispatcher := NewDispatcher()
	handler := NewMemorySyscallHandler(invoker, nil)
	dispatcher.Register(SysMemoryPut, handler)
	dispatcher.Register(SysMemorySearch, handler)

	client := NewClient(dispatcher)
	id := AttemptIdentity{
		TenantID:     "tenant-mem",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	// 1. Put Memory
	putRes, ver, err := client.PutMemory(context.Background(), id, MemoryPutPayload{
		Namespace:   "user-profile",
		Key:         "theme",
		ContentType: "text/plain",
		Content:     "dark",
	})
	if err != nil {
		t.Fatalf("PutMemory error: %v", err)
	}
	if ver != 1 || putRes.Record.Content != "dark" {
		t.Fatalf("unexpected put result: ver=%d, res=%+v", ver, putRes)
	}

	// 2. Search Memory
	searchRes, err := client.SearchMemory(context.Background(), id, MemorySearchPayload{
		Namespace: "user-profile",
	})
	if err != nil {
		t.Fatalf("SearchMemory error: %v", err)
	}
	if len(searchRes.Records) != 1 || searchRes.Records[0].Key != "theme" {
		t.Fatalf("unexpected search result: %+v", searchRes)
	}
}

func TestIPCSyscalls(t *testing.T) {
	ipcService := ipc.NewService(ipc.ServiceConfig{})
	dispatcher := NewDispatcher()
	handler := NewIPCSyscallHandler(ipcService)
	dispatcher.Register(SysIPCSend, handler)
	dispatcher.Register(SysIPCReceive, handler)
	dispatcher.Register(SysIPCAck, handler)

	client := NewClient(dispatcher)
	runID := uuid.New()
	id := AttemptIdentity{
		TenantID:     "tenant-ipc",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	toAddr := ipc.AgentAddress{
		TenantID:   "tenant-ipc",
		Namespace:  "default",
		AgentID:    "target-agent",
		InstanceID: runID.String(),
	}

	// 1. Send IPC
	sendRes, err := client.SendIPC(context.Background(), id, IPCSendPayload{
		AgentVersionRef: "sender-agent@1.0.0",
		To:              toAddr,
		Kind:            "request",
		PayloadJSON:     json.RawMessage(`{"msg":"hello"}`),
	})
	if err != nil {
		t.Fatalf("SendIPC error: %v", err)
	}
	if sendRes.MessageID == "" {
		t.Fatal("expected non-empty MessageID")
	}

	// 2. Receive IPC
	recvId := AttemptIdentity{
		TenantID:     "tenant-ipc",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}
	// We need dispatcher with assignment or direct address
	// Let's test with assignment fence
	fence := &mockFence{
		validToken: 1,
		assignment: store.RuntimeAssignment{
			Task: store.Task{
				Namespace:       "default",
				AgentVersionRef: "target-agent@1.0.0",
			},
			Run: store.Run{
				ID: runID,
			},
		},
	}
	fencedDispatcher := NewDispatcher(WithRuntimeFence(fence))
	fencedDispatcher.Register(SysIPCReceive, handler)
	fencedDispatcher.Register(SysIPCAck, handler)
	fencedClient := NewClient(fencedDispatcher)

	recvRes, err := fendedReceive(fencedClient, recvId)
	if err != nil {
		t.Fatalf("ReceiveIPC error: %v", err)
	}
	if len(recvRes.Messages) != 1 || string(recvRes.Messages[0].Payload) != `{"msg":"hello"}` {
		t.Fatalf("unexpected received messages: %+v", recvRes)
	}

	// 3. Ack IPC
	ackRes, err := fencedClient.AckIPC(context.Background(), recvId, IPCAckPayload{
		AgentVersionRef: "target-agent@1.0.0",
		MessageIDs:      []string{recvRes.Messages[0].ID},
	})
	if err != nil {
		t.Fatalf("AckIPC error: %v", err)
	}
	if len(ackRes.AcknowledgedIDs) != 1 {
		t.Fatalf("expected 1 acked ID, got %+v", ackRes)
	}
}

func fendedReceive(client *SyscallClient, id AttemptIdentity) (IPCReceiveResult, error) {
	return client.ReceiveIPC(context.Background(), id, IPCReceivePayload{
		AgentVersionRef: "target-agent@1.0.0",
		MaxMessages:     5,
		WaitMillis:      100,
	})
}

func TestModelSyscalls(t *testing.T) {
	runner := &mockModelRunner{}
	invoker := &mockModelInvoker{}
	dispatcher := NewDispatcher()
	handler := NewModelSyscallHandler(runner, invoker, nil)
	dispatcher.Register(SysModelInvoke, handler)
	dispatcher.Register(SysModelBegin, handler)
	dispatcher.Register(SysModelSettle, handler)
	dispatcher.Register(SysModelFinish, handler)

	client := NewClient(dispatcher)
	id := AttemptIdentity{
		TenantID:     "tenant-model",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	// 1. Model Invoke
	invRes, err := client.InvokeModel(context.Background(), id, ModelInvokePayload{
		ModelRef: "openai/gpt-4o",
		Messages: []ModelChatMessage{
			{Role: "user", Content: "Hello"},
		},
	})
	if err != nil {
		t.Fatalf("InvokeModel error: %v", err)
	}
	if invRes.Content != "model generated response" || invRes.Status != string(store.ModelCallCompleted) {
		t.Fatalf("unexpected model invoke response: %+v", invRes)
	}

	// 2. Model Begin
	beginRes, ver, err := client.BeginModel(context.Background(), id, ModelBeginPayload{
		ModelRef: "openai/gpt-4o",
	})
	if err != nil {
		t.Fatalf("BeginModel error: %v", err)
	}
	if ver != 1 || beginRes.CallID == uuid.Nil {
		t.Fatalf("unexpected model begin response: %+v", beginRes)
	}

	// 3. Model Settle
	settleRes, err := client.SettleModel(context.Background(), id, ModelSettlePayload{
		CallID:       beginRes.CallID,
		Sequence:     1,
		InputTokens:  50,
		OutputTokens: 20,
	})
	if err != nil {
		t.Fatalf("SettleModel error: %v", err)
	}
	if settleRes.CallID != beginRes.CallID {
		t.Fatalf("unexpected settle response: %+v", settleRes)
	}

	// 4. Model Finish
	finishRes, ver, err := client.FinishModel(context.Background(), id, ModelFinishPayload{
		CallID:          beginRes.CallID,
		ExpectedVersion: 1,
		Status:          string(store.ModelCallCompleted),
		InputTokens:     50,
		OutputTokens:    20,
	})
	if err != nil {
		t.Fatalf("FinishModel error: %v", err)
	}
	if ver != 2 || finishRes.Status != string(store.ModelCallCompleted) {
		t.Fatalf("unexpected finish response: %+v", finishRes)
	}
}

func TestRuntimeLifecycleSyscalls(t *testing.T) {
	invoker := &mockRuntimeInvoker{}
	dispatcher := NewDispatcher()
	handler := NewRuntimeSyscallHandler(invoker)
	dispatcher.Register(SysRuntimeCheckpoint, handler)
	dispatcher.Register(SysRuntimeComplete, handler)
	dispatcher.Register(SysRuntimeYield, handler)

	client := NewClient(dispatcher)
	id := AttemptIdentity{
		TenantID:     "tenant-runtime",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	// 1. Checkpoint
	chkRes, ver, err := client.Checkpoint(context.Background(), id, RuntimeCheckpointPayload{
		ExpectedAttemptVersion: 1,
		CheckpointID:           uuid.New().String(),
		State: store.ArtifactReference{
			URI:       "s3://state/1",
			SHA256:    [32]byte{1, 2, 3},
			SizeBytes: 1024,
			MediaType: "application/json",
		},
	})
	if err != nil {
		t.Fatalf("Checkpoint error: %v", err)
	}
	if ver != 2 || chkRes.AttemptVersion != 2 {
		t.Fatalf("unexpected checkpoint response: ver=%d, %+v", ver, chkRes)
	}

	// 2. Complete
	compRes, ver, err := client.Complete(context.Background(), id, RuntimeCompletePayload{
		ExpectedAttemptVersion: 2,
		Result: store.ArtifactReference{
			URI:       "s3://result/1",
			SHA256:    [32]byte{4, 5, 6},
			SizeBytes: 512,
			MediaType: "application/json",
		},
	})
	if err != nil {
		t.Fatalf("Complete error: %v", err)
	}
	if ver != 3 || compRes.ResultRef != "s3://result/1" {
		t.Fatalf("unexpected complete response: ver=%d, %+v", ver, compRes)
	}

	// 3. Yield
	yieldRes, err := client.Yield(context.Background(), id, RuntimeYieldPayload{
		Reason: "timeslice_expired",
	})
	if err != nil {
		t.Fatalf("Yield error: %v", err)
	}
	if !yieldRes.Yielded {
		t.Fatal("expected yielded true")
	}
}

func TestServiceSyscalls(t *testing.T) {
	memStore := supervisor.NewMemoryStore()
	dispatcher := NewDispatcher()
	handler := NewServiceSyscallHandler(memStore)
	dispatcher.Register(SysServiceHeartbeat, handler)
	dispatcher.Register(SysServiceQuery, handler)

	ctx := context.Background()
	svc := &supervisor.Service{
		ID:        uuid.New().String(),
		TenantID:  "tenant-service",
		Namespace: "default",
		Name:      "order-service",
		AgentID:   "agent-orders@1.0.0",
		Spec: supervisor.ServiceSpec{
			Replicas: 1,
		},
	}
	if err := memStore.CreateService(ctx, svc); err != nil {
		t.Fatalf("create service error: %v", err)
	}

	inst := &supervisor.Instance{
		ID:        "inst-1",
		TenantID:  "tenant-service",
		ServiceID: svc.ID,
		Phase:     supervisor.InstanceStarting,
	}
	if err := memStore.CreateInstance(ctx, inst); err != nil {
		t.Fatalf("create instance error: %v", err)
	}

	client := NewClient(dispatcher)
	id := AttemptIdentity{
		TenantID:     "tenant-service",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	// 1. Heartbeat
	hbRes, err := client.HeartbeatService(ctx, id, ServiceHeartbeatPayload{
		ServiceID:  uuid.MustParse(svc.ID),
		InstanceID: "inst-1",
		Status:     "Running",
	})
	if err != nil {
		t.Fatalf("HeartbeatService error: %v", err)
	}
	if !hbRes.Acknowledged {
		t.Fatal("expected heartbeat acknowledged")
	}

	// Verify instance phase updated in store
	updatedInst, err := memStore.GetInstance(ctx, "tenant-service", svc.ID, "inst-1")
	if err != nil || updatedInst.Phase != supervisor.InstanceRunning {
		t.Fatalf("expected instance running, got %+v (err: %v)", updatedInst, err)
	}

	// 2. Query Services
	qRes, err := client.QueryServices(ctx, id, ServiceQueryPayload{
		Namespace: "default",
		Name:      "order-service",
	})
	if err != nil {
		t.Fatalf("QueryServices error: %v", err)
	}
	if len(qRes.Services) != 1 || qRes.Services[0].Name != "order-service" {
		t.Fatalf("unexpected query result: %+v", qRes)
	}
}

func TestSyscallABIVersion(t *testing.T) {
	if SyscallABIVersion != "1.0.0" {
		t.Fatalf("expected ABI version 1.0.0, got %s", SyscallABIVersion)
	}
	if SyscallABIMajor != 1 || SyscallABIMinor != 0 || SyscallABIPatch != 0 {
		t.Fatalf("unexpected semver components: %d.%d.%d", SyscallABIMajor, SyscallABIMinor, SyscallABIPatch)
	}

	client := NewClient(nil)
	if client.ABIVersion() != SyscallABIVersion {
		t.Fatalf("client.ABIVersion() = %s, expected %s", client.ABIVersion(), SyscallABIVersion)
	}

	descriptors := AllDescriptors()
	if len(descriptors) == 0 {
		t.Fatal("expected at least one descriptor")
	}

	for _, d := range descriptors {
		if d.SinceVersion == "" {
			t.Errorf("descriptor %s missing SinceVersion", d.Name)
		}
		if d.Name == "" {
			t.Errorf("descriptor %d missing Name", d.Number)
		}
		if d.Category == "" {
			t.Errorf("descriptor %s missing Category", d.Name)
		}
	}
}
