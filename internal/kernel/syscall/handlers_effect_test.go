package syscall

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/effect"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type fakeEffectInvoker struct {
	executeFn func(ctx context.Context, req *effect.EffectRequest) (*effect.EffectReceipt, error)
	getFn     func(ctx context.Context, tenantID, effectID string) (*effect.EffectRecord, error)
	getByKey  func(ctx context.Context, tenantID, agentID, idempotencyKey string) (*effect.EffectRecord, error)
}

func (f *fakeEffectInvoker) ExecuteEffect(ctx context.Context, req *effect.EffectRequest) (*effect.EffectReceipt, error) {
	if f.executeFn != nil {
		return f.executeFn(ctx, req)
	}
	return &effect.EffectReceipt{
		EffectID:     req.ID,
		TenantID:     req.TenantID,
		Status:       effect.EffectStatusCommitted,
		FencingToken: req.FencingToken,
	}, nil
}

func (f *fakeEffectInvoker) GetEffect(ctx context.Context, tenantID, effectID string) (*effect.EffectRecord, error) {
	if f.getFn != nil {
		return f.getFn(ctx, tenantID, effectID)
	}
	return nil, effect.ErrEffectNotFound
}

func (f *fakeEffectInvoker) GetEffectByIdempotencyKey(ctx context.Context, tenantID, agentID, idempotencyKey string) (*effect.EffectRecord, error) {
	if f.getByKey != nil {
		return f.getByKey(ctx, tenantID, agentID, idempotencyKey)
	}
	return nil, effect.ErrEffectNotFound
}

func TestEffectSyscall_Execute_Success(t *testing.T) {
	invoker := &fakeEffectInvoker{}
	handler := NewEffectSyscallHandler(invoker, nil)

	payload := EffectExecutePayload{
		AgentID:        "agent-1",
		Provider:       "payment",
		Operation:      "charge",
		IdempotencyKey: "charge-123",
		Payload:        json.RawMessage(`{"amount":10}`),
	}
	payloadBytes, _ := json.Marshal(payload)

	ctx := &SyscallContext{
		Context: context.Background(),
		Request: SyscallRequest{
			Syscall: SysEffectExecute,
			Identity: AttemptIdentity{
				TenantID:     "tenant-1",
				AttemptID:    uuid.New(),
				FencingToken: 1,
			},
			PayloadJSON: payloadBytes,
		},
		Assignment: store.RuntimeAssignment{
			Task: store.Task{
				AgentVersionRef: "agent-1:v1",
			},
		},
	}

	resJSON, token, err := handler.Handle(ctx)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if token != 1 {
		t.Fatalf("expected token 1, got %d", token)
	}

	var receipt effect.EffectReceipt
	if err := json.Unmarshal(resJSON, &receipt); err != nil {
		t.Fatalf("failed to unmarshal receipt: %v", err)
	}
	if receipt.Status != effect.EffectStatusCommitted {
		t.Fatalf("expected COMMITTED, got %s", receipt.Status)
	}
}

func TestEffectSyscall_Execute_Fenced(t *testing.T) {
	invoker := &fakeEffectInvoker{
		executeFn: func(ctx context.Context, req *effect.EffectRequest) (*effect.EffectReceipt, error) {
			return nil, effect.ErrEffectFenced
		},
	}
	handler := NewEffectSyscallHandler(invoker, nil)

	payloadBytes, _ := json.Marshal(EffectExecutePayload{
		AgentID:        "agent-1",
		Provider:       "webhook",
		Operation:      "post",
		IdempotencyKey: "key-fenced",
	})

	ctx := &SyscallContext{
		Context: context.Background(),
		Request: SyscallRequest{
			Syscall:     SysEffectExecute,
			Identity:    AttemptIdentity{TenantID: "tenant-1", AttemptID: uuid.New(), FencingToken: 1},
			PayloadJSON: payloadBytes,
		},
	}

	_, _, err := handler.Handle(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var sysErr *SyscallError
	if !errors.As(err, &sysErr) {
		t.Fatalf("expected SyscallError, got %v", err)
	}
	if sysErr.Code != SyscallEFENCE {
		t.Fatalf("expected SyscallEFENCE, got %v", sysErr.Code)
	}
}

func TestEffectSyscall_Execute_Unknown(t *testing.T) {
	invoker := &fakeEffectInvoker{
		executeFn: func(ctx context.Context, req *effect.EffectRequest) (*effect.EffectReceipt, error) {
			return &effect.EffectReceipt{
				EffectID: req.ID,
				Status:   effect.EffectStatusUnknown,
			}, effect.ErrEffectUnknown
		},
	}
	handler := NewEffectSyscallHandler(invoker, nil)

	payloadBytes, _ := json.Marshal(EffectExecutePayload{
		AgentID:        "agent-1",
		Provider:       "webhook",
		Operation:      "post",
		IdempotencyKey: "key-unknown",
	})

	ctx := &SyscallContext{
		Context: context.Background(),
		Request: SyscallRequest{
			Syscall:     SysEffectExecute,
			Identity:    AttemptIdentity{TenantID: "tenant-1", AttemptID: uuid.New(), FencingToken: 1},
			PayloadJSON: payloadBytes,
		},
	}

	_, _, err := handler.Handle(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var sysErr *SyscallError
	if !errors.As(err, &sysErr) {
		t.Fatalf("expected SyscallError, got %v", err)
	}
	if sysErr.Code != SyscallEUNKNOWN {
		t.Fatalf("expected SyscallEUNKNOWN, got %v", sysErr.Code)
	}
}

func TestEffectSyscall_Get(t *testing.T) {
	record := &effect.EffectRecord{
		Request: effect.EffectRequest{
			ID:             "eff-123",
			TenantID:       "tenant-1",
			AgentID:        "agent-1",
			IdempotencyKey: "key-123",
		},
		Status:  effect.EffectStatusCommitted,
		Version: 3,
	}

	invoker := &fakeEffectInvoker{
		getFn: func(ctx context.Context, tenantID, effectID string) (*effect.EffectRecord, error) {
			if effectID == "eff-123" {
				return record, nil
			}
			return nil, effect.ErrEffectNotFound
		},
		getByKey: func(ctx context.Context, tenantID, agentID, idempotencyKey string) (*effect.EffectRecord, error) {
			if idempotencyKey == "key-123" {
				return record, nil
			}
			return nil, effect.ErrEffectNotFound
		},
	}
	handler := NewEffectSyscallHandler(invoker, nil)

	// Test Get by ID
	payload1, _ := json.Marshal(EffectGetPayload{EffectID: "eff-123"})
	ctx1 := &SyscallContext{
		Context: context.Background(),
		Request: SyscallRequest{
			Syscall:     SysEffectGet,
			Identity:    AttemptIdentity{TenantID: "tenant-1"},
			PayloadJSON: payload1,
		},
	}
	res1, version1, err := handler.Handle(ctx1)
	if err != nil {
		t.Fatalf("get by ID failed: %v", err)
	}
	if version1 != 3 {
		t.Fatalf("expected version 3, got %d", version1)
	}

	var rec1 effect.EffectRecord
	if err := json.Unmarshal(res1, &rec1); err != nil {
		t.Fatalf("failed to decode record: %v", err)
	}
	if rec1.Request.ID != "eff-123" {
		t.Fatalf("expected eff-123, got %s", rec1.Request.ID)
	}

	// Test Get by IdempotencyKey
	payload2, _ := json.Marshal(EffectGetPayload{IdempotencyKey: "key-123", AgentID: "agent-1"})
	ctx2 := &SyscallContext{
		Context: context.Background(),
		Request: SyscallRequest{
			Syscall:     SysEffectGet,
			Identity:    AttemptIdentity{TenantID: "tenant-1"},
			PayloadJSON: payload2,
		},
	}
	res2, version2, err := handler.Handle(ctx2)
	if err != nil {
		t.Fatalf("get by key failed: %v", err)
	}
	if version2 != 3 {
		t.Fatalf("expected version 3, got %d", version2)
	}
	var rec2 effect.EffectRecord
	if err := json.Unmarshal(res2, &rec2); err != nil {
		t.Fatalf("failed to decode record: %v", err)
	}
	if rec2.Request.IdempotencyKey != "key-123" {
		t.Fatalf("expected key-123, got %s", rec2.Request.IdempotencyKey)
	}
}
