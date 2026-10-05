package syscall

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type mockFence struct {
	validToken int64
	assignment store.RuntimeAssignment
	err        error
}

func (m *mockFence) GetRuntimeAssignment(ctx context.Context, tenantID string, attemptID uuid.UUID, fencingToken int64) (store.RuntimeAssignment, error) {
	if m.err != nil {
		return store.RuntimeAssignment{}, m.err
	}
	if fencingToken != m.validToken {
		return store.RuntimeAssignment{}, errors.New("stale fencing token")
	}
	return m.assignment, nil
}

func TestSyscallDescriptorsAndParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected SyscallNumber
	}{
		{"SYS_TOOL_INVOKE", SysToolInvoke},
		{"sys_tool_invoke", SysToolInvoke},
		{"101", SysToolInvoke},
		{"SYS_MEMORY_PUT", SysMemoryPut},
		{"sys_ipc_send", SysIPCSend},
		{"501", SysRuntimeCheckpoint},
	}

	for _, tc := range tests {
		num, err := ParseSyscallNumber(tc.input)
		if err != nil {
			t.Fatalf("ParseSyscallNumber(%q) error: %v", tc.input, err)
		}
		if num != tc.expected {
			t.Fatalf("ParseSyscallNumber(%q) = %v, expected %v", tc.input, num, tc.expected)
		}
	}

	if _, err := ParseSyscallNumber("invalid_syscall_xyz"); err == nil {
		t.Fatal("expected error parsing invalid syscall name")
	}

	desc, ok := SysToolInvoke.Descriptor()
	if !ok || desc.Category != "tool" || desc.IsReadOnly {
		t.Fatalf("unexpected descriptor for SysToolInvoke: %+v", desc)
	}

	descList, ok := SysToolList.Descriptor()
	if !ok || !descList.IsReadOnly {
		t.Fatalf("expected SysToolList to be read-only")
	}

	if len(AllDescriptors()) == 0 {
		t.Fatal("expected non-empty AllDescriptors()")
	}
}

func TestSyscallErrorCodes(t *testing.T) {
	err := NewSyscallError(SyscallEPERM, "permission denied")
	if err.Code != SyscallEPERM {
		t.Fatalf("expected EPERM, got %v", err.Code)
	}
	if err.Error() != "[EPERM] permission denied" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}

	wrapped := WrapError(SyscallEBUSY, "resource locked", errors.New("lock contention"))
	if !errors.Is(wrapped, wrapped.Cause) {
		t.Fatal("expected error unwrap to succeed")
	}

	code, _ := ErrorToCode(errors.New("stale fencing token observed"))
	if code != SyscallEFENCE {
		t.Fatalf("expected SyscallEFENCE, got %v", code)
	}

	code, _ = ErrorToCode(errors.New("invalid parameter supplied"))
	if code != SyscallEINVAL {
		t.Fatalf("expected SyscallEINVAL, got %v", code)
	}
}

func TestDispatcherRegistrationAndDispatch(t *testing.T) {
	dispatcher := NewDispatcher()

	// 1. Dispatch without registered handler -> ENOSYS
	req := SyscallRequest{
		Syscall: SysToolInvoke,
		Identity: AttemptIdentity{
			TenantID:     "tenant-1",
			AttemptID:    uuid.New(),
			FencingToken: 1,
		},
	}
	resp := dispatcher.Dispatch(context.Background(), req)
	if resp.ErrorCode != SyscallENOSYS {
		t.Fatalf("expected ENOSYS, got %v (%s)", resp.ErrorCode, resp.ErrorMessage)
	}

	// 2. Register handler and dispatch -> OK
	dispatcher.RegisterFunc(SysToolInvoke, func(ctx *SyscallContext) (json.RawMessage, int64, error) {
		return json.RawMessage(`{"status":"ok"}`), 42, nil
	})

	resp = dispatcher.Dispatch(context.Background(), req)
	if resp.ErrorCode != SyscallOK {
		t.Fatalf("expected SyscallOK, got %v: %s", resp.ErrorCode, resp.ErrorMessage)
	}
	if resp.ResourceVersion != 42 {
		t.Fatalf("expected resource version 42, got %d", resp.ResourceVersion)
	}
	if string(resp.ResultJSON) != `{"status":"ok"}` {
		t.Fatalf("unexpected result: %s", string(resp.ResultJSON))
	}
}

func TestDispatcherIdentityValidation(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatcher.RegisterFunc(SysToolInvoke, func(ctx *SyscallContext) (json.RawMessage, int64, error) {
		return nil, 0, nil
	})

	// Missing tenant ID
	resp := dispatcher.Dispatch(context.Background(), SyscallRequest{
		Syscall:  SysToolInvoke,
		Identity: AttemptIdentity{AttemptID: uuid.New(), FencingToken: 1},
	})
	if resp.ErrorCode != SyscallEINVAL {
		t.Fatalf("expected EINVAL for missing tenant, got %v", resp.ErrorCode)
	}

	// Missing attempt ID
	resp = dispatcher.Dispatch(context.Background(), SyscallRequest{
		Syscall:  SysToolInvoke,
		Identity: AttemptIdentity{TenantID: "tenant-a", FencingToken: 1},
	})
	if resp.ErrorCode != SyscallEINVAL {
		t.Fatalf("expected EINVAL for nil attempt id, got %v", resp.ErrorCode)
	}
}

func TestDispatcherTenantBoundary(t *testing.T) {
	dispatcher := NewDispatcher(WithAllowedTenant("allowed-tenant"))
	dispatcher.RegisterFunc(SysToolInvoke, func(ctx *SyscallContext) (json.RawMessage, int64, error) {
		return nil, 0, nil
	})

	// Tenant mismatch
	resp := dispatcher.Dispatch(context.Background(), SyscallRequest{
		Syscall: SysToolInvoke,
		Identity: AttemptIdentity{
			TenantID:     "wrong-tenant",
			AttemptID:    uuid.New(),
			FencingToken: 1,
		},
	})
	if resp.ErrorCode != SyscallEPERM {
		t.Fatalf("expected EPERM for tenant mismatch, got %v", resp.ErrorCode)
	}

	// Tenant match
	resp = dispatcher.Dispatch(context.Background(), SyscallRequest{
		Syscall: SysToolInvoke,
		Identity: AttemptIdentity{
			TenantID:     "allowed-tenant",
			AttemptID:    uuid.New(),
			FencingToken: 1,
		},
	})
	if resp.ErrorCode != SyscallOK {
		t.Fatalf("expected OK for matching tenant, got %v", resp.ErrorCode)
	}
}

func TestDispatcherRuntimeFence(t *testing.T) {
	fence := &mockFence{
		validToken: 10,
		assignment: store.RuntimeAssignment{
			Task: store.Task{
				ID:              uuid.New(),
				TenantID:        "tenant-1",
				AgentVersionRef: "default/agent@1.0.0",
			},
			Run: store.Run{
				ID: uuid.New(),
			},
		},
	}

	dispatcher := NewDispatcher(WithRuntimeFence(fence))
	var capturedAssignment store.RuntimeAssignment
	dispatcher.RegisterFunc(SysToolInvoke, func(ctx *SyscallContext) (json.RawMessage, int64, error) {
		capturedAssignment = ctx.Assignment
		return json.RawMessage(`{}`), 0, nil
	})

	// Stale fencing token
	resp := dispatcher.Dispatch(context.Background(), SyscallRequest{
		Syscall: SysToolInvoke,
		Identity: AttemptIdentity{
			TenantID:     "tenant-1",
			AttemptID:    uuid.New(),
			FencingToken: 5, // Stale! Expected 10
		},
	})
	if resp.ErrorCode != SyscallEFENCE {
		t.Fatalf("expected EFENCE for stale token, got %v (%s)", resp.ErrorCode, resp.ErrorMessage)
	}

	// Valid fencing token
	resp = dispatcher.Dispatch(context.Background(), SyscallRequest{
		Syscall: SysToolInvoke,
		Identity: AttemptIdentity{
			TenantID:     "tenant-1",
			AttemptID:    uuid.New(),
			FencingToken: 10, // Valid!
		},
	})
	if resp.ErrorCode != SyscallOK {
		t.Fatalf("expected OK for valid token, got %v (%s)", resp.ErrorCode, resp.ErrorMessage)
	}
	if capturedAssignment.Task.AgentVersionRef != "default/agent@1.0.0" {
		t.Fatalf("expected assignment to be passed to handler, got %+v", capturedAssignment)
	}
}

func TestDispatcherConcurrency(t *testing.T) {
	metrics := NewStandardMetrics()
	dispatcher := NewDispatcher(WithMetrics(metrics))

	dispatcher.RegisterFunc(SysToolInvoke, func(ctx *SyscallContext) (json.RawMessage, int64, error) {
		time.Sleep(1 * time.Millisecond)
		return json.RawMessage(`{"res":"ok"}`), 1, nil
	})

	const numGoroutines = 20
	const callsPerGoroutine = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				req := SyscallRequest{
					Syscall: SysToolInvoke,
					Identity: AttemptIdentity{
						TenantID:     "tenant-concurrency",
						AttemptID:    uuid.New(),
						FencingToken: 1,
					},
				}
				resp := dispatcher.Dispatch(context.Background(), req)
				if resp.ErrorCode != SyscallOK {
					t.Errorf("expected OK, got %v", resp.ErrorCode)
				}
			}
		}()
	}

	wg.Wait()

	if metrics.TotalCount() != numGoroutines*callsPerGoroutine {
		t.Fatalf("expected total calls %d, got %d", numGoroutines*callsPerGoroutine, metrics.TotalCount())
	}
	if metrics.SuccessCount() != numGoroutines*callsPerGoroutine {
		t.Fatalf("expected success calls %d, got %d", numGoroutines*callsPerGoroutine, metrics.SuccessCount())
	}
}

func TestDispatcherNamespaceEnforcer(t *testing.T) {
	ctx := context.Background()
	nsStore := namespace.NewMemoryStore()
	enforcer := namespace.NewEnforcer(nsStore)

	tenant := "tenant-ns-disp"
	activeNS := "active-ns"
	termNS := "terminating-ns"

	_ = nsStore.CreateNamespace(ctx, &namespace.Namespace{
		TenantID: tenant,
		Name:     activeNS,
		Phase:    namespace.NamespacePhaseActive,
	})
	_ = nsStore.CreateNamespace(ctx, &namespace.Namespace{
		TenantID: tenant,
		Name:     termNS,
		Phase:    namespace.NamespacePhaseTerminating,
	})

	fence := &mockFence{
		validToken: 1,
		assignment: store.RuntimeAssignment{
			Task: store.Task{
				TenantID:  tenant,
				Namespace: activeNS,
			},
		},
	}

	dispatcher := NewDispatcher(
		WithAllowedTenant(tenant),
		WithRuntimeFence(fence),
		WithNamespaceEnforcer(enforcer),
	)

	dispatcher.RegisterFunc(SysToolInvoke, func(ctx *SyscallContext) (json.RawMessage, int64, error) {
		return json.RawMessage(`{"res":"ok"}`), 1, nil
	})

	// 1. Dispatch with active namespace succeeds
	req := SyscallRequest{
		Syscall: SysToolInvoke,
		Identity: AttemptIdentity{
			TenantID:     tenant,
			AttemptID:    uuid.New(),
			FencingToken: 1,
		},
	}
	resp := dispatcher.Dispatch(ctx, req)
	if resp.ErrorCode != SyscallOK {
		t.Fatalf("expected OK with active namespace, got %v: %s", resp.ErrorCode, resp.ErrorMessage)
	}

	// 2. Dispatch with terminating namespace returns SyscallEPERM
	fence.assignment.Task.Namespace = termNS
	resp = dispatcher.Dispatch(ctx, req)
	if resp.ErrorCode != SyscallEPERM {
		t.Fatalf("expected SyscallEPERM with terminating namespace, got %v: %s", resp.ErrorCode, resp.ErrorMessage)
	}
}
