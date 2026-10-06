package syscall

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// RuntimeFence validates that an attempt's fencing token is current.
type RuntimeFence interface {
	GetRuntimeAssignment(ctx context.Context, tenantID string, attemptID uuid.UUID, fencingToken int64) (store.RuntimeAssignment, error)
}

// SyscallContext encapsulates execution context, request payload, and verified attempt assignment.
type SyscallContext struct {
	context.Context
	Request    SyscallRequest
	Assignment store.RuntimeAssignment
}

// SyscallHandler handles execution of a specific system call.
type SyscallHandler interface {
	Handle(ctx *SyscallContext) (json.RawMessage, int64, error)
}

// HandlerFunc adapts a function to satisfy the SyscallHandler interface.
type HandlerFunc func(ctx *SyscallContext) (json.RawMessage, int64, error)

// Handle executes the underlying function.
func (f HandlerFunc) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	return f(ctx)
}

// SyscallDispatcher is the central kernel execution engine for all agent system calls.
type SyscallDispatcher struct {
	mu            sync.RWMutex
	handlers      map[SyscallNumber]SyscallHandler
	fences        RuntimeFence
	enforcer      *namespace.Enforcer
	allowedTenant string
	metrics       Metrics
	tracer        trace.Tracer
}

// DispatcherOption configures the SyscallDispatcher.
type DispatcherOption func(*SyscallDispatcher)

// WithRuntimeFence attaches a fence verifier to the dispatcher.
func WithRuntimeFence(fences RuntimeFence) DispatcherOption {
	return func(d *SyscallDispatcher) {
		d.fences = fences
	}
}

// WithNamespaceEnforcer attaches a namespace and quota enforcer to the dispatcher.
func WithNamespaceEnforcer(enforcer *namespace.Enforcer) DispatcherOption {
	return func(d *SyscallDispatcher) {
		d.enforcer = enforcer
	}
}

// WithAllowedTenant sets the tenant boundary for this dispatcher instance.
func WithAllowedTenant(tenant string) DispatcherOption {
	return func(d *SyscallDispatcher) {
		d.allowedTenant = tenant
	}
}

// WithMetrics attaches a metrics collector to the dispatcher.
func WithMetrics(m Metrics) DispatcherOption {
	return func(d *SyscallDispatcher) {
		d.metrics = m
	}
}

// WithTracer attaches an OpenTelemetry tracer to the dispatcher.
func WithTracer(t trace.Tracer) DispatcherOption {
	return func(d *SyscallDispatcher) {
		d.tracer = t
	}
}

// NewDispatcher creates a new SyscallDispatcher.
func NewDispatcher(opts ...DispatcherOption) *SyscallDispatcher {
	d := &SyscallDispatcher{
		handlers: make(map[SyscallNumber]SyscallHandler),
		metrics:  NewStandardMetrics(),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Register registers a handler for a given syscall number.
func (d *SyscallDispatcher) Register(number SyscallNumber, handler SyscallHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[number] = handler
}

// RegisterFunc registers a handler function for a given syscall number.
func (d *SyscallDispatcher) RegisterFunc(number SyscallNumber, fn func(ctx *SyscallContext) (json.RawMessage, int64, error)) {
	d.Register(number, HandlerFunc(fn))
}

// GetHandler retrieves the registered handler for a syscall number.
func (d *SyscallDispatcher) GetHandler(number SyscallNumber) (SyscallHandler, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	h, ok := d.handlers[number]
	return h, ok
}

// Dispatch processes and executes a system call request through security fences and registered handlers.
func (d *SyscallDispatcher) Dispatch(ctx context.Context, req SyscallRequest) SyscallResponse {
	start := time.Now()

	// 1. Verify syscall registration
	d.mu.RLock()
	handler, exists := d.handlers[req.Syscall]
	d.mu.RUnlock()

	if !exists || req.Syscall == SyscallNone {
		dur := time.Since(start)
		code := SyscallENOSYS
		errMsg := fmt.Sprintf("unsupported or unregistered system call: %s (%d)", req.Syscall.String(), uint32(req.Syscall))
		d.metrics.RecordSyscall(ctx, req.Syscall, code, dur)
		return SyscallResponse{
			Syscall:      req.Syscall,
			ErrorCode:    code,
			ErrorMessage: errMsg,
			DurationUS:   dur.Microseconds(),
		}
	}

	// 2. Validate Attempt Identity
	if req.Identity.TenantID == "" || req.Identity.AttemptID == uuid.Nil {
		dur := time.Since(start)
		code := SyscallEINVAL
		errMsg := "attempt identity invalid: tenant_id and attempt_id are required"
		d.metrics.RecordSyscall(ctx, req.Syscall, code, dur)
		return SyscallResponse{
			Syscall:      req.Syscall,
			ErrorCode:    code,
			ErrorMessage: errMsg,
			DurationUS:   dur.Microseconds(),
		}
	}

	// 3. Tenant authorization check
	if d.allowedTenant != "" && d.allowedTenant != "*" && req.Identity.TenantID != d.allowedTenant {
		dur := time.Since(start)
		code := SyscallEPERM
		errMsg := fmt.Sprintf("tenant %q is not authorized by this kernel instance", req.Identity.TenantID)
		d.metrics.RecordSyscall(ctx, req.Syscall, code, dur)
		return SyscallResponse{
			Syscall:      req.Syscall,
			ErrorCode:    code,
			ErrorMessage: errMsg,
			DurationUS:   dur.Microseconds(),
		}
	}

	// 4. Runtime Fence verification (stale lease check)
	var assignment store.RuntimeAssignment
	if d.fences != nil {
		var err error
		assignment, err = d.fences.GetRuntimeAssignment(ctx, req.Identity.TenantID, req.Identity.AttemptID, req.Identity.FencingToken)
		if err != nil {
			dur := time.Since(start)
			code := SyscallEFENCE
			errMsg := fmt.Sprintf("runtime fence rejection: %v", err)
			d.metrics.RecordSyscall(ctx, req.Syscall, code, dur)
			return SyscallResponse{
				Syscall:      req.Syscall,
				ErrorCode:    code,
				ErrorMessage: errMsg,
				DurationUS:   dur.Microseconds(),
			}
		}
	}

	// 5. Namespace verification if enforcer is configured and assignment is present
	if d.enforcer != nil && assignment.Task.Namespace != "" {
		if _, err := d.enforcer.CheckNamespaceActive(ctx, req.Identity.TenantID, assignment.Task.Namespace); err != nil {
			dur := time.Since(start)
			code := SyscallEPERM
			errMsg := fmt.Sprintf("namespace %q access violation: %v", assignment.Task.Namespace, err)
			d.metrics.RecordSyscall(ctx, req.Syscall, code, dur)
			return SyscallResponse{
				Syscall:      req.Syscall,
				ErrorCode:    code,
				ErrorMessage: errMsg,
				DurationUS:   dur.Microseconds(),
			}
		}
	}

	// 6. Execute handler
	sCtx := &SyscallContext{
		Context:    ctx,
		Request:    req,
		Assignment: assignment,
	}

	resultJSON, resVer, err := handler.Handle(sCtx)
	dur := time.Since(start)

	code, errMsg := ErrorToCode(err)
	d.metrics.RecordSyscall(ctx, req.Syscall, code, dur)

	return SyscallResponse{
		Syscall:         req.Syscall,
		ErrorCode:       code,
		ErrorMessage:    errMsg,
		ResultJSON:      resultJSON,
		DurationUS:      dur.Microseconds(),
		ResourceVersion: resVer,
	}
}
