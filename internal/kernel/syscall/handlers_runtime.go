package syscall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// RuntimeLifecycleInvoker defines the execution boundary for runtime attempts and checkpoints.
type RuntimeLifecycleInvoker interface {
	CommitCheckpoint(context.Context, store.CommitCheckpointInput) (store.Checkpoint, store.Attempt, error)
	CompleteAttempt(context.Context, store.CompleteAttemptInput) (store.CompleteAttemptResult, error)
}

// RuntimeSyscallHandler handles SYS_RUNTIME_CHECKPOINT, SYS_RUNTIME_COMPLETE, and SYS_RUNTIME_YIELD.
type RuntimeSyscallHandler struct {
	invoker RuntimeLifecycleInvoker
}

// NewRuntimeSyscallHandler creates a new RuntimeSyscallHandler.
func NewRuntimeSyscallHandler(invoker RuntimeLifecycleInvoker) *RuntimeSyscallHandler {
	return &RuntimeSyscallHandler{
		invoker: invoker,
	}
}

// Handle executes runtime lifecycle syscalls.
func (h *RuntimeSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	switch ctx.Request.Syscall {
	case SysRuntimeCheckpoint:
		return h.handleCheckpoint(ctx)
	case SysRuntimeComplete:
		return h.handleComplete(ctx)
	case SysRuntimeYield:
		return h.handleYield(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported runtime syscall: %v", ctx.Request.Syscall))
	}
}

func (h *RuntimeSyscallHandler) handleCheckpoint(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "runtime invoker is not configured")
	}

	var payload RuntimeCheckpointPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed runtime checkpoint payload", err)
	}

	checkpointID := uuid.New()
	if payload.CheckpointID != "" {
		if parsed, err := uuid.Parse(payload.CheckpointID); err == nil {
			checkpointID = parsed
		}
	}

	idempotencyKey := ctx.Request.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = checkpointID.String()
	}

	providerName := ctx.Assignment.Attempt.RuntimeClass
	if providerName == "" {
		providerName = "default"
	}

	input := store.CommitCheckpointInput{
		TenantID:               ctx.Request.Identity.TenantID,
		AttemptID:              ctx.Request.Identity.AttemptID,
		FencingToken:           ctx.Request.Identity.FencingToken,
		ExpectedAttemptVersion: payload.ExpectedAttemptVersion,
		IdempotencyKey:         idempotencyKey,
		CheckpointID:           checkpointID,
		AgentVersionRef:        ctx.Assignment.Task.AgentVersionRef,
		Provider:               providerName,
		RuntimeABI:             "fenced-v1",
		SchemaVersion:          "v1.0",
		State:                  payload.State,
		ConfirmedReceiptIDs:    payload.ConfirmedReceiptIDs,
	}

	chk, att, err := h.invoker.CommitCheckpoint(ctx, input)
	if err != nil {
		return nil, 0, err
	}

	result := RuntimeCheckpointResult{
		Checkpoint:     chk,
		AttemptVersion: att.ResourceVersion,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize checkpoint result", err)
	}

	return raw, att.ResourceVersion, nil
}

func (h *RuntimeSyscallHandler) handleComplete(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "runtime invoker is not configured")
	}

	var payload RuntimeCompletePayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed runtime complete payload", err)
	}

	idempotencyKey := ctx.Request.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = uuid.New().String()
	}

	input := store.CompleteAttemptInput{
		TenantID:               ctx.Request.Identity.TenantID,
		AttemptID:              ctx.Request.Identity.AttemptID,
		FencingToken:           ctx.Request.Identity.FencingToken,
		ExpectedAttemptVersion: payload.ExpectedAttemptVersion,
		IdempotencyKey:         idempotencyKey,
		Result:                 payload.Result,
	}

	out, err := h.invoker.CompleteAttempt(ctx, input)
	if err != nil {
		return nil, 0, err
	}

	result := RuntimeCompleteResult{
		AttemptVersion: out.Attempt.ResourceVersion,
		ResultRef:      payload.Result.URI,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize completion result", err)
	}

	return raw, out.Attempt.ResourceVersion, nil
}

func (h *RuntimeSyscallHandler) handleYield(ctx *SyscallContext) (json.RawMessage, int64, error) {
	result := RuntimeYieldResult{
		Yielded: true,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize yield result", err)
	}

	return raw, 0, nil
}
