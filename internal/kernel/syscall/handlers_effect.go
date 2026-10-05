package syscall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/effect"
)

// EffectInvoker defines the kernel boundary for external effect execution.
type EffectInvoker interface {
	ExecuteEffect(ctx context.Context, req *effect.EffectRequest) (*effect.EffectReceipt, error)
	GetEffect(ctx context.Context, tenantID, effectID string) (*effect.EffectRecord, error)
	GetEffectByIdempotencyKey(ctx context.Context, tenantID, agentID, idempotencyKey string) (*effect.EffectRecord, error)
}

// EffectSyscallHandler handles SYS_EFFECT_EXECUTE and SYS_EFFECT_GET.
type EffectSyscallHandler struct {
	invoker      EffectInvoker
	capabilities *capability.Authorizer
}

// NewEffectSyscallHandler creates a new EffectSyscallHandler.
func NewEffectSyscallHandler(invoker EffectInvoker, caps *capability.Authorizer) *EffectSyscallHandler {
	return &EffectSyscallHandler{
		invoker:      invoker,
		capabilities: caps,
	}
}

// Handle executes the effect syscall based on SyscallNumber.
func (h *EffectSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "effect invoker is not configured")
	}

	switch ctx.Request.Syscall {
	case SysEffectExecute:
		return h.handleExecute(ctx)
	case SysEffectGet:
		return h.handleGet(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported effect syscall: %v", ctx.Request.Syscall))
	}
}

func (h *EffectSyscallHandler) handleExecute(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload EffectExecutePayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed effect execute payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	agentID := payload.AgentID
	if agentID == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		agentID = ctx.Assignment.Task.AgentVersionRef
	}

	runID := payload.RunID
	if runID == "" && ctx.Assignment.Run.ID != [16]byte{} {
		runID = ctx.Assignment.Run.ID.String()
	}

	idempotencyKey := payload.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = ctx.Request.IdempotencyKey
	}

	req := &effect.EffectRequest{
		TenantID:       tenantID,
		AgentID:        agentID,
		RunID:          runID,
		AttemptID:      ctx.Request.Identity.AttemptID.String(),
		FencingToken:   ctx.Request.Identity.FencingToken,
		Provider:       payload.Provider,
		Operation:      payload.Operation,
		IdempotencyKey: idempotencyKey,
		Payload:        payload.Payload,
		Deadline:       payload.Deadline,
		TraceID:        payload.TraceID,
	}

	receipt, err := h.invoker.ExecuteEffect(ctx, req)
	if err != nil {
		if errors.Is(err, effect.ErrEffectFenced) {
			return nil, 0, WrapError(SyscallEFENCE, "effect execution fenced", err)
		}
		if errors.Is(err, effect.ErrEffectUnknown) {
			// Ambiguous outcome: return EUNKNOWN with the receipt if available
			var receiptJSON json.RawMessage
			if receipt != nil {
				receiptJSON, _ = json.Marshal(receipt)
			}
			return receiptJSON, 0, WrapError(SyscallEUNKNOWN, "effect outcome is unknown; auto-replay forbidden", err)
		}
		if errors.Is(err, effect.ErrEffectExecuting) {
			return nil, 0, WrapError(SyscallEBUSY, "effect is currently executing", err)
		}
		if errors.Is(err, effect.ErrPayloadMismatch) {
			return nil, 0, WrapError(SyscallEINVAL, "idempotency key conflict: payload mismatch", err)
		}
		if errors.Is(err, effect.ErrInvalidRequest) {
			return nil, 0, WrapError(SyscallEINVAL, "invalid effect request", err)
		}
		if errors.Is(err, effect.ErrEffectExpired) {
			return nil, 0, WrapError(SyscallETIMEDOUT, "effect deadline expired", err)
		}
		if errors.Is(err, effect.ErrProviderNotFound) {
			return nil, 0, WrapError(SyscallENOENT, "effect provider not found", err)
		}
		if errors.Is(err, effect.ErrProviderFailed) {
			return nil, 0, WrapError(SyscallEINTERNAL, "effect provider execution failed", err)
		}
		return nil, 0, WrapError(SyscallEINTERNAL, "effect execution failed", err)
	}

	resJSON, err := json.Marshal(receipt)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize effect receipt", err)
	}

	return resJSON, receipt.FencingToken, nil
}

func (h *EffectSyscallHandler) handleGet(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload EffectGetPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed effect get payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	var record *effect.EffectRecord
	var err error

	if payload.EffectID != "" {
		record, err = h.invoker.GetEffect(ctx, tenantID, payload.EffectID)
	} else if payload.IdempotencyKey != "" {
		agentID := payload.AgentID
		if agentID == "" && ctx.Assignment.Task.AgentVersionRef != "" {
			agentID = ctx.Assignment.Task.AgentVersionRef
		}
		record, err = h.invoker.GetEffectByIdempotencyKey(ctx, tenantID, agentID, payload.IdempotencyKey)
	} else {
		return nil, 0, NewSyscallError(SyscallEINVAL, "either effect_id or idempotency_key is required")
	}

	if err != nil {
		if errors.Is(err, effect.ErrEffectNotFound) {
			return nil, 0, WrapError(SyscallENOENT, "effect not found", err)
		}
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to get effect", err)
	}

	resJSON, err := json.Marshal(record)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize effect record", err)
	}

	return resJSON, record.Version, nil
}
