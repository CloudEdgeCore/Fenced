package syscall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/google/uuid"
)

// IPCKernelService abstracts the kernel-level IPC communication engine.
type IPCKernelService interface {
	Send(ctx context.Context, msg *ipc.AgentMessage) error
	Receive(ctx context.Context, addr ipc.AgentAddress, maxMessages int) ([]*ipc.AgentMessage, error)
	Ack(ctx context.Context, addr ipc.AgentAddress, messageIDs []string) error
}

// IPCSyscallHandler handles SYS_IPC_SEND, SYS_IPC_RECEIVE, and SYS_IPC_ACK.
type IPCSyscallHandler struct {
	service IPCKernelService
}

// NewIPCSyscallHandler creates a new IPCSyscallHandler.
func NewIPCSyscallHandler(service IPCKernelService) *IPCSyscallHandler {
	return &IPCSyscallHandler{
		service: service,
	}
}

// Handle executes IPC syscalls.
func (h *IPCSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.service == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "ipc service is not configured")
	}

	switch ctx.Request.Syscall {
	case SysIPCSend:
		return h.handleSend(ctx)
	case SysIPCReceive:
		return h.handleReceive(ctx)
	case SysIPCAck:
		return h.handleAck(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported ipc syscall: %v", ctx.Request.Syscall))
	}
}

func (h *IPCSyscallHandler) handleSend(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload IPCSendPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed ipc send payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	senderAddr := resolveAddress(tenantID, payload.AgentVersionRef, payload.To.EffectiveNamespace(), ctx.Assignment.Run.ID.String())

	msgType := ipc.MessageType(payload.Kind)

	if msgType == "" {
		msgType = ipc.MessageTypeRequest
	}

	ttl := 1 * time.Minute
	if payload.Deadline != nil && !payload.Deadline.IsZero() {
		dur := time.Until(*payload.Deadline)
		if dur > 0 {
			ttl = dur
		}
	}

	msg, err := ipc.NewMessage(senderAddr, payload.To, msgType, payload.PayloadJSON, ttl)
	if err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "failed to create ipc message", err)
	}

	if payload.CorrelationID != "" {
		msg.CorrelationID = payload.CorrelationID
	}
	if ctx.Request.IdempotencyKey != "" {
		// Use idempotency key as message ID if provided
		if _, parseErr := uuid.Parse(ctx.Request.IdempotencyKey); parseErr == nil {
			msg.ID = ctx.Request.IdempotencyKey
		}
	}

	if err := h.service.Send(ctx, msg); err != nil {
		if errors.Is(err, ipc.ErrPeerDenied) || errors.Is(err, ipc.ErrReceiverDenied) || errors.Is(err, ipc.ErrUnauthorizedSignal) || errors.Is(err, ipc.ErrCrossTenantDenied) || errors.Is(err, ipc.ErrCrossNamespaceDenied) || errors.Is(err, ipc.ErrCapabilityDenied) {
			return nil, 0, WrapError(SyscallEPERM, "ipc send authorization failed", err)
		}
		if errors.Is(err, ipc.ErrFenced) {
			return nil, 0, WrapError(SyscallEFENCE, "ipc send fenced", err)
		}
		return nil, 0, err
	}

	result := IPCSendResult{
		MessageID:  msg.ID,
		Replayed:   false,
		AcceptedAt: msg.CreatedAt,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize ipc send result", err)
	}

	return raw, 0, nil
}

func (h *IPCSyscallHandler) handleReceive(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload IPCReceivePayload
	if len(ctx.Request.PayloadJSON) > 0 {
		_ = json.Unmarshal(ctx.Request.PayloadJSON, &payload)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	addr := resolveAddress(tenantID, versionRef, ctx.Assignment.Task.Namespace, ctx.Assignment.Run.ID.String())

	maxMessages := payload.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 10
	}

	messages, err := h.service.Receive(ctx, addr, maxMessages)
	if err != nil {
		return nil, 0, err
	}

	result := IPCReceiveResult{
		Messages: messages,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize ipc receive result", err)
	}

	return raw, 0, nil
}

func (h *IPCSyscallHandler) handleAck(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload IPCAckPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed ipc ack payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	addr := resolveAddress(tenantID, versionRef, ctx.Assignment.Task.Namespace, ctx.Assignment.Run.ID.String())

	if err := h.service.Ack(ctx, addr, payload.MessageIDs); err != nil {
		return nil, 0, err
	}

	result := IPCAckResult{
		AcknowledgedIDs: payload.MessageIDs,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize ipc ack result", err)
	}

	return raw, 0, nil
}

func resolveAddress(tenantID, versionRef, defaultNamespace, instanceID string) ipc.AgentAddress {
	agentID := versionRef
	namespace := defaultNamespace
	if ns, name, _, err := agentversion.ParseRef(versionRef); err == nil {
		agentID = name
		if ns != "" {
			namespace = ns
		}
	} else if clean := strings.ReplaceAll(strings.ReplaceAll(agentID, "@", "-"), "/", "-"); clean != "" {
		agentID = clean
	}
	if agentID == "" {
		agentID = "anonymous"
	}
	return ipc.AgentAddress{
		TenantID:   tenantID,
		Namespace:  namespace,
		AgentID:    agentID,
		InstanceID: instanceID,
	}
}
