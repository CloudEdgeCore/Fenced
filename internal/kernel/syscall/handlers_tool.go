package syscall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
)

// ToolInvoker defines the kernel tool execution boundary.
type ToolInvoker interface {
	InvokeTool(context.Context, tool.InvokeInput) (tool.InvokeResult, error)
	ListTools(context.Context, string) ([]store.ToolDescriptor, error)
	GetToolDescriptor(ctx context.Context, tenantID, name, version string) (store.ToolDescriptor, error)
}

// ToolSyscallHandler handles SYS_TOOL_INVOKE and SYS_TOOL_LIST.
type ToolSyscallHandler struct {
	invoker      ToolInvoker
	capabilities *capability.Authorizer
}

// NewToolSyscallHandler creates a new ToolSyscallHandler.
func NewToolSyscallHandler(invoker ToolInvoker, caps *capability.Authorizer) *ToolSyscallHandler {
	return &ToolSyscallHandler{
		invoker:      invoker,
		capabilities: caps,
	}
}

// Handle executes the tool syscall according to the request's SyscallNumber.
func (h *ToolSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "tool invoker is not configured")
	}

	switch ctx.Request.Syscall {
	case SysToolInvoke:
		return h.handleInvoke(ctx)
	case SysToolList:
		return h.handleList(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported tool syscall: %v", ctx.Request.Syscall))
	}
}

func (h *ToolSyscallHandler) handleInvoke(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload ToolInvokePayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed tool invoke payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	// Enforce capability authorization if authorizer is present
	if h.capabilities != nil && versionRef != "" {
		if err := h.capabilities.Authorize(ctx, tenantID, versionRef, capability.Tool, payload.ToolName); err != nil {
			return nil, 0, WrapError(SyscallEPERM, fmt.Sprintf("capability denied for tool %q", payload.ToolName), err)
		}
	}

	taskID := payload.TaskID
	if taskID == uuid.Nil {
		taskID = ctx.Assignment.Task.ID
	}
	runID := payload.RunID
	if runID == uuid.Nil {
		runID = ctx.Assignment.Run.ID
	}

	var approvalID *uuid.UUID
	if payload.ApprovalID != "" {
		if parsed, err := uuid.Parse(payload.ApprovalID); err == nil {
			approvalID = &parsed
		}
	}

	input := tool.InvokeInput{
		TenantID:        tenantID,
		TaskID:          taskID,
		RunID:           runID,
		AttemptID:       ctx.Request.Identity.AttemptID,
		FencingToken:    ctx.Request.Identity.FencingToken,
		AgentVersionRef: versionRef,
		ToolName:        payload.ToolName,
		ToolVersion:     payload.ToolVersion,
		Action:          payload.Action,
		Resource:        payload.Resource,
		Args:            payload.ArgsJSON,
		IdempotencyKey:  ctx.Request.IdempotencyKey,
		ApprovalID:      approvalID,
		SecretRef:       payload.SecretRef,
	}

	res, err := h.invoker.InvokeTool(ctx, input)
	if err != nil {
		return nil, 0, err
	}

	result := ToolInvokeResult{
		Outcome:          string(res.Outcome),
		ResultJSON:       res.Result,
		DenyReasons:      res.DenyReasons,
		PolicyRevision:   res.PolicyRevision,
		ReceiptOperation: res.ReceiptOperation,
	}
	if res.ApprovalID != nil {
		result.ApprovalID = res.ApprovalID.String()
	}
	if res.ToolCall.ID != uuid.Nil {
		result.ToolCallID = res.ToolCall.ID.String()
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize tool invoke result", err)
	}

	return raw, 0, nil
}

func (h *ToolSyscallHandler) handleList(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload ToolListPayload
	if len(ctx.Request.PayloadJSON) > 0 {
		_ = json.Unmarshal(ctx.Request.PayloadJSON, &payload)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	tools, err := h.invoker.ListTools(ctx, tenantID)
	if err != nil {
		return nil, 0, err
	}

	// Filter by capability if configured
	filtered := tools
	if h.capabilities != nil && versionRef != "" {
		filtered = make([]store.ToolDescriptor, 0, len(tools))
		for _, t := range tools {
			if err := h.capabilities.Authorize(ctx, tenantID, versionRef, capability.Tool, t.Name); err == nil {
				filtered = append(filtered, t)
			}
		}
	}

	result := ToolListResult{
		Tools: filtered,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize tool list result", err)
	}

	return raw, 0, nil
}
