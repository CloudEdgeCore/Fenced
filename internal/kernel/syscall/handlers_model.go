package syscall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// ModelRunner executes real model invocations.
type ModelRunner interface {
	InvokeStream(context.Context, model.InvokeInput, func(string)) (model.InvokeOutput, error)
}

// ModelInvoker governs the begin/settle/finish model session lifecycle.
type ModelInvoker interface {
	Begin(context.Context, model.BeginInput) (model.BeginResult, error)
	GetModelCall(context.Context, string, uuid.UUID) (store.ModelCall, error)
	Settle(context.Context, store.ModelCall, int64, model.Usage) error
	Finish(context.Context, store.ModelCall, model.FinishInput) (store.ModelCall, error)
}

// ModelSyscallHandler handles SYS_MODEL_INVOKE, SYS_MODEL_BEGIN, SYS_MODEL_SETTLE, SYS_MODEL_FINISH.
type ModelSyscallHandler struct {
	runner       ModelRunner
	invoker      ModelInvoker
	capabilities *capability.Authorizer
}

// NewModelSyscallHandler creates a new ModelSyscallHandler.
func NewModelSyscallHandler(runner ModelRunner, invoker ModelInvoker, caps *capability.Authorizer) *ModelSyscallHandler {
	return &ModelSyscallHandler{
		runner:       runner,
		invoker:      invoker,
		capabilities: caps,
	}
}

// Handle executes model syscalls.
func (h *ModelSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	switch ctx.Request.Syscall {
	case SysModelInvoke:
		return h.handleInvoke(ctx)
	case SysModelBegin:
		return h.handleBegin(ctx)
	case SysModelSettle:
		return h.handleSettle(ctx)
	case SysModelFinish:
		return h.handleFinish(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported model syscall: %v", ctx.Request.Syscall))
	}
}

func (h *ModelSyscallHandler) handleInvoke(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.runner == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "model runner is not configured")
	}

	var payload ModelInvokePayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed model invoke payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	if h.capabilities != nil && versionRef != "" {
		if err := h.capabilities.Authorize(ctx, tenantID, versionRef, capability.Model, payload.ModelRef); err != nil {
			return nil, 0, WrapError(SyscallEPERM, fmt.Sprintf("capability denied for model %q", payload.ModelRef), err)
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

	messages := make([]provider.Message, 0, len(payload.Messages))
	for _, m := range payload.Messages {
		calls := make([]provider.ToolCall, 0, len(m.ToolCalls))
		for _, tc := range m.ToolCalls {
			calls = append(calls, provider.ToolCall{
				ID:        tc.ID,
				Name:      tc.Name,
				Arguments: tc.Arguments,
			})
		}
		messages = append(messages, provider.Message{
			Role:       m.Role,
			Content:    m.Content,
			ToolCallID: m.ToolCallID,
			ToolCalls:  calls,
		})
	}

	tools := make([]provider.ToolDefinition, 0, len(payload.Tools))
	for _, t := range payload.Tools {
		tools = append(tools, provider.ToolDefinition{
			Name:        t.Name,
			Description: t.Description,
			Parameters:  t.Parameters,
		})
	}

	in := model.InvokeInput{
		TenantID:        tenantID,
		TaskID:          taskID,
		RunID:           runID,
		AttemptID:       ctx.Request.Identity.AttemptID,
		FencingToken:    ctx.Request.Identity.FencingToken,
		AgentVersionRef: versionRef,
		ModelRef:        payload.ModelRef,
		IdempotencyKey:  ctx.Request.IdempotencyKey,
		Messages:        messages,
		Tools:           tools,
		Stream:          payload.Stream,
		Temperature:     payload.Temperature,
		MaxOutputTokens: payload.MaxOutputTokens,
	}

	output, err := h.runner.InvokeStream(ctx, in, nil)
	if err != nil {
		return nil, 0, err
	}

	toolCalls := make([]ModelChatToolCall, 0, len(output.ToolCalls))
	for _, tc := range output.ToolCalls {
		toolCalls = append(toolCalls, ModelChatToolCall{
			ID:        tc.ID,
			Name:      tc.Name,
			Arguments: tc.Arguments,
		})
	}

	result := ModelInvokeResult{
		CallID:            output.Call.ID,
		ModelRef:          output.Call.ModelRef,
		Status:            string(output.Call.Status),
		Content:           output.Content,
		InputTokens:       output.Call.InputTokens,
		OutputTokens:      output.Call.OutputTokens,
		CostUSD:           output.Call.CostMicroUSD.USD(),
		PriceRevision:     output.Call.PriceRevision,
		FinishReason:      output.Call.FinishReason,
		ProviderRequestID: output.Call.ProviderRequestID,
		ToolCalls:         toolCalls,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize model invoke result", err)
	}

	return raw, 0, nil
}

func (h *ModelSyscallHandler) handleBegin(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "model invoker is not configured")
	}

	var payload ModelBeginPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed model begin payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	if h.capabilities != nil && versionRef != "" {
		if err := h.capabilities.Authorize(ctx, tenantID, versionRef, capability.Model, payload.ModelRef); err != nil {
			return nil, 0, WrapError(SyscallEPERM, fmt.Sprintf("capability denied for model %q", payload.ModelRef), err)
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

	beginRes, err := h.invoker.Begin(ctx, model.BeginInput{
		TenantID:        tenantID,
		TaskID:          taskID,
		RunID:           runID,
		AttemptID:       ctx.Request.Identity.AttemptID,
		FencingToken:    ctx.Request.Identity.FencingToken,
		AgentVersionRef: versionRef,
		ModelRef:        payload.ModelRef,
		IdempotencyKey:  ctx.Request.IdempotencyKey,
	})
	if err != nil {
		return nil, 0, err
	}

	result := ModelBeginResult{
		CallID:          beginRes.Call.ID,
		ModelRef:        beginRes.Call.ModelRef,
		Status:          string(beginRes.Call.Status),
		PriceRevision:   beginRes.Call.PriceRevision,
		PolicyRevision:  beginRes.PolicyRevision,
		ResourceVersion: beginRes.Call.ResourceVersion,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize model begin result", err)
	}

	return raw, beginRes.Call.ResourceVersion, nil
}

func (h *ModelSyscallHandler) handleSettle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "model invoker is not configured")
	}

	var payload ModelSettlePayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed model settle payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	call, err := h.invoker.GetModelCall(ctx, tenantID, payload.CallID)
	if err != nil {
		return nil, 0, err
	}

	usage := model.Usage{
		InputTokens:  payload.InputTokens,
		OutputTokens: payload.OutputTokens,
	}

	if err := h.invoker.Settle(ctx, call, payload.Sequence, usage); err != nil {
		return nil, 0, err
	}

	result := ModelSettleResult{
		CallID: payload.CallID,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize model settle result", err)
	}

	return raw, 0, nil
}

func (h *ModelSyscallHandler) handleFinish(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "model invoker is not configured")
	}

	var payload ModelFinishPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed model finish payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	call, err := h.invoker.GetModelCall(ctx, tenantID, payload.CallID)
	if err != nil {
		return nil, 0, err
	}

	finishIn := model.FinishInput{
		ExpectedVersion:   payload.ExpectedVersion,
		Status:            store.ModelCallStatus(payload.Status),
		InputTokens:       payload.InputTokens,
		OutputTokens:      payload.OutputTokens,
		ProviderRequestID: payload.ProviderRequestID,
		FinishReason:      payload.FinishReason,
	}

	finishedCall, err := h.invoker.Finish(ctx, call, finishIn)
	if err != nil {
		return nil, 0, err
	}

	result := ModelFinishResult{
		CallID:        finishedCall.ID,
		ModelRef:      finishedCall.ModelRef,
		Status:        string(finishedCall.Status),
		InputTokens:   finishedCall.InputTokens,
		OutputTokens:  finishedCall.OutputTokens,
		CostUSD:       finishedCall.CostMicroUSD.USD(),
		PriceRevision: finishedCall.PriceRevision,
		FinishReason:  finishedCall.FinishReason,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize model finish result", err)
	}

	return raw, finishedCall.ResourceVersion, nil
}
