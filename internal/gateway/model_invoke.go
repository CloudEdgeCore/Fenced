package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/redact"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ModelRunner executes real model invocations behind the fenced decision
// chain. The model.Invoker satisfies it.
type ModelRunner interface {
	InvokeStream(context.Context, model.InvokeInput, func(string)) (model.InvokeOutput, error)
}

// ModelInvocationService is the additive v1 execution surface of the Model
// Gateway: it runs policy, budget and the call ledger around a real provider
// invocation and streams the completion. Credentials never cross this
// boundary in either direction.
type ModelInvocationService struct {
	modelv1.UnimplementedModelInvocationServiceServer
	runner        ModelRunner
	allowedTenant string
	capabilities  *capability.Authorizer
}

// NewModelInvocationService builds the execution surface. allowedTenant "*"
// disables the static tenant gate (production relies on SPIFFE mTLS).
func NewModelInvocationService(runner ModelRunner, allowedTenant string, capabilities ...*capability.Authorizer) *ModelInvocationService {
	service := &ModelInvocationService{runner: runner, allowedTenant: allowedTenant}
	if len(capabilities) > 0 {
		service.capabilities = capabilities[0]
	}
	return service
}

// Invoke executes one model invocation. Content deltas stream as chunks; the
// terminal chunk is either a finish (ledger metadata and the assembled
// content) or a structured provider failure. Governance failures (identity,
// capability, policy, budget) are gRPC status errors — no stream is opened.
func (s *ModelInvocationService) Invoke(request *modelv1.InvokeRequest, stream modelv1.ModelInvocationService_InvokeServer) error {
	if request == nil || request.GetIdentity() == nil || request.GetIdentity().GetFencingToken() <= 0 {
		return status.Error(codes.InvalidArgument, "attempt identity and positive fencing token are required")
	}
	if err := authorizeTenant(stream.Context(), s.allowedTenant, request.GetIdentity().GetTenantId()); err != nil {
		return err
	}
	if s.capabilities != nil {
		if err := s.capabilities.Authorize(stream.Context(), request.GetIdentity().GetTenantId(), request.GetAgentVersionRef(),
			capability.Model, request.GetModelRef()); err != nil {
			return status.Error(codes.PermissionDenied, err.Error())
		}
	}
	taskID, err := parseUUID(request.GetTaskId(), "task ID")
	if err != nil {
		return err
	}
	runID, err := parseUUID(request.GetRunId(), "run ID")
	if err != nil {
		return err
	}
	attemptID, err := parseUUID(request.GetIdentity().GetAttemptId(), "attempt ID")
	if err != nil {
		return err
	}
	if len(request.GetMessages()) == 0 {
		return status.Error(codes.InvalidArgument, "at least one message is required")
	}
	tools, err := protoTools(request.GetTools())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	// Defense in depth (P0-01): the broker resolves tool names against the
	// AgentVersion's capability grants, but the gateway re-authorizes every
	// tool a caller attaches so a compromised runtime worker cannot widen
	// the tool surface at the execution boundary.
	if s.capabilities != nil {
		for _, tool := range tools {
			if err := s.capabilities.Authorize(stream.Context(), request.GetIdentity().GetTenantId(), request.GetAgentVersionRef(),
				capability.Tool, tool.Name); err != nil {
				return status.Error(codes.PermissionDenied, err.Error())
			}
		}
	}
	input := model.InvokeInput{
		TenantID: request.GetIdentity().GetTenantId(), TaskID: taskID, RunID: runID, AttemptID: attemptID,
		FencingToken: request.GetIdentity().GetFencingToken(), AgentVersionRef: request.GetAgentVersionRef(),
		ModelRef: request.GetModelRef(), IdempotencyKey: request.GetIdempotencyKey(),
		Messages: protoMessages(request.GetMessages()), Stream: request.GetStream(),
		MaxOutputTokens: request.GetMaxOutputTokens(), Tools: tools,
	}
	if request.Temperature != nil {
		temperature := request.GetTemperature()
		input.Temperature = &temperature
	}

	output, invokeErr := s.runner.InvokeStream(stream.Context(), input, func(delta string) {
		if delta != "" {
			_ = stream.Send(&modelv1.InvokeResponse{Delta: delta})
		}
	})
	if invokeErr != nil {
		// A terminal ledger row may still exist (budget stop, provider
		// failure): report it as a structured failure chunk so callers can
		// distinguish execution outcomes from transport errors.
		if output.Call.ID != uuid.Nil && output.Call.Status != "" {
			_ = stream.Send(&modelv1.InvokeResponse{Failure: &modelv1.InvokeFailure{
				Code: invokeCode(invokeErr), Message: sanitizeInvokeError(invokeErr), FinishReason: output.Call.FinishReason,
			}})
			return nil
		}
		return invokeStatus(invokeErr)
	}
	for _, call := range output.ToolCalls {
		if err := stream.Send(&modelv1.InvokeResponse{ToolCall: &modelv1.ChatToolCall{
			Id: call.ID, Name: call.Name, ArgumentsJson: call.Arguments,
		}}); err != nil {
			return err
		}
	}
	return stream.Send(&modelv1.InvokeResponse{Finish: &modelv1.InvokeFinish{
		CallId: output.Call.ID.String(), ModelRef: output.Call.ModelRef, Status: string(output.Call.Status),
		InputTokens: output.Call.InputTokens, OutputTokens: output.Call.OutputTokens, CostUsd: output.Call.CostMicroUSD.USD(),
		PriceRevision: output.Call.PriceRevision, FinishReason: output.Call.FinishReason,
		ProviderRequestId: output.Call.ProviderRequestID, Content: output.Content,
	}})
}

// protoTools converts the wire tool definitions into provider definitions.
// The broker already generated these schemas from the capability-filtered
// registry, so this boundary only re-validates structure defensively: every
// tool needs a name and, when present, a syntactically valid JSON Schema
// document. The name is re-authorized against the AgentVersion grant by the
// caller; this function does not widen the surface.
func protoTools(tools []*modelv1.ToolDefinition) ([]provider.ToolDefinition, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	converted := make([]provider.ToolDefinition, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		name := tool.GetName()
		if name == "" {
			return nil, errors.New("tool definition requires a name")
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, errors.New("tool " + name + " is declared more than once")
		}
		seen[name] = struct{}{}
		var parameters json.RawMessage
		if schema := tool.GetParametersJson(); schema != "" {
			if !json.Valid([]byte(schema)) {
				return nil, errors.New("tool " + name + " parameters are not valid JSON")
			}
			parameters = json.RawMessage(schema)
		}
		converted = append(converted, provider.ToolDefinition{
			Name: name, Description: tool.GetDescription(), Parameters: parameters,
		})
	}
	return converted, nil
}

func protoMessages(messages []*modelv1.ChatMessage) []provider.Message {
	converted := make([]provider.Message, 0, len(messages))
	for _, message := range messages {
		entry := provider.Message{Role: message.GetRole(), Content: message.GetContent(), ToolCallID: message.GetToolCallId()}
		for _, call := range message.GetToolCalls() {
			entry.ToolCalls = append(entry.ToolCalls, provider.ToolCall{
				ID: call.GetId(), Name: call.GetName(), Arguments: call.GetArgumentsJson(),
			})
		}
		converted = append(converted, entry)
	}
	return converted
}

// invokeCode maps kernel/provider errors onto stable client-facing codes.
func invokeCode(err error) string {
	switch {
	case errors.Is(err, model.ErrBudgetExhausted):
		return "BUDGET_EXHAUSTED"
	case errors.Is(err, model.ErrModelDenied):
		return "MODEL_DENIED"
	case errors.Is(err, model.ErrNoProviderExecution):
		return "NO_PROVIDER_EXECUTION"
	case errors.Is(err, provider.ErrProviderUnavailable):
		return "PROVIDER_UNAVAILABLE"
	case errors.Is(err, provider.ErrProviderRejected):
		return "PROVIDER_REJECTED"
	case errors.Is(err, provider.ErrStreamAborted):
		return "PROVIDER_STREAM_ABORTED"
	case errors.Is(err, store.ErrBudgetExceeded):
		return "BUDGET_EXHAUSTED"
	default:
		return "INVOCATION_FAILED"
	}
}

// sanitizeInvokeError keeps provider detail but never retries, URLs or keys —
// the executor already sanitized; this bounds the length defensively.
func sanitizeInvokeError(err error) string {
	message := redact.RedactText(err.Error())
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

// invokeStatus maps governance failures onto gRPC status codes.
func invokeStatus(err error) error {
	if store.IsRetryableTransaction(err) {
		return status.Error(codes.Unavailable, "transient transaction conflict; retry")
	}
	switch {
	case errors.Is(err, model.ErrModelDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, model.ErrBudgetExhausted), errors.Is(err, store.ErrBudgetExceeded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, model.ErrNoProviderExecution):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrModelNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrIdempotencyConflict),
		errors.Is(err, store.ErrInvalidTransition):
		return status.Error(codes.Aborted, err.Error())
	default:
		slog.Warn("model invocation unmapped failure", "error", err.Error(), "cause", fmt.Sprintf("%+v", errors.Unwrap(err)))
		return status.Error(codes.Internal, "model invocation failed")
	}
}
