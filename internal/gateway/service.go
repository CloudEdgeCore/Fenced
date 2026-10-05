// Package gateway implements the fenced Tool Gateway boundary that runtime
// workers call on behalf of an Attempt. The gateway enforces policy, approval
// binding, budget hard-stop and durable side-effect receipts outside the LLM;
// it is a separate process in production (tech baseline §5) because it sits on
// the credential and external-side-effect security boundary.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxArgsBytes = 64 << 10

// ToolInvoker is the decision chain the service exposes. The tool.Gateway
// satisfies it; tests substitute fakes.
type ToolInvoker interface {
	InvokeTool(context.Context, tool.InvokeInput) (tool.InvokeResult, error)
	ListTools(context.Context, string) ([]store.ToolDescriptor, error)
	GetToolDescriptor(ctx context.Context, tenantID, name, version string) (store.ToolDescriptor, error)
}

// Service is the fenced gRPC surface of the Tool Gateway. Development mode
// accepts a fixed tenant on a loopback-only listener; production exposure
// requires SPIFFE mTLS (ADR-006).
type Service struct {
	gatewayv1.UnimplementedToolGatewayServiceServer
	invoker       ToolInvoker
	fences        RuntimeFence
	allowedTenant string
	capabilities  *capability.Authorizer
}

func NewService(invoker ToolInvoker, allowedTenant string, capabilities ...*capability.Authorizer) *Service {
	service := &Service{invoker: invoker, allowedTenant: allowedTenant}
	if len(capabilities) > 0 {
		service.capabilities = capabilities[0]
	}
	return service
}

// WithRuntimeFence attaches a fence validator to the tool gateway service.
func (s *Service) WithRuntimeFence(fences RuntimeFence) *Service {
	s.fences = fences
	return s
}

func (s *Service) ListTools(ctx context.Context, request *gatewayv1.ListToolsRequest) (*gatewayv1.ListToolsResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := authorizeTenant(ctx, s.allowedTenant, request.GetTenantId()); err != nil {
		return nil, err
	}
	descriptors, err := s.invoker.ListTools(ctx, request.GetTenantId())
	if err != nil {
		return nil, rpcError(err)
	}
	var freeze capability.ToolFreeze
	if s.capabilities != nil {
		freeze, err = s.capabilities.ToolFreeze(ctx, request.GetTenantId(), request.GetAgentVersionRef())
		if err != nil {
			if errors.Is(err, capability.ErrDenied) {
				// A portable AgentVersion that fails to resolve or declares no
				// capabilities sees an empty registry, never the full one.
				return &gatewayv1.ListToolsResponse{}, nil
			}
			return nil, status.Error(codes.Internal, "agent capability lookup failed")
		}
	}
	response := &gatewayv1.ListToolsResponse{Tools: make([]*gatewayv1.ToolDescriptor, 0, len(descriptors))}
	visible := make([]store.ToolDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if s.capabilities != nil && !freeze.Allow(descriptor.Name, descriptor.Version, descriptor.CreatedAt) {
			continue
		}
		visible = append(visible, descriptor)
	}
	// P1-02: one entry per tool name — the latest granted version — so the
	// model never sees two same-named tools and a bare-name invocation
	// resolves to exactly what the listing advertised.
	for _, descriptor := range tool.LatestVersionPerName(visible) {
		response.Tools = append(response.Tools, &gatewayv1.ToolDescriptor{
			Name: descriptor.Name, Version: descriptor.Version,
			SideEffectRisk: string(descriptor.SideEffectRisk),
			Actions:        descriptor.Actions, ResourcePatterns: descriptor.ResourcePatterns,
			ParamsSchemaJson: descriptor.ParamsSchema, SpecDigest: fmt.Sprintf("%x", descriptor.SpecHash),
		})
	}
	return response, nil
}

func (s *Service) InvokeTool(ctx context.Context, request *gatewayv1.InvokeToolRequest) (*gatewayv1.InvokeToolResponse, error) {
	if request == nil || request.GetIdentity() == nil || request.GetIdentity().GetFencingToken() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "attempt identity and positive fencing token are required")
	}
	if err := authorizeTenant(ctx, s.allowedTenant, request.GetIdentity().GetTenantId()); err != nil {
		return nil, err
	}
	if len(request.GetArgsJson()) > maxArgsBytes {
		return nil, status.Errorf(codes.InvalidArgument, "tool arguments exceed %d bytes", maxArgsBytes)
	}
	if s.capabilities != nil {
		freeze, err := s.capabilities.ToolFreeze(ctx, request.GetIdentity().GetTenantId(), request.GetAgentVersionRef())
		if err != nil {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if freeze.Enforced() {
			// The platform pins tool versions for portable publications, so an
			// authoritative invocation must name the exact version the frozen
			// registry advertised. An empty version would let the store resolve
			// its own latest and defeat the freeze; a compromised adapter that
			// forges a newer version is caught by the freeze check below.
			if request.GetToolVersion() == "" {
				return nil, status.Error(codes.PermissionDenied,
					"the platform freezes tool versions; invoke a specific name@version")
			}
			descriptor, err := s.invoker.GetToolDescriptor(ctx, request.GetIdentity().GetTenantId(),
				request.GetToolName(), request.GetToolVersion())
			if err != nil {
				return nil, rpcError(err)
			}
			if !freeze.Allow(descriptor.Name, descriptor.Version, descriptor.CreatedAt) {
				return nil, status.Error(codes.PermissionDenied,
					"tool version is frozen out for this AgentVersion; pin it at publish or grant name@* to float")
			}
		}
		if request.GetSecretRef() != "" {
			if err := s.capabilities.Authorize(ctx, request.GetIdentity().GetTenantId(), request.GetAgentVersionRef(),
				capability.Secret, request.GetSecretRef()); err != nil {
				return nil, status.Error(codes.PermissionDenied, err.Error())
			}
		}
	}
	taskID, err := parseUUID(request.GetTaskId(), "task ID")
	if err != nil {
		return nil, err
	}
	runID, err := parseUUID(request.GetRunId(), "run ID")
	if err != nil {
		return nil, err
	}
	attemptID, err := parseUUID(request.GetIdentity().GetAttemptId(), "attempt ID")
	if err != nil {
		return nil, err
	}
	if s.fences != nil {
		assignment, err := s.fences.GetRuntimeAssignment(ctx, request.GetIdentity().GetTenantId(), attemptID, request.GetIdentity().GetFencingToken())
		if err != nil {
			if errors.Is(err, store.ErrFenced) || errors.Is(err, store.ErrNotFound) {
				return nil, status.Error(codes.PermissionDenied, "attempt identity is stale or lease expired")
			}
			return nil, rpcError(err)
		}
		if assignment.Task.AgentVersionRef != request.GetAgentVersionRef() {
			return nil, status.Error(codes.PermissionDenied, "agent version does not match the fenced Attempt")
		}
	}
	input := tool.InvokeInput{
		TenantID: request.GetIdentity().GetTenantId(),
		TaskID:   taskID, RunID: runID, AttemptID: attemptID,
		FencingToken:    request.GetIdentity().GetFencingToken(),
		AgentVersionRef: request.GetAgentVersionRef(),
		SecretRef:       request.GetSecretRef(),
		ToolName:        request.GetToolName(), ToolVersion: request.GetToolVersion(),
		Action: request.GetAction(), Resource: request.GetResource(),
		Args: json.RawMessage(request.GetArgsJson()), IdempotencyKey: request.GetIdempotencyKey(),
	}
	if approvalID := request.GetApprovalId(); approvalID != "" {
		parsed, err := uuid.Parse(approvalID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "approval ID must be a UUID")
		}
		input.ApprovalID = &parsed
	}
	result, err := s.invoker.InvokeTool(ctx, input)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &gatewayv1.InvokeToolResponse{
		Outcome: string(result.Outcome), ResultJson: result.Result,
		DenyReasons: result.DenyReasons, PolicyRevision: result.PolicyRevision,
		ReceiptOperation: result.ReceiptOperation,
	}
	if result.ToolCall.ID != uuid.Nil {
		response.ToolCallId = result.ToolCall.ID.String()
	}
	if result.ApprovalID != nil {
		response.ApprovalId = result.ApprovalID.String()
	}
	return response, nil
}

func parseUUID(value, field string) (uuid.UUID, error) {
	if strings.TrimSpace(value) == "" {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s is required", field)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s must be a UUID", field)
	}
	return parsed, nil
}

func rpcError(err error) error {
	if store.IsRetryableTransaction(err) {
		// Transient serialization failure: the caller must retry with
		// bounded backoff rather than treating the operation as failed.
		return status.Error(codes.Unavailable, "transient transaction conflict; retry")
	}
	var approvalErr *tool.ApprovalNotUsableError
	if errors.As(err, &approvalErr) {
		// Pending parks the attempt; rejected, expired and mismatched
		// approvals can never authorize and must fail it.
		if approvalErr.Reason == tool.ApprovalPending {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		return status.Error(codes.PermissionDenied, err.Error())
	}
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrToolNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrIdempotencyConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, store.ErrApprovalNotUsable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, tool.ErrBudgetExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, tool.ErrToolExecutionFailed):
		var executionErr *tool.ToolExecutionError
		if errors.As(err, &executionErr) {
			return status.Error(codes.Aborted, executionErr.Code)
		}
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, tool.ErrToolArgsInvalid), errors.Is(err, store.ErrInvalidTransition):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "tool gateway operation failed")
	}
}
