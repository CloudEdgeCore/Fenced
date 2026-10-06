package reference

import (
	"context"
	"encoding/json"
	"fmt"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/google/uuid"
)

// GrpcToolInvoker forwards MCP tool calls over the fenced Tool Gateway gRPC
// boundary, preserving the attempt identity (tenant, attempt, fencing token)
// so the gateway's decision chain is exercised exactly like scripted calls.
type GrpcToolInvoker struct {
	client gatewayv1.ToolGatewayServiceClient
}

func NewGrpcToolInvoker(client gatewayv1.ToolGatewayServiceClient) *GrpcToolInvoker {
	return &GrpcToolInvoker{client: client}
}

var _ mcp.ToolInvoker = (*GrpcToolInvoker)(nil)

func (g *GrpcToolInvoker) ListTools(ctx context.Context, tenantID string) ([]store.ToolDescriptor, error) {
	return g.ListToolsForAgent(ctx, tenantID, "")
}

func (g *GrpcToolInvoker) ListToolsForAgent(ctx context.Context, tenantID, agentVersionRef string) ([]store.ToolDescriptor, error) {
	response, err := g.client.ListTools(ctx, &gatewayv1.ListToolsRequest{
		TenantId: tenantID, AgentVersionRef: agentVersionRef,
	})
	if err != nil {
		return nil, fmt.Errorf("list tools over Runtime Protocol: %w", err)
	}
	descriptors := make([]store.ToolDescriptor, 0, len(response.GetTools()))
	for _, proto := range response.GetTools() {
		descriptors = append(descriptors, store.ToolDescriptor{
			Name: proto.GetName(), Version: proto.GetVersion(),
			SideEffectRisk: store.ToolRisk(proto.GetSideEffectRisk()),
			Actions:        proto.GetActions(), ResourcePatterns: proto.GetResourcePatterns(),
			ParamsSchema: json.RawMessage(proto.GetParamsSchemaJson()),
		})
	}
	return descriptors, nil
}

func (g *GrpcToolInvoker) InvokeTool(ctx context.Context, in tool.InvokeInput) (tool.InvokeResult, error) {
	request := &gatewayv1.InvokeToolRequest{
		Identity: &gatewayv1.AttemptIdentity{
			TenantId: in.TenantID, AttemptId: in.AttemptID.String(), FencingToken: in.FencingToken,
		},
		TaskId: in.TaskID.String(), RunId: in.RunID.String(),
		AgentVersionRef: in.AgentVersionRef, ToolName: in.ToolName, ToolVersion: in.ToolVersion,
		SecretRef: in.SecretRef,
		Action:    in.Action, Resource: in.Resource, ArgsJson: in.Args, IdempotencyKey: in.IdempotencyKey,
	}
	if in.ApprovalID != nil {
		request.ApprovalId = in.ApprovalID.String()
	}
	response, err := g.client.InvokeTool(ctx, request)
	if err != nil {
		return tool.InvokeResult{}, err
	}
	result := tool.InvokeResult{
		Outcome: tool.InvokeOutcome(response.GetOutcome()),
		Result:  response.GetResultJson(), DenyReasons: response.GetDenyReasons(),
		PolicyRevision: response.GetPolicyRevision(), ReceiptOperation: response.GetReceiptOperation(),
	}
	if callID := response.GetToolCallId(); callID != "" {
		if parsed, parseErr := uuid.Parse(callID); parseErr == nil {
			result.ToolCall.ID = parsed
		}
	}
	if approvalID := response.GetApprovalId(); approvalID != "" {
		if parsed, parseErr := uuid.Parse(approvalID); parseErr == nil {
			result.ApprovalID = &parsed
		}
	}
	return result, nil
}
