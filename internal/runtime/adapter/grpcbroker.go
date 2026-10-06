// gRPC broker clients: the adapter worker's loopback MCP endpoint reaches
// the Model execution layer and the Memory store through the same fenced
// gateway services every other runtime uses.
package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	ipcv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/ipc/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	kernelmodel "github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GrpcModelBroker implements mcp.ModelBroker against ModelInvocationService.
type GrpcModelBroker struct {
	client modelv1.ModelInvocationServiceClient
}

// NewGrpcModelBroker binds the broker to a gateway connection.
func NewGrpcModelBroker(client modelv1.ModelInvocationServiceClient) *GrpcModelBroker {
	return &GrpcModelBroker{client: client}
}

// InvokeStream streams one invocation, forwarding deltas and mapping the
// terminal chunk onto the kernel output shape. The agent-offered tool
// definitions ride along: the broker resolved the names against the
// capability-filtered registry, and dropping them here would silently strip
// the model's tool surface (a real-model run answered without ever seeing a
// tool — the fake provider could not catch it because it scripts tool calls
// regardless of the offered surface).
func (b *GrpcModelBroker) InvokeStream(ctx context.Context, in model.InvokeInput, onDelta func(string)) (model.InvokeOutput, error) {
	messages := make([]*modelv1.ChatMessage, 0, len(in.Messages))
	for _, message := range in.Messages {
		entry := &modelv1.ChatMessage{Role: message.Role, Content: message.Content, ToolCallId: message.ToolCallID}
		for _, call := range message.ToolCalls {
			entry.ToolCalls = append(entry.ToolCalls, &modelv1.ChatToolCall{
				Id: call.ID, Name: call.Name, ArgumentsJson: call.Arguments,
			})
		}
		messages = append(messages, entry)
	}
	tools := make([]*modelv1.ToolDefinition, 0, len(in.Tools))
	for _, tool := range in.Tools {
		tools = append(tools, &modelv1.ToolDefinition{
			Name: tool.Name, Description: tool.Description, ParametersJson: string(tool.Parameters),
		})
	}
	stream, err := b.client.Invoke(ctx, &modelv1.InvokeRequest{
		Identity: &modelv1.AttemptIdentity{
			TenantId: in.TenantID, AttemptId: in.AttemptID.String(), FencingToken: in.FencingToken,
		},
		TaskId: in.TaskID.String(), RunId: in.RunID.String(), AgentVersionRef: in.AgentVersionRef,
		ModelRef: in.ModelRef, IdempotencyKey: in.IdempotencyKey, Messages: messages,
		Stream: in.Stream, MaxOutputTokens: in.MaxOutputTokens, Temperature: in.Temperature,
		Tools: tools,
	})
	if err != nil {
		return model.InvokeOutput{}, fmt.Errorf("open model invocation: %w", err)
	}
	output := model.InvokeOutput{}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return output, nil
		}
		if err != nil {
			return output, fmt.Errorf("model invocation stream: %w", err)
		}
		switch {
		case chunk.GetDelta() != "":
			if onDelta != nil {
				onDelta(chunk.GetDelta())
			}
		case chunk.GetToolCall() != nil:
			output.ToolCalls = append(output.ToolCalls, provider.ToolCall{
				ID: chunk.GetToolCall().GetId(), Name: chunk.GetToolCall().GetName(),
				Arguments: chunk.GetToolCall().GetArgumentsJson(),
			})
		case chunk.GetFinish() != nil:
			finish := chunk.GetFinish()
			callID, parseErr := uuid.Parse(finish.GetCallId())
			if parseErr != nil {
				return output, fmt.Errorf("finish chunk call id: %w", parseErr)
			}
			cost, parseErr := money.FromUSD(finish.GetCostUsd())
			if parseErr != nil {
				return output, fmt.Errorf("finish chunk cost: %w", parseErr)
			}
			output.Content = finish.GetContent()
			output.Call = store.ModelCall{
				ID: callID, TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
				ModelRef: finish.GetModelRef(), Status: store.ModelCallStatus(finish.GetStatus()),
				InputTokens: finish.GetInputTokens(), OutputTokens: finish.GetOutputTokens(),
				CostMicroUSD: cost, PriceRevision: finish.GetPriceRevision(),
				ProviderRequestID: finish.GetProviderRequestId(), FinishReason: finish.GetFinishReason(),
			}
		case chunk.GetFailure() != nil:
			failure := chunk.GetFailure()
			output.Call.Status = store.ModelCallFailed
			output.Call.FinishReason = failure.GetFinishReason()
			return output, fmt.Errorf("model invocation failed: %s: %s", failure.GetCode(), failure.GetMessage())
		}
	}
}

// GrpcMemoryBroker implements mcp.MemoryBroker against MemoryGatewayService.
type GrpcMemoryBroker struct {
	client gatewayv1.MemoryGatewayServiceClient
}

// NewGrpcMemoryBroker binds the broker to a gateway connection.
func NewGrpcMemoryBroker(client gatewayv1.MemoryGatewayServiceClient) *GrpcMemoryBroker {
	return &GrpcMemoryBroker{client: client}
}

// Put writes one record with the fenced identity of the calling attempt.
func (b *GrpcMemoryBroker) Put(ctx context.Context, identity mcp.AttemptContext, in kernelmodel.PutInput) (store.MemoryRecord, bool, error) {
	put, err := b.client.PutMemory(ctx, &gatewayv1.PutMemoryRequest{
		Identity:        &gatewayv1.AttemptIdentity{TenantId: identity.TenantID, AttemptId: identity.AttemptID.String(), FencingToken: identity.FencingToken},
		AgentVersionRef: identity.AgentVersionRef,
		Namespace:       in.Namespace, Key: in.Key, ContentType: in.ContentType,
		Content: in.Content, Sensitivity: in.Sensitivity,
	})
	if err != nil {
		return store.MemoryRecord{}, false, fmt.Errorf("memory put: %w", err)
	}
	return memoryRecordFromProto(put.GetRecord()), put.GetReplayed(), nil
}

// Search retrieves records with the fenced identity of the calling attempt.
func (b *GrpcMemoryBroker) Search(ctx context.Context, identity mcp.AttemptContext, in kernelmodel.SearchInput) ([]store.MemoryRecord, error) {
	found, err := b.client.SearchMemory(ctx, &gatewayv1.SearchMemoryRequest{
		Identity:        &gatewayv1.AttemptIdentity{TenantId: identity.TenantID, AttemptId: identity.AttemptID.String(), FencingToken: identity.FencingToken},
		AgentVersionRef: identity.AgentVersionRef,
		Namespace:       in.Namespace, Query: in.Query, Sensitivity: in.Sensitivity, Limit: int32(in.Limit),
	})
	if err != nil {
		return nil, fmt.Errorf("memory search: %w", err)
	}
	records := make([]store.MemoryRecord, 0, len(found.GetRecords()))
	for _, record := range found.GetRecords() {
		records = append(records, memoryRecordFromProto(record))
	}
	return records, nil
}

func memoryRecordFromProto(record *gatewayv1.MemoryRecord) store.MemoryRecord {
	converted := store.MemoryRecord{
		Namespace: record.GetNamespace(), Key: record.GetKey(), ContentType: record.GetContentType(),
		Content: record.GetContent(), Sensitivity: record.GetSensitivity(), ResourceVersion: record.GetResourceVersion(),
	}
	if id, err := uuid.Parse(record.GetId()); err == nil {
		converted.ID = id
	}
	if record.GetCreatedAt() != nil {
		converted.CreatedAt = record.GetCreatedAt().AsTime()
	}
	if record.GetUpdatedAt() != nil {
		converted.UpdatedAt = record.GetUpdatedAt().AsTime()
	}
	return converted
}

// GrpcMailboxBroker implements mcp.MailboxBroker against IPCGatewayService.
type GrpcMailboxBroker struct {
	client ipcv1.IPCGatewayServiceClient
}

// NewGrpcMailboxBroker binds the broker to a gateway connection.
func NewGrpcMailboxBroker(client ipcv1.IPCGatewayServiceClient) *GrpcMailboxBroker {
	return &GrpcMailboxBroker{client: client}
}

// SendMessage stores one message in the recipient's mailbox with the fenced
// identity of the calling attempt. The sender's version reference is sent
// alongside the identity because the service cross-checks it against the fenced
// assignment; the recipient is two fields rather than one canonical address
// because the address encoding is not the agent's business.
func (b *GrpcMailboxBroker) SendMessage(ctx context.Context, identity mcp.AttemptContext, in mcp.MailboxSendInput) (mcp.MailboxSendOutcome, error) {
	request := &ipcv1.SendMessageRequest{
		Identity:        ipcAttemptIdentity(identity),
		AgentVersionRef: identity.AgentVersionRef,
		TaskId:          identity.TaskID.String(), RunId: identity.RunID.String(),
		To:   &ipcv1.AgentAddress{AgentVersionRef: in.ToAgentVersionRef, Instance: in.ToRunID},
		Kind: in.Kind, PayloadJson: in.PayloadJSON, IdempotencyKey: in.IdempotencyKey,
		CorrelationId: in.CorrelationID, ReplyToMessageId: in.ReplyToMessageID,
	}
	if !in.Deadline.IsZero() {
		request.Deadline = timestamppb.New(in.Deadline)
	}
	response, err := b.client.SendMessage(ctx, request)
	if err != nil {
		return mcp.MailboxSendOutcome{}, fmt.Errorf("ipc send: %w", err)
	}
	outcome := mcp.MailboxSendOutcome{MessageID: response.GetMessageId(), Replayed: response.GetReplayed()}
	if response.GetAcceptedAt() != nil {
		outcome.AcceptedAt = response.GetAcceptedAt().AsTime()
	}
	return outcome, nil
}

// ReceiveMessages drains the calling run's mailbox. No address is sent: the
// service derives the mailbox from the fenced assignment, so the runtime cannot
// drain a mailbox other than its own.
func (b *GrpcMailboxBroker) ReceiveMessages(ctx context.Context, identity mcp.AttemptContext, in mcp.MailboxReceiveInput) ([]mcp.MailboxMessage, error) {
	response, err := b.client.ReceiveMessages(ctx, &ipcv1.ReceiveMessagesRequest{
		Identity:        ipcAttemptIdentity(identity),
		AgentVersionRef: identity.AgentVersionRef,
		MaxMessages:     in.MaxMessages, WaitMillis: in.WaitMillis,
	})
	if err != nil {
		return nil, fmt.Errorf("ipc receive: %w", err)
	}
	messages := make([]mcp.MailboxMessage, 0, len(response.GetMessages()))
	for _, message := range response.GetMessages() {
		entry := mcp.MailboxMessage{
			MessageID:           message.GetMessageId(),
			FromAgentVersionRef: message.GetFromAgentVersionRef(),
			ToAgentVersionRef:   message.GetTo().GetAgentVersionRef(),
			ToRunID:             message.GetTo().GetInstance(),
			Kind:                message.GetKind(),
			PayloadJSON:         json.RawMessage(message.GetPayloadJson()),
			CorrelationID:       message.GetCorrelationId(),
			ReplyToMessageID:    message.GetReplyToMessageId(),
			SentAt:              message.GetSentAt().AsTime(),
		}
		if message.GetDeadline() != nil {
			entry.Deadline = message.GetDeadline().AsTime()
		}
		if len(message.GetAttachments()) > 0 {
			references := make([]map[string]any, 0, len(message.GetAttachments()))
			for _, attachment := range message.GetAttachments() {
				references = append(references, map[string]any{
					"uri": attachment.GetUri(), "sha256": attachment.GetSha256(),
					"sizeBytes": attachment.GetSizeBytes(), "mediaType": attachment.GetMediaType(),
				})
			}
			encoded, marshalErr := json.Marshal(references)
			if marshalErr != nil {
				return nil, fmt.Errorf("encode ipc attachments: %w", marshalErr)
			}
			entry.AttachmentsJSON = encoded
		}
		messages = append(messages, entry)
	}
	return messages, nil
}

// AcknowledgeMessages receipts a drained batch. The consumer name is left unset
// on purpose: the service derives it from the fenced run, and sending one would
// only be a value the service has to reject.
func (b *GrpcMailboxBroker) AcknowledgeMessages(ctx context.Context, identity mcp.AttemptContext, in mcp.MailboxAckInput) (mcp.MailboxAckOutcome, error) {
	response, err := b.client.AcknowledgeMessage(ctx, &ipcv1.AcknowledgeMessageRequest{
		Identity:        ipcAttemptIdentity(identity),
		AgentVersionRef: identity.AgentVersionRef,
		MessageIds:      in.MessageIDs,
	})
	if err != nil {
		return mcp.MailboxAckOutcome{}, fmt.Errorf("ipc acknowledge: %w", err)
	}
	return mcp.MailboxAckOutcome{
		Acknowledged:        response.GetAcknowledgedMessageIds(),
		AlreadyAcknowledged: response.GetAlreadyAcknowledgedMessageIds(),
	}, nil
}

func ipcAttemptIdentity(identity mcp.AttemptContext) *ipcv1.AttemptIdentity {
	return &ipcv1.AttemptIdentity{
		TenantId: identity.TenantID, AttemptId: identity.AttemptID.String(), FencingToken: identity.FencingToken,
	}
}

// GrpcWorkflowSpawner implements mcp.WorkflowSpawner against the
// orchestrator's WorkflowSpawnService (v1.3).
type GrpcWorkflowSpawner struct {
	client runtimev1.WorkflowSpawnServiceClient
}

// NewGrpcWorkflowSpawner binds the spawner to an orchestrator connection.
func NewGrpcWorkflowSpawner(client runtimev1.WorkflowSpawnServiceClient) *GrpcWorkflowSpawner {
	return &GrpcWorkflowSpawner{client: client}
}

// Spawn forwards one dynamic-step spawn; guard denials arrive as structured
// outcomes, not gRPC errors.
func (s *GrpcWorkflowSpawner) Spawn(ctx context.Context, in mcp.SpawnRequest) (mcp.SpawnOutcome, error) {
	response, err := s.client.SpawnStep(ctx, &runtimev1.SpawnStepRequest{
		Identity: &runtimev1.AttemptIdentity{
			TenantId: in.TenantID, AttemptId: in.AttemptID.String(), FencingToken: in.FencingToken,
		},
		WorkflowId: in.WorkflowID.String(),
		ParentStep: in.ParentStepName, Name: in.Name, Goal: in.Goal,
		AgentVersionRef: in.AgentVersionRef, SpecJson: string(in.Spec),
		MaxAttempts: int32(in.MaxAttempts), IdempotencyKey: in.IdempotencyKey,
		ArgumentsJson: string(in.Arguments),
	})
	if err != nil {
		return mcp.SpawnOutcome{}, fmt.Errorf("spawn step: %w", err)
	}
	return mcp.SpawnOutcome{
		Code: response.GetOutcome(), Message: response.GetMessage(),
		StepName: response.GetStepName(), SpawnDepth: int(response.GetSpawnDepth()),
	}, nil
}
