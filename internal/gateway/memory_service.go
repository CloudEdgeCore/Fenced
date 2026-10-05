package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxMemoryProvenanceBytes = 64 << 10

// MemoryInvoker is the canonical memory decision chain exposed to Agents.
type MemoryInvoker interface {
	Put(context.Context, memory.PutInput) (store.MemoryRecord, bool, error)
	Search(context.Context, memory.SearchInput) ([]store.MemoryRecord, error)
}

// RuntimeFence resolves an Attempt only when its fencing token is current.
type RuntimeFence interface {
	GetRuntimeAssignment(context.Context, string, uuid.UUID, int64) (store.RuntimeAssignment, error)
}

// MemoryService is a fenced, capability-aware Agent memory boundary. The
// existing Control API remains an operator surface; Agent runtimes use this
// service so they cannot choose a tenant, version, or namespace grant.
type MemoryService struct {
	gatewayv1.UnimplementedMemoryGatewayServiceServer
	memories      MemoryInvoker
	fences        RuntimeFence
	allowedTenant string
	capabilities  *capability.Authorizer
}

func NewMemoryService(memories MemoryInvoker, fences RuntimeFence, allowedTenant string, capabilities *capability.Authorizer) *MemoryService {
	return &MemoryService{memories: memories, fences: fences, allowedTenant: allowedTenant, capabilities: capabilities}
}

func (s *MemoryService) SearchMemory(ctx context.Context, request *gatewayv1.SearchMemoryRequest) (*gatewayv1.SearchMemoryResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	_, err := s.authorize(ctx, request.GetIdentity(), request.GetAgentVersionRef(), request.GetNamespace(), "read")
	if err != nil {
		return nil, err
	}
	input := memory.SearchInput{
		TenantID: request.GetIdentity().GetTenantId(), Namespace: request.GetNamespace(), Query: request.GetQuery(),
		Sensitivity: request.GetSensitivity(), Limit: int(request.GetLimit()),
	}
	if input.Limit == 0 {
		input.Limit = 20
	}
	if err := (store.SearchMemoryInput{
		TenantID: input.TenantID, Query: input.Query, Namespace: input.Namespace,
		Sensitivity: input.Sensitivity, Limit: input.Limit,
	}).Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	records, err := s.memories.Search(ctx, input)
	if err != nil {
		return nil, memoryRPCError(err)
	}
	response := &gatewayv1.SearchMemoryResponse{Records: make([]*gatewayv1.MemoryRecord, 0, len(records))}
	for _, record := range records {
		response.Records = append(response.Records, memoryRecord(record))
	}
	return response, nil
}

func (s *MemoryService) PutMemory(ctx context.Context, request *gatewayv1.PutMemoryRequest) (*gatewayv1.PutMemoryResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	assignment, err := s.authorize(ctx, request.GetIdentity(), request.GetAgentVersionRef(), request.GetNamespace(), "write")
	if err != nil {
		return nil, err
	}
	if len(request.GetProvenanceJson()) > maxMemoryProvenanceBytes {
		return nil, status.Error(codes.InvalidArgument, "memory provenance exceeds 64 KiB")
	}
	var provenance map[string]any
	if len(request.GetProvenanceJson()) > 0 {
		if err := json.Unmarshal(request.GetProvenanceJson(), &provenance); err != nil || provenance == nil {
			return nil, status.Error(codes.InvalidArgument, "memory provenance must be a JSON object")
		}
	}
	if err := (store.PutMemoryInput{
		TenantID: request.GetIdentity().GetTenantId(), Namespace: request.GetNamespace(), Key: request.GetKey(),
		ContentType: request.GetContentType(), Content: request.GetContent(),
		Embedding: make([]float32, store.MemoryEmbeddingDimension), EmbeddingProvider: "gateway-validation",
		Sensitivity: request.GetSensitivity(), Provenance: request.GetProvenanceJson(),
	}).Validate(); err != nil {
		if errors.Is(err, store.ErrMemoryTooLarge) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	record, replayed, err := s.memories.Put(ctx, memory.PutInput{
		TenantID: request.GetIdentity().GetTenantId(), Namespace: request.GetNamespace(), Key: request.GetKey(),
		ContentType: request.GetContentType(), Content: request.GetContent(), Sensitivity: request.GetSensitivity(),
		SourceTaskID: &assignment.Task.ID, SourceRunID: &assignment.Run.ID, SourceAttemptID: &assignment.Attempt.ID,
		Provenance: provenance,
	})
	if err != nil {
		return nil, memoryRPCError(err)
	}
	return &gatewayv1.PutMemoryResponse{Record: memoryRecord(record), Replayed: replayed}, nil
}

func (s *MemoryService) authorize(ctx context.Context, identity *gatewayv1.AttemptIdentity, versionRef, namespace, operation string) (store.RuntimeAssignment, error) {
	var zero store.RuntimeAssignment
	if identity == nil || identity.GetFencingToken() <= 0 {
		return zero, status.Error(codes.InvalidArgument, "attempt identity and positive fencing token are required")
	}
	if err := authorizeTenant(ctx, s.allowedTenant, identity.GetTenantId()); err != nil {
		return zero, err
	}
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(versionRef) == "" {
		return zero, status.Error(codes.InvalidArgument, "namespace and agent version reference are required")
	}
	if s.fences == nil || s.memories == nil || s.capabilities == nil {
		return zero, status.Error(codes.PermissionDenied, "memory gateway enforcement is not configured")
	}
	attemptID, err := uuid.Parse(identity.GetAttemptId())
	if err != nil {
		return zero, status.Error(codes.InvalidArgument, "attempt ID must be a UUID")
	}
	assignment, err := s.fences.GetRuntimeAssignment(ctx, identity.GetTenantId(), attemptID, identity.GetFencingToken())
	if err != nil {
		return zero, memoryRPCError(err)
	}
	if assignment.Task.AgentVersionRef != versionRef {
		return zero, status.Error(codes.PermissionDenied, "agent version does not match the fenced Attempt")
	}
	candidates := []string{namespace, namespace + ":" + operation}
	if err := s.capabilities.Authorize(ctx, identity.GetTenantId(), versionRef, capability.Memory, candidates...); err != nil {
		return zero, status.Error(codes.PermissionDenied, err.Error())
	}
	return assignment, nil
}

func memoryRecord(record store.MemoryRecord) *gatewayv1.MemoryRecord {
	return &gatewayv1.MemoryRecord{
		Id: record.ID.String(), Namespace: record.Namespace, Key: record.Key, ContentType: record.ContentType,
		Content: record.Content, Sensitivity: record.Sensitivity, ResourceVersion: record.ResourceVersion,
		CreatedAt: timestamppb.New(record.CreatedAt), UpdatedAt: timestamppb.New(record.UpdatedAt),
	}
}

func memoryRPCError(err error) error {
	switch {
	case errors.Is(err, store.ErrFenced):
		return status.Error(codes.PermissionDenied, "attempt identity is stale")
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrMemoryNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrMemoryTooLarge):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, store.ErrMemorySearchRequired):
		return status.Error(codes.InvalidArgument, err.Error())
	case store.IsRetryableTransaction(err):
		return status.Error(codes.Unavailable, "transient transaction conflict; retry")
	default:
		return status.Error(codes.Internal, "memory gateway operation failed")
	}
}
