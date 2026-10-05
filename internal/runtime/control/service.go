// Package control implements the fenced Kernel-to-Runtime gRPC boundary.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	runtimev1.UnimplementedRuntimeControlServiceServer
	store             store.RuntimeStore
	allowedTenant     string
	maxLeaseTTL       time.Duration
	spiffeTrustDomain string // empty = claim binding disabled (dev plaintext transport)
}

// Option configures the runtime control service.
type Option func(*Service)

// WithSpiffeClaimBinding binds every request's tenant (and, where present,
// runtime instance) claims to the peer's X.509-SVID SPIFFE ID (ADR-011). The
// transport must be mutual TLS; requests without a verified SVID are
// rejected.
func WithSpiffeClaimBinding(trustDomain string) Option {
	return func(s *Service) { s.spiffeTrustDomain = trustDomain }
}

func NewService(repository store.RuntimeStore, allowedTenant string, maxLeaseTTL time.Duration, options ...Option) *Service {
	service := &Service{store: repository, allowedTenant: allowedTenant, maxLeaseTTL: maxLeaseTTL}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) PollAssignment(ctx context.Context, request *runtimev1.PollAssignmentRequest) (*runtimev1.PollAssignmentResponse, error) {
	if request == nil || strings.TrimSpace(request.GetRuntimeInstanceId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime instance ID is required")
	}
	if err := s.authorizeTenant(request.GetTenantId()); err != nil {
		return nil, err
	}
	if err := s.authorizePeer(ctx, request.GetTenantId(), request.GetRuntimeInstanceId()); err != nil {
		return nil, err
	}
	assignment, err := s.store.PollRuntimeAssignment(ctx, request.GetTenantId(), request.GetRuntimeInstanceId())
	if err != nil {
		return nil, rpcError(err)
	}
	proto := assignmentProto(assignment)
	proto.WorkflowLineage = s.workflowLineage(ctx, assignment)
	return &runtimev1.PollAssignmentResponse{Assignment: proto}, nil
}

func (s *Service) GetAssignment(ctx context.Context, request *runtimev1.GetAssignmentRequest) (*runtimev1.GetAssignmentResponse, error) {
	identity, attemptID, err := s.parseIdentity(ctx, request.GetIdentity())
	if err != nil {
		return nil, err
	}
	assignment, err := s.store.GetRuntimeAssignment(ctx, identity.GetTenantId(), attemptID, identity.GetFencingToken())
	if err != nil {
		return nil, rpcError(err)
	}
	proto := assignmentProto(assignment)
	proto.WorkflowLineage = s.workflowLineage(ctx, assignment)
	return &runtimev1.GetAssignmentResponse{Assignment: proto}, nil
}

func (s *Service) TransitionAttempt(ctx context.Context, request *runtimev1.TransitionAttemptRequest) (*runtimev1.TransitionAttemptResponse, error) {
	identity, attemptID, err := s.parseIdentity(ctx, request.GetIdentity())
	if err != nil {
		return nil, err
	}
	if request.GetExpectedAttemptVersion() <= 0 || strings.TrimSpace(request.GetIdempotencyKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "expected attempt version and idempotency key are required")
	}
	target, err := domainPhase(request.GetTargetPhase())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	updated, err := s.store.TransitionAttempt(ctx, store.TransitionAttemptInput{
		TenantID: identity.GetTenantId(), AttemptID: attemptID, FencingToken: identity.GetFencingToken(),
		ExpectedAttemptVersion: request.GetExpectedAttemptVersion(), To: target,
		FailureCode: request.GetFailureCode(), FailureMessage: request.GetFailureMessage(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &runtimev1.TransitionAttemptResponse{
		Phase: protoPhase(updated.Phase), AttemptVersion: updated.ResourceVersion,
	}, nil
}

func (s *Service) Heartbeat(ctx context.Context, request *runtimev1.HeartbeatRequest) (*runtimev1.HeartbeatResponse, error) {
	identity, attemptID, err := s.parseIdentity(ctx, request.GetIdentity())
	if err != nil {
		return nil, err
	}
	ttl := time.Duration(request.GetRequestedTtlSeconds()) * time.Second
	if request.GetExpectedLeaseVersion() <= 0 || strings.TrimSpace(request.GetIdempotencyKey()) == "" || ttl <= 0 || ttl > s.maxLeaseTTL {
		return nil, status.Error(codes.InvalidArgument, "valid lease version, idempotency key, and bounded TTL are required")
	}
	lease, err := s.store.HeartbeatLease(ctx, store.HeartbeatLeaseInput{
		TenantID: identity.GetTenantId(), AttemptID: attemptID, FencingToken: identity.GetFencingToken(),
		ExpectedLeaseVersion: request.GetExpectedLeaseVersion(), TTL: ttl,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	// The narrow renewal read answers the cancel check without
	// re-materializing the full assignment on every heartbeat.
	status, err := s.store.GetHeartbeatStatus(ctx, identity.GetTenantId(), attemptID, identity.GetFencingToken())
	if err != nil {
		return nil, rpcError(err)
	}
	return &runtimev1.HeartbeatResponse{
		LeaseVersion: lease.ResourceVersion, ExpiresAt: timestamppb.New(lease.ExpiresAt),
		CancelRequested: status.CancelRequested, AttemptVersion: status.AttemptVersion,
	}, nil
}

func (s *Service) CommitCheckpoint(ctx context.Context, request *runtimev1.CommitCheckpointRequest) (*runtimev1.CommitCheckpointResponse, error) {
	identity, attemptID, err := s.parseIdentity(ctx, request.GetIdentity())
	if err != nil {
		return nil, err
	}
	checkpointID, err := uuid.Parse(request.GetCheckpointId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "checkpoint ID must be a UUID")
	}
	artifact, err := artifactFromProto(request.GetState())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	checkpoint, attempt, err := s.store.CommitCheckpoint(ctx, store.CommitCheckpointInput{
		TenantID: identity.GetTenantId(), AttemptID: attemptID, FencingToken: identity.GetFencingToken(),
		ExpectedAttemptVersion: request.GetExpectedAttemptVersion(), IdempotencyKey: request.GetIdempotencyKey(),
		CheckpointID: checkpointID, AgentVersionRef: request.GetAgentVersionRef(), Provider: request.GetProvider(),
		RuntimeABI: request.GetRuntimeAbi(), SchemaVersion: request.GetSchemaVersion(), State: artifact,
		ConfirmedReceiptIDs: request.GetConfirmedReceiptIds(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &runtimev1.CommitCheckpointResponse{
		Checkpoint: checkpointProto(checkpoint), AttemptVersion: attempt.ResourceVersion,
	}, nil
}

func (s *Service) CompleteAttempt(ctx context.Context, request *runtimev1.CompleteAttemptRequest) (*runtimev1.CompleteAttemptResponse, error) {
	identity, attemptID, err := s.parseIdentity(ctx, request.GetIdentity())
	if err != nil {
		return nil, err
	}
	artifact, err := artifactFromProto(request.GetResult())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.store.CompleteAttempt(ctx, store.CompleteAttemptInput{
		TenantID: identity.GetTenantId(), AttemptID: attemptID, FencingToken: identity.GetFencingToken(),
		ExpectedAttemptVersion: request.GetExpectedAttemptVersion(), IdempotencyKey: request.GetIdempotencyKey(), Result: artifact,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &runtimev1.CompleteAttemptResponse{
		AttemptVersion: result.Attempt.ResourceVersion, RunVersion: result.Run.ResourceVersion,
		TaskVersion: result.Task.ResourceVersion, ResultRef: result.Task.ResultRef,
	}, nil
}

func (s *Service) AcknowledgeCancellation(ctx context.Context, request *runtimev1.AcknowledgeCancellationRequest) (*runtimev1.AcknowledgeCancellationResponse, error) {
	identity, attemptID, err := s.parseIdentity(ctx, request.GetIdentity())
	if err != nil {
		return nil, err
	}
	result, err := s.store.AcknowledgeCancellation(ctx, store.CancelAttemptInput{
		TenantID: identity.GetTenantId(), AttemptID: attemptID, FencingToken: identity.GetFencingToken(),
		ExpectedAttemptVersion: request.GetExpectedAttemptVersion(), IdempotencyKey: request.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &runtimev1.AcknowledgeCancellationResponse{
		AttemptVersion: result.Attempt.ResourceVersion, RunVersion: result.Run.ResourceVersion,
		TaskVersion: result.Task.ResourceVersion,
	}, nil
}

func (s *Service) parseIdentity(ctx context.Context, identity *runtimev1.AttemptIdentity) (*runtimev1.AttemptIdentity, uuid.UUID, error) {
	if identity == nil || identity.GetFencingToken() <= 0 {
		return nil, uuid.Nil, status.Error(codes.InvalidArgument, "attempt identity and positive fencing token are required")
	}
	if err := s.authorizeTenant(identity.GetTenantId()); err != nil {
		return nil, uuid.Nil, err
	}
	if err := s.authorizePeer(ctx, identity.GetTenantId(), ""); err != nil {
		return nil, uuid.Nil, err
	}
	attemptID, err := uuid.Parse(identity.GetAttemptId())
	if err != nil {
		return nil, uuid.Nil, status.Error(codes.InvalidArgument, "attempt ID must be a UUID")
	}
	return identity, attemptID, nil
}

func (s *Service) authorizeTenant(tenantID string) error {
	if strings.TrimSpace(tenantID) == "" ||
		(s.allowedTenant == "*" && s.spiffeTrustDomain == "") ||
		(s.allowedTenant != "*" && tenantID != s.allowedTenant) {
		return status.Error(codes.PermissionDenied, "tenant is not authorized by this runtime endpoint")
	}
	return nil
}

// authorizePeer binds the request's identity claims to the peer's X.509-SVID
// (ADR-011). With claim binding configured, every call must arrive over
// mutual TLS presenting an SVID whose SPIFFE ID names the same tenant (and,
// when known, the same runtime instance) the request claims. Without claim
// binding (development plaintext transport) the check is inert; deployments
// enable it together with the mTLS server options.
func (s *Service) authorizePeer(ctx context.Context, tenantID, runtimeInstanceID string) error {
	if s.spiffeTrustDomain == "" {
		return nil
	}
	identity, err := spiffe.PeerIdentity(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, "mutual TLS worker identity is required: "+err.Error())
	}
	trustDomain, peerTenant, peerInstance, err := spiffe.WorkerClaims(identity)
	if err != nil || trustDomain != s.spiffeTrustDomain || peerTenant != tenantID {
		return status.Error(codes.PermissionDenied, "peer SPIFFE identity does not match the tenant claim")
	}
	if runtimeInstanceID != "" {
		if peerInstance != runtimeInstanceID {
			return status.Error(codes.PermissionDenied, "peer SPIFFE identity does not match the runtime instance claim")
		}
	}
	return nil
}

// workflowLineage renders the task's explicit workflow origin token
// (workflow_id/step_name/version).
func (s *Service) workflowLineage(ctx context.Context, assignment store.RuntimeAssignment) string {
	workflowID, stepName, version, ok, err := s.store.WorkflowLineage(ctx, assignment.Task.TenantID, assignment.Task.ID)
	if err != nil || !ok {
		return ""
	}
	return workflowID.String() + "/" + stepName + "/" + strconv.FormatInt(version, 10)
}

func assignmentProto(assignment store.RuntimeAssignment) *runtimev1.Assignment {
	result := &runtimev1.Assignment{
		Identity: &runtimev1.AttemptIdentity{
			TenantId: assignment.Attempt.TenantID, AttemptId: assignment.Attempt.ID.String(),
			FencingToken: assignment.Attempt.FencingToken,
		},
		RunId: assignment.Run.ID.String(), TaskId: assignment.Task.ID.String(),
		AgentVersionRef: assignment.Task.AgentVersionRef, Goal: assignment.Task.Goal,
		WorkloadSpecJson: append([]byte(nil), assignment.Task.Spec...), RuntimeClass: assignment.Attempt.RuntimeClass,
		RuntimePoolId: assignment.Attempt.RuntimePoolID, RuntimeInstanceId: assignment.Attempt.RuntimeInstanceID,
		AttemptVersion: assignment.Attempt.ResourceVersion, LeaseVersion: assignment.Lease.ResourceVersion,
		LeaseExpiresAt: timestamppb.New(assignment.Lease.ExpiresAt), Phase: string(assignment.Attempt.Phase),
	}
	if assignment.PendingApprovalID != nil {
		result.ApprovalId = assignment.PendingApprovalID.String()
	}
	if assignment.AgentVersion != nil {
		result.AgentVersionSpecJson = append([]byte(nil), assignment.AgentVersion.Spec...)
	}
	if assignment.ResumeCheckpoint != nil {
		result.ResumeCheckpoint = checkpointProto(*assignment.ResumeCheckpoint)
	}
	return result
}

func checkpointProto(checkpoint store.Checkpoint) *runtimev1.CheckpointReference {
	return &runtimev1.CheckpointReference{
		CheckpointId: checkpoint.ID.String(), AgentVersionRef: checkpoint.AgentVersionRef,
		RuntimeClass: checkpoint.RuntimeClass, Provider: checkpoint.Provider, RuntimeAbi: checkpoint.RuntimeABI,
		SchemaVersion: checkpoint.SchemaVersion, State: artifactProto(checkpoint.State),
		ConfirmedReceiptIds: append([]string(nil), checkpoint.ConfirmedReceiptIDs...),
		EnvelopeSha256:      hex.EncodeToString(checkpoint.EnvelopeSHA256[:]), CreatedAt: timestamppb.New(checkpoint.CreatedAt),
	}
}

func artifactProto(artifact store.ArtifactReference) *runtimev1.ArtifactReference {
	return &runtimev1.ArtifactReference{
		Uri: artifact.URI, Sha256: artifact.DigestHex(), SizeBytes: artifact.SizeBytes, MediaType: artifact.MediaType,
	}
}

func artifactFromProto(artifact *runtimev1.ArtifactReference) (store.ArtifactReference, error) {
	var result store.ArtifactReference
	if artifact == nil {
		return result, fmt.Errorf("artifact reference is required")
	}
	digest, err := hex.DecodeString(artifact.GetSha256())
	if err != nil || len(digest) != sha256.Size {
		return result, fmt.Errorf("artifact SHA-256 must be 64 hexadecimal characters")
	}
	copy(result.SHA256[:], digest)
	result.URI = artifact.GetUri()
	result.SizeBytes = artifact.GetSizeBytes()
	result.MediaType = artifact.GetMediaType()
	return result, result.Validate()
}

func domainPhase(phase runtimev1.AttemptPhase) (domain.AttemptPhase, error) {
	switch phase {
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_STARTING:
		return domain.AttemptStarting, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING:
		return domain.AttemptRunning, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_TOOL:
		return domain.AttemptWaitingTool, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_AGENT:
		return domain.AttemptWaitingAgent, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_APPROVAL:
		return domain.AttemptWaitingApproval, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_CHECKPOINTING:
		return domain.AttemptCheckpointing, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED:
		return domain.AttemptFailed, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_CANCEL_REQUESTED:
		return domain.AttemptCancelRequested, nil
	case runtimev1.AttemptPhase_ATTEMPT_PHASE_CANCELLED:
		return domain.AttemptCancelled, nil
	default:
		return "", fmt.Errorf("unsupported attempt phase %s", phase)
	}
}

func protoPhase(phase domain.AttemptPhase) runtimev1.AttemptPhase {
	switch phase {
	case domain.AttemptStarting:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_STARTING
	case domain.AttemptRunning:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING
	case domain.AttemptWaitingTool:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_TOOL
	case domain.AttemptWaitingAgent:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_AGENT
	case domain.AttemptWaitingApproval:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_APPROVAL
	case domain.AttemptCheckpointing:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_CHECKPOINTING
	case domain.AttemptFailed:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED
	case domain.AttemptCancelRequested:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_CANCEL_REQUESTED
	case domain.AttemptCancelled:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_CANCELLED
	default:
		return runtimev1.AttemptPhase_ATTEMPT_PHASE_UNSPECIFIED
	}
}

func rpcError(err error) error {
	if store.IsRetryableTransaction(err) {
		// Transient serialization failure: the caller must retry with
		// bounded backoff rather than treating the operation as failed.
		return status.Error(codes.Unavailable, "transient transaction conflict; retry")
	}
	switch {
	case errors.Is(err, store.ErrNoAssignment), errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrFenced):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrIdempotencyConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, store.ErrInvalidTransition), errors.Is(err, store.ErrLeaseNotExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, "runtime control operation failed")
	}
}
