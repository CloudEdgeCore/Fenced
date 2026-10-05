// The orchestrator's guarded dynamic-spawn surface: the runtime adapter calls
// SpawnStep when a sandboxed agent invokes the brokered fenced.task.spawn
// system tool. All guards (recursion depth, dynamic-step caps, workflow
// budgets, spawn idempotency) run inside the kernel store transaction; this
// facade only maps the fenced request onto it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	workflowkernel "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// spawnService adapts WorkflowSpawnService onto the kernel store.
type spawnService struct {
	runtimev1.UnimplementedWorkflowSpawnServiceServer
	workflows kernelstore.WorkflowStore
	runtime   spawnRuntimeStore
	// spiffeTrustDomain enables peer tenant binding in production. Empty is
	// permitted only behind the command's explicit loopback dev mode.
	spiffeTrustDomain string
}

type spawnRuntimeStore interface {
	GetRuntimeAssignment(context.Context, string, uuid.UUID, int64) (kernelstore.RuntimeAssignment, error)
	WorkflowLineage(context.Context, string, uuid.UUID) (uuid.UUID, string, int64, bool, error)
}

// SpawnStep creates one dynamic workflow step (or replays an identical
// spawn). The runtime-adapter tenant must match the workflow's tenant.
func (s *spawnService) SpawnStep(ctx context.Context, request *runtimev1.SpawnStepRequest) (*runtimev1.SpawnStepResponse, error) {
	if request == nil || request.GetIdentity() == nil {
		return nil, status.Error(codes.InvalidArgument, "fenced attempt identity is required")
	}
	identity := request.GetIdentity()
	attemptID, err := uuid.Parse(identity.GetAttemptId())
	if err != nil || identity.GetTenantId() == "" || identity.GetFencingToken() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "tenant, attempt id and positive fencing token are required")
	}
	if s.spiffeTrustDomain != "" {
		trustDomain, peerTenant, _, peerErr := spiffe.PeerWorkerClaims(ctx)
		if peerErr != nil {
			return nil, status.Error(codes.Unauthenticated, "verified worker identity is required")
		}
		if trustDomain != s.spiffeTrustDomain || peerTenant != identity.GetTenantId() {
			return nil, status.Error(codes.PermissionDenied, "peer SPIFFE identity does not match the tenant claim")
		}
	}
	workflowID, err := uuid.Parse(request.GetWorkflowId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "workflow id must be a UUID")
	}
	assignment, err := s.runtime.GetRuntimeAssignment(ctx, identity.GetTenantId(), attemptID, identity.GetFencingToken())
	if err != nil {
		if errors.Is(err, kernelstore.ErrFenced) || errors.Is(err, kernelstore.ErrNotFound) {
			return nil, status.Error(codes.PermissionDenied, "attempt identity is not current")
		}
		return nil, status.Error(codes.Internal, "resolve attempt assignment")
	}
	if !spawnEligibleAttemptPhase(assignment.Attempt.Phase) || assignment.Task.Phase != domain.TaskRunning ||
		!assignment.Lease.ExpiresAt.After(time.Now().UTC()) {
		return nil, status.Error(codes.PermissionDenied, "attempt is not active for dynamic spawning")
	}
	lineageID, lineageStep, _, ok, err := s.runtime.WorkflowLineage(ctx, identity.GetTenantId(), assignment.Task.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, "resolve workflow lineage")
	}
	if !ok || lineageID != workflowID || lineageStep != request.GetParentStep() {
		return nil, status.Error(codes.PermissionDenied, "attempt does not own the requested workflow parent")
	}
	if err := authorizeSpawnTarget(assignment, request.GetAgentVersionRef()); err != nil {
		return nil, err
	}
	target, err := s.workflows.GetWorkflow(ctx, identity.GetTenantId(), workflowID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}
	if request.GetName() == "" || strings.TrimSpace(request.GetGoal()) == "" || request.GetAgentVersionRef() == "" ||
		strings.TrimSpace(request.GetIdempotencyKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name, goal, agentVersionRef and idempotencyKey are required")
	}
	if err := kernelstore.ValidateStepName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(request.GetGoal()) > 8192 || len(request.GetIdempotencyKey()) > 512 || request.GetMaxAttempts() < 0 || request.GetMaxAttempts() > 10 {
		return nil, status.Error(codes.InvalidArgument, "goal, idempotencyKey or maxAttempts exceeds its bound")
	}
	if _, _, _, err := agentversion.ParseRef(request.GetAgentVersionRef()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if raw := request.GetArgumentsJson(); raw != "" && !json.Valid([]byte(raw)) {
		return nil, status.Error(codes.InvalidArgument, "argumentsJson must contain one JSON value")
	}
	storedSpec, err := workflowkernel.DecodeWorkflowSpec(target.Spec)
	if err != nil {
		return nil, status.Error(codes.Internal, "stored workflow specification is invalid")
	}
	mergedSpec, err := storedSpec.MergeTaskSpec([]byte(request.GetSpecJson()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	policy := kernelstore.DecodeWorkflowRuntimePolicy(target.Spec)
	result, err := s.workflows.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflowID, TenantID: identity.GetTenantId(),
		WorkflowVersion: target.ResourceVersion, ParentStepName: request.GetParentStep(),
		Name: request.GetName(), Goal: request.GetGoal(), AgentVersionRef: request.GetAgentVersionRef(),
		Spec: mergedSpec, MaxAttempts: int(request.GetMaxAttempts()), Guards: policy.SpawnGuards,
		IdempotencyKey: request.GetIdempotencyKey(), Arguments: []byte(request.GetArgumentsJson()),
	})
	if err != nil {
		if code, ok := kernelstore.DenialCode(err); ok {
			agentmetrics.SpawnOutcome(ctx, code)
		} else {
			agentmetrics.SpawnOutcome(ctx, "internal_error")
		}
		return spawnFailure(err)
	}
	outcome := "created"
	if !result.Created {
		outcome = "replayed"
	}
	agentmetrics.SpawnOutcome(ctx, outcome)
	return &runtimev1.SpawnStepResponse{
		Outcome: outcome, StepName: result.Step.Name, SpawnDepth: int32(result.Step.SpawnDepth),
	}, nil
}

func spawnEligibleAttemptPhase(phase domain.AttemptPhase) bool {
	switch phase {
	case domain.AttemptRunning, domain.AttemptWaitingTool, domain.AttemptWaitingAgent, domain.AttemptCheckpointing:
		return true
	default:
		return false
	}
}

// authorizeSpawnTarget is the server-side capability check. The MCP broker
// performs the same check for a useful agent-facing error, but this check is
// authoritative if an adapter is compromised or misconfigured.
func authorizeSpawnTarget(assignment kernelstore.RuntimeAssignment, target string) error {
	if assignment.AgentVersion == nil {
		return status.Error(codes.PermissionDenied, "assignment has no immutable AgentVersion")
	}
	var spec agentversion.Spec
	if err := json.Unmarshal(assignment.AgentVersion.Spec, &spec); err != nil || spec.Capabilities == nil {
		return status.Error(codes.PermissionDenied, "AgentVersion has no valid capability declaration")
	}
	if !spec.Capabilities.SpawnTasks {
		return status.Error(codes.PermissionDenied, "AgentVersion is not allowed to spawn tasks")
	}
	// The child allowlist honors the shared MatchGrant wildcard contract
	// (exact, global "*", or a namespace suffix wildcard like "team-a/*"), so
	// namespaced child references authorize identically to the broker's
	// agent-facing pre-check (P1-07).
	for _, allowed := range spec.Capabilities.ChildAgents {
		if capability.MatchGrant(allowed, target) {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "child AgentVersion is not allowlisted")
}

// spawnFailure maps kernel guard denials onto structured outcomes (the
// agent sees the code, not a raw error).
func spawnFailure(err error) (*runtimev1.SpawnStepResponse, error) {
	if code, ok := kernelstore.DenialCode(err); ok {
		return &runtimev1.SpawnStepResponse{Outcome: code, Message: err.Error()}, nil
	}
	return nil, status.Error(codes.Internal, err.Error())
}
