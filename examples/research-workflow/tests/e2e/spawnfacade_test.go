//go:build integration

package research_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	workflowkernel "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// spawnFacade mirrors the orchestrator's guarded WorkflowSpawnService onto
// the kernel store, so tests exercise the exact production guards (fencing,
// lineage ownership, capability allowlist, dynamic-step caps, budgets and
// spawn idempotency) without importing a main package.
type spawnFacade struct {
	runtimev1.UnimplementedWorkflowSpawnServiceServer
	workflows kernelstore.WorkflowStore
	runtime   kernelstore.RuntimeStore
}

func newSpawnFacade(store *postgresstore.Store) *spawnFacade {
	return &spawnFacade{workflows: store, runtime: store}
}

func (s *spawnFacade) SpawnStep(ctx context.Context, request *runtimev1.SpawnStepRequest) (*runtimev1.SpawnStepResponse, error) {
	if request == nil || request.GetIdentity() == nil {
		return nil, status.Error(codes.InvalidArgument, "fenced attempt identity is required")
	}
	identity := request.GetIdentity()
	attemptID, err := uuid.Parse(identity.GetAttemptId())
	if err != nil || identity.GetTenantId() == "" || identity.GetFencingToken() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "tenant, attempt id and positive fencing token are required")
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
	if !eligibleAttemptPhase(string(assignment.Attempt.Phase)) || assignment.Task.Phase != "RUNNING" ||
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
	if len(request.GetGoal()) > 8192 || request.GetMaxAttempts() < 0 || request.GetMaxAttempts() > 10 {
		return nil, status.Error(codes.InvalidArgument, "goal or maxAttempts out of bounds")
	}
	if _, _, _, err := agentversion.ParseRef(request.GetAgentVersionRef()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
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
			return &runtimev1.SpawnStepResponse{Outcome: code, Message: err.Error()}, nil
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	outcome := "created"
	if !result.Created {
		outcome = "replayed"
	}
	return &runtimev1.SpawnStepResponse{
		Outcome: outcome, StepName: result.Step.Name, SpawnDepth: int32(result.Step.SpawnDepth),
	}, nil
}

func eligibleAttemptPhase(phase string) bool {
	switch phase {
	case "RUNNING", "WAITING_TOOL", "WAITING_AGENT", "CHECKPOINTING":
		return true
	default:
		return false
	}
}

// authorizeSpawnTarget re-checks the immutable capability declaration.
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
	for _, allowed := range spec.Capabilities.ChildAgents {
		if capability.MatchGrant(allowed, target) {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "child AgentVersion is not allowlisted")
}
