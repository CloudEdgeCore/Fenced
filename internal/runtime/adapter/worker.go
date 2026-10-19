// Package adapter implements the Runtime Protocol provider that delegates an
// assignment to a language/framework-neutral Agent Runtime Interface endpoint.
//
// The package holds one concern: the adapter worker. The assignment loop,
// the transport to the Agent endpoint, the execution wait, the checkpoint
// handling and the fenced protocol calls live in the files next to this
// one, so each of them can be read on its own.
package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	agentversion "github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	domain "github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	store "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	mcp "github.com/CloudEdgeCore/Fenced/internal/mcp"
	redact "github.com/CloudEdgeCore/Fenced/internal/platform/redact"
	leasekeeper "github.com/CloudEdgeCore/Fenced/internal/runtime/leasekeeper"
	agent "github.com/CloudEdgeCore/Fenced/sdk/agent"
	uuid "github.com/google/uuid"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const (
	ProviderName        = "adapter-http"
	CheckpointSchema    = "fenced.adapter-checkpoint/v1"
	checkpointMediaType = "application/vnd.fenced.adapter-checkpoint+json"
	resultMediaType     = "application/vnd.fenced.adapter-result+json"
	controlRPCTimeout   = 15 * time.Second
	defaultPollInterval = 250 * time.Millisecond
	maxPollInterval     = 2 * time.Second
	maxResultBytes      = 2 << 20
	maxCollectedEvents  = 4096
)

type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader) (store.ArtifactReference, error)
	Open(context.Context, string, store.ArtifactReference) (io.ReadCloser, error)
}

// ExecutionWindow receives the fenced execution context for the duration of
// one assignment: the runtime opens the sandbox Agent's brokered access
// (MCP) when the attempt is RUNNING and closes it on every exit path
// (default deny outside execution windows). Open returns the closer the
// worker defers, so concurrent workers sharing one Agent endpoint bind
// their identities per attempt.
type ExecutionWindow interface {
	Open(mcp.AttemptContext) func()
}

type Worker struct {
	control           runtimev1.RuntimeControlServiceClient
	artifacts         ArtifactStore
	runtime           *agent.Client
	endpoint          string
	tenantID          string
	runtimeInstanceID string
	heartbeatTTL      time.Duration
	pollInterval      time.Duration
	window            ExecutionWindow
	// bindings resolve logical manifest entrypoints (fenced-binding://…)
	// to concrete deployment endpoints, keeping mutable endpoint state out
	// of immutable AgentVersions. The resolved client is assignment-local:
	// resolveRuntime never mutates shared worker state, so a future
	// concurrent RunOnce cannot misroute another assignment's client.
	bindings   *RuntimeBindings
	httpClient *http.Client
	clients    map[string]*agent.Client
}

// WithExecutionWindow attaches the sandbox brokered-access window (the
// loopback MCP identity slot). The worker publishes the fenced identity and
// the AgentVersion capability grants for the duration of each assignment.
func (w *Worker) WithExecutionWindow(window ExecutionWindow) *Worker {
	w.window = window
	return w
}

// WithRuntimeBindings attaches the deployment's runtime binding table. When
// set, assignments whose manifest entrypoint is a logical binding reference
// (or whose version ref has an explicit binding) resolve through it; the
// worker's constructor endpoint remains the highest-priority override - a
// dedicated-worker configuration ValidateEndpointOverride guards.
func (w *Worker) WithRuntimeBindings(bindings *RuntimeBindings) *Worker {
	w.bindings = bindings
	return w
}

func NewWorker(
	control runtimev1.RuntimeControlServiceClient,
	artifacts ArtifactStore,
	endpoint, tenantID, runtimeInstanceID string,
	heartbeatTTL time.Duration,
	httpClient *http.Client,
) (*Worker, error) {
	if control == nil || artifacts == nil {
		return nil, fmt.Errorf("runtime Protocol client and artifact store are required")
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(runtimeInstanceID) == "" || heartbeatTTL <= 0 {
		return nil, fmt.Errorf("tenant, runtime instance and positive heartbeat TTL are required")
	}
	// An empty endpoint is valid when runtime bindings resolve every
	// assignment; resolveRuntime fails closed on unresolvable entrypoints.
	// The production endpoint policy applies at construction: a plaintext
	// constructor endpoint must be loopback (co-located runtime) and a
	// remote endpoint must be HTTPS.
	var client *agent.Client
	if strings.TrimSpace(endpoint) != "" {
		if err := (EndpointPolicy{}).Validate(endpoint); err != nil {
			return nil, err
		}
		if err := refuseInsecureTransport(httpClient, strings.TrimRight(endpoint, "/")); err != nil {
			return nil, err
		}
		var err error
		client, err = agent.NewClient(endpoint, httpClient)
		if err != nil {
			return nil, err
		}
	}
	return &Worker{
		control: control, artifacts: artifacts, runtime: client, endpoint: strings.TrimRight(endpoint, "/"),
		tenantID: tenantID, runtimeInstanceID: runtimeInstanceID, heartbeatTTL: heartbeatTTL,
		pollInterval: defaultPollInterval, httpClient: httpClient, clients: map[string]*agent.Client{},
	}, nil
}

// resolveRuntime picks the Runtime Interface client for one assignment.
// Priority: the worker's explicit constructor endpoint (the operator's
// per-worker override), then a runtime binding for the agent version
// (exact ref or name wildcard), then a concrete http(s) entrypoint embedded
// in the manifest. A logical binding entrypoint with no covering binding
// fails closed - an unresolved deployment reference must never silently
// fall through to another agent's endpoint.

func (w *Worker) RunOnce(ctx context.Context) (processed bool, runErr error) {
	assignmentAcquired := false
	defer func() {
		// Once polling succeeded, PermissionDenied means this concrete fenced
		// assignment was lost (expired lease or a newer recovery owner). The
		// long-lived worker must continue polling instead of terminating and
		// reducing fleet capacity. A real endpoint authorization failure is
		// still surfaced by the next PollAssignment call.
		if assignmentAcquired && status.Code(runErr) == codes.PermissionDenied {
			processed, runErr = false, nil
		}
	}()
	pollCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	polled, err := w.control.PollAssignment(pollCtx, &runtimev1.PollAssignmentRequest{
		TenantId: w.tenantID, RuntimeInstanceId: w.runtimeInstanceID,
	})
	cancel()
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("poll adapter assignment: %w", err)
	}
	assignment := polled.GetAssignment()
	if assignment == nil || assignment.GetIdentity() == nil {
		return false, fmt.Errorf("runtime assignment is incomplete")
	}
	if assignment.GetIdentity().GetTenantId() != w.tenantID || assignment.GetRuntimeInstanceId() != w.runtimeInstanceID {
		return false, fmt.Errorf("runtime assignment identity does not match adapter worker")
	}
	assignmentAcquired = true
	target, capabilities, checkpointPolicy, err := w.target(assignment)
	if err != nil {
		return w.fail(ctx, assignment.GetIdentity(), assignment.GetAttemptVersion(), "adapter_manifest_invalid", err)
	}
	runtimeClient, err := w.resolveRuntime(assignment.GetAgentVersionRef(), target)
	if err != nil {
		return w.fail(ctx, assignment.GetIdentity(), assignment.GetAttemptVersion(), "adapter_endpoint_unresolved", err)
	}
	identity := assignment.GetIdentity()
	taskID, runID, attemptID, err := parseAssignmentIdentity(assignment)
	if err != nil {
		return false, fmt.Errorf("adapter assignment identity is invalid: %w", err)
	}
	version, proceed, err := w.advanceAttempt(ctx, identity, assignment.GetAttemptVersion(),
		domain.AttemptPlaced, domain.AttemptStarting, runtimev1.AttemptPhase_ATTEMPT_PHASE_STARTING)
	if err != nil {
		return false, err
	}
	if !proceed {
		return false, nil
	}
	version, proceed, err = w.advanceAttempt(ctx, identity, version,
		domain.AttemptStarting, domain.AttemptRunning, runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING)
	if err != nil {
		return false, err
	}
	if !proceed {
		return false, nil
	}
	if w.window != nil {
		lineage := parseWorkflowLineage(assignment.GetWorkflowLineage())
		closeWindow := w.window.Open(mcp.AttemptContext{
			TenantID: identity.GetTenantId(), TaskID: taskID,
			RunID: runID, AttemptID: attemptID,
			FencingToken: identity.GetFencingToken(), AgentVersionRef: assignment.GetAgentVersionRef(),
			WorkflowID: lineage.workflowID, WorkflowVersion: lineage.workflowVersion, ParentStepName: lineage.stepName,
			AllowedModels:              cloneExplicit(capabilities.Models),
			AllowedMemoryNamespaces:    cloneExplicit(capabilities.Memory),
			AllowedMemorySensitivities: memorySensitivities(capabilities.MemorySensitivities),
			CanSpawnTasks:              capabilities.SpawnTasks,
			AllowedChildAgents:         cloneExplicit(capabilities.ChildAgents),
		})
		defer closeWindow()
	}
	heartbeat, err := w.heartbeat(ctx, assignment)
	if err != nil {
		return false, err
	}
	if heartbeat.GetCancelRequested() {
		return true, nil
	}
	keeper, executionCtx := leasekeeper.Start(ctx, leasekeeper.Options{
		Client: w.control, Identity: identity, AttemptID: identity.GetAttemptId(),
		HeartbeatTTL: w.heartbeatTTL, RPCTimeout: controlRPCTimeout,
	}, heartbeat.GetLeaseVersion(), heartbeat.GetAttemptVersion())
	defer keeper.Stop()

	if assignment.GetResumeCheckpoint() != nil {
		if checkpointPolicy.Mode != agentversion.CheckpointLogical {
			return w.fail(ctx, identity, version, "adapter_restore_failed", fmt.Errorf("checkpoint resume is disabled by the AgentVersion manifest"))
		}
		if err := w.restore(executionCtx, runtimeClient, assignment, target, checkpointPolicy); err != nil {
			return w.fail(ctx, identity, version, "adapter_restore_failed", err)
		}
	}
	start := agent.StartRequest{
		ExecutionID: identity.GetAttemptId(), AgentVersionRef: assignment.GetAgentVersionRef(),
		Goal: assignment.GetGoal(), Input: json.RawMessage(assignment.GetWorkloadSpecJson()),
		Capabilities: capabilities,
	}
	if _, err := runtimeClient.Start(executionCtx, start); err != nil {
		if keeper.Cancelled() {
			return true, nil
		}
		return w.fail(ctx, identity, version, "adapter_start_failed", err)
	}
	checkpointSequence := 0
	result, events, err := w.wait(executionCtx, runtimeClient, identity.GetAttemptId(),
		time.Duration(checkpointPolicy.IntervalSeconds)*time.Second, func() error {
			checkpointSequence++
			nextVersion, checkpointErr := w.commitLogicalCheckpoint(executionCtx, runtimeClient, assignment, target,
				checkpointPolicy, version, fmt.Sprintf("checkpoint-%d", checkpointSequence))
			if checkpointErr == nil {
				version = nextVersion
			}
			return checkpointErr
		})
	if err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = runtimeClient.Stop(stopCtx, identity.GetAttemptId())
		stopCancel()
		if keeper.Cancelled() {
			return true, nil
		}
		if fenceErr := keeper.FenceError(); fenceErr != nil {
			return false, fenceErr
		}
		return w.fail(ctx, identity, version, "adapter_execution_failed", err)
	}
	if result.Status != agent.StatusSucceeded {
		return w.fail(ctx, identity, version, "adapter_result_"+strings.ToLower(result.Status),
			fmt.Errorf("%s: %s", result.ErrorCode, result.Error))
	}
	if keeper.Cancelled() {
		return true, nil
	}
	if checkpointPolicy.Mode == agentversion.CheckpointLogical {
		version, err = w.commitLogicalCheckpoint(executionCtx, runtimeClient, assignment, target, checkpointPolicy, version, "checkpoint-final")
		if err != nil {
			return w.fail(ctx, identity, version, "adapter_checkpoint_failed", err)
		}
	}
	eventSummary := map[string]any{"count": len(events)}
	if len(events) > 0 {
		eventSummary["lastSequence"] = events[len(events)-1].Sequence
	}
	safeOutput, ok := redact.RedactJSON(result.Output)
	if !ok {
		return w.fail(ctx, identity, version, "adapter_invalid_result", fmt.Errorf("runtime result is not valid JSON"))
	}
	resultDocument, err := json.Marshal(map[string]any{
		"agentVersionRef": assignment.GetAgentVersionRef(), "attemptId": identity.GetAttemptId(),
		"provider": ProviderName, "runtimeABI": target.RuntimeABI,
		"output": json.RawMessage(safeOutput), "eventSummary": eventSummary, "resumed": assignment.GetResumeCheckpoint() != nil,
		"checkpointMode": checkpointPolicy.Mode,
	})
	if err != nil {
		return false, err
	}
	if len(resultDocument) > maxResultBytes {
		return w.fail(ctx, identity, version, "adapter_result_too_large", fmt.Errorf("adapter result exceeds the durable artifact limit"))
	}
	resultArtifact, err := w.artifacts.Put(executionCtx, w.tenantID, resultMediaType, bytes.NewReader(resultDocument))
	if err != nil {
		return false, fmt.Errorf("persist adapter result: %w", err)
	}
	if _, err := w.control.CompleteAttempt(executionCtx, &runtimev1.CompleteAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: operationKey(assignment, "complete"), Result: artifactProto(resultArtifact),
	}); err != nil {
		return false, fmt.Errorf("complete adapter attempt: %w", err)
	}
	return true, nil
}

func memorySensitivities(configured []string) []string {
	if len(configured) == 0 {
		return []string{"internal"}
	}
	return cloneExplicit(configured)
}

func (w *Worker) target(assignment *runtimev1.Assignment) (agentversion.RuntimeTarget, agent.CapabilityGrant, agentversion.CheckpointPolicy, error) {
	var spec agentversion.Spec
	if err := json.Unmarshal(assignment.GetAgentVersionSpecJson(), &spec); err != nil {
		return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("decode immutable AgentVersion spec: %w", err)
	}
	for _, target := range spec.Runtimes {
		if target.Class != assignment.GetRuntimeClass() {
			continue
		}
		if (target.Interface != agentversion.RuntimeInterfaceV1 && target.Interface != agentversion.RuntimeInterfaceV1Alpha1) || len(target.Entrypoint) == 0 {
			return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{},
				fmt.Errorf("runtime target does not declare a supported interface and logical entrypoint")
		}
		if spec.Capabilities == nil {
			return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("capability declaration is required")
		}
		if spec.Checkpoint == nil {
			return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("checkpoint policy is required")
		}
		// The agent-facing grant list is compared against bare tool names,
		// so a grant that pins a version contributes its tool name here.
		capabilityTools := agentversion.PublicationToolGrants(
			spec.Capabilities.Tools, spec.Capabilities.ToolVersions).Capability
		return target, agent.CapabilityGrant{
			Tools: cloneExplicit(capabilityTools), Models: cloneExplicit(spec.Capabilities.Models),
			Memory: cloneExplicit(spec.Capabilities.Memory), Secrets: cloneExplicit(spec.Capabilities.Secrets),
			MemorySensitivities: memorySensitivities(spec.Capabilities.MemorySensitivities),
			SpawnTasks:          spec.Capabilities.SpawnTasks, ChildAgents: cloneExplicit(spec.Capabilities.ChildAgents),
		}, *spec.Checkpoint, nil
	}
	return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("no runtime target for class %q", assignment.GetRuntimeClass())
}

func cloneExplicit(values []string) []string {
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func parseAssignmentIdentity(assignment *runtimev1.Assignment) (uuid.UUID, uuid.UUID, uuid.UUID, error) {
	values := []struct {
		name  string
		value string
	}{{"taskId", assignment.GetTaskId()}, {"runId", assignment.GetRunId()}, {"attemptId", assignment.GetIdentity().GetAttemptId()}}
	parsed := make([]uuid.UUID, len(values))
	for index, value := range values {
		id, err := uuid.Parse(value.value)
		if err != nil || id == uuid.Nil {
			return uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("%s must be a non-zero UUID", value.name)
		}
		parsed[index] = id
	}
	return parsed[0], parsed[1], parsed[2], nil
}

// wait observes one execution to its terminal state. Stream-capable
// runtimes hold a single long-lived event-stream connection (one HTTP
// request instead of a poll cycle, with the terminal frame carrying the
// result); a stream that drops mid-flight reconnects from the last consumed
// sequence (bounded attempts, no event loss or duplication) and finally
// falls back to the frozen v1 polling endpoints for v1-only runtimes,
// resuming from the stream cursor.

type workflowLineage struct {
	workflowID      uuid.UUID
	stepName        string
	workflowVersion int64
}

// parseWorkflowLineage decodes the assignment's workflow lineage token.
func parseWorkflowLineage(value string) workflowLineage {
	if value == "" {
		return workflowLineage{}
	}
	parts := strings.Split(value, "/")
	if len(parts) != 3 {
		return workflowLineage{}
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		return workflowLineage{}
	}
	version, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return workflowLineage{}
	}
	return workflowLineage{workflowID: id, stepName: parts[1], workflowVersion: version}
}
