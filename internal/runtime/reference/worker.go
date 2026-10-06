// Package reference provides a deterministic Runtime Protocol conformance worker.
// It is a development and fault-injection provider, not a sandbox for untrusted code.
package reference

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/attemptstate"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/leasekeeper"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// controlRPCTimeout bounds every fenced control-plane RPC so a hung
	// endpoint can never wedge the worker loop.
	controlRPCTimeout = 15 * time.Second
	// gatewayRPCTimeout bounds each tool/model gateway invocation.
	gatewayRPCTimeout = 60 * time.Second
	// artifactRPCTimeout bounds local artifact I/O.
	artifactRPCTimeout = 30 * time.Second
)

var (
	// errApprovalRequired parks the attempt in WAITING_APPROVAL: a high-risk
	// tool in the script needs a human decision before it can execute.
	errApprovalRequired = errors.New("tool approval required")
	// errApprovalPending keeps the attempt parked: the human decision is not
	// final yet.
	errApprovalPending = errors.New("tool approval still pending")
)

const (
	ProviderName        = "reference-go"
	RuntimeABI          = "fenced.reference/v1"
	CheckpointSchema    = "fenced.reference-state/v1"
	checkpointMediaType = "application/vnd.fenced.reference-state+json"
	resultMediaType     = "application/vnd.fenced.reference-result+json"
)

type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader) (store.ArtifactReference, error)
	Open(context.Context, string, store.ArtifactReference) (io.ReadCloser, error)
}

type Worker struct {
	client            runtimev1.RuntimeControlServiceClient
	artifacts         ArtifactStore
	toolGateway       gatewayv1.ToolGatewayServiceClient
	modelGateway      modelv1.ModelGatewayServiceClient
	tenantID          string
	runtimeInstanceID string
	heartbeatTTL      time.Duration
	identity          *IdentitySlot
}

func NewWorker(client runtimev1.RuntimeControlServiceClient, artifacts ArtifactStore, tenantID, runtimeInstanceID string, heartbeatTTL time.Duration) *Worker {
	return &Worker{client: client, artifacts: artifacts, tenantID: tenantID, runtimeInstanceID: runtimeInstanceID, heartbeatTTL: heartbeatTTL}
}

// WithToolGateway wires the Tool Gateway boundary so workload-spec tool
// scripts execute through the full decision chain (policy, budget, receipts)
// before the attempt completes.
func (w *Worker) WithToolGateway(gateway gatewayv1.ToolGatewayServiceClient) *Worker {
	w.toolGateway = gateway
	return w
}

// WithModelGateway wires the Model Gateway boundary so workload-spec model
// scripts are metered (Begin/Settle/Finish) against the task budget.
func (w *Worker) WithModelGateway(gateway modelv1.ModelGatewayServiceClient) *Worker {
	w.modelGateway = gateway
	return w
}

// WithIdentitySlot publishes the current Attempt's fenced identity for the
// duration of each assignment execution window. A sandboxed Agent's MCP
// endpoint resolves its identity from this slot; calls outside the window are
// denied by default.
func (w *Worker) WithIdentitySlot(slot *IdentitySlot) *Worker {
	w.identity = slot
	return w
}

// startLeaseKeeper renews the runtime lease on a background loop for the
// duration of one execution window, returning the keeper and the execution
// context it cancels when the lease can no longer be renewed (fence broken)
// or the kernel requests cancellation (acknowledged).
func (w *Worker) startLeaseKeeper(ctx context.Context, assignment *runtimev1.Assignment, initialLeaseVersion, initialAttemptVersion int64) (*leasekeeper.Keeper, context.Context) {
	return leasekeeper.Start(ctx, leasekeeper.Options{
		Client:       w.client,
		Identity:     assignment.GetIdentity(),
		AttemptID:    assignment.GetIdentity().GetAttemptId(),
		HeartbeatTTL: w.heartbeatTTL,
		RPCTimeout:   controlRPCTimeout,
	}, initialLeaseVersion, initialAttemptVersion)
}

// rpcContext bounds one fenced RPC so a hung endpoint cannot wedge the
// worker loop.
func rpcContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout)
}

func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	pollCtx, cancelPoll := rpcContext(ctx, controlRPCTimeout)
	polled, err := w.client.PollAssignment(pollCtx, &runtimev1.PollAssignmentRequest{
		TenantId: w.tenantID, RuntimeInstanceId: w.runtimeInstanceID,
	})
	cancelPoll()
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("poll runtime assignment: %w", err)
	}
	assignment := polled.GetAssignment()
	if assignment == nil || assignment.GetIdentity() == nil {
		return false, fmt.Errorf("runtime assignment is incomplete")
	}
	if assignment.GetRuntimeInstanceId() != w.runtimeInstanceID || assignment.GetIdentity().GetTenantId() != w.tenantID {
		return false, fmt.Errorf("runtime assignment identity does not match worker")
	}
	if w.identity != nil {
		identity := assignment.GetIdentity()
		w.identity.Set(mcp.AttemptContext{
			TenantID: identity.GetTenantId(), TaskID: uuid.MustParse(assignment.GetTaskId()),
			RunID: uuid.MustParse(assignment.GetRunId()), AttemptID: uuid.MustParse(identity.GetAttemptId()),
			FencingToken: identity.GetFencingToken(), AgentVersionRef: assignment.GetAgentVersionRef(),
		})
		defer w.identity.Clear()
	}
	identity := assignment.GetIdentity()
	version := assignment.GetAttemptVersion()
	if assignment.GetPhase() == string(domain.AttemptWaitingApproval) {
		return w.resumeWaitingApproval(ctx, assignment)
	}

	starting, err := w.transition(ctx, identity, version,
		runtimev1.AttemptPhase_ATTEMPT_PHASE_STARTING, "starting")
	if err != nil {
		if status.Code(err) == codes.Aborted {
			// Another worker already claimed this attempt: skip and re-poll.
			return false, nil
		}
		return false, err
	}
	version = starting.GetAttemptVersion()
	running, err := w.transition(ctx, identity, version,
		runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING, "running")
	if err != nil {
		if status.Code(err) == codes.Aborted {
			return false, nil
		}
		return false, err
	}
	version = running.GetAttemptVersion()

	heartbeat, err := w.heartbeat(ctx, assignment)
	if err != nil {
		return false, err
	}
	if heartbeat.GetCancelRequested() {
		return true, nil
	}

	goalDigest := sha256.Sum256([]byte(assignment.GetGoal()))
	var toolResults, modelResults []map[string]any
	if assignment.GetResumeCheckpoint() == nil {
		// Keep the runtime lease alive while the workload executes so a run
		// longer than the lease TTL can never be fenced mid-execution; a
		// cancellation or a broken fence cancels the execution context.
		keeper, execCtx := w.startLeaseKeeper(ctx, assignment, heartbeat.GetLeaseVersion(), heartbeat.GetAttemptVersion())
		defer keeper.Stop()
		spec, err := workload.Decode(assignment.GetWorkloadSpecJson())
		if err != nil {
			return w.fail(ctx, identity, version, "invalid_workload_spec", err)
		}
		var confirmedReceipts []string
		toolResults, confirmedReceipts, err = w.executeToolScript(execCtx, assignment, spec)
		switch {
		case err == nil:
			// fall through to checkpoint and completion
		case errors.Is(err, errApprovalRequired):
			// First encounter of a high-risk tool: park the attempt in
			// WAITING_APPROVAL so a human decision can resume it.
			if _, parkErr := w.transition(ctx, identity, version,
				runtimev1.AttemptPhase_ATTEMPT_PHASE_WAITING_APPROVAL, "park-waiting-approval"); parkErr != nil {
				return false, fmt.Errorf("park attempt for approval: %w", parkErr)
			}
			return false, nil
		case errors.Is(err, errApprovalPending):
			return false, nil
		default:
			if keeper.Cancelled() {
				// Cancellation requested mid-script: the keeper acknowledged
				// it, so the attempt is CANCEL_REQUESTED and the kernel will
				// finalize it; the worker must not touch it again.
				return true, nil
			}
			if fenceErr := keeper.FenceError(); fenceErr != nil {
				return false, fenceErr
			}
			return w.fail(ctx, identity, version, "tool_script_failed", err)
		}
		modelResults, err = w.executeModelScript(execCtx, assignment, spec)
		if err != nil {
			if keeper.Cancelled() {
				return true, nil
			}
			if fenceErr := keeper.FenceError(); fenceErr != nil {
				return false, fenceErr
			}
			return w.fail(ctx, identity, version, "model_script_failed", err)
		}
		version, err = w.commitCheckpoint(execCtx, assignment, version, toolResults, modelResults, confirmedReceipts, goalDigest)
		if err != nil {
			if keeper.Cancelled() {
				return true, nil
			}
			if fenceErr := keeper.FenceError(); fenceErr != nil {
				return false, fenceErr
			}
			return false, err
		}
		if keeper.Cancelled() {
			// The cancellation won after the checkpoint: skip completion.
			return true, nil
		}
	} else if err := w.restoreCheckpoint(ctx, assignment, goalDigest); err != nil {
		return false, err
	}

	// The result document is built once: the base envelope plus the tool and
	// model results when present (or the confirmed results from the resume
	// checkpoint after recovery).
	document := map[string]any{
		"agentVersionRef": assignment.GetAgentVersionRef(), "attemptId": identity.GetAttemptId(),
		"goal": assignment.GetGoal(), "provider": ProviderName, "resumed": assignment.GetResumeCheckpoint() != nil,
	}
	if assignment.GetResumeCheckpoint() != nil {
		if err := w.mergeConfirmedResults(ctx, assignment, document); err != nil {
			return false, err
		}
	} else {
		if len(toolResults) > 0 {
			document["toolResults"] = toolResults
		}
		if len(modelResults) > 0 {
			document["modelResults"] = modelResults
		}
	}
	resultDocument, err := json.Marshal(document)
	if err != nil {
		return false, err
	}
	if err := w.complete(ctx, assignment, version, resultDocument); err != nil {
		return false, err
	}
	return true, nil
}

// resumeWaitingApproval handles an assignment polled while the attempt is
// parked in WAITING_APPROVAL: the worker renews the lease, re-presents the
// pending approval to the Tool Gateway, and either resumes execution or parks
// again. Rejected or expired approvals fail the attempt.
func (w *Worker) resumeWaitingApproval(ctx context.Context, assignment *runtimev1.Assignment) (bool, error) {
	identity := assignment.GetIdentity()
	version := assignment.GetAttemptVersion()
	heartbeat, err := w.heartbeat(ctx, assignment)
	if err != nil {
		return false, err
	}
	if heartbeat.GetCancelRequested() {
		return true, nil
	}
	if assignment.GetApprovalId() == "" {
		return w.fail(ctx, identity, version, "approval_missing",
			fmt.Errorf("attempt is waiting for approval but the assignment carries none"))
	}

	goalDigest := sha256.Sum256([]byte(assignment.GetGoal()))
	keeper, execCtx := w.startLeaseKeeper(ctx, assignment, heartbeat.GetLeaseVersion(), heartbeat.GetAttemptVersion())
	defer keeper.Stop()
	spec, err := workload.Decode(assignment.GetWorkloadSpecJson())
	if err != nil {
		return w.fail(ctx, identity, version, "invalid_workload_spec", err)
	}
	toolResults, confirmedReceipts, err := w.executeToolScript(execCtx, assignment, spec)
	switch {
	case err == nil:
		// The approval granted execution: move back to RUNNING, then commit
		// the checkpoint and complete exactly like a first run.
		resumed, err := w.transition(ctx, identity, version,
			runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING, "resume-running")
		if err != nil {
			return false, err
		}
		modelResults, err := w.executeModelScript(execCtx, assignment, spec)
		if err != nil {
			if keeper.Cancelled() {
				return true, nil
			}
			if fenceErr := keeper.FenceError(); fenceErr != nil {
				return false, fenceErr
			}
			return w.fail(ctx, identity, version, "model_script_failed", err)
		}
		committedVersion, err := w.commitCheckpoint(execCtx, assignment, resumed.GetAttemptVersion(),
			toolResults, modelResults, confirmedReceipts, goalDigest)
		if err != nil {
			if keeper.Cancelled() {
				return true, nil
			}
			if fenceErr := keeper.FenceError(); fenceErr != nil {
				return false, fenceErr
			}
			return false, err
		}
		if keeper.Cancelled() {
			return true, nil
		}
		document := map[string]any{
			"agentVersionRef": assignment.GetAgentVersionRef(), "attemptId": identity.GetAttemptId(),
			"goal": assignment.GetGoal(), "provider": ProviderName, "resumed": false,
		}
		if len(toolResults) > 0 {
			document["toolResults"] = toolResults
		}
		if len(modelResults) > 0 {
			document["modelResults"] = modelResults
		}
		resultDocument, err := json.Marshal(document)
		if err != nil {
			return false, err
		}
		if err := w.complete(ctx, assignment, committedVersion, resultDocument); err != nil {
			return false, err
		}
		return true, nil
	case errors.Is(err, errApprovalPending):
		// The human has not decided yet: stay parked and poll again.
		return false, nil
	default:
		// Rejected, expired or mismatched approvals surface as permission
		// failures; other gateway errors fail the attempt too. Cancellation
		// and fence breaks stop the worker without touching the attempt.
		if keeper.Cancelled() {
			return true, nil
		}
		if fenceErr := keeper.FenceError(); fenceErr != nil {
			return false, fenceErr
		}
		return w.fail(ctx, identity, version, "tool_script_failed", err)
	}
}

// commitCheckpoint persists the logical checkpoint state and commits it with
// bounded retry on transient transaction conflicts.
func (w *Worker) commitCheckpoint(ctx context.Context, assignment *runtimev1.Assignment, version int64,
	toolResults, modelResults []map[string]any, confirmedReceipts []string, goalDigest [sha256.Size]byte) (int64, error) {
	state, err := json.Marshal(checkpointState{
		GoalSHA256: hex.EncodeToString(goalDigest[:]), Step: "prepared",
		ToolResults: toolResults, ModelResults: modelResults, ConfirmedReceipts: confirmedReceipts,
	})
	if err != nil {
		return 0, err
	}
	stateArtifact, err := w.artifacts.Put(ctx, w.tenantID, checkpointMediaType, bytes.NewReader(state))
	if err != nil {
		return 0, fmt.Errorf("persist logical checkpoint: %w", err)
	}
	checkpointID, err := uuid.NewV7()
	if err != nil {
		return 0, fmt.Errorf("create checkpoint ID: %w", err)
	}
	var committed *runtimev1.CommitCheckpointResponse
	rpcCtx, cancel := rpcContext(ctx, controlRPCTimeout)
	defer cancel()
	err = w.retryUnavailable(rpcCtx, "commit logical checkpoint", func() error {
		committed, err = w.client.CommitCheckpoint(rpcCtx, &runtimev1.CommitCheckpointRequest{
			Identity: assignment.GetIdentity(), ExpectedAttemptVersion: version,
			IdempotencyKey: operationKey(assignment, "checkpoint-prepared"), CheckpointId: checkpointID.String(),
			AgentVersionRef: assignment.GetAgentVersionRef(), Provider: ProviderName, RuntimeAbi: RuntimeABI,
			SchemaVersion: CheckpointSchema, State: artifactProto(stateArtifact),
			ConfirmedReceiptIds: confirmedReceipts,
		})
		return err
	})
	if err != nil {
		return 0, err
	}
	return committed.GetAttemptVersion(), nil
}

// heartbeat renews the runtime lease and acknowledges a pending cancellation.
func (w *Worker) heartbeat(ctx context.Context, assignment *runtimev1.Assignment) (*runtimev1.HeartbeatResponse, error) {
	rpcCtx, cancel := rpcContext(ctx, controlRPCTimeout)
	heartbeat, err := w.client.Heartbeat(rpcCtx, &runtimev1.HeartbeatRequest{
		Identity: assignment.GetIdentity(), ExpectedLeaseVersion: assignment.GetLeaseVersion(),
		IdempotencyKey: operationKey(assignment, "heartbeat"), RequestedTtlSeconds: int64(w.heartbeatTTL / time.Second),
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("renew runtime lease: %w", err)
	}
	if heartbeat.GetCancelRequested() {
		ackCtx, ackCancel := rpcContext(ctx, controlRPCTimeout)
		_, err := w.client.AcknowledgeCancellation(ackCtx, &runtimev1.AcknowledgeCancellationRequest{
			Identity: assignment.GetIdentity(), ExpectedAttemptVersion: heartbeat.GetAttemptVersion(),
			IdempotencyKey: operationKey(assignment, "cancel"),
		})
		ackCancel()
		if err != nil {
			return nil, fmt.Errorf("acknowledge runtime cancellation: %w", err)
		}
	}
	return heartbeat, nil
}

// complete registers the durable result artifact and completes the attempt.
// Transient transaction conflicts are retried with bounded backoff: an
// attempt stuck in RUNNING cannot be re-polled, so completion must converge
// or fall back to lease-expiry recovery.
func (w *Worker) complete(ctx context.Context, assignment *runtimev1.Assignment, version int64, resultDocument []byte) error {
	resultArtifact, err := w.artifacts.Put(ctx, w.tenantID, resultMediaType, bytes.NewReader(resultDocument))
	if err != nil {
		return fmt.Errorf("persist runtime result: %w", err)
	}
	rpcCtx, cancel := rpcContext(ctx, controlRPCTimeout)
	defer cancel()
	return w.retryUnavailable(rpcCtx, "complete runtime attempt", func() error {
		_, err := w.client.CompleteAttempt(rpcCtx, &runtimev1.CompleteAttemptRequest{
			Identity: assignment.GetIdentity(), ExpectedAttemptVersion: version,
			IdempotencyKey: operationKey(assignment, "complete"), Result: artifactProto(resultArtifact),
		})
		return err
	})
}

// retryUnavailable retries a fenced RPC on transient transaction conflicts
// (Unavailable) with bounded exponential backoff. SERIALIZABLE operations
// (completion finalization) conflict under load, so the retry budget is
// generous; convergence or lease-expiry recovery follows.
func (w *Worker) retryUnavailable(ctx context.Context, operation string, fn func() error) error {
	var err error
	for attempt := 0; attempt < 6; attempt++ {
		err = fn()
		if err == nil || status.Code(err) != codes.Unavailable {
			return err
		}
		delay := time.Duration(1<<attempt) * 10 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%s: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}
	return fmt.Errorf("%s after retries: %w", operation, err)
}

// executeToolScript runs the workload-spec tool script through the Tool
// Gateway, returning the sanitized results and the confirmed receipt
// operations carried into the checkpoint envelope (ADR-007).
func (w *Worker) executeToolScript(ctx context.Context, assignment *runtimev1.Assignment, spec workload.Spec) ([]map[string]any, []string, error) {
	if len(spec.ToolCalls) == 0 {
		return nil, nil, nil
	}
	if w.toolGateway == nil {
		return nil, nil, fmt.Errorf("workload spec requests tools but no Tool Gateway is wired")
	}
	var results []map[string]any
	var receipts []string
	for _, call := range spec.ToolCalls {
		response, err := w.invokeTool(ctx, assignment, call, "")
		if err != nil {
			return nil, nil, fmt.Errorf("invoke tool %s: %w", call.ToolName, err)
		}
		switch response.GetOutcome() {
		case "EXECUTED", "REPLAYED":
			results = append(results, map[string]any{
				"tool": call.ToolName, "action": call.Action, "resource": call.Resource,
				"outcome": response.GetOutcome(), "result": json.RawMessage(response.GetResultJson()),
			})
			receipts = append(receipts, response.GetReceiptOperation())
		case "DENIED":
			return nil, nil, fmt.Errorf("tool %s denied by policy: %v", call.ToolName, response.GetDenyReasons())
		case "REQUIRES_APPROVAL":
			if pending := assignment.GetApprovalId(); pending != "" {
				// Resume mode: a human decided (or is deciding) this approval;
				// re-present it. A still-pending decision parks again; a
				// rejected or expired approval fails the attempt.
				resumed, err := w.invokeTool(ctx, assignment, call, pending)
				if err != nil {
					if status.Code(err) == codes.FailedPrecondition {
						return nil, nil, errApprovalPending
					}
					return nil, nil, fmt.Errorf("resume tool %s with approval: %w", call.ToolName, err)
				}
				if resumed.GetOutcome() == "EXECUTED" || resumed.GetOutcome() == "REPLAYED" {
					results = append(results, map[string]any{
						"tool": call.ToolName, "action": call.Action, "resource": call.Resource,
						"outcome": resumed.GetOutcome(), "result": json.RawMessage(resumed.GetResultJson()),
					})
					receipts = append(receipts, resumed.GetReceiptOperation())
					continue
				}
				return nil, nil, fmt.Errorf("tool %s returned %s after approval", call.ToolName, resumed.GetOutcome())
			}
			// First encounter: park the attempt until a human decides.
			return nil, nil, errApprovalRequired
		default:
			return nil, nil, fmt.Errorf("tool %s returned unexpected outcome %s", call.ToolName, response.GetOutcome())
		}
	}
	return results, receipts, nil
}

// executeModelScript runs the workload-spec model script through the Model
// Gateway after the tool script succeeds. Every call is opened (Begin) and
// finalized (Finish) with the pre-declared usage and the CAS version the
// gateway pinned at Begin. Finish settles exactly the pre-declared usage
// against the task budget (a deterministic script has no intermediate steps),
// so an exhausted reservation hard-stops the attempt; retries and replays
// converge through per-call idempotency keys.
func (w *Worker) executeModelScript(ctx context.Context, assignment *runtimev1.Assignment, spec workload.Spec) ([]map[string]any, error) {
	if len(spec.ModelCalls) == 0 {
		return nil, nil
	}
	if w.modelGateway == nil {
		return nil, fmt.Errorf("workload spec requests model calls but no Model Gateway is wired")
	}
	identity := assignment.GetIdentity()
	modelIdentity := &modelv1.AttemptIdentity{
		TenantId: identity.GetTenantId(), AttemptId: identity.GetAttemptId(),
		FencingToken: identity.GetFencingToken(),
	}
	var results []map[string]any
	for _, call := range spec.ModelCalls {
		beginCtx, beginCancel := rpcContext(ctx, gatewayRPCTimeout)
		began, err := w.modelGateway.Begin(beginCtx, &modelv1.BeginRequest{
			Identity: modelIdentity, TaskId: assignment.GetTaskId(), RunId: assignment.GetRunId(),
			AgentVersionRef: assignment.GetAgentVersionRef(), ModelRef: call.ModelRef,
			IdempotencyKey: call.IdempotencyKey,
		})
		beginCancel()
		if err != nil {
			return nil, fmt.Errorf("begin model %s: %w", call.ModelRef, err)
		}
		// Finalize: the gateway charges the pre-declared usage (exactly once,
		// per call idempotency) and computes cost from the pinned price
		// revision; content never crosses the boundary.
		finishCtx, finishCancel := rpcContext(ctx, gatewayRPCTimeout)
		finished, err := w.modelGateway.Finish(finishCtx, &modelv1.FinishRequest{
			Identity: modelIdentity, CallId: began.GetCallId(), ExpectedVersion: began.GetResourceVersion(),
			Status: string(store.ModelCallCompleted), InputTokens: call.InputTokens, OutputTokens: call.OutputTokens,
			FinishReason: "deterministic-script",
		})
		finishCancel()
		if err != nil {
			return nil, fmt.Errorf("finish model %s: %w", call.ModelRef, err)
		}
		results = append(results, map[string]any{
			"model": finished.GetModelRef(), "status": finished.GetStatus(),
			"inputTokens": finished.GetInputTokens(), "outputTokens": finished.GetOutputTokens(),
			"costUsd": finished.GetCostUsd(), "finishReason": finished.GetFinishReason(),
		})
	}
	return results, nil
}

// invokeTool performs one fenced gateway invocation for a script call,
// optionally re-presenting a pending approval.
func (w *Worker) invokeTool(ctx context.Context, assignment *runtimev1.Assignment, call workload.ToolCall, approvalID string) (*gatewayv1.InvokeToolResponse, error) {
	identity := assignment.GetIdentity()
	request := &gatewayv1.InvokeToolRequest{
		Identity: &gatewayv1.AttemptIdentity{
			TenantId: identity.GetTenantId(), AttemptId: identity.GetAttemptId(),
			FencingToken: identity.GetFencingToken(),
		},
		TaskId: assignment.GetTaskId(), RunId: assignment.GetRunId(),
		AgentVersionRef: assignment.GetAgentVersionRef(), ToolName: call.ToolName, ToolVersion: call.ToolVersion,
		Action: call.Action, Resource: call.Resource, ArgsJson: call.Args, IdempotencyKey: call.IdempotencyKey,
	}
	if approvalID != "" {
		request.ApprovalId = approvalID
	}
	invokeCtx, cancel := rpcContext(ctx, gatewayRPCTimeout)
	defer cancel()
	return w.toolGateway.InvokeTool(invokeCtx, request)
}

// mergeConfirmedResults attaches tool and model results recorded in the
// resume checkpoint's state to the result document after recovery.
func (w *Worker) mergeConfirmedResults(ctx context.Context, assignment *runtimev1.Assignment, document map[string]any) error {
	if assignment.GetResumeCheckpoint() == nil {
		return fmt.Errorf("no resume checkpoint to merge tool results from")
	}
	reference, err := artifactFromProto(assignment.GetResumeCheckpoint().GetState())
	if err != nil {
		return err
	}
	openCtx, cancelOpen := rpcContext(ctx, artifactRPCTimeout)
	reader, err := w.artifacts.Open(openCtx, w.tenantID, reference)
	cancelOpen()
	if err != nil {
		return fmt.Errorf("open checkpoint state: %w", err)
	}
	defer reader.Close()
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	var state checkpointState
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("decode checkpoint state: %w", err)
	}
	if len(state.ToolResults) > 0 {
		document["toolResults"] = state.ToolResults
	}
	if len(state.ModelResults) > 0 {
		document["modelResults"] = state.ModelResults
	}
	return nil
}

// fail marks the Attempt ATTEMPT_FAILED with a machine-readable code so
// deterministic script failures converge instead of spinning until lease
// expiry.
func (w *Worker) fail(ctx context.Context, identity *runtimev1.AttemptIdentity, version int64, code string, cause error) (bool, error) {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		rpcCtx, cancel := rpcContext(ctx, controlRPCTimeout)
		_, err := w.client.TransitionAttempt(rpcCtx, &runtimev1.TransitionAttemptRequest{
			Identity: identity, ExpectedAttemptVersion: version,
			IdempotencyKey: identity.GetAttemptId() + ":" + code,
			TargetPhase:    runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED,
			FailureCode:    code, FailureMessage: cause.Error(),
		})
		cancel()
		if err == nil {
			return true, nil
		}
		if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return false, fmt.Errorf("mark attempt failed (%s): %w", code, err)
		}
		current, refreshErr := attemptstate.Refresh(ctx, w.client, identity, controlRPCTimeout)
		if refreshErr != nil {
			return false, fmt.Errorf("mark attempt failed (%s): %w", code, refreshErr)
		}
		if current.Settled() {
			return true, nil
		}
		version = current.Version
	}
}

func (w *Worker) transition(ctx context.Context, identity *runtimev1.AttemptIdentity, version int64, phase runtimev1.AttemptPhase, operation string) (*runtimev1.TransitionAttemptResponse, error) {
	rpcCtx, cancel := rpcContext(ctx, controlRPCTimeout)
	defer cancel()
	response, err := w.client.TransitionAttempt(rpcCtx, &runtimev1.TransitionAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: identity.GetAttemptId() + ":" + operation, TargetPhase: phase,
	})
	if err != nil {
		return nil, fmt.Errorf("transition attempt to %s: %w", phase, err)
	}
	return response, nil
}

func (w *Worker) restoreCheckpoint(ctx context.Context, assignment *runtimev1.Assignment, goalDigest [sha256.Size]byte) error {
	checkpoint := assignment.GetResumeCheckpoint()
	if checkpoint.GetAgentVersionRef() != assignment.GetAgentVersionRef() || checkpoint.GetRuntimeClass() != assignment.GetRuntimeClass() ||
		checkpoint.GetProvider() != ProviderName || checkpoint.GetRuntimeAbi() != RuntimeABI || checkpoint.GetSchemaVersion() != CheckpointSchema {
		return fmt.Errorf("checkpoint is not compatible with reference runtime assignment")
	}
	reference, err := artifactFromProto(checkpoint.GetState())
	if err != nil {
		return err
	}
	openCtx, cancelOpen := rpcContext(ctx, artifactRPCTimeout)
	reader, err := w.artifacts.Open(openCtx, w.tenantID, reference)
	cancelOpen()
	if err != nil {
		return fmt.Errorf("open checkpoint state: %w", err)
	}
	defer reader.Close()
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	var state checkpointState
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("decode checkpoint state: %w", err)
	}
	if state.Step != "prepared" || !strings.EqualFold(state.GoalSHA256, hex.EncodeToString(goalDigest[:])) {
		return fmt.Errorf("checkpoint logical state does not match assigned task")
	}
	return nil
}

type checkpointState struct {
	GoalSHA256        string           `json:"goalSha256"`
	Step              string           `json:"step"`
	ToolResults       []map[string]any `json:"toolResults,omitempty"`
	ModelResults      []map[string]any `json:"modelResults,omitempty"`
	ConfirmedReceipts []string         `json:"confirmedReceipts,omitempty"`
}

func operationKey(assignment *runtimev1.Assignment, operation string) string {
	return assignment.GetIdentity().GetAttemptId() + ":" + operation
}

func artifactProto(reference store.ArtifactReference) *runtimev1.ArtifactReference {
	return &runtimev1.ArtifactReference{
		Uri: reference.URI, Sha256: reference.DigestHex(), SizeBytes: reference.SizeBytes, MediaType: reference.MediaType,
	}
}

func artifactFromProto(reference *runtimev1.ArtifactReference) (store.ArtifactReference, error) {
	var result store.ArtifactReference
	if reference == nil {
		return result, fmt.Errorf("checkpoint artifact reference is required")
	}
	digest, err := hex.DecodeString(reference.GetSha256())
	if err != nil || len(digest) != sha256.Size {
		return result, fmt.Errorf("checkpoint artifact digest is invalid")
	}
	copy(result.SHA256[:], digest)
	result.URI, result.SizeBytes, result.MediaType = reference.GetUri(), reference.GetSizeBytes(), reference.GetMediaType()
	return result, result.Validate()
}
