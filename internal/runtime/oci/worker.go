package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/attemptstate"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/leasekeeper"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// controlRPCTimeout bounds every fenced control-plane RPC so a hung endpoint
// can never wedge the worker loop.
const controlRPCTimeout = 15 * time.Second

// Worker runs the fenced Runtime Protocol loop for one RuntimeInstance,
// delegating workload execution to an Executor. It mirrors the protocol
// choreography of the reference provider while remaining provider-agnostic:
// every mutation carries the AttemptIdentity fencing token and expected
// resource version, and a caller that loses a response must re-read the
// assignment before retrying (ADR-006).
type Worker struct {
	client            runtimev1.RuntimeControlServiceClient
	artifacts         ArtifactStore
	executor          Executor
	tenantID          string
	runtimeInstanceID string
	heartbeatTTL      time.Duration
	imageRef          string
	workspaceBytes    int64
	cpuQuotaMillis    int64
	memoryLimitMiB    int64
}

// NewWorker constructs a worker. imageRef is the digest-pinned OCI image that
// runs the published AgentVersion; resource limits are worker configuration
// until Admission passes them through the Assignment.
func NewWorker(client runtimev1.RuntimeControlServiceClient, artifacts ArtifactStore, executor Executor,
	tenantID, runtimeInstanceID string, heartbeatTTL time.Duration, imageRef string) *Worker {
	return &Worker{
		client: client, artifacts: artifacts, executor: executor,
		tenantID: tenantID, runtimeInstanceID: runtimeInstanceID, heartbeatTTL: heartbeatTTL, imageRef: imageRef,
	}
}

// WithResourceLimits sets the sandbox resource envelope for executions started
// by this worker.
func (w *Worker) WithResourceLimits(cpuQuotaMillis, memoryLimitMiB, workspaceBytes int64) *Worker {
	w.cpuQuotaMillis, w.memoryLimitMiB, w.workspaceBytes = cpuQuotaMillis, memoryLimitMiB, workspaceBytes
	return w
}

// RunOnce polls for one assignment and executes it to a terminal state.
// It returns processed=true when an assignment was consumed (completed,
// failed, or cancelled) and false when no assignment was available.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	polled, err := w.client.PollAssignment(ctx, &runtimev1.PollAssignmentRequest{
		TenantId: w.tenantID, RuntimeInstanceId: w.runtimeInstanceID,
	})
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
	identity, version := assignment.GetIdentity(), assignment.GetAttemptVersion()

	version, err = w.transition(ctx, identity, version, runtimev1.AttemptPhase_ATTEMPT_PHASE_STARTING, "", "")
	if err != nil {
		return false, err
	}
	version, err = w.transition(ctx, identity, version, runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING, "", "")
	if err != nil {
		return false, err
	}

	heartbeat, err := w.client.Heartbeat(ctx, &runtimev1.HeartbeatRequest{
		Identity: identity, ExpectedLeaseVersion: assignment.GetLeaseVersion(),
		IdempotencyKey: operationKey(assignment, "heartbeat"), RequestedTtlSeconds: int64(w.heartbeatTTL / time.Second),
	})
	if err != nil {
		return false, fmt.Errorf("renew runtime lease: %w", err)
	}
	if heartbeat.GetCancelRequested() {
		_, err := w.client.AcknowledgeCancellation(ctx, &runtimev1.AcknowledgeCancellationRequest{
			Identity: identity, ExpectedAttemptVersion: heartbeat.GetAttemptVersion(),
			IdempotencyKey: operationKey(assignment, "cancel"),
		})
		if err != nil {
			return false, fmt.Errorf("acknowledge runtime cancellation: %w", err)
		}
		return true, nil
	}

	goalDigest := sha256.Sum256([]byte(assignment.GetGoal()))
	if assignment.GetResumeCheckpoint() != nil {
		if err := w.verifyCheckpoint(ctx, assignment, goalDigest); err != nil {
			return w.fail(ctx, identity, version, "incompatible_checkpoint", err)
		}
	}

	// Image pin enforcement (ADR-010): the assignment's workload spec may pin
	// the OCI image; the worker runs exactly that reference and refuses
	// anything else. The configured image is the operator's binding — when
	// both exist they must agree (the configured ref may itself be
	// digest-pinned, so both the bare ref and the canonical ref@digest form
	// are accepted). The spec also carries the decided sandbox limits
	// (hardening checklist §4.1): container classes must declare explicit
	// CPU/memory/workspace values, and the worker enforces them
	// independently of Admission.
	imageRef := w.imageRef
	cpuQuota, memoryMiB, workspaceBytes := w.cpuQuotaMillis, w.memoryLimitMiB, w.workspaceBytes
	workloadSpec, decodeErr := workload.Decode(assignment.GetWorkloadSpecJson())
	if decodeErr != nil {
		return w.fail(ctx, identity, version, "workload_spec_invalid", decodeErr)
	}
	if workloadSpec.Image != nil {
		if err := workloadSpec.Image.Validate(); err != nil {
			return w.fail(ctx, identity, version, "image_pin_invalid", err)
		}
		if w.imageRef != "" && w.imageRef != workloadSpec.Image.Ref && w.imageRef != workloadSpec.Image.Canonical() {
			return w.fail(ctx, identity, version, "image_pin_mismatch",
				fmt.Errorf("configured image %q does not match the spec pin %q", w.imageRef, workloadSpec.Image.Canonical()))
		}
		imageRef = workloadSpec.Image.Ref
	} else if !strings.Contains(assignment.GetRuntimeClass(), "oci") {
		return w.fail(ctx, identity, version, "image_required",
			fmt.Errorf("runtime class %q requires a digest-pinned image in the workload spec", assignment.GetRuntimeClass()))
	}
	if workloadSpec.Placement.CPU > 0 {
		cpuQuota = workloadSpec.Placement.CPU
	}
	if workloadSpec.Placement.Memory > 0 {
		memoryMiB = workloadSpec.Placement.Memory
	}
	if workloadSpec.Placement.WorkspaceBytes > 0 {
		workspaceBytes = workloadSpec.Placement.WorkspaceBytes
	}
	if strings.Contains(assignment.GetRuntimeClass(), "oci") &&
		(cpuQuota <= 0 || memoryMiB <= 0 || workspaceBytes <= 0) {
		return w.fail(ctx, identity, version, "resource_limits_required",
			fmt.Errorf("container runtime class %q requires explicit cpuMillis, memoryMiB and workspaceBytes in the workload spec", assignment.GetRuntimeClass()))
	}
	var command []string
	if workloadSpec.Runtime != nil {
		if err := workloadSpec.Runtime.ValidateCommand(); err != nil {
			return w.fail(ctx, identity, version, "runtime_command_invalid", err)
		}
		command = append([]string(nil), workloadSpec.Runtime.Command...)
	}

	execSpec := ExecutionSpec{
		TenantID: w.tenantID, AttemptID: identity.GetAttemptId(), AgentVersionRef: assignment.GetAgentVersionRef(),
		WorkloadSpecJSON: assignment.GetWorkloadSpecJson(), ImageRef: imageRef, Command: command,
		WorkspaceBytes: workspaceBytes, CPUQuotaMillis: cpuQuota, MemoryLimitMiB: memoryMiB,
		RuntimeClass: assignment.GetRuntimeClass(), RuntimePoolID: assignment.GetRuntimePoolId(),
		RuntimeInstanceID: w.runtimeInstanceID,
		// Bounded output spooling (hardening checklist §4.4): container
		// stdout/stderr land in the artifact store, never in kernel messages.
		OutputSpooler: w,
	}
	// Keep the runtime lease alive while the sandbox runs: a container that
	// outlives the lease TTL must never be fenced mid-execution. Cancellation
	// or a broken fence cancels the execution context and stops the run.
	keeper, execCtx := leasekeeper.Start(ctx, leasekeeper.Options{
		Client: w.client, Identity: identity, AttemptID: identity.GetAttemptId(),
		HeartbeatTTL: w.heartbeatTTL, RPCTimeout: controlRPCTimeout,
	}, heartbeat.GetLeaseVersion(), heartbeat.GetAttemptVersion())
	defer keeper.Stop()
	execution, err := w.executor.Prepare(execCtx, execSpec)
	if err != nil {
		if keeper.Cancelled() {
			return true, nil
		}
		if fenceErr := keeper.FenceError(); fenceErr != nil {
			return false, fenceErr
		}
		return w.fail(ctx, identity, version, "execution_prepare_failed", err)
	}
	result, waitErr := execution.Wait(execCtx)
	destroyErr := w.executor.Destroy(execCtx, execution)
	if waitErr != nil {
		if keeper.Cancelled() {
			// Cancellation requested while the sandbox ran: the keeper
			// acknowledged it, so the kernel finalizes the cancellation.
			return true, nil
		}
		if fenceErr := keeper.FenceError(); fenceErr != nil {
			return false, fenceErr
		}
		if destroyErr != nil {
			waitErr = fmt.Errorf("%w (destroy failed: %v)", waitErr, destroyErr)
		}
		return w.fail(ctx, identity, version, "execution_failed", waitErr)
	}
	if destroyErr != nil {
		return false, fmt.Errorf("destroy sandboxed execution: %w", destroyErr)
	}
	if keeper.Cancelled() {
		// The cancellation won after the sandbox exited: skip completion.
		return true, nil
	}
	if result.ExitCode != 0 {
		code := result.FailureCode
		if code == "" {
			code = fmt.Sprintf("exit_code_%d", result.ExitCode)
		}
		return w.fail(ctx, identity, version, code, fmt.Errorf("workload exited with code %d", result.ExitCode))
	}

	state, err := json.Marshal(checkpointState{
		GoalSHA256: hex.EncodeToString(goalDigest[:]), Step: "executed", UsageMillis: result.UsageMillis,
	})
	if err != nil {
		return false, err
	}
	stateArtifact, err := w.artifacts.Put(ctx, w.tenantID, checkpointMediaType, bytes.NewReader(state))
	if err != nil {
		return false, fmt.Errorf("persist logical checkpoint: %w", err)
	}
	checkpointID, err := uuid.NewV7()
	if err != nil {
		return false, fmt.Errorf("create checkpoint ID: %w", err)
	}
	committed, err := w.client.CommitCheckpoint(ctx, &runtimev1.CommitCheckpointRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: operationKey(assignment, "checkpoint-executed"), CheckpointId: checkpointID.String(),
		AgentVersionRef: assignment.GetAgentVersionRef(), Provider: ProviderName, RuntimeAbi: RuntimeABI,
		SchemaVersion: CheckpointSchema, State: artifactProto(stateArtifact),
	})
	if err != nil {
		return false, fmt.Errorf("commit logical checkpoint: %w", err)
	}
	version = committed.GetAttemptVersion()

	resultDocument, err := json.Marshal(map[string]any{
		"agentVersionRef": assignment.GetAgentVersionRef(), "attemptId": identity.GetAttemptId(),
		"goal": assignment.GetGoal(), "provider": ProviderName,
		"runtimeClass": assignment.GetRuntimeClass(), "resumed": assignment.GetResumeCheckpoint() != nil,
		"exitCode": result.ExitCode, "usageMillis": result.UsageMillis,
		"stdoutRef": stdoutReference(result), "stdoutTruncated": result.StdoutTruncated,
	})
	if err != nil {
		return false, err
	}
	resultArtifact, err := w.artifacts.Put(ctx, w.tenantID, resultMediaType, bytes.NewReader(resultDocument))
	if err != nil {
		return false, fmt.Errorf("persist runtime result: %w", err)
	}
	if _, err := w.client.CompleteAttempt(ctx, &runtimev1.CompleteAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: operationKey(assignment, "complete"), Result: artifactProto(resultArtifact),
	}); err != nil {
		return false, fmt.Errorf("complete runtime attempt: %w", err)
	}
	return true, nil
}

// fail marks the Attempt ATTEMPT_FAILED with a machine-readable code, mirroring
// ADR-007: the recovery controller decides retry vs. terminal failure.
func (w *Worker) fail(ctx context.Context, identity *runtimev1.AttemptIdentity, version int64, code string, cause error) (bool, error) {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		if _, err := w.transition(ctx, identity, version, runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED, code, cause.Error()); err == nil {
			return true, nil
		} else if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return false, fmt.Errorf("mark attempt failed (%s): %w", code, err)
		}
		current, err := attemptstate.Refresh(ctx, w.client, identity, controlRPCTimeout)
		if err != nil {
			return false, fmt.Errorf("mark attempt failed (%s): %w", code, err)
		}
		if current.Settled() {
			return true, nil
		}
		version = current.Version
	}
}

func (w *Worker) transition(ctx context.Context, identity *runtimev1.AttemptIdentity, version int64,
	phase runtimev1.AttemptPhase, failureCode, failureMessage string) (int64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	response, err := w.client.TransitionAttempt(rpcCtx, &runtimev1.TransitionAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: identity.GetAttemptId() + ":" + strings.ToLower(phase.String()),
		TargetPhase:    phase, FailureCode: failureCode, FailureMessage: failureMessage,
	})
	if err != nil {
		return 0, fmt.Errorf("transition attempt to %s: %w", phase, err)
	}
	return response.GetAttemptVersion(), nil
}

// verifyCheckpoint enforces ADR-007 envelope compatibility before resuming:
// Provider, Runtime ABI, Checkpoint schema and AgentVersion must match the
// assignment, and the logical state must belong to the same goal.
func (w *Worker) verifyCheckpoint(ctx context.Context, assignment *runtimev1.Assignment, goalDigest [sha256.Size]byte) error {
	checkpoint := assignment.GetResumeCheckpoint()
	if checkpoint.GetAgentVersionRef() != assignment.GetAgentVersionRef() ||
		checkpoint.GetRuntimeClass() != assignment.GetRuntimeClass() ||
		checkpoint.GetProvider() != ProviderName ||
		checkpoint.GetRuntimeAbi() != RuntimeABI ||
		checkpoint.GetSchemaVersion() != CheckpointSchema {
		return fmt.Errorf("checkpoint is not compatible with OCI/gVisor runtime assignment")
	}
	reference, err := artifactFromProto(checkpoint.GetState())
	if err != nil {
		return err
	}
	reader, err := w.artifacts.Open(ctx, w.tenantID, reference)
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
	if state.Step != "executed" || !strings.EqualFold(state.GoalSHA256, hex.EncodeToString(goalDigest[:])) {
		return fmt.Errorf("checkpoint logical state does not match assigned task")
	}
	return nil
}

type checkpointState struct {
	GoalSHA256  string `json:"goalSha256"`
	Step        string `json:"step"`
	UsageMillis int64  `json:"usageMillis,omitempty"`
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

// Spool implements the OutputSpooler contract (hardening checklist §4.4):
// bounded workload output is persisted to the artifact store.
func (w *Worker) Spool(ctx context.Context, tenantID, attemptID, mediaType string, reader io.Reader) (store.ArtifactReference, error) {
	return w.artifacts.Put(ctx, tenantID, mediaType, reader)
}

// stdoutReference renders the spooled stdout artifact URI for the result
// document, or an empty string when the execution produced no spool.
func stdoutReference(result RunResult) string {
	if result.Stdout == nil {
		return ""
	}
	return result.Stdout.URI
}
