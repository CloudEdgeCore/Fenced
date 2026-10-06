//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/google/uuid"
)

func TestRuntimeCheckpointCompletionAndIdempotency(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "runtime-complete", 3)

	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1, ExpectedAttemptVersion: 1, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1, ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpointInput := kernelstore.CommitCheckpointInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: running.ResourceVersion, IdempotencyKey: "checkpoint-1", CheckpointID: uuid.New(),
		AgentVersionRef: "agent@1", Provider: "reference-go", RuntimeABI: "fenced.reference/v1",
		SchemaVersion: "state/v1", State: artifactReference("artifact://tenant-a/sha256/checkpoint", "checkpoint-state"),
		ConfirmedReceiptIDs: []string{"receipt-b", "receipt-a", "receipt-a"},
	}
	checkpoint, checkpointedAttempt, err := repository.CommitCheckpoint(ctx, checkpointInput)
	if err != nil {
		t.Fatalf("commit checkpoint: %v", err)
	}
	if checkpoint.Ordinal != 1 || checkpointedAttempt.ResourceVersion != running.ResourceVersion+1 || len(checkpoint.ConfirmedReceiptIDs) != 2 {
		t.Fatalf("unexpected checkpoint: %+v attempt=%+v", checkpoint, checkpointedAttempt)
	}
	var boundVersionID string
	if err := pool.QueryRow(ctx, `SELECT agent_version_id::text FROM checkpoints WHERE id = $1`, checkpoint.ID.String()).Scan(&boundVersionID); err != nil {
		t.Fatalf("read checkpoint version binding: %v", err)
	}
	if boundVersionID != assignment.Task.AgentVersionID.String() {
		t.Fatalf("checkpoint bound to %s, want %s", boundVersionID, assignment.Task.AgentVersionID)
	}
	retriedCheckpoint, retriedAttempt, err := repository.CommitCheckpoint(ctx, checkpointInput)
	if err != nil || retriedCheckpoint.ID != checkpoint.ID || retriedAttempt.ResourceVersion != checkpointedAttempt.ResourceVersion {
		t.Fatalf("idempotent checkpoint retry: checkpoint=%+v attempt=%+v err=%v", retriedCheckpoint, retriedAttempt, err)
	}
	conflict := checkpointInput
	conflict.SchemaVersion = "state/v2"
	if _, _, err := repository.CommitCheckpoint(ctx, conflict); !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("expected checkpoint idempotency conflict, got %v", err)
	}

	completion := kernelstore.CompleteAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: checkpointedAttempt.ResourceVersion, IdempotencyKey: "complete-1",
		Result: artifactReference("artifact://tenant-a/sha256/result", "durable-result"),
	}
	completed, err := repository.CompleteAttempt(ctx, completion)
	if err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	if completed.Attempt.Phase != domain.AttemptCompleted || completed.Run.Phase != domain.RunCompleted ||
		completed.Task.Phase != domain.TaskSucceeded || completed.Task.ResultRef != completion.Result.URI {
		t.Fatalf("completion did not converge atomically: %+v", completed)
	}
	retriedCompletion, err := repository.CompleteAttempt(ctx, completion)
	if err != nil || retriedCompletion.Task.ID != completed.Task.ID || retriedCompletion.Task.Phase != domain.TaskSucceeded {
		t.Fatalf("idempotent completion retry: %+v err=%v", retriedCompletion, err)
	}
	if _, err := repository.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1, ExpectedLeaseVersion: 1, TTL: time.Minute,
	}); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("completed owner retained a lease: %v", err)
	}
}

func TestExpiredRuntimeRecoversCheckpointWithHigherFence(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "runtime-recovery", 2)
	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1, ExpectedAttemptVersion: 1, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1, ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, _, err := repository.CommitCheckpoint(ctx, kernelstore.CommitCheckpointInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: running.ResourceVersion, IdempotencyKey: "recoverable-checkpoint", CheckpointID: uuid.New(),
		AgentVersionRef: "agent@1", Provider: "reference-go", RuntimeABI: "fenced.reference/v1",
		SchemaVersion: "state/v1", State: artifactReference("artifact://tenant-a/sha256/recovery", "recoverable-state"),
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(31 * time.Second)
	candidates, err := repository.ListExpiredAttempts(ctx, clock.Now(), 10)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("expired candidates=%+v err=%v", candidates, err)
	}
	recovered, err := repository.RecoverExpiredAttempt(ctx, kernelstore.RecoverExpiredAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		NewAttemptID: uuid.New(), NewLeaseID: uuid.New(), LeaseTTL: 30 * time.Second, MaxAttempts: 2,
	})
	if err != nil || !recovered.Retried || recovered.Lease.Attempt.FencingToken != 2 || recovered.Lease.Attempt.Ordinal != 2 {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	resumed, err := repository.GetRuntimeAssignment(ctx, "tenant-a", recovered.Lease.Attempt.ID, 2)
	if err != nil || resumed.ResumeCheckpoint == nil || resumed.ResumeCheckpoint.ID != checkpoint.ID {
		t.Fatalf("resumed assignment=%+v err=%v", resumed, err)
	}
	if _, err := repository.GetRuntimeAssignment(ctx, "tenant-a", assignment.Attempt.ID, 1); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("old owner was not fenced: %v", err)
	}
	secondStarting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: recovered.Lease.Attempt.ID, FencingToken: 2,
		ExpectedAttemptVersion: recovered.Lease.Attempt.ResourceVersion, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start recovered attempt: %v", err)
	}
	if _, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: recovered.Lease.Attempt.ID, FencingToken: 2,
		ExpectedAttemptVersion: secondStarting.ResourceVersion, To: domain.AttemptRunning,
	}); err != nil {
		t.Fatalf("run recovered attempt: %v", err)
	}

	clock.Advance(31 * time.Second)
	exhausted, err := repository.RecoverExpiredAttempt(ctx, kernelstore.RecoverExpiredAttemptInput{
		TenantID: "tenant-a", AttemptID: recovered.Lease.Attempt.ID, FencingToken: 2,
		NewAttemptID: uuid.New(), NewLeaseID: uuid.New(), LeaseTTL: 30 * time.Second, MaxAttempts: 2,
	})
	if err != nil || exhausted.Retried {
		t.Fatalf("exhausted recovery=%+v err=%v", exhausted, err)
	}
	task, err := repository.GetTask(ctx, "tenant-a", assignment.Task.ID)
	if err != nil || task.Phase != domain.TaskFailed {
		t.Fatalf("exhausted task=%+v err=%v", task, err)
	}
}

func TestWallTimeDeadlineStopsHealthyLeaseWithoutRetry(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "runtime-wall-stop", 3, 1)
	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: 1, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedLeaseVersion: 1, TTL: 5 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	candidates, err := repository.ListExpiredAttempts(ctx, clock.Now(), 10)
	if err != nil || len(candidates) != 1 || !candidates[0].DeadlineExceeded {
		t.Fatalf("deadline candidates=%+v err=%v", candidates, err)
	}
	stopped, err := repository.RecoverExpiredAttempt(ctx, kernelstore.RecoverExpiredAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		NewAttemptID: uuid.New(), NewLeaseID: uuid.New(), LeaseTTL: 30 * time.Second, MaxAttempts: 3,
		DeadlineExceeded: true,
	})
	if err != nil || stopped.Retried {
		t.Fatalf("wall stop=%+v err=%v", stopped, err)
	}
	task, err := repository.GetTask(ctx, "tenant-a", assignment.Task.ID)
	if err != nil || task.Phase != domain.TaskFailed {
		t.Fatalf("wall-stopped task=%+v err=%v", task, err)
	}
	if _, err := repository.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedLeaseVersion: 2, TTL: time.Minute,
	}); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("wall-stopped owner not fenced: %v", err)
	}
	var failures int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = 'tenant-a' AND aggregate_id = $1 AND event_type = 'TaskFailed'
		AND payload->>'failureCode' = 'WALL_TIME_EXCEEDED'`, task.ID.String()).Scan(&failures); err != nil || failures != 1 {
		t.Fatalf("wall-time evidence=%d err=%v", failures, err)
	}
}

func TestRunningTaskCancellationConvergesThroughRuntime(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "runtime-cancel", 3)
	cancelRequested, err := repository.RequestTaskCancellation(ctx, "tenant-a", assignment.Task.ID, assignment.Task.ResourceVersion)
	if err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	if cancelRequested.Phase != domain.TaskRunning || cancelRequested.CancelRequestedAt == nil {
		t.Fatalf("cancellation was not durably requested: %+v", cancelRequested)
	}
	current, err := repository.GetRuntimeAssignment(ctx, "tenant-a", assignment.Attempt.ID, 1)
	if err != nil || current.Attempt.Phase != domain.AttemptCancelRequested {
		t.Fatalf("cancelled assignment=%+v err=%v", current, err)
	}
	input := kernelstore.CancelAttemptInput{
		TenantID: "tenant-a", AttemptID: current.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: current.Attempt.ResourceVersion, IdempotencyKey: "cancel-ack-1",
	}
	cancelled, err := repository.AcknowledgeCancellation(ctx, input)
	if err != nil {
		t.Fatalf("acknowledge cancellation: %v", err)
	}
	if cancelled.Attempt.Phase != domain.AttemptCancelled || cancelled.Run.Phase != domain.RunCancelled || cancelled.Task.Phase != domain.TaskCancelled {
		t.Fatalf("cancellation did not converge atomically: %+v", cancelled)
	}
	// An orchestrator that read the task just before the runtime acknowledgement
	// receives a convergence conflict, not a fatal lifecycle error. Its next
	// read can replay cancellation idempotently against the terminal task.
	if _, err := repository.RequestTaskCancellation(ctx, "tenant-a", assignment.Task.ID, cancelRequested.ResourceVersion); !errors.Is(err, kernelstore.ErrVersionConflict) {
		t.Fatalf("stale cancellation replay error=%v, want version conflict", err)
	}
	replayedTask, err := repository.RequestTaskCancellation(ctx, "tenant-a", assignment.Task.ID, cancelled.Task.ResourceVersion)
	if err != nil || replayedTask.Phase != domain.TaskCancelled {
		t.Fatalf("terminal cancellation replay=%+v err=%v", replayedTask, err)
	}
	retried, err := repository.AcknowledgeCancellation(ctx, input)
	if err != nil || retried.Task.Phase != domain.TaskCancelled {
		t.Fatalf("idempotent cancellation retry=%+v err=%v", retried, err)
	}
}

func scheduleRuntimeTask(t *testing.T, ctx context.Context, repository *postgresstore.Store, key string, maxAttempts int, wallSeconds ...int) kernelstore.RuntimeAssignment {
	t.Helper()
	wall := 60
	if len(wallSeconds) > 0 {
		wall = wallSeconds[0]
	}
	publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"runtimeClassPolicy":{"allowed":["oci"]}}`)
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1", Goal: key,
		IdempotencyKey: key, Spec: []byte(`{
			"priority":70,"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":` + strconv.Itoa(wall) + `},
			"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1},
			"retryPolicy":{"maxAttempts":` + strconv.Itoa(maxAttempts) + `}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10), MaxToolCalls: 100,
		MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	if count, err := admission.NewController(repository, engine, testPolicyEngine(t), "admission-"+key, 10, time.Minute).Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("admission count=%d err=%v", count, err)
	}
	pools := staticPools{{
		ID: "pool-cn-1", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci", RuntimeInstanceID: "worker-cn-1",
		Region: "cn-east", Ready: true, AvailableCPU: 100_000, AvailableMemory: 1_000_000, AvailableLLMSlots: 1_000,
	}}
	if err := repository.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register fixture pools: %v", err)
	}
	if count, err := scheduler.NewController(repository, pools, "scheduler-"+key, 10, time.Minute, 30*time.Second).Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("scheduler count=%d err=%v", count, err)
	}
	assignment, err := repository.PollRuntimeAssignment(ctx, "tenant-a", "worker-cn-1")
	if err != nil || assignment.Task.ID != created.Task.ID {
		t.Fatalf("poll assignment=%+v err=%v", assignment, err)
	}
	if assignment.AgentVersion == nil || assignment.AgentVersion.Ref() != created.Task.AgentVersionRef ||
		len(assignment.AgentVersion.Spec) == 0 {
		t.Fatalf("assignment omitted immutable AgentVersion declaration: %+v", assignment.AgentVersion)
	}
	return assignment
}

func artifactReference(uri, content string) kernelstore.ArtifactReference {
	digest := sha256.Sum256([]byte(content))
	return kernelstore.ArtifactReference{URI: uri, SHA256: digest, SizeBytes: int64(len(content)), MediaType: "application/octet-stream"}
}
