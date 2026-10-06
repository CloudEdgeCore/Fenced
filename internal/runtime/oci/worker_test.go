package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	runtimecontrol "github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorkerCompletesAssignmentThroughRuntimeProtocol(t *testing.T) {
	repository := newFakeRuntimeStore()
	executor := &fakeExecutor{result: RunResult{ExitCode: 0, UsageMillis: 42}}
	worker := newTestWorker(t, repository, executor, "worker-1")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptCompleted || repository.task.Phase != domain.TaskSucceeded ||
		repository.checkpoint == nil || repository.task.ResultRef == "" {
		t.Fatalf("worker did not complete execution: attempt=%+v task=%+v checkpoint=%+v", repository.attempt, repository.task, repository.checkpoint)
	}
	if len(executor.prepared) != 1 || len(executor.destroyed) != 1 {
		t.Fatalf("executor lifecycle: prepared=%v destroyed=%v", executor.prepared, executor.destroyed)
	}
	if spec := executor.prepared[0]; spec.AttemptID != repository.attempt.ID.String() ||
		spec.AgentVersionRef != repository.task.AgentVersionRef || spec.ImageRef != "example.com/agent@sha256:abcd" {
		t.Fatalf("execution spec does not match assignment: %+v", spec)
	}
	if repository.checkpoint.Provider != ProviderName || repository.checkpoint.RuntimeABI != RuntimeABI ||
		repository.checkpoint.SchemaVersion != CheckpointSchema {
		t.Fatalf("checkpoint envelope is not provider-compatible: %+v", repository.checkpoint)
	}

	processed, err = worker.RunOnce(context.Background())
	if err != nil || processed {
		t.Fatalf("completed assignment was polled again: %v, %v", processed, err)
	}
}

func TestWorkerFailsAttemptOnPrepareError(t *testing.T) {
	repository := newFakeRuntimeStore()
	executor := &fakeExecutor{prepareErr: errors.New("runsc unavailable")}
	worker := newTestWorker(t, repository, executor, "worker-1")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptFailed {
		t.Fatalf("attempt phase = %s, want ATTEMPT_FAILED", repository.attempt.Phase)
	}
	if repository.attempt.FailureCode != "execution_prepare_failed" {
		t.Fatalf("failure code = %q, want execution_prepare_failed", repository.attempt.FailureCode)
	}
	if repository.task.Phase != domain.TaskRunning || repository.checkpoint != nil {
		t.Fatalf("task must stay non-terminal with no checkpoint: task=%s checkpoint=%+v", repository.task.Phase, repository.checkpoint)
	}
}

func TestWorkerFailsAttemptOnNonZeroExit(t *testing.T) {
	repository := newFakeRuntimeStore()
	executor := &fakeExecutor{result: RunResult{ExitCode: 7, UsageMillis: 100}}
	worker := newTestWorker(t, repository, executor, "worker-1")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptFailed || repository.attempt.FailureCode != "exit_code_7" {
		t.Fatalf("attempt = %s (%s), want ATTEMPT_FAILED exit_code_7", repository.attempt.Phase, repository.attempt.FailureCode)
	}
	if len(executor.destroyed) != 1 {
		t.Fatalf("sandbox must be destroyed after exit: %v", executor.destroyed)
	}
}

// TestWorkerEnforcesImagePinMismatch proves the worker refuses to run an
// image that does not match the assignment's digest pin (ADR-010): the
// attempt fails with image_pin_mismatch and nothing is prepared.
func TestWorkerEnforcesImagePinMismatch(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"placement":{"runtimeClasses":["oci"]},
		"image":{"ref":"example.com/evil","digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}`)
	executor := &fakeExecutor{result: RunResult{ExitCode: 0}}
	worker := newTestWorker(t, repository, executor, "worker-1")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptFailed || repository.attempt.FailureCode != "image_pin_mismatch" {
		t.Fatalf("attempt = %s (%s), want ATTEMPT_FAILED image_pin_mismatch", repository.attempt.Phase, repository.attempt.FailureCode)
	}
	if len(executor.prepared) != 0 {
		t.Fatalf("executor prepared %d executions for a mismatched pin", len(executor.prepared))
	}
}

// TestWorkerRunsPinnedImageWhenItMatches proves a matching spec pin is
// honored and reaches the executor, in both the bare-ref and canonical
// ref@digest configured forms.
func TestWorkerRunsPinnedImageWhenItMatches(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, test := range []struct {
		name     string
		imageRef string
	}{
		{"bare ref", "example.com/agent"},
		{"canonical ref", "example.com/agent@" + digest},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeRuntimeStore()
			repository.task.Spec = []byte(`{"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":1048576,"llmConcurrency":1},
				"image":{"ref":"example.com/agent","digest":"` + digest + `"},"runtime":{"command":["/bin/agent","serve"]}}`)
			executor := &fakeExecutor{result: RunResult{ExitCode: 0, UsageMillis: 7}}
			worker := newTestWorkerWithImage(t, repository, executor, "worker-1", test.imageRef)

			processed, err := worker.RunOnce(context.Background())
			if err != nil || !processed {
				t.Fatalf("RunOnce() = %v, %v", processed, err)
			}
			if len(executor.prepared) != 1 {
				t.Fatalf("executor prepared %d executions, want 1", len(executor.prepared))
			}
			if executor.prepared[0].ImageRef != "example.com/agent" {
				t.Fatalf("execution spec image = %q, want the spec pin ref", executor.prepared[0].ImageRef)
			}
			if !slices.Equal(executor.prepared[0].Command, []string{"/bin/agent", "serve"}) {
				t.Fatalf("execution command = %q, want preserved argv", executor.prepared[0].Command)
			}
		})
	}
}

// TestWorkerRejectsMalformedImagePin proves a malformed pin fails closed
// before any execution.
func TestWorkerRejectsMalformedImagePin(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"placement":{"runtimeClasses":["oci"]},
		"image":{"ref":"example.com/agent","digest":"md5:abc"}}`)
	executor := &fakeExecutor{result: RunResult{ExitCode: 0}}
	worker := newTestWorker(t, repository, executor, "worker-1")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptFailed || repository.attempt.FailureCode != "image_pin_invalid" {
		t.Fatalf("attempt = %s (%s), want ATTEMPT_FAILED image_pin_invalid", repository.attempt.Phase, repository.attempt.FailureCode)
	}
	if len(executor.prepared) != 0 {
		t.Fatalf("executor prepared %d executions for a malformed pin", len(executor.prepared))
	}
}

// TestWorkerRejectsMissingContainerLimits proves the worker enforces the
// container-class limit requirement independently of Admission: a task with
// no explicit cpu/memory/workspace never reaches the executor.
func TestWorkerRejectsMissingContainerLimits(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":0,"workspaceBytes":1048576,"llmConcurrency":1}}`)
	executor := &fakeExecutor{result: RunResult{ExitCode: 0}}
	worker := newTestWorker(t, repository, executor, "worker-1")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptFailed || repository.attempt.FailureCode != "resource_limits_required" {
		t.Fatalf("attempt = %s (%s), want ATTEMPT_FAILED resource_limits_required", repository.attempt.Phase, repository.attempt.FailureCode)
	}
	if len(executor.prepared) != 0 {
		t.Fatalf("executor prepared %d executions without limits", len(executor.prepared))
	}
}

// TestWorkerUsesSpecDrivenLimits proves the decided sandbox limits come from
// the workload spec, overriding the operator's configured defaults.
func TestWorkerUsesSpecDrivenLimits(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":250,"memoryMiB":512,"workspaceBytes":4194304,"llmConcurrency":1}}`)
	executor := &fakeExecutor{result: RunResult{ExitCode: 0, UsageMillis: 9}}
	worker := newTestWorkerWithImage(t, repository, executor, "worker-1", "example.com/agent@sha256:abcd")

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if len(executor.prepared) != 1 {
		t.Fatalf("executor prepared %d executions, want 1", len(executor.prepared))
	}
	prepared := executor.prepared[0]
	if prepared.CPUQuotaMillis != 250 || prepared.MemoryLimitMiB != 512 || prepared.WorkspaceBytes != 4194304 {
		t.Fatalf("spec-driven limits = %d/%d/%d, want 250/512/4194304", prepared.CPUQuotaMillis, prepared.MemoryLimitMiB, prepared.WorkspaceBytes)
	}
	if prepared.OutputSpooler == nil {
		t.Fatal("execution spec must carry the worker's output spooler")
	}
}

// TestWorkerSpoolsStdoutIntoResult proves spooled stdout is recorded in the
// result document (hardening checklist §4.4).
func TestWorkerSpoolsStdoutIntoResult(t *testing.T) {
	repository := newFakeRuntimeStore()
	executor := &fakeExecutor{result: RunResult{
		ExitCode: 0, UsageMillis: 5,
		Stdout: &store.ArtifactReference{
			URI: "artifact://tenant-a/sha256/ab", SHA256: [32]byte{0xab}, SizeBytes: 3, MediaType: "application/vnd.fenced.stdout+octet-stream",
		}, StdoutTruncated: false,
	}}
	base, err := artifact.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}
	artifacts := &recordingArtifacts{ArtifactStore: base}
	worker := newTestWorkerWithArtifacts(t, repository, executor, "worker-1", "example.com/agent@sha256:abcd", artifacts)

	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	var document map[string]any
	if err := json.Unmarshal(artifacts.lastPut(), &document); err != nil {
		t.Fatalf("decode result document: %v", err)
	}
	if document["stdoutRef"] != "artifact://tenant-a/sha256/ab" || document["stdoutTruncated"] != false {
		t.Fatalf("result document = %+v", document)
	}
}

func newTestWorker(t *testing.T, repository store.RuntimeStore, executor Executor, instanceID string) *Worker {
	t.Helper()
	return newTestWorkerWithImage(t, repository, executor, instanceID, "example.com/agent@sha256:abcd")
}

func newTestWorkerWithImage(t *testing.T, repository store.RuntimeStore, executor Executor, instanceID, imageRef string) *Worker {
	t.Helper()
	return newTestWorkerWithArtifacts(t, repository, executor, instanceID, imageRef, nil)
}

func newTestWorkerWithArtifacts(t *testing.T, repository store.RuntimeStore, executor Executor, instanceID, imageRef string, artifacts ArtifactStore) *Worker {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	runtimev1.RegisterRuntimeControlServiceServer(server, runtimecontrol.NewService(repository, "tenant-a", time.Minute))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if artifacts == nil {
		artifacts, err = artifact.NewFilesystem(t.TempDir(), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
	}
	return NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifacts, executor,
		"tenant-a", instanceID, 30*time.Second, imageRef)
}

// recordingArtifacts captures every Put so tests can assert the exact bytes
// the worker persists (e.g. the result document).
type recordingArtifacts struct {
	ArtifactStore
	mu   sync.Mutex
	puts [][]byte
}

func (r *recordingArtifacts) Put(ctx context.Context, tenantID, mediaType string, source io.Reader) (store.ArtifactReference, error) {
	content, err := io.ReadAll(source)
	if err != nil {
		return store.ArtifactReference{}, err
	}
	r.mu.Lock()
	r.puts = append(r.puts, content)
	r.mu.Unlock()
	return r.ArtifactStore.Put(ctx, tenantID, mediaType, bytes.NewReader(content))
}

func (r *recordingArtifacts) lastPut() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.puts) == 0 {
		return nil
	}
	return r.puts[len(r.puts)-1]
}

// fakeExecutor records the lifecycle and returns scripted outcomes.
type fakeExecutor struct {
	prepared   []ExecutionSpec
	destroyed  []string
	prepareErr error
	waitErr    error
	result     RunResult
}

func (f *fakeExecutor) Prepare(_ context.Context, spec ExecutionSpec) (Execution, error) {
	f.prepared = append(f.prepared, spec)
	if f.prepareErr != nil {
		return nil, f.prepareErr
	}
	return &fakeExecution{executor: f, id: "fenced-" + spec.AttemptID}, nil
}

func (f *fakeExecutor) Destroy(_ context.Context, execution Execution) error {
	f.destroyed = append(f.destroyed, execution.ID())
	return nil
}

type fakeExecution struct {
	executor *fakeExecutor
	id       string
}

func (e *fakeExecution) ID() string { return e.id }

func (e *fakeExecution) Wait(context.Context) (RunResult, error) {
	if e.executor.waitErr != nil {
		return RunResult{}, e.executor.waitErr
	}
	return e.executor.result, nil
}

// fakeRuntimeStore implements the store methods the Runtime Protocol service
// touches by embedding the interface; any other method panics on the nil
// embedded value, which is a test failure, not a production path.
type fakeRuntimeStore struct {
	store.RuntimeStore
	task       store.Task
	run        store.Run
	attempt    store.Attempt
	lease      store.Lease
	checkpoint *store.Checkpoint
}

func newFakeRuntimeStore() *fakeRuntimeStore {
	taskID, runID, attemptID := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	return &fakeRuntimeStore{
		task: store.Task{ID: taskID, TenantID: "tenant-a", AgentVersionRef: "agent:v1", Goal: "deterministic result",
			Spec:  []byte(`{"retryPolicy":{"maxAttempts":3},"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":1048576,"llmConcurrency":1}}`),
			Phase: domain.TaskRunning, ActiveRunID: &runID, ResourceVersion: 3},
		run: store.Run{ID: runID, TenantID: "tenant-a", TaskID: taskID, Phase: domain.RunRunning,
			ActiveAttemptID: &attemptID, CurrentFencingToken: 1, ResourceVersion: 1},
		attempt: store.Attempt{ID: attemptID, TenantID: "tenant-a", RunID: runID, Phase: domain.AttemptPlaced,
			RuntimeClass: "oci", RuntimePoolID: "pool-1", RuntimeInstanceID: "worker-1", FencingToken: 1, ResourceVersion: 1},
		lease: store.Lease{ID: uuid.New(), TenantID: "tenant-a", RunID: runID, AttemptID: attemptID,
			FencingToken: 1, ResourceVersion: 1, AcquiredAt: now, HeartbeatAt: now, ExpiresAt: now.Add(time.Minute)},
	}
}

func (f *fakeRuntimeStore) PollRuntimeAssignment(_ context.Context, tenantID, instanceID string) (store.RuntimeAssignment, error) {
	if tenantID != f.task.TenantID || instanceID != f.attempt.RuntimeInstanceID || f.attempt.Phase != domain.AttemptPlaced {
		return store.RuntimeAssignment{}, store.ErrNoAssignment
	}
	return f.assignment(), nil
}

func (f *fakeRuntimeStore) GetRuntimeAssignment(_ context.Context, tenantID string, attemptID uuid.UUID, token int64) (store.RuntimeAssignment, error) {
	if tenantID != f.task.TenantID || attemptID != f.attempt.ID || token != f.attempt.FencingToken {
		return store.RuntimeAssignment{}, store.ErrFenced
	}
	return f.assignment(), nil
}

func (f *fakeRuntimeStore) assignment() store.RuntimeAssignment {
	return store.RuntimeAssignment{Task: f.task, Run: f.run, Attempt: f.attempt, Lease: f.lease, ResumeCheckpoint: f.checkpoint}
}

func (f *fakeRuntimeStore) TransitionAttempt(_ context.Context, input store.TransitionAttemptInput) (store.Attempt, error) {
	if input.AttemptID != f.attempt.ID || input.FencingToken != f.attempt.FencingToken {
		return store.Attempt{}, store.ErrFenced
	}
	if input.ExpectedAttemptVersion != f.attempt.ResourceVersion {
		return store.Attempt{}, store.ErrVersionConflict
	}
	if err := domain.ValidateAttemptTransition(f.attempt.Phase, input.To); err != nil {
		return store.Attempt{}, errors.Join(store.ErrInvalidTransition, err)
	}
	f.attempt.Phase = input.To
	f.attempt.FailureCode, f.attempt.FailureMessage = input.FailureCode, input.FailureMessage
	f.attempt.ResourceVersion++
	return f.attempt, nil
}

func (f *fakeRuntimeStore) HeartbeatLease(_ context.Context, input store.HeartbeatLeaseInput) (store.Lease, error) {
	if input.FencingToken != f.lease.FencingToken || input.ExpectedLeaseVersion != f.lease.ResourceVersion {
		return store.Lease{}, store.ErrFenced
	}
	f.lease.ResourceVersion++
	f.lease.ExpiresAt = time.Now().Add(input.TTL)
	return f.lease, nil
}

func (f *fakeRuntimeStore) GetHeartbeatStatus(_ context.Context, tenantID string, attemptID uuid.UUID, token int64) (store.HeartbeatStatus, error) {
	if tenantID != f.task.TenantID || attemptID != f.attempt.ID || token != f.attempt.FencingToken {
		return store.HeartbeatStatus{}, store.ErrFenced
	}
	return store.HeartbeatStatus{CancelRequested: f.task.CancelRequestedAt != nil, AttemptVersion: f.attempt.ResourceVersion}, nil
}

func (f *fakeRuntimeStore) CommitCheckpoint(_ context.Context, input store.CommitCheckpointInput) (store.Checkpoint, store.Attempt, error) {
	if input.ExpectedAttemptVersion != f.attempt.ResourceVersion || input.FencingToken != f.attempt.FencingToken {
		return store.Checkpoint{}, store.Attempt{}, store.ErrVersionConflict
	}
	normalized, digest, err := input.Normalize()
	if err != nil {
		return store.Checkpoint{}, store.Attempt{}, err
	}
	checkpoint := store.Checkpoint{ID: normalized.CheckpointID, TenantID: normalized.TenantID, RunID: f.run.ID,
		AttemptID: f.attempt.ID, Ordinal: 1, FencingToken: input.FencingToken, AgentVersionRef: input.AgentVersionRef,
		RuntimeClass: f.attempt.RuntimeClass, Provider: input.Provider, RuntimeABI: input.RuntimeABI,
		SchemaVersion: input.SchemaVersion, State: input.State, EnvelopeSHA256: digest, CreatedAt: time.Now().UTC()}
	f.checkpoint = &checkpoint
	f.attempt.ResourceVersion++
	return checkpoint, f.attempt, nil
}

func (f *fakeRuntimeStore) CompleteAttempt(_ context.Context, input store.CompleteAttemptInput) (store.CompleteAttemptResult, error) {
	if input.ExpectedAttemptVersion != f.attempt.ResourceVersion || input.FencingToken != f.attempt.FencingToken {
		return store.CompleteAttemptResult{}, store.ErrVersionConflict
	}
	f.attempt.Phase = domain.AttemptCompleted
	f.attempt.ResourceVersion++
	f.run.Phase, f.run.ResourceVersion, f.run.ResultRef = domain.RunCompleted, f.run.ResourceVersion+1, input.Result.URI
	f.task.Phase, f.task.ResourceVersion, f.task.ResultRef = domain.TaskSucceeded, f.task.ResourceVersion+1, input.Result.URI
	return store.CompleteAttemptResult{Attempt: f.attempt, Run: f.run, Task: f.task}, nil
}

func (f *fakeRuntimeStore) WorkflowLineage(context.Context, string, uuid.UUID) (uuid.UUID, string, int64, bool, error) {
	return uuid.Nil, "", 0, false, nil
}
