package reference

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	runtimecontrol "github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorkerCompletesAssignmentThroughRuntimeProtocol(t *testing.T) {
	repository := newFakeRuntimeStore()
	worker := newTestWorker(t, repository, "worker-1")
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptCompleted || repository.task.Phase != domain.TaskSucceeded ||
		repository.checkpoint == nil || repository.task.ResultRef == "" {
		t.Fatalf("worker did not complete execution: attempt=%+v task=%+v checkpoint=%+v", repository.attempt, repository.task, repository.checkpoint)
	}
	processed, err = worker.RunOnce(context.Background())
	if err != nil || processed {
		t.Fatalf("completed assignment was polled again: %v, %v", processed, err)
	}
}

type fakeRuntimeStore struct {
	task              store.Task
	run               store.Run
	attempt           store.Attempt
	lease             store.Lease
	checkpoint        *store.Checkpoint
	pendingApprovalID *uuid.UUID
	heartbeats        int
	// cancelAfterHeartbeat arms a pending cancellation once the fake has
	// served this many lease renewals (0 = never).
	cancelAfterHeartbeat int
}

func newFakeRuntimeStore() *fakeRuntimeStore {
	taskID, runID, attemptID := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	return &fakeRuntimeStore{
		task: store.Task{ID: taskID, TenantID: "tenant-a", AgentVersionRef: "agent:v1", Goal: "deterministic result",
			Spec: []byte(`{"retryPolicy":{"maxAttempts":3}}`), Phase: domain.TaskRunning, ActiveRunID: &runID, ResourceVersion: 3},
		run: store.Run{ID: runID, TenantID: "tenant-a", TaskID: taskID, Phase: domain.RunRunning,
			ActiveAttemptID: &attemptID, CurrentFencingToken: 1, ResourceVersion: 1},
		attempt: store.Attempt{ID: attemptID, TenantID: "tenant-a", RunID: runID, Phase: domain.AttemptPlaced,
			RuntimeClass: "oci", RuntimePoolID: "pool-1", RuntimeInstanceID: "worker-1", FencingToken: 1, ResourceVersion: 1},
		lease: store.Lease{ID: uuid.New(), TenantID: "tenant-a", RunID: runID, AttemptID: attemptID,
			FencingToken: 1, ResourceVersion: 1, AcquiredAt: now, HeartbeatAt: now, ExpiresAt: now.Add(time.Minute)},
	}
}

func (f *fakeRuntimeStore) PollRuntimeAssignment(_ context.Context, tenantID, instanceID string) (store.RuntimeAssignment, error) {
	if tenantID != f.task.TenantID || instanceID != f.attempt.RuntimeInstanceID ||
		(f.attempt.Phase != domain.AttemptPlaced && f.attempt.Phase != domain.AttemptWaitingApproval) {
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
	result := store.RuntimeAssignment{Task: f.task, Run: f.run, Attempt: f.attempt, Lease: f.lease, ResumeCheckpoint: f.checkpoint}
	if f.attempt.Phase == domain.AttemptWaitingApproval && f.pendingApprovalID != nil {
		result.PendingApprovalID = f.pendingApprovalID
	}
	return result
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
	f.attempt.ResourceVersion++
	return f.attempt, nil
}

func (f *fakeRuntimeStore) HeartbeatLease(_ context.Context, input store.HeartbeatLeaseInput) (store.Lease, error) {
	if input.FencingToken != f.lease.FencingToken || input.ExpectedLeaseVersion != f.lease.ResourceVersion {
		return store.Lease{}, store.ErrFenced
	}
	f.heartbeats++
	f.lease.ResourceVersion++
	f.lease.ExpiresAt = time.Now().Add(input.TTL)
	return f.lease, nil
}

func (f *fakeRuntimeStore) GetHeartbeatStatus(_ context.Context, tenantID string, attemptID uuid.UUID, token int64) (store.HeartbeatStatus, error) {
	if tenantID != f.task.TenantID || attemptID != f.attempt.ID || token != f.attempt.FencingToken {
		return store.HeartbeatStatus{}, store.ErrFenced
	}
	if f.cancelAfterHeartbeat > 0 && f.heartbeats >= f.cancelAfterHeartbeat && f.task.CancelRequestedAt == nil {
		now := time.Now().UTC()
		f.task.CancelRequestedAt = &now
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
		SchemaVersion: input.SchemaVersion, State: input.State, ConfirmedReceiptIDs: normalized.ConfirmedReceiptIDs,
		EnvelopeSHA256: digest, CreatedAt: time.Now().UTC()}
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

func (f *fakeRuntimeStore) AcknowledgeCancellation(_ context.Context, in store.CancelAttemptInput) (store.CancelAttemptResult, error) {
	if in.AttemptID != f.attempt.ID || in.FencingToken != f.attempt.FencingToken {
		return store.CancelAttemptResult{}, store.ErrFenced
	}
	if in.ExpectedAttemptVersion != f.attempt.ResourceVersion {
		return store.CancelAttemptResult{}, store.ErrVersionConflict
	}
	if err := domain.ValidateAttemptTransition(f.attempt.Phase, domain.AttemptCancelRequested); err != nil {
		return store.CancelAttemptResult{}, errors.Join(store.ErrInvalidTransition, err)
	}
	f.attempt.Phase = domain.AttemptCancelRequested
	f.attempt.ResourceVersion++
	return store.CancelAttemptResult{Attempt: f.attempt, Run: f.run, Task: f.task}, nil
}

func (f *fakeRuntimeStore) ListExpiredAttempts(context.Context, time.Time, int) ([]store.RecoveryCandidate, error) {
	return nil, nil
}
func (f *fakeRuntimeStore) RecoverExpiredAttempt(context.Context, store.RecoverExpiredAttemptInput) (store.RecoveryResult, error) {
	return store.RecoveryResult{}, nil
}
func (f *fakeRuntimeStore) CreateTask(context.Context, store.CreateTaskInput) (store.CreateTaskResult, error) {
	panic("not used")
}
func (f *fakeRuntimeStore) TransitionTask(context.Context, string, uuid.UUID, int64, domain.TaskPhase) (store.Task, error) {
	panic("not used")
}
func (f *fakeRuntimeStore) CreateRun(context.Context, store.CreateRunInput) (store.Run, error) {
	panic("not used")
}
func (f *fakeRuntimeStore) AcquireAttempt(context.Context, store.AcquireAttemptInput) (store.AttemptLease, error) {
	panic("not used")
}
func (f *fakeRuntimeStore) CompleteRun(context.Context, store.CompleteRunInput) (store.Run, store.Task, error) {
	panic("not used")
}

func TestWorkerParksAndResumesOnHumanApproval(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"tools":[{"name":"fs.write","action":"write","resource":"fs:/tmp","args":{"path":"a.txt"},"idempotencyKey":"tool-1"}]}`)
	approvalID := uuid.New()
	invoker := &scriptedInvoker{approvalID: approvalID, approvalGated: true}
	worker := newTestWorkerWithGateway(t, repository, "worker-1", invoker)
	ctx := context.Background()

	// First run: the high-risk tool requires approval, so the attempt parks.
	processed, err := worker.RunOnce(ctx)
	if err != nil || processed {
		t.Fatalf("RunOnce() = %v, %v, want parked", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptWaitingApproval {
		t.Fatalf("attempt phase = %s, want WAITING_APPROVAL", repository.attempt.Phase)
	}
	if invoker.calls != 1 {
		t.Fatalf("gateway invocations = %d, want 1", invoker.calls)
	}

	// The kernel resolves the pending approval into the assignment.
	repository.pendingApprovalID = &approvalID

	// Human decision still pending: the worker parks again without failing.
	invoker.status = tool.ApprovalPending
	processed, err = worker.RunOnce(ctx)
	if err != nil || processed {
		t.Fatalf("pending RunOnce() = %v, %v, want parked", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptWaitingApproval || repository.task.Phase != domain.TaskRunning {
		t.Fatalf("pending decision must keep the attempt parked: attempt=%s task=%s", repository.attempt.Phase, repository.task.Phase)
	}

	// Human approved: the worker resumes and completes the run.
	invoker.status = ""
	processed, err = worker.RunOnce(ctx)
	if err != nil || !processed {
		t.Fatalf("resumed RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptCompleted || repository.task.Phase != domain.TaskSucceeded {
		t.Fatalf("resumed run did not complete: attempt=%s task=%s", repository.attempt.Phase, repository.task.Phase)
	}
	if repository.checkpoint == nil || len(repository.checkpoint.ConfirmedReceiptIDs) != 1 ||
		repository.checkpoint.ConfirmedReceiptIDs[0] != "TOOL:fs.write@1.0.0" {
		t.Fatalf("checkpoint must confirm the tool receipt: %+v", repository.checkpoint)
	}
}

func TestWorkerFailsAttemptOnRejectedApproval(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"tools":[{"name":"fs.write","action":"write","resource":"fs:/tmp","args":{"path":"a.txt"},"idempotencyKey":"tool-1"}]}`)
	approvalID := uuid.New()
	invoker := &scriptedInvoker{approvalID: approvalID, approvalGated: true}
	worker := newTestWorkerWithGateway(t, repository, "worker-1", invoker)
	ctx := context.Background()

	if processed, err := worker.RunOnce(ctx); err != nil || processed {
		t.Fatalf("initial RunOnce() = %v, %v, want parked", processed, err)
	}
	repository.pendingApprovalID = &approvalID
	invoker.status = tool.ApprovalRejected

	processed, err := worker.RunOnce(ctx)
	if err != nil || !processed {
		t.Fatalf("rejected RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptFailed || repository.task.Phase != domain.TaskRunning {
		t.Fatalf("rejected approval must fail the attempt: attempt=%s task=%s", repository.attempt.Phase, repository.task.Phase)
	}
}

// scriptedInvoker mimics the Tool Gateway decision chain for worker tests:
// high-risk calls require approval first, then execute once the approval is
// presented (unless it is still pending or was rejected).
type scriptedInvoker struct {
	descriptors   []store.ToolDescriptor
	approvalID    uuid.UUID
	status        tool.ApprovalNotUsableReason
	approvalGated bool
	calls         int
	input         tool.InvokeInput
}

func (s *scriptedInvoker) InvokeTool(_ context.Context, in tool.InvokeInput) (tool.InvokeResult, error) {
	s.calls++
	s.input = in
	if !s.approvalGated {
		return tool.InvokeResult{
			Outcome: tool.OutcomeExecuted, Result: json.RawMessage(`{"ok":true}`),
			ReceiptOperation: "TOOL:fs.write@1.0.0",
			ToolCall:         store.ToolCall{ID: uuid.New(), Status: store.ToolCallExecuted},
		}, nil
	}
	if in.ApprovalID == nil {
		return tool.InvokeResult{
			Outcome: tool.OutcomeRequiresApproval, ApprovalID: &s.approvalID,
			ToolCall: store.ToolCall{ID: s.approvalID, Status: store.ToolCallRequiresApproval},
		}, nil
	}
	if *in.ApprovalID != s.approvalID {
		return tool.InvokeResult{}, &tool.ApprovalNotUsableError{Reason: tool.ApprovalBindingMismatch, Err: store.ErrApprovalNotUsable}
	}
	if s.status == tool.ApprovalPending || s.status == tool.ApprovalRejected || s.status == tool.ApprovalExpired {
		return tool.InvokeResult{}, &tool.ApprovalNotUsableError{Reason: s.status, Err: store.ErrApprovalNotUsable}
	}
	return tool.InvokeResult{
		Outcome: tool.OutcomeExecuted, Result: json.RawMessage(`{"ok":true}`),
		ReceiptOperation: "TOOL:fs.write@1.0.0",
		ToolCall:         store.ToolCall{ID: s.approvalID, Status: store.ToolCallExecuted},
	}, nil
}

func (s *scriptedInvoker) ListTools(context.Context, string) ([]store.ToolDescriptor, error) {
	return s.descriptors, nil
}

func (s *scriptedInvoker) GetToolDescriptor(_ context.Context, _, name, version string) (store.ToolDescriptor, error) {
	for _, descriptor := range s.descriptors {
		if descriptor.Name == name && (version == "" || descriptor.Version == version) {
			return descriptor, nil
		}
	}
	return store.ToolDescriptor{Name: name, Version: version}, nil
}

func newTestWorkerWithGateway(t *testing.T, repository store.RuntimeStore, instanceID string, invoker gateway.ToolInvoker) *Worker {
	t.Helper()
	worker := newTestWorker(t, repository, instanceID)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	gatewayv1.RegisterToolGatewayServiceServer(server, gateway.NewService(invoker, "tenant-a"))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return worker.WithToolGateway(gatewayv1.NewToolGatewayServiceClient(connection))
}

func newTestWorker(t *testing.T, repository store.RuntimeStore, instanceID string) *Worker {
	t.Helper()
	return newTestWorkerWithTTL(t, repository, instanceID, 30*time.Second)
}

func newTestWorkerWithTTL(t *testing.T, repository store.RuntimeStore, instanceID string, heartbeatTTL time.Duration) *Worker {
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
	artifacts, err := artifact.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifacts,
		"tenant-a", instanceID, heartbeatTTL)
}

// slowInvoker delays gateway invocations until the context is cancelled, so
// tests can hold a workload in flight while the lease keeper runs.
type slowInvoker struct {
	inner   *scriptedInvoker
	delay   time.Duration
	started chan struct{}
}

func (s *slowInvoker) InvokeTool(ctx context.Context, in tool.InvokeInput) (tool.InvokeResult, error) {
	if s.started != nil {
		close(s.started)
		s.started = nil
	}
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return tool.InvokeResult{}, ctx.Err()
	case <-timer.C:
	}
	return s.inner.InvokeTool(ctx, in)
}

func (s *slowInvoker) ListTools(ctx context.Context, tenantID string) ([]store.ToolDescriptor, error) {
	return s.inner.ListTools(ctx, tenantID)
}

func (s *slowInvoker) GetToolDescriptor(ctx context.Context, tenantID, name, version string) (store.ToolDescriptor, error) {
	return s.inner.GetToolDescriptor(ctx, tenantID, name, version)
}

// TestWorkerRenewsLeaseDuringExecution proves the fencing fix: a workload
// that runs longer than the lease TTL renews the lease while executing, so
// the run completes instead of being fenced mid-execution.
func TestWorkerRenewsLeaseDuringExecution(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"tools":[{"name":"fs.write","action":"write","resource":"fs:/tmp","args":{"path":"a.txt"},"idempotencyKey":"tool-1"}]}`)
	inner := &scriptedInvoker{}
	slow := &slowInvoker{inner: inner, delay: 900 * time.Millisecond, started: make(chan struct{})}
	worker := newTestWorkerWithGatewayWithTTL(t, repository, "worker-1", time.Second, slow)
	ctx := context.Background()

	processed, err := worker.RunOnce(ctx)
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptCompleted || repository.task.Phase != domain.TaskSucceeded {
		t.Fatalf("run did not complete: attempt=%s task=%s", repository.attempt.Phase, repository.task.Phase)
	}
	// The initial heartbeat plus at least one renewal while the tool script
	// was in flight (1s TTL -> 333ms ticks over a 900ms execution).
	if repository.heartbeats < 2 {
		t.Fatalf("lease renewals = %d, want >= 2 (workload outlived one TTL)", repository.heartbeats)
	}
}

// TestWorkerStopsOnCancellationMidExecution proves that a cancellation
// requested while the workload runs is acknowledged by the lease keeper, the
// execution is cancelled, and the worker never completes (or fails) the
// attempt itself.
func TestWorkerStopsOnCancellationMidExecution(t *testing.T) {
	repository := newFakeRuntimeStore()
	repository.task.Spec = []byte(`{"tools":[{"name":"fs.write","action":"write","resource":"fs:/tmp","args":{"path":"a.txt"},"idempotencyKey":"tool-1"}]}`)
	inner := &scriptedInvoker{}
	slow := &slowInvoker{inner: inner, delay: 900 * time.Millisecond, started: make(chan struct{})}
	worker := newTestWorkerWithGatewayWithTTL(t, repository, "worker-1", time.Second, slow)
	// The second renewal observes the cancellation.
	repository.cancelAfterHeartbeat = 2
	ctx := context.Background()

	processed, err := worker.RunOnce(ctx)
	if err != nil || !processed {
		t.Fatalf("RunOnce() = %v, %v, want acknowledged cancellation", processed, err)
	}
	if repository.attempt.Phase != domain.AttemptCancelRequested {
		t.Fatalf("attempt phase = %s, want CANCEL_REQUESTED", repository.attempt.Phase)
	}
	if repository.task.Phase == domain.TaskSucceeded || repository.task.Phase == domain.TaskFailed {
		t.Fatalf("task must not be finalized by the cancelled worker: %s", repository.task.Phase)
	}
	if repository.checkpoint != nil {
		t.Fatalf("cancelled worker must not commit a checkpoint")
	}
}

func newTestWorkerWithGatewayWithTTL(t *testing.T, repository store.RuntimeStore, instanceID string, heartbeatTTL time.Duration, invoker gateway.ToolInvoker) *Worker {
	t.Helper()
	worker := newTestWorkerWithTTL(t, repository, instanceID, heartbeatTTL)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	gatewayv1.RegisterToolGatewayServiceServer(server, gateway.NewService(invoker, "tenant-a"))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return worker.WithToolGateway(gatewayv1.NewToolGatewayServiceClient(connection))
}

func (f *fakeRuntimeStore) WorkflowLineage(context.Context, string, uuid.UUID) (uuid.UUID, string, int64, bool, error) {
	return uuid.Nil, "", 0, false, nil
}
