package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type runtimeFixture struct {
	mu    sync.Mutex
	state map[string]json.RawMessage
}

type delayedRuntime struct{ delay time.Duration }

func (r delayedRuntime) Run(context.Context, agent.StartRequest, agent.Emitter) (json.RawMessage, error) {
	time.Sleep(r.delay)
	return json.RawMessage(`{"done":true}`), nil
}

func (delayedRuntime) Checkpoint(context.Context, string) (agent.Checkpoint, error) {
	return agent.Checkpoint{SchemaVersion: "delayed/v1", State: json.RawMessage(`{}`), CreatedAt: time.Now().UTC()}, nil
}

func (delayedRuntime) Restore(context.Context, agent.RestoreRequest) error { return nil }

func (r *runtimeFixture) Run(_ context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	if err := emit("fixture.started", json.RawMessage("{\"ok\":true}")); err != nil {
		return nil, err
	}
	output := json.RawMessage("{\"adapter\":\"complete\",\"apiKey\":\"must-not-reach-artifact\"}")
	r.mu.Lock()
	r.state[request.ExecutionID] = output
	r.mu.Unlock()
	return output, nil
}
func (r *runtimeFixture) Checkpoint(_ context.Context, executionID string) (agent.Checkpoint, error) {
	r.mu.Lock()
	state := r.state[executionID]
	r.mu.Unlock()
	return agent.Checkpoint{SchemaVersion: "fixture/v1", State: state, CreatedAt: time.Now().UTC()}, nil
}
func (r *runtimeFixture) Restore(_ context.Context, request agent.RestoreRequest) error {
	r.mu.Lock()
	r.state[request.ExecutionID] = request.Checkpoint.State
	r.mu.Unlock()
	return nil
}

type fakeControl struct {
	assignment  *runtimev1.Assignment
	version     int64
	completed   *runtimev1.CompleteAttemptRequest
	failure     string
	checkpoints int
	cancelled   bool
}

type conflictControl struct {
	*fakeControl
	mu                  sync.Mutex
	failureConflicts    int
	failureTransitions  int
	refreshPhase        string
	transitionConflicts map[runtimev1.AttemptPhase]int
	transitionCalls     map[runtimev1.AttemptPhase]int
	transitionErrors    map[runtimev1.AttemptPhase]error
}

func (c *conflictControl) GetAssignment(context.Context, *runtimev1.GetAssignmentRequest, ...grpc.CallOption) (*runtimev1.GetAssignmentResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	assignment := proto.Clone(c.assignment).(*runtimev1.Assignment)
	assignment.AttemptVersion = c.version
	assignment.Phase = c.refreshPhase
	return &runtimev1.GetAssignmentResponse{Assignment: assignment}, nil
}

func (c *conflictControl) TransitionAttempt(ctx context.Context, request *runtimev1.TransitionAttemptRequest, options ...grpc.CallOption) (*runtimev1.TransitionAttemptResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.transitionCalls != nil {
		c.transitionCalls[request.GetTargetPhase()]++
	}
	if err := c.transitionErrors[request.GetTargetPhase()]; err != nil {
		delete(c.transitionErrors, request.GetTargetPhase())
		return nil, err
	}
	if c.transitionConflicts[request.GetTargetPhase()] > 0 {
		c.transitionConflicts[request.GetTargetPhase()]--
		return nil, status.Error(codes.Aborted, "attempt version changed")
	}
	if request.GetTargetPhase() == runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED {
		c.failureTransitions++
		if c.failureConflicts > 0 {
			c.failureConflicts--
			return nil, status.Error(codes.Aborted, "attempt version changed")
		}
	}
	return c.fakeControl.TransitionAttempt(ctx, request, options...)
}

func TestLifecycleTransitionRefreshesAndRetriesExpectedPredecessor(t *testing.T) {
	identity := &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.NewString(), FencingToken: 1}
	base := &fakeControl{version: 4, assignment: &runtimev1.Assignment{
		Identity: identity, AttemptVersion: 4, Phase: string(domain.AttemptStarting),
	}}
	control := &conflictControl{
		fakeControl: base, refreshPhase: string(domain.AttemptStarting),
		transitionConflicts: map[runtimev1.AttemptPhase]int{runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING: 1},
		transitionCalls:     map[runtimev1.AttemptPhase]int{},
	}
	version, proceed, err := (&Worker{control: control}).advanceAttempt(context.Background(), identity, 3,
		domain.AttemptStarting, domain.AttemptRunning, runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING)
	if err != nil || !proceed {
		t.Fatalf("version=%d proceed=%v error=%v", version, proceed, err)
	}
	if version != 5 || control.transitionCalls[runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING] != 2 {
		t.Fatalf("version=%d transitions=%d, want refreshed retry to version 5", version,
			control.transitionCalls[runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING])
	}
}

func TestLifecycleTransitionAcknowledgesCancellation(t *testing.T) {
	identity := &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.NewString(), FencingToken: 1}
	base := &fakeControl{version: 4, assignment: &runtimev1.Assignment{
		Identity: identity, AttemptVersion: 4, Phase: string(domain.AttemptCancelRequested),
	}}
	control := &conflictControl{
		fakeControl: base, refreshPhase: string(domain.AttemptCancelRequested),
		transitionConflicts: map[runtimev1.AttemptPhase]int{runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING: 1},
		transitionCalls:     map[runtimev1.AttemptPhase]int{},
	}
	version, proceed, err := (&Worker{control: control}).advanceAttempt(context.Background(), identity, 3,
		domain.AttemptStarting, domain.AttemptRunning, runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING)
	if err != nil || proceed || version != 4 || !base.cancelled {
		t.Fatalf("version=%d proceed=%v error=%v, cancellation must win", version, proceed, err)
	}
	if control.transitionCalls[runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING] != 1 {
		t.Fatalf("transitions=%d, want no retry after cancellation",
			control.transitionCalls[runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING])
	}
}

func TestLifecycleTransitionDropsFencedAssignmentWithoutStoppingWorker(t *testing.T) {
	identity := &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.NewString(), FencingToken: 1}
	control := &conflictControl{
		fakeControl: &fakeControl{version: 2},
		transitionErrors: map[runtimev1.AttemptPhase]error{
			runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING: status.Error(codes.PermissionDenied, "lease expired"),
		},
	}
	version, proceed, err := (&Worker{control: control}).advanceAttempt(context.Background(), identity, 2,
		domain.AttemptStarting, domain.AttemptRunning, runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING)
	if err != nil || proceed || version != 0 {
		t.Fatalf("version=%d proceed=%v error=%v, fenced assignment must be dropped", version, proceed, err)
	}
}

func (f *fakeControl) PollAssignment(context.Context, *runtimev1.PollAssignmentRequest, ...grpc.CallOption) (*runtimev1.PollAssignmentResponse, error) {
	return &runtimev1.PollAssignmentResponse{Assignment: f.assignment}, nil
}
func (f *fakeControl) GetAssignment(context.Context, *runtimev1.GetAssignmentRequest, ...grpc.CallOption) (*runtimev1.GetAssignmentResponse, error) {
	return &runtimev1.GetAssignmentResponse{Assignment: f.assignment}, nil
}
func (f *fakeControl) TransitionAttempt(_ context.Context, request *runtimev1.TransitionAttemptRequest, _ ...grpc.CallOption) (*runtimev1.TransitionAttemptResponse, error) {
	f.version++
	if request.FailureCode != "" {
		f.failure = request.FailureCode + ": " + request.FailureMessage
	}
	return &runtimev1.TransitionAttemptResponse{AttemptVersion: f.version, Phase: request.TargetPhase}, nil
}
func (f *fakeControl) Heartbeat(context.Context, *runtimev1.HeartbeatRequest, ...grpc.CallOption) (*runtimev1.HeartbeatResponse, error) {
	return &runtimev1.HeartbeatResponse{LeaseVersion: 2, AttemptVersion: f.version}, nil
}
func (f *fakeControl) CommitCheckpoint(_ context.Context, _ *runtimev1.CommitCheckpointRequest, _ ...grpc.CallOption) (*runtimev1.CommitCheckpointResponse, error) {
	f.checkpoints++
	f.version++
	return &runtimev1.CommitCheckpointResponse{AttemptVersion: f.version}, nil
}
func (f *fakeControl) CompleteAttempt(_ context.Context, request *runtimev1.CompleteAttemptRequest, _ ...grpc.CallOption) (*runtimev1.CompleteAttemptResponse, error) {
	f.completed = request
	return &runtimev1.CompleteAttemptResponse{AttemptVersion: f.version + 1}, nil
}
func (f *fakeControl) AcknowledgeCancellation(context.Context, *runtimev1.AcknowledgeCancellationRequest, ...grpc.CallOption) (*runtimev1.AcknowledgeCancellationResponse, error) {
	f.cancelled = true
	return &runtimev1.AcknowledgeCancellationResponse{}, nil
}

type memoryArtifacts struct {
	mu      sync.Mutex
	content map[string][]byte
}

func (m *memoryArtifacts) Put(_ context.Context, _ string, mediaType string, reader io.Reader) (store.ArtifactReference, error) {
	encoded, err := io.ReadAll(reader)
	if err != nil {
		return store.ArtifactReference{}, err
	}
	digest := sha256.Sum256(encoded)
	uri := "memory://" + uuid.NewString()
	m.mu.Lock()
	m.content[uri] = encoded
	m.mu.Unlock()
	return store.ArtifactReference{URI: uri, SHA256: digest, SizeBytes: int64(len(encoded)), MediaType: mediaType}, nil
}
func (m *memoryArtifacts) Open(_ context.Context, _ string, reference store.ArtifactReference) (io.ReadCloser, error) {
	m.mu.Lock()
	content := bytes.Clone(m.content[reference.URI])
	m.mu.Unlock()
	return io.NopCloser(bytes.NewReader(content)), nil
}

func TestWorkerExecutesManifestThroughRuntimeInterface(t *testing.T) {
	host, err := agent.NewHost(&runtimeFixture{state: map[string]json.RawMessage{}}, agent.HostOptions{Adapter: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	spec := agentversion.Spec{
		RuntimeClassPolicy: agentversion.RuntimeClassPolicy{Allowed: []string{"remote"}, Preferred: "remote"},
		Runtimes: []agentversion.RuntimeTarget{{
			Class: "remote", Interface: agentversion.RuntimeInterfaceV1,
			RuntimeABI: "fenced.remote/v1", Entrypoint: []string{server.URL},
		}},
		Capabilities: &agentversion.Capabilities{
			Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{},
		},
		Resources:  &agentversion.ResourceLimits{CPUMillis: 100, MemoryMiB: 128},
		Budget:     &agentversion.Budget{WallSeconds: 60},
		Checkpoint: &agentversion.CheckpointPolicy{Mode: agentversion.CheckpointLogical, SchemaVersion: "fixture/v1"},
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	attemptID := uuid.NewString()
	control := &fakeControl{version: 1, assignment: &runtimev1.Assignment{
		Identity: &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: attemptID, FencingToken: 1},
		RunId:    uuid.NewString(), TaskId: uuid.NewString(), AgentVersionRef: "fixture@0.9.0",
		Goal: "execute", WorkloadSpecJson: []byte("{}"), AgentVersionSpecJson: specJSON,
		RuntimeClass: "remote", RuntimePoolId: "remote-pool", RuntimeInstanceId: "adapter-1",
		AttemptVersion: 1, LeaseVersion: 1,
	}}
	artifacts := &memoryArtifacts{content: map[string][]byte{}}
	worker, err := NewWorker(control, artifacts, server.URL, "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if control.completed == nil || control.completed.GetResult() == nil {
		t.Fatalf("adapter worker did not complete with a durable result; failure=%s", control.failure)
	}
	if control.checkpoints != 1 {
		t.Fatalf("logical checkpoint commits=%d, want 1", control.checkpoints)
	}
	artifacts.mu.Lock()
	result := artifacts.content[control.completed.GetResult().GetUri()]
	artifacts.mu.Unlock()
	if !bytes.Contains(result, []byte("\"provider\":\"adapter-http\"")) ||
		!bytes.Contains(result, []byte("\"adapter\":\"complete\"")) {
		t.Fatalf("unexpected adapter result: %s", result)
	}
	if bytes.Contains(result, []byte("must-not-reach-artifact")) {
		t.Fatalf("runtime credential leaked into result artifact: %s", result)
	}
}

func TestWorkerHonorsCheckpointNone(t *testing.T) {
	host, err := agent.NewHost(&runtimeFixture{state: map[string]json.RawMessage{}}, agent.HostOptions{Adapter: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	spec := agentversion.Spec{
		RuntimeClassPolicy: agentversion.RuntimeClassPolicy{Allowed: []string{"remote"}, Preferred: "remote"},
		Runtimes: []agentversion.RuntimeTarget{{
			Class: "remote", Interface: agentversion.RuntimeInterfaceV1,
			RuntimeABI: "fenced.remote/v1", Entrypoint: []string{server.URL},
		}},
		Capabilities: &agentversion.Capabilities{
			Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{},
		},
		Resources:  &agentversion.ResourceLimits{CPUMillis: 100, MemoryMiB: 128},
		Budget:     &agentversion.Budget{WallSeconds: 60},
		Checkpoint: &agentversion.CheckpointPolicy{Mode: agentversion.CheckpointNone},
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	control := &fakeControl{version: 1, assignment: &runtimev1.Assignment{
		Identity: &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.NewString(), FencingToken: 1},
		RunId:    uuid.NewString(), TaskId: uuid.NewString(), AgentVersionRef: "fixture@0.9.0",
		Goal: "execute", WorkloadSpecJson: []byte("{}"), AgentVersionSpecJson: specJSON,
		RuntimeClass: "remote", RuntimePoolId: "remote-pool", RuntimeInstanceId: "adapter-1",
		AttemptVersion: 1, LeaseVersion: 1,
	}}
	artifacts := &memoryArtifacts{content: map[string][]byte{}}
	worker, err := NewWorker(control, artifacts, server.URL, "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed || control.completed == nil {
		t.Fatalf("processed=%v completed=%v error=%v failure=%s", processed, control.completed != nil, err, control.failure)
	}
	if control.checkpoints != 0 {
		t.Fatalf("checkpoint-none publication committed %d checkpoints", control.checkpoints)
	}
}

func TestCheckpointCadenceCannotStarveTerminalResultPolling(t *testing.T) {
	host, err := agent.NewHost(delayedRuntime{delay: 75 * time.Millisecond}, agent.HostOptions{Adapter: "delayed"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	worker, err := NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		server.URL, "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	worker.pollInterval = 25 * time.Millisecond
	runtimeClient, err := agent.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	executionID := uuid.NewString()
	if _, err := runtimeClient.Start(context.Background(), agent.StartRequest{
		ExecutionID: executionID, AgentVersionRef: "delayed@1", Goal: "finish",
		Input: json.RawMessage(`{}`), Capabilities: agent.CapabilityGrant{
			Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{}, ChildAgents: []string{},
		},
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	checkpoints := 0
	result, _, err := worker.wait(ctx, runtimeClient, executionID, 10*time.Millisecond, func() error {
		checkpoints++
		return nil
	})
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if result.Status != agent.StatusSucceeded || checkpoints == 0 {
		t.Fatalf("result=%s checkpoints=%d, want terminal result despite faster checkpoints", result.Status, checkpoints)
	}
}

func TestFailureTransitionRefreshesAttemptVersionAfterConflict(t *testing.T) {
	identity := &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.NewString(), FencingToken: 1}
	base := &fakeControl{
		version: 4,
		assignment: &runtimev1.Assignment{
			Identity: identity, AttemptVersion: 4, Phase: "RUNNING",
		},
	}
	control := &conflictControl{
		fakeControl: base, failureConflicts: 1, refreshPhase: "RUNNING",
	}
	processed, err := (&Worker{control: control}).fail(context.Background(), identity, 3, "execution_failed", io.ErrUnexpectedEOF)
	if err != nil || !processed {
		t.Fatalf("processed=%v error=%v", processed, err)
	}
	if control.failureTransitions != 2 || !strings.HasPrefix(base.failure, "execution_failed:") {
		t.Fatalf("failure transitions=%d failure=%q, want one conflict followed by a committed failure", control.failureTransitions, base.failure)
	}
}

func TestFailureTransitionNeverOverwritesCancellation(t *testing.T) {
	identity := &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: uuid.NewString(), FencingToken: 1}
	base := &fakeControl{
		version: 4,
		assignment: &runtimev1.Assignment{
			Identity: identity, AttemptVersion: 4, Phase: "CANCEL_REQUESTED",
		},
	}
	control := &conflictControl{
		fakeControl: base, failureConflicts: 1, refreshPhase: "CANCEL_REQUESTED",
	}
	processed, err := (&Worker{control: control}).fail(context.Background(), identity, 3, "execution_failed", io.ErrUnexpectedEOF)
	if err != nil || !processed {
		t.Fatalf("processed=%v error=%v", processed, err)
	}
	if control.failureTransitions != 1 || base.failure != "" {
		t.Fatalf("failure transitions=%d failure=%q, cancellation must win", control.failureTransitions, base.failure)
	}
}
