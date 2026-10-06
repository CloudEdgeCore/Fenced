//go:build integration

package postgres_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	runtimecontrol "github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// serviceCheckpointArtifacts pauses real checkpoint I/O after the worker has
// entered RUNNING. Execution keeps its real runtime lease while the test observes it.
type serviceCheckpointArtifacts struct {
	reference.ArtifactStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	unblock sync.Once
}

func (a *serviceCheckpointArtifacts) Put(ctx context.Context, tenant, mediaType string, reader io.Reader) (kernelstore.ArtifactReference, error) {
	if strings.Contains(mediaType, "reference-state") {
		a.once.Do(func() { close(a.entered) })
		select {
		case <-ctx.Done():
			return kernelstore.ArtifactReference{}, ctx.Err()
		case <-a.release:
		}
	}
	return a.ArtifactStore.Put(ctx, tenant, mediaType, reader)
}

func (a *serviceCheckpointArtifacts) Release() {
	a.unblock.Do(func() { close(a.release) })
}

type serviceWorkerFixture struct {
	pool       *pgxpool.Pool
	repository *postgresstore.Store
	supervisor *supervisor.Supervisor
	service    *supervisor.Service
	instance   *supervisor.Instance
	worker     *reference.Worker
	artifacts  *serviceCheckpointArtifacts
	allowAck   func()
}

func prepareServiceWorker(t *testing.T, ctx context.Context, pauseAcknowledgement bool) serviceWorkerFixture {
	t.Helper()
	pool, repository := prepare(t, time.Now)
	publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"runtimeClassPolicy":{"allowed":["reference"]}}`)
	engine := supervisor.NewSupervisor(repository, supervisor.WithSpawner(supervisor.NewTaskSpawner(repository)))
	svc := runtimeService("tenant-a", "service-reference-worker")
	svc.Spec.RuntimeClass = "reference"
	svc.Spec.WorkloadSpec = []byte(`{
		"budget":{"tokens":100,"costUsd":1,"toolCalls":1,"wallSeconds":60},
		"placement":{"runtimeClasses":["reference"],"preferredClass":"reference","region":"cn-east",
			"cpuMillis":100,"memoryMiB":64,"llmConcurrency":1},
		"retryPolicy":{"maxAttempts":1}
	}`)
	created, err := engine.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("create task-backed service: %v", err)
	}
	instances, err := repository.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil || len(instances) != 1 || instances[0].TaskID == nil ||
		instances[0].Phase != supervisor.InstanceStarting || created.Status.ReadyReplicas != 0 {
		t.Fatalf("service must wait for real execution: service=%+v instances=%+v err=%v", created, instances, err)
	}
	limits := admission.New(admission.Limits{
		RuntimeClasses: []string{"reference"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
		MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	if count, err := admission.NewController(repository, limits, testPolicyEngine(t), "service-worker-admission", 10, time.Minute).Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("service task admission: count=%d err=%v", count, err)
	}
	pools := staticPools{{
		ID: "service-worker-pool", TenantIDs: []string{svc.TenantID}, RuntimeClass: "reference",
		RuntimeInstanceID: "service-worker-instance", Region: "cn-east", Ready: true,
		AvailableCPU: 2000, AvailableMemory: 4096, AvailableLLMSlots: 4,
	}}
	if err := repository.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatal(err)
	}
	if count, err := scheduler.NewController(repository, pools, "service-worker-scheduler", 10, time.Minute, 30*time.Second).Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("service task placement: count=%d err=%v", count, err)
	}
	if err := engine.Reconcile(ctx, svc.TenantID); err != nil {
		t.Fatal(err)
	}
	placed, err := repository.GetServiceTaskExecution(ctx, svc.TenantID, *instances[0].TaskID)
	observed, observationErr := repository.GetInstance(ctx, svc.TenantID, svc.ID, instances[0].ID)
	if err != nil || observationErr != nil || placed.Attempt == nil || placed.Attempt.Phase != domain.AttemptPlaced ||
		observed.Phase != supervisor.InstanceStarting {
		t.Fatalf("placement must not advertise process readiness: task=%+v instance=%+v errors=%v/%v", placed, observed, err, observationErr)
	}

	filesystem, err := artifact.NewFilesystem(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := &serviceCheckpointArtifacts{
		ArtifactStore: filesystem, entered: make(chan struct{}), release: make(chan struct{}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var ackGate chan struct{}
	if pauseAcknowledgement {
		ackGate = make(chan struct{})
	}
	var ackOnce sync.Once
	allowAck := func() {
		if ackGate != nil {
			ackOnce.Do(func() { close(ackGate) })
		}
	}
	t.Cleanup(allowAck)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Pause only cancellation acknowledgement to observe the durable
		// Stopping boundary. Heartbeats and all runtime handlers stay real.
		if _, ok := req.(*runtimev1.AcknowledgeCancellationRequest); ok && ackGate != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ackGate:
			}
		}
		return handler(ctx, req)
	}))
	runtimev1.RegisterRuntimeControlServiceServer(server, runtimecontrol.NewService(repository, svc.TenantID, time.Minute))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	worker := reference.NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifacts,
		svc.TenantID, "service-worker-instance", 3*time.Second)
	return serviceWorkerFixture{pool, repository, engine, svc, instances[0], worker, artifacts, allowAck}
}

type serviceWorkerResult struct {
	processed bool
	err       error
}

func startServiceWorker(t *testing.T, ctx context.Context, fixture serviceWorkerFixture) <-chan serviceWorkerResult {
	t.Helper()
	workCtx, cancel := context.WithCancel(ctx)
	results := make(chan serviceWorkerResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		processed, err := fixture.worker.RunOnce(workCtx)
		results <- serviceWorkerResult{processed, err}
	}()
	t.Cleanup(func() {
		cancel()
		fixture.allowAck()
		fixture.artifacts.Release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("service worker did not exit after cleanup cancellation")
		}
	})
	select {
	case <-fixture.artifacts.entered:
	case result := <-results:
		t.Fatalf("worker exited before reaching checkpoint: %+v", result)
	case <-ctx.Done():
		t.Fatalf("worker did not reach checkpoint: %v", ctx.Err())
	}
	if err := fixture.supervisor.Reconcile(ctx, fixture.service.TenantID); err != nil {
		t.Fatal(err)
	}
	state, err := fixture.repository.GetServiceTaskExecution(ctx, fixture.service.TenantID, *fixture.instance.TaskID)
	instance, instanceErr := fixture.repository.GetInstance(ctx, fixture.service.TenantID, fixture.service.ID, fixture.instance.ID)
	svc, serviceErr := fixture.repository.GetService(ctx, fixture.service.TenantID, fixture.service.ID)
	if err != nil || instanceErr != nil || serviceErr != nil || state.Attempt == nil || state.Lease == nil ||
		state.Attempt.Phase != domain.AttemptRunning || instance.Phase != supervisor.InstanceRunning ||
		instance.FencingToken != uint64(state.Attempt.FencingToken) || svc.Status.ReadyReplicas != 1 {
		t.Fatalf("real execution must drive Running/readiness: execution=%+v instance=%+v service=%+v errors=%v/%v/%v",
			state, instance, svc, err, instanceErr, serviceErr)
	}
	return results
}

func awaitServiceWorker(t *testing.T, ctx context.Context, results <-chan serviceWorkerResult) {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil || !result.processed {
			t.Fatalf("service worker did not complete its assignment: %+v", result)
		}
	case <-ctx.Done():
		t.Fatalf("service worker did not exit: %v", ctx.Err())
	}
}

func assertOnlyServiceWorkerTask(t *testing.T, ctx context.Context, fixture serviceWorkerFixture) {
	t.Helper()
	instances, err := fixture.repository.ListInstances(ctx, fixture.service.TenantID, fixture.service.ID)
	if err != nil || len(instances) != 1 || instances[0].TaskID == nil || *instances[0].TaskID != *fixture.instance.TaskID {
		t.Fatalf("service replaced its execution generation: instances=%+v err=%v", instances, err)
	}
	var count int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE tenant_id = $1`, fixture.service.TenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("service produced %d tasks, want one execution generation", count)
	}
}

func TestServiceReferenceWorkerCompletesThroughRealRuntimeProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture := prepareServiceWorker(t, ctx, false)
	results := startServiceWorker(t, ctx, fixture)
	fixture.artifacts.Release()
	awaitServiceWorker(t, ctx, results)
	state, err := fixture.repository.GetServiceTaskExecution(ctx, fixture.service.TenantID, *fixture.instance.TaskID)
	if err != nil || state.Task.Phase != domain.TaskSucceeded || state.Task.ResultRef == "" || state.Lease != nil {
		t.Fatalf("worker did not commit a durable terminal result: %+v err=%v", state, err)
	}
	for range 3 {
		if err := fixture.supervisor.Reconcile(ctx, fixture.service.TenantID); err != nil {
			t.Fatal(err)
		}
	}
	instance, err := fixture.repository.GetInstance(ctx, fixture.service.TenantID, fixture.service.ID, fixture.instance.ID)
	if err != nil || instance.Phase != supervisor.InstanceStopped || instance.ExitCode != 0 {
		t.Fatalf("completed worker did not stop its service instance: %+v err=%v", instance, err)
	}
	assertOnlyServiceWorkerTask(t, ctx, fixture)
}

func TestServiceReferenceWorkerCancellationWaitsForRealRuntimeAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture := prepareServiceWorker(t, ctx, true)
	results := startServiceWorker(t, ctx, fixture)
	if err := fixture.supervisor.StopService(ctx, fixture.service.TenantID, fixture.service.ID); err != nil {
		t.Fatal(err)
	}
	instance, err := fixture.repository.GetInstance(ctx, fixture.service.TenantID, fixture.service.ID, fixture.instance.ID)
	state, stateErr := fixture.repository.GetServiceTaskExecution(ctx, fixture.service.TenantID, *fixture.instance.TaskID)
	if err != nil || stateErr != nil || instance.Phase != supervisor.InstanceStopping || instance.TerminatedAt != nil ||
		state.Task.Phase != domain.TaskRunning || state.Task.CancelRequestedAt == nil || state.Attempt == nil ||
		state.Attempt.Phase != domain.AttemptCancelRequested {
		t.Fatalf("service must wait for runtime acknowledgement: instance=%+v execution=%+v errors=%v/%v", instance, state, err, stateErr)
	}
	assertOnlyServiceWorkerTask(t, ctx, fixture)
	fixture.allowAck()
	// The checkpoint stays blocked until the real leasekeeper acknowledges the
	// cancellation and cancels the execution context. No direct test ACK is sent.
	awaitServiceWorker(t, ctx, results)
	state, err = fixture.repository.GetServiceTaskExecution(ctx, fixture.service.TenantID, *fixture.instance.TaskID)
	if err != nil || state.Task.Phase != domain.TaskCancelled || state.Lease != nil {
		t.Fatalf("runtime cancellation did not converge: %+v err=%v", state, err)
	}
	if err := fixture.supervisor.Reconcile(ctx, fixture.service.TenantID); err != nil {
		t.Fatal(err)
	}
	instance, err = fixture.repository.GetInstance(ctx, fixture.service.TenantID, fixture.service.ID, fixture.instance.ID)
	if err != nil || instance.Phase != supervisor.InstanceStopped || instance.ExitReason != "service stopped" {
		t.Fatalf("acknowledged cancellation did not stop the instance: %+v err=%v", instance, err)
	}
	assertOnlyServiceWorkerTask(t, ctx, fixture)
}
