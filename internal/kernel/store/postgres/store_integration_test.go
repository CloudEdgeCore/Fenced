//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDatabaseEnvironment = "FENCED_TEST_DATABASE_URL"

func TestTaskIdempotencyAndCAS(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()
	input := kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		AgentVersionRef: "agent:v1", Goal: "prove idempotency",
		Spec: []byte(`{"limits":{"tokens":1000},"model":"test"}`), IdempotencyKey: "request-1",
	}

	created, err := store.CreateTask(ctx, input)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if created.Existing || created.Task.Phase != domain.TaskQueued || created.Task.ResourceVersion != 1 {
		t.Fatalf("unexpected created task: %+v", created)
	}

	retry := input
	retry.ID = uuid.New()
	retry.Spec = []byte(`{"model":"test","limits":{"tokens":1000}}`)
	existing, err := store.CreateTask(ctx, retry)
	if err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if !existing.Existing || existing.Task.ID != created.Task.ID {
		t.Fatalf("retry did not return original task: %+v", existing)
	}

	conflict := retry
	conflict.Goal = "different request"
	if _, err := store.CreateTask(ctx, conflict); !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}

	admitted, err := store.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, 1, domain.TaskAdmitted)
	if err != nil {
		t.Fatalf("admit task: %v", err)
	}
	if admitted.ResourceVersion != 2 {
		t.Fatalf("task version = %d, want 2", admitted.ResourceVersion)
	}
	if _, err := store.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, 1, domain.TaskCancelled); !errors.Is(err, kernelstore.ErrVersionConflict) {
		t.Fatalf("expected stale CAS rejection, got %v", err)
	}
	if _, err := store.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, 2, domain.TaskSucceeded); !errors.Is(err, kernelstore.ErrResultRequired) {
		t.Fatalf("expected direct-success rejection, got %v", err)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_type = 'Task'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 2 {
		t.Fatalf("outbox count = %d, want 2 (Task aggregate only; audit events are separate)", outboxCount)
	}
}

func TestConcurrentTaskCreationConvergesOnOneIdentity(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()
	const callers = 8
	start := make(chan struct{})
	results := make(chan kernelstore.CreateTaskResult, callers)
	errorsFound := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			result, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
				ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
				AgentVersionRef: "agent:v1", Goal: "concurrent idempotency",
				Spec: []byte(`{"model":"test"}`), IdempotencyKey: "concurrent-request",
			})
			if err != nil {
				errorsFound <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent create: %v", err)
	}
	var identity uuid.UUID
	created := 0
	count := 0
	for result := range results {
		count++
		if identity == uuid.Nil {
			identity = result.Task.ID
		}
		if result.Task.ID != identity {
			t.Errorf("tasks did not converge: got %s and %s", identity, result.Task.ID)
		}
		if !result.Existing {
			created++
		}
	}
	if count != callers || created != 1 {
		t.Fatalf("successful callers=%d new creations=%d, want %d and 1", count, created, callers)
	}
}

func TestCompletionCommitsResultBeforeTaskSuccess(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()
	task, run := createAdmittedRun(t, ctx, store, "completion")

	owned, err := store.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: task.TenantID, AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci", RuntimeInstanceID: "worker-1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire attempt: %v", err)
	}
	starting, err := store.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: task.TenantID, AttemptID: owned.Attempt.ID, FencingToken: owned.Attempt.FencingToken,
		ExpectedAttemptVersion: owned.Attempt.ResourceVersion, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	running, err := store.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: task.TenantID, AttemptID: starting.ID, FencingToken: starting.FencingToken,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}
	lease, err := store.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: task.TenantID, AttemptID: running.ID, FencingToken: running.FencingToken,
		ExpectedLeaseVersion: owned.Lease.ResourceVersion, TTL: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if lease.ResourceVersion != 2 {
		t.Fatalf("lease version = %d, want 2", lease.ResourceVersion)
	}
	completed, err := store.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: task.TenantID, AttemptID: running.ID, FencingToken: running.FencingToken,
		ExpectedAttemptVersion: running.ResourceVersion, To: domain.AttemptCompleted,
	})
	if err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	clock.Advance(3 * time.Minute)
	if _, err := store.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: task.TenantID, AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: owned.Run.ResourceVersion, RuntimeClass: "oci", RuntimeInstanceID: "worker-2", TTL: time.Minute,
	}); !errors.Is(err, kernelstore.ErrCompletionPending) {
		t.Fatalf("expected completed attempt to retain result-commit ownership, got %v", err)
	}

	var phase domain.TaskPhase
	var result *string
	if err := pool.QueryRow(ctx, `SELECT phase, result_ref FROM tasks WHERE id = $1`, task.ID.String()).Scan(&phase, &result); err != nil {
		t.Fatalf("read task before result commit: %v", err)
	}
	if phase != domain.TaskRunning || result != nil {
		t.Fatalf("task became terminal before result commit: phase=%s result=%v", phase, result)
	}

	finalRun, finalTask, err := store.CompleteRun(ctx, kernelstore.CompleteRunInput{
		TenantID: task.TenantID, RunID: run.ID, AttemptID: completed.ID, FencingToken: completed.FencingToken,
		ExpectedRunVersion: owned.Run.ResourceVersion, ResultRef: "cas://sha256/result-1",
	})
	if err != nil {
		t.Fatalf("commit run result: %v", err)
	}
	if finalRun.Phase != domain.RunCompleted || finalTask.Phase != domain.TaskSucceeded {
		t.Fatalf("unexpected completion: run=%s task=%s", finalRun.Phase, finalTask.Phase)
	}
	if finalRun.ResultRef != "cas://sha256/result-1" || finalTask.ResultRef != finalRun.ResultRef {
		t.Fatalf("result references diverged: run=%q task=%q", finalRun.ResultRef, finalTask.ResultRef)
	}
}

func TestExpiredLeaseIssuesNewFenceAndRejectsOldOwner(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()
	_, run := createAdmittedRun(t, ctx, store, "fencing")

	oldOwner, err := store.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: "tenant-a", AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci", RuntimeInstanceID: "worker-old", TTL: time.Second,
	})
	if err != nil {
		t.Fatalf("acquire old owner: %v", err)
	}
	clock.Advance(2 * time.Second)
	newOwner, err := store.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: "tenant-a", AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: oldOwner.Run.ResourceVersion, RuntimeClass: "oci", RuntimeInstanceID: "worker-new", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("reacquire expired run: %v", err)
	}
	if newOwner.Attempt.FencingToken <= oldOwner.Attempt.FencingToken {
		t.Fatalf("fencing token did not increase: old=%d new=%d", oldOwner.Attempt.FencingToken, newOwner.Attempt.FencingToken)
	}
	if _, err := store.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: oldOwner.Attempt.ID, FencingToken: oldOwner.Attempt.FencingToken,
		ExpectedLeaseVersion: oldOwner.Lease.ResourceVersion, TTL: time.Minute,
	}); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("expected stale heartbeat fencing, got %v", err)
	}
	if _, err := store.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: oldOwner.Attempt.ID, FencingToken: oldOwner.Attempt.FencingToken,
		ExpectedAttemptVersion: oldOwner.Attempt.ResourceVersion, To: domain.AttemptStarting,
	}); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("expected stale transition fencing, got %v", err)
	}
}

func TestControllersClaimAdmitAndScheduleAtomically(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	published := publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"runtimeClassPolicy":{"allowed":["oci"]}}`)
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "controller chain", IdempotencyKey: "controller-chain",
		Spec: []byte(`{
			"priority":70,
			"deadline":"2099-08-14T12:00:00Z",
			"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
			"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","dataResidency":"cn","artifactRegion":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
		}`),
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
		MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	admissionController := admission.NewController(repository, engine, testPolicyEngine(t), "admission-1", 10, time.Minute)
	processed, err := admissionController.Reconcile(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("admission reconcile processed=%d err=%v", processed, err)
	}
	admitted, err := repository.GetTask(ctx, "tenant-a", created.Task.ID)
	if err != nil {
		t.Fatalf("get admitted task: %v", err)
	}
	if admitted.Phase != domain.TaskAdmitted || admitted.AdmissionReasonCode != "ADMISSION_PASSED" {
		t.Fatalf("unexpected admission state: %+v", admitted)
	}
	if admitted.AgentVersionID == nil || *admitted.AgentVersionID != published.AgentVersion.ID {
		t.Fatalf("task was not bound to the published agent version: %+v", admitted)
	}
	var policyRevision string
	if err := pool.QueryRow(ctx, `SELECT policy_revision FROM admission_decisions
		WHERE tenant_id = $1 AND task_id = $2`, "tenant-a", admitted.ID.String()).Scan(&policyRevision); err != nil {
		t.Fatalf("read policy revision: %v", err)
	}
	if policyRevision != policy.Revision {
		t.Fatalf("policy revision = %q, want %q", policyRevision, policy.Revision)
	}

	fixturePools := staticPools{{
		ID: "pool-cn-1", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci", RuntimeInstanceID: "worker-cn-1",
		Region: "cn-east", DataResidency: "cn", Ready: true, AvailableCPU: 2000,
		AvailableMemory: 4096, AvailableLLMSlots: 4, ArtifactRegions: []string{"cn-east"},
	}}
	if err := repository.RegisterRuntimePools(ctx, fixturePools); err != nil {
		t.Fatalf("register fixture pools: %v", err)
	}
	schedulerController := scheduler.NewController(repository, fixturePools, "scheduler-1", 10, time.Minute, 30*time.Second)
	processed, err = schedulerController.Reconcile(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("scheduler reconcile processed=%d err=%v", processed, err)
	}
	running, err := repository.GetTask(ctx, "tenant-a", created.Task.ID)
	if err != nil {
		t.Fatalf("get running task: %v", err)
	}
	if running.Phase != domain.TaskRunning || running.ActiveRunID == nil {
		t.Fatalf("task was not scheduled: %+v", running)
	}
	var attempts, leases int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM attempts WHERE tenant_id = $1 AND runtime_pool_id = 'pool-cn-1'),
		(SELECT count(*) FROM runtime_leases WHERE tenant_id = $1 AND released_at IS NULL)`, "tenant-a").Scan(&attempts, &leases); err != nil {
		t.Fatalf("read scheduled objects: %v", err)
	}
	if attempts != 1 || leases != 1 {
		t.Fatalf("attempts=%d leases=%d, want 1/1", attempts, leases)
	}
}

func TestControllerClaimFencingAndOutboxOwnership(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: "claim fencing", Spec: []byte(`{}`), IdempotencyKey: "claim-fencing",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	oldClaims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "old", Limit: 1, TTL: time.Second,
	})
	if err != nil || len(oldClaims) != 1 {
		t.Fatalf("claim old controller: claims=%d err=%v", len(oldClaims), err)
	}
	clock.Advance(2 * time.Second)
	newClaims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "new", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(newClaims) != 1 {
		t.Fatalf("claim new controller: claims=%d err=%v", len(newClaims), err)
	}
	if newClaims[0].FencingToken <= oldClaims[0].FencingToken {
		t.Fatalf("claim token did not increase: old=%d new=%d", oldClaims[0].FencingToken, newClaims[0].FencingToken)
	}
	_, err = repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "old",
		ClaimFencingToken: oldClaims[0].FencingToken, ExpectedTaskVersion: 1,
		Admit: true, ReasonCode: "ADMISSION_PASSED", EvaluatorVersion: "test/v1",
	})
	if !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("expected stale controller fencing, got %v", err)
	}

	events, err := repository.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{DispatcherID: "dispatcher-a", Limit: 10, LockTTL: time.Minute})
	if err != nil {
		t.Fatalf("claim outbox: err=%v", err)
	}
	var taskEvents []kernelstore.OutboxEvent
	for _, event := range events {
		if event.AggregateType == "Task" {
			taskEvents = append(taskEvents, event)
		}
	}
	if len(taskEvents) != 1 {
		t.Fatalf("claim outbox: Task events=%d (total %d) err=%v", len(taskEvents), len(events), err)
	}
	events = taskEvents
	other, err := repository.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{DispatcherID: "dispatcher-b", Limit: 10, LockTTL: time.Minute})
	if err != nil || len(other) != 0 {
		t.Fatalf("second dispatcher stole event: events=%d err=%v", len(other), err)
	}
	if err := repository.MarkOutboxPublished(ctx, events[0].ID, "dispatcher-b", events[0].LockFencingToken, clock.Now()); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("expected outbox owner fencing, got %v", err)
	}
	clock.Advance(2 * time.Minute)
	if err := repository.MarkOutboxPublished(ctx, events[0].ID, "dispatcher-a", events[0].LockFencingToken, clock.Now()); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("expected expired outbox claim fencing, got %v", err)
	}
	reclaimed, err := repository.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{DispatcherID: "dispatcher-a", Limit: 10, LockTTL: time.Minute})
	if err != nil {
		t.Fatalf("reclaim outbox: err=%v", err)
	}
	var reclaimedTask []kernelstore.OutboxEvent
	for _, event := range reclaimed {
		if event.AggregateType == "Task" {
			reclaimedTask = append(reclaimedTask, event)
		}
	}
	if len(reclaimedTask) != 1 {
		t.Fatalf("reclaim outbox: Task events=%d (total %d) err=%v", len(reclaimedTask), len(reclaimed), err)
	}
	reclaimed = reclaimedTask
	if reclaimed[0].LockFencingToken <= events[0].LockFencingToken {
		t.Fatalf("outbox token did not increase: old=%d new=%d", events[0].LockFencingToken, reclaimed[0].LockFencingToken)
	}
	if err := repository.MarkOutboxPublished(ctx, events[0].ID, "dispatcher-a", events[0].LockFencingToken, clock.Now()); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("expected reused dispatcher ID to require the new token, got %v", err)
	}
	if err := repository.MarkOutboxPublished(ctx, reclaimed[0].ID, "dispatcher-a", reclaimed[0].LockFencingToken, clock.Now()); err != nil {
		t.Fatalf("publish owned event: %v", err)
	}
}

func TestOutboxClaimsOnlyTheNextAggregateVersion(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: "ordered outbox", Spec: []byte(`{}`), IdempotencyKey: "ordered-outbox",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim task: claims=%d err=%v", len(claims), err)
	}
	if _, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "admission",
		ClaimFencingToken: claims[0].FencingToken, ExpectedTaskVersion: 1,
		Admit: true, ReasonCode: "ADMISSION_PASSED", EvaluatorVersion: "test/v1",
	}); err != nil {
		t.Fatalf("admit task: %v", err)
	}
	first, err := repository.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{DispatcherID: "first", Limit: 10, LockTTL: time.Minute})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// Audit projection events share the outbox; the ordering rule applies per
	// aggregate, so filter to the Task aggregate for the version assertions.
	var taskClaims []kernelstore.OutboxEvent
	for _, event := range first {
		if event.AggregateType == "Task" {
			taskClaims = append(taskClaims, event)
		}
	}
	if len(taskClaims) != 1 || taskClaims[0].AggregateVersion != 1 {
		t.Fatalf("first Task aggregate claim=%+v err=%v", taskClaims, err)
	}
	if err := repository.MarkOutboxPublished(ctx, taskClaims[0].ID, "first", taskClaims[0].LockFencingToken, clock.Now()); err != nil {
		t.Fatalf("publish first version: %v", err)
	}
	second, err := repository.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{DispatcherID: "second", Limit: 10, LockTTL: time.Minute})
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	var secondTask []kernelstore.OutboxEvent
	for _, event := range second {
		if event.AggregateType == "Task" {
			secondTask = append(secondTask, event)
		}
	}
	if len(secondTask) != 1 || secondTask[0].AggregateVersion != 2 {
		t.Fatalf("second Task aggregate claim=%+v err=%v", secondTask, err)
	}
}

func createAdmittedRun(t *testing.T, ctx context.Context, store *postgresstore.Store, key string) (kernelstore.Task, kernelstore.Run) {
	t.Helper()
	created, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: key, Spec: []byte(`{"model":"test"}`), IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	admitted, err := store.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, created.Task.ResourceVersion, domain.TaskAdmitted)
	if err != nil {
		t.Fatalf("admit task: %v", err)
	}
	run, err := store.CreateRun(ctx, kernelstore.CreateRunInput{
		ID: uuid.New(), TenantID: admitted.TenantID, TaskID: admitted.ID, ExpectedTaskVersion: admitted.ResourceVersion,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	return admitted, run
}

func publishVersion(t *testing.T, ctx context.Context, repository *postgresstore.Store, tenantID, name, version, spec string) kernelstore.CreateAgentVersionResult {
	t.Helper()
	published, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: tenantID, Namespace: "default", Name: name, Version: version,
		Spec: []byte(spec),
	})
	if err != nil {
		t.Fatalf("publish agent version: %v", err)
	}
	return published
}

func testPolicyEngine(t *testing.T) *policy.Engine {
	t.Helper()
	engine, err := policy.New(policy.TenantPolicies{"tenant-a": {MaxPriority: 100}})
	if err != nil {
		t.Fatalf("prepare test policy engine: %v", err)
	}
	return engine
}

func prepare(t *testing.T, clock func() time.Time) (*pgxpool.Pool, *postgresstore.Store) {
	t.Helper()
	url := os.Getenv(testDatabaseEnvironment)
	if url == "" {
		t.Skipf("%s is not set", testDatabaseEnvironment)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	migrations := filepath.Join("..", "..", "..", "..", "db", "migrations")
	if _, err := migrate.Apply(ctx, pool, migrations); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE agent_service_instances, agent_services,
		task_usage_reservations,
		runtime_capacity_reservations, runtime_pool_capacities, runtime_pool_tenant_grants, runtime_pools,
		provider_circuit_breakers, workflow_usage_ledgers, workflow_steps, workflows,
		model_calls, model_descriptors, tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		audit_events,
		tenant_consumption_windows, tenant_quotas,
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	return pool, postgresstore.NewWithClock(pool, clock)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

type staticPools []scheduler.RuntimePool

func (s staticPools) ListRuntimePools(context.Context, string) ([]scheduler.RuntimePool, error) {
	return s, nil
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
