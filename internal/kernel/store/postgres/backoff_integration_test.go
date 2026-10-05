//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// createAdmittedTask creates a task and admits it through the real claim +
// DecideAdmission path.
func createAdmittedTask(t *testing.T, ctx context.Context, repository interface {
	CreateTask(context.Context, kernelstore.CreateTaskInput) (kernelstore.CreateTaskResult, error)
	ClaimTasks(context.Context, kernelstore.ClaimTasksInput) ([]kernelstore.TaskClaim, error)
	DecideAdmission(context.Context, kernelstore.DecideAdmissionInput) (kernelstore.Task, error)
}, key string) kernelstore.Task {
	t.Helper()
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: key, Spec: []byte(`{}`), IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim admission: claims=%d err=%v", len(claims), err)
	}
	admitted, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "admission",
		ClaimFencingToken: claims[0].FencingToken, ExpectedTaskVersion: created.Task.ResourceVersion,
		Admit: true, ReasonCode: "ADMISSION_PASSED", EvaluatorVersion: "test/v1",
	})
	if err != nil {
		t.Fatalf("admit task: %v", err)
	}
	return admitted
}

// TestNoPlacementDeferralGatesClaimsAndResetsOnSchedule proves the O6 loop:
// deferral releases the claim and records a backoff deadline on the task,
// scheduling claims honor the deadline, and a successful placement resets
// the deferral state. The deferral is also visible in the outbox and audit
// ledger.
func TestNoPlacementDeferralGatesClaimsAndResetsOnSchedule(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	if err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{{
		ID: "pool-1", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-1", Region: "cn-east", Ready: true, Status: "ACTIVE",
		AvailableCPU: 1000, AvailableMemory: 1024, AvailableLLMSlots: 1,
	}}); err != nil {
		t.Fatalf("register pool: %v", err)
	}
	task := createAdmittedTask(t, ctx, repository, "backoff-loop")

	claim, err := claimScheduling(t, ctx, repository, "scheduler-1")
	if err != nil {
		t.Fatalf("claim scheduling: %v", err)
	}
	until := clock.Now().Add(30 * time.Second)
	deferred, err := repository.DeferTaskSchedule(ctx, kernelstore.DeferTaskScheduleInput{
		TaskID: task.ID, TenantID: "tenant-a", OwnerID: "scheduler-1",
		ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: task.ResourceVersion,
		Until: until,
	})
	if err != nil {
		t.Fatalf("defer schedule: %v", err)
	}
	if deferred.ScheduleRetryCount != 1 || deferred.ResourceVersion != task.ResourceVersion+1 {
		t.Fatalf("unexpected deferred task: retry=%d version=%d", deferred.ScheduleRetryCount, deferred.ResourceVersion)
	}
	if deferred.NextScheduleAttemptAt == nil || !deferred.NextScheduleAttemptAt.Equal(until) {
		t.Fatalf("deferral deadline = %v, want %v", deferred.NextScheduleAttemptAt, until)
	}

	// The claim is gone and the task is not claimable before the deadline.
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerScheduling, Phase: domain.TaskAdmitted, OwnerID: "scheduler-2", Limit: 10, TTL: time.Minute,
	})
	if err != nil || len(claims) != 0 {
		t.Fatalf("claim before deadline: claims=%d err=%v", len(claims), err)
	}
	var claimRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_controller_claims
		WHERE tenant_id = $1 AND task_id = $2 AND controller_kind = $3`,
		"tenant-a", task.ID.String(), kernelstore.ControllerScheduling).Scan(&claimRows); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claimRows != 0 {
		t.Fatalf("claim rows = %d, want 0 (claim released immediately)", claimRows)
	}

	// The deferral is observable: outbox event + audit row + version bump.
	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = $1 AND aggregate_id = $2 AND event_type = 'TaskScheduleDeferred'`,
		"tenant-a", task.ID.String()).Scan(&events); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if events != 1 {
		t.Fatalf("deferral outbox events = %d, want 1", events)
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_events
		WHERE tenant_id = $1 AND resource_id = $2 AND event_type = 'task.schedule.deferred'`,
		"tenant-a", task.ID.String()).Scan(&audits); err != nil {
		t.Fatalf("count audit: %v", err)
	}
	if audits != 1 {
		t.Fatalf("deferral audit events = %d, want 1", audits)
	}

	// After the deadline the task is claimable again and scheduling resets
	// the deferral state.
	clock.Advance(31 * time.Second)
	claim, err = claimScheduling(t, ctx, repository, "scheduler-2")
	if err != nil {
		t.Fatalf("claim after deadline: %v", err)
	}
	scheduled, err := repository.ScheduleTask(ctx, kernelstore.ScheduleTaskInput{
		TaskID: task.ID, TenantID: "tenant-a", OwnerID: "scheduler-2",
		ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: deferred.ResourceVersion,
		RunID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
		RuntimePoolID: "pool-1", RuntimeClass: "oci", RuntimeInstanceID: "worker-1", LeaseTTL: time.Minute,
		RequestedCPU: 100, RequestedMemory: 128, RequestedLLMSlots: 1,
	})
	if err != nil {
		t.Fatalf("schedule task: %v", err)
	}
	if scheduled.Run.Phase != domain.RunRunning {
		t.Fatalf("unexpected run phase: %s", scheduled.Run.Phase)
	}
	after, err := repository.GetTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get scheduled task: %v", err)
	}
	if after.Phase != domain.TaskRunning || after.ScheduleRetryCount != 0 || after.NextScheduleAttemptAt != nil {
		t.Fatalf("deferral state not reset on placement: %+v", after)
	}
}

// TestDeferTaskScheduleRequiresOwnershipAndAdmittedPhase proves the deferral
// is fenced like every claim mutation: a stale owner is rejected and a task
// outside the admitted phase cannot be deferred.
func TestDeferTaskScheduleRequiresOwnershipAndAdmittedPhase(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	task := createAdmittedTask(t, ctx, repository, "backoff-fenced")

	claim, err := claimScheduling(t, ctx, repository, "scheduler-1")
	if err != nil {
		t.Fatalf("claim scheduling: %v", err)
	}
	stale := kernelstore.DeferTaskScheduleInput{
		TaskID: task.ID, TenantID: "tenant-a", OwnerID: "scheduler-1",
		ClaimFencingToken: claim.FencingToken + 1, ExpectedTaskVersion: task.ResourceVersion,
		Until: clock.Now().Add(time.Minute),
	}
	if _, err := repository.DeferTaskSchedule(ctx, stale); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("stale owner deferral = %v, want fenced", err)
	}
	current, err := repository.GetTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if current.ScheduleRetryCount != 0 || current.NextScheduleAttemptAt != nil {
		t.Fatalf("stale deferral mutated the task: %+v", current)
	}

	// A deferral is bound to a scheduling-kind claim: presenting an
	// admission-kind claim token is fenced even though the task row exists.
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
		Goal: "backoff-queued", Spec: []byte(`{}`), IdempotencyKey: "backoff-queued",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	admissionClaims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(admissionClaims) != 1 {
		t.Fatalf("claim queued task: claims=%d err=%v", len(admissionClaims), err)
	}
	if _, err := repository.DeferTaskSchedule(ctx, kernelstore.DeferTaskScheduleInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "admission",
		ClaimFencingToken: admissionClaims[0].FencingToken, ExpectedTaskVersion: created.Task.ResourceVersion,
		Until: clock.Now().Add(time.Minute),
	}); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("admission-kind claim deferral = %v, want fenced (claim kind mismatch)", err)
	}
}

// TestDeferralRetryCountEscalatesAcrossDeferrals proves the retry count
// accumulates on the task across consecutive no-placements, which drives the
// exponential backoff progression shared by every controller instance.
func TestDeferralRetryCountEscalatesAcrossDeferrals(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	task := createAdmittedTask(t, ctx, repository, "backoff-escalate")

	for want := int64(1); want <= 3; want++ {
		claim, err := claimScheduling(t, ctx, repository, "scheduler-1")
		if err != nil {
			t.Fatalf("claim scheduling round %d: %v", want, err)
		}
		deferred, err := repository.DeferTaskSchedule(ctx, kernelstore.DeferTaskScheduleInput{
			TaskID: task.ID, TenantID: "tenant-a", OwnerID: "scheduler-1",
			ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: task.ResourceVersion + want - 1,
			Until: clock.Now().Add(time.Duration(want) * time.Minute),
		})
		if err != nil {
			t.Fatalf("defer round %d: %v", want, err)
		}
		if deferred.ScheduleRetryCount != want {
			t.Fatalf("retry count = %d, want %d", deferred.ScheduleRetryCount, want)
		}
		clock.Advance(time.Duration(want)*time.Minute + time.Second)
	}
}

func claimScheduling(t *testing.T, ctx context.Context, repository interface {
	ClaimTasks(context.Context, kernelstore.ClaimTasksInput) ([]kernelstore.TaskClaim, error)
}, owner string) (kernelstore.TaskClaim, error) {
	t.Helper()
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerScheduling, Phase: domain.TaskAdmitted, OwnerID: owner, Limit: 1, TTL: time.Minute,
	})
	if err != nil {
		return kernelstore.TaskClaim{}, err
	}
	if len(claims) != 1 {
		return kernelstore.TaskClaim{}, errors.New("no scheduling claim available")
	}
	return claims[0], nil
}
