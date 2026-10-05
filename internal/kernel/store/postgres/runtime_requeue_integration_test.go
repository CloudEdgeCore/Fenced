//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// A cleanly failed attempt of a multi-attempt task must requeue the task for
// a fresh dispatch instead of stranding it RUNNING with no active attempt
// (the recovery controller only scans unreleased leases).
func TestFailedAttemptWithRetriesRequeuesTask(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "runtime-requeue", 3)

	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: 1, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}
	failed, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: running.ResourceVersion, To: domain.AttemptFailed,
		FailureCode: "ADAPTER_FAILED", FailureMessage: "injected adapter failure",
	})
	if err != nil {
		t.Fatalf("fail attempt (requeue expected): %v", err)
	}
	if failed.Phase != domain.AttemptFailed {
		t.Fatalf("attempt phase = %s", failed.Phase)
	}
	var taskPhase string
	var activeRun *string
	if err := pool.QueryRow(ctx,
		`SELECT phase::text, active_run_id::text FROM tasks WHERE tenant_id = 'tenant-a' AND id = $1`,
		assignment.Task.ID).Scan(&taskPhase, &activeRun); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if taskPhase != "QUEUED" || activeRun != nil {
		t.Fatalf("task after failure: phase=%s activeRun=%v, want QUEUED with no active run", taskPhase, activeRun)
	}
	// The released lease must fence the old owner out of further updates.
	if _, err := repository.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedLeaseVersion: 1, TTL: time.Minute,
	}); !errors.Is(err, kernelstore.ErrFenced) {
		t.Fatalf("released owner still holds a heartbeat: %v", err)
	}

	// The standard pipeline re-dispatches the requeued task as attempt two.
	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
		MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	pools := staticPools{{
		ID: "pool-cn-1", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci", RuntimeInstanceID: "worker-cn-1",
		Region: "cn-east", Ready: true, AvailableCPU: 100_000, AvailableMemory: 1_000_000, AvailableLLMSlots: 1_000,
	}}
	if count, err := admission.NewController(repository, engine, testPolicyEngine(t), "admission-requeue", 10, time.Minute).
		Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("readmission count=%d err=%v", count, err)
	}
	if count, err := scheduler.NewController(repository, pools, "scheduler-requeue", 10, time.Minute, 30*time.Second).
		Reconcile(ctx); err != nil || count != 1 {
		t.Fatalf("replace scheduler count=%d err=%v", count, err)
	}
	retried, err := repository.PollRuntimeAssignment(ctx, "tenant-a", "worker-cn-1")
	if err != nil {
		t.Fatalf("poll retried assignment: %v", err)
	}
	// Ordinals are per-run: a requeued task gets a fresh run (ordinal two of
	// the task) carrying its own first attempt.
	if retried.Run.Ordinal != 2 || retried.Attempt.Ordinal != 1 {
		t.Fatalf("retry assignment run=%d attempt=%d, want run 2 attempt 1", retried.Run.Ordinal, retried.Attempt.Ordinal)
	}
	if retried.Task.Phase != domain.TaskRunning {
		t.Fatalf("retried task phase = %s", retried.Task.Phase)
	}
}

// Exhausting the retry budget finalizes the task as FAILED instead of
// looping forever.
func TestExhaustedRetriesFinalizeTask(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "runtime-exhaustion", 2)

	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: 1, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}
	for round := 0; round < 2; round++ {
		if _, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
			TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
			ExpectedAttemptVersion: running.ResourceVersion, To: domain.AttemptFailed,
			FailureCode: "X",
		}); err != nil {
			t.Fatalf("failure %d: %v", round+1, err)
		}
		if round == 0 {
			var taskPhase string
			if err := pool.QueryRow(ctx,
				`SELECT phase::text FROM tasks WHERE tenant_id='tenant-a' AND id=$1`, assignment.Task.ID).Scan(&taskPhase); err != nil {
				t.Fatalf("reload task: %v", err)
			}
			if taskPhase != "QUEUED" {
				t.Fatalf("phase after first failure = %s, want QUEUED", taskPhase)
			}
			// Simulate the pipeline placing attempt two on the same task.
			if count, err := admission.NewController(repository, admission.New(admission.Limits{
				RuntimeClasses: []string{"oci"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
				MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
			}), testPolicyEngine(t), "admission-exhaust", 10, time.Minute).Reconcile(ctx); err != nil || count != 1 {
				t.Fatalf("readmission count=%d err=%v", count, err)
			}
			pools := staticPools{{
				ID: "pool-cn-1", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci", RuntimeInstanceID: "worker-cn-1",
				Region: "cn-east", Ready: true, AvailableCPU: 100_000, AvailableMemory: 1_000_000, AvailableLLMSlots: 1_000,
			}}
			if count, err := scheduler.NewController(repository, pools, "scheduler-exhaust", 10, time.Minute, 30*time.Second).
				Reconcile(ctx); err != nil || count != 1 {
				t.Fatalf("rescheduler count=%d err=%v", count, err)
			}
			retried, err := repository.PollRuntimeAssignment(ctx, "tenant-a", "worker-cn-1")
			if err != nil {
				t.Fatalf("poll attempt two: %v", err)
			}
			starting2, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
				TenantID: "tenant-a", AttemptID: retried.Attempt.ID, FencingToken: retried.Attempt.FencingToken,
				ExpectedAttemptVersion: retried.Attempt.ResourceVersion, To: domain.AttemptStarting,
			})
			if err != nil {
				t.Fatalf("start attempt two: %v", err)
			}
			running = starting2
			assignment.Attempt = retried.Attempt
		}
	}
	var taskPhase string
	if err := pool.QueryRow(ctx,
		`SELECT phase::text FROM tasks WHERE tenant_id='tenant-a' AND id=$1`, assignment.Task.ID).Scan(&taskPhase); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if taskPhase != "FAILED" {
		t.Fatalf("phase after exhausting retries = %s, want FAILED", taskPhase)
	}
}
