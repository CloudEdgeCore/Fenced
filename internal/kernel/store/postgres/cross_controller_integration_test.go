//go:build integration

package postgres_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestCrossControllerKindIsolation proves that different controller kinds
// (ADMISSION vs SCHEDULING) can claim the same task concurrently without
// conflict, while two controllers of the same kind cannot both claim the
// same task. This validates the (tenant, task, controller_kind) unique key
// on task_controller_claims. It then proves the recovery controller's
// fencing model: a live scheduler lease blocks recovery, and only an expired
// lease is recoverable.
func TestCrossControllerKindIsolation(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"runtimeClassPolicy":{"allowed":["oci"]}}`)

	// Create one task in QUEUED phase.
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "cross-controller", IdempotencyKey: "cross-ctrl",
		Spec: []byte(`{"priority":70,"budget":{"tokens":500},"placement":{"runtimeClasses":["oci"]}}`),
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	// Phase A: four Admission controllers race to claim the same queued task.
	// Only one claim should win (same controller kind is mutually exclusive).
	var mu sync.Mutex
	admissionClaims := 0
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
				Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued,
				OwnerID: "admission-" + strconv.Itoa(id), Limit: 10, TTL: 30 * time.Second,
			})
			if err != nil {
				return
			}
			mu.Lock()
			admissionClaims += len(claims)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if admissionClaims != 1 {
		t.Fatalf("exactly one admission claim expected, got %d", admissionClaims)
	}

	// Phase B: advance past the Phase A claim TTL, then admit the task.
	clock.Advance(31 * time.Second)

	// Phase C: admit the task, then race four Scheduling controllers for the
	// same task — only one scheduling claim should win.
	admitClaims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued,
		OwnerID: "admission-final", Limit: 1, TTL: 30 * time.Second,
	})
	if err != nil || len(admitClaims) != 1 {
		t.Fatalf("admission claim: claims=%d err=%v", len(admitClaims), err)
	}
	if _, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: "tenant-a", OwnerID: "admission-final",
		ClaimFencingToken: admitClaims[0].FencingToken, ExpectedTaskVersion: created.Task.ResourceVersion,
		Admit: true, ReasonCode: "ADMISSION_PASSED", EvaluatorVersion: "test/v1",
	}); err != nil {
		t.Fatalf("admit: %v", err)
	}

	schedulingClaims := 0
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
				Kind: kernelstore.ControllerScheduling, Phase: domain.TaskAdmitted,
				OwnerID: "scheduler-" + strconv.Itoa(id), Limit: 10, TTL: 30 * time.Second,
			})
			if err != nil {
				return
			}
			mu.Lock()
			schedulingClaims += len(claims)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if schedulingClaims != 1 {
		t.Fatalf("exactly one scheduling claim expected, got %d", schedulingClaims)
	}

	// Phase D: schedule the task and create a live runtime lease, then verify
	// the recovery controller cannot recover it while the lease is live.
	assignment := scheduleRuntimeTask(t, ctx, repository, "cross-ctrl-schedule", 2)
	clock.Advance(5 * time.Second)
	expired, err := repository.ListExpiredAttempts(ctx, clock.Now(), 10)
	if err != nil {
		t.Fatalf("list expired: %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("expected 0 expired attempts (lease live), got %d", len(expired))
	}

	// Phase E: after the runtime lease expires, the same attempt becomes a
	// recovery candidate for takeover.
	clock.Advance(5 * time.Minute)
	expired, err = repository.ListExpiredAttempts(ctx, clock.Now(), 10)
	if err != nil {
		t.Fatalf("list expired after TTL: %v", err)
	}
	if len(expired) == 0 {
		t.Fatal("expected expired attempts after lease TTL")
	}
	found := false
	for _, candidate := range expired {
		if candidate.AttemptID == assignment.Attempt.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("scheduled attempt %s not among expired candidates", assignment.Attempt.ID)
	}
}
