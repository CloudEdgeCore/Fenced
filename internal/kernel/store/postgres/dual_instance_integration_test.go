//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

const dualInstanceSpec = `{
	"priority":70,
	"deadline":"2099-08-14T12:00:00Z",
	"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
	"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","dataResidency":"cn","artifactRegion":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
}`

// TestDualControllerInstancesAdmitEachTaskExactlyOnce runs two admission
// controller instances against the same real Postgres queue and proves the
// claim fencing model: every task is admitted exactly once, no task is
// processed twice, no task is lost, and the outbox carries exactly one
// TaskAdmitted event per task.
func TestDualControllerInstancesAdmitEachTaskExactlyOnce(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"runtimeClassPolicy":{"allowed":["oci"]}}`)

	const tasks = 40
	for i := 0; i < tasks; i++ {
		if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
			Goal: "dual-instance", Spec: []byte(dualInstanceSpec),
			IdempotencyKey: "dual-instance-" + uuid.NewString()[:8],
		}); err != nil {
			t.Fatalf("create task %d: %v", i, err)
		}
	}

	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
		MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096, MaxLLMConcurrency: 4,
	})
	policyEngine := testPolicyEngine(t)

	// Two independent controller instances with distinct owner IDs race the
	// same queue for many rounds.
	controllers := []*admission.Controller{
		admission.NewController(repository, engine, policyEngine, "admission-a", 10, time.Minute),
		admission.NewController(repository, engine, policyEngine, "admission-b", 10, time.Minute),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(controllers))
	for _, controller := range controllers {
		wg.Add(1)
		go func(controller *admission.Controller) {
			defer wg.Done()
			for round := 0; round < 50; round++ {
				if _, err := controller.Reconcile(ctx); err != nil {
					errs <- err
					return
				}
			}
		}(controller)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("dual reconcile: %v", err)
	}

	// Every task reached ADMITTED exactly once; none are left queued.
	var admitted, queued, rejected int
	if err := pool.QueryRow(ctx, `SELECT
		COALESCE(SUM((phase = 'ADMITTED')::int), 0),
		COALESCE(SUM((phase = 'QUEUED')::int), 0),
		COALESCE(SUM((phase = 'REJECTED')::int), 0)
		FROM tasks WHERE tenant_id = $1`, "tenant-a").Scan(&admitted, &queued, &rejected); err != nil {
		t.Fatalf("aggregate task phases: %v", err)
	}
	if admitted != tasks || queued != 0 || rejected != 0 {
		t.Fatalf("phases: admitted=%d queued=%d rejected=%d, want %d/0/0", admitted, queued, rejected, tasks)
	}

	// Exactly one ADMIT decision per task: no task was decided twice.
	var decisions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM admission_decisions
		WHERE tenant_id = $1 AND decision = 'ADMIT'`, "tenant-a").Scan(&decisions); err != nil {
		t.Fatalf("count admit decisions: %v", err)
	}
	if decisions != tasks {
		t.Fatalf("admit decisions = %d, want %d (one per task)", decisions, tasks)
	}

	// The outbox carries exactly one TaskAdmitted event per task.
	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = $1 AND aggregate_type = 'Task' AND event_type = 'TaskAdmitted'`,
		"tenant-a").Scan(&events); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if events != tasks {
		t.Fatalf("TaskAdmitted outbox events = %d, want %d (one per task)", events, tasks)
	}

	// No lingering claims.
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_controller_claims
		WHERE tenant_id = $1 AND controller_kind = $2`, "tenant-a", kernelstore.ControllerAdmission).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 0 {
		t.Fatalf("lingering admission claims = %d, want 0", claims)
	}
}

// TestDualOutboxDispatchersPublishEachEventExactlyOnce runs two outbox
// dispatchers against the same event stream and proves the lock fencing
// model: every event is published exactly once, none is re-processed
// (fencing rejects) and none is dropped.
func TestDualOutboxDispatchersPublishEachEventExactlyOnce(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	const tasks = 30
	for i := 0; i < tasks; i++ {
		if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent:v1",
			Goal: "dual-outbox", Spec: []byte(`{}`), IdempotencyKey: "dual-outbox-" + uuid.NewString()[:8],
		}); err != nil {
			t.Fatalf("create task %d: %v", i, err)
		}
	}

	var totalEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tenant_id = $1`, "tenant-a").Scan(&totalEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if totalEvents < tasks {
		t.Fatalf("outbox events = %d, want >= %d", totalEvents, tasks)
	}

	var mu sync.Mutex
	var published, fenced int
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, dispatcherID := range []string{"dispatcher-a", "dispatcher-b"} {
		wg.Add(1)
		go func(dispatcherID string) {
			defer wg.Done()
			for {
				events, err := repository.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{
					DispatcherID: dispatcherID, Limit: 20, LockTTL: time.Minute,
				})
				if err != nil {
					errs <- err
					return
				}
				if len(events) == 0 {
					return
				}
				for _, event := range events {
					err := repository.MarkOutboxPublished(ctx, event.ID, dispatcherID, event.LockFencingToken, clock.Now())
					mu.Lock()
					if err != nil {
						fenced++
					} else {
						published++
					}
					mu.Unlock()
				}
			}
		}(dispatcherID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("dual dispatcher: %v", err)
	}

	// Zero re-process: every claimed event was published by its owner.
	if published != totalEvents {
		t.Fatalf("published = %d, want %d (every event exactly once)", published, totalEvents)
	}
	if fenced != 0 {
		t.Fatalf("fencing rejections = %d, want 0 (no re-processing)", fenced)
	}
	// Zero drop: nothing remains unpublished.
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = $1 AND published_at IS NULL`, "tenant-a").Scan(&remaining); err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("unpublished events = %d, want 0", remaining)
	}
}
