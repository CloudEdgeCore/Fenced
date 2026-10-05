//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func createV13Workflow(t *testing.T, ctx context.Context, repository kernelstore.WorkflowStore, tenant, key string, options ...func(*kernelstore.CreateWorkflowInput)) kernelstore.Workflow {
	t.Helper()
	input := kernelstore.CreateWorkflowInput{
		ID: uuid.New(), TenantID: tenant, Namespace: "default", IdempotencyKey: key,
		Goal: "dynamic orchestration", Spec: []byte(`{"steps":[{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{}}]}`),
		Steps: []kernelstore.CreateWorkflowStepInput{{Name: "planner", AgentVersionRef: "planner@1", Goal: "plan", Spec: []byte(`{}`)}},
	}
	for _, option := range options {
		option(&input)
	}
	created, err := repository.CreateWorkflow(ctx, input)
	if err != nil {
		t.Fatalf("create workflow %s/%s: %v", tenant, key, err)
	}
	return created.Workflow
}

func TestV13SpawnGuardsAreTransactionalAndIdempotent(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "spawn-guards")
	if _, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
		ParentStepName: "planner", Name: "disabled", Goal: "must not run", AgentVersionRef: "worker@1",
		Spec: []byte(`{}`), IdempotencyKey: "disabled", Arguments: []byte(`{}`),
	}); !errors.Is(err, kernelstore.ErrSpawnDenied) {
		t.Fatalf("default-disabled spawn error = %v", err)
	} else if code, _ := kernelstore.DenialCode(err); code != "SPAWN_DISABLED" {
		t.Fatalf("default-disabled denial code = %q", code)
	}
	guards := kernelstore.SpawnGuards{
		Enabled: true, MaxDynamicSteps: 2, MaxChildrenPerStep: 2, MaxSpawnDepth: 1, MaxWorkflowSteps: 3,
	}
	spawn := func(name, parent, key, arguments string) (kernelstore.SpawnWorkflowStepResult, error) {
		return repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
			ParentStepName: parent, Name: name, Goal: "execute " + name, AgentVersionRef: "worker@1",
			Spec: []byte(`{}`), MaxAttempts: 2, Guards: guards,
			IdempotencyKey: key, Arguments: []byte(arguments),
		})
	}

	first, err := spawn("child-a", "planner", "call-a", `{"n":1}`)
	if err != nil || !first.Created || first.Step.SpawnDepth != 1 {
		t.Fatalf("first spawn = %+v err=%v", first, err)
	}
	replay, err := spawn("child-a", "planner", "call-a", `{"n":1}`)
	if err != nil || replay.Created || replay.Step.ID != first.Step.ID {
		t.Fatalf("spawn replay = %+v err=%v", replay, err)
	}
	if _, err := spawn("child-a", "planner", "different-call", `{"n":2}`); !errors.Is(err, kernelstore.ErrSpawnDenied) {
		t.Fatalf("duplicate name error = %v, want ErrSpawnDenied", err)
	} else if code, _ := kernelstore.DenialCode(err); code != "SPAWN_NAME_CONFLICT" {
		t.Fatalf("duplicate name denial code = %q", code)
	}
	if _, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflow.ID, TenantID: "tenant-other", WorkflowVersion: workflow.ResourceVersion,
		ParentStepName: "planner", Name: "cross-tenant", Goal: "must not run", AgentVersionRef: "worker@1",
		Spec: []byte(`{}`), MaxAttempts: 1, Guards: guards,
		IdempotencyKey: "cross-tenant", Arguments: []byte(`{}`),
	}); !errors.Is(err, kernelstore.ErrWorkflowNotFound) {
		t.Fatalf("cross-tenant spawn error = %v, want ErrWorkflowNotFound", err)
	}
	if _, err := spawn("grandchild", "child-a", "call-deep", `{}`); !errors.Is(err, kernelstore.ErrSpawnDenied) {
		t.Fatalf("recursive spawn error = %v, want ErrSpawnDenied", err)
	} else if code, _ := kernelstore.DenialCode(err); code != "SPAWN_DEPTH_EXCEEDED" {
		t.Fatalf("recursive denial code = %q", code)
	}
	if _, err := spawn("child-b", "planner", "call-b", `{}`); err != nil {
		t.Fatalf("second child: %v", err)
	}
	if _, err := spawn("child-c", "planner", "call-c", `{}`); !errors.Is(err, kernelstore.ErrSpawnDenied) {
		t.Fatalf("task explosion error = %v, want ErrSpawnDenied", err)
	} else if code, _ := kernelstore.DenialCode(err); code != "SPAWN_TOTAL_STEPS_EXCEEDED" && code != "SPAWN_DYNAMIC_LIMIT_EXCEEDED" {
		t.Fatalf("task explosion denial code = %q", code)
	}
	steps, err := repository.ListWorkflowSteps(ctx, workflow.TenantID, workflow.ID)
	if err != nil || len(steps) != 3 {
		t.Fatalf("steps after guarded spawns = %d err=%v", len(steps), err)
	}
}

func TestV13ConcurrentSpawnCreatesExactlyOneStep(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "concurrent-spawn")
	input := kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
		ParentStepName: "planner", Name: "shared-child", Goal: "execute once", AgentVersionRef: "worker@1",
		Spec: []byte(`{}`), MaxAttempts: 2,
		Guards:         kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 10, MaxChildrenPerStep: 10, MaxSpawnDepth: 2, MaxWorkflowSteps: 11},
		IdempotencyKey: "same-call", Arguments: []byte(`{"work":"same"}`),
	}

	const callers = 24
	type outcome struct {
		result kernelstore.SpawnWorkflowStepResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, callers)
	var workers sync.WaitGroup
	for index := 0; index < callers; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			result, err := repository.SpawnWorkflowStep(ctx, input)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(outcomes)

	created := 0
	var stepID uuid.UUID
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("concurrent spawn: %v", outcome.err)
		}
		if outcome.result.Created {
			created++
		}
		if stepID == uuid.Nil {
			stepID = outcome.result.Step.ID
		} else if outcome.result.Step.ID != stepID {
			t.Fatalf("idempotent callers observed different steps: %s and %s", stepID, outcome.result.Step.ID)
		}
	}
	if created != 1 {
		t.Fatalf("created results = %d, want exactly 1", created)
	}
	steps, err := repository.ListWorkflowSteps(ctx, workflow.TenantID, workflow.ID)
	if err != nil || len(steps) != 2 {
		t.Fatalf("workflow steps after concurrent spawn = %d err=%v", len(steps), err)
	}
}

func TestWorkflowApprovalPreservesActorDecisionAndLifecycleEvidence(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-evidence", "workflow-evidence")
	steps, err := repository.ListWorkflowSteps(ctx, workflow.TenantID, workflow.ID)
	if err != nil || len(steps) != 1 {
		t.Fatalf("list initial step: len=%d err=%v", len(steps), err)
	}
	waiting, err := repository.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
		TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: steps[0].Name,
		ExpectedVersion: steps[0].ResourceVersion, To: kernelstore.StepWaitingApproval,
	})
	if err != nil {
		t.Fatal(err)
	}
	decided, err := repository.DecideWorkflowStepApproval(ctx, kernelstore.DecideWorkflowStepApprovalInput{
		TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: waiting.Name,
		ExpectedVersion: waiting.ResourceVersion, Approved: false, DecidedBy: "user:reviewer-42",
	})
	if err != nil {
		t.Fatal(err)
	}
	if decided.DecidedBy != "user:reviewer-42" || decided.ApprovalDecision != "rejected" {
		t.Fatalf("approval identity = actor %q decision %q", decided.DecidedBy, decided.ApprovalDecision)
	}
	var lifecycleEvents, auditEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = $1 AND payload->>'workflowId' = $2`, workflow.TenantID, workflow.ID.String()).Scan(&lifecycleEvents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_events
		WHERE tenant_id = $1 AND (resource_id = $2::uuid OR details->>'workflowId' = $3)`,
		workflow.TenantID, workflow.ID.String(), workflow.ID.String()).Scan(&auditEvents); err != nil {
		t.Fatal(err)
	}
	if lifecycleEvents < 3 || auditEvents < 3 {
		t.Fatalf("workflow evidence incomplete: outbox=%d audit=%d", lifecycleEvents, auditEvents)
	}
}

func TestV13SpawnStopsAtWorkflowBudgetAndDeadline(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	budgeted := createV13Workflow(t, ctx, repository, "tenant-v13", "budget-stop", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTasks = 1
	})
	steps, err := repository.ListWorkflowSteps(ctx, budgeted.TenantID, budgeted.ID)
	if err != nil || len(steps) != 1 {
		t.Fatalf("list budget workflow steps: len=%d err=%v", len(steps), err)
	}
	if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: budgeted.TenantID, Namespace: "default", AgentVersionRef: "planner@1",
		Goal: "already consumed", Spec: []byte(`{}`), IdempotencyKey: fmt.Sprintf("workflow/%s/planner/1", budgeted.ID),
		WorkflowID: &budgeted.ID, WorkflowStepID: &steps[0].ID, WorkflowStepName: steps[0].Name, WorkflowAttempt: 1,
	}); err != nil {
		t.Fatalf("seed workflow task: %v", err)
	}
	_, err = repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: budgeted.ID, TenantID: budgeted.TenantID, WorkflowVersion: budgeted.ResourceVersion,
		ParentStepName: "planner", Name: "over-budget", Goal: "must not run", AgentVersionRef: "worker@1",
		Spec: []byte(`{}`), Guards: kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 10, MaxChildrenPerStep: 10}, IdempotencyKey: "budget", Arguments: []byte(`{}`),
	})
	if code, ok := kernelstore.DenialCode(err); !ok || code != "SPAWN_BUDGET_EXHAUSTED" {
		t.Fatalf("budget denial = %q err=%v", code, err)
	}

	past := clock.Now().Add(-time.Second)
	expired := createV13Workflow(t, ctx, repository, "tenant-v13", "deadline-stop", func(input *kernelstore.CreateWorkflowInput) {
		input.DeadlineAt = &past
	})
	_, err = repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: expired.ID, TenantID: expired.TenantID, WorkflowVersion: expired.ResourceVersion,
		ParentStepName: "planner", Name: "too-late", Goal: "must not run", AgentVersionRef: "worker@1",
		Spec: []byte(`{}`), Guards: kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 10, MaxChildrenPerStep: 10}, IdempotencyKey: "deadline", Arguments: []byte(`{}`),
	})
	if code, ok := kernelstore.DenialCode(err); !ok || code != "SPAWN_DEADLINE_EXCEEDED" {
		t.Fatalf("deadline denial = %q err=%v", code, err)
	}
}

func TestV13WorkflowClaimsAreFairExclusiveAndRecoverable(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	createV13Workflow(t, ctx, repository, "tenant-a", "claim-a", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTokens = 1000 // budgeted workflows must remain claimable
	})
	createV13Workflow(t, ctx, repository, "tenant-b", "claim-b")

	claimed, err := repository.ClaimWorkflows(ctx, kernelstore.ClaimWorkflowsInput{Owner: "orchestrator-a", Batch: 10, Lease: time.Minute})
	if err != nil || len(claimed) != 2 {
		t.Fatalf("first claims = %d err=%v", len(claimed), err)
	}
	seen := map[string]bool{}
	for _, workflow := range claimed {
		seen[workflow.TenantID] = true
	}
	if !seen["tenant-a"] || !seen["tenant-b"] {
		t.Fatalf("round-robin claims lost a tenant: %v", seen)
	}
	contended, err := repository.ClaimWorkflows(ctx, kernelstore.ClaimWorkflowsInput{Owner: "orchestrator-b", Batch: 10, Lease: time.Minute})
	if err != nil || len(contended) != 0 {
		t.Fatalf("live claims were stolen: %d err=%v", len(contended), err)
	}
	clock.Advance(2 * time.Minute)
	recovered, err := repository.ClaimWorkflows(ctx, kernelstore.ClaimWorkflowsInput{Owner: "orchestrator-b", Batch: 10, Lease: time.Minute})
	if err != nil || len(recovered) != 2 {
		t.Fatalf("expired claim recovery = %d err=%v", len(recovered), err)
	}
}

func TestV13WorkflowClaimsRotateAcrossMoreThanOneBatchOfTenants(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	const tenants = 120
	for index := 0; index < tenants; index++ {
		createV13Workflow(t, ctx, repository, fmt.Sprintf("tenant-%03d", index), fmt.Sprintf("rotate-%03d", index))
	}
	seen := map[string]bool{}
	for round := 0; round < 2; round++ {
		claimed, err := repository.ClaimWorkflows(ctx, kernelstore.ClaimWorkflowsInput{
			Owner: "orchestrator-a", Batch: 100, Lease: time.Minute,
		})
		if err != nil {
			t.Fatalf("claim round %d: %v", round, err)
		}
		for _, workflow := range claimed {
			seen[workflow.TenantID] = true
		}
	}
	if len(seen) != tenants {
		t.Fatalf("two fair claim rounds covered %d/%d tenants", len(seen), tenants)
	}
}

func TestV13WorkflowClaimsFillBatchRoundRobinAcrossTenantBacklogs(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	for index := 0; index < 4; index++ {
		createV13Workflow(t, ctx, repository, "tenant-a", fmt.Sprintf("backlog-a-%d", index))
		createV13Workflow(t, ctx, repository, "tenant-b", fmt.Sprintf("backlog-b-%d", index))
	}
	claimed, err := repository.ClaimWorkflows(ctx, kernelstore.ClaimWorkflowsInput{
		Owner: "orchestrator-a", Batch: 6, Lease: time.Minute,
	})
	if err != nil || len(claimed) != 6 {
		t.Fatalf("round-robin batch = %d err=%v", len(claimed), err)
	}
	counts := map[string]int{}
	for _, workflow := range claimed {
		counts[workflow.TenantID]++
	}
	if counts["tenant-a"] != 3 || counts["tenant-b"] != 3 {
		t.Fatalf("round-robin backlog distribution = %v", counts)
	}
}

func TestV13DynamicSpawnScale10K(t *testing.T) {
	if os.Getenv("FENCED_V13_SCALE_TEST") != "1" {
		t.Skip("set FENCED_V13_SCALE_TEST=1 to run the 10k dynamic-step acceptance leg")
	}
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-scale", "dynamic-10k")
	guards := kernelstore.SpawnGuards{
		Enabled: true, MaxDynamicSteps: 10_000, MaxChildrenPerStep: 10_000,
		MaxSpawnDepth: 1, MaxWorkflowSteps: 10_001,
	}
	started := time.Now()
	for index := 0; index < 10_000; index++ {
		name := fmt.Sprintf("child-%05d", index)
		result, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
			ParentStepName: "planner", Name: name, Goal: "execute scale child", AgentVersionRef: "worker@1",
			Spec: []byte(`{"priority":50}`), MaxAttempts: 1, Guards: guards,
			IdempotencyKey: name, Arguments: []byte(fmt.Sprintf(`{"index":%d}`, index)),
		})
		if err != nil || !result.Created {
			t.Fatalf("spawn %d: created=%v err=%v", index, result.Created, err)
		}
	}
	elapsed := time.Since(started)
	steps, err := repository.ListWorkflowSteps(ctx, workflow.TenantID, workflow.ID)
	if err != nil || len(steps) != 10_001 {
		t.Fatalf("10k scale steps = %d err=%v", len(steps), err)
	}
	if elapsed > v13SpawnScaleBound {
		t.Fatalf("10k dynamic spawns took %s, exceeds %s local acceptance bound", elapsed, v13SpawnScaleBound)
	}
	t.Logf("10k dynamic spawns committed in %s (%.1f spawns/s)", elapsed, 10_000/elapsed.Seconds())
}
