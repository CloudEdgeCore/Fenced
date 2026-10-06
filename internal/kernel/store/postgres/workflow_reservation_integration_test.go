//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelmoney "github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	kernelworkflow "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// workflowReservation reads the workflow usage ledger's step-reservation
// columns for direct invariant assertions.
func workflowReservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string, workflowID uuid.UUID) (tasks, tokens, costMicroUSD int64) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT step_reserved_tasks, step_reserved_tokens, step_reserved_cost_micro_usd
		FROM workflow_usage_ledgers WHERE tenant_id = $1 AND workflow_id = $2`,
		tenantID, workflowID).Scan(&tasks, &tokens, &costMicroUSD)
	if err != nil {
		t.Fatalf("read workflow reservation ledger: %v", err)
	}
	return tasks, tokens, costMicroUSD
}

// assertReservationReconciles proves the crash-recovery invariant: the
// ledger's step reservations always equal the sum of the per-step held
// reservations, so any reconciliation after a crash converges from the
// durable state alone.
func assertReservationReconciles(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string, workflowID uuid.UUID) {
	t.Helper()
	var ledgerTasks, ledgerTokens, ledgerCost int64
	if err := pool.QueryRow(ctx, `SELECT step_reserved_tasks, step_reserved_tokens, step_reserved_cost_micro_usd
		FROM workflow_usage_ledgers WHERE tenant_id = $1 AND workflow_id = $2`,
		tenantID, workflowID).Scan(&ledgerTasks, &ledgerTokens, &ledgerCost); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	var stepTasks, stepTokens, stepCost int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(reserved_tasks),0), COALESCE(SUM(reserved_tokens),0),
		COALESCE(SUM(reserved_cost_micro_usd),0) FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2`,
		tenantID, workflowID).Scan(&stepTasks, &stepTokens, &stepCost); err != nil {
		t.Fatalf("sum step reservations: %v", err)
	}
	if ledgerTasks != stepTasks || ledgerTokens != stepTokens || ledgerCost != stepCost {
		t.Fatalf("reservation drift: ledger=(%d tasks, %d tokens, %d micro-USD) steps=(%d tasks, %d tokens, %d micro-USD)",
			ledgerTasks, ledgerTokens, ledgerCost, stepTasks, stepTokens, stepCost)
	}
}

// TestV13ConcurrentSpawnNeverOvercommitsWorkflowBudget closes the P0-01
// loop: one hundred concurrent dynamic spawns race against a task-count
// budget that only fits ten of them. Every spawn reserves its task slot in
// the same transaction, so the committed total never exceeds the budget no
// matter how the race interleaves.
func TestV13ConcurrentSpawnNeverOvercommitsWorkflowBudget(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	// One declared step plus ten dynamic children fit exactly.
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "spawn-budget-race", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTasks = 11
	})
	guards := kernelstore.SpawnGuards{
		Enabled: true, MaxDynamicSteps: 200, MaxChildrenPerStep: 200, MaxSpawnDepth: 2, MaxWorkflowSteps: 200,
	}
	spawn := func(index int) (kernelstore.SpawnWorkflowStepResult, error) {
		name := fmt.Sprintf("racer-%03d", index)
		return repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
			ParentStepName: "planner", Name: name, Goal: "race", AgentVersionRef: "worker@1",
			Spec: []byte(`{}`), Guards: guards, IdempotencyKey: name, Arguments: []byte(fmt.Sprintf(`{"i":%d}`, index)),
		})
	}

	const callers = 100
	start := make(chan struct{})
	var created, exhausted, unexpected atomic.Int64
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			result, err := spawn(index)
			switch {
			case err == nil && result.Created:
				created.Add(1)
			case err == nil:
				exhausted.Add(1) // idempotent replay should be impossible: distinct keys
			default:
				if code, ok := kernelstore.DenialCode(err); ok && code == "SPAWN_BUDGET_EXHAUSTED" {
					exhausted.Add(1)
					return
				}
				unexpected.Add(1)
				t.Errorf("unexpected spawn error: %v", err)
			}
		}(index)
	}
	close(start)
	wg.Wait()

	if created.Load() != 10 || exhausted.Load() != callers-10 || unexpected.Load() != 0 {
		t.Fatalf("created=%d exhausted=%d unexpected=%d, want 10/%d/0", created.Load(), exhausted.Load(), unexpected.Load(), callers-10)
	}
	usage, err := repository.WorkflowUsageSnapshot(ctx, workflow.TenantID, workflow.ID)
	if err != nil {
		t.Fatalf("usage snapshot: %v", err)
	}
	if usage.CommittedTasks() != 11 || usage.Tasks != 0 {
		t.Fatalf("committed tasks = %d (created %d), want exactly the budget of 11", usage.CommittedTasks(), usage.Tasks)
	}
	if tasks, _, _ := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID); tasks != 11 {
		t.Fatalf("ledger step_reserved_tasks = %d, want 11", tasks)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)
}

// TestV13SpawnTokenReservationsBlockOvercommit proves the token dimension:
// each spawned step reserves the token ceiling its merged task spec will
// claim at admission, so concurrent spawns cannot collectively promise
// past budget.maxTokens before any task ledger exists.
func TestV13SpawnTokenReservationsBlockOvercommit(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "spawn-token-budget", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTasks = 50
		input.BudgetMaxTokens = 1000
	})
	guards := kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 50, MaxChildrenPerStep: 50, MaxSpawnDepth: 2, MaxWorkflowSteps: 60}
	spawn := func(name string) error {
		_, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
			ParentStepName: "planner", Name: name, Goal: "consume tokens", AgentVersionRef: "worker@1",
			Spec: []byte(`{"budget":{"tokens":100}}`), Guards: guards, IdempotencyKey: name, Arguments: []byte(`{}`),
		})
		return err
	}
	for index := 0; index < 10; index++ {
		if err := spawn(fmt.Sprintf("token-%d", index)); err != nil {
			t.Fatalf("spawn %d within budget: %v", index, err)
		}
	}
	_, tokens, _ := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID)
	if tokens != 1000 {
		t.Fatalf("reserved tokens = %d, want exactly the 1000-token budget", tokens)
	}
	if err := spawn("over-commit"); !errors.Is(err, kernelstore.ErrSpawnDenied) {
		t.Fatalf("over-commit spawn error = %v, want denial", err)
	} else if code, _ := kernelstore.DenialCode(err); code != "SPAWN_BUDGET_EXHAUSTED" {
		t.Fatalf("over-commit denial = %q, want SPAWN_BUDGET_EXHAUSTED", code)
	}
	// The same spawn replayed (identical key and arguments) must not move
	// the reservation.
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)
}

// TestV13SkippedAndCancelledStepsReleaseReservations proves the release
// path: an undispatched step that is skipped or cancelled returns its
// reservation to the workflow budget.
func TestV13SkippedAndCancelledStepsReleaseReservations(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "spawn-release", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTasks = 50
		input.BudgetMaxTokens = 500
	})
	guards := kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 50, MaxChildrenPerStep: 50, MaxSpawnDepth: 2, MaxWorkflowSteps: 60}
	spawnStep := func(name string) kernelstore.WorkflowStep {
		t.Helper()
		result, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
			ParentStepName: "planner", Name: name, Goal: "never dispatched", AgentVersionRef: "worker@1",
			Spec: []byte(`{"budget":{"tokens":100}}`), Guards: guards, IdempotencyKey: name, Arguments: []byte(`{}`),
		})
		if err != nil {
			t.Fatalf("spawn %s: %v", name, err)
		}
		return result.Step
	}
	skipped := spawnStep("doomed")
	cancelled := spawnStep("halted")

	if _, err := repository.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
		TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: skipped.Name,
		ExpectedVersion: skipped.ResourceVersion, To: kernelstore.StepSkipped, FailureCode: "CONDITION_NOT_MET",
	}); err != nil {
		t.Fatalf("skip step: %v", err)
	}
	if _, err := repository.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
		TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: cancelled.Name,
		ExpectedVersion: cancelled.ResourceVersion, To: kernelstore.StepCancelled, FailureCode: "WORKFLOW_CANCELLED",
	}); err != nil {
		t.Fatalf("cancel step: %v", err)
	}
	// Both released: only the declared planner step keeps its reservation.
	if tasks, tokens, _ := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID); tasks != 1 || tokens != 0 {
		t.Fatalf("after release ledger = (%d tasks, %d tokens), want (1, 0)", tasks, tokens)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)
}

// TestV13ReservationTransfersToTaskBudgetAndBack proves the full transfer
// chain: spawn reserves, dispatch moves the task slot, admission moves the
// token/cost ceiling into the task's own ledger, and the terminal task
// releases the residual reservation — each exactly once.
func TestV13ReservationTransfersToTaskBudgetAndBack(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "spawn-transfer", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTasks = 50
		input.BudgetMaxTokens = 1000
	})
	guards := kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 50, MaxChildrenPerStep: 50, MaxSpawnDepth: 2, MaxWorkflowSteps: 60}
	result, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
		ParentStepName: "planner", Name: "worker", Goal: "dispatched", AgentVersionRef: "worker@1",
		Spec: []byte(`{"budget":{"tokens":300,"costUsd":1.5}}`), Guards: guards,
		IdempotencyKey: "transfer", Arguments: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	step := result.Step

	// Dispatch: create the task carrying the step lineage. The task-count
	// slot moves atomically; token/cost stay reserved until admission.
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: workflow.TenantID, Namespace: "default", AgentVersionRef: "worker@1",
		Goal: "dispatched", Spec: step.Spec, IdempotencyKey: fmt.Sprintf("workflow/%s/worker/1", workflow.ID),
		WorkflowID: &workflow.ID, WorkflowStepID: &step.ID, WorkflowStepName: step.Name, WorkflowAttempt: 1,
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	// An idempotent replay of the dispatch must not consume a second slot.
	if replay, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: workflow.TenantID, Namespace: "default", AgentVersionRef: "worker@1",
		Goal: "dispatched", Spec: step.Spec, IdempotencyKey: fmt.Sprintf("workflow/%s/worker/1", workflow.ID),
		WorkflowID: &workflow.ID, WorkflowStepID: &step.ID, WorkflowStepName: step.Name, WorkflowAttempt: 1,
	}); err != nil || !replay.Existing {
		t.Fatalf("task replay = %+v err=%v, want existing", replay.Task, err)
	}
	if tasks, tokens, cost := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID); tasks != 1 || tokens != 300 || cost != 1_500_000 {
		t.Fatalf("after dispatch ledger = (%d tasks, %d tokens, %d cost), want (1, 300, 1500000)", tasks, tokens, cost)
	}

	// Admission: the task's own budget ledger now carries the ceiling and
	// the step's token/cost reservation transfers exactly once.
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim admission: %d %v", len(claims), err)
	}
	budget := kernelstore.TaskBudget{Tokens: 300, CostMicroUSD: kernelmoney.MustFromUSD(1.5)}
	admitted, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: workflow.TenantID, OwnerID: "admission",
		ClaimFencingToken: claims[0].FencingToken, ExpectedTaskVersion: created.Task.ResourceVersion,
		Admit: true, ReasonCode: "ADMISSION_PASSED", EvaluatorVersion: "test/v1", Budget: &budget,
	})
	if err != nil {
		t.Fatalf("admit task: %v", err)
	}
	usage, err := repository.WorkflowUsageSnapshot(ctx, workflow.TenantID, workflow.ID)
	if err != nil {
		t.Fatalf("usage after admission: %v", err)
	}
	if usage.Tasks != 1 || usage.ReservedTokens != 300 || usage.StepReservedTokens != 0 || usage.ReservedCostMicroUSD != kernelmoney.MustFromUSD(1.5) {
		t.Fatalf("after admission usage = %+v, want task-reserved 300 tokens / $1.5 and zero step reservation", usage)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)

	// Terminal task: the unconsumed reservation returns to the budget.
	if _, err := repository.TransitionTask(ctx, workflow.TenantID, admitted.ID, admitted.ResourceVersion, domain.TaskCancelled); err != nil {
		t.Fatalf("cancel task: %v", err)
	}
	usage, err = repository.WorkflowUsageSnapshot(ctx, workflow.TenantID, workflow.ID)
	if err != nil {
		t.Fatalf("usage after terminal: %v", err)
	}
	// The dispatched task's reservations are fully released; the declared
	// planner step still holds its own undispatched slot, so the workflow
	// commits exactly two tasks (one created, one promised).
	if usage.ReservedTokens != 0 || usage.ReservedCostMicroUSD != 0 || usage.StepReservedTokens != 0 || usage.CommittedTasks() != 2 {
		t.Fatalf("after terminal usage = %+v, want zero task reservations and two committed tasks", usage)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)
}

// TestV13RejectedTaskReleasesStepReservation proves a task that terminates
// without ever establishing a budget ledger (admission rejection) releases
// the step reservation covering it.
func TestV13RejectedTaskReleasesStepReservation(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	workflow := createV13Workflow(t, ctx, repository, "tenant-v13", "spawn-reject", func(input *kernelstore.CreateWorkflowInput) {
		input.BudgetMaxTasks = 50
		input.BudgetMaxTokens = 1000
	})
	guards := kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 50, MaxChildrenPerStep: 50, MaxSpawnDepth: 2, MaxWorkflowSteps: 60}
	result, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
		ParentStepName: "planner", Name: "rejected-worker", Goal: "denied at admission", AgentVersionRef: "worker@1",
		Spec: []byte(`{"budget":{"tokens":250}}`), Guards: guards, IdempotencyKey: "reject", Arguments: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	step := result.Step
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: workflow.TenantID, Namespace: "default", AgentVersionRef: "worker@1",
		Goal: "denied at admission", Spec: step.Spec, IdempotencyKey: fmt.Sprintf("workflow/%s/rejected-worker/1", workflow.ID),
		WorkflowID: &workflow.ID, WorkflowStepID: &step.ID, WorkflowStepName: step.Name, WorkflowAttempt: 1,
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claims, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerAdmission, Phase: domain.TaskQueued, OwnerID: "admission", Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim admission: %d %v", len(claims), err)
	}
	if _, err := repository.DecideAdmission(ctx, kernelstore.DecideAdmissionInput{
		TaskID: created.Task.ID, TenantID: workflow.TenantID, OwnerID: "admission",
		ClaimFencingToken: claims[0].FencingToken, ExpectedTaskVersion: created.Task.ResourceVersion,
		Admit: false, ReasonCode: "POLICY_DENIED", EvaluatorVersion: "test/v1",
	}); err != nil {
		t.Fatalf("reject task: %v", err)
	}
	if _, tokens, _ := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID); tokens != 0 {
		t.Fatalf("after rejection step_reserved_tokens = %d, want 0", tokens)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)
}

// TestV13ReconcileBackfillsUnderivedStepReservationsAfterUpgrade closes the
// P1-05 loop: it reconstructs the exact state the 000028 upgrade left behind —
// a workflow with a thousand undispatched steps whose task-count slots were
// restored but whose token/cost reservations were zeroed, because those
// ceilings live inside the merged task spec and pure SQL cannot decode and
// merge them. The controller's reconcile pass must re-derive every step's
// reservation through the same default/overlay merge the dispatcher uses,
// resync the ledger, and clear the flag, so that afterwards the reservation
// invariant holds at the true nonzero total: ledger commitment = sum(step
// commitment). While the flag is set, dynamic spawning stays paused fail-safe.
func TestV13ReconcileBackfillsUnderivedStepReservationsAfterUpgrade(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	// A thousand declared steps inherit their token/cost ceiling from the
	// workflow defaultTaskSpec through the shallow merge. The budget is absent
	// from every per-step overlay, so it exists only in the merged spec — the
	// reservation 000028 could not reconstruct without the Go merge path.
	const steps = 1000
	doc := kernelworkflow.WorkflowSpec{
		DefaultTaskSpec: json.RawMessage(`{"budget":{"tokens":100,"costUsd":1}}`),
		Steps:           make([]kernelworkflow.StepSpec, steps),
	}
	for index := range doc.Steps {
		doc.Steps[index] = kernelworkflow.StepSpec{
			Name: fmt.Sprintf("step-%04d", index), AgentVersionRef: "worker@1",
			Goal: "declared", Spec: json.RawMessage(`{}`),
		}
	}
	specJSON, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal workflow spec: %v", err)
	}
	stepInputs := doc.StepInputs()
	if len(stepInputs) != steps {
		t.Fatalf("StepInputs derived %d steps, want %d", len(stepInputs), steps)
	}

	workflow := createV13Workflow(t, ctx, repository, "tenant-recon", "reservation-backfill", func(in *kernelstore.CreateWorkflowInput) {
		in.Spec = specJSON
		in.Steps = stepInputs
	})

	// The derivation is byte-identical to reconcile's, so creation already
	// reserves the true totals; this anchors the expected nonzero result.
	const wantTokens = int64(steps) * 100
	const wantCost = int64(steps) * 1_000_000
	if _, tokens, cost := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID); tokens != wantTokens || cost != wantCost {
		t.Fatalf("post-create ledger = (%d tokens, %d cost), want (%d, %d)", tokens, cost, wantTokens, wantCost)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)

	// Reproduce the 000028 upgrade state: task-count slots survived, but the
	// token/cost reservations were left at zero on both the steps and the
	// ledger aggregate, and the workflow is flagged for reconciliation.
	if _, err := pool.Exec(ctx, `UPDATE workflow_steps SET reserved_tokens = 0, reserved_cost_micro_usd = 0
		WHERE tenant_id = $1 AND workflow_id = $2 AND status IN ('PENDING','WAITING_APPROVAL')`,
		workflow.TenantID, workflow.ID); err != nil {
		t.Fatalf("simulate 000028 step zeroing: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE workflow_usage_ledgers SET step_reserved_tokens = 0, step_reserved_cost_micro_usd = 0
		WHERE tenant_id = $1 AND workflow_id = $2`, workflow.TenantID, workflow.ID); err != nil {
		t.Fatalf("simulate 000028 ledger zeroing: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE workflows SET needs_budget_reconciliation = true
		WHERE tenant_id = $1 AND id = $2`, workflow.TenantID, workflow.ID); err != nil {
		t.Fatalf("flag workflow for reconciliation: %v", err)
	}

	spawn := func(name string, version int64) error {
		_, err := repository.SpawnWorkflowStep(ctx, kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: version,
			ParentStepName: "step-0000", Name: name, Goal: "dynamic child", AgentVersionRef: "worker@1",
			Spec:           json.RawMessage(`{"budget":{"tokens":100}}`),
			Guards:         kernelstore.SpawnGuards{Enabled: true, MaxDynamicSteps: 10, MaxChildrenPerStep: 10, MaxSpawnDepth: 2, MaxWorkflowSteps: steps + 10},
			IdempotencyKey: name, Arguments: json.RawMessage(`{}`),
		})
		return err
	}

	// While flagged, a genuinely new dynamic spawn is refused fail-safe rather
	// than admitted against an understated commitment. The raw flag update did
	// not bump resource_version, so the creation version still matches and the
	// denial is the reconciliation guard, not a version conflict.
	if err := spawn("blocked", workflow.ResourceVersion); !errors.Is(err, kernelstore.ErrSpawnDenied) {
		t.Fatalf("spawn while reconciling = %v, want ErrSpawnDenied", err)
	} else if code, _ := kernelstore.DenialCode(err); code != "SPAWN_RECONCILIATION_PENDING" {
		t.Fatalf("spawn denial code = %q, want SPAWN_RECONCILIATION_PENDING", code)
	}

	report, err := repository.ReconcileWorkflowReservations(ctx, true)
	if err != nil {
		t.Fatalf("reconcile workflow reservations: %v", err)
	}
	if report.Flagged != 1 || report.Reconciled != 1 || report.StepsAdjusted != steps {
		t.Fatalf("reconcile report = %+v, want 1 flagged / 1 reconciled / %d steps adjusted", report, steps)
	}

	// Acceptance: ledger commitment equals the sum of the per-step reservations,
	// and it is the true derived total rather than the backfilled zero.
	tasks, tokens, cost := workflowReservation(t, ctx, pool, workflow.TenantID, workflow.ID)
	if tasks != int64(steps) || tokens != wantTokens || cost != wantCost {
		t.Fatalf("post-reconcile ledger = (%d tasks, %d tokens, %d cost), want (%d, %d, %d)",
			tasks, tokens, cost, steps, wantTokens, wantCost)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)

	// The flag is cleared, so dynamic spawning resumes. Reconcile bumped the
	// workflow's resource_version, so the resumed spawn reads the fresh one.
	reloaded, err := repository.GetWorkflow(ctx, workflow.TenantID, workflow.ID)
	if err != nil {
		t.Fatalf("reload workflow: %v", err)
	}
	if reloaded.NeedsBudgetReconciliation {
		t.Fatalf("needs_budget_reconciliation still set after reconcile")
	}
	if err := spawn("unblocked", reloaded.ResourceVersion); err != nil {
		t.Fatalf("spawn after reconcile: %v", err)
	}
	assertReservationReconciles(t, ctx, pool, workflow.TenantID, workflow.ID)

	// A second pass is a no-op: nothing remains flagged and no step is touched.
	if report, err := repository.ReconcileWorkflowReservations(ctx, true); err != nil || report.Flagged != 0 || report.Reconciled != 0 {
		t.Fatalf("idempotent reconcile = %+v err=%v, want zero flagged and reconciled", report, err)
	}
}
