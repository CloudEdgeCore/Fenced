//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/google/uuid"
)

// submitWorkflow publishes one workflow run through the kernel validation.
func submitWorkflow(ctx context.Context, t *testing.T, env *e2eEnv, key, goal, spec string) kernelstore.Workflow {
	t.Helper()
	steps, err := workflow.DecodeSpec([]byte(spec))
	if err != nil {
		t.Fatalf("decode workflow spec: %v", err)
	}
	created, err := env.store.CreateWorkflow(ctx, kernelstore.CreateWorkflowInput{
		ID: uuid.New(), TenantID: e2eTenant, Namespace: "default", IdempotencyKey: key,
		Goal: goal, Spec: []byte(spec), Steps: steps,
	})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return created.Workflow
}

func workflowStatus(ctx context.Context, t *testing.T, env *e2eEnv, id uuid.UUID) kernelstore.Workflow {
	t.Helper()
	current, err := env.store.GetWorkflow(ctx, e2eTenant, id)
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	return current
}

func workflowSteps(ctx context.Context, t *testing.T, env *e2eEnv, id uuid.UUID) []kernelstore.WorkflowStep {
	t.Helper()
	steps, err := env.store.ListWorkflowSteps(ctx, e2eTenant, id)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	return steps
}

func waitForWorkflowTerminal(ctx context.Context, t *testing.T, env *e2eEnv, id uuid.UUID, timeout time.Duration) kernelstore.Workflow {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current := workflowStatus(ctx, t, env, id)
		if current.Status.Terminal() {
			return current
		}
		time.Sleep(50 * time.Millisecond)
	}
	current := workflowStatus(ctx, t, env, id)
	t.Fatalf("workflow %s did not reach a terminal phase (status=%s)", id, current.Status)
	return current
}

// driveOrchestrators runs n concurrent orchestrator instances in the durable
// claim mode required for a multi-instance deployment. Running several
// claim-lease=0 controllers would intentionally select the same full batch on
// every round (that mode is for one instance), measuring avoidable CAS
// contention instead of distributed orchestrator latency.
func driveOrchestrators(ctx context.Context, t *testing.T, env *e2eEnv, n int, samples *latencySamples) {
	t.Helper()
	claimBatch := max(1, 100/n)
	for i := 0; i < n; i++ {
		controller := workflow.NewController(env.store, env.store, env.artifacts, fmt.Sprintf("e2e-orchestrator-%d", i), claimBatch).
			WithClaiming(5*time.Second, 0)
		go func(controller *workflow.Controller) {
			for ctx.Err() == nil {
				start := time.Now()
				if _, err := controller.Reconcile(ctx); err != nil && ctx.Err() == nil {
					t.Errorf("orchestrator reconcile: %v", err)
					return
				}
				if samples != nil {
					samples.record(float64(time.Since(start).Microseconds()) / 1000.0)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(40 * time.Millisecond):
				}
			}
		}(controller)
	}
}

// latencySamples is the mutex-guarded reconcile latency log.
type latencySamples struct {
	mu     sync.Mutex
	values []float64
}

func (l *latencySamples) record(milliseconds float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.values = append(l.values, milliseconds)
}

func (l *latencySamples) snapshot() []float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]float64(nil), l.values...)
}

const dualAgentSpec = `{
  "defaultTaskSpec": {"priority":50,"budget":{"tokens":1000,"costUsd":5,"toolCalls":20,"wallSeconds":600},"placement":{"runtimeClasses":["adapter"],"preferredClass":"adapter","region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":8388608,"llmConcurrency":2}},
  "steps": [
    {"name":"research","agentVersionRef":"%s","goal":"Report the weather. CITY"},
    {"name":"review","agentVersionRef":"%s","goal":"Summarize the research for the operator.","dependsOn":["research"]}
  ]
}`

// TestV12DualAgentWorkflowRegression is the Phase 2 acceptance: 1,000
// research→review workflows through the real governed loop, with two
// concurrent orchestrator instances, proving no reordering (review only
// runs after research succeeded), no duplication (exactly two tasks per
// workflow) and no cross-talk (research output reaches only its own
// review).
func TestV12DualAgentWorkflowRegression(t *testing.T) {
	total := e2eCount("FENCED_E2E_WORKFLOWS", 1000)
	env := newE2EEnv(t, "fenced_wf_regression", 0, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stack := startStack(t, env)
	instances := []string{"worker-wf-1", "worker-wf-2", "worker-wf-3", "worker-wf-4"}
	for _, instance := range instances {
		go driveWorker(ctx, t, makeWorker(t, env, stack, instance, 30*time.Second), instance)
	}
	pools := poolsFor(instances...)
	publishVersion(ctx, t, env, stack.agentURL)
	go func() {
		for ctx.Err() == nil {
			reconcileQuiet(ctx, env, pools)
			time.Sleep(25 * time.Millisecond)
		}
	}()
	driveOrchestrators(ctx, t, env, 2, nil)

	started := time.Now()
	const window = 64
	submitted := 0
	pending := make([]uuid.UUID, 0, window)
	succeeded := 0
	for submitted < total || len(pending) > 0 {
		for len(pending) < window && submitted < total {
			spec := strings.Replace(fmt.Sprintf(dualAgentSpec, e2eVersionRef, e2eVersionRef),
				"CITY", fmt.Sprintf("city:wf-%d", submitted), 1)
			created := submitWorkflow(ctx, t, env, fmt.Sprintf("wf-reg-%d", submitted),
				fmt.Sprintf("dual agent regression %d", submitted), spec)
			pending = append(pending, created.ID)
			submitted++
		}
		final := waitForWorkflowTerminal(ctx, t, env, pending[0], 15*time.Minute)
		if final.Status == kernelstore.WorkflowSucceeded {
			succeeded++
		} else {
			t.Errorf("workflow %s ended in %s (%s)", pending[0], final.Status, final.FailureCode)
		}
		pending = pending[1:]
	}
	elapsed := time.Since(started)

	// No duplication: every workflow created exactly two tasks.
	if tasks := queryInt(ctx, t, env, `SELECT count(*) FROM tasks WHERE tenant_id = $1`, e2eTenant); tasks != int64(total)*2 {
		t.Fatalf("tasks = %d, want exactly 2 per workflow (%d)", tasks, total*2)
	}
	// No reordering: every review task's goal embeds its own research
	// output (appended only after research SUCCEEDED), and no other
	// workflow's city token.
	rows, err := env.pool.Query(ctx, `SELECT s.name, t.goal FROM workflow_steps s
		JOIN tasks t ON t.id = s.task_id WHERE s.tenant_id = $1 AND s.name = 'review'`, e2eTenant)
	if err != nil {
		t.Fatalf("read review goals: %v", err)
	}
	defer rows.Close()
	cityPattern := regexp.MustCompile(`wf-(\d+)`)
	reviewGoals := 0
	for rows.Next() {
		var name, goal string
		if err := rows.Scan(&name, &goal); err != nil {
			t.Fatalf("scan review goal: %v", err)
		}
		matches := cityPattern.FindAllStringSubmatch(goal, -1)
		if len(matches) != 1 {
			t.Fatalf("review goal carries %d workflow tokens (cross-talk or missing upstream): %q", len(matches), goal)
		}
		reviewGoals++
	}
	if reviewGoals != total {
		t.Fatalf("review goals = %d, want %d", reviewGoals, total)
	}
	rate := float64(succeeded) / float64(total)
	t.Logf("v1.2 dual-agent regression: workflows=%d succeeded=%d rate=%.4f elapsed=%s throughput=%.1f wf/s",
		total, succeeded, rate, elapsed, float64(total)/elapsed.Seconds())
	if rate < 0.99 {
		t.Fatalf("workflow success rate %.4f below the 99%% gate", rate)
	}
}

// TestV12WorkflowSemantics proves the Phase 3 mechanics in one governed
// run: parallel fan-out, join, condition skipping, single-step retry after
// a scripted provider failure, human approval, and (in a second workflow)
// cancellation propagation. Recovery is proven by driving the semantics
// workflow with a freshly constructed orchestrator after every round.
func TestV12WorkflowSemantics(t *testing.T) {
	env := newE2EEnv(t, "fenced_wf_semantics", 700*time.Millisecond, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stack := startStack(t, env)
	instances := []string{"worker-sem-1", "worker-sem-2"}
	for _, instance := range instances {
		go driveWorker(ctx, t, makeWorker(t, env, stack, instance, 30*time.Second), instance)
	}
	pools := poolsFor(instances...)
	publishVersion(ctx, t, env, stack.agentURL)
	go func() {
		for ctx.Err() == nil {
			reconcileQuiet(ctx, env, pools)
			time.Sleep(25 * time.Millisecond)
		}
	}()

	// The flaky city fails the first model call; the step retry recovers.
	env.provider.mu.Lock()
	if env.provider.failCities == nil {
		env.provider.failCities = map[string]int{}
	}
	env.provider.failCities["flaky-7"] = 3 // exceeds the provider bounded retry (2), so the failure reaches the task
	env.provider.mu.Unlock()

	spec := fmt.Sprintf(`{
	  "defaultTaskSpec": {"priority":50,"budget":{"tokens":2000,"costUsd":10,"toolCalls":40,"wallSeconds":900},"placement":{"runtimeClasses":["adapter"],"preferredClass":"adapter","region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":8388608,"llmConcurrency":2}},
	  "steps": [
	    {"name":"root","agentVersionRef":"%s","goal":"Report the weather. city:root-9"},
	    {"name":"left","agentVersionRef":"%s","goal":"Report the weather. city:left-3","dependsOn":["root"]},
	    {"name":"right","agentVersionRef":"%s","goal":"Report the weather. city:right-4","dependsOn":["root"],"retry":{"maxAttempts":3}},
	    {"name":"flaky","agentVersionRef":"%s","goal":"Report the weather. city:flaky-7","dependsOn":["root"],"retry":{"maxAttempts":2}},
	    {"name":"join","agentVersionRef":"%s","goal":"Join the branch outputs.","dependsOn":["left","right","flaky"]},
	    {"name":"conditional","agentVersionRef":"%s","goal":"Only when root says clear.","dependsOn":["root"],"condition":{"step":"root","outputContains":"weather acquired for root-9"}},
	    {"name":"never","agentVersionRef":"%s","goal":"Only when root says snow.","dependsOn":["root"],"condition":{"step":"root","outputEquals":"snow"}},
	    {"name":"risky","agentVersionRef":"%s","goal":"High risk branch needing a human.","dependsOn":["join"],"requiresApproval":true}
	  ]
	}`, e2eVersionRef, e2eVersionRef, e2eVersionRef, e2eVersionRef, e2eVersionRef, e2eVersionRef, e2eVersionRef, e2eVersionRef)
	workflowID := submitWorkflow(ctx, t, env, "wf-semantics-1", "semantics coverage", spec).ID

	// Recovery: every round runs on a freshly constructed orchestrator
	// (equivalent to a restart between rounds).
	deadline := time.Now().Add(2 * time.Minute)
	approved := false
	for time.Now().Before(deadline) {
		fresh := workflow.NewController(env.store, env.store, env.artifacts, "e2e-orchestrator-fresh", 100)
		if _, err := fresh.Reconcile(ctx); err != nil {
			t.Fatalf("fresh orchestrator reconcile: %v", err)
		}
		steps := workflowSteps(ctx, t, env, workflowID)
		byName := map[string]kernelstore.WorkflowStep{}
		for _, step := range steps {
			byName[step.Name] = step
		}
		if !approved && byName["risky"].Status == kernelstore.StepWaitingApproval {
			approved = true
			_, err := env.store.DecideWorkflowStepApproval(ctx, kernelstore.DecideWorkflowStepApprovalInput{
				TenantID: e2eTenant, WorkflowID: workflowID, StepName: "risky",
				ExpectedVersion: byName["risky"].ResourceVersion, Approved: true, DecidedBy: "human-e2e",
			})
			if err != nil {
				t.Fatalf("approve risky step: %v", err)
			}
		}
		if workflowStatus(ctx, t, env, workflowID).Status.Terminal() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !workflowStatus(ctx, t, env, workflowID).Status.Terminal() {
		t.Logf("semantic workflow runtime diagnostics: %s", stack.pythonOut.String())
	}
	final := waitForWorkflowTerminal(ctx, t, env, workflowID, 30*time.Second)
	if final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow status = %s (%s)", final.Status, final.FailureCode)
	}
	steps := workflowSteps(ctx, t, env, workflowID)
	byName := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		byName[step.Name] = step
	}
	for _, name := range []string{"root", "left", "right", "flaky", "join", "conditional", "risky"} {
		if byName[name].Status != kernelstore.StepSucceeded {
			t.Fatalf("%s = %s (%s)", name, byName[name].Status, byName[name].FailureCode)
		}
	}
	if byName["never"].Status != kernelstore.StepSkipped || byName["never"].FailureCode != "CONDITION_NOT_MET" {
		t.Fatalf("never = %s (%s)", byName["never"].Status, byName["never"].FailureCode)
	}
	// Single-step retry really retried: flaky burned two attempts.
	if byName["flaky"].AttemptCount != 2 {
		for _, step := range steps {
			t.Logf("step %s: status=%s attempts=%d task=%v failure=%s", step.Name, step.Status, step.AttemptCount, step.TaskID, step.FailureCode)
		}
		if byName["flaky"].TaskID != nil {
			task, _ := env.store.GetTask(ctx, e2eTenant, *byName["flaky"].TaskID)
			t.Logf("flaky task phase=%s result=%q", task.Phase, task.ResultRef)
		}
		t.Fatalf("flaky attempts = %d, want 2 (scripted failure + retry)", byName["flaky"].AttemptCount)
	}
	// Join received every branch output.
	if byName["join"].TaskID == nil {
		t.Fatal("join missing task")
	}
	joinTask, err := env.store.GetTask(ctx, e2eTenant, *byName["join"].TaskID)
	if err != nil {
		t.Fatalf("read join task: %v", err)
	}
	for _, marker := range []string{"[left]", "[right]", "[flaky]"} {
		if !strings.Contains(joinTask.Goal, "Upstream result "+marker) {
			t.Fatalf("join goal missing %s: %q", marker, joinTask.Goal)
		}
	}

	// Cancellation propagation on a separate slow workflow.
	cancelSpec := strings.Replace(fmt.Sprintf(dualAgentSpec, e2eVersionRef, e2eVersionRef), "CITY", "city:cancel-1", 1)
	cancelID := submitWorkflow(ctx, t, env, "wf-cancel-1", "cancellation coverage", cancelSpec).ID
	cancelOrchestrator := workflow.NewController(env.store, env.store, env.artifacts, "e2e-orchestrator-cancel", 100)
	// Dispatch the research step.
	deadline = time.Now().Add(30 * time.Second)
	dispatched := false
	for time.Now().Before(deadline) {
		if _, err := cancelOrchestrator.Reconcile(ctx); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if steps := workflowSteps(ctx, t, env, cancelID); steps[0].Status == kernelstore.StepRunning {
			dispatched = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !dispatched {
		t.Fatal("cancellation workflow research step was not dispatched")
	}
	current := workflowStatus(ctx, t, env, cancelID)
	if _, err := env.store.RequestWorkflowCancellation(ctx, e2eTenant, cancelID, current.ResourceVersion); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	// Cancellation can take longer than the dispatch deadline under the race
	// detector. Keep an orchestrator alive until it observes the terminal task
	// and propagates that state into the step and workflow.
	cancelDeadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(cancelDeadline) {
		if _, err := cancelOrchestrator.Reconcile(ctx); err != nil {
			t.Fatalf("reconcile cancel: %v", err)
		}
		if workflowStatus(ctx, t, env, cancelID).Status.Terminal() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	finalCancel := workflowStatus(ctx, t, env, cancelID)
	if finalCancel.Status != kernelstore.WorkflowCancelled {
		t.Fatalf("cancelled workflow = %s", finalCancel.Status)
	}
	cancelSteps := workflowSteps(ctx, t, env, cancelID)
	if cancelSteps[1].Status != kernelstore.StepSkipped {
		t.Fatalf("review after cancel = %s, want SKIPPED", cancelSteps[1].Status)
	}
	t.Logf("v1.2 semantics: join/condition/retry/approval/cancel/recovery all verified")
}

// TestV12WorkflowScale is the Phase 3 capacity gate: one workflow with
// FENCED_E2E_WF_STEPS steps (default 1000, wide fan-out with a final
// join), FENCED_E2E_CONCURRENT_WF concurrent workflows (default 100), and
// the orchestrator P95 reconcile latency below 500ms.
func TestV12WorkflowScale(t *testing.T) {
	steps := e2eCount("FENCED_E2E_WF_STEPS", 1000)
	concurrent := e2eCount("FENCED_E2E_CONCURRENT_WF", 100)
	env := newE2EEnv(t, "fenced_wf_scale", 0, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stack := startStack(t, env)
	instances := []string{"worker-scale-1", "worker-scale-2", "worker-scale-3", "worker-scale-4"}
	for _, instance := range instances {
		go driveWorker(ctx, t, makeWorker(t, env, stack, instance, 30*time.Second), instance)
	}
	pools := poolsFor(instances...)
	publishVersion(ctx, t, env, stack.agentURL)
	go func() {
		for ctx.Err() == nil {
			reconcileQuiet(ctx, env, pools)
			time.Sleep(25 * time.Millisecond)
		}
	}()
	reconcileMs := &latencySamples{}
	driveOrchestrators(ctx, t, env, 2, reconcileMs)

	// One wide workflow: root -> (steps-2) parallel branches -> join.
	// The join carries every upstream result in its governed prompt, so its
	// reservation must scale with fan-in. Keeping the ordinary 1,000-token
	// per-step ceiling would make the scale fixture fail closed before the
	// provider call for exactly the budget-safety behavior under test.
	scaleTokenBudget := max(1000, steps*2000)
	var builder strings.Builder
	fmt.Fprintf(&builder, `{"defaultTaskSpec":{"priority":50,"budget":{"tokens":%d,"costUsd":5,"toolCalls":20,"wallSeconds":900},"placement":{"runtimeClasses":["adapter"],"preferredClass":"adapter","region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":8388608,"llmConcurrency":2}},"steps":[{"name":"root","agentVersionRef":%q,"goal":"Report the weather. city:scale-root"}`, scaleTokenBudget, e2eVersionRef)
	branches := steps - 2
	if branches < 1 {
		branches = 1
	}
	deps := make([]string, 0, branches)
	for i := 0; i < branches; i++ {
		fmt.Fprintf(&builder, `,{"name":"branch-%d","agentVersionRef":%q,"goal":"Report the weather. city:scale-%d","dependsOn":["root"]}`, i, e2eVersionRef, i)
		deps = append(deps, fmt.Sprintf("branch-%d", i))
	}
	fmt.Fprintf(&builder, `,{"name":"join","agentVersionRef":%q,"goal":"Join every branch.","dependsOn":[%s]}`,
		e2eVersionRef, `"`+strings.Join(deps, `","`)+`"`)
	builder.WriteString("]}")

	if !json.Valid([]byte(builder.String())) {
		tail := builder.String()
		if len(tail) > 200 {
			tail = tail[len(tail)-200:]
		}
		head := builder.String()
		if len(head) > 300 {
			head = head[300:600]
		}
		t.Fatalf("built spec invalid (len=%d): head=...%s... tail=...%s", builder.Len(), head, tail)
	}
	started := time.Now()
	wideID := submitWorkflow(ctx, t, env, "wf-scale-wide", "single wide workflow", builder.String()).ID
	concurrentIDs := make([]uuid.UUID, 0, concurrent)
	for i := 0; i < concurrent; i++ {
		spec := strings.Replace(fmt.Sprintf(dualAgentSpec, e2eVersionRef, e2eVersionRef),
			"CITY", fmt.Sprintf("city:conc-%d", i), 1)
		concurrentIDs = append(concurrentIDs, submitWorkflow(ctx, t, env, fmt.Sprintf("wf-scale-conc-%d", i),
			fmt.Sprintf("concurrent %d", i), spec).ID)
	}

	wideFinal := waitForWorkflowTerminal(ctx, t, env, wideID, 30*time.Minute)
	if wideFinal.Status != kernelstore.WorkflowSucceeded {
		t.Logf("python runtime diagnostics: %s", stack.pythonOut.String())
		t.Logf("wide workflow diagnostics: status=%s code=%s cancel=%v budgetStop=%v deadline=%v deadlineStop=%v version=%d",
			wideFinal.Status, wideFinal.FailureCode, wideFinal.CancelRequestedAt, wideFinal.BudgetExhaustedAt,
			wideFinal.DeadlineAt, wideFinal.DeadlineExceededAt, wideFinal.ResourceVersion)
		rows, queryErr := env.pool.Query(ctx, `SELECT s.name, s.status, COALESCE(s.failure_code, ''),
			COALESCE(t.phase, ''), COALESCE(r.phase, ''), COALESCE(a.phase, ''),
			COALESCE(a.failure_code, ''), COALESCE(a.failure_message, ''),
			COALESCE(b.reserved_tokens, 0), COALESCE(b.consumed_tokens, 0),
			(SELECT count(*) FROM model_calls m WHERE m.task_id = s.task_id), t.cancel_requested_at
			FROM workflow_steps s
			LEFT JOIN tasks t ON t.id = s.task_id
			LEFT JOIN task_budget_ledgers b ON b.task_id = s.task_id
			LEFT JOIN runs r ON r.task_id = s.task_id
			LEFT JOIN attempts a ON a.run_id = r.id
			WHERE s.workflow_id = $1 AND s.status <> 'SUCCEEDED'
			ORDER BY s.ordinal LIMIT 20`, wideID)
		if queryErr == nil {
			defer rows.Close()
			for rows.Next() {
				var name, status, stepCode, taskPhase, runPhase, attemptPhase, attemptCode, attemptMessage string
				var cancelRequestedAt *time.Time
				var reservedTokens, consumedTokens, modelCalls int64
				if scanErr := rows.Scan(&name, &status, &stepCode, &taskPhase, &runPhase, &attemptPhase, &attemptCode, &attemptMessage, &reservedTokens, &consumedTokens, &modelCalls, &cancelRequestedAt); scanErr == nil {
					t.Logf("failed step diagnostic: name=%s status=%s stepCode=%s task=%s run=%s attempt=%s attemptCode=%s message=%s budget=%d consumed=%d modelCalls=%d cancel=%v",
						name, status, stepCode, taskPhase, runPhase, attemptPhase, attemptCode, attemptMessage, reservedTokens, consumedTokens, modelCalls, cancelRequestedAt)
				}
			}
		}
		t.Fatalf("wide workflow = %s (%s)", wideFinal.Status, wideFinal.FailureCode)
	}
	succeeded := 0
	for _, id := range concurrentIDs {
		if waitForWorkflowTerminal(ctx, t, env, id, 30*time.Minute).Status == kernelstore.WorkflowSucceeded {
			succeeded++
		}
	}
	elapsed := time.Since(started)

	// Exactness across everything: exactly one task per dispatched step.
	expectedTasks := int64(steps + concurrent*2)
	if tasks := queryInt(ctx, t, env, `SELECT count(*) FROM tasks WHERE tenant_id = $1`, e2eTenant); tasks != expectedTasks {
		t.Fatalf("tasks = %d, want exactly %d (no double dispatch)", tasks, expectedTasks)
	}

	// Orchestrator P95 reconcile latency (Phase 3 gate; the instrumented
	// race build doubles the threshold — see scale_gate_*_test.go).
	p95 := float64(0)
	if samples := reconcileMs.snapshot(); len(samples) > 0 {
		sort.Float64s(samples)
		p95 = samples[(len(samples)*95)/100]
	}
	concurrentRate := float64(succeeded) / float64(concurrent)
	t.Logf("v1.2 scale evidence: wideWorkflowSteps=%d concurrentWorkflows=%d/%d succeeded elapsed=%s orchestratorP95=%.1fms",
		steps, succeeded, concurrent, elapsed, p95)
	if concurrentRate < 0.99 {
		t.Fatalf("concurrent workflow success rate %.4f below 99%%", concurrentRate)
	}
	if p95 >= reconcileP95GateMs {
		t.Fatalf("orchestrator P95 reconcile = %.1fms, want < %dms", p95, reconcileP95GateMs)
	}
	if wideSteps := queryInt(ctx, t, env, `SELECT count(*) FROM workflow_steps s WHERE s.workflow_id = $1`, wideID); wideSteps != int64(steps) {
		t.Fatalf("wide workflow steps = %d, want %d", wideSteps, steps)
	}
}
