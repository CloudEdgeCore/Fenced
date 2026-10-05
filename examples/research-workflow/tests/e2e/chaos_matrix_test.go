//go:build integration

package research_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// chaosCase is one row of the §Phase-3 unified failure matrix.
type chaosCase struct {
	Name string
	// Tune applies the failure injection to the harness before the workflow
	// starts (provider status codes, tool failures, worker crashes).
	Tune func(*harness)
	// Want is the expected terminal workflow status.
	Want string
	// Timeout for the run (0 = default settleTimeout+3m).
	Timeout time.Duration
}

// TestChaosMatrix is the §Phase-3 unified failure matrix. Every case
// exercises one failure mode (model, tool, or runtime) and verifies the
// workflow converges to a deterministic terminal state. Workflow-level
// failure modes (budget exhaustion, deadline exceeded, child denial,
// cancellation, spawn overflow) are verified by dedicated tests:
// TestResearchWorkflowBudgetStop, TestEngineHardStopsExpiredWorkflow
// (kernel), the spawn denial suite, and TestResearchAppAPICancel.
func TestChaosMatrix(t *testing.T) {
	goal := "Chaos matrix run: agent runtime infrastructure."

	cases := []chaosCase{
		// ── Model failures (§Phase-3 model matrix) ──────────────────────────────
		// 429 is unambiguous-retryable: the invoker retries, the task
		// recovers, and the workflow SUCCEEDs.
		{"model-429", func(h *harness) { h.provider.InjectHTTPFailures(http.StatusTooManyRequests, 3) }, "SUCCEEDED", 0},
		// 5xx is ambiguous: the bounded invoker retry is consumed but the
		// attempt is not workflow-retried (ambiguous → no safe retry), so
		// the planner task fails and the workflow converges to FAILED.
		{"model-500", func(h *harness) { h.provider.InjectHTTPFailures(http.StatusInternalServerError, 3) }, "FAILED", 0},
		{"model-502", func(h *harness) { h.provider.InjectHTTPFailures(http.StatusBadGateway, 3) }, "FAILED", 0},
		{"model-503", func(h *harness) { h.provider.InjectHTTPFailures(http.StatusServiceUnavailable, 3) }, "FAILED", 0},
		// Reader model failures exhaust bounded retries, but the search step
		// seeds snippet evidence that sustains the analyst → the workflow
		// still SUCCEEDs (deterministic convergence, robustness).
		{"model-reader-exhausted", func(h *harness) {
			h.provider.InjectRoleHTTPFailures("You extract verifiable factual claims", http.StatusInternalServerError, 100)
		}, "SUCCEEDED", 0},

		// ── Tool failures (§Phase-3 tool matrix) ────────────────────────────────
		// Every fetch fails with 500, but the search step has already seeded
		// snippet-level evidence → the workflow SUCCEEDs (no duplicate side
		// effects, deterministic convergence).
		{"tool-500-exhausted", func(h *harness) { h.webtools.InjectFetchFailures("corpus.fenced.dev", 100) }, "SUCCEEDED", 0},

		// ── Runtime failures (§Phase-3 runtime matrix) ──────────────────────────
		// Worker crash + lease expiry + restart: recovery requeues onto the
		// same instance, which completes the stranded attempts. The stronger
		// cross-pool re-placement (cordon after placement) is proven by
		// TestMultiRuntimeWorkerRecoveryReplacement.
		{"worker-crash-restart", nil, "SUCCEEDED", 0},
	}

	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.Name, func(t *testing.T) {
			runChaosCase(t, goal, testCase)
		})
	}
}

func runChaosCase(t *testing.T, goal string, cc chaosCase) {
	timeout := cc.Timeout
	if timeout <= 0 {
		timeout = settleTimeout + 3*time.Minute
	}
	workers := 4
	if cc.Name == "worker-crash-restart" {
		workers = 6 // two sandbox pools so the restart target is unambiguous
	}
	h := newHarnessWith(t, "chaos-"+cc.Name, nil, HarnessConfig{Workers: workers})
	if cc.Tune != nil {
		cc.Tune(h)
	}

	if cc.Name == "worker-crash-restart" {
		runChaosWorkerCrash(t, h, goal, timeout)
		return
	}

	id, err := h.createResearch(goal)
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	workflow, _, err := h.awaitWorkflow(id, timeout)
	if err != nil {
		dumpSteps(t, h, id)
		t.Fatalf("await workflow: %v", err)
	}
	if string(workflow.Status) != cc.Want {
		dumpSteps(t, h, id)
		t.Fatalf("chaos case %s: workflow status = %s, want %s", cc.Name, workflow.Status, cc.Want)
	}
}

// runChaosWorkerCrash exercises the crash-restart recovery: kill the sandbox
// worker mid-flight, force its leases to expiry, and restart the same
// instance; the requeued attempts complete on the restarted worker and the
// workflow SUCCEEDs.
func runChaosWorkerCrash(t *testing.T, h *harness, goal string, timeout time.Duration) {
	ctx := context.Background()
	id, err := h.createResearch(goal)
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		var placed int
		if err := h.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM attempts a
			 JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
			 JOIN workflow_steps s ON s.task_id = r.task_id AND s.tenant_id = r.tenant_id
			 WHERE s.workflow_id = $1 AND s.tenant_id = $2
			   AND s.agent_version_ref = 'research-reader@1.0.0'
			   AND a.runtime_instance_id = 'research-worker-02'`,
			id, researchTenant).Scan(&placed); err != nil {
			t.Fatalf("count placed reader attempts: %v", err)
		}
		if placed >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no reader placed on worker-02 within 90s")
		}
		time.Sleep(150 * time.Millisecond)
	}
	h.KillWorker("research-worker-02")
	recoverDeadline := time.Now().Add(60 * time.Second)
	for {
		var retried int
		if err := h.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM attempts a WHERE a.runtime_instance_id = 'research-worker-02' AND a.ordinal >= 2`,
		).Scan(&retried); err != nil {
			t.Fatalf("count retries: %v", err)
		}
		if retried >= 1 {
			break
		}
		if time.Now().After(recoverDeadline) {
			t.Fatalf("no replacement attempt on worker-02")
		}
		h.pool.Exec(ctx, `
			UPDATE runtime_leases l SET expires_at = l.acquired_at + INTERVAL '1 microsecond'
			FROM attempts a WHERE a.id = l.attempt_id
			  AND a.runtime_instance_id = 'research-worker-02' AND l.released_at IS NULL`)
		time.Sleep(100 * time.Millisecond)
	}
	h.startWorker(h.loopCtx, "research-worker-02")

	workflow, _, err := h.awaitWorkflow(id, timeout)
	if err != nil {
		dumpSteps(t, h, id)
		t.Fatalf("await workflow: %v", err)
	}
	if string(workflow.Status) != "SUCCEEDED" {
		dumpSteps(t, h, id)
		t.Fatalf("worker-crash workflow status = %s, want SUCCEEDED", workflow.Status)
	}
	var completed int
	if err := h.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM attempts a WHERE a.runtime_instance_id = 'research-worker-02' AND a.ordinal >= 2 AND a.phase = 'COMPLETED'`,
	).Scan(&completed); err != nil {
		t.Fatalf("count completed replacements: %v", err)
	}
	if completed == 0 {
		t.Fatalf("no replacement attempt completed on the restarted worker")
	}
}

func dumpSteps(t *testing.T, h *harness, id uuid.UUID) {
	t.Helper()
	steps, err := h.store.ListWorkflowSteps(context.Background(), researchTenant, id)
	if err != nil {
		t.Logf("list steps: %v", err)
		return
	}
	for _, step := range steps {
		t.Logf("step %-22s status=%-10s attempt=%d code=%q", step.Name, step.Status, step.AttemptCount, step.FailureCode)
	}
}
