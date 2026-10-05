//go:build integration

package devops_test

import (
	"context"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// TestDevOpsWorkflowFull is Phase 4 acceptance: the full incident workflow
// runs through observe → diagnose → approval → execute → verify → (rollback
// skipped because the fix healed). The fake cluster starts unhealthy, restart
// heals it, verify reports HEALTHY, rollback is skipped.
func TestDevOpsWorkflowFull(t *testing.T) {
	h := newHarness(t, "full", false)
	id, err := h.createWorkflow("Checkout service is unhealthy — diagnose and fix")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	// Wait until the execute step needs human approval.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("execute step never reached WAITING_APPROVAL")
		}
		steps, err := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
		if err != nil {
			t.Fatalf("list steps: %v", err)
		}
		found := false
		for _, step := range steps {
			if step.Name == "execute" && step.Status == kernelstore.StepWaitingApproval {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	// Approve the execute step.
	h.approveStep(id, "execute", true)

	// Wait for completion.
	workflow := h.requireCompleted(id, settleTimeout+3*time.Minute)

	// Assert steps.
	steps, _ := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
	stepMap := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		stepMap[step.Name] = step
	}
	for _, name := range []string{"planner", "observe", "diagnose", "execute", "verify"} {
		if stepMap[name].Status != kernelstore.StepSucceeded {
			t.Fatalf("step %s = %s, want SUCCEEDED", name, stepMap[name].Status)
		}
	}
	// Rollback should be SKIPPED (the fix healed).
	if stepMap["rollback"].Status != kernelstore.StepSkipped {
		t.Fatalf("rollback = %s, want SKIPPED (healthy fix)", stepMap["rollback"].Status)
	}

	// The cluster should be healthy now.
	state := h.cluster.Snapshot()
	if !state.Healthy {
		t.Fatalf("cluster is still unhealthy after restart")
	}
	if state.RestartCount != 1 {
		t.Fatalf("restart count = %d, want 1", state.RestartCount)
	}
	t.Logf("devops workflow complete: %s", workflow.Status)
}

// TestDevOpsWorkflowApprovalRejected proves the approval rejection path:
// the execute step is SKIPPED with APPROVAL_REJECTED, and the workflow
// converges to SUCCEEDED (downstream steps are skipped).
func TestDevOpsWorkflowApprovalRejected(t *testing.T) {
	h := newHarness(t, "reject", false)
	id, err := h.createWorkflow("Reject approval test")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("execute step never reached WAITING_APPROVAL")
		}
		steps, err := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
		if err != nil {
			t.Fatalf("list steps: %v", err)
		}
		found := false
		for _, step := range steps {
			if step.Name == "execute" && step.Status == kernelstore.StepWaitingApproval {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	// Reject the execute step.
	h.approveStep(id, "execute", false)

	workflow := h.requireCompleted(id, settleTimeout+3*time.Minute)
	steps, _ := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
	stepMap := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		stepMap[step.Name] = step
	}
	if stepMap["execute"].Status != kernelstore.StepSkipped || stepMap["execute"].FailureCode != "APPROVAL_REJECTED" {
		t.Fatalf("execute = %s (%s), want SKIPPED APPROVAL_REJECTED",
			stepMap["execute"].Status, stepMap["execute"].FailureCode)
	}
	// Verify and rollback are skipped because execute was skipped.
	for _, name := range []string{"verify", "rollback"} {
		if stepMap[name].Status != kernelstore.StepSkipped {
			t.Fatalf("step %s = %s, want SKIPPED after rejection", name, stepMap[name].Status)
		}
	}
	// The cluster should remain UNHEALTHY (no restart was executed).
	state := h.cluster.Snapshot()
	if state.Healthy {
		t.Fatalf("cluster became healthy despite rejection")
	}
	if state.RestartCount != 0 {
		t.Fatalf("restart count = %d, want 0 (no restart executed)", state.RestartCount)
	}
	t.Logf("devops rejection complete: %s, execute rejected", workflow.Status)
}

// TestDevOpsWorkflowRollback drives the rollback path: the cluster is
// stubborn (restart never heals), so verify outputs ROLLBACK_REQUIRED, and
// the rollback step runs (scale back). The workflow still SUCCEEDs.
func TestDevOpsWorkflowRollback(t *testing.T) {
	h := newHarness(t, "rollback", true) // stubborn cluster
	id, err := h.createWorkflow("Stubborn failure — verify then rollback")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("execute step never reached WAITING_APPROVAL")
		}
		steps, err := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
		if err != nil {
			t.Fatalf("list steps: %v", err)
		}
		found := false
		for _, step := range steps {
			if step.Name == "execute" && step.Status == kernelstore.StepWaitingApproval {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.approveStep(id, "execute", true)

	h.requireCompleted(id, settleTimeout+3*time.Minute)

	steps, _ := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
	stepMap := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		stepMap[step.Name] = step
	}
	// Verify must have succeeded (it detected the fix didn't heal).
	if stepMap["verify"].Status != kernelstore.StepSucceeded {
		t.Fatalf("verify = %s, want SUCCEEDED", stepMap["verify"].Status)
	}
	// Rollback must have RUN (not skipped), because verify's output
	// contained "ROLLBACK_REQUIRED".
	if stepMap["rollback"].Status != kernelstore.StepSucceeded {
		t.Fatalf("rollback = %s, want SUCCEEDED (rollback ran)", stepMap["rollback"].Status)
	}
	// The cluster should still be unhealthy (stubborn — restart doesn't heal).
	state := h.cluster.Snapshot()
	if state.Healthy {
		t.Fatalf("stubborn cluster healed despite rollback")
	}
	if state.RestartCount != 1 {
		t.Fatalf("restart count = %d, want 1 (executor restarted)", state.RestartCount)
	}
	t.Logf("devops rollback complete: restart executed, verify detected failure, rollback scaled")
}
