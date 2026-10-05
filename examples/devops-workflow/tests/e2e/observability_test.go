//go:build integration

package devops_test

import (
	"context"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/observability"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestObservabilityCorrelationChain proves the §Phase-7 correlation contract:
// one execution exposes the full chain Workflow → Step → Task → Run →
// Attempt → Tool Call → Memory → Result, with every hop linked and audited.
func TestObservabilityCorrelationChain(t *testing.T) {
	h := newHarness(t, "obs-chain", false)
	id, err := h.createWorkflow("Observability chain: checkout incident")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	// Approve the execute step (same flow as the full acceptance test).
	waitForApproval(t, h, id)
	h.approveStep(id, "execute", true)
	h.requireCompleted(id, settleTimeout+3*time.Minute)

	correlation, err := h.store.GetExecutionCorrelation(context.Background(), devopsTenant, id)
	if err != nil {
		t.Fatalf("get execution correlation: %v", err)
	}
	if issues := observability.ValidateCorrelation(correlation); len(issues) != 0 {
		t.Fatalf("correlation chain broken: %v", issues)
	}

	// The chain must be complete.
	if len(correlation.Steps) != 6 {
		t.Fatalf("steps = %d, want 6", len(correlation.Steps))
	}
	// The rollback step is skipped when the fix heals, so 6 steps → 5 tasks.
	if len(correlation.Tasks) < 5 {
		t.Fatalf("tasks = %d, want >= 5", len(correlation.Tasks))
	}
	if len(correlation.Attempts) < 5 {
		t.Fatalf("attempts = %d, want >= 5", len(correlation.Attempts))
	}
	// Tools: observer calls get+logs, diagnoser get, executor restart, verifier get.
	if len(correlation.ToolCalls) < 4 {
		t.Fatalf("tool calls = %d, want >= 4", len(correlation.ToolCalls))
	}
	var restarts int
	for _, call := range correlation.ToolCalls {
		if call.ToolName == "kubernetes.restart" {
			restarts++
		}
	}
	if restarts != 1 {
		t.Fatalf("restart tool calls = %d, want exactly 1 (no duplicate side effect)", restarts)
	}
	// Memory: plan, observation, diagnosis, execution, verification records.
	if len(correlation.Memories) < 5 {
		t.Fatalf("memory records = %d, want >= 5", len(correlation.Memories))
	}
	if correlation.RuntimeOperationReceipts < 1 {
		t.Fatalf("idempotency receipts = %d, want >= 1", correlation.RuntimeOperationReceipts)
	}
	if len(correlation.AuditEvents) < 1 {
		t.Fatalf("audit events = %d, want >= 1", len(correlation.AuditEvents))
	}
	// Every task reached a result reference.
	for _, task := range correlation.Tasks {
		if task.ResultRef == "" {
			t.Fatalf("task %s (%s) has no result reference", task.ID, task.AgentVersionRef)
		}
	}
	t.Logf("correlation chain complete: %d steps, %d tasks, %d attempts, %d tool calls, %d memory, %d audit",
		len(correlation.Steps), len(correlation.Tasks), len(correlation.Attempts),
		len(correlation.ToolCalls), len(correlation.Memories), len(correlation.AuditEvents))
}

// TestObservabilityMetrics proves the §Phase-7 core metrics aggregate from
// the durable store after real executions.
func TestObservabilityMetrics(t *testing.T) {
	h := newHarness(t, "obs-metrics", false)
	started := time.Now().Add(-time.Minute)

	// Two full executions (both SUCCEED; the approval-rejected variant also
	// converges to SUCCEEDED via SKIPPED steps).
	for index := 0; index < 2; index++ {
		id, err := h.createWorkflow("Metrics run: checkout incident")
		if err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		waitForApproval(t, h, id)
		h.approveStep(id, "execute", true)
		h.requireCompleted(id, settleTimeout+3*time.Minute)
	}

	metrics, err := h.store.AggregateMetrics(context.Background(), devopsTenant, started)
	if err != nil {
		t.Fatalf("aggregate metrics: %v", err)
	}
	if metrics.WorkflowCount != 2 || metrics.WorkflowSucceeded != 2 {
		t.Fatalf("workflow metrics = %d/%d succeeded, want 2/2", metrics.WorkflowSucceeded, metrics.WorkflowCount)
	}
	if metrics.WorkflowSuccessRate != 1.0 || metrics.TaskSuccessRate != 1.0 {
		t.Fatalf("success rates = %.2f/%.2f, want 1.0/1.0", metrics.WorkflowSuccessRate, metrics.TaskSuccessRate)
	}
	if metrics.TaskCount < 10 {
		t.Fatalf("tasks = %d, want >= 8", metrics.TaskCount)
	}
	if metrics.ToolCalls < 8 {
		t.Fatalf("tool calls = %d, want >= 8", metrics.ToolCalls)
	}
	if metrics.MemoryRecords < 10 {
		t.Fatalf("memory records = %d, want >= 10", metrics.MemoryRecords)
	}
	if metrics.AuditEvents < 2 {
		t.Fatalf("audit events = %d, want >= 2", metrics.AuditEvents)
	}
	if metrics.SchedulingLatencyMillis.P50 < 0 || metrics.SchedulingLatencyMillis.P95 < 0 {
		t.Fatalf("scheduling latency negative: %+v", metrics.SchedulingLatencyMillis)
	}
	if metrics.RetryRate < 0 || metrics.RetryRate > 1 {
		t.Fatalf("retry rate out of range: %v", metrics.RetryRate)
	}
	if metrics.RecoveryRate < 0 || metrics.RecoveryRate > 1 {
		t.Fatalf("recovery rate out of range: %v", metrics.RecoveryRate)
	}
	if metrics.BudgetDrift {
		t.Fatalf("budget drift detected (reserved != settled)")
	}
	if metrics.CapacityDrift != 0 {
		t.Fatalf("capacity drift = %d ACTIVE reservations", metrics.CapacityDrift)
	}
	if metrics.DuplicateSideEffects != 0 {
		t.Fatalf("duplicate side effects = %d, want 0", metrics.DuplicateSideEffects)
	}
	if metrics.CrossTenantViolations != 0 {
		t.Fatalf("cross-tenant violations = %d, want 0", metrics.CrossTenantViolations)
	}
	t.Logf("metrics: workflows=%d success=%.0f%% tasks=%d toolCalls=%d scheduleP50=%.1fms retry=%.2f recovery=%.2f dup=0 drift=0",
		metrics.WorkflowCount, metrics.WorkflowSuccessRate*100, metrics.TaskCount, metrics.ToolCalls,
		metrics.SchedulingLatencyMillis.P50, metrics.RetryRate, metrics.RecoveryRate)
}

// waitForApproval waits until the execute step needs human approval.
func waitForApproval(t *testing.T, h *harness, id uuid.UUID) {
	t.Helper()
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
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
}
