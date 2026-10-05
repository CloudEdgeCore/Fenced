//go:build integration

package devops_test

import (
	"context"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestCrossClassTakeover is the P1 takeover drill: a task whose placement
// allows two runtime classes (research-network + research-sandbox) is placed
// on its PREFERRED class when available, and on the OTHER class when the
// preferred pools are cordoned. Proves cross-class placement, the kernel's
// pool-aware scheduling, and fencing correctness.
func TestCrossClassTakeover(t *testing.T) {
	h := newHarness(t, "crossclass", false)
	ctx := context.Background()

	createTask := func(goal, idemKey string) uuid.UUID {
		t.Helper()
		taskID := uuid.New()
		if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: taskID, TenantID: devopsTenant, Namespace: "default",
			AgentVersionRef: "hello-agent@1.0.0", Goal: goal,
			Spec: []byte(`{"priority":50,"budget":{"tokens":2000,"costUsd":0.10,"toolCalls":8,"wallSeconds":120},
				"placement":{"runtimeClasses":["research-network","research-sandbox"],"preferredClass":"research-network",
					"region":"cn-east","cpuMillis":250,"memoryMiB":256,"workspaceBytes":8388608,"llmConcurrency":2},
				"retryPolicy":{"maxAttempts":3}}`),
			IdempotencyKey: idemKey,
		}); err != nil {
			t.Fatalf("create task: %v", err)
		}
		return taskID
	}

	awaitTerminal := func(taskID uuid.UUID) (phase, class, instance string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			if err := h.pool.QueryRow(ctx,
				`SELECT t.phase, COALESCE(a.runtime_class,''), COALESCE(a.runtime_instance_id,'') FROM tasks t
				 LEFT JOIN runs r ON r.task_id=t.id AND r.tenant_id=t.tenant_id
				 LEFT JOIN attempts a ON a.run_id=r.id AND a.tenant_id=r.tenant_id
				 WHERE t.id = $1 ORDER BY a.ordinal DESC LIMIT 1`, taskID).Scan(&phase, &class, &instance); err != nil {
				t.Fatalf("get task state: %v", err)
			}
			if phase == "SUCCEEDED" || phase == "FAILED" || phase == "CANCELLED" || phase == "TIMED_OUT" || phase == "REJECTED" {
				return phase, class, instance
			}
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatalf("task %s did not settle within 3m", taskID)
		return "", "", ""
	}

	// Phase 1: all pools ACTIVE → the task lands on its preferred class
	// (research-network, worker-01 or worker-03).
	task1 := createTask("cross-class: preferred placement", "cross-class/preferred")
	phase1, class1, instance1 := awaitTerminal(task1)
	if phase1 != "SUCCEEDED" {
		t.Fatalf("preferred placement: phase = %s, want SUCCEEDED", phase1)
	}
	if class1 != "research-network" {
		t.Fatalf("preferred placement: class = %s, want research-network", class1)
	}
	t.Logf("preferred placement: %s on %s (research-network)", phase1, instance1)

	// Phase 2: cordon ALL network pools → the task must land on the
	// sandbox class (cross-class placement).
	h.cordonPool("devops-pool-1")
	h.cordonPool("devops-pool-3")
	task2 := createTask("cross-class: cordoned fallback", "cross-class/cordoned")
	phase2, class2, instance2 := awaitTerminal(task2)
	if phase2 != "SUCCEEDED" {
		t.Fatalf("cordoned fallback: phase = %s, want SUCCEEDED", phase2)
	}
	if class2 != "research-sandbox" {
		t.Fatalf("cross-class takeover: class = %s, want research-sandbox", class2)
	}
	t.Logf("cross-class takeover: %s on %s (research-sandbox, network cordoned)", phase2, instance2)

	// Phase 3: uncordon network pools → the preferred class is restored.
	h.uncordonPool("devops-pool-1")
	h.uncordonPool("devops-pool-3")
	task3 := createTask("cross-class: uncordoned restore", "cross-class/restored")
	phase3, class3, instance3 := awaitTerminal(task3)
	if phase3 != "SUCCEEDED" {
		t.Fatalf("uncordoned restore: phase = %s, want SUCCEEDED", phase3)
	}
	if class3 != "research-network" {
		t.Fatalf("uncordoned restore: class = %s, want research-network", class3)
	}
	t.Logf("uncordoned restore: %s on %s (research-network)", phase3, instance3)
}
