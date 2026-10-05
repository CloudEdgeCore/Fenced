//go:build integration

package research_test

import (
	"context"
	"testing"
	"time"

	workflowkernel "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/google/uuid"
)

// expectedClassByRef is the §2.1 role→runtime mapping the acceptance gates
// assert: reasoning roles on the reasoning class, search/collector on the
// network class, readers on the sandbox class.
var expectedClassByRef = map[string]string{
	"research-planner@1.0.0":            "research-reasoning",
	"research-analyst@1.0.0":            "research-reasoning",
	"research-critic@1.0.0":             "research-reasoning",
	"research-writer@1.0.0":             "research-reasoning",
	"research-citation-validator@1.0.0": "research-reasoning",
	"research-collector@1.0.0":          "research-network",
	"research-search@1.0.0":             "research-network",
	"research-reader@1.0.0":             "research-sandbox",
}

// stepPoolClasses returns the LATEST attempt placement per workflow step
// (the successful attempt's runtime class and instance), keyed by step name.
// The attempt row records the class it was placed on, so released leases do
// not matter. Static steps store their agent ref in the workflow spec (not
// the step row); dynamic steps carry it on the row.
func (h *harness) stepPoolClasses(workflowID uuid.UUID) map[string]stepPlacement {
	h.t.Helper()
	ctx := context.Background()
	workflow, err := h.store.GetWorkflow(ctx, researchTenant, workflowID)
	if err != nil {
		h.t.Fatalf("get workflow: %v", err)
	}
	spec, err := workflowkernel.DecodeWorkflowSpec(workflow.Spec)
	if err != nil {
		h.t.Fatalf("decode workflow spec: %v", err)
	}
	refByStep := map[string]string{}
	for _, step := range spec.Steps {
		refByStep[step.Name] = step.AgentVersionRef
	}
	rows, err := h.pool.Query(ctx, `
		SELECT DISTINCT ON (s.name) s.name, COALESCE(s.agent_version_ref, ''), a.runtime_class, a.runtime_instance_id
		FROM workflow_steps s
		JOIN tasks t ON t.id = s.task_id AND t.tenant_id = s.tenant_id
		JOIN runs r ON r.task_id = t.id AND r.tenant_id = t.tenant_id
		JOIN attempts a ON a.run_id = r.id AND a.tenant_id = r.tenant_id
		WHERE s.workflow_id = $1 AND s.tenant_id = $2
		ORDER BY s.name, a.created_at DESC`, workflowID, researchTenant)
	if err != nil {
		h.t.Fatalf("query step placements: %v", err)
	}
	defer rows.Close()
	placements := map[string]stepPlacement{}
	for rows.Next() {
		var placement stepPlacement
		if err := rows.Scan(&placement.Name, &placement.AgentVersionRef, &placement.RuntimeClass, &placement.RuntimeInstanceID); err != nil {
			h.t.Fatalf("scan step placement: %v", err)
		}
		if placement.AgentVersionRef == "" {
			placement.AgentVersionRef = refByStep[placement.Name]
		}
		placement.PoolID = h.poolIDForInstance(placement.RuntimeInstanceID)
		placements[placement.Name] = placement
	}
	if err := rows.Err(); err != nil {
		h.t.Fatalf("iterate step placements: %v", err)
	}
	return placements
}

// poolIDForInstance resolves the pool id that owns a runtime instance.
func (h *harness) poolIDForInstance(instance string) string {
	h.t.Helper()
	for _, pool := range h.pools {
		if pool.RuntimeInstanceID == instance {
			return pool.ID
		}
	}
	return ""
}

type stepPlacement struct {
	Name              string
	AgentVersionRef   string
	PoolID            string
	RuntimeClass      string
	RuntimeInstanceID string
}

// assertRolePlacement verifies every dispatched step landed on its role's
// runtime class, and that each role actually dispatched.
func (h *harness) assertRolePlacement(workflowID uuid.UUID) {
	h.t.Helper()
	placements := h.stepPoolClasses(workflowID)
	if len(placements) == 0 {
		h.t.Fatalf("no step placements recorded for workflow %s", workflowID)
	}
	seen := map[string]bool{}
	for name, placement := range placements {
		expected, ok := expectedClassByRef[placement.AgentVersionRef]
		if !ok {
			h.t.Fatalf("step %s has unexpected agent ref %s", name, placement.AgentVersionRef)
		}
		seen[placement.AgentVersionRef] = true
		if placement.RuntimeClass != expected {
			h.t.Fatalf("step %s (%s) placed on class %s (pool %s), want %s",
				name, placement.AgentVersionRef, placement.RuntimeClass, placement.PoolID, expected)
		}
	}
	for _, ref := range []string{
		"research-planner@1.0.0", "research-analyst@1.0.0", "research-critic@1.0.0",
		"research-writer@1.0.0", "research-citation-validator@1.0.0",
		"research-collector@1.0.0", "research-search@1.0.0", "research-reader@1.0.0",
	} {
		if !seen[ref] {
			h.t.Fatalf("role %s never dispatched a step", ref)
		}
	}
}

// TestMultiRuntimeRolePlacement is Phase 1 acceptance: one workflow fans out
// across heterogeneous runtime classes, and every role runs on its mapped
// class (planner/analyst/critic/writer/validator → reasoning, search/collector
// → network, reader → sandbox) with the workflow declaring only the class set.
func TestMultiRuntimeRolePlacement(t *testing.T) {
	h := newHarness(t, "multiruntime", nil)
	id, err := h.createResearch("Multi-runtime survey of agent infrastructure")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout+3*time.Minute)
	h.assertRolePlacement(id)
}

// TestMultiRuntimeMigration is Phase 1 acceptance: the same workflow document
// (unchanged) migrates readers from runtime A to runtime B when A is
// cordoned. Every reader lands on the surviving sandbox pool.
func TestMultiRuntimeMigration(t *testing.T) {
	h := newHarnessWith(t, "migration", nil, HarnessConfig{Workers: 6})
	h.cordonPool("sandbox-pool")
	id, err := h.createResearch("Migration survey of agent runtime isolation")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout+3*time.Minute)
	placements := h.stepPoolClasses(id)
	for name, placement := range placements {
		if placement.AgentVersionRef == "research-reader@1.0.0" {
			if placement.PoolID != "sandbox-pool-2" {
				t.Fatalf("reader step %s placed on %s, want the surviving sandbox pool sandbox-pool-2", name, placement.PoolID)
			}
		}
	}
	h.assertRolePlacement(id)
}

// TestMultiRuntimeCapacityExhaustion is Phase 1 acceptance: with one sandbox
// pool capacity-exhausted, placement walks the ranked candidates and readers
// run on the other sandbox pool.
func TestMultiRuntimeCapacityExhaustion(t *testing.T) {
	h := newHarnessWith(t, "capacity", nil, HarnessConfig{Workers: 6})
	h.exhaustPool("sandbox-pool")
	id, err := h.createResearch("Capacity survey of agent runtime scheduling")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout+3*time.Minute)
	placements := h.stepPoolClasses(id)
	for name, placement := range placements {
		if placement.AgentVersionRef == "research-reader@1.0.0" && placement.PoolID != "sandbox-pool-2" {
			t.Fatalf("reader step %s placed on %s, want the capacity-available sandbox pool", name, placement.PoolID)
		}
	}
	h.assertRolePlacement(id)
}

// TestMultiRuntimeWorkerRecoveryReplacement is Phase 1 acceptance: a worker
// crash on the sandbox class plus lease expiry triggers re-placement to the
// surviving sandbox pool (the kernel re-routes the expired attempt through
// the scheduler when its pool is cordoned). The workflow still SUCCEEDs, and
// the replacement attempt is recorded (fencing + re-placement, no duplicate
// side effects).
func TestMultiRuntimeWorkerRecoveryReplacement(t *testing.T) {
	h := newHarnessWith(t, "replacement", nil, HarnessConfig{Workers: 6})
	id, err := h.createResearch("Recovery survey of multi-runtime agents")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	// Wait until at least one reader attempt is actually PLACED on the
	// sandbox-pool instance (worker-02).
	deadline := time.Now().Add(90 * time.Second)
	for {
		var placed int
		if err := h.pool.QueryRow(context.Background(),
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
			t.Fatalf("no reader attempt placed on worker-02 within 90s")
		}
		time.Sleep(150 * time.Millisecond)
	}
	// Crash the sandbox-class worker and cordon its pool. The kernel
	// re-placement re-routes requeued attempts through the scheduler onto
	// the surviving sandbox pool (sandbox-pool-2, worker-05).
	h.KillWorker("research-worker-02")
	h.cordonPool("sandbox-pool")

	recoverDeadline := time.Now().Add(60 * time.Second)
	for {
		var retried int
		if err := h.pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM attempts a
			 JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
			 WHERE a.runtime_instance_id = 'research-worker-05' AND a.ordinal >= 1`,
		).Scan(&retried); err != nil {
			t.Fatalf("count replacement attempts on survivor: %v", err)
		}
		if retried >= 1 {
			break
		}
		if time.Now().After(recoverDeadline) {
			t.Fatalf("re-placement placed no attempt on the surviving sandbox pool")
		}
		h.pool.Exec(context.Background(), `
			UPDATE runtime_leases l
			SET expires_at = l.acquired_at + INTERVAL '1 microsecond'
			FROM attempts a
			WHERE a.id = l.attempt_id
			  AND a.runtime_instance_id = 'research-worker-02'
			  AND l.released_at IS NULL`)
		time.Sleep(100 * time.Millisecond)
	}

	h.requireCompleted(id, settleTimeout+3*time.Minute)

	// Every reader's LATEST attempt must be on the surviving sandbox pool.
	placements := h.stepPoolClasses(id)
	readerOnOldPool := 0
	for name, placement := range placements {
		if placement.AgentVersionRef != "research-reader@1.0.0" {
			continue
		}
		if placement.PoolID == "sandbox-pool" {
			readerOnOldPool++
		}
		if placement.PoolID != "sandbox-pool-2" {
			t.Fatalf("replaced reader step %s landed on %s, want sandbox-pool-2", name, placement.PoolID)
		}
	}
	if readerOnOldPool != 0 {
		t.Fatalf("%d reader steps still bound to the crashed pool", readerOnOldPool)
	}
	h.assertRolePlacement(id)
	t.Logf("multi-runtime re-placement complete: readers migrated to sandbox-pool-2")
}

// exhaustPool drains one pool's LLM capacity so placement rejects it with
// LLM_CAPACITY_EXHAUSTED and walks the remaining candidates.
func (h *harness) exhaustPool(id string) {
	h.t.Helper()
	for index := range h.pools {
		if h.pools[index].ID == id {
			h.pools[index].AvailableLLMSlots = 0
			return
		}
	}
	h.t.Fatalf("pool %q not found in harness pools", id)
}
