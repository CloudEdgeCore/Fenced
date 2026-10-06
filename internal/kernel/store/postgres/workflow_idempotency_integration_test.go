//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestWorkflowIdempotencyScopeIncludesNamespace proves the workflow replay
// scope is (tenant, namespace, idempotency key): the same key under
// different namespaces creates independent workflows, while the same scope
// replays the original definition and rejects a conflicting one.
func TestWorkflowIdempotencyScopeIncludesNamespace(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	base := kernelstore.CreateWorkflowInput{
		TenantID: "tenant-scope", Namespace: "orders", IdempotencyKey: "key-123",
		Goal: "namespace-scoped", Spec: []byte(`{"steps":[{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{}}]}`),
		Steps: []kernelstore.CreateWorkflowStepInput{{Name: "planner", AgentVersionRef: "planner@1", Goal: "plan", Spec: []byte(`{}`)}},
	}
	base.ID = uuid.New()
	first, err := repository.CreateWorkflow(ctx, base)
	if err != nil || first.Existing {
		t.Fatalf("create first: %+v err=%v", first.Workflow, err)
	}

	// The same key in another namespace is a different workflow.
	otherNamespace := base
	otherNamespace.ID = uuid.New()
	otherNamespace.Namespace = "reports"
	second, err := repository.CreateWorkflow(ctx, otherNamespace)
	if err != nil || second.Existing || second.Workflow.ID == first.Workflow.ID {
		t.Fatalf("same key in another namespace: %+v err=%v", second.Workflow, err)
	}

	// The same scope replays the original definition.
	replay := base
	replay.ID = uuid.New()
	replayed, err := repository.CreateWorkflow(ctx, replay)
	if err != nil || !replayed.Existing || replayed.Workflow.ID != first.Workflow.ID {
		t.Fatalf("same-scope replay: existing=%v id=%s err=%v", replayed.Existing, replayed.Workflow.ID, err)
	}

	// The same scope with a different definition is a conflict.
	conflicting := base
	conflicting.ID = uuid.New()
	conflicting.Goal = "different definition"
	_, err = repository.CreateWorkflow(ctx, conflicting)
	if !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("conflicting definition error = %v, want ErrIdempotencyConflict", err)
	}
}
