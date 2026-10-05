//go:build integration

package research_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	research "github.com/CloudEdgeCore/Fenced/examples/research-workflow/runtime"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	workflowkernel "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/google/uuid"
)

func agentRefs() []string {
	return []string{
		"research-planner@1.0.0",
		"research-search@1.0.0",
		"research-reader@1.0.0",
		"research-collector@1.0.0",
		"research-analyst@1.0.0",
		"research-critic@1.0.0",
		"research-writer@1.0.0",
		"research-citation-validator@1.0.0",
	}
}

type staticPools []scheduler.RuntimePool

func (s staticPools) ListRuntimePools(context.Context, string) ([]scheduler.RuntimePool, error) {
	return s, nil
}

// loggedRuntime surfaces adapter errors that the host intentionally reduces
// to ADAPTER_FAILED, so harness diagnostics show the root cause.
type loggedRuntime struct {
	inner *research.Runtime
}

func (l *loggedRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	output, err := l.inner.Run(ctx, request, emit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[research-e2e] runtime run %s goal=%.120s: %v\n", request.ExecutionID, request.Goal, err)
	}
	return output, err
}

func (l *loggedRuntime) Checkpoint(ctx context.Context, executionID string) (agent.Checkpoint, error) {
	return l.inner.Checkpoint(ctx, executionID)
}

func (l *loggedRuntime) Restore(ctx context.Context, request agent.RestoreRequest) error {
	return l.inner.Restore(ctx, request)
}

// newAgentHandler hosts the multi-role runtime behind the Runtime Interface.
// models carries the ACTIVE logical model refs (deterministic fake refs or
// live logical routes).
func newAgentHandler(mcpURL string, models research.Models) (http.Handler, error) {
	host, err := agent.NewHost(&loggedRuntime{inner: research.NewRuntime(mcpURL, models)}, agent.HostOptions{Adapter: "research-e2e", MaxConcurrent: 64})
	if err != nil {
		return nil, err
	}
	return host, nil
}

// publishAgents renders the manifest templates and publishes all eight
// versions into the tenant registry.
func (h *harness) publishAgents() {
	h.t.Helper()
	manifestDir, err := filepath.Abs(filepath.Join("..", "..", "agents"))
	if err != nil {
		h.t.Fatalf("manifest dir: %v", err)
	}
	ctx := context.Background()
	for _, ref := range agentRefs() {
		name, _, _ := strings.Cut(ref, "@")
		fileName := strings.TrimPrefix(name, "research-")
		raw, err := os.ReadFile(filepath.Join(manifestDir, fileName+".json"))
		if err != nil {
			h.t.Fatalf("read manifest %s: %v", name, err)
		}
		rendered := strings.NewReplacer(
			"__MODEL_FAST__", h.models.Fast,
			"__MODEL_READER__", h.models.Reader,
			"__MODEL_REASONING__", h.models.Reasoning,
		).Replace(string(raw))
		var document struct {
			Metadata struct {
				Name      string `json:"name"`
				Version   string `json:"version"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(rendered), &document); err != nil {
			h.t.Fatalf("decode manifest %s: %v", name, err)
		}
		specStart := strings.Index(rendered, `"spec"`)
		if specStart < 0 {
			h.t.Fatalf("manifest %s has no spec", name)
		}
		// Validate the spec object exactly as publication would.
		var full struct {
			Spec json.RawMessage `json:"spec"`
		}
		if err := json.Unmarshal([]byte(rendered), &full); err != nil {
			h.t.Fatalf("decode spec %s: %v", name, err)
		}
		if err := agentversion.ValidateSpec(full.Spec); err != nil {
			h.t.Fatalf("spec %s invalid: %v", name, err)
		}
		if _, err := h.store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
			ID: uuid.New(), TenantID: researchTenant, Namespace: document.Metadata.Namespace,
			Name: document.Metadata.Name, Version: document.Metadata.Version, Spec: full.Spec,
		}); err != nil {
			h.t.Fatalf("publish %s: %v", ref, err)
		}
	}
}

// createResearch renders the workflow template for one goal and publishes the
// workflow run through the kernel store (the same path the Control API uses).
func (h *harness) createResearch(goal string) (uuid.UUID, error) {
	templatePath, err := filepath.Abs(filepath.Join("..", "..", "workflow", "research-workflow.json"))
	if err != nil {
		return uuid.Nil, err
	}
	template, err := os.ReadFile(templatePath)
	if err != nil {
		return uuid.Nil, err
	}
	workflowID := uuid.New()
	envelope := func(role string, round int) string {
		payload, _ := json.Marshal(map[string]any{
			"role": role, "goal": goal, "workflowId": workflowID.String(), "round": round,
		})
		return research.EnvelopePrefix() + string(payload)
	}
	// Build the concrete spec document programmatically: placeholders are
	// replaced at the object level so envelope strings stay properly escaped.
	var document map[string]any
	if err := json.Unmarshal(template, &document); err != nil {
		return uuid.Nil, fmt.Errorf("workflow template: %w", err)
	}
	document["deadline"] = time.Now().Add(45 * time.Minute).UTC().Format(time.RFC3339)
	goalPlaceholders := map[string]string{
		"__ENVELOPE_PLANNER__":      envelope("planner", 0),
		"__ENVELOPE_ANALYST_R1__":   envelope("analyst", 1),
		"__ENVELOPE_CRITIC_R1__":    envelope("critic", 1),
		"__ENVELOPE_COLLECTOR_R2__": envelope("collector", 2),
		"__ENVELOPE_ANALYST_R2__":   envelope("analyst", 2),
		"__ENVELOPE_CRITIC_R2__":    envelope("critic", 2),
		"__ENVELOPE_COLLECTOR_R3__": envelope("collector", 3),
		"__ENVELOPE_ANALYST_R3__":   envelope("analyst", 3),
		"__ENVELOPE_CRITIC_R3__":    envelope("critic", 3),
		"__ENVELOPE_WRITER__":       envelope("writer", 3),
		"__ENVELOPE_VALIDATOR__":    envelope("validator", 3),
	}
	rawSteps, ok := document["steps"].([]any)
	if !ok {
		return uuid.Nil, fmt.Errorf("workflow template has no steps array")
	}
	for _, rawStep := range rawSteps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if placeholder, ok := step["goal"].(string); ok {
			if envelopeValue, matched := goalPlaceholders[placeholder]; matched {
				step["goal"] = envelopeValue
			}
		}
	}
	rendered, err := json.Marshal(document)
	if err != nil {
		return uuid.Nil, err
	}
	spec, err := workflowkernel.DecodeWorkflowSpec(rendered)
	if err != nil {
		return uuid.Nil, fmt.Errorf("workflow spec: %w", err)
	}
	tasks, tokens, cost := spec.Budgets()
	deadline := time.Now().Add(45 * time.Minute).UTC()
	result, err := h.store.CreateWorkflow(context.Background(), kernelstore.CreateWorkflowInput{
		ID: workflowID, TenantID: researchTenant, Namespace: "default",
		IdempotencyKey: "research/" + workflowID.String(), Goal: goal, Spec: rendered,
		Steps: spec.StepInputs(), BudgetMaxTasks: tasks, BudgetMaxTokens: tokens,
		BudgetMaxCostMicroUSD: cost, DeadlineAt: &deadline,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return result.Workflow.ID, nil
}

// awaitWorkflow polls until the workflow reaches a terminal state or the
// deadline expires, returning the final record.
func (h *harness) awaitWorkflow(id uuid.UUID, timeout time.Duration) (kernelstore.Workflow, []kernelstore.WorkflowStep, error) {
	deadline := time.Now().Add(timeout)
	var last kernelstore.Workflow
	for time.Now().Before(deadline) {
		workflow, err := h.store.GetWorkflow(context.Background(), researchTenant, id)
		if err != nil {
			return kernelstore.Workflow{}, nil, err
		}
		last = workflow
		switch workflow.Status {
		case kernelstore.WorkflowSucceeded, kernelstore.WorkflowFailed, kernelstore.WorkflowCancelled:
			steps, err := h.store.ListWorkflowSteps(context.Background(), researchTenant, id)
			return workflow, steps, err
		}
		time.Sleep(150 * time.Millisecond)
	}
	steps, listErr := h.store.ListWorkflowSteps(context.Background(), researchTenant, id)
	if listErr != nil {
		return last, nil, fmt.Errorf("workflow %s did not settle within %s (list steps: %w)", id, timeout, listErr)
	}
	return last, steps, fmt.Errorf("workflow %s did not settle within %s", id, timeout)
}

// requireCompleted fails the test unless the workflow succeeded, dumping the
// step table for diagnosis.
func (h *harness) requireCompleted(id uuid.UUID, timeout time.Duration) kernelstore.Workflow {
	h.t.Helper()
	workflow, steps, err := h.awaitWorkflow(id, timeout)
	if err != nil {
		for _, step := range steps {
			h.t.Logf("step %-22s status=%-10s attempt=%d code=%q", step.Name, step.Status, step.AttemptCount, step.FailureCode)
		}
		h.t.Fatalf("await workflow: %v", err)
	}
	if h.t.Failed() {
		return workflow
	}
	if workflow.Status != kernelstore.WorkflowSucceeded {
		for _, step := range steps {
			h.t.Logf("step %-22s status=%-10s attempt=%d code=%q", step.Name, step.Status, step.AttemptCount, step.FailureCode)
		}
		h.t.Fatalf("workflow status = %s, want SUCCEEDED", workflow.Status)
	}
	return workflow
}

var _ = context.Background
