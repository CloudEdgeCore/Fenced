//go:build integration

package devops_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	devops "github.com/CloudEdgeCore/Fenced/examples/devops-workflow/runtime"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	workflowkernel "github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/google/uuid"
)

func (h *harness) publishAgents() {
	h.t.Helper()
	manifestDir, err := filepath.Abs(filepath.Join("..", "..", "agents"))
	if err != nil {
		h.t.Fatalf("manifest dir: %v", err)
	}
	ctx := context.Background()
	for _, ref := range agentRefs() {
		name, _, _ := strings.Cut(ref, "@")
		manifestPath := filepath.Join(manifestDir, strings.TrimPrefix(name, "devops-")+".json")
		if slices.Contains(thirdPartyRefs(), ref) {
			// Third-party manifests live outside the reference workload.
			manifestPath, err = filepath.Abs(filepath.Join("..", "..", "..", "third-party", name, "agent.json"))
			if err != nil {
				h.t.Fatalf("third-party manifest dir: %v", err)
			}
		}
		raw, err := os.ReadFile(manifestPath)
		if err != nil {
			h.t.Fatalf("read manifest %s: %v", name, err)
		}
		rendered := strings.NewReplacer(
			"__MODEL_FAST__", fakeModelRef,
			"__MODEL_REASONING__", fakeModelRef,
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
			ID: uuid.New(), TenantID: devopsTenant, Namespace: document.Metadata.Namespace,
			Name: document.Metadata.Name, Version: document.Metadata.Version, Spec: full.Spec,
		}); err != nil {
			h.t.Fatalf("publish %s: %v", ref, err)
		}
	}
}

func (h *harness) createWorkflow(goal string) (uuid.UUID, error) {
	templatePath, err := filepath.Abs(filepath.Join("..", "..", "workflow", "devops-workflow.json"))
	if err != nil {
		return uuid.Nil, err
	}
	template, err := os.ReadFile(templatePath)
	if err != nil {
		return uuid.Nil, err
	}
	workflowID := uuid.New()
	envelope := func(role string) string {
		payload, _ := json.Marshal(map[string]any{
			"role": role, "goal": goal, "workflowId": workflowID.String(),
		})
		return devops.EnvelopePrefix() + string(payload)
	}
	var document map[string]any
	if err := json.Unmarshal(template, &document); err != nil {
		return uuid.Nil, fmt.Errorf("workflow template: %w", err)
	}
	document["deadline"] = time.Now().Add(45 * time.Minute).UTC().Format(time.RFC3339)
	goalPlaceholders := map[string]string{
		"__ENVELOPE_PLANNER__":   envelope("planner"),
		"__ENVELOPE_OBSERVER__":  envelope("observer"),
		"__ENVELOPE_DIAGNOSER__": envelope("diagnoser"),
		"__ENVELOPE_EXECUTOR__":  envelope("executor"),
		"__ENVELOPE_VERIFIER__":  envelope("verifier"),
		"__ENVELOPE_ROLLBACK__":  envelope("rollback"),
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
		ID: workflowID, TenantID: devopsTenant, Namespace: "default",
		IdempotencyKey: "devops/" + workflowID.String(), Goal: goal, Spec: rendered,
		Steps: spec.StepInputs(), BudgetMaxTasks: tasks, BudgetMaxTokens: tokens,
		BudgetMaxCostMicroUSD: cost, DeadlineAt: &deadline,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return result.Workflow.ID, nil
}

func (h *harness) awaitWorkflow(id uuid.UUID, timeout time.Duration) (kernelstore.Workflow, []kernelstore.WorkflowStep, error) {
	deadline := time.Now().Add(timeout)
	var last kernelstore.Workflow
	for time.Now().Before(deadline) {
		workflow, err := h.store.GetWorkflow(context.Background(), devopsTenant, id)
		if err != nil {
			return kernelstore.Workflow{}, nil, err
		}
		last = workflow
		switch workflow.Status {
		case kernelstore.WorkflowSucceeded, kernelstore.WorkflowFailed, kernelstore.WorkflowCancelled:
			steps, err := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
			return workflow, steps, err
		}
		time.Sleep(150 * time.Millisecond)
	}
	steps, listErr := h.store.ListWorkflowSteps(context.Background(), devopsTenant, id)
	if listErr != nil {
		return last, nil, fmt.Errorf("workflow %s did not settle within %s (list steps: %w)", id, timeout, listErr)
	}
	return last, steps, fmt.Errorf("workflow %s did not settle within %s", id, timeout)
}

func (h *harness) requireCompleted(id uuid.UUID, timeout time.Duration) kernelstore.Workflow {
	h.t.Helper()
	workflow, steps, err := h.awaitWorkflow(id, timeout)
	if err != nil {
		for _, step := range steps {
			h.t.Logf("step %-22s status=%-10s attempt=%d code=%q", step.Name, step.Status, step.AttemptCount, step.FailureCode)
		}
		h.t.Fatalf("await workflow: %v", err)
	}
	if workflow.Status != kernelstore.WorkflowSucceeded {
		for _, step := range steps {
			h.t.Logf("step %-22s status=%-10s attempt=%d code=%q", step.Name, step.Status, step.AttemptCount, step.FailureCode)
		}
		h.t.Fatalf("workflow status = %s, want SUCCEEDED", workflow.Status)
	}
	return workflow
}

func (h *harness) approveStep(workflowID uuid.UUID, stepName string, approve bool) {
	h.t.Helper()
	// Fetch the workflow to get the step's resource version.
	wf, err := h.store.GetWorkflow(context.Background(), devopsTenant, workflowID)
	if err != nil {
		h.t.Fatalf("get workflow: %v", err)
	}
	steps, err := h.store.ListWorkflowSteps(context.Background(), devopsTenant, workflowID)
	if err != nil {
		h.t.Fatalf("list steps: %v", err)
	}
	var step kernelstore.WorkflowStep
	for _, s := range steps {
		if s.Name == stepName {
			step = s
			break
		}
	}
	if step.Name == "" {
		h.t.Fatalf("step %q not found", stepName)
	}
	// Use the kernel store directly (the control API handler is also mounted
	// but the in-process store path is simpler).
	_, err = h.store.DecideWorkflowStepApproval(context.Background(), kernelstore.DecideWorkflowStepApprovalInput{
		TenantID: devopsTenant, WorkflowID: wf.ID, StepName: stepName,
		ExpectedVersion: step.ResourceVersion, Approved: approve, DecidedBy: "devops-test",
	})
	if err != nil {
		h.t.Fatalf("approve step %s: %v", stepName, err)
	}
}
