package workflow

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/errorcode"
	"github.com/google/uuid"
)

func mustSpec(t *testing.T, document string) []kernelstore.CreateWorkflowStepInput {
	t.Helper()
	steps, err := DecodeSpec([]byte(document))
	if err != nil {
		t.Fatalf("decode spec: %v", err)
	}
	return steps
}

func TestDecodeSpecValidatesTypedOutputContractAndJSONPointerCondition(t *testing.T) {
	raw := []byte(`{
	  "steps": [
	    {"name":"producer","agentVersionRef":"a@1","goal":"produce","spec":{"priority":1},
	     "output":{"contentType":"application/json","schemaVersion":"result/v1",
	       "schema":{"type":"object","required":["score"],"properties":{"score":{"type":"integer"}}}}},
	    {"name":"consumer","agentVersionRef":"b@1","goal":"consume","spec":{"priority":1},
	     "dependsOn":["producer"],"condition":{"step":"producer","jsonPointer":"/score","equalsJson":7}}
	  ]
	}`)
	if _, err := DecodeWorkflowSpec(raw); err != nil {
		t.Fatalf("typed workflow contract rejected: %v", err)
	}

	invalid := []byte(`{"steps":[{"name":"a","agentVersionRef":"a@1","goal":"g","spec":{"priority":1},"output":{"schemaVersion":"v1","schema":{"type":"definitely-not-a-json-schema-type"}}}]}`)
	if _, err := DecodeWorkflowSpec(invalid); err == nil {
		t.Fatal("invalid JSON Schema was accepted")
	}

	document := map[string]any{"nested": map[string]any{"value": float64(7)}}
	value, ok := resolveJSONPointer(document, "/nested/value")
	if !ok || value != float64(7) {
		t.Fatalf("pointer result = %#v, %v", value, ok)
	}
}

const validTwoStepSpec = `{
  "defaultTaskSpec": {"budget":{"tokens":500,"costUsd":1,"toolCalls":10,"wallSeconds":60},"placement":{"runtimeClasses":["adapter"]}},
  "steps": [
    {"name":"research","agentVersionRef":"research-agent@1","goal":"research the topic"},
    {"name":"review","agentVersionRef":"review-agent@1","goal":"review the findings","dependsOn":["research"]}
  ]
}`

func TestDecodeSpecAcceptsValidDAG(t *testing.T) {
	steps := mustSpec(t, validTwoStepSpec)
	if len(steps) != 2 || steps[0].Name != "research" || steps[1].DependsOn[0] != "research" {
		t.Fatalf("steps = %+v", steps)
	}
	merged := objectMap(steps[0].Spec)
	if merged["budget"] == nil || merged["placement"] == nil {
		t.Fatalf("default task spec not merged: %s", steps[0].Spec)
	}
	overlay := mustSpec(t, `{
	  "defaultTaskSpec": {"placement":{"runtimeClasses":["adapter"]}},
	  "steps": [{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{"priority":70,"placement":{"runtimeClasses":["oci"]}}}]
	}`)[0]
	overlayMap := objectMap(overlay.Spec)
	if string(overlayMap["placement"]) != `{"runtimeClasses":["oci"]}` {
		t.Fatalf("step overlay must win: %s", overlayMap["placement"])
	}
	if overlayMap["priority"] == nil {
		t.Fatalf("default keys retained under overlay: %s", overlay.Spec)
	}
}

func TestDecodeSpecRejectsInvalidGraphs(t *testing.T) {
	cases := map[string]string{
		"cycle":              `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","dependsOn":["b"]},{"name":"b","agentVersionRef":"x@1","goal":"g","dependsOn":["a"]}]}`,
		"self dependency":    `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","dependsOn":["a"]}]}`,
		"unknown dependency": `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","dependsOn":["ghost"]}]}`,
		"duplicate names":    `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g"},{"name":"a","agentVersionRef":"x@1","goal":"g"}]}`,
		"no spec anywhere":   `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g"}]}`,
		"bad name":           `{"steps":[{"name":"A!","agentVersionRef":"x@1","goal":"g","spec":{}}]}`,
		"no steps":           `{"steps":[]}`,
		"condition without dep": `{"steps":[
			{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{}},
			{"name":"b","agentVersionRef":"x@1","goal":"g","spec":{},"condition":{"step":"a","outputEquals":"x"}}]}`,
		"condition two predicates": `{"steps":[
			{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{}},
			{"name":"b","agentVersionRef":"x@1","goal":"g","spec":{},"dependsOn":["a"],"condition":{"step":"a","outputEquals":"x","outputContains":"y"}}]}`,
		"retry out of bounds":           `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{},"retry":{"maxAttempts":11}}]}`,
		"unknown field":                 `{"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{},"bogus":true}]}`,
		"dynamic limits without enable": `{"runtime":{"dynamic":{"maxDynamicSteps":2}},"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{"priority":1}}]}`,
		"dynamic without hard envelope": `{"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":2,"maxChildrenPerStep":2,"maxSpawnDepth":2,"maxWorkflowSteps":4}},"steps":[{"name":"a","agentVersionRef":"x@1","goal":"g","spec":{"priority":1}}]}`,
	}
	for name, document := range cases {
		if _, err := DecodeSpec([]byte(document)); err == nil {
			t.Errorf("%s: expected rejection", name)
		} else {
			t.Logf("%s rejected: %v", name, err)
		}
	}
}

func TestDecodeSpecPreservesV13BudgetRuntimeAndDeadline(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	document := fmt.Sprintf(`{
		"budget":{"maxTasks":25,"maxTokens":5000,"maxCostUsd":2.5},
		"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":10,"maxChildrenPerStep":4,"maxSpawnDepth":3,"maxWorkflowSteps":20}},
		"deadline":%q,
		"steps":[{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{"priority":1}}]
	}`, deadline.Format(time.RFC3339))
	spec, err := DecodeWorkflowSpec([]byte(document))
	if err != nil {
		t.Fatalf("decode v1.3 spec: %v", err)
	}
	tasks, tokens, cost := spec.Budgets()
	if tasks != 25 || tokens != 5000 || cost != money.MustFromUSD(2.5) || spec.Deadline == nil || !spec.Deadline.Equal(deadline) {
		t.Fatalf("v1.3 policy lost: %+v", spec)
	}
	if spec.Runtime == nil || spec.Runtime.Dynamic.MaxSpawnDepth != 3 {
		t.Fatalf("dynamic policy lost: %+v", spec.Runtime)
	}
}

func TestDecodeSpecAcceptsDynamicGroupJoinOnlyWhenEnabled(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	document := fmt.Sprintf(`{
		"budget":{"maxTasks":10},
		"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":5,"maxChildrenPerStep":5,"maxSpawnDepth":2,"maxWorkflowSteps":7}},
		"deadline":%q,
		"steps":[
			{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{"priority":1}},
			{"name":"join","agentVersionRef":"join@1","goal":"join","spec":{"priority":1},"dependsOn":["spawn:planner"]}
		]
	}`, deadline)
	if _, err := DecodeWorkflowSpec([]byte(document)); err != nil {
		t.Fatalf("dynamic group join rejected: %v", err)
	}
	if _, err := DecodeWorkflowSpec([]byte(`{
		"steps":[
			{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{}},
			{"name":"join","agentVersionRef":"join@1","goal":"join","spec":{},"dependsOn":["spawn:planner"]}
		]
	}`)); err == nil {
		t.Fatal("dynamic group join accepted without runtime.dynamic.enabled")
	}
}

// ---- engine fakes -----------------------------------------------------------

type fakeWorkflowStore struct {
	mu        sync.Mutex
	workflows map[uuid.UUID]*kernelstore.Workflow
	steps     map[uuid.UUID]map[string]*kernelstore.WorkflowStep
	artifacts map[string]kernelstore.ArtifactReference
	// retryDenial, when set, is returned by the RUNNING→PENDING retry
	// transition (the real store denies it when re-reserving the next
	// attempt would exceed the workflow budget).
	retryDenial error
}

func newFakeWorkflowStore() *fakeWorkflowStore {
	return &fakeWorkflowStore{
		workflows: map[uuid.UUID]*kernelstore.Workflow{},
		steps:     map[uuid.UUID]map[string]*kernelstore.WorkflowStep{},
		artifacts: map[string]kernelstore.ArtifactReference{},
	}
}

func (f *fakeWorkflowStore) CreateWorkflow(_ context.Context, in kernelstore.CreateWorkflowInput) (kernelstore.CreateWorkflowResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.workflows {
		if existing.TenantID == in.TenantID && existing.Namespace == in.Namespace && existing.IdempotencyKey == in.IdempotencyKey {
			return kernelstore.CreateWorkflowResult{Workflow: *existing, Existing: true}, nil
		}
	}
	workflow := kernelstore.Workflow{
		ID: in.ID, TenantID: in.TenantID, Namespace: in.Namespace, IdempotencyKey: in.IdempotencyKey,
		Goal: in.Goal, Spec: in.Spec, Status: kernelstore.WorkflowPending, ResourceVersion: 1,
		BudgetMaxTasks: in.BudgetMaxTasks, BudgetMaxTokens: in.BudgetMaxTokens,
		BudgetMaxCostMicroUSD: in.BudgetMaxCostMicroUSD, DeadlineAt: in.DeadlineAt,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	f.workflows[workflow.ID] = &workflow
	f.steps[workflow.ID] = map[string]*kernelstore.WorkflowStep{}
	for ordinal, step := range in.Steps {
		stored := kernelstore.WorkflowStep{
			ID: uuid.New(), TenantID: in.TenantID, WorkflowID: workflow.ID, Name: step.Name,
			Ordinal: ordinal, Status: kernelstore.StepPending, ResourceVersion: 1,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		f.steps[workflow.ID][step.Name] = &stored
	}
	return kernelstore.CreateWorkflowResult{Workflow: workflow}, nil
}

func (f *fakeWorkflowStore) GetWorkflow(_ context.Context, tenantID string, id uuid.UUID) (kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[id]
	if !ok || workflow.TenantID != tenantID {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	return *workflow, nil
}

func (f *fakeWorkflowStore) ListActiveWorkflows(context.Context, int) ([]kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	active := []kernelstore.Workflow{}
	for _, workflow := range f.workflows {
		if !workflow.Status.Terminal() {
			active = append(active, *workflow)
		}
	}
	return active, nil
}

func (f *fakeWorkflowStore) ListWorkflowSteps(_ context.Context, _ string, workflowID uuid.UUID) ([]kernelstore.WorkflowStep, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	steps := []kernelstore.WorkflowStep{}
	for _, step := range f.steps[workflowID] {
		steps = append(steps, *step)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i].Ordinal < steps[j].Ordinal })
	return steps, nil
}

func (f *fakeWorkflowStore) GetWorkflowStep(_ context.Context, tenantID string, workflowID uuid.UUID, name string) (kernelstore.WorkflowStep, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	step, ok := f.steps[workflowID][name]
	if !ok || step.TenantID != tenantID {
		return kernelstore.WorkflowStep{}, kernelstore.ErrStepNotFound
	}
	return *step, nil
}

func (f *fakeWorkflowStore) TransitionWorkflow(_ context.Context, in kernelstore.TransitionWorkflowInput) (kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[in.WorkflowID]
	if !ok || workflow.TenantID != in.TenantID {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	if workflow.ResourceVersion != in.ExpectedVersion {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow", kernelstore.ErrVersionConflict)
	}
	if !kernelstore.CanTransitionWorkflow(workflow.Status, in.To) {
		return kernelstore.Workflow{}, fmt.Errorf("%w: %s -> %s", kernelstore.ErrInvalidTransition, workflow.Status, in.To)
	}
	workflow.Status = in.To
	workflow.FailureCode = in.FailureCode
	workflow.ResourceVersion++
	workflow.UpdatedAt = time.Now()
	return *workflow, nil
}

func (f *fakeWorkflowStore) TransitionWorkflowStep(_ context.Context, in kernelstore.TransitionWorkflowStepInput) (kernelstore.WorkflowStep, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	step, ok := f.steps[in.WorkflowID][in.StepName]
	if !ok {
		return kernelstore.WorkflowStep{}, kernelstore.ErrStepNotFound
	}
	if step.ResourceVersion != in.ExpectedVersion {
		return kernelstore.WorkflowStep{}, fmt.Errorf("%w: step", kernelstore.ErrVersionConflict)
	}
	if !kernelstore.CanTransitionStep(step.Status, in.To) {
		return kernelstore.WorkflowStep{}, fmt.Errorf("%w: %s -> %s", kernelstore.ErrInvalidTransition, step.Status, in.To)
	}
	if f.retryDenial != nil && step.Status == kernelstore.StepRunning && in.To == kernelstore.StepPending {
		return kernelstore.WorkflowStep{}, f.retryDenial
	}
	step.Status = in.To
	if in.TaskID != nil {
		step.TaskID = in.TaskID
	}
	if in.AttemptCount != nil {
		step.AttemptCount = *in.AttemptCount
	}
	if in.ResultSummary != nil {
		step.ResultSummary = in.ResultSummary
	}
	step.FailureCode = in.FailureCode
	step.ResourceVersion++
	step.UpdatedAt = time.Now()
	return *step, nil
}

func (f *fakeWorkflowStore) DecideWorkflowStepApproval(_ context.Context, in kernelstore.DecideWorkflowStepApprovalInput) (kernelstore.WorkflowStep, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	step, ok := f.steps[in.WorkflowID][in.StepName]
	if !ok {
		return kernelstore.WorkflowStep{}, kernelstore.ErrStepNotFound
	}
	if step.ResourceVersion != in.ExpectedVersion || step.Status != kernelstore.StepWaitingApproval || step.DecidedBy != "" {
		return kernelstore.WorkflowStep{}, fmt.Errorf("%w: approval state", kernelstore.ErrInvalidTransition)
	}
	if in.Approved {
		step.ApprovalDecision = "approved"
	} else {
		step.ApprovalDecision = "rejected"
	}
	step.DecidedBy = in.DecidedBy
	step.DecidedAt = &[]time.Time{time.Now()}[0]
	step.ResourceVersion++
	return *step, nil
}

func (f *fakeWorkflowStore) RequestWorkflowCancellation(_ context.Context, tenantID string, workflowID uuid.UUID, expectedVersion int64) (kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[workflowID]
	if !ok || workflow.TenantID != tenantID {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	if workflow.ResourceVersion != expectedVersion || workflow.Status.Terminal() {
		return kernelstore.Workflow{}, fmt.Errorf("%w: cancellation state", kernelstore.ErrInvalidTransition)
	}
	if workflow.CancelRequestedAt == nil {
		now := time.Now()
		workflow.CancelRequestedAt = &now
		workflow.ResourceVersion++
	}
	return *workflow, nil
}

func (f *fakeWorkflowStore) ArtifactMetadata(_ context.Context, _, _ string) ([]byte, int64, string, error) {
	return make([]byte, 32), 0, "application/json", nil
}

func (f *fakeWorkflowStore) ClaimWorkflows(_ context.Context, in kernelstore.ClaimWorkflowsInput) ([]kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	claimed := []kernelstore.Workflow{}
	for _, workflow := range f.workflows {
		if !workflow.Status.Terminal() && workflow.BudgetExhaustedAt == nil {
			claimed = append(claimed, *workflow)
		}
		if len(claimed) >= in.Batch {
			break
		}
	}
	return claimed, nil
}

func (f *fakeWorkflowStore) SpawnWorkflowStep(_ context.Context, in kernelstore.SpawnWorkflowStepInput) (kernelstore.SpawnWorkflowStepResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[in.WorkflowID]
	if !ok || workflow.TenantID != in.TenantID {
		return kernelstore.SpawnWorkflowStepResult{}, kernelstore.ErrWorkflowNotFound
	}
	spawnKey := in.IdempotencyKey + "-" + kernelstore.SpawnKeyHash(in.Arguments)
	for _, step := range f.steps[in.WorkflowID] {
		if step.SpawnKey == spawnKey {
			return kernelstore.SpawnWorkflowStepResult{Step: *step, Workflow: *workflow}, nil
		}
	}
	parentDepth := 0
	if in.ParentStepName != "" {
		if parent, ok := f.steps[in.WorkflowID][in.ParentStepName]; ok {
			parentDepth = parent.SpawnDepth
		}
	}
	ordinal := 0
	for _, step := range f.steps[in.WorkflowID] {
		if step.Ordinal >= ordinal {
			ordinal = step.Ordinal + 1
		}
	}
	created := kernelstore.WorkflowStep{
		ID: uuid.New(), TenantID: in.TenantID, WorkflowID: in.WorkflowID, Name: in.Name,
		Ordinal: ordinal, Status: kernelstore.StepPending, ParentStepName: in.ParentStepName,
		SpawnDepth: parentDepth + 1, IsDynamic: true, SpawnKey: spawnKey, Goal: in.Goal,
		AgentVersionRef: in.AgentVersionRef, Spec: in.Spec, MaxAttempts: in.MaxAttempts,
		ResourceVersion: 1, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	f.steps[in.WorkflowID][in.Name] = &created
	return kernelstore.SpawnWorkflowStepResult{Step: created, Workflow: *workflow, Created: true}, nil
}

func (f *fakeWorkflowStore) MarkWorkflowBudgetExhausted(_ context.Context, tenantID string, workflowID uuid.UUID, expectedVersion int64) (kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[workflowID]
	if !ok || workflow.TenantID != tenantID {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	if workflow.ResourceVersion != expectedVersion {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow", kernelstore.ErrVersionConflict)
	}
	if workflow.BudgetExhaustedAt == nil {
		now := time.Now()
		workflow.BudgetExhaustedAt = &now
		workflow.ResourceVersion++
	}
	return *workflow, nil
}

func (f *fakeWorkflowStore) MarkWorkflowDeadlineExceeded(_ context.Context, tenantID string, workflowID uuid.UUID, expectedVersion int64) (kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[workflowID]
	if !ok || workflow.TenantID != tenantID {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	if workflow.ResourceVersion != expectedVersion {
		return kernelstore.Workflow{}, fmt.Errorf("%w: workflow", kernelstore.ErrVersionConflict)
	}
	if workflow.DeadlineExceededAt == nil {
		now := time.Now()
		workflow.DeadlineExceededAt = &now
		workflow.ResourceVersion++
	}
	return *workflow, nil
}

func (f *fakeWorkflowStore) WorkflowUsageSnapshot(_ context.Context, _ string, workflowID uuid.UUID) (kernelstore.WorkflowUsage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workflow, ok := f.workflows[workflowID]
	if !ok {
		return kernelstore.WorkflowUsage{}, kernelstore.ErrWorkflowNotFound
	}
	return kernelstore.WorkflowUsage{
		BudgetMaxTasks: workflow.BudgetMaxTasks, BudgetMaxTokens: workflow.BudgetMaxTokens,
		BudgetMaxCostMicroUSD: workflow.BudgetMaxCostMicroUSD,
	}, nil
}

type fakeTaskPipeline struct {
	mu    sync.Mutex
	tasks map[uuid.UUID]*kernelstore.Task
	byKey map[string]uuid.UUID
	goals map[uuid.UUID]string
	// failFirstAttempt marks idempotency keys whose first task fails.
	failFirstAttempt map[string]bool
	attemptsByKey    map[string]int
}

func newFakeTaskPipeline() *fakeTaskPipeline {
	return &fakeTaskPipeline{
		tasks: map[uuid.UUID]*kernelstore.Task{}, byKey: map[string]uuid.UUID{},
		goals: map[uuid.UUID]string{}, failFirstAttempt: map[string]bool{}, attemptsByKey: map[string]int{},
	}
}

func (f *fakeTaskPipeline) CreateTask(_ context.Context, in kernelstore.CreateTaskInput) (kernelstore.CreateTaskResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, replay := f.byKey[in.IdempotencyKey]; replay {
		return kernelstore.CreateTaskResult{Task: *f.tasks[id], Existing: true}, nil
	}
	phase := domain.TaskQueued
	f.attemptsByKey[in.IdempotencyKey]++
	if f.failFirstAttempt[in.IdempotencyKey] && f.attemptsByKey[in.IdempotencyKey] == 1 {
		phase = domain.TaskQueued // the harness flips phases explicitly
	}
	task := kernelstore.Task{
		ID: in.ID, TenantID: in.TenantID, AgentVersionRef: in.AgentVersionRef, Goal: in.Goal,
		Phase: phase, ResourceVersion: 1,
	}
	if phase == domain.TaskQueued {
		// Default script: succeed once observed.
		task.Phase = domain.TaskSucceeded
		task.ResultRef = "file://results/" + in.ID.String()
	}
	f.tasks[task.ID] = &task
	f.byKey[in.IdempotencyKey] = task.ID
	f.goals[task.ID] = in.Goal
	return kernelstore.CreateTaskResult{Task: task}, nil
}

func (f *fakeTaskPipeline) GetTask(_ context.Context, _ string, id uuid.UUID) (kernelstore.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[id]
	if !ok {
		return kernelstore.Task{}, kernelstore.ErrNotFound
	}
	return *task, nil
}

func (f *fakeTaskPipeline) RequestTaskCancellation(_ context.Context, _ string, id uuid.UUID, expectedVersion int64) (kernelstore.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[id]
	if !ok || task.ResourceVersion != expectedVersion {
		return kernelstore.Task{}, fmt.Errorf("%w: task", kernelstore.ErrVersionConflict)
	}
	if !task.Phase.Terminal() {
		task.Phase = domain.TaskCancelled
		task.ResourceVersion++
	}
	return *task, nil
}

func (f *fakeTaskPipeline) fail(id uuid.UUID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[id].Phase = domain.TaskFailed
	f.tasks[id].ResourceVersion++
}

func (f *fakeTaskPipeline) pause(id uuid.UUID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[id].Phase = domain.TaskRunning
}

func (f *fakeTaskPipeline) setPhase(id uuid.UUID, phase domain.TaskPhase) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[id].Phase = phase
	f.tasks[id].ResourceVersion++
}

type fakeResultReader struct{}

func (fakeResultReader) Open(_ context.Context, _ string, _ kernelstore.ArtifactReference) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(`{"output":{"answer":"research says 42"}}`)), nil
}

func newEngine(workflows *fakeWorkflowStore, tasks *fakeTaskPipeline) *Controller {
	return NewController(workflows, tasks, fakeResultReader{}, "test-orchestrator", 16)
}

func createWorkflowForTest(t *testing.T, store *fakeWorkflowStore, spec string) kernelstore.Workflow {
	t.Helper()
	decoded, err := DecodeWorkflowSpec([]byte(spec))
	if err != nil {
		t.Fatalf("decode workflow: %v", err)
	}
	tasksBudget, tokensBudget, costBudget := decoded.Budgets()
	created, err := store.CreateWorkflow(context.Background(), kernelstore.CreateWorkflowInput{
		ID: uuid.New(), TenantID: "tenant-1", Namespace: "default", IdempotencyKey: "wf-" + uuid.NewString(),
		Goal: "workflow goal", Spec: []byte(spec), Steps: decoded.StepInputs(),
		BudgetMaxTasks: tasksBudget, BudgetMaxTokens: tokensBudget, BudgetMaxCostMicroUSD: costBudget,
		DeadlineAt: decoded.Deadline,
	})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return created.Workflow
}

func reconcileUntil(t *testing.T, engine *Controller, predicate func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := engine.Reconcile(context.Background()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not reached within %s", timeout)
}

func TestEngineRunsSequentialDependencyChain(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, validTwoStepSpec)

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)

	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow status = %s", final.Status)
	}
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if len(steps) != 2 || steps[0].Status != kernelstore.StepSucceeded || steps[1].Status != kernelstore.StepSucceeded {
		t.Fatalf("steps = %+v", steps)
	}
	// A's result reached B's goal through Fenced.
	if !strings.Contains(tasks.goals[*steps[1].TaskID], "research says 42") {
		t.Fatalf("downstream goal missing upstream output: %q", tasks.goals[*steps[1].TaskID])
	}
	if !strings.Contains(tasks.goals[*steps[1].TaskID], "Upstream result [research]") {
		t.Fatalf("downstream goal not annotated: %q", tasks.goals[*steps[1].TaskID])
	}
	// Exactly one task per step.
	if len(tasks.byKey) != 2 {
		t.Fatalf("tasks created = %d, want 2", len(tasks.byKey))
	}
}

func TestEngineRunsDynamicChildThenGroupJoin(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	spec := fmt.Sprintf(`{
		"budget":{"maxTasks":10},
		"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":5,"maxChildrenPerStep":5,"maxSpawnDepth":2,"maxWorkflowSteps":7}},
		"deadline":%q,
		"steps":[
			{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{"priority":1}},
			{"name":"join","agentVersionRef":"join@1","goal":"combine children","spec":{"priority":1},"dependsOn":["spawn:planner"]}
		]
	}`, deadline)
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)
	spawned, err := store.SpawnWorkflowStep(context.Background(), kernelstore.SpawnWorkflowStepInput{
		WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
		ParentStepName: "planner", Name: "child-1", Goal: "execute child", AgentVersionRef: "worker@1",
		Spec: []byte(`{}`), MaxAttempts: 1, IdempotencyKey: "child-1", Arguments: []byte(`{}`),
	})
	if err != nil || !spawned.Created {
		t.Fatalf("spawn dynamic child: %+v err=%v", spawned, err)
	}

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), workflow.TenantID, workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	steps, _ := store.ListWorkflowSteps(context.Background(), workflow.TenantID, workflow.ID)
	byName := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		byName[step.Name] = step
	}
	for _, name := range []string{"planner", "child-1", "join"} {
		if byName[name].Status != kernelstore.StepSucceeded {
			t.Fatalf("step %s = %s", name, byName[name].Status)
		}
	}
	childGoal := tasks.goals[*byName["child-1"].TaskID]
	if !strings.Contains(childGoal, "Upstream result [planner]") {
		t.Fatalf("dynamic child did not receive parent output: %q", childGoal)
	}
	joinGoal := tasks.goals[*byName["join"].TaskID]
	if !strings.Contains(joinGoal, "Upstream result [child-1]") {
		t.Fatalf("group join did not receive dynamic child output: %q", joinGoal)
	}
}

func TestEngineDoesNotCloseEmptySpawnGroupBeforeParentCompletes(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	spec := fmt.Sprintf(`{
		"budget":{"maxTasks":10},
		"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":5,"maxChildrenPerStep":5,"maxSpawnDepth":2,"maxWorkflowSteps":7}},
		"deadline":%q,
		"steps":[
			{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{"priority":1}},
			{"name":"join","agentVersionRef":"join@1","goal":"join empty group","spec":{"priority":1},"dependsOn":["spawn:planner"]}
		]
	}`, deadline)
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)
	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	steps, _ := store.ListWorkflowSteps(context.Background(), workflow.TenantID, workflow.ID)
	byName := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		byName[step.Name] = step
	}
	if byName["planner"].Status != kernelstore.StepRunning || byName["join"].Status != kernelstore.StepPending {
		t.Fatalf("empty group closed before parent completed: planner=%s join=%s", byName["planner"].Status, byName["join"].Status)
	}
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), workflow.TenantID, workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	final, _ := store.GetWorkflow(context.Background(), workflow.TenantID, workflow.ID)
	if final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("empty dynamic group workflow = %s", final.Status)
	}
}

func TestV13Orchestrates10KDynamicTasks(t *testing.T) {
	if os.Getenv("FENCED_V13_SCALE_TEST") != "1" {
		t.Skip("set FENCED_V13_SCALE_TEST=1 to run the 10k dynamic-task acceptance leg")
	}
	deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	spec := fmt.Sprintf(`{
		"budget":{"maxTasks":10000},
		"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":9999,"maxChildrenPerStep":9999,"maxSpawnDepth":1,"maxWorkflowSteps":10000}},
		"deadline":%q,
		"steps":[{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{"priority":1}}]
	}`, deadline)
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks).WithMaxInFlightSteps(64)
	workflow := createWorkflowForTest(t, store, spec)
	for index := 0; index < 9_999; index++ {
		name := fmt.Sprintf("child-%05d", index)
		result, err := store.SpawnWorkflowStep(context.Background(), kernelstore.SpawnWorkflowStepInput{
			WorkflowID: workflow.ID, TenantID: workflow.TenantID, WorkflowVersion: workflow.ResourceVersion,
			ParentStepName: "planner", Name: name, Goal: "execute child", AgentVersionRef: "worker@1",
			Spec: []byte(`{"priority":1}`), MaxAttempts: 1,
			IdempotencyKey: name, Arguments: []byte(fmt.Sprintf(`{"index":%d}`, index)),
		})
		if err != nil || !result.Created {
			t.Fatalf("seed dynamic step %d: created=%v err=%v", index, result.Created, err)
		}
	}
	started := time.Now()
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), workflow.TenantID, workflow.ID)
		return current.Status.Terminal()
	}, v13ScaleLegWait)
	elapsed := time.Since(started)
	final, _ := store.GetWorkflow(context.Background(), workflow.TenantID, workflow.ID)
	if final.Status != kernelstore.WorkflowSucceeded || len(tasks.byKey) != 10_000 {
		t.Fatalf("10k task workflow status=%s tasks=%d", final.Status, len(tasks.byKey))
	}
	t.Logf("10k dynamic tasks orchestrated in %s (%.1f tasks/s)", elapsed, 10_000/elapsed.Seconds())
}

func TestEngineSkipsDependentsOfFailedStep(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, validTwoStepSpec)

	// research's task fails; review must never execute.
	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[0].Status == kernelstore.StepRunning
	}, 5*time.Second)
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	tasks.fail(*steps[0].TaskID)

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	steps, _ = store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if steps[0].Status != kernelstore.StepFailed || steps[1].Status != kernelstore.StepSkipped {
		t.Fatalf("steps = %s/%s", steps[0].Status, steps[1].Status)
	}
	if steps[1].FailureCode != "UPSTREAM_NOT_SUCCEEDED" {
		t.Fatalf("skip code = %s", steps[1].FailureCode)
	}
	if steps[1].TaskID != nil {
		t.Fatal("skipped step must not have a task")
	}
	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowFailed {
		t.Fatalf("workflow = %s", final.Status)
	}
}

func TestEngineRetriesSingleStepWithoutTouchingSiblings(t *testing.T) {
	spec := `{
	  "defaultTaskSpec": {"budget":{"tokens":100}},
	  "steps": [
	    {"name":"a","agentVersionRef":"x@1","goal":"stable"},
	    {"name":"flaky","agentVersionRef":"x@1","goal":"flaky step","dependsOn":["a"],"retry":{"maxAttempts":2}}
	  ]
	}`
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)

	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[1].Status == kernelstore.StepRunning
	}, 5*time.Second)
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	siblingTask := *steps[0].TaskID
	flakyTask := *steps[1].TaskID
	tasks.fail(flakyTask)

	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[1].Status == kernelstore.StepRunning && steps[1].AttemptCount == 2
	}, 5*time.Second)
	steps, _ = store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if steps[0].Status != kernelstore.StepSucceeded || *steps[0].TaskID != siblingTask {
		t.Fatalf("completed sibling disturbed: %+v", steps[0])
	}
	if *steps[1].TaskID == flakyTask {
		t.Fatal("retry must create a fresh task")
	}
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow = %s", final.Status)
	}
}

func TestEngineParallelJoinAndCondition(t *testing.T) {
	spec := `{
	  "defaultTaskSpec": {"budget":{"tokens":100}},
	  "steps": [
	    {"name":"left","agentVersionRef":"x@1","goal":"left"},
	    {"name":"right","agentVersionRef":"x@1","goal":"right"},
	    {"name":"join","agentVersionRef":"x@1","goal":"join both","dependsOn":["left","right"]},
	    {"name":"conditional","agentVersionRef":"x@1","goal":"only when left says 42","dependsOn":["left"],"condition":{"step":"left","outputContains":"42"}},
	    {"name":"never","agentVersionRef":"x@1","goal":"only when left says 99","dependsOn":["left"],"condition":{"step":"left","outputEquals":"research says 99"}}
	  ]
	}`
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)

	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	byName := map[string]kernelstore.WorkflowStep{}
	for _, step := range steps {
		byName[step.Name] = step
	}
	for _, name := range []string{"left", "right", "join", "conditional"} {
		if byName[name].Status != kernelstore.StepSucceeded {
			t.Fatalf("%s = %s", name, byName[name].Status)
		}
	}
	if byName["never"].Status != kernelstore.StepSkipped || byName["never"].FailureCode != "CONDITION_NOT_MET" {
		t.Fatalf("never = %s (%s)", byName["never"].Status, byName["never"].FailureCode)
	}
	// The join step received both upstream outputs.
	joinGoal := tasks.goals[*byName["join"].TaskID]
	if !strings.Contains(joinGoal, "Upstream result [left]") || !strings.Contains(joinGoal, "Upstream result [right]") {
		t.Fatalf("join goal = %q", joinGoal)
	}
	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow = %s (skips count as success)", final.Status)
	}
}

func TestEngineHumanApprovalParkAndDecisions(t *testing.T) {
	spec := `{
	  "defaultTaskSpec": {"budget":{"tokens":100}},
	  "steps": [
	    {"name":"risky","agentVersionRef":"x@1","goal":"needs a human","requiresApproval":true}
	  ]
	}`
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)

	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[0].Status == kernelstore.StepWaitingApproval
	}, 5*time.Second)
	if steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID); steps[0].TaskID != nil {
		t.Fatal("parked step must not have a task")
	}

	// Approve: the step dispatches.
	approved, err := store.DecideWorkflowStepApproval(context.Background(), kernelstore.DecideWorkflowStepApprovalInput{
		TenantID: "tenant-1", WorkflowID: workflow.ID, StepName: "risky",
		ExpectedVersion: func() int64 {
			s, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
			return s[0].ResourceVersion
		}(),
		Approved: true, DecidedBy: "human-1",
	})
	if err != nil || approved.DecidedBy != "human-1" || approved.ApprovalDecision != "approved" {
		t.Fatalf("approve: %+v err=%v", approved, err)
	}
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	if final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID); final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow = %s", final.Status)
	}
}

func TestEngineApprovalRejectionSkips(t *testing.T) {
	spec := `{
	  "defaultTaskSpec": {"budget":{"tokens":100}},
	  "steps": [{"name":"risky","agentVersionRef":"x@1","goal":"needs a human","requiresApproval":true}]
	}`
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)

	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[0].Status == kernelstore.StepWaitingApproval
	}, 5*time.Second)
	version := func() int64 {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[0].ResourceVersion
	}
	if _, err := store.DecideWorkflowStepApproval(context.Background(), kernelstore.DecideWorkflowStepApprovalInput{
		TenantID: "tenant-1", WorkflowID: workflow.ID, StepName: "risky",
		ExpectedVersion: version(), Approved: false, DecidedBy: "human-1",
	}); err != nil {
		t.Fatalf("reject: %v", err)
	}
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if steps[0].Status != kernelstore.StepSkipped || steps[0].FailureCode != "APPROVAL_REJECTED" {
		t.Fatalf("step = %s (%s)", steps[0].Status, steps[0].FailureCode)
	}
}

func TestEngineCancelPropagatesToActiveSteps(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, validTwoStepSpec)

	// research dispatches, and its task is paused mid-flight (not
	// terminal) when the cancellation request lands.
	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		if steps[0].Status == kernelstore.StepRunning && steps[0].TaskID != nil {
			tasks.pause(*steps[0].TaskID)
			return true
		}
		return false
	}, 5*time.Second)
	current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	cancelled, err := store.RequestWorkflowCancellation(context.Background(), "tenant-1", workflow.ID, current.ResourceVersion)
	if err != nil || cancelled.CancelRequestedAt == nil {
		t.Fatalf("request cancellation: %+v err=%v", cancelled, err)
	}

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowCancelled {
		t.Fatalf("workflow = %s", final.Status)
	}
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if steps[0].Status != kernelstore.StepCancelled || steps[1].Status != kernelstore.StepSkipped {
		taskPhase := "none"
		if steps[0].TaskID != nil {
			task, _ := tasks.GetTask(context.Background(), "tenant-1", *steps[0].TaskID)
			taskPhase = string(task.Phase)
		}
		t.Fatalf("steps = %s/%s (step0 task phase=%s failure=%s)", steps[0].Status, steps[1].Status, taskPhase, steps[0].FailureCode)
	}
}

func TestEngineReportsRejectedTaskAsFailureNotCancellation(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, `{
	  "defaultTaskSpec":{"budget":{"tokens":10}},
	  "steps":[{"name":"root","agentVersionRef":"agent@1","goal":"run"}]
	}`)

	if _, err := engine.Reconcile(context.Background()); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if steps[0].TaskID == nil {
		t.Fatal("root task was not dispatched")
	}
	tasks.setPhase(*steps[0].TaskID, domain.TaskRejected)
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)

	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	steps, _ = store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowFailed || steps[0].Status != kernelstore.StepFailed || steps[0].FailureCode != "TASK_REJECTED" {
		t.Fatalf("workflow=%s step=%s code=%s", final.Status, steps[0].Status, steps[0].FailureCode)
	}
}

func TestEngineConcurrentInstancesNeverDoubleDispatch(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	engineA := newEngine(store, tasks)
	engineB := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, validTwoStepSpec)

	var wg sync.WaitGroup
	wg.Add(2)
	for _, engine := range []*Controller{engineA, engineB} {
		go func(engine *Controller) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if _, err := engine.Reconcile(context.Background()); err != nil {
					t.Errorf("reconcile: %v", err)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(engine)
	}
	wg.Wait()
	reconcileUntil(t, engineA, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)

	if created := len(tasks.byKey); created != 2 {
		t.Fatalf("tasks created = %d, want exactly one per step", created)
	}
}

func TestEngineRecoversAfterRestart(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	workflow := createWorkflowForTest(t, store, validTwoStepSpec)

	// No orchestrator runs for a while (simulated restart window); state is
	// durable. A fresh instance resumes and completes.
	time.Sleep(10 * time.Millisecond)
	engine := newEngine(store, tasks)
	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	if final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID); final.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow = %s", final.Status)
	}
}

func TestEngineHardStopsExpiredWorkflow(t *testing.T) {
	store := newFakeWorkflowStore()
	tasks := newFakeTaskPipeline()
	deadline := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	spec := fmt.Sprintf(`{
		"deadline":%q,
		"steps":[{"name":"planner","agentVersionRef":"planner@1","goal":"plan","spec":{"priority":1}}]
	}`, deadline.Format(time.RFC3339))
	created := createWorkflowForTest(t, store, spec)
	engine := newEngine(store, tasks)
	engine.now = func() time.Time { return deadline.Add(time.Minute) }

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", created.ID)
		return current.Status.Terminal()
	}, 2*time.Second)
	final, _ := store.GetWorkflow(context.Background(), "tenant-1", created.ID)
	if final.Status != kernelstore.WorkflowFailed || final.FailureCode != "WORKFLOW_DEADLINE_EXCEEDED" || final.DeadlineExceededAt == nil {
		t.Fatalf("expired workflow = %+v", final)
	}
	if len(tasks.tasks) != 0 {
		t.Fatalf("expired workflow dispatched %d tasks", len(tasks.tasks))
	}
}

// TestEngineBudgetDeniedRetryFailsTheStep proves a retry whose reservation
// would exceed the workflow budget fails the step with the durable budget
// code instead of retry-looping against a hard ceiling.
func TestEngineBudgetDeniedRetryFailsTheStep(t *testing.T) {
	spec := `{
	  "budget":{"maxTasks":10},
	  "defaultTaskSpec": {"budget":{"tokens":100}},
	  "steps": [
	    {"name":"flaky","agentVersionRef":"x@1","goal":"flaky step","retry":{"maxAttempts":3}}
	  ]
	}`
	store := newFakeWorkflowStore()
	store.retryDenial = kernelstore.SpawnDenial{Code: errorcode.SpawnBudgetExhausted, Message: "retry denied: workflow budget exhausted"}
	tasks := newFakeTaskPipeline()
	engine := newEngine(store, tasks)
	workflow := createWorkflowForTest(t, store, spec)

	reconcileUntil(t, engine, func() bool {
		steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
		return steps[0].Status == kernelstore.StepRunning
	}, 5*time.Second)
	steps, _ := store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	flakyTask := *steps[0].TaskID
	tasks.fail(flakyTask)

	reconcileUntil(t, engine, func() bool {
		current, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
		return current.Status.Terminal()
	}, 5*time.Second)
	final, _ := store.GetWorkflow(context.Background(), "tenant-1", workflow.ID)
	if final.Status != kernelstore.WorkflowFailed || final.FailureCode != errorcode.WorkflowBudgetExhausted {
		t.Fatalf("budget-denied retry outcome = %s/%s, want FAILED/WORKFLOW_BUDGET_EXHAUSTED", final.Status, final.FailureCode)
	}
	steps, _ = store.ListWorkflowSteps(context.Background(), "tenant-1", workflow.ID)
	if steps[0].Status != kernelstore.StepFailed || steps[0].FailureCode != errorcode.WorkflowBudgetExhausted {
		t.Fatalf("budget-denied step = %s/%s, want FAILED/WORKFLOW_BUDGET_EXHAUSTED", steps[0].Status, steps[0].FailureCode)
	}
	if steps[0].AttemptCount != 1 {
		t.Fatalf("budget-denied retry must not dispatch another task, attempts = %d", steps[0].AttemptCount)
	}
}
