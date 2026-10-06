// Package workflow implements the durable orchestrator: WorkflowRun and Step
// state, dependency dispatch with conditions and joins, single-step retry,
// human approval, cancellation propagation and restart recovery. The
// orchestrator decides who executes when and creates ordinary Tasks; it
// never schedules (that stays with the scheduler) and agents never talk to
// each other directly — Agent A's result reaches Agent B through Fenced.
package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	maxSteps           = 1024
	maxGoalBytes       = 8192
	maxSpecBytes       = 1 << 20
	maxConditionBytes  = 1024
	maxDefaultAttempts = 1
	maxRetryAttempts   = 10
	// Budgets bound one workflow run (tasks, tokens, USD); each is
	// optional and independently enforced. maxWorkflowSteps also bounds the
	// runtime dynamic-spawn guards.
	maxWorkflowTasks        = 100000
	maxWorkflowTokens       = 1 << 40
	maxWorkflowCostMicroUSD = money.MicroUSD(100000 * money.MicroPerUSD)
	maxWorkflowSteps        = 100000
)

// StepCondition is the structured condition of one step: evaluated against
// the stored result summary of one dependency step. Exactly one predicate
// must be set; a false predicate skips the step (CONDITION_NOT_MET).
type StepCondition struct {
	Step           string `json:"step"`
	OutputContains string `json:"outputContains,omitempty"`
	OutputEquals   string `json:"outputEquals,omitempty"`
	// JSONPointer selects a typed value from the upstream output using RFC
	// 6901. EqualsJSON compares JSON values structurally (not textually).
	JSONPointer string          `json:"jsonPointer,omitempty"`
	EqualsJSON  json.RawMessage `json:"equalsJson,omitempty"`
}

// StepOutputContract defines the durable Agent A -> Agent B data contract.
// Schema is JSON Schema 2020-12 and validates the result document's output
// value before the step may become SUCCEEDED.
type StepOutputContract struct {
	ContentType   string          `json:"contentType,omitempty"`
	SchemaVersion string          `json:"schemaVersion"`
	Schema        json.RawMessage `json:"schema"`
}

// StepRetry bounds the single-step retry budget (total task attempts).
type StepRetry struct {
	MaxAttempts int `json:"maxAttempts"`
}

// StepSpec is one declared DAG node of the workflow document.
type StepSpec struct {
	Name             string              `json:"name"`
	AgentVersionRef  string              `json:"agentVersionRef"`
	Goal             string              `json:"goal"`
	Spec             json.RawMessage     `json:"spec,omitempty"`
	DependsOn        []string            `json:"dependsOn,omitempty"`
	Condition        *StepCondition      `json:"condition,omitempty"`
	Retry            *StepRetry          `json:"retry,omitempty"`
	RequiresApproval bool                `json:"requiresApproval"`
	Output           *StepOutputContract `json:"output,omitempty"`
}

// WorkflowBudget is the workflow-level ceiling set of the workflow document
// Every dimension is optional; zero leaves it unbounded.
type WorkflowBudget struct {
	MaxTasks        int64          `json:"maxTasks,omitempty"`
	MaxTokens       int64          `json:"maxTokens,omitempty"`
	MaxCostMicroUSD money.MicroUSD `json:"maxCostUsd,omitempty"`
}

// WorkflowRuntimePolicy is the dynamic-orchestration policy of the workflow
// document: the guards the spawning path enforces at runtime.
type WorkflowRuntimePolicy struct {
	Dynamic WorkflowDynamicPolicy `json:"dynamic,omitempty"`
}

// WorkflowDynamicPolicy bounds dynamic spawning.
type WorkflowDynamicPolicy struct {
	Enabled              bool           `json:"enabled,omitempty"`
	MaxDynamicSteps      int64          `json:"maxDynamicSteps,omitempty"`
	MaxChildrenPerStep   int64          `json:"maxChildrenPerStep,omitempty"`
	MaxSpawnDepth        int            `json:"maxSpawnDepth,omitempty"`
	MaxWorkflowSteps     int64          `json:"maxWorkflowSteps,omitempty"`
	MaxSpawnTasks        int64          `json:"maxSpawnTasks,omitempty"`
	MaxSpawnTokens       int64          `json:"maxSpawnTokens,omitempty"`
	MaxSpawnCostMicroUSD money.MicroUSD `json:"maxSpawnCostUsd,omitempty"`
}

// WorkflowSpec is the user-facing workflow document.
type WorkflowSpec struct {
	DefaultTaskSpec json.RawMessage        `json:"defaultTaskSpec,omitempty"`
	Budget          *WorkflowBudget        `json:"budget,omitempty"`
	Runtime         *WorkflowRuntimePolicy `json:"runtime,omitempty"`
	Deadline        *time.Time             `json:"deadline,omitempty"`
	Steps           []StepSpec             `json:"steps"`
}

// DecodeSpec strictly decodes and fully validates one workflow document,
// returning the step inputs (with default/step spec overlays merged) for
// durable storage.
func DecodeSpec(raw []byte) ([]kernelstore.CreateWorkflowStepInput, error) {
	spec, err := DecodeWorkflowSpec(raw)
	if err != nil {
		return nil, err
	}
	return spec.StepInputs(), nil
}

// DecodeWorkflowSpec strictly decodes and fully validates one workflow
// document, preserving the budget and runtime policy alongside the step
// inputs.
func DecodeWorkflowSpec(raw []byte) (WorkflowSpec, error) {
	var spec WorkflowSpec
	if len(raw) == 0 || len(raw) > maxSpecBytes {
		return spec, fmt.Errorf("workflow spec must be 1..%d bytes (a 1024-step DAG needs ~150KB)", maxSpecBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return spec, fmt.Errorf("workflow spec: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return spec, fmt.Errorf("workflow spec contains more than one JSON value")
	}
	if len(spec.Steps) == 0 || len(spec.Steps) > maxSteps {
		return spec, fmt.Errorf("workflow must declare 1..%d steps", maxSteps)
	}
	if len(spec.DefaultTaskSpec) > 0 {
		if !json.Valid(spec.DefaultTaskSpec) {
			return spec, fmt.Errorf("defaultTaskSpec must be a JSON object")
		}
	}
	if err := validateBudget(spec.Budget); err != nil {
		return spec, err
	}
	if err := validateRuntimePolicy(spec.Runtime); err != nil {
		return spec, err
	}
	if spec.Deadline != nil && spec.Deadline.IsZero() {
		return spec, fmt.Errorf("deadline must be a valid RFC3339 timestamp")
	}
	if spec.Runtime != nil && spec.Runtime.Dynamic.Enabled {
		if spec.Budget == nil || spec.Budget.MaxTasks <= 0 {
			return spec, fmt.Errorf("budget.maxTasks is required when dynamic spawning is enabled")
		}
		if spec.Deadline == nil {
			return spec, fmt.Errorf("deadline is required when dynamic spawning is enabled")
		}
		if spec.Runtime.Dynamic.MaxWorkflowSteps < int64(len(spec.Steps)) {
			return spec, fmt.Errorf("runtime.dynamic.maxWorkflowSteps cannot be below the declared step count")
		}
	}

	names := make(map[string]int, len(spec.Steps))
	for index, step := range spec.Steps {
		if err := kernelstore.ValidateStepName(step.Name); err != nil {
			return spec, fmt.Errorf("steps[%d]: %w", index, err)
		}
		if _, duplicate := names[step.Name]; duplicate {
			return spec, fmt.Errorf("duplicate step name %q", step.Name)
		}
		names[step.Name] = index
	}

	// Validate each node, then the graph.
	edges := make(map[string][]string, len(spec.Steps))
	for index, step := range spec.Steps {
		if step.AgentVersionRef == "" || len(step.AgentVersionRef) > 256 {
			return spec, fmt.Errorf("steps[%d].agentVersionRef is required and bounded", index)
		}
		if step.Goal == "" || len(step.Goal) > maxGoalBytes {
			return spec, fmt.Errorf("steps[%d].goal must be 1..%d bytes", index, maxGoalBytes)
		}
		if len(step.Spec) > 0 && !json.Valid(step.Spec) {
			return spec, fmt.Errorf("steps[%d].spec must be a JSON object", index)
		}
		seenDeps := map[string]struct{}{}
		normalizedDependencies := make([]string, 0, len(step.DependsOn))
		for _, dependency := range step.DependsOn {
			normalized := dependency
			if parent, group := strings.CutPrefix(dependency, "spawn:"); group {
				if spec.Runtime == nil || !spec.Runtime.Dynamic.Enabled {
					return spec, fmt.Errorf("steps[%d] uses dynamic group dependency %q without runtime.dynamic.enabled", index, dependency)
				}
				if parent == "" {
					return spec, fmt.Errorf("steps[%d] has an empty dynamic group dependency", index)
				}
				normalized = parent
			}
			if _, ok := names[normalized]; !ok {
				return spec, fmt.Errorf("steps[%d] depends on unknown step %q", index, dependency)
			}
			if normalized == step.Name {
				return spec, fmt.Errorf("steps[%d] depends on itself", index)
			}
			if _, duplicate := seenDeps[normalized]; duplicate {
				return spec, fmt.Errorf("steps[%d] declares dependency %q twice", index, dependency)
			}
			seenDeps[normalized] = struct{}{}
			normalizedDependencies = append(normalizedDependencies, normalized)
		}
		edges[step.Name] = normalizedDependencies

		if step.Retry != nil && (step.Retry.MaxAttempts < 1 || step.Retry.MaxAttempts > maxRetryAttempts) {
			return spec, fmt.Errorf("steps[%d].retry.maxAttempts must be 1..%d", index, maxRetryAttempts)
		}
		if step.Condition != nil {
			if len(step.DependsOn) == 0 {
				return spec, fmt.Errorf("steps[%d].condition requires dependsOn", index)
			}
			if _, ok := names[step.Condition.Step]; !ok {
				return spec, fmt.Errorf("steps[%d].condition.step %q is not a declared step", index, step.Condition.Step)
			}
			referenced := false
			for _, dependency := range step.DependsOn {
				if dependency == step.Condition.Step {
					referenced = true
				}
			}
			if !referenced {
				return spec, fmt.Errorf("steps[%d].condition.step must be one of dependsOn", index)
			}
			predicates := 0
			if step.Condition.OutputContains != "" {
				predicates++
				if len(step.Condition.OutputContains) > maxConditionBytes {
					return spec, fmt.Errorf("steps[%d].condition.outputContains exceeds %d bytes", index, maxConditionBytes)
				}
			}
			if step.Condition.OutputEquals != "" {
				predicates++
				if len(step.Condition.OutputEquals) > maxConditionBytes {
					return spec, fmt.Errorf("steps[%d].condition.outputEquals exceeds %d bytes", index, maxConditionBytes)
				}
			}
			if step.Condition.JSONPointer != "" || len(step.Condition.EqualsJSON) != 0 {
				if step.Condition.JSONPointer == "" || (step.Condition.JSONPointer[0] != '/' && step.Condition.JSONPointer != "") {
					return spec, fmt.Errorf("steps[%d].condition.jsonPointer must be an RFC 6901 pointer", index)
				}
				if len(step.Condition.EqualsJSON) == 0 || !json.Valid(step.Condition.EqualsJSON) {
					return spec, fmt.Errorf("steps[%d].condition.equalsJson must be valid JSON", index)
				}
				predicates++
			}
			if predicates != 1 {
				return spec, fmt.Errorf("steps[%d].condition must set exactly one predicate", index)
			}
		}
		if step.Output != nil {
			if strings.TrimSpace(step.Output.SchemaVersion) == "" || len(step.Output.SchemaVersion) > 64 {
				return spec, fmt.Errorf("steps[%d].output.schemaVersion is required and bounded", index)
			}
			if step.Output.ContentType != "" && step.Output.ContentType != "application/json" {
				return spec, fmt.Errorf("steps[%d].output.contentType currently supports application/json only", index)
			}
			if len(step.Output.Schema) == 0 || len(step.Output.Schema) > 64<<10 {
				return spec, fmt.Errorf("steps[%d].output.schema must be 1..65536 bytes", index)
			}
			var schemaDocument any
			if err := json.Unmarshal(step.Output.Schema, &schemaDocument); err != nil {
				return spec, fmt.Errorf("steps[%d].output.schema: %w", index, err)
			}
			compiler := jsonschema.NewCompiler()
			resource := fmt.Sprintf("workflow-step-%d-output.json", index)
			if err := compiler.AddResource(resource, schemaDocument); err != nil {
				return spec, fmt.Errorf("steps[%d].output.schema: %w", index, err)
			}
			if _, err := compiler.Compile(resource); err != nil {
				return spec, fmt.Errorf("steps[%d].output.schema: %w", index, err)
			}
		}
	}
	if cyclic := detectCycle(edges); cyclic {
		return spec, fmt.Errorf("workflow steps must form a directed acyclic graph")
	}
	if err := spec.validateStepSpecs(); err != nil {
		return spec, err
	}
	if err := spec.validateBudgetCommitment(); err != nil {
		return spec, err
	}
	return spec, nil
}

// validateBudgetCommitment rejects documents whose declared steps already
// promise more than the workflow budget: every step reserves its future
// Task's ceiling at creation, so the declaration itself must fit.
func (s WorkflowSpec) validateBudgetCommitment() error {
	if s.Budget == nil {
		return nil
	}
	if s.Budget.MaxTasks > 0 && int64(len(s.Steps)) > s.Budget.MaxTasks {
		return fmt.Errorf("budget.maxTasks %d is below the %d declared steps", s.Budget.MaxTasks, len(s.Steps))
	}
	if s.Budget.MaxTokens <= 0 && s.Budget.MaxCostMicroUSD <= 0 {
		return nil
	}
	defaults := objectMap(s.DefaultTaskSpec)
	var tokens, costMicroUSD int64
	for _, step := range s.Steps {
		merged, err := mergeSpecs(defaults, objectMap(step.Spec))
		if err != nil {
			continue // validateStepSpecs already rejected unmergeable steps
		}
		stepTokens, stepCost := kernelstore.TaskSpecBudgetReservation(merged)
		tokens += stepTokens
		costMicroUSD += int64(stepCost)
	}
	if s.Budget.MaxTokens > 0 && tokens > s.Budget.MaxTokens {
		return fmt.Errorf("declared step budgets reserve %d tokens over budget.maxTokens %d", tokens, s.Budget.MaxTokens)
	}
	if s.Budget.MaxCostMicroUSD > 0 && costMicroUSD > int64(s.Budget.MaxCostMicroUSD) {
		return fmt.Errorf("declared step budgets reserve $%.6f over budget.maxCostUsd $%.6f",
			float64(costMicroUSD)/1_000_000, s.Budget.MaxCostMicroUSD.USD())
	}
	return nil
}

func validateBudget(budget *WorkflowBudget) error {
	if budget == nil {
		return nil
	}
	if budget.MaxTasks < 0 || budget.MaxTasks > maxWorkflowTasks {
		return fmt.Errorf("budget.maxTasks must be 0..%d", maxWorkflowTasks)
	}
	if budget.MaxTokens < 0 || budget.MaxTokens > maxWorkflowTokens {
		return fmt.Errorf("budget.maxTokens must be 0..%d", maxWorkflowTokens)
	}
	if budget.MaxCostMicroUSD < 0 || budget.MaxCostMicroUSD > maxWorkflowCostMicroUSD {
		return fmt.Errorf("budget.maxCostUsd must be 0..%d", int64(maxWorkflowCostMicroUSD)/money.MicroPerUSD)
	}
	return nil
}

func validateRuntimePolicy(runtime *WorkflowRuntimePolicy) error {
	if runtime == nil {
		return nil
	}
	dynamic := runtime.Dynamic
	if !dynamic.Enabled {
		if dynamic.MaxDynamicSteps != 0 || dynamic.MaxChildrenPerStep != 0 || dynamic.MaxSpawnDepth != 0 ||
			dynamic.MaxWorkflowSteps != 0 || dynamic.MaxSpawnTasks != 0 || dynamic.MaxSpawnTokens != 0 || dynamic.MaxSpawnCostMicroUSD != 0 {
			return fmt.Errorf("runtime.dynamic.enabled must be true when dynamic limits are declared")
		}
		return nil
	}
	if dynamic.MaxDynamicSteps <= 0 || dynamic.MaxChildrenPerStep <= 0 || dynamic.MaxSpawnDepth <= 0 || dynamic.MaxWorkflowSteps <= 0 {
		return fmt.Errorf("enabled dynamic spawning requires positive maxDynamicSteps, maxChildrenPerStep, maxSpawnDepth, and maxWorkflowSteps")
	}
	if dynamic.MaxDynamicSteps < 0 || dynamic.MaxDynamicSteps > maxWorkflowSteps {
		return fmt.Errorf("runtime.dynamic.maxDynamicSteps must be 0..%d", maxWorkflowSteps)
	}
	if dynamic.MaxChildrenPerStep < 0 || dynamic.MaxChildrenPerStep > maxWorkflowSteps {
		return fmt.Errorf("runtime.dynamic.maxChildrenPerStep must be 0..%d", maxWorkflowSteps)
	}
	if dynamic.MaxSpawnDepth < 0 || dynamic.MaxSpawnDepth > 16 {
		return fmt.Errorf("runtime.dynamic.maxSpawnDepth must be 0..16")
	}
	if dynamic.MaxWorkflowSteps < 0 || dynamic.MaxWorkflowSteps > maxWorkflowSteps {
		return fmt.Errorf("runtime.dynamic.maxWorkflowSteps must be 0..%d", maxWorkflowSteps)
	}
	if dynamic.MaxSpawnTasks < 0 || dynamic.MaxSpawnTasks > maxWorkflowTasks {
		return fmt.Errorf("runtime.dynamic.maxSpawnTasks must be 0..%d", maxWorkflowTasks)
	}
	if dynamic.MaxSpawnTokens < 0 || dynamic.MaxSpawnTokens > maxWorkflowTokens {
		return fmt.Errorf("runtime.dynamic.maxSpawnTokens must be 0..%d", maxWorkflowTokens)
	}
	if dynamic.MaxSpawnCostMicroUSD < 0 || dynamic.MaxSpawnCostMicroUSD > maxWorkflowCostMicroUSD {
		return fmt.Errorf("runtime.dynamic.maxSpawnCostUsd must be 0..%d", int64(maxWorkflowCostMicroUSD)/money.MicroPerUSD)
	}
	return nil
}

// Budgets returns the validated workflow budget ceilings.
func (s WorkflowSpec) Budgets() (tasks, tokens int64, costMicroUSD money.MicroUSD) {
	if s.Budget == nil {
		return 0, 0, 0
	}
	return s.Budget.MaxTasks, s.Budget.MaxTokens, s.Budget.MaxCostMicroUSD
}

// StepInputs renders the durable step inputs of a validated spec (with the
// default/step overlay merged).
func (s WorkflowSpec) StepInputs() []kernelstore.CreateWorkflowStepInput {
	defaults := objectMap(s.DefaultTaskSpec)
	inputs := make([]kernelstore.CreateWorkflowStepInput, 0, len(s.Steps))
	for _, step := range s.Steps {
		merged, err := mergeSpecs(defaults, objectMap(step.Spec))
		if err != nil {
			continue // validation below already rejected unmergeable steps
		}
		input := kernelstore.CreateWorkflowStepInput{
			Name: step.Name, Spec: merged, AgentVersionRef: step.AgentVersionRef, Goal: step.Goal,
			DependsOn: append([]string(nil), step.DependsOn...), RequiresApproval: step.RequiresApproval,
		}
		if step.Condition != nil {
			input.ConditionStep = step.Condition.Step
			input.ConditionContains = step.Condition.OutputContains
			input.ConditionEquals = step.Condition.OutputEquals
		}
		if step.Retry != nil {
			input.MaxAttempts = step.Retry.MaxAttempts
		} else {
			input.MaxAttempts = maxDefaultAttempts
		}
		inputs = append(inputs, input)
	}
	return inputs
}

// MergeTaskSpec applies the workflow default task specification to a
// dynamic-step overlay. Dynamic children therefore enter the ordinary Task
// pipeline with the same placement and budget defaults as declared steps.
func (s WorkflowSpec) MergeTaskSpec(overlay json.RawMessage) (json.RawMessage, error) {
	var overlayObject map[string]json.RawMessage
	if len(overlay) > 0 {
		if err := json.Unmarshal(overlay, &overlayObject); err != nil || overlayObject == nil {
			return nil, fmt.Errorf("dynamic step spec must be a JSON object")
		}
	}
	return mergeSpecs(objectMap(s.DefaultTaskSpec), overlayObject)
}

// validateStepSpecs rejects steps whose default/overlay merge yields no task
// spec at all (a step with neither defaultTaskSpec nor its own spec).
func (s WorkflowSpec) validateStepSpecs() error {
	defaults := objectMap(s.DefaultTaskSpec)
	for index, step := range s.Steps {
		if _, err := mergeSpecs(defaults, objectMap(step.Spec)); err != nil {
			return fmt.Errorf("steps[%d]: %w", index, err)
		}
	}
	return nil
}

// detectCycle runs reverse Kahn elimination over the dependency graph:
// dependents[x] counts the steps that still depend on x. Sinks (nothing
// depends on them) are removed first; removing a step releases its own
// dependencies. A graph that cannot be fully eliminated is cyclic.
func detectCycle(edges map[string][]string) bool {
	dependents := make(map[string]int, len(edges))
	for name := range edges {
		dependents[name] = 0
	}
	for _, dependencies := range edges {
		for _, dependency := range dependencies {
			dependents[dependency]++
		}
	}
	queue := make([]string, 0, len(dependents))
	for name, count := range dependents {
		if count == 0 {
			queue = append(queue, name)
		}
	}
	sort.Strings(queue)
	visited := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++
		for _, dependency := range edges[current] {
			dependents[dependency]--
			if dependents[dependency] == 0 {
				queue = append(queue, dependency)
			}
		}
	}
	return visited != len(edges)
}

func objectMap(raw json.RawMessage) map[string]json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil
	}
	return object
}

// mergeSpecs merges the workflow default task spec with the step/spawn
// overlay (step keys win) and returns canonical JSON. The placement
// sub-object is merged FIELD-WISE rather than wholesale so an overlay can
// narrow only the runtime classes or preferred class of one step or spawned
// child while inheriting the region, capacity and residency the workflow
// default placement declares — the mechanism behind per-role runtime
// affinity for static steps (step.spec.placement) and dynamic children
// (spawn spec.placement).
func mergeSpecs(defaults, overlay map[string]json.RawMessage) (json.RawMessage, error) {
	if len(defaults) == 0 && len(overlay) == 0 {
		return nil, fmt.Errorf("a task spec is required (defaultTaskSpec or step spec)")
	}
	merged := make(map[string]json.RawMessage, len(defaults)+len(overlay))
	for key, value := range defaults {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	if overlayPlacement, hasOverlay := overlay["placement"]; hasOverlay {
		basePlacement := objectMap(defaults["placement"])
		overlayPlacementObject := objectMap(overlayPlacement)
		if len(basePlacement) > 0 && len(overlayPlacementObject) > 0 {
			fieldMerged := make(map[string]json.RawMessage, len(basePlacement)+len(overlayPlacementObject))
			for key, value := range basePlacement {
				fieldMerged[key] = value
			}
			for key, value := range overlayPlacementObject {
				fieldMerged[key] = value
			}
			encoded, err := json.Marshal(fieldMerged)
			if err != nil {
				return nil, fmt.Errorf("encode merged placement: %w", err)
			}
			merged["placement"] = encoded
		}
	}
	encoded, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("encode merged task spec: %w", err)
	}
	return encoded, nil
}
