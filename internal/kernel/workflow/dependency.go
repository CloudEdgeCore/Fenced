package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// stepTransitioner is the store surface DependencyResolver needs to record
// a step as SKIPPED when an upstream failed or a condition was not met.
type stepTransitioner interface {
	TransitionWorkflowStep(context.Context, kernelstore.TransitionWorkflowStepInput) (kernelstore.WorkflowStep, error)
}

// DependencyResolver gates one step on its declared dependencies and
// condition. It collects the succeeded upstream outputs for condition
// evaluation and downstream goals, marks the step SKIPPED when an upstream
// failed (or the condition is not met), and reports whether the step must
// keep waiting.
type DependencyResolver struct {
	workflows stepTransitioner
	owner     string
}

// NewDependencyResolver builds a DependencyResolver bound to a step store.
func NewDependencyResolver(workflows stepTransitioner, owner string) *DependencyResolver {
	return &DependencyResolver{workflows: workflows, owner: owner}
}

// Resolve evaluates one step's dependencies and condition against the
// current step set.
//
// It returns (nil, false, nil) while an upstream is still non-terminal and
// (nil, true, err) when the step was transitioned to SKIPPED (failed
// upstream, unmet condition) — the caller must stop advancing the step. On
// success it returns the bounded dependency outputs and ready=true.
func (r *DependencyResolver) Resolve(ctx context.Context, workflow kernelstore.Workflow, step kernelstore.WorkflowStep, declared *StepSpec, byName map[string]kernelstore.WorkflowStep) (map[string]string, bool, error) {
	var dependencyOutputs map[string]string
	if step.IsDynamic {
		dependencyOutputs = make(map[string]string)
	} else {
		dependencyOutputs = make(map[string]string, len(declared.DependsOn))
	}
	for _, dependency := range stepDependencies(step, declared) {
		if parentName, group := strings.CutPrefix(dependency, "spawn:"); group {
			parent, exists := byName[parentName]
			if !exists {
				return nil, false, fmt.Errorf("dynamic group parent %q is missing", parentName)
			}
			switch parent.Status {
			case kernelstore.StepSucceeded:
				// The spawning attempt is terminal, so the child set is now
				// closed and can safely be joined (including an empty set).
			case kernelstore.StepFailed, kernelstore.StepSkipped, kernelstore.StepCancelled:
				return nil, true, r.skip(ctx, workflow, step, "UPSTREAM_NOT_SUCCEEDED")
			default:
				return nil, false, nil
			}
		}
		upstreams := resolveDependencySteps(dependency, step, byName)
		if len(upstreams) == 0 && isGroupDependency(dependency) {
			// The group is empty: no dynamic children were ever spawned, so
			// the dependency is vacuously satisfied.
			continue
		}
		for _, upstream := range upstreams {
			switch upstream.Status {
			case kernelstore.StepSucceeded:
				dependencyOutputs[upstream.Name] = resultSummaryOutput(upstream.ResultSummary)
			case kernelstore.StepFailed, kernelstore.StepSkipped, kernelstore.StepCancelled:
				// A failed upstream never executes its dependents.
				return nil, true, r.skip(ctx, workflow, step, "UPSTREAM_NOT_SUCCEEDED")
			default:
				return nil, false, nil // still waiting on the dependency
			}
		}
	}
	// Condition evaluation against the stored dependency output.
	if condition := stepCondition(step, declared); condition != nil {
		output := dependencyOutputs[condition.Step]
		met := false
		switch {
		case condition.OutputContains != "":
			met = containsBounded(output, condition.OutputContains)
		case condition.OutputEquals != "":
			met = output == condition.OutputEquals
		case condition.JSONPointer != "":
			var upstream any
			if err := json.Unmarshal([]byte(output), &upstream); err == nil {
				actual, ok := resolveJSONPointer(upstream, condition.JSONPointer)
				var expected any
				if ok && json.Unmarshal(condition.EqualsJSON, &expected) == nil {
					met = reflect.DeepEqual(actual, expected)
				}
			}
		}
		if !met {
			return nil, true, r.skip(ctx, workflow, step, "CONDITION_NOT_MET")
		}
	}
	return dependencyOutputs, true, nil
}

func (r *DependencyResolver) skip(ctx context.Context, workflow kernelstore.Workflow, step kernelstore.WorkflowStep, code string) error {
	_, err := r.workflows.TransitionWorkflowStep(ctx, skipStepInput(workflow, step, code, r.owner))
	return err
}

// parentTaskID resolves the task of the step that spawned a dynamic child.
func parentTaskID(step kernelstore.WorkflowStep, byName map[string]kernelstore.WorkflowStep) *uuid.UUID {
	if !step.IsDynamic || step.ParentStepName == "" {
		return nil
	}
	parent, ok := byName[step.ParentStepName]
	if !ok || parent.TaskID == nil {
		return nil
	}
	id := *parent.TaskID
	return &id
}

// stepDependencies returns the dependency names of one step: a dynamic
// child waits for the exact step that spawned it, while a declared step may
// use "spawn:<parent>" to join every child spawned by that parent.
func stepDependencies(step kernelstore.WorkflowStep, declared *StepSpec) []string {
	if step.IsDynamic {
		if step.ParentStepName != "" {
			return []string{step.ParentStepName}
		}
		return nil
	}
	if declared == nil {
		return nil
	}
	return declared.DependsOn
}

// isGroupDependency reports whether a dependency token names a spawn group.
func isGroupDependency(dependency string) bool {
	return strings.HasPrefix(dependency, "spawn:")
}

// resolveDependencySteps maps one dependency token to its step records: the
// named step, or every dynamic child of the group parent.
func resolveDependencySteps(dependency string, step kernelstore.WorkflowStep, byName map[string]kernelstore.WorkflowStep) []kernelstore.WorkflowStep {
	if parent, ok := strings.CutPrefix(dependency, "spawn:"); ok {
		children := make([]kernelstore.WorkflowStep, 0, 4)
		for _, candidate := range byName {
			if candidate.ParentStepName == parent {
				children = append(children, candidate)
			}
		}
		return children
	}
	if upstream, ok := byName[dependency]; ok {
		return []kernelstore.WorkflowStep{upstream}
	}
	return nil
}

// stepCondition returns the effective condition of one step.
func stepCondition(step kernelstore.WorkflowStep, declared *StepSpec) *StepCondition {
	if declared == nil {
		return nil
	}
	return declared.Condition
}

// requiresApproval reports whether one step parks for human approval.
func requiresApproval(step kernelstore.WorkflowStep, declared *StepSpec) bool {
	return declared != nil && declared.RequiresApproval
}

// resultSummaryOutput extracts the plain-text output embedded in a step
// result summary (bounded, for goal rendering).
func resultSummaryOutput(raw json.RawMessage) string {
	var summary map[string]json.RawMessage
	if json.Unmarshal(raw, &summary) == nil {
		output := summary["output"]
		var text string
		if json.Unmarshal(output, &text) == nil {
			return text
		}
		return string(output)
	}
	return ""
}

// resolveJSONPointer evaluates a JSON Pointer against a decoded document.
func resolveJSONPointer(document any, pointer string) (any, bool) {
	if pointer == "" {
		return document, true
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, false
	}
	current := document
	for _, rawToken := range strings.Split(pointer[1:], "/") {
		token := strings.ReplaceAll(strings.ReplaceAll(rawToken, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[token]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// containsBounded reports whether haystack contains needle, bounded to 1 MiB.
func containsBounded(haystack, needle string) bool {
	if len(haystack) > 1<<20 {
		haystack = haystack[:1<<20]
	}
	return len(needle) > 0 && strings.Contains(haystack, needle)
}
