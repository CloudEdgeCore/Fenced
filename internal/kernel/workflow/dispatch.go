package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// StepDispatcher creates the fenced Task for one ready step and transitions
// the step to RUNNING. Task creation is idempotent per (workflow, step,
// attempt), so racing orchestrators dispatch exactly once.
type StepDispatcher struct {
	tasks     TaskPipeline
	workflows stepTransitioner
	newID     func() uuid.UUID
	owner     string
}

// NewStepDispatcher builds a dispatcher bound to the task pipeline and step
// store of one controller.
func NewStepDispatcher(tasks TaskPipeline, workflows stepTransitioner, newID func() uuid.UUID, owner string) *StepDispatcher {
	return &StepDispatcher{tasks: tasks, workflows: workflows, newID: newID, owner: owner}
}

// Dispatch creates the step's task and records the RUNNING transition.
// dependencyOutputs are the resolved upstream results rendered into the goal.
func (d *StepDispatcher) Dispatch(ctx context.Context, workflow kernelstore.Workflow, step kernelstore.WorkflowStep, declared *StepSpec, defaultTaskSpec json.RawMessage, dependencyOutputs map[string]string, byName map[string]kernelstore.WorkflowStep) (bool, error) {
	// The stored spec is the raw document; re-apply the default/overlay
	// merge exactly as publication validation did. Dynamic steps carry
	// their merged spec inline (spawned through the broker, after the
	// same merge).
	var (
		agentVersionRef string
		goal            string
		mergedSpec      json.RawMessage
	)
	if step.IsDynamic {
		agentVersionRef, goal, mergedSpec = step.AgentVersionRef, step.Goal, step.Spec
	} else {
		var err error
		mergedSpec, err = mergeSpecs(objectMap(defaultTaskSpec), objectMap(declared.Spec))
		if err != nil {
			return false, fmt.Errorf("step %q task spec: %w", step.Name, err)
		}
		agentVersionRef, goal = declared.AgentVersionRef, declared.Goal
	}

	// Idempotent per (workflow, step, attempt): racing orchestrators create
	// exactly one Task.
	attempt := step.AttemptCount + 1
	created, err := d.tasks.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: d.newID(), TenantID: workflow.TenantID, Namespace: workflow.Namespace,
		AgentVersionRef: agentVersionRef, Goal: renderGoal(goal, dependencyOutputs),
		Spec: mergedSpec, IdempotencyKey: fmt.Sprintf("workflow/%s/%s/%d", workflow.ID, step.Name, attempt),
		WorkflowID: &workflow.ID, WorkflowStepID: &step.ID, WorkflowStepName: step.Name,
		WorkflowAttempt: attempt, ParentTaskID: parentTaskID(step, byName),
	})
	if err != nil {
		return false, fmt.Errorf("create task for step %q: %w", step.Name, err)
	}
	taskID := created.Task.ID
	nextAttempt := attempt
	_, err = d.workflows.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
		TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: step.Name,
		ExpectedVersion: step.ResourceVersion, To: kernelstore.StepRunning,
		TaskID: &taskID, AttemptCount: &nextAttempt, ExpectedOwner: d.owner,
	})
	if err != nil {
		if errors.Is(err, kernelstore.ErrVersionConflict) {
			return false, nil // a concurrent instance dispatched it
		}
		return false, err
	}
	return true, nil
}
