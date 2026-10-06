package workflow

import (
	"context"
	"errors"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/errorcode"
	"github.com/google/uuid"
)

// drainStore is the store surface FailureDrainer needs to re-read steps and
// transition workflows and steps.
type drainStore interface {
	ListWorkflowSteps(context.Context, string, uuid.UUID) ([]kernelstore.WorkflowStep, error)
	TransitionWorkflow(context.Context, kernelstore.TransitionWorkflowInput) (kernelstore.Workflow, error)
	TransitionWorkflowStep(context.Context, kernelstore.TransitionWorkflowStepInput) (kernelstore.WorkflowStep, error)
}

// FailureDrainer cancels active step tasks, skips undispatched steps, and
// finalizes a failing or cancelled workflow once nothing is running. A step
// that already succeeded keeps its result.
type FailureDrainer struct {
	workflows drainStore
	tasks     TaskPipeline
	owner     string
}

// NewFailureDrainer builds a drainer bound to the workflow store and task
// pipeline of one controller.
func NewFailureDrainer(workflows drainStore, tasks TaskPipeline, owner string) *FailureDrainer {
	return &FailureDrainer{workflows: workflows, tasks: tasks, owner: owner}
}

// Drain cancels active tasks, skips undispatched steps, and finalizes the
// workflow FAILED with the durable failureCode once every step is terminal.
func (d *FailureDrainer) Drain(ctx context.Context, workflow kernelstore.Workflow, steps []kernelstore.WorkflowStep, failureCode string) (bool, error) {
	moved := false
	for _, step := range steps {
		if step.Status.Terminal() {
			continue
		}
		if step.Status == kernelstore.StepRunning && step.TaskID != nil {
			task, err := d.tasks.GetTask(ctx, workflow.TenantID, *step.TaskID)
			if err != nil {
				return moved, err
			}
			if !task.Phase.Terminal() {
				if _, err := d.tasks.RequestTaskCancellation(ctx, workflow.TenantID, task.ID, task.ResourceVersion); err != nil {
					if errors.Is(err, kernelstore.ErrVersionConflict) {
						continue
					}
					return moved, err
				}
				moved = true
				continue
			}
			target, code := kernelstore.StepCancelled, failureCode
			if task.Phase == "SUCCEEDED" {
				target, code = kernelstore.StepSucceeded, ""
			}
			_, err = d.workflows.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
				TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: step.Name,
				ExpectedVersion: step.ResourceVersion, To: target, FailureCode: code, ExpectedOwner: d.owner,
			})
			if err != nil && !errors.Is(err, kernelstore.ErrVersionConflict) {
				return moved, err
			}
			moved = true
			continue
		}
		_, err := d.workflows.TransitionWorkflowStep(ctx, skipStepInput(workflow, step, failureCode, d.owner))
		if err != nil {
			if errors.Is(err, kernelstore.ErrVersionConflict) {
				continue
			}
			return moved, err
		}
		moved = true
	}
	current, err := d.workflows.ListWorkflowSteps(ctx, workflow.TenantID, workflow.ID)
	if err != nil {
		return moved, err
	}
	for _, step := range current {
		if !step.Status.Terminal() {
			return moved, nil
		}
	}
	if workflow.Status != kernelstore.WorkflowFailed {
		if _, err := d.workflows.TransitionWorkflow(ctx, kernelstore.TransitionWorkflowInput{
			TenantID: workflow.TenantID, WorkflowID: workflow.ID, ExpectedVersion: workflow.ResourceVersion,
			To: kernelstore.WorkflowFailed, FailureCode: failureCode, ExpectedOwner: d.owner,
		}); err != nil && !errors.Is(err, kernelstore.ErrVersionConflict) {
			return moved, err
		}
		return true, nil
	}
	return moved, nil
}

// DrainBudgetExhausted drains a budget-stopped workflow exactly like Drain
// with the durable budget-exhausted reason.
func (d *FailureDrainer) DrainBudgetExhausted(ctx context.Context, workflow kernelstore.Workflow, steps []kernelstore.WorkflowStep) (bool, error) {
	return d.Drain(ctx, workflow, steps, errorcode.WorkflowBudgetExhausted)
}

// Cancel propagates the durable cancel intent: active step tasks are
// cancelled through the kernel, undispatched steps are skipped, and the
// workflow finalizes CANCELLED once nothing is running.
func (d *FailureDrainer) Cancel(ctx context.Context, workflow kernelstore.Workflow, steps []kernelstore.WorkflowStep) (bool, error) {
	moved := false
	for _, step := range steps {
		if step.Status.Terminal() {
			continue
		}
		if step.Status == kernelstore.StepRunning && step.TaskID != nil {
			task, err := d.tasks.GetTask(ctx, workflow.TenantID, *step.TaskID)
			if err != nil {
				return moved, err
			}
			if !task.Phase.Terminal() {
				if _, err := d.tasks.RequestTaskCancellation(ctx, workflow.TenantID, task.ID, task.ResourceVersion); err != nil {
					if errors.Is(err, kernelstore.ErrVersionConflict) {
						continue // already moving; next round observes it
					}
					return moved, err
				}
				moved = true
				continue
			}
			// The task reached a terminal phase during cancellation: record
			// the observed outcome (success stays success).
			target, code := kernelstore.StepCancelled, "WORKFLOW_CANCELLED"
			if task.Phase == "SUCCEEDED" {
				target, code = kernelstore.StepSucceeded, ""
			}
			_, err = d.workflows.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
				TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: step.Name,
				ExpectedVersion: step.ResourceVersion, To: target, FailureCode: code, ExpectedOwner: d.owner,
			})
			if err != nil && !errors.Is(err, kernelstore.ErrVersionConflict) {
				return moved, err
			}
			moved = true
			continue
		}
		_, err := d.workflows.TransitionWorkflowStep(ctx, skipStepInput(workflow, step, "WORKFLOW_CANCELLED", d.owner))
		if err != nil {
			if errors.Is(err, kernelstore.ErrVersionConflict) {
				continue
			}
			return moved, err
		}
		moved = true
	}
	// Finalize when the drain finished.
	steps, err := d.workflows.ListWorkflowSteps(ctx, workflow.TenantID, workflow.ID)
	if err != nil {
		return moved, err
	}
	for _, step := range steps {
		if !step.Status.Terminal() {
			return moved, nil
		}
	}
	if workflow.Status != kernelstore.WorkflowCancelled {
		if _, err := d.workflows.TransitionWorkflow(ctx, kernelstore.TransitionWorkflowInput{
			TenantID: workflow.TenantID, WorkflowID: workflow.ID, ExpectedVersion: workflow.ResourceVersion,
			To: kernelstore.WorkflowCancelled, FailureCode: "WORKFLOW_CANCELLED", ExpectedOwner: d.owner,
		}); err != nil && !errors.Is(err, kernelstore.ErrVersionConflict) {
			return moved, err
		}
		return true, nil
	}
	return moved, nil
}
