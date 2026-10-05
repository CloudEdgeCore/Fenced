package workflow

import (
	"context"
	"errors"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/errorcode"
)

// RetryController decides what happens to a step whose task failed: retry
// within the step retry budget (back to PENDING) or fail the step when the
// budget is exhausted. A retry denied by the workflow budget fails the step
// instead of retry-looping against a hard ceiling.
type RetryController struct {
	workflows stepTransitioner
	owner     string
}

// NewRetryController builds a retry gate bound to a step store.
func NewRetryController(workflows stepTransitioner, owner string) *RetryController {
	return &RetryController{workflows: workflows, owner: owner}
}

// HandleFailed reacts to a FAILED step task. It reports whether the step
// state moved (retried or failed); nil error means the step is now terminal
// or parked for retry.
func (r *RetryController) HandleFailed(ctx context.Context, workflow kernelstore.Workflow, step kernelstore.WorkflowStep, declared *StepSpec) (bool, error) {
	maxAttempts := 1
	if step.IsDynamic {
		maxAttempts = step.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
	} else if declared != nil && declared.Retry != nil {
		maxAttempts = declared.Retry.MaxAttempts
	}
	if step.AttemptCount < maxAttempts {
		// Single-step retry: back to PENDING; completed siblings stay.
		_, err := r.workflows.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
			TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: step.Name,
			ExpectedVersion: step.ResourceVersion, To: kernelstore.StepPending,
			FailureCode: "RETRY_AFTER_TASK_FAILED", ExpectedOwner: r.owner,
		})
		if err != nil && errors.Is(err, kernelstore.ErrVersionConflict) {
			return false, nil
		}
		if code, denied := kernelstore.DenialCode(err); denied && code == errorcode.SpawnBudgetExhausted {
			// The retry's reservation exceeds the workflow budget: fail
			// the step instead of retry-looping against a hard ceiling.
			_, failErr := r.workflows.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
				TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: step.Name,
				ExpectedVersion: step.ResourceVersion, To: kernelstore.StepFailed,
				FailureCode: errorcode.WorkflowBudgetExhausted, ExpectedOwner: r.owner,
			})
			if failErr != nil && !errors.Is(failErr, kernelstore.ErrVersionConflict) {
				return true, failErr
			}
			return true, nil
		}
		return true, err
	}
	_, err := r.workflows.TransitionWorkflowStep(ctx, kernelstore.TransitionWorkflowStepInput{
		TenantID: workflow.TenantID, WorkflowID: workflow.ID, StepName: step.Name,
		ExpectedVersion: step.ResourceVersion, To: kernelstore.StepFailed,
		FailureCode: "TASK_FAILED", ExpectedOwner: r.owner,
	})
	if err != nil && errors.Is(err, kernelstore.ErrVersionConflict) {
		return false, nil
	}
	return true, err
}
