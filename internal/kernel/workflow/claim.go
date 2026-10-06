package workflow

import (
	"context"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// workflowClaimer is the store surface ClaimManager needs; the full
// kernelstore.WorkflowStore satisfies it.
type workflowClaimer interface {
	ListActiveWorkflows(context.Context, int) ([]kernelstore.Workflow, error)
	ClaimWorkflows(context.Context, kernelstore.ClaimWorkflowsInput) ([]kernelstore.Workflow, error)
}

// ClaimManager acquires the batch of workflows one reconcile round will
// process, either under an expiring per-instance claim or as every visible
// active workflow in single-instance mode.
type ClaimManager struct {
	workflows   workflowClaimer
	owner       string
	batch       int
	lease       time.Duration
	tokenBudget int64
}

// NewClaimManager builds a ClaimManager for one controller instance.
// A zero lease keeps single-instance behavior (reconcile everything visible).
func NewClaimManager(workflows workflowClaimer, owner string, batch int, lease time.Duration, tokenBudget int64) *ClaimManager {
	return &ClaimManager{
		workflows: workflows, owner: owner, batch: batch,
		lease: lease, tokenBudget: tokenBudget,
	}
}

// Claim returns the active workflows this instance should reconcile this
// round. In claiming mode the store leases each workflow under the manager's
// lease so a dead instance's batch is taken over by peers after expiry.
func (m *ClaimManager) Claim(ctx context.Context) ([]kernelstore.Workflow, error) {
	if m.lease > 0 {
		return m.workflows.ClaimWorkflows(ctx, kernelstore.ClaimWorkflowsInput{
			Owner: m.owner, Batch: m.batch, Lease: m.lease, MaxTokens: m.tokenBudget,
		})
	}
	return m.workflows.ListActiveWorkflows(ctx, m.batch)
}
