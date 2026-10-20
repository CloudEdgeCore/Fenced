package postgres

import (
	"context"
	"strings"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// This file holds the operator read surface: the list queries the console
// and the operator scripts use to observe a tenant. Every query is scoped to
// one tenant, ordered newest first, and bounded - an operator view is never
// an unbounded table scan.

// ListTasks returns the most recent tasks of a tenant, newest first. An
// empty namespace lists every namespace of the tenant.
func (s *Store) ListTasks(ctx context.Context, tenantID, namespace string, limit int) ([]kernelstore.Task, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, kernelstore.ErrNotFound
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	query := `SELECT ` + taskColumns + ` FROM tasks WHERE tenant_id = $1`
	args := []any{tenantID}
	if strings.TrimSpace(namespace) != "" {
		query += ` AND namespace = $2`
		args = append(args, namespace)
	}
	query += ` ORDER BY created_at DESC LIMIT $` + itoa(len(args)+1)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	tasks := []kernelstore.Task{}
	for rows.Next() {
		task, scanErr := scanTask(rows)
		if scanErr != nil {
			return nil, classify(scanErr)
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// ListAgentVersions returns the most recent immutable publications of a
// tenant, newest first.
func (s *Store) ListAgentVersions(ctx context.Context, tenantID string, limit int) ([]kernelstore.AgentVersion, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, kernelstore.ErrNotFound
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `SELECT `+agentVersionColumns+` FROM agent_versions
		WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	versions := []kernelstore.AgentVersion{}
	for rows.Next() {
		version, scanErr := scanAgentVersion(rows)
		if scanErr != nil {
			return nil, classify(scanErr)
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}

// ListToolApprovals returns the approval requests of a tenant that are still
// waiting for a human decision, oldest first: the queue an operator has to
// work through.
func (s *Store) ListToolApprovals(ctx context.Context, tenantID string, limit int) ([]kernelstore.ToolApproval, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, kernelstore.ErrNotFound
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `SELECT `+toolApprovalColumns+` FROM tool_approvals
		WHERE tenant_id = $1 AND status = 'PENDING' ORDER BY requested_at ASC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	approvals := []kernelstore.ToolApproval{}
	for rows.Next() {
		approval, scanErr := scanToolApproval(rows)
		if scanErr != nil {
			return nil, classify(scanErr)
		}
		approvals = append(approvals, approval)
	}
	return approvals, rows.Err()
}

// ListWorkflows returns the most recent workflows of a tenant, newest first.
// The orchestrator's own ListActiveWorkflows is deliberately global: it
// schedules across every tenant. An operator view is never global.
func (s *Store) ListWorkflows(ctx context.Context, tenantID string, limit int) ([]kernelstore.Workflow, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, kernelstore.ErrNotFound
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `SELECT `+workflowColumns+` FROM workflows
		WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	workflows := []kernelstore.Workflow{}
	for rows.Next() {
		workflow, scanErr := scanWorkflowRow(rows)
		if scanErr != nil {
			return nil, classify(scanErr)
		}
		workflows = append(workflows, workflow)
	}
	return workflows, rows.Err()
}

// itoa renders a small integer for the positional parameter markers of a
// query.
func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}
