package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/observability"
	"github.com/google/uuid"
)

// GetExecutionCorrelation reconstructs the full execution chain of one
// workflow: steps, tasks, runs/attempts, model and tool calls, memory
// records, audit events, and idempotency receipts.
func (s *Store) GetExecutionCorrelation(ctx context.Context, tenantID string, workflowID uuid.UUID) (observability.ExecutionCorrelation, error) {
	var c observability.ExecutionCorrelation

	// Workflow node.
	if err := s.pool.QueryRow(ctx, `SELECT id::text, status, goal, created_at, updated_at
		FROM workflows WHERE tenant_id = $1 AND id = $2`, tenantID, workflowID).Scan(
		&c.Workflow.ID, &c.Workflow.Status, &c.Workflow.Goal,
		&c.Workflow.CreatedAt, &c.Workflow.CompletedAt); err != nil {
		return c, fmt.Errorf("workflow: %w", err)
	}

	// Steps.
	stepRows, err := s.pool.Query(ctx, `SELECT name, status, COALESCE(task_id::text, ''), COALESCE(failure_code, ''), attempt_count
		FROM workflow_steps WHERE tenant_id = $1 AND workflow_id = $2 ORDER BY ordinal`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("steps: %w", err)
	}
	defer stepRows.Close()
	for stepRows.Next() {
		var step observability.StepNode
		if err := stepRows.Scan(&step.Name, &step.Status, &step.TaskID, &step.FailureCode, &step.AttemptCount); err != nil {
			return c, fmt.Errorf("scan step: %w", err)
		}
		c.Steps = append(c.Steps, step)
	}
	stepRows.Close()

	// Tasks.
	taskRows, err := s.pool.Query(ctx, `SELECT id::text, agent_version_ref, phase, COALESCE(result_ref, ''), created_at
		FROM tasks WHERE tenant_id = $1 AND workflow_id = $2 ORDER BY created_at`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("tasks: %w", err)
	}
	defer taskRows.Close()
	for taskRows.Next() {
		var task observability.TaskNode
		if err := taskRows.Scan(&task.ID, &task.AgentVersionRef, &task.Phase, &task.ResultRef, &task.CreatedAt); err != nil {
			return c, fmt.Errorf("scan task: %w", err)
		}
		c.Tasks = append(c.Tasks, task)
	}
	taskRows.Close()

	// Attempts (via runs).
	attemptRows, err := s.pool.Query(ctx, `SELECT t.id::text, r.id::text, r.ordinal, a.ordinal, a.phase,
		a.runtime_class, a.runtime_instance_id, COALESCE(a.failure_code, ''), a.fencing_token, a.created_at
		FROM attempts a JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
		JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
		WHERE t.tenant_id = $1 AND t.workflow_id = $2
		ORDER BY a.created_at`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("attempts: %w", err)
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var attempt observability.AttemptNode
		if err := attemptRows.Scan(&attempt.TaskID, &attempt.RunID, &attempt.RunOrdinal, &attempt.Ordinal,
			&attempt.Phase, &attempt.RuntimeClass, &attempt.RuntimeInstanceID, &attempt.FailureCode,
			&attempt.FencingToken, &attempt.CreatedAt); err != nil {
			return c, fmt.Errorf("scan attempt: %w", err)
		}
		c.Attempts = append(c.Attempts, attempt)
	}
	attemptRows.Close()

	// Model calls.
	modelRows, err := s.pool.Query(ctx, `SELECT mc.task_id::text, mc.run_id::text, mc.attempt_id::text, mc.model_ref, mc.status
		FROM model_calls mc JOIN tasks t ON t.id = mc.task_id AND t.tenant_id = mc.tenant_id
		WHERE t.tenant_id = $1 AND t.workflow_id = $2 ORDER BY mc.created_at`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("model calls: %w", err)
	}
	defer modelRows.Close()
	for modelRows.Next() {
		var call observability.ModelCallNode
		if err := modelRows.Scan(&call.TaskID, &call.RunID, &call.AttemptID, &call.ModelRef, &call.Status); err != nil {
			return c, fmt.Errorf("scan model call: %w", err)
		}
		c.ModelCalls = append(c.ModelCalls, call)
	}
	modelRows.Close()

	// Tool calls.
	toolRows, err := s.pool.Query(ctx, `SELECT tc.task_id::text, tc.run_id::text, tc.attempt_id::text, tc.tool_name, tc.action, tc.status
		FROM tool_calls tc JOIN tasks t ON t.id = tc.task_id AND t.tenant_id = tc.tenant_id
		WHERE t.tenant_id = $1 AND t.workflow_id = $2 ORDER BY tc.created_at`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("tool calls: %w", err)
	}
	defer toolRows.Close()
	for toolRows.Next() {
		var call observability.ToolCallNode
		if err := toolRows.Scan(&call.TaskID, &call.RunID, &call.AttemptID, &call.ToolName, &call.Action, &call.Status); err != nil {
			return c, fmt.Errorf("scan tool call: %w", err)
		}
		c.ToolCalls = append(c.ToolCalls, call)
	}
	toolRows.Close()

	// Memory records written by this workflow's tasks/attempts.
	memRows, err := s.pool.Query(ctx, `SELECT namespace, key, COALESCE(source_task_id::text, ''),
		COALESCE(source_attempt_id::text, '')
		FROM memory_records
		WHERE tenant_id = $1 AND (
			source_task_id IN (SELECT id FROM tasks WHERE tenant_id = $1 AND workflow_id = $2)
			OR namespace LIKE 'research/' || $2::text || '/%'
			OR namespace LIKE 'devops/' || $2::text || '/%')
		ORDER BY namespace, key`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("memory: %w", err)
	}
	defer memRows.Close()
	for memRows.Next() {
		var mem observability.MemoryNode
		if err := memRows.Scan(&mem.Namespace, &mem.Key, &mem.SourceTaskID, &mem.SourceAttemptID); err != nil {
			return c, fmt.Errorf("scan memory: %w", err)
		}
		c.Memories = append(c.Memories, mem)
	}
	memRows.Close()

	// Idempotency receipts.
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM runtime_operation_receipts r
		JOIN attempts a ON a.id = r.attempt_id AND a.tenant_id = r.tenant_id
		JOIN runs rn ON rn.id = a.run_id AND rn.tenant_id = a.tenant_id
		JOIN tasks t ON t.id = rn.task_id AND t.tenant_id = rn.tenant_id
		WHERE t.tenant_id = $1 AND t.workflow_id = $2`, tenantID, workflowID).Scan(&c.RuntimeOperationReceipts); err != nil {
		return c, fmt.Errorf("receipts: %w", err)
	}

	// Audit events for the workflow's resources.
	auditRows, err := s.pool.Query(ctx, `SELECT event_type, resource_type, resource_id::text, actor,
		COALESCE(details::text, ''), occurred_at
		FROM audit_events
		WHERE tenant_id = $1 AND (
			resource_id = $2
			OR resource_id IN (SELECT id FROM tasks WHERE tenant_id = $1 AND workflow_id = $2)
			OR resource_id IN (SELECT id FROM runs WHERE tenant_id = $1 AND task_id IN
				(SELECT id FROM tasks WHERE tenant_id = $1 AND workflow_id = $2)))
		ORDER BY seq`, tenantID, workflowID)
	if err != nil {
		return c, fmt.Errorf("audit: %w", err)
	}
	defer auditRows.Close()
	for auditRows.Next() {
		var audit observability.AuditNode
		if err := auditRows.Scan(&audit.EventType, &audit.ResourceType, &audit.ResourceID, &audit.Actor,
			&audit.Details, &audit.OccurredAt); err != nil {
			return c, fmt.Errorf("scan audit: %w", err)
		}
		c.AuditEvents = append(c.AuditEvents, audit)
	}
	auditRows.Close()

	return c, nil
}

// AggregateMetrics computes the §Phase-7 core metrics over a tenant's
// workflow window (since, inclusive).
func (s *Store) AggregateMetrics(ctx context.Context, tenantID string, since time.Time) (*observability.Metrics, error) {
	var m observability.Metrics

	// Workflow phases.
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM workflows WHERE tenant_id = $1 AND created_at >= $2),
		(SELECT COUNT(*) FROM workflows WHERE tenant_id = $1 AND created_at >= $2 AND status = 'SUCCEEDED'),
		(SELECT COUNT(*) FROM workflows WHERE tenant_id = $1 AND created_at >= $2 AND status IN ('FAILED','CANCELLED'))`,
		tenantID, since).Scan(&m.WorkflowCount, &m.WorkflowSucceeded, &m.WorkflowFailed); err != nil {
		return nil, fmt.Errorf("workflow counts: %w", err)
	}

	// Task phases.
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND created_at >= $2),
		(SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND created_at >= $2 AND phase = 'SUCCEEDED'),
		(SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND created_at >= $2 AND phase IN ('FAILED','CANCELLED','TIMED_OUT'))`,
		tenantID, since).Scan(&m.TaskCount, &m.TaskSucceeded, &m.TaskFailed); err != nil {
		return nil, fmt.Errorf("task counts: %w", err)
	}

	// Calls and records.
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM model_calls WHERE tenant_id = $1 AND created_at >= $2),
		(SELECT COUNT(*) FROM tool_calls WHERE tenant_id = $1 AND created_at >= $2),
		(SELECT COUNT(*) FROM memory_records WHERE tenant_id = $1 AND created_at >= $2),
		(SELECT COUNT(*) FROM audit_events WHERE tenant_id = $1 AND occurred_at >= $2),
		(SELECT COUNT(*) FROM runtime_operation_receipts r
			JOIN attempts a ON a.id = r.attempt_id AND a.tenant_id = r.tenant_id
			JOIN runs rn ON rn.id = a.run_id AND rn.tenant_id = a.tenant_id
			JOIN tasks t ON t.id = rn.task_id AND t.tenant_id = rn.tenant_id
			WHERE t.tenant_id = $1 AND t.created_at >= $2)`,
		tenantID, since).Scan(&m.ModelCalls, &m.ToolCalls, &m.MemoryRecords, &m.AuditEvents, &m.Receipts); err != nil {
		return nil, fmt.Errorf("call counts: %w", err)
	}

	if m.WorkflowCount > 0 {
		m.WorkflowSuccessRate = float64(m.WorkflowSucceeded) / float64(m.WorkflowCount)
	}
	if m.TaskCount > 0 {
		m.TaskSuccessRate = float64(m.TaskSucceeded) / float64(m.TaskCount)
	}

	// Scheduling latency: first-run placement latency (task created → run created).
	latencyRows, err := s.pool.Query(ctx, `SELECT EXTRACT(EPOCH FROM (r.created_at - t.created_at)) * 1000
		FROM runs r JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
		WHERE t.tenant_id = $1 AND t.created_at >= $2 AND r.ordinal = 1
		AND r.created_at >= t.created_at`, tenantID, since)
	if err != nil {
		return nil, fmt.Errorf("latency: %w", err)
	}
	defer latencyRows.Close()
	var latencies []float64
	for latencyRows.Next() {
		var value float64
		if err := latencyRows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scan latency: %w", err)
		}
		latencies = append(latencies, value)
	}
	latencyRows.Close()
	sort.Float64s(latencies)
	m.SchedulingLatencyMillis = observability.Percentiles{
		P50: observability.Percentile(latencies, 50),
		P95: observability.Percentile(latencies, 95),
		P99: observability.Percentile(latencies, 99),
	}

	// Retry and recovery rates.
	var totalAttempts, retried, recovered int64
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM attempts a JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
			JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
			WHERE t.tenant_id = $1 AND t.created_at >= $2),
		(SELECT COUNT(*) FROM attempts a JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
			JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
			WHERE t.tenant_id = $1 AND t.created_at >= $2 AND a.ordinal >= 2),
		(SELECT COUNT(*) FROM attempts a JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
			JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
			WHERE t.tenant_id = $1 AND t.created_at >= $2
			  AND a.failure_code IN ('LEASE_EXPIRED','LEASE_EXPIRED_UNCLAIMED'))`,
		tenantID, since).Scan(&totalAttempts, &retried, &recovered); err != nil {
		return nil, fmt.Errorf("attempt metrics: %w", err)
	}
	if totalAttempts > 0 {
		m.RetryRate = float64(retried) / float64(totalAttempts)
		m.RecoveryRate = float64(recovered) / float64(totalAttempts)
	}

	// Budget drift: any workflow whose reserved != settled tokens in the window.
	var driftCount int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM workflow_usage_ledgers
		WHERE tenant_id = $1 AND updated_at >= $2 AND reserved_tokens <> settled_tokens`,
		tenantID, since).Scan(&driftCount); err != nil {
		return nil, fmt.Errorf("budget drift: %w", err)
	}
	m.BudgetDrift = driftCount > 0

	// Capacity drift: ACTIVE reservations still outstanding.
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM runtime_capacity_reservations
		WHERE tenant_id = $1 AND status = 'ACTIVE'`, tenantID).Scan(&m.CapacityDrift); err != nil {
		return nil, fmt.Errorf("capacity drift: %w", err)
	}

	// Duplicate side effects: tool calls EXECUTED more than once for the same
	// (task, tool, action, args hash) — a retry-after-failure leaves the first
	// call FAILED, so only genuine double-execution is counted.
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM (
		SELECT task_id, tool_name, action, args_hash
		FROM tool_calls WHERE tenant_id = $1 AND created_at >= $2
		GROUP BY task_id, tool_name, action, args_hash
		HAVING COUNT(*) FILTER (WHERE status = 'EXECUTED') > 1) x`,
		tenantID, since).Scan(&m.DuplicateSideEffects); err != nil {
		return nil, fmt.Errorf("duplicate side effects: %w", err)
	}

	// Cross-tenant violations: any call or record in another tenant referencing
	// this tenant's tasks.
	var violations int
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM tool_calls tc WHERE tc.tenant_id <> $1 AND tc.task_id IN
			(SELECT id FROM tasks WHERE tenant_id = $1))
		+ (SELECT COUNT(*) FROM model_calls mc WHERE mc.tenant_id <> $1 AND mc.task_id IN
			(SELECT id FROM tasks WHERE tenant_id = $1))`,
		tenantID).Scan(&violations); err != nil {
		return nil, fmt.Errorf("cross-tenant: %w", err)
	}
	m.CrossTenantViolations = violations

	return &m, nil
}
