package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
)

var _ supervisor.ServiceLockStore = (*Store)(nil)
var _ supervisor.AllServicesStore = (*Store)(nil)
var _ supervisor.TaskRepository = (*Store)(nil)

// WithServiceLock serializes every operation for one service across control and
// controller processes. The callback store is bound to the lock transaction;
// its lifecycle transactions are savepoints and commit only with the callback.
func (s *Store) WithServiceLock(ctx context.Context, tenantID, serviceID string, fn func(supervisor.Store) error) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(serviceID) == "" || fn == nil {
		return fmt.Errorf("service lock requires tenant, service ID, and callback")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	// Length-prefix the tenant so arbitrary IDs cannot alias another tuple.
	key := fmt.Sprintf("fenced/service/%d:%s%s", len(tenantID), tenantID, serviceID)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return classify(err)
	}
	scoped := &Store{pool: transactionDatabase{Tx: tx}, clock: s.clock, newID: s.newID}
	if err := fn(scoped); err != nil {
		return err
	}
	return classify(tx.Commit(ctx))
}

// ListAllServices is the internal controller enumeration. Tenant-scoped API
// reads must keep using ListServices, including when their tenant is empty.
func (s *Store) ListAllServices(ctx context.Context) ([]*supervisor.Service, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, tenant_id, namespace, name, agent_id,
		spec, status, created_at, updated_at FROM agent_services ORDER BY tenant_id, created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var services []*supervisor.Service
	for rows.Next() {
		var service supervisor.Service
		var spec, status []byte
		if err := rows.Scan(&service.ID, &service.TenantID, &service.Namespace, &service.Name,
			&service.AgentID, &spec, &status, &service.CreatedAt, &service.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(spec, &service.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal service spec: %w", err)
		}
		if err := json.Unmarshal(status, &service.Status); err != nil {
			return nil, fmt.Errorf("unmarshal service status: %w", err)
		}
		services = append(services, &service)
	}
	return services, rows.Err()
}

type serviceExecutionScanner struct {
	row            scanner
	attempt, lease *[]byte
}

func (s serviceExecutionScanner) Scan(destinations ...any) error {
	return s.row.Scan(append(destinations, s.attempt, s.lease)...)
}

// GetServiceTaskExecution reads the task and its current owner in one statement
// snapshot. A completed task remains observable after its lease is released.
func (s *Store) GetServiceTaskExecution(ctx context.Context, tenantID string, taskID uuid.UUID) (supervisor.TaskExecution, error) {
	var execution supervisor.TaskExecution
	columns := strings.Split(taskColumns, ",")
	for index, column := range columns {
		columns[index] = "t." + strings.TrimSpace(column)
	}
	query := `SELECT ` + strings.Join(columns, ", ") + `,
		CASE WHEN a.id IS NOT NULL THEN jsonb_build_object(
			'ID', a.id, 'TenantID', a.tenant_id, 'RunID', a.run_id, 'Ordinal', a.ordinal,
			'Phase', a.phase, 'RuntimeClass', a.runtime_class,
			'RuntimePoolID', COALESCE(a.runtime_pool_id, ''), 'RuntimeInstanceID', a.runtime_instance_id,
			'FencingToken', a.fencing_token, 'FailureCode', COALESCE(a.failure_code, ''),
			'FailureMessage', COALESCE(a.failure_message, ''), 'ResourceVersion', a.resource_version,
			'CreatedAt', a.created_at, 'UpdatedAt', a.updated_at, 'StartedAt', a.started_at,
			'FinishedAt', a.finished_at) END,
		CASE WHEN l.id IS NOT NULL THEN jsonb_build_object(
			'ID', l.id, 'TenantID', l.tenant_id, 'RunID', l.run_id, 'AttemptID', l.attempt_id,
			'FencingToken', l.fencing_token, 'ResourceVersion', l.resource_version,
			'AcquiredAt', l.acquired_at, 'HeartbeatAt', l.heartbeat_at, 'ExpiresAt', l.expires_at) END
		FROM tasks t
		LEFT JOIN runs r ON r.tenant_id = t.tenant_id AND r.id = t.active_run_id
		LEFT JOIN attempts a ON a.tenant_id = r.tenant_id AND a.id = r.active_attempt_id
		LEFT JOIN runtime_leases l ON l.tenant_id = a.tenant_id AND l.attempt_id = a.id
			AND l.fencing_token = a.fencing_token AND l.released_at IS NULL
		WHERE t.tenant_id = $1 AND t.id = $2`
	var attempt, lease []byte
	task, err := scanTask(serviceExecutionScanner{s.pool.QueryRow(ctx, query, tenantID, taskID), &attempt, &lease})
	if err != nil {
		return execution, classify(err)
	}
	execution.Task = task
	if len(attempt) > 0 {
		if err := json.Unmarshal(attempt, &execution.Attempt); err != nil {
			return execution, fmt.Errorf("decode service task attempt: %w", err)
		}
	}
	if len(lease) > 0 {
		if err := json.Unmarshal(lease, &execution.Lease); err != nil {
			return execution, fmt.Errorf("decode service task lease: %w", err)
		}
	}
	return execution, nil
}
