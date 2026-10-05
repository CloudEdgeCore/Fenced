package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// reserveRuntimeCapacity serializes every placement for a pool on its
// capacity row and commits the reservation with Task/Run/Attempt/Lease.
//
// Total capacity is operator-owned: only the runtime pool registry writes
// total_*. Scheduling reads totals under the row lock taken by the guarded
// UPDATE and may only move reserved_*, so a stale scheduler snapshot can
// never overwrite an operator scaling decision. A pool without a registered
// capacity row fails closed instead of bootstrapping totals from a snapshot.
func (s *Store) reserveRuntimeCapacity(ctx context.Context, tx pgx.Tx, in kernelstore.ScheduleTaskInput, now time.Time) error {
	command, err := tx.Exec(ctx, `UPDATE runtime_pool_capacities SET
		reserved_cpu_millis = reserved_cpu_millis + $1,
		reserved_memory_mib = reserved_memory_mib + $2,
		reserved_llm_slots = reserved_llm_slots + $3,
		resource_version = resource_version + 1, updated_at = $4
		WHERE pool_id = $5
		  AND reserved_cpu_millis + $1 <= total_cpu_millis
		  AND reserved_memory_mib + $2 <= total_memory_mib
		  AND reserved_llm_slots + $3 <= total_llm_slots`,
		in.RequestedCPU, in.RequestedMemory, in.RequestedLLMSlots, now, in.RuntimePoolID)
	if err != nil {
		return classify(err)
	}
	if command.RowsAffected() != 1 {
		var registered bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_pool_capacities WHERE pool_id=$1)`,
			in.RuntimePoolID).Scan(&registered); err != nil {
			return classify(err)
		}
		if !registered {
			return fmt.Errorf("%w: pool %s has no registered capacity ledger", kernelstore.ErrPoolCapacityNotInitialized, in.RuntimePoolID)
		}
		return fmt.Errorf("%w: pool=%s cpu=%d memory=%d llm=%d", kernelstore.ErrCapacityExhausted,
			in.RuntimePoolID, in.RequestedCPU, in.RequestedMemory, in.RequestedLLMSlots)
	}
	// The reservation row is task-scoped for the task's whole life
	// (PRIMARY KEY tenant/task): a requeued task re-reserves after its
	// previous attempt released, so the insert upgrades the released row.
	if _, err := tx.Exec(ctx, `INSERT INTO runtime_capacity_reservations (
		tenant_id, task_id, pool_id, cpu_millis, memory_mib, llm_slots,
		owner_fencing_token, status, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE', $8)
	ON CONFLICT (tenant_id, task_id) DO UPDATE SET
		pool_id = EXCLUDED.pool_id,
		cpu_millis = EXCLUDED.cpu_millis,
		memory_mib = EXCLUDED.memory_mib,
		llm_slots = EXCLUDED.llm_slots,
		owner_fencing_token = EXCLUDED.owner_fencing_token,
		status = 'ACTIVE',
		created_at = EXCLUDED.created_at,
		released_at = NULL`,
		in.TenantID, in.TaskID.String(), in.RuntimePoolID, in.RequestedCPU,
		in.RequestedMemory, in.RequestedLLMSlots, in.ClaimFencingToken, now); err != nil {
		return classify(err)
	}
	return nil
}

// releaseRuntimeCapacity is idempotent and task-scoped. The reservation row
// is locked before decrementing the pool, so stale completion retries cannot
// release capacity owned by another task or twice.
func (s *Store) releaseRuntimeCapacity(ctx context.Context, tx pgx.Tx, tenantID string, taskID uuid.UUID, now time.Time) error {
	var poolID string
	var cpu, memory int64
	var llm int
	err := tx.QueryRow(ctx, `SELECT pool_id, cpu_millis, memory_mib, llm_slots
		FROM runtime_capacity_reservations
		WHERE tenant_id = $1 AND task_id = $2 AND status = 'ACTIVE' FOR UPDATE`,
		tenantID, taskID.String()).Scan(&poolID, &cpu, &memory, &llm)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return classify(err)
	}
	command, err := tx.Exec(ctx, `UPDATE runtime_pool_capacities SET
		reserved_cpu_millis = reserved_cpu_millis - $1,
		reserved_memory_mib = reserved_memory_mib - $2,
		reserved_llm_slots = reserved_llm_slots - $3,
		resource_version = resource_version + 1, updated_at = $4
	WHERE pool_id = $5
	  AND reserved_cpu_millis >= $1
	  AND reserved_memory_mib >= $2
	  AND reserved_llm_slots >= $3`, cpu, memory, llm, now, poolID)
	if err != nil {
		return classify(err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("capacity ledger drift for pool %s", poolID)
	}
	command, err = tx.Exec(ctx, `UPDATE runtime_capacity_reservations
		SET status = 'RELEASED', released_at = $1
		WHERE tenant_id = $2 AND task_id = $3 AND status = 'ACTIVE'`, now, tenantID, taskID.String())
	if err != nil {
		return classify(err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("capacity reservation changed before release for task %s", taskID)
	}
	return nil
}
