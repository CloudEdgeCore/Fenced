package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// RegisterRuntimePools upserts development/bootstrap pool declarations into
// the durable registry. Tenant grants are replaced per named pool atomically;
// unrelated pools are never deleted.
func (s *Store) RegisterRuntimePools(ctx context.Context, pools []scheduler.RuntimePool) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	now := s.now()
	seen := map[string]struct{}{}
	for _, pool := range pools {
		if strings.TrimSpace(pool.ID) == "" || strings.TrimSpace(pool.RuntimeClass) == "" ||
			strings.TrimSpace(pool.RuntimeInstanceID) == "" || strings.TrimSpace(pool.Region) == "" ||
			len(pool.TenantIDs) == 0 || pool.AvailableCPU < 0 || pool.AvailableMemory < 0 || pool.AvailableLLMSlots < 0 ||
			pool.CostWeight < 0 || math.IsNaN(pool.CostWeight) || math.IsInf(pool.CostWeight, 0) {
			return fmt.Errorf("runtime pool %q is invalid", pool.ID)
		}
		if _, duplicate := seen[pool.ID]; duplicate {
			return fmt.Errorf("duplicate runtime pool %q", pool.ID)
		}
		seen[pool.ID] = struct{}{}
		status := strings.ToUpper(strings.TrimSpace(pool.Status))
		if status == "" {
			status = "ACTIVE"
		}
		if status != "ACTIVE" && status != "CORDONED" && status != "DRAINING" {
			return fmt.Errorf("runtime pool %q status is invalid", pool.ID)
		}
		regions, err := json.Marshal(pool.ArtifactRegions)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runtime_pools (
			id,runtime_class,runtime_instance_id,region,data_residency,ready,status,failure_domain,
			available_cpu_millis,available_memory_mib,available_llm_slots,artifact_regions,cost_weight,created_at,updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14)
			ON CONFLICT (id) DO UPDATE SET runtime_class=EXCLUDED.runtime_class,
				runtime_instance_id=EXCLUDED.runtime_instance_id,region=EXCLUDED.region,
				data_residency=EXCLUDED.data_residency,ready=EXCLUDED.ready,status=EXCLUDED.status,
				failure_domain=EXCLUDED.failure_domain,available_cpu_millis=EXCLUDED.available_cpu_millis,
				available_memory_mib=EXCLUDED.available_memory_mib,available_llm_slots=EXCLUDED.available_llm_slots,
				artifact_regions=EXCLUDED.artifact_regions,cost_weight=EXCLUDED.cost_weight,
				resource_version=runtime_pools.resource_version+1,updated_at=EXCLUDED.updated_at`,
			pool.ID, pool.RuntimeClass, pool.RuntimeInstanceID, pool.Region, pool.DataResidency, pool.Ready,
			status, pool.FailureDomain, pool.AvailableCPU, pool.AvailableMemory, pool.AvailableLLMSlots,
			regions, pool.CostWeight, now); err != nil {
			return classify(err)
		}
		// The registry is the single authoritative writer of pool total
		// capacity. Scheduling never creates or rescales these rows; it only
		// moves reserved_*. Shrinking below the active reservation of a live
		// pool is rejected so operators cannot silently oversubscribe it.
		command, err := tx.Exec(ctx, `INSERT INTO runtime_pool_capacities (
			pool_id, total_cpu_millis, total_memory_mib, total_llm_slots, updated_at)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (pool_id) DO UPDATE SET
				total_cpu_millis = EXCLUDED.total_cpu_millis,
				total_memory_mib = EXCLUDED.total_memory_mib,
				total_llm_slots = EXCLUDED.total_llm_slots,
				resource_version = runtime_pool_capacities.resource_version + 1,
				updated_at = EXCLUDED.updated_at
			WHERE runtime_pool_capacities.reserved_cpu_millis <= EXCLUDED.total_cpu_millis
			  AND runtime_pool_capacities.reserved_memory_mib <= EXCLUDED.total_memory_mib
			  AND runtime_pool_capacities.reserved_llm_slots <= EXCLUDED.total_llm_slots`,
			pool.ID, pool.AvailableCPU, pool.AvailableMemory, pool.AvailableLLMSlots, now)
		if err != nil {
			return classify(err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: runtime pool %q cannot shrink below active reservations",
				kernelstore.ErrCapacityExhausted, pool.ID)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM runtime_pool_tenant_grants WHERE pool_id=$1`, pool.ID); err != nil {
			return classify(err)
		}
		tenants := map[string]struct{}{}
		for _, tenantID := range pool.TenantIDs {
			if strings.TrimSpace(tenantID) == "" {
				return fmt.Errorf("runtime pool %q has an empty tenant grant", pool.ID)
			}
			if _, duplicate := tenants[tenantID]; duplicate {
				continue
			}
			tenants[tenantID] = struct{}{}
			if _, err := tx.Exec(ctx, `INSERT INTO runtime_pool_tenant_grants(pool_id,tenant_id,created_at)
				VALUES($1,$2,$3)`, pool.ID, tenantID, now); err != nil {
				return classify(err)
			}
		}
		// Operator grants are replaced per pool exactly like tenant grants,
		// but they are a separate authority: operator subjects never derive
		// from tenant grants and vice versa.
		if _, err := tx.Exec(ctx, `DELETE FROM runtime_pool_operator_grants WHERE pool_id=$1`, pool.ID); err != nil {
			return classify(err)
		}
		operators := map[string]struct{}{}
		for _, subject := range pool.Operators {
			if strings.TrimSpace(subject) == "" {
				return fmt.Errorf("runtime pool %q has an empty operator grant", pool.ID)
			}
			if _, duplicate := operators[subject]; duplicate {
				continue
			}
			operators[subject] = struct{}{}
			if _, err := tx.Exec(ctx, `INSERT INTO runtime_pool_operator_grants(pool_id,subject,created_at)
				VALUES($1,$2,$3)`, pool.ID, subject, now); err != nil {
				return classify(err)
			}
		}
	}
	return classify(tx.Commit(ctx))
}

// UpdateRuntimePoolStatus performs an operator-authorized, CAS-guarded
// status transition. Cordon/drain affect new placement only; existing
// leases remain recoverable until their normal terminal path. The caller's
// subject must hold an operator grant for the pool: a tenant usage grant —
// which only controls scheduler visibility — is never sufficient, so one
// tenant of a shared pool cannot operate it for its peers. The deciding
// operator subject is recorded in the audit chain.
func (s *Store) UpdateRuntimePoolStatus(ctx context.Context, in kernelstore.UpdateRuntimePoolStatusInput) (kernelstore.RuntimePoolState, error) {
	var state kernelstore.RuntimePoolState
	status := strings.ToUpper(strings.TrimSpace(in.Status))
	if strings.TrimSpace(in.TenantID) == "" || strings.TrimSpace(in.PoolID) == "" || strings.TrimSpace(in.OperatorSubject) == "" ||
		in.ExpectedVersion <= 0 || (status != "ACTIVE" && status != "CORDONED" && status != "DRAINING") {
		return state, fmt.Errorf("tenant, pool, operator subject, positive expected version, and ACTIVE|CORDONED|DRAINING status are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return state, err
	}
	defer rollback(ctx, tx)
	now := s.now()
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM runtime_pools p WHERE p.id = $1 AND EXISTS(
				SELECT 1 FROM runtime_pool_tenant_grants g WHERE g.pool_id = p.id AND g.tenant_id = $2))`,
		in.PoolID, in.TenantID).Scan(&exists); err != nil {
		return state, classify(err)
	}
	if !exists {
		return state, fmt.Errorf("%w: runtime pool %q is not visible in tenant %q",
			kernelstore.ErrNotFound, in.PoolID, in.TenantID)
	}
	var operatorGranted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM runtime_pool_operator_grants WHERE pool_id = $1 AND subject = $2)`,
		in.PoolID, in.OperatorSubject).Scan(&operatorGranted); err != nil {
		return state, classify(err)
	}
	if !operatorGranted {
		return state, fmt.Errorf("%w: subject %q cannot operate runtime pool %q",
			kernelstore.ErrPoolOperatorDenied, in.OperatorSubject, in.PoolID)
	}
	err = tx.QueryRow(ctx, `UPDATE runtime_pools p SET status=$1,resource_version=p.resource_version+1,updated_at=$2
		WHERE p.id=$3 AND p.resource_version=$4
		RETURNING p.id,p.status,p.resource_version`, status, now, in.PoolID, in.ExpectedVersion).
		Scan(&state.ID, &state.Status, &state.ResourceVersion)
	if err != nil {
		return state, classify(err)
	}
	// The pool id is a string; derive a stable UUID so the audit chain's
	// resource reference stays typed.
	if err := auditHook(ctx, tx, in.TenantID, "runtime_pool.status", "RuntimePool",
		uuid.NewMD5(uuid.NameSpaceOID, []byte(in.PoolID)), map[string]any{
			"poolId": in.PoolID, "status": status, "operatorSubject": in.OperatorSubject,
		}, now); err != nil {
		return state, err
	}
	return state, classify(tx.Commit(ctx))
}

// ListRuntimePools resolves tenant authorization dynamically from the
// registry and never accepts an implicit/global tenant grant.
//
// The returned Available* fields are effective capacity: declared totals
// minus the pool's durable active reservation ledger, clamped at zero. A
// pool whose capacity ledger row was never registered by the operator is
// flagged CapacityLedgerMissing and fails placement closed.
func (s *Store) ListRuntimePools(ctx context.Context, tenantID string) ([]scheduler.RuntimePool, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("tenant is required")
	}
	rows, err := s.pool.Query(ctx, `SELECT p.id,p.runtime_class,p.runtime_instance_id,p.region,p.data_residency,
			p.ready,p.status,p.failure_domain,
			GREATEST(p.available_cpu_millis - COALESCE(c.reserved_cpu_millis, 0), 0),
			GREATEST(p.available_memory_mib - COALESCE(c.reserved_memory_mib, 0), 0),
			GREATEST(p.available_llm_slots - COALESCE(c.reserved_llm_slots, 0), 0),
			(c.pool_id IS NULL),p.artifact_regions,p.cost_weight
			FROM runtime_pools p JOIN runtime_pool_tenant_grants g ON g.pool_id=p.id
			LEFT JOIN runtime_pool_capacities c ON c.pool_id=p.id
			WHERE g.tenant_id=$1 ORDER BY p.id`, tenantID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var pools []scheduler.RuntimePool
	for rows.Next() {
		var pool scheduler.RuntimePool
		var regions []byte
		if err := rows.Scan(&pool.ID, &pool.RuntimeClass, &pool.RuntimeInstanceID, &pool.Region,
			&pool.DataResidency, &pool.Ready, &pool.Status, &pool.FailureDomain, &pool.AvailableCPU,
			&pool.AvailableMemory, &pool.AvailableLLMSlots, &pool.CapacityLedgerMissing, &regions, &pool.CostWeight); err != nil {
			return nil, classify(err)
		}
		if err := json.Unmarshal(regions, &pool.ArtifactRegions); err != nil {
			return nil, fmt.Errorf("decode runtime pool %q artifact regions: %w", pool.ID, err)
		}
		pool.TenantIDs = []string{tenantID}
		pools = append(pools, pool)
	}
	return pools, classify(rows.Err())
}
