package postgres

import (
	"context"
	"fmt"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

var _ kernelstore.PoolHealthStore = (*Store)(nil)

// PoolInstanceHealth derives runtime instance liveness from lease heartbeats
// (v0.6). An instance is presumed dead while it holds an unreleased lease
// that is stale: expired (expires_at <= now) or not renewed within the
// freshness window (heartbeat_at <= now - freshness). Released leases (normal
// completion or recovery takeover) and absent leases (idle workers) never
// mark an instance unhealthy, so recovery naturally heals a pool: the moment
// the stale lease is released, the pool is schedulable again.
func (s *Store) PoolInstanceHealth(ctx context.Context, instanceIDs []string, now time.Time, freshness time.Duration) (map[string]bool, error) {
	health := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		health[id] = true
	}
	if len(instanceIDs) == 0 {
		return health, nil
	}
	if freshness <= 0 {
		return nil, fmt.Errorf("positive lease freshness window is required")
	}
	now = now.UTC()
	staleBefore := now.Add(-freshness)
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT a.runtime_instance_id
		FROM runtime_leases l
		JOIN attempts a ON a.id = l.attempt_id
		WHERE a.runtime_instance_id = ANY($1) AND l.released_at IS NULL
		  AND (l.expires_at <= $2 OR l.heartbeat_at <= $3)`, instanceIDs, now, staleBefore)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	for rows.Next() {
		var instanceID string
		if err := rows.Scan(&instanceID); err != nil {
			return nil, classify(err)
		}
		health[instanceID] = false
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return health, nil
}
