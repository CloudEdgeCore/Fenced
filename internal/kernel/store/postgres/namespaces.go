package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ namespace.Store = (*Store)(nil)

// CreateNamespace inserts a new namespace record.
func (s *Store) CreateNamespace(ctx context.Context, ns *namespace.Namespace) error {
	if ns == nil {
		return namespace.ErrInvalidNamespaceName
	}
	tenantID := strings.TrimSpace(ns.TenantID)
	name := strings.TrimSpace(ns.Name)
	if tenantID == "" {
		return namespace.ErrInvalidNamespaceName
	}
	if err := namespace.ValidateNamespaceName(name); err != nil {
		return err
	}

	labelsBytes, err := json.Marshal(ns.Labels)
	if err != nil {
		return fmt.Errorf("marshal namespace labels: %w", err)
	}
	quotaBytes, err := json.Marshal(ns.Quota)
	if err != nil {
		return fmt.Errorf("marshal namespace quota: %w", err)
	}

	now := s.clock()
	if ns.CreatedAt.IsZero() {
		ns.CreatedAt = now
	}
	ns.UpdatedAt = now
	phase := ns.Phase
	if phase == "" {
		phase = namespace.NamespacePhaseActive
	}

	const query = `
		INSERT INTO namespaces (tenant_id, name, display_name, description, phase, labels, quota, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err = s.pool.Exec(ctx, query,
		tenantID, name, ns.DisplayName, ns.Description, string(phase),
		labelsBytes, quotaBytes, ns.CreatedAt, ns.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return namespace.ErrNamespaceAlreadyExists
		}
		return err
	}

	// Initialize usage record
	const usageQuery = `
		INSERT INTO namespace_usage (tenant_id, namespace, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, namespace) DO NOTHING
	`
	_, _ = s.pool.Exec(ctx, usageQuery, tenantID, name, now)

	return nil
}

// ensureDefaultPG ensures that the default namespace exists in PostgreSQL.
func (s *Store) ensureDefaultPG(ctx context.Context, tenantID string) error {
	def := namespace.NewDefaultNamespace(tenantID)
	labelsBytes, _ := json.Marshal(def.Labels)
	quotaBytes, _ := json.Marshal(def.Quota)
	now := s.clock()

	const query = `
		INSERT INTO namespaces (tenant_id, name, display_name, description, phase, labels, quota, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_id, name) DO NOTHING
	`
	_, err := s.pool.Exec(ctx, query,
		tenantID, namespace.DefaultNamespace, def.DisplayName, def.Description, string(def.Phase),
		labelsBytes, quotaBytes, now, now,
	)
	if err != nil {
		return err
	}

	const usageQuery = `
		INSERT INTO namespace_usage (tenant_id, namespace, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, namespace) DO NOTHING
	`
	_, err = s.pool.Exec(ctx, usageQuery, tenantID, namespace.DefaultNamespace, now)
	return err
}

// GetNamespace retrieves a namespace by tenant and name.
func (s *Store) GetNamespace(ctx context.Context, tenantID, name string) (*namespace.Namespace, error) {
	tenantID = strings.TrimSpace(tenantID)
	name = strings.TrimSpace(name)
	if tenantID == "" || name == "" {
		return nil, namespace.ErrNamespaceNotFound
	}

	if name == namespace.DefaultNamespace {
		_ = s.ensureDefaultPG(ctx, tenantID)
	}

	const query = `
		SELECT tenant_id, name, display_name, description, phase, labels, quota, created_at, updated_at
		FROM namespaces
		WHERE tenant_id = $1 AND name = $2
	`
	row := s.pool.QueryRow(ctx, query, tenantID, name)

	var ns namespace.Namespace
	var phase string
	var labelsBytes, quotaBytes []byte
	err := row.Scan(
		&ns.TenantID, &ns.Name, &ns.DisplayName, &ns.Description,
		&phase, &labelsBytes, &quotaBytes, &ns.CreatedAt, &ns.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, namespace.ErrNamespaceNotFound
		}
		return nil, err
	}

	ns.Phase = namespace.NamespacePhase(phase)
	if len(labelsBytes) > 0 {
		_ = json.Unmarshal(labelsBytes, &ns.Labels)
	}
	if len(quotaBytes) > 0 {
		_ = json.Unmarshal(quotaBytes, &ns.Quota)
	}

	return &ns, nil
}

// UpdateNamespace updates the metadata, labels, and quota of an existing namespace.
func (s *Store) UpdateNamespace(ctx context.Context, ns *namespace.Namespace) error {
	if ns == nil {
		return namespace.ErrNamespaceNotFound
	}
	tenantID := strings.TrimSpace(ns.TenantID)
	name := strings.TrimSpace(ns.Name)
	if tenantID == "" || name == "" {
		return namespace.ErrNamespaceNotFound
	}

	labelsBytes, err := json.Marshal(ns.Labels)
	if err != nil {
		return fmt.Errorf("marshal namespace labels: %w", err)
	}
	quotaBytes, err := json.Marshal(ns.Quota)
	if err != nil {
		return fmt.Errorf("marshal namespace quota: %w", err)
	}

	now := s.clock()
	phase := ns.Phase
	if phase == "" {
		phase = namespace.NamespacePhaseActive
	}

	const query = `
		UPDATE namespaces
		SET display_name = $1, description = $2, phase = $3, labels = $4, quota = $5, updated_at = $6
		WHERE tenant_id = $7 AND name = $8
	`
	tag, err := s.pool.Exec(ctx, query,
		ns.DisplayName, ns.Description, string(phase),
		labelsBytes, quotaBytes, now, tenantID, name,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return namespace.ErrNamespaceNotFound
	}

	return nil
}

// DeleteNamespace removes a namespace if it is not "default" and has no active resources.
func (s *Store) DeleteNamespace(ctx context.Context, tenantID, name string) error {
	tenantID = strings.TrimSpace(tenantID)
	name = strings.TrimSpace(name)
	if name == namespace.DefaultNamespace {
		return namespace.ErrDefaultNamespaceProtected
	}

	// Check usage
	usage, err := s.GetUsage(ctx, tenantID, name)
	if err == nil {
		if usage.ActiveServices > 0 || usage.ActiveInstances > 0 || usage.ActiveTasks > 0 || usage.MailboxMessages > 0 {
			return namespace.ErrNamespaceNotEmpty
		}
	}

	const deleteUsage = `DELETE FROM namespace_usage WHERE tenant_id = $1 AND namespace = $2`
	_, _ = s.pool.Exec(ctx, deleteUsage, tenantID, name)

	const query = `DELETE FROM namespaces WHERE tenant_id = $1 AND name = $2`
	tag, err := s.pool.Exec(ctx, query, tenantID, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return namespace.ErrNamespaceNotFound
	}

	return nil
}

// ListNamespaces lists all namespaces within a tenant.
func (s *Store) ListNamespaces(ctx context.Context, tenantID string) ([]*namespace.Namespace, error) {
	tenantID = strings.TrimSpace(tenantID)
	_ = s.ensureDefaultPG(ctx, tenantID)

	const query = `
		SELECT tenant_id, name, display_name, description, phase, labels, quota, created_at, updated_at
		FROM namespaces
		WHERE tenant_id = $1
		ORDER BY name ASC
	`
	rows, err := s.pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*namespace.Namespace
	for rows.Next() {
		var ns namespace.Namespace
		var phase string
		var labelsBytes, quotaBytes []byte
		err := rows.Scan(
			&ns.TenantID, &ns.Name, &ns.DisplayName, &ns.Description,
			&phase, &labelsBytes, &quotaBytes, &ns.CreatedAt, &ns.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		ns.Phase = namespace.NamespacePhase(phase)
		if len(labelsBytes) > 0 {
			_ = json.Unmarshal(labelsBytes, &ns.Labels)
		}
		if len(quotaBytes) > 0 {
			_ = json.Unmarshal(quotaBytes, &ns.Quota)
		}
		result = append(result, &ns)
	}

	return result, rows.Err()
}

// GetUsage returns the current active and settled resource usage in the namespace.
func (s *Store) GetUsage(ctx context.Context, tenantID, name string) (*namespace.ResourceUsage, error) {
	tenantID = strings.TrimSpace(tenantID)
	name = strings.TrimSpace(name)

	const query = `
		SELECT tenant_id, namespace, active_services, active_instances, active_tasks, mailbox_messages,
		       consumed_tokens, consumed_cost_micro_usd, consumed_tool_calls, consumed_wall_seconds, updated_at
		FROM namespace_usage
		WHERE tenant_id = $1 AND namespace = $2
	`
	row := s.pool.QueryRow(ctx, query, tenantID, name)

	var u namespace.ResourceUsage
	err := row.Scan(
		&u.TenantID, &u.Namespace, &u.ActiveServices, &u.ActiveInstances, &u.ActiveTasks, &u.MailboxMessages,
		&u.ConsumedTokens, &u.ConsumedCostMicroUSD, &u.ConsumedToolCalls, &u.ConsumedWallSeconds, &u.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &namespace.ResourceUsage{
				TenantID:  tenantID,
				Namespace: name,
				UpdatedAt: s.clock(),
			}, nil
		}
		return nil, err
	}
	return &u, nil
}

// RecordUsageDelta applies incremental changes to resource usage counters.
func (s *Store) RecordUsageDelta(ctx context.Context, tenantID, name string, delta namespace.ResourceUsageDelta) error {
	tenantID = strings.TrimSpace(tenantID)
	name = strings.TrimSpace(name)
	now := s.clock()

	const query = `
		INSERT INTO namespace_usage (
			tenant_id, namespace, active_services, active_instances, active_tasks, mailbox_messages,
			consumed_tokens, consumed_cost_micro_usd, consumed_tool_calls, consumed_wall_seconds, updated_at
		)
		VALUES ($1, $2, GREATEST(0, $3), GREATEST(0, $4), GREATEST(0, $5), GREATEST(0, $6), GREATEST(0, $7), GREATEST(0, $8), GREATEST(0, $9), GREATEST(0, $10), $11)
		ON CONFLICT (tenant_id, namespace) DO UPDATE SET
			active_services = GREATEST(0, namespace_usage.active_services + EXCLUDED.active_services),
			active_instances = GREATEST(0, namespace_usage.active_instances + EXCLUDED.active_instances),
			active_tasks = GREATEST(0, namespace_usage.active_tasks + EXCLUDED.active_tasks),
			mailbox_messages = GREATEST(0, namespace_usage.mailbox_messages + EXCLUDED.mailbox_messages),
			consumed_tokens = GREATEST(0, namespace_usage.consumed_tokens + EXCLUDED.consumed_tokens),
			consumed_cost_micro_usd = GREATEST(0, namespace_usage.consumed_cost_micro_usd + EXCLUDED.consumed_cost_micro_usd),
			consumed_tool_calls = GREATEST(0, namespace_usage.consumed_tool_calls + EXCLUDED.consumed_tool_calls),
			consumed_wall_seconds = GREATEST(0, namespace_usage.consumed_wall_seconds + EXCLUDED.consumed_wall_seconds),
			updated_at = EXCLUDED.updated_at
	`
	_, err := s.pool.Exec(ctx, query,
		tenantID, name,
		delta.ActiveServicesDelta, delta.ActiveInstancesDelta, delta.ActiveTasksDelta, delta.MailboxMessagesDelta,
		delta.ConsumedTokensDelta, delta.ConsumedCostMicroUSDDelta, delta.ConsumedToolCallsDelta, delta.ConsumedWallSecondsDelta,
		now,
	)
	return err
}
