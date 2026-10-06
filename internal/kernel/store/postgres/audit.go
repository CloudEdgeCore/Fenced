package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ kernelstore.AuditStore = (*Store)(nil)

// insertAudit appends one audit event inside the caller's transaction
// (ADR-014). Appends are serialized per tenant with a transactional advisory
// lock so seq and the hash chain are race-free even on the first event.
// The audit row and its projection outbox event commit atomically with the
// state change.
func insertAudit(ctx context.Context, tx pgx.Tx, tenantID string, eventType, resourceType string, resourceID uuid.UUID, details map[string]any, at time.Time, eventID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, tenantID); err != nil {
		return classify(err)
	}
	prevHash := kernelstore.AuditGenesisHash
	seq := int64(1)
	var lastSeq int64
	var lastChainHash []byte
	err := tx.QueryRow(ctx, `SELECT seq, chain_hash FROM audit_events
		WHERE tenant_id = $1 ORDER BY seq DESC LIMIT 1`, tenantID).Scan(&lastSeq, &lastChainHash)
	if err == nil {
		seq = lastSeq + 1
		copy(prevHash[:], lastChainHash)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return classify(err)
	}
	encodedDetails, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("encode audit details: %w", err)
	}
	event := kernelstore.AuditEvent{
		ID: eventID, TenantID: tenantID, Seq: seq, PrevHash: prevHash,
		EventType: eventType, ResourceType: resourceType, ResourceID: resourceID,
		Actor: "kernel", Details: encodedDetails, OccurredAt: at.UTC(),
	}
	event.ChainHash = event.ComputeChainHash()
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (
		id, tenant_id, seq, prev_hash, chain_hash, event_type, resource_type,
		resource_id, actor, details, occurred_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		event.ID.String(), event.TenantID, event.Seq, event.PrevHash[:], event.ChainHash[:],
		event.EventType, event.ResourceType, event.ResourceID.String(), event.Actor,
		event.Details, event.OccurredAt); err != nil {
		return classify(err)
	}
	// Projection feed (ADR-013 pattern): audit records flow to the search
	// projection over the same outbox pipeline.
	return insertEvent(ctx, tx, tenantID, "Audit", event.ID, event.Seq, "AuditRecorded", map[string]any{
		"tenantId": tenantID, "auditId": event.ID.String(), "seq": event.Seq,
	}, at, uuid.New())
}

// auditHook appends an audit event with a fresh ID, for call sites that do
// not need the returned record.
func auditHook(ctx context.Context, tx pgx.Tx, tenantID string, eventType, resourceType string, resourceID uuid.UUID, details map[string]any, at time.Time) error {
	return insertAudit(ctx, tx, tenantID, eventType, resourceType, resourceID, details, at, uuid.New())
}

func (s *Store) ListAudit(ctx context.Context, in kernelstore.ListAuditInput) ([]kernelstore.AuditEvent, error) {
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, fmt.Errorf("tenant is required")
	}
	if in.Limit <= 0 || in.Limit > 1000 {
		return nil, fmt.Errorf("limit must be between 1 and 1000")
	}
	rows, err := s.pool.Query(ctx, `SELECT id::text, tenant_id, seq, prev_hash, chain_hash,
		event_type, resource_type, resource_id::text, actor, details, occurred_at
		FROM audit_events
		WHERE tenant_id = $1 AND seq > $2
		ORDER BY seq ASC
		LIMIT $3`, in.TenantID, in.AfterSeq, in.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var events []kernelstore.AuditEvent
	for rows.Next() {
		event, scanErr := scanAuditEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return events, nil
}

func (s *Store) GetAuditEvent(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.AuditEvent, error) {
	if strings.TrimSpace(tenantID) == "" || id == uuid.Nil {
		return kernelstore.AuditEvent{}, fmt.Errorf("tenant and audit ID are required")
	}
	event, err := scanAuditEvent(s.pool.QueryRow(ctx, `SELECT id::text, tenant_id, seq, prev_hash, chain_hash,
		event_type, resource_type, resource_id::text, actor, details, occurred_at
		FROM audit_events WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.AuditEvent{}, kernelstore.ErrNotFound
	}
	return event, classify(err)
}

func (s *Store) VerifyAuditChain(ctx context.Context, tenantID string) (kernelstore.AuditVerification, error) {
	if strings.TrimSpace(tenantID) == "" {
		return kernelstore.AuditVerification{}, fmt.Errorf("tenant is required")
	}
	rows, err := s.pool.Query(ctx, `SELECT id::text, tenant_id, seq, prev_hash, chain_hash,
		event_type, resource_type, resource_id::text, actor, details, occurred_at
		FROM audit_events WHERE tenant_id = $1 ORDER BY seq ASC`, tenantID)
	if err != nil {
		return kernelstore.AuditVerification{}, classify(err)
	}
	defer rows.Close()
	verification := kernelstore.AuditVerification{Valid: true}
	expectedPrev := kernelstore.AuditGenesisHash
	for rows.Next() {
		event, scanErr := scanAuditEvent(rows)
		if scanErr != nil {
			return verification, scanErr
		}
		verification.Checked++
		verification.HeadSeq = event.Seq
		verification.HeadHash = event.ChainHash
		if verification.Valid {
			if event.PrevHash != expectedPrev {
				verification.Valid = false
				verification.FirstBrokenSeq = event.Seq
			} else if event.ChainHash != event.ComputeChainHash() {
				verification.Valid = false
				verification.FirstBrokenSeq = event.Seq
			}
		}
		expectedPrev = event.ChainHash
	}
	if err := rows.Err(); err != nil {
		return verification, classify(err)
	}
	if verification.Valid && verification.Checked > 0 {
		return verification, nil
	}
	return verification, nil
}

func (s *Store) ExportAuditChain(ctx context.Context, tenantID string) ([]kernelstore.AuditEvent, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("tenant is required")
	}
	rows, err := s.pool.Query(ctx, `SELECT id::text, tenant_id, seq, prev_hash, chain_hash,
		event_type, resource_type, resource_id::text, actor, details, occurred_at
		FROM audit_events WHERE tenant_id = $1 ORDER BY seq ASC`, tenantID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var events []kernelstore.AuditEvent
	for rows.Next() {
		event, scanErr := scanAuditEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return events, nil
}

func scanAuditEvent(row scanner) (kernelstore.AuditEvent, error) {
	var event kernelstore.AuditEvent
	var id, resourceID string
	var prevHash, chainHash []byte
	var details []byte
	err := row.Scan(&id, &event.TenantID, &event.Seq, &prevHash, &chainHash,
		&event.EventType, &event.ResourceType, &resourceID, &event.Actor, &details, &event.OccurredAt)
	if err != nil {
		return event, err
	}
	if event.ID, err = uuid.Parse(id); err != nil {
		return event, fmt.Errorf("parse audit ID: %w", err)
	}
	if event.ResourceID, err = uuid.Parse(resourceID); err != nil {
		return event, fmt.Errorf("parse audit resource ID: %w", err)
	}
	if len(prevHash) != sha256.Size || len(chainHash) != sha256.Size {
		return event, fmt.Errorf("audit chain hash has unexpected length")
	}
	copy(event.PrevHash[:], prevHash)
	copy(event.ChainHash[:], chainHash)
	if len(details) > 0 {
		event.Details = append(json.RawMessage(nil), details...)
	}
	return event, nil
}
