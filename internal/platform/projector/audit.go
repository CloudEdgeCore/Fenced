package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/opensearch"
	"github.com/google/uuid"
)

// AuditMapping is the OpenSearch index mapping for projected audit events
// (ADR-014 projection). The index is append-only: audit records are never
// deleted, so there is no tombstone path.
var AuditMapping = []byte(`{
	"settings": { "number_of_shards": 1, "number_of_replicas": 0 },
	"mappings": {
		"properties": {
			"tenantId":     { "type": "keyword" },
			"seq":          { "type": "long" },
			"eventType":    { "type": "keyword" },
			"resourceType": { "type": "keyword" },
			"resourceId":   { "type": "keyword" },
			"actor":        { "type": "keyword" },
			"details":      { "type": "object", "enabled": false },
			"prevHash":     { "type": "keyword" },
			"chainHash":    { "type": "keyword" },
			"occurredAt":   { "type": "date" }
		}
	}
}`)

// AuditEventPayload is the AuditRecorded outbox payload.
type AuditEventPayload struct {
	TenantID string `json:"tenantId"`
	AuditID  string `json:"auditId"`
	Seq      int64  `json:"seq"`
}

// AuditStore is the canonical ledger read surface the projector needs.
type AuditStore interface {
	GetAuditEvent(context.Context, string, uuid.UUID) (store.AuditEvent, error)
}

// AuditProjector applies AuditRecorded events to the OpenSearch audit index
// (append-only). Application is idempotent by seq: a stale event never
// overwrites a newer projected record.
type AuditProjector struct {
	store  AuditStore
	search SearchIndex
}

// NewAuditProjector wires the audit projector to the canonical ledger and
// the OpenSearch index.
func NewAuditProjector(store AuditStore, search SearchIndex) *AuditProjector {
	return &AuditProjector{store: store, search: search}
}

// Apply applies one audit event idempotently.
func (p *AuditProjector) Apply(ctx context.Context, event store.OutboxEvent) error {
	if event.EventType != "AuditRecorded" {
		return nil
	}
	var payload AuditEventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode audit projection payload: %w", err)
	}
	if strings.TrimSpace(payload.TenantID) == "" || strings.TrimSpace(payload.AuditID) == "" || payload.Seq <= 0 {
		return fmt.Errorf("audit projection payload is incomplete")
	}
	auditID, err := uuid.Parse(payload.AuditID)
	if err != nil {
		return fmt.Errorf("audit projection ID is not a UUID: %w", err)
	}
	record, err := p.store.GetAuditEvent(ctx, payload.TenantID, auditID)
	if errors.Is(err, store.ErrNotFound) {
		// The record disappeared before projection; drop the event.
		return nil
	}
	if err != nil {
		return fmt.Errorf("fetch canonical audit record: %w", err)
	}
	documentID := payload.TenantID + "/" + auditID.String()
	stored, err := p.search.Get(ctx, documentID)
	if err == nil {
		if projected, parseErr := projectedSeq(stored); parseErr == nil && projected >= record.Seq {
			return nil
		}
	} else if !errors.Is(err, opensearch.ErrNotFound) {
		return err
	}
	return p.search.Index(ctx, documentID, map[string]any{
		"tenantId":     record.TenantID,
		"seq":          record.Seq,
		"eventType":    record.EventType,
		"resourceType": record.ResourceType,
		"resourceId":   record.ResourceID.String(),
		"actor":        record.Actor,
		"details":      json.RawMessage(record.Details),
		"prevHash":     fmt.Sprintf("%x", record.PrevHash),
		"chainHash":    fmt.Sprintf("%x", record.ChainHash),
		"occurredAt":   record.OccurredAt.UTC().Format(time.RFC3339Nano),
	})
}

// projectedSeq reads the stored document's seq for idempotent skip.
func projectedSeq(document []byte) (int64, error) {
	var stored struct {
		Source struct {
			Seq int64 `json:"seq"`
		} `json:"_source"`
	}
	if err := json.Unmarshal(document, &stored); err != nil {
		return 0, err
	}
	if stored.Source.Seq <= 0 {
		return 0, fmt.Errorf("stored audit document has no seq")
	}
	return stored.Source.Seq, nil
}
