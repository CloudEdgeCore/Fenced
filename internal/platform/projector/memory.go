// Package projector applies canonical store events to the OpenSearch search
// projection (ADR-013). PostgreSQL remains the source of truth; OpenSearch is
// an eventual, idempotent, tenant-scoped read model. Deletions propagate from
// tombstones: a MemoryTombstoned event deletes the projected document, so
// search results never resurrect a deleted record.
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

// MemoryMapping is the OpenSearch index mapping for projected memory records.
// Vectors stay in the canonical pgvector store; OpenSearch carries the text
// fields for full-text search across records.
var MemoryMapping = []byte(`{
	"settings": { "number_of_shards": 1, "number_of_replicas": 0 },
	"mappings": {
		"properties": {
			"tenantId":     { "type": "keyword" },
			"namespace":    { "type": "keyword" },
			"key":          { "type": "keyword" },
			"contentType":  { "type": "keyword" },
			"content":      { "type": "text" },
			"sensitivity":  { "type": "keyword" },
			"resourceVersion": { "type": "long" },
			"createdAt":    { "type": "date" },
			"updatedAt":    { "type": "date" },
			"tombstoneAt":  { "type": "date" }
		}
	}
}`)

// MemoryEvent is the projection event payload emitted by the canonical store.
type MemoryEvent struct {
	TenantID        string `json:"tenantId"`
	MemoryID        string `json:"memoryId"`
	Namespace       string `json:"namespace"`
	Key             string `json:"key"`
	ResourceVersion int64  `json:"resourceVersion"`
}

// MemoryStore is the read surface the projector needs (the canonical store).
type MemoryStore interface {
	GetMemory(context.Context, string, uuid.UUID) (store.MemoryRecord, error)
}

// SearchIndex is the OpenSearch surface the projector writes to; the
// concrete *opensearch.Client satisfies it.
type SearchIndex interface {
	Get(context.Context, string) ([]byte, error)
	Index(context.Context, string, any) error
	Delete(context.Context, string) error
}

// MemoryProjector applies memory events to the OpenSearch index.
type MemoryProjector struct {
	store  MemoryStore
	search SearchIndex
	now    func() time.Time
}

// NewMemoryProjector wires the projector to the canonical store and the
// OpenSearch index.
func NewMemoryProjector(store MemoryStore, search SearchIndex) *MemoryProjector {
	return &MemoryProjector{store: store, search: search, now: time.Now}
}

// Apply applies one memory event idempotently:
//   - MemoryUpserted fetches the canonical record and indexes it; a stale
//     event (record version <= projected version) is skipped, and a record
//     that no longer exists is dropped.
//   - MemoryTombstoned deletes the projected document (deletion
//     propagation); a missing document is idempotent success.
func (p *MemoryProjector) Apply(ctx context.Context, event store.OutboxEvent) error {
	switch event.EventType {
	case "MemoryUpserted":
		return p.applyUpsert(ctx, event)
	case "MemoryTombstoned":
		return p.applyTombstone(ctx, event)
	default:
		return nil // unrelated event type: the consumer filter already narrowed it
	}
}

func (p *MemoryProjector) applyUpsert(ctx context.Context, event store.OutboxEvent) error {
	payload, err := parseEvent(event)
	if err != nil {
		return err
	}
	memoryID, err := uuid.Parse(payload.MemoryID)
	if err != nil {
		return fmt.Errorf("projection event memory ID is not a UUID: %w", err)
	}
	record, err := p.store.GetMemory(ctx, payload.TenantID, memoryID)
	if errors.Is(err, store.ErrMemoryNotFound) {
		// The record disappeared before projection (e.g. it was never
		// committed or was superseded); drop the event.
		return nil
	}
	if err != nil {
		return fmt.Errorf("fetch canonical memory record: %w", err)
	}
	if record.TombstoneAt != nil {
		// The tombstone already won; propagate the deletion.
		return p.search.Delete(ctx, documentID(payload.TenantID, memoryID))
	}
	stored, err := p.search.Get(ctx, documentID(payload.TenantID, memoryID))
	if err == nil {
		if projected, parseErr := projectedVersion(stored); parseErr == nil && projected >= record.ResourceVersion {
			return nil // already projected at this or a newer version
		}
	} else if !errors.Is(err, opensearch.ErrNotFound) {
		return err
	}
	return p.search.Index(ctx, documentID(payload.TenantID, memoryID), map[string]any{
		"tenantId":        record.TenantID,
		"namespace":       record.Namespace,
		"key":             record.Key,
		"contentType":     record.ContentType,
		"content":         record.Content,
		"sensitivity":     record.Sensitivity,
		"resourceVersion": record.ResourceVersion,
		"createdAt":       record.CreatedAt.UTC().Format(time.RFC3339Nano),
		"updatedAt":       record.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (p *MemoryProjector) applyTombstone(ctx context.Context, event store.OutboxEvent) error {
	payload, err := parseEvent(event)
	if err != nil {
		return err
	}
	memoryID, err := uuid.Parse(payload.MemoryID)
	if err != nil {
		return fmt.Errorf("projection event memory ID is not a UUID: %w", err)
	}
	return p.search.Delete(ctx, documentID(payload.TenantID, memoryID))
}

// documentID scopes projected documents per tenant so cross-tenant isolation
// holds at the index level even though all tenants share one index.
func documentID(tenantID string, memoryID uuid.UUID) string {
	return tenantID + "/" + memoryID.String()
}

func parseEvent(event store.OutboxEvent) (MemoryEvent, error) {
	var payload MemoryEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return payload, fmt.Errorf("decode projection event payload: %w", err)
	}
	if strings.TrimSpace(payload.TenantID) == "" || strings.TrimSpace(payload.MemoryID) == "" || payload.ResourceVersion <= 0 {
		return payload, fmt.Errorf("projection event payload is incomplete")
	}
	return payload, nil
}

// projectedVersion reads the stored document's resource version for
// idempotent skip.
func projectedVersion(document []byte) (int64, error) {
	var stored struct {
		Source struct {
			ResourceVersion int64 `json:"resourceVersion"`
		} `json:"_source"`
	}
	if err := json.Unmarshal(document, &stored); err != nil {
		return 0, err
	}
	if stored.Source.ResourceVersion <= 0 {
		return 0, fmt.Errorf("stored document has no resource version")
	}
	return stored.Source.ResourceVersion, nil
}
