package projector

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/opensearch"
	"github.com/google/uuid"
)

// fakeAuditStore serves canonical audit records.
type fakeAuditStore struct {
	records map[uuid.UUID]store.AuditEvent
}

func (f *fakeAuditStore) GetAuditEvent(_ context.Context, tenantID string, id uuid.UUID) (store.AuditEvent, error) {
	record, ok := f.records[id]
	if !ok || record.TenantID != tenantID {
		return store.AuditEvent{}, store.ErrNotFound
	}
	return record, nil
}

// recordingAuditSearch records every projection operation.
type recordingAuditSearch struct {
	mu      sync.Mutex
	docs    map[string]map[string]any
	indexed []string
	seqByID map[string]int64
}

func (r *recordingAuditSearch) Get(_ context.Context, id string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	doc, ok := r.docs[id]
	if !ok {
		return nil, opensearch.ErrNotFound
	}
	return json.Marshal(map[string]any{"_source": doc})
}

func (r *recordingAuditSearch) Index(_ context.Context, id string, document any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	doc := document.(map[string]any)
	r.docs[id] = doc
	r.indexed = append(r.indexed, id)
	r.seqByID[id] = doc["seq"].(int64)
	return nil
}

func (r *recordingAuditSearch) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.docs, id)
	return nil
}

func auditRecordedEvent(tenantID string, id uuid.UUID, seq int64) store.OutboxEvent {
	payload, _ := json.Marshal(map[string]any{"tenantId": tenantID, "auditId": id.String(), "seq": seq})
	return store.OutboxEvent{
		ID: uuid.New(), TenantID: tenantID, AggregateType: "Audit", AggregateID: id,
		AggregateVersion: seq, EventType: "AuditRecorded", Payload: payload,
		OccurredAt: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestAuditProjectorIndexesAppendOnly(t *testing.T) {
	records := &fakeAuditStore{records: map[uuid.UUID]store.AuditEvent{}}
	search := &recordingAuditSearch{docs: map[string]map[string]any{}, seqByID: map[string]int64{}}
	projector := NewAuditProjector(records, search)
	ctx := context.Background()

	id := uuid.New()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	records.records[id] = store.AuditEvent{
		ID: id, TenantID: "tenant-a", Seq: 7, PrevHash: [32]byte{1}, ChainHash: [32]byte{2},
		EventType: "task.queued", ResourceType: "Task", ResourceID: uuid.New(),
		Actor: "kernel", Details: json.RawMessage(`{"namespace":"default"}`), OccurredAt: now,
	}
	if err := projector.Apply(ctx, auditRecordedEvent("tenant-a", id, 7)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	docID := "tenant-a/" + id.String()
	if len(search.indexed) != 1 || search.indexed[0] != docID {
		t.Fatalf("indexed = %v, want [%s]", search.indexed, docID)
	}
	doc := search.docs[docID]
	if doc["eventType"] != "task.queued" || doc["seq"] != int64(7) || doc["actor"] != "kernel" {
		t.Fatalf("projected document = %+v", doc)
	}
	if doc["prevHash"] != "0100000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("prevHash not projected as hex: %v", doc["prevHash"])
	}

	// A stale redelivery is skipped.
	if err := projector.Apply(ctx, auditRecordedEvent("tenant-a", id, 4)); err != nil {
		t.Fatalf("apply stale: %v", err)
	}
	if search.seqByID[docID] != 7 {
		t.Fatalf("stale event overwrote seq: %d", search.seqByID[docID])
	}
	if len(search.indexed) != 1 {
		t.Fatalf("stale event indexed again: %d", len(search.indexed))
	}
}

func TestAuditProjectorDropsMissingAndForeignEvents(t *testing.T) {
	records := &fakeAuditStore{records: map[uuid.UUID]store.AuditEvent{}}
	search := &recordingAuditSearch{docs: map[string]map[string]any{}, seqByID: map[string]int64{}}
	projector := NewAuditProjector(records, search)
	ctx := context.Background()

	ghost := uuid.New()
	if err := projector.Apply(ctx, auditRecordedEvent("tenant-a", ghost, 1)); err != nil {
		t.Fatalf("apply ghost: %v", err)
	}
	if len(search.indexed) != 0 {
		t.Fatalf("ghost record was indexed")
	}
	// Non-audit events are ignored.
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", ghost, 1)); err != nil {
		t.Fatalf("apply foreign event: %v", err)
	}
	if len(search.indexed) != 0 {
		t.Fatalf("foreign event was indexed")
	}
	// Cross-tenant reads never resolve: the store enforces the boundary.
	id := uuid.New()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	records.records[id] = store.AuditEvent{
		ID: id, TenantID: "tenant-a", Seq: 1, PrevHash: [32]byte{1}, ChainHash: [32]byte{2},
		EventType: "task.queued", ResourceType: "Task", ResourceID: uuid.New(),
		Actor: "kernel", Details: json.RawMessage(`{}`), OccurredAt: now,
	}
	if err := projector.Apply(ctx, auditRecordedEvent("tenant-b", id, 1)); err != nil {
		t.Fatalf("apply cross-tenant: %v", err)
	}
	if len(search.indexed) != 0 {
		t.Fatalf("cross-tenant audit record was indexed")
	}
}
