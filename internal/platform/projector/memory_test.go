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

// fakeMemoryStore serves canonical records for the projector.
type fakeMemoryStore struct {
	records map[uuid.UUID]store.MemoryRecord
}

func (f *fakeMemoryStore) GetMemory(_ context.Context, tenantID string, id uuid.UUID) (store.MemoryRecord, error) {
	record, ok := f.records[id]
	if !ok || record.TenantID != tenantID {
		return store.MemoryRecord{}, store.ErrMemoryNotFound
	}
	return record, nil
}

// recordingSearch records every projection operation.
type recordingSearch struct {
	mu       sync.Mutex
	docs     map[string]map[string]any
	indexed  []string
	deleted  []string
	upserted map[string]int64
}

func (r *recordingSearch) Get(_ context.Context, id string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	doc, ok := r.docs[id]
	if !ok {
		return nil, opensearch.ErrNotFound
	}
	return json.Marshal(map[string]any{"_source": doc})
}

func (r *recordingSearch) Index(_ context.Context, id string, document any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	doc := document.(map[string]any)
	r.docs[id] = doc
	r.indexed = append(r.indexed, id)
	r.upserted[id] = doc["resourceVersion"].(int64)
	return nil
}

func (r *recordingSearch) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.docs, id)
	r.deleted = append(r.deleted, id)
	return nil
}

func memoryEvent(eventType string, tenantID string, id uuid.UUID, version int64) store.OutboxEvent {
	payload, _ := json.Marshal(map[string]any{
		"tenantId": tenantID, "memoryId": id.String(), "namespace": "default",
		"key": "k", "resourceVersion": version,
	})
	return store.OutboxEvent{
		ID: uuid.New(), TenantID: tenantID, AggregateType: "Memory", AggregateID: id,
		AggregateVersion: version, EventType: eventType, Payload: payload,
		OccurredAt: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestProjectorIndexesUpsertAndDeletesTombstone(t *testing.T) {
	records := &fakeMemoryStore{records: map[uuid.UUID]store.MemoryRecord{}}
	search := &recordingSearch{docs: map[string]map[string]any{}, upserted: map[string]int64{}}
	projector := NewMemoryProjector(records, search)
	ctx := context.Background()

	id := uuid.New()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	records.records[id] = store.MemoryRecord{
		ID: id, TenantID: "tenant-a", Namespace: "default", Key: "notes",
		ContentType: "text/markdown", Content: "hello world", Sensitivity: "internal",
		ResourceVersion: 4, CreatedAt: now, UpdatedAt: now,
	}

	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", id, 4)); err != nil {
		t.Fatalf("apply upsert: %v", err)
	}
	docID := "tenant-a/" + id.String()
	if len(search.indexed) != 1 || search.indexed[0] != docID {
		t.Fatalf("indexed = %v, want [%s]", search.indexed, docID)
	}
	if search.docs[docID]["content"] != "hello world" || search.docs[docID]["resourceVersion"] != int64(4) {
		t.Fatalf("projected document = %+v", search.docs[docID])
	}

	// The tombstone event propagates the deletion.
	if err := projector.Apply(ctx, memoryEvent("MemoryTombstoned", "tenant-a", id, 5)); err != nil {
		t.Fatalf("apply tombstone: %v", err)
	}
	if len(search.deleted) != 1 || search.deleted[0] != docID {
		t.Fatalf("deleted = %v, want [%s]", search.deleted, docID)
	}
	if _, ok := search.docs[docID]; ok {
		t.Fatal("document survived the tombstone")
	}
}

func TestProjectorSkipsStaleUpserts(t *testing.T) {
	records := &fakeMemoryStore{records: map[uuid.UUID]store.MemoryRecord{}}
	search := &recordingSearch{docs: map[string]map[string]any{}, upserted: map[string]int64{}}
	projector := NewMemoryProjector(records, search)
	ctx := context.Background()

	id := uuid.New()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	records.records[id] = store.MemoryRecord{
		ID: id, TenantID: "tenant-a", Namespace: "default", Key: "notes",
		ContentType: "text/markdown", Content: "v7", Sensitivity: "internal",
		ResourceVersion: 7, CreatedAt: now, UpdatedAt: now,
	}
	// Apply v7 first, then a redelivered v4: the stale event must not
	// overwrite the newer projection.
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", id, 7)); err != nil {
		t.Fatalf("apply v7: %v", err)
	}
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", id, 4)); err != nil {
		t.Fatalf("apply stale v4: %v", err)
	}
	if search.upserted["tenant-a/"+id.String()] != 7 {
		t.Fatalf("projected version = %d, want 7 (stale event overwrote it)", search.upserted["tenant-a/"+id.String()])
	}
}

func TestProjectorDropsMissingRecordsAndUnknownEvents(t *testing.T) {
	records := &fakeMemoryStore{records: map[uuid.UUID]store.MemoryRecord{}}
	search := &recordingSearch{docs: map[string]map[string]any{}, upserted: map[string]int64{}}
	projector := NewMemoryProjector(records, search)
	ctx := context.Background()

	// Upsert for a record that does not exist: dropped silently.
	ghost := uuid.New()
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", ghost, 1)); err != nil {
		t.Fatalf("apply ghost upsert: %v", err)
	}
	if len(search.indexed) != 0 {
		t.Fatalf("ghost upsert indexed %d documents", len(search.indexed))
	}
	// Unknown event types are ignored.
	if err := projector.Apply(ctx, memoryEvent("SomethingElse", "tenant-a", ghost, 1)); err != nil {
		t.Fatalf("apply unknown event: %v", err)
	}
	// Tombstone of a never-indexed document is idempotent.
	if err := projector.Apply(ctx, memoryEvent("MemoryTombstoned", "tenant-a", ghost, 1)); err != nil {
		t.Fatalf("apply tombstone of missing document: %v", err)
	}
}

func TestProjectorTombstonedRecordDeletesOnUpsert(t *testing.T) {
	records := &fakeMemoryStore{records: map[uuid.UUID]store.MemoryRecord{}}
	search := &recordingSearch{docs: map[string]map[string]any{}, upserted: map[string]int64{}}
	projector := NewMemoryProjector(records, search)
	ctx := context.Background()

	id := uuid.New()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	tombstone := now.Add(time.Hour)
	records.records[id] = store.MemoryRecord{
		ID: id, TenantID: "tenant-a", Namespace: "default", Key: "notes",
		ContentType: "text/markdown", Content: "gone", Sensitivity: "internal",
		ResourceVersion: 6, CreatedAt: now, UpdatedAt: tombstone, TombstoneAt: &tombstone,
	}
	// A delayed upsert event arriving after the tombstone propagates the
	// deletion instead of resurrecting the document.
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", id, 6)); err != nil {
		t.Fatalf("apply upsert of tombstoned record: %v", err)
	}
	if len(search.deleted) != 1 || len(search.indexed) != 0 {
		t.Fatalf("deleted = %v indexed = %v, want deletion only", search.deleted, search.indexed)
	}
}

func TestProjectorTenantIsolation(t *testing.T) {
	records := &fakeMemoryStore{records: map[uuid.UUID]store.MemoryRecord{}}
	search := &recordingSearch{docs: map[string]map[string]any{}, upserted: map[string]int64{}}
	projector := NewMemoryProjector(records, search)
	ctx := context.Background()

	id := uuid.New()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	records.records[id] = store.MemoryRecord{
		ID: id, TenantID: "tenant-a", Namespace: "default", Key: "notes",
		ContentType: "text/markdown", Content: "secret-a", Sensitivity: "restricted",
		ResourceVersion: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-a", id, 1)); err != nil {
		t.Fatalf("apply upsert: %v", err)
	}
	// The same record ID under another tenant must not resolve: the store
	// enforces the tenant boundary, so the upsert is dropped.
	if err := projector.Apply(ctx, memoryEvent("MemoryUpserted", "tenant-b", id, 1)); err != nil {
		t.Fatalf("apply cross-tenant upsert: %v", err)
	}
	if len(search.indexed) != 1 || search.indexed[0] != "tenant-a/"+id.String() {
		t.Fatalf("indexed = %v, want only the tenant-a document", search.indexed)
	}
}
