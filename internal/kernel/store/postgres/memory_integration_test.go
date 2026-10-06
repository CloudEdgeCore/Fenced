//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func testEmbedding(seed float32) []float32 {
	values := make([]float32, kernelstore.MemoryEmbeddingDimension)
	values[0] = seed
	values[1] = 1 - seed
	return values
}

// TestMemoryCanonicalStoreLifecycle drives the canonical store semantics
// (ADR-009) against real PostgreSQL: idempotent writes, corrections as new
// versions, tenant isolation, FTS/trigram/vector retrieval, tombstones and
// retention.
func TestMemoryCanonicalStoreLifecycle(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	put := func(key, content string, embedding []float32) (kernelstore.MemoryRecord, bool, error) {
		return repository.PutMemory(ctx, kernelstore.PutMemoryInput{
			TenantID: "tenant-a", Namespace: "default", Key: key,
			ContentType: "text/plain", Content: content,
			Embedding: embedding, EmbeddingProvider: "test",
			Sensitivity: "internal",
		})
	}

	created, existing, err := put("k1", "the quick brown fox jumps over the lazy dog", testEmbedding(0.9))
	if err != nil || existing {
		t.Fatalf("create: existing=%v err=%v", existing, err)
	}
	if created.ResourceVersion != 1 {
		t.Fatalf("create version = %d, want 1", created.ResourceVersion)
	}

	// An identical document replays idempotently without a version bump.
	replayed, existing, err := put("k1", "the quick brown fox jumps over the lazy dog", testEmbedding(0.9))
	if err != nil || !existing || replayed.ResourceVersion != 1 {
		t.Fatalf("replay: existing=%v version=%d err=%v", existing, replayed.ResourceVersion, err)
	}

	// A different document under the same key is a correction (new version).
	corrected, existing, err := put("k1", "the quick brown fox now runs very fast", testEmbedding(0.9))
	if err != nil || existing || corrected.ResourceVersion != 2 {
		t.Fatalf("correction: existing=%v version=%d err=%v", existing, corrected.ResourceVersion, err)
	}

	// Tenant isolation applies to reads.
	if _, err := repository.GetMemory(ctx, "tenant-b", created.ID); !errors.Is(err, kernelstore.ErrMemoryNotFound) {
		t.Fatalf("cross-tenant GetMemory: %v, want ErrMemoryNotFound", err)
	}
	got, err := repository.GetMemory(ctx, "tenant-a", created.ID)
	if err != nil || got.Content != "the quick brown fox now runs very fast" || got.ResourceVersion != 2 {
		t.Fatalf("GetMemory: %+v err=%v", got, err)
	}

	// Latin keyword search via FTS (content was corrected but keeps the fox).
	results, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{TenantID: "tenant-a", Query: "quick brown", Limit: 10})
	if err != nil || len(results) == 0 {
		t.Fatalf("FTS search: %d results err=%v", len(results), err)
	}
	if results[0].Key != "k1" {
		t.Fatalf("FTS top result = %s, want k1", results[0].Key)
	}

	// CJK substring search via trigram (content has no Latin tokens).
	if _, _, err := put("k2", "人工智能是计算机科学的重要分支", testEmbedding(0.1)); err != nil {
		t.Fatalf("put CJK: %v", err)
	}
	cjk, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{TenantID: "tenant-a", Query: "智能", Limit: 10})
	if err != nil || len(cjk) == 0 || cjk[0].Key != "k2" {
		t.Fatalf("CJK search: %+v err=%v", cjk, err)
	}

	// Vector-only search ranks by cosine distance.
	vector, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{TenantID: "tenant-a", Embedding: testEmbedding(0.9), Limit: 10})
	if err != nil || len(vector) == 0 || vector[0].Key != "k1" {
		t.Fatalf("vector search: %+v err=%v", vector, err)
	}

	// Filters scope the result set.
	scoped, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{
		TenantID: "tenant-a", Query: "智能", Namespace: "other", Limit: 10,
	})
	if err != nil || len(scoped) != 0 {
		t.Fatalf("namespace filter: %d results err=%v", len(scoped), err)
	}
	confidential, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{
		TenantID: "tenant-a", Query: "智能", Sensitivity: "restricted", Limit: 10,
	})
	if err != nil || len(confidential) != 0 {
		t.Fatalf("sensitivity filter: %d results err=%v", len(confidential), err)
	}

	// Tombstone: CAS-versioned soft delete; the record disappears from
	// search while GetMemory still observes the deletion intent.
	tombstoned, err := repository.TombstoneMemory(ctx, "tenant-a", created.ID, 2)
	if err != nil || tombstoned.TombstoneAt == nil {
		t.Fatalf("tombstone: %+v err=%v", tombstoned, err)
	}
	if _, err := repository.TombstoneMemory(ctx, "tenant-a", created.ID, 2); !errors.Is(err, kernelstore.ErrVersionConflict) {
		t.Fatalf("stale tombstone CAS: %v, want ErrVersionConflict", err)
	}
	if _, err := repository.TombstoneMemory(ctx, "tenant-a", created.ID, 3); !errors.Is(err, kernelstore.ErrInvalidTransition) {
		t.Fatalf("double tombstone: %v, want ErrInvalidTransition", err)
	}
	after, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{TenantID: "tenant-a", Query: "quick brown", Limit: 10})
	if err != nil {
		t.Fatalf("search after tombstone: %v", err)
	}
	for _, record := range after {
		if record.Key == "k1" {
			t.Fatal("tombstoned record still appears in search")
		}
	}
	observable, err := repository.GetMemory(ctx, "tenant-a", created.ID)
	if err != nil || observable.TombstoneAt == nil {
		t.Fatalf("deletion intent must survive: %+v err=%v", observable, err)
	}

	// A write to a tombstoned key resurrects it.
	resurrected, existing, err := put("k1", "the quick brown fox is back", testEmbedding(0.9))
	if err != nil || existing || resurrected.TombstoneAt != nil {
		t.Fatalf("resurrection: existing=%v tombstone=%v err=%v", existing, resurrected.TombstoneAt, err)
	}

	// Retention: expired records are excluded from search.
	expired := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)
	if _, _, err := repository.PutMemory(ctx, kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "expired",
		ContentType: "text/plain", Content: "doomed ephemeral fact",
		Embedding: testEmbedding(0.5), EmbeddingProvider: "test",
		Sensitivity: "internal", RetentionUntil: &expired,
	}); err != nil {
		t.Fatalf("put expired: %v", err)
	}
	if _, _, err := repository.PutMemory(ctx, kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "kept",
		ContentType: "text/plain", Content: "durable ephemeral fact",
		Embedding: testEmbedding(0.5), EmbeddingProvider: "test",
		Sensitivity: "internal", RetentionUntil: &future,
	}); err != nil {
		t.Fatalf("put kept: %v", err)
	}
	retention, err := repository.SearchMemory(ctx, kernelstore.SearchMemoryInput{TenantID: "tenant-a", Query: "ephemeral fact", Limit: 10})
	if err != nil {
		t.Fatalf("retention search: %v", err)
	}
	for _, record := range retention {
		if record.Key == "expired" {
			t.Fatal("expired record still appears in search")
		}
		if record.Key == "kept" {
			return // the surviving record is present: all good
		}
	}
	t.Fatalf("kept record missing from search: %+v", retention)
}

// TestMemoryProjectionEvents verifies the OpenSearch projection feed
// (ADR-013): every durable upsert and tombstone emits an outbox event in the
// same transaction, and idempotent replays emit nothing.
func TestMemoryProjectionEvents(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	countEvents := func(eventType string) int {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
			WHERE aggregate_type = 'Memory' AND event_type = $1`, eventType).Scan(&count); err != nil {
			t.Fatalf("count outbox events: %v", err)
		}
		return count
	}

	created, existing, err := repository.PutMemory(ctx, kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "k1",
		ContentType: "text/plain", Content: "first version",
		Embedding: testEmbedding(0.9), EmbeddingProvider: "test", Sensitivity: "internal",
	})
	if err != nil || existing {
		t.Fatalf("put: existing=%v err=%v", existing, err)
	}
	if countEvents("MemoryUpserted") != 1 {
		t.Fatalf("MemoryUpserted count = %d, want 1", countEvents("MemoryUpserted"))
	}

	// Idempotent replay emits no event (nothing changed).
	if _, existing, err := repository.PutMemory(ctx, kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "k1",
		ContentType: "text/plain", Content: "first version",
		Embedding: testEmbedding(0.9), EmbeddingProvider: "test", Sensitivity: "internal",
	}); err != nil || !existing {
		t.Fatalf("replay: existing=%v err=%v", existing, err)
	}
	if countEvents("MemoryUpserted") != 1 {
		t.Fatalf("replay emitted a MemoryUpserted event")
	}

	// A correction is a new version and emits again.
	if _, existing, err := repository.PutMemory(ctx, kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "k1",
		ContentType: "text/plain", Content: "corrected version",
		Embedding: testEmbedding(0.9), EmbeddingProvider: "test", Sensitivity: "internal",
	}); err != nil || existing {
		t.Fatalf("correction: existing=%v err=%v", existing, err)
	}
	if countEvents("MemoryUpserted") != 2 {
		t.Fatalf("MemoryUpserted count after correction = %d, want 2", countEvents("MemoryUpserted"))
	}

	// The tombstone emits MemoryTombstoned with the bumped version.
	tombstoned, err := repository.TombstoneMemory(ctx, "tenant-a", created.ID, 2)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	if tombstoned.ResourceVersion != 3 {
		t.Fatalf("tombstone version = %d, want 3", tombstoned.ResourceVersion)
	}
	if countEvents("MemoryTombstoned") != 1 {
		t.Fatalf("MemoryTombstoned count = %d, want 1", countEvents("MemoryTombstoned"))
	}
	var payload map[string]any
	if err := pool.QueryRow(ctx, `SELECT payload FROM outbox_events
		WHERE aggregate_type = 'Memory' AND event_type = 'MemoryTombstoned'`).Scan(&payload); err != nil {
		t.Fatalf("read tombstone payload: %v", err)
	}
	if payload["memoryId"] != created.ID.String() || payload["tenantId"] != "tenant-a" || payload["resourceVersion"] != float64(3) {
		t.Fatalf("tombstone payload = %+v", payload)
	}
}

func TestMemoryValidation(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	base := kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "k",
		ContentType: "text/plain", Content: "x",
		Embedding: testEmbedding(0.5), EmbeddingProvider: "test", Sensitivity: "internal",
	}
	if _, _, err := repository.PutMemory(ctx, base); err != nil {
		t.Fatalf("valid put: %v", err)
	}
	bad := base
	bad.Content = string(make([]byte, kernelstore.MemoryContentLimit+1))
	if _, _, err := repository.PutMemory(ctx, bad); !errors.Is(err, kernelstore.ErrMemoryTooLarge) {
		t.Fatalf("oversized content: %v, want ErrMemoryTooLarge", err)
	}
	bad = base
	bad.Embedding = []float32{1, 2, 3}
	if _, _, err := repository.PutMemory(ctx, bad); err == nil {
		t.Fatal("wrong embedding dimension must fail")
	}
	search := kernelstore.SearchMemoryInput{TenantID: "tenant-a", Limit: 10}
	if _, err := repository.SearchMemory(ctx, search); !errors.Is(err, kernelstore.ErrMemorySearchRequired) {
		t.Fatalf("empty search: %v, want ErrMemorySearchRequired", err)
	}
	if _, err := repository.GetMemory(ctx, "tenant-a", uuid.Nil); err == nil {
		t.Fatal("nil memory ID must fail")
	}
}
