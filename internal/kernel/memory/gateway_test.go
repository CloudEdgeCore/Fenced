package memory

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestDevEmbedderIsDeterministicAndNormalized(t *testing.T) {
	first, err := DevEmbedder{}.Embed(context.Background(), "同一个内容")
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	second, err := DevEmbedder{}.Embed(context.Background(), "同一个内容")
	if err != nil {
		t.Fatalf("embed replay: %v", err)
	}
	if len(first) != store.MemoryEmbeddingDimension || len(second) != store.MemoryEmbeddingDimension {
		t.Fatalf("dimension = %d/%d, want %d", len(first), len(second), store.MemoryEmbeddingDimension)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("dev embedding is not deterministic at index %d", i)
		}
	}
	var norm float64
	for _, value := range first {
		norm += float64(value) * float64(value)
	}
	if math.Abs(norm-1) > 1e-5 {
		t.Fatalf("dev embedding norm = %v, want 1", norm)
	}
	// Different content must produce a different vector.
	other, err := DevEmbedder{}.Embed(context.Background(), "不同的内容")
	if err != nil {
		t.Fatalf("embed other: %v", err)
	}
	same := true
	for i := range first {
		if first[i] != other[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different content produced the same dev embedding")
	}
}

type fakeMemoryStore struct {
	putInput  store.PutMemoryInput
	searchIn  store.SearchMemoryInput
	record    store.MemoryRecord
	putErr    error
	searchErr error
}

func (f *fakeMemoryStore) PutMemory(_ context.Context, in store.PutMemoryInput) (store.MemoryRecord, bool, error) {
	f.putInput = in
	return f.record, false, f.putErr
}
func (f *fakeMemoryStore) GetMemory(context.Context, string, uuid.UUID) (store.MemoryRecord, error) {
	return f.record, nil
}
func (f *fakeMemoryStore) SearchMemory(_ context.Context, in store.SearchMemoryInput) ([]store.MemoryRecord, error) {
	f.searchIn = in
	return nil, f.searchErr
}
func (f *fakeMemoryStore) TombstoneMemory(context.Context, string, uuid.UUID, int64) (store.MemoryRecord, error) {
	return f.record, nil
}

func TestGatewayPutEmbedsContentWhenNoVectorGiven(t *testing.T) {
	fake := &fakeMemoryStore{record: store.MemoryRecord{ID: uuid.New(), ResourceVersion: 1}}
	gateway := NewGateway(DevEmbedder{}, fake)
	_, _, err := gateway.Put(context.Background(), PutInput{
		TenantID: "tenant-a", Namespace: "default", Key: "k", ContentType: "text/plain",
		Content: "hello", Sensitivity: "internal",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(fake.putInput.Embedding) != store.MemoryEmbeddingDimension {
		t.Fatalf("content was not embedded: %d dimensions", len(fake.putInput.Embedding))
	}
	if fake.putInput.EmbeddingProvider == "" {
		t.Fatal("embedding provider was not recorded")
	}
}

func TestGatewayPutRequiresTenant(t *testing.T) {
	fake := &fakeMemoryStore{}
	gateway := NewGateway(DevEmbedder{}, fake)
	if _, _, err := gateway.Put(context.Background(), PutInput{
		Namespace: "default", Key: "k", ContentType: "text/plain", Content: "x",
	}); err == nil {
		t.Fatal("Put without tenant must fail")
	}
}

func TestGatewaySearchEmbedsQuery(t *testing.T) {
	fake := &fakeMemoryStore{}
	gateway := NewGateway(DevEmbedder{}, fake)
	if _, err := gateway.Search(context.Background(), SearchInput{TenantID: "tenant-a", Query: "hello", Limit: 5}); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(fake.searchIn.Embedding) != store.MemoryEmbeddingDimension {
		t.Fatal("keyword query was not embedded for the hybrid search")
	}
	if fake.searchIn.Query != "hello" {
		t.Fatalf("keyword query = %q", fake.searchIn.Query)
	}
}

func TestGatewaySearchRequiresTenant(t *testing.T) {
	gateway := NewGateway(DevEmbedder{}, &fakeMemoryStore{})
	if _, err := gateway.Search(context.Background(), SearchInput{Query: "x"}); err == nil {
		t.Fatal("Search without tenant must fail")
	}
}

func TestGatewayDelegatesErrors(t *testing.T) {
	fake := &fakeMemoryStore{putErr: store.ErrMemoryTooLarge}
	gateway := NewGateway(DevEmbedder{}, fake)
	embedding := make([]float32, store.MemoryEmbeddingDimension)
	_, _, err := gateway.Put(context.Background(), PutInput{
		TenantID: "tenant-a", Namespace: "default", Key: "k", ContentType: "text/plain",
		Content: "x", Sensitivity: "internal", Embedding: embedding,
	})
	if !errors.Is(err, store.ErrMemoryTooLarge) {
		t.Fatalf("Put error = %v, want ErrMemoryTooLarge", err)
	}
}
