// Package memory implements the v0.1 Memory decision chain (ADR-009): the
// canonical store lives in PostgreSQL (FTS + pg_trgm + pgvector share the
// transaction system), embeddings are produced by a pluggable Embedder, and
// retrieval filters tenant/sensitivity before scoring. Deletion is a
// tombstone (deletion intent survives; projections can rebuild).
package memory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// Embedder turns memory content into the canonical vector space. The dev
// embedder is deterministic but semantically meaningless; production
// deployments plug a real embedding service behind this interface.
type Embedder interface {
	Embed(context.Context, string) ([]float32, error)
}

// DevEmbedder produces a deterministic, normalized 384-dimension vector from
// the content hash. It exists so the full pipeline (store, FTS/vector hybrid
// retrieval, control API) runs without an external embedding service; it must
// not be used where semantic similarity matters.
type DevEmbedder struct{}

func (DevEmbedder) Embed(_ context.Context, content string) ([]float32, error) {
	digest := sha256.Sum256([]byte(content))
	seed := int64(binary.BigEndian.Uint64(digest[:8]))
	generator := rand.New(rand.NewSource(seed))
	values := make([]float32, store.MemoryEmbeddingDimension)
	var norm float64
	for i := range values {
		value := generator.NormFloat64()
		values[i] = float32(value)
		norm += value * value
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		return nil, fmt.Errorf("dev embedding collapsed to zero")
	}
	for i := range values {
		values[i] = float32(float64(values[i]) / norm)
	}
	return values, nil
}

// Gateway orchestrates memory writes and retrieval: it embeds content,
// validates the decision inputs and delegates persistence to the canonical
// store.
type Gateway struct {
	embedder Embedder
	memories store.MemoryStore
	now      func() time.Time
	newID    func() uuid.UUID
}

func NewGateway(embedder Embedder, memories store.MemoryStore) *Gateway {
	return &Gateway{
		embedder: embedder,
		memories: memories,
		now:      time.Now,
		newID: func() uuid.UUID {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.New()
			}
			return id
		},
	}
}

// PutInput is one memory write.
type PutInput struct {
	TenantID        string
	Namespace       string
	Key             string
	ContentType     string
	Content         string
	Embedding       []float32
	EmbeddingSource string // caller-supplied embedding; empty = embed content
	Sensitivity     string
	SourceTaskID    *uuid.UUID
	SourceRunID     *uuid.UUID
	SourceAttemptID *uuid.UUID
	Provenance      map[string]any
	RetentionUntil  *time.Time
}

// Put embeds (unless a caller-supplied embedding is given), validates, and
// persists. It returns the record and whether the write replayed an existing
// identical document.
func (g *Gateway) Put(ctx context.Context, in PutInput) (store.MemoryRecord, bool, error) {
	var zero store.MemoryRecord
	if strings.TrimSpace(in.TenantID) == "" {
		return zero, false, fmt.Errorf("tenant is required")
	}
	embedding := in.Embedding
	provider := in.EmbeddingSource
	if len(embedding) == 0 {
		embedded, err := g.embedder.Embed(ctx, in.Content)
		if err != nil {
			return zero, false, fmt.Errorf("embed memory content: %w", err)
		}
		embedding = embedded
		provider = "dev-hash"
	}
	provenance, err := marshalProvenance(in.Provenance)
	if err != nil {
		return zero, false, err
	}
	return g.memories.PutMemory(ctx, store.PutMemoryInput{
		TenantID:          in.TenantID,
		Namespace:         strings.TrimSpace(in.Namespace),
		Key:               strings.TrimSpace(in.Key),
		ContentType:       strings.TrimSpace(in.ContentType),
		Content:           in.Content,
		Embedding:         embedding,
		EmbeddingProvider: provider,
		Sensitivity:       in.Sensitivity,
		SourceTaskID:      in.SourceTaskID,
		SourceRunID:       in.SourceRunID,
		SourceAttemptID:   in.SourceAttemptID,
		Provenance:        provenance,
		RetentionUntil:    in.RetentionUntil,
	})
}

// SearchInput is one retrieval request; at least one of Query or Embedding
// must be present.
type SearchInput struct {
	TenantID    string
	Query       string
	Embedding   []float32
	Namespace   string
	Sensitivity string
	Limit       int
}

// Search embeds the keyword query when no vector is given and delegates the
// hybrid scoring to the canonical store.
func (g *Gateway) Search(ctx context.Context, in SearchInput) ([]store.MemoryRecord, error) {
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, fmt.Errorf("tenant is required")
	}
	if in.Limit == 0 {
		in.Limit = 20
	}
	if len(in.Embedding) == 0 && strings.TrimSpace(in.Query) != "" {
		embedded, err := g.embedder.Embed(ctx, in.Query)
		if err != nil {
			return nil, fmt.Errorf("embed search query: %w", err)
		}
		in.Embedding = embedded
	}
	return g.memories.SearchMemory(ctx, store.SearchMemoryInput{
		TenantID:    in.TenantID,
		Query:       strings.TrimSpace(in.Query),
		Embedding:   in.Embedding,
		Namespace:   strings.TrimSpace(in.Namespace),
		Sensitivity: strings.TrimSpace(in.Sensitivity),
		Limit:       in.Limit,
	})
}

// Get reads one record (tombstone state included).
func (g *Gateway) Get(ctx context.Context, tenantID string, id uuid.UUID) (store.MemoryRecord, error) {
	return g.memories.GetMemory(ctx, tenantID, id)
}

// Tombstone soft-deletes a record with a CAS on its resource version.
func (g *Gateway) Tombstone(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64) (store.MemoryRecord, error) {
	return g.memories.TombstoneMemory(ctx, tenantID, id, expectedVersion)
}

func marshalProvenance(provenance map[string]any) ([]byte, error) {
	if len(provenance) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(provenance)
	if err != nil {
		return nil, fmt.Errorf("encode provenance: %w", err)
	}
	return encoded, nil
}
