package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ kernelstore.MemoryStore = (*Store)(nil)

const memoryColumns = `
	id::text, tenant_id, namespace, key, content_type, content,
	embedding::text, embedding_provider, sensitivity,
	source_task_id::text, source_run_id::text, source_attempt_id::text,
	provenance, retention_until, tombstone_at, superseded_by::text,
	resource_version, created_at, updated_at`

func (s *Store) PutMemory(ctx context.Context, in kernelstore.PutMemoryInput) (kernelstore.MemoryRecord, bool, error) {
	var zero kernelstore.MemoryRecord
	if err := in.Validate(); err != nil {
		return zero, false, err
	}
	now := s.now()
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, false, err
	}
	defer rollback(ctx, tx)

	// Idempotent replay: an identical document under the same stable key is a
	// no-op; a different document is a correction (new version).
	existing, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryColumns+`
		FROM memory_records WHERE tenant_id = $1 AND namespace = $2 AND key = $3`,
		in.TenantID, in.Namespace, in.Key))
	if err == nil {
		hash, hashErr := existing.ContentHash()
		if hashErr != nil {
			return zero, false, hashErr
		}
		inputHash, inputHashErr := kernelstore.MemoryRecord{
			Namespace: in.Namespace, Key: in.Key, ContentType: in.ContentType,
			Content: in.Content, Embedding: in.Embedding, Sensitivity: in.Sensitivity,
			Provenance: in.Provenance, RetentionUntil: in.RetentionUntil,
		}.ContentHash()
		if inputHashErr != nil {
			return zero, false, inputHashErr
		}
		if hash == inputHash {
			if err := tx.Commit(ctx); err != nil {
				return zero, false, classify(err)
			}
			return existing, true, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return zero, false, classify(err)
	}

	updated, err := scanMemory(tx.QueryRow(ctx, `INSERT INTO memory_records (
		id, tenant_id, namespace, key, content_type, content, embedding, embedding_provider,
		sensitivity, source_task_id, source_run_id, source_attempt_id, provenance,
		retention_until, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8, $9, $10, $11, $12, $13, $14, $15, $15)
	ON CONFLICT (tenant_id, namespace, key) DO UPDATE SET
		content_type = EXCLUDED.content_type,
		content = EXCLUDED.content,
		embedding = EXCLUDED.embedding,
		embedding_provider = EXCLUDED.embedding_provider,
		sensitivity = EXCLUDED.sensitivity,
		source_task_id = EXCLUDED.source_task_id,
		source_run_id = EXCLUDED.source_run_id,
		source_attempt_id = EXCLUDED.source_attempt_id,
		provenance = EXCLUDED.provenance,
		retention_until = EXCLUDED.retention_until,
		tombstone_at = NULL,
		resource_version = memory_records.resource_version + 1,
		updated_at = EXCLUDED.updated_at
	RETURNING `+memoryColumns,
		s.newID().String(), in.TenantID, in.Namespace, in.Key, in.ContentType, in.Content,
		formatVector(in.Embedding), in.EmbeddingProvider, in.Sensitivity,
		nullableUUID(in.SourceTaskID), nullableUUID(in.SourceRunID), nullableUUID(in.SourceAttemptID),
		memoryProvenance(in.Provenance), nullableTime(in.RetentionUntil), now))
	if err != nil {
		return zero, false, classify(err)
	}
	// Projection feed (ADR-013): every durable upsert emits a MemoryUpserted
	// event in the same transaction so the OpenSearch projector can apply it
	// idempotently by resource version.
	if err := insertEvent(ctx, tx, in.TenantID, "Memory", updated.ID, updated.ResourceVersion, "MemoryUpserted",
		memoryEventPayload(updated), now, s.newID()); err != nil {
		return zero, false, err
	}
	if err := auditHook(ctx, tx, in.TenantID, "memory.upserted", "Memory", updated.ID, map[string]any{
		"namespace": updated.Namespace, "key": updated.Key, "contentType": updated.ContentType,
		"sensitivity": updated.Sensitivity, "resourceVersion": updated.ResourceVersion,
	}, now); err != nil {
		return zero, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, false, classify(err)
	}
	return updated, false, nil
}

func (s *Store) GetMemory(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.MemoryRecord, error) {
	if strings.TrimSpace(tenantID) == "" || id == uuid.Nil {
		return kernelstore.MemoryRecord{}, fmt.Errorf("tenant and memory ID are required")
	}
	record, err := scanMemory(s.pool.QueryRow(ctx, `SELECT `+memoryColumns+`
		FROM memory_records WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return kernelstore.MemoryRecord{}, kernelstore.ErrMemoryNotFound
	}
	if err != nil {
		return kernelstore.MemoryRecord{}, classify(err)
	}
	return record, nil
}

func (s *Store) SearchMemory(ctx context.Context, in kernelstore.SearchMemoryInput) ([]kernelstore.MemoryRecord, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	query := nullableString(strings.TrimSpace(in.Query))
	embedding := vectorOrNil(in.Embedding)
	rows, err := s.pool.Query(ctx, `SELECT `+memoryColumns+`,
		(CASE WHEN $2::text IS NOT NULL THEN ts_rank_cd(m.search, websearch_to_tsquery('simple', $2::text)) ELSE 0 END
		 + CASE WHEN $2::text IS NOT NULL THEN similarity(m.content, $2::text) ELSE 0 END
		 + CASE WHEN $3::vector IS NOT NULL THEN 1 - (m.embedding <=> $3::vector) ELSE 0 END) AS score
		FROM memory_records m
		WHERE m.tenant_id = $1
		  AND m.tombstone_at IS NULL
		  AND (m.retention_until IS NULL OR m.retention_until > now())
		  AND ($4::text IS NULL OR m.namespace = $4::text)
		  AND ($5::text IS NULL OR m.sensitivity = $5::text)
		  AND (
				($2::text IS NULL AND $3::vector IS NOT NULL)
				OR ($2::text IS NOT NULL
					AND (m.search @@ websearch_to_tsquery('simple', $2::text) OR m.content ILIKE '%' || $2::text || '%'))
			  )
		ORDER BY score DESC, m.updated_at DESC
		LIMIT $6`,
		in.TenantID, query, embedding, nullableString(in.Namespace), nullableString(in.Sensitivity), in.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var records []kernelstore.MemoryRecord
	for rows.Next() {
		var record kernelstore.MemoryRecord
		var score float64
		if err := scanMemoryInto(rows, &record, &score); err != nil {
			return nil, err
		}
		_ = score // ordering is handled by SQL; the score column keeps the scan aligned
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return records, nil
}

func (s *Store) TombstoneMemory(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64) (kernelstore.MemoryRecord, error) {
	var zero kernelstore.MemoryRecord
	if strings.TrimSpace(tenantID) == "" || id == uuid.Nil || expectedVersion <= 0 {
		return zero, fmt.Errorf("tenant, memory ID, and expected version are required")
	}
	now := s.now()
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)
	updated, err := scanMemory(tx.QueryRow(ctx, `UPDATE memory_records
		SET tombstone_at = $1, resource_version = resource_version + 1, updated_at = $1
		WHERE tenant_id = $2 AND id = $3 AND resource_version = $4 AND tombstone_at IS NULL
		RETURNING `+memoryColumns, now, tenantID, id.String(), expectedVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		// Distinguish a stale CAS from an already-tombstoned or missing row.
		current, lookupErr := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryColumns+`
			FROM memory_records WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
		if lookupErr != nil {
			if errors.Is(lookupErr, pgx.ErrNoRows) {
				return zero, kernelstore.ErrMemoryNotFound
			}
			return zero, classify(lookupErr)
		}
		if current.ResourceVersion != expectedVersion {
			return zero, versionConflict("memory record", id, expectedVersion, current.ResourceVersion)
		}
		return zero, fmt.Errorf("%w: memory record is already tombstoned", kernelstore.ErrInvalidTransition)
	}
	if err != nil {
		return zero, classify(err)
	}
	// Projection feed (ADR-013): the tombstone emits a MemoryTombstoned event
	// in the same transaction so the OpenSearch projector propagates the
	// deletion.
	if err := insertEvent(ctx, tx, tenantID, "Memory", updated.ID, updated.ResourceVersion, "MemoryTombstoned",
		memoryEventPayload(updated), now, s.newID()); err != nil {
		return zero, err
	}
	if err := auditHook(ctx, tx, tenantID, "memory.tombstoned", "Memory", updated.ID, map[string]any{
		"namespace": updated.Namespace, "key": updated.Key, "resourceVersion": updated.ResourceVersion,
	}, now); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

// memoryEventPayload is the projection event payload: identity plus the
// version the projector applies idempotently. Content is not embedded; the
// projector fetches the canonical record from the store.
func memoryEventPayload(record kernelstore.MemoryRecord) map[string]any {
	return map[string]any{
		"tenantId": record.TenantID, "memoryId": record.ID.String(),
		"namespace": record.Namespace, "key": record.Key, "resourceVersion": record.ResourceVersion,
	}
}

// vectorOrNil renders an embedding as a pgvector literal or nil.
func vectorOrNil(embedding []float32) any {
	if len(embedding) == 0 {
		return nil
	}
	return formatVector(embedding)
}

// formatVector renders []float32 as the pgvector text literal [a,b,...].
func formatVector(embedding []float32) string {
	var builder strings.Builder
	builder.WriteByte('[')
	for i, value := range embedding {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
	}
	builder.WriteByte(']')
	return builder.String()
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC()
}

// memoryProvenance renders the provenance document, defaulting to an empty
// object (the column is NOT NULL).
func memoryProvenance(value json.RawMessage) any {
	if len(value) == 0 || string(value) == "null" {
		return json.RawMessage(`{}`)
	}
	return value
}

func scanMemory(row scanner) (kernelstore.MemoryRecord, error) {
	var record kernelstore.MemoryRecord
	err := scanMemoryInto(row, &record, nil)
	return record, err
}

// scanMemoryInto scans the memoryColumns (plus the trailing score column when
// score is non-nil) into the record.
func scanMemoryInto(row scanner, record *kernelstore.MemoryRecord, score *float64) error {
	var id, tenantID, namespace, key, contentType, content, embedding, embeddingProvider, sensitivity string
	var sourceTaskID, sourceRunID, sourceAttemptID, supersededBy sql.NullString
	var provenance []byte
	var retentionUntil, tombstoneAt sql.NullTime
	var err error
	base := []any{&id, &tenantID, &namespace, &key, &contentType, &content,
		&embedding, &embeddingProvider, &sensitivity,
		&sourceTaskID, &sourceRunID, &sourceAttemptID, &provenance,
		&retentionUntil, &tombstoneAt, &supersededBy,
		&record.ResourceVersion, &record.CreatedAt, &record.UpdatedAt}
	if score != nil {
		base = append(base, score)
	}
	if err := row.Scan(base...); err != nil {
		return err
	}
	if record.ID, err = uuid.Parse(id); err != nil {
		return fmt.Errorf("parse memory ID: %w", err)
	}
	if record.Embedding, err = parseVector(embedding); err != nil {
		return err
	}
	if sourceTaskID.Valid {
		parsed, parseErr := uuid.Parse(sourceTaskID.String)
		if parseErr != nil {
			return fmt.Errorf("parse source task ID: %w", parseErr)
		}
		record.SourceTaskID = &parsed
	}
	if sourceRunID.Valid {
		parsed, parseErr := uuid.Parse(sourceRunID.String)
		if parseErr != nil {
			return fmt.Errorf("parse source run ID: %w", parseErr)
		}
		record.SourceRunID = &parsed
	}
	if sourceAttemptID.Valid {
		parsed, parseErr := uuid.Parse(sourceAttemptID.String)
		if parseErr != nil {
			return fmt.Errorf("parse source attempt ID: %w", parseErr)
		}
		record.SourceAttemptID = &parsed
	}
	if supersededBy.Valid {
		parsed, parseErr := uuid.Parse(supersededBy.String)
		if parseErr != nil {
			return fmt.Errorf("parse superseded ID: %w", parseErr)
		}
		record.SupersededBy = &parsed
	}
	record.TenantID, record.Namespace, record.Key = tenantID, namespace, key
	record.ContentType, record.Content = contentType, content
	record.EmbeddingProvider, record.Sensitivity = embeddingProvider, sensitivity
	if len(provenance) > 0 {
		record.Provenance = append(json.RawMessage(nil), provenance...)
	}
	if retentionUntil.Valid {
		record.RetentionUntil = &retentionUntil.Time
	}
	if tombstoneAt.Valid {
		record.TombstoneAt = &tombstoneAt.Time
	}
	return nil
}

// parseVector decodes the pgvector text literal "[a,b,...]".
func parseVector(literal string) ([]float32, error) {
	trimmed := strings.TrimSpace(literal)
	if len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return nil, fmt.Errorf("malformed vector literal")
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if inner == "" {
		return nil, fmt.Errorf("empty vector literal")
	}
	parts := strings.Split(inner, ",")
	values := make([]float32, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return nil, fmt.Errorf("malformed vector component: %w", err)
		}
		values = append(values, float32(parsed))
	}
	return values, nil
}
