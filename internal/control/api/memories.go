// The memory endpoints: write, read, search and tombstone of a tenant
// memory record.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type createMemoryRequest struct {
	Namespace       string         `json:"namespace"`
	Key             string         `json:"key"`
	ContentType     string         `json:"contentType"`
	Content         string         `json:"content"`
	Sensitivity     string         `json:"sensitivity"`
	Provenance      map[string]any `json:"provenance"`
	RetentionUntil  *time.Time     `json:"retentionUntil"`
	Embedding       []float64      `json:"embedding"`
	SourceTaskID    *string        `json:"sourceTaskId"`
	SourceRunID     *string        `json:"sourceRunId"`
	SourceAttemptID *string        `json:"sourceAttemptId"`
}

type memoryResponse struct {
	APIVersion        string          `json:"apiVersion"`
	Kind              string          `json:"kind"`
	ID                string          `json:"id"`
	TenantID          string          `json:"tenantId"`
	Namespace         string          `json:"namespace"`
	Key               string          `json:"key"`
	ContentType       string          `json:"contentType"`
	Content           string          `json:"content"`
	EmbeddingProvider string          `json:"embeddingProvider"`
	Sensitivity       string          `json:"sensitivity"`
	Provenance        json.RawMessage `json:"provenance,omitempty"`
	SourceTaskID      *string         `json:"sourceTaskId,omitempty"`
	SourceRunID       *string         `json:"sourceRunId,omitempty"`
	SourceAttemptID   *string         `json:"sourceAttemptId,omitempty"`
	RetentionUntil    *time.Time      `json:"retentionUntil,omitempty"`
	TombstoneAt       *time.Time      `json:"tombstoneAt,omitempty"`
	SupersededBy      *string         `json:"supersededBy,omitempty"`
	ResourceVersion   int64           `json:"resourceVersion"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	TraceID           string          `json:"traceId"`
}

// createMemory persists one canonical memory entry (ADR-009). The stable
// (namespace, key) pair makes an identical write idempotent; a different
// document under the same key is a correction (new version).
func (h *Handler) createMemory(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.memories == nil {
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "MEMORY_UNAVAILABLE", "memory API is not configured", traceID)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeProblem(writer, request, http.StatusUnsupportedMediaType, "CONTENT_TYPE_REQUIRED", "Content-Type must be application/json", traceID)
		return
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if !idempotencyKeyPattern.MatchString(idempotencyKey) {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must be 1..128 safe ASCII characters", traceID)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBody)
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := rejectDuplicateJSONKeys(encoded); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var body createMemoryRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	put, problemErr := h.memoryPutFrom(principal.TenantID, body)
	if problemErr != nil {
		problemErr.write(writer, request, traceID)
		return
	}
	record, existing, err := h.memories.Put(request.Context(), put)
	if err != nil {
		h.writeMemoryProblem(writer, request, err, traceID)
		return
	}
	status := http.StatusCreated
	if existing {
		status = http.StatusOK
		writer.Header().Set("Idempotent-Replayed", "true")
	}
	writer.Header().Set("Location", "/v1/memories/"+record.ID.String())
	h.writeMemory(writer, status, record, traceID)
}

func (h *Handler) getMemory(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.memories == nil {
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "MEMORY_UNAVAILABLE", "memory API is not configured", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("memoryID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "MEMORY_NOT_FOUND", "memory record was not found", traceID)
		return
	}
	record, err := h.memories.Get(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeMemoryProblem(writer, request, err, traceID)
		return
	}
	h.writeMemory(writer, http.StatusOK, record, traceID)
}

// searchMemory runs the hybrid retrieval (FTS + trigram + optional vector).
// The tenant scope and sensitivity filter are applied before scoring.
func (h *Handler) searchMemory(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.memories == nil {
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "MEMORY_UNAVAILABLE", "memory API is not configured", traceID)
		return
	}
	query := strings.TrimSpace(request.URL.Query().Get("query"))
	var embedding []float64
	if raw := strings.TrimSpace(request.URL.Query().Get("embedding")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &embedding); err != nil || len(embedding) == 0 {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_EMBEDDING", "embedding must be a JSON array of numbers", traceID)
			return
		}
	}
	limit := 20
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 100 {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 100", traceID)
			return
		}
		limit = parsed
	}
	if query == "" && len(embedding) == 0 {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_SEARCH", "query or embedding is required", traceID)
		return
	}
	records, err := h.memories.Search(request.Context(), memory.SearchInput{
		TenantID:    principal.TenantID,
		Query:       query,
		Embedding:   float64sToFloat32s(embedding),
		Namespace:   strings.TrimSpace(request.URL.Query().Get("namespace")),
		Sensitivity: strings.TrimSpace(request.URL.Query().Get("sensitivity")),
		Limit:       limit,
	})
	if err != nil {
		h.writeMemoryProblem(writer, request, err, traceID)
		return
	}
	memories := make([]memoryResponse, 0, len(records))
	for _, record := range records {
		memories = append(memories, memoryResponseFrom(record, traceID))
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"apiVersion": agentversion.APIVersion, "kind": "MemoryList", "memories": memories, "traceId": traceID,
	})
}

// tombstoneMemory soft-deletes a record (ADR-009 deletion intent survives).
func (h *Handler) tombstoneMemory(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.memories == nil {
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "MEMORY_UNAVAILABLE", "memory API is not configured", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("memoryID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "MEMORY_NOT_FOUND", "memory record was not found", traceID)
		return
	}
	expectedVersion, err := parseEntityVersion(request.Header.Get("If-Match"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusPreconditionRequired, "IF_MATCH_REQUIRED", "If-Match must contain the current weak resource-version ETag", traceID)
		return
	}
	record, err := h.memories.Tombstone(request.Context(), principal.TenantID, id, expectedVersion)
	if err != nil {
		h.writeMemoryProblem(writer, request, err, traceID)
		return
	}
	h.writeMemory(writer, http.StatusOK, record, traceID)
}

// memoryPutFrom validates the request body and builds the decision-chain
// input, converting caller-supplied embeddings to the canonical vector space.
func (h *Handler) memoryPutFrom(tenantID string, body createMemoryRequest) (memory.PutInput, *handlerProblem) {
	put := memory.PutInput{
		TenantID:       tenantID,
		Namespace:      strings.TrimSpace(body.Namespace),
		Key:            strings.TrimSpace(body.Key),
		ContentType:    strings.TrimSpace(body.ContentType),
		Content:        body.Content,
		Sensitivity:    body.Sensitivity,
		Provenance:     body.Provenance,
		RetentionUntil: body.RetentionUntil,
	}
	if strings.TrimSpace(put.Namespace) == "" || len(put.Namespace) > 255 {
		return put, &handlerProblem{status: http.StatusUnprocessableEntity, reason: "INVALID_MEMORY", detail: "namespace is required and must not exceed 255 bytes"}
	}
	if strings.TrimSpace(put.Key) == "" || len(put.Key) > 255 {
		return put, &handlerProblem{status: http.StatusUnprocessableEntity, reason: "INVALID_MEMORY", detail: "key is required and must not exceed 255 bytes"}
	}
	if strings.TrimSpace(put.Content) == "" {
		return put, &handlerProblem{status: http.StatusUnprocessableEntity, reason: "INVALID_MEMORY", detail: "content is required"}
	}
	if put.Sensitivity == "" {
		put.Sensitivity = "internal"
	}
	if len(body.Embedding) > 0 {
		put.Embedding = float64sToFloat32s(body.Embedding)
		put.EmbeddingSource = "caller"
	}
	var problem *handlerProblem
	put.SourceTaskID, problem = optionalUUID(body.SourceTaskID, "sourceTaskId")
	if problem != nil {
		return put, problem
	}
	put.SourceRunID, problem = optionalUUID(body.SourceRunID, "sourceRunId")
	if problem != nil {
		return put, problem
	}
	put.SourceAttemptID, problem = optionalUUID(body.SourceAttemptID, "sourceAttemptId")
	if problem != nil {
		return put, problem
	}
	return put, nil
}

func optionalUUID(raw *string, field string) (*uuid.UUID, *handlerProblem) {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil, nil
	}
	parsed, err := uuid.Parse(strings.TrimSpace(*raw))
	if err != nil {
		return nil, &handlerProblem{status: http.StatusUnprocessableEntity, reason: "INVALID_MEMORY", detail: field + " must be a UUID"}
	}
	return &parsed, nil
}

func float64sToFloat32s(values []float64) []float32 {
	if len(values) == 0 {
		return nil
	}
	converted := make([]float32, len(values))
	for i, value := range values {
		converted[i] = float32(value)
	}
	return converted
}

// handlerProblem is a locally-decided request failure (validation) that does
// not need store error mapping.
type handlerProblem struct {
	status int
	reason string
	detail string
}

func memoryResponseFrom(record store.MemoryRecord, traceID string) memoryResponse {
	response := memoryResponse{
		APIVersion: agentversion.APIVersion, Kind: "Memory", ID: record.ID.String(),
		TenantID: record.TenantID, Namespace: record.Namespace, Key: record.Key,
		ContentType: record.ContentType, Content: record.Content,
		EmbeddingProvider: record.EmbeddingProvider, Sensitivity: record.Sensitivity,
		ResourceVersion: record.ResourceVersion,
		CreatedAt:       record.CreatedAt.UTC(), UpdatedAt: record.UpdatedAt.UTC(), TraceID: traceID,
	}
	if len(record.Provenance) > 0 {
		response.Provenance = record.Provenance
	}
	if record.SourceTaskID != nil {
		value := record.SourceTaskID.String()
		response.SourceTaskID = &value
	}
	if record.SourceRunID != nil {
		value := record.SourceRunID.String()
		response.SourceRunID = &value
	}
	if record.SourceAttemptID != nil {
		value := record.SourceAttemptID.String()
		response.SourceAttemptID = &value
	}
	if record.RetentionUntil != nil {
		value := record.RetentionUntil.UTC()
		response.RetentionUntil = &value
	}
	if record.TombstoneAt != nil {
		value := record.TombstoneAt.UTC()
		response.TombstoneAt = &value
	}
	if record.SupersededBy != nil {
		value := record.SupersededBy.String()
		response.SupersededBy = &value
	}
	return response
}

func (h *Handler) writeMemory(writer http.ResponseWriter, status int, record store.MemoryRecord, traceID string) {
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, record.ResourceVersion))
	writeJSON(writer, status, memoryResponseFrom(record, traceID))
}

func (h *Handler) writeMemoryProblem(writer http.ResponseWriter, request *http.Request, err error, traceID string) {
	switch {
	case errors.Is(err, store.ErrMemoryNotFound):
		h.writeProblem(writer, request, http.StatusNotFound, "MEMORY_NOT_FOUND", "memory record was not found", traceID)
	case errors.Is(err, store.ErrVersionConflict):
		h.writeProblem(writer, request, http.StatusConflict, "RESOURCE_VERSION_CONFLICT", "resource changed concurrently", traceID)
	case errors.Is(err, store.ErrInvalidTransition):
		h.writeProblem(writer, request, http.StatusConflict, "MEMORY_STATE_CONFLICT", "memory record is already tombstoned", traceID)
	case errors.Is(err, store.ErrMemoryTooLarge):
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "MEMORY_TOO_LARGE", "memory content exceeds 256 KiB", traceID)
	case errors.Is(err, store.ErrMemorySearchRequired):
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_SEARCH", "query or embedding is required", traceID)
	default:
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed", traceID)
	}
}

// getMetrics returns the §Phase-7 aggregated platform metrics for the
// authenticated tenant (GET /v1/metrics). The optional since query parameter
// bounds the aggregation window (RFC3339; default 24h).

func (p *handlerProblem) write(writer http.ResponseWriter, request *http.Request, traceID string) {
	(&Handler{}).writeProblem(writer, request, p.status, p.reason, p.detail, traceID)
}
