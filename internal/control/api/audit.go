// The audit endpoints: the hash-chained ledger, its verification and the
// signed export archive.
package api

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

type auditEventResponse struct {
	ID           string          `json:"id"`
	Seq          int64           `json:"seq"`
	EventType    string          `json:"eventType"`
	ResourceType string          `json:"resourceType"`
	ResourceID   string          `json:"resourceId"`
	Actor        string          `json:"actor"`
	Details      json.RawMessage `json:"details"`
	PrevHash     string          `json:"prevHash"`
	ChainHash    string          `json:"chainHash"`
	OccurredAt   time.Time       `json:"occurredAt"`
}

func auditEventResponseOf(event store.AuditEvent) auditEventResponse {
	return auditEventResponse{
		ID: event.ID.String(), Seq: event.Seq, EventType: event.EventType,
		ResourceType: event.ResourceType, ResourceID: event.ResourceID.String(), Actor: event.Actor,
		Details: event.Details, PrevHash: hex.EncodeToString(event.PrevHash[:]),
		ChainHash: hex.EncodeToString(event.ChainHash[:]), OccurredAt: event.OccurredAt.UTC(),
	}
}

type auditListResponse struct {
	Events       []auditEventResponse `json:"events"`
	NextAfterSeq *int64               `json:"nextAfterSeq,omitempty"`
}

func (h *Handler) listAudit(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.audit == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "AUDIT_DISABLED", "audit ledger is not configured on this endpoint", traceID)
		return
	}
	limit := 100
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 1000 {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 1000", traceID)
			return
		}
		limit = parsed
	}
	var afterSeq int64
	if raw := strings.TrimSpace(request.URL.Query().Get("afterSeq")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_CURSOR", "afterSeq must be a non-negative integer", traceID)
			return
		}
		afterSeq = parsed
	}
	events, err := h.audit.ListAudit(request.Context(), store.ListAuditInput{TenantID: principal.TenantID, AfterSeq: afterSeq, Limit: limit})
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "audit ledger could not be read", traceID)
		return
	}
	response := auditListResponse{Events: []auditEventResponse{}}
	for _, event := range events {
		response.Events = append(response.Events, auditEventResponseOf(event))
	}
	if len(events) > 0 {
		next := events[len(events)-1].Seq
		response.NextAfterSeq = &next
	}
	writeJSON(writer, http.StatusOK, response)
}

type auditVerificationResponse struct {
	Valid          bool   `json:"valid"`
	Checked        int64  `json:"checked"`
	FirstBrokenSeq int64  `json:"firstBrokenSeq"`
	HeadSeq        int64  `json:"headSeq"`
	HeadHash       string `json:"headHash"`
}

func (h *Handler) verifyAudit(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.audit == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "AUDIT_DISABLED", "audit ledger is not configured on this endpoint", traceID)
		return
	}
	verification, err := h.audit.VerifyAuditChain(request.Context(), principal.TenantID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "audit chain could not be verified", traceID)
		return
	}
	writeJSON(writer, http.StatusOK, auditVerificationResponse{
		Valid: verification.Valid, Checked: verification.Checked,
		FirstBrokenSeq: verification.FirstBrokenSeq, HeadSeq: verification.HeadSeq,
		HeadHash: hex.EncodeToString(verification.HeadHash[:]),
	})
}

type auditSignature struct {
	KeyID   string `json:"keyId"`
	Ed25519 string `json:"ed25519"`
}

type auditArchiveResponse struct {
	SchemaVersion string               `json:"schemaVersion"`
	TenantID      string               `json:"tenantId"`
	GeneratedAt   time.Time            `json:"generatedAt"`
	Signed        bool                 `json:"signed"`
	Events        []auditEventResponse `json:"events"`
	Signature     *auditSignature      `json:"signature,omitempty"`
}

// exportAudit returns the tenant's full ledger as an archive, signed with the
// control plane's audit key when configured (ADR-014 signed WORM export).
func (h *Handler) exportAudit(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.audit == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "AUDIT_DISABLED", "audit ledger is not configured on this endpoint", traceID)
		return
	}
	events, err := h.audit.ExportAuditChain(request.Context(), principal.TenantID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "audit ledger could not be exported", traceID)
		return
	}
	now := time.Now().UTC()
	response := auditArchiveResponse{
		SchemaVersion: store.AuditSchema, TenantID: principal.TenantID,
		GeneratedAt: now, Events: []auditEventResponse{},
	}
	for _, event := range events {
		response.Events = append(response.Events, auditEventResponseOf(event))
	}
	if len(h.auditSigningKey) == ed25519.PrivateKeySize {
		payload, err := json.Marshal(struct {
			SchemaVersion string               `json:"schemaVersion"`
			TenantID      string               `json:"tenantId"`
			GeneratedAt   time.Time            `json:"generatedAt"`
			Events        []auditEventResponse `json:"events"`
		}{response.SchemaVersion, response.TenantID, response.GeneratedAt, response.Events})
		if err != nil {
			h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "audit archive could not be encoded", traceID)
			return
		}
		digest := sha256.Sum256(payload)
		signature := ed25519.Sign(h.auditSigningKey, digest[:])
		response.Signed = true
		response.Signature = &auditSignature{KeyID: h.auditKeyID, Ed25519: base64.RawStdEncoding.EncodeToString(signature)}
	}
	writeJSON(writer, http.StatusOK, response)
}

// --- tenant aggregate consumption quota (v0.6) ---

// tenantQuotaResponse reports a tenant's configured window limits and the
// current window's settled usage. All quota endpoints operate on the
// authenticated principal's own tenant: the tenant never appears in the URL,
// so the tenant boundary is enforced by identity, not by path scoping.
