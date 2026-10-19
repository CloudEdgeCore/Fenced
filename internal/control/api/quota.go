// The tenant quota endpoints: the consumption ceiling of a tenant and
// its window usage.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

type tenantQuotaResponse struct {
	APIVersion      string       `json:"apiVersion"`
	Kind            string       `json:"kind"`
	TenantID        string       `json:"tenantId"`
	WindowSeconds   int64        `json:"windowSeconds"`
	WindowStart     *time.Time   `json:"windowStart,omitempty"`
	Limits          usageSummary `json:"limits"`
	Usage           usageSummary `json:"usage"`
	Reserved        usageSummary `json:"reserved"`
	ResourceVersion int64        `json:"resourceVersion"`
	UpdatedAt       time.Time    `json:"updatedAt"`
	TraceID         string       `json:"traceId"`
}

type setTenantQuotaRequest struct {
	WindowSeconds int64        `json:"windowSeconds"`
	Limits        usageSummary `json:"limits"`
}

func (h *Handler) getTenantQuota(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.quotas == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "QUOTA_DISABLED", "tenant quotas are not configured on this endpoint", traceID)
		return
	}
	quota, err := h.quotas.GetTenantQuota(request.Context(), principal.TenantID)
	if err != nil {
		h.writeQuotaProblem(writer, request, err, traceID)
		return
	}
	usage, err := h.quotas.GetTenantQuotaUsage(request.Context(), principal.TenantID, time.Now().UTC())
	if err != nil {
		h.writeQuotaProblem(writer, request, err, traceID)
		return
	}
	h.writeQuota(writer, http.StatusOK, quota, usage, traceID)
}

// setTenantQuota configures or replaces the principal tenant's quota
// (PUT /v1/quota). Existing window consumption is preserved; only the limits
// and window length change.
func (h *Handler) setTenantQuota(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.quotas == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "QUOTA_DISABLED", "tenant quotas are not configured on this endpoint", traceID)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeProblem(writer, request, http.StatusUnsupportedMediaType, "CONTENT_TYPE_REQUIRED", "Content-Type must be application/json", traceID)
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
	var body setTenantQuotaRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	input := store.SetTenantQuotaInput{
		TenantID: principal.TenantID, WindowSeconds: body.WindowSeconds,
		Limits: store.TaskBudget{
			Tokens: body.Limits.Tokens, CostMicroUSD: body.Limits.CostMicroUSD,
			ToolCalls: body.Limits.ToolCalls, WallSeconds: body.Limits.WallSeconds,
		},
	}
	if !input.Valid() {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_QUOTA",
			"windowSeconds must be at least 60 and limits must be non-negative", traceID)
		return
	}
	quota, err := h.quotas.SetTenantQuota(request.Context(), input)
	if err != nil {
		h.writeQuotaProblem(writer, request, err, traceID)
		return
	}
	usage, err := h.quotas.GetTenantQuotaUsage(request.Context(), principal.TenantID, time.Now().UTC())
	if err != nil {
		h.writeQuotaProblem(writer, request, err, traceID)
		return
	}
	h.writeQuota(writer, http.StatusOK, quota, usage, traceID)
}

// deleteTenantQuota removes the principal tenant's quota configuration
// (DELETE /v1/quota). Window consumption rows become inert and future
// admissions are unlimited; the operation is idempotent.
func (h *Handler) deleteTenantQuota(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.quotas == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "QUOTA_DISABLED", "tenant quotas are not configured on this endpoint", traceID)
		return
	}
	if err := h.quotas.DeleteTenantQuota(request.Context(), principal.TenantID); err != nil {
		h.writeQuotaProblem(writer, request, err, traceID)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeQuota(writer http.ResponseWriter, status int, quota store.TenantQuota, usage store.TenantWindowUsage, traceID string) {
	response := tenantQuotaResponse{
		APIVersion: agentversion.APIVersion, Kind: "TenantQuota", TenantID: quota.TenantID,
		WindowSeconds: quota.WindowSeconds,
		Limits: usageSummary{
			Tokens: quota.Limits.Tokens, CostMicroUSD: quota.Limits.CostMicroUSD,
			ToolCalls: quota.Limits.ToolCalls, WallSeconds: quota.Limits.WallSeconds,
		},
		Usage: usageSummary{
			Tokens: usage.Consumed.Tokens, CostMicroUSD: usage.Consumed.CostMicroUSD,
			ToolCalls: usage.Consumed.ToolCalls, WallSeconds: usage.Consumed.WallSeconds,
		},
		Reserved: usageSummary{
			Tokens: usage.Reserved.Tokens, CostMicroUSD: usage.Reserved.CostMicroUSD,
			ToolCalls: usage.Reserved.ToolCalls, WallSeconds: usage.Reserved.WallSeconds,
		},
		ResourceVersion: quota.ResourceVersion, UpdatedAt: quota.UpdatedAt.UTC(), TraceID: traceID,
	}
	if !usage.WindowStart.IsZero() {
		value := usage.WindowStart.UTC()
		response.WindowStart = &value
	}
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, quota.ResourceVersion))
	writeJSON(writer, status, response)
}

func (h *Handler) writeQuotaProblem(writer http.ResponseWriter, request *http.Request, err error, traceID string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		h.writeProblem(writer, request, http.StatusNotFound, "TENANT_QUOTA_NOT_FOUND", "tenant has no configured consumption quota", traceID)
	default:
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed", traceID)
	}
}
