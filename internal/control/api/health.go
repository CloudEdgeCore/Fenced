// The operational endpoints: metrics and the health, readiness and
// version probes.
package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/version"
)

func (h *Handler) getMetrics(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.metrics == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "METRICS_DISABLED", "metrics are not configured", traceID)
		return
	}
	since := time.Now().UTC().Add(-24 * time.Hour)
	if raw := request.URL.Query().Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_SINCE", "since must be an RFC3339 timestamp", traceID)
			return
		}
		since = parsed
	}
	metrics, err := h.metrics.AggregateMetrics(request.Context(), principal.TenantID, since)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	writeJSON(writer, http.StatusOK, metrics)
}

func (h *Handler) health(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, `{"status":"ok"}`)
}

func (h *Handler) ready(writer http.ResponseWriter, request *http.Request) {
	if h.readiness == nil {
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "DEPENDENCY_NOT_READY", "readiness probe is not configured", traceIDFrom(request.Context()))
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := h.readiness(ctx); err != nil {
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "DEPENDENCY_NOT_READY", "control-plane dependency is unavailable", traceIDFrom(request.Context()))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, `{"status":"ready"}`)
}

func (h *Handler) version(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(version.Current())
}

func (h *Handler) methodNotAllowed(writer http.ResponseWriter, request *http.Request) {
	h.writeProblem(writer, request, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method is not allowed for this resource", traceIDFrom(request.Context()))
}

func (h *Handler) notFound(writer http.ResponseWriter, request *http.Request) {
	h.writeProblem(writer, request, http.StatusNotFound, "ROUTE_NOT_FOUND", "route was not found", traceIDFrom(request.Context()))
}
