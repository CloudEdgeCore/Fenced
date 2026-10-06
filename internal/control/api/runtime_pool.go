package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func (h *Handler) updateRuntimePoolStatus(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.runtimePools == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "RUNTIME_POOL_OPERATIONS_DISABLED", "runtime pool operations are not configured", traceID)
		return
	}
	expected, err := parseEntityVersion(request.Header.Get("If-Match"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusPreconditionRequired, "IF_MATCH_REQUIRED", "If-Match must contain the current weak resource-version ETag", traceID)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 4096)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var body struct {
		Status string `json:"status"`
	}
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	state, err := h.runtimePools.UpdateRuntimePoolStatus(request.Context(), store.UpdateRuntimePoolStatusInput{
		TenantID: principal.TenantID, PoolID: request.PathValue("poolID"), Status: body.Status, ExpectedVersion: expected,
		OperatorSubject: principal.Subject,
	})
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, state.ResourceVersion))
	writeJSON(writer, http.StatusOK, map[string]any{"runtimePool": state, "traceId": traceID})
}
