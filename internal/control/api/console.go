package api

import (
	"net/http"
	"strconv"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/control/console"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// The operator read surface: the lists an operator console and the operator
// scripts need to observe a tenant. Every list is tenant-scoped by the
// authenticated principal, bounded, and ordered so the newest state is
// first.

// listLimit reads the bounded page size of a list request.
func listLimit(request *http.Request) int {
	limit, err := strconv.Atoi(request.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		return 50
	}
	if limit > 500 {
		return 500
	}
	return limit
}

// requirePrincipal answers the authenticated tenant of a request, or writes
// the unauthorized problem itself.
func (h *Handler) requirePrincipal(writer http.ResponseWriter, request *http.Request) (auth.Principal, bool) {
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceIDFrom(request.Context()))
		return auth.Principal{}, false
	}
	return principal, true
}

// listTasks answers GET /v1/tasks: the tenant's tasks, newest first.
func (h *Handler) listTasks(writer http.ResponseWriter, request *http.Request) {
	principal, ok := h.requirePrincipal(writer, request)
	if !ok {
		return
	}
	tasks, err := h.tasks.ListTasks(request.Context(), principal.TenantID, request.URL.Query().Get("namespace"), listLimit(request))
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceIDFrom(request.Context()))
		return
	}
	responses := make([]taskResponse, 0, len(tasks))
	for _, task := range tasks {
		responses = append(responses, taskResponseFrom(task, usageSummary{}, false, traceIDFrom(request.Context())))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"tasks": responses})
}

// listAgentVersions answers GET /v1/agent-versions: the tenant's immutable
// publications, newest first.
func (h *Handler) listAgentVersions(writer http.ResponseWriter, request *http.Request) {
	principal, ok := h.requirePrincipal(writer, request)
	if !ok {
		return
	}
	versions, err := h.agentVersion.ListAgentVersions(request.Context(), principal.TenantID, listLimit(request))
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceIDFrom(request.Context()))
		return
	}
	responses := make([]agentVersionResponse, 0, len(versions))
	for _, version := range versions {
		responses = append(responses, agentVersionResponseOf(version, traceIDFrom(request.Context())))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"agentVersions": responses})
}

// listApprovals answers GET /v1/approvals: the tool calls still waiting for
// a human decision, oldest first.
func (h *Handler) listApprovals(writer http.ResponseWriter, request *http.Request) {
	principal, ok := h.requirePrincipal(writer, request)
	if !ok {
		return
	}
	approvals, err := h.approvals.ListToolApprovals(request.Context(), principal.TenantID, listLimit(request))
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceIDFrom(request.Context()))
		return
	}
	responses := make([]toolApprovalResponse, 0, len(approvals))
	for _, approval := range approvals {
		responses = append(responses, toolApprovalResponseOf(approval, traceIDFrom(request.Context())))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"approvals": responses})
}

// listWorkflows answers GET /v1/workflows: the tenant's workflows, newest
// first.
func (h *Handler) listWorkflows(writer http.ResponseWriter, request *http.Request) {
	// Authentication comes first: an unauthenticated caller must not learn
	// whether a subsystem is configured.
	principal, ok := h.requirePrincipal(writer, request)
	if !ok {
		return
	}
	if h.workflows == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NOT_FOUND", "workflows are not configured", traceIDFrom(request.Context()))
		return
	}
	workflows, err := h.workflows.ListWorkflows(request.Context(), principal.TenantID, listLimit(request))
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceIDFrom(request.Context()))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"workflows": workflows})
}

// listRuntimePools answers GET /v1/runtime-pools: the runtime pools the
// tenant may place work on.
func (h *Handler) listRuntimePools(writer http.ResponseWriter, request *http.Request) {
	principal, ok := h.requirePrincipal(writer, request)
	if !ok {
		return
	}
	if h.runtimePools == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NOT_FOUND", "runtime pools are not configured", traceIDFrom(request.Context()))
		return
	}
	pools, err := h.runtimePools.ListRuntimePools(request.Context(), principal.TenantID)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceIDFrom(request.Context()))
		return
	}
	responses := make([]runtimePoolResponse, 0, len(pools))
	for _, pool := range pools {
		responses = append(responses, runtimePoolResponseOf(pool))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"runtimePools": responses})
}

// runtimePoolResponse is the wire shape of one runtime pool.
type runtimePoolResponse struct {
	ID                string   `json:"id"`
	RuntimeClass      string   `json:"runtimeClass"`
	RuntimeInstanceID string   `json:"runtimeInstanceId"`
	Region            string   `json:"region"`
	DataResidency     string   `json:"dataResidency"`
	Ready             bool     `json:"ready"`
	AvailableCPU      int64    `json:"availableCpuMillis"`
	AvailableMemory   int64    `json:"availableMemoryMiB"`
	AvailableLLM      int      `json:"availableLlmSlots"`
	TenantIDs         []string `json:"tenantIds"`
}

// runtimePoolResponseOf converts a runtime pool to its wire shape.
func runtimePoolResponseOf(pool scheduler.RuntimePool) runtimePoolResponse {
	return runtimePoolResponse{
		ID:                pool.ID,
		RuntimeClass:      pool.RuntimeClass,
		RuntimeInstanceID: pool.RuntimeInstanceID,
		Region:            pool.Region,
		DataResidency:     pool.DataResidency,
		Ready:             pool.Ready,
		AvailableCPU:      pool.AvailableCPU,
		AvailableMemory:   pool.AvailableMemory,
		AvailableLLM:      pool.AvailableLLMSlots,
		TenantIDs:         pool.TenantIDs,
	}
}

// compile-time proof that the list responses keep the store shapes.
var _ = []store.Task(nil)

// console serves the operator console of the control plane.
func (h *Handler) console(writer http.ResponseWriter, request *http.Request) {
	console.Handler().ServeHTTP(writer, request)
}
