// The task endpoints: submission, observation, cancellation and the
// server-sent event stream of a task.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// Task event stream configuration. Polling keeps PostgreSQL as the source of
// truth (ADR-002): the stream observes resource-version changes instead of
// duplicating state in the API process.
const (
	defaultEventPoll     = 500 * time.Millisecond
	minEventPoll         = 100 * time.Millisecond
	maxEventPoll         = 5 * time.Second
	sseKeepAliveInterval = 15 * time.Second
	eventStreamMediaType = "text/event-stream"
)

type createTaskRequest struct {
	AgentVersionRef string          `json:"agentVersionRef"`
	Goal            string          `json:"goal"`
	Namespace       string          `json:"namespace"`
	Spec            json.RawMessage `json:"spec"`
}

type usageSummary struct {
	Tokens       int64          `json:"tokens"`
	CostMicroUSD money.MicroUSD `json:"costUsd"`
	ToolCalls    int64          `json:"toolCalls"`
	WallSeconds  int64          `json:"wallSeconds"`
}

type taskResponse struct {
	APIVersion        string          `json:"apiVersion"`
	Kind              string          `json:"kind"`
	ID                string          `json:"id"`
	TenantID          string          `json:"tenantId"`
	Namespace         string          `json:"namespace"`
	AgentVersionRef   string          `json:"agentVersionRef"`
	AgentVersionID    *string         `json:"agentVersionId"`
	Goal              string          `json:"goal"`
	Spec              json.RawMessage `json:"spec"`
	Phase             string          `json:"phase"`
	ActiveRunID       *string         `json:"activeRunId"`
	ResultRef         *string         `json:"resultRef"`
	ReasonCode        *string         `json:"reasonCode"`
	CancelRequestedAt *time.Time      `json:"cancelRequestedAt"`
	Usage             usageSummary    `json:"usage"`
	BudgetExhausted   bool            `json:"budgetExhausted"`
	ResourceVersion   int64           `json:"resourceVersion"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	TraceID           string          `json:"traceId"`
}

func (h *Handler) createTask(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
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
	var body createTaskRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	if err := validateCreateTask(body); err != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_TASK", err.Error(), traceID)
		return
	}
	result, err := h.tasks.CreateTask(request.Context(), store.CreateTaskInput{
		ID: h.newID(), TenantID: principal.TenantID, Namespace: body.Namespace,
		AgentVersionRef: body.AgentVersionRef, Goal: body.Goal, Spec: body.Spec,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	status := http.StatusAccepted
	if result.Existing {
		status = http.StatusOK
		writer.Header().Set("Idempotent-Replayed", "true")
	}
	writer.Header().Set("Location", "/v1/tasks/"+result.Task.ID.String())
	h.writeTask(writer, status, result.Task, usageSummary{}, false, traceID)
}

func (h *Handler) getTask(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("taskID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "TASK_NOT_FOUND", "task was not found", traceID)
		return
	}
	task, err := h.tasks.GetTask(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	usage, budgetExhausted := h.readUsage(request.Context(), principal.TenantID, id)
	h.writeTask(writer, http.StatusOK, task, usage, budgetExhausted, traceID)
}

func (h *Handler) cancelTask(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	taskID, ok := strings.CutSuffix(request.PathValue("taskAction"), ":cancel")
	if !ok {
		h.notFound(writer, request)
		return
	}
	id, err := uuid.Parse(taskID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "TASK_NOT_FOUND", "task was not found", traceID)
		return
	}
	expectedVersion, err := parseEntityVersion(request.Header.Get("If-Match"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusPreconditionRequired, "IF_MATCH_REQUIRED", "If-Match must contain the current weak resource-version ETag", traceID)
		return
	}
	task, err := h.tasks.RequestTaskCancellation(request.Context(), principal.TenantID, id, expectedVersion)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	usage, budgetExhausted := h.readUsage(request.Context(), principal.TenantID, id)
	h.writeTask(writer, http.StatusAccepted, task, usage, budgetExhausted, traceID)
}

// taskEvents streams task lifecycle events over Server-Sent Events
// (GET /v1/tasks/{taskID}/events). The stream opens with the current task
// state and then emits task.updated on every resource-version change, with a
// final task.terminal event closing the stream once the task reaches a
// terminal phase. Comment heartbeats keep proxies from timing the connection
// out. Events are best-effort: the cursor is the resource version, so clients
// must reconcile with GET /v1/tasks/{taskID} after a reconnect.
func (h *Handler) taskEvents(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	// Bounded concurrency (O5): streams are long-lived, so a flood of slow
	// clients must not exhaust the server.
	if h.sseSlots > 0 && h.activeSSE.Add(1) > int64(h.sseSlots) {
		h.activeSSE.Add(-1)
		h.writeProblem(writer, request, http.StatusServiceUnavailable, "STREAM_CAPACITY_EXCEEDED",
			"too many concurrent event streams; retry later", traceID)
		return
	}
	defer h.activeSSE.Add(-1)
	id, err := uuid.Parse(request.PathValue("taskID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "TASK_NOT_FOUND", "task was not found", traceID)
		return
	}
	task, err := h.tasks.GetTask(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	pollInterval := defaultEventPoll
	if raw := request.URL.Query().Get("poll"); raw != "" {
		parsed, parseErr := time.ParseDuration(raw)
		if parseErr != nil || parsed < minEventPoll || parsed > maxEventPoll {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_POLL_INTERVAL",
				"poll must be a duration between 100ms and 5s", traceID)
			return
		}
		pollInterval = parsed
	}
	controller := http.NewResponseController(writer)
	writer.Header().Set("Content-Type", eventStreamMediaType)
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Accel-Buffering", "no")
	// The stream outlives the normal request lifecycle: lift the write
	// deadline so idle connections are not killed mid-stream. Backends that
	// cannot set deadlines (e.g. test recorders) are fine — the stream simply
	// inherits the server's default behavior.
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		h.writeProblem(writer, request, http.StatusInternalServerError, "EVENTS_UNSUPPORTED",
			"streaming is not supported by this server", traceID)
		return
	}
	writer.WriteHeader(http.StatusOK)
	usage, budgetExhausted := h.readUsage(request.Context(), principal.TenantID, id)
	if !writeTaskEvent(writer, controller, "task.updated", task, usage, budgetExhausted, traceID) {
		return
	}
	if task.Phase.Terminal() {
		// Already terminal at connect: the stream is a single snapshot.
		_ = writeTaskEvent(writer, controller, "task.terminal", task, usage, budgetExhausted, traceID)
		return
	}
	lastVersion := task.ResourceVersion
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()
	keepAlive := time.NewTicker(sseKeepAliveInterval)
	defer keepAlive.Stop()
	for {
		select {
		case <-request.Context().Done():
			// The client disconnected: end the stream.
			return
		case <-keepAlive.C:
			if !writeSSEKeepAlive(writer, controller) {
				return
			}
		case <-poll.C:
			current, err := h.tasks.GetTask(request.Context(), principal.TenantID, id)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// The task vanished: close the stream; the client
					// reconciles with the resource endpoint.
					return
				}
				// Transient store failure: keep the stream alive and retry on
				// the next tick instead of dropping every subscriber.
				continue
			}
			if current.ResourceVersion == lastVersion {
				continue
			}
			lastVersion = current.ResourceVersion
			usage, budgetExhausted := h.readUsage(request.Context(), principal.TenantID, id)
			if !writeTaskEvent(writer, controller, "task.updated", current, usage, budgetExhausted, traceID) {
				return
			}
			if current.Phase.Terminal() {
				_ = writeTaskEvent(writer, controller, "task.terminal", current, usage, budgetExhausted, traceID)
				return
			}
		}
	}
}

// writeTaskEvent emits one SSE event carrying a Task snapshot. It reports
// whether the client is still connected.
func writeTaskEvent(writer http.ResponseWriter, controller *http.ResponseController, event string, task store.Task, usage usageSummary, budgetExhausted bool, traceID string) bool {
	payload, err := json.Marshal(taskResponseFrom(task, usage, budgetExhausted, traceID))
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(writer, "event: %s\nid: %d\ndata: %s\n\n", event, task.ResourceVersion, payload); err != nil {
		return false
	}
	return controller.Flush() == nil
}

// writeSSEKeepAlive emits a comment frame that proxies treat as activity.
func writeSSEKeepAlive(writer http.ResponseWriter, controller *http.ResponseController) bool {
	if _, err := io.WriteString(writer, ": keepalive\n\n"); err != nil {
		return false
	}
	return controller.Flush() == nil
}

// readUsage reads the cumulative consumed budget dimensions of a task.
func (h *Handler) readUsage(ctx context.Context, tenantID string, id uuid.UUID) (usageSummary, bool) {
	status, err := h.tasks.GetTaskBudget(ctx, tenantID, id)
	if err != nil {
		return usageSummary{}, false
	}
	return usageSummary{
		Tokens: status.Consumed.Tokens, CostMicroUSD: status.Consumed.CostMicroUSD,
		ToolCalls: status.Consumed.ToolCalls, WallSeconds: status.Consumed.WallSeconds,
	}, status.Exhausted
}

func (h *Handler) writeTask(writer http.ResponseWriter, status int, task store.Task, usage usageSummary, budgetExhausted bool, traceID string) {
	response := taskResponseFrom(task, usage, budgetExhausted, traceID)
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, task.ResourceVersion))
	writeJSON(writer, status, response)
}

func taskResponseFrom(task store.Task, usage usageSummary, budgetExhausted bool, traceID string) taskResponse {
	response := taskResponse{
		APIVersion: agentversion.APIVersion, Kind: "Task", ID: task.ID.String(),
		TenantID: task.TenantID, Namespace: task.Namespace, AgentVersionRef: task.AgentVersionRef,
		Goal: task.Goal, Spec: task.Spec, Phase: string(task.Phase), ResourceVersion: task.ResourceVersion,
		Usage: usage, BudgetExhausted: budgetExhausted,
		CreatedAt: task.CreatedAt.UTC(), UpdatedAt: task.UpdatedAt.UTC(), TraceID: traceID,
	}
	if task.AgentVersionID != nil {
		value := task.AgentVersionID.String()
		response.AgentVersionID = &value
	}
	if task.ActiveRunID != nil {
		value := task.ActiveRunID.String()
		response.ActiveRunID = &value
	}
	if task.ResultRef != "" {
		response.ResultRef = &task.ResultRef
	}
	if task.AdmissionReasonCode != "" {
		response.ReasonCode = &task.AdmissionReasonCode
	}
	if task.CancelRequestedAt != nil {
		value := task.CancelRequestedAt.UTC()
		response.CancelRequestedAt = &value
	}
	return response
}

func validateCreateTask(body createTaskRequest) error {
	if _, _, _, err := agentversion.ParseRef(body.AgentVersionRef); err != nil {
		return fmt.Errorf("agentVersionRef must be a canonical name@version reference: %v", err)
	}
	if strings.TrimSpace(body.Goal) == "" || len(body.Goal) > 16*1024 {
		return fmt.Errorf("goal is required and must not exceed 16 KiB")
	}
	if strings.TrimSpace(body.Namespace) == "" || len(body.Namespace) > 255 {
		return fmt.Errorf("namespace is required and must not exceed 255 bytes")
	}
	if len(body.Spec) == 0 || string(body.Spec) == "null" {
		return fmt.Errorf("spec is required")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body.Spec, &object); err != nil || object == nil {
		return fmt.Errorf("spec must be a JSON object")
	}
	return nil
}
