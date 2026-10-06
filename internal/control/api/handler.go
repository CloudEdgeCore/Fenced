// Package api implements the versioned public REST control contract.
package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/observability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/CloudEdgeCore/Fenced/internal/version"
	"github.com/google/uuid"
)

const maxRequestBody = 1 << 20

// Route budgets (N9): reads answer quickly, mutations get the full budget,
// streams are unbounded. Overridable with WithRequestTimeouts.
const (
	defaultReadTimeout  = 5 * time.Second
	defaultWriteTimeout = 10 * time.Second
	// defaultMaxSSESubscribers bounds concurrent event streams so slow
	// clients cannot exhaust the server (O5).
	defaultMaxSSESubscribers = 64
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

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type TaskStore interface {
	CreateTask(context.Context, store.CreateTaskInput) (store.CreateTaskResult, error)
	GetTask(context.Context, string, uuid.UUID) (store.Task, error)
	RequestTaskCancellation(context.Context, string, uuid.UUID, int64) (store.Task, error)
	GetTaskBudget(context.Context, string, uuid.UUID) (store.TaskBudgetStatus, error)
}

type AgentVersionStore interface {
	CreateAgentVersion(context.Context, store.CreateAgentVersionInput) (store.CreateAgentVersionResult, error)
	GetAgentVersion(context.Context, string, uuid.UUID) (store.AgentVersion, error)
}

// ApprovalStore is the human decision surface for high-risk tool calls:
// capability discovery and bound approval decisions.
type ApprovalStore interface {
	ListToolDescriptors(context.Context, string) ([]store.ToolDescriptor, error)
	GetToolApproval(context.Context, string, uuid.UUID) (store.ToolApproval, error)
	DecideToolApproval(context.Context, store.DecideToolApprovalInput) (store.ToolApproval, error)
}

// MemoryAPI is the v0.1 Memory decision-chain surface (ADR-009): writes embed
// content, retrieval is tenant/sensitivity-filtered before scoring, deletion
// is a CAS tombstone.
type MemoryAPI interface {
	Put(context.Context, memory.PutInput) (store.MemoryRecord, bool, error)
	Get(context.Context, string, uuid.UUID) (store.MemoryRecord, error)
	Search(context.Context, memory.SearchInput) ([]store.MemoryRecord, error)
	Tombstone(context.Context, string, uuid.UUID, int64) (store.MemoryRecord, error)
}

// MetricsStore is the §Phase-7 aggregated observability read surface.
type MetricsStore interface {
	AggregateMetrics(context.Context, string, time.Time) (*observability.Metrics, error)
}

// AuditStore is the v0.4 audit ledger read surface (ADR-014).
type AuditStore interface {
	ListAudit(context.Context, store.ListAuditInput) ([]store.AuditEvent, error)
	VerifyAuditChain(context.Context, string) (store.AuditVerification, error)
	ExportAuditChain(context.Context, string) ([]store.AuditEvent, error)
}

// TenantQuotaStore is the v0.6 tenant aggregate consumption quota surface.
// The endpoints always operate on the authenticated principal's own tenant,
// so no cross-tenant identifier ever appears in a quota URL.
type TenantQuotaStore interface {
	SetTenantQuota(context.Context, store.SetTenantQuotaInput) (store.TenantQuota, error)
	GetTenantQuota(context.Context, string) (store.TenantQuota, error)
	DeleteTenantQuota(context.Context, string) error
	GetTenantQuotaUsage(context.Context, string, time.Time) (store.TenantWindowUsage, error)
}

type RuntimePoolOperatorStore interface {
	UpdateRuntimePoolStatus(context.Context, store.UpdateRuntimePoolStatusInput) (store.RuntimePoolState, error)
}

type Handler struct {
	tasks        TaskStore
	agentVersion AgentVersionStore
	approvals    ApprovalStore
	memories     MemoryAPI
	newID        func() uuid.UUID
	// packages is the ADR-010 publish gate. When non-nil, every publication
	// must carry a package signed by a trusted key (fail-closed). When nil,
	// unsigned dev publications are allowed, but a presented package is
	// still verified.
	packages *agentpkg.Registry
	// audit is the ADR-014 ledger; when nil the audit endpoints answer 404.
	audit AuditStore
	// quotas is the v0.6 tenant aggregate consumption quota surface; when
	// nil the quota endpoints answer 404.
	quotas TenantQuotaStore
	// workflows is the v1.2 orchestration surface; when nil the workflow
	// endpoints answer 404.
	workflows    WorkflowStore
	runtimePools RuntimePoolOperatorStore
	// metrics is the §Phase-7 aggregated observability surface; when nil
	// the metrics endpoint answers 404.
	metrics MetricsStore
	// supervisor and serviceStore provide long-running Agent Service management.
	supervisor   *supervisor.Supervisor
	serviceStore supervisor.Store
	// namespaces provides first-class namespace and resource quota management.
	namespaces namespace.Store
	// auditKeyID / auditSigningKey sign exported audit archives.
	auditKeyID      string
	auditSigningKey ed25519.PrivateKey
	// readTimeout / writeTimeout are the per-route request budgets (N9).
	readTimeout  time.Duration
	writeTimeout time.Duration
	// sseSlots bounds concurrent event streams so slow clients cannot
	// exhaust the server (O5); activeSSE is the live stream count.
	sseSlots  int
	activeSSE atomic.Int64
	readiness func(context.Context) error
}

// Option configures the control handler.
type Option func(*Handler)

// WithPackageAdmission installs the signed Agent Package publish gate
// (ADR-010): publications without a package verified against the trusted key
// registry are rejected.
func WithPackageAdmission(registry *agentpkg.Registry) Option {
	return func(h *Handler) { h.packages = registry }
}

// WithAuditStore installs the audit ledger read surface (ADR-014); without
// it the audit endpoints are disabled.
func WithAuditStore(audit AuditStore) Option {
	return func(h *Handler) { h.audit = audit }
}

// WithWorkflowStore installs the v1.2 workflow orchestration surface;
// without it the workflow endpoints are disabled.
func WithWorkflowStore(workflows WorkflowStore) Option {
	return func(h *Handler) { h.workflows = workflows }
}

func WithRuntimePoolOperatorStore(runtimePools RuntimePoolOperatorStore) Option {
	return func(h *Handler) { h.runtimePools = runtimePools }
}

// WithTenantQuotaStore installs the tenant aggregate consumption quota
// surface (v0.6); without it the quota endpoints are disabled.
func WithTenantQuotaStore(quotas TenantQuotaStore) Option {
	return func(h *Handler) { h.quotas = quotas }
}

// WithMetricsStore installs the §Phase-7 aggregated observability surface;
// without it the metrics endpoint answers 404.
func WithMetricsStore(metrics MetricsStore) Option {
	return func(h *Handler) { h.metrics = metrics }
}

// WithSupervisor installs the Agent Service supervisor and store;
// without it the service endpoints answer 404.
func WithSupervisor(sup *supervisor.Supervisor, store supervisor.Store) Option {
	return func(h *Handler) {
		h.supervisor = sup
		h.serviceStore = store
	}
}

// WithNamespaceStore installs the namespace store; without it namespace endpoints answer 404.
func WithNamespaceStore(store namespace.Store) Option {
	return func(h *Handler) {
		h.namespaces = store
	}
}

// WithAuditSigningKey configures the key that signs exported audit archives.
func WithAuditSigningKey(keyID string, key ed25519.PrivateKey) Option {
	return func(h *Handler) { h.auditKeyID, h.auditSigningKey = keyID, key }
}

// WithRequestTimeouts overrides the per-route read and write budgets (N9);
// non-positive values disable the bound for that class.
func WithRequestTimeouts(readTimeout, writeTimeout time.Duration) Option {
	return func(h *Handler) { h.readTimeout, h.writeTimeout = readTimeout, writeTimeout }
}

// WithMaxSSESubscribers bounds concurrent task event streams (O5).
func WithMaxSSESubscribers(limit int) Option {
	return func(h *Handler) {
		if limit > 0 {
			h.sseSlots = limit
		}
	}
}

// WithReadiness installs the dependency probe used by /readyz. Liveness is
// process-only; readiness must fail during database outages and rolling
// migration windows so load balancers stop routing new work.
func WithReadiness(probe func(context.Context) error) Option {
	return func(h *Handler) { h.readiness = probe }
}

func NewHandler(taskStore TaskStore, agentVersions AgentVersionStore, approvals ApprovalStore, memories MemoryAPI, options ...Option) http.Handler {
	handler := &Handler{
		tasks:        taskStore,
		agentVersion: agentVersions,
		approvals:    approvals,
		memories:     memories,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		sseSlots:     defaultMaxSSESubscribers,
		newID: func() uuid.UUID {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.New()
			}
			return id
		},
	}
	for _, option := range options {
		option(handler)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tasks", handler.createTask)
	mux.HandleFunc("GET /v1/tasks/{taskID}", handler.getTask)
	mux.HandleFunc("GET /v1/tasks/{taskID}/events", handler.taskEvents)
	mux.HandleFunc("POST /v1/tasks/{taskAction}", handler.cancelTask)
	mux.HandleFunc("POST /v1/agent-versions", handler.createAgentVersion)
	mux.HandleFunc("GET /v1/agent-versions/{agentVersionID}", handler.getAgentVersion)
	mux.HandleFunc("GET /v1/tools", handler.listTools)
	mux.HandleFunc("POST /v1/approvals/{approvalAction}", handler.decideApproval)
	mux.HandleFunc("POST /v1/memories", handler.createMemory)
	mux.HandleFunc("GET /v1/memories", handler.searchMemory)
	mux.HandleFunc("GET /v1/memories/{memoryID}", handler.getMemory)
	mux.HandleFunc("DELETE /v1/memories/{memoryID}", handler.tombstoneMemory)
	mux.HandleFunc("GET /v1/audit", handler.listAudit)
	mux.HandleFunc("GET /v1/audit/verify", handler.verifyAudit)
	mux.HandleFunc("GET /v1/audit/export", handler.exportAudit)
	mux.HandleFunc("GET /v1/quota", handler.getTenantQuota)
	mux.HandleFunc("PUT /v1/quota", handler.setTenantQuota)
	mux.HandleFunc("DELETE /v1/quota", handler.deleteTenantQuota)
	mux.HandleFunc("POST /v1/workflows", handler.createWorkflow)
	mux.HandleFunc("GET /v1/metrics", handler.getMetrics)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", handler.getWorkflow)
	mux.HandleFunc("POST /v1/workflows/{workflowID}/cancel", handler.cancelWorkflow)
	mux.HandleFunc("POST /v1/workflows/{workflowID}/steps/{stepName}/approval", handler.decideWorkflowStepApproval)
	mux.HandleFunc("PUT /v1/runtime-pools/{poolID}/status", handler.updateRuntimePoolStatus)
	mux.HandleFunc("POST /v1/services", handler.createService)
	mux.HandleFunc("GET /v1/services", handler.listServices)
	mux.HandleFunc("GET /v1/services/{serviceID}", handler.getService)
	mux.HandleFunc("PUT /v1/services/{serviceID}", handler.updateService)
	mux.HandleFunc("DELETE /v1/services/{serviceID}", handler.deleteService)
	mux.HandleFunc("POST /v1/services/{serviceID}/scale", handler.scaleService)
	mux.HandleFunc("POST /v1/services/{serviceID}/restart", handler.restartService)
	mux.HandleFunc("POST /v1/services/{serviceID}/stop", handler.stopService)
	mux.HandleFunc("GET /v1/services/{serviceID}/instances", handler.listServiceInstances)
	mux.HandleFunc("POST /v1/services/{serviceID}/instances/{instanceID}/heartbeat", handler.heartbeatServiceInstance)
	mux.HandleFunc("POST /v1/namespaces", handler.createNamespace)
	mux.HandleFunc("GET /v1/namespaces", handler.listNamespaces)
	mux.HandleFunc("GET /v1/namespaces/{name}", handler.getNamespace)
	mux.HandleFunc("PUT /v1/namespaces/{name}", handler.updateNamespace)
	mux.HandleFunc("DELETE /v1/namespaces/{name}", handler.deleteNamespace)
	mux.HandleFunc("GET /v1/namespaces/{name}/usage", handler.getNamespaceUsage)
	mux.HandleFunc("GET /healthz", handler.health)
	mux.HandleFunc("GET /readyz", handler.ready)
	mux.HandleFunc("GET /versionz", handler.version)
	mux.HandleFunc("/v1/namespaces", handler.methodNotAllowed)
	mux.HandleFunc("/v1/namespaces/{name}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/namespaces/{name}/usage", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services/{serviceID}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services/{serviceID}/scale", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services/{serviceID}/restart", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services/{serviceID}/stop", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services/{serviceID}/instances", handler.methodNotAllowed)
	mux.HandleFunc("/v1/services/{serviceID}/instances/{instanceID}/heartbeat", handler.methodNotAllowed)
	mux.HandleFunc("/v1/workflows", handler.methodNotAllowed)
	mux.HandleFunc("/v1/workflows/{workflowID}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/workflows/{workflowID}/cancel", handler.methodNotAllowed)
	mux.HandleFunc("/v1/workflows/{workflowID}/steps/{stepName}/approval", handler.methodNotAllowed)
	mux.HandleFunc("/v1/runtime-pools/{poolID}/status", handler.methodNotAllowed)
	mux.HandleFunc("/v1/tasks", handler.methodNotAllowed)
	mux.HandleFunc("/v1/tasks/{taskID}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/tasks/{taskID}/events", handler.methodNotAllowed)
	mux.HandleFunc("/v1/agent-versions", handler.methodNotAllowed)
	mux.HandleFunc("/v1/agent-versions/{agentVersionID}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/tools", handler.methodNotAllowed)
	mux.HandleFunc("/v1/approvals/{approvalAction}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/memories", handler.methodNotAllowed)
	mux.HandleFunc("/v1/memories/{memoryID}", handler.methodNotAllowed)
	mux.HandleFunc("/v1/audit", handler.methodNotAllowed)
	mux.HandleFunc("/v1/audit/verify", handler.methodNotAllowed)
	mux.HandleFunc("/v1/audit/export", handler.methodNotAllowed)
	mux.HandleFunc("/v1/quota", handler.methodNotAllowed)
	mux.HandleFunc("/healthz", handler.methodNotAllowed)
	mux.HandleFunc("/readyz", handler.methodNotAllowed)
	mux.HandleFunc("/versionz", handler.methodNotAllowed)
	mux.HandleFunc("/", handler.notFound)
	return handler.requestContext(mux)
}

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

type problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	Instance   string `json:"instance"`
	ReasonCode string `json:"reasonCode"`
	TraceID    string `json:"traceId"`
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

type createAgentVersionRequest struct {
	Name      string          `json:"name"`
	Version   string          `json:"version"`
	Namespace string          `json:"namespace"`
	Spec      json.RawMessage `json:"spec"`
	// Manifest is the v0.9 portable publication contract. It is mutually
	// exclusive with the legacy name/version/namespace/spec fields and is
	// normalized into those fields before package admission and persistence.
	Manifest *agentversion.Manifest `json:"manifest,omitempty"`
	// Package is the signed Agent Package (ADR-010). With publish admission
	// installed it is required; without it, a presented package is still
	// verified fail-closed.
	Package *agentpkg.Package `json:"package,omitempty"`
}

type agentVersionResponse struct {
	APIVersion            string          `json:"apiVersion"`
	Kind                  string          `json:"kind"`
	ID                    string          `json:"id"`
	TenantID              string          `json:"tenantId"`
	Namespace             string          `json:"namespace"`
	Name                  string          `json:"name"`
	Version               string          `json:"version"`
	Ref                   string          `json:"ref"`
	Spec                  json.RawMessage `json:"spec"`
	SpecDigest            string          `json:"specDigest"`
	ResourceVersion       int64           `json:"resourceVersion"`
	CreatedAt             time.Time       `json:"createdAt"`
	TraceID               string          `json:"traceId"`
	PackageKeyID          string          `json:"packageKeyId,omitempty"`
	PackageManifestDigest string          `json:"packageManifestDigest,omitempty"`
}

func (h *Handler) createAgentVersion(writer http.ResponseWriter, request *http.Request) {
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
	var body createAgentVersionRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	body, err = normalizeCreateAgentVersion(body)
	if err != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_AGENT_VERSION", err.Error(), traceID)
		return
	}
	if err := validateCreateAgentVersion(body); err != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_AGENT_VERSION", err.Error(), traceID)
		return
	}
	var packageSignature *store.PackageSignature
	if body.Package != nil {
		if err := h.admitPackage(body); err != nil {
			h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PACKAGE_SIGNATURE_INVALID", err.Error(), traceID)
			return
		}
		manifestDigest, err := agentpkg.ManifestDigest(body.Package.Manifest)
		if err != nil {
			h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PACKAGE_SIGNATURE_INVALID", err.Error(), traceID)
			return
		}
		packageSignature = &store.PackageSignature{
			KeyID: body.Package.Signature.KeyID, Signature: body.Package.Signature.Ed25519,
			ManifestDigest: hex.EncodeToString(manifestDigest[:]),
		}
	} else if h.packages != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PACKAGE_REQUIRED", "publication requires a signed agent package verified against the trust registry", traceID)
		return
	}
	result, err := h.agentVersion.CreateAgentVersion(request.Context(), store.CreateAgentVersionInput{
		ID: h.newID(), TenantID: principal.TenantID, Namespace: body.Namespace,
		Name: body.Name, Version: body.Version, Spec: body.Spec,
		PackageSignature: packageSignature,
	})
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	status := http.StatusCreated
	if result.Existing {
		status = http.StatusOK
		writer.Header().Set("Idempotent-Replayed", "true")
	}
	writer.Header().Set("Location", "/v1/agent-versions/"+result.AgentVersion.ID.String())
	h.writeAgentVersion(writer, status, result.AgentVersion, traceID)
}

func (h *Handler) getAgentVersion(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("agentVersionID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "AGENT_VERSION_NOT_FOUND", "agent version was not found", traceID)
		return
	}
	version, err := h.agentVersion.GetAgentVersion(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	h.writeAgentVersion(writer, http.StatusOK, version, traceID)
}

// listTools exposes the tenant's registered tool descriptors for capability
// discovery.
func (h *Handler) listTools(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	descriptors, err := h.approvals.ListToolDescriptors(request.Context(), principal.TenantID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed", traceID)
		return
	}
	tools := make([]toolDescriptorResponse, 0, len(descriptors))
	for _, descriptor := range descriptors {
		tools = append(tools, toolDescriptorResponse{
			Name: descriptor.Name, Version: descriptor.Version,
			SideEffectRisk: string(descriptor.SideEffectRisk),
			Actions:        descriptor.Actions, ResourcePatterns: descriptor.ResourcePatterns,
			ParamsSchema: descriptor.ParamsSchema, SpecDigest: fmt.Sprintf("%x", descriptor.SpecHash),
			CreatedAt: descriptor.CreatedAt,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"apiVersion": agentversion.APIVersion, "kind": "ToolList", "tools": tools, "traceId": traceID,
	})
}

type decideApprovalRequest struct {
	Decision  string `json:"decision"`
	DecidedBy string `json:"decidedBy"`
}

type toolApprovalResponse struct {
	ID              string     `json:"id"`
	TenantID        string     `json:"tenantId"`
	CallID          string     `json:"callId"`
	TaskID          string     `json:"taskId"`
	RunID           string     `json:"runId"`
	AttemptID       string     `json:"attemptId"`
	ToolName        string     `json:"toolName"`
	ToolVersion     string     `json:"toolVersion"`
	Action          string     `json:"action"`
	Resource        string     `json:"resource"`
	ArgsHash        string     `json:"argsHash"`
	Status          string     `json:"status"`
	RequestedAt     time.Time  `json:"requestedAt"`
	ExpiresAt       time.Time  `json:"expiresAt"`
	DecidedAt       *time.Time `json:"decidedAt,omitempty"`
	DecidedBy       string     `json:"decidedBy,omitempty"`
	ResourceVersion int64      `json:"resourceVersion"`
	TraceID         string     `json:"traceId"`
}

type toolDescriptorResponse struct {
	Name             string          `json:"name"`
	Version          string          `json:"version"`
	SideEffectRisk   string          `json:"sideEffectRisk"`
	Actions          []string        `json:"actions"`
	ResourcePatterns []string        `json:"resourcePatterns"`
	ParamsSchema     json.RawMessage `json:"paramsSchema"`
	SpecDigest       string          `json:"specDigest"`
	CreatedAt        time.Time       `json:"createdAt"`
}

// decideApproval is the human decision endpoint for high-risk tool calls
// (POST /v1/approvals/{approvalId}:decide). The decision is bound to the
// canonical call summary and expires with the approval.
func (h *Handler) decideApproval(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	approvalRef, ok := strings.CutSuffix(request.PathValue("approvalAction"), ":decide")
	if !ok {
		h.notFound(writer, request)
		return
	}
	id, err := uuid.Parse(approvalRef)
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "APPROVAL_NOT_FOUND", "approval was not found", traceID)
		return
	}
	expectedVersion, err := parseEntityVersion(request.Header.Get("If-Match"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusPreconditionRequired, "IF_MATCH_REQUIRED", "If-Match must contain the current weak resource-version ETag", traceID)
		return
	}
	var body decideApprovalRequest
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
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	decision := store.ToolApprovalStatus(body.Decision)
	if decision != store.ToolApprovalApproved && decision != store.ToolApprovalRejected {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_APPROVAL_DECISION",
			"decision must be APPROVED or REJECTED", traceID)
		return
	}
	if strings.TrimSpace(body.DecidedBy) == "" {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "DECIDED_BY_REQUIRED",
			"decidedBy must identify the human decision maker", traceID)
		return
	}
	approval, err := h.approvals.DecideToolApproval(request.Context(), store.DecideToolApprovalInput{
		TenantID: principal.TenantID, ApprovalID: id, ExpectedVersion: expectedVersion,
		Decision: decision, DecidedBy: body.DecidedBy, Now: time.Now().UTC(),
	})
	if err != nil {
		h.writeApprovalProblem(writer, request, err, traceID)
		return
	}
	h.writeApproval(writer, http.StatusOK, approval, traceID)
}

func (h *Handler) writeApproval(writer http.ResponseWriter, status int, approval store.ToolApproval, traceID string) {
	response := toolApprovalResponse{
		ID: approval.ID.String(), TenantID: approval.TenantID, CallID: approval.CallID.String(),
		TaskID: approval.TaskID.String(), RunID: approval.RunID.String(), AttemptID: approval.AttemptID.String(),
		ToolName: approval.ToolName, ToolVersion: approval.ToolVersion, Action: approval.Action,
		Resource: approval.Resource, ArgsHash: fmt.Sprintf("%x", approval.ArgsHash),
		Status: string(approval.Status), RequestedAt: approval.RequestedAt, ExpiresAt: approval.ExpiresAt,
		DecidedAt: approval.DecidedAt, DecidedBy: approval.DecidedBy,
		ResourceVersion: approval.ResourceVersion, TraceID: traceID,
	}
	writeJSON(writer, status, response)
}

func (h *Handler) writeApprovalProblem(writer http.ResponseWriter, request *http.Request, err error, traceID string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		h.writeProblem(writer, request, http.StatusNotFound, "APPROVAL_NOT_FOUND", "approval was not found", traceID)
	case errors.Is(err, store.ErrVersionConflict):
		h.writeProblem(writer, request, http.StatusConflict, "RESOURCE_VERSION_CONFLICT", "resource changed concurrently", traceID)
	case errors.Is(err, store.ErrInvalidTransition):
		h.writeProblem(writer, request, http.StatusConflict, "APPROVAL_STATE_CONFLICT", "approval is not pending", traceID)
	case errors.Is(err, store.ErrApprovalNotUsable):
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "APPROVAL_EXPIRED", "approval request has expired", traceID)
	default:
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed", traceID)
	}
}

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

func (p *handlerProblem) write(writer http.ResponseWriter, request *http.Request, traceID string) {
	(&Handler{}).writeProblem(writer, request, p.status, p.reason, p.detail, traceID)
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

func (h *Handler) requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx := request.Context()
		path := request.URL.Path
		if strings.HasSuffix(path, "/events") {
			// Event streams live until the client disconnects or the task
			// reaches a terminal phase; no request budget applies.
		} else if strings.HasSuffix(path, "/audit/export") {
			// Exports walk the whole ledger: give them the write budget.
			if h.writeTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(request.Context(), h.writeTimeout)
				defer cancel()
			}
		} else {
			// Per-route budgets (N9): reads answer quickly, mutations get
			// the full budget.
			budget := h.writeTimeout
			if request.Method == http.MethodGet || request.Method == http.MethodHead {
				budget = h.readTimeout
			}
			if budget > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(request.Context(), budget)
				defer cancel()
			}
		}
		traceID := request.Header.Get("X-Request-ID")
		if _, err := uuid.Parse(traceID); err != nil {
			traceID = h.newID().String()
		}
		writer.Header().Set("X-Request-ID", traceID)
		next.ServeHTTP(writer, request.WithContext(context.WithValue(ctx, traceKey{}, traceID)))
	})
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

func (h *Handler) writeAgentVersion(writer http.ResponseWriter, status int, version store.AgentVersion, traceID string) {
	response := agentVersionResponse{
		APIVersion: agentversion.APIVersion, Kind: agentversion.Kind, ID: version.ID.String(),
		TenantID: version.TenantID, Namespace: version.Namespace, Name: version.Name,
		Version: version.Version, Ref: version.Ref(), Spec: version.Spec,
		SpecDigest: fmt.Sprintf("%x", version.SpecDigest), ResourceVersion: version.ResourceVersion,
		CreatedAt: version.CreatedAt.UTC(), TraceID: traceID,
	}
	if version.PackageSignature != nil {
		response.PackageKeyID = version.PackageSignature.KeyID
		response.PackageManifestDigest = version.PackageSignature.ManifestDigest
	}
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, version.ResourceVersion))
	writeJSON(writer, status, response)
}

// admitPackage is the ADR-010 publish gate. Verification is fail-closed
// (signature, trusted key identity, canonical manifest) and additionally
// binds the signed manifest to this exact publication: the manifest must sign
// the same name@version reference and the same spec bytes being published.
func (h *Handler) admitPackage(body createAgentVersionRequest) error {
	if h.packages == nil {
		// Dev mode without a trust registry: unsigned publications are
		// allowed, but a presented package is still verified fail-closed
		// against the empty trust set (every key is unknown).
		return agentpkg.Verify(body.Package, nil)
	}
	if err := h.packages.Verify(body.Package); err != nil {
		return err
	}
	publicationRef := agentversion.FormatRef(body.Namespace, body.Name, body.Version)
	if body.Package.Manifest.AgentVersionRef != publicationRef {
		return fmt.Errorf("%w: manifest signs %q but the publication is %q",
			agentpkg.ErrPackageBindingMismatch, body.Package.Manifest.AgentVersionRef, publicationRef)
	}
	if !body.Package.Manifest.SpecDigest.Verify(body.Spec) {
		return fmt.Errorf("%w: spec does not match the signed spec digest", agentpkg.ErrPackageBindingMismatch)
	}
	return nil
}

// --- audit ledger (ADR-014) ---

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

func (h *Handler) writeStoreProblem(writer http.ResponseWriter, request *http.Request, err error, traceID string) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		h.writeProblem(writer, request, http.StatusGatewayTimeout, "REQUEST_TIMEOUT", "request exceeded its route budget", traceID)
	case errors.Is(err, store.ErrNotFound):
		h.writeProblem(writer, request, http.StatusNotFound, "TASK_NOT_FOUND", "task was not found", traceID)
	case errors.Is(err, store.ErrIdempotencyConflict):
		h.writeProblem(writer, request, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency key was already used for a different request", traceID)
	case errors.Is(err, store.ErrVersionConflict):
		h.writeProblem(writer, request, http.StatusConflict, "RESOURCE_VERSION_CONFLICT", "resource changed concurrently", traceID)
	case errors.Is(err, store.ErrInvalidTransition):
		h.writeProblem(writer, request, http.StatusConflict, "TASK_STATE_CONFLICT", "task lifecycle does not allow this operation", traceID)
	case errors.Is(err, store.ErrAgentVersionConflict):
		h.writeProblem(writer, request, http.StatusConflict, "AGENT_VERSION_CONFLICT", "agent version identity was already published with a different spec", traceID)
	case errors.Is(err, store.ErrPoolOperatorDenied):
		h.writeProblem(writer, request, http.StatusForbidden, "RUNTIME_POOL_OPERATOR_DENIED", "principal holds no operator grant for this runtime pool", traceID)
	default:
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed", traceID)
	}
}

func (h *Handler) writeDecodeProblem(writer http.ResponseWriter, request *http.Request, err error, traceID string) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		h.writeProblem(writer, request, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "request body exceeds 1 MiB", traceID)
		return
	}
	h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", "request body is not valid for this API version", traceID)
}

func (h *Handler) writeProblem(writer http.ResponseWriter, request *http.Request, status int, reasonCode, detail, traceID string) {
	writer.Header().Set("Content-Type", "application/problem+json")
	writeJSON(writer, status, problem{
		Type:  "https://fenced.dev/problems/" + strings.ToLower(strings.ReplaceAll(reasonCode, "_", "-")),
		Title: http.StatusText(status), Status: status, Detail: detail,
		Instance: request.URL.Path, ReasonCode: reasonCode, TraceID: traceID,
	})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	if writer.Header().Get("Content-Type") == "" {
		writer.Header().Set("Content-Type", "application/json")
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
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

func validateCreateAgentVersion(body createAgentVersionRequest) error {
	if err := agentversion.ValidateName(body.Name); err != nil {
		return err
	}
	if err := agentversion.ValidateVersion(body.Version); err != nil {
		return err
	}
	if err := agentversion.ValidateNamespace(body.Namespace); err != nil {
		return err
	}
	if err := agentversion.ValidateSpec(body.Spec); err != nil {
		return err
	}
	return nil
}

func normalizeCreateAgentVersion(body createAgentVersionRequest) (createAgentVersionRequest, error) {
	if body.Manifest == nil {
		return body, nil
	}
	if body.Name != "" || body.Version != "" || body.Namespace != "" || len(body.Spec) != 0 {
		return body, fmt.Errorf("manifest cannot be combined with name, version, namespace, or spec")
	}
	if err := body.Manifest.Validate(); err != nil {
		return body, err
	}
	spec, err := json.Marshal(body.Manifest.Spec)
	if err != nil {
		return body, fmt.Errorf("canonicalize agent manifest spec: %w", err)
	}
	body.Name = body.Manifest.Metadata.Name
	body.Version = body.Manifest.Metadata.Version
	body.Namespace = body.Manifest.Metadata.Namespace
	body.Spec = spec
	return body, nil
}

func parseEntityVersion(value string) (int64, error) {
	if len(value) < 5 || !strings.HasPrefix(value, `W/"`) || !strings.HasSuffix(value, `"`) {
		return 0, fmt.Errorf("weak ETag is required")
	}
	version, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(value, `W/"`), `"`), 10, 64)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("weak ETag is invalid")
	}
	if value != fmt.Sprintf(`W/"%d"`, version) {
		return 0, fmt.Errorf("weak ETag is not canonical")
	}
	return version, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return fmt.Errorf("request body must contain exactly one JSON document")
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return fmt.Errorf("request body contains invalid or ambiguous JSON")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("array is not closed")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return nil
}

type traceKey struct{}

func traceIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(traceKey{}).(string)
	return value
}
