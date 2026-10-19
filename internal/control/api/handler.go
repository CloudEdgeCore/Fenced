// Package api implements the operator-facing Control API: the HTTP
// contract for tasks, agent versions, tools, approvals, memories, audit,
// quotas and the runtime health endpoints.
//
// This file holds the assembly of the API: the store interfaces the handler
// depends on, the options that wire them, the route table, and the helpers
// every resource shares. The resources themselves live in one file each.
package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/observability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
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

// Route budgets (N9): reads answer quickly, mutations get the full budget,
// streams are unbounded. Overridable with WithRequestTimeouts.
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

type problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	Instance   string `json:"instance"`
	ReasonCode string `json:"reasonCode"`
	TraceID    string `json:"traceId"`
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
