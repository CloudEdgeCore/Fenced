package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
)

type createServiceRequest struct {
	ID        string                 `json:"id,omitempty"`
	Namespace string                 `json:"namespace"`
	Name      string                 `json:"name"`
	AgentID   string                 `json:"agentId"`
	Spec      supervisor.ServiceSpec `json:"spec"`
}

type updateServiceRequest struct {
	Spec supervisor.ServiceSpec `json:"spec"`
}

type scaleServiceRequest struct {
	Replicas int `json:"replicas"`
}

type rolloutServiceRequest struct {
	NewVersion string                    `json:"newVersion"`
	Config     *supervisor.RolloutConfig `json:"config,omitempty"`
}

type serviceResponse struct {
	APIVersion string                   `json:"apiVersion"`
	Kind       string                   `json:"kind"`
	ID         string                   `json:"id"`
	TenantID   string                   `json:"tenantId"`
	Namespace  string                   `json:"namespace"`
	Name       string                   `json:"name"`
	AgentID    string                   `json:"agentId"`
	Spec       supervisor.ServiceSpec   `json:"spec"`
	Status     supervisor.ServiceStatus `json:"status"`
	CreatedAt  time.Time                `json:"createdAt"`
	UpdatedAt  time.Time                `json:"updatedAt"`
	TraceID    string                   `json:"traceId"`
}

func formatServiceResponse(svc *supervisor.Service, traceID string) serviceResponse {
	return serviceResponse{
		APIVersion: "fenced.dev/v1",
		Kind:       "AgentService",
		ID:         svc.ID,
		TenantID:   svc.TenantID,
		Namespace:  svc.Namespace,
		Name:       svc.Name,
		AgentID:    svc.AgentID,
		Spec:       svc.Spec,
		Status:     svc.Status,
		CreatedAt:  svc.CreatedAt.UTC(),
		UpdatedAt:  svc.UpdatedAt.UTC(),
		TraceID:    traceID,
	}
}

// rolloutResponse is the wire shape of the rollout of a service.
type rolloutResponse struct {
	APIVersion      string                  `json:"apiVersion"`
	Kind            string                  `json:"kind"`
	ServiceID       string                  `json:"serviceId"`
	TenantID        string                  `json:"tenantId"`
	Namespace       string                  `json:"namespace"`
	Phase           supervisor.RolloutPhase `json:"phase"`
	FromVersion     string                  `json:"fromVersion,omitempty"`
	TargetVersion   string                  `json:"targetVersion,omitempty"`
	ActiveVersion   string                  `json:"activeVersion,omitempty"`
	PreviousVersion string                  `json:"previousVersion,omitempty"`
	DesiredReplicas int                     `json:"desiredReplicas"`
	ReadyReplicas   int                     `json:"readyReplicas"`
	UpdatedReplicas int                     `json:"updatedReplicas"`
	StartedAt       time.Time               `json:"startedAt,omitempty"`
	UpdatedAt       time.Time               `json:"updatedAt,omitempty"`
	FailureReason   string                  `json:"failureReason,omitempty"`
	Message         string                  `json:"message,omitempty"`
	TraceID         string                  `json:"traceId"`
}

func formatRolloutResponse(svc *supervisor.Service, traceID string) rolloutResponse {
	return rolloutResponse{
		APIVersion:      "fenced.dev/v1",
		Kind:            "ServiceRollout",
		ServiceID:       svc.ID,
		TenantID:        svc.TenantID,
		Namespace:       svc.Namespace,
		Phase:           svc.Status.RolloutPhase,
		FromVersion:     svc.Status.RolloutFromVersion,
		TargetVersion:   svc.Status.RolloutTargetVersion,
		ActiveVersion:   svc.Status.ActiveVersion,
		PreviousVersion: svc.Status.PreviousVersion,
		DesiredReplicas: svc.Status.DesiredReplicas,
		ReadyReplicas:   svc.Status.ReadyReplicas,
		UpdatedReplicas: svc.Status.UpdatedReplicas,
		StartedAt:       svc.Status.RolloutStartedAt.UTC(),
		UpdatedAt:       svc.Status.RolloutUpdatedAt.UTC(),
		FailureReason:   svc.Status.RolloutFailureReason,
		Message:         svc.Status.Message,
		TraceID:         traceID,
	}
}

type instanceResponse struct {
	APIVersion    string                   `json:"apiVersion"`
	Kind          string                   `json:"kind"`
	ID            string                   `json:"id"`
	ServiceID     string                   `json:"serviceId"`
	TenantID      string                   `json:"tenantId"`
	Namespace     string                   `json:"namespace"`
	AgentID       string                   `json:"agentId"`
	TaskID        *uuid.UUID               `json:"taskId,omitempty"`
	AgentVersion  string                   `json:"agentVersion,omitempty"`
	RuntimeClass  string                   `json:"runtimeClass,omitempty"`
	FencingToken  uint64                   `json:"fencingToken,omitempty"`
	Address       string                   `json:"address"`
	Phase         supervisor.InstancePhase `json:"phase"`
	RestartCount  int                      `json:"restartCount"`
	LastHeartbeat time.Time                `json:"lastHeartbeat"`
	CreatedAt     time.Time                `json:"createdAt"`
	ExitCode      int                      `json:"exitCode,omitempty"`
	ExitReason    string                   `json:"exitReason,omitempty"`
	TraceID       string                   `json:"traceId"`
}

func formatInstanceResponse(inst *supervisor.Instance, traceID string) instanceResponse {
	return instanceResponse{
		APIVersion:    "fenced.dev/v1",
		Kind:          "ServiceInstance",
		ID:            inst.ID,
		ServiceID:     inst.ServiceID,
		TenantID:      inst.TenantID,
		Namespace:     inst.Namespace,
		AgentID:       inst.AgentID,
		TaskID:        inst.TaskID,
		AgentVersion:  inst.AgentVersion,
		RuntimeClass:  inst.RuntimeClass,
		FencingToken:  inst.FencingToken,
		Address:       inst.Address.String(),
		Phase:         inst.Phase,
		RestartCount:  inst.RestartCount,
		LastHeartbeat: inst.LastHeartbeat.UTC(),
		CreatedAt:     inst.CreatedAt.UTC(),
		ExitCode:      inst.ExitCode,
		ExitReason:    inst.ExitReason,
		TraceID:       traceID,
	}
}

// createService creates a new supervised AgentService (POST /v1/services).
func (h *Handler) createService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeProblem(writer, request, http.StatusUnsupportedMediaType, "CONTENT_TYPE_REQUIRED", "Content-Type must be application/json", traceID)
		return
	}

	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBody)
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := rejectDuplicateJSONKeys(bodyBytes); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "DUPLICATE_KEY", err.Error(), traceID)
		return
	}

	var req createServiceRequest
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("invalid service payload: %v", err), traceID)
		return
	}

	if req.Namespace == "" {
		req.Namespace = "default"
	}
	if strings.TrimSpace(req.Name) == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_NAME", "name is required", traceID)
		return
	}
	if strings.TrimSpace(req.AgentID) == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_AGENT_ID", "agentId is required", traceID)
		return
	}

	svc := &supervisor.Service{
		ID:        req.ID,
		TenantID:  principal.TenantID,
		Namespace: req.Namespace,
		Name:      req.Name,
		AgentID:   req.AgentID,
		Spec:      req.Spec,
	}

	created, err := h.supervisor.CreateService(request.Context(), svc)
	if err != nil {
		if errors.Is(err, supervisor.ErrServiceAlreadyExists) {
			h.writeProblem(writer, request, http.StatusConflict, "SERVICE_ALREADY_EXISTS", err.Error(), traceID)
			return
		}
		if errors.Is(err, supervisor.ErrInvalidServiceSpec) {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_SPEC", err.Error(), traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writeJSON(writer, http.StatusCreated, formatServiceResponse(created, traceID))
}

// listServices lists services in the tenant (GET /v1/services).
func (h *Handler) listServices(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	namespace := request.URL.Query().Get("namespace")
	services, err := h.supervisor.ListServices(request.Context(), principal.TenantID, namespace)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	res := make([]serviceResponse, 0, len(services))
	for _, s := range services {
		res = append(res, formatServiceResponse(s, traceID))
	}
	writeJSON(writer, http.StatusOK, res)
}

// getService fetches a single service (GET /v1/services/{serviceID}).
func (h *Handler) getService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	svc, err := h.supervisor.GetService(request.Context(), principal.TenantID, serviceID)
	if err != nil {
		if errors.Is(err, supervisor.ErrServiceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writeJSON(writer, http.StatusOK, formatServiceResponse(svc, traceID))
}

// updateService updates a service specification (PUT /v1/services/{serviceID}).
func (h *Handler) updateService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeProblem(writer, request, http.StatusUnsupportedMediaType, "CONTENT_TYPE_REQUIRED", "Content-Type must be application/json", traceID)
		return
	}

	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBody)
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}

	var req updateServiceRequest
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("invalid service update payload: %v", err), traceID)
		return
	}

	existing, err := h.supervisor.GetService(request.Context(), principal.TenantID, serviceID)
	if err != nil {
		if errors.Is(err, supervisor.ErrServiceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	existing.Spec = req.Spec
	updated, err := h.supervisor.UpdateService(request.Context(), existing)
	if err != nil {
		if errors.Is(err, supervisor.ErrInvalidServiceSpec) {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_SPEC", err.Error(), traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writeJSON(writer, http.StatusOK, formatServiceResponse(updated, traceID))
}

// scaleService adjusts the replica count of a service (POST /v1/services/{serviceID}/scale).
func (h *Handler) scaleService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBody)
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}

	var req scaleServiceRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_REQUEST", "invalid scale request payload", traceID)
		return
	}

	scaled, err := h.supervisor.ScaleService(request.Context(), principal.TenantID, serviceID, req.Replicas)
	if err != nil {
		if errors.Is(err, supervisor.ErrServiceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
			return
		}
		if errors.Is(err, supervisor.ErrInvalidServiceSpec) {
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_REPLICAS", err.Error(), traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writeJSON(writer, http.StatusOK, formatServiceResponse(scaled, traceID))
}

// restartService restarts all instances of a service (POST /v1/services/{serviceID}/restart).
func (h *Handler) restartService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	if err := h.supervisor.RestartService(request.Context(), principal.TenantID, serviceID); err != nil {
		if errors.Is(err, supervisor.ErrServiceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	svc, _ := h.supervisor.GetService(request.Context(), principal.TenantID, serviceID)
	writeJSON(writer, http.StatusOK, formatServiceResponse(svc, traceID))
}

// rolloutService moves a service to another agent version without dropping
// it (POST /v1/services/{serviceID}/rollout). A serving instance is only
// replaced after its replacement has proven itself, and a rollout that fails
// returns the service to the version it ran before when the service asked
// for an automatic rollback.
func (h *Handler) rolloutService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	var body rolloutServiceRequest
	if err := json.NewDecoder(io.LimitReader(request.Body, 1<<20)).Decode(&body); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_REQUEST", "request body is not a valid rollout request", traceID)
		return
	}
	if strings.TrimSpace(body.NewVersion) == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_REQUEST", "newVersion is required", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	svc, err := h.supervisor.RolloutUpgradeWithConfig(request.Context(), principal.TenantID, serviceID, body.NewVersion, body.Config)
	if err != nil {
		h.writeServiceProblem(writer, request, err, traceID)
		return
	}
	writeJSON(writer, http.StatusAccepted, formatServiceResponse(svc, traceID))
}

// rollbackService returns a service to the version it ran before
// (POST /v1/services/{serviceID}/rollback).
func (h *Handler) rollbackService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	svc, err := h.supervisor.RollbackService(request.Context(), principal.TenantID, serviceID)
	if err != nil {
		h.writeServiceProblem(writer, request, err, traceID)
		return
	}
	writeJSON(writer, http.StatusAccepted, formatServiceResponse(svc, traceID))
}

// getServiceRollout reports the rollout of a service
// (GET /v1/services/{serviceID}/rollout).
func (h *Handler) getServiceRollout(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	svc, err := h.supervisor.GetService(request.Context(), principal.TenantID, serviceID)
	if err != nil {
		h.writeServiceProblem(writer, request, err, traceID)
		return
	}
	writeJSON(writer, http.StatusOK, formatRolloutResponse(svc, traceID))
}

// writeServiceProblem maps a supervisor failure to a problem document.
func (h *Handler) writeServiceProblem(writer http.ResponseWriter, request *http.Request, err error, traceID string) {
	switch {
	case errors.Is(err, supervisor.ErrServiceNotFound):
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
	case errors.Is(err, supervisor.ErrRolloutInProgress):
		h.writeProblem(writer, request, http.StatusConflict, "ROLLOUT_IN_PROGRESS", err.Error(), traceID)
	case errors.Is(err, supervisor.ErrNoPreviousVersion):
		h.writeProblem(writer, request, http.StatusConflict, "NO_PREVIOUS_VERSION", err.Error(), traceID)
	case errors.Is(err, supervisor.ErrInvalidServiceSpec):
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_SERVICE_SPEC", err.Error(), traceID)
	case errors.Is(err, supervisor.ErrServiceTerminated):
		h.writeProblem(writer, request, http.StatusConflict, "SERVICE_TERMINATED", err.Error(), traceID)
	default:
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
	}
}

// stopService disables autowake and requests cancellation of every active task.
func (h *Handler) stopService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}
	serviceID := request.PathValue("serviceID")
	if err := h.supervisor.StopService(request.Context(), principal.TenantID, serviceID); err != nil {
		switch {
		case errors.Is(err, supervisor.ErrServiceNotFound):
			h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
		case errors.Is(err, supervisor.ErrServiceTerminated):
			h.writeProblem(writer, request, http.StatusConflict, "SERVICE_TERMINATED", "service is terminated", traceID)
		default:
			h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		}
		return
	}
	svc, err := h.supervisor.GetService(request.Context(), principal.TenantID, serviceID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}
	writeJSON(writer, http.StatusOK, formatServiceResponse(svc, traceID))
}

// deleteService deletes a service and its instances (DELETE /v1/services/{serviceID}).
func (h *Handler) deleteService(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	if err := h.supervisor.DeleteService(request.Context(), principal.TenantID, serviceID); err != nil {
		if errors.Is(err, supervisor.ErrServiceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "SERVICE_NOT_FOUND", "service not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writer.WriteHeader(http.StatusNoContent)
}

// listServiceInstances lists instances belonging to a service (GET /v1/services/{serviceID}/instances).
func (h *Handler) listServiceInstances(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.serviceStore == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service store is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	instances, err := h.serviceStore.ListInstances(request.Context(), principal.TenantID, serviceID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	res := make([]instanceResponse, 0, len(instances))
	for _, inst := range instances {
		res = append(res, formatInstanceResponse(inst, traceID))
	}
	writeJSON(writer, http.StatusOK, res)
}

// heartbeatServiceInstance records an instance heartbeat (POST /v1/services/{serviceID}/instances/{instanceID}/heartbeat).
func (h *Handler) heartbeatServiceInstance(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.supervisor == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "SERVICES_DISABLED", "service supervisor is not configured", traceID)
		return
	}

	serviceID := request.PathValue("serviceID")
	instanceID := request.PathValue("instanceID")

	if err := h.supervisor.RecordHeartbeat(request.Context(), principal.TenantID, serviceID, instanceID); err != nil {
		if errors.Is(err, supervisor.ErrInstanceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "INSTANCE_NOT_FOUND", "instance not found", traceID)
			return
		}
		if errors.Is(err, supervisor.ErrInstanceTerminated) {
			h.writeProblem(writer, request, http.StatusConflict, "INSTANCE_TERMINATED", "instance is already terminated", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writer.WriteHeader(http.StatusNoContent)
}
