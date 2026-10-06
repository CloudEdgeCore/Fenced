package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
)

type createNamespaceRequest struct {
	Name        string                  `json:"name"`
	DisplayName string                  `json:"displayName,omitempty"`
	Description string                  `json:"description,omitempty"`
	Labels      map[string]string       `json:"labels,omitempty"`
	Quota       namespace.ResourceQuota `json:"quota,omitempty"`
}

type updateNamespaceRequest struct {
	DisplayName string                  `json:"displayName,omitempty"`
	Description string                  `json:"description,omitempty"`
	Labels      map[string]string       `json:"labels,omitempty"`
	Quota       namespace.ResourceQuota `json:"quota,omitempty"`
}

type namespaceResponse struct {
	APIVersion  string                  `json:"apiVersion"`
	Kind        string                  `json:"kind"`
	TenantID    string                  `json:"tenantId"`
	Name        string                  `json:"name"`
	DisplayName string                  `json:"displayName,omitempty"`
	Description string                  `json:"description,omitempty"`
	Phase       string                  `json:"phase"`
	Labels      map[string]string       `json:"labels,omitempty"`
	Quota       namespace.ResourceQuota `json:"quota"`
	CreatedAt   time.Time               `json:"createdAt"`
	UpdatedAt   time.Time               `json:"updatedAt"`
	TraceID     string                  `json:"traceId"`
}

type namespaceUsageResponse struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	TenantID   string                  `json:"tenantId"`
	Namespace  string                  `json:"namespace"`
	Usage      namespace.ResourceUsage `json:"usage"`
	TraceID    string                  `json:"traceId"`
}

func formatNamespaceResponse(ns *namespace.Namespace, traceID string) namespaceResponse {
	return namespaceResponse{
		APIVersion:  "fenced.dev/v1",
		Kind:        "Namespace",
		TenantID:    ns.TenantID,
		Name:        ns.Name,
		DisplayName: ns.DisplayName,
		Description: ns.Description,
		Phase:       string(ns.Phase),
		Labels:      ns.Labels,
		Quota:       ns.Quota,
		CreatedAt:   ns.CreatedAt.UTC(),
		UpdatedAt:   ns.UpdatedAt.UTC(),
		TraceID:     traceID,
	}
}

// createNamespace handles POST /v1/namespaces.
func (h *Handler) createNamespace(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.namespaces == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NAMESPACES_DISABLED", "namespace store is not configured", traceID)
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

	var req createNamespaceRequest
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	if err := namespace.ValidateNamespaceName(req.Name); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error(), traceID)
		return
	}

	ns := &namespace.Namespace{
		TenantID:    principal.TenantID,
		Name:        req.Name,
		DisplayName: req.DisplayName,
		Description: req.Description,
		Phase:       namespace.NamespacePhaseActive,
		Labels:      req.Labels,
		Quota:       req.Quota,
	}

	if err := h.namespaces.CreateNamespace(request.Context(), ns); err != nil {
		switch {
		case errors.Is(err, namespace.ErrNamespaceAlreadyExists):
			h.writeProblem(writer, request, http.StatusConflict, "ALREADY_EXISTS", "namespace already exists", traceID)
		case errors.Is(err, namespace.ErrInvalidNamespaceName):
			h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error(), traceID)
		default:
			h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		}
		return
	}

	created, err := h.namespaces.GetNamespace(request.Context(), principal.TenantID, req.Name)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(writer).Encode(formatNamespaceResponse(created, traceID))
}

// listNamespaces handles GET /v1/namespaces.
func (h *Handler) listNamespaces(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.namespaces == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NAMESPACES_DISABLED", "namespace store is not configured", traceID)
		return
	}

	list, err := h.namespaces.ListNamespaces(request.Context(), principal.TenantID)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	items := make([]namespaceResponse, 0, len(list))
	for _, ns := range list {
		items = append(items, formatNamespaceResponse(ns, traceID))
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"apiVersion": "fenced.dev/v1",
		"kind":       "NamespaceList",
		"items":      items,
		"traceId":    traceID,
	})
}

// getNamespace handles GET /v1/namespaces/{name}.
func (h *Handler) getNamespace(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.namespaces == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NAMESPACES_DISABLED", "namespace store is not configured", traceID)
		return
	}

	name := strings.TrimSpace(request.PathValue("name"))
	if name == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_ARGUMENT", "namespace name is required", traceID)
		return
	}

	ns, err := h.namespaces.GetNamespace(request.Context(), principal.TenantID, name)
	if err != nil {
		if errors.Is(err, namespace.ErrNamespaceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "NOT_FOUND", "namespace not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(formatNamespaceResponse(ns, traceID))
}

// updateNamespace handles PUT /v1/namespaces/{name}.
func (h *Handler) updateNamespace(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.namespaces == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NAMESPACES_DISABLED", "namespace store is not configured", traceID)
		return
	}

	name := strings.TrimSpace(request.PathValue("name"))
	if name == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_ARGUMENT", "namespace name is required", traceID)
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

	var req updateNamespaceRequest
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}

	existing, err := h.namespaces.GetNamespace(request.Context(), principal.TenantID, name)
	if err != nil {
		if errors.Is(err, namespace.ErrNamespaceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "NOT_FOUND", "namespace not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	if req.DisplayName != "" {
		existing.DisplayName = req.DisplayName
	}
	if req.Description != "" {
		existing.Description = req.Description
	}
	if req.Labels != nil {
		existing.Labels = req.Labels
	}
	existing.Quota = req.Quota

	if err := h.namespaces.UpdateNamespace(request.Context(), existing); err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	updated, err := h.namespaces.GetNamespace(request.Context(), principal.TenantID, name)
	if err != nil {
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(formatNamespaceResponse(updated, traceID))
}

// deleteNamespace handles DELETE /v1/namespaces/{name}.
func (h *Handler) deleteNamespace(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.namespaces == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NAMESPACES_DISABLED", "namespace store is not configured", traceID)
		return
	}

	name := strings.TrimSpace(request.PathValue("name"))
	if name == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_ARGUMENT", "namespace name is required", traceID)
		return
	}

	err := h.namespaces.DeleteNamespace(request.Context(), principal.TenantID, name)
	if err != nil {
		switch {
		case errors.Is(err, namespace.ErrDefaultNamespaceProtected):
			h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PROTECTED_RESOURCE", "default namespace cannot be deleted", traceID)
		case errors.Is(err, namespace.ErrNamespaceNotEmpty):
			h.writeProblem(writer, request, http.StatusConflict, "NAMESPACE_NOT_EMPTY", "namespace contains active resources and cannot be deleted", traceID)
		case errors.Is(err, namespace.ErrNamespaceNotFound):
			h.writeProblem(writer, request, http.StatusNotFound, "NOT_FOUND", "namespace not found", traceID)
		default:
			h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		}
		return
	}

	writer.WriteHeader(http.StatusNoContent)
}

// getNamespaceUsage handles GET /v1/namespaces/{name}/usage.
func (h *Handler) getNamespaceUsage(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.namespaces == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "NAMESPACES_DISABLED", "namespace store is not configured", traceID)
		return
	}

	name := strings.TrimSpace(request.PathValue("name"))
	if name == "" {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_ARGUMENT", "namespace name is required", traceID)
		return
	}

	usage, err := h.namespaces.GetUsage(request.Context(), principal.TenantID, name)
	if err != nil {
		if errors.Is(err, namespace.ErrNamespaceNotFound) {
			h.writeProblem(writer, request, http.StatusNotFound, "NOT_FOUND", "namespace not found", traceID)
			return
		}
		h.writeProblem(writer, request, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), traceID)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(namespaceUsageResponse{
		APIVersion: "fenced.dev/v1",
		Kind:       "NamespaceUsage",
		TenantID:   principal.TenantID,
		Namespace:  name,
		Usage:      *usage,
		TraceID:    traceID,
	})
}
