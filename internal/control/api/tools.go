// The tool endpoints: the granted tool list and the human approval
// decisions on a tool call.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

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
