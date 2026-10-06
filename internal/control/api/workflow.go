// Workflow orchestration endpoints of the Control API: publish a
// workflow run, inspect it with its steps, record the durable cancellation
// intent, and decide parked human-approval steps.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/google/uuid"
)

// WorkflowStore is the durable orchestration surface the workflow
// endpoints expose. The kernel store satisfies it.
type WorkflowStore interface {
	CreateWorkflow(context.Context, store.CreateWorkflowInput) (store.CreateWorkflowResult, error)
	GetWorkflow(context.Context, string, uuid.UUID) (store.Workflow, error)
	ListWorkflowSteps(context.Context, string, uuid.UUID) ([]store.WorkflowStep, error)
	RequestWorkflowCancellation(context.Context, string, uuid.UUID, int64) (store.Workflow, error)
	DecideWorkflowStepApproval(context.Context, store.DecideWorkflowStepApprovalInput) (store.WorkflowStep, error)
}

type createWorkflowRequest struct {
	Namespace string          `json:"namespace"`
	Goal      string          `json:"goal"`
	Workflow  json.RawMessage `json:"workflow"`
}

// createWorkflow publishes one workflow run (POST /v1/workflows). The
// workflow document is strictly decoded and DAG-validated by the kernel
// before anything is persisted; the response carries the workflow with its
// steps.
func (h *Handler) createWorkflow(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.workflows == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOWS_DISABLED", "workflow orchestration is not configured", traceID)
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
	var body createWorkflowRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	namespace := body.Namespace
	if namespace == "" {
		namespace = "default"
	}
	if body.Goal == "" || len(body.Goal) > 8192 {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_WORKFLOW", "goal must be 1..8192 bytes", traceID)
		return
	}
	workflowSpec, err := workflow.DecodeWorkflowSpec(body.Workflow)
	if err != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_WORKFLOW", err.Error(), traceID)
		return
	}
	steps := workflowSpec.StepInputs()
	budgetTasks, budgetTokens, budgetCost := workflowSpec.Budgets()
	result, err := h.workflows.CreateWorkflow(request.Context(), store.CreateWorkflowInput{
		ID: h.newID(), TenantID: principal.TenantID, Namespace: namespace,
		IdempotencyKey: idempotencyKey, Goal: body.Goal, Spec: body.Workflow, Steps: steps,
		BudgetMaxTasks: budgetTasks, BudgetMaxTokens: budgetTokens, BudgetMaxCostMicroUSD: budgetCost,
		DeadlineAt: workflowSpec.Deadline,
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
	writer.Header().Set("Location", "/v1/workflows/"+result.Workflow.ID.String())
	stepsOut, err := h.workflows.ListWorkflowSteps(request.Context(), principal.TenantID, result.Workflow.ID)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	h.writeWorkflow(writer, status, result.Workflow, stepsOut, traceID)
}

// getWorkflow returns one workflow with its steps (GET /v1/workflows/{id}).
func (h *Handler) getWorkflow(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.workflows == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOWS_DISABLED", "workflow orchestration is not configured", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("workflowID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOW_NOT_FOUND", "workflow was not found", traceID)
		return
	}
	found, err := h.workflows.GetWorkflow(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	steps, err := h.workflows.ListWorkflowSteps(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	h.writeWorkflow(writer, http.StatusOK, found, steps, traceID)
}

// cancelWorkflow records the durable cancellation intent
// (POST /v1/workflows/{id}/cancel); the orchestrator propagates it.
func (h *Handler) cancelWorkflow(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.workflows == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOWS_DISABLED", "workflow orchestration is not configured", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("workflowID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOW_NOT_FOUND", "workflow was not found", traceID)
		return
	}
	expectedVersion, err := parseEntityVersion(request.Header.Get("If-Match"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusPreconditionRequired, "IF_MATCH_REQUIRED", "If-Match must contain the current weak resource-version ETag", traceID)
		return
	}
	updated, err := h.workflows.RequestWorkflowCancellation(request.Context(), principal.TenantID, id, expectedVersion)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	steps, err := h.workflows.ListWorkflowSteps(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	h.writeWorkflow(writer, http.StatusAccepted, updated, steps, traceID)
}

type decideWorkflowApprovalRequest struct {
	Decision string `json:"decision"`
}

// decideWorkflowStepApproval records a human decision on a parked step
// (POST /v1/workflows/{id}/steps/{name}/approval, decision approve|reject).
func (h *Handler) decideWorkflowStepApproval(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	if h.workflows == nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOWS_DISABLED", "workflow orchestration is not configured", traceID)
		return
	}
	workflowID, err := uuid.Parse(request.PathValue("workflowID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "WORKFLOW_NOT_FOUND", "workflow was not found", traceID)
		return
	}
	stepName := request.PathValue("stepName")
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
	var body decideWorkflowApprovalRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	approved := false
	switch body.Decision {
	case "approve":
		approved = true
	case "reject":
	default:
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_DECISION", "decision must be approve or reject", traceID)
		return
	}
	expectedVersion, err := parseEntityVersion(request.Header.Get("If-Match"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusPreconditionRequired, "IF_MATCH_REQUIRED", "If-Match must contain the current weak resource-version ETag", traceID)
		return
	}
	_, err = h.workflows.DecideWorkflowStepApproval(request.Context(), store.DecideWorkflowStepApprovalInput{
		TenantID: principal.TenantID, WorkflowID: workflowID, StepName: stepName,
		ExpectedVersion: expectedVersion, Approved: approved, DecidedBy: principal.Subject,
	})
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	found, err := h.workflows.GetWorkflow(request.Context(), principal.TenantID, workflowID)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	steps, err := h.workflows.ListWorkflowSteps(request.Context(), principal.TenantID, workflowID)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	h.writeWorkflow(writer, http.StatusAccepted, found, steps, traceID)
}

// writeWorkflow serializes one workflow document with its steps.
func (h *Handler) writeWorkflow(writer http.ResponseWriter, status int, target store.Workflow, steps []store.WorkflowStep, traceID string) {
	document := map[string]any{
		"id": target.ID.String(), "namespace": target.Namespace, "goal": target.Goal,
		"status":          string(target.Status),
		"resourceVersion": target.ResourceVersion,
		"createdAt":       target.CreatedAt.UTC().Format(time.RFC3339),
		"updatedAt":       target.UpdatedAt.UTC().Format(time.RFC3339),
		"traceId":         traceID,
	}
	if target.FailureCode != "" {
		document["failureCode"] = target.FailureCode
	}
	if target.CancelRequestedAt != nil {
		document["cancelRequestedAt"] = target.CancelRequestedAt.UTC().Format(time.RFC3339)
	}
	if target.DeadlineAt != nil {
		document["deadline"] = target.DeadlineAt.UTC().Format(time.RFC3339)
	}
	if target.DeadlineExceededAt != nil {
		document["deadlineExceededAt"] = target.DeadlineExceededAt.UTC().Format(time.RFC3339)
	}
	encodedSteps := make([]map[string]any, 0, len(steps))
	for _, step := range steps {
		entry := map[string]any{
			"name": step.Name, "ordinal": step.Ordinal, "status": string(step.Status),
			"attemptCount": step.AttemptCount, "resourceVersion": step.ResourceVersion,
		}
		if step.TaskID != nil {
			entry["taskId"] = step.TaskID.String()
		}
		if step.FailureCode != "" {
			entry["failureCode"] = step.FailureCode
		}
		if step.DecidedBy != "" {
			entry["decidedBy"] = step.DecidedBy
			entry["approvalDecision"] = step.ApprovalDecision
		}
		if step.ParentStepName != "" {
			entry["parentStepName"] = step.ParentStepName
		}
		if step.IsDynamic {
			entry["isDynamic"] = true
			entry["spawnDepth"] = step.SpawnDepth
			entry["spawnKey"] = step.SpawnKey
		}
		encodedSteps = append(encodedSteps, entry)
	}
	document["steps"] = encodedSteps
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, target.ResourceVersion))
	writeJSON(writer, status, document)
}
