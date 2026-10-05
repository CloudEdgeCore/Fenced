// Package api exposes the §13 application-layer research REST API. It is a
// thin HTTP server that composes workflow documents, submits them through
// the Control API v1, and materializes the domain state from observable
// kernel surfaces. The server is stateless between runs except for the
// namespace-key → kernel-workflow-id mapping stored in the artifact root.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/domain"
	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/report"
	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/repository"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/google/uuid"
)

const maxBodyBytes = 1 << 20

// BudgetOverride carries the optional user-supplied budget in a POST /research
// request (§13). Zero values use the workflow template defaults.
type BudgetOverride struct {
	MaxTasks  int64 `json:"maxTasks,omitempty"`
	MaxTokens int64 `json:"maxTokens,omitempty"`
}

// createResearchRequest mirrors §13 POST /research body.
type createResearchRequest struct {
	Goal   string          `json:"goal"`
	Budget *BudgetOverride `json:"budget,omitempty"`
}

// Server routes the application REST API.
type Server struct {
	control       *repository.Client
	template      []byte
	artifactStore *artifact.Filesystem
	tenant        string
	namespace     string
	deadlineDur   time.Duration
	now           func() time.Time
	mappingDir    string
}

// NewServer builds the application-layer research API. The mapping directory
// (namespace-key → kernel-workflow-id) is created under mappingRoot on first
// use so the server survives restarts.
func NewServer(control *repository.Client, template []byte, artifactStore *artifact.Filesystem, tenant, namespace string, deadline time.Duration, mappingRoot string) *Server {
	return &Server{
		control:       control,
		template:      template,
		artifactStore: artifactStore,
		tenant:        tenant,
		namespace:     namespace,
		deadlineDur:   deadline,
		now:           time.Now,
		mappingDir:    filepath.Join(mappingRoot, ".research-mapping"),
	}
}

// Handler returns the HTTP handler with all routes mounted.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /research", s.handleCreate)
	mux.HandleFunc("GET /research/{id}", s.handleGet)
	mux.HandleFunc("GET /research/{id}/report", s.handleReport)
	mux.HandleFunc("POST /research/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return mux
}

// handleCreate implements POST /research (§13).
func (s *Server) handleCreate(writer http.ResponseWriter, request *http.Request) {
	body, err := s.decodeBody(writer, request)
	if err != nil {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	var req createResearchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	if strings.TrimSpace(req.Goal) == "" || len(req.Goal) > 8192 {
		writeProblem(writer, http.StatusUnprocessableEntity, "INVALID_GOAL", "goal must be 1..8192 bytes")
		return
	}
	if req.Budget != nil {
		if err := domain.ValidateRequestBudget(req.Budget.MaxTasks, req.Budget.MaxTokens); err != nil {
			writeProblem(writer, http.StatusUnprocessableEntity, "INVALID_BUDGET", err.Error())
			return
		}
	}

	namespaceKey := uuid.New().String()
	deadline := s.now().Add(s.deadlineDur)
	composed, err := ComposeWorkflowDocument(s.template, req.Goal, namespaceKey, deadline, req.Budget)
	if err != nil {
		writeProblem(writer, http.StatusInternalServerError, "COMPOSITION_FAILED", err.Error())
		return
	}
	// Validate the composed document against the kernel decoder BEFORE
	// submission so the caller gets a 422 with the exact reason.
	if _, err := workflow.DecodeWorkflowSpec(composed); err != nil {
		writeProblem(writer, http.StatusUnprocessableEntity, "INVALID_WORKFLOW", err.Error())
		return
	}
	workflowView, err := s.control.CreateWorkflow(request.Context(), s.namespace, req.Goal, composed, "research-"+namespaceKey)
	if err != nil {
		writeProblem(writer, http.StatusBadGateway, "CONTROL_API_ERROR", err.Error())
		return
	}
	// Persist the namespace-key to kernel-workflow-id mapping so the server
	// survives restarts.
	if err := s.writeMapping(namespaceKey, workflowView.ID); err != nil {
		log.Printf("[research-api] mapping write error: %v", err)
		// Non-fatal: the create succeeded; the mapping is recoverable by
		// scanning the artifact root on restart.
	}
	writeJSON(writer, http.StatusAccepted, map[string]string{
		"researchId": repository.ResearchIDFor(namespaceKey),
		"workflowId": workflowView.ID,
		"status":     string(domain.StateCreated),
	})
}

// handleGet implements GET /research/{id} (§13).
func (s *Server) handleGet(writer http.ResponseWriter, request *http.Request) {
	researchID := request.PathValue("id")
	workflowID, namespaceKey, err := s.resolveWorkflowID(request.Context(), researchID)
	if err != nil {
		writeProblem(writer, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	view, err := s.control.LoadRunView(request.Context(), workflowID, namespaceKey)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeProblem(writer, http.StatusNotFound, "NOT_FOUND", "research run not found")
			return
		}
		writeProblem(writer, http.StatusBadGateway, "CONTROL_API_ERROR", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, view)
}

// handleReport implements GET /research/{id}/report (§13).
func (s *Server) handleReport(writer http.ResponseWriter, request *http.Request) {
	researchID := request.PathValue("id")
	workflowID, namespaceKey, err := s.resolveWorkflowID(request.Context(), researchID)
	if err != nil {
		writeProblem(writer, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	workflowView, err := s.control.GetWorkflow(request.Context(), workflowID)
	if err != nil {
		writeProblem(writer, http.StatusNotFound, "NOT_FOUND", "research run not found")
		return
	}
	if view := domain.DeriveRunState(domain.WorkflowView{
		Status: workflowView.Status, FailureCode: workflowView.FailureCode,
	}); view != domain.StateCompleted {
		writeProblem(writer, http.StatusConflict, "REPORT_NOT_READY", fmt.Sprintf("research is %s, need COMPLETED", view))
		return
	}
	namespace := "research/" + namespaceKey
	reportDoc, err := s.fetchMemoryRecord(request.Context(), namespace+"/report", "report", "citations")
	if err != nil {
		writeProblem(writer, http.StatusNotFound, "REPORT_NOT_FOUND", err.Error())
		return
	}
	validationDoc, err := s.fetchMemoryRecord(request.Context(), namespace+"/report", "validation", "citationCoverage")
	if err != nil {
		// Validator verdict may be absent on older runs; tolerate.
		validationDoc = `{"citationCoverage":0,"valid":false,"unsupportedClaims":0}`
	}
	document, err := report.Decode(reportDoc)
	if err != nil {
		writeProblem(writer, http.StatusInternalServerError, "REPORT_CORRUPT", err.Error())
		return
	}
	verdict, err := report.DecodeVerdict(validationDoc)
	if err != nil {
		verdict = report.Verdict{}
	}
	markdown := report.RenderMarkdown(document, verdict)
	// Store the markdown as a content-addressed artifact.
	artifactRef, err := report.Store(request.Context(), s.artifactStore, s.tenant, report.MediaTypeMarkdown, []byte(markdown))
	if err != nil {
		writeProblem(writer, http.StatusInternalServerError, "ARTIFACT_FAILED", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"report": map[string]any{
			"id":               "report-" + workflowID,
			"researchRunId":    researchID,
			"artifactRef":      artifactRef,
			"citationCoverage": verdict.CitationCoverage,
		},
		"markdown": markdown,
	})
}

// handleCancel implements POST /research/{id}/cancel (§13). The kernel
// cancel is CAS-versioned, so a concurrent orchestrator update is retried
// with a fresh resource version (bounded).
func (s *Server) handleCancel(writer http.ResponseWriter, request *http.Request) {
	researchID := request.PathValue("id")
	workflowID, _, err := s.resolveWorkflowID(request.Context(), researchID)
	if err != nil {
		writeProblem(writer, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		workflowView, err := s.control.GetWorkflow(request.Context(), workflowID)
		if err != nil {
			writeProblem(writer, http.StatusNotFound, "NOT_FOUND", "research run not found")
			return
		}
		if workflowView.Status != "RUNNING" && workflowView.Status != "PENDING" {
			writeProblem(writer, http.StatusConflict, "NOT_RUNNING", "research is not running")
			return
		}
		_, err = s.control.CancelWorkflow(request.Context(), workflowID, workflowView.ResourceVersion)
		if err == nil {
			break
		}
		if !errors.Is(err, repository.ErrConflict) {
			writeProblem(writer, http.StatusBadGateway, "CONTROL_API_ERROR", err.Error())
			return
		}
		// The orchestrator advanced the workflow between read and cancel;
		// re-read and retry with the fresh version.
		time.Sleep(50 * time.Millisecond)
		if attempt == 4 {
			writeProblem(writer, http.StatusConflict, "CANCEL_RACE", "workflow changed concurrently; retry the cancel")
			return
		}
	}
	view, err := s.control.LoadRunView(request.Context(), workflowID, "")
	if err != nil {
		writeProblem(writer, http.StatusBadGateway, "CONTROL_API_ERROR", err.Error())
		return
	}
	writeJSON(writer, http.StatusAccepted, view)
}

func (s *Server) handleHealth(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

// -- helpers ----------------------------------------------------------------

// resolveWorkflowID maps a research ID ("research-<namespaceKey>") to the
// kernel workflow UUID (via the on-disk mapping) and the namespace key.
func (s *Server) resolveWorkflowID(ctx context.Context, researchID string) (workflowID, namespaceKey string, err error) {
	namespaceKey, err = repository.WorkflowIDFrom(researchID)
	if err != nil {
		return "", "", err
	}
	// First try the on-disk mapping (app-created runs).
	if kernelID, err := s.readMapping(namespaceKey); err == nil {
		return kernelID, namespaceKey, nil
	}
	// Fallback: the namespace key IS the kernel workflow ID (happens when
	// the server was created with a direct-store path, or the mapping file
	// was lost). Try to load the workflow directly.
	_, err = s.control.GetWorkflow(ctx, namespaceKey)
	if err == nil {
		return namespaceKey, namespaceKey, nil
	}
	return "", "", fmt.Errorf("research %q could not be resolved", researchID)
}

func (s *Server) decodeBody(writer http.ResponseWriter, request *http.Request) ([]byte, error) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBodyBytes)
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, fmt.Errorf("request body: %w", err)
	}
	return encoded, nil
}

func (s *Server) writeMapping(namespaceKey, workflowID string) error {
	if err := os.MkdirAll(s.mappingDir, 0o755); err != nil {
		return err
	}
	entry, err := json.Marshal(map[string]string{"workflowId": workflowID, "mappedAt": s.now().UTC().Format(time.RFC3339)})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.mappingDir, namespaceKey+".json"), entry, 0o644)
}

func (s *Server) readMapping(namespaceKey string) (string, error) {
	encoded, err := os.ReadFile(filepath.Join(s.mappingDir, namespaceKey+".json"))
	if err != nil {
		return "", err
	}
	var entry struct {
		WorkflowID string `json:"workflowId"`
	}
	if err := json.Unmarshal(encoded, &entry); err != nil {
		return "", err
	}
	return entry.WorkflowID, nil
}

// fetchMemoryRecord retrieves one memory record by searching the namespace
// with a query token matching the record's content. Exact key matching is
// avoided because the hybrid search index may not match on the key alone.
func (s *Server) fetchMemoryRecord(ctx context.Context, namespace, key, query string) (string, error) {
	records, err := s.control.SearchMemories(ctx, namespace, query, 10)
	if err != nil {
		return "", err
	}
	for _, record := range records {
		if record.Key == key {
			return record.Content, nil
		}
	}
	return "", fmt.Errorf("memory record %s/%s not found", namespace, key)
}

func writeJSON(writer http.ResponseWriter, statusCode int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeProblem(writer http.ResponseWriter, statusCode int, code, message string) {
	writeJSON(writer, statusCode, map[string]string{"code": code, "message": message})
}
