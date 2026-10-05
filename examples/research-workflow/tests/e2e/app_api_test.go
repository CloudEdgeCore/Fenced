//go:build integration

package research_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/api"
	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/repository"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/google/uuid"
)

// newResearchAPI mounts the application-layer research API (§13) in front of
// the harness Control API and returns its httptest server.
func (h *harness) newResearchAPI() *httptest.Server {
	h.t.Helper()
	template, err := os.ReadFile(filepath.Join("..", "..", "workflow", "research-workflow.json"))
	if err != nil {
		h.t.Fatalf("read workflow template: %v", err)
	}
	artifacts, err := artifact.NewFilesystem(h.t.TempDir(), 64<<20)
	if err != nil {
		h.t.Fatalf("artifact store: %v", err)
	}
	control := repository.NewClient(h.controlURL, nil, "")
	server := api.NewServer(control, template, artifacts, researchTenant, "default", 45*time.Minute, h.t.TempDir())
	httpServer := httptest.NewServer(server.Handler())
	h.t.Cleanup(httpServer.Close)
	return httpServer
}

// pollResearch polls GET /research/{id} until the run is terminal.
func pollResearch(t *testing.T, endpoint, researchID string, timeout time.Duration) repository.RunView {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var view repository.RunView
		if err := getTestJSON(endpoint+"/research/"+researchID, &view); err != nil {
			t.Fatalf("GET /research/%s: %v", researchID, err)
		}
		if strings.EqualFold(view.Run.Status, "COMPLETED") ||
			strings.EqualFold(view.Run.Status, "FAILED") ||
			strings.EqualFold(view.Run.Status, "CANCELLED") ||
			strings.EqualFold(view.Run.Status, "BUDGET_EXHAUSTED") {
			return view
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("research %s did not reach a terminal state within %s", researchID, timeout)
	return repository.RunView{}
}

func getTestJSON(endpoint string, out any) error {
	response, err := http.Get(endpoint)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, encoded)
	}
	return json.Unmarshal(encoded, out)
}

func postTestJSON(endpoint string, body any, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	response, err := http.Post(endpoint, "application/json", strings.NewReader(string(encoded)))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, payload)
	}
	if out != nil {
		return json.Unmarshal(payload, out)
	}
	return nil
}

// TestResearchAppAPIAndReport drives the §13 application API over real HTTP:
// create a research run, watch it complete, and read the final report
// artifact with its citation coverage.
func TestResearchAppAPIAndReport(t *testing.T) {
	h := newHarness(t, "appapi", nil)
	app := h.newResearchAPI()

	var created struct {
		ResearchID string `json:"researchId"`
		WorkflowID string `json:"workflowId"`
		Status     string `json:"status"`
	}
	if err := postTestJSON(app.URL+"/research", map[string]any{
		"goal": "Analyze the future of agent runtime infrastructure.",
	}, &created); err != nil {
		t.Fatalf("POST /research: %v", err)
	}
	if created.ResearchID == "" || created.WorkflowID == "" || created.Status != "CREATED" {
		t.Fatalf("create response missing fields: %+v", created)
	}
	if !strings.HasPrefix(created.ResearchID, "research-") {
		t.Fatalf("researchId %q must be prefixed with research-", created.ResearchID)
	}

	view := pollResearch(t, app.URL, created.ResearchID, 4*time.Minute)
	if view.Run.Status != "COMPLETED" {
		t.Fatalf("research status = %s, want COMPLETED (workflow %s)", view.Run.Status, created.WorkflowID)
	}
	if len(view.Questions) < 3 {
		t.Fatalf("questions = %d, want >= 3", len(view.Questions))
	}
	if len(view.Sources) < 1 {
		t.Fatalf("sources = %d, want >= 1", len(view.Sources))
	}
	if len(view.Evidence) < 30 {
		t.Fatalf("evidence = %d, want >= 30", len(view.Evidence))
	}
	if view.CriticVerdict == "" {
		t.Fatalf("critic verdict is empty")
	}

	var reportBody struct {
		Report struct {
			ID               string  `json:"id"`
			ResearchRunID    string  `json:"researchRunId"`
			ArtifactRef      string  `json:"artifactRef"`
			CitationCoverage float64 `json:"citationCoverage"`
		} `json:"report"`
		Markdown string `json:"markdown"`
	}
	if err := getTestJSON(app.URL+"/research/"+created.ResearchID+"/report", &reportBody); err != nil {
		t.Fatalf("GET /research/%s/report: %v", created.ResearchID, err)
	}
	if reportBody.Report.ArtifactRef == "" || !strings.HasPrefix(reportBody.Report.ArtifactRef, "artifact://") {
		t.Fatalf("artifactRef = %q, want artifact:// URI", reportBody.Report.ArtifactRef)
	}
	if reportBody.Report.CitationCoverage < 0.90 {
		t.Fatalf("citationCoverage = %.3f, want >= 0.90", reportBody.Report.CitationCoverage)
	}
	if !strings.Contains(reportBody.Markdown, "## References") {
		t.Fatalf("markdown report is missing the References section")
	}
}

// TestResearchAppAPICancel cancels a freshly created run and asserts the
// application state machine reports CANCELLED.
func TestResearchAppAPICancel(t *testing.T) {
	h := newHarness(t, "appcancel", nil)
	app := h.newResearchAPI()

	var created struct {
		ResearchID string `json:"researchId"`
	}
	if err := postTestJSON(app.URL+"/research", map[string]any{
		"goal": "Analyze agent runtime security models.",
	}, &created); err != nil {
		t.Fatalf("POST /research: %v", err)
	}
	var cancelled struct {
		Run struct {
			Status string `json:"status"`
		} `json:"run"`
	}
	if err := postTestJSON(app.URL+"/research/"+created.ResearchID+"/cancel", nil, &cancelled); err != nil {
		t.Fatalf("POST cancel: %v", err)
	}
	view := pollResearch(t, app.URL, created.ResearchID, 2*time.Minute)
	if view.Run.Status != "CANCELLED" {
		t.Fatalf("research status = %s, want CANCELLED", view.Run.Status)
	}
}

// TestResearch1000Runs is the §16/§14-P5 scale gate: 1000 total ResearchRun
// submissions all settle SUCCEEDED (gated by FENCED_RESEARCH_SCALE_1000=1).
// The run count is tunable with FENCED_E2E_RESEARCH_RUNS.
func TestResearch1000Runs(t *testing.T) {
	if os.Getenv("FENCED_RESEARCH_SCALE_1000") != "1" {
		t.Skip("FENCED_RESEARCH_SCALE_1000 is not set")
	}
	const defaultRuns = 1000
	runs := defaultRuns
	if value := strings.TrimSpace(os.Getenv("FENCED_E2E_RESEARCH_RUNS")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			t.Fatalf("FENCED_E2E_RESEARCH_RUNS must be a positive integer, got %q", value)
		}
		runs = parsed
	}
	h := newHarness(t, "scale1000", nil)

	startedAt := time.Now()
	ids := make([]uuid.UUID, 0, runs)
	for index := 0; index < runs; index++ {
		id, err := h.createResearch(fmt.Sprintf("Scale run %d: agent runtime infrastructure.", index))
		if err != nil {
			t.Fatalf("create run %d: %v", index, err)
		}
		ids = append(ids, id)
	}
	createDuration := time.Since(startedAt)

	var (
		mu       sync.Mutex
		failures []string
	)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, id := range ids {
		id := id
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			workflow, _, err := h.awaitWorkflow(id, 60*time.Minute)
			mu.Lock()
			defer mu.Unlock()
			if err != nil || workflow.Status != kernelstore.WorkflowSucceeded {
				failures = append(failures, fmt.Sprintf("%s: %v (status %s)", id, err, workflow.Status))
			}
		}()
	}
	wg.Wait()
	total := time.Since(startedAt)

	if len(failures) > 0 {
		for _, failure := range failures {
			t.Logf("failed run: %s", failure)
		}
		t.Fatalf("%d of %d research runs failed (see log)", len(failures), runs)
	}
	t.Logf("scale-1000: %d runs succeeded in %s (create %s, await %s)", runs, total, createDuration, total-createDuration)
}

// TestResearchTaskSSEDisconnect validates the task event stream contract
// under disconnects (design doc §14 Phase 4 "SSE Disconnect"): a client that
// drops mid-stream reconnects, reconciles with GET, and still receives the
// terminal event.
func TestResearchTaskSSEDisconnect(t *testing.T) {
	h := newHarness(t, "ssedisconnect", nil)
	id, err := h.createResearch("Analyze agent runtime observability.")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	ctx := context.Background()

	// Wait until the planner step is dispatched so it has a task id.
	var taskID uuid.UUID
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		steps, err := h.store.ListWorkflowSteps(ctx, researchTenant, id)
		if err != nil {
			t.Fatalf("list steps: %v", err)
		}
		for _, step := range steps {
			if step.Name == "planner" && step.TaskID != nil {
				taskID = *step.TaskID
				break
			}
		}
		if taskID != uuid.Nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if taskID == uuid.Nil {
		t.Fatalf("planner step never received a task")
	}

	eventsURL := h.controlURL + "/v1/tasks/" + taskID.String() + "/events"
	// First connection: read one snapshot, then drop.
	first := readSSEFrames(t, eventsURL, 1, 15*time.Second)
	if len(first) == 0 {
		t.Fatalf("first SSE connection produced no frames")
	}
	// Reconnect and stream until terminal.
	frames := readSSEFrames(t, eventsURL, 0, 90*time.Second)
	seenTerminal := false
	seenUpdate := false
	for _, frame := range frames {
		switch {
		case strings.Contains(frame, "event: task.updated"):
			seenUpdate = true
		case strings.Contains(frame, "event: task.terminal"):
			seenTerminal = true
		}
	}
	if !seenUpdate {
		t.Fatalf("reconnected stream produced no task.updated frames: %q", frames)
	}
	if !seenTerminal {
		t.Fatalf("reconnected stream never reached task.terminal: %q", frames)
	}
}

// readSSEFrames opens the event stream and returns up to limit frames
// (limit 0 = stream until the terminal event or timeout).
func readSSEFrames(t *testing.T, url string, limit int, timeout time.Duration) []string {
	t.Helper()
	client := &http.Client{Timeout: timeout}
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("open SSE stream: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d", response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var frames []string
	var current strings.Builder
	deadline := time.Now().Add(timeout)
	for scanner.Scan() {
		if time.Now().After(deadline) {
			break
		}
		line := scanner.Text()
		if line == "" {
			if current.Len() > 0 {
				frames = append(frames, current.String())
				current.Reset()
				if limit > 0 && len(frames) >= limit {
					break
				}
			}
			continue
		}
		current.WriteString(line + "\n")
	}
	if current.Len() > 0 {
		frames = append(frames, current.String())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("SSE scan: %v", err)
	}
	return frames
}

// TestResearchSoak is the §18 "24h soak" evidence gate: continuous research
// runs with no budget or capacity drift. Gated by FENCED_RESEARCH_SOAK=1;
// duration tunable with FENCED_E2E_SOAK_MINUTES (default 10; 1440 for a
// full 24h run).
func TestResearchSoak(t *testing.T) {
	if os.Getenv("FENCED_RESEARCH_SOAK") != "1" {
		t.Skip("FENCED_RESEARCH_SOAK is not set")
	}
	minutes := int64(10)
	if value := strings.TrimSpace(os.Getenv("FENCED_E2E_SOAK_MINUTES")); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			t.Fatalf("FENCED_E2E_SOAK_MINUTES must be a positive integer, got %q", value)
		}
		minutes = parsed
	}
	h := newHarness(t, "soak", nil)
	ctx := context.Background()

	deadline := time.Now().Add(time.Duration(minutes) * time.Minute)
	var succeeded, failed int
	startedAt := time.Now()
	for time.Now().Before(deadline) {
		id, err := h.createResearch("Soak run: agent runtime infrastructure direction.")
		if err != nil {
			t.Fatalf("create soak run: %v", err)
		}
		workflow, _, err := h.awaitWorkflow(id, 30*time.Minute)
		if err != nil || workflow.Status != kernelstore.WorkflowSucceeded {
			failed++
			t.Logf("soak run %s failed: %v (status %s)", id, err, workflow.Status)
			if failed > 5 {
				t.Fatalf("soak exceeded 5 failures after %s", time.Since(startedAt))
			}
			continue
		}
		succeeded++
	}
	total := time.Since(startedAt)

	// Capacity drift check: ACTIVE reservations must drain to zero once
	// everything settles. RELEASED rows are history and expected. Release
	// lags the terminal state by a reconcile tick, so poll briefly before
	// declaring drift.
	var reservations int
	drainDeadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(drainDeadline) {
		if err := h.pool.QueryRow(ctx,
			`SELECT count(*) FROM runtime_capacity_reservations WHERE tenant_id = $1 AND status = 'ACTIVE'`, researchTenant,
		).Scan(&reservations); err != nil {
			t.Fatalf("query reservations: %v", err)
		}
		if reservations == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if reservations != 0 {
		t.Fatalf("capacity drift: %d ACTIVE reservations did not drain", reservations)
	}
	if failed != 0 {
		t.Fatalf("soak: %d of %d runs failed", failed, succeeded+failed)
	}
	t.Logf("soak: %d runs succeeded in %s, completion rate 100%%, zero capacity drift", succeeded, total)
}
