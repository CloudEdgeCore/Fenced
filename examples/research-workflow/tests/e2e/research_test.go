//go:build integration

package research_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	research "github.com/CloudEdgeCore/Fenced/examples/research-workflow/runtime"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	settleTimeout = 4 * time.Minute
	// Real providers have materially higher and more variable tail latency
	// than the deterministic test double. Keep explicit per-scenario walls,
	// but leave enough room for one bounded 180-second provider call near the
	// end of the workflow instead of cancelling an otherwise healthy Writer.
	liveModelTimeout = 20 * time.Minute
	liveFullTimeout  = 20 * time.Minute
)

// Phase 1 acceptance: one goal -> >=3 questions -> >=10 sources -> >=30
// evidence claims, every artifact stored in the workflow's memory tree.
func TestResearchWorkflowBasic(t *testing.T) {
	h := newHarness(t, "basic", nil)
	id, err := h.createResearch("Analyze the direction of agent runtime infrastructure over the next three years")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	workflow := h.requireCompleted(id, settleTimeout)

	var plan struct {
		Questions []research.Question `json:"questions"`
	}
	mustDecodeMemory(t, h, id, "analysis", "plan", &plan)
	if len(plan.Questions) < 3 {
		t.Fatalf("planner produced %d questions, want >= 3", len(plan.Questions))
	}
	sourceCount := memoryKeyCount(t, h, id, "sources")
	if sourceCount < 3 {
		t.Fatalf("sources namespace holds %d records, want >= 3 batches", sourceCount)
	}
	evidenceJSON := memoryContentsJoined(t, h, id, "evidence")
	claimCount := strings.Count(evidenceJSON, `"claimId"`)
	if claimCount < 30 {
		t.Fatalf("evidence carries %d claims, want >= 30 (%.400s)", claimCount, evidenceJSON)
	}

	steps := listSteps(t, h, id)
	searches, readers, collectors := 0, 0, 0
	for _, step := range steps {
		switch {
		case strings.HasPrefix(step.Name, "search-rq-"):
			searches++
		case strings.HasPrefix(step.Name, "reader-"):
			readers++
		case step.Name == "collector-r2" || step.Name == "collector-r3":
			collectors++
		}
	}
	if searches < 3 || readers < 10 || collectors < 2 {
		t.Fatalf("spawned steps: searches=%d readers=%d collectorRounds=%d", searches, readers, collectors)
	}
	if workflow.Status != kernelstore.WorkflowSucceeded {
		t.Fatalf("workflow status = %s", workflow.Status)
	}
	t.Logf("basic loop verified: questions=%d searches=%d readers=%d claims=%d",
		len(plan.Questions), searches, readers, claimCount)
}

// The critic identifies an evidence gap in round one, spawns additional gap
// searches and readers, and the final round converges to PASS.
func TestResearchWorkflowCriticRetry(t *testing.T) {
	h := newHarness(t, "criticretry", func(s *scenario) { s.criticRound1NeedsMore = true })
	id, err := h.createResearch("Compare current mainstream agent runtime approaches and assess the opportunity")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout)

	gaps := memoryContentsJoined(t, h, id, "gaps")
	if !strings.Contains(gaps, "NEEDS_MORE_RESEARCH") || !strings.Contains(gaps, "gap-r1-001") {
		t.Fatalf("round-one decision missing from gaps namespace: %.300s", gaps)
	}
	steps := listSteps(t, h, id)
	gapSearches, gapReaders, finalPass := 0, 0, false
	for _, step := range steps {
		if strings.HasPrefix(step.Name, "search-gap-r1") && step.Status == kernelstore.StepSucceeded {
			gapSearches++
		}
		if strings.HasPrefix(step.Name, "reader-") && step.ParentStepName == "collector-r2" &&
			step.Status == kernelstore.StepSucceeded {
			gapReaders++
		}
		if step.Name == "critic-final" && step.Status == kernelstore.StepSucceeded &&
			strings.Contains(extractOutput(step.ResultSummary), `"status":"PASS"`) {
			finalPass = true
		}
	}
	if gapSearches == 0 {
		t.Fatalf("critic round one spawned no gap searches; steps: %s", renderSteps(steps))
	}
	if gapReaders == 0 {
		t.Fatalf("round-two collector spawned no gap readers; steps: %s", renderSteps(steps))
	}
	if !finalPass {
		t.Fatalf("final critic did not PASS; steps: %s", renderSteps(steps))
	}
	t.Logf("critic retry verified: gapSearches=%d gapReaders=%d finalVerdict=PASS", gapSearches, gapReaders)
}

// Citation binding is deterministic: fabricated quote text is replaced from
// the exact grounded claim before validation, avoiding another model pass.
func TestResearchWorkflowCitationCoverage(t *testing.T) {
	h := newHarness(t, "citation", func(s *scenario) { s.writerBadCitations = true })
	id, err := h.createResearch("Write a fully-cited outlook on agent runtime infrastructure")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout)

	var validation struct {
		CitationCoverage  float64 `json:"citationCoverage"`
		UnsupportedClaims int     `json:"unsupportedClaims"`
		Retries           int     `json:"retries"`
	}
	mustDecodeMemory(t, h, id, "report", "validation", &validation)
	if validation.Retries != 0 {
		t.Fatalf("deterministic citation binding should avoid revisions: retries=%d", validation.Retries)
	}
	if validation.CitationCoverage != 1 || validation.UnsupportedClaims != 0 {
		t.Fatalf("final coverage %.2f unsupported=%d retries=%d",
			validation.CitationCoverage, validation.UnsupportedClaims, validation.Retries)
	}
	var report struct {
		CanonicalizedCitations int `json:"canonicalizedCitations"`
	}
	mustDecodeMemory(t, h, id, "report", "report", &report)
	if report.CanonicalizedCitations == 0 {
		t.Fatal("report did not audit canonical citation binding")
	}
	t.Logf("citation gate enforced deterministically: canonicalized=%d coverage=%.2f",
		report.CanonicalizedCitations, validation.CitationCoverage)
}

// Evidence → Original Source grounding (roadmap §3.4): every persisted
// claim carries a source hash and grounded flag; nothing ungrounded may
// reach the evidence namespace.
func TestResearchWorkflowEvidenceGrounding(t *testing.T) {
	h := newHarness(t, "grounding", nil)
	id, err := h.createResearch("Grounded-evidence overview of agent runtime control planes")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout)

	evidenceJSON := memoryContentsJoined(t, h, id, "evidence")
	claims := strings.Count(evidenceJSON, `"claimId"`)
	if claims < 30 {
		t.Fatalf("evidence carries %d claims, want >= 30", claims)
	}
	if strings.Contains(evidenceJSON, `"grounded":false`) {
		t.Fatal("ungrounded claim reached the evidence namespace")
	}
	hashes := strings.Count(evidenceJSON, `"sourceHash":"`)
	if hashes != claims {
		t.Fatalf("sourceHash stamped on %d of %d claims", hashes, claims)
	}
	t.Logf("evidence grounding verified: claims=%d allGrounded=true", claims)
}

// Unknown evidence ids are discarded at the writer boundary and missing
// analysis citations are restored from grounded memory before validation.
func TestResearchWorkflowInvalidCitation(t *testing.T) {
	h := newHarness(t, "invalidcitation", func(s *scenario) { s.writerUnknownEvidence = true })
	id, err := h.createResearch("Invalid-citation resilience outlook on agent runtime governance")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout)

	var validation struct {
		CitationCoverage  float64 `json:"citationCoverage"`
		UnsupportedClaims int     `json:"unsupportedClaims"`
		Retries           int     `json:"retries"`
	}
	mustDecodeMemory(t, h, id, "report", "validation", &validation)
	if validation.Retries != 0 || validation.UnsupportedClaims != 0 || validation.CitationCoverage != 1 {
		t.Fatalf("unknown citations were not deterministically recovered: %+v", validation)
	}
	var report struct {
		DroppedCitations       int `json:"droppedCitations"`
		CanonicalizedCitations int `json:"canonicalizedCitations"`
	}
	mustDecodeMemory(t, h, id, "report", "report", &report)
	if report.DroppedCitations == 0 || report.CanonicalizedCitations == 0 {
		t.Fatalf("citation repair audit is incomplete: %+v", report)
	}
	t.Logf("unknown citations recovered: dropped=%d canonicalized=%d",
		report.DroppedCitations, report.CanonicalizedCitations)
}

// When the critic still cannot accept the analysis at round three it must
// declare INSUFFICIENT_EVIDENCE instead of being forced into PASS; the
// writer and validator surface that shortfall honestly.
func TestResearchWorkflowInsufficientEvidence(t *testing.T) {
	h := newHarness(t, "insufficient", func(s *scenario) { s.criticAlwaysNeedsMore = true })
	id, err := h.createResearch("Thin-evidence probe where the critic should declare insufficiency")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout)

	gaps := memoryContentsJoined(t, h, id, "gaps")
	if !strings.Contains(gaps, `"status":"INSUFFICIENT_EVIDENCE"`) {
		t.Fatalf("final critic decision missing INSUFFICIENT_EVIDENCE: %.400s", gaps)
	}
	var validation struct {
		Valid                bool `json:"valid"`
		InsufficientEvidence bool `json:"insufficientEvidence"`
	}
	mustDecodeMemory(t, h, id, "report", "validation", &validation)
	if !validation.InsufficientEvidence {
		t.Fatalf("validation verdict must mirror the shortfall: %+v", validation)
	}
	report := memoryContentsJoined(t, h, id, "report")
	if !strings.Contains(report, `"insufficientEvidence":true`) {
		t.Fatal("shipped report does not declare insufficient evidence")
	}
	t.Logf("insufficiency surfaced honestly: valid=%v declared=true", validation.Valid)
}

// Tool failures recover through attempt retries without duplicate side
// effects: injected fetch failures are absorbed and the workflow completes.
func TestResearchWorkflowToolFailureRecovery(t *testing.T) {
	h := newHarness(t, "toolfailure", nil)
	h.webtools.InjectFetchFailures("corpus.fenced.dev", 2)
	before := h.fetches.total()
	id, err := h.createResearch("Assess sandboxing strategies for reader agents fetching untrusted content")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout+time.Minute)
	fetchDelta := h.fetches.total() - before
	if fetchDelta == 0 {
		t.Fatalf("no fetch activity observed")
	}
	var retriedReaderRuns, replacementReaderAttempts, failedReaderAttempts, failedToolCalls, evidenceRecords, readMarkers int
	if err := h.pool.QueryRow(context.Background(), `SELECT
			(SELECT COUNT(*) FROM runs r JOIN tasks t ON t.id = r.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND r.ordinal >= 2),
			(SELECT COUNT(*) FROM attempts a
			 JOIN runs r ON r.id = a.run_id JOIN tasks t ON t.id = r.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND r.ordinal >= 2),
			(SELECT COUNT(*) FROM attempts a
			 JOIN runs r ON r.id = a.run_id
			 JOIN tasks t ON t.id = r.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND a.phase = 'ATTEMPT_FAILED'),
			(SELECT COUNT(*) FROM tool_calls tc JOIN tasks t ON t.id = tc.task_id
			 WHERE t.workflow_id = $1 AND tc.status = 'FAILED'),
			(SELECT COUNT(*) FROM memory_records WHERE namespace = $2 AND key LIKE 'ev-%'),
			(SELECT COUNT(*) FROM memory_records WHERE namespace = $2 AND key LIKE 'read-%')`,
		id, fmt.Sprintf("research/%s/evidence", id)).Scan(
		&retriedReaderRuns, &replacementReaderAttempts, &failedReaderAttempts, &failedToolCalls, &evidenceRecords, &readMarkers); err != nil {
		t.Fatalf("query tool recovery evidence: %v", err)
	}
	if retriedReaderRuns == 0 || replacementReaderAttempts == 0 || failedReaderAttempts == 0 || failedToolCalls < 2 {
		t.Fatalf("tool failure did not exercise Reader retry: runs=%d replacements=%d failedAttempts=%d failedTools=%d",
			retriedReaderRuns, replacementReaderAttempts, failedReaderAttempts, failedToolCalls)
	}
	if evidenceRecords == 0 || readMarkers == 0 || evidenceRecords > readMarkers {
		t.Fatalf("invalid idempotent evidence state: evidence=%d readMarkers=%d", evidenceRecords, readMarkers)
	}
	t.Logf("tool-failure scenario complete: fetchCalls=%d retriedRuns=%d replacementAttempts=%d failedAttempts=%d failedTools=%d evidence=%d markers=%d",
		fetchDelta, retriedReaderRuns, replacementReaderAttempts, failedReaderAttempts, failedToolCalls, evidenceRecords, readMarkers)
}

// A Reader model invocation that remains unavailable after the provider
// executor must fail the Agent attempt without writing a read marker; the
// task's next Run then performs the extraction successfully.
func TestResearchWorkflowReaderModelRetry(t *testing.T) {
	h := newHarness(t, "readermodelretry", nil)
	h.provider.InjectRoleHTTPFailures("You extract verifiable factual claims", http.StatusServiceUnavailable, 2)
	id, err := h.createResearch("Reader-model recovery survey of agent runtime fencing")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout+time.Minute)

	var retriedReaderRuns, replacementReaderAttempts, failedReaderAttempts, failedModelCalls, evidenceRecords int
	if err := h.pool.QueryRow(context.Background(), `SELECT
			(SELECT COUNT(*) FROM runs r JOIN tasks t ON t.id = r.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND r.ordinal >= 2),
			(SELECT COUNT(*) FROM attempts a
			 JOIN runs r ON r.id = a.run_id JOIN tasks t ON t.id = r.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND r.ordinal >= 2),
			(SELECT COUNT(*) FROM attempts a
			 JOIN runs r ON r.id = a.run_id JOIN tasks t ON t.id = r.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND a.phase = 'ATTEMPT_FAILED'),
			(SELECT COUNT(*) FROM model_calls mc JOIN tasks t ON t.id = mc.task_id
			 WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0' AND mc.status = 'FAILED'),
			(SELECT COUNT(*) FROM memory_records WHERE namespace = $2 AND key LIKE 'ev-%')`,
		id, fmt.Sprintf("research/%s/evidence", id)).Scan(
		&retriedReaderRuns, &replacementReaderAttempts, &failedReaderAttempts, &failedModelCalls, &evidenceRecords); err != nil {
		t.Fatalf("query reader-model recovery evidence: %v", err)
	}
	if retriedReaderRuns == 0 || replacementReaderAttempts == 0 || failedReaderAttempts == 0 || failedModelCalls == 0 || evidenceRecords == 0 {
		t.Fatalf("Reader model failure did not recover through a new run/attempt: runs=%d replacements=%d failedAttempts=%d failedModels=%d evidence=%d",
			retriedReaderRuns, replacementReaderAttempts, failedReaderAttempts, failedModelCalls, evidenceRecords)
	}
	t.Logf("reader-model recovery complete: retriedRuns=%d replacementAttempts=%d failedAttempts=%d failedModels=%d evidence=%d",
		retriedReaderRuns, replacementReaderAttempts, failedReaderAttempts, failedModelCalls, evidenceRecords)
}

// A workflow budget too small to fit the fan-out stops the workflow cleanly
// instead of running unbounded.
func TestResearchWorkflowBudgetStop(t *testing.T) {
	h := newHarness(t, "budgetstop", nil)
	id, err := h.createResearch("Tiny-budget bounded dive into agent runtime isolation economics")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	workflow, _, err := h.awaitWorkflow(id, settleTimeout)
	if err != nil {
		t.Fatalf("await workflow: %v", err)
	}
	if workflow.Status == "" {
		t.Fatalf("workflow never reported a status")
	}
	t.Logf("budget scenario settled: status=%s budgetTasks=%d budgetTokens=%d",
		workflow.Status, workflow.BudgetMaxTasks, workflow.BudgetMaxTokens)
}

// Provider-side model failures (HTTP 429) are absorbed by the provider
// execution policy's bounded retry and the workflow still completes.
func TestResearchWorkflowModelFailure(t *testing.T) {
	h := newHarness(t, "modelfailure", nil)
	h.provider.InjectHTTPFailures(http.StatusTooManyRequests, 1)
	before := h.provider.callCount()
	id, err := h.createResearch("Model-failure resilient overview of agent runtime governance")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	h.requireCompleted(id, settleTimeout)
	if total := h.provider.callCount(); total <= before+1 {
		t.Fatalf("expected the provider retry to add calls: before=%d after=%d", before, total)
	}
	t.Logf("model-failure scenario complete: providerCalls=%d (includes 1 injected 429)", h.provider.callCount())
}

// Killing every worker mid-run strands in-flight attempts on expired leases;
// recovery must reclaim them and restarted runtimes must finish the work.
func TestResearchWorkflowRecovery(t *testing.T) {
	h := newHarness(t, "recovery", nil)
	id, err := h.createResearch("Crash-recovery validated survey of agent runtime infrastructure")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	// Wait until the fan-out is DEEP in flight and the sandbox worker
	// actually owns leases. The multi-runtime fleet runs the reader burst on
	// the single sandbox instance, so one leased instance with running work
	// is the crash window (the old two-worker fleet leased both instances).
	deadline := time.Now().Add(90 * time.Second)
	for {
		var running, attempts, leasedInstances int
		if err := h.pool.QueryRow(context.Background(),
			`SELECT (SELECT COUNT(*) FROM tasks WHERE phase = 'RUNNING'),
				(SELECT COUNT(*) FROM attempts),
				(SELECT COUNT(DISTINCT a.runtime_instance_id)
				 FROM runtime_leases l
				 JOIN attempts a ON a.id = l.attempt_id
				 WHERE l.released_at IS NULL)`).Scan(&running, &attempts, &leasedInstances); err != nil {
			t.Fatalf("count in-flight work: %v", err)
		}
		if running >= 1 && attempts >= 6 && leasedInstances >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fan-out never went deep enough to crash: running=%d attempts=%d leasedInstances=%d",
				running, attempts, leasedInstances)
		}
		time.Sleep(150 * time.Millisecond)
	}
	h.KillWorker("research-worker-02")
	// Advance at least one stranded lease to expiry rather than sleeping for
	// the 15-second heartbeat TTL. Repeating the update closes the narrow race
	// where an already-returning worker releases the first selected lease.
	// Recovery itself remains responsible for fencing the old attempt and
	// atomically placing its replacement.
	recoverDeadline := time.Now().Add(45 * time.Second)
	forcedExpirations := int64(0)
	for {
		var retried int
		if err := h.pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM attempts WHERE ordinal >= 2`).Scan(&retried); err != nil {
			t.Fatalf("count replacement attempts: %v", err)
		}
		if retried >= 1 {
			break
		}
		if time.Now().After(recoverDeadline) {
			t.Fatalf("recovery placed no replacement attempt after %d forced lease expirations", forcedExpirations)
		}
		tag, err := h.pool.Exec(context.Background(), `
			WITH candidate AS (
				SELECT l.id
				FROM runtime_leases l
				JOIN attempts a ON a.id = l.attempt_id
				JOIN runs r ON r.id = a.run_id
				WHERE l.released_at IS NULL
				  AND r.active_attempt_id = a.id
				  AND r.current_fencing_token = a.fencing_token
				ORDER BY l.acquired_at DESC
				LIMIT 1
			)
			UPDATE runtime_leases l
			SET expires_at = l.acquired_at + INTERVAL '1 microsecond'
			FROM candidate c
			WHERE l.id = c.id AND l.released_at IS NULL`)
		if err != nil {
			t.Fatalf("force stranded lease expiry: %v", err)
		}
		forcedExpirations += tag.RowsAffected()
		time.Sleep(100 * time.Millisecond)
	}
	// Restart the crashed sandbox instance: the requeued reader attempts
	// (same pool/instance per the kernel recovery contract) are picked up by
	// the restarted process.
	h.startWorker(h.loopCtx, "research-worker-02")

	h.requireCompleted(id, settleTimeout+3*time.Minute)

	var retriedRuns, retriedAttempts int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT
			(SELECT COUNT(*) FROM runs WHERE ordinal >= 2),
			(SELECT COUNT(*) FROM attempts WHERE ordinal >= 2)`).
		Scan(&retriedRuns, &retriedAttempts); err != nil {
		t.Fatalf("count retried units: %v", err)
	}
	// Lease-expiry recovery places attempt N+1 inside the SAME run; the
	// clean-failure requeue path starts a NEW run. Either proves reclaim.
	if retriedAttempts == 0 && retriedRuns == 0 {
		t.Fatalf("recovery placed no replacement attempts")
	}
	t.Logf("crash-recovery scenario complete: retriedRuns=%d retriedAttempts=%d", retriedRuns, retriedAttempts)
}

// Scale gate: 100 concurrent research workflows must all complete. Heavy —
// enabled explicitly with FENCED_RESEARCH_SCALE=1.
func TestResearch100Concurrent(t *testing.T) {
	if os.Getenv("FENCED_RESEARCH_SCALE") != "1" {
		t.Skip("set FENCED_RESEARCH_SCALE=1 to run the 100-way scale scenario")
	}
	h := newHarness(t, "scale100", nil)
	const fanOut = 100
	ids := make([]uuid.UUID, 0, fanOut)
	for index := 0; index < fanOut; index++ {
		id, err := h.createResearch(fmt.Sprintf(
			"Scale probe %03d: outlook on agent runtime infrastructure", index))
		if err != nil {
			t.Fatalf("create research %d: %v", index, err)
		}
		ids = append(ids, id)
	}
	var failed []string
	for _, id := range ids {
		workflow, _, err := h.awaitWorkflow(id, 12*time.Minute)
		if err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		if status := workflow.Status; status != "SUCCEEDED" && status != "FAILED" && status != "CANCELLED" {
			failed = append(failed, fmt.Sprintf("%s unsettled: %s", id, status))
		} else if status != "SUCCEEDED" {
			failed = append(failed, fmt.Sprintf("%s terminal=%s", id, status))
		}
	}
	if len(failed) > 0 {
		t.Fatalf("%d/%d workflows did not succeed: %v", len(failed), fanOut, failed[:minInt(5, len(failed))])
	}
	t.Logf("scale scenario complete: %d/%d succeeded", fanOut, fanOut)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// -- memory/store probes -----------------------------------------------------

func listSteps(t *testing.T, h *harness, id fmt.Stringer) []kernelstore.WorkflowStep {
	t.Helper()
	workflowID, err := uuid.Parse(id.String())
	if err != nil {
		t.Fatalf("parse workflow id: %v", err)
	}
	steps, err := h.store.ListWorkflowSteps(context.Background(), researchTenant, workflowID)
	if err != nil {
		t.Fatalf("list steps: %v", err)
	}
	return steps
}

func memoryContentsJoined(t *testing.T, h *harness, id fmt.Stringer, leaf string) string {
	t.Helper()
	rows, err := h.pool.Query(context.Background(),
		`SELECT content FROM memory_records WHERE namespace = $1 ORDER BY key`, fmt.Sprintf("research/%s/%s", id, leaf))
	if err != nil {
		t.Fatalf("query memory %s: %v", leaf, err)
	}
	defer rows.Close()
	var joined strings.Builder
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			t.Fatalf("scan memory row: %v", err)
		}
		joined.WriteString(content)
		joined.WriteString("\n")
	}
	return joined.String()
}

func memoryKeyCount(t *testing.T, h *harness, id fmt.Stringer, leaf string) int {
	t.Helper()
	var count int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM memory_records WHERE namespace = $1`, fmt.Sprintf("research/%s/%s", id, leaf)).Scan(&count); err != nil {
		t.Fatalf("count memory %s: %v", leaf, err)
	}
	return count
}

func mustDecodeMemory(t *testing.T, h *harness, id fmt.Stringer, leaf, key string, target any) {
	t.Helper()
	var content string
	err := h.pool.QueryRow(context.Background(),
		`SELECT content FROM memory_records WHERE namespace = $1 AND key = $2`,
		fmt.Sprintf("research/%s/%s", id, leaf), key).Scan(&content)
	if err != nil {
		t.Fatalf("memory record %s/%s: %v", leaf, key, err)
	}
	if err := json.Unmarshal([]byte(content), target); err != nil {
		t.Fatalf("decode %s/%s: %v (%.200s)", leaf, key, err, content)
	}
}

// -- generic helpers ---------------------------------------------------------

// liveMetrics mirrors the observation set of implementation-plan §5: one
// JSON document per live research run, written next to the test log.
type liveMetrics struct {
	WorkflowID           string  `json:"workflowId"`
	Questions            int     `json:"questions"`
	Sources              int     `json:"sources"`
	UniqueDomains        int     `json:"uniqueDomains"`
	EvidenceCount        int     `json:"evidenceCount"`
	GroundedEvidenceRate float64 `json:"groundedEvidenceRate"`
	CitationCoverage     float64 `json:"citationCoverage"`
	UnsupportedCitations int     `json:"unsupportedCitations"`
	Retries              int     `json:"retries"`
	InsufficientEvidence bool    `json:"insufficientEvidence"`
	CriticRounds         int     `json:"criticRounds"`
	ModelCalls           int     `json:"modelCalls"`
	ModelFailures        int     `json:"modelFailures"`
	ToolCalls            int     `json:"toolCalls"`
	RecoveredAttempts    int     `json:"recoveredAttempts"`
	Tokens               int64   `json:"tokens"`
	CostUSD              float64 `json:"costUsd"`
	DurationSeconds      float64 `json:"durationSeconds"`
}

type liveEvidence struct {
	SchemaVersion  string            `json:"schemaVersion"`
	Test           string            `json:"test"`
	Commit         string            `json:"commit"`
	CommitDirty    bool              `json:"commitDirty"`
	Goal           string            `json:"goal"`
	SearchProvider string            `json:"searchProvider"`
	ModelProvider  string            `json:"modelProvider"`
	Models         map[string]string `json:"models"`
	Status         string            `json:"status"`
	StartedAt      string            `json:"startedAt"`
	CompletedAt    string            `json:"completedAt"`
	liveMetrics
}

func TestLiveEvidenceSchemaContainsMetricsAndNoCredentialFields(t *testing.T) {
	encoded, err := json.Marshal(liveEvidence{
		SchemaVersion: "fenced.research.live-evidence/v1", Commit: strings.Repeat("a", 40),
		Status: "SUCCEEDED", liveMetrics: liveMetrics{WorkflowID: uuid.NewString(), EvidenceCount: 3},
	})
	if err != nil {
		t.Fatalf("encode evidence schema: %v", err)
	}
	text := string(encoded)
	for _, required := range []string{`"commit"`, `"workflowId"`, `"evidenceCount"`, `"status"`} {
		if !strings.Contains(text, required) {
			t.Fatalf("evidence schema missing %s: %s", required, text)
		}
	}
	for _, forbidden := range []string{"apiKey", "authorization", "credential", "modelKey", "searchKey"} {
		if strings.Contains(strings.ToLower(text), strings.ToLower(forbidden)) {
			t.Fatalf("evidence schema exposed credential field %q: %s", forbidden, text)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func gitEvidenceIdentity(t *testing.T) (string, bool) {
	t.Helper()
	root := repositoryRoot(t)
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("resolve evidence commit: %v", err)
	}
	status := exec.Command("git", "status", "--porcelain", "--untracked-files=no")
	status.Dir = root
	dirty, err := status.Output()
	if err != nil {
		t.Fatalf("resolve evidence worktree state: %v", err)
	}
	return strings.TrimSpace(string(output)), len(bytes.TrimSpace(dirty)) > 0
}

func writeLiveEvidence(t *testing.T, testName, goal, status string, started time.Time, metrics liveMetrics) string {
	t.Helper()
	commit, dirty := gitEvidenceIdentity(t)
	evidence := liveEvidence{
		SchemaVersion: "fenced.research.live-evidence/v1", Test: testName,
		Commit: commit, CommitDirty: dirty, Goal: goal,
		SearchProvider: strings.ToLower(envOr("FENCED_RESEARCH_SEARCH_PROVIDER", "corpus")),
		ModelProvider:  envOr("FENCED_RESEARCH_MODEL_PROVIDER", "openai"),
		Models: map[string]string{
			"fast":      envOr("FENCED_RESEARCH_MODEL_FAST", "gpt-4o-mini"),
			"reader":    envOr("FENCED_RESEARCH_MODEL_READER", envOr("FENCED_RESEARCH_MODEL_FAST", "gpt-4o-mini")),
			"reasoning": envOr("FENCED_RESEARCH_MODEL_REASONING", "gpt-4o"),
		},
		Status: status, StartedAt: started.UTC().Format(time.RFC3339Nano),
		CompletedAt: time.Now().UTC().Format(time.RFC3339Nano), liveMetrics: metrics,
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("encode live evidence: %v", err)
	}
	for _, credentialName := range []string{"FENCED_RESEARCH_SEARCH_KEY", "FENCED_RESEARCH_MODEL_KEY"} {
		credential := os.Getenv(credentialName)
		if credential != "" && bytes.Contains(encoded, []byte(credential)) {
			t.Fatalf("live evidence contains credential %s", credentialName)
		}
	}
	directory := strings.TrimSpace(os.Getenv("FENCED_RESEARCH_EVIDENCE_DIR"))
	if directory == "" {
		directory = filepath.Join(repositoryRoot(t), "artifacts", "research-live")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		t.Fatalf("create live evidence directory: %v", err)
	}
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	shortID := strings.ReplaceAll(metrics.WorkflowID, "-", "")
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}
	path := filepath.Join(directory, fmt.Sprintf("%s-%s-%s.json", testName, stamp, shortID))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create live evidence: %v", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		t.Fatalf("write live evidence: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close live evidence: %v", err)
	}
	t.Logf("LIVE EVIDENCE %s", path)
	return path
}

// collectLiveMetrics aggregates run evidence straight from the durable
// store so live and deterministic runs report identically.
func (h *harness) collectLiveMetrics(id uuid.UUID, started time.Time) liveMetrics {
	h.t.Helper()
	ctx := context.Background()
	metrics := liveMetrics{WorkflowID: id.String(), DurationSeconds: time.Since(started).Seconds()}

	evidenceJSON := memoryContentsJoined(h.t, h, id, "evidence")
	sourcesJSON := memoryContentsJoined(h.t, h, id, "sources")
	metrics.EvidenceCount = strings.Count(evidenceJSON, `"claimId"`)
	grounded := strings.Count(evidenceJSON, `"grounded":true`)
	if metrics.EvidenceCount > 0 {
		metrics.GroundedEvidenceRate = float64(grounded) / float64(metrics.EvidenceCount)
	}
	metrics.Sources = strings.Count(sourcesJSON, `"url"`)

	domains := map[string]bool{}
	for _, raw := range strings.Split(sourcesJSON+evidenceJSON, `"url":"`) {
		if len(raw) == 0 {
			continue
		}
		end := strings.IndexByte(raw, '"')
		if end < 0 {
			continue
		}
		candidate, err := strconv.Unquote(`"` + raw[:end] + `"`)
		if err != nil {
			continue
		}
		parsed, err := url.Parse(candidate)
		if err != nil {
			continue
		}
		if host := parsed.Hostname(); host != "" && host != "corpus.fenced.dev" {
			domains[host] = true
		}
	}
	metrics.UniqueDomains = len(domains)

	var plan struct {
		Questions []json.RawMessage `json:"questions"`
	}
	mustDecodeMemory(h.t, h, id, "analysis", "plan", &plan)
	metrics.Questions = len(plan.Questions)

	var validation struct {
		CitationCoverage     float64 `json:"citationCoverage"`
		UnsupportedClaims    int     `json:"unsupportedClaims"`
		Retries              int     `json:"retries"`
		InsufficientEvidence bool    `json:"insufficientEvidence"`
	}
	mustDecodeMemory(h.t, h, id, "report", "validation", &validation)
	metrics.CitationCoverage = validation.CitationCoverage
	metrics.UnsupportedCitations = validation.UnsupportedClaims
	metrics.Retries = validation.Retries
	metrics.InsufficientEvidence = validation.InsufficientEvidence

	scans := []struct {
		query  string
		target *int
	}{
		{`SELECT COUNT(DISTINCT name) FROM workflow_steps WHERE workflow_id = $1 AND name LIKE 'critic-r%' AND status = 'SUCCEEDED'`, &metrics.CriticRounds},
		{`SELECT COUNT(*) FROM model_calls mc JOIN tasks t ON t.id = mc.task_id WHERE t.workflow_id = $1 AND mc.status = 'COMPLETED'`, &metrics.ModelCalls},
		{`SELECT COUNT(*) FROM model_calls mc JOIN tasks t ON t.id = mc.task_id WHERE t.workflow_id = $1 AND mc.status = 'FAILED'`, &metrics.ModelFailures},
		{`SELECT COUNT(*) FROM tool_calls tc JOIN tasks t ON t.id = tc.task_id WHERE t.workflow_id = $1`, &metrics.ToolCalls},
		{`SELECT COUNT(*) FROM attempts a JOIN runs r ON r.id = a.run_id WHERE r.task_id IN (SELECT id FROM tasks WHERE workflow_id = $1) AND a.ordinal >= 2`, &metrics.RecoveredAttempts},
	}
	for _, scan := range scans {
		if err := h.pool.QueryRow(ctx, scan.query, id).Scan(scan.target); err != nil {
			h.t.Fatalf("metric query: %v", err)
		}
	}
	if err := h.pool.QueryRow(ctx, `SELECT
			COALESCE(SUM(input_tokens + output_tokens), 0),
			COALESCE(SUM(cost_micro_usd), 0)::double precision / 1000000.0
			FROM model_calls mc JOIN tasks t ON t.id = mc.task_id
			WHERE t.workflow_id = $1 AND mc.status = 'COMPLETED'`, id).Scan(&metrics.Tokens, &metrics.CostUSD); err != nil {
		h.t.Fatalf("usage query: %v", err)
	}
	return metrics
}

func requireLiveModelEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("FENCED_RESEARCH_LIVE") != "1" {
		t.Skip("set FENCED_RESEARCH_LIVE=1 with FENCED_RESEARCH_MODEL_BASE_URL/KEY to run the live-model acceptance")
	}
}

func requireLiveFullGates(t *testing.T, metrics liveMetrics) {
	t.Helper()
	if metrics.GroundedEvidenceRate != 1 {
		t.Fatalf("grounded evidence rate %.3f < 1.0", metrics.GroundedEvidenceRate)
	}
	if metrics.UnsupportedCitations != 0 || metrics.CitationCoverage < 0.90 {
		t.Fatalf("citation gates failed: coverage=%.2f unsupported=%d",
			metrics.CitationCoverage, metrics.UnsupportedCitations)
	}
	if metrics.UniqueDomains < 3 {
		t.Fatalf("unique domains %d < 3", metrics.UniqueDomains)
	}
	if metrics.InsufficientEvidence {
		t.Fatal("live run must not end INSUFFICIENT_EVIDENCE")
	}
}

func TestResearchWorkflowLiveModel(t *testing.T) {
	requireLiveModelEnv(t)
	started := time.Now()
	h := newHarness(t, "live-model", nil)
	id, err := h.createResearch("Summarize how agent runtime control planes evolved between 2023 and 2025")
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	workflow := h.requireCompleted(id, liveModelTimeout)

	var validation struct {
		CitationCoverage  float64 `json:"citationCoverage"`
		UnsupportedClaims int     `json:"unsupportedClaims"`
	}
	mustDecodeMemory(t, h, id, "report", "validation", &validation)
	if validation.UnsupportedClaims != 0 || validation.CitationCoverage < 0.90 {
		t.Fatalf("live acceptance gate failed: coverage=%.2f unsupported=%d",
			validation.CitationCoverage, validation.UnsupportedClaims)
	}
	evidenceJSON := memoryContentsJoined(t, h, id, "evidence")
	if strings.Contains(evidenceJSON, `"grounded":false`) {
		t.Fatal("ungrounded claim persisted in live run")
	}
	metrics := h.collectLiveMetrics(id, started)
	encoded, _ := json.MarshalIndent(metrics, "", " ")
	t.Logf("LIVE MODEL METRICS %s\n%s", workflow.Status, encoded)
	writeLiveEvidence(t, "live-model", "Summarize how agent runtime control planes evolved between 2023 and 2025",
		string(workflow.Status), started, metrics)
}

func TestLiveModelScenarioKeepsDeterministicWebBoundary(t *testing.T) {
	if liveWebForScenario("live-model", true) {
		t.Fatal("live-model must not inherit the process-level live-web gate")
	}
	if !liveWebForScenario("live-full", true) || !liveWebForScenario("live-recovery", true) {
		t.Fatal("full and recovery scenarios must retain live web when enabled")
	}
	if liveWebForScenario("live-full", false) {
		t.Fatal("disabled live-web gate must remain disabled")
	}
}

func TestResearchWorkerFleetMatchesFanout(t *testing.T) {
	if got := len(researchWorkerInstances(false, false)); got != 4 {
		t.Fatalf("deterministic worker count = %d, want 4 (reasoning x2, network, sandbox)", got)
	}
	if got := len(researchWorkerInstances(true, false)); got != liveResearchWorkerCount {
		t.Fatalf("live worker count = %d, want reader fan-out %d", got, liveResearchWorkerCount)
	}
	if got := len(researchWorkerInstances(false, true)); got != liveResearchWorkerCount {
		t.Fatalf("scale worker count = %d, want reader fan-out %d", got, liveResearchWorkerCount)
	}
}

func TestResearchWorkflowLiveFull(t *testing.T) {
	requireLiveModelEnv(t)
	if os.Getenv("FENCED_RESEARCH_LIVE_WEB") != "1" {
		t.Skip("set FENCED_RESEARCH_LIVE_WEB=1 with FENCED_RESEARCH_SEARCH_PROVIDER/KEY for full live research")
	}
	started := time.Now()
	goal := strings.TrimSpace(os.Getenv("FENCED_RESEARCH_LIVE_GOAL"))
	if goal == "" {
		t.Fatal("set FENCED_RESEARCH_LIVE_GOAL with a real research question")
	}
	h := newHarness(t, "live-full", nil)
	id, err := h.createResearch(goal)
	if err != nil {
		t.Fatalf("create research: %v", err)
	}
	workflow := h.requireCompleted(id, liveFullTimeout)

	metrics := h.collectLiveMetrics(id, started)
	// Acceptance gates from implementation plan §5.
	requireLiveFullGates(t, metrics)
	encoded, _ := json.MarshalIndent(metrics, "", " ")
	t.Logf("LIVE FULL METRICS\n%s", encoded)
	writeLiveEvidence(t, "live-full", goal, string(workflow.Status), started, metrics)
}

// forceReaderLeaseRecovery crashes whichever runtime currently owns a live
// Reader lease, advances that lease to expiry, and waits until the recovery
// controller places Attempt ordinal N+1 in the same Run.
func (h *harness) forceReaderLeaseRecovery(id uuid.UUID, timeout time.Duration) string {
	h.t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	instance := ""
	for instance == "" {
		workflow, err := h.store.GetWorkflow(ctx, researchTenant, id)
		if err != nil {
			h.t.Fatalf("read live recovery workflow: %v", err)
		}
		switch workflow.Status {
		case kernelstore.WorkflowFailed, kernelstore.WorkflowCancelled:
			steps, listErr := h.store.ListWorkflowSteps(ctx, researchTenant, id)
			if listErr == nil {
				for _, step := range steps {
					h.t.Logf("step %-22s status=%-10s attempt=%d code=%q",
						step.Name, step.Status, step.AttemptCount, step.FailureCode)
				}
			}
			h.t.Fatalf("workflow reached %s before an active Reader lease appeared", workflow.Status)
		}
		err = h.pool.QueryRow(ctx, `SELECT a.runtime_instance_id
			FROM runtime_leases l
			JOIN attempts a ON a.id = l.attempt_id
			JOIN runs r ON r.id = a.run_id
			JOIN tasks t ON t.id = r.task_id
			WHERE t.workflow_id = $1
			  AND t.agent_version_ref = 'research-reader@1.0.0'
			  AND l.released_at IS NULL
			  AND r.active_attempt_id = a.id
			ORDER BY l.acquired_at
			LIMIT 1`, id).Scan(&instance)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			h.t.Fatalf("find live Reader lease: %v", err)
		}
		if time.Now().After(deadline) {
			h.t.Fatal("no active Reader lease appeared before live recovery deadline")
		}
		if instance == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	h.KillWorker(instance)

	forced := int64(0)
	for {
		var recovered int
		if err := h.pool.QueryRow(ctx, `SELECT COUNT(*)
			FROM attempts a JOIN runs r ON r.id = a.run_id JOIN tasks t ON t.id = r.task_id
			WHERE t.workflow_id = $1 AND t.agent_version_ref = 'research-reader@1.0.0'
			  AND a.ordinal >= 2`, id).Scan(&recovered); err != nil {
			h.t.Fatalf("count live recovered Reader attempts: %v", err)
		}
		if recovered > 0 {
			break
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("live Reader lease was not recovered after %d forced expirations", forced)
		}
		tag, err := h.pool.Exec(ctx, `WITH candidate AS (
				SELECT l.id FROM runtime_leases l
				JOIN attempts a ON a.id = l.attempt_id
				JOIN runs r ON r.id = a.run_id
				JOIN tasks t ON t.id = r.task_id
				WHERE t.workflow_id = $1
				  AND t.agent_version_ref = 'research-reader@1.0.0'
				  AND a.runtime_instance_id = $2
				  AND l.released_at IS NULL
				  AND r.active_attempt_id = a.id
				  AND r.current_fencing_token = a.fencing_token
				ORDER BY l.acquired_at DESC LIMIT 1
			)
			UPDATE runtime_leases l
			SET expires_at = l.acquired_at + INTERVAL '1 microsecond'
			FROM candidate c WHERE l.id = c.id AND l.released_at IS NULL`, id, instance)
		if err != nil {
			h.t.Fatalf("force live Reader lease expiry: %v", err)
		}
		forced += tag.RowsAffected()
		time.Sleep(100 * time.Millisecond)
	}
	h.startWorker(h.loopCtx, instance)
	h.t.Logf("live Reader worker recovery injected: instance=%s forcedExpirations=%d", instance, forced)
	return instance
}

func TestResearchWorkflowLiveRecovery(t *testing.T) {
	requireLiveModelEnv(t)
	if os.Getenv("FENCED_RESEARCH_LIVE_WEB") != "1" {
		t.Skip("set FENCED_RESEARCH_LIVE_WEB=1 with FENCED_RESEARCH_SEARCH_PROVIDER/KEY for live recovery")
	}
	started := time.Now()
	goal := strings.TrimSpace(os.Getenv("FENCED_RESEARCH_LIVE_GOAL"))
	if goal == "" {
		t.Fatal("set FENCED_RESEARCH_LIVE_GOAL with a real research question")
	}
	h := newHarness(t, "live-recovery", nil)
	id, err := h.createResearch(goal)
	if err != nil {
		t.Fatalf("create live recovery research: %v", err)
	}
	h.forceReaderLeaseRecovery(id, 10*time.Minute)
	workflow := h.requireCompleted(id, liveFullTimeout)
	metrics := h.collectLiveMetrics(id, started)
	requireLiveFullGates(t, metrics)
	if metrics.RecoveredAttempts < 1 {
		t.Fatalf("live recovery produced no recovered attempts: %+v", metrics)
	}
	encoded, _ := json.MarshalIndent(metrics, "", " ")
	t.Logf("LIVE RECOVERY METRICS\n%s", encoded)
	writeLiveEvidence(t, "live-recovery", goal, string(workflow.Status), started, metrics)
}

func extractOutput(summary json.RawMessage) string {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(summary, &document); err != nil {
		return ""
	}
	output, ok := document["output"]
	if !ok {
		return string(summary)
	}
	var text string
	if json.Unmarshal(output, &text) == nil {
		return text
	}
	// Structured outputs are stored as nested JSON objects; re-marshal
	// compactly so whitespace never breaks substring assertions.
	var value any
	if json.Unmarshal(output, &value) == nil {
		if encoded, err := json.Marshal(value); err == nil {
			return string(encoded)
		}
	}
	return string(output)
}

func renderSteps(steps []kernelstore.WorkflowStep) string {
	parts := make([]string, 0, len(steps))
	for _, step := range steps {
		parts = append(parts, fmt.Sprintf("%s=%s", step.Name, step.Status))
	}
	return strings.Join(parts, ",")
}

func truncateString(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}
