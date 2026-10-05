package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/domain"
)

// RunView is the materialized application view of one research run: the §5
// domain objects plus derived counters the API and CLI surface.
type RunView struct {
	Run           domain.ResearchRun
	Questions     []domain.ResearchQuestion
	Sources       []domain.Source
	Evidence      []domain.Evidence
	Findings      []domain.Finding
	Gaps          []domain.ResearchGap
	CriticVerdict string
	StepCount     int
	Retries       int
}

// ResearchIDFor derives the stable application research ID of one workflow.
func ResearchIDFor(workflowID string) string {
	return "research-" + workflowID
}

// WorkflowIDFrom parses the research ID back into the kernel workflow ID.
func WorkflowIDFrom(researchID string) (string, error) {
	workflowID, ok := strings.CutPrefix(researchID, "research-")
	if !ok || workflowID == "" {
		return "", fmt.Errorf("invalid research id %q", researchID)
	}
	return workflowID, nil
}

// LoadRunView materializes one research run from the workflow document and
// its memory namespaces. workflowID is the kernel workflow UUID used for the
// Control API; namespaceKey is the id embedded in the agent envelopes (the
// memory namespace is research/<namespaceKey>). When the envelope key equals
// the kernel workflow ID (direct-store test paths), pass namespaceKey = "".
// Memory queries follow the runtime's own search tokens so the hybrid index
// reliably matches each record family.
func (c *Client) LoadRunView(ctx context.Context, workflowID, namespaceKey string) (RunView, error) {
	if namespaceKey == "" {
		namespaceKey = workflowID
	}
	workflow, err := c.GetWorkflow(ctx, workflowID)
	if err != nil {
		return RunView{}, err
	}
	steps := make([]domain.StepView, 0, len(workflow.Steps))
	for _, step := range workflow.Steps {
		steps = append(steps, domain.StepView{Name: step.Name, Status: step.Status, ParentStepName: step.ParentStepName})
	}
	state := domain.DeriveRunState(domain.WorkflowView{
		Status: workflow.Status, FailureCode: workflow.FailureCode, Steps: steps,
	})
	run := domain.ResearchRun{
		ID:         ResearchIDFor(workflow.ID),
		WorkflowID: workflow.ID,
		Goal:       workflow.Goal,
		Status:     string(state),
		CreatedAt:  workflow.CreatedAt,
	}
	if state.Terminal() {
		completedAt := workflow.UpdatedAt
		run.CompletedAt = &completedAt
	}

	view := RunView{Run: run, StepCount: len(workflow.Steps)}
	for _, step := range workflow.Steps {
		if step.AttemptCount > 1 {
			view.Retries += step.AttemptCount - 1
		}
	}

	namespace := "research/" + namespaceKey
	if questions, err := c.loadQuestions(ctx, namespace); err != nil {
		return RunView{}, fmt.Errorf("questions: %w", err)
	} else {
		view.Questions = questions
	}
	if sources, err := c.loadSources(ctx, namespace); err != nil {
		return RunView{}, fmt.Errorf("sources: %w", err)
	} else {
		view.Sources = sources
	}
	if evidence, err := c.loadEvidence(ctx, namespace); err != nil {
		return RunView{}, fmt.Errorf("evidence: %w", err)
	} else {
		view.Evidence = evidence
	}
	if findings, analysisRounds, err := c.loadFindings(ctx, namespace); err != nil {
		return RunView{}, fmt.Errorf("findings: %w", err)
	} else {
		view.Findings = findings
		if gaps, verdict, err := c.loadGaps(ctx, namespace, analysisRounds); err != nil {
			return RunView{}, fmt.Errorf("gaps: %w", err)
		} else {
			view.Gaps = gaps
			view.CriticVerdict = verdict
		}
	}
	return view, nil
}

// UsageSummary aggregates the cumulative settlement usage of every workflow
// step task (tokens, tool calls, cost, wall time).
type UsageSummary struct {
	Tokens      int64   `json:"tokens"`
	ToolCalls   int64   `json:"toolCalls"`
	CostUSD     float64 `json:"costUsd"`
	WallSeconds int64   `json:"wallSeconds"`
}

// LoadUsage sums the task usage of every step task. Every declared step has
// a taskId once the orchestrator dispatches it, so at terminal state the
// aggregation is complete; dynamic children appear in the step list as the
// workflow extends.
func (c *Client) LoadUsage(ctx context.Context, workflowID string) (UsageSummary, error) {
	workflow, err := c.GetWorkflow(ctx, workflowID)
	if err != nil {
		return UsageSummary{}, err
	}
	var summary UsageSummary
	for _, step := range workflow.Steps {
		if step.TaskID == "" {
			continue
		}
		task, err := c.GetTask(ctx, step.TaskID)
		if err != nil {
			return UsageSummary{}, fmt.Errorf("task %s (step %s): %w", step.TaskID, step.Name, err)
		}
		if task.Usage == nil {
			continue
		}
		summary.Tokens += task.Usage.Tokens
		summary.ToolCalls += task.Usage.ToolCalls
		summary.CostUSD += task.Usage.CostUSD
		summary.WallSeconds += task.Usage.WallSeconds
	}
	return summary, nil
}

// -- memory record decoding --------------------------------------------------

type plannerWireQuestion struct {
	ID            string   `json:"id"`
	Question      string   `json:"question"`
	Priority      int      `json:"priority"`
	SearchQueries []string `json:"searchQueries"`
}

func (c *Client) loadQuestions(ctx context.Context, namespace string) ([]domain.ResearchQuestion, error) {
	records, err := c.SearchMemories(ctx, namespace+"/analysis", "questions", 10)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Key != "plan" {
			continue
		}
		var plan struct {
			Questions []plannerWireQuestion `json:"questions"`
		}
		if err := json.Unmarshal([]byte(record.Content), &plan); err != nil {
			return nil, fmt.Errorf("decode plan record %s: %w", record.ID, err)
		}
		questions := make([]domain.ResearchQuestion, 0, len(plan.Questions))
		for _, question := range plan.Questions {
			questions = append(questions, domain.ResearchQuestion{
				ID: question.ID, ResearchRunID: strings.TrimPrefix(namespace, "research/"),
				Question: question.Question, Priority: question.Priority, Status: "OPEN",
			})
		}
		return questions, nil
	}
	return nil, nil
}

type sourceHitWire struct {
	SourceID           string `json:"sourceId"`
	Title              string `json:"title"`
	URL                string `json:"url"`
	PublishedAt        string `json:"publishedAt"`
	ResearchQuestionID string `json:"researchQuestionId"`
}

func (c *Client) loadSources(ctx context.Context, namespace string) ([]domain.Source, error) {
	records, err := c.SearchMemories(ctx, namespace+"/sources", "sourceId", 100)
	if err != nil {
		return nil, err
	}
	byID := map[string]domain.Source{}
	for _, record := range records {
		if !strings.HasPrefix(record.Key, "found-") {
			continue
		}
		// The search role persists a bare source array (roles.go); tolerate
		// the wrapper shape for forward compatibility.
		var hits []sourceHitWire
		if err := json.Unmarshal([]byte(record.Content), &hits); err != nil {
			var batch struct {
				Sources []sourceHitWire `json:"sources"`
			}
			if err := json.Unmarshal([]byte(record.Content), &batch); err != nil {
				return nil, fmt.Errorf("decode sources record %s: %w", record.ID, err)
			}
			hits = batch.Sources
		}
		for _, hit := range hits {
			if hit.SourceID == "" || hit.URL == "" {
				continue
			}
			byID[hit.SourceID] = domain.Source{
				ID: hit.SourceID, QuestionID: hit.ResearchQuestionID, URL: hit.URL,
				Title: hit.Title, PublishedAt: hit.PublishedAt,
				RetrievedAt: time.Now().UTC().Format(time.RFC3339),
			}
		}
	}
	return sortedSources(byID), nil
}

func sortedSources(byID map[string]domain.Source) []domain.Source {
	sources := make([]domain.Source, 0, len(byID))
	for _, source := range byID {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })
	return sources
}

type evidenceClaimWire struct {
	ClaimID    string  `json:"claimId"`
	Claim      string  `json:"claim"`
	Evidence   string  `json:"evidence"`
	Confidence float64 `json:"confidence"`
}

type evidenceBundleWire struct {
	SourceID           string              `json:"sourceId"`
	ResearchQuestionID string              `json:"researchQuestionId"`
	Claims             []evidenceClaimWire `json:"claims"`
}

func (c *Client) loadEvidence(ctx context.Context, namespace string) ([]domain.Evidence, error) {
	records, err := c.SearchMemories(ctx, namespace+"/evidence", "claims evidence", 100)
	if err != nil {
		return nil, err
	}
	evidence := make([]domain.Evidence, 0, 30)
	for _, record := range records {
		if !strings.HasPrefix(record.Key, "ev-") {
			continue
		}
		var bundle evidenceBundleWire
		if err := json.Unmarshal([]byte(record.Content), &bundle); err != nil {
			return nil, fmt.Errorf("decode evidence record %s: %w", record.ID, err)
		}
		for _, claim := range bundle.Claims {
			if claim.ClaimID == "" || claim.Claim == "" {
				continue
			}
			evidence = append(evidence, domain.Evidence{
				ID: claim.ClaimID, SourceID: bundle.SourceID, QuestionID: bundle.ResearchQuestionID,
				Claim: claim.Claim, Evidence: claim.Evidence, Confidence: claim.Confidence,
			})
		}
	}
	return evidence, nil
}

type analysisFindingWire struct {
	FindingID   string   `json:"findingId"`
	Statement   string   `json:"statement"`
	EvidenceIDs []string `json:"evidenceIds"`
	Confidence  float64  `json:"confidence"`
}

func (c *Client) loadFindings(ctx context.Context, namespace string) ([]domain.Finding, map[int]bool, error) {
	records, err := c.SearchMemories(ctx, namespace+"/analysis", "findings", 50)
	if err != nil {
		return nil, nil, err
	}
	analysisRounds := map[int]bool{}
	var latest []domain.Finding
	latestRound := -1
	for _, record := range records {
		round, matched := analysisRound(record.Key)
		if !matched {
			continue
		}
		analysisRounds[round] = true
		var document struct {
			Findings []analysisFindingWire `json:"findings"`
		}
		if err := json.Unmarshal([]byte(record.Content), &document); err != nil {
			return nil, nil, fmt.Errorf("decode analysis record %s: %w", record.ID, err)
		}
		if round >= latestRound {
			latestRound = round
			latest = make([]domain.Finding, 0, len(document.Findings))
			for _, finding := range document.Findings {
				latest = append(latest, domain.Finding{
					ID: finding.FindingID, Statement: finding.Statement,
					EvidenceIDs: finding.EvidenceIDs, Confidence: finding.Confidence,
				})
			}
		}
	}
	return latest, analysisRounds, nil
}

type criticGapWire struct {
	GapID    string `json:"gapId"`
	Question string `json:"question"`
	Severity string `json:"severity"`
}

type criticDecisionWire struct {
	Status string          `json:"status"`
	Gaps   []criticGapWire `json:"gaps"`
}

func (c *Client) loadGaps(ctx context.Context, namespace string, analysisRounds map[int]bool) ([]domain.ResearchGap, string, error) {
	records, err := c.SearchMemories(ctx, namespace+"/gaps", "status", 10)
	if err != nil {
		return nil, "", err
	}
	type decision struct {
		round   int
		verdict string
		decoded criticDecisionWire
	}
	byKey := map[string]decision{}
	for _, record := range records {
		round, matched := analysisRound(record.Key)
		if !matched {
			continue
		}
		var decoded criticDecisionWire
		if err := json.Unmarshal([]byte(record.Content), &decoded); err != nil {
			return nil, "", fmt.Errorf("decode critic record %s: %w", record.ID, err)
		}
		// Later revisions of one round overwrite earlier ones; only the
		// latest decision of each round survives.
		byKey[record.Key] = decision{round: round, verdict: decoded.Status, decoded: decoded}
	}
	latestRound := -1
	verdict := ""
	for _, entry := range byKey {
		if entry.round > latestRound {
			latestRound = entry.round
			verdict = entry.verdict
		}
	}
	gaps := make([]domain.ResearchGap, 0, 4)
	for _, entry := range byKey {
		for _, gap := range entry.decoded.Gaps {
			gaps = append(gaps, domain.ResearchGap{
				ID: gap.GapID, Question: gap.Question, Severity: gap.Severity,
				Resolved: analysisRounds[entry.round+1],
			})
		}
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].ID < gaps[j].ID })
	return gaps, verdict, nil
}

// analysisRound extracts the round from "analysis-rN" / "critic-rN" keys.
func analysisRound(key string) (int, bool) {
	_, suffix, found := strings.Cut(key, "-r")
	if !found || suffix == "" {
		return 0, false
	}
	round, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, false
	}
	return round, true
}
