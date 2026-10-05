// Package domain holds the research domain model of the reference
// application (design doc §5). These objects live in the application layer
// on purpose: the Fenced kernel never sees them, and every state they
// carry is derived from observable kernel surfaces (workflow documents,
// memory namespaces), never from kernel internals.
package domain

import "time"

// ResearchRun is one research submission (§5). The ID is the stable
// application-level handle ("research-<workflowId>") so the mapping between
// the application surface and the kernel WorkflowRun is deterministic and
// survives process restarts.
type ResearchRun struct {
	ID          string     `json:"id"`
	WorkflowID  string     `json:"workflowId"`
	Goal        string     `json:"goal"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt"`
}

// ResearchQuestion is one planner decomposition unit (§5).
type ResearchQuestion struct {
	ID            string `json:"id"`
	ResearchRunID string `json:"researchRunId"`
	Question      string `json:"question"`
	Priority      int    `json:"priority"`
	Status        string `json:"status"`
}

// Source is one discovered document (§5).
type Source struct {
	ID          string `json:"id"`
	QuestionID  string `json:"questionId"`
	URL         string `json:"url"`
	Title       string `json:"title"`
	PublishedAt string `json:"publishedAt"`
	RetrievedAt string `json:"retrievedAt"`
}

// Evidence is one grounded claim extracted from a source (§5).
type Evidence struct {
	ID         string  `json:"id"`
	SourceID   string  `json:"sourceId"`
	QuestionID string  `json:"questionId"`
	Claim      string  `json:"claim"`
	Evidence   string  `json:"evidence"`
	Confidence float64 `json:"confidence"`
}

// Finding is one analyst synthesis backed by evidence IDs (§5).
type Finding struct {
	ID          string   `json:"id"`
	Statement   string   `json:"statement"`
	EvidenceIDs []string `json:"evidenceIds"`
	Confidence  float64  `json:"confidence"`
}

// ResearchGap is one critic-identified hole (§5).
type ResearchGap struct {
	ID       string `json:"id"`
	Question string `json:"question"`
	Severity string `json:"severity"`
	Resolved bool   `json:"resolved"`
}

// Report is the final deliverable reference (§5). The artifactRef points at
// the content-addressed artifact store entry that holds the rendered
// Markdown report.
type Report struct {
	ID               string  `json:"id"`
	ResearchRunID    string  `json:"researchRunId"`
	ArtifactRef      string  `json:"artifactRef"`
	CitationCoverage float64 `json:"citationCoverage"`
}
