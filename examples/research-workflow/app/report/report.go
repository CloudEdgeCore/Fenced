// Package report renders the final research report (design doc §14 Phase 3:
// "Markdown Renderer", "Citation Coverage", "Artifact Store"). The writer
// agent already produces the structured report document and the citation
// validator stamps the verdict; this package turns the persisted record into
// the deliverable Markdown artifact and hands back the content-addressed
// artifact reference of §5.
package report

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
)

// Document mirrors the writer agent's report record.
type Document struct {
	Title                string     `json:"title"`
	Summary              string     `json:"summary"`
	Sections             []Section  `json:"sections"`
	Citations            []Citation `json:"citations"`
	InsufficientEvidence bool       `json:"insufficientEvidence,omitempty"`
}

// Section is one report body section.
type Section struct {
	Heading string `json:"heading"`
	Body    string `json:"body"`
}

// Citation binds one in-body marker to one evidence ID with a verbatim
// quote, per the validator's 1:1 marker binding rule.
type Citation struct {
	Marker     string `json:"marker"`
	EvidenceID string `json:"evidenceId"`
	SourceID   string `json:"sourceId,omitempty"`
	Quote      string `json:"quote"`
}

// Verdict mirrors the citation validator's validation record.
type Verdict struct {
	Valid                bool     `json:"valid"`
	CitationCoverage     float64  `json:"citationCoverage"`
	UnsupportedClaims    int      `json:"unsupportedClaims"`
	Retries              int      `json:"retries"`
	InsufficientEvidence bool     `json:"insufficientEvidence,omitempty"`
	MarkerIssues         []string `json:"markerIssues,omitempty"`
}

// Decode parses the persisted writer record.
func Decode(raw string) (Document, error) {
	var document Document
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return Document{}, fmt.Errorf("decode report document: %w", err)
	}
	if strings.TrimSpace(document.Title) == "" {
		return Document{}, fmt.Errorf("report document has no title")
	}
	return document, nil
}

// DecodeVerdict parses the persisted validator verdict.
func DecodeVerdict(raw string) (Verdict, error) {
	var verdict Verdict
	if err := json.Unmarshal([]byte(raw), &verdict); err != nil {
		return Verdict{}, fmt.Errorf("decode report verdict: %w", err)
	}
	return verdict, nil
}

// RenderMarkdown renders the final deliverable (§14 "Markdown Renderer").
// Every citation appears in a References section so the report is traceable
// back to its evidence IDs (§18 "Report Traceable to Evidence").
func RenderMarkdown(document Document, verdict Verdict) string {
	var builder strings.Builder
	builder.WriteString("# " + document.Title + "\n\n")
	if document.Summary != "" {
		builder.WriteString(document.Summary + "\n\n")
	}
	for _, section := range document.Sections {
		if strings.TrimSpace(section.Heading) == "" && strings.TrimSpace(section.Body) == "" {
			continue
		}
		if strings.TrimSpace(section.Heading) != "" {
			builder.WriteString("## " + section.Heading + "\n\n")
		}
		builder.WriteString(section.Body + "\n\n")
	}
	if len(document.Citations) > 0 {
		builder.WriteString("## References\n\n")
		for _, citation := range document.Citations {
			builder.WriteString(fmt.Sprintf("- %s %s (%s) %q\n", citation.Marker, citation.EvidenceID, citation.SourceID, citation.Quote))
		}
		builder.WriteString("\n")
	}
	builder.WriteString(fmt.Sprintf("---\nCitation coverage: %.0f%% (%d unsupported)\n",
		verdict.CitationCoverage*100, verdict.UnsupportedClaims))
	if document.InsufficientEvidence || verdict.InsufficientEvidence {
		builder.WriteString("insufficientEvidence: true\n")
	}
	return builder.String()
}

// Media types of the stored artifacts.
const (
	MediaTypeMarkdown = "text/markdown"
	MediaTypeJSON     = "application/json"
)

// Store persists one deliverable into the content-addressed artifact store
// and returns the artifactRef URI. Storing is idempotent: identical content
// addresses to the same digest.
func Store(ctx context.Context, store *artifact.Filesystem, tenantID string, mediaType string, content []byte) (string, error) {
	reference, err := store.Put(ctx, tenantID, mediaType, bytes.NewReader(content))
	if err != nil {
		return "", fmt.Errorf("store report artifact: %w", err)
	}
	return reference.URI, nil
}
