//go:build integration

package research_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	research "github.com/CloudEdgeCore/Fenced/examples/research-workflow/runtime"
)

// Probe: the scripted writer must recover verbatim quotes from a rendered
// writer prompt (regression for HTML-escape corruption of citations).
func TestProbeBundleClaims(t *testing.T) {
	bundle := research.EvidenceBundle{
		SourceID: "src-001", ResearchQuestionID: "rq-001", Title: "T", URL: "https://x",
		Claims: []research.Claim{
			{ClaimID: "src-001-claim-001", Claim: "Markets split.", Evidence: "R&D & scale <matter> today.", Confidence: 0.82},
			{ClaimID: "src-001-claim-002", Claim: "Second.", Evidence: "Second evidence sentence here.", Confidence: 0.7},
		},
	}
	rendered := "[" + mustJSON(t, bundle) + "]"
	user := fmt.Sprintf("Findings:\n%s\n\nEvidence bundles:\n%s",
		`{"findings":[{"findingId":"f1","statement":"S","evidenceIds":["src-001-claim-001"],"confidence":0.9}]}`,
		rendered)
	claims := bundleClaims(user)
	if len(claims) != 2 {
		t.Fatalf("claims = %d (%v)", len(claims), claims)
	}
	if got, _ := claims[0]["evidence"].(string); got != "R&D & scale <matter> today." {
		t.Fatalf("evidence not decoded verbatim: %q", got)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(raw))
}
