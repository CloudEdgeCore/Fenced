package mcp

import (
	"net/http"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
)

func TestExecutionIdentityAcceptsLegacyHeader(t *testing.T) {
	current := http.Header{}
	current.Set(ExecutionHeader, "attempt-new")
	if got := executionIdentity(current); got != "attempt-new" {
		t.Fatalf("current header: got %q, want %q", got, "attempt-new")
	}

	legacy := http.Header{}
	legacy.Set(compat.LegacyExecutionHeader, "attempt-old")
	if got := executionIdentity(legacy); got != "attempt-old" {
		t.Fatalf("legacy header: got %q, want %q", got, "attempt-old")
	}

	both := http.Header{}
	both.Set(ExecutionHeader, "attempt-new")
	both.Set(compat.LegacyExecutionHeader, "attempt-old")
	if got := executionIdentity(both); got != "attempt-new" {
		t.Fatalf("current header must win: got %q, want %q", got, "attempt-new")
	}

	if got := executionIdentity(http.Header{}); got != "" {
		t.Fatalf("absent header: got %q, want empty", got)
	}
}
