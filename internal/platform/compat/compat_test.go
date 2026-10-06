package compat

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestAliasLegacyEnv(t *testing.T) {
	t.Setenv("AGENTOS_COMPAT_PROBE", "legacy-value")
	os.Unsetenv("FENCED_COMPAT_PROBE")
	t.Cleanup(func() { os.Unsetenv("FENCED_COMPAT_PROBE") })

	aliased := AliasLegacyEnv()

	if got := os.Getenv("FENCED_COMPAT_PROBE"); got != "legacy-value" {
		t.Fatalf("FENCED_COMPAT_PROBE = %q, want %q", got, "legacy-value")
	}
	if !slices.Contains(aliased, "FENCED_COMPAT_PROBE") {
		t.Fatalf("aliased = %v, want it to contain FENCED_COMPAT_PROBE", aliased)
	}
}

func TestAliasLegacyEnvCurrentWins(t *testing.T) {
	t.Setenv("AGENTOS_COMPAT_WINS", "legacy")
	t.Setenv("FENCED_COMPAT_WINS", "current")

	aliased := AliasLegacyEnv()

	if got := os.Getenv("FENCED_COMPAT_WINS"); got != "current" {
		t.Fatalf("FENCED_COMPAT_WINS = %q, want the already-set %q", got, "current")
	}
	if slices.Contains(aliased, "FENCED_COMPAT_WINS") {
		t.Fatalf("aliased = %v, want it to NOT contain FENCED_COMPAT_WINS", aliased)
	}
}

func TestAliasLegacyEnvExplicitEmptyWins(t *testing.T) {
	t.Setenv("AGENTOS_COMPAT_EMPTY", "legacy")
	t.Setenv("FENCED_COMPAT_EMPTY", "")

	aliased := AliasLegacyEnv()

	if got := os.Getenv("FENCED_COMPAT_EMPTY"); got != "" {
		t.Fatalf("FENCED_COMPAT_EMPTY = %q, want it left empty", got)
	}
	if slices.Contains(aliased, "FENCED_COMPAT_EMPTY") {
		t.Fatalf("aliased = %v, want it to NOT contain FENCED_COMPAT_EMPTY", aliased)
	}
}

func TestAliasLegacyEnvIsIdempotent(t *testing.T) {
	t.Setenv("AGENTOS_COMPAT_IDEM", "v")
	os.Unsetenv("FENCED_COMPAT_IDEM")
	t.Cleanup(func() { os.Unsetenv("FENCED_COMPAT_IDEM") })

	first := AliasLegacyEnv()
	second := AliasLegacyEnv()

	if !slices.Contains(first, "FENCED_COMPAT_IDEM") {
		t.Fatalf("first call aliased = %v, want FENCED_COMPAT_IDEM", first)
	}
	if slices.Contains(second, "FENCED_COMPAT_IDEM") {
		t.Fatalf("second call aliased = %v, want no re-aliasing", second)
	}
}

func TestWarnLegacyEnv(t *testing.T) {
	var buf bytes.Buffer
	WarnLegacyEnv(&buf, nil)
	if buf.Len() != 0 {
		t.Fatalf("WarnLegacyEnv wrote %q for an empty list, want nothing", buf.String())
	}

	WarnLegacyEnv(&buf, []string{"FENCED_TOKEN"})
	out := buf.String()
	if !strings.Contains(out, "FENCED_TOKEN") {
		t.Fatalf("WarnLegacyEnv output %q does not mention the aliased variable", out)
	}
	if !strings.Contains(out, LegacyPrefix) {
		t.Fatalf("WarnLegacyEnv output %q does not mention the legacy prefix", out)
	}
}

func TestWarnLegacyEnvNilWriter(t *testing.T) {
	WarnLegacyEnv(nil, []string{"FENCED_TOKEN"})
}

func TestNormalizeProtocol(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"runtime interface", "agentos.runtime.interface/v1", "fenced.runtime.interface/v1"},
		{"runtime interface alpha", "agentos.runtime.interface/v1alpha1", "fenced.runtime.interface/v1alpha1"},
		{"api group", "agentos.dev/v1", "fenced.dev/v1"},
		{"api group alpha", "agentos.dev/v1alpha1", "fenced.dev/v1alpha1"},
		{"runtime abi oci", "agentos.oci/v1", "fenced.oci/v1"},
		{"runtime abi reference", "agentos.reference/v1", "fenced.reference/v1"},
		{"runtime abi adapter", "agentos.adapter-http/v1", "fenced.adapter-http/v1"},
		{"already canonical", "fenced.runtime.interface/v1", "fenced.runtime.interface/v1"},
		{"empty", "", ""},
		{"unrelated", "langgraph.runtime/v1", "langgraph.runtime/v1"},
		{"bare word", "agentos", "agentos"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeProtocol(tc.in); got != tc.want {
				t.Fatalf("NormalizeProtocol(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeHeader(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"X-Agentos-Execution", CurrentExecutionHeader},
		{"x-agentos-execution", CurrentExecutionHeader},
		{"X-Fenced-Execution", CurrentExecutionHeader},
		{"Authorization", "Authorization"},
	}
	for _, tc := range cases {
		if got := NormalizeHeader(tc.in); got != tc.want {
			t.Fatalf("NormalizeHeader(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
