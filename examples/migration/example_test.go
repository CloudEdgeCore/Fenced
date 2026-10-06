// Package migration guards the before/after pairs under examples/migration.
//
// The point of this example is that the mapping is mechanical and complete:
// every variable that carries the product prefix in the "before" file appears
// with the new prefix in the "after" file, and nothing else changes. These
// tests fail if the two halves drift apart.
package migration

import (
	"os"
	"sort"
	"strings"
	"testing"
)

const legacyPrefix = "AGENTOS_"
const currentPrefix = "FENCED_"

// TestEnvFilesCarryTheSameVariables asserts env/fenced.env is exactly
// env/agentos.env with the prefix renamed — no variable added, dropped, or
// given a different value.
func TestEnvFilesCarryTheSameVariables(t *testing.T) {
	before := readEnv(t, "env/agentos.env")
	after := readEnv(t, "env/fenced.env")

	renamed := make(map[string]string, len(before))
	for name, value := range before {
		renamed[renamePrefix(name)] = value
	}

	for name, value := range renamed {
		got, ok := after[name]
		if !ok {
			t.Errorf("env/fenced.env: missing %s (present in env/agentos.env as its legacy name)", name)
			continue
		}
		if got != value {
			t.Errorf("env/fenced.env: %s value %q does not match the pre-rename value %q", name, got, value)
		}
	}
	for name := range after {
		if _, ok := renamed[name]; !ok {
			t.Errorf("env/fenced.env: unexpected %s — no matching variable in env/agentos.env", name)
		}
	}
}

// TestUnprefixedVariablesAreUntouched pins the one variable that must survive
// the rename unchanged: it never carried the product prefix.
func TestUnprefixedVariablesAreUntouched(t *testing.T) {
	before := readEnv(t, "env/agentos.env")
	after := readEnv(t, "env/fenced.env")

	if _, ok := before["DATABASE_URL"]; !ok {
		t.Fatal("env/agentos.env: DATABASE_URL is missing; the example no longer demonstrates an unprefixed variable")
	}
	if before["DATABASE_URL"] != after["DATABASE_URL"] {
		t.Errorf("DATABASE_URL must be identical across the rename: before %q, after %q",
			before["DATABASE_URL"], after["DATABASE_URL"])
	}
}

// TestSystemdUnitsKeepTheSameShape asserts the migrated unit has the same
// sections and keys as its pre-rename counterpart. Only the values move.
func TestSystemdUnitsKeepTheSameShape(t *testing.T) {
	before := readUnitKeys(t, "systemd/agentos-control.service")
	after := readUnitKeys(t, "systemd/fenced-control.service")

	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Errorf("systemd unit shape changed across the rename\nbefore:\n  %s\nafter:\n  %s",
			strings.Join(before, "\n  "), strings.Join(after, "\n  "))
	}
}

// renamePrefix applies the environment-variable half of the rename.
func renamePrefix(name string) string {
	if strings.HasPrefix(name, legacyPrefix) {
		return currentPrefix + strings.TrimPrefix(name, legacyPrefix)
	}
	return name
}

// readEnv parses a KEY=value file, ignoring blanks and comments.
func readEnv(t *testing.T, path string) map[string]string {
	t.Helper()
	values := map[string]string{}
	for _, line := range strings.Split(readFile(t, path), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("%s: malformed line %q", path, line)
		}
		values[strings.TrimSpace(name)] = value
	}
	if len(values) == 0 {
		t.Fatalf("%s: parsed no variables", path)
	}
	return values
}

// readUnitKeys returns the "Section/Key" identifiers of a systemd unit, sorted,
// so two units can be compared for structural equivalence.
func readUnitKeys(t *testing.T, path string) []string {
	t.Helper()
	var keys []string
	section := ""
	for _, line := range strings.Split(readFile(t, path), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.Trim(line, "[]")
			keys = append(keys, section)
			continue
		}
		name, _, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		keys = append(keys, section+"/"+strings.TrimSpace(name))
	}
	if len(keys) == 0 {
		t.Fatalf("%s: parsed no keys", path)
	}
	sort.Strings(keys)
	return keys
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
