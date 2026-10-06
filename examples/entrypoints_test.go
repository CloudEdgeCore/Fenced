// Package examples holds structural guards over the example tree.
//
// It carries no non-test code. The guards exist so a new example entry point
// cannot silently skip the AgentOS -> Fenced compatibility shim that every
// shipped binary applies at start-up: examples must behave like cmd/*/main.go,
// not diverge from it.
package examples

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// goCompatMarker is the exact start-up line cmd/*/main.go uses.
const goCompatMarker = "compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())"

// pythonCompatMarker is the Python equivalent, exported by fenced_runtime.
const pythonCompatMarker = "apply_legacy_compat()"

// TestGoEntryPointsWireLegacyCompat asserts every Go entry point under
// examples/ aliases legacy AGENTOS_* environment variables at start-up, the
// same way the shipped binaries do.
func TestGoEntryPointsWireLegacyCompat(t *testing.T) {
	paths := goEntryPoints(t)
	if len(paths) == 0 {
		t.Fatal("no Go entry points found under examples/")
	}
	for _, path := range paths {
		source := readFile(t, path)
		if !strings.Contains(source, goCompatMarker) {
			t.Errorf("%s: Go entry point does not wire the legacy compatibility shim", path)
		}
		if !strings.Contains(source, `"github.com/CloudEdgeCore/Fenced/internal/platform/compat"`) {
			t.Errorf("%s: Go entry point is missing the compat import", path)
		}
	}
}

// TestPythonEntryPointsWireLegacyCompat asserts every Python entry point under
// examples/ calls apply_legacy_compat(), the Python equivalent of the Go shim.
func TestPythonEntryPointsWireLegacyCompat(t *testing.T) {
	paths := pythonEntryPoints(t)
	if len(paths) == 0 {
		t.Fatal("no Python entry points found under examples/")
	}
	for _, path := range paths {
		source := readFile(t, path)
		if !strings.Contains(source, pythonCompatMarker) {
			t.Errorf("%s: Python entry point does not wire the legacy compatibility shim", path)
		}
		if !strings.Contains(source, "from fenced_runtime") {
			t.Errorf("%s: Python entry point is missing the fenced_runtime import", path)
		}
	}
}

// goEntryPoints returns every Go file under examples/ that declares a main
// function, using a slash-separated path relative to the examples root.
func goEntryPoints(t *testing.T) []string {
	t.Helper()
	var found []string
	walk(t, func(path string, entry os.DirEntry) {
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return
		}
		if strings.Contains(readFile(t, path), "func main() {") {
			found = append(found, path)
		}
	})
	sort.Strings(found)
	return found
}

// pythonEntryPoints returns every Python file under examples/ that guards an
// entry point with __main__, using a slash-separated path relative to the
// examples root.
func pythonEntryPoints(t *testing.T) []string {
	t.Helper()
	var found []string
	walk(t, func(path string, entry os.DirEntry) {
		if !strings.HasSuffix(path, ".py") {
			return
		}
		if strings.Contains(readFile(t, path), `if __name__ == "__main__":`) {
			found = append(found, path)
		}
	})
	sort.Strings(found)
	return found
}

// walk visits every file under the examples root, skipping the directories
// that never hold shipped entry points.
func walk(t *testing.T, visit func(path string, entry os.DirEntry)) {
	t.Helper()
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "node_modules", "__pycache__", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		visit(filepath.ToSlash(path), entry)
		return nil
	})
	if err != nil {
		t.Fatalf("walk examples tree: %v", err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
