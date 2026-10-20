package console_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/control/console"
)

// The console reads the control plane through the published contract. A
// path the console calls but the contract does not describe would fail at
// run time, in a browser, with no failing test.
func TestConsoleCallsOnlyDocumentedEndpoints(t *testing.T) {
	document := console.Document()
	calls := regexp.MustCompile(`['"](/v1/[a-zA-Z0-9/_{}:-]+)`).FindAllStringSubmatch(document, -1)
	if len(calls) == 0 {
		t.Fatal("the console document must call the control plane")
	}

	seen := map[string]bool{}
	for _, call := range calls {
		seen[call[1]] = true
	}

	contract := readContract(t)
	for path := range seen {
		if contract[path] {
			continue
		}
		// The console builds some paths at run time ("/v1/tasks/" + id +
		// "/events"). The static prefix still has to exist in the contract.
		prefix := path
		if index := strings.LastIndex(prefix, "/"); index > 0 {
			prefix = prefix[:index+1]
		}
		covered := false
		for candidate := range contract {
			if strings.HasPrefix(candidate, prefix) {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("the console calls %q, which the published contract does not describe", path)
		}
	}
}

// readContract collects the paths of the published Control API contract.
func readContract(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile("../../../api/openapi/control-v1.yaml")
	if err != nil {
		t.Fatalf("read the Control API contract: %v", err)
	}
	paths := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "/v1/") {
			continue
		}
		paths[strings.TrimSuffix(strings.TrimSpace(strings.SplitN(trimmed, ":", 2)[0]), ":")] = true
	}
	if len(paths) == 0 {
		t.Fatal("the Control API contract has no paths")
	}
	return paths
}

// The console must be a document that a browser can run: it carries its own
// styles and script, and it never loads a third-party asset.
func TestConsoleLoadsNoThirdPartyAsset(t *testing.T) {
	document := console.Document()
	// An inline SVG may carry an XML namespace; only a loaded asset counts.
	loads := regexp.MustCompile(`(?i)(src|href)\s*=\s*["']https?://`)
	if loads.MatchString(document) {
		t.Errorf("the console document loads an external asset; the console must be self-contained")
	}
	if !strings.Contains(document, "<style>") || !strings.Contains(document, "<script>") {
		t.Error("the console document must carry its own stylesheet and script")
	}
}

// The console is served without authentication because it holds no data; the
// data calls carry the operator's token. The response must say so, and must
// not be cached.
func TestConsoleHandlerServesTheDocument(t *testing.T) {
	handler := console.Handler()
	request := httptest.NewRequest(http.MethodGet, "/console", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("content type = %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("cache control = %q, want no-store", got)
	}
	if !strings.Contains(recorder.Body.String(), "Operator Console") {
		t.Error("the served document is not the operator console")
	}
}
