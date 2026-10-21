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

// The console reads the control plane through the published contract. A path
// the console calls but the contract does not describe would fail at run
// time, in a browser, with no failing test.
func TestConsoleCallsOnlyDocumentedEndpoints(t *testing.T) {
	bundle := console.Bundle()
	if bundle == "" {
		t.Fatal("the console bundle is missing")
	}
	calls := regexp.MustCompile(`/v1/[a-zA-Z0-9/_{}:$.-]+`).FindAllString(bundle, -1)
	if len(calls) == 0 {
		t.Fatal("the console must call the control plane")
	}

	contract := readContract(t)
	seen := map[string]bool{}
	for _, path := range calls {
		// A path parameter carries a value in the bundle; the contract
		// describes the parameter.
		normalized := regexp.MustCompile(`\$\{[^}]+\}`).ReplaceAllString(path, "{id}")
		normalized = regexp.MustCompile(`\{id\}[a-zA-Z0-9/_{}:$.-]*`).ReplaceAllString(normalized, "{id}")
		seen[normalized] = true
	}

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
// stylesheet and script, and it never loads a third-party asset.
func TestConsoleLoadsNoThirdPartyAsset(t *testing.T) {
	document := console.Document()
	bundle := console.Bundle()
	styles := console.Styles()
	if document == "" || bundle == "" || styles == "" {
		t.Fatal("the console bundle is incomplete")
	}
	// An inline SVG may carry an XML namespace; only a loaded asset counts.
	loads := regexp.MustCompile(`(?i)(src|href)\s*=\s*["']https?://`)
	for name, content := range map[string]string{"document": document, "bundle": bundle, "styles": styles} {
		if loads.MatchString(content) {
			t.Errorf("the console %s loads an external asset; the console must be self-contained", name)
		}
	}
}

// The event stream must be read with fetch, not with EventSource: an
// EventSource cannot carry an Authorization header, and the operator's token
// must not travel in the query string, where it would end up in access logs.
func TestConsoleStreamsWithAnAuthorizationHeader(t *testing.T) {
	bundle := console.Bundle()
	if strings.Contains(bundle, "new EventSource(") || strings.Contains(bundle, "EventSource(") {
		t.Error("the console must not use EventSource: it cannot authenticate the stream")
	}
	if !strings.Contains(bundle, "Authorization") || !strings.Contains(bundle, "Bearer ") {
		t.Error("the console must carry the operator's bearer token")
	}
	if strings.Contains(bundle, "events?token=") || strings.Contains(bundle, "events?access_token=") {
		t.Error("the operator's token must not travel in the event stream query string")
	}
}

// The console is served from this origin: the document, the script and the
// stylesheet are all served by the control plane.
func TestConsoleHandlerServesTheDocumentAndItsAssets(t *testing.T) {
	handler := console.Handler()

	request := httptest.NewRequest(http.MethodGet, "/console/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("content type = %q", got)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("cache control = %q, want no-store", got)
	}
	if !strings.Contains(response.Body.String(), "Fenced Management Console") {
		t.Error("the served document is not the management console")
	}
	// The content security policy stays strict: the console needs no inline
	// script and no inline style.
	csp := response.Header().Get("Content-Security-Policy")
	if strings.Contains(csp, "unsafe-inline") || strings.Contains(csp, "unsafe-eval") {
		t.Errorf("the content security policy is not strict: %q", csp)
	}

	for _, asset := range []string{"/console/assets/app.js", "/console/assets/app.css"} {
		request = httptest.NewRequest(http.MethodGet, asset, nil)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Errorf("%s: status = %d", asset, response.Code)
		}
		if response.Body.Len() == 0 {
			t.Errorf("%s: the asset is empty", asset)
		}
	}

	// A deep link still reaches the console.
	request = httptest.NewRequest(http.MethodGet, "/console/services", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Errorf("a deep link: status = %d", response.Code)
	}
}
