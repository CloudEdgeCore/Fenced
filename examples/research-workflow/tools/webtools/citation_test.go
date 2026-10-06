// Citation.check and tool version resolution tests (design doc §7, §16).
package webtools

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

func invokeCitationCheck(t *testing.T, server *Server, args map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, "/", bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	// Overwrite the body with the full tool envelope.
	envelope, _ := json.Marshal(map[string]any{
		"action": "invoke", "resource": "citation:check:*", "args": args,
	})
	request.Body = nil
	request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(envelope))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("citation.check status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return result
}

func TestCitationCheckSupported(t *testing.T) {
	server := New(nil)
	result := invokeCitationCheck(t, server, map[string]any{
		"claims": []map[string]any{
			{"claimId": "claim-001", "evidence": "Agent runtimes are converging on Wasm-based isolation in 2026."},
		},
		"citations": []map[string]any{
			{"marker": "[1]", "evidenceId": "claim-001", "quote": "Agent runtimes are converging on  Wasm-based   isolation in 2026."},
		},
	})
	if result["valid"] != true {
		t.Fatalf("valid = %v, want true (%v)", result["valid"], result)
	}
	if coverage, ok := result["citationCoverage"].(float64); !ok || coverage != 1.0 {
		t.Fatalf("citationCoverage = %v, want 1.0", result["citationCoverage"])
	}
}

func TestCitationCheckUnsupportedReasons(t *testing.T) {
	server := New(nil)
	result := invokeCitationCheck(t, server, map[string]any{
		"claims": []map[string]any{
			{"claimId": "claim-001", "evidence": "The reference evidence text."},
		},
		"citations": []map[string]any{
			{"marker": "[1]", "evidenceId": "claim-001", "quote": "reference evidence text"},
			{"marker": "[2]", "evidenceId": "claim-missing", "quote": "anything"},
			{"marker": "", "evidenceId": "claim-001", "quote": "The reference"},
			{"marker": "[4]", "evidenceId": "claim-001", "quote": ""},
			{"marker": "[5]", "evidenceId": "claim-001", "quote": "does not appear"},
		},
	})
	if result["valid"] != false {
		t.Fatalf("valid = %v, want false", result["valid"])
	}
	unsupported, ok := result["unsupportedClaims"].([]any)
	if !ok || len(unsupported) != 4 {
		t.Fatalf("unsupportedClaims = %v, want 4", result["unsupportedClaims"])
	}
	// Only the first citation is supported; coverage must be 0.20.
	if coverage, ok := result["citationCoverage"].(float64); !ok || coverage != 0.20 {
		t.Fatalf("citationCoverage = %v, want 0.20", result["citationCoverage"])
	}
	// The quote-mismatch reason must surface.
	if !strings.Contains(mustStringify(t, result["unsupportedClaims"]), "does not appear") {
		t.Fatalf("quote mismatch reason missing: %v", result["unsupportedClaims"])
	}
}

func mustStringify(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(encoded)
}

// TestToolVersionResolution is the §16 "Tool Version Resolution" unit subject:
// the Tool Gateway resolves immutable name@version references to configured
// endpoints and fails closed on unknown versions.
func TestToolVersionResolution(t *testing.T) {
	server := New(nil)
	listener, client, endpoint, err := SelfSignedTLSListener(server)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	defer listener.Close()
	go func() { _ = (&http.Server{Handler: server}).Serve(listener) }()

	executor, err := gateway.NewWebhookExecutor(map[string]string{
		"web.search@1.0.0":     endpoint,
		"citation.check@1.0.0": endpoint,
	}, client)
	if err != nil {
		t.Fatalf("webhook executor: %v", err)
	}
	ctx := context.Background()

	// Resolved versions execute and reach the webtools handler.
	result, err := executor.Execute(ctx, tool.ExecutionRequest{
		Descriptor: store.ToolDescriptor{Name: "citation.check", Version: "1.0.0"},
		Action:     "invoke", Resource: "citation:check:*",
		Args: mustMarshal(t, map[string]any{
			"claims":    []map[string]any{{"claimId": "c", "evidence": "text"}},
			"citations": []map[string]any{{"marker": "[1]", "evidenceId": "c", "quote": "text"}},
		}),
	})
	if err != nil {
		t.Fatalf("execute resolved version: %v", err)
	}
	if !bytes.Contains(result.Output, []byte(`"valid":true`)) {
		t.Fatalf("resolved invocation output: %s", result.Output)
	}

	// An unknown version must fail closed before any HTTP call.
	if _, err := executor.Execute(ctx, tool.ExecutionRequest{
		Descriptor: store.ToolDescriptor{Name: "web.search", Version: "9.9.9"},
		Action:     "invoke", Resource: "web:search:*",
		Args: mustMarshal(t, map[string]any{"query": "x"}),
	}); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unknown version must fail closed, got %v", err)
	}
}

func mustMarshal(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}
