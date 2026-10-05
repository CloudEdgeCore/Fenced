package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseRequestAcceptsRequestsAndNotifications(t *testing.T) {
	request, rpcErr := ParseRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	if rpcErr != nil {
		t.Fatalf("ParseRequest: %v", rpcErr)
	}
	if request.IsNotification() || request.Method != "ping" {
		t.Fatalf("unexpected request: %+v", request)
	}
	notification, rpcErr := ParseRequest([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if rpcErr != nil || !notification.IsNotification() {
		t.Fatalf("notification: %+v err=%v", notification, rpcErr)
	}
}

func TestParseRequestRejectsMalformedDocuments(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty", ``},
		{"batch", `[{"jsonrpc":"2.0","id":1,"method":"ping"}]`},
		{"wrong version", `{"jsonrpc":"1.0","id":1,"method":"ping"}`},
		{"missing method", `{"jsonrpc":"2.0","id":1}`},
		{"unknown field", `{"jsonrpc":"2.0","id":1,"method":"ping","extra":true}`},
		{"duplicate key", `{"jsonrpc":"2.0","id":1,"method":"ping","method":"ping"}`},
		{"trailing value", `{"jsonrpc":"2.0","id":1,"method":"ping"} {}`},
		{"not an object", `"ping"`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, rpcErr := ParseRequest([]byte(test.body)); rpcErr == nil {
				t.Fatalf("body accepted: %s", test.body)
			}
		})
	}
}

type fakeHandler struct {
	tools any
	err   *Error
}

func (f *fakeHandler) ListTools(context.Context, json.RawMessage) (any, *Error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.tools != nil {
		return f.tools, nil
	}
	return map[string]any{"tools": []any{}}, nil
}

func (f *fakeHandler) CallTool(context.Context, json.RawMessage) (any, *Error) {
	if f.err != nil {
		return nil, f.err
	}
	return textResult(json.RawMessage(`{"ok":true}`), false), nil
}

func post(t *testing.T, server http.Handler, body, accept string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if accept != "" {
		request.Header.Set("Accept", accept)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func TestServerInitializationNegotiatesPinnedVersion(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})
	response := post(t, server, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`, "application/json")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var message Response
	if err := json.Unmarshal(response.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result := message.Result.(map[string]any)
	if result["protocolVersion"] != ProtocolVersion {
		t.Fatalf("protocolVersion = %v, want %s", result["protocolVersion"], ProtocolVersion)
	}
	if message.Error != nil {
		t.Fatalf("unexpected error: %+v", message.Error)
	}
}

func TestServerPingAndUnknownMethod(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})
	response := post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "application/json")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"result":{}`) {
		t.Fatalf("ping: %d %s", response.Code, response.Body.String())
	}
	response = post(t, server, `{"jsonrpc":"2.0","id":1,"method":"unknown"}`, "application/json")
	var message Response
	if err := json.Unmarshal(response.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if message.Error == nil || message.Error.Code != codeMethodNotFound {
		t.Fatalf("unknown method: %+v", message.Error)
	}
}

func TestServerNotificationAcknowledgedWithNoContent(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})
	response := post(t, server, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, "application/json")
	if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
		t.Fatalf("notification: %d %s", response.Code, response.Body.String())
	}
}

func TestServerTransportEnforcement(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "text/plain")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("bad content type status = %d", response.Code)
	}

	response = post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "application/xml")
	if response.Code != http.StatusNotAcceptable {
		t.Fatalf("bad accept status = %d", response.Code)
	}
}

func TestServerAnswersOverSSEWhenRequested(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})
	response := post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "text/event-stream")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("sse: %d %s", response.Code, response.Header())
	}
	if !strings.Contains(response.Body.String(), "event: message") || !strings.Contains(response.Body.String(), `"result":{}`) {
		t.Fatalf("sse body: %s", response.Body.String())
	}
	if response.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("sse buffering hint missing: %v", response.Header())
	}
}

func TestServerAcceptNegotiationHonorsQValuesAndWildcards(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})

	// A bare wildcard (what most HTTP clients send) must serve JSON, not 406.
	response := post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "*/*")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("*/*: %d %s", response.Code, response.Header())
	}

	// An absent Accept header defaults to JSON.
	response = post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("absent accept: %d %s", response.Code, response.Header())
	}

	// Both media types listed with equal preference: the streaming form.
	response = post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "application/json, text/event-stream")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("equal preference: %d %s", response.Code, response.Header())
	}

	// Explicit q-values decide: JSON preferred.
	response = post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "text/event-stream;q=0.1, application/json;q=1")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("q-values: %d %s", response.Code, response.Header())
	}

	// A media type marked unacceptable with q=0 alone is still a 406.
	response = post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, "text/event-stream;q=0")
	if response.Code != http.StatusNotAcceptable {
		t.Fatalf("q=0: %d %s", response.Code, response.Body.String())
	}
}

func TestServerRejectsOversizedBody(t *testing.T) {
	server := NewServer("fenced-mcp", "v0.1", &fakeHandler{})
	server.maxBodyBytes = 16
	response := post(t, server, `{"jsonrpc":"2.0","id":1,"method":"ping","params":{"padding":"aaaaaaaaaaaaaaaaaaaaaaaa"}}`, "application/json")
	if response.Code != http.StatusOK {
		t.Fatalf("oversized body must be answered as a JSON-RPC error: %d", response.Code)
	}
	var message Response
	if err := json.Unmarshal(response.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if message.Error == nil || message.Error.Code != codeInvalidRequest {
		t.Fatalf("oversized: %+v", message.Error)
	}
}
