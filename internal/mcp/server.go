package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
)

// ExecutionHeader carries the agent's execution id (the Runtime Interface
// executionId, i.e. the attempt id) so a shared endpoint can scope brokered
// calls to one open execution window.
const ExecutionHeader = "X-Fenced-Execution"

// executionIdentity reads the execution id from the current header, falling
// back to the legacy pre-rename header so a worker built before the AgentOS →
// Fenced rename keeps working.
func executionIdentity(header http.Header) string {
	if value := strings.TrimSpace(header.Get(ExecutionHeader)); value != "" {
		return value
	}
	return strings.TrimSpace(header.Get(compat.LegacyExecutionHeader))
}

type executionContextKey struct{}

// ExecutionID returns the execution id of the inbound call, empty when the
// agent did not report one.
func ExecutionID(ctx context.Context) string {
	value, _ := ctx.Value(executionContextKey{}).(string)
	return value
}

// Handler implements the MCP methods beyond the core lifecycle. Params are
// raw JSON for the method; implementations parse them strictly.
type Handler interface {
	// ListTools returns the MCP tool declarations (result object).
	ListTools(context.Context, json.RawMessage) (any, *Error)
	// CallTool executes one tool invocation (result object).
	CallTool(context.Context, json.RawMessage) (any, *Error)
}

// Server exposes a Handler over the MCP Streamable HTTP transport:
// POST / with a strict JSON-RPC 2.0 body, answered as application/json or
// text/event-stream depending on the client's Accept header. Notifications
// are acknowledged with 204. There is no session state.
type Server struct {
	name          string
	version       string
	handler       Handler
	maxBodyBytes  int64
	maxConcurrent int
	active        atomic.Int64
}

// NewServer constructs the MCP boundary. name/version identify the server in
// the initialize handshake. Concurrent in-flight requests are capped (O8);
// NewServerWithConcurrencyLimit overrides the default.
func NewServer(name, version string, handler Handler) *Server {
	return NewServerWithConcurrencyLimit(name, version, handler, 16)
}

// NewServerWithConcurrencyLimit constructs the MCP boundary with an explicit
// cap on concurrent in-flight requests; excess requests answer 503.
func NewServerWithConcurrencyLimit(name, version string, handler Handler, maxConcurrent int) *Server {
	if maxConcurrent <= 0 {
		maxConcurrent = 16
	}
	return &Server{name: name, version: version, handler: handler, maxBodyBytes: 1 << 20, maxConcurrent: maxConcurrent}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Bounded concurrency (O8): the MCP endpoint sits on the agent's loopback
	// boundary; a flood of calls must not exhaust the runtime.
	if s.maxConcurrent > 0 && s.active.Add(1) > int64(s.maxConcurrent) {
		s.active.Add(-1)
		http.Error(writer, "concurrency limit reached", http.StatusServiceUnavailable)
		return
	}
	defer s.active.Add(-1)
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.URL.Path != "/" {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		http.Error(writer, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	accept := request.Header.Get("Accept")
	wantsSSE, acceptable := negotiateResponseFormat(accept)
	if !acceptable {
		http.Error(writer, "Accept must include application/json or text/event-stream", http.StatusNotAcceptable)
		return
	}

	body, err := io.ReadAll(io.LimitReader(request.Body, s.maxBodyBytes+1))
	if err != nil {
		http.Error(writer, "read request body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > s.maxBodyBytes {
		writeError(writer, wantsSSE, nil, &Error{Code: codeInvalidRequest, Message: "request body too large"})
		return
	}
	if execution := executionIdentity(request.Header); execution != "" {
		request = request.WithContext(context.WithValue(request.Context(), executionContextKey{}, execution))
	}
	parsed, rpcErr := ParseRequest(body)
	if rpcErr != nil {
		writeError(writer, wantsSSE, nil, rpcErr)
		return
	}
	if parsed.IsNotification() {
		writer.WriteHeader(http.StatusNoContent)
		return
	}

	result, methodErr := s.dispatch(request.Context(), parsed.Method, parsed.Params)
	if methodErr != nil {
		writeError(writer, wantsSSE, parsed.ID, methodErr)
		return
	}
	writeResult(writer, wantsSSE, parsed.ID, result)
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *Error) {
	switch method {
	case "initialize":
		return map[string]any{
			"protocolVersion": ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": s.name, "version": s.version},
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		if s.handler == nil {
			return nil, &Error{Code: codeMethodNotFound, Message: "Method not found"}
		}
		return s.handler.ListTools(ctx, params)
	case "tools/call":
		if s.handler == nil {
			return nil, &Error{Code: codeMethodNotFound, Message: "Method not found"}
		}
		return s.handler.CallTool(ctx, params)
	default:
		return nil, &Error{Code: codeMethodNotFound, Message: fmt.Sprintf("Method not found: %s", method)}
	}
}

// negotiateResponseFormat decides between application/json and
// text/event-stream from the client's Accept header, honoring q-values and
// wildcards. An absent header defaults to JSON; */* and application/* accept
// both and tie-break toward JSON; an explicit tie between the two media
// types prefers the streaming form, which is what MCP SDKs negotiate for
// long-running tool calls.
func negotiateResponseFormat(accept string) (sse bool, acceptable bool) {
	accept = strings.TrimSpace(accept)
	if accept == "" {
		return false, true
	}
	jsonQ, sseQ := -1.0, -1.0
	wildcard := false
	for _, part := range strings.Split(accept, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		mediaType, params, err := mime.ParseMediaType(part)
		if err != nil {
			continue
		}
		q := 1.0
		if raw, ok := params["q"]; ok {
			if parsed, parseErr := strconv.ParseFloat(raw, 64); parseErr == nil && parsed >= 0 && parsed <= 1 {
				q = parsed
			}
		}
		if q <= 0 {
			continue // q=0 marks the media type unacceptable
		}
		switch {
		case mediaType == "application/json":
			if q > jsonQ {
				jsonQ = q
			}
		case mediaType == "text/event-stream":
			if q > sseQ {
				sseQ = q
			}
		case mediaType == "*/*" || mediaType == "application/*":
			wildcard = true
			if q > jsonQ {
				jsonQ = q
			}
			if q > sseQ {
				sseQ = q
			}
		}
	}
	switch {
	case jsonQ < 0 && sseQ < 0:
		return false, false
	case jsonQ > sseQ:
		return false, true
	case sseQ > jsonQ:
		return true, true
	case wildcard:
		// A bare */* prefers the simple JSON form.
		return false, true
	default:
		// Explicit equal preference: prefer the streaming form.
		return true, true
	}
}

func writeResult(writer http.ResponseWriter, sse bool, id json.RawMessage, result any) {
	writeJSON(writer, sse, &Response{JSONRPC: "2.0", ID: id, Result: result})
}

func writeError(writer http.ResponseWriter, sse bool, id json.RawMessage, rpcErr *Error) {
	writeJSON(writer, sse, &Response{JSONRPC: "2.0", ID: id, Error: rpcErr})
}

func writeJSON(writer http.ResponseWriter, sse bool, response *Response) {
	encoded, err := json.Marshal(response)
	if err != nil {
		http.Error(writer, "encode response", http.StatusInternalServerError)
		return
	}
	if sse {
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Accel-Buffering", "no")
		writer.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(writer, "event: message\ndata: %s\n\n", encoded)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}
