package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeAnthropic is a scriptable Anthropic Messages endpoint. It records the
// request path, the authentication headers and the decoded request body, and
// serves either a typed SSE stream or a plain JSON message.
type fakeAnthropic struct {
	mu          sync.Mutex
	requests    int
	paths       []string
	apiKeys     []string
	versions    []string
	authorizatn []string
	bodies      []map[string]any
	stream      []string
	body        string
	status      int
}

func (f *fakeAnthropic) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.requests++
	f.paths = append(f.paths, request.URL.Path)
	f.apiKeys = append(f.apiKeys, request.Header.Get("x-api-key"))
	f.versions = append(f.versions, request.Header.Get("anthropic-version"))
	f.authorizatn = append(f.authorizatn, request.Header.Get("Authorization"))
	var body map[string]any
	_ = json.NewDecoder(request.Body).Decode(&body)
	f.bodies = append(f.bodies, body)
	stream := f.stream
	payload := f.body
	status := f.status
	f.mu.Unlock()

	if status == 0 {
		status = http.StatusOK
	}
	if len(stream) > 0 {
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(status)
		for _, line := range stream {
			_, _ = writer.Write([]byte(line))
		}
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(payload))
}

func anthropicExecutor(baseURL string) *Executor {
	return NewExecutor(Config{
		Name: "anthropic", BaseURL: baseURL, APIKey: "test-key",
		Protocol: ProtocolAnthropic, MaxAttempts: 1,
	}, nil)
}

func TestAnthropicEncodeRequestShape(t *testing.T) {
	temperature := 1.8
	body, err := anthropicDialect{}.encode(Invocation{
		ModelName: "claude-sonnet-4", Stream: true, MaxOutputTokens: 256, Temperature: &temperature,
		Messages: []Message{
			{Role: "system", Content: "be brief"},
			{Role: "user", Content: "hello"},
			{Role: "assistant", Content: "calling a tool", ToolCalls: []ToolCall{{ID: "toolu_1", Name: "lookup", Arguments: `{"q":"go"}`}}},
			{Role: "tool", ToolCallID: "toolu_1", Content: "result text"},
		},
		Tools: []ToolDefinition{{Name: "lookup", Description: "look up", Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)}},
	}, CapabilityProfile{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode encoded body: %v", err)
	}
	if decoded["system"] != "be brief" {
		t.Fatalf("system = %v, want the top-level system prompt", decoded["system"])
	}
	if _, ok := decoded["max_tokens"]; !ok {
		t.Fatalf("max_tokens is required by the Anthropic protocol: %s", body)
	}
	if decoded["max_tokens"].(float64) != 256 {
		t.Fatalf("max_tokens = %v, want 256", decoded["max_tokens"])
	}
	if decoded["temperature"].(float64) != anthropicMaxTemperature {
		t.Fatalf("temperature = %v, want it clamped to %v", decoded["temperature"], anthropicMaxTemperature)
	}
	messages := decoded["messages"].([]any)
	if len(messages) != 3 {
		t.Fatalf("messages = %d, want 3 (user, assistant, tool-result user turn)", len(messages))
	}
	assistant := messages[1].(map[string]any)
	blocks := assistant["content"].([]any)
	if len(blocks) != 2 {
		t.Fatalf("assistant content blocks = %d, want text+tool_use", len(blocks))
	}
	toolUse := blocks[1].(map[string]any)
	if toolUse["type"] != "tool_use" || toolUse["id"] != "toolu_1" || toolUse["name"] != "lookup" {
		t.Fatalf("tool_use block = %v", toolUse)
	}
	if _, ok := toolUse["input"].(map[string]any); !ok {
		t.Fatalf("tool_use input must be an object: %v", toolUse["input"])
	}
	toolResult := messages[2].(map[string]any)
	if toolResult["role"] != "user" {
		t.Fatalf("tool result turn role = %v, want user", toolResult["role"])
	}
	resultBlock := toolResult["content"].([]any)[0].(map[string]any)
	if resultBlock["type"] != "tool_result" || resultBlock["tool_use_id"] != "toolu_1" {
		t.Fatalf("tool_result block = %v", resultBlock)
	}
	tools := decoded["tools"].([]any)
	tool := tools[0].(map[string]any)
	if _, ok := tool["input_schema"]; !ok {
		t.Fatalf("anthropic tools need input_schema: %v", tool)
	}
	if _, ok := tool["function"]; ok {
		t.Fatalf("anthropic tools must not use the OpenAI function wrapper: %v", tool)
	}
}

func TestAnthropicEncodeRejectsInvalidInvocations(t *testing.T) {
	if _, err := (anthropicDialect{}).encode(Invocation{ModelName: "m", Messages: []Message{{Role: "assistant", Content: "hi"}}}, CapabilityProfile{}); err == nil {
		t.Fatal("a first assistant turn must be rejected")
	}
	if _, err := (anthropicDialect{}).encode(Invocation{ModelName: "m"}, CapabilityProfile{}); err == nil {
		t.Fatal("an empty conversation must be rejected")
	}
	if _, err := (anthropicDialect{}).encode(Invocation{ModelName: "m", Messages: []Message{{Role: "user", Content: "hi"}}, MaxOutputTokens: -1}, CapabilityProfile{}); err == nil {
		t.Fatal("a negative max output token must be rejected")
	}
	disabled := false
	if _, err := (anthropicDialect{}).encode(Invocation{ModelName: "m", Messages: []Message{{Role: "user", Content: "hi"}}, Tools: []ToolDefinition{{Name: "t"}}}, CapabilityProfile{ToolCalling: &disabled}); err == nil {
		t.Fatal("a capability profile that disables tool calling must be honoured")
	}
}

func TestAnthropicDecodeCompletion(t *testing.T) {
	body := []byte(`{
		"id":"msg_123","type":"message","role":"assistant",
		"content":[
			{"type":"text","text":"hello "},
			{"type":"text","text":"world"},
			{"type":"tool_use","id":"toolu_9","name":"lookup","input":{"q":"go"}}
		],
		"stop_reason":"tool_use",
		"usage":{"input_tokens":11,"output_tokens":7}
	}`)
	parsed, err := (anthropicDialect{}).decodeCompletion(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed.Content != "hello world" {
		t.Fatalf("content = %q", parsed.Content)
	}
	if len(parsed.ToolCalls) != 1 {
		t.Fatalf("tool calls = %d, want 1", len(parsed.ToolCalls))
	}
	call := parsed.ToolCalls[0]
	if call.ID != "toolu_9" || call.Name != "lookup" || call.Arguments != `{"q":"go"}` {
		t.Fatalf("tool call = %+v", call)
	}
	if parsed.FinishReason != "tool_calls" {
		t.Fatalf("finish reason = %q, want the OpenAI-style tool_calls", parsed.FinishReason)
	}
	if !parsed.UsageReported || parsed.InputTokens != 11 || parsed.OutputTokens != 7 {
		t.Fatalf("usage = %d/%d reported=%v", parsed.InputTokens, parsed.OutputTokens, parsed.UsageReported)
	}
	if parsed.rawID != "msg_123" {
		t.Fatalf("raw id = %q", parsed.rawID)
	}
}

func TestAnthropicConsumeStream(t *testing.T) {
	stream := strings.Join([]string{
		"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":30,\"output_tokens\":0}}}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"lookup\",\"input\":{}}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"go\\\"}\"}}\n\n",
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":25}}\n\n",
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}, "")
	var content []string
	var generated []string
	parsed, delivered, err := (anthropicDialect{}).consumeStream(strings.NewReader(stream), StreamObserver{
		OnContent:   func(delta string) { content = append(content, delta) },
		OnGenerated: func(delta string) { generated = append(generated, delta) },
	})
	if err != nil {
		t.Fatalf("consume stream: %v", err)
	}
	if !delivered {
		t.Fatal("delivered must be true after content deltas")
	}
	if strings.Join(content, "") != "Hello" || parsed.Content != "Hello" {
		t.Fatalf("content = %q deltas = %v", parsed.Content, content)
	}
	if len(parsed.ToolCalls) != 1 {
		t.Fatalf("tool calls = %d, want 1", len(parsed.ToolCalls))
	}
	call := parsed.ToolCalls[0]
	if call.ID != "toolu_1" || call.Name != "lookup" {
		t.Fatalf("tool call = %+v", call)
	}
	if call.Arguments != `{"q":"go"}` {
		t.Fatalf("tool arguments = %q, want the assembled partial JSON", call.Arguments)
	}
	if parsed.FinishReason != "tool_calls" {
		t.Fatalf("finish reason = %q", parsed.FinishReason)
	}
	if parsed.InputTokens != 30 || parsed.OutputTokens != 25 || !parsed.UsageReported {
		t.Fatalf("usage = %d/%d reported=%v", parsed.InputTokens, parsed.OutputTokens, parsed.UsageReported)
	}
	if len(generated) == 0 {
		t.Fatal("generated bytes must be metered for the budget observer")
	}
}

func TestAnthropicStreamRequiresMessageStop(t *testing.T) {
	stream := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n"
	if _, _, err := (anthropicDialect{}).consumeStream(strings.NewReader(stream), StreamObserver{}); err == nil {
		t.Fatal("a stream without message_stop must fail instead of reporting success")
	}
}

func TestAnthropicExecutorAgainstFakeEndpoint(t *testing.T) {
	fake := &fakeAnthropic{body: `{"id":"msg_7","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":4}}`}
	server := httptest.NewServer(fake)
	defer server.Close()

	executor := anthropicExecutor(server.URL)
	result, err := executor.Complete(context.Background(), Invocation{
		ModelName: "claude-sonnet-4", Messages: []Message{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if result.Content != "hi" || result.FinishReason != "stop" {
		t.Fatalf("result = %+v", result)
	}
	if result.InputTokens != 3 || result.OutputTokens != 4 || !result.UsageReported {
		t.Fatalf("usage = %d/%d reported=%v", result.InputTokens, result.OutputTokens, result.UsageReported)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.paths[0] != "/v1/messages" {
		t.Fatalf("path = %q, want /v1/messages", fake.paths[0])
	}
	if fake.apiKeys[0] != "test-key" {
		t.Fatalf("x-api-key = %q", fake.apiKeys[0])
	}
	if fake.versions[0] != anthropicVersion {
		t.Fatalf("anthropic-version = %q", fake.versions[0])
	}
	if fake.authorizatn[0] != "" {
		t.Fatalf("the Anthropic dialect must not send an Authorization header, got %q", fake.authorizatn[0])
	}
	if fake.bodies[0]["max_tokens"] == nil {
		t.Fatalf("the request must carry max_tokens: %v", fake.bodies[0])
	}
}

func TestAnthropicExecutorStreamsAgainstFakeEndpoint(t *testing.T) {
	fake := &fakeAnthropic{stream: []string{
		"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_2\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"streamed\"}}\n\n",
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":9}}\n\n",
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	var deltas []string
	result, err := anthropicExecutor(server.URL).Stream(context.Background(), Invocation{
		ModelName: "claude-sonnet-4", Stream: true, Messages: []Message{{Role: "user", Content: "hello"}},
	}, func(delta string) { deltas = append(deltas, delta) })
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if strings.Join(deltas, "") != "streamed" || result.Content != "streamed" {
		t.Fatalf("deltas = %v content = %q", deltas, result.Content)
	}
	if result.InputTokens != 5 || result.OutputTokens != 9 || !result.UsageReported {
		t.Fatalf("usage = %d/%d reported=%v", result.InputTokens, result.OutputTokens, result.UsageReported)
	}
}

func TestProviderProtocolSelection(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(Config{Name: "default", BaseURL: "http://127.0.0.1:1/v1", APIKeyEnv: "X"}); err != nil {
		t.Fatalf("register default: %v", err)
	}
	if err := registry.Register(Config{Name: "claude", BaseURL: "http://127.0.0.1:1", APIKeyEnv: "X", Protocol: ProtocolAnthropic}); err != nil {
		t.Fatalf("register anthropic: %v", err)
	}
	if err := registry.Register(Config{Name: "broken", BaseURL: "http://127.0.0.1:1", APIKeyEnv: "X", Protocol: "grpc"}); err == nil {
		t.Fatal("an unsupported protocol must be rejected at registration")
	}
	executor, err := registry.Resolve("claude")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if executor.dialect.path("https://api.anthropic.com") != "/v1/messages" {
		t.Fatalf("anthropic path = %q", executor.dialect.path("https://api.anthropic.com"))
	}
	if executor.dialect.path("https://api.anthropic.com/v1") != "/messages" {
		t.Fatalf("anthropic path for a /v1 base = %q", executor.dialect.path("https://api.anthropic.com/v1"))
	}
	openAI, err := registry.Resolve("default")
	if err != nil {
		t.Fatalf("resolve default: %v", err)
	}
	if openAI.dialect.path("http://127.0.0.1:1/v1") != "/chat/completions" {
		t.Fatalf("openai path = %q", openAI.dialect.path("http://127.0.0.1:1/v1"))
	}
}

func TestUnsupportedProtocolFailsLoudly(t *testing.T) {
	executor := NewExecutor(Config{Name: "broken", BaseURL: "http://127.0.0.1:1", APIKeyEnv: "X", Protocol: "grpc"}, nil)
	if _, err := executor.Complete(context.Background(), Invocation{ModelName: "m", Messages: []Message{{Role: "user", Content: "hi"}}}); err == nil {
		t.Fatal("an unsupported protocol must fail at call time, not fall back to OpenAI")
	}
}
