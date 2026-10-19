package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The reasoning channel of an OpenAI-compatible endpoint must reach the
// thinking observer and never the content observer: reasoning is
// provider-generated text, not part of the answer.
func TestOpenAIReasoningReachesTheThinkingChannel(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"reasoning":"pondering"}}]}`,
		`data: {"choices":[{"delta":{"reasoning_content":"more"}}]}`,
		`data: {"choices":[{"delta":{"content":"answer"}}]}`,
		`data: [DONE]`,
		"",
	}, "\n")
	var content, thinking, generated []string
	parsed, delivered, err := (openAIDialect{}).consumeStream(strings.NewReader(stream), StreamObserver{
		OnContent:   func(delta string) { content = append(content, delta) },
		OnThinking:  func(delta string) { thinking = append(thinking, delta) },
		OnGenerated: func(delta string) { generated = append(generated, delta) },
	})
	if err != nil {
		t.Fatalf("consume stream: %v", err)
	}
	if !delivered {
		t.Fatal("delivered must be true")
	}
	if strings.Join(content, "") != "answer" {
		t.Fatalf("content = %q, want only the answer", strings.Join(content, ""))
	}
	if strings.Join(thinking, "") != "ponderingmore" {
		t.Fatalf("thinking = %q", strings.Join(thinking, ""))
	}
	if strings.Join(generated, "") != "ponderingmoreanswer" {
		t.Fatalf("generated = %q, want every provider byte", strings.Join(generated, ""))
	}
	if parsed.Content != "answer" {
		t.Fatalf("parsed content = %q", parsed.Content)
	}
}

// The Anthropic thinking blocks must reach the same channel.
func TestAnthropicThinkingReachesTheThinkingChannel(t *testing.T) {
	stream := strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":4,"output_tokens":0}}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"pondering"}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"answer"}}`,
		``,
		`event: message_stop`,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
	var content, thinking []string
	parsed, _, err := (anthropicDialect{}).consumeStream(strings.NewReader(stream), StreamObserver{
		OnContent:  func(delta string) { content = append(content, delta) },
		OnThinking: func(delta string) { thinking = append(thinking, delta) },
	})
	if err != nil {
		t.Fatalf("consume stream: %v", err)
	}
	if strings.Join(thinking, "") != "pondering" {
		t.Fatalf("thinking = %q", strings.Join(thinking, ""))
	}
	if strings.Join(content, "") != "answer" {
		t.Fatalf("content = %q, want only the answer", strings.Join(content, ""))
	}
	if parsed.Content != "answer" {
		t.Fatalf("parsed content = %q", parsed.Content)
	}
}

// Extra headers carry provider hints. They must reach the endpoint and must
// never override the authentication header of the dialect.
func TestExtraHeadersNeverOverrideAuthentication(t *testing.T) {
	var seen http.Header
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen = request.Header.Clone()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"id":"c1","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer server.Close()

	executor := NewExecutor(Config{
		Name: "hinted", BaseURL: server.URL, APIKey: "real-key", MaxAttempts: 1,
		ExtraHeaders: map[string]string{
			"HTTP-Referer":   "https://fenced.dev",
			"X-Title":        "Fenced-CLI",
			"Authorization":  "Bearer forged-key",
			"X-Empty-Header": "",
		},
	}, nil)
	if _, err := executor.Complete(context.Background(), Invocation{
		ModelName: "m", Messages: []Message{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := seen.Get("HTTP-Referer"); got != "https://fenced.dev" {
		t.Fatalf("HTTP-Referer = %q", got)
	}
	if got := seen.Get("X-Title"); got != "Fenced-CLI" {
		t.Fatalf("X-Title = %q", got)
	}
	if got := seen.Get("Authorization"); got != "Bearer real-key" {
		t.Fatalf("authorization = %q, want the configured key", got)
	}
	if _, present := seen["X-Empty-Header"]; present {
		t.Fatal("an empty extra header must not be sent")
	}
}
