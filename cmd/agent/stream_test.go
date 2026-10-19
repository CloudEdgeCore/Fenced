package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
)

// The CLI must speak the kernel's protocols: one implementation of the
// OpenAI-compatible and Anthropic Messages dialects serves both the gateway
// and this command.
func TestCLIExecutorResolvesTheKernelProtocol(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		provider string
		baseURL  string
		protocol string
		want     string
	}{
		{name: "explicit anthropic", provider: "claude", baseURL: "https://api.example", protocol: "anthropic", want: provider.ProtocolAnthropic},
		{name: "detected anthropic", provider: "anthropic", baseURL: "https://api.example", want: provider.ProtocolAnthropic},
		{name: "detected anthropic by url", provider: "custom", baseURL: "https://api.anthropic.com", want: provider.ProtocolAnthropic},
		{name: "openai by default", provider: "openrouter", baseURL: "https://openrouter.ai/api/v1", want: provider.ProtocolOpenAI},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			config := &AgentYAMLConfig{LLM: LLMConfig{DefaultProvider: testCase.provider, Providers: map[string]ProviderConfig{
				testCase.provider: {Model: "model-x", APIKey: "key", BaseURL: testCase.baseURL, Protocol: testCase.protocol},
			}}}
			executor, err := newCLIExecutor(config)
			if err != nil {
				t.Fatalf("executor: %v", err)
			}
			if executor == nil {
				t.Fatal("executor must not be nil")
			}
			if got := executor.Name(); got != testCase.provider {
				t.Fatalf("executor name = %q", got)
			}
		})
	}
}

func TestCLIExecutorRejectsAnIncompleteProvider(t *testing.T) {
	config := &AgentYAMLConfig{LLM: LLMConfig{DefaultProvider: "broken", Providers: map[string]ProviderConfig{
		"broken": {Model: "model-x", APIKey: "key"},
	}}}
	if _, err := newCLIExecutor(config); err == nil {
		t.Fatal("a provider without a base URL must be rejected")
	}
}

// The CLI streams through the kernel executor, so the reasoning channel and
// the answer stay separated and the provider-reported usage is authoritative.
func TestCLIStreamsThroughTheKernelExecutor(t *testing.T) {
	var seenPath, seenAuth, seenReferer string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seenPath = request.URL.Path
		seenAuth = request.Header.Get("Authorization")
		seenReferer = request.Header.Get("HTTP-Referer")
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning\":\"pondering\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7}}\n\n"))
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	config := &AgentYAMLConfig{LLM: LLMConfig{DefaultProvider: "fake", Providers: map[string]ProviderConfig{
		"fake": {Model: "model-x", APIKey: "secret-key", BaseURL: server.URL, Protocol: "openai"},
	}}}
	executor, err := newCLIExecutor(config)
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	var content, thinking []string
	result, err := executor.StreamObserved(context.Background(), provider.Invocation{
		ModelName: "model-x",
		Messages:  []provider.Message{{Role: "user", Content: "hello"}},
		Stream:    true,
	}, provider.StreamObserver{
		OnContent:  func(delta string) { content = append(content, delta) },
		OnThinking: func(delta string) { thinking = append(thinking, delta) },
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if strings.Join(content, "") != "answer" {
		t.Fatalf("content = %v", content)
	}
	if strings.Join(thinking, "") != "pondering" {
		t.Fatalf("thinking = %v", thinking)
	}
	if !result.UsageReported || result.InputTokens != 11 || result.OutputTokens != 7 {
		t.Fatalf("usage = %d/%d reported=%v", result.InputTokens, result.OutputTokens, result.UsageReported)
	}
	if seenPath != "/chat/completions" {
		t.Fatalf("path = %q", seenPath)
	}
	if seenAuth != "Bearer secret-key" {
		t.Fatalf("authorization = %q", seenAuth)
	}
	if seenReferer != "https://fenced.dev" {
		t.Fatalf("attribution header = %q", seenReferer)
	}
}
