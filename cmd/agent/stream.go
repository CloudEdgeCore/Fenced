package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
)

// cliStreamDeadline bounds one developer-CLI model call. The kernel executor
// gives every attempt its own deadline; the CLI keeps a generous one so a
// long reasoning stream is not cut off, and the header timeout still comes
// from the provider configuration.
const cliStreamDeadline = 30 * time.Minute

// StreamLLM streams one completion through the kernel model layer and prints
// it, keeping the provider's reasoning channel separate from the answer.
//
// The wire protocol is the kernel's: one implementation of the OpenAI
// compatible and Anthropic Messages dialects serves both the gateway and
// this CLI, so a protocol change lands in one place.
func StreamLLM(cfg *AgentYAMLConfig, systemPrompt, userPrompt string) (int, error) {
	providerConfig := cfg.CurrentProvider()
	if providerConfig.APIKey == "" {
		return 0, fmt.Errorf("provider %q has no API key configured", cfg.LLM.DefaultProvider)
	}

	executor, err := newCLIExecutor(cfg)
	if err != nil {
		return 0, err
	}

	messages := make([]provider.Message, 0, 2)
	if strings.TrimSpace(systemPrompt) != "" {
		messages = append(messages, provider.Message{Role: "system", Content: systemPrompt})
	}
	messages = append(messages, provider.Message{Role: "user", Content: userPrompt})

	totalChars := 0
	inReasoning := false
	observer := provider.StreamObserver{
		OnContent: func(delta string) {
			if inReasoning {
				fmt.Print("\n\n\033[32m[content] \033[0m\n")
				inReasoning = false
			}
			fmt.Print(delta)
			_ = os.Stdout.Sync()
			totalChars += len(delta)
		},
		OnThinking: func(delta string) {
			if !inReasoning {
				fmt.Print("\n\033[36m[reasoning] \033[0m")
				inReasoning = true
			}
			fmt.Print(delta)
			_ = os.Stdout.Sync()
			totalChars += len(delta)
		},
	}

	result, err := executor.StreamObserved(context.Background(), provider.Invocation{
		ModelName: providerConfig.Model,
		Messages:  messages,
		Stream:    true,
	}, observer)
	if err != nil {
		fmt.Println()
		return 0, err
	}
	fmt.Println()

	// The provider-reported usage is authoritative; the character estimate is
	// only a fallback for endpoints that do not report it.
	if result.UsageReported {
		return int(result.InputTokens + result.OutputTokens), nil
	}
	approxTokens := totalChars / 3
	if approxTokens < 100 {
		approxTokens = 150
	}
	return approxTokens, nil
}

// newCLIExecutor builds the kernel model executor for the configured
// provider. The CLI resolves the same protocol the kernel does: an explicit
// protocol wins, otherwise an Anthropic endpoint is detected by name.
func newCLIExecutor(cfg *AgentYAMLConfig) (*provider.Executor, error) {
	providerConfig := cfg.CurrentProvider()
	if strings.TrimSpace(providerConfig.BaseURL) == "" {
		return nil, fmt.Errorf("provider %q has no base URL configured", cfg.LLM.DefaultProvider)
	}
	if strings.TrimSpace(providerConfig.Model) == "" {
		return nil, fmt.Errorf("provider %q has no model configured", cfg.LLM.DefaultProvider)
	}

	// CurrentProvider already resolves the protocol, including the
	// Anthropic detection by provider name or base URL.
	protocol := provider.ProtocolOpenAI
	switch strings.ToLower(strings.TrimSpace(providerConfig.Protocol)) {
	case provider.ProtocolAnthropic:
		protocol = provider.ProtocolAnthropic
	case provider.ProtocolOpenAI:
		protocol = provider.ProtocolOpenAI
	default:
		return nil, fmt.Errorf("provider %q declares an unsupported protocol %q", cfg.LLM.DefaultProvider, providerConfig.Protocol)
	}

	headerTimeout := 45 * time.Second
	if providerConfig.TimeoutSec > 0 {
		headerTimeout = time.Duration(providerConfig.TimeoutSec) * time.Second
	}
	client := &http.Client{
		Transport: &http.Transport{ResponseHeaderTimeout: headerTimeout},
		// The stream itself is long-lived: the deadline is the executor's.
		Timeout: 0,
	}

	config := provider.Config{
		Name:        cfg.LLM.DefaultProvider,
		BaseURL:     providerConfig.BaseURL,
		APIKey:      providerConfig.APIKey,
		Protocol:    protocol,
		Timeout:     cliStreamDeadline,
		MaxAttempts: 2,
	}
	if protocol == provider.ProtocolOpenAI {
		// Provider attribution hints; they are never credentials.
		config.ExtraHeaders = map[string]string{
			"HTTP-Referer": "https://fenced.dev",
			"X-Title":      "Fenced-CLI",
		}
	}
	return provider.NewExecutor(config, client), nil
}
