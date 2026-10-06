package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// OpenAI streaming delta structs
type StreamDelta struct {
	Content   string `json:"content"`
	Reasoning string `json:"reasoning"`
}

type StreamChoice struct {
	Delta StreamDelta `json:"delta"`
}

type StreamChunk struct {
	Choices []StreamChoice `json:"choices"`
}

// Anthropic streaming chunk structs
type AnthropicStreamDelta struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Thinking string `json:"thinking"`
}

type AnthropicStreamChunk struct {
	Type  string               `json:"type"`
	Delta AnthropicStreamDelta `json:"delta"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func buildOpenAIEndpoint(baseURL string) string {
	u := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(u, "/chat/completions") {
		return u
	}
	return u + "/chat/completions"
}

func buildAnthropicEndpoint(baseURL string) string {
	u := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(u, "/messages") {
		return u
	}
	if strings.HasSuffix(u, "/v1") {
		return u + "/messages"
	}
	return u + "/v1/messages"
}

func StreamLLM(cfg *AgentYAMLConfig, systemPrompt, userPrompt string) (int, error) {
	provider := cfg.CurrentProvider()
	if provider.APIKey == "" {
		return 0, fmt.Errorf("provider %q has no API key configured", cfg.LLM.DefaultProvider)
	}

	protocol := strings.ToLower(strings.TrimSpace(provider.Protocol))
	if protocol == "" {
		if strings.EqualFold(cfg.LLM.DefaultProvider, "anthropic") || strings.Contains(strings.ToLower(provider.BaseURL), "anthropic") {
			protocol = "anthropic"
		} else {
			protocol = "openai"
		}
	}

	var endpoint string
	var reqBody map[string]any

	if protocol == "anthropic" {
		endpoint = buildAnthropicEndpoint(provider.BaseURL)
		reqBody = map[string]any{
			"model": provider.Model,
			"messages": []map[string]string{
				{"role": "user", "content": userPrompt},
			},
			"max_tokens": 4096,
			"stream":     true,
		}
		if systemPrompt != "" {
			reqBody["system"] = systemPrompt
		}
	} else {
		endpoint = buildOpenAIEndpoint(provider.BaseURL)
		reqBody = map[string]any{
			"model": provider.Model,
			"messages": []map[string]string{
				{"role": "system", "content": systemPrompt},
				{"role": "user", "content": userPrompt},
			},
			"stream": true,
		}
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	if protocol == "anthropic" {
		req.Header.Set("x-api-key", provider.APIKey)
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("Authorization", "Bearer "+provider.APIKey)
	} else {
		req.Header.Set("Authorization", "Bearer "+provider.APIKey)
		req.Header.Set("HTTP-Referer", "https://fenced.dev")
		req.Header.Set("X-Title", "Fenced-CLI")
	}

	headerTimeout := 45 * time.Second
	if provider.TimeoutSec > 0 {
		headerTimeout = time.Duration(provider.TimeoutSec) * time.Second
	}

	tr := &http.Transport{
		ResponseHeaderTimeout: headerTimeout,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   0, // long-lived streaming to prevent timeout during reasoning
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("upstream API error (status %d): %s", resp.StatusCode, string(b))
	}

	scanner := bufio.NewScanner(resp.Body)
	totalChars := 0
	inReasoning := false

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		if protocol == "anthropic" {
			var aChunk AnthropicStreamChunk
			if err := json.Unmarshal([]byte(data), &aChunk); err != nil {
				continue
			}
			if aChunk.Error != nil {
				return totalChars / 3, fmt.Errorf("anthropic stream error: %s", aChunk.Error.Message)
			}
			if aChunk.Delta.Thinking != "" {
				if !inReasoning {
					fmt.Print("\n\033[36m[reasoning] \033[0m")
					inReasoning = true
				}
				fmt.Print(aChunk.Delta.Thinking)
				_ = os.Stdout.Sync()
				totalChars += len(aChunk.Delta.Thinking)
			}
			if aChunk.Delta.Text != "" {
				if inReasoning {
					fmt.Print("\n\n\033[32m[content] \033[0m\n")
					inReasoning = false
				}
				fmt.Print(aChunk.Delta.Text)
				_ = os.Stdout.Sync()
				totalChars += len(aChunk.Delta.Text)
			}
		} else {
			var chunk StreamChunk
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue
			}

			if len(chunk.Choices) > 0 {
				c := chunk.Choices[0]
				if c.Delta.Reasoning != "" {
					if !inReasoning {
						fmt.Print("\n\033[36m[reasoning] \033[0m")
						inReasoning = true
					}
					fmt.Print(c.Delta.Reasoning)
					_ = os.Stdout.Sync()
					totalChars += len(c.Delta.Reasoning)
				}
				if c.Delta.Content != "" {
					if inReasoning {
						fmt.Print("\n\n\033[32m[content] \033[0m\n")
						inReasoning = false
					}
					fmt.Print(c.Delta.Content)
					_ = os.Stdout.Sync()
					totalChars += len(c.Delta.Content)
				}
			}
		}
	}

	fmt.Println()
	approxTokens := totalChars / 3
	if approxTokens < 100 {
		approxTokens = 150
	}
	return approxTokens, nil
}
