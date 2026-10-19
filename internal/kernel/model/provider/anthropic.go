package provider

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
)

// anthropicDialect implements the Anthropic Messages protocol
// (POST /v1/messages).
//
// The differences from the OpenAI-compatible dialect that matter to the
// kernel:
//
//   - the system prompt is a top-level field, not a chat turn;
//   - max_tokens is required, and temperature is limited to 0..1;
//   - tools are declared with input_schema, not {"type":"function"};
//   - assistant tool calls are tool_use content blocks and tool results are
//     tool_result blocks inside a user turn;
//   - the response content is a block list and the stop reason is
//     stop_reason, not finish_reason;
//   - the stream is a typed event sequence terminated by message_stop
//     instead of a [DONE] sentinel.
//
// The kernel's internal shapes (Message, ToolCall with a JSON argument
// string, Result with OpenAI-style finish reasons) stay unchanged: the
// translation happens only at this wire boundary, exactly like the
// OpenAI-compatible function-tool conversion.
type anthropicDialect struct{}

// anthropicVersion is the Anthropic API version header value.
const anthropicVersion = "2023-06-01"

// anthropicDefaultMaxTokens is used when the caller did not bound the
// output: the Anthropic protocol rejects a request without max_tokens.
const anthropicDefaultMaxTokens int32 = 4096

// anthropicMaxTokensCeiling is the largest output Anthropic accepts.
const anthropicMaxTokensCeiling int32 = 64000

// anthropicMaxTemperature is the largest temperature Anthropic accepts.
const anthropicMaxTemperature = 1.0

func (anthropicDialect) path(baseURL string) string {
	trimmed := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(trimmed, "/v1") {
		return "/messages"
	}
	return "/v1/messages"
}

func (anthropicDialect) defaultHealthPath() string { return "/v1/models" }

func (anthropicDialect) headers(e *Executor, request *http.Request) {
	key := e.apiKey()
	if key == "" {
		return
	}
	// Anthropic authenticates with x-api-key; the Authorization header is
	// deliberately not set so a proxy that forwards both schemes cannot
	// reject the request.
	request.Header.Set("x-api-key", key)
	request.Header.Set("anthropic-version", anthropicVersion)
}

// --- request encoding ---

type anthropicRequest struct {
	Model         string          `json:"model"`
	MaxTokens     int32           `json:"max_tokens"`
	System        string          `json:"system,omitempty"`
	Messages      []anthropicTurn `json:"messages"`
	Temperature   *float64        `json:"temperature,omitempty"`
	Stream        bool            `json:"stream,omitempty"`
	Tools         []anthropicTool `json:"tools,omitempty"`
	StopSequences []string        `json:"stop_sequences,omitempty"`
}

type anthropicTurn struct {
	Role    string             `json:"role"`
	Content []anthropicContent `json:"content"`
}

type anthropicContent struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Result    string          `json:"content,omitempty"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

func (anthropicDialect) encode(invocation Invocation, profile CapabilityProfile) ([]byte, error) {
	if strings.TrimSpace(invocation.ModelName) == "" {
		return nil, fmt.Errorf("provider invocation requires a model")
	}
	if invocation.MaxOutputTokens < 0 {
		return nil, fmt.Errorf("max output tokens must not be negative")
	}
	if len(invocation.Tools) > 0 && profile.ToolCalling != nil && !*profile.ToolCalling {
		return nil, fmt.Errorf("provider capability profile disables tool calling")
	}
	if invocation.Temperature != nil {
		if math.IsNaN(*invocation.Temperature) || math.IsInf(*invocation.Temperature, 0) || *invocation.Temperature < 0 {
			return nil, fmt.Errorf("temperature must be finite and not negative")
		}
	}
	maxTokens := invocation.MaxOutputTokens
	if maxTokens <= 0 {
		maxTokens = anthropicDefaultMaxTokens
	}
	if maxTokens > anthropicMaxTokensCeiling {
		maxTokens = anthropicMaxTokensCeiling
	}
	temperature := invocation.Temperature
	if temperature != nil && *temperature > anthropicMaxTemperature {
		clamped := anthropicMaxTemperature
		temperature = &clamped
	}

	system, turns, err := encodeAnthropicTurns(invocation.Messages)
	if err != nil {
		return nil, err
	}
	tools, err := encodeAnthropicTools(invocation.Tools)
	if err != nil {
		return nil, err
	}
	request := anthropicRequest{
		Model:       invocation.ModelName,
		MaxTokens:   maxTokens,
		System:      system,
		Messages:    turns,
		Temperature: temperature,
		Stream:      invocation.Stream,
		Tools:       tools,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode provider invocation: %w", err)
	}
	if len(encoded) > maxRequestBytes {
		return nil, fmt.Errorf("provider request exceeds %d bytes", maxRequestBytes)
	}
	return encoded, nil
}

// encodeAnthropicTurns converts the internal chat turns into Anthropic
// turns. System turns become the top-level system prompt, tool turns become
// tool_result blocks inside a user turn, and assistant tool calls become
// tool_use blocks.
func encodeAnthropicTurns(messages []Message) (string, []anthropicTurn, error) {
	var systemParts []string
	turns := make([]anthropicTurn, 0, len(messages))
	for _, message := range messages {
		switch message.Role {
		case "system":
			if strings.TrimSpace(message.Content) != "" {
				systemParts = append(systemParts, message.Content)
			}
		case "tool":
			if message.ToolCallID == "" {
				return "", nil, fmt.Errorf("tool turn requires the tool call id it answers")
			}
			turns = append(turns, anthropicTurn{Role: "user", Content: []anthropicContent{{
				Type: "tool_result", ToolUseID: message.ToolCallID, Result: message.Content,
			}}})
		case "assistant":
			blocks := make([]anthropicContent, 0, 1+len(message.ToolCalls))
			if message.Content != "" {
				blocks = append(blocks, anthropicContent{Type: "text", Text: message.Content})
			}
			for _, call := range message.ToolCalls {
				input, err := anthropicToolInput(call.Arguments)
				if err != nil {
					return "", nil, err
				}
				blocks = append(blocks, anthropicContent{Type: "tool_use", ID: call.ID, Name: call.Name, Input: input})
			}
			if len(blocks) == 0 {
				continue
			}
			turns = append(turns, anthropicTurn{Role: "assistant", Content: blocks})
		default:
			if message.Content == "" {
				continue
			}
			turns = append(turns, anthropicTurn{Role: "user", Content: []anthropicContent{{Type: "text", Text: message.Content}}})
		}
	}
	if len(turns) == 0 {
		return "", nil, fmt.Errorf("provider invocation requires at least one user message")
	}
	if turns[0].Role != "user" {
		return "", nil, fmt.Errorf("anthropic protocol requires the first message to be a user turn")
	}
	return strings.Join(systemParts, "\n\n"), mergeAnthropicTurns(turns), nil
}

// mergeAnthropicTurns folds consecutive turns of the same role into one
// turn: the Anthropic protocol expects alternating user and assistant turns.
func mergeAnthropicTurns(turns []anthropicTurn) []anthropicTurn {
	merged := make([]anthropicTurn, 0, len(turns))
	for _, turn := range turns {
		if len(merged) > 0 && merged[len(merged)-1].Role == turn.Role {
			merged[len(merged)-1].Content = append(merged[len(merged)-1].Content, turn.Content...)
			continue
		}
		merged = append(merged, turn)
	}
	return merged
}

// anthropicToolInput converts the internal JSON argument string into the
// object Anthropic expects as tool_use input. A non-object argument is
// wrapped rather than dropped, so no provider information is lost.
func anthropicToolInput(arguments string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(arguments)
	if trimmed == "" {
		return json.RawMessage("{}"), nil
	}
	var value any
	if err := json.Unmarshal([]byte(trimmed), &value); err != nil {
		return nil, fmt.Errorf("tool call arguments are not valid JSON: %w", err)
	}
	if _, ok := value.(map[string]any); ok {
		return json.RawMessage(trimmed), nil
	}
	encoded, err := json.Marshal(map[string]any{"value": value})
	if err != nil {
		return nil, fmt.Errorf("wrap tool call arguments: %w", err)
	}
	return encoded, nil
}

// encodeAnthropicTools converts tool declarations into the Anthropic shape.
// input_schema is required, so an empty declaration becomes an empty object
// schema instead of an omitted field.
func encodeAnthropicTools(tools []ToolDefinition) ([]anthropicTool, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	converted := make([]anthropicTool, 0, len(tools))
	for _, tool := range tools {
		schema := tool.Parameters
		if len(strings.TrimSpace(string(schema))) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		if !json.Valid(schema) {
			return nil, fmt.Errorf("tool %q parameters are not valid JSON", tool.Name)
		}
		converted = append(converted, anthropicTool{
			Name: tool.Name, Description: tool.Description, InputSchema: schema,
		})
	}
	return converted, nil
}

// --- response decoding ---

type anthropicResponse struct {
	ID         string             `json:"id"`
	Type       string             `json:"type"`
	Role       string             `json:"role"`
	Content    []anthropicContent `json:"content"`
	Model      string             `json:"model"`
	StopReason string             `json:"stop_reason"`
	Usage      *anthropicUsage    `json:"usage"`
}

type anthropicUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

func (anthropicDialect) decodeCompletion(body []byte) (parsedCompletion, error) {
	var response anthropicResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return parsedCompletion{}, err
	}
	parsed := parsedCompletion{rawID: response.ID}
	var content strings.Builder
	for _, block := range response.Content {
		switch block.Type {
		case "text":
			content.WriteString(block.Text)
		case "tool_use":
			arguments := "{}"
			if len(block.Input) > 0 {
				arguments = string(block.Input)
			}
			parsed.ToolCalls = append(parsed.ToolCalls, ToolCall{ID: block.ID, Name: block.Name, Arguments: arguments})
		}
	}
	parsed.Content = content.String()
	parsed.FinishReason = anthropicFinishReason(response.StopReason)
	if response.Usage != nil {
		parsed.InputTokens = response.Usage.InputTokens
		parsed.OutputTokens = response.Usage.OutputTokens
		parsed.UsageReported = true
	}
	return parsed, nil
}

// anthropicFinishReason maps an Anthropic stop reason onto the
// OpenAI-style finish reasons the kernel ledger stores, so downstream
// consumers (agent loops, receipts) do not need a second vocabulary.
func anthropicFinishReason(stopReason string) string {
	switch stopReason {
	case "":
		return ""
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "pause_turn":
		return "stop"
	default:
		return stopReason
	}
}

// --- stream consumption ---

type anthropicStreamEvent struct {
	Type         string             `json:"type"`
	Index        int                `json:"index"`
	Message      *anthropicMessage  `json:"message"`
	ContentBlock *anthropicContent  `json:"content_block"`
	Delta        *anthropicDelta    `json:"delta"`
	Usage        *anthropicUsage    `json:"usage"`
	Error        *anthropicStreamEr `json:"error"`
}

type anthropicMessage struct {
	ID    string          `json:"id"`
	Usage *anthropicUsage `json:"usage"`
}

type anthropicDelta struct {
	Type        string `json:"type"`
	Text        string `json:"text"`
	PartialJSON string `json:"partial_json"`
	Thinking    string `json:"thinking"`
	StopReason  string `json:"stop_reason"`
}

type anthropicStreamEr struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// anthropicToolStream accumulates one streamed tool_use block.
type anthropicToolStream struct {
	id      string
	name    string
	partial strings.Builder
}

func (anthropicDialect) consumeStream(body io.Reader, observer StreamObserver) (parsed parsedCompletion, delivered bool, err error) {
	reader := bufio.NewReaderSize(body, 64<<10)
	var content strings.Builder
	tools := map[int]*anthropicToolStream{}
	var finishReason string
	var usage anthropicUsage
	haveUsage := false
	delivered = false

	for {
		line, readErr := reader.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed != "" {
			// Anthropic sends "event:" and "data:" lines; the event type is
			// also carried inside the payload, so the data line is enough.
			if data, ok := strings.CutPrefix(trimmed, "data:"); ok {
				payload := strings.TrimSpace(data)
				if payload != "" && payload != "[DONE]" {
					var event anthropicStreamEvent
					if err := json.Unmarshal([]byte(payload), &event); err != nil {
						return parsedCompletion{}, delivered, fmt.Errorf("decode anthropic stream event: %w", err)
					}
					switch event.Type {
					case "message_start":
						if event.Message != nil {
							if event.Message.ID != "" {
								parsed.rawID = event.Message.ID
							}
							if event.Message.Usage != nil {
								usage = *event.Message.Usage
								haveUsage = true
							}
						}
					case "content_block_start":
						if event.ContentBlock != nil && event.ContentBlock.Type == "tool_use" {
							tools[event.Index] = &anthropicToolStream{id: event.ContentBlock.ID, name: event.ContentBlock.Name}
						}
					case "content_block_delta":
						if event.Delta == nil {
							break
						}
						switch event.Delta.Type {
						case "text_delta":
							if event.Delta.Text == "" {
								break
							}
							delivered = true
							if observer.OnContent != nil {
								observer.OnContent(event.Delta.Text)
							}
							if observer.OnGenerated != nil {
								observer.OnGenerated(event.Delta.Text)
							}
							content.WriteString(event.Delta.Text)
						case "thinking_delta":
							// Extended thinking is provider-generated text
							// that is not part of the user-visible answer.
							if event.Delta.Thinking == "" {
								break
							}
							delivered = true
							if observer.OnThinking != nil {
								observer.OnThinking(event.Delta.Thinking)
							}
							if observer.OnGenerated != nil {
								observer.OnGenerated(event.Delta.Thinking)
							}
						case "input_json_delta":
							delivered = true
							stream := tools[event.Index]
							if stream == nil {
								stream = &anthropicToolStream{}
								tools[event.Index] = stream
							}
							stream.partial.WriteString(event.Delta.PartialJSON)
							if observer.OnGenerated != nil {
								observer.OnGenerated(event.Delta.PartialJSON)
							}
						}
					case "message_delta":
						if event.Delta != nil && event.Delta.StopReason != "" {
							finishReason = anthropicFinishReason(event.Delta.StopReason)
						}
						if event.Usage != nil {
							if event.Usage.InputTokens > 0 {
								usage.InputTokens = event.Usage.InputTokens
							}
							usage.OutputTokens = event.Usage.OutputTokens
							haveUsage = true
						}
					case "message_stop":
						parsed.Content = content.String()
						for _, stream := range tools {
							arguments := stream.partial.String()
							if strings.TrimSpace(arguments) == "" {
								arguments = "{}"
							}
							parsed.ToolCalls = append(parsed.ToolCalls, ToolCall{ID: stream.id, Name: stream.name, Arguments: arguments})
						}
						parsed.FinishReason = finishReason
						if haveUsage {
							parsed.InputTokens, parsed.OutputTokens = usage.InputTokens, usage.OutputTokens
							parsed.UsageReported = true
						}
						return parsed, delivered, nil
					case "error":
						if event.Error != nil {
							return parsedCompletion{}, delivered, fmt.Errorf("anthropic stream error: %s: %s", event.Error.Type, event.Error.Message)
						}
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return parsedCompletion{}, delivered, fmt.Errorf("stream ended without a message_stop event")
			}
			return parsedCompletion{}, delivered, readErr
		}
	}
}
