package provider

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Protocol identifiers accepted in provider configuration.
const (
	// ProtocolOpenAI is the /chat/completions dialect spoken by OpenAI
	// itself and by vLLM, Qwen, DeepSeek, GLM and other compatible
	// endpoints. It is the default.
	ProtocolOpenAI = "openai"
	// ProtocolAnthropic is the Anthropic Messages dialect: /v1/messages
	// with a top-level system prompt, tool_use/tool_result content blocks
	// and typed server-sent events.
	ProtocolAnthropic = "anthropic"
)

// dialect abstracts the provider wire protocol. The kernel speaks two
// dialects; everything above this boundary (budget reservation, usage
// settlement, receipts, circuit breaking) is protocol-agnostic.
type dialect interface {
	// path returns the request path relative to the provider base URL.
	path(baseURL string) string
	// defaultHealthPath returns the readiness probe path used when the
	// configuration does not pin one.
	defaultHealthPath() string
	// headers sets the authentication and protocol headers of one request.
	headers(e *Executor, request *http.Request)
	// encode renders an invocation in the provider wire format.
	encode(invocation Invocation, profile CapabilityProfile) ([]byte, error)
	// decodeCompletion parses one non-streaming response.
	decodeCompletion(body []byte) (parsedCompletion, error)
	// consumeStream parses one server-sent-event stream.
	consumeStream(body io.Reader, observer StreamObserver) (parsedCompletion, bool, error)
}

// newDialect resolves the configured protocol. An unknown protocol yields a
// failing dialect instead of a silent OpenAI fallback: a misconfigured
// provider must fail loudly at the first call, never answer with the wrong
// wire format.
func newDialect(config Config) dialect {
	switch normalizeProtocol(config.Protocol) {
	case ProtocolOpenAI:
		return openAIDialect{}
	case ProtocolAnthropic:
		return anthropicDialect{}
	default:
		return failingDialect{err: fmt.Errorf("provider %q: unsupported protocol %q (supported: %s, %s)",
			config.Name, config.Protocol, ProtocolOpenAI, ProtocolAnthropic)}
	}
}

// normalizeProtocol maps a configured protocol onto a known identifier. An
// empty value means the OpenAI-compatible dialect.
func normalizeProtocol(protocol string) string {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "":
		return ProtocolOpenAI
	case ProtocolOpenAI:
		return ProtocolOpenAI
	case ProtocolAnthropic:
		return ProtocolAnthropic
	default:
		return strings.ToLower(strings.TrimSpace(protocol))
	}
}

// openAIDialect is the /chat/completions dialect. Its behaviour is the
// historical kernel behaviour and stays byte-identical: the dialect only
// routes the request and response handling.
type openAIDialect struct{}

func (openAIDialect) path(string) string { return "/chat/completions" }

func (openAIDialect) defaultHealthPath() string { return "/models" }

func (openAIDialect) headers(e *Executor, request *http.Request) {
	if key := e.apiKey(); key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
}

func (openAIDialect) encode(invocation Invocation, profile CapabilityProfile) ([]byte, error) {
	return encodeInvocation(invocation, profile)
}

func (openAIDialect) decodeCompletion(body []byte) (parsedCompletion, error) {
	return decodeCompletion(body)
}

func (openAIDialect) consumeStream(body io.Reader, observer StreamObserver) (parsedCompletion, bool, error) {
	return consumeStream(body, observer)
}

// failingDialect reports an unsupported protocol at call time.
type failingDialect struct{ err error }

func (f failingDialect) path(string) string { return "" }

func (f failingDialect) defaultHealthPath() string { return "/models" }

func (f failingDialect) headers(*Executor, *http.Request) {}

func (f failingDialect) encode(Invocation, CapabilityProfile) ([]byte, error) {
	return nil, f.err
}

func (f failingDialect) decodeCompletion([]byte) (parsedCompletion, error) {
	return parsedCompletion{}, f.err
}

func (f failingDialect) consumeStream(io.Reader, StreamObserver) (parsedCompletion, bool, error) {
	return parsedCompletion{}, false, f.err
}
