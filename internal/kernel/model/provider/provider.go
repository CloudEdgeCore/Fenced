// Package provider implements the real model execution layer behind the Model
// Gateway decision chain: an OpenAI-compatible HTTP executor
// covering vLLM, Qwen, DeepSeek, GLM and any endpoint that speaks the
// /chat/completions dialect, with streaming, bounded retries, a per-provider
// circuit breaker, exact usage extraction and provider request-id capture.
//
// Credentials never cross this boundary in either direction: the API key is
// resolved from the configured environment variable per request, is attached
// only to the outbound Authorization header, and is stripped from every error
// message and telemetry surface.
package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/redact"
)

// Sentinel errors the invoker maps onto ledger outcomes.
var (
	// ErrProviderUnavailable reports a provider endpoint that could not be
	// reached after the bounded retry budget, or one whose circuit breaker
	// is open (fast fail).
	ErrProviderUnavailable = errors.New("model provider unavailable")
	// ErrProviderRejected reports a non-retryable provider refusal (4xx other
	// than 408/429): authentication, permission, malformed request or model.
	ErrProviderRejected = errors.New("model provider rejected the request")
	// ErrStreamAborted reports a stream that failed after content had already
	// been delivered; it is never retried, because a retry would duplicate
	// billed output.
	ErrStreamAborted = errors.New("model provider stream aborted after delivery")
	// ErrCircuitOpen is returned by a shared breaker backend when another
	// gateway instance has opened or is probing the provider circuit.
	ErrCircuitOpen = errors.New("model provider circuit breaker open")
)

// CircuitBreaker is the optional distributed provider-health coordination
// surface. PostgreSQL implements it so every gateway instance observes the
// same open/half-open state; a nil breaker retains the process-local fallback.
type CircuitBreaker interface {
	AcquireCircuit(context.Context, CircuitAcquire) (CircuitPermit, error)
	RecordCircuit(context.Context, CircuitRecord) error
}

type CircuitAcquire struct {
	Provider  string
	Threshold int
	Cooldown  time.Duration
	ProbeTTL  time.Duration
	Now       time.Time
}

type CircuitPermit struct{ ProbeToken string }

type CircuitRecord struct {
	Provider   string
	ProbeToken string
	Success    bool
	Retryable  bool
	Threshold  int
	Now        time.Time
}

// Message is one chat turn. Role is system|user|assistant|tool; ToolCalls
// carries OpenAI-style function calls on assistant turns; ToolCallID binds a
// tool-role turn to its call.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

// ToolCall is one function call the model requested.
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDefinition declares one callable tool to the model. The internal shape
// stays flat; encodeInvocation converts it into the OpenAI-compatible
// function-tool wire shape ({"type":"function","function":{…}}) at the
// request boundary.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// Invocation is one provider request.
type Invocation struct {
	ModelName       string           `json:"model"`
	Messages        []Message        `json:"messages"`
	Temperature     *float64         `json:"temperature,omitempty"`
	MaxOutputTokens int32            `json:"max_tokens,omitempty"`
	Stream          bool             `json:"stream"`
	Tools           []ToolDefinition `json:"tools,omitempty"`
	IdempotencyKey  string           `json:"-"`
	TraceID         string           `json:"-"`
	TaskID          string           `json:"-"`
	AttemptID       string           `json:"-"`
	ModelCallID     string           `json:"-"`
}

// Result is the outcome of one invocation. Usage is the exact provider-reported
// token counts (never estimated by the caller).
type Result struct {
	Content           string     `json:"content"`
	ToolCalls         []ToolCall `json:"toolCalls,omitempty"`
	FinishReason      string     `json:"finishReason"`
	InputTokens       int64      `json:"inputTokens"`
	OutputTokens      int64      `json:"outputTokens"`
	ProviderRequestID string     `json:"providerRequestId"`
	UsageReported     bool       `json:"usageReported"`
}

// Config is one provider endpoint. APIKeyEnv names the environment variable
// the key is resolved from on every call; the key itself is never stored.
type Config struct {
	Name                string            `json:"name"`
	BaseURL             string            `json:"baseUrl"`
	APIKeyEnv           string            `json:"apiKeyEnv"`
	APIKey              string            `json:"-"` // programmatic (tests); file loads must use APIKeyEnv
	Timeout             time.Duration     `json:"-"`
	TimeoutMs           int64             `json:"timeoutMs"`
	MaxAttempts         int               `json:"maxAttempts"`
	BreakerOpens        int               `json:"breakerOpens"`
	BreakerCoolMs       int64             `json:"breakerCooldownMs"`
	MaxConcurrent       int               `json:"maxConcurrent"`
	SupportsIdempotency bool              `json:"supportsIdempotency"`
	IdempotencyHeader   string            `json:"idempotencyHeader"`
	RetryAmbiguous      bool              `json:"retryAmbiguous"`
	HealthPath          string            `json:"healthPath"`
	HealthMethod        string            `json:"healthMethod"`
	Capabilities        CapabilityProfile `json:"capabilities,omitempty"`
	// Tokenizer names the token estimator used for this provider's budget
	// reservations: "heuristic" (default, script-aware) or "conservative"
	// (uncharacterized tokenizers). Provider-reported usage remains the
	// authoritative settlement.
	Tokenizer string `json:"tokenizer,omitempty"`
}

// CapabilityProfile makes OpenAI-compatible dialect differences explicit.
// Pointer booleans distinguish an omitted field (safe default) from false.
type CapabilityProfile struct {
	StreamUsage    *bool  `json:"streamUsage,omitempty"`
	ToolCalling    *bool  `json:"toolCalling,omitempty"`
	JSONSchema     *bool  `json:"jsonSchema,omitempty"`
	Reasoning      *bool  `json:"reasoning,omitempty"`
	Usage          *bool  `json:"usage,omitempty"`
	RequestID      *bool  `json:"requestId,omitempty"`
	MaxTokensField string `json:"maxTokensField,omitempty"` // max_tokens|max_completion_tokens
}

// Defaults applied by (Config).resolved().
const (
	defaultTimeout       = 120 * time.Second
	defaultMaxAttempts   = 3
	defaultBreakerOpen   = 5
	defaultBreakerCool   = 30 * time.Second
	maxAttemptsCeiling   = 8
	retryBaseDelay       = 250 * time.Millisecond
	retryMaxDelay        = 5 * time.Second
	maxRequestBytes      = 4 << 20
	maxResponseBytes     = 32 << 20
	defaultMaxConcurrent = 32
)

func (c Config) resolved() Config {
	if c.Timeout <= 0 {
		if c.TimeoutMs > 0 {
			c.Timeout = time.Duration(c.TimeoutMs) * time.Millisecond
		} else {
			c.Timeout = defaultTimeout
		}
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.MaxAttempts > maxAttemptsCeiling {
		c.MaxAttempts = maxAttemptsCeiling
	}
	if c.BreakerOpens <= 0 {
		c.BreakerOpens = defaultBreakerOpen
	}
	if c.BreakerCoolMs <= 0 {
		c.BreakerCoolMs = int64(defaultBreakerCool / time.Millisecond)
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = defaultMaxConcurrent
	}
	if c.IdempotencyHeader == "" {
		c.IdempotencyHeader = "Idempotency-Key"
	}
	if c.HealthPath == "" {
		c.HealthPath = "/models"
	}
	if c.HealthMethod == "" {
		c.HealthMethod = http.MethodGet
	}
	return c
}

// Executor executes invocations against one OpenAI-compatible endpoint.
type Executor struct {
	config Config
	client *http.Client

	breakerMu       sync.Mutex
	consecutiveBad  int
	breakerOpenedAt time.Time
	probing         bool

	jitter   func(time.Duration) time.Duration
	now      func() time.Time
	bulkhead chan struct{}
	shared   CircuitBreaker
}

// WithCircuitBreaker installs a distributed breaker backend.
func (e *Executor) WithCircuitBreaker(shared CircuitBreaker) *Executor {
	e.shared = shared
	return e
}

// NewExecutor builds the executor for one provider endpoint.
func NewExecutor(config Config, client *http.Client) *Executor {
	if client == nil {
		client = &http.Client{}
	}
	resolved := config.resolved()
	return &Executor{
		config: resolved, client: client,
		jitter: func(d time.Duration) time.Duration { return d/2 + time.Duration(rand.Int64N(int64(d)+1)) },
		now:    time.Now, bulkhead: make(chan struct{}, resolved.MaxConcurrent),
	}
}

// Name returns the provider name model references resolve against.
func (e *Executor) Name() string { return e.config.Name }

// Tokenizer returns the configured token-estimator name for budget
// reservations on this provider.
func (e *Executor) Tokenizer() string { return e.config.Tokenizer }

// Health probes the endpoint with a short-lived /models request. It is the
// half-open probe of the circuit breaker as well as the readiness signal.
func (e *Executor) Health(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := e.newRequest(probeCtx, e.config.HealthMethod, e.config.HealthPath, nil)
	if err != nil {
		return err
	}
	response, err := e.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: health probe: %s", ErrProviderUnavailable, sanitize(err.Error(), e))
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("%w: health probe status %d", ErrProviderUnavailable, response.StatusCode)
	}
	return nil
}

// Complete performs one non-streaming invocation with bounded retries.
func (e *Executor) Complete(ctx context.Context, invocation Invocation) (Result, error) {
	invocation.Stream = false
	if err := e.acquireBulkhead(ctx); err != nil {
		return Result{}, err
	}
	defer e.releaseBulkhead()
	var result Result
	err := e.withRetries(ctx, invocation, func(ctx context.Context, request *http.Request) error {
		response, err := e.client.Do(request)
		if err != nil {
			return transportError(err, e)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		if err != nil {
			return transportError(err, e)
		}
		if len(body) > maxResponseBytes {
			return &classifiedError{
				err:       fmt.Errorf("%w: response exceeds %d bytes", ErrProviderUnavailable, maxResponseBytes),
				retryable: true,
			}
		}
		if response.StatusCode/100 != 2 {
			return statusError(response, body, e)
		}
		parsed, err := decodeCompletion(body)
		if err != nil {
			return &classifiedError{err: fmt.Errorf("%w: decode completion: %s", ErrProviderUnavailable, sanitize(err.Error(), e)), retryable: true, ambiguous: true}
		}
		parsed.ProviderRequestID = requestID(response, parsed.rawID)
		result = parsed.Result
		return nil
	})
	return result, err
}

// Stream performs one streaming invocation, delivering content deltas to
// onDelta as they arrive. Retries happen only before the first delta is
// delivered; after that, a failure aborts the stream without retry so no
// output is duplicated.
func (e *Executor) Stream(ctx context.Context, invocation Invocation, onDelta func(string)) (Result, error) {
	return e.StreamObserved(ctx, invocation, StreamObserver{OnContent: onDelta})
}

// StreamObserver separates user-visible content from all provider-generated
// bytes. Budget meters observe content, tool names and tool arguments while
// callers only receive textual content.
type StreamObserver struct {
	OnContent   func(string)
	OnGenerated func(string)
}

// StreamObserved performs one streaming invocation with complete generation
// metering and retry-delivery semantics.
func (e *Executor) StreamObserved(ctx context.Context, invocation Invocation, observer StreamObserver) (Result, error) {
	invocation.Stream = true
	if err := e.acquireBulkhead(ctx); err != nil {
		return Result{}, err
	}
	defer e.releaseBulkhead()
	var result Result
	err := e.withRetries(ctx, invocation, func(ctx context.Context, request *http.Request) error {
		response, err := e.client.Do(request)
		if err != nil {
			return transportError(err, e)
		}
		defer response.Body.Close()
		if response.StatusCode/100 != 2 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			return statusError(response, body, e)
		}
		providerRequestID := response.Header.Get("x-request-id")
		parsed, delivered, err := consumeStream(response.Body, observer)
		parsed.ProviderRequestID = requestID(response, parsed.rawID)
		if providerRequestID != "" {
			parsed.ProviderRequestID = providerRequestID
		}
		result = parsed.Result
		if err != nil {
			if delivered {
				return aborted(err, e)
			}
			return transportError(err, e)
		}
		return nil
	})
	return result, err
}

// classifiedError is the single retry classifier: Retryable says whether the
// attempt budget is consumed, RetryAfter carries the provider's hint.
type classifiedError struct {
	err        error
	retryable  bool
	retryAfter time.Duration
	ambiguous  bool
}

// UsageKnownZero reports whether an executor error proves the provider did
// not accept billable generation (for example a definitive 4xx or 429).
// Every ambiguous transport/5xx/partial-stream outcome returns false.
func UsageKnownZero(err error) bool {
	if errors.Is(err, ErrProviderRejected) {
		return true
	}
	var classified *classifiedError
	return errors.As(err, &classified) && !classified.ambiguous
}

func (c *classifiedError) Error() string { return c.err.Error() }
func (c *classifiedError) Unwrap() error { return c.err }

func transportError(err error, e *Executor) error {
	return &classifiedError{
		err:       fmt.Errorf("%w: %s", ErrProviderUnavailable, sanitize(err.Error(), e)),
		retryable: true, ambiguous: true,
	}
}

func aborted(err error, e *Executor) error {
	return fmt.Errorf("%w: %s", ErrStreamAborted, sanitize(err.Error(), e))
}

func statusError(response *http.Response, body []byte, e *Executor) error {
	detail := sanitize(string(body), e)
	if len(detail) > 512 {
		detail = detail[:512]
	}
	statusCode := response.StatusCode
	classified := &classifiedError{retryAfter: parseRetryAfter(response)}
	if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		classified.err = fmt.Errorf("%w: status %d: %s", ErrProviderUnavailable, statusCode, detail)
		classified.retryable = true
		classified.ambiguous = statusCode != http.StatusTooManyRequests
		return classified
	}
	classified.err = fmt.Errorf("%w: status %d: %s", ErrProviderRejected, statusCode, detail)
	return classified
}

// withRetries runs attempt under the circuit breaker with bounded exponential
// backoff. Only retryable errors (network, 408/429/5xx, malformed 2xx stream
// before delivery) consume the retry budget.
func (e *Executor) withRetries(ctx context.Context, invocation Invocation, attempt func(context.Context, *http.Request) error) error {
	permit, err := e.acquire(ctx)
	if err != nil {
		return err
	}
	var lastErr error
	for try := 0; try < e.config.MaxAttempts; try++ {
		if try > 0 {
			if err := sleepBackoff(ctx, try, lastErr, e); err != nil {
				return err
			}
		}
		// Each attempt gets a fresh deadline; a slow provider consumes its
		// own budget, not the whole retry window.
		attemptCtx, cancel := context.WithTimeout(ctx, e.config.Timeout)
		body, err := encodeInvocation(invocation, e.config.Capabilities)
		if err != nil {
			cancel()
			return err
		}
		request, err := e.newRequest(attemptCtx, http.MethodPost, "/chat/completions", body)
		if err != nil {
			cancel()
			return err
		}
		if invocation.IdempotencyKey != "" && e.config.SupportsIdempotency {
			request.Header.Set(e.config.IdempotencyHeader, invocation.IdempotencyKey)
		}
		setCorrelationHeaders(request, invocation)
		err = attempt(attemptCtx, request)
		cancel()
		if err == nil {
			e.recordSuccess(ctx, permit)
			return nil
		}
		lastErr = err
		var classified *classifiedError
		if !errors.As(err, &classified) || !classified.retryable {
			e.recordFailure(ctx, permit, false)
			return err
		}
		if classified.ambiguous && !(e.config.SupportsIdempotency || e.config.RetryAmbiguous) {
			e.recordFailure(ctx, permit, true)
			return fmt.Errorf("%w: provider outcome is ambiguous and automatic retry is disabled", ErrProviderUnavailable)
		}
		// A distributed breaker counts failed logical invocations, not the
		// executor's internal attempts, and keeps a half-open probe fenced until
		// its final outcome. The local fallback retains its historic per-attempt
		// behavior.
		if e.shared == nil || try == e.config.MaxAttempts-1 {
			e.recordFailure(ctx, permit, true)
		}
	}
	return lastErr
}

func (e *Executor) acquireBulkhead(ctx context.Context) error {
	select {
	case e.bulkhead <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("%w: provider concurrency limit reached", ErrProviderUnavailable)
	}
}

func (e *Executor) releaseBulkhead() { <-e.bulkhead }

func setCorrelationHeaders(request *http.Request, invocation Invocation) {
	for name, value := range map[string]string{
		"X-Fenced-Trace-ID":      invocation.TraceID,
		"X-Fenced-Task-ID":       invocation.TaskID,
		"X-Fenced-Attempt-ID":    invocation.AttemptID,
		"X-Fenced-Model-Call-ID": invocation.ModelCallID,
	} {
		if value != "" {
			request.Header.Set(name, value)
		}
	}
}

func (e *Executor) newRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(e.config.BaseURL, "/")+path, reader)
	if err != nil {
		return nil, fmt.Errorf("build provider request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if key := e.apiKey(); key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	request.Header.Set("Accept", invocationAccept(body))
	return request, nil
}

func invocationAccept(body []byte) string {
	if bytes.Contains(body, []byte(`"stream":true`)) {
		return "text/event-stream"
	}
	return "application/json"
}

func (e *Executor) apiKey() string {
	if e.config.APIKey != "" {
		return e.config.APIKey
	}
	if e.config.APIKeyEnv == "" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(e.config.APIKeyEnv))
}

// sleepBackoff waits before the next attempt, honoring Retry-After when the
// provider sent it on an HTTP status error.
func sleepBackoff(ctx context.Context, attempt int, cause error, e *Executor) error {
	delay := retryBaseDelay << (attempt - 1)
	if delay > retryMaxDelay {
		delay = retryMaxDelay
	}
	delay = e.jitter(delay)
	var classified *classifiedError
	if errors.As(cause, &classified) && classified.retryAfter > delay {
		delay = classified.retryAfter
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// parseRetryAfter extracts the Retry-After hint of an HTTP error response
// (delta-seconds or an HTTP date, bounded so a hostile value cannot stall
// the runtime).
func parseRetryAfter(response *http.Response) time.Duration {
	raw := strings.TrimSpace(response.Header.Get("Retry-After"))
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 && seconds <= 300 {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(raw); err == nil {
		if delay := time.Until(date); delay > 0 && delay <= 5*time.Minute {
			return delay
		}
	}
	return 0
}

// Circuit breaker: acquire admits the call (fast-failing while open), release
// is handled by recordSuccess/recordFailure through withRetries' defer-free
// flow. Half-open admits exactly one probe after the cooldown elapses.
func (e *Executor) acquire(ctx context.Context) (CircuitPermit, error) {
	if e.shared != nil {
		permit, err := e.shared.AcquireCircuit(ctx, CircuitAcquire{
			Provider: e.config.Name, Threshold: e.config.BreakerOpens,
			Cooldown: time.Duration(e.config.BreakerCoolMs) * time.Millisecond,
			ProbeTTL: e.config.Timeout, Now: e.now().UTC(),
		})
		if err != nil {
			return CircuitPermit{}, fmt.Errorf("%w: distributed circuit for provider %q: %v", ErrProviderUnavailable, e.config.Name, err)
		}
		return permit, nil
	}
	e.breakerMu.Lock()
	defer e.breakerMu.Unlock()
	if e.consecutiveBad < e.config.BreakerOpens {
		return CircuitPermit{}, nil
	}
	cooldown := time.Duration(e.config.BreakerCoolMs) * time.Millisecond
	if e.now().Sub(e.breakerOpenedAt) < cooldown {
		return CircuitPermit{}, fmt.Errorf("%w: circuit breaker open for provider %q", ErrProviderUnavailable, e.config.Name)
	}
	if e.probing {
		return CircuitPermit{}, fmt.Errorf("%w: circuit breaker half-open probe in flight", ErrProviderUnavailable)
	}
	e.probing = true
	return CircuitPermit{}, nil
}

func (e *Executor) recordSuccess(ctx context.Context, permit CircuitPermit) {
	if e.shared != nil {
		_ = e.shared.RecordCircuit(ctx, CircuitRecord{
			Provider: e.config.Name, ProbeToken: permit.ProbeToken, Success: true,
			Threshold: e.config.BreakerOpens, Now: e.now().UTC(),
		})
		return
	}
	e.breakerMu.Lock()
	defer e.breakerMu.Unlock()
	e.consecutiveBad = 0
	e.probing = false
}

func (e *Executor) recordFailure(ctx context.Context, permit CircuitPermit, retryable bool) {
	if e.shared != nil {
		_ = e.shared.RecordCircuit(ctx, CircuitRecord{
			Provider: e.config.Name, ProbeToken: permit.ProbeToken, Retryable: retryable,
			Threshold: e.config.BreakerOpens, Now: e.now().UTC(),
		})
		return
	}
	e.breakerMu.Lock()
	defer e.breakerMu.Unlock()
	if !retryable {
		// Definitive refusals say nothing about endpoint health.
		e.probing = false
		return
	}
	e.consecutiveBad++
	if e.consecutiveBad >= e.config.BreakerOpens {
		e.breakerOpenedAt = e.now()
	}
	e.probing = false
}

// wireToolRequest is the OpenAI-compatible function-tool wire shape of one
// request-side tool declaration: {"type":"function","function":{...}}. The
// kernel's internal ToolDefinition stays flat; the conversion happens only
// at this wire boundary, because strict OpenAI-compatible providers (vLLM,
// Qwen, DeepSeek, GLM, OpenAI itself) reject or ignore a bare
// {"name",...} object in "tools".
type wireToolRequest struct {
	Type     string             `json:"type"`
	Function wireToolDefinition `json:"function"`
}

type wireToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// wireMessage is the OpenAI-compatible wire shape of one chat turn. An
// assistant turn's tool calls must serialize as
// [{"id","type":"function","function":{"name","arguments"}}]: the internal
// flat ToolCall shape is a parse convenience, and sending it verbatim makes
// strict providers reject the follow-up turn with the tool results (the
// real-model closed loop died exactly there).
type wireChatTurn struct {
	Role       string             `json:"role"`
	Content    string             `json:"content"`
	ToolCallID string             `json:"tool_call_id,omitempty"`
	ToolCalls  []wireToolCallItem `json:"tool_calls,omitempty"`
}

type wireToolCallItem struct {
	ID       string               `json:"id"`
	Type     string               `json:"type"`
	Function wireToolCallFunction `json:"function"`
}

type wireToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// encodeMessages converts internal chat turns into their wire shape.
func encodeMessages(messages []Message) []wireChatTurn {
	converted := make([]wireChatTurn, 0, len(messages))
	for _, message := range messages {
		wire := wireChatTurn{Role: message.Role, Content: message.Content, ToolCallID: message.ToolCallID}
		for _, call := range message.ToolCalls {
			wire.ToolCalls = append(wire.ToolCalls, wireToolCallItem{
				ID:   call.ID,
				Type: "function",
				Function: wireToolCallFunction{
					Name: call.Name, Arguments: call.Arguments,
				},
			})
		}
		converted = append(converted, wire)
	}
	return converted
}

func encodeInvocation(invocation Invocation, profiles ...CapabilityProfile) ([]byte, error) {
	profile := CapabilityProfile{}
	if len(profiles) > 0 {
		profile = profiles[0]
	}
	type wire struct {
		Model               string            `json:"model"`
		Messages            []wireChatTurn    `json:"messages"`
		Temperature         *float64          `json:"temperature,omitempty"`
		MaxTokens           int32             `json:"max_tokens,omitempty"`
		MaxCompletionTokens int32             `json:"max_completion_tokens,omitempty"`
		Stream              bool              `json:"stream,omitempty"`
		StreamOptions       *wireStreamOpts   `json:"stream_options,omitempty"`
		Tools               []wireToolRequest `json:"tools,omitempty"`
	}
	value := wire{
		Model: invocation.ModelName, Messages: encodeMessages(invocation.Messages),
		Temperature: invocation.Temperature, MaxTokens: invocation.MaxOutputTokens,
		Stream: invocation.Stream, Tools: encodeTools(invocation.Tools),
	}
	if profile.MaxTokensField != "" && profile.MaxTokensField != "max_tokens" && profile.MaxTokensField != "max_completion_tokens" {
		return nil, fmt.Errorf("unsupported provider maxTokensField %q", profile.MaxTokensField)
	}
	if profile.MaxTokensField == "max_completion_tokens" {
		value.MaxCompletionTokens, value.MaxTokens = value.MaxTokens, 0
	}
	if len(invocation.Tools) > 0 && profile.ToolCalling != nil && !*profile.ToolCalling {
		return nil, fmt.Errorf("provider capability profile disables tool calling")
	}
	if invocation.Stream && (profile.StreamUsage == nil || *profile.StreamUsage) {
		value.StreamOptions = &wireStreamOpts{IncludeUsage: true}
	}
	if strings.TrimSpace(invocation.ModelName) == "" {
		return nil, fmt.Errorf("provider invocation requires a model")
	}
	if invocation.MaxOutputTokens < 0 {
		return nil, fmt.Errorf("max output tokens must not be negative")
	}
	if invocation.Temperature != nil && (math.IsNaN(*invocation.Temperature) || math.IsInf(*invocation.Temperature, 0) || *invocation.Temperature < 0 || *invocation.Temperature > 2) {
		return nil, fmt.Errorf("temperature must be finite and between 0 and 2")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode provider invocation: %w", err)
	}
	if len(encoded) > maxRequestBytes {
		return nil, fmt.Errorf("provider request exceeds %d bytes", maxRequestBytes)
	}
	return encoded, nil
}

// encodeTools converts internal tool declarations into the OpenAI-compatible
// function-tool wire shape. It never mutates the invocation's declarations.
func encodeTools(tools []ToolDefinition) []wireToolRequest {
	if len(tools) == 0 {
		return nil
	}
	converted := make([]wireToolRequest, 0, len(tools))
	for _, tool := range tools {
		// wireToolDefinition mirrors ToolDefinition field-for-field, so the
		// conversion is a direct type conversion.
		converted = append(converted, wireToolRequest{
			Type:     "function",
			Function: wireToolDefinition(tool),
		})
	}
	return converted
}

type wireStreamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

type wireMessage struct {
	Role      string     `json:"role"`
	Content   string     `json:"content"`
	ToolCalls []wireTool `json:"tool_calls"`
}

type wireCompletion struct {
	ID      string `json:"id"`
	Choices []struct {
		Index        int          `json:"index"`
		Message      *wireMessage `json:"message"`
		Delta        *wireMessage `json:"delta"`
		FinishReason *string      `json:"finish_reason"`
	} `json:"choices"`
	Usage *wireUsage `json:"usage"`
}

type wireTool struct {
	ID       string `json:"id"`
	Index    *int   `json:"index"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type wireUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

type parsedCompletion struct {
	Result
	rawID string
}

func decodeCompletion(body []byte) (parsedCompletion, error) {
	var completion wireCompletion
	if err := json.Unmarshal(body, &completion); err != nil {
		return parsedCompletion{}, err
	}
	parsed := parsedCompletion{rawID: completion.ID}
	if len(completion.Choices) == 0 {
		return parsed, fmt.Errorf("completion has no choices")
	}
	choice := completion.Choices[0]
	if choice.Message != nil {
		parsed.Content = choice.Message.Content
		for _, call := range choice.Message.ToolCalls {
			parsed.ToolCalls = append(parsed.ToolCalls, ToolCall{
				ID: call.ID, Name: call.Function.Name, Arguments: call.Function.Arguments,
			})
		}
	}
	if choice.FinishReason != nil {
		parsed.FinishReason = *choice.FinishReason
	}
	if completion.Usage != nil {
		parsed.InputTokens = completion.Usage.PromptTokens
		parsed.OutputTokens = completion.Usage.CompletionTokens
		parsed.UsageReported = true
	}
	return parsed, nil
}

// consumeStream reads one SSE stream. It returns the assembled result and
// whether any delta was delivered (retry safety).
func consumeStream(body io.Reader, observer StreamObserver) (parsed parsedCompletion, delivered bool, err error) {
	reader := bufio.NewReaderSize(body, 64<<10)
	var content strings.Builder
	toolCalls := map[int]*ToolCall{}
	var finishReason string
	var usage wireUsage
	haveUsage := false
	for {
		line, readErr := reader.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed != "" {
			if data, ok := strings.CutPrefix(trimmed, "data:"); ok {
				payload := strings.TrimSpace(data)
				if payload == "[DONE]" {
					parsed.Content = content.String()
					for _, call := range toolCalls {
						parsed.ToolCalls = append(parsed.ToolCalls, *call)
					}
					parsed.FinishReason = finishReason
					if haveUsage {
						parsed.InputTokens, parsed.OutputTokens = usage.PromptTokens, usage.CompletionTokens
						parsed.UsageReported = true
					}
					return parsed, delivered, nil
				}
				var chunk wireCompletion
				if json.Unmarshal([]byte(payload), &chunk) == nil {
					if chunk.ID != "" && parsed.rawID == "" {
						parsed.rawID = chunk.ID
					}
					if chunk.Usage != nil {
						usage = *chunk.Usage
						haveUsage = true
					}
					for _, choice := range chunk.Choices {
						if choice.FinishReason != nil && *choice.FinishReason != "" {
							finishReason = *choice.FinishReason
						}
						// OpenAI streams put increments in delta; some
						// compatible endpoints reuse message. Either is
						// accepted.
						increment := choice.Delta
						if increment == nil {
							increment = choice.Message
						}
						if increment == nil {
							continue
						}
						if increment.Content != "" {
							delivered = true
							if observer.OnContent != nil {
								observer.OnContent(increment.Content)
							}
							if observer.OnGenerated != nil {
								observer.OnGenerated(increment.Content)
							}
							content.WriteString(increment.Content)
						}
						for _, call := range increment.ToolCalls {
							if call.ID != "" || call.Function.Name != "" || call.Function.Arguments != "" {
								delivered = true
							}
							if observer.OnGenerated != nil {
								observer.OnGenerated(call.Function.Name)
								observer.OnGenerated(call.Function.Arguments)
							}
							index := 0
							if call.Index != nil {
								index = *call.Index
							}
							existing := toolCalls[index]
							if existing == nil {
								existing = &ToolCall{}
								toolCalls[index] = existing
							}
							if call.ID != "" {
								existing.ID = call.ID
							}
							if call.Function.Name != "" {
								existing.Name = call.Function.Name
							}
							existing.Arguments += call.Function.Arguments
						}
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return parsed, delivered, fmt.Errorf("stream ended without a [DONE] sentinel")
			}
			return parsed, delivered, readErr
		}
	}
}

// requestID prefers the header, falls back to the response body id.
func requestID(response *http.Response, bodyID string) string {
	if header := strings.TrimSpace(response.Header.Get("x-request-id")); header != "" {
		return header
	}
	return bodyID
}

// sanitize removes credential material and endpoint URLs from provider error
// text before it crosses into kernel surfaces.
func sanitize(detail string, e *Executor) string {
	return redact.RedactText(detail, e.apiKey(), e.config.BaseURL)
}
