// Real model invocation: Invoker binds the fenced decision
// chain (Begin → Settle → Finish) to a provider execution layer. The gateway
// still owns policy, budget, the call ledger and cost computation; the
// executor owns the wire. Content flows through and is never persisted — only
// metadata (usage, cost, provider request id, finish reason) reaches the
// ledger and the audit receipt.
package model

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/tokens"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
	"github.com/google/uuid"
)

// ErrNoProviderExecution reports a model whose provider has no execution
// endpoint configured; the invocation fails closed before any ledger row or
// budget consumption is opened.
var ErrNoProviderExecution = errors.New("model provider has no execution endpoint configured")

// InvokeInput is one fenced real model invocation.
type InvokeInput struct {
	TenantID        string
	TaskID          uuid.UUID
	RunID           uuid.UUID
	AttemptID       uuid.UUID
	FencingToken    int64
	AgentVersionRef string
	ModelRef        string
	IdempotencyKey  string

	Messages        []provider.Message
	Temperature     *float64
	MaxOutputTokens int32
	Stream          bool
	// Tools are the platform-described tool contracts the model may call.
	// Callers resolve them from the AgentVersion's capability grants; the
	// invoker never invents or widens them.
	Tools []provider.ToolDefinition
}

// InvokeOutput carries the terminal ledger row and the completion content.
type InvokeOutput struct {
	Call      store.ModelCall
	Content   string
	ToolCalls []provider.ToolCall
}

// midStreamSettleTokens is the estimated-token granularity of the hard-stop
// guard during streaming: accumulated output is settled in increments so a
// runaway stream trips the budget ceiling before Finish. The final Finish
// settlement corrects the estimate to the provider's exact usage.
const midStreamSettleTokens = 128

// Invoker executes real model calls behind the Model Gateway decision chain.
type Invoker struct {
	gateway   *Gateway
	providers *provider.Registry
	now       func() time.Time
}

// NewInvoker binds the decision gateway to a provider registry. A nil
// registry fails every invocation closed (governance without execution).
func NewInvoker(gateway *Gateway, providers *provider.Registry) *Invoker {
	if providers == nil {
		providers = provider.NewRegistry()
	}
	return &Invoker{gateway: gateway, providers: providers, now: time.Now}
}

// InvokeStream performs one invocation with streaming intent: content deltas
// are delivered to onDelta as they arrive (nil disables delivery). A
// descriptor that declares no streaming support transparently falls back to
// the non-streaming wire call. When the budget ceiling trips mid-stream the
// provider call is cancelled, the ledger row finishes STOPPED, and
// ErrBudgetExhausted is returned.
func (inv *Invoker) InvokeStream(ctx context.Context, in InvokeInput, onDelta func(string)) (InvokeOutput, error) {
	in.Stream = true
	return inv.invoke(ctx, in, onDelta)
}

// Invoke performs one non-streaming invocation.
func (inv *Invoker) Invoke(ctx context.Context, in InvokeInput) (InvokeOutput, error) {
	in.Stream = false
	return inv.invoke(ctx, in, nil)
}

func (inv *Invoker) invoke(ctx context.Context, in InvokeInput, onDelta func(string)) (InvokeOutput, error) {
	if in.MaxOutputTokens < 0 {
		return InvokeOutput{}, fmt.Errorf("max output tokens must not be negative")
	}
	providerName, _, _ := strings.Cut(in.ModelRef, "/")
	executor, wireModel, err := inv.providers.ResolveModel(in.ModelRef)
	if err != nil {
		return InvokeOutput{}, fmt.Errorf("%w: %s", ErrNoProviderExecution, providerName)
	}
	estimator, estimatorErr := tokens.ForName(executor.Tokenizer())
	if estimatorErr != nil {
		// An unresolvable tokenizer name fails safe to the conservative
		// estimator instead of blocking execution.
		estimator = tokens.Conservative
	}

	begin, err := inv.gateway.Begin(ctx, BeginInput{
		TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
		FencingToken: in.FencingToken, AgentVersionRef: in.AgentVersionRef,
		ModelRef: in.ModelRef, IdempotencyKey: in.IdempotencyKey,
		EstimatedInputTokens: estimateInputTokens(in.Messages, in.Tools, estimator), MaxOutputTokens: int64(in.MaxOutputTokens),
	})
	if err != nil {
		return InvokeOutput{}, err
	}
	in.MaxOutputTokens = int32(begin.EffectiveMaxOutputTokens)
	call := begin.Call

	// The hard-stop guard: stream deltas are settled as estimated increments
	// so the ceiling trips without waiting for the final usage report. The
	// same provider tokenizer drives the estimate as the input reservation.
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	sequence := int64(0)
	pendingTokens := int64(0)
	settledEstimate := int64(0)
	var exhausted error
	guardedGenerated := func(delta string) {
		if exhausted != nil {
			return
		}
		pendingTokens += estimator(delta)
		increment := pendingTokens - settledEstimate
		if increment < midStreamSettleTokens {
			return
		}
		sequence++
		settledEstimate = pendingTokens
		if err := inv.gateway.Settle(streamCtx, call, sequence, Usage{OutputTokens: increment}); err != nil {
			if errors.Is(err, ErrBudgetExhausted) {
				exhausted = err
				cancelStream()
				return
			}
			// Settlement infrastructure failures must not silently unbound
			// the stream; treat them as fatal too.
			exhausted = err
			cancelStream()
		}
	}

	invocation := provider.Invocation{
		ModelName: wireModel, Messages: in.Messages,
		Temperature: in.Temperature, MaxOutputTokens: in.MaxOutputTokens, Stream: in.Stream,
		Tools:          in.Tools,
		IdempotencyKey: in.IdempotencyKey, TaskID: in.TaskID.String(), AttemptID: in.AttemptID.String(),
		ModelCallID: call.ID.String(),
	}
	var result provider.Result
	if in.Stream && !begin.Descriptor.SupportsStreaming {
		// The tenant descriptor declares the capability; honor it.
		result, err = executor.Complete(streamCtx, invocation)
	} else if in.Stream {
		// Time-to-first-token is measured here, at the kernel invoker — the
		// innermost layer that observes provider deltas — so it is recorded even
		// when no downstream consumer is attached (onDelta nil, the current MCP
		// broker seam). This makes streaming first-token latency measurable
		// today; continuous delta delivery to the client is deferred transport
		// work (decision P1-03 in the internal remediation decision log, 2026-08-23).
		streamStart := inv.now()
		firstToken := true
		observedContent := func(delta string) {
			if firstToken {
				firstToken = false
				agentmetrics.ModelFirstTokenLatency(ctx, providerName,
					float64(inv.now().Sub(streamStart).Microseconds())/1000.0)
			}
			if onDelta != nil {
				onDelta(delta)
			}
		}
		result, err = executor.StreamObserved(streamCtx, invocation, provider.StreamObserver{
			OnContent: observedContent, OnGenerated: guardedGenerated,
		})
	} else {
		result, err = executor.Complete(streamCtx, invocation)
	}
	if exhausted != nil {
		return inv.finishGuarded(ctx, call, result, exhausted, executor.Name(), wireModel)
	}
	if err != nil {
		// Provider failure: definitive rejection/overload is known zero usage;
		// transport loss, 5xx, and partial streams are explicitly unknown and
		// enter reconciliation instead of being silently treated as free.
		finishReason := "provider_error"
		if finish := providerFinishReason(err); finish != "" {
			finishReason = finish
		}
		usageCertainty := store.ModelUsageUnknown
		if provider.UsageKnownZero(err) {
			usageCertainty = store.ModelUsageKnownZero
		}
		failed, finishErr := inv.gateway.Finish(ctx, call, FinishInput{
			TenantID: call.TenantID, ModelCallID: call.ID, ExpectedVersion: call.ResourceVersion,
			Status: store.ModelCallFailed, ProviderRequestID: result.ProviderRequestID,
			FinishReason: finishReason, UsageCertainty: usageCertainty,
			ProviderName: executor.Name(), WireModel: wireModel,
		})
		if finishErr != nil && !errors.Is(finishErr, ErrBudgetExhausted) {
			return InvokeOutput{}, errors.Join(err, finishErr)
		}
		return InvokeOutput{Call: failed}, err
	}

	usageCertainty := store.ModelUsageUnknown
	if result.UsageReported {
		usageCertainty = store.ModelUsageKnownZero
		if result.InputTokens+result.OutputTokens > 0 {
			usageCertainty = store.ModelUsageKnown
		}
	}
	finished, err := inv.gateway.Finish(ctx, call, FinishInput{
		TenantID: call.TenantID, ModelCallID: call.ID, ExpectedVersion: call.ResourceVersion,
		Status: store.ModelCallCompleted, InputTokens: result.InputTokens, OutputTokens: result.OutputTokens,
		ProviderRequestID: result.ProviderRequestID, FinishReason: result.FinishReason,
		UsageCertainty: usageCertainty,
		ProviderName:   executor.Name(), WireModel: wireModel,
	})
	output := InvokeOutput{Call: finished, Content: result.Content, ToolCalls: result.ToolCalls}
	if err != nil {
		return output, err
	}
	return output, nil
}

// estimateInputTokens estimates the reservation envelope for a prompt
// (including the serialized tool contracts, which the provider counts as
// input tokens) using the provider's tokenizer-aware estimator. It never
// under-counts the legacy bytes/4 floor and errs high: the provider's
// reported usage at Finish settles the exact consumption.
func estimateInputTokens(messages []provider.Message, tools []provider.ToolDefinition, estimator tokens.Estimator) int64 {
	var estimated int64
	for _, message := range messages {
		estimated += estimator(message.Role) + estimator(message.Content) + estimator(message.ToolCallID)
		for _, call := range message.ToolCalls {
			estimated += estimator(call.ID) + estimator(call.Name) + estimator(call.Arguments)
		}
	}
	for _, tool := range tools {
		estimated += estimator(tool.Name) + estimator(tool.Description) + estimator(string(tool.Parameters))
	}
	return estimated
}

// finishGuarded closes a stream that the budget guard cancelled: exact usage
// is unknown, so the row finishes STOPPED with the provider-reported usage if
// any arrived before cancellation.
func (inv *Invoker) finishGuarded(ctx context.Context, call store.ModelCall, result provider.Result, cause error, providerName, wireModel string) (InvokeOutput, error) {
	usageCertainty := store.ModelUsageUnknown
	if result.UsageReported {
		usageCertainty = store.ModelUsageKnownZero
		if result.InputTokens+result.OutputTokens > 0 {
			usageCertainty = store.ModelUsageKnown
		}
	}
	finished, err := inv.gateway.Finish(ctx, call, FinishInput{
		TenantID: call.TenantID, ModelCallID: call.ID, ExpectedVersion: call.ResourceVersion,
		Status: store.ModelCallStopped, InputTokens: result.InputTokens, OutputTokens: result.OutputTokens,
		ProviderRequestID: result.ProviderRequestID, FinishReason: "budget_guard",
		UsageCertainty: usageCertainty,
		ProviderName:   providerName, WireModel: wireModel,
	})
	if err != nil && !errors.Is(err, ErrBudgetExhausted) {
		return InvokeOutput{}, errors.Join(cause, err)
	}
	return InvokeOutput{Call: finished}, cause
}

// providerFinishReason maps executor error classes onto bounded ledger
// finish-reason strings.
func providerFinishReason(err error) string {
	switch {
	case errors.Is(err, provider.ErrProviderUnavailable):
		return "provider_unavailable"
	case errors.Is(err, provider.ErrProviderRejected):
		return "provider_rejected"
	case errors.Is(err, provider.ErrStreamAborted):
		return "provider_stream_aborted"
	default:
		return ""
	}
}
