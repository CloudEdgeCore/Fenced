// Package model implements the Model Gateway decision chain of the Agent OS
// kernel: versioned model descriptors with price tables, policy checks,
// incremental token/cost settlement against the task budget ledger, and the
// hard stop when a reservation is exhausted (tech baseline §12.1; invariant
// 12). Prompt and completion content never enters kernel telemetry.
package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

var (
	// ErrModelDenied reports a model reference outside the tenant's policy.
	ErrModelDenied = errors.New("model is not allowed by tenant policy")
	// ErrBudgetExhausted is the hard-stop outcome: the task budget is
	// exhausted and new consumption must stop (invariant 12).
	ErrBudgetExhausted = errors.New("task budget exhausted: new consumption stopped")
)

// defaultMaxOutputTokens is the bounded provider limit used when a caller
// omits max_tokens. The gateway further reduces it to the task's currently
// affordable token and cost headroom before opening the call.
const defaultMaxOutputTokens int64 = 512

// PolicyChecker decides model calls outside the LLM. The engine is
// fail-closed: missing tenant data and evaluation errors deny by default.
type PolicyChecker interface {
	EvaluateModel(context.Context, string, policy.ModelContext) policy.Decision
}

// ModelStore is the model registry and call ledger. The postgres Store
// satisfies it.
type ModelStore interface {
	GetModelDescriptor(context.Context, string, string, string) (store.ModelDescriptor, error)
	CreateModelCall(context.Context, store.CreateModelCallInput) (store.CreateModelCallResult, error)
	GetModelCall(context.Context, string, uuid.UUID) (store.ModelCall, error)
	FinishModelCall(context.Context, store.FinishModelCallInput) (store.ModelCall, error)
}

// BeginInput starts one fenced model invocation.
type BeginInput struct {
	TenantID        string
	TaskID          uuid.UUID
	RunID           uuid.UUID
	AttemptID       uuid.UUID
	FencingToken    int64
	AgentVersionRef string
	// ModelRef is the canonical provider/model reference.
	ModelRef       string
	IdempotencyKey string
	// EstimatedInputTokens and MaxOutputTokens define the worst-case call
	// envelope reserved before the provider request is sent. A zero output
	// limit asks the gateway to select a budget-aware bounded default.
	EstimatedInputTokens int64
	MaxOutputTokens      int64
}

func (in BeginInput) validate() error {
	if strings.TrimSpace(in.TenantID) == "" || in.TaskID == uuid.Nil || in.RunID == uuid.Nil ||
		in.AttemptID == uuid.Nil || strings.TrimSpace(in.AgentVersionRef) == "" ||
		strings.TrimSpace(in.IdempotencyKey) == "" {
		return fmt.Errorf("tenant, task, run, attempt, agent version, and idempotency key are required")
	}
	if in.EstimatedInputTokens < 0 || in.MaxOutputTokens < 0 {
		return fmt.Errorf("estimated input and maximum output tokens must not be negative")
	}
	return store.ValidateModelRef(in.ModelRef)
}

// BeginResult is the outcome of starting an invocation.
type BeginResult struct {
	Call                     store.ModelCall
	Descriptor               store.ModelDescriptor
	EffectiveMaxOutputTokens int64
	PolicyRevision           string
	DenyReasons              []string
}

// Usage is one settlement step of a streaming call.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

func (u Usage) valid() bool { return u.InputTokens >= 0 && u.OutputTokens >= 0 }

// Gateway orchestrates the model call lifecycle: Begin (policy + budget
// preflight + ledger row), Settle (idempotent incremental settlement with the
// ledger enforcing the hard stop), Finish (final settlement, terminal state
// and durable audit receipt). Every settlement carries its own idempotency
// key derived from the call and sequence, so retries never double-charge.
type Gateway struct {
	policy   PolicyChecker
	models   ModelStore
	budget   store.BudgetStore
	receipts ReceiptStore
	now      func() time.Time
	newID    func() uuid.UUID
}

// ReceiptStore persists the model call outcome for audit. Content is never
// stored — only metadata.
type ReceiptStore interface {
	WriteRuntimeReceipt(context.Context, store.WriteRuntimeReceiptInput) error
}

func NewGateway(policyChecker PolicyChecker, models ModelStore, budget store.BudgetStore, receipts ReceiptStore) *Gateway {
	return &Gateway{
		policy: policyChecker, models: models, budget: budget, receipts: receipts,
		now: time.Now, newID: func() uuid.UUID {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.New()
			}
			return id
		},
	}
}

// GetModelCall reads one ledger row for fenced transports.
func (g *Gateway) GetModelCall(ctx context.Context, tenantID string, callID uuid.UUID) (store.ModelCall, error) {
	return g.models.GetModelCall(ctx, tenantID, callID)
}

// Begin validates policy and budget headroom and opens the call ledger row.
func (g *Gateway) Begin(ctx context.Context, in BeginInput) (BeginResult, error) {
	var result BeginResult
	if err := in.validate(); err != nil {
		return result, err
	}
	provider, modelName, ok := strings.Cut(in.ModelRef, "/")
	if !ok {
		return result, fmt.Errorf("model reference must be provider/model")
	}
	descriptor, err := g.models.GetModelDescriptor(ctx, in.TenantID, provider, modelName)
	if err != nil {
		return result, fmt.Errorf("resolve model %s: %w", in.ModelRef, err)
	}
	decision := g.policy.EvaluateModel(ctx, in.TenantID, policy.ModelContext{Name: descriptor.Ref()})
	if !decision.Allow {
		return result, fmt.Errorf("%w: %v", ErrModelDenied, decision.DenyReasons)
	}
	result.Descriptor, result.PolicyRevision = descriptor, policy.Revision
	result.DenyReasons = decision.DenyReasons

	effectiveMax, err := g.effectiveMaxOutputTokens(ctx, in, descriptor)
	if err != nil {
		return result, err
	}
	result.EffectiveMaxOutputTokens = effectiveMax
	in.MaxOutputTokens = effectiveMax
	reservationKey := modelReservationKey(in.AttemptID, in.ModelRef, in.IdempotencyKey)
	worstCaseCost, err := costMicroUSD(descriptor, in.EstimatedInputTokens, in.MaxOutputTokens)
	if err != nil {
		return result, fmt.Errorf("calculate model reservation: %w", err)
	}
	reservation := store.TaskBudget{
		Tokens:       in.EstimatedInputTokens + in.MaxOutputTokens,
		CostMicroUSD: worstCaseCost,
	}
	if !reservation.Zero() {
		if err := g.budget.ReserveTaskUsage(ctx, store.ReserveTaskUsageInput{
			TenantID: in.TenantID, TaskID: in.TaskID, ReservationKey: reservationKey,
			Amount: reservation, ExpiresAt: g.now().UTC().Add(10 * time.Minute),
		}); err != nil && !errors.Is(err, store.ErrBudgetNotReserved) {
			if errors.Is(err, store.ErrBudgetExceeded) {
				return result, ErrBudgetExhausted
			}
			return result, fmt.Errorf("reserve model usage: %w", err)
		}
	}
	created, err := g.models.CreateModelCall(ctx, store.CreateModelCallInput{
		ID: g.newID(), TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID,
		AttemptID: in.AttemptID, ModelRef: descriptor.Ref(), PriceRevision: descriptor.PriceRevision,
		IdempotencyKey: in.IdempotencyKey,
	})
	if err != nil {
		_ = g.budget.ReleaseTaskUsageReservation(ctx, in.TenantID, in.TaskID, reservationKey)
		return result, err
	}
	result.Call = created.ModelCall
	return result, nil
}

// effectiveMaxOutputTokens resolves an omitted provider limit against the
// task's remaining token and fixed-point cost ceilings. The result is only a
// preflight hint: ReserveTaskUsage remains the atomic serialization point and
// may reject concurrent headroom contention.
func (g *Gateway) effectiveMaxOutputTokens(ctx context.Context, in BeginInput, descriptor store.ModelDescriptor) (int64, error) {
	status, err := g.budget.GetTaskBudget(ctx, in.TenantID, in.TaskID)
	if err != nil {
		if errors.Is(err, store.ErrBudgetNotReserved) {
			if in.MaxOutputTokens > 0 {
				return in.MaxOutputTokens, nil
			}
			return defaultMaxOutputTokens, nil
		}
		return 0, fmt.Errorf("read task budget: %w", err)
	}
	if status.Exhausted {
		return 0, ErrBudgetExhausted
	}
	if in.MaxOutputTokens > 0 {
		return in.MaxOutputTokens, nil
	}

	limit := defaultMaxOutputTokens
	if status.Reserved.Tokens > 0 {
		remaining := status.Reserved.Tokens - status.Consumed.Tokens - in.EstimatedInputTokens
		if remaining <= 0 {
			return 0, ErrBudgetExhausted
		}
		if remaining < limit {
			limit = remaining
		}
	}
	if status.Reserved.CostMicroUSD > 0 {
		remaining := status.Reserved.CostMicroUSD - status.Consumed.CostMicroUSD
		inputCost, costErr := money.TokenCost(in.EstimatedInputTokens, descriptor.InputPriceMicroUSDPerMillion)
		if costErr != nil {
			return 0, fmt.Errorf("calculate input reservation: %w", costErr)
		}
		remaining -= inputCost
		if remaining < 0 || (remaining == 0 && descriptor.OutputPriceMicroUSDPerMillion > 0) {
			return 0, ErrBudgetExhausted
		}
		// Find the largest bounded output whose rounded-up fixed-point charge
		// fits. This avoids floating-point under-reservation at price edges.
		low, high := int64(0), limit
		for low < high {
			mid := low + (high-low+1)/2
			cost, costErr := money.TokenCost(mid, descriptor.OutputPriceMicroUSDPerMillion)
			if costErr != nil {
				return 0, fmt.Errorf("calculate output reservation: %w", costErr)
			}
			if cost <= remaining {
				low = mid
			} else {
				high = mid - 1
			}
		}
		limit = low
	}
	if limit <= 0 {
		return 0, ErrBudgetExhausted
	}
	return limit, nil
}

// Settle appends one idempotent usage step to the ledger. A settlement that
// would exceed the reservation is rejected and the ledger is marked
// exhausted: the caller must stop the stream within bounded time. A task
// without a reservation carries no ceiling and is not gated (mirroring the
// Tool Gateway).
func (g *Gateway) Settle(ctx context.Context, call store.ModelCall, sequence int64, usage Usage) error {
	if !usage.valid() {
		return fmt.Errorf("usage must not be negative")
	}
	if sequence < 1 {
		return fmt.Errorf("settlement sequence must be positive")
	}
	_, err := g.budget.SettleTaskUsage(ctx, store.SettleTaskUsageInput{
		TenantID: call.TenantID, TaskID: call.TaskID,
		IdempotencyKey: fmt.Sprintf("model:%s:%d", call.ID, sequence),
		Usage:          store.TaskBudget{Tokens: usage.InputTokens + usage.OutputTokens},
		ReservationKey: modelReservationKey(call.AttemptID, call.ModelRef, call.IdempotencyKey),
	})
	if err != nil {
		if errors.Is(err, store.ErrBudgetNotReserved) {
			return nil
		}
		if errors.Is(err, store.ErrBudgetExceeded) {
			return ErrBudgetExhausted
		}
		return fmt.Errorf("settle model usage: %w", err)
	}
	return nil
}

// FinishInput finalizes a model invocation. Usage is reported by the caller;
// cost is computed by the gateway from the descriptor's price table (pinned
// price revision), never trusted from the caller.
type FinishInput struct {
	TenantID          string
	ModelCallID       uuid.UUID
	ExpectedVersion   int64
	Status            store.ModelCallStatus
	InputTokens       int64
	OutputTokens      int64
	ProviderRequestID string
	FinishReason      string
	UsageCertainty    store.ModelUsageCertainty
	// ProviderName and WireModel are the physical deployment identity the
	// logical modelRef actually resolved to (P2-10). They are recorded in the
	// audit receipt so a call is reproducible against the exact provider route
	// that served it, distinct from the tenant-visible logical reference.
	// ProviderName is a name only — endpoints/credentials never enter a receipt.
	ProviderName string
	WireModel    string
}

// Finish finalizes the invocation: the final usage is settled idempotently,
// cost is computed from the pinned price table, the ledger row reaches its
// terminal state, and an audit receipt records the metadata (never content).
func (g *Gateway) Finish(ctx context.Context, call store.ModelCall, in FinishInput) (store.ModelCall, error) {
	if in.ModelCallID != call.ID || in.TenantID != call.TenantID {
		return store.ModelCall{}, fmt.Errorf("finish input does not match the call")
	}
	if !in.Status.Terminal() {
		return store.ModelCall{}, fmt.Errorf("finish status must be terminal")
	}
	if in.InputTokens < 0 || in.OutputTokens < 0 {
		return store.ModelCall{}, fmt.Errorf("usage must not be negative")
	}
	if in.UsageCertainty == "" {
		if in.InputTokens+in.OutputTokens > 0 {
			in.UsageCertainty = store.ModelUsageKnown
		} else if in.Status == store.ModelCallCompleted {
			in.UsageCertainty = store.ModelUsageKnownZero
		} else {
			in.UsageCertainty = store.ModelUsageUnknown
		}
	}
	if !in.UsageCertainty.Valid() {
		return store.ModelCall{}, fmt.Errorf("usage certainty is invalid")
	}
	total := in.InputTokens + in.OutputTokens
	if (in.UsageCertainty == store.ModelUsageKnownZero && total != 0) ||
		(in.UsageCertainty == store.ModelUsageKnown && total == 0) ||
		(in.UsageCertainty == store.ModelUsageUnknown && total != 0) {
		return store.ModelCall{}, fmt.Errorf("usage certainty does not match reported usage")
	}
	provider, modelName, ok := strings.Cut(call.ModelRef, "/")
	if !ok {
		return store.ModelCall{}, fmt.Errorf("call model reference is malformed")
	}
	descriptor, err := g.models.GetModelDescriptor(ctx, call.TenantID, provider, modelName)
	if err != nil {
		return store.ModelCall{}, fmt.Errorf("resolve model %s: %w", call.ModelRef, err)
	}
	status := in.Status
	if total > 0 && in.UsageCertainty == store.ModelUsageKnown {
		// Exact settlement: the final usage is the cumulative target of this
		// call's settlement family (model:<callID>:*), so the gateway charges
		// only the remainder after the per-step Settle records — a stream
		// that settled steps and finalizes with the cumulative total is
		// charged exactly once, and a retried Finish after a crash converges.
		settlementCost, costErr := costMicroUSD(descriptor, in.InputTokens, in.OutputTokens)
		if costErr != nil {
			return store.ModelCall{}, fmt.Errorf("calculate final model usage: %w", costErr)
		}
		if _, err := g.budget.SettleTaskUsageDelta(ctx, store.SettleTaskUsageDeltaInput{
			TenantID: call.TenantID, TaskID: call.TaskID,
			FamilyPrefix: "model:" + call.ID.String(), Target: store.TaskBudget{
				Tokens: total, CostMicroUSD: settlementCost,
			},
			IdempotencyKey: fmt.Sprintf("model:%s:finish", call.ID),
			ReservationKey: modelReservationKey(call.AttemptID, call.ModelRef, call.IdempotencyKey),
		}); err != nil {
			if errors.Is(err, store.ErrBudgetNotReserved) {
				// No reservation: no ceiling, nothing to meter.
			} else if errors.Is(err, store.ErrBudgetExceeded) {
				status = store.ModelCallStopped
			} else {
				return store.ModelCall{}, fmt.Errorf("settle final model usage: %w", err)
			}
		}
	}
	cost, err := costMicroUSD(descriptor, in.InputTokens, in.OutputTokens)
	if err != nil {
		return store.ModelCall{}, fmt.Errorf("calculate model cost: %w", err)
	}
	updated, err := g.models.FinishModelCall(ctx, store.FinishModelCallInput{
		TenantID: call.TenantID, ModelCallID: call.ID, ExpectedVersion: in.ExpectedVersion,
		Status: status, InputTokens: in.InputTokens, OutputTokens: in.OutputTokens,
		CostMicroUSD: cost, PriceRevision: descriptor.PriceRevision,
		ProviderRequestID: in.ProviderRequestID, FinishReason: in.FinishReason,
		UsageCertainty: in.UsageCertainty,
	})
	if err != nil {
		return store.ModelCall{}, err
	}
	if err := g.budget.ReleaseTaskUsageReservation(ctx, call.TenantID, call.TaskID,
		modelReservationKey(call.AttemptID, call.ModelRef, call.IdempotencyKey)); err != nil {
		return store.ModelCall{}, fmt.Errorf("release model usage reservation: %w", err)
	}
	receiptFields := map[string]any{
		"modelRef": updated.ModelRef, "status": updated.Status,
		"inputTokens": updated.InputTokens, "outputTokens": updated.OutputTokens,
		"costUsd": updated.CostMicroUSD.USD(), "costMicroUsd": updated.CostMicroUSD,
		"priceRevision": updated.PriceRevision,
		"finishReason":  updated.FinishReason, "providerRequestId": updated.ProviderRequestID,
		"usageCertainty": updated.UsageCertainty,
	}
	// P2-10: pin the physical deployment identity the logical modelRef resolved
	// to, plus a content-addressed route revision. A route repoint (same logical
	// ref, different provider/wire model) changes the revision, so audit makes
	// the reproducibility boundary explicit. Names only — never endpoints/keys.
	if in.ProviderName != "" || in.WireModel != "" {
		receiptFields["providerDeployment"] = map[string]any{
			"provider": in.ProviderName, "wireModel": in.WireModel,
		}
		routeDigest := sha256.Sum256([]byte(updated.ModelRef + "|" + in.ProviderName + "|" + in.WireModel))
		receiptFields["routeRevision"] = hex.EncodeToString(routeDigest[:])[:16]
	}
	receipt, err := json.Marshal(receiptFields)
	if err != nil {
		return store.ModelCall{}, err
	}
	if err := g.receipts.WriteRuntimeReceipt(ctx, store.WriteRuntimeReceiptInput{
		TenantID: call.TenantID, AttemptID: call.AttemptID,
		Operation: "MODEL:" + updated.ModelRef, IdempotencyKey: fmt.Sprintf("model:%s:finish", call.ID),
		RequestHash: call.RequestHash, Response: receipt,
	}); err != nil {
		return store.ModelCall{}, fmt.Errorf("write model receipt: %w", err)
	}
	if status == store.ModelCallStopped {
		return updated, ErrBudgetExhausted
	}
	return updated, nil
}

func costMicroUSD(descriptor store.ModelDescriptor, inputTokens, outputTokens int64) (money.MicroUSD, error) {
	inputCost, err := money.TokenCost(inputTokens, descriptor.InputPriceMicroUSDPerMillion)
	if err != nil {
		return 0, err
	}
	outputCost, err := money.TokenCost(outputTokens, descriptor.OutputPriceMicroUSDPerMillion)
	if err != nil {
		return 0, err
	}
	return money.Add(inputCost, outputCost)
}

func modelReservationKey(attemptID uuid.UUID, modelRef, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(attemptID.String() + "\x00" + modelRef + "\x00" + idempotencyKey))
	return "model:" + hex.EncodeToString(digest[:])
}
