package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/google/uuid"
)

var (
	// ErrModelNotFound reports a model descriptor that is not registered in
	// the tenant's registry.
	ErrModelNotFound = errors.New("model descriptor not found")
	// ErrModelSpecConflict reports a registration that reuses an existing
	// (tenant, provider, model) identity with a different spec.
	ErrModelSpecConflict = errors.New("model identity already registered with a different spec")
	// ErrModelCallTerminal reports usage settlement or finishing on a call
	// that already reached a terminal state.
	ErrModelCallTerminal = errors.New("model call already reached a terminal state")
)

// ModelCallStatus is the lifecycle of one model invocation.
type ModelCallStatus string
type ModelUsageCertainty string

const (
	ModelCallStarted   ModelCallStatus = "STARTED"
	ModelCallCompleted ModelCallStatus = "COMPLETED"
	ModelCallFailed    ModelCallStatus = "FAILED"
	ModelCallStopped   ModelCallStatus = "STOPPED"
)

const (
	ModelUsageKnownZero ModelUsageCertainty = "KNOWN_ZERO_USAGE"
	ModelUsageKnown     ModelUsageCertainty = "KNOWN_USAGE"
	ModelUsageUnknown   ModelUsageCertainty = "UNKNOWN_USAGE"
)

func (c ModelUsageCertainty) Valid() bool {
	return c == ModelUsageKnownZero || c == ModelUsageKnown || c == ModelUsageUnknown
}

func (s ModelCallStatus) Terminal() bool {
	switch s {
	case ModelCallCompleted, ModelCallFailed, ModelCallStopped:
		return true
	default:
		return false
	}
}

var modelTokenPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._/-]{0,127})$`)

// ValidateModelRef rejects model references that are not safe canonical
// provider/model tokens.
func ValidateModelRef(ref string) error {
	if !modelTokenPattern.MatchString(ref) {
		return fmt.Errorf("model reference must match %s", modelTokenPattern)
	}
	return nil
}

// ModelDescriptor is a versioned declaration of a model capability and its
// price table. It is immutable per (tenant, provider, model): a price change
// registers a new price revision under the same identity.
type ModelDescriptor struct {
	ID                            uuid.UUID
	TenantID                      string
	Provider                      string
	ModelName                     string
	SupportsStreaming             bool
	InputPriceMicroUSDPerMillion  money.MicroUSD
	OutputPriceMicroUSDPerMillion money.MicroUSD
	PriceRevision                 string
	SpecHash                      [sha256.Size]byte
	CreatedAt                     time.Time
}

// Ref returns the canonical provider/model reference.
func (d ModelDescriptor) Ref() string { return d.Provider + "/" + d.ModelName }

func (d ModelDescriptor) Validate() error {
	if strings.TrimSpace(d.TenantID) == "" || strings.TrimSpace(d.Provider) == "" ||
		strings.TrimSpace(d.ModelName) == "" || strings.TrimSpace(d.PriceRevision) == "" {
		return fmt.Errorf("tenant, provider, model name, and price revision are required")
	}
	if err := ValidateModelRef(d.Ref()); err != nil {
		return err
	}
	if d.InputPriceMicroUSDPerMillion < 0 || d.OutputPriceMicroUSDPerMillion < 0 {
		return fmt.Errorf("prices must not be negative")
	}
	return nil
}

type RegisterModelDescriptorInput struct {
	TenantID                      string
	Provider                      string
	ModelName                     string
	SupportsStreaming             bool
	InputPriceMicroUSDPerMillion  money.MicroUSD
	OutputPriceMicroUSDPerMillion money.MicroUSD
	PriceRevision                 string
}

func (in RegisterModelDescriptorInput) ValidateAndHash() (ModelDescriptor, [sha256.Size]byte, error) {
	descriptor := ModelDescriptor{
		TenantID: in.TenantID, Provider: in.Provider, ModelName: in.ModelName,
		SupportsStreaming:             in.SupportsStreaming,
		InputPriceMicroUSDPerMillion:  in.InputPriceMicroUSDPerMillion,
		OutputPriceMicroUSDPerMillion: in.OutputPriceMicroUSDPerMillion,
		PriceRevision:                 in.PriceRevision,
	}
	if err := descriptor.Validate(); err != nil {
		return descriptor, [sha256.Size]byte{}, err
	}
	encoded, err := json.Marshal(struct {
		Provider                      string         `json:"provider"`
		ModelName                     string         `json:"modelName"`
		SupportsStreaming             bool           `json:"supportsStreaming"`
		InputPriceMicroUSDPerMillion  money.MicroUSD `json:"inputPriceMicroUsdPerMillion"`
		OutputPriceMicroUSDPerMillion money.MicroUSD `json:"outputPriceMicroUsdPerMillion"`
		PriceRevision                 string         `json:"priceRevision"`
	}{descriptor.Provider, descriptor.ModelName, descriptor.SupportsStreaming,
		descriptor.InputPriceMicroUSDPerMillion, descriptor.OutputPriceMicroUSDPerMillion, descriptor.PriceRevision})
	if err != nil {
		return descriptor, [sha256.Size]byte{}, fmt.Errorf("encode model descriptor: %w", err)
	}
	return descriptor, sha256.Sum256(encoded), nil
}

// ModelCall is the durable ledger row of one model invocation. Usage and cost
// are accumulated at settlement; content is never stored (prompt/completion
// stay out of kernel telemetry by policy).
type ModelCall struct {
	ID                uuid.UUID
	TenantID          string
	TaskID            uuid.UUID
	RunID             uuid.UUID
	AttemptID         uuid.UUID
	ModelRef          string
	Status            ModelCallStatus
	IdempotencyKey    string
	RequestHash       [sha256.Size]byte
	InputTokens       int64
	OutputTokens      int64
	CostMicroUSD      money.MicroUSD
	PriceRevision     string
	ProviderRequestID string
	FinishReason      string
	UsageCertainty    ModelUsageCertainty
	ResourceVersion   int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type CreateModelCallInput struct {
	ID        uuid.UUID
	TenantID  string
	TaskID    uuid.UUID
	RunID     uuid.UUID
	AttemptID uuid.UUID
	ModelRef  string
	// PriceRevision pins the price table the call is metered against.
	PriceRevision  string
	IdempotencyKey string
}

func (in CreateModelCallInput) RequestHash() ([sha256.Size]byte, error) {
	if strings.TrimSpace(in.TenantID) == "" || in.TaskID == uuid.Nil || in.RunID == uuid.Nil || in.AttemptID == uuid.Nil ||
		strings.TrimSpace(in.ModelRef) == "" || strings.TrimSpace(in.PriceRevision) == "" || strings.TrimSpace(in.IdempotencyKey) == "" {
		return [sha256.Size]byte{}, fmt.Errorf("tenant, task, run, attempt, model reference, price revision, and idempotency key are required")
	}
	encoded, err := json.Marshal(struct {
		AttemptID      string `json:"attemptId"`
		ModelRef       string `json:"modelRef"`
		PriceRevision  string `json:"priceRevision"`
		IdempotencyKey string `json:"idempotencyKey"`
	}{in.AttemptID.String(), in.ModelRef, in.PriceRevision, in.IdempotencyKey})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode model call request: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

type CreateModelCallResult struct {
	ModelCall ModelCall
	Existing  bool
}

// FinishModelCallInput finalizes one model invocation with the final usage.
type FinishModelCallInput struct {
	TenantID          string
	ModelCallID       uuid.UUID
	ExpectedVersion   int64
	Status            ModelCallStatus
	InputTokens       int64
	OutputTokens      int64
	CostMicroUSD      money.MicroUSD
	PriceRevision     string
	ProviderRequestID string
	FinishReason      string
	UsageCertainty    ModelUsageCertainty
}

// CanTransitionModelCall defines the model-call state machine.
func CanTransitionModelCall(from, to ModelCallStatus) bool {
	if from == ModelCallStarted {
		return to == ModelCallCompleted || to == ModelCallFailed || to == ModelCallStopped
	}
	return false
}

type ModelStore interface {
	RegisterModelDescriptor(context.Context, RegisterModelDescriptorInput) (ModelDescriptor, error)
	GetModelDescriptor(context.Context, string, string, string) (ModelDescriptor, error)
	CreateModelCall(context.Context, CreateModelCallInput) (CreateModelCallResult, error)
	GetModelCall(context.Context, string, uuid.UUID) (ModelCall, error)
	FinishModelCall(context.Context, FinishModelCallInput) (ModelCall, error)
}
