package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrToolNotFound reports a tool descriptor that is not registered in the
	// tenant's registry.
	ErrToolNotFound = errors.New("tool descriptor not found")
	// ErrToolSpecConflict reports a registration that reuses an existing
	// (tenant, name, version) identity with a different spec.
	ErrToolSpecConflict = errors.New("tool version identity already registered with a different spec")
	// ErrApprovalNotUsable reports an approval that cannot authorize an
	// invocation: wrong binding, expired, or not approved.
	ErrApprovalNotUsable = errors.New("approval cannot authorize this invocation")
)

// ToolRisk classifies the side-effect surface of a tool. High-risk tools
// require human approval before execution.
type ToolRisk string

const (
	ToolRiskNone ToolRisk = "none"
	ToolRiskLow  ToolRisk = "low"
	ToolRiskHigh ToolRisk = "high"
)

func (r ToolRisk) Valid() bool {
	switch r {
	case ToolRiskNone, ToolRiskLow, ToolRiskHigh:
		return true
	default:
		return false
	}
}

// ToolCallStatus is the decision ledger status of one invocation.
type ToolCallStatus string

const (
	ToolCallPending          ToolCallStatus = "PENDING"
	ToolCallRequiresApproval ToolCallStatus = "REQUIRES_APPROVAL"
	ToolCallApproved         ToolCallStatus = "APPROVED"
	ToolCallExecuted         ToolCallStatus = "EXECUTED"
	ToolCallDenied           ToolCallStatus = "DENIED"
	ToolCallFailed           ToolCallStatus = "FAILED"
)

// ToolApprovalStatus is the lifecycle of one approval request.
type ToolApprovalStatus string

const (
	ToolApprovalPending  ToolApprovalStatus = "PENDING"
	ToolApprovalApproved ToolApprovalStatus = "APPROVED"
	ToolApprovalRejected ToolApprovalStatus = "REJECTED"
	ToolApprovalExpired  ToolApprovalStatus = "EXPIRED"
)

var toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,127})$`)

// ValidateToolName rejects tool names that are not safe canonical tokens.
func ValidateToolName(name string) error {
	if !toolNamePattern.MatchString(name) {
		return fmt.Errorf("tool name must match %s", toolNamePattern)
	}
	return nil
}

// ToolDescriptor is a versioned declaration of an external capability. A
// descriptor is immutable per (tenant, name, version): a change is a new
// version.
type ToolDescriptor struct {
	ID               uuid.UUID
	TenantID         string
	Name             string
	Version          string
	SideEffectRisk   ToolRisk
	Actions          []string
	ResourcePatterns []string
	ParamsSchema     json.RawMessage
	SpecHash         [sha256.Size]byte
	CreatedAt        time.Time
}

// Validate checks the structural bounds of a descriptor. ParamsSchema must be
// valid JSON and bounded; semantic schema validation happens at invocation
// time in the tool domain.
func (d ToolDescriptor) Validate() error {
	if strings.TrimSpace(d.TenantID) == "" || strings.TrimSpace(d.Version) == "" {
		return fmt.Errorf("tenant and version are required")
	}
	if err := ValidateToolName(d.Name); err != nil {
		return err
	}
	if strings.HasPrefix(strings.ToLower(d.Name), "fenced.") {
		return fmt.Errorf("tool namespace fenced.* is reserved for kernel system tools")
	}
	if !d.SideEffectRisk.Valid() {
		return fmt.Errorf("side effect risk must be one of none, low, high")
	}
	if len(d.ParamsSchema) == 0 || len(d.ParamsSchema) > 1<<16 {
		return fmt.Errorf("params schema must be non-empty and bounded to 64 KiB")
	}
	if !json.Valid(d.ParamsSchema) {
		return fmt.Errorf("params schema must be valid JSON")
	}
	return nil
}

// ToolCall is the durable decision record of one invocation, bound to
// taskId + runId + attemptId + idempotencyKey (invariant 6).
type ToolCall struct {
	ID              uuid.UUID
	TenantID        string
	TaskID          uuid.UUID
	RunID           uuid.UUID
	AttemptID       uuid.UUID
	ToolName        string
	ToolVersion     string
	Action          string
	Resource        string
	ArgsHash        [sha256.Size]byte
	Status          ToolCallStatus
	DecisionReasons []string
	PolicyRevision  string
	ApprovalID      *uuid.UUID
	IdempotencyKey  string
	RequestHash     [sha256.Size]byte
	ResourceVersion int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ToolApproval is a human authorization bound to a canonical call summary.
type ToolApproval struct {
	ID              uuid.UUID
	TenantID        string
	CallID          uuid.UUID
	TaskID          uuid.UUID
	RunID           uuid.UUID
	AttemptID       uuid.UUID
	ToolName        string
	ToolVersion     string
	Action          string
	Resource        string
	ArgsHash        [sha256.Size]byte
	Status          ToolApprovalStatus
	RequestedAt     time.Time
	ExpiresAt       time.Time
	DecidedAt       *time.Time
	DecidedBy       string
	ResourceVersion int64
}

type RegisterToolDescriptorInput struct {
	TenantID         string
	Name             string
	Version          string
	SideEffectRisk   ToolRisk
	Actions          []string
	ResourcePatterns []string
	ParamsSchema     json.RawMessage
}

// ValidateAndHash returns the normalized descriptor document and its digest.
func (in RegisterToolDescriptorInput) ValidateAndHash() (ToolDescriptor, [sha256.Size]byte, error) {
	descriptor := ToolDescriptor{
		TenantID: in.TenantID, Name: in.Name, Version: in.Version, SideEffectRisk: in.SideEffectRisk,
		Actions: slices.Clone(in.Actions), ResourcePatterns: slices.Clone(in.ResourcePatterns),
		ParamsSchema: slices.Clone(in.ParamsSchema),
	}
	if err := descriptor.Validate(); err != nil {
		return descriptor, [sha256.Size]byte{}, err
	}
	encoded, err := json.Marshal(struct {
		Name             string          `json:"name"`
		Version          string          `json:"version"`
		SideEffectRisk   ToolRisk        `json:"sideEffectRisk"`
		Actions          []string        `json:"actions"`
		ResourcePatterns []string        `json:"resourcePatterns"`
		ParamsSchema     json.RawMessage `json:"paramsSchema"`
	}{descriptor.Name, descriptor.Version, descriptor.SideEffectRisk,
		descriptor.Actions, descriptor.ResourcePatterns, descriptor.ParamsSchema})
	if err != nil {
		return descriptor, [sha256.Size]byte{}, fmt.Errorf("encode tool descriptor: %w", err)
	}
	return descriptor, sha256.Sum256(encoded), nil
}

type CreateToolCallInput struct {
	ID             uuid.UUID
	TenantID       string
	TaskID         uuid.UUID
	RunID          uuid.UUID
	AttemptID      uuid.UUID
	ToolName       string
	ToolVersion    string
	Action         string
	Resource       string
	ArgsHash       [sha256.Size]byte
	IdempotencyKey string
}

// RequestHash is the canonical digest of the invocation intent, used to
// detect idempotency-key reuse with different semantics.
func (in CreateToolCallInput) RequestHash() ([sha256.Size]byte, error) {
	if strings.TrimSpace(in.TenantID) == "" || in.TaskID == uuid.Nil || in.RunID == uuid.Nil || in.AttemptID == uuid.Nil ||
		strings.TrimSpace(in.ToolName) == "" || strings.TrimSpace(in.ToolVersion) == "" ||
		strings.TrimSpace(in.Action) == "" || strings.TrimSpace(in.Resource) == "" ||
		strings.TrimSpace(in.IdempotencyKey) == "" || in.ArgsHash == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, fmt.Errorf("tenant, task, run, attempt, tool, action, resource, args hash, and idempotency key are required")
	}
	encoded, err := json.Marshal(struct {
		AttemptID      string `json:"attemptId"`
		ToolName       string `json:"toolName"`
		ToolVersion    string `json:"toolVersion"`
		Action         string `json:"action"`
		Resource       string `json:"resource"`
		ArgsHash       string `json:"argsHash"`
		IdempotencyKey string `json:"idempotencyKey"`
	}{in.AttemptID.String(), in.ToolName, in.ToolVersion, in.Action, in.Resource,
		fmt.Sprintf("%x", in.ArgsHash), in.IdempotencyKey})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode tool call request: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

type CreateToolCallResult struct {
	ToolCall ToolCall
	Existing bool
}

type UpdateToolCallInput struct {
	TenantID        string
	ToolCallID      uuid.UUID
	ExpectedVersion int64
	Status          ToolCallStatus
	DecisionReasons []string
	PolicyRevision  string
	ApprovalID      *uuid.UUID
}

// CanTransitionTo defines the decision-ledger state machine.
func CanTransitionTo(from, to ToolCallStatus) bool {
	switch from {
	case ToolCallPending:
		return to == ToolCallRequiresApproval || to == ToolCallApproved || to == ToolCallExecuted ||
			to == ToolCallDenied || to == ToolCallFailed
	case ToolCallRequiresApproval:
		return to == ToolCallApproved || to == ToolCallDenied || to == ToolCallFailed
	case ToolCallApproved:
		return to == ToolCallExecuted || to == ToolCallFailed
	default:
		return false
	}
}

type CreateToolApprovalInput struct {
	ID          uuid.UUID
	TenantID    string
	CallID      uuid.UUID
	TaskID      uuid.UUID
	RunID       uuid.UUID
	AttemptID   uuid.UUID
	ToolName    string
	ToolVersion string
	Action      string
	Resource    string
	ArgsHash    [sha256.Size]byte
	RequestedAt time.Time
	ExpiresAt   time.Time
}

type CreateToolApprovalResult struct {
	ToolApproval ToolApproval
	Existing     bool
}

type DecideToolApprovalInput struct {
	TenantID        string
	ApprovalID      uuid.UUID
	ExpectedVersion int64
	Decision        ToolApprovalStatus
	DecidedBy       string
	Now             time.Time
}

// Valid checks the decision request: only APPROVED/REJECTED decisions, an
// identified decider, and a current clock.
func (in DecideToolApprovalInput) Valid() bool {
	if strings.TrimSpace(in.TenantID) == "" || in.ApprovalID == uuid.Nil || in.ExpectedVersion <= 0 ||
		strings.TrimSpace(in.DecidedBy) == "" || in.Now.IsZero() {
		return false
	}
	return in.Decision == ToolApprovalApproved || in.Decision == ToolApprovalRejected
}

// RuntimeReceipt is the durable outcome record of one runtime operation.
type RuntimeReceipt struct {
	RequestHash [sha256.Size]byte
	Response    json.RawMessage
	ProcessedAt time.Time
}

type WriteRuntimeReceiptInput struct {
	TenantID       string
	AttemptID      uuid.UUID
	Operation      string
	IdempotencyKey string
	RequestHash    [sha256.Size]byte
	Response       json.RawMessage
}

type ToolStore interface {
	RegisterToolDescriptor(context.Context, RegisterToolDescriptorInput) (ToolDescriptor, error)
	GetToolDescriptor(context.Context, string, string, string) (ToolDescriptor, error)
	ListToolDescriptors(context.Context, string) ([]ToolDescriptor, error)
	CreateToolCall(context.Context, CreateToolCallInput) (CreateToolCallResult, error)
	GetToolCall(context.Context, string, uuid.UUID) (ToolCall, error)
	UpdateToolCall(context.Context, UpdateToolCallInput) (ToolCall, error)
	CreateToolApproval(context.Context, CreateToolApprovalInput) (CreateToolApprovalResult, error)
	GetToolApproval(context.Context, string, uuid.UUID) (ToolApproval, error)
	DecideToolApproval(context.Context, DecideToolApprovalInput) (ToolApproval, error)
}

type ReceiptStore interface {
	GetRuntimeReceipt(context.Context, string, uuid.UUID, string, string) (RuntimeReceipt, error)
	WriteRuntimeReceipt(context.Context, WriteRuntimeReceiptInput) error
}
