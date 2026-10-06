package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/google/uuid"
)

var (
	// ErrBudgetExceeded reports a usage settlement that would push a task past
	// its reserved ceiling. The settlement is rejected and the ledger is
	// marked exhausted: new consumption must stop.
	ErrBudgetExceeded = errors.New("task budget would be exceeded")
	// ErrBudgetNotReserved reports a settlement or read for a task without a
	// budget reservation.
	ErrBudgetNotReserved = errors.New("task has no budget reservation")
	// ErrUsageReservationConflict reports a replay that changes the amount
	// reserved under an existing reservation key.
	ErrUsageReservationConflict = errors.New("usage reservation conflicts with its replay")
)

// TaskBudget is the reserved or consumed four-dimensional budget of a task.
// All fields are non-negative; a zero budget means the dimension is unlimited.
type TaskBudget struct {
	Tokens       int64
	CostMicroUSD money.MicroUSD
	ToolCalls    int64
	WallSeconds  int64
}

func (b TaskBudget) Zero() bool {
	return b.Tokens == 0 && b.CostMicroUSD == 0 && b.ToolCalls == 0 && b.WallSeconds == 0
}

func (b TaskBudget) Valid() bool {
	return b.Tokens >= 0 && b.CostMicroUSD >= 0 &&
		b.ToolCalls >= 0 && b.WallSeconds >= 0
}

// TaskBudgetStatus is the current reservation and cumulative consumption of a
// task, as derived from the append-only settlement ledger.
type TaskBudgetStatus struct {
	TaskID          uuid.UUID
	TenantID        string
	Reserved        TaskBudget
	Consumed        TaskBudget
	Exhausted       bool
	ResourceVersion int64
	UpdatedAt       time.Time
}

type SettleTaskUsageInput struct {
	TenantID       string
	TaskID         uuid.UUID
	IdempotencyKey string
	Usage          TaskBudget
	// ReservationKey identifies the active pre-consumption reservation that
	// this settlement replaces. It is excluded from concurrent headroom.
	ReservationKey string
}

func (in SettleTaskUsageInput) Valid() bool {
	return strings.TrimSpace(in.TenantID) != "" && in.TaskID != uuid.Nil &&
		strings.TrimSpace(in.IdempotencyKey) != "" && in.Usage.Valid() && !in.Usage.Zero()
}

// SettleTaskUsageDeltaInput settles a usage family up to a cumulative target:
// the delta between what the family already settled and the target is recorded
// under one idempotency key. The family is the set of settlements whose
// idempotency key is FamilyPrefix followed by ":<suffix>". This is the exact
// accounting primitive for model calls: per-step Settle records accrue under
// "model:<callID>:<seq>" and Finish settles only the remainder to reach the
// final cumulative total, so a stream that both settles steps and finalizes is
// charged exactly once.
type SettleTaskUsageDeltaInput struct {
	TenantID string
	TaskID   uuid.UUID
	// FamilyPrefix scopes the already-settled sum (e.g. "model:<callID>").
	// Wildcards are rejected so the prefix can never widen the match.
	FamilyPrefix string
	// Target is the cumulative usage the family must reach.
	Target TaskBudget
	// IdempotencyKey is the key of the delta settlement row.
	IdempotencyKey string
	ReservationKey string
}

type ReserveTaskUsageInput struct {
	TenantID       string
	TaskID         uuid.UUID
	ReservationKey string
	Amount         TaskBudget
	ExpiresAt      time.Time
}

func (in ReserveTaskUsageInput) Valid() bool {
	return strings.TrimSpace(in.TenantID) != "" && in.TaskID != uuid.Nil &&
		strings.TrimSpace(in.ReservationKey) != "" && in.Amount.Valid() && !in.Amount.Zero() && !in.ExpiresAt.IsZero()
}

func (in SettleTaskUsageDeltaInput) Valid() bool {
	return strings.TrimSpace(in.TenantID) != "" && in.TaskID != uuid.Nil &&
		strings.TrimSpace(in.FamilyPrefix) != "" && !strings.ContainsAny(in.FamilyPrefix, "%_") &&
		strings.TrimSpace(in.IdempotencyKey) != "" && in.Target.Valid()
}

type BudgetStore interface {
	// GetTaskBudget returns the reservation and cumulative consumption of a
	// task, or ErrBudgetNotReserved when the task carries no budget.
	GetTaskBudget(context.Context, string, uuid.UUID) (TaskBudgetStatus, error)
	// SettleTaskUsage appends one idempotent usage settlement. A settlement
	// that would exceed the reservation is rejected with ErrBudgetExceeded and
	// marks the ledger exhausted without being recorded. Replays of an
	// already-recorded settlement return the current status.
	SettleTaskUsage(context.Context, SettleTaskUsageInput) (TaskBudgetStatus, error)
	// SettleTaskUsageDelta settles the remainder between a family's already
	// settled usage and a cumulative target, exactly once per IdempotencyKey.
	// The hard stop and replay semantics match SettleTaskUsage; a family that
	// already settled at least the target settles nothing.
	SettleTaskUsageDelta(context.Context, SettleTaskUsageDeltaInput) (TaskBudgetStatus, error)
	// ReserveTaskUsage atomically protects headroom before a provider or tool
	// call starts. Replays with the same key and amount are idempotent.
	ReserveTaskUsage(context.Context, ReserveTaskUsageInput) error
	// ReleaseTaskUsageReservation is idempotent and must run on every terminal
	// call path. Expired reservations are reclaimed by reconciliation.
	ReleaseTaskUsageReservation(context.Context, string, uuid.UUID, string) error
}
