package store

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/google/uuid"
)

var ErrCapacityExhausted = errors.New("runtime pool capacity exhausted")

// ErrPoolCapacityNotInitialized is returned when placement targets a pool
// whose durable capacity ledger row has not been registered by the runtime
// pool registry. Scheduling fails closed instead of bootstrapping totals
// from a caller snapshot.
var ErrPoolCapacityNotInitialized = errors.New("runtime pool capacity ledger is not initialized")

type ControllerKind string

const (
	ControllerAdmission  ControllerKind = "ADMISSION"
	ControllerScheduling ControllerKind = "SCHEDULING"
)

type TaskClaim struct {
	Task         Task
	Kind         ControllerKind
	OwnerID      string
	FencingToken int64
	ExpiresAt    time.Time
}

type ClaimTasksInput struct {
	Kind    ControllerKind
	Phase   domain.TaskPhase
	OwnerID string
	Limit   int
	TTL     time.Duration
	// ShardIndex / ShardCount are the tenant-consistent shard filter
	// (ADR-016): when ShardCount > 0, only tasks whose tenant maps to
	// ShardIndex are claimable. Zero values mean no sharding (every instance
	// claims every tenant). All instances must share the same shard count.
	ShardIndex int
	ShardCount int
}

// TenantShard returns the ADR-016 shard for a tenant: the first 32 bits of
// md5(tenant_id) modulo count. It mirrors the SQL expression used by
// ClaimTasks exactly, so tests can assert the filter deterministically.
// md5 is used instead of hashing functions like hashtext, which PostgreSQL
// does not guarantee stable across major versions.
func TenantShard(tenantID string, count int) int {
	if count <= 0 {
		return 0
	}
	sum := md5.Sum([]byte(tenantID))
	return int(binary.BigEndian.Uint32(sum[:4]) % uint32(count))
}

type AdmissionReason struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

type DecideAdmissionInput struct {
	TaskID              uuid.UUID
	TenantID            string
	OwnerID             string
	ClaimFencingToken   int64
	ExpectedTaskVersion int64
	Admit               bool
	ReasonCode          string
	Reasons             []AdmissionReason
	EvaluatorVersion    string
	// AgentVersionID binds the task to the immutable published version its
	// reference resolved to. It is set by the admission controller whenever
	// the reference is resolvable, regardless of the admission outcome.
	AgentVersionID *uuid.UUID
	// Budget is the task's reserved ceiling, derived from its workload spec
	// by the admission controller. When set and the task is admitted, the
	// ledger row is created in the same transaction as the admission
	// decision; nil means the task carries no budget.
	Budget *TaskBudget
	// PolicyRevision records the Rego policy revision that produced the
	// decision; empty when no policy engine participated.
	PolicyRevision string
}

type ScheduleTaskInput struct {
	TaskID              uuid.UUID
	TenantID            string
	OwnerID             string
	ClaimFencingToken   int64
	ExpectedTaskVersion int64
	RunID               uuid.UUID
	AttemptID           uuid.UUID
	LeaseID             uuid.UUID
	RuntimePoolID       string
	RuntimeClass        string
	RuntimeInstanceID   string
	LeaseTTL            time.Duration
	RequestedCPU        int64
	RequestedMemory     int64
	RequestedLLMSlots   int
}

// DeferTaskScheduleInput releases the scheduler's claim on a task that no
// pool could place and defers its next scheduling attempt until Until (O6).
// The deferral is recorded on the task — not on the claim — so every
// controller instance agrees on the backoff, and the claim is deleted in the
// same transaction (immediate release, no waiting out the claim TTL).
type DeferTaskScheduleInput struct {
	TaskID              uuid.UUID
	TenantID            string
	OwnerID             string
	ClaimFencingToken   int64
	ExpectedTaskVersion int64
	Until               time.Time
	Rejection           json.RawMessage
}

type OutboxEvent struct {
	ID               uuid.UUID
	TenantID         string
	AggregateType    string
	AggregateID      uuid.UUID
	AggregateVersion int64
	EventType        string
	Payload          json.RawMessage
	OccurredAt       time.Time
	PublishAttempts  int
	LockedBy         string
	LockedUntil      time.Time
	LockFencingToken int64
}

type ClaimOutboxInput struct {
	DispatcherID string
	Limit        int
	LockTTL      time.Duration
}

type ControlStore interface {
	GetTask(context.Context, string, uuid.UUID) (Task, error)
	ClaimTasks(context.Context, ClaimTasksInput) ([]TaskClaim, error)
	ReleaseTaskClaim(context.Context, TaskClaim) error
	DecideAdmission(context.Context, DecideAdmissionInput) (Task, error)
	ScheduleTask(context.Context, ScheduleTaskInput) (AttemptLease, error)
	DeferTaskSchedule(context.Context, DeferTaskScheduleInput) (Task, error)
	ClaimOutbox(context.Context, ClaimOutboxInput) ([]OutboxEvent, error)
	MarkOutboxPublished(context.Context, uuid.UUID, string, int64, time.Time) error
	MarkOutboxFailed(context.Context, uuid.UUID, string, int64, string, time.Time) error
	GetAgentVersionByRef(context.Context, string, string) (AgentVersion, error)
}

type TaskCancellationStore interface {
	RequestTaskCancellation(context.Context, string, uuid.UUID, int64) (Task, error)
}
