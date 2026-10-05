package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/google/uuid"
)

// Agent Orchestration: durable WorkflowRun and Step state. The
// orchestrator owns who executes when; the scheduler (unchanged) owns where
// a Task runs. Workflows never bypass the Task pipeline — steps create
// ordinary fenced Tasks.

var (
	// ErrWorkflowNotFound reports a workflow outside the tenant's scope.
	ErrWorkflowNotFound = errors.New("workflow not found")
	// ErrStepNotFound reports a step that does not belong to the workflow.
	ErrStepNotFound = errors.New("workflow step not found")
)

// WorkflowStatus is the lifecycle of one workflow run.
type WorkflowStatus string

const (
	WorkflowPending   WorkflowStatus = "PENDING"
	WorkflowRunning   WorkflowStatus = "RUNNING"
	WorkflowSucceeded WorkflowStatus = "SUCCEEDED"
	WorkflowFailed    WorkflowStatus = "FAILED"
	WorkflowCancelled WorkflowStatus = "CANCELLED"
)

func (s WorkflowStatus) Terminal() bool {
	switch s {
	case WorkflowSucceeded, WorkflowFailed, WorkflowCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionWorkflow defines the workflow state machine.
func CanTransitionWorkflow(from, to WorkflowStatus) bool {
	switch from {
	case WorkflowPending:
		return to == WorkflowRunning || to == WorkflowCancelled
	case WorkflowRunning:
		return to == WorkflowSucceeded || to == WorkflowFailed || to == WorkflowCancelled
	default:
		return false
	}
}

// StepStatus is the lifecycle of one workflow step.
type StepStatus string

const (
	StepPending         StepStatus = "PENDING"
	StepWaitingApproval StepStatus = "WAITING_APPROVAL"
	StepRunning         StepStatus = "RUNNING"
	StepSucceeded       StepStatus = "SUCCEEDED"
	StepFailed          StepStatus = "FAILED"
	StepSkipped         StepStatus = "SKIPPED"
	StepCancelled       StepStatus = "CANCELLED"
)

func (s StepStatus) Terminal() bool {
	switch s {
	case StepSucceeded, StepFailed, StepSkipped, StepCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionStep defines the step state machine. RUNNING → PENDING is the
// single-step retry path; it never touches sibling steps.
func CanTransitionStep(from, to StepStatus) bool {
	switch from {
	case StepPending:
		return to == StepWaitingApproval || to == StepRunning || to == StepSkipped || to == StepCancelled
	case StepWaitingApproval:
		return to == StepRunning || to == StepSkipped || to == StepCancelled
	case StepRunning:
		return to == StepSucceeded || to == StepFailed || to == StepCancelled || to == StepPending
	default:
		return false
	}
}

// Workflow is one durable workflow run.
type Workflow struct {
	ID                uuid.UUID
	TenantID          string
	Namespace         string
	IdempotencyKey    string
	Goal              string
	Spec              json.RawMessage
	Status            WorkflowStatus
	FailureCode       string
	CancelRequestedAt *time.Time
	// Budget ceilings. Zero means the dimension is unbounded.
	BudgetMaxTasks        int64
	BudgetMaxTokens       int64
	BudgetMaxCostMicroUSD money.MicroUSD
	BudgetExhaustedAt     *time.Time
	DeadlineAt            *time.Time
	DeadlineExceededAt    *time.Time
	// NeedsBudgetReconciliation is set when a step's token/cost reservation may
	// be understated relative to its derived task spec — e.g. after the 000028
	// upgrade backfilled task-count slots but could not re-derive merged specs
	// in pure SQL. While set, dynamic spawning is paused (a stale-low commitment
	// could slip a spawn past a ceiling); the controller's reconcile loop
	// re-derives the reservations and clears the flag.
	NeedsBudgetReconciliation bool
	ResourceVersion           int64
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

// WorkflowStep is one step of a workflow: a declared DAG node plus its
// dispatch state. AttemptCount drives the single-step retry budget. Dynamic
// Dynamic steps carry their whole definition here — declared steps keep theirs
// in the workflow document — with the spawning parent and depth lineage for
// the recursion guard.
type WorkflowStep struct {
	ID               uuid.UUID
	TenantID         string
	WorkflowID       uuid.UUID
	Name             string
	Ordinal          int
	Status           StepStatus
	AttemptCount     int
	TaskID           *uuid.UUID
	ResultSummary    json.RawMessage
	FailureCode      string
	DecidedBy        string
	ApprovalDecision string
	DecidedAt        *time.Time
	ParentStepName   string
	SpawnDepth       int
	IsDynamic        bool
	SpawnKey         string
	Goal             string
	AgentVersionRef  string
	Spec             json.RawMessage
	MaxAttempts      int
	// Reserved* is the outstanding workflow budget reservation held by an
	// undispatched or retrying step: the future Task's token/cost ceiling
	// plus its task-count slot. It transfers to the task's own budget
	// ledger at admission and returns to zero when the step reaches a
	// terminal state without one.
	ReservedTasks        int64
	ReservedTokens       int64
	ReservedCostMicroUSD money.MicroUSD
	ResourceVersion      int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

var stepNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,63})$`)

// ValidateStepName rejects step names that are not canonical tokens.
func ValidateStepName(name string) error {
	if !stepNamePattern.MatchString(name) {
		return fmt.Errorf("step name must match %s", stepNamePattern)
	}
	return nil
}

// CreateWorkflowStepInput is one declared step of a new workflow.
type CreateWorkflowStepInput struct {
	Name              string
	Spec              json.RawMessage // per-step workload spec overlay
	AgentVersionRef   string
	Goal              string
	DependsOn         []string
	ConditionStep     string
	ConditionContains string
	ConditionEquals   string
	RequiresApproval  bool
	MaxAttempts       int
}

// CreateWorkflowInput creates a workflow and its steps atomically.
type CreateWorkflowInput struct {
	ID             uuid.UUID
	TenantID       string
	Namespace      string
	IdempotencyKey string
	Goal           string
	Spec           json.RawMessage
	Steps          []CreateWorkflowStepInput
	// Budget ceilings decoded from the workflow document; zero leaves
	// the dimension unbounded.
	BudgetMaxTasks        int64
	BudgetMaxTokens       int64
	BudgetMaxCostMicroUSD money.MicroUSD
	DeadlineAt            *time.Time
}

// RequestHash fingerprints the tenant-scoped workflow definition so an
// idempotency-key replay of a different definition is rejected.
func (in CreateWorkflowInput) RequestHash() ([sha256.Size]byte, error) {
	if strings.TrimSpace(in.TenantID) == "" || strings.TrimSpace(in.Goal) == "" ||
		len(in.Steps) == 0 || strings.TrimSpace(in.IdempotencyKey) == "" {
		return [sha256.Size]byte{}, fmt.Errorf("tenant, goal, idempotency key, and steps are required")
	}
	encoded, err := json.Marshal(struct {
		Goal                  string                    `json:"goal"`
		Spec                  json.RawMessage           `json:"spec"`
		Steps                 []CreateWorkflowStepInput `json:"steps"`
		BudgetMaxTasks        int64                     `json:"budgetMaxTasks"`
		BudgetMaxTokens       int64                     `json:"budgetMaxTokens"`
		BudgetMaxCostMicroUSD money.MicroUSD            `json:"budgetMaxCostMicroUsd"`
		DeadlineAt            *time.Time                `json:"deadlineAt,omitempty"`
	}{in.Goal, json.RawMessage(canonicalJSON(in.Spec)), in.Steps, in.BudgetMaxTasks, in.BudgetMaxTokens, in.BudgetMaxCostMicroUSD, in.DeadlineAt})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode workflow request: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func canonicalJSON(raw json.RawMessage) []byte {
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return raw
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return raw
	}
	return encoded
}

// CreateWorkflowResult reports the created workflow and whether an
// idempotency-key replay returned the existing definition.
type CreateWorkflowResult struct {
	Workflow Workflow
	Existing bool
}

// TransitionWorkflowInput CAS-transitions one workflow. ExpectedOwner, when
// non-empty, fences the transition to the orchestrator instance that holds a
// live claim: the write fails with ErrFenced when the claim was lost or
// expired. Empty disables the check (single-instance mode, non-claiming
// controllers).
type TransitionWorkflowInput struct {
	TenantID        string
	WorkflowID      uuid.UUID
	ExpectedVersion int64
	To              WorkflowStatus
	FailureCode     string
	ExpectedOwner   string
}

// DecideWorkflowStepApprovalInput records a human decision on one step.
type DecideWorkflowStepApprovalInput struct {
	TenantID        string
	WorkflowID      uuid.UUID
	StepName        string
	ExpectedVersion int64
	Approved        bool
	DecidedBy       string
}

// TransitionWorkflowStepInput CAS-transitions one step. Zero fields are
// left unchanged; TaskID/AttemptCount set when provided. ExpectedOwner, when
// non-empty, fences the transition to the orchestrator instance that holds a
// live claim on the parent workflow (see TransitionWorkflowInput).
type TransitionWorkflowStepInput struct {
	TenantID        string
	WorkflowID      uuid.UUID
	StepName        string
	ExpectedVersion int64
	To              StepStatus
	TaskID          *uuid.UUID
	AttemptCount    *int
	ResultSummary   json.RawMessage
	FailureCode     string
	ExpectedOwner   string
}

// SpawnWorkflowStepInput creates one dynamic step. SpawnKey is the
// brokered tool call's idempotency fingerprint: replaying the same spawn
// returns the existing step instead of creating a second one. The guards are
// the runtime policy resolved from the workflow document.
type SpawnWorkflowStepInput struct {
	WorkflowID      uuid.UUID
	TenantID        string
	WorkflowVersion int64
	ParentStepName  string
	Name            string
	Goal            string
	AgentVersionRef string
	Spec            json.RawMessage
	MaxAttempts     int
	Guards          SpawnGuards
	IdempotencyKey  string
	Arguments       json.RawMessage
}

// SpawnWorkflowStepResult reports the step (new or replayed), the workflow's
// budget ceilings, and the usage snapshot the guard evaluated against.
type SpawnWorkflowStepResult struct {
	Step              WorkflowStep
	Workflow          Workflow
	WorkflowExhausted bool
	Created           bool
	Usage             WorkflowUsage
}

// WorkflowUsage is the aggregated consumption of one workflow: tasks that
// carry its idempotency prefix plus their settled usage.
type WorkflowUsage struct {
	Tasks                 int64
	Tokens                int64
	CostMicroUSD          money.MicroUSD
	ReservedTokens        int64
	ReservedCostMicroUSD  money.MicroUSD
	PendingOverage        bool
	BudgetMaxTasks        int64
	BudgetMaxTokens       int64
	BudgetMaxCostMicroUSD money.MicroUSD
	// StepReserved* counts the workflow budget reservations still held by
	// undispatched (or retrying) steps: the outstanding future commitment
	// that exists before any task budget ledger does.
	StepReservedTasks        int64
	StepReservedTokens       int64
	StepReservedCostMicroUSD money.MicroUSD
}

// Exhausted reports whether any declared ceiling is met or exceeded by
// created tasks and their settled or task-reserved usage. Step reservations
// are excluded: a promise that exactly fills the budget must still run, and
// over-promising is already impossible because spawn and creation reserve
// inside their transactions.
func (u WorkflowUsage) Exhausted() bool {
	if u.BudgetMaxTasks > 0 && u.Tasks >= u.BudgetMaxTasks {
		return true
	}
	if u.BudgetMaxTokens > 0 && u.Tokens+u.ReservedTokens >= u.BudgetMaxTokens {
		return true
	}
	if u.BudgetMaxCostMicroUSD > 0 && u.CostMicroUSD+u.ReservedCostMicroUSD >= u.BudgetMaxCostMicroUSD {
		return true
	}
	return false
}

// CommittedTasks returns the workflow's total task promise: created tasks
// plus the task slots reserved by undispatched steps.
func (u WorkflowUsage) CommittedTasks() int64 {
	return u.Tasks + u.StepReservedTasks
}

// CommittedTokens returns settled plus task-reserved plus step-reserved
// tokens: the workflow's total token commitment.
func (u WorkflowUsage) CommittedTokens() int64 {
	return u.Tokens + u.ReservedTokens + u.StepReservedTokens
}

// CommittedCostMicroUSD returns settled plus task-reserved plus
// step-reserved cost: the workflow's total cost commitment.
func (u WorkflowUsage) CommittedCostMicroUSD() money.MicroUSD {
	return u.CostMicroUSD + u.ReservedCostMicroUSD + u.StepReservedCostMicroUSD
}

// TaskSpecBudgetReservation extracts the token and cost ceiling a merged
// workflow step spec will reserve at admission. It mirrors the admission
// controller's workload budget decode so the step reservation equals the
// task ledger reservation that later replaces it.
func TaskSpecBudgetReservation(spec json.RawMessage) (tokens int64, costMicroUSD money.MicroUSD) {
	if len(spec) == 0 {
		return 0, 0
	}
	var document struct {
		Budget workload.Budget `json:"budget"`
	}
	if json.Unmarshal(spec, &document) != nil {
		return 0, 0
	}
	return document.Budget.Tokens, document.Budget.CostMicroUSD
}

// SpawnKeyHash fingerprints the spawn arguments into the idempotent
// spawn_key suffix.
func SpawnKeyHash(arguments json.RawMessage) string {
	var document any
	if err := json.Unmarshal(arguments, &document); err != nil {
		document = string(arguments)
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		canonical = arguments
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}

// ErrSpawnDenied reports a dynamic-spawn guard rejection.
var ErrSpawnDenied = errors.New("spawn denied")

// SpawnDenial is a structured guard rejection: the brokered tool surfaces
// the stable code to the agent instead of a raw error string.
type SpawnDenial struct {
	Code    string
	Message string
}

func (d SpawnDenial) Error() string { return d.Message }

func (d SpawnDenial) Unwrap() error { return ErrSpawnDenied }

// DenialCode extracts the stable denial code of a spawn error, if any.
func DenialCode(err error) (string, bool) {
	var denial SpawnDenial
	if errors.As(err, &denial) {
		return denial.Code, true
	}
	return "", false
}

// SpawnGuards carries the dynamic-orchestration runtime limits the
// spawning path enforces. Zero disables a guard.
type SpawnGuards struct {
	Enabled              bool
	MaxDynamicSteps      int64
	MaxChildrenPerStep   int64
	MaxSpawnDepth        int
	MaxWorkflowSteps     int64
	MaxSpawnTasks        int64
	MaxSpawnTokens       int64
	MaxSpawnCostMicroUSD money.MicroUSD
}

// RuntimePolicy is the dynamic-orchestration policy the controller resolves
// from the workflow document at runtime.
type RuntimePolicy struct {
	SpawnGuards SpawnGuards
}

// DecodeWorkflowRuntimePolicy extracts the runtime policy from a stored
// workflow document. Unknown documents yield the zero policy (dynamic spawn
// disabled).
func DecodeWorkflowRuntimePolicy(spec json.RawMessage) RuntimePolicy {
	var document struct {
		Runtime struct {
			Dynamic struct {
				Enabled              bool           `json:"enabled"`
				MaxDynamicSteps      int64          `json:"maxDynamicSteps"`
				MaxChildrenPerStep   int64          `json:"maxChildrenPerStep"`
				MaxSpawnDepth        int            `json:"maxSpawnDepth"`
				MaxWorkflowSteps     int64          `json:"maxWorkflowSteps"`
				MaxSpawnTasks        int64          `json:"maxSpawnTasks"`
				MaxSpawnTokens       int64          `json:"maxSpawnTokens"`
				MaxSpawnCostMicroUSD money.MicroUSD `json:"maxSpawnCostUsd"`
			} `json:"dynamic"`
		} `json:"runtime"`
	}
	if json.Unmarshal(spec, &document) != nil {
		return RuntimePolicy{}
	}
	dynamic := document.Runtime.Dynamic
	return RuntimePolicy{SpawnGuards: SpawnGuards{
		Enabled: dynamic.Enabled, MaxDynamicSteps: dynamic.MaxDynamicSteps,
		MaxChildrenPerStep: dynamic.MaxChildrenPerStep, MaxSpawnDepth: dynamic.MaxSpawnDepth,
		MaxWorkflowSteps: dynamic.MaxWorkflowSteps, MaxSpawnTasks: dynamic.MaxSpawnTasks,
		MaxSpawnTokens: dynamic.MaxSpawnTokens, MaxSpawnCostMicroUSD: dynamic.MaxSpawnCostMicroUSD,
	}}
}

// ClaimWorkflowsInput bounds one orchestrator claim round.
type ClaimWorkflowsInput struct {
	Owner     string
	Batch     int
	Lease     time.Duration
	MaxTokens int64
}

// WorkflowStore is the durable workflow surface. The postgres Store
// satisfies it.
type WorkflowStore interface {
	CreateWorkflow(context.Context, CreateWorkflowInput) (CreateWorkflowResult, error)
	GetWorkflow(context.Context, string, uuid.UUID) (Workflow, error)
	// ListActiveWorkflows returns non-terminal workflows oldest-first for
	// the orchestrator reconcile loop (all tenants; creation is where
	// tenant scoping is enforced).
	ListActiveWorkflows(context.Context, int) ([]Workflow, error)
	ListWorkflowSteps(context.Context, string, uuid.UUID) ([]WorkflowStep, error)
	GetWorkflowStep(context.Context, string, uuid.UUID, string) (WorkflowStep, error)
	TransitionWorkflow(context.Context, TransitionWorkflowInput) (Workflow, error)
	TransitionWorkflowStep(context.Context, TransitionWorkflowStepInput) (WorkflowStep, error)
	DecideWorkflowStepApproval(context.Context, DecideWorkflowStepApprovalInput) (WorkflowStep, error)
	// RequestWorkflowCancellation records the durable cancellation intent;
	// the orchestrator propagates it to active steps and finalizes.
	RequestWorkflowCancellation(context.Context, string, uuid.UUID, int64) (Workflow, error)
	// ArtifactMetadata reads the digest, size and media type of one stored
	// artifact URI so the orchestrator can open task result documents.
	ArtifactMetadata(context.Context, string, string) ([]byte, int64, string, error)
	// ClaimWorkflows leases active workflows to one orchestrator instance
	// Expired claims are stolen, so a dead instance's workflows are
	// taken over by its peers. Tenants are claimed round-robin.
	ClaimWorkflows(context.Context, ClaimWorkflowsInput) ([]Workflow, error)
	// SpawnWorkflowStep creates one dynamic step with all recursion, fan-out
	// and budget guards applied in the same transaction. A spawn_key
	// replay returns the existing step.
	SpawnWorkflowStep(context.Context, SpawnWorkflowStepInput) (SpawnWorkflowStepResult, error)
	// MarkWorkflowBudgetExhausted records the durable budget-stop intent.
	MarkWorkflowBudgetExhausted(context.Context, string, uuid.UUID, int64) (Workflow, error)
	// MarkWorkflowDeadlineExceeded records the durable deadline-stop intent.
	MarkWorkflowDeadlineExceeded(context.Context, string, uuid.UUID, int64) (Workflow, error)
	// WorkflowUsageSnapshot aggregates a workflow's task count and settled
	// usage.
	WorkflowUsageSnapshot(context.Context, string, uuid.UUID) (WorkflowUsage, error)
}
