// Package errorcode is the canonical registry for stable public Fenced error
// codes. Internal errors must be translated to one of these codes before they
// cross an Agent, SDK, API, artifact, event, or audit boundary.
package errorcode

import "sort"

const (
	Internal                 = "INTERNAL_ERROR"
	InvalidTask              = "INVALID_TASK"
	InvalidWorkflow          = "INVALID_WORKFLOW"
	ResourceVersionConflict  = "RESOURCE_VERSION_CONFLICT"
	IdempotencyConflict      = "IDEMPOTENCY_CONFLICT"
	AuthenticationRequired   = "AUTHENTICATION_REQUIRED"
	PolicyDenied             = "POLICY_DENIED"
	TenantQuotaExceeded      = "TENANT_QUOTA_EXCEEDED"
	BudgetExhausted          = "BUDGET_EXHAUSTED"
	WallTimeExceeded         = "WALL_TIME_EXCEEDED"
	ProviderUnavailable      = "PROVIDER_UNAVAILABLE"
	ProviderRejected         = "PROVIDER_REJECTED"
	ProviderStreamAborted    = "PROVIDER_STREAM_ABORTED"
	ToolInvocationFailed     = "TOOL_INVOCATION_FAILED"
	OutputContractViolation  = "OUTPUT_CONTRACT_VIOLATION"
	WorkflowDeadlineExceeded = "WORKFLOW_DEADLINE_EXCEEDED"
	WorkflowBudgetExhausted  = "WORKFLOW_BUDGET_EXHAUSTED"
	SpawnDisabled            = "SPAWN_DISABLED"
	SpawnCancelled           = "SPAWN_CANCELLED"
	SpawnDeadlineExceeded    = "SPAWN_DEADLINE_EXCEEDED"
	SpawnBudgetExhausted     = "SPAWN_BUDGET_EXHAUSTED"
	SpawnDepthExceeded       = "SPAWN_DEPTH_EXCEEDED"
	SpawnFanoutExceeded      = "SPAWN_FANOUT_EXCEEDED"
	SpawnDynamicExceeded     = "SPAWN_DYNAMIC_LIMIT_EXCEEDED"
	SpawnTotalStepsExceeded  = "SPAWN_TOTAL_STEPS_EXCEEDED"
	SpawnTaskLimitExceeded   = "SPAWN_TASK_LIMIT_EXCEEDED"
	SpawnTokenLimitExceeded  = "SPAWN_TOKEN_LIMIT_EXCEEDED"
	SpawnCostLimitExceeded   = "SPAWN_COST_LIMIT_EXCEEDED"
	SpawnNameConflict        = "SPAWN_NAME_CONFLICT"
	// SpawnReconciliationPending pauses dynamic spawning while a workflow's
	// step budget reservations are being re-derived after an upgrade: the
	// committed total is briefly understated, so admitting a new spawn could
	// slip past a ceiling. The agent should retry once reconciliation clears.
	SpawnReconciliationPending = "SPAWN_RECONCILIATION_PENDING"
)

var known = map[string]struct{}{
	Internal: {}, InvalidTask: {}, InvalidWorkflow: {}, ResourceVersionConflict: {}, IdempotencyConflict: {},
	AuthenticationRequired: {}, PolicyDenied: {}, TenantQuotaExceeded: {}, BudgetExhausted: {}, WallTimeExceeded: {},
	ProviderUnavailable: {}, ProviderRejected: {}, ProviderStreamAborted: {}, ToolInvocationFailed: {},
	OutputContractViolation: {}, WorkflowDeadlineExceeded: {}, WorkflowBudgetExhausted: {}, SpawnDisabled: {},
	SpawnCancelled: {}, SpawnDeadlineExceeded: {}, SpawnBudgetExhausted: {}, SpawnDepthExceeded: {},
	SpawnFanoutExceeded: {}, SpawnDynamicExceeded: {}, SpawnTotalStepsExceeded: {}, SpawnTaskLimitExceeded: {},
	SpawnTokenLimitExceeded: {}, SpawnCostLimitExceeded: {}, SpawnNameConflict: {},
	SpawnReconciliationPending: {},
}

func Known(code string) bool {
	_, ok := known[code]
	return ok
}

func All() []string {
	codes := make([]string, 0, len(known))
	for code := range known {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// Class is the public disposition of an error code (P3-04): it tells every
// SDK, agent and operator what to do next without inspecting message text.
// The stable code strings never change; the class is metadata carried
// alongside them.
type Class string

const (
	// Retryable: the same request may succeed after a bounded backoff
	// (transient infrastructure state).
	Retryable Class = "retryable"
	// Terminal: retrying the identical request can never succeed.
	Terminal Class = "terminal"
	// UserActionRequired: only the caller (tenant/publisher) can change the
	// outcome — by fixing the request, credentials, grants or budgets.
	UserActionRequired Class = "user-action-required"
	// OperatorActionRequired: only a platform operator can change the
	// outcome (infrastructure or internal failure).
	OperatorActionRequired Class = "operator-action-required"
)

var dispositions = map[string]Class{
	Internal:                   OperatorActionRequired,
	InvalidTask:                UserActionRequired,
	InvalidWorkflow:            UserActionRequired,
	ResourceVersionConflict:    Retryable,
	IdempotencyConflict:        UserActionRequired,
	AuthenticationRequired:     UserActionRequired,
	PolicyDenied:               UserActionRequired,
	TenantQuotaExceeded:        UserActionRequired,
	BudgetExhausted:            UserActionRequired,
	WallTimeExceeded:           Terminal,
	ProviderUnavailable:        Retryable,
	ProviderRejected:           Terminal,
	ProviderStreamAborted:      Terminal,
	ToolInvocationFailed:       Terminal,
	OutputContractViolation:    UserActionRequired,
	WorkflowDeadlineExceeded:   Terminal,
	WorkflowBudgetExhausted:    UserActionRequired,
	SpawnDisabled:              UserActionRequired,
	SpawnCancelled:             Terminal,
	SpawnDeadlineExceeded:      Terminal,
	SpawnBudgetExhausted:       UserActionRequired,
	SpawnDepthExceeded:         UserActionRequired,
	SpawnFanoutExceeded:        UserActionRequired,
	SpawnDynamicExceeded:       UserActionRequired,
	SpawnTotalStepsExceeded:    UserActionRequired,
	SpawnTaskLimitExceeded:     UserActionRequired,
	SpawnTokenLimitExceeded:    UserActionRequired,
	SpawnCostLimitExceeded:     UserActionRequired,
	SpawnNameConflict:          UserActionRequired,
	SpawnReconciliationPending: Retryable,
}

// ClassOf returns the public disposition of one error code. Every known code
// has exactly one class; an unknown code reports not-ok so callers fail
// closed instead of guessing a retry policy for an unrecognized string.
func ClassOf(code string) (Class, bool) {
	class, ok := dispositions[code]
	return class, ok
}
