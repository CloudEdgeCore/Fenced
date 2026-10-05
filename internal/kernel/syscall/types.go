package syscall

import (
	"encoding/json"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
)

// AttemptIdentity binds a syscall invocation to an authenticated attempt lease.
type AttemptIdentity struct {
	TenantID     string    `json:"tenant_id"`
	AttemptID    uuid.UUID `json:"attempt_id"`
	FencingToken int64     `json:"fencing_token"`
}

// SyscallRequest is the canonical system call execution envelope.
type SyscallRequest struct {
	Syscall        SyscallNumber     `json:"syscall"`
	Identity       AttemptIdentity   `json:"identity"`
	PayloadJSON    json.RawMessage   `json:"payload_json"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// SyscallResponse is the canonical system call return envelope.
type SyscallResponse struct {
	Syscall         SyscallNumber    `json:"syscall"`
	ErrorCode       SyscallErrorCode `json:"error_code"`
	ErrorMessage    string           `json:"error_message,omitempty"`
	ResultJSON      json.RawMessage  `json:"result_json,omitempty"`
	DurationUS      int64            `json:"duration_us"`
	ResourceVersion int64            `json:"resource_version,omitempty"`
}

// IsSuccess returns true if the syscall completed without error.
func (r SyscallResponse) IsSuccess() bool {
	return r.ErrorCode == SyscallOK
}

// ==========================================
// Tool Subsystem Types (100 - 199)
// ==========================================

// ToolInvokePayload arguments for SYS_TOOL_INVOKE.
type ToolInvokePayload struct {
	TaskID          uuid.UUID       `json:"task_id"`
	RunID           uuid.UUID       `json:"run_id"`
	AgentVersionRef string          `json:"agent_version_ref"`
	ToolName        string          `json:"tool_name"`
	ToolVersion     string          `json:"tool_version"`
	Action          string          `json:"action"`
	Resource        string          `json:"resource"`
	ArgsJSON        json.RawMessage `json:"args_json"`
	ApprovalID      string          `json:"approval_id,omitempty"`
	SecretRef       string          `json:"secret_ref,omitempty"`
}

// ToolInvokeResult returned by SYS_TOOL_INVOKE.
type ToolInvokeResult struct {
	Outcome          string          `json:"outcome"`
	ResultJSON       json.RawMessage `json:"result_json,omitempty"`
	ApprovalID       string          `json:"approval_id,omitempty"`
	DenyReasons      []string        `json:"deny_reasons,omitempty"`
	PolicyRevision   string          `json:"policy_revision,omitempty"`
	ReceiptOperation string          `json:"receipt_operation,omitempty"`
	ToolCallID       string          `json:"tool_call_id,omitempty"`
}

// ToolListPayload arguments for SYS_TOOL_LIST.
type ToolListPayload struct {
	AgentVersionRef string `json:"agent_version_ref"`
}

// ToolListResult returned by SYS_TOOL_LIST.
type ToolListResult struct {
	Tools []store.ToolDescriptor `json:"tools"`
}

// ==========================================
// Model Subsystem Types (200 - 299)
// ==========================================

// ModelChatToolCall represents a tool call requested by an LLM model.
type ModelChatToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ModelChatMessage represents a message turn in conversation.
type ModelChatMessage struct {
	Role       string              `json:"role"`
	Content    string              `json:"content"`
	ToolCallID string              `json:"tool_call_id,omitempty"`
	ToolCalls  []ModelChatToolCall `json:"tool_calls,omitempty"`
}

// ModelToolDef defines a callable tool presented to the model.
type ModelToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// ModelInvokePayload arguments for SYS_MODEL_INVOKE.
type ModelInvokePayload struct {
	TaskID          uuid.UUID          `json:"task_id"`
	RunID           uuid.UUID          `json:"run_id"`
	AgentVersionRef string             `json:"agent_version_ref"`
	ModelRef        string             `json:"model_ref"`
	Messages        []ModelChatMessage `json:"messages"`
	Stream          bool               `json:"stream,omitempty"`
	Temperature     *float64           `json:"temperature,omitempty"`
	MaxOutputTokens int32              `json:"max_output_tokens,omitempty"`
	Tools           []ModelToolDef     `json:"tools,omitempty"`
}

// ModelInvokeResult returned by SYS_MODEL_INVOKE.
type ModelInvokeResult struct {
	CallID            uuid.UUID           `json:"call_id"`
	ModelRef          string              `json:"model_ref"`
	Status            string              `json:"status"`
	Content           string              `json:"content,omitempty"`
	InputTokens       int64               `json:"input_tokens"`
	OutputTokens      int64               `json:"output_tokens"`
	CostUSD           float64             `json:"cost_usd"`
	PriceRevision     string              `json:"price_revision,omitempty"`
	FinishReason      string              `json:"finish_reason,omitempty"`
	ProviderRequestID string              `json:"provider_request_id,omitempty"`
	ToolCalls         []ModelChatToolCall `json:"tool_calls,omitempty"`
}

// ModelBeginPayload arguments for SYS_MODEL_BEGIN.
type ModelBeginPayload struct {
	TaskID          uuid.UUID `json:"task_id"`
	RunID           uuid.UUID `json:"run_id"`
	AgentVersionRef string    `json:"agent_version_ref"`
	ModelRef        string    `json:"model_ref"`
}

// ModelBeginResult returned by SYS_MODEL_BEGIN.
type ModelBeginResult struct {
	CallID          uuid.UUID `json:"call_id"`
	ModelRef        string    `json:"model_ref"`
	Status          string    `json:"status"`
	PriceRevision   string    `json:"price_revision,omitempty"`
	PolicyRevision  string    `json:"policy_revision,omitempty"`
	ResourceVersion int64     `json:"resource_version"`
}

// ModelSettlePayload arguments for SYS_MODEL_SETTLE.
type ModelSettlePayload struct {
	CallID       uuid.UUID `json:"call_id"`
	Sequence     int64     `json:"sequence"`
	InputTokens  int64     `json:"input_tokens"`
	OutputTokens int64     `json:"output_tokens"`
}

// ModelSettleResult returned by SYS_MODEL_SETTLE.
type ModelSettleResult struct {
	CallID uuid.UUID `json:"call_id"`
}

// ModelFinishPayload arguments for SYS_MODEL_FINISH.
type ModelFinishPayload struct {
	CallID            uuid.UUID `json:"call_id"`
	ExpectedVersion   int64     `json:"expected_version"`
	Status            string    `json:"status"`
	InputTokens       int64     `json:"input_tokens"`
	OutputTokens      int64     `json:"output_tokens"`
	ProviderRequestID string    `json:"provider_request_id,omitempty"`
	FinishReason      string    `json:"finish_reason,omitempty"`
}

// ModelFinishResult returned by SYS_MODEL_FINISH.
type ModelFinishResult struct {
	CallID        uuid.UUID `json:"call_id"`
	ModelRef      string    `json:"model_ref"`
	Status        string    `json:"status"`
	InputTokens   int64     `json:"input_tokens"`
	OutputTokens  int64     `json:"output_tokens"`
	CostUSD       float64   `json:"cost_usd"`
	PriceRevision string    `json:"price_revision,omitempty"`
	FinishReason  string    `json:"finish_reason,omitempty"`
}

// ==========================================
// Memory Subsystem Types (300 - 399)
// ==========================================

// MemoryPutPayload arguments for SYS_MEMORY_PUT.
type MemoryPutPayload struct {
	AgentVersionRef string          `json:"agent_version_ref"`
	Namespace       string          `json:"namespace"`
	Key             string          `json:"key"`
	ContentType     string          `json:"content_type"`
	Content         string          `json:"content"`
	Sensitivity     string          `json:"sensitivity,omitempty"`
	ProvenanceJSON  json.RawMessage `json:"provenance_json,omitempty"`
}

// MemoryPutResult returned by SYS_MEMORY_PUT.
type MemoryPutResult struct {
	Record   store.MemoryRecord `json:"record"`
	Replayed bool               `json:"replayed"`
}

// MemorySearchPayload arguments for SYS_MEMORY_SEARCH.
type MemorySearchPayload struct {
	AgentVersionRef string `json:"agent_version_ref"`
	Namespace       string `json:"namespace"`
	Query           string `json:"query"`
	Sensitivity     string `json:"sensitivity,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

// MemorySearchResult returned by SYS_MEMORY_SEARCH.
type MemorySearchResult struct {
	Records []store.MemoryRecord `json:"records"`
}

// ==========================================
// IPC Subsystem Types (400 - 499)
// ==========================================

// IPCSendPayload arguments for SYS_IPC_SEND.
type IPCSendPayload struct {
	AgentVersionRef  string           `json:"agent_version_ref"`
	TaskID           uuid.UUID        `json:"task_id"`
	RunID            uuid.UUID        `json:"run_id"`
	To               ipc.AgentAddress `json:"to"`
	Kind             string           `json:"kind"`
	PayloadJSON      json.RawMessage  `json:"payload_json"`
	CorrelationID    string           `json:"correlation_id,omitempty"`
	ReplyToMessageID string           `json:"reply_to_message_id,omitempty"`
	Deadline         *time.Time       `json:"deadline,omitempty"`
}

// IPCSendResult returned by SYS_IPC_SEND.
type IPCSendResult struct {
	MessageID  string    `json:"message_id"`
	Replayed   bool      `json:"replayed"`
	AcceptedAt time.Time `json:"accepted_at"`
}

// IPCReceivePayload arguments for SYS_IPC_RECEIVE.
type IPCReceivePayload struct {
	AgentVersionRef string `json:"agent_version_ref"`
	MaxMessages     int    `json:"max_messages"`
	WaitMillis      int    `json:"wait_millis"`
}

// IPCReceiveResult returned by SYS_IPC_RECEIVE.
type IPCReceiveResult struct {
	Messages []*ipc.AgentMessage `json:"messages"`
}

// IPCAckPayload arguments for SYS_IPC_ACK.
type IPCAckPayload struct {
	AgentVersionRef string   `json:"agent_version_ref"`
	MessageIDs      []string `json:"message_ids"`
}

// IPCAckResult returned by SYS_IPC_ACK.
type IPCAckResult struct {
	AcknowledgedIDs []string `json:"acknowledged_ids"`
}

// ==========================================
// Runtime Lifecycle Subsystem Types (500 - 599)
// ==========================================

// RuntimeCheckpointPayload arguments for SYS_RUNTIME_CHECKPOINT.
type RuntimeCheckpointPayload struct {
	ExpectedAttemptVersion int64                   `json:"expected_attempt_version"`
	CheckpointID           string                  `json:"checkpoint_id"`
	State                  store.ArtifactReference `json:"state"`
	ConfirmedReceiptIDs    []string                `json:"confirmed_receipt_ids,omitempty"`
}

// RuntimeCheckpointResult returned by SYS_RUNTIME_CHECKPOINT.
type RuntimeCheckpointResult struct {
	Checkpoint     store.Checkpoint `json:"checkpoint"`
	AttemptVersion int64            `json:"attempt_version"`
}

// RuntimeCompletePayload arguments for SYS_RUNTIME_COMPLETE.
type RuntimeCompletePayload struct {
	ExpectedAttemptVersion int64                   `json:"expected_attempt_version"`
	Result                 store.ArtifactReference `json:"result"`
}

// RuntimeCompleteResult returned by SYS_RUNTIME_COMPLETE.
type RuntimeCompleteResult struct {
	AttemptVersion int64  `json:"attempt_version"`
	ResultRef      string `json:"result_ref"`
}

// RuntimeYieldPayload arguments for SYS_RUNTIME_YIELD.
type RuntimeYieldPayload struct {
	Reason string `json:"reason,omitempty"`
}

// RuntimeYieldResult returned by SYS_RUNTIME_YIELD.
type RuntimeYieldResult struct {
	Yielded bool `json:"yielded"`
}

// ==========================================
// Service Subsystem Types (600 - 699)
// ==========================================

// ServiceHeartbeatPayload arguments for SYS_SERVICE_HEARTBEAT.
type ServiceHeartbeatPayload struct {
	ServiceID  uuid.UUID `json:"service_id"`
	InstanceID string    `json:"instance_id"`
	Status     string    `json:"status"`
	LoadScore  float64   `json:"load_score,omitempty"`
}

// ServiceHeartbeatResult returned by SYS_SERVICE_HEARTBEAT.
type ServiceHeartbeatResult struct {
	Acknowledged       bool `json:"acknowledged"`
	RebalanceRequested bool `json:"rebalance_requested"`
}

// ServiceQueryPayload arguments for SYS_SERVICE_QUERY.
type ServiceQueryPayload struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

// ServiceQueryResult returned by SYS_SERVICE_QUERY.
type ServiceQueryResult struct {
	Services []*supervisor.Service `json:"services"`
}

// ==========================================
// Resource / Namespace Subsystem Types (700 - 799)
// ==========================================

// NamespaceGetPayload arguments for SYS_NAMESPACE_GET.
type NamespaceGetPayload struct {
	Namespace string `json:"namespace"`
}

// NamespaceInfo returned by SYS_NAMESPACE_GET.
type NamespaceInfo struct {
	TenantID    string                   `json:"tenant_id"`
	Name        string                   `json:"name"`
	DisplayName string                   `json:"display_name,omitempty"`
	Description string                   `json:"description,omitempty"`
	Phase       namespace.NamespacePhase `json:"phase"`
	Labels      map[string]string        `json:"labels,omitempty"`
	Quota       namespace.ResourceQuota  `json:"quota"`
	CreatedAt   time.Time                `json:"created_at"`
	UpdatedAt   time.Time                `json:"updated_at"`
}

// ResourceQuotaPayload arguments for SYS_RESOURCE_QUOTA_GET.
type ResourceQuotaPayload struct {
	Namespace string `json:"namespace"`
}

// ResourceQuotaInfo returned by SYS_RESOURCE_QUOTA_GET.
type ResourceQuotaInfo struct {
	TenantID  string                  `json:"tenant_id"`
	Namespace string                  `json:"namespace"`
	Quota     namespace.ResourceQuota `json:"quota"`
}

// ResourceUsagePayload arguments for SYS_RESOURCE_USAGE_GET.
type ResourceUsagePayload struct {
	Namespace string `json:"namespace"`
}

// ResourceUsageInfo returned by SYS_RESOURCE_USAGE_GET.
type ResourceUsageInfo struct {
	Usage namespace.ResourceUsage `json:"usage"`
}

// ==========================================
// Effect Subsystem Types (800 - 899)
// ==========================================

// EffectExecutePayload arguments for SYS_EFFECT_EXECUTE.
type EffectExecutePayload struct {
	AgentID        string          `json:"agent_id"`
	RunID          string          `json:"run_id,omitempty"`
	Provider       string          `json:"provider"`
	Operation      string          `json:"operation"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Deadline       *time.Time      `json:"deadline,omitempty"`
	TraceID        string          `json:"trace_id,omitempty"`
}

// EffectGetPayload arguments for SYS_EFFECT_GET.
type EffectGetPayload struct {
	EffectID       string `json:"effect_id,omitempty"`
	AgentID        string `json:"agent_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}
