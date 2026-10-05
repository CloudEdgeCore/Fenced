package syscall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/effect"
)

// SyscallInvoker executes a system call request and returns the system call response.
type SyscallInvoker interface {
	Dispatch(ctx context.Context, req SyscallRequest) SyscallResponse
}

// SyscallClient is the unified client interface for executing kernel system calls.
type SyscallClient struct {
	invoker SyscallInvoker
}

// NewClient creates a new SyscallClient.
func NewClient(invoker SyscallInvoker) *SyscallClient {
	return &SyscallClient{
		invoker: invoker,
	}
}

// ABIVersion returns the Syscall ABI semantic version supported by this client.
func (c *SyscallClient) ABIVersion() string {
	return SyscallABIVersion
}

// Call executes an arbitrary system call with payload and returns the raw result and resource version.
func (c *SyscallClient) Call(ctx context.Context, syscall SyscallNumber, identity AttemptIdentity, payload any) (json.RawMessage, int64, error) {
	if c.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "syscall invoker is not configured")
	}

	var payloadRaw json.RawMessage
	if payload != nil {
		bytes, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, WrapError(SyscallEINVAL, "failed to marshal syscall payload", err)
		}
		payloadRaw = bytes
	}

	req := SyscallRequest{
		Syscall:     syscall,
		Identity:    identity,
		PayloadJSON: payloadRaw,
	}

	resp := c.invoker.Dispatch(ctx, req)
	if resp.ErrorCode != SyscallOK {
		return nil, 0, NewSyscallError(resp.ErrorCode, resp.ErrorMessage)
	}

	return resp.ResultJSON, resp.ResourceVersion, nil
}

// ==========================================
// Tool Subsystem Helpers
// ==========================================

// InvokeTool calls SYS_TOOL_INVOKE.
func (c *SyscallClient) InvokeTool(ctx context.Context, id AttemptIdentity, payload ToolInvokePayload) (ToolInvokeResult, error) {
	raw, _, err := c.Call(ctx, SysToolInvoke, id, payload)
	if err != nil {
		return ToolInvokeResult{}, err
	}
	var res ToolInvokeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ToolInvokeResult{}, fmt.Errorf("decode tool invoke result: %w", err)
	}
	return res, nil
}

// ListTools calls SYS_TOOL_LIST.
func (c *SyscallClient) ListTools(ctx context.Context, id AttemptIdentity, payload ToolListPayload) (ToolListResult, error) {
	raw, _, err := c.Call(ctx, SysToolList, id, payload)
	if err != nil {
		return ToolListResult{}, err
	}
	var res ToolListResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ToolListResult{}, fmt.Errorf("decode tool list result: %w", err)
	}
	return res, nil
}

// ==========================================
// Model Subsystem Helpers
// ==========================================

// InvokeModel calls SYS_MODEL_INVOKE.
func (c *SyscallClient) InvokeModel(ctx context.Context, id AttemptIdentity, payload ModelInvokePayload) (ModelInvokeResult, error) {
	raw, _, err := c.Call(ctx, SysModelInvoke, id, payload)
	if err != nil {
		return ModelInvokeResult{}, err
	}
	var res ModelInvokeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ModelInvokeResult{}, fmt.Errorf("decode model invoke result: %w", err)
	}
	return res, nil
}

// BeginModel calls SYS_MODEL_BEGIN.
func (c *SyscallClient) BeginModel(ctx context.Context, id AttemptIdentity, payload ModelBeginPayload) (ModelBeginResult, int64, error) {
	raw, ver, err := c.Call(ctx, SysModelBegin, id, payload)
	if err != nil {
		return ModelBeginResult{}, 0, err
	}
	var res ModelBeginResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ModelBeginResult{}, 0, fmt.Errorf("decode model begin result: %w", err)
	}
	return res, ver, nil
}

// SettleModel calls SYS_MODEL_SETTLE.
func (c *SyscallClient) SettleModel(ctx context.Context, id AttemptIdentity, payload ModelSettlePayload) (ModelSettleResult, error) {
	raw, _, err := c.Call(ctx, SysModelSettle, id, payload)
	if err != nil {
		return ModelSettleResult{}, err
	}
	var res ModelSettleResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ModelSettleResult{}, fmt.Errorf("decode model settle result: %w", err)
	}
	return res, nil
}

// FinishModel calls SYS_MODEL_FINISH.
func (c *SyscallClient) FinishModel(ctx context.Context, id AttemptIdentity, payload ModelFinishPayload) (ModelFinishResult, int64, error) {
	raw, ver, err := c.Call(ctx, SysModelFinish, id, payload)
	if err != nil {
		return ModelFinishResult{}, 0, err
	}
	var res ModelFinishResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ModelFinishResult{}, 0, fmt.Errorf("decode model finish result: %w", err)
	}
	return res, ver, nil
}

// ==========================================
// Memory Subsystem Helpers
// ==========================================

// PutMemory calls SYS_MEMORY_PUT.
func (c *SyscallClient) PutMemory(ctx context.Context, id AttemptIdentity, payload MemoryPutPayload) (MemoryPutResult, int64, error) {
	raw, ver, err := c.Call(ctx, SysMemoryPut, id, payload)
	if err != nil {
		return MemoryPutResult{}, 0, err
	}
	var res MemoryPutResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return MemoryPutResult{}, 0, fmt.Errorf("decode memory put result: %w", err)
	}
	return res, ver, nil
}

// SearchMemory calls SYS_MEMORY_SEARCH.
func (c *SyscallClient) SearchMemory(ctx context.Context, id AttemptIdentity, payload MemorySearchPayload) (MemorySearchResult, error) {
	raw, _, err := c.Call(ctx, SysMemorySearch, id, payload)
	if err != nil {
		return MemorySearchResult{}, err
	}
	var res MemorySearchResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return MemorySearchResult{}, fmt.Errorf("decode memory search result: %w", err)
	}
	return res, nil
}

// ==========================================
// IPC Subsystem Helpers
// ==========================================

// SendIPC calls SYS_IPC_SEND.
func (c *SyscallClient) SendIPC(ctx context.Context, id AttemptIdentity, payload IPCSendPayload) (IPCSendResult, error) {
	raw, _, err := c.Call(ctx, SysIPCSend, id, payload)
	if err != nil {
		return IPCSendResult{}, err
	}
	var res IPCSendResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return IPCSendResult{}, fmt.Errorf("decode ipc send result: %w", err)
	}
	return res, nil
}

// ReceiveIPC calls SYS_IPC_RECEIVE.
func (c *SyscallClient) ReceiveIPC(ctx context.Context, id AttemptIdentity, payload IPCReceivePayload) (IPCReceiveResult, error) {
	raw, _, err := c.Call(ctx, SysIPCReceive, id, payload)
	if err != nil {
		return IPCReceiveResult{}, err
	}
	var res IPCReceiveResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return IPCReceiveResult{}, fmt.Errorf("decode ipc receive result: %w", err)
	}
	return res, nil
}

// AckIPC calls SYS_IPC_ACK.
func (c *SyscallClient) AckIPC(ctx context.Context, id AttemptIdentity, payload IPCAckPayload) (IPCAckResult, error) {
	raw, _, err := c.Call(ctx, SysIPCAck, id, payload)
	if err != nil {
		return IPCAckResult{}, err
	}
	var res IPCAckResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return IPCAckResult{}, fmt.Errorf("decode ipc ack result: %w", err)
	}
	return res, nil
}

// ==========================================
// Runtime Lifecycle Helpers
// ==========================================

// Checkpoint calls SYS_RUNTIME_CHECKPOINT.
func (c *SyscallClient) Checkpoint(ctx context.Context, id AttemptIdentity, payload RuntimeCheckpointPayload) (RuntimeCheckpointResult, int64, error) {
	raw, ver, err := c.Call(ctx, SysRuntimeCheckpoint, id, payload)
	if err != nil {
		return RuntimeCheckpointResult{}, 0, err
	}
	var res RuntimeCheckpointResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return RuntimeCheckpointResult{}, 0, fmt.Errorf("decode runtime checkpoint result: %w", err)
	}
	return res, ver, nil
}

// Complete calls SYS_RUNTIME_COMPLETE.
func (c *SyscallClient) Complete(ctx context.Context, id AttemptIdentity, payload RuntimeCompletePayload) (RuntimeCompleteResult, int64, error) {
	raw, ver, err := c.Call(ctx, SysRuntimeComplete, id, payload)
	if err != nil {
		return RuntimeCompleteResult{}, 0, err
	}
	var res RuntimeCompleteResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return RuntimeCompleteResult{}, 0, fmt.Errorf("decode runtime complete result: %w", err)
	}
	return res, ver, nil
}

// Yield calls SYS_RUNTIME_YIELD.
func (c *SyscallClient) Yield(ctx context.Context, id AttemptIdentity, payload RuntimeYieldPayload) (RuntimeYieldResult, error) {
	raw, _, err := c.Call(ctx, SysRuntimeYield, id, payload)
	if err != nil {
		return RuntimeYieldResult{}, err
	}
	var res RuntimeYieldResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return RuntimeYieldResult{}, fmt.Errorf("decode runtime yield result: %w", err)
	}
	return res, nil
}

// ==========================================
// Service Subsystem Helpers
// ==========================================

// HeartbeatService calls SYS_SERVICE_HEARTBEAT.
func (c *SyscallClient) HeartbeatService(ctx context.Context, id AttemptIdentity, payload ServiceHeartbeatPayload) (ServiceHeartbeatResult, error) {
	raw, _, err := c.Call(ctx, SysServiceHeartbeat, id, payload)
	if err != nil {
		return ServiceHeartbeatResult{}, err
	}
	var res ServiceHeartbeatResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ServiceHeartbeatResult{}, fmt.Errorf("decode service heartbeat result: %w", err)
	}
	return res, nil
}

// QueryServices calls SYS_SERVICE_QUERY.
func (c *SyscallClient) QueryServices(ctx context.Context, id AttemptIdentity, payload ServiceQueryPayload) (ServiceQueryResult, error) {
	raw, _, err := c.Call(ctx, SysServiceQuery, id, payload)
	if err != nil {
		return ServiceQueryResult{}, err
	}
	var res ServiceQueryResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return ServiceQueryResult{}, fmt.Errorf("decode service query result: %w", err)
	}
	return res, nil
}

// ==========================================
// Resource / Namespace Subsystem Helpers
// ==========================================

// GetNamespace calls SYS_NAMESPACE_GET.
func (c *SyscallClient) GetNamespace(ctx context.Context, id AttemptIdentity, payload NamespaceGetPayload) (NamespaceInfo, error) {
	raw, _, err := c.Call(ctx, SysNamespaceGet, id, payload)
	if err != nil {
		return NamespaceInfo{}, err
	}
	var res NamespaceInfo
	if err := json.Unmarshal(raw, &res); err != nil {
		return NamespaceInfo{}, fmt.Errorf("decode namespace info result: %w", err)
	}
	return res, nil
}

// GetResourceQuota calls SYS_RESOURCE_QUOTA_GET.
func (c *SyscallClient) GetResourceQuota(ctx context.Context, id AttemptIdentity, payload ResourceQuotaPayload) (ResourceQuotaInfo, error) {
	raw, _, err := c.Call(ctx, SysResourceQuotaGet, id, payload)
	if err != nil {
		return ResourceQuotaInfo{}, err
	}
	var res ResourceQuotaInfo
	if err := json.Unmarshal(raw, &res); err != nil {
		return ResourceQuotaInfo{}, fmt.Errorf("decode resource quota result: %w", err)
	}
	return res, nil
}

// GetResourceUsage calls SYS_RESOURCE_USAGE_GET.
func (c *SyscallClient) GetResourceUsage(ctx context.Context, id AttemptIdentity, payload ResourceUsagePayload) (ResourceUsageInfo, error) {
	raw, _, err := c.Call(ctx, SysResourceUsageGet, id, payload)
	if err != nil {
		return ResourceUsageInfo{}, err
	}
	var res ResourceUsageInfo
	if err := json.Unmarshal(raw, &res); err != nil {
		return ResourceUsageInfo{}, fmt.Errorf("decode resource usage result: %w", err)
	}
	return res, nil
}

// ==========================================
// Effect Subsystem Helpers
// ==========================================

// ExecuteEffect calls SYS_EFFECT_EXECUTE.
func (c *SyscallClient) ExecuteEffect(ctx context.Context, id AttemptIdentity, payload EffectExecutePayload) (effect.EffectReceipt, int64, error) {
	raw, ver, err := c.Call(ctx, SysEffectExecute, id, payload)
	if err != nil {
		return effect.EffectReceipt{}, 0, err
	}
	var res effect.EffectReceipt
	if err := json.Unmarshal(raw, &res); err != nil {
		return effect.EffectReceipt{}, 0, fmt.Errorf("decode effect receipt: %w", err)
	}
	return res, ver, nil
}

// GetEffect calls SYS_EFFECT_GET.
func (c *SyscallClient) GetEffect(ctx context.Context, id AttemptIdentity, payload EffectGetPayload) (effect.EffectRecord, int64, error) {
	raw, ver, err := c.Call(ctx, SysEffectGet, id, payload)
	if err != nil {
		return effect.EffectRecord{}, 0, err
	}
	var res effect.EffectRecord
	if err := json.Unmarshal(raw, &res); err != nil {
		return effect.EffectRecord{}, 0, fmt.Errorf("decode effect record: %w", err)
	}
	return res, ver, nil
}
