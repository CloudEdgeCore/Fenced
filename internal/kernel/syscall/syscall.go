package syscall

import (
	"fmt"
	"strconv"
	"strings"
)

// SyscallNumber represents a unique kernel-level system call identifier.
type SyscallNumber uint32

const (
	// SyscallNone represents an uninitialized or unknown system call.
	SyscallNone SyscallNumber = 0

	// Tool Subsystem (100 - 199)
	SysToolInvoke SyscallNumber = 101
	SysToolList   SyscallNumber = 102

	// Model Subsystem (200 - 299)
	SysModelInvoke SyscallNumber = 201
	SysModelBegin  SyscallNumber = 202
	SysModelSettle SyscallNumber = 203
	SysModelFinish SyscallNumber = 204

	// Memory Subsystem (300 - 399)
	SysMemoryPut    SyscallNumber = 301
	SysMemorySearch SyscallNumber = 302

	// IPC Subsystem (400 - 499)
	SysIPCSend    SyscallNumber = 401
	SysIPCReceive SyscallNumber = 402
	SysIPCAck     SyscallNumber = 403

	// Runtime Subsystem (500 - 599)
	SysRuntimeCheckpoint SyscallNumber = 501
	SysRuntimeComplete   SyscallNumber = 502
	SysRuntimeYield      SyscallNumber = 503

	// Service / Supervisor Subsystem (600 - 699)
	SysServiceHeartbeat SyscallNumber = 601
	SysServiceQuery     SyscallNumber = 602

	// Resource / Namespace Subsystem (700 - 799)
	SysNamespaceGet     SyscallNumber = 701
	SysResourceQuotaGet SyscallNumber = 702
	SysResourceUsageGet SyscallNumber = 703

	// Effect Subsystem (800 - 899)
	SysEffectExecute SyscallNumber = 801
	SysEffectGet     SyscallNumber = 802
)

// SyscallABIVersion specifies the semantic version of the Fenced Syscall ABI.
const SyscallABIVersion = "1.0.0"

// SyscallABIMajor is the major version component of the Syscall ABI.
const SyscallABIMajor uint32 = 1

// SyscallABIMinor is the minor version component of the Syscall ABI.
const SyscallABIMinor uint32 = 0

// SyscallABIPatch is the patch version component of the Syscall ABI.
const SyscallABIPatch uint32 = 0

// SyscallDescriptor describes metadata and execution properties of a system call.
type SyscallDescriptor struct {
	Number       SyscallNumber
	Name         string
	Category     string
	IsReadOnly   bool
	Description  string
	SinceVersion string
}

var syscallTable = map[SyscallNumber]SyscallDescriptor{
	SysToolInvoke: {
		Number:       SysToolInvoke,
		Name:         "sys_tool_invoke",
		Category:     "tool",
		IsReadOnly:   false,
		Description:  "Invokes a verified capability tool with arguments and receipt generation",
		SinceVersion: "1.0.0",
	},
	SysToolList: {
		Number:       SysToolList,
		Name:         "sys_tool_list",
		Category:     "tool",
		IsReadOnly:   true,
		Description:  "Lists available registered tools granted to the agent version",
		SinceVersion: "1.0.0",
	},
	SysModelInvoke: {
		Number:       SysModelInvoke,
		Name:         "sys_model_invoke",
		Category:     "model",
		IsReadOnly:   false,
		Description:  "Executes an LLM model invocation behind policy and budget checks",
		SinceVersion: "1.0.0",
	},
	SysModelBegin: {
		Number:       SysModelBegin,
		Name:         "sys_model_begin",
		Category:     "model",
		IsReadOnly:   false,
		Description:  "Initializes a model call session and reserves token budget",
		SinceVersion: "1.0.0",
	},
	SysModelSettle: {
		Number:       SysModelSettle,
		Name:         "sys_model_settle",
		Category:     "model",
		IsReadOnly:   false,
		Description:  "Settles intermediate token usage for streaming model execution",
		SinceVersion: "1.0.0",
	},
	SysModelFinish: {
		Number:       SysModelFinish,
		Name:         "sys_model_finish",
		Category:     "model",
		IsReadOnly:   false,
		Description:  "Finalizes a model call session and reconciles exact cost",
		SinceVersion: "1.0.0",
	},
	SysMemoryPut: {
		Number:       SysMemoryPut,
		Name:         "sys_memory_put",
		Category:     "memory",
		IsReadOnly:   false,
		Description:  "Persists a scoped memory record into durable memory store",
		SinceVersion: "1.0.0",
	},
	SysMemorySearch: {
		Number:       SysMemorySearch,
		Name:         "sys_memory_search",
		Category:     "memory",
		IsReadOnly:   true,
		Description:  "Searches scoped memory records by semantic query or key prefix",
		SinceVersion: "1.0.0",
	},
	SysIPCSend: {
		Number:       SysIPCSend,
		Name:         "sys_ipc_send",
		Category:     "ipc",
		IsReadOnly:   false,
		Description:  "Sends a durable message to another agent mailbox",
		SinceVersion: "1.0.0",
	},
	SysIPCReceive: {
		Number:       SysIPCReceive,
		Name:         "sys_ipc_receive",
		Category:     "ipc",
		IsReadOnly:   false,
		Description:  "Drains or polls pending messages from the current agent mailbox",
		SinceVersion: "1.0.0",
	},
	SysIPCAck: {
		Number:       SysIPCAck,
		Name:         "sys_ipc_ack",
		Category:     "ipc",
		IsReadOnly:   false,
		Description:  "Acknowledges successfully processed durable mailbox messages",
		SinceVersion: "1.0.0",
	},
	SysRuntimeCheckpoint: {
		Number:       SysRuntimeCheckpoint,
		Name:         "sys_runtime_checkpoint",
		Category:     "runtime",
		IsReadOnly:   false,
		Description:  "Commits an execution checkpoint for crash-consistent state recovery",
		SinceVersion: "1.0.0",
	},
	SysRuntimeComplete: {
		Number:       SysRuntimeComplete,
		Name:         "sys_runtime_complete",
		Category:     "runtime",
		IsReadOnly:   false,
		Description:  "Marks the current attempt as completed with final output reference",
		SinceVersion: "1.0.0",
	},
	SysRuntimeYield: {
		Number:       SysRuntimeYield,
		Name:         "sys_runtime_yield",
		Category:     "runtime",
		IsReadOnly:   true,
		Description:  "Yields execution timeslice cooperatively back to the kernel scheduler",
		SinceVersion: "1.0.0",
	},
	SysServiceHeartbeat: {
		Number:       SysServiceHeartbeat,
		Name:         "sys_service_heartbeat",
		Category:     "service",
		IsReadOnly:   false,
		Description:  "Sends periodic instance heartbeat to supervisor engine",
		SinceVersion: "1.0.0",
	},
	SysServiceQuery: {
		Number:       SysServiceQuery,
		Name:         "sys_service_query",
		Category:     "service",
		IsReadOnly:   true,
		Description:  "Queries registered agent services and their instance topology",
		SinceVersion: "1.0.0",
	},
	SysNamespaceGet: {
		Number:       SysNamespaceGet,
		Name:         "sys_namespace_get",
		Category:     "namespace",
		IsReadOnly:   true,
		Description:  "Retrieves metadata, lifecycle phase, and status of a namespace",
		SinceVersion: "1.0.0",
	},
	SysResourceQuotaGet: {
		Number:       SysResourceQuotaGet,
		Name:         "sys_resource_quota_get",
		Category:     "resource",
		IsReadOnly:   true,
		Description:  "Retrieves configured resource quotas and limits for a namespace",
		SinceVersion: "1.0.0",
	},
	SysResourceUsageGet: {
		Number:       SysResourceUsageGet,
		Name:         "sys_resource_usage_get",
		Category:     "resource",
		IsReadOnly:   true,
		Description:  "Retrieves active and settled resource usage for a namespace",
		SinceVersion: "1.0.0",
	},
	SysEffectExecute: {
		Number:       SysEffectExecute,
		Name:         "sys_effect_execute",
		Category:     "effect",
		IsReadOnly:   false,
		Description:  "Executes an external side effect with idempotent receipt and fencing enforcement",
		SinceVersion: "1.0.0",
	},
	SysEffectGet: {
		Number:       SysEffectGet,
		Name:         "sys_effect_get",
		Category:     "effect",
		IsReadOnly:   true,
		Description:  "Retrieves an existing effect record and receipt by ID or idempotency key",
		SinceVersion: "1.0.0",
	},
}

// String returns the uppercase symbolic name of the syscall.
func (s SyscallNumber) String() string {
	switch s {
	case SysToolInvoke:
		return "SYS_TOOL_INVOKE"
	case SysToolList:
		return "SYS_TOOL_LIST"
	case SysModelInvoke:
		return "SYS_MODEL_INVOKE"
	case SysModelBegin:
		return "SYS_MODEL_BEGIN"
	case SysModelSettle:
		return "SYS_MODEL_SETTLE"
	case SysModelFinish:
		return "SYS_MODEL_FINISH"
	case SysMemoryPut:
		return "SYS_MEMORY_PUT"
	case SysMemorySearch:
		return "SYS_MEMORY_SEARCH"
	case SysIPCSend:
		return "SYS_IPC_SEND"
	case SysIPCReceive:
		return "SYS_IPC_RECEIVE"
	case SysIPCAck:
		return "SYS_IPC_ACK"
	case SysRuntimeCheckpoint:
		return "SYS_RUNTIME_CHECKPOINT"
	case SysRuntimeComplete:
		return "SYS_RUNTIME_COMPLETE"
	case SysRuntimeYield:
		return "SYS_RUNTIME_YIELD"
	case SysServiceHeartbeat:
		return "SYS_SERVICE_HEARTBEAT"
	case SysServiceQuery:
		return "SYS_SERVICE_QUERY"
	case SysNamespaceGet:
		return "SYS_NAMESPACE_GET"
	case SysResourceQuotaGet:
		return "SYS_RESOURCE_QUOTA_GET"
	case SysResourceUsageGet:
		return "SYS_RESOURCE_USAGE_GET"
	case SysEffectExecute:
		return "SYS_EFFECT_EXECUTE"
	case SysEffectGet:
		return "SYS_EFFECT_GET"
	default:
		return fmt.Sprintf("SYS_UNKNOWN(%d)", uint32(s))
	}
}

// Name returns the canonical lowercase syscall name (e.g. "sys_tool_invoke").
func (s SyscallNumber) Name() string {
	if desc, ok := syscallTable[s]; ok {
		return desc.Name
	}
	return strings.ToLower(s.String())
}

// Category returns the subsystem category of the syscall (e.g. "tool", "model", "ipc").
func (s SyscallNumber) Category() string {
	if desc, ok := syscallTable[s]; ok {
		return desc.Category
	}
	return "unknown"
}

// IsReadOnly returns true if the syscall does not mutate kernel or domain state.
func (s SyscallNumber) IsReadOnly() bool {
	if desc, ok := syscallTable[s]; ok {
		return desc.IsReadOnly
	}
	return false
}

// Descriptor returns the full metadata descriptor for this syscall.
func (s SyscallNumber) Descriptor() (SyscallDescriptor, bool) {
	desc, ok := syscallTable[s]
	return desc, ok
}

// ParseSyscallNumber parses a string (either uppercase symbolic name, lowercase name, or integer)
// into a SyscallNumber.
func ParseSyscallNumber(str string) (SyscallNumber, error) {
	trimmed := strings.TrimSpace(str)
	if trimmed == "" {
		return SyscallNone, fmt.Errorf("empty syscall identifier")
	}

	// Try numeric parsing
	if num, err := strconv.ParseUint(trimmed, 10, 32); err == nil {
		s := SyscallNumber(num)
		if _, ok := syscallTable[s]; ok {
			return s, nil
		}
		return s, nil
	}

	upper := strings.ToUpper(trimmed)
	lower := strings.ToLower(trimmed)

	for num, desc := range syscallTable {
		if strings.ToUpper(desc.Name) == upper || desc.Name == lower || num.String() == upper {
			return num, nil
		}
	}

	return SyscallNone, fmt.Errorf("unknown syscall identifier: %q", str)
}

// AllDescriptors returns a slice of all known syscall descriptors.
func AllDescriptors() []SyscallDescriptor {
	result := make([]SyscallDescriptor, 0, len(syscallTable))
	for _, desc := range syscallTable {
		result = append(result, desc)
	}
	return result
}
