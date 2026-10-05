package syscall

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
)

// ResourceSyscallHandler handles Resource and Namespace syscalls:
// SysNamespaceGet (701), SysResourceQuotaGet (702), SysResourceUsageGet (703).
type ResourceSyscallHandler struct {
	store namespace.Store
}

// NewResourceSyscallHandler creates a new ResourceSyscallHandler.
func NewResourceSyscallHandler(store namespace.Store) *ResourceSyscallHandler {
	return &ResourceSyscallHandler{
		store: store,
	}
}

// Handle executes resource/namespace system calls.
func (h *ResourceSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.store == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "namespace store is not configured")
	}

	switch ctx.Request.Syscall {
	case SysNamespaceGet:
		return h.handleNamespaceGet(ctx)
	case SysResourceQuotaGet:
		return h.handleResourceQuotaGet(ctx)
	case SysResourceUsageGet:
		return h.handleResourceUsageGet(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported resource syscall: %v", ctx.Request.Syscall))
	}
}

func (h *ResourceSyscallHandler) handleNamespaceGet(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload NamespaceGetPayload
	if len(ctx.Request.PayloadJSON) > 0 {
		if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
			return nil, 0, WrapError(SyscallEINVAL, "malformed namespace get payload", err)
		}
	}

	targetNS := strings.TrimSpace(payload.Namespace)
	if targetNS == "" {
		targetNS = namespace.DefaultNamespace
	}

	tenantID := ctx.Request.Identity.TenantID
	ns, err := h.store.GetNamespace(ctx, tenantID, targetNS)
	if err != nil {
		return nil, 0, err
	}

	info := NamespaceInfo{
		TenantID:    ns.TenantID,
		Name:        ns.Name,
		DisplayName: ns.DisplayName,
		Description: ns.Description,
		Phase:       ns.Phase,
		Labels:      ns.Labels,
		Quota:       ns.Quota,
		CreatedAt:   ns.CreatedAt,
		UpdatedAt:   ns.UpdatedAt,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal namespace info: %w", err)
	}

	return data, 0, nil
}

func (h *ResourceSyscallHandler) handleResourceQuotaGet(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload ResourceQuotaPayload
	if len(ctx.Request.PayloadJSON) > 0 {
		if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
			return nil, 0, WrapError(SyscallEINVAL, "malformed resource quota payload", err)
		}
	}

	targetNS := strings.TrimSpace(payload.Namespace)
	if targetNS == "" {
		targetNS = namespace.DefaultNamespace
	}

	tenantID := ctx.Request.Identity.TenantID
	ns, err := h.store.GetNamespace(ctx, tenantID, targetNS)
	if err != nil {
		return nil, 0, err
	}

	info := ResourceQuotaInfo{
		TenantID:  ns.TenantID,
		Namespace: ns.Name,
		Quota:     ns.Quota,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal resource quota info: %w", err)
	}

	return data, 0, nil
}

func (h *ResourceSyscallHandler) handleResourceUsageGet(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload ResourceUsagePayload
	if len(ctx.Request.PayloadJSON) > 0 {
		if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
			return nil, 0, WrapError(SyscallEINVAL, "malformed resource usage payload", err)
		}
	}

	targetNS := strings.TrimSpace(payload.Namespace)
	if targetNS == "" {
		targetNS = namespace.DefaultNamespace
	}

	tenantID := ctx.Request.Identity.TenantID
	usage, err := h.store.GetUsage(ctx, tenantID, targetNS)
	if err != nil {
		return nil, 0, err
	}

	info := ResourceUsageInfo{
		Usage: *usage,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal resource usage info: %w", err)
	}

	return data, 0, nil
}
