package syscall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// MemoryInvoker defines the memory execution boundary.
type MemoryInvoker interface {
	Put(context.Context, memory.PutInput) (store.MemoryRecord, bool, error)
	Search(context.Context, memory.SearchInput) ([]store.MemoryRecord, error)
}

// MemorySyscallHandler handles SYS_MEMORY_PUT and SYS_MEMORY_SEARCH.
type MemorySyscallHandler struct {
	invoker      MemoryInvoker
	capabilities *capability.Authorizer
}

// NewMemorySyscallHandler creates a new MemorySyscallHandler.
func NewMemorySyscallHandler(invoker MemoryInvoker, caps *capability.Authorizer) *MemorySyscallHandler {
	return &MemorySyscallHandler{
		invoker:      invoker,
		capabilities: caps,
	}
}

// Handle executes memory syscalls.
func (h *MemorySyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.invoker == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "memory invoker is not configured")
	}

	switch ctx.Request.Syscall {
	case SysMemoryPut:
		return h.handlePut(ctx)
	case SysMemorySearch:
		return h.handleSearch(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported memory syscall: %v", ctx.Request.Syscall))
	}
}

func (h *MemorySyscallHandler) handlePut(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload MemoryPutPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed memory put payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	if h.capabilities != nil && versionRef != "" {
		if err := h.capabilities.Authorize(ctx, tenantID, versionRef, capability.Memory, payload.Namespace); err != nil {
			return nil, 0, WrapError(SyscallEPERM, fmt.Sprintf("capability denied for memory namespace %q", payload.Namespace), err)
		}
	}

	var provenance map[string]any
	if len(payload.ProvenanceJSON) > 0 {
		_ = json.Unmarshal(payload.ProvenanceJSON, &provenance)
	}

	taskID := ctx.Assignment.Task.ID
	runID := ctx.Assignment.Run.ID
	attemptID := ctx.Request.Identity.AttemptID

	record, replayed, err := h.invoker.Put(ctx, memory.PutInput{
		TenantID:        tenantID,
		Namespace:       payload.Namespace,
		Key:             payload.Key,
		ContentType:     payload.ContentType,
		Content:         payload.Content,
		Sensitivity:     payload.Sensitivity,
		SourceTaskID:    &taskID,
		SourceRunID:     &runID,
		SourceAttemptID: &attemptID,
		Provenance:      provenance,
	})
	if err != nil {
		return nil, 0, err
	}

	result := MemoryPutResult{
		Record:   record,
		Replayed: replayed,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize memory put result", err)
	}

	return raw, record.ResourceVersion, nil
}

func (h *MemorySyscallHandler) handleSearch(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload MemorySearchPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed memory search payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	versionRef := payload.AgentVersionRef
	if versionRef == "" && ctx.Assignment.Task.AgentVersionRef != "" {
		versionRef = ctx.Assignment.Task.AgentVersionRef
	}

	if h.capabilities != nil && versionRef != "" {
		if err := h.capabilities.Authorize(ctx, tenantID, versionRef, capability.Memory, payload.Namespace); err != nil {
			return nil, 0, WrapError(SyscallEPERM, fmt.Sprintf("capability denied for memory namespace %q", payload.Namespace), err)
		}
	}

	records, err := h.invoker.Search(ctx, memory.SearchInput{
		TenantID:    tenantID,
		Namespace:   payload.Namespace,
		Query:       payload.Query,
		Sensitivity: payload.Sensitivity,
		Limit:       payload.Limit,
	})
	if err != nil {
		return nil, 0, err
	}

	result := MemorySearchResult{
		Records: records,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize memory search result", err)
	}

	return raw, 0, nil
}
