package syscall

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
)

// ServiceSyscallHandler handles SYS_SERVICE_HEARTBEAT and SYS_SERVICE_QUERY.
type ServiceSyscallHandler struct {
	store supervisor.Store
}

// NewServiceSyscallHandler creates a new ServiceSyscallHandler.
func NewServiceSyscallHandler(store supervisor.Store) *ServiceSyscallHandler {
	return &ServiceSyscallHandler{
		store: store,
	}
}

// Handle executes supervisor service syscalls.
func (h *ServiceSyscallHandler) Handle(ctx *SyscallContext) (json.RawMessage, int64, error) {
	if h.store == nil {
		return nil, 0, NewSyscallError(SyscallENOSYS, "supervisor store is not configured")
	}

	switch ctx.Request.Syscall {
	case SysServiceHeartbeat:
		return h.handleHeartbeat(ctx)
	case SysServiceQuery:
		return h.handleQuery(ctx)
	default:
		return nil, 0, NewSyscallError(SyscallENOSYS, fmt.Sprintf("unsupported service syscall: %v", ctx.Request.Syscall))
	}
}

func (h *ServiceSyscallHandler) handleHeartbeat(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload ServiceHeartbeatPayload
	if err := json.Unmarshal(ctx.Request.PayloadJSON, &payload); err != nil {
		return nil, 0, WrapError(SyscallEINVAL, "malformed service heartbeat payload", err)
	}

	tenantID := ctx.Request.Identity.TenantID
	serviceID := payload.ServiceID.String()
	instanceID := payload.InstanceID

	inst, err := h.store.GetInstance(ctx, tenantID, serviceID, instanceID)
	if err != nil {
		return nil, 0, err
	}

	inst.LastHeartbeat = time.Now().UTC()
	if payload.Status != "" {
		inst.Phase = supervisor.InstancePhase(payload.Status)
	} else {
		inst.Phase = supervisor.InstanceRunning
	}

	if err := h.store.UpdateInstance(ctx, inst); err != nil {
		return nil, 0, err
	}

	result := ServiceHeartbeatResult{
		Acknowledged:       true,
		RebalanceRequested: false,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize heartbeat result", err)
	}

	return raw, 0, nil
}

func (h *ServiceSyscallHandler) handleQuery(ctx *SyscallContext) (json.RawMessage, int64, error) {
	var payload ServiceQueryPayload
	if len(ctx.Request.PayloadJSON) > 0 {
		_ = json.Unmarshal(ctx.Request.PayloadJSON, &payload)
	}

	tenantID := ctx.Request.Identity.TenantID
	namespace := payload.Namespace
	if namespace == "" {
		namespace = ctx.Assignment.Task.Namespace
	}

	var services []*supervisor.Service
	if payload.Name != "" {
		svc, err := h.store.GetServiceByName(ctx, tenantID, namespace, payload.Name)
		if err != nil {
			return nil, 0, err
		}
		services = []*supervisor.Service{svc}
	} else {
		svcs, err := h.store.ListServices(ctx, tenantID, namespace)
		if err != nil {
			return nil, 0, err
		}
		services = svcs
	}

	result := ServiceQueryResult{
		Services: services,
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, 0, WrapError(SyscallEINTERNAL, "failed to serialize service query result", err)
	}

	return raw, 0, nil
}
