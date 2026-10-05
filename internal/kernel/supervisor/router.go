package supervisor

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
)

// Router resolves logical AgentAddress targets to concrete, healthy Service instances.
type Router struct {
	supervisor *Supervisor
	store      Store
	roundRobin uint64
}

// NewRouter creates a new service-level IPC router.
func NewRouter(supervisor *Supervisor, store Store) *Router {
	return &Router{
		supervisor: supervisor,
		store:      store,
	}
}

// ResolveAddress maps a logical or instance AgentAddress to a running, healthy instance address.
// If the target address already has a specific InstanceID, it verifies that the instance is alive.
// If the target address is logical (empty InstanceID), it finds a healthy instance via round-robin.
// If AutoWake is enabled and no instances are running, it triggers a wakeup pass.
func (r *Router) ResolveAddress(ctx context.Context, target ipc.AgentAddress) (ipc.AgentAddress, error) {
	if err := target.Validate(); err != nil {
		return ipc.AgentAddress{}, err
	}

	// 1. If an explicit instance is targeted, verify its health
	if target.IsInstance() {
		// Find service by agentID
		services, err := r.store.ListServices(ctx, target.TenantID, target.EffectiveNamespace())
		if err != nil {
			return ipc.AgentAddress{}, err
		}
		var matchedService *Service
		for _, s := range services {
			if s.AgentID == target.AgentID {
				matchedService = s
				break
			}
		}
		if matchedService == nil {
			// If not supervised as a service, let the direct address pass through
			return target, nil
		}

		inst, err := r.store.GetInstance(ctx, target.TenantID, matchedService.ID, target.InstanceID)
		if err != nil {
			return ipc.AgentAddress{}, err
		}
		if !inst.IsHealthy() {
			return ipc.AgentAddress{}, fmt.Errorf("%w: instance %s is in state %s", ErrNoHealthyInstance, inst.ID, inst.Phase)
		}
		return inst.Address, nil
	}

	// 2. Logical address: look up the corresponding service
	services, err := r.store.ListServices(ctx, target.TenantID, target.EffectiveNamespace())
	if err != nil {
		return ipc.AgentAddress{}, err
	}
	var matchedService *Service
	for _, s := range services {
		if s.AgentID == target.AgentID {
			matchedService = s
			break
		}
	}
	if matchedService == nil {
		// Not a supervised service; logical target remains unchanged
		return target, nil
	}

	// 3. Find healthy instances of the matched service
	instances, err := r.store.ListInstances(ctx, target.TenantID, matchedService.ID)
	if err != nil {
		return ipc.AgentAddress{}, err
	}

	var healthy []*Instance
	for _, inst := range instances {
		if inst.IsHealthy() && !inst.IsDraining() {
			healthy = append(healthy, inst)
		}
	}

	// 4. If none are healthy and AutoWake is enabled, trigger wakeup
	if len(healthy) == 0 && matchedService.Spec.AutoWake && r.supervisor != nil {
		_ = r.supervisor.ReconcileService(ctx, matchedService)
		// Check instances again after reconcile
		instances, _ = r.store.ListInstances(ctx, target.TenantID, matchedService.ID)
		for _, inst := range instances {
			if inst.IsHealthy() && !inst.IsDraining() {
				healthy = append(healthy, inst)
			}
		}
	}

	if len(healthy) == 0 {
		return ipc.AgentAddress{}, fmt.Errorf("%w: for service %s (agent %s)", ErrNoHealthyInstance, matchedService.Name, matchedService.AgentID)
	}

	// Sort healthy instances deterministically to ensure stable round-robin distribution
	slices.SortFunc(healthy, func(a, b *Instance) int {
		return strings.Compare(a.ID, b.ID)
	})

	// 5. Select instance via round-robin distribution
	idx := atomic.AddUint64(&r.roundRobin, 1) % uint64(len(healthy))
	selected := healthy[idx]

	return selected.Address, nil
}
