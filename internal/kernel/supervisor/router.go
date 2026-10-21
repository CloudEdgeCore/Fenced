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
//
// The routing is version aware. A service runs one agent version until a
// rollout replaces it, and during the rollout it runs two at once. An
// address that names a version therefore reaches exactly the instances of
// that version, and an address that names no instance but a version is
// resolved to a run of that version. Without this, the traffic of a task
// that was admitted against one version would reach an instance of another
// one, and the two versions of an agent do not have to understand each
// other.
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
// If the target address is logical (empty InstanceID), it finds a healthy instance of the
// named version via round-robin.
// If AutoWake is enabled and no instances are running, it triggers a wakeup pass.
func (r *Router) ResolveAddress(ctx context.Context, target ipc.AgentAddress) (ipc.AgentAddress, error) {
	if err := target.Validate(); err != nil {
		return ipc.AgentAddress{}, err
	}

	// 1. If an explicit instance is targeted, verify its health
	if target.IsInstance() {
		matchedService, err := r.findService(ctx, target)
		if err != nil {
			return ipc.AgentAddress{}, err
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
		// An address that names a version must reach that version.
		if target.AgentVersionRef != "" && inst.AgentVersion != "" &&
			!ipc.SameVersion(target.AgentVersionRef, inst.AgentVersion) {
			return ipc.AgentAddress{}, fmt.Errorf("%w: instance %s runs %s, not %s",
				ErrNoHealthyInstance, inst.ID, inst.AgentVersion, target.AgentVersionRef)
		}
		return inst.Address, nil
	}

	// 2. Logical address: look up the corresponding service
	matchedService, err := r.findService(ctx, target)
	if err != nil {
		return ipc.AgentAddress{}, err
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

	healthy := healthyInstances(instances)

	// 4. If none are healthy and AutoWake is enabled, trigger wakeup
	if len(healthy) == 0 && matchedService.Spec.AutoWake && r.supervisor != nil {
		_ = r.supervisor.ReconcileService(ctx, matchedService)
		// Check instances again after reconcile
		instances, _ = r.store.ListInstances(ctx, target.TenantID, matchedService.ID)
		healthy = healthyInstances(instances)
	}

	if len(healthy) == 0 {
		return ipc.AgentAddress{}, fmt.Errorf("%w: for service %s (agent %s)", ErrNoHealthyInstance, matchedService.Name, matchedService.AgentID)
	}

	// 5. Keep the instances of the addressed version. A service that runs two
	// versions at once is exactly what a rollout looks like; the address
	// decides which of them the traffic belongs to.
	if target.AgentVersionRef != "" {
		matching := make([]*Instance, 0, len(healthy))
		for _, inst := range healthy {
			if inst.AgentVersion == "" || ipc.SameVersion(target.AgentVersionRef, inst.AgentVersion) {
				matching = append(matching, inst)
			}
		}
		if len(matching) == 0 {
			return ipc.AgentAddress{}, fmt.Errorf("%w: for service %s (agent %s) no instance runs %s",
				ErrNoHealthyInstance, matchedService.Name, matchedService.AgentID, target.AgentVersionRef)
		}
		healthy = matching
	}

	// Sort healthy instances deterministically to ensure stable round-robin distribution
	slices.SortFunc(healthy, func(a, b *Instance) int {
		return strings.Compare(a.ID, b.ID)
	})

	// 6. Select instance via round-robin distribution
	idx := atomic.AddUint64(&r.roundRobin, 1) % uint64(len(healthy))
	selected := healthy[idx]

	return selected.Address, nil
}

// findService returns the supervised service of an address, or nil when the
// agent is not supervised.
func (r *Router) findService(ctx context.Context, target ipc.AgentAddress) (*Service, error) {
	services, err := r.store.ListServices(ctx, target.TenantID, target.EffectiveNamespace())
	if err != nil {
		return nil, err
	}
	for _, service := range services {
		if service.AgentID == target.AgentID {
			return service, nil
		}
	}
	return nil, nil
}

// healthyInstances returns the instances that can take traffic: running and
// not draining.
func healthyInstances(instances []*Instance) []*Instance {
	healthy := make([]*Instance, 0, len(instances))
	for _, inst := range instances {
		if inst.IsHealthy() && !inst.IsDraining() {
			healthy = append(healthy, inst)
		}
	}
	return healthy
}
