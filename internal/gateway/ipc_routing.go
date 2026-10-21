package gateway

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
)

// ServiceInstanceResolver maps a logical agent target to a concrete run of
// the named version.
//
// A logical target names the agent version, not a run. The platform resolves
// it to a run that may take the traffic, and the resolution is version
// aware: a service that runs two versions at once during a rollout only
// offers the instances of the addressed version. Without that, a message
// that was addressed to one version would land in the mailbox of another one.
type ServiceInstanceResolver interface {
	// ResolveServiceInstance returns the run and the version of an instance
	// that may take the traffic of a logical target. The key spreads the
	// traffic over the instances of the version; the same key always picks
	// the same instance, so a retried message lands in the same mailbox.
	ResolveServiceInstance(ctx context.Context, tenantID, versionRef, key string) (uuid.UUID, string, error)
}

// ServiceInstanceStore is the narrow read surface the resolver needs.
type ServiceInstanceStore interface {
	ListServiceInstancesByAgent(ctx context.Context, tenantID, namespace, agentID string) ([]*supervisor.Instance, error)
}

// storeInstanceResolver resolves a logical target over the durable instances.
type storeInstanceResolver struct {
	store ServiceInstanceStore
}

// NewServiceInstanceResolver returns a resolver over the durable instances.
func NewServiceInstanceResolver(store ServiceInstanceStore) ServiceInstanceResolver {
	return &storeInstanceResolver{store: store}
}

func (r *storeInstanceResolver) ResolveServiceInstance(ctx context.Context, tenantID, versionRef, key string) (uuid.UUID, string, error) {
	namespace, name, _, err := agentversion.ParseRef(strings.TrimSpace(versionRef))
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("logical target: %w", err)
	}
	instances, err := r.store.ListServiceInstancesByAgent(ctx, tenantID, namespace, name)
	if err != nil {
		return uuid.Nil, "", err
	}

	candidates := make([]*supervisor.Instance, 0, len(instances))
	for _, inst := range instances {
		// A running instance is one that may take traffic. A draining or
		// failing instance does not.
		if inst.Phase != supervisor.InstanceRunning || inst.TaskID == nil {
			continue
		}
		if inst.AgentVersion != "" && !ipc.SameVersion(versionRef, inst.AgentVersion) {
			continue
		}
		candidates = append(candidates, inst)
	}
	if len(candidates) == 0 {
		return uuid.Nil, "", fmt.Errorf("no instance of %s is running", versionRef)
	}
	slices.SortFunc(candidates, func(left, right *supervisor.Instance) int {
		return strings.Compare(left.ID, right.ID)
	})
	chosen := candidates[instanceIndex(key, len(candidates))]
	return *chosen.TaskID, chosen.AgentVersion, nil
}

// instanceIndex spreads the traffic over the instances of a version.
func instanceIndex(key string, count int) int {
	if count <= 1 || key == "" {
		return 0
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(count))
}
