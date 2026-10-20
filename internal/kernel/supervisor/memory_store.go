package supervisor

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is an in-memory, thread-safe implementation of Store.
type MemoryStore struct {
	mu        sync.RWMutex
	services  map[string]*Service  // key: tenantID + ":" + serviceID
	instances map[string]*Instance // key: tenantID + ":" + serviceID + ":" + instanceID
	clock     func() time.Time
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithClock(func() time.Time {
		return time.Now().UTC()
	})
}

// NewMemoryStoreWithClock creates a MemoryStore with a custom clock.
func NewMemoryStoreWithClock(clock func() time.Time) *MemoryStore {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &MemoryStore{
		services:  make(map[string]*Service),
		instances: make(map[string]*Instance),
		clock:     clock,
	}
}

func serviceKey(tenantID, serviceID string) string {
	return tenantID + ":" + serviceID
}

func instanceKey(tenantID, serviceID, instanceID string) string {
	return tenantID + ":" + serviceID + ":" + instanceID
}

func cloneService(s *Service) *Service {
	if s == nil {
		return nil
	}
	cp := *s
	if s.Spec.Labels != nil {
		cp.Spec.Labels = make(map[string]string, len(s.Spec.Labels))
		for k, v := range s.Spec.Labels {
			cp.Spec.Labels[k] = v
		}
	}
	if s.Spec.WorkloadSpec != nil {
		cp.Spec.WorkloadSpec = make([]byte, len(s.Spec.WorkloadSpec))
		copy(cp.Spec.WorkloadSpec, s.Spec.WorkloadSpec)
	}
	return &cp
}

func cloneInstance(inst *Instance) *Instance {
	if inst == nil {
		return nil
	}
	cp := *inst
	if inst.TaskID != nil {
		id := *inst.TaskID
		cp.TaskID = &id
	}
	cp.LaunchSpec = append([]byte(nil), inst.LaunchSpec...)
	if inst.NextRestartAt != nil {
		t := *inst.NextRestartAt
		cp.NextRestartAt = &t
	}
	if inst.DrainingAt != nil {
		t := *inst.DrainingAt
		cp.DrainingAt = &t
	}
	if inst.ReadyAt != nil {
		t := *inst.ReadyAt
		cp.ReadyAt = &t
	}
	if inst.DrainDeadline != nil {
		t := *inst.DrainDeadline
		cp.DrainDeadline = &t
	}
	if inst.TerminatedAt != nil {
		t := *inst.TerminatedAt
		cp.TerminatedAt = &t
	}
	return &cp
}

// ListAllServices is reserved for the internal controller.
func (m *MemoryStore) ListAllServices(ctx context.Context) ([]*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Service, 0, len(m.services))
	for _, svc := range m.services {
		result = append(result, cloneService(svc))
	}
	return result, nil
}

// CreateService creates an agent service.
func (m *MemoryStore) CreateService(ctx context.Context, svc *Service) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if svc == nil {
		return fmt.Errorf("%w: service cannot be nil", ErrInvalidServiceSpec)
	}
	if err := svc.Validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := serviceKey(svc.TenantID, svc.ID)
	if _, exists := m.services[key]; exists {
		return fmt.Errorf("%w: id %s already exists", ErrServiceAlreadyExists, svc.ID)
	}

	// Check name uniqueness within tenant + namespace
	for _, existing := range m.services {
		if existing.TenantID == svc.TenantID && existing.Namespace == svc.Namespace && existing.Name == svc.Name {
			return fmt.Errorf("%w: name %s already exists in namespace %s", ErrServiceAlreadyExists, svc.Name, svc.Namespace)
		}
	}

	now := m.clock()
	cloned := cloneService(svc)
	if cloned.CreatedAt.IsZero() {
		cloned.CreatedAt = now
	}
	cloned.UpdatedAt = now
	if cloned.Status.LastTransitionAt.IsZero() {
		cloned.Status.LastTransitionAt = now
	}

	m.services[key] = cloned
	return nil
}

// GetService retrieves a service by tenant and ID.
func (m *MemoryStore) GetService(ctx context.Context, tenantID, serviceID string) (*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := serviceKey(tenantID, serviceID)
	svc, ok := m.services[key]
	if !ok {
		return nil, ErrServiceNotFound
	}
	return cloneService(svc), nil
}

// GetServiceByName retrieves a service by tenant, namespace, and name.
func (m *MemoryStore) GetServiceByName(ctx context.Context, tenantID, namespace, name string) (*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, svc := range m.services {
		if svc.TenantID == tenantID && svc.Namespace == namespace && svc.Name == name {
			return cloneService(svc), nil
		}
	}
	return nil, ErrServiceNotFound
}

// UpdateService updates an existing service.
func (m *MemoryStore) UpdateService(ctx context.Context, svc *Service) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if svc == nil {
		return fmt.Errorf("%w: service cannot be nil", ErrInvalidServiceSpec)
	}
	if err := svc.Validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := serviceKey(svc.TenantID, svc.ID)
	existing, ok := m.services[key]
	if !ok {
		return ErrServiceNotFound
	}

	// Check name uniqueness if name changed
	if existing.Name != svc.Name || existing.Namespace != svc.Namespace {
		for _, s := range m.services {
			if s.ID != svc.ID && s.TenantID == svc.TenantID && s.Namespace == svc.Namespace && s.Name == svc.Name {
				return fmt.Errorf("%w: name %s already exists in namespace %s", ErrServiceAlreadyExists, svc.Name, svc.Namespace)
			}
		}
	}

	cloned := cloneService(svc)
	cloned.UpdatedAt = m.clock()
	m.services[key] = cloned
	return nil
}

// DeleteService removes a service and its instances.
func (m *MemoryStore) DeleteService(ctx context.Context, tenantID, serviceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := serviceKey(tenantID, serviceID)
	if _, ok := m.services[key]; !ok {
		return ErrServiceNotFound
	}
	delete(m.services, key)

	// Clean up associated instances
	prefix := tenantID + ":" + serviceID + ":"
	for instKey := range m.instances {
		if len(instKey) >= len(prefix) && instKey[:len(prefix)] == prefix {
			delete(m.instances, instKey)
		}
	}
	return nil
}

// ListServices lists all services in a tenant (and optionally namespace).
func (m *MemoryStore) ListServices(ctx context.Context, tenantID, namespace string) ([]*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]*Service, 0)
	for _, svc := range m.services {
		if svc.TenantID == tenantID {
			if namespace == "" || svc.Namespace == namespace {
				res = append(res, cloneService(svc))
			}
		}
	}
	return res, nil
}

// CreateInstance records a new service instance.
func (m *MemoryStore) CreateInstance(ctx context.Context, inst *Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if inst == nil {
		return fmt.Errorf("%w: instance cannot be nil", ErrInstanceNotFound)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := instanceKey(inst.TenantID, inst.ServiceID, inst.ID)
	if _, ok := m.instances[key]; ok {
		return ErrInstanceAlreadyExists
	}

	now := m.clock()
	cloned := cloneInstance(inst)
	if cloned.CreatedAt.IsZero() {
		cloned.CreatedAt = now
	}
	cloned.UpdatedAt = now
	if cloned.LastHeartbeat.IsZero() {
		cloned.LastHeartbeat = now
	}

	m.instances[key] = cloned
	return nil
}

// GetInstance retrieves an instance.
func (m *MemoryStore) GetInstance(ctx context.Context, tenantID, serviceID, instanceID string) (*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := instanceKey(tenantID, serviceID, instanceID)
	inst, ok := m.instances[key]
	if !ok {
		return nil, ErrInstanceNotFound
	}
	return cloneInstance(inst), nil
}

// UpdateInstance updates an instance's state or heartbeat.
func (m *MemoryStore) UpdateInstance(ctx context.Context, inst *Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if inst == nil {
		return fmt.Errorf("%w: instance cannot be nil", ErrInstanceNotFound)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := instanceKey(inst.TenantID, inst.ServiceID, inst.ID)
	if _, ok := m.instances[key]; !ok {
		return ErrInstanceNotFound
	}

	cloned := cloneInstance(inst)
	cloned.UpdatedAt = m.clock()
	m.instances[key] = cloned
	return nil
}

// DeleteInstance removes an instance.
func (m *MemoryStore) DeleteInstance(ctx context.Context, tenantID, serviceID, instanceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := instanceKey(tenantID, serviceID, instanceID)
	if _, ok := m.instances[key]; !ok {
		return ErrInstanceNotFound
	}
	delete(m.instances, key)
	return nil
}

// ListInstances lists all instances belonging to a service.
func (m *MemoryStore) ListInstances(ctx context.Context, tenantID, serviceID string) ([]*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := tenantID + ":" + serviceID + ":"
	res := make([]*Instance, 0)
	for key, inst := range m.instances {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			res = append(res, cloneInstance(inst))
		}
	}
	return res, nil
}

// ListAllInstances lists all instances for a tenant.
func (m *MemoryStore) ListAllInstances(ctx context.Context, tenantID string) ([]*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]*Instance, 0)
	for _, inst := range m.instances {
		if tenantID == "" || inst.TenantID == tenantID {
			res = append(res, cloneInstance(inst))
		}
	}
	return res, nil
}
