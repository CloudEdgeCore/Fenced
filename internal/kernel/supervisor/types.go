package supervisor

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/google/uuid"
)

// ServicePhase indicates the lifecycle state of an AgentService.
type ServicePhase string

const (
	ServicePending    ServicePhase = "Pending"
	ServiceActive     ServicePhase = "Active"
	ServiceDegraded   ServicePhase = "Degraded"
	ServiceSuspended  ServicePhase = "Suspended"
	ServiceFailed     ServicePhase = "Failed"
	ServiceTerminated ServicePhase = "Terminated"
)

// RestartPolicy defines how the supervisor handles crashed instances.
type RestartPolicy string

const (
	RestartAlways    RestartPolicy = "Always"
	RestartOnFailure RestartPolicy = "OnFailure"
	RestartNever     RestartPolicy = "Never"
)

// InstancePhase indicates the operational state of an individual service instance.
type InstancePhase string

const (
	InstanceCreated    InstancePhase = "Created"
	InstanceStarting   InstancePhase = "Starting"
	InstanceRunning    InstancePhase = "Running"
	InstanceDegraded   InstancePhase = "Degraded"
	InstanceDraining   InstancePhase = "Draining"
	InstanceStopping   InstancePhase = "Stopping"
	InstanceStopped    InstancePhase = "Stopped"
	InstanceFailed     InstancePhase = "Failed"
	InstanceRecovering InstancePhase = "Recovering"
)

// ResourceRequirements defines CPU, Memory, and LLM resource allocations.
type ResourceRequirements struct {
	CPU      string `json:"cpu,omitempty"`
	Memory   string `json:"memory,omitempty"`
	LLMSlots int    `json:"llmSlots,omitempty"`
}

// RolloutConfig controls progressive updates and rollouts.
type RolloutConfig struct {
	MaxUnavailable int           `json:"maxUnavailable"`
	MaxSurge       int           `json:"maxSurge"`
	MinReadyTime   time.Duration `json:"minReadyTime,omitempty"`
}

// DefaultRolloutConfig returns default rollout settings (maxUnavailable=1, maxSurge=1).
func DefaultRolloutConfig() RolloutConfig {
	return RolloutConfig{
		MaxUnavailable: 1,
		MaxSurge:       1,
	}
}

// BackoffConfig defines exponential backoff settings for instance restarts.
type BackoffConfig struct {
	InitialInterval time.Duration `json:"initialInterval"`
	MaxInterval     time.Duration `json:"maxInterval"`
	Factor          float64       `json:"factor"`
	MaxRetries      int           `json:"maxRetries"` // 0 = unlimited retries
}

// DefaultBackoffConfig returns sensible defaults for exponential backoff.
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		Factor:          2.0,
		MaxRetries:      5,
	}
}

// CalculateDelay computes the backoff duration for a given restart count.
func (b BackoffConfig) CalculateDelay(restartCount int) time.Duration {
	if restartCount <= 0 {
		return b.InitialInterval
	}
	initial := b.InitialInterval
	if initial <= 0 {
		initial = 500 * time.Millisecond
	}
	factor := b.Factor
	if factor < 1.0 {
		factor = 2.0
	}
	maxInterval := b.MaxInterval
	if maxInterval <= 0 {
		maxInterval = 30 * time.Second
	}

	delay := float64(initial) * math.Pow(factor, float64(restartCount))
	if delay > float64(maxInterval) || math.IsInf(delay, 0) {
		return maxInterval
	}
	return time.Duration(delay)
}

// HealthConfig configures heartbeat timeouts and failure thresholds.
type HealthConfig struct {
	HeartbeatTTL       time.Duration `json:"heartbeatTtl"`
	UnhealthyThreshold int           `json:"unhealthyThreshold"`
	CheckInterval      time.Duration `json:"checkInterval"`
}

// DefaultHealthConfig returns sensible defaults for instance health checks.
func DefaultHealthConfig() HealthConfig {
	return HealthConfig{
		HeartbeatTTL:       30 * time.Second,
		UnhealthyThreshold: 3,
		CheckInterval:      5 * time.Second,
	}
}

// ServiceSpec declares the desired state and policy configuration for an AgentService.
type ServiceSpec struct {
	Replicas        int                  `json:"replicas"`
	AgentVersion    string               `json:"agentVersion,omitempty"`
	AgentVersionRef string               `json:"agentVersionRef,omitempty"`
	RuntimeClass    string               `json:"runtimeClass,omitempty"`
	RestartPolicy   RestartPolicy        `json:"restartPolicy"`
	Resources       ResourceRequirements `json:"resources,omitempty"`
	Backoff         BackoffConfig        `json:"backoff"`
	Health          HealthConfig         `json:"health"`
	Rollout         RolloutConfig        `json:"rollout"`
	DrainTimeout    time.Duration        `json:"drainTimeout,omitempty"`
	AutoWake        bool                 `json:"autoWake"`
	ScaleToZeroTTL  time.Duration        `json:"scaleToZeroTtl,omitempty"`
	Labels          map[string]string    `json:"labels,omitempty"`
	WorkloadSpec    json.RawMessage      `json:"workloadSpec,omitempty"`
}

// Validate checks the service specification for correctness.
func (s *ServiceSpec) Validate() error {
	if s.Replicas < 0 {
		return fmt.Errorf("%w: replicas must be non-negative", ErrInvalidServiceSpec)
	}
	if s.RestartPolicy == "" {
		s.RestartPolicy = RestartAlways
	}
	switch s.RestartPolicy {
	case RestartAlways, RestartOnFailure, RestartNever:
	default:
		return fmt.Errorf("%w: invalid restart policy %q", ErrInvalidServiceSpec, s.RestartPolicy)
	}

	if s.Backoff.InitialInterval <= 0 {
		s.Backoff.InitialInterval = 500 * time.Millisecond
	}
	if s.Backoff.MaxInterval <= 0 {
		s.Backoff.MaxInterval = 30 * time.Second
	}
	if s.Backoff.Factor < 1.0 {
		s.Backoff.Factor = 2.0
	}
	if s.Backoff.MaxRetries < 0 {
		return fmt.Errorf("%w: maxRetries cannot be negative", ErrInvalidServiceSpec)
	}

	if s.Health.HeartbeatTTL <= 0 {
		s.Health.HeartbeatTTL = 30 * time.Second
	}
	if s.Health.UnhealthyThreshold <= 0 {
		s.Health.UnhealthyThreshold = 3
	}
	if s.Health.CheckInterval <= 0 {
		s.Health.CheckInterval = 5 * time.Second
	}

	if s.Rollout.MaxUnavailable <= 0 {
		s.Rollout.MaxUnavailable = 1
	}
	if s.Rollout.MaxSurge <= 0 {
		s.Rollout.MaxSurge = 1
	}
	if s.DrainTimeout < 0 {
		s.DrainTimeout = 0
	}

	return nil
}

// ServiceStatus tracks the observed state of an AgentService.
type ServiceStatus struct {
	Phase             ServicePhase `json:"phase"`
	DesiredReplicas   int          `json:"desiredReplicas"`
	CurrentReplicas   int          `json:"currentReplicas"`
	ReadyReplicas     int          `json:"readyReplicas"`
	UpdatedReplicas   int          `json:"updatedReplicas"`
	AvailableReplicas int          `json:"availableReplicas"`
	ActiveVersion     string       `json:"activeVersion,omitempty"`
	PreviousVersion   string       `json:"previousVersion,omitempty"`
	RestartCount      int          `json:"restartCount"`
	LastTransitionAt  time.Time    `json:"lastTransitionAt"`
	Message           string       `json:"message,omitempty"`
}

// Service represents a supervised long-running agent daemon.
type Service struct {
	ID        string        `json:"id"`
	TenantID  string        `json:"tenantId"`
	Namespace string        `json:"namespace"`
	Name      string        `json:"name"`
	AgentID   string        `json:"agentId"`
	Spec      ServiceSpec   `json:"spec"`
	Status    ServiceStatus `json:"status"`
	CreatedAt time.Time     `json:"createdAt"`
	UpdatedAt time.Time     `json:"updatedAt"`
}

// Validate verifies that the service has valid tenant, namespace, agent IDs, and spec.
func (s *Service) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("%w: service ID is required", ErrInvalidServiceSpec)
	}
	if strings.TrimSpace(s.TenantID) == "" {
		return fmt.Errorf("%w: tenant ID is required", ErrInvalidServiceSpec)
	}
	if strings.TrimSpace(s.AgentID) == "" {
		return fmt.Errorf("%w: agent ID is required", ErrInvalidServiceSpec)
	}
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("%w: service name is required", ErrInvalidServiceSpec)
	}
	if s.Namespace == "" {
		s.Namespace = "default"
	}
	return s.Spec.Validate()
}

// LogicalAddress returns the service's base logical IPC address.
func (s *Service) LogicalAddress() ipc.AgentAddress {
	return ipc.NewAddress(s.TenantID, s.Namespace, s.AgentID)
}

// Instance represents a running or terminating process instance of an AgentService.
type Instance struct {
	ID                  string           `json:"id"`
	ServiceID           string           `json:"serviceId"`
	TenantID            string           `json:"tenantId"`
	Namespace           string           `json:"namespace"`
	AgentID             string           `json:"agentId"`
	AgentVersion        string           `json:"agentVersion,omitempty"`
	RuntimeClass        string           `json:"runtimeClass,omitempty"`
	FencingToken        uint64           `json:"fencingToken,omitempty"`
	TaskID              *uuid.UUID       `json:"taskId,omitempty"`
	LaunchSpec          json.RawMessage  `json:"launchSpec,omitempty"`
	Address             ipc.AgentAddress `json:"address"`
	Phase               InstancePhase    `json:"phase"`
	RestartCount        int              `json:"restartCount"`
	ConsecutiveFailures int              `json:"consecutiveFailures"`
	NextRestartAt       *time.Time       `json:"nextRestartAt,omitempty"`
	LastHeartbeat       time.Time        `json:"lastHeartbeat"`
	DrainingAt          *time.Time       `json:"drainingAt,omitempty"`
	DrainDeadline       *time.Time       `json:"drainDeadline,omitempty"`
	CreatedAt           time.Time        `json:"createdAt"`
	UpdatedAt           time.Time        `json:"updatedAt"`
	TerminatedAt        *time.Time       `json:"terminatedAt,omitempty"`
	ExitCode            int              `json:"exitCode,omitempty"`
	ExitReason          string           `json:"exitReason,omitempty"`
}

// IsHealthy returns true if the instance is currently running and active.
func (inst *Instance) IsHealthy() bool {
	return inst.Phase == InstanceRunning
}

// IsActive returns true if the instance is in an active lifecycle state.
func (inst *Instance) IsActive() bool {
	return inst.Phase == InstanceCreated ||
		inst.Phase == InstanceStarting ||
		inst.Phase == InstanceRunning ||
		inst.Phase == InstanceDegraded
}

// IsDraining returns true if the instance is currently draining.
func (inst *Instance) IsDraining() bool {
	return inst.Phase == InstanceDraining
}

// IsTerminal returns true if the instance has exited and cannot be reused.
func (inst *Instance) IsTerminal() bool {
	return inst.Phase == InstanceStopped || inst.Phase == InstanceFailed
}

// IsRecovering returns true if the instance is waiting for a restart backoff.
func (inst *Instance) IsRecovering() bool {
	return inst.Phase == InstanceRecovering || (inst.Phase == InstanceFailed && inst.NextRestartAt != nil)
}
