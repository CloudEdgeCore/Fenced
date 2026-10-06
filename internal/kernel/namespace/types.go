package namespace

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// DefaultNamespace is the standard fallback namespace for all resources.
const DefaultNamespace = "default"

var (
	// rfc1123Regex validates that a namespace name is a valid DNS-1123 label:
	// lowercase alphanumeric and hyphens, 1-63 characters, start/end with alphanumeric.
	rfc1123Regex = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
)

// NamespacePhase represents the operational lifecycle state of a namespace.
type NamespacePhase string

const (
	// NamespacePhaseActive indicates the namespace is healthy and accepts workloads.
	NamespacePhaseActive NamespacePhase = "Active"
	// NamespacePhaseTerminating indicates the namespace is being deleted; new resources are rejected.
	NamespacePhaseTerminating NamespacePhase = "Terminating"
)

// ResourceQuota defines hard resource limits and policy gates enforced within a namespace.
// Any dimension <= 0 is treated as unlimited.
type ResourceQuota struct {
	MaxServices            int   `json:"max_services,omitempty"`
	MaxReplicasPerService  int   `json:"max_replicas_per_service,omitempty"`
	MaxTasks               int   `json:"max_tasks,omitempty"`
	MaxTokens              int64 `json:"max_tokens,omitempty"`
	MaxCostMicroUSD        int64 `json:"max_cost_micro_usd,omitempty"`
	MaxToolCalls           int64 `json:"max_tool_calls,omitempty"`
	MaxWallSeconds         int64 `json:"max_wall_seconds,omitempty"`
	MaxIPCMailboxMessages  int   `json:"max_ipc_mailbox_messages,omitempty"`
	AllowCrossNamespaceIPC bool  `json:"allow_cross_namespace_ipc,omitempty"`
}

// ResourceUsage tracks the current live and settled resource consumption in a namespace.
type ResourceUsage struct {
	TenantID             string    `json:"tenant_id"`
	Namespace            string    `json:"namespace"`
	ActiveServices       int       `json:"active_services"`
	ActiveInstances      int       `json:"active_instances"`
	ActiveTasks          int       `json:"active_tasks"`
	MailboxMessages      int       `json:"mailbox_messages"`
	ConsumedTokens       int64     `json:"consumed_tokens"`
	ConsumedCostMicroUSD int64     `json:"consumed_cost_micro_usd"`
	ConsumedToolCalls    int64     `json:"consumed_tool_calls"`
	ConsumedWallSeconds  int64     `json:"consumed_wall_seconds"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// ResourceUsageDelta represents incremental changes to resource usage counters.
type ResourceUsageDelta struct {
	ActiveServicesDelta       int
	ActiveInstancesDelta      int
	ActiveTasksDelta          int
	MailboxMessagesDelta      int
	ConsumedTokensDelta       int64
	ConsumedCostMicroUSDDelta int64
	ConsumedToolCallsDelta    int64
	ConsumedWallSecondsDelta  int64
}

// Namespace is a first-class isolation and resource management boundary in Fenced.
type Namespace struct {
	TenantID    string            `json:"tenant_id"`
	Name        string            `json:"name"`
	DisplayName string            `json:"display_name,omitempty"`
	Description string            `json:"description,omitempty"`
	Phase       NamespacePhase    `json:"phase"`
	Labels      map[string]string `json:"labels,omitempty"`
	Quota       ResourceQuota     `json:"quota"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// NamespaceSpec contains parameters for creating or updating a Namespace.
type NamespaceSpec struct {
	TenantID    string            `json:"tenant_id"`
	Name        string            `json:"name"`
	DisplayName string            `json:"display_name,omitempty"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Quota       ResourceQuota     `json:"quota"`
}

// ValidateNamespaceName ensures a namespace name conforms strictly to RFC 1123 DNS-1123 label rules.
func ValidateNamespaceName(name string) error {
	name = strings.TrimSpace(name)
	if len(name) == 0 {
		return fmt.Errorf("%w: name cannot be empty", ErrInvalidNamespaceName)
	}
	if len(name) > 63 {
		return fmt.Errorf("%w: name length %d exceeds maximum of 63 characters", ErrInvalidNamespaceName, len(name))
	}
	if !rfc1123Regex.MatchString(name) {
		return fmt.Errorf("%w: %q must be lowercase alphanumeric and hyphens, and start/end with an alphanumeric character", ErrInvalidNamespaceName, name)
	}
	return nil
}

// NewDefaultNamespace returns a standard active default namespace for a tenant.
func NewDefaultNamespace(tenantID string) *Namespace {
	now := time.Now().UTC()
	return &Namespace{
		TenantID:    strings.TrimSpace(tenantID),
		Name:        DefaultNamespace,
		DisplayName: "Default Namespace",
		Description: "System default workload and resource boundary",
		Phase:       NamespacePhaseActive,
		Labels: map[string]string{
			"fenced.dev/system": "true",
		},
		Quota: ResourceQuota{
			AllowCrossNamespaceIPC: true,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}
