// Package provider defines the unified Fenced Provider Plugin Protocol.
// It allows Model, Tool, Memory, Browser, Storage, and Runtime providers
// to be developed, registered, and discovered without modifying the Kernel.
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ProviderType identifies the category of capability a provider supplies.
type ProviderType string

const (
	TypeModel   ProviderType = "model"
	TypeTool    ProviderType = "tool"
	TypeMemory  ProviderType = "memory"
	TypeBrowser ProviderType = "browser"
	TypeStorage ProviderType = "storage"
	TypeRuntime ProviderType = "runtime"
)

// HealthStatus reports the operational state of a provider.
type HealthStatus struct {
	Status    string             `json:"status"` // HEALTHY, DEGRADED, UNHEALTHY
	Message   string             `json:"message,omitempty"`
	Timestamp time.Time          `json:"timestamp"`
	Metrics   map[string]float64 `json:"metrics,omitempty"`
}

// ResourceRequirements declares CPU, memory, and acceleration needed by the provider.
type ResourceRequirements struct {
	CPU     string `json:"cpu,omitempty"`     // e.g. "500m", "2"
	Memory  string `json:"memory,omitempty"`  // e.g. "512Mi", "4Gi"
	GPU     string `json:"gpu,omitempty"`     // e.g. "1" (NVIDIA / Metal)
	Storage string `json:"storage,omitempty"` // e.g. "10Gi"
}

// Manifest defines the formal metadata contract every Provider must declare.
type Manifest struct {
	Name                 string               `json:"name"`
	Version              string               `json:"version"`
	Type                 ProviderType         `json:"type"`
	Capabilities         []string             `json:"capabilities"`
	ConfigSchema         json.RawMessage      `json:"configSchema,omitempty"`
	Secrets              []string             `json:"secrets,omitempty"`
	ResourceRequirements ResourceRequirements `json:"resourceRequirements,omitempty"`
}

// Validate checks the structural integrity of the provider manifest.
func (m Manifest) Validate() error {
	if strings.TrimSpace(m.Name) == "" {
		return errors.New("provider manifest: name is required")
	}
	if strings.TrimSpace(m.Version) == "" {
		return errors.New("provider manifest: version is required")
	}
	if m.Type == "" {
		return errors.New("provider manifest: type is required")
	}
	switch m.Type {
	case TypeModel, TypeTool, TypeMemory, TypeBrowser, TypeStorage, TypeRuntime:
	default:
		return fmt.Errorf("provider manifest: unknown provider type %q", m.Type)
	}
	if len(m.Capabilities) == 0 {
		return errors.New("provider manifest: at least one capability must be declared")
	}
	if len(m.ConfigSchema) > 0 && !json.Valid(m.ConfigSchema) {
		return errors.New("provider manifest: configSchema must be valid JSON")
	}
	return nil
}

// Provider is the base interface implemented by all Fenced providers.
type Provider interface {
	Manifest() Manifest
	Health(ctx context.Context) HealthStatus
	Close() error
}
