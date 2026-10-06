package provider

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/tokens"
)

// Registry resolves provider names (the first path segment of a model
// reference, e.g. "deepseek" in deepseek/deepseek-chat) to their executors.
// An empty registry fails closed: every reference resolves to an explicit
// "no execution endpoint configured" error rather than silently bypassing
// the Model Gateway.
type Registry struct {
	mu        sync.RWMutex
	executors map[string]*Executor
	routes    map[string]Route
	breaker   CircuitBreaker
}

// NewRegistry builds an empty registry. client may be nil (a default HTTP
// client is used per executor).
func NewRegistry() *Registry {
	return &Registry{executors: map[string]*Executor{}, routes: map[string]Route{}}
}

// Route separates a tenant-visible logical model reference from the provider
// endpoint and wire model selected by operations.
type Route struct {
	ModelRef string `json:"modelRef"`
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// Register adds one provider endpoint configuration. A provider name is
// declared once: registering an existing name fails instead of silently
// replacing the endpoint (a duplicate in static configuration is a mistake,
// not a rotation).
func (r *Registry) Register(config Config) error {
	if strings.TrimSpace(config.Name) == "" {
		return fmt.Errorf("provider name is required")
	}
	if strings.TrimSpace(config.BaseURL) == "" {
		return fmt.Errorf("provider %q: base URL is required", config.Name)
	}
	if !strings.HasPrefix(config.BaseURL, "https://") && !strings.HasPrefix(config.BaseURL, "http://") {
		return fmt.Errorf("provider %q: base URL must be absolute", config.Name)
	}
	if _, err := tokens.ForName(config.Tokenizer); err != nil {
		return fmt.Errorf("provider %q: %w", config.Name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, duplicate := r.executors[config.Name]; duplicate {
		return fmt.Errorf("provider %q is declared twice: remove one entry instead of relying on file order", config.Name)
	}
	r.executors[config.Name] = NewExecutor(config, nil).WithCircuitBreaker(r.breaker)
	return nil
}

// UseCircuitBreaker applies one distributed breaker to current and future
// provider executors.
func (r *Registry) UseCircuitBreaker(breaker CircuitBreaker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.breaker = breaker
	for _, executor := range r.executors {
		executor.WithCircuitBreaker(breaker)
	}
}

// Resolve returns the executor for one provider name.
func (r *Registry) Resolve(name string) (*Executor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[name]
	if !ok {
		return nil, fmt.Errorf("model provider %q has no execution endpoint configured", name)
	}
	return executor, nil
}

// RegisterRoute adds one explicit logical-model route. A modelRef is routed
// once: re-declaring it fails instead of silently overriding the previous
// route.
func (r *Registry) RegisterRoute(route Route) error {
	if _, model, ok := strings.Cut(strings.TrimSpace(route.ModelRef), "/"); !ok || strings.TrimSpace(model) == "" ||
		strings.TrimSpace(route.Provider) == "" || strings.TrimSpace(route.Model) == "" {
		return fmt.Errorf("model route requires modelRef, provider, and wire model")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.executors[route.Provider]; !ok {
		return fmt.Errorf("model route %q references unknown provider %q", route.ModelRef, route.Provider)
	}
	if _, duplicate := r.routes[route.ModelRef]; duplicate {
		return fmt.Errorf("model route %q is declared twice: remove one entry instead of relying on file order", route.ModelRef)
	}
	r.routes[route.ModelRef] = route
	return nil
}

// ResolveModel resolves an exact logical route, falling back to the legacy
// provider/model convention for backwards compatibility.
func (r *Registry) ResolveModel(modelRef string) (*Executor, string, error) {
	r.mu.RLock()
	route, routed := r.routes[modelRef]
	if routed {
		executor := r.executors[route.Provider]
		r.mu.RUnlock()
		if executor == nil {
			return nil, "", fmt.Errorf("model provider %q has no execution endpoint configured", route.Provider)
		}
		return executor, route.Model, nil
	}
	r.mu.RUnlock()
	providerName, modelName, ok := strings.Cut(modelRef, "/")
	if !ok || strings.TrimSpace(providerName) == "" || strings.TrimSpace(modelName) == "" {
		return nil, "", fmt.Errorf("model reference must be provider/model")
	}
	executor, err := r.Resolve(providerName)
	return executor, modelName, err
}

// Names lists registered provider names in stable order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.executors))
	for name := range r.executors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// registryFile is the on-disk shape of the -model-providers configuration:
//
//	{
//	  "providers": [
//	    {"name": "deepseek", "baseUrl": "https://api.deepseek.com/v1",
//	     "apiKeyEnv": "DEEPSEEK_API_KEY", "timeoutMs": 120000,
//	     "maxAttempts": 3, "breakerOpens": 5, "breakerCooldownMs": 30000},
//	    {"name": "qwen", "baseUrl": "https://dashscope.example/v1",
//	     "apiKeyEnv": "QWEN_API_KEY"}
//	  ],
//	  "routes": [
//	    {"modelRef":"logical/fast", "provider":"deepseek", "model":"DeepSeek-V4-Flash"}
//	  ]
//	}
//
// Every provider speaks the OpenAI-compatible /chat/completions dialect.
// API keys are referenced by environment variable name only and are never
// persisted.
type registryFile struct {
	Providers []Config `json:"providers"`
	Routes    []Route  `json:"routes,omitempty"`
}

// LoadRegistryFile reads a provider configuration file. An empty path yields
// an empty (fail-closed) registry.
func LoadRegistryFile(path string) (*Registry, error) {
	registry := NewRegistry()
	if strings.TrimSpace(path) == "" {
		return registry, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read model providers file: %w", err)
	}
	var file registryFile
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("decode model providers file: %w", err)
	}
	for _, config := range file.Providers {
		if config.APIKey != "" {
			return nil, fmt.Errorf("provider %q: apiKey must not be embedded in the file; use apiKeyEnv", config.Name)
		}
		if err := registry.Register(config); err != nil {
			return nil, err
		}
	}
	for _, route := range file.Routes {
		if err := registry.RegisterRoute(route); err != nil {
			return nil, err
		}
	}
	return registry, nil
}
