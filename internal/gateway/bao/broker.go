// Package bao implements the reference Secret Broker backed by OpenBao
// (ADR-012). Secrets live in KV v2 under
//
//	<mount>/fenced/<tenant>/<tool>/<resource>
//
// and are read on demand per invocation scope; issued handles are cached for
// a bounded TTL so the gateway does not hammer the broker on every tool call.
// The secret never leaves the gateway process: the handle is injected into
// the executing adapter only, and the gateway redacts it from results.
package bao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

var (
	// ErrSecretNotFound reports a scope that has no secret material in the
	// broker. The gateway fails the invocation closed.
	ErrSecretNotFound = errors.New("openbao secret not found for scope")
	// ErrSecretUnavailable reports the broker being unreachable or denying
	// the read; the invocation fails closed rather than running without a
	// scoped credential.
	ErrSecretUnavailable = errors.New("openbao secret broker unavailable")
)

// DefaultMount is the KV v2 mount the broker reads.
const DefaultMount = "secret"

// Broker issues scope-limited secret handles from OpenBao KV v2.
type Broker struct {
	addr        string
	token       string
	namespace   string
	mount       string
	client      *http.Client
	cacheTTL    time.Duration
	now         func() time.Time
	mu          sync.Mutex
	cache       map[string]cacheEntry
	requestPath func(tool.SecretScope) string
}

type cacheEntry struct {
	handle   tool.SecretHandle
	expires  time.Time
	attempts int
}

// sharedOptions are the settings both broker flavors accept.
type sharedOptions struct {
	namespace string
	mount     string
	cacheTTL  time.Duration
	client    *http.Client
}

// Option configures a broker.
type Option func(*sharedOptions)

// WithNamespace sets the OpenBao namespace header (enterprise namespaces /
// OpenBao namespaces when enabled).
func WithNamespace(namespace string) Option {
	return func(o *sharedOptions) { o.namespace = namespace }
}

// WithMount overrides the KV v2 mount path (default "secret").
func WithMount(mount string) Option {
	return func(o *sharedOptions) { o.mount = strings.Trim(mount, "/") }
}

// WithCacheTTL bounds how long an issued handle is reused for the same scope
// (default 30 seconds). Zero disables caching.
func WithCacheTTL(ttl time.Duration) Option {
	return func(o *sharedOptions) { o.cacheTTL = ttl }
}

// WithHTTPClient injects the HTTP client (tests and custom transport).
func WithHTTPClient(client *http.Client) Option {
	return func(o *sharedOptions) { o.client = client }
}

// NewBroker connects the broker to an OpenBao endpoint with the given token.
func NewBroker(addr, token string, options ...Option) (*Broker, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("openbao address is required")
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("openbao token is required")
	}
	config := sharedOptions{mount: DefaultMount, cacheTTL: 30 * time.Second, client: &http.Client{Timeout: 10 * time.Second}}
	for _, option := range options {
		option(&config)
	}
	broker := &Broker{
		addr:      strings.TrimRight(addr, "/"),
		token:     token,
		namespace: config.namespace,
		mount:     config.mount,
		client:    config.client,
		cacheTTL:  config.cacheTTL,
		now:       time.Now,
		cache:     map[string]cacheEntry{},
	}
	if broker.client == nil {
		broker.client = &http.Client{Timeout: 10 * time.Second}
	}
	return broker, nil
}

// Ping verifies the broker endpoint is reachable and the token is accepted.
func (b *Broker) Ping(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.addr+"/v1/sys/health", nil)
	if err != nil {
		return fmt.Errorf("openbao health request: %w", err)
	}
	b.authorize(request)
	response, err := b.client.Do(request)
	if err != nil {
		return fmt.Errorf("openbao health check: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusTooEarly || response.StatusCode == http.StatusUnprocessableEntity {
		// 200 initialized+unsealed; 429/472/473 mean initialized but sealed
		// or standby — still a reachable broker.
		return nil
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return fmt.Errorf("openbao token was rejected: %s", response.Status)
	}
	return fmt.Errorf("openbao health check returned %s", response.Status)
}

// Issue returns the secret material bound to the invocation scope, caching it
// for the bounded TTL. Fail-closed: any broker error fails the scope.
func (b *Broker) Issue(ctx context.Context, scope tool.SecretScope) (tool.SecretHandle, error) {
	key := scopeKey(scope)
	if b.cacheTTL > 0 {
		b.mu.Lock()
		if entry, ok := b.cache[key]; ok && b.now().Before(entry.expires) {
			b.mu.Unlock()
			entry.attempts++
			return entry.handle, nil
		}
		b.mu.Unlock()
	}
	handle, err := b.fetch(ctx, scope)
	if err != nil {
		return "", err
	}
	if b.cacheTTL > 0 {
		b.mu.Lock()
		b.cache[key] = cacheEntry{handle: handle, expires: b.now().Add(b.cacheTTL)}
		b.mu.Unlock()
	}
	slog.Info("issued openbao scoped secret handle", "tenant", scope.TenantID, "tool", scope.ToolName, "resource", scope.Resource, "cacheTtl", b.cacheTTL.String())
	return handle, nil
}

// fetch reads the KV v2 secret for the scope and renders its material: the
// string value of the "value" key when present, otherwise the compact JSON of
// the data object.
func (b *Broker) fetch(ctx context.Context, scope tool.SecretScope) (tool.SecretHandle, error) {
	path := b.scopePath(scope)
	requestURL := b.addr + "/v1/" + b.mount + "/data/" + path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", fmt.Errorf("%w: build request: %v", ErrSecretUnavailable, err)
	}
	b.authorize(request)
	response, err := b.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSecretUnavailable, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("%w: read response: %v", ErrSecretUnavailable, err)
	}
	switch {
	case response.StatusCode == http.StatusNotFound:
		return "", fmt.Errorf("%w: mount=%s path=%s", ErrSecretNotFound, b.mount, path)
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return "", fmt.Errorf("%w: token lacks read on %s: %s", ErrSecretUnavailable, path, response.Status)
	case response.StatusCode != http.StatusOK:
		return "", fmt.Errorf("%w: %s for %s: %s", ErrSecretUnavailable, response.Status, path, firstLine(body))
	}
	var envelope struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "", fmt.Errorf("%w: decode secret: %v", ErrSecretUnavailable, err)
	}
	if len(envelope.Data.Data) == 0 {
		return "", fmt.Errorf("%w: empty secret at %s", ErrSecretNotFound, path)
	}
	if value, ok := envelope.Data.Data["value"].(string); ok {
		return tool.SecretHandle(value), nil
	}
	rendered, err := json.Marshal(envelope.Data.Data)
	if err != nil {
		return "", fmt.Errorf("%w: render secret: %v", ErrSecretUnavailable, err)
	}
	return tool.SecretHandle(rendered), nil
}

// scopePath renders the KV v2 key path for a scope. The resource is escaped
// so tool resources containing slashes (fs:/tmp) stay one path segment.
func (b *Broker) scopePath(scope tool.SecretScope) string {
	if b.requestPath != nil {
		return b.requestPath(scope)
	}
	resource := scope.Resource
	if strings.TrimSpace(scope.SecretRef) != "" {
		resource = scope.SecretRef
	}
	segments := []string{"fenced", scope.TenantID, scope.ToolName, url.PathEscape(resource)}
	return strings.Join(segments, "/")
}

func (b *Broker) authorize(request *http.Request) {
	request.Header.Set("X-Vault-Token", b.token)
	if b.namespace != "" {
		request.Header.Set("X-Vault-Namespace", b.namespace)
	}
}

func scopeKey(scope tool.SecretScope) string {
	return scope.TenantID + "\x00" + scope.ToolName + "\x00" + scope.Resource + "\x00" + scope.SecretRef
}

func firstLine(body []byte) string {
	if index := strings.IndexByte(string(body), '\n'); index >= 0 {
		return string(body[:index])
	}
	return string(body)
}
