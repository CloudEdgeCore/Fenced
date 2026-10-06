package bao

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

// DynamicCredentials are the username/password material issued by the
// OpenBao database secrets engine. The handle the gateway injects into the
// executing adapter is the JSON rendering of this struct.
type DynamicCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// leaseState tracks one issued credential lease for renewal/revocation.
type leaseState struct {
	leaseID    string
	renewable  bool
	expiresAt  time.Time
	renewCount int
}

// DynamicBroker is the reference Secret Broker over the OpenBao database
// secrets engine (ADR-015): every issuance is a dynamic credential with a
// lease, renewed while the gateway needs it and revoked on Close or expiry —
// the credential can never outlive the gateway process.
type DynamicBroker struct {
	addr      string
	token     string
	namespace string
	role      string
	client    *http.Client
	cacheTTL  time.Duration
	renewAt   time.Duration // fraction of remaining TTL to renew at
	now       func() time.Time
	mu        sync.Mutex
	cache     map[string]cacheEntry
	leases    map[string]*leaseState
	closed    bool
}

// NewDynamicBroker connects the broker to the database secrets engine role
// database/creds/<role>.
func NewDynamicBroker(addr, token, role string, options ...Option) (*DynamicBroker, error) {
	if strings.TrimSpace(addr) == "" || strings.TrimSpace(token) == "" || strings.TrimSpace(role) == "" {
		return nil, fmt.Errorf("openbao address, token, and database role are required")
	}
	config := sharedOptions{cacheTTL: 30 * time.Second, client: &http.Client{Timeout: 10 * time.Second}}
	for _, option := range options {
		option(&config)
	}
	broker := &DynamicBroker{
		addr:      strings.TrimRight(addr, "/"),
		token:     token,
		namespace: config.namespace,
		role:      strings.Trim(role, "/"),
		client:    config.client,
		cacheTTL:  config.cacheTTL,
		renewAt:   2 * time.Minute,
		now:       time.Now,
		cache:     map[string]cacheEntry{},
		leases:    map[string]*leaseState{},
	}
	if broker.client == nil {
		broker.client = &http.Client{Timeout: 10 * time.Second}
	}
	return broker, nil
}

// Issue returns dynamic credentials for the invocation scope, cached for the
// bounded TTL. Each fresh issuance registers a lease that the janitor renews
// and Close revokes.
func (b *DynamicBroker) Issue(ctx context.Context, scope tool.SecretScope) (tool.SecretHandle, error) {
	key := scopeKey(scope)
	if b.cacheTTL > 0 {
		b.mu.Lock()
		if entry, ok := b.cache[key]; ok && b.now().Before(entry.expires) {
			b.mu.Unlock()
			return entry.handle, nil
		}
		b.mu.Unlock()
	}
	credentials, leaseID, renewable, leaseTTL, err := b.fetch(ctx)
	if err != nil {
		return "", err
	}
	rendered, err := json.Marshal(credentials)
	if err != nil {
		return "", fmt.Errorf("%w: encode credentials: %v", ErrSecretUnavailable, err)
	}
	handle := tool.SecretHandle(rendered)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		// The broker is shutting down: revoke what was just issued.
		revokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.revoke(revokeCtx, leaseID)
		return "", fmt.Errorf("%w: broker is closed", ErrSecretUnavailable)
	}
	if b.cacheTTL > 0 {
		b.cache[key] = cacheEntry{handle: handle, expires: b.now().Add(b.cacheTTL)}
	}
	b.leases[leaseID] = &leaseState{
		leaseID: leaseID, renewable: renewable,
		expiresAt: b.now().Add(time.Duration(float64(leaseTTL) * 0.7)), // renew before expiry
	}
	slog.Info("issued openbao dynamic credential", "tenant", scope.TenantID, "tool", scope.ToolName, "resource", scope.Resource, "lease", leaseID)
	return handle, nil
}

// fetch calls database/creds/<role>.
func (b *DynamicBroker) fetch(ctx context.Context) (DynamicCredentials, string, bool, time.Duration, error) {
	var zero DynamicCredentials
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.addr+"/v1/database/creds/"+b.role, nil)
	if err != nil {
		return zero, "", false, 0, fmt.Errorf("%w: build request: %v", ErrSecretUnavailable, err)
	}
	b.authorize(request)
	response, err := b.client.Do(request)
	if err != nil {
		return zero, "", false, 0, fmt.Errorf("%w: %v", ErrSecretUnavailable, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return zero, "", false, 0, fmt.Errorf("%w: read response: %v", ErrSecretUnavailable, err)
	}
	switch {
	case response.StatusCode == http.StatusNotFound:
		return zero, "", false, 0, fmt.Errorf("%w: role %s does not exist", ErrSecretNotFound, b.role)
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return zero, "", false, 0, fmt.Errorf("%w: token lacks read on database/creds/%s: %s", ErrSecretUnavailable, b.role, response.Status)
	case response.StatusCode != http.StatusOK:
		return zero, "", false, 0, fmt.Errorf("%w: %s for database/creds/%s", ErrSecretUnavailable, response.Status, b.role)
	}
	var envelope struct {
		LeaseID       string `json:"lease_id"`
		LeaseDuration int64  `json:"lease_duration"`
		Renewable     bool   `json:"renewable"`
		Data          struct {
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return zero, "", false, 0, fmt.Errorf("%w: decode credentials: %v", ErrSecretUnavailable, err)
	}
	if envelope.LeaseID == "" || envelope.Data.Username == "" || envelope.Data.Password == "" {
		return zero, "", false, 0, fmt.Errorf("%w: incomplete credentials response", ErrSecretUnavailable)
	}
	return DynamicCredentials{Username: envelope.Data.Username, Password: envelope.Data.Password},
		envelope.LeaseID, envelope.Renewable, time.Duration(envelope.LeaseDuration) * time.Second, nil
}

// Janitor renews leases approaching expiry and revokes leases that can no
// longer be renewed, so credentials never outlive their usefulness.
func (b *DynamicBroker) Janitor(ctx context.Context) error {
	b.mu.Lock()
	dueRenewal := []*leaseState{}
	dueRevoke := []*leaseState{}
	now := b.now()
	for _, lease := range b.leases {
		if lease.expiresAt.Before(now) {
			dueRevoke = append(dueRevoke, lease)
			continue
		}
		if lease.renewable && now.Add(b.renewAt).After(lease.expiresAt) {
			dueRenewal = append(dueRenewal, lease)
		}
	}
	b.mu.Unlock()

	for _, lease := range dueRenewal {
		renewCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		duration, err := b.Renew(renewCtx, lease.leaseID)
		cancel()
		if err != nil {
			slog.Warn("openbao lease renewal failed; will revoke", "lease", lease.leaseID, "error", err)
			b.mu.Lock()
			if state, ok := b.leases[lease.leaseID]; ok {
				state.renewable = false
				state.expiresAt = now.Add(30 * time.Second)
			}
			b.mu.Unlock()
			continue
		}
		b.mu.Lock()
		if state, ok := b.leases[lease.leaseID]; ok {
			state.expiresAt = b.now().Add(time.Duration(float64(duration) * 0.7))
			state.renewCount++
		}
		b.mu.Unlock()
	}
	for _, lease := range dueRevoke {
		revokeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := b.revoke(revokeCtx, lease.leaseID)
		cancel()
		b.mu.Lock()
		delete(b.leases, lease.leaseID)
		b.mu.Unlock()
		if err != nil {
			slog.Warn("openbao lease revocation failed", "lease", lease.leaseID, "error", err)
		}
	}
	return nil
}

// Renew extends the lease with the OpenBao API.
func (b *DynamicBroker) Renew(ctx context.Context, leaseID string) (time.Duration, error) {
	payload, err := json.Marshal(map[string]string{"lease_id": leaseID})
	if err != nil {
		return 0, fmt.Errorf("encode renewal: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, b.addr+"/v1/sys/leases/renew", bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("%w: build renewal: %v", ErrSecretUnavailable, err)
	}
	request.Header.Set("Content-Type", "application/json")
	b.authorize(request)
	response, err := b.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrSecretUnavailable, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil {
		return 0, fmt.Errorf("%w: read renewal: %v", ErrSecretUnavailable, err)
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%w: renewal returned %s", ErrSecretUnavailable, response.Status)
	}
	var envelope struct {
		LeaseDuration int64 `json:"lease_duration"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, fmt.Errorf("%w: decode renewal: %v", ErrSecretUnavailable, err)
	}
	if envelope.LeaseDuration <= 0 {
		return 0, fmt.Errorf("%w: renewal returned no duration", ErrSecretUnavailable)
	}
	return time.Duration(envelope.LeaseDuration) * time.Second, nil
}

// Close revokes every outstanding lease: dynamic credentials never outlive
// the gateway process.
func (b *DynamicBroker) Close(ctx context.Context) error {
	b.mu.Lock()
	b.closed = true
	leases := make([]*leaseState, 0, len(b.leases))
	for _, lease := range b.leases {
		leases = append(leases, lease)
	}
	b.mu.Unlock()
	var firstErr error
	for _, lease := range leases {
		if err := b.revoke(ctx, lease.leaseID); err != nil && firstErr == nil {
			firstErr = err
		}
		b.mu.Lock()
		delete(b.leases, lease.leaseID)
		b.mu.Unlock()
	}
	if firstErr != nil {
		return fmt.Errorf("revoke on close: %w", firstErr)
	}
	slog.Info("openbao dynamic broker closed", "revoked", len(leases))
	return nil
}

func (b *DynamicBroker) revoke(ctx context.Context, leaseID string) error {
	payload, err := json.Marshal(map[string]string{"lease_id": leaseID})
	if err != nil {
		return fmt.Errorf("encode revocation: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, b.addr+"/v1/sys/leases/revoke", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%w: build revocation: %v", ErrSecretUnavailable, err)
	}
	request.Header.Set("Content-Type", "application/json")
	b.authorize(request)
	response, err := b.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSecretUnavailable, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: revocation returned %s", ErrSecretUnavailable, response.Status)
	}
	return nil
}

func (b *DynamicBroker) authorize(request *http.Request) {
	request.Header.Set("X-Vault-Token", b.token)
	if b.namespace != "" {
		request.Header.Set("X-Vault-Namespace", b.namespace)
	}
}

// OutstandingLeases reports the number of tracked leases (tests and health).
func (b *DynamicBroker) OutstandingLeases() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.leases)
}
