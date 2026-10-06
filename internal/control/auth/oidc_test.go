package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// fakeOIDCProvider serves OIDC discovery + JWKS and mints RS256 ID tokens,
// exercising the real verification path without an external issuer.
type fakeOIDCProvider struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	kid    string
}

func newFakeOIDCProvider(t *testing.T) *fakeOIDCProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	kid := "test-key-1"
	provider := &fakeOIDCProvider{key: key, kid: kid}
	mux := http.NewServeMux()
	issuer := ""
	mux.HandleFunc("/.well-known/openid-configuration", func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"issuer":   issuer,
			"jwks_uri": issuer + "/keys",
		})
	})
	mux.HandleFunc("/keys", func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"keys": []any{map[string]any{
				"kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig",
				"n": base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
			}},
		})
	})
	provider.server = httptest.NewServer(mux)
	issuer = provider.server.URL
	t.Cleanup(provider.server.Close)
	return provider
}

func (p *fakeOIDCProvider) token(t *testing.T, issuer, audience, subject, tenantClaim, tenant string, expiresAt time.Time) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss": issuer, "aud": audience, "sub": subject, "exp": expiresAt.Unix(),
	}
	if tenantClaim != "" {
		claims[tenantClaim] = tenant
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = p.kid
	signed, err := token.SignedString(p.key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

func (p *fakeOIDCProvider) roundTrip(t *testing.T, handler http.Handler, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks", nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestOIDCMiddlewareAcceptsValidToken(t *testing.T) {
	provider := newFakeOIDCProvider(t)
	handler, err := OIDCMiddleware(provider.server.URL, "fenced-control", "tenant",
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			principal, ok := PrincipalFromContext(request.Context())
			if !ok || principal.Subject != "user-42" || principal.TenantID != "tenant-a" {
				http.Error(writer, "bad principal", http.StatusInternalServerError)
				return
			}
			writer.WriteHeader(http.StatusOK)
		}))
	if err != nil {
		t.Fatalf("OIDCMiddleware: %v", err)
	}
	token := provider.token(t, provider.server.URL, "fenced-control", "user-42", "tenant", "tenant-a", time.Now().Add(time.Hour))
	if response := provider.roundTrip(t, handler, token); response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestOIDCMiddlewareHonorsCustomTenantClaim(t *testing.T) {
	provider := newFakeOIDCProvider(t)
	handler, err := OIDCMiddleware(provider.server.URL, "fenced-control", "org",
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			principal, ok := PrincipalFromContext(request.Context())
			if !ok || principal.TenantID != "org-9" {
				http.Error(writer, "bad tenant", http.StatusInternalServerError)
				return
			}
			writer.WriteHeader(http.StatusOK)
		}))
	if err != nil {
		t.Fatalf("OIDCMiddleware: %v", err)
	}
	token := provider.token(t, provider.server.URL, "fenced-control", "user-1", "org", "org-9", time.Now().Add(time.Hour))
	if response := provider.roundTrip(t, handler, token); response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestOIDCMiddlewareRejectsInvalidTokens(t *testing.T) {
	provider := newFakeOIDCProvider(t)
	handler, err := OIDCMiddleware(provider.server.URL, "fenced-control", "tenant", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err != nil {
		t.Fatalf("OIDCMiddleware: %v", err)
	}

	assertReason := func(response *httptest.ResponseRecorder, expected string) {
		t.Helper()
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401: %s", response.Code, response.Body.String())
		}
		var body struct {
			ReasonCode string `json:"reasonCode"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode problem: %v", err)
		}
		if body.ReasonCode != expected {
			t.Fatalf("reason = %q, want %q", body.ReasonCode, expected)
		}
	}

	// No Authorization header at all.
	assertReason(provider.roundTrip(t, handler, ""), "BEARER_REQUIRED")

	// Not a Bearer token.
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks", nil)
	request.Header.Set("Authorization", "Basic abc")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("basic auth status = %d", response.Code)
	}

	// Garbage token.
	assertReason(provider.roundTrip(t, handler, "not.a.jwt"), "INVALID_ID_TOKEN")

	// Wrong audience.
	wrongAudience := provider.token(t, provider.server.URL, "other-client", "user-1", "tenant", "tenant-a", time.Now().Add(time.Hour))
	assertReason(provider.roundTrip(t, handler, wrongAudience), "INVALID_ID_TOKEN")

	// Expired token.
	expired := provider.token(t, provider.server.URL, "fenced-control", "user-1", "tenant", "tenant-a", time.Now().Add(-time.Hour))
	assertReason(provider.roundTrip(t, handler, expired), "INVALID_ID_TOKEN")

	// Wrong issuer.
	wrongIssuer := provider.token(t, "https://evil.example", "fenced-control", "user-1", "tenant", "tenant-a", time.Now().Add(time.Hour))
	assertReason(provider.roundTrip(t, handler, wrongIssuer), "INVALID_ID_TOKEN")

	// Missing tenant claim.
	noTenant := provider.token(t, provider.server.URL, "fenced-control", "user-1", "", "", time.Now().Add(time.Hour))
	assertReason(provider.roundTrip(t, handler, noTenant), "TENANT_CLAIM_REQUIRED")
}

func TestOIDCMiddlewareRequiresConfiguration(t *testing.T) {
	if _, err := OIDCMiddleware("", "client", "tenant", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})); err == nil {
		t.Fatal("empty issuer must fail")
	}
	if _, err := OIDCMiddleware("https://issuer.example", "", "tenant", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})); err == nil {
		t.Fatal("empty client ID must fail")
	}
}
