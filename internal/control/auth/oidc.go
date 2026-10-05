// Package auth defines the authenticated identity carried by control-plane
// requests.
package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// verifyTimeout bounds the discovery/JWKS round trips of one token
// verification; keys are cached by the verifier between requests.
const verifyTimeout = 10 * time.Second

// OIDCMiddleware authenticates control-plane requests with an OIDC ID token
// (Authorization: Bearer <id_token>) issued by the configured issuer. The
// token's subject becomes the Principal subject; the tenant claim (default
// "tenant", configurable) becomes the tenant scope every store query is
// bounded by. This is the production identity path the tech baseline requires
// before any non-development deployment.
func OIDCMiddleware(issuer, clientID, tenantClaim string, next http.Handler) (http.Handler, error) {
	if strings.TrimSpace(issuer) == "" || strings.TrimSpace(clientID) == "" {
		return nil, errInvalidConfig("issuer and client ID are required")
	}
	if strings.TrimSpace(tenantClaim) == "" {
		tenantClaim = "tenant"
	}
	provider, err := oidc.NewProvider(context.Background(), issuer)
	if err != nil {
		return nil, err
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: clientID})
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" || request.URL.Path == "/readyz" || request.URL.Path == "/versionz" {
			next.ServeHTTP(writer, request)
			return
		}
		header := request.Header.Get("Authorization")
		token, ok := strings.CutPrefix(header, "Bearer ")
		if !ok || strings.TrimSpace(token) == "" {
			writeAuthProblem(writer, request, "BEARER_REQUIRED", "Authorization: Bearer <id_token> is required")
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), verifyTimeout)
		defer cancel()
		idToken, err := verifier.Verify(ctx, strings.TrimSpace(token))
		if err != nil {
			writeAuthProblem(writer, request, "INVALID_ID_TOKEN", "id token could not be verified")
			return
		}
		var claims map[string]any
		if err := idToken.Claims(&claims); err != nil {
			writeAuthProblem(writer, request, "INVALID_ID_TOKEN", "id token claims could not be decoded")
			return
		}
		subject, _ := claims["sub"].(string)
		tenant, _ := claims[tenantClaim].(string)
		if strings.TrimSpace(subject) == "" || strings.TrimSpace(tenant) == "" {
			writeAuthProblem(writer, request, "TENANT_CLAIM_REQUIRED",
				"id token must carry a non-empty sub and a non-empty "+tenantClaim+" claim")
			return
		}
		next.ServeHTTP(writer, request.WithContext(WithPrincipal(request.Context(), Principal{Subject: subject, TenantID: tenant})))
	}), nil
}

type configError string

func (e configError) Error() string { return string(e) }

func errInvalidConfig(message string) error { return configError(message) }

// authProblem mirrors the control API's problem+json shape so auth failures
// are indistinguishable in structure from API failures.
type authProblem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	Instance   string `json:"instance"`
	ReasonCode string `json:"reasonCode"`
}

func writeAuthProblem(writer http.ResponseWriter, request *http.Request, reasonCode, detail string) {
	writer.Header().Set("Content-Type", "application/problem+json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(writer).Encode(authProblem{
		Type:  "https://fenced.dev/problems/" + strings.ToLower(strings.ReplaceAll(reasonCode, "_", "-")),
		Title: http.StatusText(http.StatusUnauthorized), Status: http.StatusUnauthorized,
		Detail: detail, Instance: request.URL.Path, ReasonCode: reasonCode,
	})
}
