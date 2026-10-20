// The agent version endpoints: publication and observation of an
// immutable publication.
package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type createAgentVersionRequest struct {
	Name      string          `json:"name"`
	Version   string          `json:"version"`
	Namespace string          `json:"namespace"`
	Spec      json.RawMessage `json:"spec"`
	// Manifest is the v0.9 portable publication contract. It is mutually
	// exclusive with the legacy name/version/namespace/spec fields and is
	// normalized into those fields before package admission and persistence.
	Manifest *agentversion.Manifest `json:"manifest,omitempty"`
	// Package is the signed Agent Package (ADR-010). With publish admission
	// installed it is required; without it, a presented package is still
	// verified fail-closed.
	Package *agentpkg.Package `json:"package,omitempty"`
}

type agentVersionResponse struct {
	APIVersion            string          `json:"apiVersion"`
	Kind                  string          `json:"kind"`
	ID                    string          `json:"id"`
	TenantID              string          `json:"tenantId"`
	Namespace             string          `json:"namespace"`
	Name                  string          `json:"name"`
	Version               string          `json:"version"`
	Ref                   string          `json:"ref"`
	Spec                  json.RawMessage `json:"spec"`
	SpecDigest            string          `json:"specDigest"`
	ResourceVersion       int64           `json:"resourceVersion"`
	CreatedAt             time.Time       `json:"createdAt"`
	TraceID               string          `json:"traceId"`
	PackageKeyID          string          `json:"packageKeyId,omitempty"`
	PackageManifestDigest string          `json:"packageManifestDigest,omitempty"`
}

func (h *Handler) createAgentVersion(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeProblem(writer, request, http.StatusUnsupportedMediaType, "CONTENT_TYPE_REQUIRED", "Content-Type must be application/json", traceID)
		return
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if !idempotencyKeyPattern.MatchString(idempotencyKey) {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must be 1..128 safe ASCII characters", traceID)
		return
	}

	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBody)
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := rejectDuplicateJSONKeys(encoded); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var body createAgentVersionRequest
	if err := decoder.Decode(&body); err != nil {
		h.writeDecodeProblem(writer, request, err, traceID)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		h.writeProblem(writer, request, http.StatusBadRequest, "INVALID_JSON", err.Error(), traceID)
		return
	}
	body, err = normalizeCreateAgentVersion(body)
	if err != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_AGENT_VERSION", err.Error(), traceID)
		return
	}
	if err := validateCreateAgentVersion(body); err != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "INVALID_AGENT_VERSION", err.Error(), traceID)
		return
	}
	var packageSignature *store.PackageSignature
	if body.Package != nil {
		if err := h.admitPackage(body); err != nil {
			h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PACKAGE_SIGNATURE_INVALID", err.Error(), traceID)
			return
		}
		manifestDigest, err := agentpkg.ManifestDigest(body.Package.Manifest)
		if err != nil {
			h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PACKAGE_SIGNATURE_INVALID", err.Error(), traceID)
			return
		}
		packageSignature = &store.PackageSignature{
			KeyID: body.Package.Signature.KeyID, Signature: body.Package.Signature.Ed25519,
			ManifestDigest: hex.EncodeToString(manifestDigest[:]),
		}
	} else if h.packages != nil {
		h.writeProblem(writer, request, http.StatusUnprocessableEntity, "PACKAGE_REQUIRED", "publication requires a signed agent package verified against the trust registry", traceID)
		return
	}
	result, err := h.agentVersion.CreateAgentVersion(request.Context(), store.CreateAgentVersionInput{
		ID: h.newID(), TenantID: principal.TenantID, Namespace: body.Namespace,
		Name: body.Name, Version: body.Version, Spec: body.Spec,
		PackageSignature: packageSignature,
	})
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	status := http.StatusCreated
	if result.Existing {
		status = http.StatusOK
		writer.Header().Set("Idempotent-Replayed", "true")
	}
	writer.Header().Set("Location", "/v1/agent-versions/"+result.AgentVersion.ID.String())
	h.writeAgentVersion(writer, status, result.AgentVersion, traceID)
}

func (h *Handler) getAgentVersion(writer http.ResponseWriter, request *http.Request) {
	traceID := traceIDFrom(request.Context())
	principal, ok := auth.PrincipalFromContext(request.Context())
	if !ok {
		h.writeProblem(writer, request, http.StatusUnauthorized, "AUTHENTICATION_REQUIRED", "authenticated principal is required", traceID)
		return
	}
	id, err := uuid.Parse(request.PathValue("agentVersionID"))
	if err != nil {
		h.writeProblem(writer, request, http.StatusNotFound, "AGENT_VERSION_NOT_FOUND", "agent version was not found", traceID)
		return
	}
	version, err := h.agentVersion.GetAgentVersion(request.Context(), principal.TenantID, id)
	if err != nil {
		h.writeStoreProblem(writer, request, err, traceID)
		return
	}
	h.writeAgentVersion(writer, http.StatusOK, version, traceID)
}

// listTools exposes the tenant's registered tool descriptors for capability
// discovery.
func (h *Handler) writeAgentVersion(writer http.ResponseWriter, status int, version store.AgentVersion, traceID string) {
	writer.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, version.ResourceVersion))
	writeJSON(writer, status, agentVersionResponseOf(version, traceID))
}

// agentVersionResponseOf builds the wire shape of one publication.
func agentVersionResponseOf(version store.AgentVersion, traceID string) agentVersionResponse {
	response := agentVersionResponse{
		APIVersion: agentversion.APIVersion, Kind: agentversion.Kind, ID: version.ID.String(),
		TenantID: version.TenantID, Namespace: version.Namespace, Name: version.Name,
		Version: version.Version, Ref: version.Ref(), Spec: version.Spec,
		SpecDigest: fmt.Sprintf("%x", version.SpecDigest), ResourceVersion: version.ResourceVersion,
		CreatedAt: version.CreatedAt.UTC(), TraceID: traceID,
	}
	if version.PackageSignature != nil {
		response.PackageKeyID = version.PackageSignature.KeyID
		response.PackageManifestDigest = version.PackageSignature.ManifestDigest
	}
	return response
}

// admitPackage is the ADR-010 publish gate. Verification is fail-closed
// (signature, trusted key identity, canonical manifest) and additionally
// binds the signed manifest to this exact publication: the manifest must sign
// the same name@version reference and the same spec bytes being published.
func (h *Handler) admitPackage(body createAgentVersionRequest) error {
	if h.packages == nil {
		// Dev mode without a trust registry: unsigned publications are
		// allowed, but a presented package is still verified fail-closed
		// against the empty trust set (every key is unknown).
		return agentpkg.Verify(body.Package, nil)
	}
	if err := h.packages.Verify(body.Package); err != nil {
		return err
	}
	publicationRef := agentversion.FormatRef(body.Namespace, body.Name, body.Version)
	if body.Package.Manifest.AgentVersionRef != publicationRef {
		return fmt.Errorf("%w: manifest signs %q but the publication is %q",
			agentpkg.ErrPackageBindingMismatch, body.Package.Manifest.AgentVersionRef, publicationRef)
	}
	if !body.Package.Manifest.SpecDigest.Verify(body.Spec) {
		return fmt.Errorf("%w: spec does not match the signed spec digest", agentpkg.ErrPackageBindingMismatch)
	}
	return nil
}

// --- audit ledger (ADR-014) ---

func validateCreateAgentVersion(body createAgentVersionRequest) error {
	if err := agentversion.ValidateName(body.Name); err != nil {
		return err
	}
	if err := agentversion.ValidateVersion(body.Version); err != nil {
		return err
	}
	if err := agentversion.ValidateNamespace(body.Namespace); err != nil {
		return err
	}
	if err := agentversion.ValidateSpec(body.Spec); err != nil {
		return err
	}
	return nil
}

func normalizeCreateAgentVersion(body createAgentVersionRequest) (createAgentVersionRequest, error) {
	if body.Manifest == nil {
		return body, nil
	}
	if body.Name != "" || body.Version != "" || body.Namespace != "" || len(body.Spec) != 0 {
		return body, fmt.Errorf("manifest cannot be combined with name, version, namespace, or spec")
	}
	if err := body.Manifest.Validate(); err != nil {
		return body, err
	}
	spec, err := json.Marshal(body.Manifest.Spec)
	if err != nil {
		return body, fmt.Errorf("canonicalize agent manifest spec: %w", err)
	}
	body.Name = body.Manifest.Metadata.Name
	body.Version = body.Manifest.Metadata.Version
	body.Namespace = body.Manifest.Metadata.Namespace
	body.Spec = spec
	return body, nil
}
