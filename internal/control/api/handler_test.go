package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	controlapi "github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestCreateTaskRequiresIdentityAndIdempotency(t *testing.T) {
	handler := controlapi.NewHandler(newMemoryStore(), newMemoryStore(), newMemoryStore(), newMemoryStore())
	body := []byte(`{"agentVersionRef":"agent@1","goal":"test","namespace":"default","spec":{}}`)

	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "request-1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401: %s", response.Code, response.Body.String())
	}

	authed := auth.StaticMiddleware(auth.Principal{Subject: "user-1", TenantID: "tenant-a"}, handler)
	request = httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	authed.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "INVALID_IDEMPOTENCY_KEY")
}

func TestOperationalProbesSeparateLivenessReadinessAndVersion(t *testing.T) {
	backend := newMemoryStore()
	ready := controlapi.NewHandler(backend, backend, backend, backend,
		controlapi.WithReadiness(func(context.Context) error { return nil }))
	for path, want := range map[string]string{
		"/healthz":  `"status":"ok"`,
		"/readyz":   `"status":"ready"`,
		"/versionz": `"productVersion":"1.3.0.0"`,
	} {
		response := httptest.NewRecorder()
		ready.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), want) {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}

	notReady := controlapi.NewHandler(backend, backend, backend, backend,
		controlapi.WithReadiness(func(context.Context) error { return errors.New("database unavailable") }))
	response := httptest.NewRecorder()
	notReady.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestCreateTaskIsStrictAndIdempotent(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	body := []byte(`{"agentVersionRef":"agent@1","goal":"test","namespace":"default","spec":{"runtimeClass":"oci"}}`)

	first := performCreate(handler, body, "request-1")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d: %s", first.Code, first.Body.String())
	}
	var task map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if task["tenantId"] != "tenant-a" || task["phase"] != "QUEUED" {
		t.Fatalf("unexpected task response: %+v", task)
	}
	if first.Header().Get("Location") == "" || first.Header().Get("ETag") != `W/"1"` {
		t.Fatalf("missing resource headers: %+v", first.Header())
	}

	replayed := performCreate(handler, body, "request-1")
	if replayed.Code != http.StatusOK || replayed.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay status=%d headers=%v body=%s", replayed.Code, replayed.Header(), replayed.Body.String())
	}

	conflict := performCreate(handler, []byte(`{"agentVersionRef":"agent@1","goal":"different","namespace":"default","spec":{}}`), "request-1")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d: %s", conflict.Code, conflict.Body.String())
	}
	assertReason(t, conflict, "IDEMPOTENCY_CONFLICT")

	unknown := performCreate(handler, []byte(`{"agentVersionRef":"agent@1","goal":"test","namespace":"default","spec":{},"status":"SUCCEEDED"}`), "request-2")
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d: %s", unknown.Code, unknown.Body.String())
	}
	assertReason(t, unknown, "INVALID_JSON")

	duplicate := performCreate(handler, []byte(`{"agentVersionRef":"agent@1","goal":"first","goal":"second","namespace":"default","spec":{"budget":{"tokens":1,"tokens":2}}}`), "request-3")
	if duplicate.Code != http.StatusBadRequest {
		t.Fatalf("duplicate key status = %d: %s", duplicate.Code, duplicate.Body.String())
	}
	assertReason(t, duplicate, "INVALID_JSON")

	malformed := performCreate(handler, []byte(`{"agentVersionRef":"not-a-ref","goal":"test","namespace":"default","spec":{}}`), "request-4")
	if malformed.Code != http.StatusUnprocessableEntity {
		t.Fatalf("malformed ref status = %d: %s", malformed.Code, malformed.Body.String())
	}
	assertReason(t, malformed, "INVALID_TASK")
}

func TestGetTaskDoesNotCrossTenantBoundary(t *testing.T) {
	backend := newMemoryStore()
	task, err := backend.CreateTask(context.Background(), store.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "private", Spec: []byte(`{}`), IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	handler := controlapi.NewHandler(backend, backend, backend, backend)

	owner := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, handler)
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.Task.ID.String(), nil)
	response := httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("owner status = %d: %s", response.Code, response.Body.String())
	}

	other := auth.StaticMiddleware(auth.Principal{Subject: "other", TenantID: "tenant-b"}, handler)
	request = httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.Task.ID.String(), nil)
	response = httptest.NewRecorder()
	other.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("other tenant status = %d, want 404: %s", response.Code, response.Body.String())
	}
}

func TestCancelTaskRequiresCurrentETag(t *testing.T) {
	backend := newMemoryStore()
	created, err := backend.CreateTask(context.Background(), store.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "cancel me", Spec: []byte(`{}`), IdempotencyKey: "cancel-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, controlapi.NewHandler(backend, backend, backend, backend))
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks/"+created.Task.ID.String()+":cancel", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/tasks/"+created.Task.ID.String()+":cancel", nil)
	request.Header.Set("If-Match", `W/"1"`)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || response.Header().Get("ETag") != `W/"2"` {
		t.Fatalf("cancel status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var body taskPhaseResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.Phase != "CANCELLED" {
		t.Fatalf("cancel response=%+v err=%v", body, err)
	}
}

type taskPhaseResponse struct {
	Phase string `json:"phase"`
}

func TestPublishAgentVersionIsStrictAndIdempotent(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	spec := []byte(`{"runtimeClassPolicy":{"allowed":["oci"],"preferred":"oci"},"lifecycle":{"maxAttempts":5}}`)
	first := performPublish(handler, spec, "publish-1")
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d: %s", first.Code, first.Body.String())
	}
	var published map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &published); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if published["tenantId"] != "tenant-a" || published["name"] != "research-agent" ||
		published["version"] != "1.3.0" || published["ref"] != "research-agent@1.3.0" {
		t.Fatalf("unexpected agent version response: %+v", published)
	}
	if first.Header().Get("Location") == "" || first.Header().Get("ETag") != `W/"1"` {
		t.Fatalf("missing resource headers: %+v", first.Header())
	}
	digest, ok := published["specDigest"].(string)
	if !ok || len(digest) != 64 {
		t.Fatalf("missing spec digest: %+v", published)
	}

	replayed := performPublish(handler, spec, "publish-2")
	if replayed.Code != http.StatusOK || replayed.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay status=%d headers=%v body=%s", replayed.Code, replayed.Header(), replayed.Body.String())
	}

	conflict := performPublish(handler, []byte(`{"runtimeClassPolicy":{"allowed":["microvm"]}}`), "publish-3")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d: %s", conflict.Code, conflict.Body.String())
	}
	assertReason(t, conflict, "AGENT_VERSION_CONFLICT")

	unknownBody := []byte(`{"name":"research-agent","version":"2.0.0","namespace":"default","spec":{"runtimeClassPolicy":{"allowed":["oci"]}},"status":"ACTIVE"}`)
	unknown := publishRequest(unknownBody, "publish-4")
	unknownResponse := httptest.NewRecorder()
	handler.ServeHTTP(unknownResponse, unknown)
	if unknownResponse.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d: %s", unknownResponse.Code, unknownResponse.Body.String())
	}
	assertReason(t, unknownResponse, "INVALID_JSON")

	invalidSpec := performPublishVersion(handler, "research-agent", "2.1.0", []byte(`{"runtimeClassPolicy":{"allowed":["oci"],"preferred":"microvm"}}`), "publish-5")
	if invalidSpec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid spec status = %d: %s", invalidSpec.Code, invalidSpec.Body.String())
	}
	assertReason(t, invalidSpec, "INVALID_AGENT_VERSION")

	missingKey := publishRequest(spec, "")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, missingKey)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing idempotency key status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "INVALID_IDEMPOTENCY_KEY")
}

func TestPublishAgentManifestNormalizesIntoAgentVersion(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	manifest := validAgentManifest()
	response := performManifestPublish(handler, manifest, nil, "manifest-publish-1")
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var published struct {
		Name      string            `json:"name"`
		Version   string            `json:"version"`
		Namespace string            `json:"namespace"`
		Ref       string            `json:"ref"`
		Spec      agentversion.Spec `json:"spec"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &published); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if published.Name != manifest.Metadata.Name || published.Version != manifest.Metadata.Version ||
		published.Namespace != manifest.Metadata.Namespace || published.Ref != manifest.Ref() {
		t.Fatalf("manifest identity was not normalized: %+v", published)
	}
	if len(published.Spec.Runtimes) != 1 || published.Spec.Runtimes[0].Interface != agentversion.RuntimeInterfaceV1 {
		t.Fatalf("portable runtime contract was not preserved: %+v", published.Spec)
	}

	mixed, err := json.Marshal(map[string]any{"manifest": manifest, "name": "legacy-name"})
	if err != nil {
		t.Fatal(err)
	}
	mixedResponse := httptest.NewRecorder()
	handler.ServeHTTP(mixedResponse, publishRequest(mixed, "manifest-publish-2"))
	if mixedResponse.Code != http.StatusUnprocessableEntity {
		t.Fatalf("mixed status = %d: %s", mixedResponse.Code, mixedResponse.Body.String())
	}
	assertReason(t, mixedResponse, "INVALID_AGENT_VERSION")

	manifest.Spec.Capabilities.Models = nil
	invalid := performManifestPublish(handler, manifest, nil, "manifest-publish-3")
	if invalid.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid status = %d: %s", invalid.Code, invalid.Body.String())
	}
	assertReason(t, invalid, "INVALID_AGENT_VERSION")
}

func TestPublishAgentManifestBindsSignedPackageToNormalizedSpec(t *testing.T) {
	signingKey, key, err := agentpkg.GenerateSigningKey("manifest-builder")
	if err != nil {
		t.Fatal(err)
	}
	registry := agentpkg.NewRegistry()
	if err := registry.Add(*key); err != nil {
		t.Fatal(err)
	}
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend, controlapi.WithPackageAdmission(registry)),
	)
	manifest := validAgentManifest()
	spec, err := json.Marshal(manifest.Spec)
	if err != nil {
		t.Fatal(err)
	}
	pkg := signedPackage(t, signingKey, manifest.Metadata.Name, manifest.Metadata.Version, spec)
	response := performManifestPublish(handler, manifest, pkg, "manifest-package-1")
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var published map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &published); err != nil {
		t.Fatal(err)
	}
	if published["packageKeyId"] != "manifest-builder" {
		t.Fatalf("packageKeyId = %v", published["packageKeyId"])
	}
}

func TestPublishAgentVersionAdmissionFailClosed(t *testing.T) {
	signingKey, key, err := agentpkg.GenerateSigningKey("ci-builder-1")
	if err != nil {
		t.Fatalf("generate package key: %v", err)
	}
	registry := agentpkg.NewRegistry()
	if err := registry.Add(*key); err != nil {
		t.Fatalf("trust key: %v", err)
	}
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend, controlapi.WithPackageAdmission(registry)),
	)
	spec := []byte(`{"runtimeClassPolicy":{"allowed":["oci"],"preferred":"oci"},"lifecycle":{"maxAttempts":5}}`)

	// Without a signed package the publication is rejected fail-closed.
	unsigned := performPublish(handler, spec, "publish-unsigned")
	if unsigned.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unsigned status = %d, want 422: %s", unsigned.Code, unsigned.Body.String())
	}
	assertReason(t, unsigned, "PACKAGE_REQUIRED")

	// A valid signed package is admitted and its envelope is recorded.
	pkg := signedPackage(t, signingKey, "research-agent", "1.3.0", spec)
	signed := performPackagePublish(handler, "research-agent", "1.3.0", spec, pkg, "publish-1")
	if signed.Code != http.StatusCreated {
		t.Fatalf("signed status = %d: %s", signed.Code, signed.Body.String())
	}
	var published map[string]any
	if err := json.Unmarshal(signed.Body.Bytes(), &published); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if published["packageKeyId"] != "ci-builder-1" {
		t.Fatalf("packageKeyId = %v, want ci-builder-1", published["packageKeyId"])
	}
	manifestDigest, ok := published["packageManifestDigest"].(string)
	if !ok || len(manifestDigest) != 64 {
		t.Fatalf("packageManifestDigest = %v, want hex sha256", published["packageManifestDigest"])
	}

	// An identical signed replay is idempotent.
	replayed := performPackagePublish(handler, "research-agent", "1.3.0", spec, pkg, "publish-2")
	if replayed.Code != http.StatusOK || replayed.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay status=%d headers=%v body=%s", replayed.Code, replayed.Header(), replayed.Body.String())
	}

	// A tampered signature is rejected before any binding check.
	tampered := *pkg
	tampered.Signature.Ed25519 = "AAAA"
	badSignature := performPackagePublish(handler, "research-agent", "1.3.0", spec, &tampered, "publish-3")
	if badSignature.Code != http.StatusUnprocessableEntity {
		t.Fatalf("tampered status = %d, want 422: %s", badSignature.Code, badSignature.Body.String())
	}
	assertReason(t, badSignature, "PACKAGE_SIGNATURE_INVALID")

	// A package from an untrusted key is rejected.
	_, attackerKey, err := agentpkg.GenerateSigningKey("attacker")
	if err != nil {
		t.Fatalf("generate attacker key: %v", err)
	}
	_ = attackerKey
	forged, err := agentpkg.Sign(agentpkg.Manifest{
		Schema: agentpkg.ManifestSchema, AgentVersionRef: "research-agent@1.3.0",
		SpecDigest: agentpkg.SpecSHA256(spec), Spec: spec,
		Provenance: agentpkg.Provenance{Builder: "test", BuildWorkflow: "unit.yml", GitCommit: "abc"},
	}, signingKey)
	if err != nil {
		t.Fatalf("sign package: %v", err)
	}
	forged.Signature.KeyID = "attacker"
	forgedPackage := performPackagePublish(handler, "research-agent", "1.3.0", spec, forged, "publish-4")
	if forgedPackage.Code != http.StatusUnprocessableEntity {
		t.Fatalf("forged key status = %d, want 422: %s", forgedPackage.Code, forgedPackage.Body.String())
	}
	assertReason(t, forgedPackage, "PACKAGE_SIGNATURE_INVALID")

	// The manifest must sign exactly this publication: wrong version binding.
	wrongVersion := signedPackage(t, signingKey, "research-agent", "9.9.9", spec)
	versionMismatch := performPackagePublish(handler, "research-agent", "1.3.0", spec, wrongVersion, "publish-5")
	if versionMismatch.Code != http.StatusUnprocessableEntity {
		t.Fatalf("version mismatch status = %d, want 422: %s", versionMismatch.Code, versionMismatch.Body.String())
	}
	assertReason(t, versionMismatch, "PACKAGE_SIGNATURE_INVALID")

	// The published spec bytes must equal the signed spec bytes.
	otherSpec := []byte(`{"runtimeClassPolicy":{"allowed":["wasm"]}}`)
	otherPackage := signedPackage(t, signingKey, "research-agent", "1.3.0", otherSpec)
	specMismatch := performPackagePublish(handler, "research-agent", "1.3.0", spec, otherPackage, "publish-6")
	if specMismatch.Code != http.StatusUnprocessableEntity {
		t.Fatalf("spec mismatch status = %d, want 422: %s", specMismatch.Code, specMismatch.Body.String())
	}
	assertReason(t, specMismatch, "PACKAGE_SIGNATURE_INVALID")
}

func TestPublishAgentVersionDevModeStillVerifiesPackages(t *testing.T) {
	signingKey, key, err := agentpkg.GenerateSigningKey("ci-builder-1")
	if err != nil {
		t.Fatalf("generate package key: %v", err)
	}
	registry := agentpkg.NewRegistry()
	if err := registry.Add(*key); err != nil {
		t.Fatalf("trust key: %v", err)
	}
	backend := newMemoryStore()
	// Dev mode: no admission registry, so unsigned publications are allowed…
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	spec := []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`)
	unsigned := performPublish(handler, spec, "dev-publish-1")
	if unsigned.Code != http.StatusCreated {
		t.Fatalf("dev unsigned status = %d: %s", unsigned.Code, unsigned.Body.String())
	}
	// …but a presented package is still verified fail-closed.
	tampered := *signedPackage(t, signingKey, "research-agent", "1.3.0", spec)
	tampered.Signature.Ed25519 = "AAAA"
	bad := performPackagePublish(handler, "research-agent", "1.3.0", spec, &tampered, "dev-publish-2")
	if bad.Code != http.StatusUnprocessableEntity {
		t.Fatalf("dev tampered status = %d, want 422: %s", bad.Code, bad.Body.String())
	}
	assertReason(t, bad, "PACKAGE_SIGNATURE_INVALID")
}

// signedPackage builds an ADR-010 signed package for the publication.
func signedPackage(t *testing.T, signingKey *agentpkg.SigningKey, name, version string, spec []byte) *agentpkg.Package {
	t.Helper()
	pkg, err := agentpkg.Sign(agentpkg.Manifest{
		Schema: agentpkg.ManifestSchema, AgentVersionRef: name + "@" + version,
		SpecDigest: agentpkg.SpecSHA256(spec), Spec: spec,
		Provenance: agentpkg.Provenance{Builder: "test", BuildWorkflow: "unit.yml", GitCommit: "abc"},
	}, signingKey)
	if err != nil {
		t.Fatalf("sign package: %v", err)
	}
	return pkg
}

func performPackagePublish(handler http.Handler, name, version string, spec []byte, pkg *agentpkg.Package, key string) *httptest.ResponseRecorder {
	body, err := json.Marshal(map[string]any{
		"name": name, "version": version, "namespace": "default",
		"spec": json.RawMessage(spec), "package": pkg,
	})
	if err != nil {
		panic(err)
	}
	request := publishRequest(body, key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestGetAgentVersionDoesNotCrossTenantBoundary(t *testing.T) {
	backend := newMemoryStore()
	published, err := backend.CreateAgentVersion(context.Background(), store.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", Name: "research-agent", Version: "1.3.0",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`),
	})
	if err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	handler := controlapi.NewHandler(backend, backend, backend, backend)

	owner := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, handler)
	request := httptest.NewRequest(http.MethodGet, "/v1/agent-versions/"+published.AgentVersion.ID.String(), nil)
	response := httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("owner status = %d: %s", response.Code, response.Body.String())
	}

	other := auth.StaticMiddleware(auth.Principal{Subject: "other", TenantID: "tenant-b"}, handler)
	request = httptest.NewRequest(http.MethodGet, "/v1/agent-versions/"+published.AgentVersion.ID.String(), nil)
	response = httptest.NewRecorder()
	other.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("other tenant status = %d, want 404: %s", response.Code, response.Body.String())
	}
}

func performPublish(handler http.Handler, spec []byte, key string) *httptest.ResponseRecorder {
	return performPublishVersion(handler, "research-agent", "1.3.0", spec, key)
}

func validAgentManifest() agentversion.Manifest {
	return agentversion.Manifest{
		APIVersion: agentversion.ManifestAPIVersion,
		Kind:       agentversion.ManifestKind,
		Metadata: agentversion.Metadata{
			Name: "portable-agent", Version: "0.9.0", Namespace: "default",
		},
		Spec: agentversion.Spec{
			RuntimeClassPolicy: agentversion.RuntimeClassPolicy{Allowed: []string{"wasmtime"}, Preferred: "wasmtime"},
			Runtimes: []agentversion.RuntimeTarget{{
				Class: "wasmtime", Interface: agentversion.RuntimeInterfaceV1,
				RuntimeABI: "wasi-preview1", Entrypoint: []string{"agent.wasm"},
			}},
			Capabilities: &agentversion.Capabilities{
				Tools: []string{"search.read"}, Models: []string{"model.default"},
				Memory: []string{"memory.session"}, Secrets: []string{},
			},
			Resources:  &agentversion.ResourceLimits{CPUMillis: 500, MemoryMiB: 256, WorkspaceBytes: 0},
			Budget:     &agentversion.Budget{Tokens: 1000, CostMicroUSD: money.MustFromUSD(1), ToolCalls: 10, WallSeconds: 60},
			Checkpoint: &agentversion.CheckpointPolicy{Mode: agentversion.CheckpointLogical, SchemaVersion: "checkpoint/v1"},
		},
	}
}

func performManifestPublish(handler http.Handler, manifest agentversion.Manifest, pkg *agentpkg.Package, key string) *httptest.ResponseRecorder {
	body := map[string]any{"manifest": manifest}
	if pkg != nil {
		body["package"] = pkg
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, publishRequest(encoded, key))
	return response
}

func performPublishVersion(handler http.Handler, name, version string, spec []byte, key string) *httptest.ResponseRecorder {
	body := []byte(`{"name":"` + name + `","version":"` + version + `","namespace":"default","spec":` + string(spec) + `}`)
	request := publishRequest(body, key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func publishRequest(body []byte, key string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/v1/agent-versions", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	return request
}

func TestGetTaskReportsBudgetUsage(t *testing.T) {
	backend := newMemoryStore()
	created, err := backend.CreateTask(context.Background(), store.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "usage", Spec: []byte(`{}`), IdempotencyKey: "usage-task",
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	backend.setBudget(created.Task.ID, store.TaskBudgetStatus{
		TaskID: created.Task.ID, TenantID: "tenant-a",
		Reserved:  store.TaskBudget{Tokens: 100, CostMicroUSD: money.MustFromUSD(1), ToolCalls: 10, WallSeconds: 60},
		Consumed:  store.TaskBudget{Tokens: 60, CostMicroUSD: money.MustFromUSD(0.5), ToolCalls: 4, WallSeconds: 30},
		Exhausted: true, ResourceVersion: 2, UpdatedAt: now,
	})
	handler := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, controlapi.NewHandler(backend, backend, backend, backend))
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+created.Task.ID.String(), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Usage           usageResponse `json:"usage"`
		BudgetExhausted bool          `json:"budgetExhausted"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Usage.Tokens != 60 || body.Usage.CostMicroUSD != money.MustFromUSD(0.5) || body.Usage.ToolCalls != 4 || body.Usage.WallSeconds != 30 {
		t.Fatalf("unexpected usage: %+v", body.Usage)
	}
	if !body.BudgetExhausted {
		t.Fatal("budgetExhausted = false, want true")
	}
}

type usageResponse struct {
	Tokens       int64          `json:"tokens"`
	CostMicroUSD money.MicroUSD `json:"costUsd"`
	ToolCalls    int64          `json:"toolCalls"`
	WallSeconds  int64          `json:"wallSeconds"`
}

func TestRoutingFailuresRemainStructured(t *testing.T) {
	handler := controlapi.NewHandler(newMemoryStore(), newMemoryStore(), newMemoryStore(), newMemoryStore())
	request := httptest.NewRequest(http.MethodDelete, "/v1/tasks", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("method response status=%d content-type=%q body=%s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	assertReason(t, response, "METHOD_NOT_ALLOWED")

	request = httptest.NewRequest(http.MethodGet, "/unknown", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("route response status=%d body=%s", response.Code, response.Body.String())
	}
	assertReason(t, response, "ROUTE_NOT_FOUND")
}

func performCreate(handler http.Handler, body []byte, key string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertReason(t *testing.T, response *httptest.ResponseRecorder, expected string) {
	t.Helper()
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

type memoryStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]store.Task
	byKey     map[string]uuid.UUID
	hashes    map[string][32]byte
	versions  []store.AgentVersion
	budgets   map[uuid.UUID]store.TaskBudgetStatus
	tools     []store.ToolDescriptor
	approvals map[uuid.UUID]store.ToolApproval
	memories  map[uuid.UUID]store.MemoryRecord
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		byID: map[uuid.UUID]store.Task{}, byKey: map[string]uuid.UUID{},
		hashes: map[string][32]byte{}, budgets: map[uuid.UUID]store.TaskBudgetStatus{},
		approvals: map[uuid.UUID]store.ToolApproval{},
		memories:  map[uuid.UUID]store.MemoryRecord{},
	}
}

func (m *memoryStore) CreateTask(_ context.Context, in store.CreateTaskInput) (store.CreateTaskResult, error) {
	normalized, hash, err := in.ValidateAndHash()
	if err != nil {
		return store.CreateTaskResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := in.TenantID + "/" + in.Namespace + "/" + in.IdempotencyKey
	if id, ok := m.byKey[key]; ok {
		if m.hashes[key] != hash {
			return store.CreateTaskResult{}, store.ErrIdempotencyConflict
		}
		return store.CreateTaskResult{Task: m.byID[id], Existing: true}, nil
	}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	task := store.Task{
		ID: in.ID, TenantID: in.TenantID, Namespace: in.Namespace, AgentVersionRef: in.AgentVersionRef,
		Goal: in.Goal, Spec: normalized, RequestHash: hash, IdempotencyKey: in.IdempotencyKey,
		Phase: domain.TaskQueued, ResourceVersion: 1, CreatedAt: now, UpdatedAt: now,
	}
	m.byID[task.ID] = task
	m.byKey[key] = task.ID
	m.hashes[key] = hash
	return store.CreateTaskResult{Task: task}, nil
}

func (m *memoryStore) GetTask(_ context.Context, tenantID string, id uuid.UUID) (store.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.byID[id]
	if !ok || task.TenantID != tenantID {
		return store.Task{}, store.ErrNotFound
	}
	return task, nil
}

func (m *memoryStore) RequestTaskCancellation(_ context.Context, tenantID string, id uuid.UUID, expectedVersion int64) (store.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.byID[id]
	if !ok || task.TenantID != tenantID {
		return store.Task{}, store.ErrNotFound
	}
	if task.ResourceVersion != expectedVersion {
		return store.Task{}, store.ErrVersionConflict
	}
	if err := domain.ValidateTaskTransition(task.Phase, domain.TaskCancelled); err != nil {
		return store.Task{}, errors.Join(store.ErrInvalidTransition, err)
	}
	now := task.UpdatedAt.Add(time.Second)
	task.Phase, task.ResourceVersion, task.UpdatedAt, task.CancelRequestedAt = domain.TaskCancelled, task.ResourceVersion+1, now, &now
	m.byID[id] = task
	return task, nil
}

func (m *memoryStore) CreateAgentVersion(_ context.Context, in store.CreateAgentVersionInput) (store.CreateAgentVersionResult, error) {
	canonical, digest, err := in.ValidateAndHash()
	if err != nil {
		return store.CreateAgentVersionResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.versions {
		if existing.TenantID == in.TenantID && existing.Name == in.Name && existing.Version == in.Version {
			if existing.SpecDigest != digest {
				return store.CreateAgentVersionResult{}, store.ErrAgentVersionConflict
			}
			return store.CreateAgentVersionResult{AgentVersion: existing, Existing: true}, nil
		}
	}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	version := store.AgentVersion{
		ID: in.ID, TenantID: in.TenantID, Namespace: in.Namespace, Name: in.Name, Version: in.Version,
		Spec: canonical, SpecDigest: digest, ResourceVersion: 1, CreatedAt: now,
		PackageSignature: in.PackageSignature,
	}
	m.versions = append(m.versions, version)
	return store.CreateAgentVersionResult{AgentVersion: version}, nil
}

func (m *memoryStore) GetAgentVersion(_ context.Context, tenantID string, id uuid.UUID) (store.AgentVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, version := range m.versions {
		if version.ID == id && version.TenantID == tenantID {
			return version, nil
		}
	}
	return store.AgentVersion{}, store.ErrNotFound
}

func (m *memoryStore) GetAgentVersionByRef(_ context.Context, tenantID, ref string) (store.AgentVersion, error) {
	namespace, name, version, err := agentversion.ParseRef(ref)
	if err != nil {
		return store.AgentVersion{}, store.ErrAgentVersionRefInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, candidate := range m.versions {
		if candidate.TenantID == tenantID && candidate.Namespace == namespace &&
			candidate.Name == name && candidate.Version == version {
			return candidate, nil
		}
	}
	return store.AgentVersion{}, store.ErrNotFound
}

func (m *memoryStore) GetTaskBudget(_ context.Context, tenantID string, id uuid.UUID) (store.TaskBudgetStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.byID[id]
	if !ok || task.TenantID != tenantID {
		return store.TaskBudgetStatus{}, store.ErrNotFound
	}
	status, ok := m.budgets[id]
	if !ok {
		return store.TaskBudgetStatus{}, store.ErrBudgetNotReserved
	}
	return status, nil
}

func (m *memoryStore) ListToolDescriptors(context.Context, string) ([]store.ToolDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.tools), nil
}

func (m *memoryStore) GetToolApproval(_ context.Context, tenantID string, id uuid.UUID) (store.ToolApproval, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	approval, ok := m.approvals[id]
	if !ok || approval.TenantID != tenantID {
		return store.ToolApproval{}, store.ErrNotFound
	}
	return approval, nil
}

func (m *memoryStore) DecideToolApproval(_ context.Context, in store.DecideToolApprovalInput) (store.ToolApproval, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	approval, ok := m.approvals[in.ApprovalID]
	if !ok || approval.TenantID != in.TenantID {
		return store.ToolApproval{}, store.ErrNotFound
	}
	if approval.ResourceVersion != in.ExpectedVersion {
		return store.ToolApproval{}, store.ErrVersionConflict
	}
	if approval.Status != store.ToolApprovalPending {
		return store.ToolApproval{}, store.ErrInvalidTransition
	}
	approval.Status = in.Decision
	approval.DecidedAt, approval.DecidedBy = &in.Now, in.DecidedBy
	approval.ResourceVersion++
	m.approvals[approval.ID] = approval
	return approval, nil
}

func (m *memoryStore) setBudget(id uuid.UUID, status store.TaskBudgetStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.budgets[id] = status
}

// MemoryAPI methods: the fake mirrors the canonical store semantics —
// idempotent (tenant, namespace, key) writes, corrections as new versions,
// tenant-scoped retrieval and CAS tombstones.

func (m *memoryStore) Put(_ context.Context, in memory.PutInput) (store.MemoryRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	embedding := in.Embedding
	if len(embedding) == 0 {
		embedding = make([]float32, store.MemoryEmbeddingDimension)
		embedding[0] = 1
	}
	provenance, _ := json.Marshal(in.Provenance)
	for _, record := range m.memories {
		if record.TenantID != in.TenantID || record.Namespace != in.Namespace || record.Key != in.Key {
			continue
		}
		existingHash, _ := record.ContentHash()
		candidate := store.MemoryRecord{
			Namespace: in.Namespace, Key: in.Key, ContentType: in.ContentType,
			Content: in.Content, Embedding: embedding, Sensitivity: in.Sensitivity,
			Provenance: provenance, RetentionUntil: in.RetentionUntil,
		}
		candidateHash, _ := candidate.ContentHash()
		if existingHash == candidateHash {
			return record, true, nil
		}
		record.Content, record.ContentType = in.Content, in.ContentType
		record.Embedding = embedding
		record.Sensitivity = in.Sensitivity
		record.Provenance = provenance
		record.RetentionUntil = in.RetentionUntil
		record.TombstoneAt = nil
		record.ResourceVersion++
		record.UpdatedAt = now
		m.memories[record.ID] = record
		return record, false, nil
	}
	record := store.MemoryRecord{
		ID: uuid.New(), TenantID: in.TenantID, Namespace: in.Namespace, Key: in.Key,
		ContentType: in.ContentType, Content: in.Content, Embedding: embedding,
		EmbeddingProvider: "test", Sensitivity: in.Sensitivity,
		Provenance: provenance, RetentionUntil: in.RetentionUntil,
		SourceTaskID: in.SourceTaskID, SourceRunID: in.SourceRunID, SourceAttemptID: in.SourceAttemptID,
		ResourceVersion: 1, CreatedAt: now, UpdatedAt: now,
	}
	m.memories[record.ID] = record
	return record, false, nil
}

func (m *memoryStore) Get(_ context.Context, tenantID string, id uuid.UUID) (store.MemoryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.memories[id]
	if !ok || record.TenantID != tenantID {
		return store.MemoryRecord{}, store.ErrMemoryNotFound
	}
	return record, nil
}

func (m *memoryStore) Search(_ context.Context, in memory.SearchInput) ([]store.MemoryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var records []store.MemoryRecord
	for _, record := range m.memories {
		if record.TenantID != in.TenantID || record.TombstoneAt != nil {
			continue
		}
		if in.Namespace != "" && record.Namespace != in.Namespace {
			continue
		}
		if in.Sensitivity != "" && record.Sensitivity != in.Sensitivity {
			continue
		}
		if in.Query != "" && !strings.Contains(record.Content, in.Query) && !strings.Contains(record.Key, in.Query) {
			continue
		}
		records = append(records, record)
	}
	slices.SortFunc(records, func(a, b store.MemoryRecord) int { return strings.Compare(a.Key, b.Key) })
	return records, nil
}

func (m *memoryStore) Tombstone(_ context.Context, tenantID string, id uuid.UUID, expectedVersion int64) (store.MemoryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.memories[id]
	if !ok || record.TenantID != tenantID {
		return store.MemoryRecord{}, store.ErrMemoryNotFound
	}
	if record.ResourceVersion != expectedVersion {
		return store.MemoryRecord{}, store.ErrVersionConflict
	}
	if record.TombstoneAt != nil {
		return store.MemoryRecord{}, store.ErrInvalidTransition
	}
	now := time.Now().UTC()
	record.TombstoneAt = &now
	record.ResourceVersion++
	record.UpdatedAt = now
	m.memories[id] = record
	return record, nil
}

func TestListToolsReturnsRegisteredDescriptors(t *testing.T) {
	backend := newMemoryStore()
	backend.tools = []store.ToolDescriptor{
		{ID: uuid.New(), TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: store.ToolRiskLow,
			Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
			ParamsSchema: json.RawMessage(`{"type":"object"}`)},
	}
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	request := httptest.NewRequest(http.MethodGet, "/v1/tools", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Tools []map[string]any `json:"tools"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Tools) != 1 || body.Tools[0]["name"] != "fs.read" || body.Tools[0]["sideEffectRisk"] != "low" {
		t.Fatalf("unexpected tools response: %+v", body)
	}
}

func TestDecideApprovalRequiresBindingAndIfMatch(t *testing.T) {
	backend := newMemoryStore()
	approvalID := uuid.New()
	now := time.Now().UTC()
	backend.approvals[approvalID] = store.ToolApproval{
		ID: approvalID, TenantID: "tenant-a", CallID: uuid.New(), TaskID: uuid.New(),
		RunID: uuid.New(), AttemptID: uuid.New(), ToolName: "fs.write", ToolVersion: "1.0.0",
		Action: "write", Resource: "fs:/tmp", ArgsHash: [32]byte{1}, Status: store.ToolApprovalPending,
		RequestedAt: now, ExpiresAt: now.Add(time.Hour), ResourceVersion: 1,
	}
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)

	request := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+approvalID.String()+":decide",
		bytes.NewReader([]byte(`{"decision":"APPROVED","decidedBy":"human-1"}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("If-Match", `W/"1"`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "APPROVED" || body["decidedBy"] != "human-1" || body["resourceVersion"] != float64(2) {
		t.Fatalf("unexpected approval response: %+v", body)
	}

	stale := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+approvalID.String()+":decide",
		bytes.NewReader([]byte(`{"decision":"REJECTED","decidedBy":"human-1"}`)))
	stale.Header.Set("Content-Type", "application/json")
	stale.Header.Set("If-Match", `W/"1"`)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, stale)
	if response.Code != http.StatusConflict {
		t.Fatalf("stale decision status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "RESOURCE_VERSION_CONFLICT")

	invalid := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+approvalID.String()+":decide",
		bytes.NewReader([]byte(`{"decision":"MAYBE","decidedBy":"human-1"}`)))
	invalid.Header.Set("Content-Type", "application/json")
	invalid.Header.Set("If-Match", `W/"2"`)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, invalid)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid decision status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "INVALID_APPROVAL_DECISION")

	missing := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+uuid.New().String()+":decide",
		bytes.NewReader([]byte(`{"decision":"APPROVED","decidedBy":"human-1"}`)))
	missing.Header.Set("Content-Type", "application/json")
	missing.Header.Set("If-Match", `W/"1"`)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, missing)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing approval status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "APPROVAL_NOT_FOUND")
}

// TestTaskEventsStreamsLifecycle proves the SSE contract: the stream opens
// with the current snapshot, emits task.updated on every resource-version
// change, and closes with task.terminal once the task is terminal.
func TestTaskEventsStreamsLifecycle(t *testing.T) {
	backend := newMemoryStore()
	task, err := backend.CreateTask(context.Background(), store.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "watch me", Spec: []byte(`{}`), IdempotencyKey: "watch-request-1",
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.Task.ID.String()+"/events?poll=100ms", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()

	// Give the stream a moment to open, then advance the task to a terminal
	// phase; the stream must observe the change and close.
	time.Sleep(50 * time.Millisecond)
	if _, err := backend.RequestTaskCancellation(context.Background(), "tenant-a", task.Task.ID, 1); err != nil {
		t.Fatalf("advance task: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("event stream did not close after the task became terminal")
	}

	body := response.Body.String()
	var initial, updated, terminal bool
	for _, frame := range strings.Split(body, "\n\n") {
		if strings.TrimSpace(frame) == "" || strings.HasPrefix(frame, ": keepalive") {
			continue
		}
		var event, id string
		var data string
		for _, line := range strings.Split(frame, "\n") {
			switch {
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "id: "):
				id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			t.Fatalf("decode event data %q: %v", data, err)
		}
		switch {
		case event == "task.updated" && id == "1" && payload["phase"] == "QUEUED":
			initial = true
		case event == "task.updated" && id == "2" && payload["phase"] == "CANCELLED":
			updated = true
		case event == "task.terminal" && id == "2" && payload["phase"] == "CANCELLED":
			terminal = true
		default:
			t.Fatalf("unexpected event frame: event=%q id=%q payload=%v", event, id, payload)
		}
	}
	if !initial || !updated || !terminal {
		t.Fatalf("stream incomplete: initial=%v updated=%v terminal=%v\n%s", initial, updated, terminal, body)
	}
}

// TestTaskEventsClosesImmediatelyForTerminalTask proves that connecting to an
// already-terminal task yields a single snapshot pair and an immediate close.
func TestTaskEventsClosesImmediatelyForTerminalTask(t *testing.T) {
	backend := newMemoryStore()
	task, err := backend.CreateTask(context.Background(), store.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "done already", Spec: []byte(`{}`), IdempotencyKey: "watch-terminal-1",
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	if _, err := backend.RequestTaskCancellation(context.Background(), "tenant-a", task.Task.ID, 1); err != nil {
		t.Fatalf("advance task: %v", err)
	}
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.Task.ID.String()+"/events", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "event: task.terminal") {
		t.Fatalf("terminal task stream: %s", response.Body.String())
	}
}

// TestTaskEventsRejectsInvalidRequests proves the events route enforces the
// same tenant boundary and validates its poll parameter.
func TestTaskEventsRejectsInvalidRequests(t *testing.T) {
	backend := newMemoryStore()
	task, err := backend.CreateTask(context.Background(), store.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "private", Spec: []byte(`{}`), IdempotencyKey: "watch-private-1",
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	handler := controlapi.NewHandler(backend, backend, backend, backend)

	// Cross-tenant observation must fail closed.
	other := auth.StaticMiddleware(auth.Principal{Subject: "other", TenantID: "tenant-b"}, handler)
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.Task.ID.String()+"/events", nil)
	response := httptest.NewRecorder()
	other.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status = %d: %s", response.Code, response.Body.String())
	}

	// An invalid poll interval must be rejected up front, not streamed.
	owner := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, handler)
	request = httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.Task.ID.String()+"/events?poll=10s", nil)
	response = httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid poll status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "INVALID_POLL_INTERVAL")

	// POST is not allowed on the stream resource.
	request = httptest.NewRequest(http.MethodPost, "/v1/tasks/"+task.Task.ID.String()+"/events", nil)
	response = httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d: %s", response.Code, response.Body.String())
	}

	// Unknown tasks fail closed.
	request = httptest.NewRequest(http.MethodGet, "/v1/tasks/"+uuid.New().String()+"/events", nil)
	response = httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown task status = %d: %s", response.Code, response.Body.String())
	}
}

func performMemoryCreate(handler http.Handler, body []byte, idempotencyKey string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/memories", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// TestMemoryCRUDAndSearchProves the memory control-plane surface: idempotent
// writes, corrections, tenant isolation, hybrid search and CAS tombstones.
func TestMemoryCRUDAndSearch(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	body := []byte(`{"namespace":"default","key":"k1","contentType":"text/plain","content":"the quick brown fox"}`)

	created := performMemoryCreate(handler, body, "mem-1")
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d: %s", created.Code, created.Body.String())
	}
	var record map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &record); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if record["key"] != "k1" || record["sensitivity"] != "internal" || record["resourceVersion"] != float64(1) {
		t.Fatalf("unexpected memory response: %+v", record)
	}
	if created.Header().Get("Location") == "" || created.Header().Get("ETag") != `W/"1"` {
		t.Fatalf("missing resource headers: %+v", created.Header())
	}

	// Identical resubmission replays idempotently.
	replayed := performMemoryCreate(handler, body, "mem-1")
	if replayed.Code != http.StatusOK || replayed.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay status=%d headers=%v body=%s", replayed.Code, replayed.Header(), replayed.Body.String())
	}

	// A different document under the same key is a correction (new version).
	corrected := performMemoryCreate(handler, []byte(`{"namespace":"default","key":"k1","contentType":"text/plain","content":"the quick brown fox runs"}`), "mem-2")
	if corrected.Code != http.StatusCreated {
		t.Fatalf("correction status = %d: %s", corrected.Code, corrected.Body.String())
	}
	var correctedRecord map[string]any
	_ = json.Unmarshal(corrected.Body.Bytes(), &correctedRecord)
	if correctedRecord["resourceVersion"] != float64(2) {
		t.Fatalf("correction version = %v, want 2", correctedRecord["resourceVersion"])
	}

	// Search by keyword.
	search := httptest.NewRequest(http.MethodGet, "/v1/memories?query=quick+brown", nil)
	searchResponse := httptest.NewRecorder()
	handler.ServeHTTP(searchResponse, search)
	if searchResponse.Code != http.StatusOK {
		t.Fatalf("search status = %d: %s", searchResponse.Code, searchResponse.Body.String())
	}
	var searchBody struct {
		Memories []map[string]any `json:"memories"`
	}
	if err := json.Unmarshal(searchResponse.Body.Bytes(), &searchBody); err != nil {
		t.Fatalf("decode search: %v", err)
	}
	if len(searchBody.Memories) != 1 || searchBody.Memories[0]["key"] != "k1" {
		t.Fatalf("unexpected search results: %+v", searchBody)
	}

	// An empty search (no query, no embedding) is rejected.
	empty := httptest.NewRequest(http.MethodGet, "/v1/memories", nil)
	emptyResponse := httptest.NewRecorder()
	handler.ServeHTTP(emptyResponse, empty)
	if emptyResponse.Code != http.StatusBadRequest {
		t.Fatalf("empty search status = %d: %s", emptyResponse.Code, emptyResponse.Body.String())
	}
	assertReason(t, emptyResponse, "INVALID_SEARCH")

	// An invalid embedding parameter is rejected.
	badVector := httptest.NewRequest(http.MethodGet, "/v1/memories?embedding=not-json", nil)
	badVectorResponse := httptest.NewRecorder()
	handler.ServeHTTP(badVectorResponse, badVector)
	if badVectorResponse.Code != http.StatusBadRequest {
		t.Fatalf("bad embedding status = %d: %s", badVectorResponse.Code, badVectorResponse.Body.String())
	}
	assertReason(t, badVectorResponse, "INVALID_EMBEDDING")

	// Get by ID, then tombstone with CAS.
	id := record["id"].(string)
	get := httptest.NewRequest(http.MethodGet, "/v1/memories/"+id, nil)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", getResponse.Code, getResponse.Body.String())
	}

	// Missing If-Match is rejected before any mutation.
	noMatch := httptest.NewRequest(http.MethodDelete, "/v1/memories/"+id, nil)
	noMatchResponse := httptest.NewRecorder()
	handler.ServeHTTP(noMatchResponse, noMatch)
	if noMatchResponse.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status = %d: %s", noMatchResponse.Code, noMatchResponse.Body.String())
	}

	// Stale If-Match conflicts.
	stale := httptest.NewRequest(http.MethodDelete, "/v1/memories/"+id, nil)
	stale.Header.Set("If-Match", `W/"1"`)
	staleResponse := httptest.NewRecorder()
	handler.ServeHTTP(staleResponse, stale)
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("stale tombstone status = %d: %s", staleResponse.Code, staleResponse.Body.String())
	}

	tombstone := httptest.NewRequest(http.MethodDelete, "/v1/memories/"+id, nil)
	tombstone.Header.Set("If-Match", `W/"2"`)
	tombstoneResponse := httptest.NewRecorder()
	handler.ServeHTTP(tombstoneResponse, tombstone)
	if tombstoneResponse.Code != http.StatusOK {
		t.Fatalf("tombstone status = %d: %s", tombstoneResponse.Code, tombstoneResponse.Body.String())
	}
	var tombstoned map[string]any
	_ = json.Unmarshal(tombstoneResponse.Body.Bytes(), &tombstoned)
	if tombstoned["tombstoneAt"] == nil {
		t.Fatalf("tombstone response: %+v", tombstoned)
	}

	// Tombstoned records disappear from search.
	after := httptest.NewRequest(http.MethodGet, "/v1/memories?query=quick+brown", nil)
	afterResponse := httptest.NewRecorder()
	handler.ServeHTTP(afterResponse, after)
	var afterBody struct {
		Memories []map[string]any `json:"memories"`
	}
	_ = json.Unmarshal(afterResponse.Body.Bytes(), &afterBody)
	if len(afterBody.Memories) != 0 {
		t.Fatalf("tombstoned record still searchable: %+v", afterBody)
	}

	// Cross-tenant reads fail closed.
	other := auth.StaticMiddleware(auth.Principal{Subject: "other", TenantID: "tenant-b"},
		controlapi.NewHandler(backend, backend, backend, backend))
	otherGet := httptest.NewRequest(http.MethodGet, "/v1/memories/"+id, nil)
	otherResponse := httptest.NewRecorder()
	other.ServeHTTP(otherResponse, otherGet)
	if otherResponse.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status = %d: %s", otherResponse.Code, otherResponse.Body.String())
	}
}

// TestMemoryValidationRejectsBadBodies proves the strict request contract.
func TestMemoryValidationRejectsBadBodies(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "user-1", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)

	// Unknown fields are rejected.
	unknown := performMemoryCreate(handler, []byte(`{"namespace":"n","key":"k","contentType":"text/plain","content":"x","status":"SUCCEEDED"}`), "mem-1")
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d: %s", unknown.Code, unknown.Body.String())
	}
	assertReason(t, unknown, "INVALID_JSON")

	// Missing key is unprocessable.
	missing := performMemoryCreate(handler, []byte(`{"namespace":"n","contentType":"text/plain","content":"x"}`), "mem-2")
	if missing.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing key status = %d: %s", missing.Code, missing.Body.String())
	}
	assertReason(t, missing, "INVALID_MEMORY")

	// A malformed source reference is unprocessable.
	badRef := performMemoryCreate(handler, []byte(`{"namespace":"n","key":"k","contentType":"text/plain","content":"x","sourceTaskId":"not-a-uuid"}`), "mem-3")
	if badRef.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad source status = %d: %s", badRef.Code, badRef.Body.String())
	}
	assertReason(t, badRef, "INVALID_MEMORY")

	// Unauthenticated requests fail.
	anonymous := controlapi.NewHandler(backend, backend, backend, backend)
	request := httptest.NewRequest(http.MethodPost, "/v1/memories", bytes.NewReader([]byte(`{"namespace":"n","key":"k","contentType":"text/plain","content":"x"}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "mem-anon")
	response := httptest.NewRecorder()
	anonymous.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous status = %d: %s", response.Code, response.Body.String())
	}
}
