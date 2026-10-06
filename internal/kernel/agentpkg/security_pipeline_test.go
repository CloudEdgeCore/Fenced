package agentpkg_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
)

func createTestPackage(t *testing.T, abiVersion string, secrets []string) (*agentpkg.Package, *agentpkg.Registry, *agentpkg.SigningKey) {
	t.Helper()
	signingKey, pubKey, err := agentpkg.GenerateSigningKey("test-publisher-key")
	if err != nil {
		t.Fatalf("failed to generate signing key: %v", err)
	}

	reg := agentpkg.NewRegistry()
	if err := reg.Add(*pubKey); err != nil {
		t.Fatalf("failed to register public key: %v", err)
	}

	specMap := map[string]any{
		"agent": "security-agent",
		"capabilities": map[string]any{
			"tools":   []string{"search_docs"},
			"models":  []string{"gpt-4o"},
			"secrets": secrets,
		},
	}
	if abiVersion != "" {
		specMap["syscallAbi"] = abiVersion
	}
	specBytes, _ := json.Marshal(specMap)
	specDigest := agentpkg.ComputeContentDigest(specBytes)

	manifest := agentpkg.Manifest{
		Schema:          agentpkg.ManifestSchema,
		AgentVersionRef: "security-agent@1.0.0",
		SpecDigest:      specDigest,
		Spec:            specBytes,
		RuntimeLock:     []agentpkg.Digest{agentpkg.ComputeContentDigest([]byte("runtime-lock"))},
		ToolLock:        []agentpkg.Digest{agentpkg.ComputeContentDigest([]byte("tool-lock"))},
		Permissions:     agentpkg.ComputeContentDigest([]byte("permissions")),
		MemorySchema:    agentpkg.ComputeContentDigest([]byte("memory-schema")),
		SBOM:            agentpkg.ComputeContentDigest([]byte("cyclonedx-sbom")),
		Provenance: agentpkg.Provenance{
			Builder:       "ci-builder",
			BuildWorkflow: "release-workflow",
			GitCommit:     "commit123",
			BuiltAt:       time.Now().UTC(),
		},
	}

	pkg, err := agentpkg.Sign(manifest, signingKey)
	if err != nil {
		t.Fatalf("failed to sign package: %v", err)
	}

	return pkg, reg, signingKey
}

func TestSecurityPipelineHappyPath(t *testing.T) {
	ctx := context.Background()
	pkg, keyReg, _ := createTestPackage(t, "1.0.0", []string{"API_KEY"})

	req := agentpkg.SecurityInstallRequest{
		Package:       pkg,
		ExpectedKeyID: "test-publisher-key",
		AllowedCapabilities: map[string]bool{
			"tool:search_docs": true,
			"secret:API_KEY":   true,
		},
		TenantID: "tenant-prod",
	}

	res, err := agentpkg.ExecuteSecurityPipeline(ctx, req, keyReg)
	if err != nil {
		t.Fatalf("expected pipeline to pass, got error: %v", err)
	}

	if !res.VerifiedSignature || !res.VerifiedDigest || !res.VerifiedSBOM || !res.VerifiedABI || !res.VerifiedPolicy {
		t.Fatalf("all verification flags must be true, got %+v", res)
	}
	if res.AgentVersionRef != "security-agent@1.0.0" {
		t.Fatalf("unexpected agent version ref: %s", res.AgentVersionRef)
	}
}

func TestSecurityPipelineRejectsTamperedDigest(t *testing.T) {
	ctx := context.Background()
	pkg, keyReg, _ := createTestPackage(t, "1.0.0", []string{})

	// Tamper with the spec bytes
	pkg.Manifest.Spec = []byte(`{"agent":"tampered-agent"}`)

	req := agentpkg.SecurityInstallRequest{
		Package:  pkg,
		TenantID: "tenant-prod",
	}

	_, err := agentpkg.ExecuteSecurityPipeline(ctx, req, keyReg)
	if err == nil {
		t.Fatalf("expected error on tampered package spec, got nil")
	}
}

func TestSecurityPipelineRejectsIncompatibleABI(t *testing.T) {
	ctx := context.Background()
	pkg, keyReg, _ := createTestPackage(t, "2.0.0-incompatible", []string{})

	req := agentpkg.SecurityInstallRequest{
		Package:  pkg,
		TenantID: "tenant-prod",
	}

	_, err := agentpkg.ExecuteSecurityPipeline(ctx, req, keyReg)
	if err == nil {
		t.Fatalf("expected ABI error, got nil")
	}
}

func TestSecurityPipelineRejectsUnauthorizedCapabilities(t *testing.T) {
	ctx := context.Background()
	pkg, keyReg, _ := createTestPackage(t, "1.0.0", []string{"UNAUTHORIZED_SECRET"})

	req := agentpkg.SecurityInstallRequest{
		Package: pkg,
		AllowedCapabilities: map[string]bool{
			"tool:search_docs": true,
			// UNAUTHORIZED_SECRET not allowed in policy!
		},
		TenantID: "tenant-restricted",
	}

	_, err := agentpkg.ExecuteSecurityPipeline(ctx, req, keyReg)
	if err == nil {
		t.Fatalf("expected capability policy violation error, got nil")
	}
}

func TestSecurityPipelineRejectsUnsignedOrWrongKey(t *testing.T) {
	ctx := context.Background()
	pkg, _, _ := createTestPackage(t, "1.0.0", []string{})

	// Use an empty key registry that does not trust the signing key
	emptyReg := agentpkg.NewRegistry()

	req := agentpkg.SecurityInstallRequest{
		Package:  pkg,
		TenantID: "tenant-prod",
	}

	_, err := agentpkg.ExecuteSecurityPipeline(ctx, req, emptyReg)
	if err == nil {
		t.Fatalf("expected signature verification failure, got nil")
	}
}
