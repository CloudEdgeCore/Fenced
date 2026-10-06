package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
)

func TestPackageCLILifecycleAndSecurityGates(t *testing.T) {
	tempDir := t.TempDir()
	origWd, _ := os.Getwd()
	_ = os.Chdir(tempDir)
	defer func() { _ = os.Chdir(origWd) }()

	// 1. Create a valid test manifest
	manifestContent := `{
  "apiVersion": "fenced.dev/v1",
  "kind": "AgentManifest",
  "metadata": {
    "name": "cli-test-agent",
    "version": "1.0.0",
    "namespace": "default"
  },
  "spec": {
    "runtimeClassPolicy": {
      "allowed": ["oci"],
      "preferred": "oci"
    },
    "runtimes": [
      {
        "class": "oci",
        "interface": "fenced.runtime.interface/v1",
        "runtimeABI": "fenced.oci/v1",
        "entrypoint": ["/agent/bin/run"]
      }
    ],
    "capabilities": {
      "tools": ["calculator"],
      "models": ["gpt-4o"],
      "secrets": ["MOCK_KEY"],
      "memory": []
    },
    "resources": {
      "cpuMillis": 500,
      "memoryMiB": 512,
      "workspaceBytes": 67108864
    },
    "budget": {
      "tokens": 100000,
      "costUsd": 10.0,
      "toolCalls": 100,
      "wallSeconds": 3600
    },
    "checkpoint": {
      "mode": "logical",
      "schemaVersion": "test-state/v1",
      "intervalSeconds": 30
    }
  }
}`
	manifestPath := filepath.Join(tempDir, "agent.json")
	if err := os.WriteFile(manifestPath, []byte(manifestContent), 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	// 2. Test fenced login
	var stdout, stderr bytes.Buffer
	loginErr := run([]string{"login", "-registry", "https://mock.registry.dev", "-token", "test-token-123"}, &stdout, &stderr)
	if loginErr != nil {
		t.Fatalf("login failed: %v, stderr: %s", loginErr, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Successfully authenticated") {
		t.Fatalf("expected login success message, got: %s", stdout.String())
	}

	// 3. Test fenced package build
	stdout.Reset()
	stderr.Reset()
	pkgManifestPath := filepath.Join(tempDir, "package-manifest.json")
	buildErr := run([]string{
		"package", "build",
		"-manifest", manifestPath,
		"-builder", "ci-bot",
		"-workflow", "release",
		"-git-commit", "abc1234",
		"-out", pkgManifestPath,
	}, &stdout, &stderr)
	if buildErr != nil {
		t.Fatalf("package build failed: %v, stderr: %s", buildErr, stderr.String())
	}
	if !strings.Contains(stdout.String(), "built successfully") {
		t.Fatalf("expected build success message, got: %s", stdout.String())
	}

	// 4. Generate Ed25519 signing key
	signingKey, pubKey, err := agentpkg.GenerateSigningKey("test-key-id")
	if err != nil {
		t.Fatalf("failed to generate signing key: %v", err)
	}
	privKeyB64 := base64.StdEncoding.EncodeToString(signingKey.PrivateKey)
	pubKeyB64 := base64.StdEncoding.EncodeToString(pubKey.PublicKey)

	// 5. Test fenced package sign
	stdout.Reset()
	stderr.Reset()
	signedPkgPath := filepath.Join(tempDir, "package.signed.json")
	signErr := run([]string{
		"package", "sign",
		"-package", pkgManifestPath,
		"-key-id", "test-key-id",
		"-private-key", privKeyB64,
		"-out", signedPkgPath,
	}, &stdout, &stderr)
	if signErr != nil {
		t.Fatalf("package sign failed: %v, stderr: %s", signErr, stderr.String())
	}
	if !strings.Contains(stdout.String(), "signed successfully") {
		t.Fatalf("expected sign success message, got: %s", stdout.String())
	}

	// 6. Test fenced package push
	stdout.Reset()
	stderr.Reset()
	pushErr := run([]string{
		"package", "push",
		"-package", signedPkgPath,
		"-registry", "https://mock.registry.dev",
	}, &stdout, &stderr)
	if pushErr != nil {
		t.Fatalf("package push failed: %v, stderr: %s", pushErr, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Package pushed and indexed successfully") {
		t.Fatalf("expected push success message, got: %s", stdout.String())
	}

	// 7. Test fenced package search
	stdout.Reset()
	stderr.Reset()
	searchErr := run([]string{"package", "search", "sre"}, &stdout, &stderr)
	if searchErr != nil {
		t.Fatalf("package search failed: %v, stderr: %s", searchErr, stderr.String())
	}
	if !strings.Contains(stdout.String(), "NAME") || !strings.Contains(stdout.String(), "CAPABILITIES") {
		t.Fatalf("expected search table headers, got: %s", stdout.String())
	}

	// 8. Test fenced package verify
	stdout.Reset()
	stderr.Reset()
	verifyErr := run([]string{
		"package", "verify",
		"-package", signedPkgPath,
		"-public-key", pubKeyB64,
	}, &stdout, &stderr)
	if verifyErr != nil {
		t.Fatalf("package verify failed: %v, stderr: %s", verifyErr, stderr.String())
	}
	if !strings.Contains(stdout.String(), "VERIFIED") {
		t.Fatalf("expected verified message, got: %s", stdout.String())
	}

	// 9. Test fenced package install (6-stage security gate)
	stdout.Reset()
	stderr.Reset()
	installErr := run([]string{
		"package", "install",
		"-package", signedPkgPath,
		"-public-key", pubKeyB64,
		"-tenant", "prod-tenant",
	}, &stdout, &stderr)
	if installErr != nil {
		t.Fatalf("package install failed: %v, stderr: %s", installErr, stderr.String())
	}
	installOutput := stdout.String()
	if !strings.Contains(installOutput, "Stage 1 [FETCH]") ||
		!strings.Contains(installOutput, "Stage 2 [SIGNATURE]") ||
		!strings.Contains(installOutput, "Stage 3 [SBOM & DIGEST]") ||
		!strings.Contains(installOutput, "Stage 4 [ABI]") ||
		!strings.Contains(installOutput, "Stage 5 [CAPABILITY & POLICY]") ||
		!strings.Contains(installOutput, "Package Installation COMPLETE") {
		t.Fatalf("expected full 6-stage security pipeline output, got: %s", installOutput)
	}

	// 10. Test security gate rejection on tampered package
	tamperedPkgPath := filepath.Join(tempDir, "package.tampered.json")
	var pkg agentpkg.Package
	pkgBytes, _ := os.ReadFile(signedPkgPath)
	_ = json.Unmarshal(pkgBytes, &pkg)
	pkg.Manifest.Spec = []byte(`{"agent":"malicious-tampered-spec"}`)
	tamperedBytes, _ := json.Marshal(pkg)
	_ = os.WriteFile(tamperedPkgPath, tamperedBytes, 0o644)

	stdout.Reset()
	stderr.Reset()
	badInstallErr := run([]string{
		"package", "install",
		"-package", tamperedPkgPath,
		"-public-key", pubKeyB64,
	}, &stdout, &stderr)
	if badInstallErr == nil {
		t.Fatalf("expected security gate to reject tampered package, but it succeeded!")
	}
	if !strings.Contains(stdout.String(), "SECURITY GATE REJECTED PACKAGE") {
		t.Fatalf("expected rejection notice, got: %s", stdout.String())
	}
}

func TestRuntimeInitCommand(t *testing.T) {
	tempDir := t.TempDir()
	origWd, _ := os.Getwd()
	_ = os.Chdir(tempDir)
	defer func() { _ = os.Chdir(origWd) }()

	var stdout, stderr bytes.Buffer
	err := run([]string{"runtime", "init", "test-worker", "-template", "docker"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runtime init failed: %v, stderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Initialized Fenced runtime project") {
		t.Fatalf("expected init success message, got: %s", stdout.String())
	}

	// Check files created
	if _, err := os.Stat(filepath.Join(tempDir, "test-worker", "main.go")); err != nil {
		t.Fatalf("main.go not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "test-worker", "manifest.json")); err != nil {
		t.Fatalf("manifest.json not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "test-worker", "README.md")); err != nil {
		t.Fatalf("README.md not created: %v", err)
	}
}
