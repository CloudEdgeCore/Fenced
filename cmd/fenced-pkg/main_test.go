package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
)

func cliManifest() agentversion.Manifest {
	return agentversion.Manifest{
		APIVersion: agentversion.ManifestAPIVersion,
		Kind:       agentversion.ManifestKind,
		Metadata: agentversion.Metadata{
			Name: "cli-agent", Version: "1.0.0", Namespace: "default",
		},
		Spec: agentversion.Spec{
			RuntimeClassPolicy: agentversion.RuntimeClassPolicy{Allowed: []string{"wasmtime"}, Preferred: "wasmtime"},
			Runtimes: []agentversion.RuntimeTarget{{
				Class: "wasmtime", Interface: agentversion.RuntimeInterfaceV1,
				RuntimeABI: "fenced.wasm-component/v1", Entrypoint: []string{"agent.wasm"},
			}},
			Capabilities: &agentversion.Capabilities{
				Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{},
			},
			Resources:  &agentversion.ResourceLimits{CPUMillis: 100, MemoryMiB: 128},
			Budget:     &agentversion.Budget{WallSeconds: 60},
			Checkpoint: &agentversion.CheckpointPolicy{Mode: agentversion.CheckpointNone},
		},
	}
}

func TestRunValidateManifestWritesCanonicalDocument(t *testing.T) {
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "agent.json")
	outputPath := filepath.Join(directory, "canonical.json")
	raw, err := json.MarshalIndent(cliManifest(), "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(inputPath, raw, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := runValidateManifest([]string{"-manifest", inputPath, "-out", outputPath}); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	canonical, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read canonical manifest: %v", err)
	}
	manifest, replay, _, err := agentversion.DecodeManifest(canonical)
	if err != nil {
		t.Fatalf("decode canonical manifest: %v", err)
	}
	if manifest.Ref() != "cli-agent@1.0.0" || string(canonical) != string(replay) {
		t.Fatalf("canonical manifest mismatch: %s", canonical)
	}
}

func TestRunValidateManifestRejectsUnknownFields(t *testing.T) {
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "agent.json")
	if err := os.WriteFile(inputPath, []byte(`{"apiVersion":"fenced.dev/v1alpha1","kind":"AgentManifest","unknown":true}`), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := runValidateManifest([]string{"-manifest", inputPath}); err == nil {
		t.Fatal("unknown manifest field was accepted")
	}
}

func TestRunPackageManifestBindsPortableSpecAndProvenance(t *testing.T) {
	directory := t.TempDir()
	agentPath := filepath.Join(directory, "agent.json")
	packagePath := filepath.Join(directory, "package-manifest.json")
	raw, err := json.Marshal(cliManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(agentPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	const builtAt = "2026-08-21T08:30:00Z"
	args := []string{
		"-agent-manifest", agentPath, "-builder", "ci-builder", "-workflow", "agent-package.yml",
		"-git-commit", "abc123", "-built-at", builtAt, "-out", packagePath,
	}
	if err := runPackageManifest(args); err != nil {
		t.Fatalf("package manifest: %v", err)
	}
	encoded, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	var packageManifest agentpkg.Manifest
	if err := json.Unmarshal(encoded, &packageManifest); err != nil {
		t.Fatal(err)
	}
	if err := packageManifest.Validate(); err != nil {
		t.Fatalf("generated package manifest is invalid: %v", err)
	}
	if packageManifest.AgentVersionRef != "cli-agent@1.0.0" ||
		packageManifest.Provenance.Builder != "ci-builder" ||
		!packageManifest.Provenance.BuiltAt.Equal(time.Date(2026, 8, 21, 8, 30, 0, 0, time.UTC)) {
		t.Fatalf("unexpected generated package manifest: %+v", packageManifest)
	}
	if !packageManifest.SpecDigest.Verify(packageManifest.Spec) {
		t.Fatal("generated package manifest does not bind its normalized spec")
	}

	secondPath := filepath.Join(directory, "package-manifest-second.json")
	args[len(args)-1] = secondPath
	if err := runPackageManifest(args); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(second) {
		t.Fatal("identical inputs did not produce a reproducible package manifest")
	}
}

func TestRunPackageManifestRequiresDeterministicTimestamp(t *testing.T) {
	if err := runPackageManifest([]string{
		"-agent-manifest", "unused.json", "-builder", "ci", "-workflow", "build.yml", "-git-commit", "abc",
	}); err == nil {
		t.Fatal("missing build timestamp was accepted")
	}
}
