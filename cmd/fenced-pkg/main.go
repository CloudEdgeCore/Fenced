// Command fenced-pkg is the CI-facing Agent Package tool (ADR-010): it
// generates package-signing keys, signs canonical manifests, and verifies
// signed packages against a trust registry. The control plane publishes a
// version only when its package verifies (fail-closed when the trust registry
// is configured).
//
// Usage:
//
//	fenced-pkg genkey -id ci-builder-1              # prints keyId + base64 keys
//	fenced-pkg sign -manifest manifest.json \
//	    -key-id ci-builder-1 -private-key <b64> -out package.json
//	fenced-pkg verify -package package.json \
//	    -trust-key ci-builder-1=<b64pub> [-trust-key ...]
//	fenced-pkg validate-manifest -manifest agent-manifest.json \
//	    -out canonical-agent-manifest.json
//	fenced-pkg package-manifest -agent-manifest agent-manifest.json \
//	    -builder ci -workflow build.yml -git-commit abc -built-at <RFC3339> \
//	    -out package-manifest.json
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	if len(os.Args) < 2 {
		fail(nil, "usage: fenced-pkg <genkey|sign|verify|validate-manifest|package-manifest|sbom|verify-sbom> [flags]")
	}
	var err error
	switch os.Args[1] {
	case "genkey":
		err = runGenKey(os.Args[2:])
	case "sign":
		err = runSign(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "validate-manifest":
		err = runValidateManifest(os.Args[2:])
	case "package-manifest":
		err = runPackageManifest(os.Args[2:])
	case "sbom":
		err = runSBOM(os.Args[2:])
	case "verify-sbom":
		err = runVerifySBOM(os.Args[2:])
	default:
		fail(nil, "unknown command %q; want genkey, sign, verify, validate-manifest, package-manifest, sbom or verify-sbom", os.Args[1])
	}
	if err != nil {
		fail(err, "")
	}
}

// runPackageManifest bridges the portable Agent Manifest to the existing
// signed Agent Package pipeline. Provenance time is an explicit input so the
// same source declaration produces byte-identical unsigned package manifests.
func runPackageManifest(args []string) error {
	flags := flag.NewFlagSet("package-manifest", flag.ExitOnError)
	manifestPath := flags.String("agent-manifest", "", "strict AgentManifest JSON file")
	builder := flags.String("builder", "", "build system identity")
	workflow := flags.String("workflow", "", "build workflow identity")
	gitCommit := flags.String("git-commit", "", "source git commit")
	builtAtText := flags.String("built-at", "", "reproducible build timestamp in RFC3339 format")
	outPath := flags.String("out", "", "output unsigned package manifest JSON file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" || *builder == "" || *workflow == "" || *gitCommit == "" || *builtAtText == "" {
		return errors.New("-agent-manifest, -builder, -workflow, -git-commit and -built-at are required")
	}
	builtAt, err := time.Parse(time.RFC3339, *builtAtText)
	if err != nil {
		return fmt.Errorf("parse -built-at: %w", err)
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return fmt.Errorf("read agent manifest: %w", err)
	}
	manifest, _, _, err := agentversion.DecodeManifest(raw)
	if err != nil {
		return err
	}
	packageManifest, err := agentpkg.FromAgentManifest(manifest,
		agentpkg.Provenance{
			Builder: *builder, BuildWorkflow: *workflow, GitCommit: *gitCommit, BuiltAt: builtAt.UTC(),
		})
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(packageManifest)
	if err != nil {
		return fmt.Errorf("encode package manifest: %w", err)
	}
	if *outPath == "" {
		fmt.Println(string(encoded))
		return nil
	}
	if err := os.WriteFile(*outPath, encoded, 0o600); err != nil {
		return fmt.Errorf("write package manifest: %w", err)
	}
	return nil
}

func runValidateManifest(args []string) error {
	flags := flag.NewFlagSet("validate-manifest", flag.ExitOnError)
	manifestPath := flags.String("manifest", "", "strict AgentManifest JSON file")
	outPath := flags.String("out", "", "write canonical AgentManifest JSON to this file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("-manifest is required")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return fmt.Errorf("read agent manifest: %w", err)
	}
	manifest, canonical, digest, err := agentversion.DecodeManifest(raw)
	if err != nil {
		return err
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, canonical, 0o600); err != nil {
			return fmt.Errorf("write canonical agent manifest: %w", err)
		}
	}
	fmt.Printf("manifest OK: ref=%s apiVersion=%s runtimeTargets=%d manifestDigest=%x\n",
		manifest.Ref(), manifest.APIVersion, len(manifest.Spec.Runtimes), digest)
	return nil
}

func runGenKey(args []string) error {
	flags := flag.NewFlagSet("genkey", flag.ExitOnError)
	keyID := flags.String("id", "", "key identity (must be unique in the trust registry)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *keyID == "" {
		return errors.New("-id is required")
	}
	signingKey, key, err := agentpkg.GenerateSigningKey(*keyID)
	if err != nil {
		return err
	}
	type keyMaterial struct {
		KeyID      string `json:"keyId"`
		PublicKey  string `json:"publicKey"`  // base64 raw std; share with the control plane
		PrivateKey string `json:"privateKey"` // base64 raw std; store in the CI secret manager
	}
	encoded, err := json.MarshalIndent(keyMaterial{
		KeyID: key.ID, PublicKey: agentpkg.EncodePublicKey(key.PublicKey),
		PrivateKey: base64.RawStdEncoding.EncodeToString(signingKey.PrivateKey),
	}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func runSign(args []string) error {
	flags := flag.NewFlagSet("sign", flag.ExitOnError)
	manifestPath := flags.String("manifest", "", "canonical manifest JSON file")
	keyID := flags.String("key-id", "", "signing key identity")
	privateKey := flags.String("private-key", "", "base64 raw std ed25519 private key")
	outPath := flags.String("out", "", "output package JSON file (default stdout)")
	imageDigest := flags.String("image-digest", "", "digest-pinned OCI image (sha256:<64 hex>) to sign and embed in the manifest (cosign-style)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" || *keyID == "" || *privateKey == "" {
		return errors.New("-manifest, -key-id and -private-key are required")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var manifest agentpkg.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	decodedKey, err := agentpkg.DecodePrivateKey(*privateKey)
	if err != nil {
		return err
	}
	signingKey := &agentpkg.SigningKey{ID: *keyID, PrivateKey: decodedKey}
	if *imageDigest != "" {
		signature, err := agentpkg.SignImage(agentpkg.Digest{Algorithm: "sha256", Hex: strings.TrimPrefix(*imageDigest, "sha256:")}, signingKey)
		if err != nil {
			return err
		}
		manifest.SignedImageDigest = agentpkg.Digest{Algorithm: "sha256", Hex: strings.TrimPrefix(*imageDigest, "sha256:")}
		manifest.ImageSignature = signature
	}
	pkg, err := agentpkg.Sign(manifest, signingKey)
	if err != nil {
		return err
	}
	return writePackage(*outPath, pkg)
}

// runSBOM generates a CycloneDX SBOM for the manifest and prints the digest
// to pin into the manifest's sbom field.
func runSBOM(args []string) error {
	flags := flag.NewFlagSet("sbom", flag.ExitOnError)
	manifestPath := flags.String("manifest", "", "canonical manifest JSON file")
	outPath := flags.String("out", "", "output SBOM JSON file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("-manifest is required")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var manifest agentpkg.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	document, err := agentpkg.GenerateSBOM(manifest)
	if err != nil {
		return err
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, document, 0o600); err != nil {
			return fmt.Errorf("write sbom: %w", err)
		}
	} else {
		fmt.Println(string(document))
	}
	digest := agentpkg.SBOMDigest(document)
	fmt.Printf("sbom digest: %s (pin into the manifest sbom field)\n", digest.String())
	return nil
}

// runVerifySBOM checks an SBOM document against the manifest's sbom pin.
func runVerifySBOM(args []string) error {
	flags := flag.NewFlagSet("verify-sbom", flag.ExitOnError)
	manifestPath := flags.String("manifest", "", "canonical manifest JSON file")
	sbomPath := flags.String("sbom", "", "SBOM JSON file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" || *sbomPath == "" {
		return errors.New("-manifest and -sbom are required")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var manifest agentpkg.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	document, err := os.ReadFile(*sbomPath)
	if err != nil {
		return fmt.Errorf("read sbom: %w", err)
	}
	if err := agentpkg.VerifySBOM(document, manifest.SBOM); err != nil {
		return fmt.Errorf("SBOM verification FAILED: %w", err)
	}
	fmt.Printf("SBOM OK: specVersion=%s digest=%s\n", agentpkg.SBOMSpecVersion, manifest.SBOM.String())
	return nil
}

func runVerify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ExitOnError)
	packagePath := flags.String("package", "", "signed package JSON file")
	var trustKeys []string
	flags.Func("trust-key", "trusted key as keyID=base64rawpub (repeatable)", func(value string) error {
		trustKeys = append(trustKeys, value)
		return nil
	})
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *packagePath == "" || len(trustKeys) == 0 {
		return errors.New("-package and at least one -trust-key are required")
	}
	raw, err := os.ReadFile(*packagePath)
	if err != nil {
		return fmt.Errorf("read package: %w", err)
	}
	var pkg agentpkg.Package
	if err := json.Unmarshal(raw, &pkg); err != nil {
		return fmt.Errorf("decode package: %w", err)
	}
	registry := agentpkg.NewRegistry()
	for _, entry := range trustKeys {
		id, encoded, ok := cutKeyEntry(entry)
		if !ok {
			return fmt.Errorf("trust key %q must be keyID=base64rawpub", entry)
		}
		publicKey, err := agentpkg.DecodePublicKey(encoded)
		if err != nil {
			return err
		}
		if err := registry.Add(agentpkg.Key{ID: id, PublicKey: publicKey}); err != nil {
			return err
		}
	}
	if err := registry.Verify(&pkg); err != nil {
		return fmt.Errorf("package verification FAILED: %w", err)
	}
	fmt.Printf("package OK: keyId=%s agentVersionRef=%s manifestDigest=", pkg.Signature.KeyID, pkg.Manifest.AgentVersionRef)
	digest, err := agentpkg.ManifestDigest(pkg.Manifest)
	if err != nil {
		return err
	}
	fmt.Printf("%x\n", digest)
	return nil
}

func cutKeyEntry(entry string) (id, encoded string, ok bool) {
	for i := 0; i < len(entry); i++ {
		if entry[i] == '=' {
			return entry[:i], entry[i+1:], i > 0
		}
	}
	return "", "", false
}

// writePackage persists the package in its canonical encoding. json.Marshal
// (compact) is mandatory: Manifest.Spec is a RawMessage and MarshalIndent
// would re-indent its bytes, breaking digest/signature round trips.
func writePackage(outPath string, pkg *agentpkg.Package) error {
	encoded, err := json.Marshal(pkg)
	if err != nil {
		return err
	}
	if outPath == "" {
		fmt.Println(string(encoded))
		return nil
	}
	if err := os.WriteFile(outPath, encoded, 0o600); err != nil {
		return fmt.Errorf("write package: %w", err)
	}
	return nil
}

func fail(err error, format string, args ...any) {
	if format != "" {
		fmt.Fprintf(os.Stderr, "fenced-pkg: "+format+"\n", args...)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "fenced-pkg: %v\n", err)
	}
	os.Exit(1)
}
