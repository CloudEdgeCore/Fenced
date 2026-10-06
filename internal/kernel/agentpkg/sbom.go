package agentpkg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// SBOMSpecVersion is the CycloneDX version the generator emits (ADR-010:
// CI produces an SPDX or CycloneDX SBOM for every package).
const SBOMSpecVersion = "1.5"

type sbomDocument struct {
	BOMFormat    string          `json:"bomFormat"`
	SpecVersion  string          `json:"specVersion"`
	SerialNumber string          `json:"serialNumber"`
	Version      int             `json:"version"`
	Metadata     sbomMetadata    `json:"metadata"`
	Components   []sbomComponent `json:"components,omitempty"`
}

type sbomMetadata struct {
	Timestamp string         `json:"timestamp"`
	Tools     []sbomTool     `json:"tools,omitempty"`
	Component *sbomComponent `json:"component,omitempty"`
}

type sbomTool struct {
	Vendor  string `json:"vendor"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

type sbomComponent struct {
	Type    string     `json:"type"`
	Name    string     `json:"name"`
	Version string     `json:"version,omitempty"`
	Hashes  []sbomHash `json:"hashes,omitempty"`
}

type sbomHash struct {
	Alg     string `json:"alg"`
	Content string `json:"content"`
}

// GenerateSBOM emits a minimal CycloneDX document describing the locked
// runtime and tool digests of the manifest. The caller pins its digest into
// the manifest's sbom field.
func GenerateSBOM(manifest Manifest) ([]byte, error) {
	document := sbomDocument{
		BOMFormat: "CycloneDX", SpecVersion: SBOMSpecVersion,
		SerialNumber: "urn:uuid:" + uuid.NewString(), Version: 1,
		Metadata: sbomMetadata{
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Tools:     []sbomTool{{Vendor: "fenced", Name: "fenced-pkg", Version: "v0.5"}},
			Component: &sbomComponent{Type: "application", Name: manifest.AgentVersionRef},
		},
	}
	for _, digest := range manifest.RuntimeLock {
		document.Components = append(document.Components, sbomComponent{
			Type: "container", Name: digest.String(),
			Hashes: []sbomHash{{Alg: "SHA-256", Content: digest.Hex}},
		})
	}
	for _, digest := range manifest.ToolLock {
		document.Components = append(document.Components, sbomComponent{
			Type: "library", Name: digest.String(),
			Hashes: []sbomHash{{Alg: "SHA-256", Content: digest.Hex}},
		})
	}
	return json.MarshalIndent(document, "", "  ")
}

// VerifySBOM checks that the document is a CycloneDX SBOM and that its
// SHA-256 digest matches the manifest pin.
func VerifySBOM(document []byte, expected Digest) error {
	if len(document) == 0 {
		return fmt.Errorf("%w: sbom document is empty", ErrPackageManifestInvalid)
	}
	if !expected.Verify(document) {
		return fmt.Errorf("%w: sbom digest does not match the manifest pin", ErrPackageManifestInvalid)
	}
	var parsed sbomDocument
	if err := json.Unmarshal(document, &parsed); err != nil {
		return fmt.Errorf("%w: sbom is not valid JSON: %v", ErrPackageManifestInvalid, err)
	}
	if parsed.BOMFormat != "CycloneDX" || parsed.SpecVersion == "" {
		return fmt.Errorf("%w: sbom is not a CycloneDX document", ErrPackageManifestInvalid)
	}
	return nil
}

// SBOMDigest returns the sha256 digest of the generated document for pinning
// into the manifest.
func SBOMDigest(document []byte) Digest {
	sum := sha256.Sum256(document)
	return Digest{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}
