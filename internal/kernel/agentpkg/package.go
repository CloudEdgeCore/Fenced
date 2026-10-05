// Package agentpkg defines the signed Agent Package manifest (ADR-010) and
// its admission verification. A package pins, by digest: the AgentVersion
// spec, runtime lock, tool lock, permissions, memory schema and SBOM, plus
// build provenance. v0.3 signs the canonical manifest with ed25519 under a
// registered key identity; production admission fails closed on any missing
// or invalid signature, provenance or unknown key. Cosign interop for OCI
// image signatures is a future projection; the manifest itself is the
// kernel's trust anchor.
package agentpkg

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrPackageUnsigned reports a package missing its signature.
var ErrPackageUnsigned = errors.New("agent package is not signed")

// ErrPackageSignatureInvalid reports a broken signature, an unknown key, or
// a manifest that does not match the signature.
var ErrPackageSignatureInvalid = errors.New("agent package signature is invalid")

// ErrPackageManifestInvalid reports a malformed manifest.
var ErrPackageManifestInvalid = errors.New("agent package manifest is invalid")

// ManifestSchema is the canonical manifest version.
const ManifestSchema = "fenced.agentpkg/v1"

// Digest is a content digest reference (algorithm + hex).
type Digest struct {
	Algorithm string `json:"algorithm"`
	Hex       string `json:"hex"`
}

func (d Digest) String() string { return d.Algorithm + ":" + d.Hex }

// VerifyDigest checks the digest against raw content.
func (d Digest) Verify(content []byte) bool {
	if d.Algorithm != "sha256" {
		return false
	}
	sum := sha256.Sum256(content)
	return strings.EqualFold(hex.EncodeToString(sum[:]), d.Hex)
}

// Provenance records who built the package and from what (in-toto/SLSA
// shape, v0.3 subset): the build workflow identity, the git commit, and the
// builder. Production admission checks workflow allow-listing on this field.
type Provenance struct {
	Builder       string    `json:"builder"`
	BuildWorkflow string    `json:"buildWorkflow"`
	GitCommit     string    `json:"gitCommit"`
	BuiltAt       time.Time `json:"builtAt"`
}

// Manifest is the canonical, signed package document.
type Manifest struct {
	Schema          string          `json:"schema"`
	AgentVersionRef string          `json:"agentVersionRef"`
	SpecDigest      Digest          `json:"specDigest"`
	Spec            json.RawMessage `json:"spec"`
	RuntimeLock     []Digest        `json:"runtimeLock"`
	ToolLock        []Digest        `json:"toolLock"`
	Permissions     Digest          `json:"permissions"`
	MemorySchema    Digest          `json:"memorySchema"`
	SBOM            Digest          `json:"sbom"`
	Provenance      Provenance      `json:"provenance"`
	// SignedImageDigest is the digest-pinned OCI image this package ships
	// (ADR-010 cosign-style binding); ImageSignature is its signature. When
	// either is set both must be set and the signature must verify.
	SignedImageDigest Digest     `json:"signedImageDigest,omitempty"`
	ImageSignature    *Signature `json:"imageSignature,omitempty"`
}

// Validate checks shape constraints; digest correctness is verified by the
// admission inputs at publish time.
func (m Manifest) Validate() error {
	if m.Schema != ManifestSchema {
		return fmt.Errorf("%w: schema must be %s", ErrPackageManifestInvalid, ManifestSchema)
	}
	if strings.TrimSpace(m.AgentVersionRef) == "" {
		return fmt.Errorf("%w: agentVersionRef is required", ErrPackageManifestInvalid)
	}
	if len(m.Spec) == 0 || !json.Valid(m.Spec) {
		return fmt.Errorf("%w: spec must be a JSON document", ErrPackageManifestInvalid)
	}
	if m.SpecDigest.Algorithm != "sha256" || !m.SpecDigest.Verify(m.Spec) {
		return fmt.Errorf("%w: spec digest does not match the spec document", ErrPackageManifestInvalid)
	}
	if strings.TrimSpace(m.Provenance.Builder) == "" {
		return fmt.Errorf("%w: provenance builder is required", ErrPackageManifestInvalid)
	}
	if (m.SignedImageDigest.Algorithm == "" && m.ImageSignature != nil) ||
		(m.SignedImageDigest.Algorithm != "" && m.ImageSignature == nil) {
		return fmt.Errorf("%w: signedImageDigest and imageSignature must be set together", ErrPackageManifestInvalid)
	}
	if m.SignedImageDigest.Algorithm != "" && m.SignedImageDigest.Algorithm != "sha256" {
		return fmt.Errorf("%w: signedImageDigest must be a sha256 digest", ErrPackageManifestInvalid)
	}
	return nil
}

// Signature binds the manifest hash to a registered key identity.
type Signature struct {
	KeyID     string    `json:"keyId"`
	Ed25519   string    `json:"ed25519"` // base64 raw std encoding
	CreatedAt time.Time `json:"createdAt"`
}

// Package is the signed manifest envelope exchanged with admission.
type Package struct {
	Manifest  Manifest  `json:"manifest"`
	Signature Signature `json:"signature"`
}

// Key is one registered package-signing identity.
type Key struct {
	ID        string
	PublicKey ed25519.PublicKey
}

// SigningKey is the private half; only the signer holds it.
type SigningKey struct {
	ID         string
	PrivateKey ed25519.PrivateKey
}

// GenerateSigningKey creates a fresh package-signing key pair.
func GenerateSigningKey(id string) (*SigningKey, *Key, error) {
	if strings.TrimSpace(id) == "" {
		return nil, nil, fmt.Errorf("key ID is required")
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate package key: %w", err)
	}
	return &SigningKey{ID: id, PrivateKey: private}, &Key{ID: id, PublicKey: public}, nil
}

// Sign produces a package for the manifest under the signing key.
func Sign(manifest Manifest, key *SigningKey) (*Package, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if key == nil || key.PrivateKey == nil {
		return nil, ErrPackageUnsigned
	}
	digest, err := ManifestDigest(manifest)
	if err != nil {
		return nil, err
	}
	signature := ed25519.Sign(key.PrivateKey, digest[:])
	return &Package{
		Manifest: manifest,
		Signature: Signature{
			KeyID: key.ID, Ed25519: base64.RawStdEncoding.EncodeToString(signature),
			CreatedAt: time.Now().UTC(),
		},
	}, nil
}

// Verify checks the signature against the manifest using the trusted key
// registry. Verification is fail-closed: an unknown key, a malformed
// signature or a manifest mismatch rejects the package.
func Verify(pkg *Package, keys map[string]ed25519.PublicKey) error {
	if pkg == nil {
		return ErrPackageUnsigned
	}
	if err := pkg.Manifest.Validate(); err != nil {
		return err
	}
	trusted, ok := keys[pkg.Signature.KeyID]
	if !ok {
		return fmt.Errorf("%w: key %q is not trusted", ErrPackageSignatureInvalid, pkg.Signature.KeyID)
	}
	signature, err := base64.RawStdEncoding.DecodeString(pkg.Signature.Ed25519)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("%w: signature encoding is invalid", ErrPackageSignatureInvalid)
	}
	digest, err := ManifestDigest(pkg.Manifest)
	if err != nil {
		return err
	}
	if !ed25519.Verify(trusted, digest[:], signature) {
		return ErrPackageSignatureInvalid
	}
	// Cosign-style image binding (ADR-010): when the manifest declares a
	// signed image digest, it must verify fail-closed under the same trust
	// registry.
	if err := VerifyImageSignature(pkg.Manifest, keys); err != nil {
		return err
	}
	return nil
}

// ManifestDigest returns the SHA-256 of the canonical manifest encoding (the
// exact bytes Sign signed and Verify re-checks).
func ManifestDigest(manifest Manifest) ([sha256.Size]byte, error) {
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode manifest: %w", err)
	}
	return sha256.Sum256(canonical), nil
}

// SpecSHA256 is a convenience for building a manifest from raw spec bytes.
func SpecSHA256(spec []byte) Digest {
	sum := sha256.Sum256(spec)
	return Digest{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}
