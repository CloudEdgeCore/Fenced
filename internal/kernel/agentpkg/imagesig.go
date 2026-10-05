package agentpkg

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"time"
)

// imageSignatureMessage is the namespaced payload an image signature covers:
// image signatures can never be replayed as manifest signatures, and vice
// versa (cosign-style binding, ADR-010).
func imageSignatureMessage(digest Digest) []byte {
	return []byte("fenced.image/v1\x00" + digest.String())
}

// SignImage signs the digest-pinned OCI image the package ships. The
// signature is embedded in the manifest (SignedImageDigest + ImageSignature)
// before the manifest itself is signed, so the manifest signature covers it.
func SignImage(digest Digest, key *SigningKey) (*Signature, error) {
	if digest.Algorithm != "sha256" || len(digest.Hex) != 64 {
		return nil, fmt.Errorf("%w: image digest must be sha256:<64 hex>", ErrPackageManifestInvalid)
	}
	if key == nil || key.PrivateKey == nil {
		return nil, ErrPackageUnsigned
	}
	signature := ed25519.Sign(key.PrivateKey, imageSignatureMessage(digest))
	return &Signature{
		KeyID: key.ID, Ed25519: base64.RawStdEncoding.EncodeToString(signature),
		CreatedAt: time.Now().UTC(),
	}, nil
}

// VerifyImageSignature checks the manifest's image signature fail-closed:
// when the manifest declares a signed image, the signature must verify over
// exactly that digest under a trusted key.
func VerifyImageSignature(manifest Manifest, keys map[string]ed25519.PublicKey) error {
	if manifest.ImageSignature == nil {
		return nil
	}
	if manifest.SignedImageDigest.Algorithm == "" {
		return fmt.Errorf("%w: signedImageDigest is required with an image signature", ErrPackageSignatureInvalid)
	}
	trusted, ok := keys[manifest.ImageSignature.KeyID]
	if !ok {
		return fmt.Errorf("%w: image signature key %q is not trusted", ErrPackageSignatureInvalid, manifest.ImageSignature.KeyID)
	}
	signature, err := base64.RawStdEncoding.DecodeString(manifest.ImageSignature.Ed25519)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("%w: image signature encoding is invalid", ErrPackageSignatureInvalid)
	}
	if !ed25519.Verify(trusted, imageSignatureMessage(manifest.SignedImageDigest), signature) {
		return fmt.Errorf("%w: image signature does not match the signed image digest", ErrPackageSignatureInvalid)
	}
	return nil
}
