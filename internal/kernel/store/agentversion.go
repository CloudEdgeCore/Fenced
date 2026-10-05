package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/google/uuid"
)

var (
	// ErrAgentVersionConflict reports a publish attempt that reuses an
	// existing (tenant, name, version) identity with a different spec.
	ErrAgentVersionConflict = errors.New("agent version identity already published with a different spec")
	// ErrAgentVersionRefInvalid reports a reference that is not a canonical
	// name@version token pair.
	ErrAgentVersionRefInvalid = errors.New("agent version reference is not canonical")
)

// PackageSignature is the verified signature envelope persisted with a
// publication (ADR-010). It is populated only when publish admission verified
// the signed Agent Package; all three fields are set together or none.
type PackageSignature struct {
	KeyID          string // signing key identity in the trust registry
	Signature      string // base64 raw std ed25519 signature over the manifest digest
	ManifestDigest string // hex sha256 of the canonical signed manifest
}

// AgentVersion is an immutable published agent specification. The resource
// version is always 1: upgrades create new rows instead of mutating this one.
type AgentVersion struct {
	ID               uuid.UUID
	TenantID         string
	Namespace        string
	Name             string
	Version          string
	Spec             json.RawMessage
	SpecDigest       [sha256.Size]byte
	ResourceVersion  int64
	CreatedAt        time.Time
	PackageSignature *PackageSignature
}

// Ref returns the canonical reference used by tasks. The default namespace is
// elided so historical "name@version" references are unchanged; any other
// namespace is written as "namespace/name@version" (P1-07).
func (v AgentVersion) Ref() string { return agentversion.FormatRef(v.Namespace, v.Name, v.Version) }

type CreateAgentVersionInput struct {
	ID               uuid.UUID
	TenantID         string
	Namespace        string
	Name             string
	Version          string
	Spec             json.RawMessage
	PackageSignature *PackageSignature
}

type CreateAgentVersionResult struct {
	AgentVersion AgentVersion
	Existing     bool
}

// ValidateAndHash bounds the publication and derives the canonical spec
// document and its immutable digest.
func (in CreateAgentVersionInput) ValidateAndHash() (json.RawMessage, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if strings.TrimSpace(in.TenantID) == "" {
		return nil, zero, fmt.Errorf("tenant is required")
	}
	if err := agentversion.ValidateNamespace(in.Namespace); err != nil {
		return nil, zero, err
	}
	if err := agentversion.ValidateName(in.Name); err != nil {
		return nil, zero, err
	}
	if err := agentversion.ValidateVersion(in.Version); err != nil {
		return nil, zero, err
	}
	canonical, digest, err := agentversion.CanonicalizeSpec(in.Spec)
	if err != nil {
		return nil, zero, err
	}
	if err := agentversion.ValidateSpec(canonical); err != nil {
		return nil, zero, err
	}
	return canonical, digest, nil
}

type AgentVersionStore interface {
	CreateAgentVersion(context.Context, CreateAgentVersionInput) (CreateAgentVersionResult, error)
	GetAgentVersion(context.Context, string, uuid.UUID) (AgentVersion, error)
	GetAgentVersionByRef(context.Context, string, string) (AgentVersion, error)
}
