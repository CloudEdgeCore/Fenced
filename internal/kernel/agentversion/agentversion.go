// Package agentversion defines the bounded AgentVersion vocabulary of the
// Agent OS kernel: the canonical reference grammar, the immutable published
// spec, and the deterministic digest over that spec. It has no infrastructure
// dependencies.
//
// An AgentVersion is immutable by contract: a published spec can never be
// modified, and an upgrade is always the publication of a new version of the
// same agent name. The database layer enforces this invariant structurally.
package agentversion

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
)

const (
	APIVersion       = "fenced.dev/v1"
	LegacyAPIVersion = "fenced.dev/v1alpha1"
	Kind             = "AgentVersion"

	MaxNameLength    = 128
	MaxVersionLength = 128
	MaxAttemptsLimit = 10

	// DefaultNamespace is the namespace an AgentVersion reference resolves to
	// when none is written explicitly. Eliding it keeps the historical
	// "name@version" grammar valid byte-for-byte while the namespace becomes a
	// real part of the identity (P1-07): two versions with the same name and
	// version can coexist under one tenant in different namespaces.
	DefaultNamespace = "default"
)

// tokenPattern bounds both agent names and version strings to safe, opaque,
// filesystem- and subject-safe tokens.
var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,127})$`)

// imageDigestPattern bounds version-level image pins to sha256 digests.
var imageDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Spec is the bounded AgentVersion vocabulary consumed by the kernel. Legacy
// fields remain valid while v0.9 portable runtime and capability declarations
// are validated when present. Unknown fields are preserved in stored specs so
// additive compatibility can be maintained across later stages.
type Spec struct {
	RuntimeClassPolicy RuntimeClassPolicy `json:"runtimeClassPolicy,omitempty"`
	Lifecycle          Lifecycle          `json:"lifecycle,omitempty"`
	// Runtimes declares the portable launch targets shipped by this version.
	// Each target binds a runtime class to the stable Runtime Interface, a
	// provider ABI and an argv entrypoint.
	Runtimes []RuntimeTarget `json:"runtimes,omitempty"`
	// Capabilities are policy identifiers, not network endpoints or secret
	// values. Adapters map them to the corresponding Kernel gateways.
	Capabilities *Capabilities     `json:"capabilities,omitempty"`
	Resources    *ResourceLimits   `json:"resources,omitempty"`
	Budget       *Budget           `json:"budget,omitempty"`
	Checkpoint   *CheckpointPolicy `json:"checkpoint,omitempty"`
	// Image pins the container image the published version is allowed to run
	// (ADR-010). When set, admission rejects tasks whose spec pins a
	// different image.
	Image *Image `json:"image,omitempty"`
}

type RuntimeClassPolicy struct {
	Allowed   []string `json:"allowed,omitempty"`
	Preferred string   `json:"preferred,omitempty"`
}

type Lifecycle struct {
	MaxAttempts int `json:"maxAttempts,omitempty"`
}

// Image is the version-level image pin; it mirrors workload.Image so the
// kernel package graph stays acyclic.
type Image struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest,omitempty"`
}

// ValidateName rejects names that are not canonical agent-name tokens.
func ValidateName(name string) error {
	if !tokenPattern.MatchString(name) {
		return fmt.Errorf("agent name must match %s", tokenPattern)
	}
	return nil
}

// ValidateVersion rejects versions that are not canonical version tokens.
func ValidateVersion(version string) error {
	if !tokenPattern.MatchString(version) {
		return fmt.Errorf("agent version must match %s", tokenPattern)
	}
	return nil
}

// ValidateNamespace rejects namespaces that are not canonical tokens. The
// namespace is part of the AgentVersion identity and rides in the reference
// grammar ahead of the '/' separator, so it must be free of both the '/' and
// '@' separators — the token grammar already excludes them.
func ValidateNamespace(namespace string) error {
	if !tokenPattern.MatchString(namespace) {
		return fmt.Errorf("agent namespace must match %s", tokenPattern)
	}
	return nil
}

// FormatRef renders the canonical reference for an identity. The default
// namespace is elided so historical "name@version" references remain
// byte-identical; any other namespace is written as the full
// "namespace/name@version" identity.
func FormatRef(namespace, name, version string) string {
	if namespace == "" || namespace == DefaultNamespace {
		return name + "@" + version
	}
	return namespace + "/" + name + "@" + version
}

// ParseRef splits the canonical "[namespace/]name@version" reference. A
// reference with no namespace segment resolves to DefaultNamespace, so every
// historical "name@version" reference keeps its exact meaning. The reference
// must contain exactly one '@'; the namespace (when written), name and version
// must all be valid tokens.
func ParseRef(ref string) (namespace, name, version string, err error) {
	namespace = DefaultNamespace
	rest := ref
	if head, tail, hasNamespace := strings.Cut(ref, "/"); hasNamespace {
		namespace, rest = head, tail
		if err := ValidateNamespace(namespace); err != nil {
			return "", "", "", fmt.Errorf("agent version reference: %w", err)
		}
	}
	name, version, found := strings.Cut(rest, "@")
	if !found {
		return "", "", "", fmt.Errorf("agent version reference must be [namespace/]name@version")
	}
	if err := ValidateName(name); err != nil {
		return "", "", "", fmt.Errorf("agent version reference: %w", err)
	}
	if err := ValidateVersion(version); err != nil {
		return "", "", "", fmt.Errorf("agent version reference: %w", err)
	}
	return namespace, name, version, nil
}

// CanonicalizeSpec normalizes a raw spec document into its canonical JSON form
// and returns the deterministic SHA-256 digest that identifies the immutable
// publication. Numbers are preserved through json.Number so that integer
// budgets are not rewritten as floats.
func CanonicalizeSpec(raw json.RawMessage) (json.RawMessage, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if len(raw) == 0 || string(raw) == "null" {
		return nil, zero, fmt.Errorf("agent version spec is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, zero, fmt.Errorf("decode agent version spec: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, zero, err
	}
	if _, ok := document.(map[string]any); !ok {
		return nil, zero, fmt.Errorf("agent version spec must be a JSON object")
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return nil, zero, fmt.Errorf("normalize agent version spec: %w", err)
	}
	return canonical, sha256.Sum256(canonical), nil
}

// ValidateSpec checks the bounded fields that kernel stages consume.
// Unknown fields are ignored so that later spec stages can evolve without a
// breaking change. Failures mean the publication must be rejected.
func ValidateSpec(raw json.RawMessage) error {
	// AgentVersion specs published before GA did not carry their own API
	// version. Detect the frozen runtime-interface discriminator so N-1 specs
	// remain readable while mixed-version specs fail closed.
	var envelope struct {
		Runtimes []RuntimeTarget `json:"runtimes"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("agent version spec is not valid for v1: %w", err)
	}
	seenInterface := ""
	for _, target := range envelope.Runtimes {
		if target.Interface != RuntimeInterfaceV1 && target.Interface != RuntimeInterfaceV1Alpha1 {
			continue
		}
		if seenInterface != "" && seenInterface != target.Interface {
			return fmt.Errorf("agent version spec mixes runtime interface versions")
		}
		seenInterface = target.Interface
	}
	runtimeInterface := RuntimeInterfaceV1
	if seenInterface == RuntimeInterfaceV1Alpha1 {
		runtimeInterface = RuntimeInterfaceV1Alpha1
	}
	return validateSpecForInterface(raw, runtimeInterface)
}

func validateSpecForInterface(raw json.RawMessage, runtimeInterface string) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var spec Spec
	if err := decoder.Decode(&spec); err != nil {
		return fmt.Errorf("agent version spec is not valid for v1: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	for _, runtimeClass := range spec.RuntimeClassPolicy.Allowed {
		if strings.TrimSpace(runtimeClass) == "" {
			return fmt.Errorf("runtimeClassPolicy.allowed must not contain empty runtime classes")
		}
	}
	if slices.ContainsFunc(spec.RuntimeClassPolicy.Allowed, func(value string) bool {
		return len(value) > 128
	}) {
		return fmt.Errorf("runtimeClassPolicy.allowed entries must not exceed 128 bytes")
	}
	if duplicates := duplicated(spec.RuntimeClassPolicy.Allowed); len(duplicates) != 0 {
		return fmt.Errorf("runtimeClassPolicy.allowed contains duplicates: %v", duplicates)
	}
	if preferred := spec.RuntimeClassPolicy.Preferred; preferred != "" {
		if len(spec.RuntimeClassPolicy.Allowed) == 0 {
			return fmt.Errorf("runtimeClassPolicy.preferred requires runtimeClassPolicy.allowed")
		}
		if !slices.Contains(spec.RuntimeClassPolicy.Allowed, preferred) {
			return fmt.Errorf("runtimeClassPolicy.preferred %q is not in runtimeClassPolicy.allowed", preferred)
		}
	}
	if attempts := spec.Lifecycle.MaxAttempts; attempts < 0 || attempts > MaxAttemptsLimit {
		return fmt.Errorf("lifecycle.maxAttempts must be between 0 and %d", MaxAttemptsLimit)
	}
	if spec.Image != nil {
		if strings.TrimSpace(spec.Image.Ref) == "" {
			return fmt.Errorf("image.ref is required when image is set")
		}
		if spec.Image.Digest != "" && !imageDigestPattern.MatchString(spec.Image.Digest) {
			return fmt.Errorf("image.digest must be sha256:<64 lowercase hex>")
		}
	}
	if err := validatePlatformSpecForInterface(spec, runtimeInterface); err != nil {
		return err
	}
	return nil
}

func duplicated(values []string) []string {
	var duplicates []string
	seen := map[string]struct{}{}
	for _, value := range values {
		if _, exists := seen[value]; exists {
			duplicates = append(duplicates, value)
		}
		seen[value] = struct{}{}
	}
	return duplicates
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("agent version spec contains more than one JSON value")
	}
	return fmt.Errorf("decode trailing agent version spec data: %w", err)
}
