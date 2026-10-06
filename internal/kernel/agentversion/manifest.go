package agentversion

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
)

const (
	// ManifestAPIVersion identifies the frozen GA Agent Manifest contract.
	// LegacyManifestAPIVersion remains readable for the published N-1 window.
	ManifestAPIVersion       = "fenced.dev/v1"
	LegacyManifestAPIVersion = "fenced.dev/v1alpha1"
	ManifestKind             = "AgentManifest"
	// RuntimeInterfaceV1 is the frozen provider-neutral lifecycle boundary.
	RuntimeInterfaceV1 = "fenced.runtime.interface/v1"
	// RuntimeInterfaceV1Alpha1 is accepted only for legacy manifests.
	RuntimeInterfaceV1Alpha1 = "fenced.runtime.interface/v1alpha1"

	CheckpointNone    = "none"
	CheckpointLogical = "logical"
)

var (
	contractRefPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}$`)
	capabilityRefPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@*-]{0,255}$`)
)

// Manifest is the strict, portable v0.9 Agent declaration consumed by SDKs,
// CLI tooling and Registry publication. Tenant identity is intentionally not
// embedded: it comes from the authenticated control-plane principal.
type Manifest struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
}

type Metadata struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Namespace string `json:"namespace"`
}

// RuntimeTarget binds one portable agent build to a runtime class.
type RuntimeTarget struct {
	Class      string   `json:"class"`
	Interface  string   `json:"interface"`
	RuntimeABI string   `json:"runtimeABI"`
	Entrypoint []string `json:"entrypoint"`
}

// Capabilities contains symbolic permission identifiers. Empty arrays are an
// explicit default-deny declaration and are different from an omitted block.
type Capabilities struct {
	Tools               []string `json:"tools"`
	Models              []string `json:"models"`
	Memory              []string `json:"memory"`
	MemorySensitivities []string `json:"memorySensitivities,omitempty"`
	Secrets             []string `json:"secrets"`
	SpawnTasks          bool     `json:"spawnTasks,omitempty"`
	ChildAgents         []string `json:"childAgents,omitempty"`
	// Peers lists the AgentVersion references this version may send IPC
	// messages to, in agentversion.FormatRef form. Empty denies every peer.
	// It constrains only the sender: the receiving run does not have to
	// declare who may write to it, so knowing a run id is enough to reach it.
	Peers []string `json:"peers,omitempty"`
}

type ResourceLimits struct {
	CPUMillis      int64 `json:"cpuMillis"`
	MemoryMiB      int64 `json:"memoryMiB"`
	WorkspaceBytes int64 `json:"workspaceBytes"`
}

type Budget struct {
	Tokens       int64          `json:"tokens"`
	CostMicroUSD money.MicroUSD `json:"costUsd"`
	ToolCalls    int64          `json:"toolCalls"`
	WallSeconds  int64          `json:"wallSeconds"`
}

type CheckpointPolicy struct {
	Mode            string `json:"mode"`
	SchemaVersion   string `json:"schemaVersion,omitempty"`
	IntervalSeconds int64  `json:"intervalSeconds,omitempty"`
}

// DecodeManifest rejects ambiguous or forward-unknown documents, validates
// the full developer contract, and returns deterministic canonical JSON and
// its SHA-256 identity.
func DecodeManifest(raw []byte) (Manifest, json.RawMessage, [sha256.Size]byte, error) {
	var manifest Manifest
	var zero [sha256.Size]byte
	if err := rejectDuplicateKeys(raw); err != nil {
		return manifest, nil, zero, fmt.Errorf("decode agent manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, nil, zero, fmt.Errorf("decode agent manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return manifest, nil, zero, err
	}
	manifest.NormalizeLegacyIdentifiers()
	if err := manifest.Validate(); err != nil {
		return manifest, nil, zero, err
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return manifest, nil, zero, fmt.Errorf("canonicalize agent manifest: %w", err)
	}
	return manifest, canonical, sha256.Sum256(canonical), nil
}

// NormalizeLegacyIdentifiers rewrites identifiers that predate the AgentOS →
// Fenced rename into their canonical Fenced form. DecodeManifest applies it
// before validation, so a manifest published under the former name stays
// loadable instead of failing with "runtimes[0].interface must be
// fenced.runtime.interface/v1".
//
// The canonical JSON and SHA-256 returned by DecodeManifest are computed on the
// normalized document, so a legacy manifest has a different identity after the
// rename. See docs/COMPATIBILITY.md.
func (m *Manifest) NormalizeLegacyIdentifiers() {
	m.APIVersion = compat.NormalizeProtocol(m.APIVersion)
	for i := range m.Spec.Runtimes {
		m.Spec.Runtimes[i].Interface = compat.NormalizeProtocol(m.Spec.Runtimes[i].Interface)
		m.Spec.Runtimes[i].RuntimeABI = compat.NormalizeProtocol(m.Spec.Runtimes[i].RuntimeABI)
	}
}

func (m Manifest) Validate() error {
	var runtimeInterface string
	switch m.APIVersion {
	case ManifestAPIVersion:
		runtimeInterface = RuntimeInterfaceV1
	case LegacyManifestAPIVersion:
		runtimeInterface = RuntimeInterfaceV1Alpha1
	default:
		return fmt.Errorf("agent manifest apiVersion must be %s or legacy %s", ManifestAPIVersion, LegacyManifestAPIVersion)
	}
	if m.Kind != ManifestKind {
		return fmt.Errorf("agent manifest kind must be %s", ManifestKind)
	}
	if err := ValidateName(m.Metadata.Name); err != nil {
		return fmt.Errorf("agent manifest metadata.name: %w", err)
	}
	if err := ValidateVersion(m.Metadata.Version); err != nil {
		return fmt.Errorf("agent manifest metadata.version: %w", err)
	}
	if err := ValidateNamespace(m.Metadata.Namespace); err != nil {
		return fmt.Errorf("agent manifest metadata.namespace: %w", err)
	}
	if len(m.Spec.Runtimes) == 0 {
		return fmt.Errorf("agent manifest spec.runtimes requires at least one target")
	}
	if m.Spec.Capabilities == nil {
		return fmt.Errorf("agent manifest spec.capabilities is required (use empty arrays for default deny)")
	}
	if m.Spec.Resources == nil {
		return fmt.Errorf("agent manifest spec.resources is required")
	}
	if m.Spec.Budget == nil {
		return fmt.Errorf("agent manifest spec.budget is required")
	}
	if m.Spec.Checkpoint == nil {
		return fmt.Errorf("agent manifest spec.checkpoint is required")
	}
	encoded, err := json.Marshal(m.Spec)
	if err != nil {
		return fmt.Errorf("encode agent manifest spec: %w", err)
	}
	if err := validateSpecForInterface(encoded, runtimeInterface); err != nil {
		return fmt.Errorf("agent manifest spec: %w", err)
	}
	return nil
}

func (m Manifest) Ref() string {
	return FormatRef(m.Metadata.Namespace, m.Metadata.Name, m.Metadata.Version)
}

// PromoteToV1 performs the only supported alpha-to-GA schema migration. The
// v1 schema is otherwise wire-identical, so canonical identity changes solely
// because the two explicit contract identifiers are promoted.
func (m Manifest) PromoteToV1() (Manifest, error) {
	if err := m.Validate(); err != nil {
		return Manifest{}, err
	}
	if m.APIVersion == ManifestAPIVersion {
		return m, nil
	}
	m.APIVersion = ManifestAPIVersion
	m.Spec.Runtimes = append([]RuntimeTarget(nil), m.Spec.Runtimes...)
	for index := range m.Spec.Runtimes {
		m.Spec.Runtimes[index].Interface = RuntimeInterfaceV1
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("promote agent manifest: %w", err)
	}
	return m, nil
}

func validatePlatformSpecForInterface(spec Spec, runtimeInterface string) error {
	if len(spec.Runtimes) > 16 {
		return fmt.Errorf("runtimes must contain at most 16 targets")
	}
	if len(spec.Runtimes) > 0 {
		seen := make(map[string]struct{}, len(spec.Runtimes))
		for index, target := range spec.Runtimes {
			if !tokenPattern.MatchString(target.Class) {
				return fmt.Errorf("runtimes[%d].class must be a canonical runtime token", index)
			}
			if _, exists := seen[target.Class]; exists {
				return fmt.Errorf("runtimes contains duplicate class %q", target.Class)
			}
			seen[target.Class] = struct{}{}
			if target.Interface != runtimeInterface {
				return fmt.Errorf("runtimes[%d].interface must be %s", index, runtimeInterface)
			}
			if !contractRefPattern.MatchString(target.RuntimeABI) {
				return fmt.Errorf("runtimes[%d].runtimeABI is invalid", index)
			}
			if err := validateEntrypoint(target.Entrypoint); err != nil {
				return fmt.Errorf("runtimes[%d].entrypoint: %w", index, err)
			}
		}
		if len(spec.RuntimeClassPolicy.Allowed) != len(seen) {
			return fmt.Errorf("runtimeClassPolicy.allowed must exactly match runtimes classes")
		}
		for class := range seen {
			if !slices.Contains(spec.RuntimeClassPolicy.Allowed, class) {
				return fmt.Errorf("runtimeClassPolicy.allowed must include runtime class %q", class)
			}
		}
	}
	if spec.Capabilities != nil {
		sets := []struct {
			name   string
			values []string
		}{
			{name: "tools", values: spec.Capabilities.Tools},
			{name: "models", values: spec.Capabilities.Models},
			{name: "memory", values: spec.Capabilities.Memory},
			{name: "secrets", values: spec.Capabilities.Secrets},
		}
		for _, set := range sets {
			if err := validateCapabilitySet(set.values); err != nil {
				return fmt.Errorf("capabilities.%s: %w", set.name, err)
			}
		}
		for _, tool := range spec.Capabilities.Tools {
			if strings.HasPrefix(strings.ToLower(tool), "fenced.") {
				return fmt.Errorf("capabilities.tools cannot claim reserved fenced.* system tools")
			}
		}
		if spec.Capabilities.ChildAgents != nil {
			if err := validateCapabilitySet(spec.Capabilities.ChildAgents); err != nil {
				return fmt.Errorf("capabilities.childAgents: %w", err)
			}
		}
		// Peers is optional like ChildAgents, so it is validated only when
		// present: validateCapabilitySet rejects a nil slice, and putting it in
		// the list above would therefore reject every manifest that predates it.
		if spec.Capabilities.Peers != nil {
			if err := validateCapabilitySet(spec.Capabilities.Peers); err != nil {
				return fmt.Errorf("capabilities.peers: %w", err)
			}
		}
		if len(spec.Capabilities.MemorySensitivities) == 0 {
			// Backward-compatible least privilege: old manifests can only
			// access internal memory.
			spec.Capabilities.MemorySensitivities = []string{"internal"}
		}
		for _, sensitivity := range spec.Capabilities.MemorySensitivities {
			if sensitivity != "internal" && sensitivity != "confidential" && sensitivity != "restricted" {
				return fmt.Errorf("capabilities.memorySensitivities contains invalid tier %q", sensitivity)
			}
		}
		if spec.Capabilities.SpawnTasks && len(spec.Capabilities.ChildAgents) == 0 {
			return fmt.Errorf("capabilities.childAgents requires at least one entry when spawnTasks is enabled")
		}
	}
	if spec.Resources != nil {
		if spec.Resources.CPUMillis <= 0 || spec.Resources.MemoryMiB <= 0 || spec.Resources.WorkspaceBytes < 0 {
			return fmt.Errorf("resources require positive cpuMillis/memoryMiB and non-negative workspaceBytes")
		}
		for _, target := range spec.Runtimes {
			if (target.Class == "oci" || target.Class == "microvm") && spec.Resources.WorkspaceBytes == 0 {
				return fmt.Errorf("resources.workspaceBytes must be positive for runtime class %q", target.Class)
			}
		}
	}
	if spec.Budget != nil {
		if spec.Budget.Tokens < 0 || spec.Budget.ToolCalls < 0 || spec.Budget.WallSeconds <= 0 ||
			spec.Budget.CostMicroUSD < 0 {
			return fmt.Errorf("budget requires non-negative ceilings and positive wallSeconds")
		}
	}
	if spec.Checkpoint != nil {
		switch spec.Checkpoint.Mode {
		case CheckpointNone:
			if spec.Checkpoint.SchemaVersion != "" || spec.Checkpoint.IntervalSeconds != 0 {
				return fmt.Errorf("checkpoint mode none cannot declare schemaVersion or intervalSeconds")
			}
		case CheckpointLogical:
			if !contractRefPattern.MatchString(spec.Checkpoint.SchemaVersion) {
				return fmt.Errorf("checkpoint logical mode requires a valid schemaVersion")
			}
			if spec.Checkpoint.IntervalSeconds < 0 || spec.Checkpoint.IntervalSeconds > 86_400 {
				return fmt.Errorf("checkpoint.intervalSeconds must be between 0 and 86400")
			}
		default:
			return fmt.Errorf("checkpoint.mode must be %q or %q", CheckpointNone, CheckpointLogical)
		}
	}
	return nil
}

func validateEntrypoint(arguments []string) error {
	if len(arguments) == 0 || len(arguments) > 128 {
		return fmt.Errorf("must contain between 1 and 128 arguments")
	}
	total := 0
	for index, argument := range arguments {
		if strings.IndexByte(argument, 0) >= 0 {
			return fmt.Errorf("argument %d contains NUL", index)
		}
		if index == 0 && strings.TrimSpace(argument) == "" {
			return fmt.Errorf("executable is required")
		}
		if len(argument) > 4096 {
			return fmt.Errorf("argument %d exceeds 4096 bytes", index)
		}
		total += len(argument)
	}
	if total > 32<<10 {
		return fmt.Errorf("arguments exceed 32768 bytes")
	}
	return nil
}

func validateCapabilitySet(values []string) error {
	if values == nil {
		return fmt.Errorf("must be an explicit array (use [] for default deny)")
	}
	if len(values) > 256 {
		return fmt.Errorf("must contain at most 256 identifiers")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !capabilityRefPattern.MatchString(value) {
			return fmt.Errorf("identifier %q is invalid", value)
		}
		if strings.Contains(value, "*") && value != "*" &&
			!(strings.HasSuffix(value, ".*") || strings.HasSuffix(value, "/*") || strings.HasSuffix(value, "@*")) {
			return fmt.Errorf("identifier %q uses an unsupported wildcard; use exact, *, .*, /*, or @* (version floating)", value)
		}
		if strings.Count(value, "*") > 1 {
			return fmt.Errorf("identifier %q contains more than one wildcard", value)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("contains duplicate identifier %q", value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return fmt.Errorf("manifest contains invalid or ambiguous JSON")
	}
	return ensureJSONEOF(decoder)
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("array is not closed")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return nil
}
