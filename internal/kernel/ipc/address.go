package ipc

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
)

// AgentAddress is the unified address of an agent in the Fenced kernel.
// It supports both logical agent addressing (InstanceID is empty) and
// specific instance addressing (InstanceID is non-empty).
//
// The address carries the agent version. The frozen IPC contract names it
// agent_version_ref (field 1 of AgentAddress): without it an address names
// an agent, and during a rollout the agent runs two versions at once - the
// address would not say which one a message belongs to.
type AgentAddress struct {
	TenantID        string `json:"tenant_id"`
	Namespace       string `json:"namespace"`
	AgentID         string `json:"agent_id"`
	AgentVersionRef string `json:"agent_version_ref,omitempty"`
	InstanceID      string `json:"instance_id,omitempty"`
}

// NewAddress creates a logical AgentAddress.
func NewAddress(tenantID, namespace, agentID string) AgentAddress {
	if namespace == "" {
		namespace = "default"
	}
	return AgentAddress{
		TenantID:  strings.TrimSpace(tenantID),
		Namespace: strings.TrimSpace(namespace),
		AgentID:   strings.TrimSpace(agentID),
	}
}

// NewVersionedAddress creates a logical AgentAddress that names the agent
// version it belongs to. A logical address without a version names an agent
// that may run several versions at once, which is not a delivery target.
func NewVersionedAddress(tenantID, namespace, agentID, versionRef string) AgentAddress {
	address := NewAddress(tenantID, namespace, agentID)
	address.AgentVersionRef = strings.TrimSpace(versionRef)
	return address
}

// NewInstanceAddress creates an instance-specific AgentAddress.
func NewInstanceAddress(tenantID, namespace, agentID, instanceID string) AgentAddress {
	if namespace == "" {
		namespace = "default"
	}
	return AgentAddress{
		TenantID:   strings.TrimSpace(tenantID),
		Namespace:  strings.TrimSpace(namespace),
		AgentID:    strings.TrimSpace(agentID),
		InstanceID: strings.TrimSpace(instanceID),
	}
}

// NewInstanceAddressForVersion creates an instance-specific AgentAddress that
// names the agent version the instance runs.
func NewInstanceAddressForVersion(tenantID, namespace, agentID, versionRef, instanceID string) AgentAddress {
	address := NewInstanceAddress(tenantID, namespace, agentID, instanceID)
	address.AgentVersionRef = strings.TrimSpace(versionRef)
	return address
}

// VersionPart returns the version part of an agent version reference: the
// "2.0.0" of "reports@2.0.0". The name and the namespace are part of the
// address path, so the URI carries them there.
func VersionPart(ref string) string {
	trimmed := strings.TrimSpace(ref)
	if at := strings.LastIndex(trimmed, "@"); at >= 0 {
		return trimmed[at+1:]
	}
	return ""
}

// SameVersion reports whether two agent version references name the same
// version. The namespace is part of the identity of a version: "prod/reports@1"
// and "default/reports@1" are two different publications. A reference without
// a namespace names the default one.
func SameVersion(left, right string) bool {
	leftKey, rightKey := canonicalVersion(left), canonicalVersion(right)
	if leftKey == "" || rightKey == "" {
		return leftKey == rightKey
	}
	return leftKey == rightKey
}

// canonicalVersion normalizes an agent version reference to the form the
// kernel publishes: the namespace is dropped when it is the default one.
func canonicalVersion(ref string) string {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return ""
	}
	namespace, name, version, err := agentversion.ParseRef(trimmed)
	if err != nil {
		return trimmed
	}
	return agentversion.FormatRef(namespace, name, version)
}

// Validate checks that the address has all required fields and valid characters.
// TenantID and AgentID are strictly required; InstanceID is optional.
func (a AgentAddress) Validate() error {
	if strings.TrimSpace(a.TenantID) == "" {
		return fmt.Errorf("%w: tenant_id is required", ErrInvalidAddress)
	}
	if strings.TrimSpace(a.AgentID) == "" {
		return fmt.Errorf("%w: agent_id is required", ErrInvalidAddress)
	}
	if strings.ContainsAny(a.TenantID, " /#?@") {
		return fmt.Errorf("%w: tenant_id contains invalid characters", ErrInvalidAddress)
	}
	if strings.ContainsAny(a.Namespace, " /#?@") {
		return fmt.Errorf("%w: namespace contains invalid characters", ErrInvalidAddress)
	}
	if strings.ContainsAny(a.AgentID, " /#?@") {
		return fmt.Errorf("%w: agent_id contains invalid characters", ErrInvalidAddress)
	}
	if strings.ContainsAny(a.InstanceID, " /#?@") {
		return fmt.Errorf("%w: instance_id contains invalid characters", ErrInvalidAddress)
	}
	if strings.ContainsAny(a.AgentVersionRef, " /#?") {
		return fmt.Errorf("%w: agent_version_ref contains invalid characters", ErrInvalidAddress)
	}
	return nil
}

// EffectiveNamespace returns the namespace, defaulting to "default" if empty.
func (a AgentAddress) EffectiveNamespace() string {
	if a.Namespace == "" {
		return "default"
	}
	return a.Namespace
}

// Logical returns the logical address with InstanceID cleared. The version
// stays: it is what tells the versions of an agent apart.
func (a AgentAddress) Logical() AgentAddress {
	return AgentAddress{
		TenantID:        a.TenantID,
		Namespace:       a.EffectiveNamespace(),
		AgentID:         a.AgentID,
		AgentVersionRef: a.AgentVersionRef,
	}
}

// IsInstance reports whether this address targets a concrete agent instance.
func (a AgentAddress) IsInstance() bool {
	return strings.TrimSpace(a.InstanceID) != ""
}

// Matches reports whether this address matches a target receiver address.
// If either address has an empty InstanceID (logical addressing), they match
// as long as TenantID, Namespace, and AgentID match.
// If both have non-empty InstanceIDs, they must also match in InstanceID.
func (a AgentAddress) Matches(target AgentAddress) bool {
	if a.TenantID != target.TenantID {
		return false
	}
	if a.EffectiveNamespace() != target.EffectiveNamespace() {
		return false
	}
	if a.AgentID != target.AgentID {
		return false
	}
	if a.InstanceID != "" && target.InstanceID != "" && a.InstanceID != target.InstanceID {
		return false
	}
	if a.AgentVersionRef != "" && target.AgentVersionRef != "" && !SameVersion(a.AgentVersionRef, target.AgentVersionRef) {
		return false
	}
	return true
}

// String returns the canonical, stably serializable URI representation:
//
//	agent://<tenant>/<namespace>/<agent>[@<version>][/<instance>]
//
// The version is the comparable part of the version reference: the namespace
// is already part of the path.
func (a AgentAddress) String() string {
	ns := a.EffectiveNamespace()
	agent := a.AgentID
	if version := VersionPart(a.AgentVersionRef); version != "" {
		agent += "@" + version
	}
	if a.InstanceID != "" {
		return fmt.Sprintf("agent://%s/%s/%s/%s", a.TenantID, ns, agent, a.InstanceID)
	}
	return fmt.Sprintf("agent://%s/%s/%s", a.TenantID, ns, agent)
}

// MarshalText implements encoding.TextMarshaler for stable serialization.
func (a AgentAddress) MarshalText() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	return []byte(a.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler for stable deserialization.
func (a *AgentAddress) UnmarshalText(text []byte) error {
	parsed, err := ParseAddress(string(text))
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}

// MarshalJSON implements json.Marshaler.
func (a AgentAddress) MarshalJSON() ([]byte, error) {
	type Alias AgentAddress
	return json.Marshal(&struct {
		Alias
		Canonical string `json:"canonical,omitempty"`
	}{
		Alias:     Alias(a),
		Canonical: a.String(),
	})
}

// UnmarshalJSON implements json.Unmarshaler, supporting both object and string forms.
func (a *AgentAddress) UnmarshalJSON(data []byte) error {
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		parsed, err := ParseAddress(s)
		if err != nil {
			return err
		}
		*a = parsed
		return nil
	}

	type Alias AgentAddress
	aux := &struct {
		*Alias
		Canonical string `json:"canonical"`
	}{
		Alias: (*Alias)(a),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if a.TenantID == "" && aux.Canonical != "" {
		parsed, err := ParseAddress(aux.Canonical)
		if err != nil {
			return err
		}
		*a = parsed
	}
	if a.Namespace == "" {
		a.Namespace = "default"
	}
	return nil
}

// ParseAddress parses a canonical agent address string in either:
//
//	agent://<tenant>/<namespace>/<agent>[@<version>][/<instance>]
//
// or slash-delimited format:
//
//	<tenant>/<namespace>/<agent>[@<version>][/<instance>]
func ParseAddress(raw string) (AgentAddress, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return AgentAddress{}, fmt.Errorf("%w: empty address", ErrInvalidAddress)
	}

	trimmed := raw
	if strings.HasPrefix(trimmed, "agent://") {
		trimmed = strings.TrimPrefix(trimmed, "agent://")
	} else if u, err := url.Parse(raw); err == nil && u.Scheme == "agent" {
		trimmed = strings.TrimPrefix(raw, "agent://")
	}

	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 || len(parts) > 4 {
		return AgentAddress{}, fmt.Errorf("%w: expected tenant/namespace/agent[@version][/instance], got %q", ErrInvalidAddress, raw)
	}

	addr := AgentAddress{
		TenantID:  parts[0],
		Namespace: parts[1],
		AgentID:   parts[2],
	}
	// The agent segment may carry the version: "name@version". The namespace
	// of the version is the namespace of the path.
	if at := strings.LastIndex(addr.AgentID, "@"); at > 0 {
		name, version := addr.AgentID[:at], addr.AgentID[at+1:]
		addr.AgentID = name
		addr.AgentVersionRef = agentversion.FormatRef(addr.Namespace, name, version)
	}
	if len(parts) == 4 {
		addr.InstanceID = parts[3]
	}
	if addr.Namespace == "" {
		addr.Namespace = "default"
	}
	if err := addr.Validate(); err != nil {
		return AgentAddress{}, err
	}
	return addr, nil
}
