package ipc

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// AgentAddress is the unified address of an agent in the Fenced kernel.
// It supports both logical agent addressing (InstanceID is empty) and
// specific instance addressing (InstanceID is non-empty).
type AgentAddress struct {
	TenantID   string `json:"tenant_id"`
	Namespace  string `json:"namespace"`
	AgentID    string `json:"agent_id"`
	InstanceID string `json:"instance_id,omitempty"`
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
	return nil
}

// EffectiveNamespace returns the namespace, defaulting to "default" if empty.
func (a AgentAddress) EffectiveNamespace() string {
	if a.Namespace == "" {
		return "default"
	}
	return a.Namespace
}

// Logical returns the logical address with InstanceID cleared.
func (a AgentAddress) Logical() AgentAddress {
	return AgentAddress{
		TenantID:  a.TenantID,
		Namespace: a.EffectiveNamespace(),
		AgentID:   a.AgentID,
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
	return true
}

// String returns the canonical, stably serializable URI representation:
//
//	agent://<tenant>/<namespace>/<agent>[/<instance>]
func (a AgentAddress) String() string {
	ns := a.EffectiveNamespace()
	if a.InstanceID != "" {
		return fmt.Sprintf("agent://%s/%s/%s/%s", a.TenantID, ns, a.AgentID, a.InstanceID)
	}
	return fmt.Sprintf("agent://%s/%s/%s", a.TenantID, ns, a.AgentID)
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
//	agent://<tenant>/<namespace>/<agent>[/<instance>]
//
// or slash-delimited format:
//
//	<tenant>/<namespace>/<agent>[/<instance>]
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
		return AgentAddress{}, fmt.Errorf("%w: expected tenant/namespace/agent[/instance], got %q", ErrInvalidAddress, raw)
	}

	addr := AgentAddress{
		TenantID:  parts[0],
		Namespace: parts[1],
		AgentID:   parts[2],
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
