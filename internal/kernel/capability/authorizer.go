// Package capability enforces immutable AgentVersion grants at Gateway
// boundaries without coupling the Agent manifest vocabulary to persistence.
package capability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

type Kind string

const (
	Tool   Kind = "tool"
	Model  Kind = "model"
	Memory Kind = "memory"
	Secret Kind = "secret"
	// IPC is the grant kind for agent-to-agent messaging. A candidate is the
	// receiving AgentVersion reference the send addresses, so the grant
	// answers "which agents may this version write to" rather than "may this
	// version use the mailbox".
	IPC Kind = "ipc"
)

var ErrDenied = errors.New("agent version capability denied")

// Authorizer resolves the immutable AgentVersion and enforces its symbolic
// grants. Legacy publications (no runtimes declaration) retain their
// tenant-policy behavior during the v1alpha1 compatibility window.
type Authorizer struct {
	versions store.AgentVersionStore
}

func NewAuthorizer(versions store.AgentVersionStore) (*Authorizer, error) {
	if versions == nil {
		return nil, errors.New("agent version store is required")
	}
	return &Authorizer{versions: versions}, nil
}

func (a *Authorizer) Authorize(
	ctx context.Context,
	tenantID, agentVersionRef string,
	kind Kind,
	candidates ...string,
) error {
	if a == nil || a.versions == nil {
		return fmt.Errorf("%w: capability authorizer is not configured", ErrDenied)
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(agentVersionRef) == "" {
		return fmt.Errorf("%w: tenant and agent version reference are required", ErrDenied)
	}
	version, err := a.versions.GetAgentVersionByRef(ctx, tenantID, agentVersionRef)
	if err != nil {
		return fmt.Errorf("%w: resolve immutable agent version: %v", ErrDenied, err)
	}
	var spec agentversion.Spec
	if err := json.Unmarshal(version.Spec, &spec); err != nil {
		return fmt.Errorf("%w: decode immutable agent version: %v", ErrDenied, err)
	}
	if len(spec.Runtimes) == 0 {
		return nil
	}
	if spec.Capabilities == nil {
		return fmt.Errorf("%w: portable publication has no capability declaration", ErrDenied)
	}
	var grants []string
	switch kind {
	case Tool:
		grants = spec.Capabilities.Tools
	case Model:
		grants = spec.Capabilities.Models
	case Memory:
		grants = spec.Capabilities.Memory
	case Secret:
		grants = spec.Capabilities.Secrets
	case IPC:
		grants = spec.Capabilities.Peers
	default:
		return fmt.Errorf("%w: unknown capability kind %q", ErrDenied, kind)
	}
	for _, grant := range grants {
		for _, candidate := range candidates {
			if MatchGrant(grant, candidate) {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: %s candidates %q are not granted to %s", ErrDenied, kind, candidates, agentVersionRef)
}

// MatchGrant is the single wildcard contract shared by every gateway:
// exact identifiers, global "*", or a suffix wildcard after a namespace
// separator ("search.*", "project/*") or a version separator ("weather@*",
// which floats across a tool's versions). Embedded wildcards never match.
func MatchGrant(pattern, value string) bool {
	if pattern == value {
		return true
	}
	if pattern == "*" {
		return value != ""
	}
	if strings.Count(pattern, "*") != 1 || !strings.HasSuffix(pattern, "*") {
		return false
	}
	prefix := strings.TrimSuffix(pattern, "*")
	if !strings.HasSuffix(prefix, ".") && !strings.HasSuffix(prefix, "/") && !strings.HasSuffix(prefix, "@") {
		return false
	}
	return strings.HasPrefix(value, prefix) && len(value) > len(prefix)
}

// ToolFreeze resolves an AgentVersion's tool grants together with the
// publish-time version freeze (P1-08). A bare name or a name wildcard is frozen
// to the tool versions that already existed when the AgentVersion was
// published: registering a newer tool version afterward does not silently
// re-target an old, immutable publication. Two grant forms opt out of the
// freeze explicitly — a pinned "name@version" (an exact, immutable choice, so
// its registration time is irrelevant) and a floating "name@*" (the caller
// deliberately accepts version drift). Legacy publications (no runtimes
// declaration) keep tenant-policy behavior during the compatibility window and
// are not frozen.
type ToolFreeze struct {
	grants      []string
	publishedAt time.Time
	enforce     bool
}

// ToolFreeze loads the immutable AgentVersion once and captures the state
// needed to decide tool visibility and invocation for the whole request. It is
// the freeze-aware counterpart of Authorize for the tool kind.
func (a *Authorizer) ToolFreeze(ctx context.Context, tenantID, agentVersionRef string) (ToolFreeze, error) {
	if a == nil || a.versions == nil {
		return ToolFreeze{}, fmt.Errorf("%w: capability authorizer is not configured", ErrDenied)
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(agentVersionRef) == "" {
		return ToolFreeze{}, fmt.Errorf("%w: tenant and agent version reference are required", ErrDenied)
	}
	version, err := a.versions.GetAgentVersionByRef(ctx, tenantID, agentVersionRef)
	if err != nil {
		return ToolFreeze{}, fmt.Errorf("%w: resolve immutable agent version: %v", ErrDenied, err)
	}
	var spec agentversion.Spec
	if err := json.Unmarshal(version.Spec, &spec); err != nil {
		return ToolFreeze{}, fmt.Errorf("%w: decode immutable agent version: %v", ErrDenied, err)
	}
	if len(spec.Runtimes) == 0 {
		return ToolFreeze{enforce: false}, nil
	}
	if spec.Capabilities == nil {
		return ToolFreeze{}, fmt.Errorf("%w: portable publication has no capability declaration", ErrDenied)
	}
	return ToolFreeze{grants: spec.Capabilities.Tools, publishedAt: version.CreatedAt, enforce: true}, nil
}

// Enforced reports whether the freeze applies. It is false for legacy
// publications, where every registered descriptor stays visible under the
// tenant-policy compatibility window.
func (f ToolFreeze) Enforced() bool { return f.enforce }

// Allow reports whether the AgentVersion may see or invoke this tool version.
// registeredAt is the descriptor's immutable registration time. A pinned grant
// matches its exact version regardless of when it was registered; a "name@*"
// floating grant matches any version of that name; a bare name or a name
// wildcard matches only versions registered no later than the AgentVersion's
// own publication (the freeze).
func (f ToolFreeze) Allow(name, version string, registeredAt time.Time) bool {
	if !f.enforce {
		return true
	}
	withinFreeze := !registeredAt.After(f.publishedAt)
	for _, grant := range f.grants {
		gname, gversion, versioned := strings.Cut(grant, "@")
		if versioned {
			if gversion == "*" {
				if gname == name {
					return true
				}
				continue
			}
			if gname == name && gversion == version {
				return true
			}
			continue
		}
		// A name-level grant (exact, global "*", or "prefix.*"/"prefix/*") is
		// frozen to the publish-time version set.
		if withinFreeze && MatchGrant(grant, name) {
			return true
		}
	}
	return false
}
