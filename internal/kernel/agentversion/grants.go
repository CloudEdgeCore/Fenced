package agentversion

import "strings"

// ToolGrantSet is one tool grant list split into the vocabularies the
// kernel compares against.
//
// The three views exist because the kernel makes three different questions
// about the same grant:
//
//   - Capability: may this AgentVersion use the tool at all? It is matched
//     against a bare tool name, so a grant that pins a version grants the
//     tool as well.
//   - NameLevel: which tool versions may it use? These are the grants
//     authored without a version, and they are frozen to the tool versions
//     that existed when the AgentVersion was published.
//   - Versioned: which exact version may it use? These are the pinned
//     ("name@version") and floating ("name@*") grants; they decide the
//     version and opt out of the freeze.
//
// Before the split, the runtime capability check and the publish-time freeze
// read the same field with two different vocabularies: the capability check
// compares bare tool names while the freeze understands the version forms.
// A correctly pinned publication was therefore denied every agent-facing
// tool call, while a bare grant kept its frozen behaviour.
type ToolGrantSet struct {
	// Capability is matched against a bare tool name.
	Capability []string
	// NameLevel holds the grants authored without a version.
	NameLevel []string
	// Versioned holds the pinned and floating grants.
	Versioned []string
}

// SplitToolGrants splits one grant list into its three views. The result is
// deduplicated and keeps first-appearance order, so a publication stays
// deterministic.
func SplitToolGrants(grants []string) ToolGrantSet {
	set := ToolGrantSet{}
	seenCapability := map[string]bool{}
	seenNameLevel := map[string]bool{}
	seenVersioned := map[string]bool{}
	for _, grant := range grants {
		trimmed := strings.TrimSpace(grant)
		if trimmed == "" {
			continue
		}
		name, _, versioned := strings.Cut(trimmed, "@")
		if versioned {
			if name == "" {
				continue
			}
			if !seenCapability[name] {
				seenCapability[name] = true
				set.Capability = append(set.Capability, name)
			}
			if !seenVersioned[trimmed] {
				seenVersioned[trimmed] = true
				set.Versioned = append(set.Versioned, trimmed)
			}
			continue
		}
		if !seenCapability[trimmed] {
			seenCapability[trimmed] = true
			set.Capability = append(set.Capability, trimmed)
		}
		if !seenNameLevel[trimmed] {
			seenNameLevel[trimmed] = true
			set.NameLevel = append(set.NameLevel, trimmed)
		}
	}
	return set
}

// PublicationToolGrants returns the views of a published specification. A
// normalized publication keeps the version vocabulary in
// capabilities.toolVersions; a publication written before that split (or by
// an older writer) carries both forms inside capabilities.tools.
//
// The merge is idempotent: normalizing an already normalized document
// reproduces it, so a publication can be canonicalized again without losing
// its pinned versions.
func PublicationToolGrants(capabilitiesTools, capabilityToolVersions []string) ToolGrantSet {
	set := SplitToolGrants(capabilitiesTools)
	seenCapability := map[string]bool{}
	for _, grant := range set.Capability {
		seenCapability[grant] = true
	}
	seenVersioned := map[string]bool{}
	for _, grant := range set.Versioned {
		seenVersioned[grant] = true
	}
	for _, grant := range capabilityToolVersions {
		trimmed := strings.TrimSpace(grant)
		if trimmed == "" {
			continue
		}
		name, _, versioned := strings.Cut(trimmed, "@")
		if name != "" && !seenCapability[name] {
			// A pinned or floating grant also grants the tool itself.
			seenCapability[name] = true
			set.Capability = append(set.Capability, name)
		}
		if versioned && !seenVersioned[trimmed] {
			seenVersioned[trimmed] = true
			set.Versioned = append(set.Versioned, trimmed)
		}
	}
	return set
}
