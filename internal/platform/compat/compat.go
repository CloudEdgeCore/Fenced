// Package compat provides backward-compatibility shims for the AgentOS →
// Fenced rename.
//
// The rename changed environment variable prefixes (AGENTOS_* → FENCED_*) and
// protocol identifiers (agentos.* → fenced.*). A deployment written before the
// rename keeps working if the process:
//
//   - calls AliasLegacyEnv once at start-up, so legacy AGENTOS_* variables are
//     visible under their FENCED_* names; and
//   - passes identifiers it reads from stored manifests through
//     NormalizeProtocol before validating or comparing them.
//
// Compatibility covers only the inputs this process reads. gRPC service names
// and protobuf type URLs changed with the protobuf package and are NOT aliased
// here; a client built before the rename cannot talk to a server built after
// it. See docs/COMPATIBILITY.md for the full boundary.
package compat

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	// LegacyPrefix is the environment variable prefix used before the rename.
	LegacyPrefix = "AGENTOS_"
	// CurrentPrefix is the environment variable prefix used after the rename.
	CurrentPrefix = "FENCED_"

	// LegacyExecutionHeader is the execution-identity header sent by workers
	// built before the rename.
	LegacyExecutionHeader = "X-Agentos-Execution"
	// CurrentExecutionHeader is the execution-identity header sent by workers
	// built after the rename.
	CurrentExecutionHeader = "X-Fenced-Execution"

	legacyProtocolPrefix  = "agentos."
	currentProtocolPrefix = "fenced."
)

// AliasLegacyEnv copies every AGENTOS_* environment variable to the matching
// FENCED_* variable when that variable is not already set. A value already
// present under FENCED_* always wins, so an operator can migrate one variable
// at a time; a variable explicitly set to the empty string also wins and is
// not overwritten.
//
// It returns the sorted names of the variables it aliased, which callers pass
// to WarnLegacyEnv. The function is idempotent.
func AliasLegacyEnv() []string {
	var aliased []string
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(name, LegacyPrefix) {
			continue
		}
		current := CurrentPrefix + strings.TrimPrefix(name, LegacyPrefix)
		if _, exists := os.LookupEnv(current); exists {
			continue
		}
		if err := os.Setenv(current, value); err != nil {
			continue
		}
		aliased = append(aliased, current)
	}
	sort.Strings(aliased)
	return aliased
}

// WarnLegacyEnv writes one deprecation notice to w when aliased is non-empty
// and is a no-op otherwise. Typical use:
//
//	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
func WarnLegacyEnv(w io.Writer, aliased []string) {
	if len(aliased) == 0 || w == nil {
		return
	}
	fmt.Fprintf(w, "fenced: deprecated %s* environment variables detected; aliased %d "+
		"variable(s) to %s* (%s); rename them before the next major release\n",
		LegacyPrefix, len(aliased), CurrentPrefix, strings.Join(aliased, ", "))
}

// NormalizeProtocol maps a legacy AgentOS protocol identifier to its Fenced
// equivalent. Already-canonical values, empty strings, and values that do not
// contain a protocol prefix are returned unchanged.
//
//	agentos.runtime.interface/v1 -> fenced.runtime.interface/v1
//	agentos.dev/v1alpha1         -> fenced.dev/v1alpha1
//	agentos.oci/v1               -> fenced.oci/v1
func NormalizeProtocol(id string) string {
	if id == "" || !strings.Contains(id, legacyProtocolPrefix) {
		return id
	}
	return strings.ReplaceAll(id, legacyProtocolPrefix, currentProtocolPrefix)
}

// NormalizeHeader maps a legacy AgentOS HTTP header name to its Fenced
// equivalent, comparing case-insensitively. Any other name is returned
// unchanged. It exists so a receiver can accept requests from a worker built
// before the rename.
func NormalizeHeader(name string) string {
	if strings.EqualFold(name, LegacyExecutionHeader) {
		return CurrentExecutionHeader
	}
	return name
}
