package tool

import (
	"sort"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"golang.org/x/mod/semver"
)

// CompareToolVersions orders two tool versions: semantic versions compare
// numerically; anything else falls back to a byte-wise comparison so
// non-semver registry labels stay deterministic.
func CompareToolVersions(left, right string) int {
	canonicalLeft, canonicalRight := "v"+left, "v"+right
	if semver.IsValid(canonicalLeft) && semver.IsValid(canonicalRight) {
		return semver.Compare(canonicalLeft, canonicalRight)
	}
	return strings.Compare(left, right)
}

// LatestVersionPerName collapses descriptors to exactly one entry per tool
// name: the latest version among the given (already capability-filtered)
// set, in stable name order (P1-02). The model-facing surface must never
// advertise two same-named tools — duplicate names are ambiguous for the
// model and rejected by downstream consumers — while invocation keeps
// resolving bare names to this same latest version, so what tools/list
// shows is exactly what a bare-name call executes. Explicit "name@version"
// references continue to pin any granted older version.
func LatestVersionPerName(descriptors []store.ToolDescriptor) []store.ToolDescriptor {
	if len(descriptors) == 0 {
		return nil
	}
	latest := make(map[string]store.ToolDescriptor, len(descriptors))
	names := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		current, seen := latest[descriptor.Name]
		if !seen {
			latest[descriptor.Name] = descriptor
			names = append(names, descriptor.Name)
			continue
		}
		if CompareToolVersions(descriptor.Version, current.Version) > 0 {
			latest[descriptor.Name] = descriptor
		}
	}
	sort.Strings(names)
	result := make([]store.ToolDescriptor, 0, len(names))
	for _, name := range names {
		result = append(result, latest[name])
	}
	return result
}
