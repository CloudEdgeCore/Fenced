package agentpkg

import (
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
)

// FromAgentManifest builds the unsigned supply-chain manifest from a strict
// portable Agent Manifest. The returned spec bytes are exactly those accepted
// by AgentVersion publication, so signing and admission bind the same object.
func FromAgentManifest(manifest agentversion.Manifest, provenance Provenance) (Manifest, error) {
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	spec, err := json.Marshal(manifest.Spec)
	if err != nil {
		return Manifest{}, fmt.Errorf("canonicalize Agent Manifest spec: %w", err)
	}
	result := Manifest{
		Schema: ManifestSchema, AgentVersionRef: manifest.Ref(),
		SpecDigest: SpecSHA256(spec), Spec: spec, Provenance: provenance,
	}
	if err := result.Validate(); err != nil {
		return Manifest{}, err
	}
	return result, nil
}
