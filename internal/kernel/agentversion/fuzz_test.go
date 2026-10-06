package agentversion

import "testing"

func FuzzDecodeManifest(f *testing.F) {
	f.Add([]byte(`{"apiVersion":"fenced.dev/v1","kind":"Agent"}`))
	f.Add([]byte(`{"apiVersion":"fenced.dev/v1","apiVersion":"fenced.dev/v1"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		manifest, canonical, _, err := DecodeManifest(raw)
		if err != nil {
			return
		}
		replayed, replayCanonical, _, replayErr := DecodeManifest(canonical)
		if replayErr != nil || replayed.Ref() != manifest.Ref() || string(replayCanonical) != string(canonical) {
			t.Fatalf("accepted manifest is not canonically stable: %v", replayErr)
		}
	})
}
