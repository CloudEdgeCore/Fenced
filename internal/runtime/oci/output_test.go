package oci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// recordingSpooler captures spooled output and returns a deterministic
// artifact reference.
type recordingSpooler struct {
	content []byte
	calls   int
}

func (s *recordingSpooler) Spool(_ context.Context, tenantID, attemptID, mediaType string, reader io.Reader) (store.ArtifactReference, error) {
	s.calls++
	content, err := io.ReadAll(reader)
	if err != nil {
		return store.ArtifactReference{}, err
	}
	s.content = append(s.content, content...)
	digest := sha256.Sum256(content)
	return store.ArtifactReference{
		URI:    "artifact://" + tenantID + "/sha256/" + hex.EncodeToString(digest[:]),
		SHA256: digest, SizeBytes: int64(len(content)), MediaType: mediaType,
	}, nil
}

func TestSpoolOutputBoundedAndTruncated(t *testing.T) {
	ctx := context.Background()

	// Under the cap: spooled whole, not truncated.
	small := &recordingSpooler{}
	ref, truncated, err := spoolOutput(ctx, small, "tenant-a", "attempt-1", "application/vnd.fenced.stdout+octet-stream",
		strings.NewReader(strings.Repeat("x", 1024)))
	if err != nil || truncated || ref == nil {
		t.Fatalf("small spool: ref=%v truncated=%v err=%v", ref, truncated, err)
	}
	if len(small.content) != 1024 || small.calls != 1 {
		t.Fatalf("spooled %d bytes in %d calls", len(small.content), small.calls)
	}

	// Over the cap: spooled to the cap and flagged truncated.
	big := &recordingSpooler{}
	_, truncated, err = spoolOutput(ctx, big, "tenant-a", "attempt-2", "application/vnd.fenced.stdout+octet-stream",
		strings.NewReader(strings.Repeat("y", SpoolCap+4096)))
	if err != nil || !truncated {
		t.Fatalf("big spool: truncated=%v err=%v", truncated, err)
	}
	if len(big.content) != SpoolCap {
		t.Fatalf("spooled %d bytes, want the %d cap", len(big.content), SpoolCap)
	}

	// Exactly at the cap is NOT truncated.
	exact := &recordingSpooler{}
	_, truncated, err = spoolOutput(ctx, exact, "tenant-a", "attempt-5", "application/vnd.fenced.stdout+octet-stream",
		strings.NewReader(strings.Repeat("w", SpoolCap)))
	if err != nil || truncated {
		t.Fatalf("exact spool: truncated=%v err=%v", truncated, err)
	}
	if len(exact.content) != SpoolCap {
		t.Fatalf("exact spooled %d bytes, want %d", len(exact.content), SpoolCap)
	}

	// Without a spooler the output is drained and discarded, bounded.
	_, truncated, err = spoolOutput(ctx, nil, "tenant-a", "attempt-3", "application/vnd.fenced.stdout+octet-stream",
		strings.NewReader(strings.Repeat("z", 2*SpoolCap)))
	if err != nil || !truncated {
		t.Fatalf("discard spool: truncated=%v err=%v", truncated, err)
	}

	// A nil reader produces no spool.
	ref, truncated, err = spoolOutput(ctx, small, "tenant-a", "attempt-4", "application/vnd.fenced.stdout+octet-stream", nil)
	if err != nil || truncated || ref != nil {
		t.Fatalf("nil reader: ref=%v truncated=%v err=%v", ref, truncated, err)
	}
}

func TestReapTargetsFiltersOwnedContainers(t *testing.T) {
	listed := []string{"fenced-orphan-1", "other-namespace-container", "fenced-11111111-1111-1111-1111-111111111111", "plain"}
	active := map[string]struct{}{"fenced-11111111-1111-1111-1111-111111111111": {}}
	targets := reapTargets(listed, active)
	if len(targets) != 1 || targets[0] != "fenced-orphan-1" {
		t.Fatalf("reap targets = %v, want only the unowned fenced-* container", targets)
	}
	if targets := reapTargets(nil, nil); len(targets) != 0 {
		t.Fatalf("empty list produced targets: %v", targets)
	}
}
