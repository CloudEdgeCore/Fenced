package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func put(t *testing.T, store *Filesystem, tenantID, content string) store.ArtifactReference {
	t.Helper()
	reference, err := store.Put(context.Background(), tenantID, "application/octet-stream", strings.NewReader(content))
	if err != nil {
		t.Fatalf("put %q: %v", content, err)
	}
	return reference
}

func TestTenantQuotaEnforced(t *testing.T) {
	store, err := NewFilesystemWithQuotas(t.TempDir(), 1<<20, 8, 0)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	put(t, store, "tenant-a", "01234567") // 8 bytes: exactly at the tenant quota
	// The next write breaches the per-tenant quota.
	if _, err := store.Put(context.Background(), "tenant-a", "application/octet-stream", strings.NewReader("more")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("tenant quota error = %v, want ErrQuotaExceeded", err)
	}
	// Another tenant is unaffected.
	put(t, store, "tenant-b", "other")
	// Dedup of an existing artifact is not charged again.
	put(t, store, "tenant-a", "01234567")
}

func TestTotalQuotaEnforced(t *testing.T) {
	store, err := NewFilesystemWithQuotas(t.TempDir(), 1<<20, 0, 16)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	put(t, store, "tenant-a", "aaaaaaaa")
	put(t, store, "tenant-b", "bbbbbbbb") // 16 bytes total: exactly at the cap
	if _, err := store.Put(context.Background(), "tenant-c", "application/octet-stream", strings.NewReader("x")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("total quota error = %v, want ErrQuotaExceeded", err)
	}
}

func TestGarbageCollectRemovesExpiredArtifacts(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemWithQuotas(root, 1<<20, 0, 0)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	fresh := put(t, store, "tenant-a", "fresh-artifact")
	old := put(t, store, "tenant-a", "old-artifact")

	// Age the old artifact beyond the retention window.
	oldPath := filepath.Join(root, "tenant-a", "sha256", old.DigestHex())
	if err := os.Chtimes(oldPath, now.Add(-48*time.Hour), now.Add(-48*time.Hour)); err != nil {
		t.Fatalf("age artifact: %v", err)
	}

	deleted, err := store.GarbageCollect(context.Background(), 24*time.Hour, now)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("gc deleted %d files, want 1", deleted)
	}
	if _, err := store.Open(context.Background(), "tenant-a", old); err == nil {
		t.Fatal("expired artifact survived GC")
	}
	freshReader, err := store.Open(context.Background(), "tenant-a", fresh)
	if err != nil {
		t.Fatalf("fresh artifact was collected: %v", err)
	}
	_ = freshReader.Close()

	// A zero retention disables GC.
	before, err := store.GarbageCollect(context.Background(), 0, now)
	if err != nil || before != 0 {
		t.Fatalf("zero retention gc = %d, %v", before, err)
	}
}

func TestGarbageCollectSkipsStagingFiles(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemWithQuotas(root, 1<<20, 0, 0)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// A stray staging file from a crashed writer must not be deleted while
	// fresh (it may still be mid-write) nor counted as an artifact.
	staging := filepath.Join(root, "tenant-a", "sha256")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	old := time.Now().Add(-48 * time.Hour)
	stagingPath := filepath.Join(staging, ".upload-1")
	if err := os.WriteFile(stagingPath, []byte("partial"), 0o600); err != nil {
		t.Fatalf("write staging: %v", err)
	}
	if err := os.Chtimes(stagingPath, old, old); err != nil {
		t.Fatalf("age staging: %v", err)
	}
	deleted, err := store.GarbageCollect(context.Background(), 24*time.Hour, time.Now())
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("gc deleted %d staging files", deleted)
	}
}
