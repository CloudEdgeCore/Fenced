// Package artifact provides content-addressed Artifact storage adapters.
package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// ErrQuotaExceeded reports an artifact write that would breach the store's
// aggregate or per-tenant quota.
var ErrQuotaExceeded = errors.New("artifact store quota exceeded")

var tenantToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

type Filesystem struct {
	root     string
	maxBytes int64
	// tenantQuota / totalQuota bound aggregate storage (O7); zero disables
	// the respective bound.
	tenantQuota int64
	totalQuota  int64
}

func NewFilesystem(root string, maxBytes int64) (*Filesystem, error) {
	return NewFilesystemWithQuotas(root, maxBytes, 0, 0)
}

// NewFilesystemWithQuotas constructs the store with aggregate bounds: the
// per-tenant quota and the total quota in bytes (0 = unlimited). Enforcement
// is best-effort O(files) for the dev adapter; production adapters index
// usage.
func NewFilesystemWithQuotas(root string, maxBytes, tenantQuota, totalQuota int64) (*Filesystem, error) {
	if strings.TrimSpace(root) == "" || maxBytes <= 0 {
		return nil, fmt.Errorf("artifact root and positive maximum size are required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve artifact root: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create artifact root: %w", err)
	}
	return &Filesystem{root: absolute, maxBytes: maxBytes, tenantQuota: tenantQuota, totalQuota: totalQuota}, nil
}

func (f *Filesystem) Put(ctx context.Context, tenantID, mediaType string, source io.Reader) (store.ArtifactReference, error) {
	var result store.ArtifactReference
	if !tenantToken.MatchString(tenantID) || strings.TrimSpace(mediaType) == "" || source == nil {
		return result, fmt.Errorf("valid tenant, media type, and source are required")
	}
	tenantRoot := filepath.Join(f.root, tenantID, "sha256")
	if err := os.MkdirAll(tenantRoot, 0o700); err != nil {
		return result, fmt.Errorf("create tenant artifact directory: %w", err)
	}
	temporary, err := os.CreateTemp(tenantRoot, ".upload-*")
	if err != nil {
		return result, fmt.Errorf("create artifact staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	hash := sha256.New()
	written, copyErr := copyContext(ctx, io.MultiWriter(temporary, hash), io.LimitReader(source, f.maxBytes+1))
	// Sync before the rename publishes the artifact: a crash must never leave
	// a committed name pointing at unflushed content.
	syncErr := temporary.Sync()
	closeErr := temporary.Close()
	if copyErr != nil {
		return result, copyErr
	}
	if syncErr != nil {
		return result, fmt.Errorf("sync staged artifact: %w", syncErr)
	}
	if closeErr != nil {
		return result, fmt.Errorf("close staged artifact: %w", closeErr)
	}
	if written > f.maxBytes {
		return result, fmt.Errorf("artifact exceeds %d byte limit", f.maxBytes)
	}
	digest := hash.Sum(nil)
	digestHex := hex.EncodeToString(digest)
	finalPath := filepath.Join(tenantRoot, digestHex)
	if _, err := os.Stat(finalPath); err == nil {
		// Content-addressed dedup: the artifact already exists; no quota
		// accounting is needed.
		return f.reference(tenantID, mediaType, digest, written), nil
	} else if !os.IsNotExist(err) {
		return result, fmt.Errorf("inspect artifact destination: %w", err)
	}
	// Aggregate quota enforcement (O7): the write must not breach the
	// per-tenant or total bounds.
	if err := f.checkQuota(tenantID, written); err != nil {
		return result, err
	}
	if err := moveIfAbsent(temporaryPath, finalPath); err != nil {
		return result, err
	}
	return f.reference(tenantID, mediaType, digest, written), nil
}

// reference renders the content-addressed reference for a committed digest.
func (f *Filesystem) reference(tenantID, mediaType string, digest []byte, size int64) store.ArtifactReference {
	var result store.ArtifactReference
	copy(result.SHA256[:], digest)
	result.URI = "artifact://" + tenantID + "/sha256/" + hex.EncodeToString(digest)
	result.SizeBytes = size
	result.MediaType = mediaType
	return result
}

// checkQuota verifies the write stays within the per-tenant and total
// bounds. The tenant's own directory is counted for the tenant bound; the
// whole store for the total bound. The staged file is not yet committed, so
// the prospective size is added to the existing usage.
func (f *Filesystem) checkQuota(tenantID string, prospective int64) error {
	if f.tenantQuota > 0 {
		used, err := dirBytes(filepath.Join(f.root, tenantID))
		if err != nil {
			return fmt.Errorf("measure tenant artifact usage: %w", err)
		}
		if used+prospective > f.tenantQuota {
			return fmt.Errorf("%w: tenant %s would use %d of %d bytes", ErrQuotaExceeded, tenantID, used+prospective, f.tenantQuota)
		}
	}
	if f.totalQuota > 0 {
		used, err := dirBytes(f.root)
		if err != nil {
			return fmt.Errorf("measure artifact usage: %w", err)
		}
		if used+prospective > f.totalQuota {
			return fmt.Errorf("%w: store would use %d of %d bytes", ErrQuotaExceeded, used+prospective, f.totalQuota)
		}
	}
	return nil
}

// GarbageCollect deletes artifacts whose modification time is older than the
// retention window (O7). The dev adapter is TTL-based: reference-aware GC
// (only unreferenced artifacts) is a projection documented with the audit
// retention trade-off. It returns the number of deleted files.
func (f *Filesystem) GarbageCollect(ctx context.Context, retention time.Duration, now time.Time) (int, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := now.Add(-retention)
	deleted := 0
	err := filepath.WalkDir(f.root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".upload-") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			deleted++
		}
		return nil
	})
	return deleted, err
}

// dirBytes sums the file sizes under a directory tree, excluding in-flight
// staging files (`.upload-*`) that a concurrent or crashed writer may still
// own.
func dirBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".upload-") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func (f *Filesystem) Open(ctx context.Context, tenantID string, reference store.ArtifactReference) (io.ReadCloser, error) {
	if !tenantToken.MatchString(tenantID) || reference.DigestHex() == strings.Repeat("0", sha256.Size*2) {
		return nil, fmt.Errorf("valid tenant and artifact digest are required")
	}
	expectedURI := "artifact://" + tenantID + "/sha256/" + reference.DigestHex()
	if reference.URI != expectedURI {
		return nil, fmt.Errorf("artifact URI does not match tenant and digest")
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
	}
	file, err := os.Open(filepath.Join(f.root, tenantID, "sha256", reference.DigestHex()))
	if err != nil {
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	return &verifyingReader{file: file, expected: reference.SHA256, expectedSize: reference.SizeBytes, hash: sha256.New()}, nil
}

type verifyingReader struct {
	file         *os.File
	expected     [sha256.Size]byte
	expectedSize int64
	read         int64
	hash         hashWriter
	verified     bool
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (r *verifyingReader) Read(buffer []byte) (int, error) {
	n, err := r.file.Read(buffer)
	if n > 0 {
		r.read += int64(n)
		_, _ = r.hash.Write(buffer[:n])
	}
	if err == io.EOF && !r.verified {
		r.verified = true
		if r.read != r.expectedSize || !strings.EqualFold(hex.EncodeToString(r.hash.Sum(nil)), hex.EncodeToString(r.expected[:])) {
			return n, fmt.Errorf("artifact content failed size or SHA-256 verification")
		}
	}
	return n, err
}

func (r *verifyingReader) Close() error { return r.file.Close() }

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 32<<10)
	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, context.Cause(ctx)
		default:
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			count, writeErr := destination.Write(buffer[:read])
			written += int64(count)
			if writeErr != nil {
				return written, writeErr
			}
			if count != read {
				return written, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}

func moveIfAbsent(source, destination string) error {
	if _, err := os.Stat(destination); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect artifact destination: %w", err)
	}
	if err := os.Rename(source, destination); err != nil {
		if _, statErr := os.Stat(destination); statErr == nil {
			return nil
		}
		return fmt.Errorf("commit artifact: %w", err)
	}
	return nil
}
