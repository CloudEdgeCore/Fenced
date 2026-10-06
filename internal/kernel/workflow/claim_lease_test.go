package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

var (
	errRenewalRejected = errors.New("renewal rejected")
	errWorkFailed      = errors.New("work failed")
)

// fakeClaimStore records how ClaimManager drives the store.
type fakeClaimStore struct {
	mu             sync.Mutex
	claimed        int
	listed         int
	lastClaimInput kernelstore.ClaimWorkflowsInput
	workflows      []kernelstore.Workflow
}

func (f *fakeClaimStore) ClaimWorkflows(_ context.Context, in kernelstore.ClaimWorkflowsInput) ([]kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimed++
	f.lastClaimInput = in
	return f.workflows, nil
}

func (f *fakeClaimStore) ListActiveWorkflows(context.Context, int) ([]kernelstore.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed++
	return f.workflows, nil
}

func TestClaimManagerClaimsWithLeaseWhenConfigured(t *testing.T) {
	store := &fakeClaimStore{workflows: []kernelstore.Workflow{{ID: uuid.New(), TenantID: "t"}}}
	manager := NewClaimManager(store, "owner-1", 16, 5*time.Second, 1000)

	active, err := manager.Claim(context.Background())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("claimed %d workflows, want 1", len(active))
	}
	if store.listed != 0 || store.claimed != 1 {
		t.Fatalf("claim paths: claimed=%d listed=%d, want claimed=1 listed=0", store.claimed, store.listed)
	}
	in := store.lastClaimInput
	if in.Owner != "owner-1" || in.Batch != 16 || in.Lease != 5*time.Second || in.MaxTokens != 1000 {
		t.Fatalf("claim input = %+v, want owner-1/16/5s/1000", in)
	}
}

func TestClaimManagerListsActiveWithoutLease(t *testing.T) {
	store := &fakeClaimStore{workflows: []kernelstore.Workflow{{ID: uuid.New(), TenantID: "t"}}}
	manager := NewClaimManager(store, "owner-1", 16, 0, 0)

	active, err := manager.Claim(context.Background())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("listed %d workflows, want 1", len(active))
	}
	if store.claimed != 0 || store.listed != 1 {
		t.Fatalf("claim paths: claimed=%d listed=%d, want claimed=0 listed=1", store.claimed, store.listed)
	}
}

// fakeRenewer counts renewals and can fail after a threshold.
type fakeRenewer struct {
	mu      sync.Mutex
	calls   int
	failAt  int // fail on the Nth call (1-based); 0 = never
	blockCh chan struct{}
}

func (f *fakeRenewer) RenewWorkflowClaim(context.Context, string, uuid.UUID, string, time.Duration) error {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()
	if f.failAt > 0 && call >= f.failAt {
		return errRenewalRejected
	}
	if f.blockCh != nil {
		<-f.blockCh
	}
	return nil
}

func (f *fakeRenewer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestLeaseManagerNoOpWithoutRenewer(t *testing.T) {
	workflow := kernelstore.Workflow{TenantID: "t", ID: uuid.New()}
	manager := NewLeaseManager(nil, "owner", 5*time.Second)

	ctx, release := manager.Guard(context.Background(), workflow)
	if ctx == nil {
		t.Fatal("guard returned nil context")
	}
	if err := release(); err != nil {
		t.Fatalf("release returned %v, want nil", err)
	}
}

func TestLeaseManagerNoOpWithZeroLease(t *testing.T) {
	workflow := kernelstore.Workflow{TenantID: "t", ID: uuid.New()}
	renewer := &fakeRenewer{}
	manager := NewLeaseManager(renewer, "owner", 0)

	_, release := manager.Guard(context.Background(), workflow)
	if err := release(); err != nil {
		t.Fatalf("release returned %v, want nil", err)
	}
	if renewer.count() != 0 {
		t.Fatalf("renewer called %d times with zero lease, want 0", renewer.count())
	}
}

func TestLeaseManagerRenewsDuringWork(t *testing.T) {
	workflow := kernelstore.Workflow{TenantID: "t", ID: uuid.New()}
	renewer := &fakeRenewer{}
	manager := NewLeaseManager(renewer, "owner", 300*time.Millisecond)

	ctx, release := manager.Guard(context.Background(), workflow)
	// Keep the guarded context alive long enough for several renewal ticks
	// (interval = lease/3 = 100ms).
	time.Sleep(350 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("work context cancelled while renewing: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release returned %v, want nil", err)
	}
	if renewer.count() < 2 {
		t.Fatalf("renewer called %d times, want at least 2", renewer.count())
	}
}

func TestLeaseManagerCancelsWorkWhenRenewalFails(t *testing.T) {
	workflow := kernelstore.Workflow{TenantID: "t", ID: uuid.New()}
	renewer := &fakeRenewer{failAt: 1}
	manager := NewLeaseManager(renewer, "owner", 300*time.Millisecond)

	ctx, release := manager.Guard(context.Background(), workflow)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := ctx.Err(); err == nil {
		t.Fatal("work context was not cancelled after renewal failure")
	}
	if err := release(); err == nil {
		t.Fatal("release returned nil, want renewal error")
	}
}

func TestLeaseManagerGuardedProcessJoinsErrors(t *testing.T) {
	workflow := kernelstore.Workflow{TenantID: "t", ID: uuid.New()}

	t.Run("work error and renewal error are joined", func(t *testing.T) {
		renewer := &fakeRenewer{failAt: 1}
		manager := NewLeaseManager(renewer, "owner", 300*time.Millisecond)
		_, err := manager.GuardedProcess(context.Background(), workflow, func(ctx context.Context) (bool, error) {
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			return false, errWorkFailed
		})
		if err == nil {
			t.Fatal("expected joined error, got nil")
		}
		if !errors.Is(err, errWorkFailed) || !errors.Is(err, errRenewalRejected) {
			t.Fatalf("joined error %v does not wrap both %v and %v", err, errWorkFailed, errRenewalRejected)
		}
	})

	t.Run("renewal error wraps when work succeeds", func(t *testing.T) {
		renewer := &fakeRenewer{failAt: 1}
		manager := NewLeaseManager(renewer, "owner", 300*time.Millisecond)
		processed, err := manager.GuardedProcess(context.Background(), workflow, func(ctx context.Context) (bool, error) {
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			return true, nil
		})
		if err == nil || !errors.Is(err, errRenewalRejected) {
			t.Fatalf("expected wrapped renewal error, got %v", err)
		}
		if !processed {
			t.Fatal("processed flag lost")
		}
	})

	t.Run("work error passes through when renewal succeeds", func(t *testing.T) {
		renewer := &fakeRenewer{}
		manager := NewLeaseManager(renewer, "owner", 300*time.Millisecond)
		_, err := manager.GuardedProcess(context.Background(), workflow, func(ctx context.Context) (bool, error) {
			return false, errWorkFailed
		})
		if !errors.Is(err, errWorkFailed) {
			t.Fatalf("expected work error to pass through, got %v", err)
		}
	})
}

// TestLeaseManagerConcurrentRelease ensures release is safe to call from a
// single goroutine after racing renewal goroutines (no deadlock, no panic).
func TestLeaseManagerConcurrentRelease(t *testing.T) {
	workflow := kernelstore.Workflow{TenantID: "t", ID: uuid.New()}
	renewer := &fakeRenewer{}
	manager := NewLeaseManager(renewer, "owner", 300*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, release := manager.Guard(context.Background(), workflow)
			time.Sleep(100 * time.Millisecond)
			_ = release()
			_ = ctx
		}()
	}
	wg.Wait()
}
