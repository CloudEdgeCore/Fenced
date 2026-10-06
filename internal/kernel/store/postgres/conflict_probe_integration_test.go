//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// TestConcurrentCompletionConflictRate is the load evidence behind ADR-002's
// isolation choice: 16-way concurrent completions measured ~94% SERIALIZABLE
// conflicts (PostgreSQL SSI page-level predicates abort disjoint-row
// writers), and 0 conflicts under READ COMMITTED with row locks and CAS —
// the same invariants at ~50x the throughput. The test asserts the conflict
// budget stays near zero so a regression cannot silently reintroduce
// SERIALIZABLE on the hot path.
func TestConcurrentCompletionConflictRate(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	const tasks = 400
	const workers = 16
	assignments := make([]kernelstore.RuntimeAssignment, tasks)
	// Scheduling is serial so each poll observes its own task; every attempt
	// is advanced to RUNNING immediately so later polls never see it. The
	// measured contention is the concurrent completion itself.
	for i := 0; i < tasks; i++ {
		a := scheduleRuntimeTask(t, ctx, repository, fmt.Sprintf("conflict-%d", i), 3)
		starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
			TenantID: "tenant-a", AttemptID: a.Attempt.ID, FencingToken: 1, ExpectedAttemptVersion: 1, To: "STARTING",
		})
		if err != nil {
			t.Fatal(err)
		}
		running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
			TenantID: "tenant-a", AttemptID: a.Attempt.ID, FencingToken: 1, ExpectedAttemptVersion: starting.ResourceVersion, To: "RUNNING",
		})
		if err != nil {
			t.Fatal(err)
		}
		a.Attempt = running
		assignments[i] = a
	}

	var conflicts, successes, other atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < tasks; i += workers {
				a := assignments[i]
				_, err := repository.CompleteAttempt(ctx, kernelstore.CompleteAttemptInput{
					TenantID: "tenant-a", AttemptID: a.Attempt.ID, FencingToken: 1,
					ExpectedAttemptVersion: 3, IdempotencyKey: fmt.Sprintf("complete-%d", i),
					Result: artifactReference(fmt.Sprintf("artifact://tenant-a/sha256/result-%d", i), fmt.Sprintf("result-%d", i)),
				})
				switch {
				case err == nil:
					successes.Add(1)
				case kernelstore.IsRetryableTransaction(err):
					conflicts.Add(1)
				default:
					other.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("CONFLICT RATE successes=%d conflicts=%d other=%d elapsed=%s (workers=%d tasks=%d)",
		successes.Load(), conflicts.Load(), other.Load(), elapsed, workers, tasks)
	if conflicts.Load() > 0 || other.Load() > 0 {
		t.Fatalf("concurrent completions must not conflict under READ COMMITTED: %d conflicts, %d other errors", conflicts.Load(), other.Load())
	}
}
