// Package recovery reconciles expired runtime leases into fenced retries.
package recovery

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
	"github.com/google/uuid"
)

type Repository interface {
	ListExpiredAttempts(context.Context, time.Time, int) ([]store.RecoveryCandidate, error)
	RecoverExpiredAttempt(context.Context, store.RecoverExpiredAttemptInput) (store.RecoveryResult, error)
}

type Controller struct {
	repository Repository
	batch      int
	leaseTTL   time.Duration
	now        func() time.Time
	newID      func() uuid.UUID
	// parallel bounds concurrent per-candidate recovery within one batch
	// (P1); 1 disables parallelism.
	parallel int
}

func NewController(repository Repository, batch int, leaseTTL time.Duration) *Controller {
	return &Controller{
		repository: repository, batch: batch, leaseTTL: leaseTTL,
		now: func() time.Time { return time.Now().UTC() }, newID: newUUIDv7, parallel: 4,
	}
}

// WithParallelism bounds concurrent per-candidate recovery within one batch
// (default 4; 1 = serial).
func WithParallelism(workers int) func(*Controller) {
	return func(c *Controller) {
		if workers > 0 {
			c.parallel = workers
		}
	}
}

// Reconcile recovers expired attempts. Transient transaction conflicts are
// retried with bounded backoff (ADR-002).
func (c *Controller) Reconcile(ctx context.Context) (int, error) {
	return store.RetryRetryable(ctx, func() (int, error) { return c.reconcileOnce(ctx) })
}

func (c *Controller) reconcileOnce(ctx context.Context) (int, error) {
	candidates, err := c.repository.ListExpiredAttempts(ctx, c.now(), c.batch)
	if err != nil {
		return 0, err
	}
	for range candidates {
		agentmetrics.LeaseExpiration(ctx)
	}
	if c.parallel <= 1 || len(candidates) <= 1 {
		processed := 0
		for _, candidate := range candidates {
			ok, err := c.processCandidate(ctx, candidate)
			if err != nil {
				return processed, err
			}
			if ok {
				processed++
			}
		}
		return processed, nil
	}
	processed := 0
	var mu sync.Mutex
	var batchErr error
	semaphore := make(chan struct{}, c.parallel)
	var wg sync.WaitGroup
	for _, candidate := range candidates {
		candidate := candidate
		mu.Lock()
		aborted := batchErr != nil
		mu.Unlock()
		if aborted {
			break
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()
			ok, err := c.processCandidate(ctx, candidate)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if batchErr == nil {
					batchErr = err
				}
				return
			}
			if ok {
				processed++
			}
		}()
	}
	wg.Wait()
	return processed, batchErr
}

// processCandidate recovers one expired attempt, reporting whether it was
// recovered. Lost races and per-candidate failures are isolated; retryable
// failures abort the batch.
func (c *Controller) processCandidate(ctx context.Context, candidate store.RecoveryCandidate) (bool, error) {
	spec, err := workload.Decode(candidate.TaskSpec)
	if err != nil {
		// Per-task error isolation: a poisoned task spec must not block
		// the recovery of every other expired attempt. The candidate is
		// skipped and logged; its lease stays expired for the next round.
		slog.Error("recovery decode failed for expired attempt; isolated", "attempt", candidate.AttemptID, "tenant", candidate.TenantID, "error", err)
		return false, nil
	}
	_, err = c.repository.RecoverExpiredAttempt(ctx, store.RecoverExpiredAttemptInput{
		TenantID: candidate.TenantID, AttemptID: candidate.AttemptID,
		FencingToken: candidate.FencingToken, NewAttemptID: c.newID(), NewLeaseID: c.newID(),
		LeaseTTL: c.leaseTTL, MaxAttempts: spec.RetryPolicy.EffectiveMaxAttempts(),
		DeadlineExceeded: candidate.DeadlineExceeded,
	})
	if errors.Is(err, store.ErrFenced) || errors.Is(err, store.ErrLeaseNotExpired) || errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if store.IsRetryableTransaction(err) {
		return false, err
	}
	if err != nil {
		slog.Error("recovery failed for expired attempt; isolated", "attempt", candidate.AttemptID, "tenant", candidate.TenantID, "error", err)
		return false, nil
	}
	agentmetrics.RuntimeTakeover(ctx)
	return true, nil
}

func newUUIDv7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
