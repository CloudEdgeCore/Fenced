package store

import (
	"context"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
)

// RetryRetryable runs fn, retrying transient transaction conflicts with
// bounded exponential backoff (ADR-002: SERIALIZABLE with bounded retries).
// Non-retryable errors and context cancellation return immediately.
func RetryRetryable(ctx context.Context, fn func() (int, error)) (int, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		processed, err := fn()
		if err == nil || !IsRetryableTransaction(err) {
			return processed, err
		}
		agentmetrics.DBTransactionConflict(ctx)
		if attempt < 3 {
			agentmetrics.DBTransactionRetry(ctx)
		}
		lastErr = err
		delay := time.Duration(1<<attempt) * 5 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return processed, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, lastErr
}
