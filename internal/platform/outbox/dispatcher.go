// Package outbox delivers PostgreSQL outbox events to a durable publisher.
// Delivery is at-least-once: a crash after broker acknowledgement and before
// the database acknowledgement may publish a duplicate.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type Repository interface {
	ClaimOutbox(context.Context, store.ClaimOutboxInput) ([]store.OutboxEvent, error)
	MarkOutboxPublished(context.Context, uuid.UUID, string, int64, time.Time) error
	MarkOutboxFailed(context.Context, uuid.UUID, string, int64, string, time.Time) error
}

type Publisher interface {
	Publish(context.Context, store.OutboxEvent) error
}

type Dispatcher struct {
	repository Repository
	publisher  Publisher
	id         string
	batch      int
	lockTTL    time.Duration
	now        func() time.Time
}

func NewDispatcher(repository Repository, publisher Publisher, id string, batch int, lockTTL time.Duration) *Dispatcher {
	return &Dispatcher{
		repository: repository, publisher: publisher, id: id, batch: batch, lockTTL: lockTTL,
		now: func() time.Time { return time.Now().UTC() },
	}
}

// operationTimeout bounds each claim/publish/acknowledge step so one slow
// broker or store round trip marks that event failed (retried by backoff)
// instead of stalling the whole dispatch pipeline.
const operationTimeout = 10 * time.Second

func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	claimCtx, cancelClaim := context.WithTimeout(ctx, operationTimeout)
	events, err := d.repository.ClaimOutbox(claimCtx, store.ClaimOutboxInput{
		DispatcherID: d.id, Limit: d.batch, LockTTL: d.lockTTL,
	})
	cancelClaim()
	if err != nil {
		return 0, err
	}
	published := 0
	var failures []error
	for _, event := range events {
		publishCtx, cancelPublish := context.WithTimeout(ctx, operationTimeout)
		publishErr := d.publisher.Publish(publishCtx, event)
		cancelPublish()
		if publishErr != nil {
			retryAt := d.now().Add(retryBackoff(event.PublishAttempts))
			markCtx, cancelMark := context.WithTimeout(ctx, operationTimeout)
			markErr := d.repository.MarkOutboxFailed(markCtx, event.ID, d.id, event.LockFencingToken, publishErr.Error(), retryAt)
			cancelMark()
			if markErr != nil {
				failures = append(failures, fmt.Errorf("publish event %s: %w; mark failed: %v", event.ID, publishErr, markErr))
			} else {
				failures = append(failures, fmt.Errorf("publish event %s: %w", event.ID, publishErr))
			}
			continue
		}
		ackCtx, cancelAck := context.WithTimeout(ctx, operationTimeout)
		ackErr := d.repository.MarkOutboxPublished(ackCtx, event.ID, d.id, event.LockFencingToken, d.now())
		cancelAck()
		if ackErr != nil {
			failures = append(failures, fmt.Errorf("acknowledge event %s: %w", event.ID, ackErr))
			continue
		}
		published++
	}
	return published, errors.Join(failures...)
}

func retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := math.Min(float64(attempt-1), 8)
	return time.Duration(math.Pow(2, exponent)) * time.Second
}
