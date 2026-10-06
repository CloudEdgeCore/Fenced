package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestDispatcherMarksSuccessAndSchedulesFailure(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	success := store.OutboxEvent{ID: uuid.New(), PublishAttempts: 1, LockFencingToken: 4}
	failure := store.OutboxEvent{ID: uuid.New(), PublishAttempts: 3, LockFencingToken: 7}
	repository := &fakeRepository{events: []store.OutboxEvent{success, failure}}
	publisher := &fakePublisher{failureID: failure.ID}
	dispatcher := NewDispatcher(repository, publisher, "dispatcher-1", 10, 30*time.Second)
	dispatcher.now = func() time.Time { return now }

	published, err := dispatcher.RunOnce(context.Background())
	if published != 1 || err == nil {
		t.Fatalf("published=%d err=%v, want 1 and error", published, err)
	}
	if len(repository.published) != 1 || repository.published[0] != success.ID {
		t.Fatalf("published acknowledgements = %v", repository.published)
	}
	if len(repository.failed) != 1 || repository.failed[0].id != failure.ID {
		t.Fatalf("failed acknowledgements = %+v", repository.failed)
	}
	if got, want := repository.failed[0].retryAt, now.Add(4*time.Second); !got.Equal(want) {
		t.Fatalf("retryAt=%s, want %s", got, want)
	}
}

func TestRetryBackoffIsBounded(t *testing.T) {
	if retryBackoff(1) != time.Second || retryBackoff(3) != 4*time.Second {
		t.Fatal("unexpected early retry backoff")
	}
	if retryBackoff(100) != 256*time.Second {
		t.Fatalf("retry backoff was not capped: %s", retryBackoff(100))
	}
}

type fakeRepository struct {
	events    []store.OutboxEvent
	published []uuid.UUID
	failed    []failedEvent
}

type failedEvent struct {
	id      uuid.UUID
	retryAt time.Time
}

func (f *fakeRepository) ClaimOutbox(_ context.Context, _ store.ClaimOutboxInput) ([]store.OutboxEvent, error) {
	return f.events, nil
}

func (f *fakeRepository) MarkOutboxPublished(_ context.Context, id uuid.UUID, _ string, _ int64, _ time.Time) error {
	f.published = append(f.published, id)
	return nil
}

func (f *fakeRepository) MarkOutboxFailed(_ context.Context, id uuid.UUID, _ string, _ int64, _ string, retryAt time.Time) error {
	f.failed = append(f.failed, failedEvent{id: id, retryAt: retryAt})
	return nil
}

type fakePublisher struct{ failureID uuid.UUID }

func (f *fakePublisher) Publish(_ context.Context, event store.OutboxEvent) error {
	if event.ID == f.failureID {
		return errors.New("broker unavailable")
	}
	return nil
}
