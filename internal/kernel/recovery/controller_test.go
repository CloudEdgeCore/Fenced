package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type fakeRepository struct {
	candidates []store.RecoveryCandidate
	inputs     []store.RecoverExpiredAttemptInput
	err        error
}

func (f *fakeRepository) ListExpiredAttempts(context.Context, time.Time, int) ([]store.RecoveryCandidate, error) {
	return f.candidates, nil
}

func (f *fakeRepository) RecoverExpiredAttempt(_ context.Context, input store.RecoverExpiredAttemptInput) (store.RecoveryResult, error) {
	f.inputs = append(f.inputs, input)
	return store.RecoveryResult{}, f.err
}

func TestControllerUsesTaskRetryPolicy(t *testing.T) {
	repository := &fakeRepository{candidates: []store.RecoveryCandidate{{
		TenantID: "tenant-a", AttemptID: uuid.New(), FencingToken: 4,
		TaskSpec: json.RawMessage(`{"retryPolicy":{"maxAttempts":5}}`),
	}}}
	controller := NewController(repository, 10, 30*time.Second)
	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	if len(repository.inputs) != 1 || repository.inputs[0].MaxAttempts != 5 || repository.inputs[0].FencingToken != 4 {
		t.Fatalf("unexpected recovery input: %+v", repository.inputs)
	}
}

func TestControllerIgnoresLostRecoveryRace(t *testing.T) {
	repository := &fakeRepository{
		candidates: []store.RecoveryCandidate{{TenantID: "tenant-a", AttemptID: uuid.New(), FencingToken: 1, TaskSpec: json.RawMessage(`{}`)}},
		err:        store.ErrFenced,
	}
	processed, err := NewController(repository, 10, time.Second).Reconcile(context.Background())
	if err != nil || processed != 0 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	if !errors.Is(repository.err, store.ErrFenced) {
		t.Fatal("test repository lost sentinel")
	}
}

// TestControllerIsolatesPoisonedCandidate proves per-task error isolation: an
// expired attempt whose task spec cannot be decoded must not block the
// recovery of other attempts.
func TestControllerIsolatesPoisonedCandidate(t *testing.T) {
	healthy := store.RecoveryCandidate{TenantID: "tenant-a", AttemptID: uuid.New(), FencingToken: 1, TaskSpec: json.RawMessage(`{"retryPolicy":{"maxAttempts":3}}`)}
	poisoned := store.RecoveryCandidate{TenantID: "tenant-a", AttemptID: uuid.New(), FencingToken: 2, TaskSpec: json.RawMessage(`not-json`)}
	repository := &fakeRepository{candidates: []store.RecoveryCandidate{poisoned, healthy}}
	processed, err := NewController(repository, 10, time.Second).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil (poisoned candidate must be isolated)", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1 (only the healthy candidate)", processed)
	}
	if len(repository.inputs) != 1 || repository.inputs[0].AttemptID != healthy.AttemptID {
		t.Fatalf("recovery inputs = %+v, want only the healthy attempt", repository.inputs)
	}
}
