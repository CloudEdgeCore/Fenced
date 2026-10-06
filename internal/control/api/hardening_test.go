package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	controlapi "github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// blockingTaskStore parks GetTask so route budgets and stream slots can be
// exercised deterministically; everything else delegates to memoryStore.
type blockingTaskStore struct {
	*memoryStore
	blocked  bool
	entered  chan struct{}
	released chan struct{}
	task     store.Task
}

func (b *blockingTaskStore) GetTask(ctx context.Context, tenantID string, id uuid.UUID) (store.Task, error) {
	if b.blocked {
		b.entered <- struct{}{}
		select {
		case <-b.released:
		case <-ctx.Done():
			return store.Task{}, ctx.Err()
		}
	}
	return b.task, nil
}

func (b *blockingTaskStore) release() {
	b.blocked = false
	close(b.released)
}

func newBlockingTaskStore() *blockingTaskStore {
	return &blockingTaskStore{
		memoryStore: newMemoryStore(),
		blocked:     true,
		entered:     make(chan struct{}, 8),
		released:    make(chan struct{}),
		task:        store.Task{ID: uuid.New(), TenantID: "tenant-a", Phase: "QUEUED", ResourceVersion: 1},
	}
}

// TestRouteReadBudgetTimesOutSlowReads proves the per-route read budget (N9):
// a GET whose store call exceeds the budget answers 504 instead of hanging.
func TestRouteReadBudgetTimesOutSlowReads(t *testing.T) {
	backend := newBlockingTaskStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "owner", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend,
			controlapi.WithRequestTimeouts(50*time.Millisecond, time.Second)),
	)
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+backend.task.ID.String(), nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()
	<-backend.entered
	<-done
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 REQUEST_TIMEOUT: %s", response.Code, response.Body.String())
	}
	backend.release()
}

// TestSSECapacityBoundsConcurrentStreams proves the stream cap (O5): a second
// stream is rejected with 503 while the first occupies the only slot.
func TestSSECapacityBoundsConcurrentStreams(t *testing.T) {
	backend := newBlockingTaskStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "owner", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend, controlapi.WithMaxSSESubscribers(1)),
	)
	path := "/v1/tasks/" + backend.task.ID.String() + "/events"

	first := httptest.NewRecorder()
	go handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, path, nil))
	<-backend.entered

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, path, nil))
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("over-cap stream status = %d, want 503: %s", second.Code, second.Body.String())
	}
	backend.release()
}
