package reference

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/google/uuid"
)

func TestIdentitySlotPublishesAndClearsFencedIdentity(t *testing.T) {
	slot := NewIdentitySlot()
	ctx := mcp.AttemptContext{
		TenantID: "tenant-a", TaskID: uuid.New(), RunID: uuid.New(), AttemptID: uuid.New(),
		FencingToken: 7, AgentVersionRef: "agent@1",
	}

	if _, err := slot.Resolve(context.Background()); !errors.Is(err, ErrNoActiveAttempt) {
		t.Fatalf("empty slot: %v, want ErrNoActiveAttempt", err)
	}
	slot.Set(ctx)
	resolved, err := slot.Resolve(context.Background())
	if err != nil || resolved.TenantID != ctx.TenantID || resolved.AttemptID != ctx.AttemptID ||
		resolved.FencingToken != ctx.FencingToken || len(resolved.AllowedModels) != 0 {
		t.Fatalf("resolved = %+v err=%v, want the published context", resolved, err)
	}
	slot.Clear()
	if _, err := slot.Resolve(context.Background()); !errors.Is(err, ErrNoActiveAttempt) {
		t.Fatalf("cleared slot: %v, want ErrNoActiveAttempt", err)
	}
}

func TestIdentitySlotIsConcurrentSafe(t *testing.T) {
	slot := NewIdentitySlot()
	ctx := mcp.AttemptContext{TenantID: "tenant-a", AttemptID: uuid.New(), FencingToken: 1, AgentVersionRef: "agent@1"}
	slot.Set(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := slot.Resolve(context.Background()); err != nil {
				t.Errorf("Resolve during publish: %v", err)
			}
		}()
	}
	wg.Wait()
}
