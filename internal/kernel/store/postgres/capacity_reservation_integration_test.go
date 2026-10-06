//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestAtomicCapacityReservationOneSlot makes 100 independent scheduler owners
// race for a one-slot pool. Exactly one placement may commit; all losing
// Task/Run/Attempt/Lease writes must roll back with the capacity reservation.
func TestAtomicCapacityReservationOneSlot(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	// The capacity ledger is operator-owned: registering the pool seeds the
	// authoritative total capacity row that every reservation serializes on.
	if err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{{
		ID: "one-slot", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-one", Region: "cn-east", Ready: true, Status: "ACTIVE",
		AvailableCPU: 100, AvailableMemory: 128, AvailableLLMSlots: 1,
	}}); err != nil {
		t.Fatalf("register one-slot pool: %v", err)
	}

	const schedulers = 100
	claims := make([]kernelstore.TaskClaim, 0, schedulers)
	for i := 0; i < schedulers; i++ {
		_ = createAdmittedTask(t, ctx, repository, fmt.Sprintf("capacity-race-%03d", i))
		owner := fmt.Sprintf("scheduler-%03d", i)
		claimed, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
			Kind: kernelstore.ControllerScheduling, Phase: domain.TaskAdmitted,
			OwnerID: owner, Limit: 1, TTL: time.Minute,
		})
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim %d: count=%d err=%v", i, len(claimed), err)
		}
		claims = append(claims, claimed[0])
	}

	start := make(chan struct{})
	var successes, exhausted, unexpected atomic.Int64
	var wg sync.WaitGroup
	for _, claim := range claims {
		claim := claim
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := repository.ScheduleTask(ctx, kernelstore.ScheduleTaskInput{
				TaskID: claim.Task.ID, TenantID: claim.Task.TenantID, OwnerID: claim.OwnerID,
				ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: claim.Task.ResourceVersion,
				RunID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
				RuntimePoolID: "one-slot", RuntimeClass: "oci", RuntimeInstanceID: "worker-one",
				LeaseTTL:     time.Minute,
				RequestedCPU: 100, RequestedMemory: 128, RequestedLLMSlots: 1,
			})
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, kernelstore.ErrCapacityExhausted):
				exhausted.Add(1)
			default:
				unexpected.Add(1)
				t.Errorf("unexpected schedule error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if successes.Load() != 1 || exhausted.Load() != schedulers-1 || unexpected.Load() != 0 {
		t.Fatalf("success=%d exhausted=%d unexpected=%d", successes.Load(), exhausted.Load(), unexpected.Load())
	}
	var reserved, active, running int
	if err := pool.QueryRow(ctx, `SELECT reserved_llm_slots FROM runtime_pool_capacities WHERE pool_id = 'one-slot'`).Scan(&reserved); err != nil {
		t.Fatalf("read pool reservation: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM runtime_capacity_reservations WHERE pool_id = 'one-slot' AND status = 'ACTIVE'`).Scan(&active); err != nil {
		t.Fatalf("read active reservations: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE tenant_id = 'tenant-a' AND phase = 'RUNNING'`).Scan(&running); err != nil {
		t.Fatalf("read running tasks: %v", err)
	}
	if reserved != 1 || active != 1 || running != 1 {
		t.Fatalf("reserved=%d active=%d running=%d, want exactly one", reserved, active, running)
	}
}
