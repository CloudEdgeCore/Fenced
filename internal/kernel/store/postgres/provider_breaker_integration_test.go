//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
)

func TestProviderCircuitIsSharedAndHalfOpenProbeIsExclusive(t *testing.T) {
	clock := newFakeClock()
	pool, first := prepare(t, clock.Now)
	second := postgresstore.NewWithClock(pool, clock.Now)
	ctx := context.Background()
	acquire := provider.CircuitAcquire{
		Provider: "deepseek", Threshold: 2, Cooldown: time.Minute,
		ProbeTTL: 30 * time.Second, Now: clock.Now(),
	}
	if _, err := first.AcquireCircuit(ctx, acquire); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if err := first.RecordCircuit(ctx, provider.CircuitRecord{
			Provider: acquire.Provider, Retryable: true, Threshold: acquire.Threshold, Now: clock.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := second.AcquireCircuit(ctx, acquire); !errors.Is(err, provider.ErrCircuitOpen) {
		t.Fatalf("peer did not observe open circuit: %v", err)
	}
	clock.Advance(time.Minute)
	acquire.Now = clock.Now()
	const contenders = 12
	start := make(chan struct{})
	var wait sync.WaitGroup
	var mu sync.Mutex
	permits := 0
	for index := 0; index < contenders; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			permit, err := second.AcquireCircuit(ctx, acquire)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && permit.ProbeToken != "" {
				permits++
			} else if !errors.Is(err, provider.ErrCircuitOpen) {
				t.Errorf("half-open acquire: permit=%+v err=%v", permit, err)
			}
		}()
	}
	close(start)
	wait.Wait()
	if permits != 1 {
		t.Fatalf("half-open permits=%d, want 1", permits)
	}
}
