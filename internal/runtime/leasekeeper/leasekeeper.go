// Package leasekeeper keeps a Runtime Protocol lease alive while a worker
// executes a workload, and stops the execution when the lease can no longer
// be renewed.
//
// Without periodic renewal, a workload running longer than the lease TTL
// (e.g. a sandboxed container or a slow tool script) lets the lease expire:
// the recovery controller fences the run and starts a replacement attempt
// while the original worker is still executing side effects — a fencing
// split-brain. The keeper renews the lease at TTL/3 intervals with the CAS
// lease version from each response, so the renewal chain never goes stale,
// and it cancels the execution context as soon as renewal fails (fence
// broken) or the kernel requests cancellation (acknowledged once, so the
// kernel can finalize it).
package leasekeeper

import (
	"context"
	"fmt"
	"sync"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
)

// Options configures one keeper run.
type Options struct {
	// Client is the fenced Runtime Protocol client.
	Client runtimev1.RuntimeControlServiceClient
	// Identity is the fenced AttemptIdentity of the assignment.
	Identity *runtimev1.AttemptIdentity
	// AttemptID is the attempt ID string used to derive idempotency keys.
	AttemptID string
	// HeartbeatTTL is the lease TTL the worker requests on every renewal.
	HeartbeatTTL time.Duration
	// RPCTimeout bounds each individual renewal RPC.
	RPCTimeout time.Duration
}

// Keeper renews the lease on a background goroutine. The execution context
// returned by Start is cancelled when the lease cannot be renewed or when the
// kernel requests cancellation; workers must treat a cancelled execution
// context as "stop now".
type Keeper struct {
	opts Options

	mu             sync.Mutex
	leaseVersion   int64
	attemptVersion int64
	cancelled      bool
	fenceErr       error

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// Start begins renewing the lease and returns the keeper plus the execution
// context derived from parent. initialLeaseVersion is the lease resource
// version the worker most recently observed (e.g. from its pre-execution
// heartbeat); the keeper CAS-chains every renewal on the version each
// heartbeat returns.
func Start(parent context.Context, opts Options, initialLeaseVersion, initialAttemptVersion int64) (*Keeper, context.Context) {
	execCtx, cancelExec := context.WithCancel(parent)
	k := &Keeper{
		opts:           opts,
		leaseVersion:   initialLeaseVersion,
		attemptVersion: initialAttemptVersion,
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	go k.loop(parent, cancelExec)
	return k, execCtx
}

// Stop halts the renewal loop and waits for it to exit.
func (k *Keeper) Stop() {
	k.once.Do(func() { close(k.stop) })
	<-k.done
}

// Cancelled reports whether the kernel requested cancellation while the
// keeper was running (the cancellation was acknowledged).
func (k *Keeper) Cancelled() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.cancelled
}

// FenceError reports why the lease could not be renewed, if it failed.
func (k *Keeper) FenceError() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.fenceErr
}

func (k *Keeper) loop(ctx context.Context, cancelExec context.CancelFunc) {
	defer close(k.done)
	ticker := time.NewTicker(k.opts.HeartbeatTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-k.stop:
			return
		case <-ticker.C:
			if !k.renew(ctx, cancelExec) {
				return
			}
		}
	}
}

// renew performs one lease renewal. It reports whether the keeper should
// continue running.
func (k *Keeper) renew(ctx context.Context, cancelExec context.CancelFunc) bool {
	k.mu.Lock()
	leaseVersion := k.leaseVersion
	k.mu.Unlock()

	rpcCtx, cancel := context.WithTimeout(ctx, k.opts.RPCTimeout)
	response, err := k.opts.Client.Heartbeat(rpcCtx, &runtimev1.HeartbeatRequest{
		Identity: k.opts.Identity, ExpectedLeaseVersion: leaseVersion,
		IdempotencyKey:      k.opts.AttemptID + ":lease-renew",
		RequestedTtlSeconds: int64(k.opts.HeartbeatTTL / time.Second),
	})
	cancel()
	if err != nil {
		k.mu.Lock()
		k.fenceErr = fmt.Errorf("renew runtime lease: %w", err)
		k.mu.Unlock()
		cancelExec()
		return false
	}
	if response.GetCancelRequested() {
		k.mu.Lock()
		k.cancelled = true
		k.mu.Unlock()
		// Acknowledge once so the kernel can finalize the cancellation; the
		// same idempotency key the worker uses for its own acks makes a
		// concurrent ack converge.
		ackCtx, ackCancel := context.WithTimeout(ctx, k.opts.RPCTimeout)
		_, ackErr := k.opts.Client.AcknowledgeCancellation(ackCtx, &runtimev1.AcknowledgeCancellationRequest{
			Identity: k.opts.Identity, ExpectedAttemptVersion: response.GetAttemptVersion(),
			IdempotencyKey: k.opts.AttemptID + ":cancel",
		})
		ackCancel()
		if ackErr != nil {
			k.mu.Lock()
			k.fenceErr = fmt.Errorf("acknowledge runtime cancellation: %w", ackErr)
			k.mu.Unlock()
		}
		cancelExec()
		return false
	}
	k.mu.Lock()
	k.leaseVersion = response.GetLeaseVersion()
	k.attemptVersion = response.GetAttemptVersion()
	k.mu.Unlock()
	return true
}
