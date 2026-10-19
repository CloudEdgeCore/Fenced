// The execution wait of the adapter worker: how an assignment observes the
// Agent endpoint until the execution ends, over a stream or by polling.
package adapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	agent "github.com/CloudEdgeCore/Fenced/sdk/agent"
)

func (w *Worker) wait(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error) (agent.Result, []agent.Event, error) {
	streamed := w.waitStreaming(ctx, client, executionID, checkpointInterval, checkpoint)
	if !streamed.unsupported {
		return streamed.result, streamed.events, streamed.err
	}
	return w.waitPolling(ctx, client, executionID, checkpointInterval, checkpoint, streamed.events, streamed.after)
}

type streamOutcome struct {
	result      agent.Result
	events      []agent.Event
	after       int64
	unsupported bool
	err         error
}

// streamReconnects bounds mid-flight stream reconnects before the worker
// degrades to polling; streamReconnectBase/Max bound the backoff between
// attempts.
const (
	streamReconnects    = 3
	streamReconnectBase = 250 * time.Millisecond
	streamReconnectMax  = 2 * time.Second
)

func (w *Worker) waitStreaming(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error) streamOutcome {
	var events []agent.Event
	var after int64
	for attempt := 0; ; attempt++ {
		attempted := w.streamOnce(ctx, client, executionID, checkpointInterval, checkpoint, &events, &after)
		if !attempted.retry {
			return attempted.outcome
		}
		if attempt >= streamReconnects {
			// The stream keeps dropping: hand the cursor and the events
			// collected so far to the polling fallback.
			return streamOutcome{events: events, after: after, unsupported: true}
		}
		delay := streamReconnectBase << attempt
		if delay > streamReconnectMax {
			delay = streamReconnectMax
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return streamOutcome{events: events, after: after, err: ctx.Err()}
		case <-timer.C:
		}
	}
}

// streamAttempt is the outcome of one stream connection attempt; retry says
// whether the worker may reconnect from the cursor.
type streamAttempt struct {
	outcome streamOutcome
	retry   bool
}

// streamOnce holds one stream connection to its terminal frame. A dropped
// connection is retryable from the cursor; the events already collected and
// their last sequence carry across attempts.
func (w *Worker) streamOnce(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error, events *[]agent.Event, after *int64) streamAttempt {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan streamAttempt, 1)
	go func() {
		result, err := client.StreamEvents(streamCtx, executionID, *after, func(event agent.Event) error {
			if len(*events) >= maxCollectedEvents {
				return fmt.Errorf("runtime event history exceeds %d events", maxCollectedEvents)
			}
			*events = append(*events, event)
			*after = event.Sequence
			return nil
		})
		switch {
		case errors.Is(err, agent.ErrStreamingUnsupported):
			done <- streamAttempt{outcome: streamOutcome{events: *events, after: *after, unsupported: true}}
		case err != nil && ctx.Err() == nil:
			// A mid-flight disconnect resumes from the cursor without
			// re-delivering consumed events (they are observations, never
			// side-effect triggers, so a replay would be safe - but the
			// cursor keeps the history exact).
			done <- streamAttempt{outcome: streamOutcome{events: *events, after: *after, err: err}, retry: true}
		default:
			done <- streamAttempt{outcome: streamOutcome{result: result, events: *events, err: err}}
		}
	}()
	var checkpointTicker *time.Ticker
	var checkpointTick <-chan time.Time
	if checkpointInterval > 0 {
		checkpointTicker = time.NewTicker(checkpointInterval)
		checkpointTick = checkpointTicker.C
		defer checkpointTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return streamAttempt{outcome: streamOutcome{err: ctx.Err()}}
		case <-checkpointTick:
			if checkpoint == nil {
				return streamAttempt{outcome: streamOutcome{err: fmt.Errorf("periodic checkpoint callback is not configured")}}
			}
			if err := checkpoint(); err != nil {
				return streamAttempt{outcome: streamOutcome{err: fmt.Errorf("periodic checkpoint: %w", err)}}
			}
		case result := <-done:
			return result
		}
	}
}

func (w *Worker) waitPolling(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error, events []agent.Event, after int64) (agent.Result, []agent.Event, error) {
	pollDelay := w.pollInterval
	if pollDelay <= 0 {
		pollDelay = defaultPollInterval
	}
	pollTimer := time.NewTimer(0)
	defer pollTimer.Stop()
	var checkpointTicker *time.Ticker
	var checkpointTick <-chan time.Time
	if checkpointInterval > 0 {
		checkpointTicker = time.NewTicker(checkpointInterval)
		checkpointTick = checkpointTicker.C
		defer checkpointTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return agent.Result{}, nil, ctx.Err()
		case <-pollTimer.C:
		case <-checkpointTick:
			if checkpoint == nil {
				return agent.Result{}, nil, fmt.Errorf("periodic checkpoint callback is not configured")
			}
			if err := checkpoint(); err != nil {
				return agent.Result{}, nil, fmt.Errorf("periodic checkpoint: %w", err)
			}
			// Checkpoint cadence is independent from result polling. Resetting
			// the poll timer here lets a checkpoint interval shorter than the
			// adaptive poll delay postpone result observation forever.
			continue
		}
		list, err := client.Events(ctx, executionID, after)
		if err != nil {
			return agent.Result{}, nil, err
		}
		events = append(events, list.Events...)
		if len(events) > maxCollectedEvents {
			return agent.Result{}, nil, fmt.Errorf("runtime event history exceeds %d events", maxCollectedEvents)
		}
		after = list.NextAfter
		result, terminal, err := client.Result(ctx, executionID)
		if err != nil {
			return agent.Result{}, nil, err
		}
		if terminal {
			return result, events, nil
		}
		if len(list.Events) > 0 {
			pollDelay = w.pollInterval
			if pollDelay <= 0 {
				pollDelay = defaultPollInterval
			}
		} else if pollDelay < maxPollInterval {
			pollDelay *= 2
			if pollDelay > maxPollInterval {
				pollDelay = maxPollInterval
			}
		}
		pollTimer.Reset(pollDelay)
	}
}
