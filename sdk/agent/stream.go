package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Streaming extension of the Runtime Interface: one long-lived
// text/event-stream connection replaces the Events/Result poll cycle for
// stream-capable runtimes. The v1 polling endpoints remain the frozen
// compatibility contract; the stream is additive and discovered by the
// client at runtime (a v1-only runtime answers 404 ROUTE_NOT_FOUND and the
// caller falls back to polling).
//
// Wire shape (server → client):
//
//	event: event
//	data: {"sequence":1,"type":"...","payload":{...},"occurredAt":"..."}
//
//	event: result
//	data: {"executionId":"...","status":"SUCCEEDED",...}
//
// Comment lines (": keepalive") may appear between frames. The cursor is
// the `after` query parameter — the sequence of the last event already
// consumed — so a dropped connection resumes without event loss and without
// replaying agent side effects (events are observations, never triggers).

const (
	streamKeepaliveEvery = 10 * time.Second
	// streamPollInterval is the host-internal state poll cadence while
	// parked on an open connection. It is not observable by the client.
	streamPollInterval = 50 * time.Millisecond
)

func (h *Host) eventStream(writer http.ResponseWriter, request *http.Request, executionID string) {
	if request.Method != http.MethodGet {
		writeProblem(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "GET is required")
		return
	}
	after := int64(0)
	if raw := request.URL.Query().Get("after"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			writeProblem(writer, http.StatusBadRequest, "INVALID_CURSOR", "after must be a non-negative integer")
			return
		}
		after = value
	}
	if !h.exists(executionID) {
		writeProblem(writer, http.StatusNotFound, "EXECUTION_NOT_FOUND", ErrExecutionNotFound.Error())
		return
	}
	controller := http.NewResponseController(writer)
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}
	ticker := time.NewTicker(streamPollInterval)
	defer ticker.Stop()
	keepalive := time.NewTicker(streamKeepaliveEvery)
	defer keepalive.Stop()
	cursor := after
	for {
		// Snapshot under the read lock, write outside it: a slow client
		// stalls only its own connection, never the emit/complete write
		// lock of the host (or of any other execution).
		var page []Event
		terminal := false
		var result Result
		h.mu.RLock()
		state := h.executions[executionID]
		if state != nil {
			for _, event := range state.events {
				if event.Sequence <= cursor {
					continue
				}
				if len(page) >= maxEventsPerPage {
					break
				}
				page = append(page, Event{
					Sequence: event.Sequence, Type: event.Type,
					Payload: clonePayload(event.Payload), OccurredAt: event.OccurredAt,
				})
			}
			terminal = state.status != StatusAccepted && state.status != StatusRunning
			result = state.result
		}
		h.mu.RUnlock()
		if state == nil {
			return
		}
		for _, event := range page {
			if err := writeSSEFrame(writer, "event", event); err != nil {
				return
			}
			cursor = event.Sequence
		}
		if err := controller.Flush(); err != nil {
			return
		}
		if terminal && len(page) == 0 {
			result.Output = clonePayload(result.Output)
			_ = writeSSEFrame(writer, "result", result)
			_ = controller.Flush()
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-keepalive.C:
			if _, err := fmt.Fprint(writer, ": keepalive\n\n"); err != nil {
				return
			}
			_ = controller.Flush()
		case <-ticker.C:
		}
	}
}

func writeSSEFrame(writer http.ResponseWriter, event string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event, encoded); err != nil {
		return err
	}
	return nil
}

func clonePayload(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	clone := make([]byte, len(payload))
	copy(clone, payload)
	return clone
}

// maxStreamFrame bounds one SSE data line (an event or the terminal
// result, each at most a page-sized JSON document plus framing).
const maxStreamFrame = maxEventPage + maxEventPayload

func newLineScanner(body io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), maxStreamFrame)
	return scanner
}

func newByteReader(payload []byte) *bytes.Reader { return bytes.NewReader(payload) }

func readBounded(body io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(body, limit))
}

// ErrStreamingUnsupported reports a runtime that implements only the frozen
// v1 polling endpoints. Callers fall back to Events/Result polling.
var ErrStreamingUnsupported = errors.New("runtime interface does not support event streaming")

// StreamEvents consumes one execution's event stream over a single
// long-lived connection. Events after the cursor are delivered to onEvent;
// the terminal frame carries the final Result so no separate Result poll
// is needed. The connection lifetime is bound to ctx.
func (c *Client) StreamEvents(ctx context.Context, executionID string, after int64, onEvent func(Event) error) (Result, error) {
	path := c.executionPath(executionID, "/events/stream") + "?after=" + strconv.FormatInt(after, 10)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return Result{}, err
	}
	request.Header.Set("Accept", "text/event-stream")
	// The shared client may carry a request-wide timeout that would sever
	// the stream; streaming uses the context alone for deadlines.
	streamClient := &http.Client{Transport: c.http.Transport}
	reply, err := streamClient.Do(request)
	if err != nil {
		return Result{}, err
	}
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		encoded, _ := readBounded(reply.Body, 4096)
		if reply.StatusCode == http.StatusNotFound || reply.StatusCode == http.StatusMethodNotAllowed {
			var problem struct {
				Code string `json:"code"`
			}
			if json.Unmarshal(encoded, &problem) == nil && problem.Code == "ROUTE_NOT_FOUND" {
				return Result{}, ErrStreamingUnsupported
			}
		}
		return Result{}, &HTTPError{Status: reply.StatusCode, Body: encoded}
	}
	if reply.Header.Get("Fenced-Runtime-Interface") != c.protocol {
		return Result{}, errors.New("runtime interface protocol negotiation failed")
	}
	return consumeEventStream(reply.Body, onEvent)
}

func consumeEventStream(body interface{ Read([]byte) (int, error) }, onEvent func(Event) error) (Result, error) {
	scanner := newLineScanner(body)
	eventName := ""
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "" || strings.HasPrefix(line, ":"):
			continue
		case strings.HasPrefix(line, "event: "):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
		case strings.HasPrefix(line, "data: "):
			payload := []byte(strings.TrimPrefix(line, "data: "))
			switch eventName {
			case "event":
				var event Event
				decoder := json.NewDecoder(newByteReader(payload))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&event); err != nil {
					return Result{}, fmt.Errorf("decode streamed event: %w", err)
				}
				event.Payload = clonePayload(event.Payload)
				if onEvent != nil {
					if err := onEvent(event); err != nil {
						return Result{}, err
					}
				}
			case "result":
				var result Result
				decoder := json.NewDecoder(newByteReader(payload))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&result); err != nil {
					return Result{}, fmt.Errorf("decode streamed result: %w", err)
				}
				result.Output = clonePayload(result.Output)
				return result, nil
			}
			eventName = ""
		}
	}
	if err := scanner.Err(); err != nil {
		return Result{}, err
	}
	return Result{}, errors.New("runtime event stream ended without a terminal result")
}
