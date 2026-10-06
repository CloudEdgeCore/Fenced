package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestClientStreamsEventsUntilTerminalResult proves the streaming
// extension round-trip: one connection delivers every event and the
// terminal frame carries the final result, so no Events/Result polling is
// needed.
func TestClientStreamsEventsUntilTerminalResult(t *testing.T) {
	client, _ := newTestClient(t, &testRuntime{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Start(ctx, startFixture("stream-ok")); err != nil {
		t.Fatal(err)
	}
	var sequences []int64
	result, err := client.StreamEvents(ctx, "stream-ok", 0, func(event Event) error {
		sequences = append(sequences, event.Sequence)
		if event.Type != "agent.started" {
			t.Fatalf("unexpected streamed event %q", event.Type)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusSucceeded || string(result.Output) != `{"answer":"done"}` {
		t.Fatalf("streamed result = %+v", result)
	}
	if len(sequences) != 1 || sequences[0] != 1 {
		t.Fatalf("streamed sequences = %v, want [1]", sequences)
	}
}

// TestClientStreamResumesFromCursor proves a dropped stream resumes from
// the `after` cursor without re-delivering earlier events.
func TestClientStreamResumesFromCursor(t *testing.T) {
	client, _ := newTestClient(t, &testRuntime{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Start(ctx, startFixture("stream-resume")); err != nil {
		t.Fatal(err)
	}
	// First connection consumes event 1 then is abandoned.
	resumeCtx, resumeCancel := context.WithCancel(ctx)
	var firstSequence int64
	_, _ = client.StreamEvents(resumeCtx, "stream-resume", 0, func(event Event) error {
		firstSequence = event.Sequence
		resumeCancel()
		return nil
	})
	if firstSequence != 1 {
		t.Fatalf("first connection cursor = %d, want 1", firstSequence)
	}
	// The resumed stream starting after the cursor returns only the
	// terminal frame.
	result, err := client.StreamEvents(ctx, "stream-resume", firstSequence, func(event Event) error {
		t.Fatalf("resumed stream re-delivered event %d", event.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusSucceeded {
		t.Fatalf("resumed result = %+v", result)
	}
}

// TestClientStreamDetectsV1OnlyRuntime proves a runtime without the
// streaming route is reported as unsupported so callers can fall back to
// the frozen polling endpoints.
func TestClientStreamDetectsV1OnlyRuntime(t *testing.T) {
	var mu sync.Mutex
	served := 0
	v1Only := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		served++
		mu.Unlock()
		writer.Header().Set("Fenced-Runtime-Interface", ProtocolVersion)
		writeProblem(writer, http.StatusNotFound, "ROUTE_NOT_FOUND", "runtime execution route not found")
	}))
	defer v1Only.Close()
	client, err := NewClient(v1Only.URL, v1Only.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.StreamEvents(context.Background(), "legacy", 0, nil)
	if !errors.Is(err, ErrStreamingUnsupported) {
		t.Fatalf("v1-only stream error = %v, want ErrStreamingUnsupported", err)
	}
}

// TestHostStreamWritesSSEFrames pins the server wire format for
// non-Go clients.
func TestHostStreamWritesSSEFrames(t *testing.T) {
	host, err := NewHost(&testRuntime{}, HostOptions{Adapter: "go-native"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Start(context.Background(), startFixture("wire")); err != nil {
		t.Fatal(err)
	}
	reply, err := server.Client().Get(server.URL + "/v1/executions/wire/events/stream?after=0")
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Body.Close()
	if reply.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream content type = %q", reply.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(io.LimitReader(reply.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	wire := string(body)
	for _, want := range []string{
		"event: event\n", "event: result\n", `"type":"agent.started"`, `"status":"SUCCEEDED"`,
	} {
		if !containsString(wire, want) {
			t.Fatalf("stream wire missing %q:\n%s", want, wire)
		}
	}
}

func containsString(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// chattyRuntime emits a large event history (far beyond any socket buffer)
// before succeeding, so a stalled SSE reader parks its server-side write.
type chattyRuntime struct{}

func (chattyRuntime) Run(_ context.Context, _ StartRequest, emit Emitter) (json.RawMessage, error) {
	payload := json.RawMessage(`{"padding":"` + strings.Repeat("x", 4<<10) + `"}`)
	for i := 0; i < 512; i++ {
		if err := emit("agent.progress", payload); err != nil {
			return nil, err
		}
	}
	return json.RawMessage(`{"answer":"done"}`), nil
}

func (chattyRuntime) Checkpoint(context.Context, string) (Checkpoint, error) {
	return Checkpoint{}, nil
}

func (chattyRuntime) Restore(context.Context, RestoreRequest) error { return nil }

// TestHostSlowStreamClientDoesNotStallHostWrites pins the P1-01 fix: the
// SSE handler snapshots under the read lock and writes outside it, so one
// client that stops reading cannot block the emit/complete write lock every
// other execution needs.
func TestHostSlowStreamClientDoesNotStallHostWrites(t *testing.T) {
	host, err := NewHost(chattyRuntime{}, HostOptions{Adapter: "go-native", MaxConcurrent: 64, EventLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Start(context.Background(), startFixture("chatty")); err != nil {
		t.Fatal(err)
	}
	// A raw connection that reads the response headers and then stalls.
	stalled, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	if _, err := stalled.Write([]byte("GET /v1/executions/chatty/events/stream?after=0 HTTP/1.1\r\nHost: fenced\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 512)
	if _, err := stalled.Read(header); err != nil {
		t.Fatal(err)
	}
	// Give the stalled stream time to fill its socket buffers and park the
	// server-side write under the pre-fix lock discipline.
	time.Sleep(500 * time.Millisecond)

	deadline := time.Now().Add(15 * time.Second)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("healthy-%d", i)
		if _, err := client.Start(context.Background(), startFixture(id)); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline))
		result, err := client.WaitResult(ctx, id, time.Millisecond)
		cancel()
		if err != nil {
			t.Fatalf("execution %s did not complete under a stalled stream peer: %v", id, err)
		}
		if result.Status != StatusSucceeded {
			t.Fatalf("execution %s status = %s", id, result.Status)
		}
	}
}
