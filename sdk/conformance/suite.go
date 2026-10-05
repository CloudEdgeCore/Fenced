// Package conformance provides the black-box certification suite every Agent
// Runtime Interface adapter must pass, independent of language or framework.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type Report struct {
	Adapter  string   `json:"adapter"`
	Protocol string   `json:"protocol"`
	Checks   []string `json:"checks"`
}

// Run exercises protocol negotiation, idempotent start, conflict rejection,
// events, result, checkpoint, restore and stop through the public client only.
func Run(ctx context.Context, client *agent.Client) (Report, error) {
	if client == nil {
		return Report{}, errors.New("runtime client is required")
	}
	report := Report{}
	health, err := client.Health(ctx)
	if err != nil {
		return report, fmt.Errorf("health: %w", err)
	}
	if health.Status != "SERVING" || len(health.ProtocolVersions) != 1 ||
		(health.ProtocolVersions[0] != agent.ProtocolVersion && health.ProtocolVersions[0] != agent.LegacyProtocolVersion) {
		return report, fmt.Errorf("health negotiation is non-conformant: %+v", health)
	}
	report.Adapter = health.Adapter
	report.Protocol = health.ProtocolVersions[0]
	report.Checks = append(report.Checks, "health", "protocol-negotiation")

	request := fixture("conformance-main")
	started, err := client.Start(ctx, request)
	if err != nil || started.ExecutionID != request.ExecutionID {
		return report, fmt.Errorf("start: response=%+v error=%w", started, err)
	}
	replayed, err := client.Start(ctx, request)
	if err != nil || !replayed.Replayed {
		return report, fmt.Errorf("idempotent start: response=%+v error=%w", replayed, err)
	}
	conflict := request
	conflict.Goal = "different goal"
	if _, err := client.Start(ctx, conflict); !hasStatus(err, http.StatusConflict) {
		return report, fmt.Errorf("conflicting start must return 409, got %v", err)
	}
	report.Checks = append(report.Checks, "start", "idempotency", "conflict")

	result, err := client.WaitResult(ctx, request.ExecutionID, 10*time.Millisecond)
	if err != nil || result.Status != agent.StatusSucceeded || !json.Valid(result.Output) {
		return report, fmt.Errorf("result: response=%+v error=%w", result, err)
	}
	events, err := client.Events(ctx, request.ExecutionID, 0)
	if err != nil || len(events.Events) == 0 || events.NextAfter == 0 {
		return report, fmt.Errorf("events: response=%+v error=%w", events, err)
	}
	empty, err := client.Events(ctx, request.ExecutionID, events.NextAfter)
	if err != nil || len(empty.Events) != 0 {
		return report, fmt.Errorf("event cursor: response=%+v error=%w", empty, err)
	}
	report.Checks = append(report.Checks, "event", "event-cursor", "result")

	checkpoint, err := client.Checkpoint(ctx, request.ExecutionID)
	if err != nil || checkpoint.Checkpoint.SchemaVersion == "" || !json.Valid(checkpoint.Checkpoint.State) {
		return report, fmt.Errorf("checkpoint: response=%+v error=%w", checkpoint, err)
	}
	restore := agent.RestoreRequest{ExecutionID: "conformance-restored", Checkpoint: checkpoint.Checkpoint}
	restored, err := client.Restore(ctx, restore)
	if err != nil || !restored.Restored {
		return report, fmt.Errorf("restore: response=%+v error=%w", restored, err)
	}
	report.Checks = append(report.Checks, "checkpoint", "restore")

	stopRequest := fixture("conformance-stop")
	stopRequest.Input = json.RawMessage("{\"blockUntilStopped\":true}")
	if _, err := client.Start(ctx, stopRequest); err != nil {
		return report, fmt.Errorf("start stoppable execution: %w", err)
	}
	if _, err := client.Stop(ctx, stopRequest.ExecutionID); err != nil {
		return report, fmt.Errorf("stop: %w", err)
	}
	stopped, err := client.WaitResult(ctx, stopRequest.ExecutionID, 10*time.Millisecond)
	if err != nil || (stopped.Status != agent.StatusCancelled && stopped.Status != agent.StatusSucceeded) {
		return report, fmt.Errorf("stop terminal result: response=%+v error=%w", stopped, err)
	}
	report.Checks = append(report.Checks, "stop")

	invalid := fixture("conformance-implicit-capability")
	invalid.Capabilities.Secrets = nil
	if _, err := client.Start(ctx, invalid); !hasStatus(err, http.StatusUnprocessableEntity) {
		return report, fmt.Errorf("implicit capability default must return 422, got %v", err)
	}
	report.Checks = append(report.Checks, "default-deny-capabilities")
	return report, nil
}

func fixture(id string) agent.StartRequest {
	return agent.StartRequest{
		ExecutionID: id, AgentVersionRef: "conformance-agent@1.0.0", Goal: "prove adapter conformance",
		Input: json.RawMessage("{\"message\":\"hello\"}"),
		Capabilities: agent.CapabilityGrant{
			Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{},
		},
	}
}

func hasStatus(err error, status int) bool {
	httpError := new(agent.HTTPError)
	return errors.As(err, &httpError) && httpError.Status == status
}
