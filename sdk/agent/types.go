// Package agent defines the provider-neutral Agent Runtime Interface used by
// native SDKs and framework adapters. It intentionally depends only on the Go
// standard library so third-party Agents do not import Kernel implementation
// packages.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	ProtocolVersion       = "fenced.runtime.interface/v1"
	LegacyProtocolVersion = "fenced.runtime.interface/v1alpha1"

	StatusAccepted  = "ACCEPTED"
	StatusRunning   = "RUNNING"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusCancelled = "CANCELLED"
)

var (
	ErrExecutionNotFound = errors.New("agent execution not found")
	ErrExecutionConflict = errors.New("agent execution id conflicts with another request")
	ErrCapacityExhausted = errors.New("agent runtime capacity exhausted")
)

// CapabilityGrant contains symbolic grants already admitted by the Kernel.
// Adapters use these identifiers to select a Gateway; they are never network
// endpoints, credentials, or raw secret values.
type CapabilityGrant struct {
	Tools               []string `json:"tools"`
	Models              []string `json:"models"`
	Memory              []string `json:"memory"`
	MemorySensitivities []string `json:"memorySensitivities,omitempty"`
	Secrets             []string `json:"secrets"`
	SpawnTasks          bool     `json:"spawnTasks,omitempty"`
	ChildAgents         []string `json:"childAgents,omitempty"`
	// Peers lists the AgentVersion references this agent may send IPC messages
	// to. An absent or empty list denies every peer.
	Peers []string `json:"peers,omitempty"`
}

type StartRequest struct {
	ExecutionID     string          `json:"executionId"`
	AgentVersionRef string          `json:"agentVersionRef"`
	Goal            string          `json:"goal"`
	Input           json.RawMessage `json:"input"`
	Capabilities    CapabilityGrant `json:"capabilities"`
}

type StartResponse struct {
	ExecutionID string `json:"executionId"`
	Status      string `json:"status"`
	Replayed    bool   `json:"replayed"`
}

type StopResponse struct {
	ExecutionID string `json:"executionId"`
	Status      string `json:"status"`
}

type Event struct {
	Sequence   int64           `json:"sequence"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt time.Time       `json:"occurredAt"`
}

type EventList struct {
	ExecutionID string  `json:"executionId"`
	Events      []Event `json:"events"`
	NextAfter   int64   `json:"nextAfter"`
	Truncated   bool    `json:"truncated,omitempty"`
}

type Result struct {
	ExecutionID string          `json:"executionId"`
	Status      string          `json:"status"`
	Output      json.RawMessage `json:"output,omitempty"`
	ErrorCode   string          `json:"errorCode,omitempty"`
	Error       string          `json:"error,omitempty"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
}

type Checkpoint struct {
	SchemaVersion string          `json:"schemaVersion"`
	State         json.RawMessage `json:"state"`
	CreatedAt     time.Time       `json:"createdAt"`
}

type CheckpointResponse struct {
	ExecutionID string     `json:"executionId"`
	Checkpoint  Checkpoint `json:"checkpoint"`
}

type RestoreRequest struct {
	ExecutionID string     `json:"executionId"`
	Checkpoint  Checkpoint `json:"checkpoint"`
}

type RestoreResponse struct {
	ExecutionID string `json:"executionId"`
	Restored    bool   `json:"restored"`
}

type HealthResponse struct {
	Status           string   `json:"status"`
	ProtocolVersions []string `json:"protocolVersions"`
	Adapter          string   `json:"adapter"`
	MaxConcurrent    int      `json:"maxConcurrent"`
	ActiveExecutions int      `json:"activeExecutions"`
}

// Emitter appends a bounded execution event. Payload must be valid JSON.
type Emitter func(eventType string, payload json.RawMessage) error

// Runtime is implemented by Native SDKs and framework adapters. Run owns one
// execution until a terminal output or error. Stop is represented by context
// cancellation. Checkpoint and Restore must use versioned logical state.
type Runtime interface {
	Run(ctx context.Context, request StartRequest, emit Emitter) (json.RawMessage, error)
	Checkpoint(ctx context.Context, executionID string) (Checkpoint, error)
	Restore(ctx context.Context, request RestoreRequest) error
}
