// Per-attempt identity binding for shared Agent endpoints: a runtime worker
// opens one execution window per assignment and the brokered MCP tools
// resolve the fenced identity from the execution id the agent echoes in the
// X-Fenced-Execution header. Concurrent attempts through one endpoint stay
// correctly fenced, and calls outside any open window deny closed.
package mcp

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

// ErrNoExecutionContext reports an MCP call whose execution id has no open
// window (default deny).
var ErrNoExecutionContext = errors.New("no open execution window for the reported execution id")

// ExecutionRegistry binds attempt identities to open execution windows. The
// most recently opened window also serves header-less calls, preserving the
// single-assignment runtime behavior.
type ExecutionRegistry struct {
	mu                       sync.RWMutex
	allowHeaderlessExecution bool
	current                  *AttemptContext
	byAttempt                map[uuid.UUID]AttemptContext
}

// NewExecutionRegistry builds a production registry. Header-less execution
// lookup is disabled: every shared-endpoint request must report its execution
// id and resolve through ResolveExecution.
func NewExecutionRegistry() *ExecutionRegistry {
	return &ExecutionRegistry{byAttempt: map[uuid.UUID]AttemptContext{}}
}

// NewDevelopmentExecutionRegistry explicitly enables the single-execution
// convenience fallback. It must only be used by loopback development setups.
func NewDevelopmentExecutionRegistry() *ExecutionRegistry {
	return &ExecutionRegistry{allowHeaderlessExecution: true, byAttempt: map[uuid.UUID]AttemptContext{}}
}

// Open registers the fenced identity for one assignment and returns the
// closer that must run on every exit path of that assignment.
func (r *ExecutionRegistry) Open(identity AttemptContext) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byAttempt == nil {
		r.byAttempt = map[uuid.UUID]AttemptContext{}
	}
	r.byAttempt[identity.AttemptID] = identity
	copy := identity
	r.current = &copy
	return func() { r.close(identity.AttemptID) }
}

func (r *ExecutionRegistry) close(attemptID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byAttempt, attemptID)
	if r.current != nil && r.current.AttemptID == attemptID {
		r.current = nil
	}
}

// Resolve implements IdentityResolver for header-less callers: the most
// recently opened window, denying when none is open.
func (r *ExecutionRegistry) Resolve(context.Context) (AttemptContext, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.allowHeaderlessExecution || r.current == nil {
		return AttemptContext{}, ErrNoActiveWindow
	}
	return *r.current, nil
}

// ErrNoActiveWindow reports a header-less call with no open window.
var ErrNoActiveWindow = errors.New("no active execution window")

// ResolveExecution implements the header-scoped lookup.
func (r *ExecutionRegistry) ResolveExecution(_ context.Context, executionID string) (AttemptContext, error) {
	parsed, err := uuid.Parse(executionID)
	if err != nil {
		return AttemptContext{}, ErrNoExecutionContext
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	identity, ok := r.byAttempt[parsed]
	if !ok {
		return AttemptContext{}, ErrNoExecutionContext
	}
	return identity, nil
}
