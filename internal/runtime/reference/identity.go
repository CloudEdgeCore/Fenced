package reference

import (
	"context"
	"errors"
	"sync"

	"github.com/CloudEdgeCore/Fenced/internal/mcp"
)

// ErrNoActiveAttempt reports an MCP call outside an assignment execution
// window: the worker holds no fenced identity to bind the call to.
var ErrNoActiveAttempt = errors.New("no active attempt context")

// IdentitySlot is the concurrent-safe fenced identity holder a runtime worker
// exposes to the sandboxed Agent's MCP endpoint. The worker sets the current
// Attempt context for the duration of one assignment execution window and
// clears it afterwards; MCP calls outside that window are denied (default
// deny — no identity, no tool).
type IdentitySlot struct {
	mu  sync.RWMutex
	ctx *mcp.AttemptContext
}

func NewIdentitySlot() *IdentitySlot {
	return &IdentitySlot{}
}

// Set publishes the fenced identity for the current execution window.
func (s *IdentitySlot) Set(ctx mcp.AttemptContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := ctx
	s.ctx = &copy
}

// Clear removes the identity at the end of the execution window.
func (s *IdentitySlot) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = nil
}

// Resolve implements mcp.IdentityResolver with default-deny semantics.
func (s *IdentitySlot) Resolve(context.Context) (mcp.AttemptContext, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ctx == nil {
		return mcp.AttemptContext{}, ErrNoActiveAttempt
	}
	return *s.ctx, nil
}
