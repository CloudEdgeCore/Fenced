package gateway

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/syscall"
)

// SyscallService exposes the unified Agent Syscall ABI over HTTP.
type SyscallService struct {
	dispatcher    *syscall.SyscallDispatcher
	allowedTenant string
}

// NewSyscallService creates a new SyscallService.
func NewSyscallService(dispatcher *syscall.SyscallDispatcher, allowedTenant string) *SyscallService {
	return &SyscallService{
		dispatcher:    dispatcher,
		allowedTenant: allowedTenant,
	}
}

// Dispatcher returns the underlying kernel SyscallDispatcher.
func (s *SyscallService) Dispatcher() *syscall.SyscallDispatcher {
	return s.dispatcher
}

// ServeHTTP handles HTTP POST requests for unified system call dispatching.
func (s *SyscallService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req syscall.SyscallRequest
	if err := json.Unmarshal(body, &req); err != nil {
		resp := syscall.SyscallResponse{
			Syscall:      req.Syscall,
			ErrorCode:    syscall.SyscallEINVAL,
			ErrorMessage: "invalid JSON syscall request: " + err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Verify tenant claim against gateway configuration / mTLS
	if err := authorizeTenant(r.Context(), s.allowedTenant, req.Identity.TenantID); err != nil {
		resp := syscall.SyscallResponse{
			Syscall:      req.Syscall,
			ErrorCode:    syscall.SyscallEPERM,
			ErrorMessage: err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	resp := s.dispatcher.Dispatch(r.Context(), req)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
