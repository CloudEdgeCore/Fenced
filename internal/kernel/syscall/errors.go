package syscall

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// SyscallErrorCode represents a POSIX-like kernel error number returned by system calls.
type SyscallErrorCode int32

const (
	// SyscallOK indicates normal successful completion.
	SyscallOK SyscallErrorCode = 0

	// SyscallEPERM indicates operation not permitted (capability denied, tenant mismatch).
	SyscallEPERM SyscallErrorCode = 1

	// SyscallENOENT indicates no such file, directory, entity, or tool.
	SyscallENOENT SyscallErrorCode = 2

	// SyscallEINVAL indicates invalid argument, malformed payload, or missing fields.
	SyscallEINVAL SyscallErrorCode = 3

	// SyscallEBUSY indicates resource or attempt is currently locked or in a conflicting state.
	SyscallEBUSY SyscallErrorCode = 4

	// SyscallETIMEDOUT indicates connection or operation deadline exceeded.
	SyscallETIMEDOUT SyscallErrorCode = 5

	// SyscallENOMEM indicates memory, token quota, or financial budget exceeded.
	SyscallENOMEM SyscallErrorCode = 6

	// SyscallEFENCE indicates stale fencing token or invalid attempt lease.
	SyscallEFENCE SyscallErrorCode = 7

	// SyscallENOSYS indicates function/system call not implemented or unknown.
	SyscallENOSYS SyscallErrorCode = 8

	// SyscallEINTERNAL indicates an unexpected internal kernel error.
	SyscallEINTERNAL SyscallErrorCode = 9

	// SyscallEUNKNOWN indicates an ambiguous external side-effect outcome; auto-replay forbidden.
	SyscallEUNKNOWN SyscallErrorCode = 10
)

// String returns a human-readable representation of the error code.
func (c SyscallErrorCode) String() string {
	switch c {
	case SyscallOK:
		return "SUCCESS"
	case SyscallEPERM:
		return "EPERM (Operation not permitted)"
	case SyscallENOENT:
		return "ENOENT (No such entity)"
	case SyscallEINVAL:
		return "EINVAL (Invalid argument)"
	case SyscallEBUSY:
		return "EBUSY (Resource busy)"
	case SyscallETIMEDOUT:
		return "ETIMEDOUT (Connection timed out)"
	case SyscallENOMEM:
		return "ENOMEM (Quota/Budget exceeded)"
	case SyscallEFENCE:
		return "EFENCE (Stale fencing token)"
	case SyscallENOSYS:
		return "ENOSYS (Function not implemented)"
	case SyscallEINTERNAL:
		return "EINTERNAL (Internal error)"
	case SyscallEUNKNOWN:
		return "EUNKNOWN (Ambiguous external effect outcome)"
	default:
		return fmt.Sprintf("UNKNOWN_ERR(%d)", int32(c))
	}
}

// StandardName returns the short POSIX-style name (e.g. "EPERM", "EINVAL").
func (c SyscallErrorCode) StandardName() string {
	switch c {
	case SyscallOK:
		return "OK"
	case SyscallEPERM:
		return "EPERM"
	case SyscallENOENT:
		return "ENOENT"
	case SyscallEINVAL:
		return "EINVAL"
	case SyscallEBUSY:
		return "EBUSY"
	case SyscallETIMEDOUT:
		return "ETIMEDOUT"
	case SyscallENOMEM:
		return "ENOMEM"
	case SyscallEFENCE:
		return "EFENCE"
	case SyscallENOSYS:
		return "ENOSYS"
	case SyscallEINTERNAL:
		return "EINTERNAL"
	case SyscallEUNKNOWN:
		return "EUNKNOWN"
	default:
		return "EUNKNOWN"
	}
}

// SyscallError represents a strongly-typed kernel error encountered during a system call.
type SyscallError struct {
	Code    SyscallErrorCode
	Message string
	Cause   error
}

// Error implements the standard error interface.
func (e *SyscallError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code.StandardName(), e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code.StandardName(), e.Message)
}

// Unwrap returns the underlying cause of the error.
func (e *SyscallError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewSyscallError creates a new SyscallError with code and formatted message.
func NewSyscallError(code SyscallErrorCode, msg string) *SyscallError {
	return &SyscallError{
		Code:    code,
		Message: msg,
	}
}

// WrapError wraps an existing error with a SyscallErrorCode.
func WrapError(code SyscallErrorCode, msg string, cause error) *SyscallError {
	return &SyscallError{
		Code:    code,
		Message: msg,
		Cause:   cause,
	}
}

// ErrorToCode inspects an error and maps it to a canonical SyscallErrorCode and message.
func ErrorToCode(err error) (SyscallErrorCode, string) {
	if err == nil {
		return SyscallOK, ""
	}

	var sysErr *SyscallError
	if errors.As(err, &sysErr) {
		return sysErr.Code, sysErr.Message
	}

	if errors.Is(err, capability.ErrDenied) {
		return SyscallEPERM, err.Error()
	}

	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrNoAssignment) {
		return SyscallENOENT, err.Error()
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return SyscallETIMEDOUT, "operation deadline exceeded"
	}

	if errors.Is(err, context.Canceled) {
		return SyscallEINVAL, "operation canceled by caller"
	}

	errMsg := err.Error()
	lowerMsg := strings.ToLower(errMsg)

	if strings.Contains(lowerMsg, "fence") || strings.Contains(lowerMsg, "fencing") || strings.Contains(lowerMsg, "stale token") {
		return SyscallEFENCE, errMsg
	}

	if strings.Contains(lowerMsg, "permission") || strings.Contains(lowerMsg, "unauthorized") || strings.Contains(lowerMsg, "forbidden") {
		return SyscallEPERM, errMsg
	}

	if strings.Contains(lowerMsg, "invalid") || strings.Contains(lowerMsg, "malformed") || strings.Contains(lowerMsg, "required") {
		return SyscallEINVAL, errMsg
	}

	if strings.Contains(lowerMsg, "busy") || strings.Contains(lowerMsg, "conflict") || strings.Contains(lowerMsg, "concurrency") {
		return SyscallEBUSY, errMsg
	}

	if strings.Contains(lowerMsg, "budget") || strings.Contains(lowerMsg, "quota") || strings.Contains(lowerMsg, "limit exceeded") {
		return SyscallENOMEM, errMsg
	}

	if strings.Contains(lowerMsg, "not implemented") || strings.Contains(lowerMsg, "unsupported") {
		return SyscallENOSYS, errMsg
	}

	return SyscallEINTERNAL, errMsg
}
