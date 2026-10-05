package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/syscall"
	"github.com/google/uuid"
)

func TestSyscallServiceHTTP(t *testing.T) {
	dispatcher := syscall.NewDispatcher()
	dispatcher.RegisterFunc(syscall.SysToolInvoke, func(ctx *syscall.SyscallContext) (json.RawMessage, int64, error) {
		return json.RawMessage(`{"outcome":"SUCCESS"}`), 77, nil
	})

	svc := NewSyscallService(dispatcher, "tenant-gateway")

	// 1. Valid Syscall Request
	reqPayload := syscall.SyscallRequest{
		Syscall: syscall.SysToolInvoke,
		Identity: syscall.AttemptIdentity{
			TenantID:     "tenant-gateway",
			AttemptID:    uuid.New(),
			FencingToken: 1,
		},
		PayloadJSON: json.RawMessage(`{"tool_name":"calc"}`),
	}
	body, _ := json.Marshal(reqPayload)

	req := httptest.NewRequest(http.MethodPost, "/v1/syscall", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	svc.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp syscall.SyscallResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ErrorCode != syscall.SyscallOK {
		t.Fatalf("expected SyscallOK, got %v (%s)", resp.ErrorCode, resp.ErrorMessage)
	}
	if resp.ResourceVersion != 77 {
		t.Fatalf("expected resource version 77, got %d", resp.ResourceVersion)
	}

	// 2. Tenant Mismatch
	reqPayload.Identity.TenantID = "tenant-other"
	body, _ = json.Marshal(reqPayload)

	req = httptest.NewRequest(http.MethodPost, "/v1/syscall", bytes.NewReader(body))
	rec = httptest.NewRecorder()

	svc.ServeHTTP(rec, req)

	var errResp syscall.SyscallResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &errResp)
	if errResp.ErrorCode != syscall.SyscallEPERM {
		t.Fatalf("expected SyscallEPERM on tenant mismatch, got %v", errResp.ErrorCode)
	}

	// 3. Method Not Allowed
	req = httptest.NewRequest(http.MethodGet, "/v1/syscall", nil)
	rec = httptest.NewRecorder()
	svc.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rec.Code)
	}
}
