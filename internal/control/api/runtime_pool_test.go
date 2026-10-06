package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

type fakeRuntimePoolOperator struct {
	input store.UpdateRuntimePoolStatusInput
}

func (f *fakeRuntimePoolOperator) UpdateRuntimePoolStatus(_ context.Context, in store.UpdateRuntimePoolStatusInput) (store.RuntimePoolState, error) {
	f.input = in
	return store.RuntimePoolState{ID: in.PoolID, Status: in.Status, ResourceVersion: in.ExpectedVersion + 1}, nil
}

func TestRuntimePoolCordonIsTenantScopedAndCASGuarded(t *testing.T) {
	backend := &fakeRuntimePoolOperator{}
	handler := auth.StaticMiddleware(auth.Principal{Subject: "operator", TenantID: "tenant-a"},
		NewHandler(nil, nil, nil, nil, WithRuntimePoolOperatorStore(backend)))
	request := httptest.NewRequest(http.MethodPut, "/v1/runtime-pools/pool-a/status", strings.NewReader(`{"status":"CORDONED"}`))
	request.Header.Set("If-Match", `W/"7"`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || backend.input.TenantID != "tenant-a" || backend.input.PoolID != "pool-a" || backend.input.ExpectedVersion != 7 {
		t.Fatalf("response=%d input=%+v body=%s", response.Code, backend.input, response.Body.String())
	}
	if backend.input.OperatorSubject != "operator" {
		t.Fatalf("operator subject = %q, want the authenticated principal subject", backend.input.OperatorSubject)
	}
	if response.Header().Get("ETag") != `W/"8"` {
		t.Fatalf("etag = %q", response.Header().Get("ETag"))
	}
}
