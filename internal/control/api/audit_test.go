package api_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	controlapi "github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// fakeAuditStore serves a canned tenant ledger.
type fakeAuditStore struct {
	events map[string][]store.AuditEvent
}

func (f *fakeAuditStore) ListAudit(_ context.Context, in store.ListAuditInput) ([]store.AuditEvent, error) {
	all := f.events[in.TenantID]
	var page []store.AuditEvent
	for _, event := range all {
		if event.Seq > in.AfterSeq {
			page = append(page, event)
			if len(page) >= in.Limit {
				break
			}
		}
	}
	return page, nil
}

func (f *fakeAuditStore) VerifyAuditChain(context.Context, string) (store.AuditVerification, error) {
	return store.AuditVerification{Valid: true, Checked: 2, HeadSeq: 2}, nil
}

func (f *fakeAuditStore) ExportAuditChain(_ context.Context, tenantID string) ([]store.AuditEvent, error) {
	return f.events[tenantID], nil
}

func auditEvent(seq int64, eventType string) store.AuditEvent {
	hash := sha256.Sum256([]byte(eventType))
	return store.AuditEvent{
		ID: uuid.New(), TenantID: "tenant-a", Seq: seq,
		PrevHash: hash, ChainHash: hash, EventType: eventType,
		ResourceType: "Task", ResourceID: uuid.New(), Actor: "kernel",
		Details:    json.RawMessage(`{"namespace":"default"}`),
		OccurredAt: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
	}
}

func newAuditHandler(t *testing.T, audit controlapi.AuditStore, signingKey ed25519.PrivateKey, keyID string) http.Handler {
	t.Helper()
	backend := newMemoryStore()
	var options []controlapi.Option
	options = append(options, controlapi.WithAuditStore(audit))
	if len(signingKey) == ed25519.PrivateKeySize {
		options = append(options, controlapi.WithAuditSigningKey(keyID, signingKey))
	}
	return auth.StaticMiddleware(
		auth.Principal{Subject: "owner", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend, options...),
	)
}

func getAudit(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestAuditListPagingAndTenantScoping(t *testing.T) {
	fake := &fakeAuditStore{events: map[string][]store.AuditEvent{
		"tenant-a": {auditEvent(1, "task.queued"), auditEvent(2, "task.transitioned"), auditEvent(3, "memory.upserted")},
		"tenant-b": {auditEvent(1, "task.queued")},
	}}
	handler := newAuditHandler(t, fake, nil, "")

	first := getAudit(t, handler, "/v1/audit?limit=2")
	if first.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", first.Code, first.Body.String())
	}
	var page map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	events, _ := page["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("page events = %d, want 2", len(events))
	}
	if page["nextAfterSeq"] != float64(2) {
		t.Fatalf("nextAfterSeq = %v, want 2", page["nextAfterSeq"])
	}
	firstEvent := events[0].(map[string]any)
	if firstEvent["eventType"] != "task.queued" || firstEvent["seq"] != float64(1) ||
		firstEvent["actor"] != "kernel" || firstEvent["prevHash"] == "" || firstEvent["chainHash"] == "" {
		t.Fatalf("event shape = %+v", firstEvent)
	}

	// Cursor continues after seq 2.
	second := getAudit(t, handler, "/v1/audit?limit=2&afterSeq=2")
	var page2 map[string]any
	if err := json.Unmarshal(second.Body.Bytes(), &page2); err != nil {
		t.Fatalf("decode page 2: %v", err)
	}
	events2, _ := page2["events"].([]any)
	if len(events2) != 1 || events2[0].(map[string]any)["seq"] != float64(3) {
		t.Fatalf("page 2 = %+v", page2)
	}
	// nextAfterSeq is the cursor for the following page.
	if page2["nextAfterSeq"] != float64(3) {
		t.Fatalf("nextAfterSeq = %v, want 3", page2["nextAfterSeq"])
	}

	// Invalid pagination is rejected.
	for _, path := range []string{"/v1/audit?limit=0", "/v1/audit?limit=1001", "/v1/audit?limit=abc", "/v1/audit?afterSeq=-1"} {
		bad := getAudit(t, handler, path)
		if bad.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", path, bad.Code)
		}
	}
}

func TestAuditVerify(t *testing.T) {
	fake := &fakeAuditStore{events: map[string][]store.AuditEvent{"tenant-a": {auditEvent(1, "task.queued")}}}
	handler := newAuditHandler(t, fake, nil, "")
	response := getAudit(t, handler, "/v1/audit/verify")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var verification map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &verification); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if verification["valid"] != true || verification["checked"] != float64(2) || verification["headSeq"] != float64(2) {
		t.Fatalf("verification = %+v", verification)
	}
	if verification["headHash"] == "" {
		t.Fatal("headHash is missing")
	}
}

func TestAuditExportSignedAndUnsigned(t *testing.T) {
	fake := &fakeAuditStore{events: map[string][]store.AuditEvent{
		"tenant-a": {auditEvent(1, "task.queued"), auditEvent(2, "task.transitioned")},
	}}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signedHandler := newAuditHandler(t, fake, privateKey, "audit-key-1")
	response := getAudit(t, signedHandler, "/v1/audit/export")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var archive struct {
		SchemaVersion string `json:"schemaVersion"`
		TenantID      string `json:"tenantId"`
		Signed        bool   `json:"signed"`
		Events        []any  `json:"events"`
		Signature     *struct {
			KeyID   string `json:"keyId"`
			Ed25519 string `json:"ed25519"`
		} `json:"signature"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &archive); err != nil {
		t.Fatalf("decode archive: %v", err)
	}
	if !archive.Signed || archive.Signature == nil || archive.Signature.KeyID != "audit-key-1" {
		t.Fatalf("archive = signed=%v signature=%+v", archive.Signed, archive.Signature)
	}
	if len(archive.Events) != 2 || archive.SchemaVersion != store.AuditSchema || archive.TenantID != "tenant-a" {
		t.Fatalf("archive shape = %+v", archive)
	}
	// The signature covers the canonical payload (schema, tenant, generatedAt,
	// events) — recompute and verify with the public key.
	payload, err := json.Marshal(struct {
		SchemaVersion string `json:"schemaVersion"`
		TenantID      string `json:"tenantId"`
		GeneratedAt   string `json:"generatedAt"`
		Events        []any  `json:"events"`
	}{
		SchemaVersion: archive.SchemaVersion, TenantID: archive.TenantID,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Events: archive.Events,
	})
	_ = payload
	_ = err
	// Recompute the exact payload the handler signed: the archive minus the
	// Signed/Signature fields, with events re-marshaled in the response's
	// field order (maps would reorder keys).
	payload = reconstructArchivePayload(t, response.Body.Bytes())
	digest := sha256.Sum256(payload)
	signature, err := base64.RawStdEncoding.DecodeString(archive.Signature.Ed25519)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	if !ed25519.Verify(publicKey, digest[:], signature) {
		t.Fatal("archive signature does not verify")
	}

	// Without a signing key the export is unsigned.
	unsignedHandler := newAuditHandler(t, fake, nil, "")
	unsigned := getAudit(t, unsignedHandler, "/v1/audit/export")
	var unsignedArchive map[string]any
	if err := json.Unmarshal(unsigned.Body.Bytes(), &unsignedArchive); err != nil {
		t.Fatalf("decode unsigned: %v", err)
	}
	if unsignedArchive["signed"] != false {
		t.Fatalf("unsigned archive = %+v", unsignedArchive)
	}
	if _, present := unsignedArchive["signature"]; present {
		t.Fatalf("unsigned archive carries a signature")
	}
}

func TestAuditEndpointsDisabledWithoutStore(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "owner", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend),
	)
	for _, path := range []string{"/v1/audit", "/v1/audit/verify", "/v1/audit/export"} {
		response := getAudit(t, handler, path)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, response.Code)
		}
	}
}

// archiveEvent mirrors the response event field order so the signed payload
// can be reconstructed byte-for-byte (maps would reorder keys).
type archiveEvent struct {
	ID           string          `json:"id"`
	Seq          int64           `json:"seq"`
	EventType    string          `json:"eventType"`
	ResourceType string          `json:"resourceType"`
	ResourceID   string          `json:"resourceId"`
	Actor        string          `json:"actor"`
	Details      json.RawMessage `json:"details"`
	PrevHash     string          `json:"prevHash"`
	ChainHash    string          `json:"chainHash"`
	OccurredAt   time.Time       `json:"occurredAt"`
}

// reconstructArchivePayload rebuilds the exact bytes the handler signed:
// {schemaVersion, tenantId, generatedAt, events} in that field order.
func reconstructArchivePayload(t *testing.T, archive []byte) []byte {
	t.Helper()
	var parsed struct {
		SchemaVersion string         `json:"schemaVersion"`
		TenantID      string         `json:"tenantId"`
		GeneratedAt   time.Time      `json:"generatedAt"`
		Events        []archiveEvent `json:"events"`
	}
	if err := json.Unmarshal(archive, &parsed); err != nil {
		t.Fatalf("parse archive: %v", err)
	}
	payload, err := json.Marshal(struct {
		SchemaVersion string         `json:"schemaVersion"`
		TenantID      string         `json:"tenantId"`
		GeneratedAt   time.Time      `json:"generatedAt"`
		Events        []archiveEvent `json:"events"`
	}{parsed.SchemaVersion, parsed.TenantID, parsed.GeneratedAt, parsed.Events})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return payload
}
