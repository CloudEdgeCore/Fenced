package adapter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/google/uuid"
)

func writeBindingsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime-bindings.json")
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadRuntimeBindingsResolvesExactAndWildcard(t *testing.T) {
	path := writeBindingsFile(t, `{
		"bindings": [
			{"agentVersionRef": "hello-agent@0.1.0", "endpoint": "http://127.0.0.1:8088/"},
			{"agentVersionRef": "fleet-agent@*", "endpoint": "http://localhost:9099"}
		]
	}`)
	bindings, err := LoadRuntimeBindings(path)
	if err != nil {
		t.Fatalf("load bindings: %v", err)
	}
	if endpoint, ok := bindings.Resolve("hello-agent@0.1.0"); !ok || endpoint != "http://127.0.0.1:8088" {
		t.Fatalf("exact resolve = %q %v", endpoint, ok)
	}
	if endpoint, ok := bindings.Resolve("fleet-agent@9.9.9"); !ok || endpoint != "http://localhost:9099" {
		t.Fatalf("wildcard resolve = %q %v", endpoint, ok)
	}
	if _, ok := bindings.Resolve("other-agent@1.0.0"); ok {
		t.Fatal("unbound version resolved")
	}
	if _, err := LoadRuntimeBindings(""); err != nil {
		t.Fatalf("empty path must disable bindings, got %v", err)
	}
}

func TestLoadRuntimeBindingsRejectsInvalidEntries(t *testing.T) {
	cases := map[string]string{
		"missing version": `{"bindings":[{"agentVersionRef":"agent","endpoint":"http://127.0.0.1:1"}]}`,
		"relative url":    `{"bindings":[{"agentVersionRef":"a@1","endpoint":"127.0.0.1:8088"}]}`,
		"unknown scheme":  `{"bindings":[{"agentVersionRef":"a@1","endpoint":"ftp://127.0.0.1:8088"}]}`,
		"unknown field":   `{"bindings":[{"agentVersionRef":"a@1","endpoint":"http://127.0.0.1:1","extra":true}]}`,
	}
	for name, content := range cases {
		path := writeBindingsFile(t, content)
		if _, err := LoadRuntimeBindings(path); err == nil {
			t.Fatalf("%s: binding file accepted", name)
		}
	}
}

// bindingAssignment builds an assignment whose manifest entrypoint is the
// environment-independent logical binding reference.
func bindingAssignment(t *testing.T, agentRef, entrypoint string, specJSON []byte) *fakeControl {
	t.Helper()
	attemptID := uuid.NewString()
	return &fakeControl{version: 1, assignment: &runtimev1.Assignment{
		Identity: &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: attemptID, FencingToken: 1},
		RunId:    uuid.NewString(), TaskId: uuid.NewString(), AgentVersionRef: agentRef,
		Goal: "execute", WorkloadSpecJson: []byte("{}"), AgentVersionSpecJson: specJSON,
		RuntimeClass: "remote", RuntimePoolId: "remote-pool", RuntimeInstanceId: "adapter-1",
		AttemptVersion: 1, LeaseVersion: 1,
	}}
}

func bindingManifest(t *testing.T, entrypoint string) []byte {
	t.Helper()
	spec := agentversion.Spec{
		RuntimeClassPolicy: agentversion.RuntimeClassPolicy{Allowed: []string{"remote"}, Preferred: "remote"},
		Runtimes: []agentversion.RuntimeTarget{{
			Class: "remote", Interface: agentversion.RuntimeInterfaceV1,
			RuntimeABI: "fenced.remote/v1", Entrypoint: []string{entrypoint},
		}},
		Capabilities: &agentversion.Capabilities{Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{}},
		Resources:    &agentversion.ResourceLimits{CPUMillis: 100, MemoryMiB: 128},
		Budget:       &agentversion.Budget{WallSeconds: 60},
		Checkpoint:   &agentversion.CheckpointPolicy{Mode: agentversion.CheckpointLogical, SchemaVersion: "fixture/v1"},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// TestWorkerResolvesLogicalEntrypointThroughBindings proves the P2-01
// decoupling: one AgentVersion whose manifest carries only a logical
// binding reference executes against whatever concrete endpoint the
// deployment's binding file maps it to.
func TestWorkerResolvesLogicalEntrypointThroughBindings(t *testing.T) {
	host, err := agent.NewHost(&runtimeFixture{state: map[string]json.RawMessage{}}, agent.HostOptions{Adapter: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(host)
	defer server.Close()
	bindings, err := LoadRuntimeBindings(writeBindingsFile(t,
		`{"bindings":[{"agentVersionRef":"fixture@0.9.0","endpoint":"`+server.URL+`"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	control := bindingAssignment(t, "fixture@0.9.0", BindingScheme+"fixture/remote", bindingManifest(t, BindingScheme+"fixture/remote"))
	// No constructor endpoint: bindings are the only resolution source.
	worker, err := NewWorker(control, &memoryArtifacts{content: map[string][]byte{}},
		"", "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	worker.WithRuntimeBindings(bindings)
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v failure=%s", processed, err, control.failure)
	}
	if control.completed == nil {
		t.Fatalf("assignment did not complete; failure=%s", control.failure)
	}
}

// TestWorkerFailsClosedOnUnresolvedLogicalEntrypoint proves an unbound
// logical entrypoint never falls through to another endpoint.
func TestWorkerFailsClosedOnUnresolvedLogicalEntrypoint(t *testing.T) {
	control := bindingAssignment(t, "ghost@1.0.0", BindingScheme+"ghost/remote", bindingManifest(t, BindingScheme+"ghost/remote"))
	worker, err := NewWorker(control, &memoryArtifacts{content: map[string][]byte{}},
		"", "tenant-a", "adapter-1", 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A contained failure still counts as processed: the attempt failed
	// durably instead of the worker erroring or the assignment completing.
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v, want a contained durable failure", processed, err)
	}
	if control.failure == "" || !contains(control.failure, "adapter_endpoint_unresolved") {
		t.Fatalf("failure = %q, want adapter_endpoint_unresolved with the unresolved reference", control.failure)
	}
	if control.completed != nil {
		t.Fatal("unresolved endpoint must never complete an attempt")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestWorkerFallsBackToPollingOnV1OnlyRuntime proves the streaming-first
// wait degrades to the frozen v1 polling endpoints when the runtime has no
// /events/stream route, resuming from the stream cursor without event loss.
func TestWorkerFallsBackToPollingOnV1OnlyRuntime(t *testing.T) {
	var mu sync.Mutex
	resultCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/executions:start", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Fenced-Runtime-Interface", "fenced.runtime.interface/v1")
		writeJSONResponse(writer, http.StatusAccepted, map[string]any{"executionId": "legacy", "status": "ACCEPTED"})
	})
	mux.HandleFunc("/v1/executions/", func(writer http.ResponseWriter, request *http.Request) {
		path := request.URL.Path
		writer.Header().Set("Fenced-Runtime-Interface", "fenced.runtime.interface/v1")
		switch {
		case strings.HasSuffix(path, "/events"):
			writeJSONResponse(writer, http.StatusOK, map[string]any{
				"executionId": "legacy", "nextAfter": 1, "truncated": false,
				"events": []map[string]any{{
					"sequence": 1, "type": "legacy.started", "payload": map[string]any{"ok": true},
					"occurredAt": "2026-08-23T00:00:00Z",
				}},
			})
		case strings.HasSuffix(path, "/result"):
			mu.Lock()
			resultCalls++
			calls := resultCalls
			mu.Unlock()
			if calls < 2 {
				writeJSONResponse(writer, http.StatusAccepted, map[string]any{"executionId": "legacy", "status": "RUNNING"})
				return
			}
			writeJSONResponse(writer, http.StatusOK, map[string]any{
				"executionId": "legacy", "status": "SUCCEEDED", "output": map[string]any{"answer": "done"},
				"completedAt": "2026-08-23T00:00:01Z",
			})
		case strings.HasSuffix(path, ":stop"):
			writeJSONResponse(writer, http.StatusAccepted, map[string]any{"executionId": "legacy", "status": "RUNNING"})
		case strings.HasSuffix(path, ":checkpoint"):
			writeJSONResponse(writer, http.StatusOK, map[string]any{
				"executionId": "legacy",
				"checkpoint": map[string]any{
					"schemaVersion": "fixture/v1", "state": map[string]any{}, "createdAt": "2026-08-23T00:00:00Z",
				},
			})
		default:
			writeJSONResponse(writer, http.StatusNotFound, map[string]any{"code": "ROUTE_NOT_FOUND", "detail": "runtime execution route not found", "status": 404})
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	spec := bindingManifest(t, server.URL)
	control := bindingAssignment(t, "legacy@1.0.0", server.URL, spec)
	worker, err := NewWorker(control, &memoryArtifacts{content: map[string][]byte{}},
		server.URL, "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	worker.pollInterval = 5 * time.Millisecond
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v failure=%s", processed, err, control.failure)
	}
	if control.completed == nil {
		t.Fatalf("v1-only fallback did not complete; failure=%s", control.failure)
	}
}

// P1-03: the worker's HTTP client cache keys include the TLS material
// fingerprint. Two bindings that share an endpoint and SNI but present
// different client certificates are different identities and must never
// share a cached client; identical material reuses its own entry.
func TestWorkerClientCacheSeparatesCertificateIdentities(t *testing.T) {
	worker, err := NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		"", "tenant-a", "adapter-1", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	caA, certA, keyA := writeTestCertMaterial(t)
	_, certB, keyB := writeTestCertMaterial(t)
	configA, identityA, err := (&BindingTLS{CAFile: caA, CertFile: certA, KeyFile: keyA}).load()
	if err != nil {
		t.Fatal(err)
	}
	configB, identityB, err := (&BindingTLS{CAFile: caA, CertFile: certB, KeyFile: keyB}).load()
	if err != nil {
		t.Fatal(err)
	}
	if identityA == "" || identityA == identityB {
		t.Fatalf("certificate identities collapsed: %q vs %q", identityA, identityB)
	}
	binding := RuntimeBinding{Endpoint: "https://runtime.internal:8443", TLSServerName: "runtime.internal"}
	clientA, err := worker.clientFor(binding, configA, identityA)
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := worker.clientFor(binding, configB, identityB)
	if err != nil {
		t.Fatal(err)
	}
	if clientA == clientB {
		t.Fatal("bindings sharing endpoint/SNI but not certificates reused one cached client")
	}
	reused, err := worker.clientFor(binding, configA, identityA)
	if err != nil {
		t.Fatal(err)
	}
	if reused != clientA {
		t.Fatal("identical TLS material did not reuse its cached client")
	}
	if len(worker.clients) != 2 {
		t.Fatalf("client cache holds %d entries, want 2", len(worker.clients))
	}
}

func writeJSONResponse(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

// P2-01: the same reference declared twice (exact or wildcard) must fail the
// load instead of silently routing by file order.
func TestLoadRuntimeBindingsRejectsDuplicateReferences(t *testing.T) {
	cases := map[string]string{
		"duplicate exact": `{"bindings":[
			{"agentVersionRef":"agent@1.0.0","endpoint":"http://127.0.0.1:8088"},
			{"agentVersionRef":"agent@1.0.0","endpoint":"http://127.0.0.1:8089"}]}`,
		"duplicate wildcard": `{"bindings":[
			{"agentVersionRef":"agent@*","endpoint":"http://127.0.0.1:8088"},
			{"agentVersionRef":"agent@*","endpoint":"http://127.0.0.1:8089"}]}`,
	}
	for name, content := range cases {
		path := writeBindingsFile(t, content)
		if _, err := LoadRuntimeBindings(path); err == nil {
			t.Fatalf("%s: duplicate binding accepted", name)
		}
	}
}

// P0-02: plaintext HTTP to a remote runtime is rejected under the production
// policy and only loads when the deployment explicitly acknowledges it; a
// loopback plaintext endpoint (co-located runtime) always loads.
func TestLoadRuntimeBindingsRejectsRemotePlaintextUnlessAcknowledged(t *testing.T) {
	const remotePlaintext = `{"bindings":[{"agentVersionRef":"a@1","endpoint":"http://10.20.30.40:8088"}]}`
	path := writeBindingsFile(t, remotePlaintext)
	if _, err := LoadRuntimeBindings(path); err == nil {
		t.Fatal("production policy accepted a remote plaintext runtime endpoint")
	}
	if _, err := LoadRuntimeBindingsFor(path, EndpointPolicy{AllowPlaintextRemote: true}); err != nil {
		t.Fatalf("acknowledged development policy rejected the endpoint: %v", err)
	}
	loopback := writeBindingsFile(t, `{"bindings":[{"agentVersionRef":"a@1","endpoint":"http://127.0.0.1:8088"}]}`)
	if _, err := LoadRuntimeBindings(loopback); err != nil {
		t.Fatalf("loopback plaintext endpoint rejected under production policy: %v", err)
	}
	// A production remote HTTPS binding carries its client identity (P1-04),
	// so this fixture presents full mutual-TLS material.
	caFile, certFile, keyFile := writeTestCertMaterial(t)
	remoteTLS := writeBindingsFile(t, fmt.Sprintf(`{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://runtime.internal:8443",
		"tls":{"caFile":"%s","certFile":"%s","keyFile":"%s"}}]}`,
		filepath.ToSlash(caFile), filepath.ToSlash(certFile), filepath.ToSlash(keyFile)))
	if _, err := LoadRuntimeBindings(remoteTLS); err != nil {
		t.Fatalf("remote HTTPS endpoint with client identity rejected: %v", err)
	}
}

// P1-04: a production remote runtime binding must present a mutual-TLS
// client certificate — server verification alone does not identify the
// caller. Loopback endpoints and deployments that explicitly acknowledged
// the development policy are exempt.
func TestLoadRuntimeBindingsRequiresClientIdentityOnRemoteProduction(t *testing.T) {
	caFile, certFile, keyFile := writeTestCertMaterial(t)
	material := func(withCert bool) string {
		if !withCert {
			return `"tls":{"caFile":"` + filepath.ToSlash(caFile) + `"}`
		}
		return `"tls":{"caFile":"` + filepath.ToSlash(caFile) + `","certFile":"` + filepath.ToSlash(certFile) + `","keyFile":"` + filepath.ToSlash(keyFile) + `"}`
	}
	binding := func(tlsSection string) string {
		separator := ""
		if tlsSection != "" {
			separator = ","
		}
		return writeBindingsFile(t, `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://runtime.internal:8443"`+separator+tlsSection+`}]}`)
	}
	if _, err := LoadRuntimeBindings(binding(``)); err == nil {
		t.Fatal("remote production binding without any TLS section accepted")
	}
	if _, err := LoadRuntimeBindings(binding(material(false))); err == nil {
		t.Fatal("remote production binding with server-only verification accepted")
	}
	if _, err := LoadRuntimeBindings(binding(material(true))); err != nil {
		t.Fatalf("remote production binding with client identity rejected: %v", err)
	}
	// The explicit development acknowledgment exempts remote endpoints.
	if _, err := LoadRuntimeBindingsFor(binding(``), EndpointPolicy{AllowPlaintextRemote: true}); err != nil {
		t.Fatalf("development policy rejected a remote binding without client identity: %v", err)
	}
	// Loopback HTTPS never requires a client identity.
	loopback := writeBindingsFile(t, `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://127.0.0.1:8443"}]}`)
	if _, err := LoadRuntimeBindings(loopback); err != nil {
		t.Fatalf("loopback HTTPS binding rejected: %v", err)
	}
}

// P2-04: deployment metadata must stay bounded and self-consistent.
func TestLoadRuntimeBindingsValidatesDeploymentMetadata(t *testing.T) {
	long := strings.Repeat("x", 257)
	cases := map[string]string{
		"oversized region":   `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","region":"` + long + `"}]}`,
		"weight over bound":  `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","weight":1001}]}`,
		"negative weight":    `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","weight":-1}]}`,
		"server name no tls": `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","tlsServerName":"r.internal"}]}`,
		"cert without key":   `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","tls":{"certFile":"c.pem"}}]}`,
		"empty tls section":  `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","tls":{}}]}`,
		"missing ca file":    `{"bindings":[{"agentVersionRef":"a@1","endpoint":"https://r.internal","tls":{"caFile":"no-such-file.pem"}}]}`,
	}
	for name, content := range cases {
		path := writeBindingsFile(t, content)
		if _, err := LoadRuntimeBindings(path); err == nil {
			t.Fatalf("%s: binding metadata accepted", name)
		}
	}
}

// writeTestCertMaterial generates a private CA plus a client key pair and
// writes PEM files, returning (caFile, certFile, keyFile).
func writeTestCertMaterial(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fenced-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "fenced-test-runtime-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCertificate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "client.pem")
	keyFile := filepath.Join(dir, "client.key")
	writePEM := func(path, blockType string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePEM(caFile, "CERTIFICATE", caDER)
	writePEM(certFile, "CERTIFICATE", clientDER)
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(keyFile, "EC PRIVATE KEY", keyDER)
	return caFile, certFile, keyFile
}

// P2-04/P0-02: a binding's TLS material loads into a client configuration
// with the private trust bundle and the mTLS client certificate.
func TestLoadRuntimeBindingsLoadsTLSMaterial(t *testing.T) {
	caFile, certFile, keyFile := writeTestCertMaterial(t)
	path := writeBindingsFile(t, `{"bindings":[{
		"agentVersionRef": "secure-agent@1.0.0",
		"endpoint": "https://runtime.internal:8443",
		"tlsServerName": "runtime.internal",
		"runtimePool": "pool-a", "runtimeClass": "remote", "region": "eu-1", "weight": 100,
		"tls": {"caFile": "`+filepath.ToSlash(caFile)+`", "certFile": "`+filepath.ToSlash(certFile)+`", "keyFile": "`+filepath.ToSlash(keyFile)+`"}
	}]}`)
	bindings, err := LoadRuntimeBindings(path)
	if err != nil {
		t.Fatalf("load bindings: %v", err)
	}
	binding, ok := bindings.ResolveBinding("secure-agent@1.0.0")
	if !ok {
		t.Fatal("binding did not resolve")
	}
	if binding.RuntimePool != "pool-a" || binding.Region != "eu-1" || binding.Weight != 100 {
		t.Fatalf("deployment metadata lost: %+v", binding)
	}
	tlsConfig := bindings.tlsConfigFor("secure-agent@1.0.0")
	if tlsConfig == nil {
		t.Fatal("TLS configuration not loaded")
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("private trust bundle not loaded")
	}
	if tlsConfig.GetClientCertificate == nil {
		t.Fatal("mTLS client certificate not loaded")
	}
	if tlsConfig.MinVersion < tls.VersionTLS12 {
		t.Fatalf("minimum TLS version too permissive: %x", tlsConfig.MinVersion)
	}
}

// P2-03: an explicit worker endpoint combined with bindings is a
// dedicated-worker configuration that must be acknowledged.
func TestValidateEndpointOverrideGuard(t *testing.T) {
	bindings, err := LoadRuntimeBindings(writeBindingsFile(t,
		`{"bindings":[{"agentVersionRef":"a@1","endpoint":"http://127.0.0.1:8088"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateEndpointOverride("", bindings, false); err != nil {
		t.Fatalf("no endpoint override must always pass: %v", err)
	}
	if err := ValidateEndpointOverride("http://127.0.0.1:8088", nil, false); err != nil {
		t.Fatalf("no bindings must always pass: %v", err)
	}
	if err := ValidateEndpointOverride("http://127.0.0.1:8088", bindings, true); err != nil {
		t.Fatalf("acknowledged dedicated worker rejected: %v", err)
	}
	if err := ValidateEndpointOverride("http://127.0.0.1:8088", bindings, false); err == nil {
		t.Fatal("unacknowledged shared endpoint with bindings accepted")
	}
}

// P0-02: the worker constructor refuses a remote plaintext endpoint outright
// and refuses an HTTPS endpoint whose transport has verification disabled
// (a man-in-the-middle must never be silently accepted).
func TestNewWorkerRejectsInsecureEndpointsAndTransports(t *testing.T) {
	_, err := NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		"http://10.20.30.40:8088", "tenant-a", "adapter-1", 30*time.Second, nil)
	if err == nil {
		t.Fatal("remote plaintext constructor endpoint accepted")
	}
	insecure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	_, err = NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		"https://runtime.internal:8443", "tenant-a", "adapter-1", 30*time.Second, insecure)
	if err == nil {
		t.Fatal("HTTPS endpoint with TLS verification disabled accepted")
	}
	if _, err := NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		"http://127.0.0.1:8088", "tenant-a", "adapter-1", 30*time.Second, nil); err != nil {
		t.Fatalf("loopback plaintext endpoint rejected: %v", err)
	}
	if err := refuseInsecureTransport(nil, "https://runtime.internal:8443"); err != nil {
		t.Fatalf("default transport must be allowed: %v", err)
	}
	if err := refuseInsecureTransport(insecure, "http://runtime.internal:8443"); err != nil {
		t.Fatalf("insecure transport to a plaintext endpoint is out of scope: %v", err)
	}
}

// streamDropServer serves the Runtime Interface endpoints of one execution
// whose event stream drops mid-flight. drops is the number of stream
// connections that end abruptly; later connections (or the polling fallback)
// serve the remaining events and the terminal result.
type streamDropServer struct {
	mu             sync.Mutex
	streamRequests int
	streamAfters   []string
	drops          int
	pollEvents     int
	resultPolls    int
}

func streamEventFrame(sequence int64) string {
	return fmt.Sprintf("event: event\ndata: {\"sequence\":%d,\"type\":\"step\",\"payload\":{\"n\":%d},\"occurredAt\":\"2026-08-23T00:00:00Z\"}\n\n", sequence, sequence)
}

const streamResultFrame = "event: result\ndata: {\"executionId\":\"reconnect\",\"status\":\"SUCCEEDED\",\"output\":{\"answer\":42}}\n\n"

func (s *streamDropServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/executions/reconnect/events/stream", func(writer http.ResponseWriter, request *http.Request) {
		s.mu.Lock()
		s.streamRequests++
		index := s.streamRequests
		s.streamAfters = append(s.streamAfters, request.URL.Query().Get("after"))
		drop := index <= s.drops
		s.mu.Unlock()
		after, _ := strconv.ParseInt(request.URL.Query().Get("after"), 10, 64)
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.Header().Set("Fenced-Runtime-Interface", "fenced.runtime.interface/v1")
		writer.WriteHeader(http.StatusOK)
		for sequence := after + 1; sequence <= 2; sequence++ {
			fmt.Fprint(writer, streamEventFrame(sequence))
		}
		writer.(http.Flusher).Flush()
		if drop {
			// Abruptly sever the connection mid-stream.
			panic(http.ErrAbortHandler)
		}
		fmt.Fprint(writer, streamEventFrame(3))
		fmt.Fprint(writer, streamResultFrame)
		writer.(http.Flusher).Flush()
	})
	mux.HandleFunc("/v1/executions/reconnect/events", func(writer http.ResponseWriter, request *http.Request) {
		s.mu.Lock()
		s.pollEvents++
		s.mu.Unlock()
		after, _ := strconv.ParseInt(request.URL.Query().Get("after"), 10, 64)
		events := []map[string]any{}
		for sequence := after + 1; sequence <= 3; sequence++ {
			events = append(events, map[string]any{
				"sequence": sequence, "type": "step", "payload": map[string]any{"n": sequence},
				"occurredAt": "2026-08-23T00:00:00Z",
			})
		}
		writer.Header().Set("Fenced-Runtime-Interface", "fenced.runtime.interface/v1")
		writeJSONResponse(writer, http.StatusOK, map[string]any{
			"executionId": "reconnect", "events": events, "nextAfter": after + int64(len(events)), "truncated": false,
		})
	})
	mux.HandleFunc("/v1/executions/reconnect/result", func(writer http.ResponseWriter, request *http.Request) {
		s.mu.Lock()
		s.resultPolls++
		calls := s.resultPolls
		s.mu.Unlock()
		writer.Header().Set("Fenced-Runtime-Interface", "fenced.runtime.interface/v1")
		if calls == 1 {
			writeJSONResponse(writer, http.StatusAccepted, map[string]any{"executionId": "reconnect", "status": "RUNNING"})
			return
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{
			"executionId": "reconnect", "status": "SUCCEEDED", "output": map[string]any{"answer": 42},
			"completedAt": "2026-08-23T00:00:01Z",
		})
	})
	return mux
}

func (s *streamDropServer) snapshot() (streamRequests, pollEvents, resultPolls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamRequests, s.pollEvents, s.resultPolls
}

// P1-02: a stream connection that drops mid-flight reconnects from the last
// consumed sequence: no event duplication, no loss, no polling degradation.
func TestWorkerStreamReconnectsFromCursorWithoutLoss(t *testing.T) {
	drop := &streamDropServer{drops: 1}
	server := httptest.NewServer(drop.handler())
	defer server.Close()
	worker, err := NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		server.URL, "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := agent.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, events, err := worker.wait(ctx, client, "reconnect", 0, nil)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if result.Status != agent.StatusSucceeded {
		t.Fatalf("result status = %s, want SUCCEEDED", result.Status)
	}
	sequences := make([]int64, len(events))
	for index, event := range events {
		sequences[index] = event.Sequence
	}
	if len(sequences) != 3 || sequences[0] != 1 || sequences[1] != 2 || sequences[2] != 3 {
		t.Fatalf("streamed sequences = %v, want exactly [1 2 3]", sequences)
	}
	streamRequests, pollEvents, resultPolls := drop.snapshot()
	if streamRequests != 2 {
		t.Fatalf("stream connections = %d, want 2 (drop + reconnect from cursor)", streamRequests)
	}
	if drop.streamAfters[1] != "2" {
		t.Fatalf("reconnect cursor = %q, want the last consumed sequence \"2\"", drop.streamAfters[1])
	}
	if pollEvents != 0 || resultPolls != 0 {
		t.Fatalf("polling degraded: events=%d result=%d, want none after a clean reconnect", pollEvents, resultPolls)
	}
}

// P1-02: when the stream keeps dropping, the bounded reconnects hand the
// cursor to the polling fallback, which resumes without loss or duplication.
func TestWorkerStreamFallsBackToPollingAfterExhaustedReconnects(t *testing.T) {
	drop := &streamDropServer{drops: 10}
	server := httptest.NewServer(drop.handler())
	defer server.Close()
	worker, err := NewWorker(&fakeControl{}, &memoryArtifacts{content: map[string][]byte{}},
		server.URL, "tenant-a", "adapter-1", 30*time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	worker.pollInterval = 5 * time.Millisecond
	client, err := agent.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, events, err := worker.wait(ctx, client, "reconnect", 0, nil)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if result.Status != agent.StatusSucceeded {
		t.Fatalf("result status = %s, want SUCCEEDED", result.Status)
	}
	sequences := make([]int64, len(events))
	for index, event := range events {
		sequences[index] = event.Sequence
	}
	if len(sequences) != 3 || sequences[0] != 1 || sequences[1] != 2 || sequences[2] != 3 {
		t.Fatalf("event sequences = %v, want exactly [1 2 3] across stream and polling", sequences)
	}
	streamRequests, _, _ := drop.snapshot()
	if streamRequests != streamReconnects+1 {
		t.Fatalf("stream connections = %d, want %d (initial plus bounded reconnects)", streamRequests, streamReconnects+1)
	}
}
