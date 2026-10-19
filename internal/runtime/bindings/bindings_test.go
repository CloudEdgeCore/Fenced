package bindings

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
