package spiffe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func fixedNow() time.Time { return time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC) }

func testCA(t *testing.T) *CA {
	t.Helper()
	ca, err := NewCA(DefaultTrustDomain, fixedNow(), 365*24*time.Hour)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	return ca
}

func TestIdentityShape(t *testing.T) {
	identity := Identity(DefaultTrustDomain, "tenant-a", "worker-1")
	want := "spiffe://fenced.dev/ns/tenant-a/worker/worker-1"
	if identity != want {
		t.Fatalf("identity = %q, want %q", identity, want)
	}
	if got := ControlPlaneIdentity(DefaultTrustDomain); got != "spiffe://fenced.dev/ns/system/control" {
		t.Fatalf("control plane identity = %q", got)
	}
}

func TestIssueAndParseSVID(t *testing.T) {
	ca := testCA(t)
	svid, err := ca.IssueSVID("tenant-a", "worker-1", fixedNow(), 24*time.Hour)
	if err != nil {
		t.Fatalf("issue SVID: %v", err)
	}
	leaf, err := ParseLeaf(svid)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	identity, err := ParsePeerIdentity(leaf)
	if err != nil {
		t.Fatalf("extract identity: %v", err)
	}
	if identity != "spiffe://fenced.dev/ns/tenant-a/worker/worker-1" {
		t.Fatalf("identity = %q", identity)
	}

	// The chain verifies against the CA trust bundle.
	pool := x509.NewCertPool()
	pool.AddCert(ca.Certificate())
	chains, err := leaf.Verify(x509.VerifyOptions{
		Roots:         pool,
		Intermediates: intermediatePool(svid),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		CurrentTime:   fixedNow(),
	})
	if err != nil || len(chains) == 0 {
		t.Fatalf("chain verification failed: %v", err)
	}
}

func intermediatePool(svid tls.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, der := range svid.Certificate[1:] {
		if cert, err := x509.ParseCertificate(der); err == nil {
			pool.AddCert(cert)
		}
	}
	return pool
}

func TestSVIDExpiryAndUntrustedCA(t *testing.T) {
	ca := testCA(t)
	// An SVID that is already expired.
	expired, err := ca.IssueSVID("tenant-a", "worker-1", fixedNow().Add(-48*time.Hour), time.Hour)
	if err != nil {
		t.Fatalf("issue expired SVID: %v", err)
	}
	leaf, err := ParseLeaf(expired)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Certificate())
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots: pool, Intermediates: intermediatePool(expired), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		CurrentTime: fixedNow(),
	}); err == nil {
		t.Fatal("expired SVID verified")
	}

	// A chain from an untrusted CA must not verify.
	otherCA := testCA(t)
	svid, err := otherCA.IssueSVID("tenant-a", "worker-1", fixedNow(), time.Hour)
	if err != nil {
		t.Fatalf("issue SVID: %v", err)
	}
	leaf, err = ParseLeaf(svid)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots: pool, Intermediates: intermediatePool(svid), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		CurrentTime: fixedNow(),
	}); err == nil {
		t.Fatal("SVID from an untrusted CA verified against the trust bundle")
	}
}

func TestPatternMatching(t *testing.T) {
	pattern, err := ParsePattern("spiffe://fenced.dev/ns/*/worker/*")
	if err != nil {
		t.Fatalf("parse pattern: %v", err)
	}
	for _, identity := range []string{
		"spiffe://fenced.dev/ns/tenant-a/worker/worker-1",
		"spiffe://fenced.dev/ns/tenant-b/worker/any",
	} {
		if !pattern.Matches(identity) {
			t.Fatalf("pattern %q must match %q", pattern, identity)
		}
	}
	for _, identity := range []string{
		"spiffe://evil.example/ns/tenant-a/worker/worker-1",
		"spiffe://fenced.dev/ns/tenant-a/model/gateway-1",
		"spiffe://fenced.dev/ns/system/control",
		"not-a-spiffe-id",
		"https://fenced.dev/ns/tenant-a/worker/worker-1",
	} {
		if pattern.Matches(identity) {
			t.Fatalf("pattern %q must reject %q", pattern, identity)
		}
	}

	exact, err := ParsePattern("spiffe://fenced.dev/ns/tenant-a/worker/worker-1")
	if err != nil {
		t.Fatalf("parse exact pattern: %v", err)
	}
	if !exact.Matches("spiffe://fenced.dev/ns/tenant-a/worker/worker-1") ||
		exact.Matches("spiffe://fenced.dev/ns/tenant-a/worker/worker-2") {
		t.Fatal("exact pattern matching is wrong")
	}

	for _, bad := range []string{"", "spiffe://fenced.dev", "spiffe://fenced.dev/ns/tenant-a", "spiffe://fenced.dev/ns/tenant-a/worker"} {
		if _, err := ParsePattern(bad); err == nil {
			t.Fatalf("malformed pattern %q was accepted", bad)
		}
	}
}

func TestWorkerClaimsAreExactAndTenantBound(t *testing.T) {
	trustDomain, tenant, instance, err := WorkerClaims("spiffe://fenced.dev/ns/tenant-a/worker/worker-1")
	if err != nil || trustDomain != "fenced.dev" || tenant != "tenant-a" || instance != "worker-1" {
		t.Fatalf("claims=%q/%q/%q err=%v", trustDomain, tenant, instance, err)
	}
	for _, invalid := range []string{
		"spiffe://fenced.dev/ns/system/control",
		"spiffe://fenced.dev/ns/*/worker/worker-1",
		"https://fenced.dev/ns/tenant-a/worker/worker-1",
	} {
		if _, _, _, err := WorkerClaims(invalid); err == nil {
			t.Fatalf("invalid worker identity %q was accepted", invalid)
		}
	}
}

func TestPeerIdentityFromContext(t *testing.T) {
	ca := testCA(t)
	svid, err := ca.IssueSVID("tenant-a", "worker-1", fixedNow(), time.Hour)
	if err != nil {
		t.Fatalf("issue SVID: %v", err)
	}
	leaf, err := ParseLeaf(svid)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}},
	})
	identity, err := PeerIdentity(ctx)
	if err != nil {
		t.Fatalf("peer identity: %v", err)
	}
	if identity != "spiffe://fenced.dev/ns/tenant-a/worker/worker-1" {
		t.Fatalf("peer identity = %q", identity)
	}

	// No TLS transport: error.
	if _, err := PeerIdentity(context.Background()); err == nil {
		t.Fatal("peer identity without TLS peer must fail")
	}
	// TLS transport without a client certificate: error.
	ctx = peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{}},
	})
	if _, err := PeerIdentity(ctx); err == nil {
		t.Fatal("peer identity without client cert must fail")
	}
	// Certificate without a SPIFFE URI SAN: error.
	noURI := &x509.Certificate{}
	ctx = peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{noURI}}},
	})
	if _, err := PeerIdentity(ctx); err == nil {
		t.Fatal("peer identity without SPIFFE URI must fail")
	}
}

func TestCheckPeerIdentity(t *testing.T) {
	ca := testCA(t)
	pattern, err := ParsePattern("spiffe://fenced.dev/ns/*/worker/*")
	if err != nil {
		t.Fatalf("parse pattern: %v", err)
	}
	ctxWith := func(tenant, instance string) context.Context {
		svid, err := ca.IssueSVID(tenant, instance, fixedNow(), time.Hour)
		if err != nil {
			t.Fatalf("issue SVID: %v", err)
		}
		leaf, err := ParseLeaf(svid)
		if err != nil {
			t.Fatalf("parse leaf: %v", err)
		}
		return peer.NewContext(context.Background(), &peer.Peer{
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}},
		})
	}
	if err := CheckPeerIdentity(ctxWith("tenant-a", "worker-1"), pattern); err != nil {
		t.Fatalf("authorized peer rejected: %v", err)
	}
	if err := CheckPeerIdentity(context.Background(), pattern); err == nil {
		t.Fatal("missing peer must fail the check")
	}
	// Control plane identity is outside the worker pattern: its path is
	// /ns/system/control, not /ns/<tenant>/worker/<instance>.
	control, err := ca.IssueSVIDFor(ControlPlaneIdentity(DefaultTrustDomain), fixedNow(), time.Hour)
	if err != nil {
		t.Fatalf("issue control SVID: %v", err)
	}
	controlLeaf, err := ParseLeaf(control)
	if err != nil {
		t.Fatalf("parse control leaf: %v", err)
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{controlLeaf}}},
	})
	if err := CheckPeerIdentity(ctx, pattern); err == nil {
		t.Fatal("control plane identity must not match the worker pattern")
	}
}

func TestTrustBundlePool(t *testing.T) {
	ca := testCA(t)
	pool, err := TrustBundlePool([][]byte{ca.PEM()})
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	if pool == nil {
		t.Fatal("pool is nil")
	}
	if _, err := TrustBundlePool([][]byte{[]byte("garbage")}); err == nil {
		t.Fatal("invalid PEM must be rejected")
	}
	if _, err := TrustBundlePool(nil); err == nil {
		t.Fatal("empty bundle must be rejected")
	}
}
