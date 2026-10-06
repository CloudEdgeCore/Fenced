package control

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// stubRuntimeStore satisfies store.RuntimeStore with only PollRuntimeAssignment
// implemented; any other method panics, which is a test failure, not a
// production path.
type stubRuntimeStore struct {
	store.RuntimeStore
}

func (s *stubRuntimeStore) PollRuntimeAssignment(context.Context, string, string) (store.RuntimeAssignment, error) {
	return store.RuntimeAssignment{}, store.ErrNoAssignment
}

// TestMTLSIdentityBoundary proves the SPIFFE X.509-SVID boundary end to end:
// a worker whose SVID matches its claims is admitted, and every mismatch —
// cross-tenant SVID, wrong instance SVID, or no client certificate — is
// rejected before the request reaches the store.
func TestMTLSIdentityBoundary(t *testing.T) {
	now := time.Now()
	ca, err := spiffe.NewCA(spiffe.DefaultTrustDomain, now, 24*time.Hour)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	serverSVID, err := ca.IssueSVIDWithSANs(spiffe.ControlPlaneIdentity(spiffe.DefaultTrustDomain), now, 24*time.Hour,
		nil, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("issue server SVID: %v", err)
	}
	workerSVID, err := ca.IssueSVID("tenant-a", "worker-1", now, 24*time.Hour)
	if err != nil {
		t.Fatalf("issue worker SVID: %v", err)
	}
	crossTenantSVID, err := ca.IssueSVID("tenant-b", "worker-1", now, 24*time.Hour)
	if err != nil {
		t.Fatalf("issue cross-tenant SVID: %v", err)
	}
	wrongInstanceSVID, err := ca.IssueSVID("tenant-a", "worker-2", now, 24*time.Hour)
	if err != nil {
		t.Fatalf("issue wrong-instance SVID: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca.Certificate())
	pattern, err := spiffe.ParsePattern("spiffe://" + spiffe.DefaultTrustDomain + "/ns/*/worker/*")
	if err != nil {
		t.Fatalf("parse pattern: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverOptions := append([]grpc.ServerOption{grpcx.WithSpiffeIdentity(pattern)},
		grpcx.ServerMTLSOptions(serverSVID, pool)...)
	serverOptions = append(serverOptions, grpcx.ServerOptions()...)
	server := grpc.NewServer(serverOptions...)
	service := NewService(&stubRuntimeStore{}, "tenant-a", 2*time.Minute, WithSpiffeClaimBinding(spiffe.DefaultTrustDomain))
	runtimev1.RegisterRuntimeControlServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	poll := func(svid tls.Certificate, pool *x509.CertPool, tenant, instance string) error {
		options := append(grpcx.ClientMTLSOptions(svid, pool), grpcx.ClientOptions()...)
		connection, err := grpc.NewClient(listener.Addr().String(), options...)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer connection.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = runtimev1.NewRuntimeControlServiceClient(connection).PollAssignment(ctx,
			&runtimev1.PollAssignmentRequest{TenantId: tenant, RuntimeInstanceId: instance})
		return err
	}

	// A worker with the correct SVID passes the identity boundary; the store
	// then reports no assignment (NotFound), proving the call was admitted.
	err = poll(workerSVID, pool, "tenant-a", "worker-1")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("authorized worker error = %v, want NotFound", err)
	}

	// A tenant-b SVID impersonating tenant-a is rejected.
	err = poll(crossTenantSVID, pool, "tenant-a", "worker-1")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant SVID error = %v, want PermissionDenied", err)
	}

	// A tenant-a SVID for a different worker instance is rejected.
	err = poll(wrongInstanceSVID, pool, "tenant-a", "worker-1")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong-instance SVID error = %v, want PermissionDenied", err)
	}

	// An SVID from an untrusted CA never completes the TLS handshake.
	rogueCA, err := spiffe.NewCA("evil.example", now, 24*time.Hour)
	if err != nil {
		t.Fatalf("create rogue CA: %v", err)
	}
	rogueSVID, err := rogueCA.IssueSVID("tenant-a", "worker-1", now, 24*time.Hour)
	if err != nil {
		t.Fatalf("issue rogue SVID: %v", err)
	}
	roguePool := x509.NewCertPool()
	roguePool.AddCert(rogueCA.Certificate())
	err = poll(rogueSVID, roguePool, "tenant-a", "worker-1")
	if err == nil {
		t.Fatal("SVID from an untrusted CA was accepted")
	}

	// A peer without a client certificate never completes the TLS handshake.
	plain, err := grpc.NewClient(listener.Addr().String(),
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, grpcx.ClientOptions()...)...)
	if err != nil {
		t.Fatalf("dial plaintext: %v", err)
	}
	defer plain.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = runtimev1.NewRuntimeControlServiceClient(plain).PollAssignment(ctx,
		&runtimev1.PollAssignmentRequest{TenantId: "tenant-a", RuntimeInstanceId: "worker-1"})
	if err == nil {
		t.Fatal("certificate-less client was accepted")
	}

	// An EXPIRED SVID (issued in the past with a validity window that has
	// already closed) never completes the TLS handshake.
	expiredSVID, err := ca.IssueSVID("tenant-a", "worker-1", now.Add(-2*time.Hour), time.Hour)
	if err != nil {
		t.Fatalf("issue expired SVID: %v", err)
	}
	err = poll(expiredSVID, pool, "tenant-a", "worker-1")
	if err == nil {
		t.Fatal("expired SVID was accepted")
	}
}

// TestRemoteRuntimeFencingTokenDenied proves the plan's "旧 Fencing Token →
// DENY" case at the runtime-control boundary: a runtime presenting a stale
// fencing token for its attempt is rejected before execution.
func TestRemoteRuntimeFencingTokenDenied(t *testing.T) {
	service := NewService(&fencedRuntimeStore{}, "tenant-a", time.Minute)
	ctx := context.Background()
	_, err := service.GetAssignment(ctx, &runtimev1.GetAssignmentRequest{
		Identity: &runtimev1.AttemptIdentity{
			TenantId: "tenant-a", AttemptId: uuid.New().String(), FencingToken: 42,
		},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale fencing token = %v, want PermissionDenied", err)
	}
}

// fencedRuntimeStore returns ErrFenced for every assignment lookup, as the
// durable store does when the presented fencing token is no longer current.
type fencedRuntimeStore struct {
	store.RuntimeStore
}

func (s *fencedRuntimeStore) GetRuntimeAssignment(context.Context, string, uuid.UUID, int64) (store.RuntimeAssignment, error) {
	return store.RuntimeAssignment{}, store.ErrFenced
}

// TestClaimBindingIsInertWithoutConfiguredTrustDomain proves the dev
// plaintext path stays unchanged when claim binding is not configured.
func TestClaimBindingIsInertWithoutConfiguredTrustDomain(t *testing.T) {
	service := NewService(&stubRuntimeStore{}, "tenant-a", time.Minute)
	if err := service.authorizePeer(context.Background(), "tenant-a", "worker-1"); err != nil {
		t.Fatalf("claim binding without trust domain must be inert, got %v", err)
	}
}

// TestClaimBindingRejectsMissingAndMismatchedIdentity covers the direct
// authorizePeer surface without a full transport.
func TestClaimBindingRejectsMissingAndMismatchedIdentity(t *testing.T) {
	now := time.Now()
	ca, err := spiffe.NewCA(spiffe.DefaultTrustDomain, now, time.Hour)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	service := NewService(&stubRuntimeStore{}, "tenant-a", time.Minute, WithSpiffeClaimBinding(spiffe.DefaultTrustDomain))

	// No TLS peer: unauthenticated.
	if err := service.authorizePeer(context.Background(), "tenant-a", "worker-1"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing peer error = %v, want Unauthenticated", err)
	}

	workerSVID, err := ca.IssueSVID("tenant-a", "worker-1", now, time.Hour)
	if err != nil {
		t.Fatalf("issue SVID: %v", err)
	}
	leaf, err := spiffe.ParseLeaf(workerSVID)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	ctx := contextWithPeerCert(leaf)
	if err := service.authorizePeer(ctx, "tenant-a", "worker-1"); err != nil {
		t.Fatalf("matching claims rejected: %v", err)
	}
	if err := service.authorizePeer(ctx, "tenant-a", "worker-9"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong instance error = %v, want PermissionDenied", err)
	}
	if err := service.authorizePeer(ctx, "tenant-c", ""); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong tenant error = %v, want PermissionDenied", err)
	}
}

func contextWithPeerCert(leaf *x509.Certificate) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}},
	})
}
