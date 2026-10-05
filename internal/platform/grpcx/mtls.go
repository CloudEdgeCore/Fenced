package grpcx

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// ServerMTLSOptions returns the transport options for a mutually
// authenticated fenced server: it serves its own X.509-SVID (issued by the
// same CA as the trust bundle) and requires every client to present a
// certificate chain rooted in the trust bundle. Combined with
// WithSpiffeIdentity this is the X.509-SVID boundary (ADR-011).
func ServerMTLSOptions(serverSVID tls.Certificate, trustBundle *x509.CertPool) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{serverSVID},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    trustBundle,
			MinVersion:   tls.VersionTLS13,
		})),
	}
}

// WithSpiffeIdentity is a unary interceptor that rejects every call whose
// peer does not present a verified X.509-SVID matching the pattern. Chain
// verification is performed by TLS itself (RequireAndVerifyClientCert); this
// interceptor enforces the SPIFFE ID authorization on top of it.
func WithSpiffeIdentity(pattern spiffe.Pattern) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := spiffe.CheckPeerIdentity(ctx, pattern); err != nil {
			return nil, status.Error(codes.PermissionDenied, "peer SPIFFE identity is not authorized: "+err.Error())
		}
		return handler(ctx, request)
	})
}

// ClientMTLSOptions returns the dial options for a worker: it presents its
// X.509-SVID and verifies the server against the trust bundle.
func ClientMTLSOptions(svid tls.Certificate, trustBundle *x509.CertPool) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{svid},
			RootCAs:      trustBundle,
			MinVersion:   tls.VersionTLS13,
		})),
	}
}
