// Package mtlsutil shares the mTLS client credential construction between
// the runtime worker commands (ADR-011 identity boundary).
package mtlsutil

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"google.golang.org/grpc/credentials"
)

// ClientCredentials builds the worker's mTLS credentials from its X.509-SVID
// and the trust bundle. It returns nil when no TLS flags were configured.
func ClientCredentials(tlsConfigured bool, tlsCert, tlsKey, trustBundle string) (credentials.TransportCredentials, error) {
	if !tlsConfigured {
		return nil, nil
	}
	if tlsCert == "" || tlsKey == "" || trustBundle == "" {
		return nil, fmt.Errorf("worker SVID certificate, key and trust bundle are required together")
	}
	svid, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
	if err != nil {
		return nil, fmt.Errorf("load worker SVID: %w", err)
	}
	bundlePEM, err := os.ReadFile(trustBundle)
	if err != nil {
		return nil, fmt.Errorf("read trust bundle: %w", err)
	}
	pool, err := spiffe.TrustBundlePool([][]byte{bundlePEM})
	if err != nil {
		return nil, fmt.Errorf("parse trust bundle: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{svid},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}
