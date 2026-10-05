// Package spiffe implements the X.509-SVID identity boundary of the Secure
// Runtime (ADR-011): every fenced gRPC peer presents a certificate whose
// SPIFFE ID names its exact principal, and every boundary verifies the chain
// against the trust bundle and matches the presented identity against the
// claims of the request. Identity shape:
//
//	spiffe://<trust-domain>/ns/<tenant>/worker/<runtime-instance>
//
// The control plane's own identity is spiffe://<trust-domain>/ns/system/control.
package spiffe

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// DefaultTrustDomain is the agent OS trust domain.
const DefaultTrustDomain = "fenced.dev"

// SystemNamespace is the SPIFFE namespace of the control plane itself.
const SystemNamespace = "system"

// CA is a self-signed SPIFFE certificate authority for one trust domain.
type CA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	trustDomain string
}

// NewCA creates a fresh CA for the trust domain. Dev/deployments generate one
// with fenced-svid; production rotates it through a real CA process.
func NewCA(trustDomain string, now time.Time, validity time.Duration) (*CA, error) {
	if strings.TrimSpace(trustDomain) == "" {
		return nil, fmt.Errorf("trust domain is required")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := randSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "fenced spiffe ca " + trustDomain},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
		URIs:                  []*url.URL{spiffeURL(trustDomain, "ca")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	return &CA{certificate: parsed, key: key, trustDomain: trustDomain}, nil
}

// Certificate returns the CA certificate.
func (c *CA) Certificate() *x509.Certificate { return c.certificate }

// TrustDomain returns the CA's trust domain.
func (c *CA) TrustDomain() string { return c.trustDomain }

// PEM encodes the CA certificate for distribution as the trust bundle.
func (c *CA) PEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.certificate.Raw})
}

// IssueSVID issues an X.509-SVID for the principal
// spiffe://<trust-domain>/ns/<tenant>/worker/<instance>. The SVID is
// dual-purpose (client and server authentication) so the control plane can
// use one for its listener and workers for their client connections.
func (c *CA) IssueSVID(tenantID, instanceID string, now time.Time, validity time.Duration) (tls.Certificate, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(instanceID) == "" {
		return tls.Certificate{}, fmt.Errorf("tenant and instance are required")
	}
	return c.IssueSVIDFor(Identity(c.trustDomain, tenantID, instanceID), now, validity)
}

// IssueSVIDFor issues an X.509-SVID for an explicit SPIFFE ID in this trust
// domain, e.g. the control plane identity spiffe://<domain>/ns/system/control.
func (c *CA) IssueSVIDFor(identity string, now time.Time, validity time.Duration) (tls.Certificate, error) {
	return c.IssueSVIDWithSANs(identity, now, validity, nil, nil)
}

// IssueSVIDWithSANs issues an SVID with additional DNS/IP subject
// alternatives so it can serve TLS endpoints (loopback development, load
// balancer DNS names). The SPIFFE URI SAN remains the identity anchor; the
// extra SANs only satisfy transport-level name verification.
func (c *CA) IssueSVIDWithSANs(identity string, now time.Time, validity time.Duration, dnsNames []string, ipAddresses []net.IP) (tls.Certificate, error) {
	parsed, err := parseSpiffeID(identity)
	if err != nil {
		return tls.Certificate{}, err
	}
	if parsed.trustDomain != c.trustDomain {
		return tls.Certificate{}, fmt.Errorf("SPIFFE ID %q is outside trust domain %q", identity, c.trustDomain)
	}
	if len(parsed.segments) < 3 || parsed.segments[0] != "ns" {
		return tls.Certificate{}, fmt.Errorf("SPIFFE ID %q is not an /ns/... identity", identity)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate SVID key: %w", err)
	}
	serial, err := randSerial()
	if err != nil {
		return tls.Certificate{}, err
	}
	uri, err := url.Parse(identity)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse SVID identity: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: identity},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{uri},
		DNSNames:     dnsNames,
		IPAddresses:  ipAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, c.certificate, &key.PublicKey, c.key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create SVID: %w", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der, c.certificate.Raw},
		PrivateKey:  key,
	}, nil
}

// Identity renders the SPIFFE ID of a worker principal.
func Identity(trustDomain, tenantID, instanceID string) string {
	return "spiffe://" + trustDomain + "/ns/" + tenantID + "/worker/" + instanceID
}

// WorkerClaims parses the tenant and runtime instance from an exact worker
// SPIFFE ID. It rejects wildcard patterns and non-worker identities.
func WorkerClaims(identity string) (trustDomain, tenantID, instanceID string, err error) {
	parsed, err := parseSpiffeID(identity)
	if err != nil {
		return "", "", "", err
	}
	if len(parsed.segments) != 4 || parsed.segments[0] != "ns" || parsed.segments[2] != "worker" ||
		parsed.segments[1] == "" || parsed.segments[3] == "" || parsed.segments[1] == "*" || parsed.segments[3] == "*" {
		return "", "", "", fmt.Errorf("SPIFFE ID %q is not an exact worker identity", identity)
	}
	return parsed.trustDomain, parsed.segments[1], parsed.segments[3], nil
}

// PeerWorkerClaims extracts an exact worker identity from the verified gRPC
// peer certificate.
func PeerWorkerClaims(ctx context.Context) (trustDomain, tenantID, instanceID string, err error) {
	identity, err := PeerIdentity(ctx)
	if err != nil {
		return "", "", "", err
	}
	return WorkerClaims(identity)
}

// ControlPlaneIdentity renders the SPIFFE ID of the control plane itself.
func ControlPlaneIdentity(trustDomain string) string {
	return "spiffe://" + trustDomain + "/ns/" + SystemNamespace + "/control"
}

// Pattern is an SPIFFE ID allow pattern; "*" matches any segment.
type Pattern struct {
	TrustDomain string
	TenantID    string
	InstanceID  string
}

// ParsePattern parses "spiffe://<trust-domain>/ns/<tenant>/worker/<instance>"
// with "*" wildcards, e.g. "spiffe://fenced.dev/ns/*/worker/*".
func ParsePattern(raw string) (Pattern, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return Pattern{}, fmt.Errorf("parse SPIFFE pattern: %w", err)
	}
	if parsed.Scheme != "spiffe" || parsed.Host == "" {
		return Pattern{}, fmt.Errorf("SPIFFE pattern must be spiffe://<trust-domain>/ns/<tenant>/worker/<instance>")
	}
	segments := splitPath(parsed.EscapedPath())
	if len(segments) != 4 || segments[0] != "ns" || segments[2] != "worker" {
		return Pattern{}, fmt.Errorf("SPIFFE pattern must be spiffe://<trust-domain>/ns/<tenant>/worker/<instance>")
	}
	return Pattern{TrustDomain: parsed.Host, TenantID: segments[1], InstanceID: segments[3]}, nil
}

// Matches reports whether the identity matches the pattern.
func (p Pattern) Matches(identity string) bool {
	parsed, err := parseSpiffeID(identity)
	if err != nil {
		return false
	}
	if !segmentMatches(p.TrustDomain, parsed.trustDomain) {
		return false
	}
	if len(parsed.segments) != 4 || parsed.segments[0] != "ns" || parsed.segments[2] != "worker" {
		return false
	}
	return segmentMatches(p.TenantID, parsed.segments[1]) && segmentMatches(p.InstanceID, parsed.segments[3])
}

func segmentMatches(pattern, value string) bool {
	return pattern == "*" || pattern == value
}

// ParsePeerIdentity extracts the SPIFFE ID from a peer's verified X.509-SVID
// (first certificate in the presented chain). It returns an error when the
// peer did not present a TLS certificate or its leaf has no single SPIFFE URI
// SAN.
func ParsePeerIdentity(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", fmt.Errorf("peer presented no client certificate")
	}
	for _, uri := range cert.URIs {
		if uri.Scheme == "spiffe" {
			return uri.String(), nil
		}
	}
	return "", fmt.Errorf("peer certificate carries no SPIFFE URI SAN")
}

// PeerIdentity extracts the SPIFFE ID of the gRPC peer from the context.
func PeerIdentity(ctx context.Context) (string, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("no gRPC peer on this call")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", fmt.Errorf("peer transport is not TLS")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", fmt.Errorf("peer presented no client certificate")
	}
	return ParsePeerIdentity(tlsInfo.State.PeerCertificates[0])
}

// CheckPeerIdentity verifies that the caller's SPIFFE ID matches the pattern.
func CheckPeerIdentity(ctx context.Context, pattern Pattern) error {
	identity, err := PeerIdentity(ctx)
	if err != nil {
		return err
	}
	if !pattern.Matches(identity) {
		return fmt.Errorf("peer SPIFFE identity %q is not authorized by %q", identity, pattern.String())
	}
	return nil
}

func (p Pattern) String() string {
	return "spiffe://" + p.TrustDomain + "/ns/" + p.TenantID + "/worker/" + p.InstanceID
}

// ParseLeaf parses the leaf of an issued SVID (tests and tooling).
func ParseLeaf(certificate tls.Certificate) (*x509.Certificate, error) {
	if len(certificate.Certificate) == 0 {
		return nil, fmt.Errorf("SVID has no certificate chain")
	}
	return x509.ParseCertificate(certificate.Certificate[0])
}

// TrustBundlePool builds an x509.CertPool from PEM certificates.
func TrustBundlePool(pemCerts [][]byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, raw := range pemCerts {
		if !pool.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("trust bundle contains an invalid certificate")
		}
	}
	if len(pemCerts) == 0 {
		return nil, fmt.Errorf("trust bundle is empty")
	}
	return pool, nil
}

// spiffeURL builds a SPIFFE URL with path segments.
func spiffeURL(trustDomain string, segments ...string) *url.URL {
	escaped := make([]string, len(segments))
	for i, segment := range segments {
		escaped[i] = url.PathEscape(segment)
	}
	return &url.URL{Scheme: "spiffe", Host: trustDomain, Path: "/" + strings.Join(escaped, "/")}
}

type parsedIdentity struct {
	trustDomain string
	segments    []string
}

func parseSpiffeID(identity string) (*parsedIdentity, error) {
	parsed, err := url.Parse(identity)
	if err != nil {
		return nil, fmt.Errorf("parse SPIFFE ID: %w", err)
	}
	if parsed.Scheme != "spiffe" || parsed.Host == "" {
		return nil, fmt.Errorf("identity must be a spiffe://<trust-domain>/... URI")
	}
	return &parsedIdentity{trustDomain: parsed.Host, segments: splitPath(parsed.EscapedPath())}, nil
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, "/")
	decoded := make([]string, len(parts))
	for i, part := range parts {
		if unescaped, err := url.PathUnescape(part); err == nil {
			decoded[i] = unescaped
		} else {
			decoded[i] = part
		}
	}
	return decoded
}

func randSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}
	return serial, nil
}
