// Command fenced-svid generates the X.509-SVID identity material for the
// Secure Runtime boundary (ADR-011): a self-signed SPIFFE CA, a control plane
// SVID (with loopback IP SAN), and one worker SVID per runtime instance.
//
// Usage:
//
//	fenced-svid -out ./svid -tenant tenant-a -worker worker-1 [-worker worker-2 ...]
//
// The trust bundle is ca.pem. The control plane runs with control-cert.pem /
// control-key.pem and -trust-bundle ca.pem; each worker runs with
// worker-<id>-cert.pem / worker-<id>-key.pem. For production, rotate the CA
// through a real CA process and keep private keys in a secret manager; the
// trust bundle is the same PEM file distributed to every boundary.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	outDir := flag.String("out", "svid", "output directory for CA, control plane and worker SVID material")
	trustDomain := flag.String("trust-domain", spiffe.DefaultTrustDomain, "SPIFFE trust domain")
	tenantID := flag.String("tenant", "", "tenant namespace of the workers (repeat -worker per instance)")
	var workers []string
	flag.Func("worker", "runtime instance ID to issue a worker SVID for (repeatable)", func(value string) error {
		workers = append(workers, value)
		return nil
	})
	validity := flag.Duration("validity", 90*24*time.Hour, "SVID validity")
	serverIP := flag.String("server-ip", "127.0.0.1", "IP SAN for the control plane SVID (loopback dev)")
	flag.Parse()

	if strings.TrimSpace(*tenantID) == "" || len(workers) == 0 {
		fmt.Fprintln(os.Stderr, "fenced-svid: -tenant and at least one -worker are required")
		os.Exit(2)
	}
	if *validity <= 0 {
		fmt.Fprintln(os.Stderr, "fenced-svid: -validity must be positive")
		os.Exit(2)
	}
	now := time.Now()
	ca, err := spiffe.NewCA(*trustDomain, now, *validity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fenced-svid: create CA: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "fenced-svid: create output directory: %v\n", err)
		os.Exit(1)
	}
	if err := writeFile(filepath.Join(*outDir, "ca.pem"), ca.PEM()); err != nil {
		fmt.Fprintf(os.Stderr, "fenced-svid: %v\n", err)
		os.Exit(1)
	}
	serverIPs := []net.IP{net.ParseIP(*serverIP)}
	controlSVID, err := ca.IssueSVIDWithSANs(spiffe.ControlPlaneIdentity(*trustDomain), now, *validity, nil, serverIPs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fenced-svid: issue control plane SVID: %v\n", err)
		os.Exit(1)
	}
	if err := writeKeyPair(filepath.Join(*outDir, "control-cert.pem"), filepath.Join(*outDir, "control-key.pem"), controlSVID); err != nil {
		fmt.Fprintf(os.Stderr, "fenced-svid: %v\n", err)
		os.Exit(1)
	}
	for _, worker := range workers {
		svid, err := ca.IssueSVID(*tenantID, worker, now, *validity)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fenced-svid: issue SVID for %s: %v\n", worker, err)
			os.Exit(1)
		}
		if err := writeKeyPair(filepath.Join(*outDir, "worker-"+worker+"-cert.pem"), filepath.Join(*outDir, "worker-"+worker+"-key.pem"), svid); err != nil {
			fmt.Fprintf(os.Stderr, "fenced-svid: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("fenced-svid: wrote %d SVIDs to %s (trust domain %s, tenant %s)\n", len(workers)+1, *outDir, *trustDomain, *tenantID)
	fmt.Printf("  trust bundle:   %s\n", filepath.Join(*outDir, "ca.pem"))
	fmt.Printf("  control plane:  %s / %s\n", filepath.Join(*outDir, "control-cert.pem"), filepath.Join(*outDir, "control-key.pem"))
	for _, worker := range workers {
		fmt.Printf("  worker %-10s %s / %s\n", worker, filepath.Join(*outDir, "worker-"+worker+"-cert.pem"), filepath.Join(*outDir, "worker-"+worker+"-key.pem"))
	}
}

func writeFile(path string, content []byte) error {
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func writeKeyPair(certPath, keyPath string, svid tls.Certificate) error {
	leaf, err := spiffe.ParseLeaf(svid)
	if err != nil {
		return fmt.Errorf("parse SVID leaf: %w", err)
	}
	certPEM := pemEncodeCert(leaf)
	keyPEM, err := pemEncodeKey(svid)
	if err != nil {
		return err
	}
	if err := writeFile(certPath, certPEM); err != nil {
		return err
	}
	return writeFile(keyPath, keyPEM)
}

func pemEncodeCert(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

func pemEncodeKey(svid tls.Certificate) ([]byte, error) {
	keyDER, err := x509.MarshalPKCS8PrivateKey(svid.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}
