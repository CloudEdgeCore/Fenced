// The transport of the adapter worker: resolving the Runtime Interface
// endpoint of an assignment and building the client that reaches it under
// the transport security boundary (loopback plaintext or pinned TLS).
package adapter

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	agentversion "github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/bindings"
	agent "github.com/CloudEdgeCore/Fenced/sdk/agent"
)

func (w *Worker) resolveRuntime(agentVersionRef string, target agentversion.RuntimeTarget) (*agent.Client, error) {
	if w.runtime != nil && w.endpoint != "" {
		return w.runtime, nil
	}
	if binding, ok := w.bindings.ResolveBinding(agentVersionRef); ok {
		tlsConfig, identity := w.bindings.TransportFor(agentVersionRef)
		return w.clientFor(binding, tlsConfig, identity)
	}
	entrypoint := target.Entrypoint[0]
	if bindings.IsLogicalEntrypoint(entrypoint) {
		return nil, fmt.Errorf("agent version %s declares logical entrypoint %q but no runtime binding resolves it",
			agentVersionRef, entrypoint)
	}
	if err := w.endpointPolicy().Validate(entrypoint); err != nil {
		return nil, fmt.Errorf("agent version %s entrypoint %q violates the runtime endpoint policy: %w",
			agentVersionRef, entrypoint, err)
	}
	if parsed, err := url.Parse(entrypoint); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Hostname() != "" {
		return w.clientFor(bindings.RuntimeBinding{Endpoint: strings.TrimRight(entrypoint, "/")}, nil, "")
	}
	return nil, fmt.Errorf("agent version %s entrypoint %q is neither bindable nor an absolute http(s) URL",
		agentVersionRef, entrypoint)
}

// endpointPolicy is the policy manifest entrypoints are validated against:
// the binding table's policy when one is loaded, otherwise production.
func (w *Worker) endpointPolicy() bindings.EndpointPolicy {
	if w.bindings != nil {
		return w.bindings.Policy
	}
	return bindings.EndpointPolicy{}
}

// clientFor builds (and caches) the Runtime Interface client of one
// endpoint. A binding with TLS material gets a dedicated transport with the
// pinned server name, private trust bundle and client certificate. The
// cache key carries the TLS material fingerprint (P1-03): two bindings that
// share an endpoint and SNI but present different certificates are
// different identities and never reuse one cached client.
func (w *Worker) clientFor(binding bindings.RuntimeBinding, tlsConfig *tls.Config, identity string) (*agent.Client, error) {
	endpoint := strings.TrimRight(binding.Endpoint, "/")
	key := endpoint
	if binding.TLSServerName != "" {
		key += "|sni=" + binding.TLSServerName
	}
	if identity != "" {
		key += "|tls=" + identity
	}
	if client, ok := w.clients[key]; ok {
		return client, nil
	}
	httpClient := w.httpClient
	if tlsConfig != nil || binding.TLSServerName != "" {
		httpClient = pinnedHTTPClient(w.httpClient, tlsConfig, binding.TLSServerName)
	}
	if err := refuseInsecureTransport(httpClient, endpoint); err != nil {
		return nil, err
	}
	client, err := agent.NewClient(endpoint, httpClient)
	if err != nil {
		return nil, err
	}
	w.clients[key] = client
	return client, nil
}

// pinnedHTTPClient clones the base client's transport with the binding's
// TLS configuration (verification name, trust bundle, client certificate).
func pinnedHTTPClient(base *http.Client, tlsConfig *tls.Config, serverName string) *http.Client {
	if base == nil {
		base = &http.Client{}
	}
	transport, ok := base.Transport.(*http.Transport)
	if ok {
		transport = transport.Clone()
	} else {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	config := tlsConfig
	if config == nil {
		config = &tls.Config{}
	}
	if serverName != "" {
		config = config.Clone()
		config.ServerName = serverName
	}
	transport.TLSClientConfig = config
	return &http.Client{Transport: transport, Timeout: base.Timeout}
}

// refuseInsecureTransport fails closed when an HTTPS endpoint would be
// dialed through a transport with certificate verification disabled: an
// intercepted (man-in-the-middle) runtime endpoint must never be accepted.
// The default transport verifies server certificates and is always allowed.
func refuseInsecureTransport(httpClient *http.Client, endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" {
		return nil
	}
	if httpClient == nil {
		return nil
	}
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		return nil
	}
	if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
		return fmt.Errorf("runtime endpoint %s refuses a transport with TLS verification disabled", endpoint)
	}
	return nil
}
