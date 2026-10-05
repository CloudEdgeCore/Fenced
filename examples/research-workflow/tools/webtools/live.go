package webtools

// Live backends for the research tools (roadmap P1): real internet search
// via provider adapters and a hardened HTTP fetcher. The agents never see
// any of this — they keep calling web.search@1.0.0 / web.fetch@1.0.0.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

const (
	liveMaxRedirects   = 5
	liveMaxBodyBytes   = 10 << 20 // 10 MiB hard cap on fetched bodies
	liveFetchTimeout   = 20 * time.Second
	braveSearchAPI     = "https://api.search.brave.com/res/v1/web/search"
	bingSearchAPI      = "https://api.bing.microsoft.com/v7.0/search"
	doubaoSearchAPI    = "https://open.feedcoopapi.com/search_api/web_search"
	doubaoMinInterval  = 250 * time.Millisecond // 4 QPS, below the documented default 5 QPS quota
	liveUserAgent      = "Fenced-Research/1.0 (+https://fenced.example/bot)"
	liveSourceIDPrefix = "live-"
)

// SearchProvider produces ranked hits for one query.
type SearchProvider interface {
	Search(context.Context, string, int) ([]SearchHit, error)
}

// FetchProvider downloads one target into a readable document.
type FetchProvider interface {
	Fetch(context.Context, string) (*FetchResult, error)
}

// CompositeBackend pairs one search provider with one fetch provider —
// the standard live assembly (e.g. BraveSearch + LiveFetch).
type CompositeBackend struct {
	SearchProvider SearchProvider
	FetchProvider  FetchProvider
}

// Search delegates to the search provider.
func (c *CompositeBackend) Search(ctx context.Context, query string, limit int) ([]SearchHit, error) {
	return c.SearchProvider.Search(ctx, query, limit)
}

// Fetch delegates to the fetch provider.
func (c *CompositeBackend) Fetch(ctx context.Context, target string) (*FetchResult, error) {
	return c.FetchProvider.Fetch(ctx, target)
}

// BraveSearch implements Backend.Search against the Brave Search API.
type BraveSearch struct {
	APIKey   string
	Endpoint string // overridable for tests; defaults to braveSearchAPI
	Client   *http.Client
}

// Search queries Brave and maps results to the unified hit shape.
func (b *BraveSearch) Search(ctx context.Context, query string, limit int) ([]SearchHit, error) {
	endpoint := b.Endpoint
	if endpoint == "" {
		endpoint = braveSearchAPI
	}
	client := b.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		endpoint+"?q="+url.QueryEscape(query)+"&count="+fmt.Sprint(min(limit, 20)), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Subscription-Token", b.APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", liveUserAgent)
	var document struct {
		Web struct {
			Results []struct {
				Title       string `json:"title"`
				URL         string `json:"url"`
				Description string `json:"description"`
				Age         string `json:"age,omitempty"`
			} `json:"results"`
		} `json:"web"`
	}
	if err := decodeLiveJSON(client, req, &document); err != nil {
		return nil, err
	}
	return mapLiveHits(document.Web.Results, func(item struct {
		Title       string `json:"title"`
		URL         string `json:"url"`
		Description string `json:"description"`
		Age         string `json:"age,omitempty"`
	}) (string, string, string) {
		return item.Title, item.URL, item.Description
	}), nil
}

// BingSearch implements Backend.Search against Azure Bing Search v7.
type BingSearch struct {
	APIKey   string
	Endpoint string
	Client   *http.Client
}

// Search queries Bing and maps results to the unified hit shape.
func (b *BingSearch) Search(ctx context.Context, query string, limit int) ([]SearchHit, error) {
	endpoint := b.Endpoint
	if endpoint == "" {
		endpoint = bingSearchAPI
	}
	client := b.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		endpoint+"?q="+url.QueryEscape(query)+"&count="+fmt.Sprint(min(limit, 20)), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Ocp-Apim-Subscription-Key", b.APIKey)
	req.Header.Set("User-Agent", liveUserAgent)
	var document struct {
		WebPages struct {
			Value []struct {
				Name    string `json:"name"`
				URL     string `json:"url"`
				Snippet string `json:"snippet"`
			} `json:"value"`
		} `json:"webPages"`
	}
	if err := decodeLiveJSON(client, req, &document); err != nil {
		return nil, err
	}
	hits := make([]SearchHit, 0, len(document.WebPages.Value))
	for _, item := range document.WebPages.Value {
		if item.URL == "" {
			continue
		}
		hits = append(hits, SearchHit{
			SourceID: liveSourceID(item.URL), Title: item.Name, URL: item.URL, Snippet: item.Snippet,
		})
	}
	return hits, nil
}

// DoubaoSearch implements Backend.Search against Volcengine Doubao Search
// Custom. The public tool contract remains web.search@1.0.0; only this
// provider adapter knows the vendor request/response envelope.
type DoubaoSearch struct {
	APIKey   string
	Endpoint string // overridable for tests; defaults to doubaoSearchAPI
	Client   *http.Client
	rateMu   sync.Mutex
	nextCall time.Time
}

func (d *DoubaoSearch) waitForQuota(ctx context.Context) error {
	d.rateMu.Lock()
	now := time.Now()
	scheduled := now
	if d.nextCall.After(scheduled) {
		scheduled = d.nextCall
	}
	d.nextCall = scheduled.Add(doubaoMinInterval)
	d.rateMu.Unlock()
	if delay := time.Until(scheduled); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

// Search queries Doubao Search Custom using its recommended API-key access
// mode. Search results without an original URL are excluded because the
// research workflow must be able to fetch and independently ground them.
func (d *DoubaoSearch) Search(ctx context.Context, query string, limit int) ([]SearchHit, error) {
	if strings.TrimSpace(d.APIKey) == "" {
		return nil, fmt.Errorf("doubao search API key is required")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("doubao search query is required")
	}
	if err := d.waitForQuota(ctx); err != nil {
		return nil, fmt.Errorf("doubao search quota wait: %w", err)
	}
	endpoint := d.Endpoint
	if endpoint == "" {
		endpoint = doubaoSearchAPI
	}
	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}
	body, err := json.Marshal(struct {
		Query      string `json:"Query"`
		SearchType string `json:"SearchType"`
		Count      int    `json:"Count"`
		Filter     struct {
			NeedContent bool `json:"NeedContent"`
			NeedURL     bool `json:"NeedUrl"`
		} `json:"Filter"`
	}{Query: query, SearchType: "web", Count: min(max(limit, 1), 50), Filter: struct {
		NeedContent bool `json:"NeedContent"`
		NeedURL     bool `json:"NeedUrl"`
	}{NeedContent: false, NeedURL: true}})
	if err != nil {
		return nil, fmt.Errorf("encode doubao search request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(d.APIKey))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", liveUserAgent)
	var document struct {
		ResponseMetadata struct {
			RequestID string `json:"RequestId"`
			Error     *struct {
				Code    string `json:"Code"`
				Message string `json:"Message"`
			} `json:"Error"`
		} `json:"ResponseMetadata"`
		Result *struct {
			WebResults []struct {
				Title       string `json:"Title"`
				URL         string `json:"Url"`
				Snippet     string `json:"Snippet"`
				Summary     string `json:"Summary"`
				PublishTime string `json:"PublishTime"`
			} `json:"WebResults"`
		} `json:"Result"`
	}
	if err := decodeLiveJSON(client, request, &document); err != nil {
		return nil, err
	}
	if document.ResponseMetadata.Error != nil {
		providerErr := document.ResponseMetadata.Error
		return nil, fmt.Errorf("doubao search error %s: %s",
			providerErr.Code, strings.TrimSpace(providerErr.Message))
	}
	if document.Result == nil {
		return nil, fmt.Errorf("doubao search response has no result (request %s)",
			document.ResponseMetadata.RequestID)
	}
	hits := make([]SearchHit, 0, len(document.Result.WebResults))
	for _, item := range document.Result.WebResults {
		if strings.TrimSpace(item.URL) == "" {
			continue
		}
		snippet := strings.TrimSpace(item.Snippet)
		if snippet == "" {
			snippet = strings.TrimSpace(item.Summary)
		}
		hits = append(hits, SearchHit{
			SourceID: liveSourceID(item.URL), Title: item.Title, URL: item.URL,
			Snippet: snippet, PublishedAt: item.PublishTime,
		})
	}
	return hits, nil
}

func mapLiveHits[T any](items []T, fields func(T) (string, string, string)) []SearchHit {
	hits := make([]SearchHit, 0, len(items))
	for _, item := range items {
		title, rawURL, snippet := fields(item)
		if rawURL == "" {
			continue
		}
		hits = append(hits, SearchHit{
			SourceID: liveSourceID(rawURL), Title: title, URL: rawURL, Snippet: snippet,
		})
	}
	return hits
}

func decodeLiveJSON(client *http.Client, request *http.Request, target any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("search api status %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(io.LimitReader(response.Body, liveMaxBodyBytes)).Decode(target)
}

// liveSourceID derives a stable id from the URL so evidence records stay
// addressable across runs without central coordination.
func liveSourceID(rawURL string) string {
	digest := sha256.Sum256([]byte(rawURL))
	return liveSourceIDPrefix + hex.EncodeToString(digest[:])[:12]
}

// LiveFetch is the hardened real-web fetcher: SSRF policy, bounded
// redirects to public hosts only, size cap, timeout, content-type checks,
// HTML→readable-text extraction, final URL preserved.
type LiveFetch struct {
	Client *http.Client

	// Test seams are intentionally private: production callers always use the
	// system resolver and net.Dialer, while package tests can prove pinning
	// without making real connections to documentation-only public addresses.
	resolver    hostResolver
	dialContext func(context.Context, string, string) (net.Conn, error)
}

type fetchPolicyError struct{ reason string }

func (e *fetchPolicyError) Error() string { return "blocked by fetch policy: " + e.reason }

type fetchStatusError struct {
	status int
	detail string
}

func (e *fetchStatusError) Error() string {
	if e.detail != "" {
		return e.detail
	}
	return fmt.Sprintf("upstream status %d", e.status)
}

type fetchContentTypeError struct{ mediaType string }

func (e *fetchContentTypeError) Error() string {
	return fmt.Sprintf("unsupported content type %q", e.mediaType)
}

// Fetch downloads one public document and returns readable text.
func (l *LiveFetch) Fetch(ctx context.Context, target string) (*FetchResult, error) {
	if _, reason := validateFetchURL(target); reason != "" {
		return nil, &fetchPolicyError{reason: reason}
	}
	client, err := l.pinnedClient()
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", liveUserAgent)
	request.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain,application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, &fetchStatusError{status: response.StatusCode}
	}
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(
		response.Header.Get("Content-Type"), ";", 2)[0]))
	switch mediaType {
	case "text/html", "application/xhtml+xml", "text/plain", "application/json", "":
	default:
		return nil, &fetchContentTypeError{mediaType: mediaType}
	}
	body, err := clampBody(response.Body)
	if err != nil {
		return nil, err
	}
	finalURL := response.Request.URL.String()
	content, title := extractReadable(body, mediaType)
	return &FetchResult{
		SourceID: liveSourceID(finalURL), Title: title, URL: finalURL,
		Content: content, FetchedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// LiveHTTPClient returns the production fetch client: bounded redirects to
// public hosts only, request timeout, and the research user agent contract.
func LiveHTTPClient() *http.Client {
	fetch := &LiveFetch{}
	client, err := fetch.pinnedClient()
	if err != nil {
		panic(err) // the default transport is always cloneable
	}
	return client
}

// pinnedClient constructs a client whose only network path is the guarded
// DialContext below. Proxy and DialTLS hooks are disabled because either can
// bypass address pinning and delegate target DNS resolution elsewhere.
func (l *LiveFetch) pinnedClient() (*http.Client, error) {
	client := &http.Client{}
	if l.Client != nil {
		*client = *l.Client
	}
	client.Timeout = liveFetchTimeout

	var transport *http.Transport
	switch configured := client.Transport.(type) {
	case nil:
		transport = http.DefaultTransport.(*http.Transport).Clone()
	case *http.Transport:
		transport = configured.Clone()
	default:
		return nil, fmt.Errorf("live fetch requires an *http.Transport to enforce DNS pinning")
	}
	resolver := l.resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	dial := l.dialContext
	if dial == nil {
		dialer := &net.Dialer{Timeout: liveFetchTimeout, KeepAlive: 30 * time.Second}
		dial = dialer.DialContext
	}
	transport.Proxy = nil
	pinnedDial := pinnedDialContext(resolver, dial)
	transport.DialContext = pinnedDial
	// A non-nil DialTLSContext takes precedence over the legacy DialTLS hook.
	// Performing the handshake here prevents a caller-supplied legacy hook
	// from bypassing address validation while preserving hostname/SNI checks.
	transport.DialTLSContext = pinnedTLSDialContext(pinnedDial, transport.TLSClientConfig)
	client.Transport = transport

	configuredRedirect := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > liveMaxRedirects {
			return fmt.Errorf("more than %d redirects", liveMaxRedirects)
		}
		if _, reason := validateFetchURL(request.URL.String()); reason != "" {
			return &fetchPolicyError{reason: "redirect: " + reason}
		}
		if configuredRedirect != nil {
			return configuredRedirect(request, via)
		}
		return nil
	}
	return client, nil
}

func pinnedTLSDialContext(dial func(context.Context, string, string) (net.Conn, error), baseConfig *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("invalid TLS dial address %q: %w", address, err)
		}
		config := &tls.Config{MinVersion: tls.VersionTLS12}
		if baseConfig != nil {
			config = baseConfig.Clone()
			if config.MinVersion == 0 {
				config.MinVersion = tls.VersionTLS12
			}
		}
		if config.ServerName == "" {
			config.ServerName = host
		}
		secured := tls.Client(raw, config)
		if err := secured.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		return secured, nil
	}
}

// pinnedDialContext makes DNS validation and network use one indivisible
// path: resolve every address, reject the whole mixed answer if any member is
// unsafe, then pass the selected numeric IP—not the hostname—to net.Dialer.
func pinnedDialContext(resolver hostResolver, dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("invalid dial address %q: %w", address, err)
		}
		addresses, err := resolvePublicIPs(ctx, resolver, host)
		if err != nil {
			var resolution *hostResolutionError
			if errors.As(err, &resolution) {
				return nil, resolution
			}
			return nil, &fetchPolicyError{reason: err.Error()}
		}
		pinned := net.JoinHostPort(addresses[0].IP.String(), port)
		return dial(ctx, network, pinned)
	}
}

// clampBody reads at most liveMaxBodyBytes from r and rejects larger
// payloads outright.
func clampBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, liveMaxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > liveMaxBodyBytes {
		return nil, fmt.Errorf("body exceeds %d bytes", liveMaxBodyBytes)
	}
	return body, nil
}

// extractReadable converts an HTML page into plain text (scripts/styles
// dropped, block structure kept as line breaks) or passes text/json through;
// it also pulls out the <title> when present.
func extractReadable(body []byte, mediaType string) (string, string) {
	if mediaType == "text/plain" || mediaType == "application/json" || mediaType == "" {
		return strings.TrimSpace(string(body)), ""
	}
	var text strings.Builder
	title := ""
	var walk func(*html.Node)
	skip := map[string]bool{"script": true, "style": true, "noscript": true, "svg": true}
	blockLevel := map[string]bool{
		"p": true, "div": true, "br": true, "li": true, "h1": true, "h2": true,
		"h3": true, "h4": true, "section": true, "article": true, "tr": true,
		"blockquote": true, "pre": true, "table": true,
	}
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode {
			if skip[node.Data] {
				return
			}
			if node.Data == "title" && node.FirstChild != nil {
				title = strings.TrimSpace(node.FirstChild.Data)
			}
			if blockLevel[node.Data] && text.Len() > 0 {
				text.WriteString("\n")
			}
		}
		if node.Type == html.TextNode {
			line := strings.TrimSpace(node.Data)
			if line != "" {
				text.WriteString(line)
				text.WriteString(" ")
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
		if node.Type == html.ElementNode && blockLevel[node.Data] {
			text.WriteString("\n")
		}
	}
	if parsed, err := html.Parse(bytes.NewReader(body)); err == nil {
		walk(parsed)
	}
	lines := strings.Split(text.String(), "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			cleaned = append(cleaned, line)
		}
	}
	return strings.Join(cleaned, "\n"), title
}
