// Package console serves the operator console of the control plane.
//
// The console is a static document: it carries the design system, the
// theme and the bilingual strings, and it reads the control plane's own
// endpoints for everything else. It never holds state of its own, and it
// never invents a number - every value it shows comes from the same API an
// operator script would call.
//
// The document is served without authentication because it holds no data;
// every request it makes carries the operator's own bearer token.
package console

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed dashboard.html
var document string

// Document returns the operator console document.
func Document() string { return document }

// Handler serves the console document. The path is ignored: the document
// owns its own routing, and the operator reaches it through a stable URL.
func Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The console reads the current tenant's state; a cached document
		// would hide a new deployment behind an old shell.
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Content-Security-Policy",
			"default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:")
		if request.Method == http.MethodHead {
			return
		}
		_, _ = writer.Write([]byte(strings.TrimSpace(document)))
	})
}
