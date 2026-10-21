// Package console serves the management console of the control plane.
//
// The console is a built application: the sources live in web/, the build
// writes the bundle into dist/, and the control plane embeds that bundle.
// The artifact is committed, and CI rebuilds it to prove that the committed
// bundle is the one the sources produce.
//
// The console reads the control plane through the published contract: every
// call carries the bearer token of one tenant, and the console holds no
// state of its own. It loads no external asset - the fonts are the system
// fonts, the scripts and the stylesheet are served from this origin - and a
// test checks that every path it calls is described by the contract.
package console

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var assets embed.FS

// basePath is the path the console is served under.
const basePath = "/console"

// Document returns the console document.
func Document() string {
	data, err := assets.ReadFile("dist/index.html")
	if err != nil {
		return ""
	}
	return string(data)
}

// Bundle returns the application script of the console.
func Bundle() string {
	data, err := assets.ReadFile("dist/assets/app.js")
	if err != nil {
		return ""
	}
	return string(data)
}

// Styles returns the stylesheet of the console.
func Styles() string {
	data, err := assets.ReadFile("dist/assets/app.css")
	if err != nil {
		return ""
	}
	return string(data)
}

// Handler serves the console. The document and its assets are served from
// the same origin, so the content security policy can stay strict.
func Handler() http.Handler {
	files, err := fs.Sub(assets, "dist")
	if err != nil {
		// The bundle is embedded at build time; a missing one is a build
		// error, not a run time condition.
		panic("console: the embedded bundle is missing: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(files))

	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		relative := strings.TrimPrefix(request.URL.Path, basePath)
		relative = strings.TrimPrefix(relative, "/")
		if relative == "" {
			relative = "index.html"
		}
		// The console is a single document; a path that does not exist is
		// the document, so a deep link keeps working.
		if _, err := fs.Stat(files, relative); err != nil {
			relative = "index.html"
		}

		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		writer.Header().Set("Content-Security-Policy",
			"default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'")
		if path.Ext(relative) == ".html" {
			// The console reads the current tenant's state; a cached
			// document would hide a new deployment behind an old shell.
			writer.Header().Set("Cache-Control", "no-store")
		} else {
			writer.Header().Set("Cache-Control", "public, max-age=300")
		}

		if relative == "index.html" {
			// The file server would redirect the document to its directory;
			// the console is served at a stable path.
			writer.Header().Set("Content-Type", "text/html; charset=utf-8")
			if request.Method == http.MethodHead {
				return
			}
			_, _ = writer.Write([]byte(Document()))
			return
		}

		request.URL.Path = "/" + relative
		fileServer.ServeHTTP(writer, request)
	})
}
