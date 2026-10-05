// Command research-api is the application-layer Research REST API of the
// multi-agent research workflow (design doc §13). It is a reference-application
// server: it talks to the Control API v1 as a client and never touches kernel
// internals or the database directly.
//
//	research-api \
//	  -control-endpoint http://127.0.0.1:8080 \
//	  -listen 127.0.0.1:9095 \
//	  -workflow-template workflow/research-workflow.json \
//	  -artifact-root tmp/research-artifacts
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/api"
	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/repository"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	listen := flag.String("listen", "127.0.0.1:9095", "HTTP listen address for the research API")
	controlEndpoint := flag.String("control-endpoint", "http://127.0.0.1:8080", "Control API v1 endpoint")
	controlToken := flag.String("control-token", os.Getenv("FENCED_TOKEN"), "bearer token for the Control API (or FENCED_TOKEN)")
	templatePath := flag.String("workflow-template", "workflow/research-workflow.json", "path to the research workflow template")
	artifactRoot := flag.String("artifact-root", "tmp/research-artifacts", "artifact store root (report artifacts + mapping files)")
	tenant := flag.String("tenant", "research-tenant", "tenant the reports are stored under")
	namespace := flag.String("namespace", "default", "namespace for created workflows")
	deadline := flag.Duration("deadline", 45*time.Minute, "research workflow deadline window")
	flag.Parse()

	template, err := os.ReadFile(*templatePath)
	if err != nil {
		log.Fatalf("read workflow template: %v", err)
	}
	if err := os.MkdirAll(*artifactRoot, 0o755); err != nil {
		log.Fatalf("create artifact root: %v", err)
	}
	artifacts, err := artifact.NewFilesystem(*artifactRoot, 64<<20)
	if err != nil {
		log.Fatalf("artifact store: %v", err)
	}
	control := repository.NewClient(*controlEndpoint, nil, *controlToken)
	server := api.NewServer(control, template, artifacts, *tenant, *namespace, *deadline, *artifactRoot)

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("research-api serving on %s (control %s, tenant %s)", *listen, *controlEndpoint, *tenant)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
