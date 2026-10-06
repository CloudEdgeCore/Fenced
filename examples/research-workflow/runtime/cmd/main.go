// research-runtime serves the seven research workflow roles behind the
// Fenced Runtime Interface (HTTP). One process hosts the whole fleet: the
// assignment's agent version reference selects the role.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	research "github.com/CloudEdgeCore/Fenced/examples/research-workflow/runtime"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	listen := flag.String("listen", "127.0.0.1:8091", "Runtime Interface listen address")
	mcp := flag.String("mcp", "", "loopback MCP endpoint of the runtime adapter (required)")
	fast := flag.String("model-fast", "research/fast", "gateway model reference for search-tier calls")
	reader := flag.String("model-reader", "research/reader", "gateway model reference for extraction-tier calls")
	reasoning := flag.String("model-reasoning", "research/reasoning", "gateway model reference for reasoning-tier calls")
	flag.Parse()
	if *mcp == "" {
		log.Fatal("--mcp is required")
	}
	host, err := agent.NewHost(research.NewRuntime(*mcp, research.Models{
		Fast: *fast, Reader: *reader, Reasoning: *reasoning,
	}), agent.HostOptions{Adapter: "research-workflow", MaxConcurrent: 32})
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("http://%s\n", listener.Addr())
	if err := http.Serve(listener, host); err != nil {
		log.Fatal(err)
	}
}
