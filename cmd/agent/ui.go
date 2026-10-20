package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// controlEndpoint resolves the address of the control plane: the
// FENCED_CONTROL_URL environment variable, or the default the stable CLI
// uses.
func controlEndpoint() string {
	if endpoint := strings.TrimSpace(os.Getenv("FENCED_CONTROL_URL")); endpoint != "" {
		return endpoint
	}
	return "http://127.0.0.1:8080"
}

// runUI points the operator at the console of a running control plane.
//
// The console is served by the control plane itself
// (internal/control/console): it reads the control plane's own endpoints, so
// every number it shows is the number the kernel holds. The developer
// configuration surface stays on the command line, where it belongs:
// `agent config`, `agent config wizard` and `agent test-llm`.
func runUI(args []string) {
	controlURL := strings.TrimRight(controlEndpoint(), "/")
	parsed, err := url.Parse(controlURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		fmt.Fprintf(os.Stderr, "[error] the control plane address %q is not an absolute URL\n", controlURL)
		os.Exit(2)
	}
	consoleURL := controlURL + "/console/"

	fmt.Println("Fenced operator console")
	fmt.Println("  console:  " + consoleURL)
	fmt.Println("  api:      " + controlURL + "/v1")
	fmt.Println()
	fmt.Println("The console reads the control plane with your own bearer token: open the")
	fmt.Println("console, paste a token, and press Connect. The token stays in the")
	fmt.Println("browser session and is never sent to the server except as a header.")
	fmt.Println()
	fmt.Println("  operator CLI:  agent config | agent config wizard | agent test-llm")
	fmt.Println("  read the API:  " + controlURL + "/v1/tasks")
}
