// Command agentos is a compatibility shim for the AgentOS → Fenced rename.
//
// It forwards to the corresponding fenced binary so that an existing script,
// container entrypoint, or systemd unit that still invokes `agentos ...` keeps
// working after the upgrade. Two forms are supported:
//
//	agentos <flags...>            -> fenced <flags...>
//	agentos control <flags...>    -> fenced-control <flags...>
//
// Prefer invoking the fenced binaries directly. This shim prints a deprecation
// notice on every run and will be removed in a future major release.
//
// The shim does not translate flags or environment variables; it only resolves
// the binary. Environment aliasing is handled by the target binary through
// internal/platform/compat, so AGENTOS_* variables keep working either way.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
)

// currentCLI is the binary that replaced the legacy `agentos` command.
const currentCLI = "fenced"

// serverSubcommands maps a legacy `agentos <name> ...` invocation to the
// fenced binary that replaced it.
var serverSubcommands = map[string]string{
	"conformance":       "fenced-conformance",
	"control":           "fenced-control",
	"controller":        "fenced-controller",
	"gateway":           "fenced-gateway",
	"migrate":           "fenced-migrate",
	"orchestrator":      "fenced-orchestrator",
	"outbox":            "fenced-outbox",
	"pkg":               "fenced-pkg",
	"projector":         "fenced-projector",
	"runtime-adapter":   "fenced-runtime-adapter",
	"runtime-control":   "fenced-runtime-control",
	"runtime-oci":       "fenced-runtime-oci",
	"runtime-reference": "fenced-runtime-reference",
	"slo":               "fenced-slo",
	"svid":              "fenced-svid",
}

// resolveTarget decides which binary to run and with which arguments. A leading
// argument that names a server subcommand selects that binary; anything else is
// forwarded verbatim to the main CLI.
func resolveTarget(args []string) (binary string, rest []string) {
	if len(args) > 0 {
		if name, ok := serverSubcommands[args[0]]; ok {
			return name, args[1:]
		}
	}
	return currentCLI, args
}

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())

	binary, args := resolveTarget(os.Args[1:])

	path, err := resolveBinary(binary)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agentos: %v\n", err)
		os.Exit(127)
	}

	fmt.Fprintf(os.Stderr, "agentos: \"agentos\" was renamed to %q; forwarding to %s. "+
		"This shim will be removed in a future major release.\n", currentCLI, path)

	cmd := exec.Command(path, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "agentos: %v\n", err)
		os.Exit(1)
	}
}

// resolveBinary finds the named fenced binary next to the running executable
// first, so a single installation directory is self-contained, then falls back
// to PATH.
func resolveBinary(name string) (string, error) {
	filename := executableName(name)
	if self, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(self), filename)
		if info, statErr := os.Stat(candidate); statErr == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	if path, err := exec.LookPath(filename); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("cannot find the %q binary; build or install the Fenced binaries and retry", filename)
}

func executableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}
