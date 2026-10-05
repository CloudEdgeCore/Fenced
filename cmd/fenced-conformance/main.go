// Command fenced-conformance certifies a running third-party adapter through
// the public Runtime Interface only.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/version"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/CloudEdgeCore/Fenced/sdk/conformance"
)

type certification struct {
	Schema         string             `json:"schema"`
	ProductVersion string             `json:"productVersion"`
	Endpoint       string             `json:"endpoint"`
	Passed         bool               `json:"passed"`
	Compatible     string             `json:"compatible"`
	StartedAt      time.Time          `json:"startedAt"`
	CompletedAt    time.Time          `json:"completedAt"`
	Report         conformance.Report `json:"report"`
}

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "fenced-conformance:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("fenced-conformance", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "", "Runtime Interface base URL")
	cmdToRun := flags.String("cmd", "", "Command to start candidate adapter server (auto-spawns and discovers URL)")
	timeout := flags.Duration("timeout", 2*time.Minute, "certification timeout")
	legacy := flags.Bool("legacy-v1alpha1", false, "certify the deprecated N-1 endpoint")
	jsonOutput := flags.Bool("json", false, "emit machine-readable JSON certification report")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *endpoint == "" && *cmdToRun == "" {
		return errors.New("either -endpoint or -cmd is required")
	}
	if *timeout <= 0 || *timeout > 10*time.Minute {
		return errors.New("timeout must be between 1ns and 10m")
	}

	targetEndpoint := *endpoint
	var cleanupProcess func()
	if *cmdToRun != "" {
		ep, cleanup, err := spawnAdapterServer(*cmdToRun, *timeout)
		if err != nil {
			return fmt.Errorf("failed to spawn adapter command: %w", err)
		}
		targetEndpoint = ep
		cleanupProcess = cleanup
		defer cleanupProcess()
	}

	client, err := agent.NewClient(targetEndpoint, nil)
	if *legacy {
		client, err = agent.NewLegacyClient(targetEndpoint, nil)
	}
	if err != nil {
		return err
	}

	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	report, runErr := conformance.Run(ctx, client)
	passed := runErr == nil
	compatStatus := "FAIL"
	if passed {
		compatStatus = "PASS"
	}

	result := certification{
		Schema:         "fenced.conformance/v1",
		ProductVersion: version.ProductVersion,
		Endpoint:       targetEndpoint,
		Passed:         passed,
		Compatible:     compatStatus,
		StartedAt:      started,
		CompletedAt:    time.Now().UTC(),
		Report:         report,
	}

	if *jsonOutput {
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encodeErr
		}
		if _, encodeErr = fmt.Fprintln(stdout, string(encoded)); encodeErr != nil {
			return encodeErr
		}
	} else {
		// Formatted certification output
		fmt.Fprintf(stdout, "\nFenced Runtime Interface Conformance Suite\n")
		fmt.Fprintf(stdout, "Adapter:  %s\n", report.Adapter)
		fmt.Fprintf(stdout, "Protocol: %s\n", report.Protocol)
		fmt.Fprintf(stdout, "Endpoint: %s\n\n", targetEndpoint)
		fmt.Fprintf(stdout, "Checks executed:\n")
		for _, check := range report.Checks {
			fmt.Fprintf(stdout, "  [PASS] %s\n", check)
		}
		fmt.Fprintf(stdout, "\nFenced Compatible = %s\n\n", compatStatus)
	}

	if runErr != nil {
		return fmt.Errorf("certification failed: %w", runErr)
	}
	return nil
}

func spawnAdapterServer(commandStr string, timeout time.Duration) (string, func(), error) {
	ctx, cancel := context.WithCancel(context.Background())
	var cmd *exec.Cmd
	parts := strings.Fields(commandStr)
	if len(parts) == 0 {
		cancel()
		return "", nil, errors.New("empty command")
	}
	if len(parts) == 1 {
		cmd = exec.CommandContext(ctx, parts[0])
	} else {
		cmd = exec.CommandContext(ctx, parts[0], parts[1:]...)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return "", nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return "", nil, err
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return "", nil, err
	}

	cleanup := func() {
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}

	urlChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
				urlChan <- line
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errChan <- err
		} else {
			errChan <- errors.New("server exited without printing URL")
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			// drain stderr
		}
	}()

	select {
	case url := <-urlChan:
		return url, cleanup, nil
	case err := <-errChan:
		cleanup()
		return "", nil, err
	case <-time.After(10 * time.Second):
		cleanup()
		return "", nil, errors.New("timeout waiting for adapter server startup")
	}
}
