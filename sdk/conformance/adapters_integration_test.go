//go:build integration

package conformance

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

func TestRemoteAdaptersPassTheUnifiedSuite(t *testing.T) {
	python, err := pythonExecutable()
	if err != nil {
		t.Skip(err)
	}
	root := repositoryRoot(t)
	for name, script := range map[string]string{
		"python-remote": filepath.Join(root, "examples", "agents", "python_remote", "server.py"),
		"langgraph":     filepath.Join(root, "examples", "agents", "langgraph", "server.py"),
		"a2a":           filepath.Join(root, "examples", "agents", "a2a", "server.py"),
		"autogen":       filepath.Join(root, "examples", "agents", "autogen", "server.py"),
		"crewai":        filepath.Join(root, "examples", "agents", "crewai", "server.py"),
		"openai-agents": filepath.Join(root, "examples", "agents", "openai_agents", "server.py"),
		"custom-agent":  filepath.Join(root, "examples", "agents", "custom", "server.py"),
	} {
		t.Run(name, func(t *testing.T) {
			endpoint := startPythonAdapter(t, python, root, script)
			client, err := agent.NewClient(endpoint, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			report, err := Run(ctx, client)
			if err != nil {
				t.Fatalf("%s conformance failed: %v", name, err)
			}
			if report.Adapter != name || len(report.Checks) < 10 {
				t.Fatalf("incomplete %s report: %+v", name, report)
			}
		})
	}
}

func startPythonAdapter(t *testing.T, python, root, script string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, python, "-u", script, "--port", "0")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "sdk", "python"))
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = command.Wait()
	})
	address := make(chan string, 1)
	readError := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				readError <- fmt.Errorf("adapter did not report address: %s: %w", stderr.String(), err)
			} else {
				readError <- fmt.Errorf("adapter exited before reporting address: %s", stderr.String())
			}
			return
		}
		address <- strings.TrimSpace(scanner.Text())
	}()
	select {
	case endpoint := <-address:
		return endpoint
	case err := <-readError:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatalf("adapter startup timed out: %s", stderr.String())
	}
	return ""
}

func pythonExecutable() (string, error) {
	for _, candidate := range []string{"python3", "python"} {
		if path, err := exec.LookPath(candidate); err == nil {
			if err := exec.Command(path, "--version").Run(); err == nil {
				return path, nil
			}
		}
	}
	return "", fmt.Errorf("python interpreter not found")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve conformance test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}
