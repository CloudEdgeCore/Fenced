package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
)

func runNamespace(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: fenced namespace <create|list|get|update|delete|usage> [flags]")
	}
	switch args[0] {
	case "create":
		return runNamespaceCreate(args[1:], stdout, stderr)
	case "list":
		return runNamespaceList(args[1:], stdout, stderr)
	case "get":
		return runNamespaceGet(args[1:], stdout, stderr)
	case "update":
		return runNamespaceUpdate(args[1:], stdout, stderr)
	case "delete":
		return runNamespaceDelete(args[1:], stdout, stderr)
	case "usage":
		return runNamespaceUsage(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown namespace command %q", args[0])
	}
}

func runNamespaceCreate(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("namespace create", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	name := flags.String("name", "", "Namespace name (RFC 1123 DNS-1123 label)")
	displayName := flags.String("display-name", "", "Display name")
	description := flags.String("description", "", "Description")
	maxServices := flags.Int("max-services", 0, "Max services (0 = unlimited)")
	maxTasks := flags.Int("max-tasks", 0, "Max active tasks (0 = unlimited)")
	maxTokens := flags.Int64("max-tokens", 0, "Max budget tokens (0 = unlimited)")
	maxCostMicroUSD := flags.Int64("max-cost-micro-usd", 0, "Max cost in micro-USD (0 = unlimited)")
	allowCrossIPC := flags.Bool("allow-cross-ipc", false, "Permit cross-namespace IPC")

	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*name) == "" {
		return errors.New("-name is required")
	}

	payload := map[string]any{
		"name":        *name,
		"displayName": *displayName,
		"description": *description,
		"quota": namespace.ResourceQuota{
			MaxServices:            *maxServices,
			MaxTasks:               *maxTasks,
			MaxTokens:              *maxTokens,
			MaxCostMicroUSD:        *maxCostMicroUSD,
			AllowCrossNamespaceIPC: *allowCrossIPC,
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := strings.TrimRight(*endpoint, "/") + "/v1/namespaces"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("control API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create namespace failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	fmt.Fprintln(stdout, string(respBytes))
	return nil
}

func runNamespaceList(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("namespace list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	asJSON := flags.Bool("json", false, "Output as JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := strings.TrimRight(*endpoint, "/") + "/v1/namespaces"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("control API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list namespaces failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	if *asJSON {
		fmt.Fprintln(stdout, string(respBytes))
		return nil
	}

	var list struct {
		Items []struct {
			Name        string `json:"name"`
			Phase       string `json:"phase"`
			DisplayName string `json:"displayName"`
		} `json:"items"`
	}
	if err := json.Unmarshal(respBytes, &list); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	tw := tabwriter.NewWriter(stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tPHASE\tDISPLAY NAME")
	for _, ns := range list.Items {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", ns.Name, ns.Phase, ns.DisplayName)
	}
	return tw.Flush()
}

func runNamespaceGet(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("namespace get", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	nameFlag := flags.String("name", "", "Namespace name")

	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
		args = args[1:]
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	if name == "" && flags.NArg() > 0 {
		name = flags.Arg(0)
	}
	if name == "" && *nameFlag != "" {
		name = *nameFlag
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("namespace name is required (use -name or pass as argument)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/v1/namespaces/%s", strings.TrimRight(*endpoint, "/"), name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("control API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get namespace failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	fmt.Fprintln(stdout, string(respBytes))
	return nil
}

func runNamespaceUpdate(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("namespace update", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	nameFlag := flags.String("name", "", "Namespace name")
	displayName := flags.String("display-name", "", "Display name")
	description := flags.String("description", "", "Description")
	maxServices := flags.Int("max-services", 0, "Max services (0 = unlimited)")
	maxTasks := flags.Int("max-tasks", 0, "Max active tasks (0 = unlimited)")
	maxTokens := flags.Int64("max-tokens", 0, "Max budget tokens (0 = unlimited)")
	allowCrossIPC := flags.Bool("allow-cross-ipc", false, "Permit cross-namespace IPC")

	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
		args = args[1:]
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	if name == "" && flags.NArg() > 0 {
		name = flags.Arg(0)
	}
	if name == "" && *nameFlag != "" {
		name = *nameFlag
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("namespace name is required")
	}

	payload := map[string]any{
		"displayName": *displayName,
		"description": *description,
		"quota": namespace.ResourceQuota{
			MaxServices:            *maxServices,
			MaxTasks:               *maxTasks,
			MaxTokens:              *maxTokens,
			AllowCrossNamespaceIPC: *allowCrossIPC,
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/v1/namespaces/%s", strings.TrimRight(*endpoint, "/"), name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("control API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("update namespace failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	fmt.Fprintln(stdout, string(respBytes))
	return nil
}

func runNamespaceDelete(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("namespace delete", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	nameFlag := flags.String("name", "", "Namespace name")

	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
		args = args[1:]
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	if name == "" && flags.NArg() > 0 {
		name = flags.Arg(0)
	}
	if name == "" && *nameFlag != "" {
		name = *nameFlag
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("namespace name is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/v1/namespaces/%s", strings.TrimRight(*endpoint, "/"), name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("control API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
		return fmt.Errorf("delete namespace failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	fmt.Fprintf(stdout, "namespace %s deleted\n", name)
	return nil
}

func runNamespaceUsage(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("namespace usage", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	nameFlag := flags.String("name", "", "Namespace name")

	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
		args = args[1:]
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	if name == "" && flags.NArg() > 0 {
		name = flags.Arg(0)
	}
	if name == "" && *nameFlag != "" {
		name = *nameFlag
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("namespace name is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/v1/namespaces/%s/usage", strings.TrimRight(*endpoint, "/"), name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("control API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get namespace usage failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	fmt.Fprintln(stdout, string(respBytes))
	return nil
}
