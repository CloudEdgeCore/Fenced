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
	"os"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
)

func runService(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: fenced service <create|list|get|scale|restart|stop|delete|instances> [flags]")
	}
	switch args[0] {
	case "create":
		return runServiceCreate(args[1:], stdout, stderr)
	case "list":
		return runServiceList(args[1:], stdout, stderr)
	case "get":
		return runServiceGet(args[1:], stdout, stderr)
	case "scale":
		return runServiceScale(args[1:], stdout, stderr)
	case "restart":
		return runServiceRestart(args[1:], stdout, stderr)
	case "stop":
		return runServiceStop(args[1:], stdout, stderr)
	case "delete":
		return runServiceDelete(args[1:], stdout, stderr)
	case "instances":
		return runServiceInstances(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown service command %q", args[0])
	}
}

func runServiceCreate(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service create", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	name := flags.String("name", "", "Service name")
	agentID := flags.String("agent", "", "Published canonical [namespace/]name@version")
	namespace := flags.String("namespace", "default", "Service namespace")
	specPath := flags.String("spec", "", "Required workload spec JSON file (budget and placement)")
	runtimeClass := flags.String("runtime-class", "", "Optional runtime class within the published AgentVersion policy")
	replicas := flags.Int("replicas", 1, "Desired replicas")
	policy := flags.String("restart-policy", "Always", "Restart policy (Always, OnFailure, Never)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*name) == "" {
		return errors.New("-name is required")
	}
	if strings.TrimSpace(*agentID) == "" {
		return errors.New("-agent is required")
	}
	refNamespace, refName, refVersion, err := agentversion.ParseRef(*agentID)
	if err != nil {
		return fmt.Errorf("-agent: %w", err)
	}
	if agentversion.FormatRef(refNamespace, refName, refVersion) != *agentID {
		return errors.New("-agent must be canonical; omit the default/ namespace prefix")
	}
	if err := agentversion.ValidateNamespace(*namespace); err != nil {
		return fmt.Errorf("-namespace: %w", err)
	}
	if refNamespace != *namespace {
		return errors.New("-agent namespace must match -namespace")
	}
	if strings.TrimSpace(*specPath) == "" {
		return errors.New("-spec is required")
	}
	workloadSpec, err := os.ReadFile(*specPath)
	if err != nil {
		return fmt.Errorf("read workload spec: %w", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(workloadSpec, &document); err != nil || document == nil {
		return errors.New("workload spec must be a JSON object")
	}
	if *runtimeClass != "" {
		if err := agentversion.ValidateName(*runtimeClass); err != nil {
			return fmt.Errorf("-runtime-class: %w", err)
		}
	}

	serviceSpec := map[string]any{
		"replicas":        *replicas,
		"restartPolicy":   *policy,
		"agentVersionRef": *agentID,
		"workloadSpec":    json.RawMessage(workloadSpec),
	}
	if *runtimeClass != "" {
		serviceSpec["runtimeClass"] = *runtimeClass
	}
	payload := map[string]any{
		"namespace": *namespace,
		"name":      *name,
		"agentId":   *agentID,
		"spec":      serviceSpec,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, strings.TrimRight(*endpoint, "/")+"/v1/services", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create service failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "%s\n", string(body))
	return nil
}

func runServiceList(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	namespace := flags.String("namespace", "", "Filter by namespace")
	if err := flags.Parse(args); err != nil {
		return err
	}

	url := strings.TrimRight(*endpoint, "/") + "/v1/services"
	if *namespace != "" {
		url += "?namespace=" + *namespace
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list services failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "%s\n", string(body))
	return nil
}

func runServiceGet(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service get", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: fenced service get <serviceId>")
	}
	serviceID := flags.Arg(0)

	url := fmt.Sprintf("%s/v1/services/%s", strings.TrimRight(*endpoint, "/"), serviceID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get service failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "%s\n", string(body))
	return nil
}

func runServiceScale(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service scale", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	replicas := flags.Int("replicas", -1, "New replica count")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: fenced service scale <serviceId> -replicas <n>")
	}
	if *replicas < 0 {
		return errors.New("-replicas must be non-negative")
	}
	serviceID := flags.Arg(0)

	payload := map[string]any{"replicas": *replicas}
	raw, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/v1/services/%s/scale", strings.TrimRight(*endpoint, "/"), serviceID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scale service failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "%s\n", string(body))
	return nil
}

func runServiceRestart(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service restart", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: fenced service restart <serviceId>")
	}
	serviceID := flags.Arg(0)

	url := fmt.Sprintf("%s/v1/services/%s/restart", strings.TrimRight(*endpoint, "/"), serviceID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	if err != nil {
		return err
	}

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("restart service failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "%s\n", string(body))
	return nil
}

func runServiceStop(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service stop", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: fenced service stop <serviceId>")
	}
	serviceID := flags.Arg(0)

	url := fmt.Sprintf("%s/v1/services/%s/stop", strings.TrimRight(*endpoint, "/"), serviceID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stop service failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "Service %s stop requested\n", serviceID)
	return nil
}

func runServiceDelete(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service delete", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: fenced service delete <serviceId>")
	}
	serviceID := flags.Arg(0)

	url := fmt.Sprintf("%s/v1/services/%s", strings.TrimRight(*endpoint, "/"), serviceID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete service failed (%d): %s", resp.StatusCode, string(body))
	}
	fmt.Fprintf(stdout, "Service %s deleted\n", serviceID)
	return nil
}

func runServiceInstances(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("service instances", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: fenced service instances <serviceId>")
	}
	serviceID := flags.Arg(0)

	url := fmt.Sprintf("%s/v1/services/%s/instances", strings.TrimRight(*endpoint, "/"), serviceID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	setBearer(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list instances failed (%d): %s", resp.StatusCode, string(body))
	}

	var instances []supervisor.Instance
	if err := json.Unmarshal(body, &instances); err != nil {
		// Output raw JSON if decoding differs
		fmt.Fprintf(stdout, "%s\n", string(body))
		return nil
	}

	fmt.Fprintf(stdout, "INSTANCES FOR SERVICE %s (%d total):\n", serviceID, len(instances))
	for _, inst := range instances {
		fmt.Fprintf(stdout, "  - ID: %s | Phase: %s | Restarts: %d | Address: %s\n",
			inst.ID, inst.Phase, inst.RestartCount, inst.Address.String())
	}
	return nil
}
