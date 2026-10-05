package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

func runWorkflow(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: fenced workflow <create|get|cancel|approve|reject|tree> [flags]")
	}
	switch args[0] {
	case "create":
		return runWorkflowCreate(args[1:], stdout, stderr)
	case "get":
		return runWorkflowGet(args[1:], stdout, stderr)
	case "cancel":
		return runWorkflowMutation(args[1:], stdout, stderr, "cancel")
	case "approve", "reject":
		return runWorkflowMutation(args[1:], stdout, stderr, args[0])
	case "tree":
		return runWorkflowTree(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown workflow command %q", args[0])
	}
}

func runWorkflowCreate(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("workflow create", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	path := flags.String("file", "workflow.json", "Workflow specification JSON file")
	goal := flags.String("goal", "", "Workflow goal")
	namespace := flags.String("namespace", "default", "Workflow namespace")
	idempotency := flags.String("idempotency-key", "", "Safe workflow idempotency key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*goal) == "" {
		return errors.New("-goal is required")
	}
	document, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	if !json.Valid(document) {
		return errors.New("workflow file is not valid JSON")
	}
	key := *idempotency
	if key == "" {
		key, err = randomKey("workflow")
		if err != nil {
			return err
		}
	}
	body := map[string]any{"namespace": *namespace, "goal": *goal, "workflow": json.RawMessage(document)}
	return controlRequest(context.Background(), http.MethodPost, *endpoint, "/v1/workflows", key, body, stdout)
}

func runWorkflowGet(args []string, stdout, stderr io.Writer) error {
	endpoint, workflowID, _, err := workflowTargetFlags("workflow get", args, stderr, false)
	if err != nil {
		return err
	}
	return controlRequest(context.Background(), http.MethodGet, endpoint, workflowPath(workflowID), "", nil, stdout)
}

func runWorkflowMutation(args []string, stdout, stderr io.Writer, action string) error {
	needsStep := action == "approve" || action == "reject"
	endpoint, workflowID, values, err := workflowTargetFlags("workflow "+action, args, stderr, needsStep)
	if err != nil {
		return err
	}
	version, err := strconv.ParseInt(values.version, 10, 64)
	if err != nil || version < 1 {
		return errors.New("-version must be a positive resource version")
	}
	path := workflowPath(workflowID) + "/cancel"
	body := any(map[string]any{})
	if needsStep {
		path = workflowPath(workflowID) + "/steps/" + url.PathEscape(values.step) + "/approval"
		body = map[string]string{"decision": action}
	}
	reply, err := controlRequestBytes(context.Background(), http.MethodPost, endpoint, path,
		map[string]string{"If-Match": fmt.Sprintf(`W/"%d"`, version)}, body)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, string(reply))
	return err
}

type workflowFlagValues struct {
	version string
	step    string
}

func workflowTargetFlags(name string, args []string, stderr io.Writer, mutation bool) (string, uuid.UUID, workflowFlagValues, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	id := flags.String("id", "", "Workflow UUID")
	values := workflowFlagValues{}
	if mutation {
		flags.StringVar(&values.step, "step", "", "Workflow step name")
	}
	flags.StringVar(&values.version, "version", "", "Current resource version")
	if err := flags.Parse(args); err != nil {
		return "", uuid.Nil, values, err
	}
	workflowID, err := uuid.Parse(strings.TrimSpace(*id))
	if err != nil {
		return "", uuid.Nil, values, errors.New("-id must be a workflow UUID")
	}
	if mutation && strings.TrimSpace(values.step) == "" {
		return "", uuid.Nil, values, errors.New("-step is required")
	}
	return *endpoint, workflowID, values, nil
}

func workflowPath(id uuid.UUID) string { return "/v1/workflows/" + id.String() }

func runWorkflowTree(args []string, stdout, stderr io.Writer) error {
	endpoint, workflowID, _, err := workflowTargetFlags("workflow tree", args, stderr, false)
	if err != nil {
		return err
	}
	reply, err := controlRequestBytes(context.Background(), http.MethodGet, endpoint, workflowPath(workflowID), nil, nil)
	if err != nil {
		return err
	}
	var document struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Steps  []struct {
			Name       string `json:"name"`
			Status     string `json:"status"`
			ParentName string `json:"parentStepName"`
			SpawnDepth int    `json:"spawnDepth"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(reply, &document); err != nil {
		return err
	}
	sort.SliceStable(document.Steps, func(i, j int) bool {
		if document.Steps[i].SpawnDepth == document.Steps[j].SpawnDepth {
			return document.Steps[i].Name < document.Steps[j].Name
		}
		return document.Steps[i].SpawnDepth < document.Steps[j].SpawnDepth
	})
	fmt.Fprintf(stdout, "%s [%s]\n", document.ID, document.Status)
	for _, step := range document.Steps {
		indent := strings.Repeat("  ", step.SpawnDepth+1)
		parent := ""
		if step.ParentName != "" {
			parent = " <- " + step.ParentName
		}
		fmt.Fprintf(stdout, "%s%s [%s]%s\n", indent, step.Name, step.Status, parent)
	}
	return nil
}
