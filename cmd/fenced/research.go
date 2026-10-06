package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/domain"
	"github.com/CloudEdgeCore/Fenced/examples/research-workflow/app/repository"
)

// researchRun is the fenced research command (design doc §17): submit one
// research goal through the application API and follow it to completion with
// a live progress timeline and a final statistics block.
func runResearch(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("research", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:9095", "research API endpoint")
	goal := flags.String("goal", "", "research goal (required)")
	maxTasks := flags.Int64("max-tasks", 0, "workflow maxTasks override (0 = template default)")
	maxTokens := flags.Int64("max-tokens", 0, "workflow maxTokens override (0 = template default)")
	poll := flags.Duration("poll", 2*time.Second, "status poll interval")
	timeout := flags.Duration("timeout", 30*time.Minute, "maximum time to wait for completion")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*goal) == "" {
		return errors.New("-goal is required")
	}
	if *poll <= 0 || *timeout <= 0 {
		return errors.New("-poll and -timeout must be positive")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	startedAt := time.Now()
	stamp := func() string {
		elapsed := time.Since(startedAt)
		return fmt.Sprintf("[%02d:%02d]", int(elapsed.Minutes()), int(elapsed.Seconds())%60)
	}

	// POST /research
	requestBody := map[string]any{"goal": *goal}
	if *maxTasks > 0 || *maxTokens > 0 {
		budget := map[string]any{}
		if *maxTasks > 0 {
			budget["maxTasks"] = *maxTasks
		}
		if *maxTokens > 0 {
			budget["maxTokens"] = *maxTokens
		}
		requestBody["budget"] = budget
	}
	encoded, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}
	var created struct {
		ResearchID string `json:"researchId"`
		WorkflowID string `json:"workflowId"`
		Status     string `json:"status"`
	}
	if err := postJSON(client, *endpoint+"/research", encoded, &created); err != nil {
		return fmt.Errorf("create research: %w", err)
	}
	fmt.Fprintf(stdout, "%s Research created %s (workflow %s)\n", stamp(), created.ResearchID, created.WorkflowID)

	lastState := ""
	lastCounts := struct{ questions, sources, evidence, gaps int }{}
	var finalView repository.RunView
	for {
		var view repository.RunView
		if err := getJSON(client, *endpoint+"/research/"+created.ResearchID, &view); err != nil {
			return fmt.Errorf("poll research: %w", err)
		}
		state := view.Run.Status
		if state != lastState {
			printResearchState(stdout, stamp(), state, view.CriticVerdict)
			lastState = state
		}
		if count := len(view.Questions); count != lastCounts.questions {
			if count > lastCounts.questions {
				fmt.Fprintf(stdout, "%s %d research questions created\n", stamp(), count)
			}
			lastCounts.questions = count
		}
		if count := len(view.Sources); count != lastCounts.sources {
			if count > lastCounts.sources {
				fmt.Fprintf(stdout, "%s %d sources discovered\n", stamp(), count)
			}
			lastCounts.sources = count
		}
		if count := len(view.Evidence); count != lastCounts.evidence {
			if count > lastCounts.evidence {
				fmt.Fprintf(stdout, "%s %d evidence records extracted\n", stamp(), count)
			}
			lastCounts.evidence = count
		}
		if count := len(view.Gaps); count != lastCounts.gaps {
			if count > lastCounts.gaps {
				fmt.Fprintf(stdout, "%s %d research gaps identified\n", stamp(), count)
			}
			lastCounts.gaps = count
		}
		if domain.RunState(state).Terminal() {
			finalView = view
			break
		}
		if time.Since(startedAt) > *timeout {
			return fmt.Errorf("research did not complete within %s (last state %s)", *timeout, state)
		}
		time.Sleep(*poll)
	}

	duration := time.Since(startedAt).Round(time.Second)
	if finalView.Run.Status == string(domain.StateCompleted) {
		// Fetch the report deliverable for the statistics block.
		var reportBody struct {
			Report struct {
				CitationCoverage float64 `json:"citationCoverage"`
				ArtifactRef      string  `json:"artifactRef"`
			} `json:"report"`
		}
		if err := getJSON(client, *endpoint+"/research/"+created.ResearchID+"/report", &reportBody); err == nil {
			fmt.Fprintf(stdout, "%s Report ready (artifact %s)\n", stamp(), reportBody.Report.ArtifactRef)
			printResearchStatistics(stdout, finalView, duration, reportBody.Report.CitationCoverage, reportBody.Report.ArtifactRef, *maxTasks, *maxTokens)
		}
	} else {
		printResearchStatistics(stdout, finalView, duration, 0, "", *maxTasks, *maxTokens)
	}
	if finalView.Run.Status != string(domain.StateCompleted) {
		return fmt.Errorf("research ended in state %s", finalView.Run.Status)
	}
	return nil
}

func printResearchState(stdout io.Writer, stamp, state, criticVerdict string) {
	phrase := map[string]string{
		string(domain.StateCreated):       "Research queued",
		string(domain.StatePlanning):      "Planner running",
		string(domain.StateResearching):   "Research in progress",
		string(domain.StateAnalyzing):     "Analyst running",
		string(domain.StateCritiquing):    "Critic running",
		string(domain.StateWriting):       "Writer running",
		string(domain.StateValidating):    "Citation validator running",
		string(domain.StateCompleted):     "Research completed",
		string(domain.StateFailed):        "Research failed",
		string(domain.StateCancelled):     "Research cancelled",
		string(domain.StateBudgetExhaust): "Research stopped: budget exhausted",
	}[state]
	if phrase == "" {
		phrase = "Status: " + state
	}
	if state == string(domain.StateCritiquing) && criticVerdict != "" {
		phrase = fmt.Sprintf("Critic: %s", criticVerdict)
	}
	fmt.Fprintf(stdout, "%s %s\n", stamp, phrase)
}

func printResearchStatistics(stdout io.Writer, view repository.RunView, duration time.Duration, coverage float64, artifactRef string, maxTasks, maxTokens int64) {
	fmt.Fprintf(stdout, "\nStatistics:\n")
	fmt.Fprintf(stdout, "Workflow Tasks     %d\n", view.StepCount)
	fmt.Fprintf(stdout, "AgentVersions      8\n")
	fmt.Fprintf(stdout, "Sources            %d\n", len(view.Sources))
	fmt.Fprintf(stdout, "Evidence           %d\n", len(view.Evidence))
	fmt.Fprintf(stdout, "Findings           %d\n", len(view.Findings))
	fmt.Fprintf(stdout, "Gaps               %d\n", len(view.Gaps))
	fmt.Fprintf(stdout, "Retries            %d\n", view.Retries)
	fmt.Fprintf(stdout, "Budget MaxTasks    %d\n", maxTasks)
	fmt.Fprintf(stdout, "Budget MaxTokens   %d\n", maxTokens)
	fmt.Fprintf(stdout, "Duration           %s\n", duration)
	if coverage > 0 {
		fmt.Fprintf(stdout, "Citation Coverage  %.0f%%\n", coverage*100)
	}
	if artifactRef != "" {
		fmt.Fprintf(stdout, "Artifact           %s\n", artifactRef)
	}
}

func postJSON(client *http.Client, endpoint string, body []byte, out any) error {
	response, err := client.Post(endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return decodeResponse(response, out)
}

func getJSON(client *http.Client, endpoint string, out any) error {
	response, err := client.Get(endpoint)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return decodeResponse(response, out)
}

func decodeResponse(response *http.Response, out any) error {
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(encoded, &problem)
		if problem.Message == "" {
			problem.Message = fmt.Sprintf("HTTP %d", response.StatusCode)
		}
		return fmt.Errorf("%s (%s)", problem.Message, problem.Code)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(encoded, out)
}
