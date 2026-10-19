package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type UIReceipt struct {
	ReceiptID  string  `json:"receipt_id"`
	Timestamp  string  `json:"timestamp"`
	Task       string  `json:"task"`
	Caller     string  `json:"caller"`
	Model      string  `json:"model"`
	DurationMs int64   `json:"duration_ms"`
	Tokens     int     `json:"tokens"`
	CostUSD    float64 `json:"cost_usd"`
	Signature  string  `json:"signature"`
	Status     string  `json:"status"`
	Output     string  `json:"output"`
}

type UIRunRequest struct {
	Prompt   string `json:"prompt"`
	Model    string `json:"model,omitempty"`
	Provider string `json:"provider,omitempty"`
}

type UIRunResponse struct {
	Success bool      `json:"success"`
	Receipt UIReceipt `json:"receipt"`
	Error   string    `json:"error,omitempty"`
}

type UIModelConfigRequest struct {
	Provider  string  `json:"provider"`
	Model     string  `json:"model"`
	BaseURL   string  `json:"base_url"`
	Protocol  string  `json:"protocol,omitempty"`
	APIKey    string  `json:"api_key"`
	BudgetUSD float64 `json:"budget_usd"`
}

type UIFullConfigRequest struct {
	Environment     string  `json:"environment"`
	DefaultProvider string  `json:"default_provider"`
	Model           string  `json:"model"`
	BaseURL         string  `json:"base_url"`
	Protocol        string  `json:"protocol,omitempty"`
	APIKey          string  `json:"api_key"`
	TimeoutSec      int     `json:"timeout_sec"`
	BudgetUSD       float64 `json:"budget_usd"`
	MaxTokens       int     `json:"max_tokens"`
	MaxToolCalls    int     `json:"max_tool_calls"`
	EnforceReceipts bool    `json:"enforce_receipts"`
	FailClosed      bool    `json:"fail_closed"`
	LogLevel        string  `json:"log_level"`
	LogFormat       string  `json:"log_format"`
	GatewayPort     int     `json:"gateway_port"`
	GatewayHost     string  `json:"gateway_host"`
}

type UIToolActionRequest struct {
	Action      string `json:"action"` // "register" or "toggle"
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Adapter     string `json:"adapter"`
	Protocol    string `json:"protocol"`
	Endpoint    string `json:"endpoint,omitempty"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
}

type UIAgentItem struct {
	Name      string   `json:"name"`
	Role      string   `json:"role"`
	Model     string   `json:"model"`
	BudgetUSD float64  `json:"budget_usd"`
	Latency   string   `json:"latency"`
	Status    string   `json:"status"`
	Tools     []string `json:"tools,omitempty"`
}

type UICreateAgentRequest struct {
	Name         string   `json:"name"`
	Role         string   `json:"role"`
	Model        string   `json:"model"`
	BudgetUSD    float64  `json:"budget_usd"`
	SystemPrompt string   `json:"system_prompt,omitempty"`
	Tools        []string `json:"tools,omitempty"`
}

var (
	uiStartTime = time.Now()
	uiMu        sync.RWMutex
	uiRedline   = false
	uiAgents    = []UIAgentItem{
		{Name: "Quality-Tracer-01", Role: "Defect RCA & SOP Traceability", Model: "deepseek/deepseek-r1", BudgetUSD: 2.00, Latency: "22ms", Status: "ONLINE"},
		{Name: "CNC-Spindle-Guard", Role: "High-Freq Vibration Telemetry", Model: "deepseek/deepseek-r1", BudgetUSD: 1.50, Latency: "18ms", Status: "ONLINE"},
		{Name: "Ingress-Gateway", Role: "Task Admission & Rate Guard", Model: "stealth/space-bunny-alpha", BudgetUSD: 1.00, Latency: "1.2ms", Status: "ONLINE"},
		{Name: "Ledger-Verifier", Role: "Cryptographic Merkle Notary", Model: "deterministic-go", BudgetUSD: 0.50, Latency: "0.4ms", Status: "ONLINE"},
	}
	uiToolsMu sync.RWMutex
	uiTools   = []map[string]any{
		{
			"id":          "tool-1",
			"name":        "industrial:sensor",
			"adapter":     "industrial.sensor.query@1.0.0",
			"protocol":    "MCP/2.0",
			"status":      "HEALTHY",
			"enabled":     true,
			"description": "Query live industrial telemetry including spindle temperature, motor vibration, and cooling pressure.",
		},
		{
			"id":          "tool-2",
			"name":        "industrial:alarm",
			"adapter":     "industrial.alarm.lookup@1.0.0",
			"protocol":    "MCP/2.0",
			"status":      "HEALTHY",
			"enabled":     true,
			"description": "Look up equipment fault codes, alarm thresholds, and recommended hardware mitigations.",
		},
		{
			"id":          "tool-3",
			"name":        "industrial:sop",
			"adapter":     "industrial.sop.search@1.0.0",
			"protocol":    "MCP/2.0",
			"status":      "HEALTHY",
			"enabled":     true,
			"description": "Semantic search across standard operating procedures (SOP), safety guidelines, and work instructions.",
		},
		{
			"id":          "tool-4",
			"name":        "quality:metrics",
			"adapter":     "custom.quality.metrics@1.0.0",
			"protocol":    "Native/Go",
			"status":      "HEALTHY",
			"enabled":     true,
			"description": "Retrieve Statistical Process Control (SPC) metrics, Cp/Cpk indices, and defect rates.",
		},
		{
			"id":          "tool-5",
			"name":        "process:telemetry",
			"adapter":     "custom.process.telemetry@1.0.0",
			"protocol":    "Native/Go",
			"status":      "HEALTHY",
			"enabled":     true,
			"description": "Stream high-frequency process time-series data from edge collectors.",
		},
		{
			"id":          "tool-6",
			"name":        "knowledge:cases",
			"adapter":     "custom.knowledge.cases@1.0.0",
			"protocol":    "pgvector/SQL",
			"status":      "HEALTHY",
			"enabled":     true,
			"description": "Semantic vector similarity lookup against historical incident postmortems.",
		},
	}
	uiReceipts = []UIReceipt{
		{
			ReceiptID:  "sha256:rcpt_9f83a21b",
			Timestamp:  time.Now().Add(-25 * time.Minute).UTC().Format(time.RFC3339),
			Task:       "CNC-03 Spindle Overheat E102 Diagnostics",
			Caller:     "agent:industrial-monitor",
			Model:      "deepseek/deepseek-r1",
			DurationMs: 420,
			Tokens:     312,
			CostUSD:    0.000468,
			Signature:  "0x7c49a15b3e20d8f1e56304cb31a298bf48d2e148a0",
			Status:     "VERIFIED",
			Output:     "Alarm E102 diagnosed: spindle bearing temperature spike at 86.4C. Recommended immediate lubrication bypass cycle.",
		},
		{
			ReceiptID:  "sha256:rcpt_4b10e8cd",
			Timestamp:  time.Now().Add(-12 * time.Minute).UTC().Format(time.RFC3339),
			Task:       "Product Defect SOP Root-Cause Analysis",
			Caller:     "agent:quality-gate",
			Model:      "deepseek/deepseek-chat",
			DurationMs: 285,
			Tokens:     184,
			CostUSD:    0.000276,
			Signature:  "0x91da283c7490f231e6498bb892f384a209cd42e11b",
			Status:     "VERIFIED",
			Output:     "Dimensional tolerance anomaly traced to lot #202609-B extruder nozzle pressure fluctuation.",
		},
		{
			ReceiptID:  "sha256:rcpt_a0300f3e",
			Timestamp:  time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339),
			Task:       "Audit bearing temperature sensors telemetry",
			Caller:     "agent:quality-tracer-01",
			Model:      "deepseek/deepseek-r1",
			DurationMs: 380,
			Tokens:     265,
			CostUSD:    0.000397,
			Signature:  "0x9fd020dc2fc3a52bbc68277c18b255588003113d",
			Status:     "VERIFIED",
			Output:     "Evaluated cross-sensor telemetry. Sensor CH-02 matches physical bearing location; no open-circuit or drift anomaly detected.",
		},
	}
)

func runUI(args []string) {
	hasPreview := false
	port := "8080"
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--preview" || arg == "--dev" || arg == "--internal" {
			hasPreview = true
		} else if arg == "-p" || arg == "--port" {
			if i+1 < len(args) {
				port = args[i+1]
				i++
			}
		} else if strings.HasPrefix(arg, "--port=") {
			port = strings.TrimPrefix(arg, "--port=")
		} else if !strings.HasPrefix(arg, "-") {
			if _, err := strconv.Atoi(arg); err == nil {
				port = arg
			}
		}
	}

	if !hasPreview {
		fmt.Println("[info] Web UI is currently under internal polishing and is disabled for external access in this release.")
		fmt.Println("[info] Only CLI operations are permitted at this stage.")
		fmt.Println("[info] For all configuration, testing, and agent management, please use the CLI commands:")
		fmt.Println("  - agent config              (display active configuration)")
		fmt.Println("  - agent config wizard       (interactive terminal configuration wizard)")
		fmt.Println("  - agent config set <K> <V>  (set configuration value)")
		fmt.Println("  - agent test-llm            (test model inference & latency)")
		fmt.Println("  - agent mcp                 (manage Model Context Protocol tools)")
		fmt.Println("  - agent init <name>         (scaffold new enterprise agent project)")
		fmt.Println("  - agent run <manifest>      (execute agent workload)")
		fmt.Println()
		fmt.Println("hint: to launch in local loopback preview mode strictly for polishing, run: agent ui --preview")
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleUIIndex)
	mux.HandleFunc("/api/status", handleUIStatus)
	mux.HandleFunc("/api/receipts", handleUIReceipts)
	mux.HandleFunc("/api/tools", handleUITools)
	mux.HandleFunc("/api/tools/probe", handleUIToolsProbe)
	mux.HandleFunc("/api/nodes", handleUINodes)
	mux.HandleFunc("/api/run", handleUIRun)
	mux.HandleFunc("/api/halt", handleUIHalt)
	mux.HandleFunc("/api/config", handleUIConfig)
	mux.HandleFunc("/api/config/model", handleUIModelConfig)
	mux.HandleFunc("/api/test-llm", handleUITestLLM)
	mux.HandleFunc("/api/agents", handleUIAgents)

	addr := "127.0.0.1:" + port
	fmt.Printf("[info] starting Fenced control plane web server (internal preview) on %s\n", addr)
	fmt.Printf("[info] dashboard ui strictly bound to loopback: http://127.0.0.1:%s\n", port)
	fmt.Println("[info] api endpoints available:")
	fmt.Println("  - GET  /api/status")
	fmt.Println("  - GET  /api/receipts")
	fmt.Println("  - GET  /api/tools")
	fmt.Println("  - GET  /api/nodes")
	fmt.Println("  - GET  /api/agents")
	fmt.Println("  - GET  /api/config")
	fmt.Println("  - POST /api/run")
	fmt.Println("  - POST /api/halt")
	fmt.Println("  - POST /api/config/model")
	fmt.Println("  - POST /api/test-llm")
	fmt.Println("  - POST /api/agents")
	fmt.Println("[info] press Ctrl+C to terminate server")

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("[error] web server failed: %v\n", err)
	}
}
