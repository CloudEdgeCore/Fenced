package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/version"
	"gopkg.in/yaml.v3"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	if len(os.Args) < 2 {
		printHelp()
		return
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "version", "-v", "--version":
		runVersion()
	case "config":
		runConfig(args)
	case "test-llm", "test":
		runTestLLM()
	case "init", "new":
		runInit(args)
	case "demo":
		runDemo(args)
	case "mcp":
		runMCP(args)
	case "ui", "dashboard":
		runUI(args)
	case "run":
		runAgent(args)
	case "help", "-h", "--help":
		printHelp()
	default:
		runLegacyFallback(command, args)
	}
}

func printHelp() {
	fmt.Print(`Fenced Developer Core CLI: agent (v1.3.0)

Usage:
  agent [command] [flags]

Core Commands:
  config                 Display active hierarchical configuration
  config path            Show configuration precedence and resolution order
  config set <K> <V>     Set configuration value using dot notation
  config env <name>      Switch active environment profile (development, staging, production)
  config wizard          Interactive terminal configuration wizard
  test-llm               Verify model connectivity, latency, and streaming reasoning
  mcp                    Manage Model Context Protocol (MCP) tool adapters
  init <name>            Scaffold a new enterprise Agent project
  run <path>             Run an Agent manifest and record audit ledger
  demo <name>            Execute demo scenario (fault or quality)
  version                Display version and runtime information

Management Commands:
  package                Package agent artifact into signed bundle
  sign                   Cryptographically sign package artifact
  workflow               Manage deterministic DAG workflows
  service                Manage service daemons and health probes
  logs                   Stream execution logs for task

Examples:
  agent config
  agent config wizard
  agent config set llm.default_provider deepseek
  agent test-llm
  agent mcp list
  agent demo quality
`)
}

func runVersion() {
	info := version.Current()
	fmt.Printf("agent CLI %s (product: %s %s, syscall ABI: %s)\n",
		info.SemVer, info.Product, info.ProductVersion, info.SyscallABI)
	cfg, _ := LoadConfig()
	if len(cfg.LoadedFiles) > 0 {
		fmt.Printf("active configuration chain: %s\n", strings.Join(cfg.LoadedFiles, " -> "))
	} else {
		fmt.Println("active configuration chain: [builtin defaults]")
	}
}

func runConfig(args []string) {
	cfg, err := LoadConfig()
	if err != nil {
		fmt.Printf("[error] loading config: %v\n", err)
		return
	}

	if len(args) > 0 {
		sub := args[0]
		switch sub {
		case "path", "files":
			fmt.Println("Configuration precedence chain:")
			fmt.Println("  1. builtin defaults")
			if len(cfg.LoadedFiles) == 0 {
				fmt.Println("  2. file: none found (using defaults)")
			} else {
				for i, f := range cfg.LoadedFiles {
					fmt.Printf("  %d. file: %s\n", i+2, f)
				}
			}
			fmt.Printf("  %d. profile: %s\n", len(cfg.LoadedFiles)+2, cfg.Environment)
			fmt.Printf("  %d. environment variables: AGENT_LLM_*\n", len(cfg.LoadedFiles)+3)
			return

		case "env":
			if len(args) < 2 {
				fmt.Printf("active environment: %s\n", cfg.Environment)
				return
			}
			newEnv := args[1]
			cfg.Environment = newEnv
			target := "agent.yaml"
			if len(cfg.LoadedFiles) > 0 {
				target = cfg.LoadedFiles[len(cfg.LoadedFiles)-1]
			}
			if err := SaveConfig(cfg, target); err != nil {
				fmt.Printf("[error] failed to save environment: %v\n", err)
				return
			}
			fmt.Printf("[info] switched environment to %s (saved to %s)\n", newEnv, target)
			return

		case "wizard", "setup", "interactive":
			runConfigWizard(cfg)
			return

		case "set":
			if len(args) < 3 {
				fmt.Println("usage: agent config set <key.path> <value>")
				fmt.Println("example: agent config set llm.default_provider deepseek")
				fmt.Println("         agent config set kernel.budget.max_cost_usd 2.50")
				return
			}
			keyPath := args[1]
			val := args[2]
			applyConfigSet(cfg, keyPath, val)
			target := "agent.yaml"
			if len(cfg.LoadedFiles) > 0 {
				target = cfg.LoadedFiles[len(cfg.LoadedFiles)-1]
			}
			if err := SaveConfig(cfg, target); err != nil {
				fmt.Printf("[error] failed to save config: %v\n", err)
				return
			}
			fmt.Printf("[info] updated %s to %s in %s\n", keyPath, val, target)
			return
		}
	}

	displayCopy := *cfg
	displayCopy.LLM.Providers = make(map[string]ProviderConfig)
	for k, v := range cfg.LLM.Providers {
		masked := v
		masked.APIKey = MaskAPIKey(v.APIKey)
		displayCopy.LLM.Providers[k] = masked
	}

	yamlBytes, err := yaml.Marshal(&displayCopy)
	if err != nil {
		fmt.Printf("[error] failed to serialize yaml: %v\n", err)
		return
	}

	fmt.Printf("Agent Configuration (environment: %s)\n", cfg.Environment)
	if len(cfg.LoadedFiles) > 0 {
		fmt.Printf("source: %s\n", strings.Join(cfg.LoadedFiles, " < "))
	} else {
		fmt.Println("source: [builtin defaults]")
	}
	fmt.Printf("provider: %s (model: %s)\n\n", cfg.LLM.DefaultProvider, cfg.CurrentProvider().Model)
	fmt.Println(string(yamlBytes))
	fmt.Println("hint: use 'agent config set <path> <value>' to modify, or edit agent.yaml directly.")
}

func applyConfigSet(cfg *AgentYAMLConfig, path, val string) {
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		if parts[0] == "environment" {
			cfg.Environment = val
		}
		return
	}

	switch parts[0] {
	case "llm":
		if len(parts) == 2 && parts[1] == "default_provider" {
			cfg.LLM.DefaultProvider = val
			return
		}
		if len(parts) >= 4 && parts[1] == "providers" {
			provName := parts[2]
			prov := cfg.LLM.Providers[provName]
			switch parts[3] {
			case "api_key", "key":
				prov.APIKey = val
			case "model":
				prov.Model = val
			case "base_url", "url":
				prov.BaseURL = val
			case "timeout_sec":
				if sec, err := strconv.Atoi(val); err == nil {
					prov.TimeoutSec = sec
				}
			}
			if cfg.LLM.Providers == nil {
				cfg.LLM.Providers = make(map[string]ProviderConfig)
			}
			cfg.LLM.Providers[provName] = prov
		}
	case "kernel":
		if len(parts) >= 3 && parts[1] == "budget" {
			switch parts[2] {
			case "max_cost_usd", "cost":
				if cost, err := strconv.ParseFloat(val, 64); err == nil {
					cfg.Kernel.Budget.MaxCostUSD = cost
				}
			case "max_tokens", "tokens":
				if tok, err := strconv.Atoi(val); err == nil {
					cfg.Kernel.Budget.MaxTokens = tok
				}
			case "max_tool_calls", "calls":
				if c, err := strconv.Atoi(val); err == nil {
					cfg.Kernel.Budget.MaxToolCalls = c
				}
			}
		}
	case "gateway":
		if len(parts) == 2 && parts[1] == "port" {
			if p, err := strconv.Atoi(val); err == nil {
				cfg.Gateway.Port = p
			}
		}
	case "logging":
		if len(parts) == 2 && parts[1] == "level" {
			cfg.Logging.Level = val
		}
	}
}

func runTestLLM() {
	cfg, _ := LoadConfig()
	provider := cfg.CurrentProvider()
	if provider.APIKey == "" {
		fmt.Printf("[error] active provider %q has no API key configured\n", cfg.LLM.DefaultProvider)
		fmt.Printf("set llm.providers.%s.api_key in agent.yaml or run:\n  agent config set llm.providers.%s.api_key <key>\n",
			cfg.LLM.DefaultProvider, cfg.LLM.DefaultProvider)
		return
	}

	fmt.Printf("[info] probing [%s] %s (%s)...\n",
		cfg.LLM.DefaultProvider, provider.BaseURL, provider.Model)
	t0 := time.Now()
	testPrompt := "Respond with one brief sentence confirming connectivity to Fenced kernel."
	tokens, err := StreamLLM(cfg, "You are a professional AI agent managed by the Fenced kernel.", testPrompt)
	if err != nil {
		fmt.Printf("[error] connection test failed: %v\n", err)
		return
	}
	dur := time.Since(t0)
	fmt.Printf("\n[info] connectivity verified: latency=%v tokens_received=%d\n", dur.Round(time.Millisecond), tokens)
}

func runInit(args []string) {
	if len(args) < 1 {
		fmt.Println("usage: agent init <agent-name>")
		return
	}
	name := args[0]
	if err := ScaffoldAgent(name); err != nil {
		fmt.Printf("[error] initialization failed: %v\n", err)
		return
	}
	fmt.Printf("[info] created agent project at ./%s/\n", name)
	fmt.Printf("  ├── %s/agent.yaml          (hierarchical config)\n", name)
	fmt.Printf("  ├── %s/agent.manifest.json (capability and budget manifest)\n", name)
	fmt.Printf("  ├── %s/prompt.md           (system prompt and output schema)\n", name)
	fmt.Printf("  └── %s/tools/main.go       (custom tool implementation)\n", name)
	fmt.Printf("\nrun 'agent run %s/agent.manifest.json' to start.\n", name)
}

func runDemo(args []string) {
	scenario := "quality"
	if len(args) > 0 {
		scenario = args[0]
	}

	switch scenario {
	case "fault", "a", "cnc":
		runFaultDemo()
	case "quality", "b", "qa":
		runQualityDemo()
	default:
		fmt.Printf("unknown demo scenario %q, available: fault | quality\n", scenario)
	}
}

func runFaultDemo() {
	fmt.Println("[info] starting scenario A: CNC-03 spindle overheat E102 alarm diagnostics...")
	cfg, _ := LoadConfig()
	provider := cfg.CurrentProvider()
	args := []string{"run", "./examples/industrial-agent/cmd"}
	if provider.APIKey != "" {
		args = append(args, "-api-key", provider.APIKey)
	}
	if provider.Model != "" {
		args = append(args, "-model", provider.Model)
	}
	cmd := exec.Command("go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	_ = cmd.Run()
}

func runQualityDemo() {
	fmt.Println("[info] starting scenario B: product A defect traceability and SOP analysis...")
	cfg, _ := LoadConfig()
	provider := cfg.CurrentProvider()
	cmd := exec.Command("go", "run", "./examples/quality-agent/cmd")
	if provider.APIKey != "" {
		cmd.Env = append(os.Environ(), "OPENROUTER_API_KEY="+provider.APIKey)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	_ = cmd.Run()
}

func runAgent(args []string) {
	if len(args) < 1 {
		fmt.Println("usage: agent run <manifest.json or agent directory>")
		return
	}

	target := args[0]
	manifestPath := target
	fi, err := os.Stat(target)
	if err != nil {
		fmt.Printf("[error] target not found: %s\n", target)
		return
	}
	if fi.IsDir() {
		manifestPath = filepath.Join(target, "agent.manifest.json")
	}

	content, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Printf("[error] failed to read manifest: %v\n", err)
		return
	}

	var manifest map[string]any
	if err := json.Unmarshal(content, &manifest); err != nil {
		fmt.Printf("[error] failed to parse manifest: %v\n", err)
		return
	}

	cfg, _ := LoadConfig()
	provider := cfg.CurrentProvider()

	fmt.Printf("[info] starting agent kernel scheduler for %s\n", filepath.Base(manifestPath))
	fmt.Printf("[info] provider: %s, model: %s\n", cfg.LLM.DefaultProvider, provider.Model)
	fmt.Printf("[info] budget ceiling: $%.2f USD, max tokens: %d\n", cfg.Kernel.Budget.MaxCostUSD, cfg.Kernel.Budget.MaxTokens)

	dir := filepath.Dir(manifestPath)
	promptPath := filepath.Join(dir, "prompt.md")
	sysPrompt := "You are a professional AI agent managed by the Fenced kernel."
	if pb, err := os.ReadFile(promptPath); err == nil {
		sysPrompt = string(pb)
		fmt.Printf("[info] loaded system prompt from: %s\n", promptPath)
	}

	userGoal := "Execute task self-check and produce execution status report based on the manifest goals."
	if len(args) > 1 {
		userGoal = strings.Join(args[1:], " ")
	}

	fmt.Println("[info] triggering model inference...")
	t0 := time.Now()
	tokens, err := StreamLLM(cfg, sysPrompt, userGoal)
	if err != nil {
		fmt.Printf("[error] execution failed: %v\n", err)
		return
	}
	dur := time.Since(t0)

	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", manifestPath, time.Now().UnixNano())))
	receiptID := "sha256:rcpt_" + hex.EncodeToString(h[:8])

	fmt.Println()
	fmt.Println("[audit] task execution ledger:")
	fmt.Printf("  duration:   %v\n", dur.Round(time.Millisecond))
	fmt.Printf("  tokens:     %d\n", tokens)
	fmt.Printf("  cost_usd:   $%.6f\n", float64(tokens)*1.5/1000000.0)
	fmt.Printf("  receipt_id: %s\n", receiptID)
}

func runLegacyFallback(command string, args []string) {
	fmt.Printf("[info] delegating command: fenced %s %s\n", command, strings.Join(args, " "))
	cmd := exec.Command("fenced", append([]string{command}, args...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		fmt.Printf("[info] process completed: %v\n", err)
	}
}

func runConfigWizard(cfg *AgentYAMLConfig) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Fenced Interactive Configuration Wizard (CLI)")
	fmt.Println("Press Enter to accept current/default value shown in brackets [].")
	fmt.Println()

	// 1. Environment
	fmt.Printf("1. Environment Profile [%s]: ", cfg.Environment)
	if envInput, _ := reader.ReadString('\n'); strings.TrimSpace(envInput) != "" {
		cfg.Environment = strings.TrimSpace(envInput)
	}

	// 2. Default Provider
	curProvName := cfg.LLM.DefaultProvider
	if curProvName == "" {
		curProvName = "deepseek"
	}
	fmt.Printf("2. Default LLM Provider (e.g. deepseek, openrouter, qwen, ollama, custom) [%s]: ", curProvName)
	if provInput, _ := reader.ReadString('\n'); strings.TrimSpace(provInput) != "" {
		curProvName = strings.TrimSpace(provInput)
		cfg.LLM.DefaultProvider = curProvName
	}

	if cfg.LLM.Providers == nil {
		cfg.LLM.Providers = make(map[string]ProviderConfig)
	}
	prov := cfg.LLM.Providers[curProvName]

	// 3. Model
	curModel := prov.Model
	if curModel == "" {
		curModel = "deepseek-chat"
	}
	fmt.Printf("3. Model Name [%s]: ", curModel)
	if modelInput, _ := reader.ReadString('\n'); strings.TrimSpace(modelInput) != "" {
		prov.Model = strings.TrimSpace(modelInput)
	} else {
		prov.Model = curModel
	}

	// 4. Base URL
	curBaseURL := prov.BaseURL
	if curBaseURL == "" {
		curBaseURL = "https://api.deepseek.com/v1"
	}
	fmt.Printf("4. Base URL [%s]: ", curBaseURL)
	if urlInput, _ := reader.ReadString('\n'); strings.TrimSpace(urlInput) != "" {
		prov.BaseURL = strings.TrimSpace(urlInput)
	} else {
		prov.BaseURL = curBaseURL
	}

	// 5. Protocol Form (OpenAI Native /v1 vs Fenced Gateway /v1alpha)
	curProtocol := prov.Protocol
	if curProtocol == "" {
		curProtocol = "openai"
	}
	fmt.Printf("5. Protocol Format (1: openai native /v1, 2: fenced gateway /v1alpha) [%s]: ", curProtocol)
	if protoInput, _ := reader.ReadString('\n'); strings.TrimSpace(protoInput) != "" {
		trimmed := strings.TrimSpace(protoInput)
		if trimmed == "1" || strings.ToLower(trimmed) == "openai" {
			prov.Protocol = "openai"
		} else if trimmed == "2" || strings.ToLower(trimmed) == "fenced" {
			prov.Protocol = "fenced"
		} else {
			prov.Protocol = trimmed
		}
	} else {
		prov.Protocol = curProtocol
	}

	// 6. API Key
	maskedKey := MaskAPIKey(prov.APIKey)
	if maskedKey == "" {
		maskedKey = "none"
	}
	fmt.Printf("6. API Key [%s]: ", maskedKey)
	if keyInput, _ := reader.ReadString('\n'); strings.TrimSpace(keyInput) != "" {
		prov.APIKey = strings.TrimSpace(keyInput)
	}

	// 7. Budget Max Cost
	curCost := cfg.Kernel.Budget.MaxCostUSD
	if curCost <= 0 {
		curCost = 2.00
	}
	fmt.Printf("7. Budget Ceiling (USD) [$%.2f]: ", curCost)
	if costInput, _ := reader.ReadString('\n'); strings.TrimSpace(costInput) != "" {
		if c, err := strconv.ParseFloat(strings.TrimSpace(costInput), 64); err == nil && c > 0 {
			cfg.Kernel.Budget.MaxCostUSD = c
		}
	} else {
		cfg.Kernel.Budget.MaxCostUSD = curCost
	}

	cfg.LLM.Providers[curProvName] = prov

	// Save
	target := "agent.yaml"
	if len(cfg.LoadedFiles) > 0 {
		target = cfg.LoadedFiles[len(cfg.LoadedFiles)-1]
	}
	if err := SaveConfig(cfg, target); err != nil {
		fmt.Printf("\n[error] failed to save config to %s: %v\n", target, err)
		return
	}

	fmt.Printf("\n[success] configuration updated and saved to %s\n", target)
	fmt.Printf("  Provider: %s | Model: %s | Protocol: %s\n", curProvName, prov.Model, prov.Protocol)
	fmt.Printf("  BaseURL:  %s\n", prov.BaseURL)
	fmt.Printf("  Budget:   $%.2f USD\n\n", cfg.Kernel.Budget.MaxCostUSD)

	fmt.Print("Would you like to test model connectivity now? [Y/n]: ")
	if testInput, _ := reader.ReadString('\n'); strings.TrimSpace(strings.ToLower(testInput)) != "n" {
		runTestLLM()
	}
}
