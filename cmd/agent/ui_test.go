package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestUIHandlers(t *testing.T) {
	// 1. Test Index HTML
	t.Run("handleUIIndex", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		handleUIIndex(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "Fenced Kernel") {
			t.Errorf("expected HTML to contain 'Fenced Kernel'")
		}
		if !strings.Contains(body, "Cluster Overview") {
			t.Errorf("expected HTML to contain 'Cluster Overview'")
		}
	})

	// 2. Test Status API
	t.Run("handleUIStatus", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
		w := httptest.NewRecorder()
		handleUIStatus(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		var status map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
			t.Fatalf("failed to parse status JSON: %v", err)
		}
		if status["product"] != "Fenced Control Plane" {
			t.Errorf("unexpected product name: %v", status["product"])
		}
		if status["status"] != "ONLINE" && status["status"] != "REDLINE_HALTED" {
			t.Errorf("unexpected status field: %v", status["status"])
		}
	})

	// 3. Test Receipts API
	t.Run("handleUIReceipts", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/receipts", nil)
		w := httptest.NewRecorder()
		handleUIReceipts(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		var receipts []UIReceipt
		if err := json.Unmarshal(w.Body.Bytes(), &receipts); err != nil {
			t.Fatalf("failed to parse receipts JSON: %v", err)
		}
		if len(receipts) == 0 {
			t.Errorf("expected non-empty receipts ledger")
		}
		for _, r := range receipts {
			if !strings.HasPrefix(r.ReceiptID, "sha256:rcpt_") {
				t.Errorf("invalid receipt ID format: %s", r.ReceiptID)
			}
			if r.Status != "VERIFIED" {
				t.Errorf("expected status VERIFIED, got: %s", r.Status)
			}
		}
	})

	// 4. Test Tools API
	t.Run("handleUITools", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
		w := httptest.NewRecorder()
		handleUITools(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		var tools []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &tools); err != nil {
			t.Fatalf("failed to parse tools JSON: %v", err)
		}
		if len(tools) < 5 {
			t.Errorf("expected at least 5 registered tools, got %d", len(tools))
		}
	})

	// 5. Test Nodes API
	t.Run("handleUINodes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/nodes", nil)
		w := httptest.NewRecorder()
		handleUINodes(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		var nodes []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &nodes); err != nil {
			t.Fatalf("failed to parse nodes JSON: %v", err)
		}
		if len(nodes) != 5 {
			t.Errorf("expected exactly 5 DAG pipeline stages, got %d", len(nodes))
		}
	})

	// 6. Test Run and Halt Interactions
	t.Run("handleUIRunAndHalt", func(t *testing.T) {
		// Ensure unhalted initially
		uiMu.Lock()
		uiRedline = false
		uiMu.Unlock()

		// Run task
		runReq := UIRunRequest{Prompt: "Verify spindle speed sensor calibration"}
		reqBytes, _ := json.Marshal(runReq)
		req := httptest.NewRequest(http.MethodPost, "/api/run", bytes.NewReader(reqBytes))
		w := httptest.NewRecorder()
		handleUIRun(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected run status 200, got %d", w.Code)
		}
		var runResp UIRunResponse
		if err := json.Unmarshal(w.Body.Bytes(), &runResp); err != nil {
			t.Fatalf("failed to parse run response: %v", err)
		}
		if !runResp.Success {
			t.Errorf("expected run success=true, got error: %s", runResp.Error)
		}
		if !strings.HasPrefix(runResp.Receipt.ReceiptID, "sha256:rcpt_") {
			t.Errorf("expected sha256 receipt ID, got: %s", runResp.Receipt.ReceiptID)
		}

		// Engage Redline Halt
		haltReq := httptest.NewRequest(http.MethodPost, "/api/halt", nil)
		haltW := httptest.NewRecorder()
		handleUIHalt(haltW, haltReq)
		if haltW.Code != http.StatusOK {
			t.Fatalf("expected halt status 200, got %d", haltW.Code)
		}

		// Run task while halted (should fail)
		req2 := httptest.NewRequest(http.MethodPost, "/api/run", bytes.NewReader(reqBytes))
		w2 := httptest.NewRecorder()
		handleUIRun(w2, req2)
		var runResp2 UIRunResponse
		json.Unmarshal(w2.Body.Bytes(), &runResp2)
		if runResp2.Success {
			t.Errorf("expected run to fail when redline halt is active")
		}

		// Disengage Redline Halt
		handleUIHalt(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/halt", nil))

		uiMu.RLock()
		finalState := uiRedline
		uiMu.RUnlock()
		if finalState {
			t.Errorf("expected redline halt to be disengaged")
		}
	})

	// 7. Test Model Configuration and Full Configuration Hub
	t.Run("handleUIFullConfig", func(t *testing.T) {
		// Test GET /api/config
		getReq := httptest.NewRequest(http.MethodGet, "/api/config", nil)
		getW := httptest.NewRecorder()
		handleUIConfig(getW, getReq)
		if getW.Code != http.StatusOK {
			t.Fatalf("expected GET /api/config status 200, got %d", getW.Code)
		}

		// Test POST /api/config with full configuration
		fullReq := UIFullConfigRequest{
			Environment:     "staging",
			DefaultProvider: "deepseek",
			Model:           "deepseek-chat",
			BaseURL:         "https://api.deepseek.com/v1",
			APIKey:          "sk-test-key-full-config",
			TimeoutSec:      150,
			BudgetUSD:       3.75,
			MaxTokens:       45000,
			MaxToolCalls:    25,
			EnforceReceipts: true,
			FailClosed:      true,
			LogLevel:        "warn",
			LogFormat:       "json",
			GatewayPort:     19090,
			GatewayHost:     "0.0.0.0",
		}
		reqBytes, _ := json.Marshal(fullReq)
		postReq := httptest.NewRequest(http.MethodPost, "/api/config", bytes.NewReader(reqBytes))
		postW := httptest.NewRecorder()
		handleUIConfig(postW, postReq)

		if postW.Code != http.StatusOK {
			t.Fatalf("expected POST /api/config status 200, got %d", postW.Code)
		}
		var resp map[string]any
		if err := json.Unmarshal(postW.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}
		if resp["success"] != true {
			t.Errorf("expected success=true, got: %v", resp["success"])
		}

		// Verify legacy /api/config/model
		cfgReq := UIModelConfigRequest{
			Provider:  "openrouter",
			Model:     "stealth/space-bunny-alpha",
			BudgetUSD: 2.50,
		}
		reqBytes2, _ := json.Marshal(cfgReq)
		req2 := httptest.NewRequest(http.MethodPost, "/api/config/model", bytes.NewReader(reqBytes2))
		w2 := httptest.NewRecorder()
		handleUIModelConfig(w2, req2)
		if w2.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w2.Code)
		}
	})

	// 8. Test Dynamic Tool Registry & Probe
	t.Run("handleUIToolsRegisterAndProbe", func(t *testing.T) {
		// Register a new tool
		regReq := UIToolActionRequest{
			Action:      "register",
			Name:        "industrial:vision",
			Adapter:     "industrial.vision.inspect@1.0.0",
			Protocol:    "MCP/2.0",
			Endpoint:    "http://127.0.0.1:8089/mcp",
			Description: "Optical surface defect inspection via edge camera inference.",
		}
		b, _ := json.Marshal(regReq)
		postW := httptest.NewRecorder()
		handleUITools(postW, httptest.NewRequest(http.MethodPost, "/api/tools", bytes.NewReader(b)))
		if postW.Code != http.StatusOK {
			t.Fatalf("expected tool register status 200, got %d", postW.Code)
		}

		// Toggle tool enabled state
		togReq := UIToolActionRequest{
			Action: "toggle",
			Name:   "industrial:vision",
		}
		bTog, _ := json.Marshal(togReq)
		togW := httptest.NewRecorder()
		handleUITools(togW, httptest.NewRequest(http.MethodPost, "/api/tools", bytes.NewReader(bTog)))
		if togW.Code != http.StatusOK {
			t.Fatalf("expected tool toggle status 200, got %d", togW.Code)
		}

		// Probe tool endpoint
		probeReq := map[string]string{"endpoint": "local:adapter"}
		bProbe, _ := json.Marshal(probeReq)
		probeW := httptest.NewRecorder()
		handleUIToolsProbe(probeW, httptest.NewRequest(http.MethodPost, "/api/tools/probe", bytes.NewReader(bProbe)))
		if probeW.Code != http.StatusOK {
			t.Fatalf("expected probe status 200, got %d", probeW.Code)
		}
	})

	// 9. Test Agent Listing, Creation, Toggle, and Deletion
	t.Run("handleUIAgentsLifecycle", func(t *testing.T) {
		agentName := "test-ui-agent"
		defer os.RemoveAll(agentName)

		// List agents
		req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
		w := httptest.NewRecorder()
		handleUIAgents(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		var list []UIAgentItem
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}
		if len(list) == 0 {
			t.Errorf("expected non-empty agents list")
		}

		// Create new agent
		createReq := UICreateAgentRequest{
			Name:      agentName,
			Role:      "Automated Health Monitor",
			Model:     "deepseek/deepseek-r1",
			BudgetUSD: 1.25,
		}
		reqBytes, _ := json.Marshal(createReq)
		w2 := httptest.NewRecorder()
		handleUIAgents(w2, httptest.NewRequest(http.MethodPost, "/api/agents", bytes.NewReader(reqBytes)))

		if w2.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w2.Code)
		}
		var createResp map[string]any
		json.Unmarshal(w2.Body.Bytes(), &createResp)
		if createResp["success"] != true {
			t.Errorf("expected agent creation success, got: %v", createResp["error"])
		}

		// Toggle agent status
		togW := httptest.NewRecorder()
		handleUIAgents(togW, httptest.NewRequest(http.MethodPost, "/api/agents?action=toggle&name="+agentName, nil))
		if togW.Code != http.StatusOK {
			t.Errorf("expected toggle status 200, got %d", togW.Code)
		}

		// Delete agent
		delW := httptest.NewRecorder()
		handleUIAgents(delW, httptest.NewRequest(http.MethodDelete, "/api/agents?name="+agentName, nil))
		if delW.Code != http.StatusOK {
			t.Errorf("expected delete status 200, got %d", delW.Code)
		}
	})
}
