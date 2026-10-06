package provider_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/CloudEdgeCore/Fenced/sdk/provider"
)

func TestProviderRegistryAndManifests(t *testing.T) {
	ctx := context.Background()
	reg := provider.NewRegistry()

	// 1. Test OpenAIProvider
	openai, err := provider.NewOpenAIProvider(provider.OpenAIConfig{
		APIKey: "sk-mock-12345",
		Model:  "gpt-4o",
	})
	if err != nil {
		t.Fatalf("failed to create OpenAIProvider: %v", err)
	}
	if err := reg.Register(openai); err != nil {
		t.Fatalf("failed to register OpenAIProvider: %v", err)
	}

	// 2. Test BrowserProvider
	browser, err := provider.NewBrowserProvider(provider.BrowserConfig{
		Headless:       true,
		TimeoutSeconds: 15,
	})
	if err != nil {
		t.Fatalf("failed to create BrowserProvider: %v", err)
	}
	if err := reg.Register(browser); err != nil {
		t.Fatalf("failed to register BrowserProvider: %v", err)
	}

	// 3. Test PostgresMemoryProvider
	memory, err := provider.NewPostgresMemoryProvider(provider.PostgresMemoryConfig{
		ConnectionString: "postgres://mock:5432/fenced",
		Table:            "agent_memories",
		VectorDimensions: 3,
	})
	if err != nil {
		t.Fatalf("failed to create PostgresMemoryProvider: %v", err)
	}
	if err := reg.Register(memory); err != nil {
		t.Fatalf("failed to register PostgresMemoryProvider: %v", err)
	}

	// Duplicate registration must fail
	if err := reg.Register(openai); err == nil {
		t.Fatalf("expected error on duplicate provider registration, got nil")
	}

	// Test List and Get
	allManifests := reg.List("")
	if len(allManifests) != 3 {
		t.Fatalf("expected 3 registered providers, got %d", len(allManifests))
	}

	modelManifests := reg.List(provider.TypeModel)
	if len(modelManifests) != 1 || modelManifests[0].Name != "openai-provider" {
		t.Fatalf("unexpected model manifests: %+v", modelManifests)
	}

	// Check Health
	healthReport := reg.CheckAllHealth(ctx)
	if len(healthReport) != 3 {
		t.Fatalf("expected 3 health reports, got %d", len(healthReport))
	}
	for name, h := range healthReport {
		if h.Status != "HEALTHY" {
			t.Errorf("expected HEALTHY for %s, got %s (%s)", name, h.Status, h.Message)
		}
	}
}

func TestOpenAIProviderGenerateAndStream(t *testing.T) {
	ctx := context.Background()
	p, err := provider.NewOpenAIProvider(provider.OpenAIConfig{
		APIKey: "sk-test",
		Model:  "gpt-4o",
	})
	if err != nil {
		t.Fatalf("failed to init provider: %v", err)
	}
	defer p.Close()

	// Test Generate
	res, err := p.Generate(ctx, provider.GenerateRequest{
		Model: "gpt-4o",
		Messages: []provider.Message{
			{Role: "user", Content: "Hello Fenced!"},
		},
	})
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if res.Message.Content == "" || res.FinishReason != "stop" {
		t.Fatalf("unexpected generate response: %+v", res)
	}

	// Test Stream
	var chunks []string
	err = p.Stream(ctx, provider.GenerateRequest{
		Messages: []provider.Message{
			{Role: "user", Content: "Stream test"},
		},
	}, func(c provider.StreamChunk) error {
		chunks = append(chunks, c.DeltaContent)
		return nil
	})
	if err != nil {
		t.Fatalf("stream failed: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatalf("expected streamed chunks, got none")
	}
}

func TestBrowserProviderAutomationAndTools(t *testing.T) {
	ctx := context.Background()
	p, err := provider.NewBrowserProvider(provider.BrowserConfig{Headless: true})
	if err != nil {
		t.Fatalf("failed to init browser provider: %v", err)
	}
	defer p.Close()

	// Test Navigate
	navRes, err := p.Navigate(ctx, provider.NavigateRequest{URL: "https://fenced.dev"})
	if err != nil || navRes.StatusCode != 200 {
		t.Fatalf("navigate failed: %v, %+v", err, navRes)
	}

	// Test Screenshot
	shot, err := p.Screenshot(ctx, provider.ScreenshotRequest{FullPage: true})
	if err != nil || len(shot) == 0 {
		t.Fatalf("screenshot failed: %v", err)
	}

	// Test ToolProvider interface
	tools, err := p.ListTools(ctx)
	if err != nil || len(tools) < 3 {
		t.Fatalf("list tools failed: %v, tools: %+v", err, tools)
	}

	// Invoke tool browser_navigate
	invRes, err := p.InvokeTool(ctx, provider.ToolInvocationRequest{
		ToolName:  "browser_navigate",
		Arguments: json.RawMessage(`{"url":"https://fenced.dev/docs"}`),
	})
	if err != nil || invRes.Error != "" {
		t.Fatalf("invoke tool failed: %v, %+v", err, invRes)
	}
}

func TestPostgresMemoryProviderVectorSearch(t *testing.T) {
	ctx := context.Background()
	p, err := provider.NewPostgresMemoryProvider(provider.PostgresMemoryConfig{
		ConnectionString: "postgres://mock:5432/fenced",
		Table:            "memories",
		VectorDimensions: 3,
	})
	if err != nil {
		t.Fatalf("failed to init memory provider: %v", err)
	}
	defer p.Close()

	// Put entries
	_ = p.Put(ctx, provider.MemoryPutRequest{
		Namespace: "tenant-a",
		Key:       "doc-1",
		Value:     json.RawMessage(`{"text":"cloud computing architecture"}`),
		Embedding: []float32{1.0, 0.0, 0.0},
	})
	_ = p.Put(ctx, provider.MemoryPutRequest{
		Namespace: "tenant-a",
		Key:       "doc-2",
		Value:     json.RawMessage(`{"text":"cooking pasta recipes"}`),
		Embedding: []float32{0.0, 1.0, 0.0},
	})

	// Get entry
	entry, err := p.Get(ctx, "tenant-a", "doc-1")
	if err != nil || entry.Key != "doc-1" {
		t.Fatalf("get entry failed: %v, %+v", err, entry)
	}

	// Search with vector close to doc-1
	results, err := p.Search(ctx, provider.MemorySearchQuery{
		Namespace:   "tenant-a",
		QueryVector: []float32{0.9, 0.1, 0.0},
		MinScore:    0.5,
	})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) == 0 || results[0].Entry.Key != "doc-1" {
		t.Fatalf("expected doc-1 as top search result, got: %+v", results)
	}
	if results[0].Score < 0.8 {
		t.Fatalf("expected high cosine similarity score, got %f", results[0].Score)
	}
}
