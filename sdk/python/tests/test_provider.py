"""Unit tests for Python Provider SDK and Reference Providers."""

import unittest
from fenced_runtime.provider import (
    BrowserProviderRef,
    OpenAIProvider,
    PostgresMemoryProvider,
    ProviderRegistry,
    ProviderType,
)


class TestProviderSDK(unittest.TestCase):
    def setUp(self):
        self.registry = ProviderRegistry()

    def test_registry_and_manifests(self):
        openai = OpenAIProvider(api_key="sk-test-123")
        browser = BrowserProviderRef(headless=True)
        memory = PostgresMemoryProvider(connection_string="postgres://localhost/test")

        self.registry.register(openai)
        self.registry.register(browser)
        self.registry.register(memory)

        # Duplicate register must fail
        with self.assertRaises(ValueError):
            self.registry.register(openai)

        manifests = self.registry.list()
        self.assertEqual(len(manifests), 3)

        models = self.registry.list(ProviderType.MODEL)
        self.assertEqual(len(models), 1)
        self.assertEqual(models[0].name, "openai-provider")

        health = self.registry.check_all_health()
        self.assertEqual(len(health), 3)
        self.assertEqual(health["model/openai-provider"].status, "HEALTHY")
        self.assertEqual(health["browser/browser-provider"].status, "HEALTHY")
        self.assertEqual(health["memory/postgres-memory-provider"].status, "HEALTHY")

    def test_openai_generate_and_stream(self):
        p = OpenAIProvider(api_key="sk-valid")
        res = p.generate("gpt-4o", [{"role": "user", "content": "Hello"}])
        self.assertIn("Python OpenAI reply", res["choices"][0]["message"]["content"])
        self.assertEqual(res["choices"][0]["finish_reason"], "stop")

        chunks = []
        p.stream("gpt-4o", [{"role": "user", "content": "Stream"}], on_chunk=chunks.append)
        self.assertGreater(len(chunks), 0)
        self.assertEqual(chunks[-1]["finish_reason"], "stop")

    def test_browser_provider(self):
        p = BrowserProviderRef()
        nav = p.navigate("https://fenced.dev")
        self.assertEqual(nav["status_code"], 200)

        shot = p.screenshot()
        self.assertTrue(shot.startswith(b"\x89PNG"))

        click = p.click("#btn")
        self.assertEqual(click["clicked"], "#btn")

        tools = p.list_tools()
        self.assertEqual(len(tools), 3)

        res = p.invoke_tool("browser_navigate", {"url": "https://example.com"})
        self.assertEqual(res["output"]["status_code"], 200)

    def test_postgres_memory_vector_search(self):
        p = PostgresMemoryProvider()
        p.put("tenant-1", "doc-a", {"text": "AI agents"}, embedding=[1.0, 0.0, 0.0])
        p.put("tenant-1", "doc-b", {"text": "baking bread"}, embedding=[0.0, 1.0, 0.0])

        entry = p.get("tenant-1", "doc-a")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["value"]["text"], "AI agents")

        # Vector search
        results = p.search("tenant-1", query_vector=[0.9, 0.1, 0.0], min_score=0.5)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["entry"]["key"], "doc-a")
        self.assertGreater(results[0]["score"], 0.8)


if __name__ == "__main__":
    unittest.main()
