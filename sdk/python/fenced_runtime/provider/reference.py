"""
Reference Provider implementations for Fenced:
- OpenAIProvider (ModelProvider)
- BrowserProvider (BrowserProvider + ToolProvider)
- PostgresMemoryProvider (MemoryProvider)
"""

import math
import time
from typing import Any, Callable, Dict, List, Optional
from .base import (
    BrowserProvider,
    HealthStatus,
    MemoryProvider,
    ModelProvider,
    ProviderManifest,
    ProviderType,
    ResourceRequirements,
    ToolProvider,
)


class OpenAIProvider(ModelProvider):
    """Reference ModelProvider for OpenAI-compatible models."""

    def __init__(self, api_key: str = "", model: str = "gpt-4o", base_url: str = "https://api.openai.com/v1"):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self._calls_count = 0
        self._manifest = ProviderManifest(
            name="openai-provider",
            version="1.0.0",
            type=ProviderType.MODEL,
            capabilities=["model:generate", "model:stream", "model:tools"],
            config_schema={
                "type": "object",
                "properties": {
                    "apiKey": {"type": "string"},
                    "model": {"type": "string", "default": "gpt-4o"},
                    "baseUrl": {"type": "string", "default": "https://api.openai.com/v1"},
                },
                "required": ["apiKey"],
            },
            secrets=["OPENAI_API_KEY"],
            resource_requirements=ResourceRequirements(cpu="100m", memory="128Mi"),
        )

    def manifest(self) -> ProviderManifest:
        return self._manifest

    def health(self) -> HealthStatus:
        status = "HEALTHY" if self.api_key else "DEGRADED"
        return HealthStatus(
            status=status,
            message="OpenAI provider operational" if self.api_key else "API key missing; mock mode",
            metrics={"calls_count": float(self._calls_count)},
        )

    def generate(self, model: str, messages: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        if not messages:
            raise ValueError("Messages cannot be empty")
        self._calls_count += 1
        last_msg = messages[-1].get("content", "")
        reply = f"Python OpenAI reply for: {last_msg}"
        return {
            "id": f"chatcmpl-{int(time.time()*1000)}",
            "model": model or self.model,
            "choices": [{"message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}],
            "usage": {"total_tokens": len(last_msg) + len(reply)},
        }

    def stream(self, model: str, messages: List[Dict[str, Any]], on_chunk: Callable[[Dict[str, Any]], None], **kwargs) -> None:
        if not messages:
            raise ValueError("Messages cannot be empty")
        self._calls_count += 1
        tokens = ["Python", " streaming", " from", " OpenAI", " provider!"]
        for i, token in enumerate(tokens):
            finish = "stop" if i == len(tokens) - 1 else None
            on_chunk({"delta": {"content": token}, "finish_reason": finish})


class BrowserProviderRef(BrowserProvider, ToolProvider):
    """Reference BrowserProvider and ToolProvider."""

    def __init__(self, headless: bool = True, timeout_seconds: int = 30):
        self.headless = headless
        self.timeout_seconds = timeout_seconds
        self.current_url = "about:blank"
        self._manifest = ProviderManifest(
            name="browser-provider",
            version="1.0.0",
            type=ProviderType.BROWSER,
            capabilities=["browser:navigate", "browser:screenshot", "browser:click", "browser:evaluate", "tool:invoke"],
            config_schema={
                "type": "object",
                "properties": {
                    "headless": {"type": "boolean", "default": True},
                    "timeoutSeconds": {"type": "integer", "default": 30},
                },
            },
            resource_requirements=ResourceRequirements(cpu="500m", memory="512Mi"),
        )

    def manifest(self) -> ProviderManifest:
        return self._manifest

    def health(self) -> HealthStatus:
        return HealthStatus(status="HEALTHY", message="Headless browser engine ready")

    def navigate(self, url: str, wait_until: str = "load", timeout_seconds: int = 30) -> Dict[str, Any]:
        if not url:
            raise ValueError("URL cannot be empty")
        self.current_url = url
        return {"url": url, "status_code": 200, "title": f"Page {url}", "content": f"<html><body>{url}</body></html>"}

    def screenshot(self, full_page: bool = True) -> bytes:
        return b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"

    def click(self, selector: str) -> Dict[str, Any]:
        if not selector:
            raise ValueError("Selector cannot be empty")
        return {"clicked": selector, "url": self.current_url}

    def evaluate(self, script: str) -> Any:
        return {"evaluated": True, "script": script}

    def list_tools(self) -> List[Dict[str, Any]]:
        return [
            {"name": "browser_navigate", "description": "Navigate browser to URL", "input_schema": {"type": "object", "properties": {"url": {"type": "string"}}}},
            {"name": "browser_screenshot", "description": "Capture screenshot", "input_schema": {"type": "object"}},
            {"name": "browser_click", "description": "Click element by CSS selector", "input_schema": {"type": "object", "properties": {"selector": {"type": "string"}}}},
        ]

    def invoke_tool(self, tool_name: str, arguments: Dict[str, Any], execution_id: str = "") -> Dict[str, Any]:
        if tool_name == "browser_navigate":
            res = self.navigate(arguments.get("url", ""))
            return {"output": res, "receipt_id": f"rcpt-nav-{int(time.time()*1000)}"}
        elif tool_name == "browser_screenshot":
            shot = self.screenshot()
            return {"output": {"bytes": len(shot)}, "receipt_id": f"rcpt-shot-{int(time.time()*1000)}"}
        elif tool_name == "browser_click":
            res = self.click(arguments.get("selector", ""))
            return {"output": res, "receipt_id": f"rcpt-click-{int(time.time()*1000)}"}
        raise KeyError(f"Unknown tool: {tool_name}")


class PostgresMemoryProvider(MemoryProvider):
    """Reference MemoryProvider with in-memory vector cosine similarity."""

    def __init__(self, connection_string: str = "", table: str = "agent_memories", vector_dimensions: int = 1536):
        self.connection_string = connection_string
        self.table = table
        self.vector_dimensions = vector_dimensions
        self._store: Dict[str, Dict[str, Any]] = {}
        self._manifest = ProviderManifest(
            name="postgres-memory-provider",
            version="1.0.0",
            type=ProviderType.MEMORY,
            capabilities=["memory:put", "memory:get", "memory:search", "memory:vector"],
            config_schema={
                "type": "object",
                "properties": {
                    "connectionString": {"type": "string"},
                    "table": {"type": "string", "default": "agent_memories"},
                    "vectorDimensions": {"type": "integer", "default": 1536},
                },
                "required": ["connectionString"],
            },
            secrets=["DATABASE_URL"],
            resource_requirements=ResourceRequirements(cpu="200m", memory="256Mi"),
        )

    def manifest(self) -> ProviderManifest:
        return self._manifest

    def health(self) -> HealthStatus:
        return HealthStatus(
            status="HEALTHY",
            message="PostgreSQL vector store active",
            metrics={"entries": float(len(self._store))},
        )

    def put(self, namespace: str, key: str, value: Any, embedding: Optional[List[float]] = None) -> None:
        if not key:
            raise ValueError("Key cannot be empty")
        composite = f"{namespace}/{key}"
        self._store[composite] = {
            "namespace": namespace,
            "key": key,
            "value": value,
            "embedding": embedding or [],
            "updated_at": time.time(),
        }

    def get(self, namespace: str, key: str) -> Optional[Dict[str, Any]]:
        return self._store.get(f"{namespace}/{key}")

    def delete(self, namespace: str, key: str) -> None:
        self._store.pop(f"{namespace}/{key}", None)

    def search(self, namespace: str, query_vector: Optional[List[float]] = None, prefix: str = "", limit: int = 10, min_score: float = 0.0) -> List[Dict[str, Any]]:
        results = []
        for entry in self._store.values():
            if namespace and entry["namespace"] != namespace:
                continue
            if prefix and not entry["key"].startswith(prefix):
                continue
            score = 1.0
            if query_vector and entry["embedding"]:
                score = self._cosine_similarity(query_vector, entry["embedding"])
            if score >= min_score:
                results.append({"entry": entry, "score": score})

        results.sort(key=lambda r: r["score"], reverse=True)
        return results[:limit]

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        if len(a) != len(b) or not a:
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(y * y for y in b))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return dot / (norm_a * norm_b)
