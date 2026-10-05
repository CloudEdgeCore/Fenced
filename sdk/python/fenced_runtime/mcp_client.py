"""Standard-library MCP client for the Agent Runtime brokered tools.

Speaks the same Streamable HTTP dialect the runtime's MCP endpoint serves:
one JSON-RPC 2.0 document per POST, answered as application/json (this client
always prefers the JSON form) or text/event-stream. No SDK dependency: the
agent-side surface stays importable with a bare interpreter.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from . import PROTOCOL_VERSION


class MCPError(RuntimeError):
    """A JSON-RPC level failure (transport or protocol)."""


class MCPToolError(RuntimeError):
    """A tool-level failure reported as an MCP isError result."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        text = _text_of(payload)
        super().__init__(text or "MCP tool call failed")


class MCPClient:
    """Minimal MCP client bound to one loopback endpoint."""

    def __init__(self, url: str, timeout: float = 120.0, execution_id: str = "") -> None:
        if not url:
            raise ValueError("MCP endpoint URL is required")
        self.url = url
        self.timeout = timeout
        self.execution_id = execution_id
        self._next_id = 1

    def initialize(self) -> dict[str, Any]:
        result = self._rpc("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "fenced-python-agent", "version": "1.0"},
        })
        self._notify("notifications/initialized")
        return result

    def list_tools(self) -> list[dict[str, Any]]:
        result = self._rpc("tools/list", {})
        return list(result.get("tools", []))

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Call one tool and return its decoded JSON payload.

        Raises MCPToolError when the tool reports isError with a JSON payload,
        so callers can inspect structured failures (denials, provider errors).
        """
        result = self._rpc("tools/call", {"name": name, "arguments": arguments})
        if result.get("isError"):
            try:
                payload = json.loads(_text_of(result))
            except (ValueError, TypeError):
                payload = {"error": _text_of(result)}
            raise MCPToolError(payload)
        try:
            return json.loads(_text_of(result))
        except (ValueError, TypeError) as error:
            raise MCPError(f"tool {name} returned a non-JSON payload: {error}") from error

    def _rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode()
        request = urllib.request.Request(
            self.url, data=body, method="POST",
            headers=self._headers(),
        )
        response = self._open(request)
        if response.startswith(b"event:") or response.startswith(b"data:"):
            response = _sse_payload(response)
        document = json.loads(response.decode())
        if "error" in document and document["error"] is not None:
            rpc_error = document["error"]
            raise MCPError(f"mcp error {rpc_error.get('code')}: {rpc_error.get('message')}")
        result = document.get("result")
        if not isinstance(result, dict):
            raise MCPError(f"unexpected result shape for {method}")
        return result

    def _notify(self, method: str) -> None:
        body = json.dumps({"jsonrpc": "2.0", "method": method}).encode()
        request = urllib.request.Request(
            self.url, data=body, method="POST",
            headers=self._headers(),
        )
        self._open(request)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.execution_id:
            # Binds brokered calls to this attempt's open execution window.
            headers["X-Fenced-Execution"] = self.execution_id
        return headers

    def _open(self, request: urllib.request.Request) -> bytes:
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            detail = error.read(512).decode("utf-8", "replace")
            raise MCPError(f"MCP endpoint HTTP {error.code}: {detail}") from error
        except urllib.error.URLError as error:
            raise MCPError(f"MCP endpoint unreachable: {error.reason}") from error


def _text_of(result: dict[str, Any]) -> str:
    content = result.get("content") or []
    parts = [item.get("text", "") for item in content if isinstance(item, dict)]
    return "\n".join(part for part in parts if part)


def _sse_payload(raw: bytes) -> bytes:
    """Extract the last JSON-RPC document from an SSE-framed response."""
    last = b""
    for line in raw.decode("utf-8", "replace").splitlines():
        if line.startswith("data:"):
            last = line[len("data:"):].strip().encode()
    if not last:
        raise MCPError("SSE response carried no data frame")
    return last
