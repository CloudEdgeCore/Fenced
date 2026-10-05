"""The real Fenced reference agent (v1.1 Phase 1.1).

A model-driven agent loop that runs entirely under Fenced governance: model
calls go through the brokered Model Gateway MCP tool (policy, budget, exact
token/cost settlement, audit), tool calls through the tenant tool registry on
the same MCP endpoint, and memory recall/write through the brokered memory
tools. The provider credential never reaches this process. Checkpoints carry
the conversation; restore resumes the loop from the recorded turns.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from typing import Any, Callable

from .mcp_client import MCPClient, MCPError, MCPToolError

SCHEMA_VERSION = "fenced.real-agent/v1"


class RealAgent:
    """AgentRuntime implementation wiring model, tools and memory via MCP."""

    def __init__(
        self,
        mcp_url: str | None = None,
        model_ref: str | None = None,
        memory_namespace: str | None = None,
        max_turns: int = 8,
    ) -> None:
        self.mcp_url = (mcp_url or os.environ.get("FENCED_MCP_URL") or "").strip()
        self.model_ref = (model_ref or os.environ.get("FENCED_MODEL_REF") or "").strip()
        self.memory_namespace = (memory_namespace or os.environ.get("FENCED_MEMORY_NAMESPACE") or "runs").strip()
        self.max_turns = max_turns or int(os.environ.get("FENCED_MAX_TURNS", "8"))
        if not self.mcp_url:
            raise ValueError("FENCED_MCP_URL is required (the runtime injects the loopback MCP endpoint)")
        if not self.model_ref:
            raise ValueError("a model reference is required (FENCED_MODEL_REF)")
        # Restored conversations keyed by execution id; cleared after use.
        self._restored: dict[str, dict[str, Any]] = {}
        # Live conversation snapshots for concurrent checkpoint requests.
        self._lock = threading.Lock()
        self._live: dict[str, dict[str, Any]] = {}

    # -- AgentRuntime protocol -------------------------------------------------

    def run(self, request: dict[str, Any], emit: Callable[[str, Any], None], stop_event: threading.Event) -> Any:
        execution_id = request["executionId"]
        goal = request["goal"]
        mcp = MCPClient(self.mcp_url, execution_id=execution_id)
        mcp.initialize()

        # Discover the tenant tools the AgentVersion is granted and offer them
        # to the model by NAME only. The platform (broker) resolves each name
        # against the capability-filtered registry and generates the schema,
        # so the model learns the real Fenced tool surface without this
        # process ever authoring or widening a tool contract (P0-01).
        tool_names = self._discover_tools(mcp)
        emit("tools.discovered", {"tools": tool_names})

        with self._lock:
            state = self._restored.pop(execution_id, None)
        resumed = state is not None and state.get("messages")
        messages: list[dict[str, Any]] = (
            list(state["messages"]) if resumed else [{"role": "system", "content": _system_prompt()}]
        )
        if not resumed:
            recalled = self._recall(mcp, goal)
            emit("memory.recalled", {"records": len(recalled)})
            context = goal
            if recalled:
                facts = "; ".join(record["content"] for record in recalled[:3])
                context = f"{goal}\n\nRelevant memory:\n{facts}"
            messages.append({"role": "user", "content": context})

        tool_calls_made: list[dict[str, Any]] = []
        usage_totals = {"inputTokens": 0, "outputTokens": 0, "costUsd": 0.0}
        final = None
        turns = 0
        while turns < self.max_turns:
            _check_cancel(stop_event)
            turns += 1
            response = self._invoke_model(mcp, messages, tool_names)
            usage_totals["inputTokens"] += response.get("usage", {}).get("inputTokens", 0)
            usage_totals["outputTokens"] += response.get("usage", {}).get("outputTokens", 0)
            usage_totals["costUsd"] += response.get("costUsd", 0.0)
            emit("model.invoked", {
                "turn": turns, "modelRef": response.get("modelRef", self.model_ref),
                "usage": response.get("usage", {}), "costUsd": response.get("costUsd", 0.0),
                "providerRequestId": response.get("providerRequestId", ""),
            })
            requested = response.get("toolCalls") or []
            if not requested:
                final = response.get("content", "")
                self._snapshot(execution_id, messages, final)
                break
            messages.append({
                "role": "assistant", "content": response.get("content", ""),
                "toolCalls": requested,
            })
            for call in requested:
                _check_cancel(stop_event)
                result = self._call_tool(mcp, call)
                tool_calls_made.append({"id": call.get("id"), "name": call.get("name")})
                emit("tool.called", {"name": call.get("name"), "turn": turns})
                messages.append({
                    "role": "tool", "toolCallId": call.get("id", ""),
                    "content": json.dumps(result)[:65536],
                })
            # Persist confirmed external side effects before the next model
            # turn. A worker crash after a tool succeeds must restore the
            # tool-role message and continue reasoning without repeating the
            # side effect. Waiting until the final answer leaves no durable
            # recovery point during the second model invocation.
            self._snapshot(execution_id, messages, None)
        if final is None:
            raise RuntimeError(f"no final answer within {self.max_turns} turns")

        memory_id = self._remember(mcp, execution_id, goal, final)
        emit("memory.written", {"id": memory_id, "namespace": self.memory_namespace})
        return {
            "answer": final,
            "turns": turns,
            "resumed": resumed,
            "toolCalls": tool_calls_made,
            "modelUsage": usage_totals,
            "memoryRecordId": memory_id,
        }

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        with self._lock:
            state = copy.deepcopy(self._live.get(execution_id, {}))
            # The adapter persists its final logical checkpoint after the
            # Runtime Interface reports a terminal result. Keep the terminal
            # snapshot alive until that read; deleting it in run() made fast
            # executions publish an empty final checkpoint. Once delivered,
            # release it so completed executions do not accumulate forever.
            if state.get("final") is not None:
                self._live.pop(execution_id, None)
        return {
            "schemaVersion": SCHEMA_VERSION,
            "state": state,
            "createdAt": _timestamp(),
        }

    def _snapshot(self, execution_id: str, messages: list[dict[str, Any]], final: str | None) -> None:
        with self._lock:
            self._live[execution_id] = {"messages": copy.deepcopy(messages), "final": final}

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint.get("schemaVersion") != SCHEMA_VERSION:
            raise ValueError("incompatible checkpoint schema")
        with self._lock:
            self._restored[execution_id] = dict(checkpoint.get("state") or {})

    # -- internals ---------------------------------------------------------------

    def _invoke_model(self, mcp: MCPClient, messages: list[dict[str, Any]], tool_names: list[str]) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "modelRef": self.model_ref,
            "messages": messages,
            "stream": True,
        }
        # Offer tools by NAME only. The broker resolves each name against the
        # AgentVersion's capability-filtered registry and authors the schema;
        # this process never submits a tool contract of its own (P0-01).
        if tool_names:
            arguments["tools"] = tool_names
        try:
            return mcp.call_tool("fenced.model.invoke", arguments)
        except MCPToolError as error:
            # Structured gateway outcomes (budget stop, provider failure) end
            # the run with the code as the failure reason.
            raise RuntimeError(f"model invocation failed: {error.payload.get('error', str(error))}") from error

    def _discover_tools(self, mcp: MCPClient) -> list[str]:
        """Return the tenant tool names to offer the model.

        tools/list is already capability-filtered by the broker to the tools
        this AgentVersion is granted. The brokered system tools (the model,
        memory and task gateways, prefixed ``fenced.``) are excluded: they are
        the agent's own plumbing, not tools the model should be prompted to
        call, and the model-tool resolver only recognizes tenant tool names.
        """
        try:
            descriptors = mcp.list_tools()
        except (MCPToolError, MCPError):
            return []
        names: list[str] = []
        for descriptor in descriptors:
            name = descriptor.get("name", "")
            if name and not name.startswith("fenced."):
                names.append(name)
        return names

    def _call_tool(self, mcp: MCPClient, call: dict[str, Any]) -> Any:
        name = call.get("name", "")
        try:
            arguments = json.loads(call.get("arguments") or "{}")
        except ValueError as error:
            raise RuntimeError(f"tool {name} returned malformed arguments") from error
        try:
            return mcp.call_tool(name, arguments)
        except MCPToolError as error:
            return {"toolError": error.payload}

    def _recall(self, mcp: MCPClient, goal: str) -> list[dict[str, Any]]:
        try:
            result = mcp.call_tool("fenced.memory.search", {"query": goal[:200], "limit": 3})
        except (MCPToolError, MCPError):
            return []
        return list(result.get("records", []))

    def _remember(self, mcp: MCPClient, execution_id: str, goal: str, final: str) -> str:
        try:
            result = mcp.call_tool("fenced.memory.put", {
                "namespace": self.memory_namespace,
                "key": f"run/{execution_id}",
                "contentType": "application/json",
                "content": json.dumps({"goal": goal, "answer": final}),
            })
            return str(result.get("id", ""))
        except (MCPToolError, MCPError):
            return ""


def _system_prompt() -> str:
    return (
        "You are an Fenced-governed agent. Answer the user's goal. "
        "When a tool would help, request it via tool calls; otherwise answer directly."
    )


def _check_cancel(stop_event: threading.Event) -> None:
    if stop_event.is_set():
        raise RuntimeError("execution cancelled")


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
