"""LangGraph adapter for the Agent OS Runtime Interface.

The adapter depends only on LangGraph's stable ``invoke`` shape. Importing
LangGraph itself remains the Agent application's responsibility, preserving
Kernel and protocol independence from framework internals.
"""

from __future__ import annotations

import threading
from typing import Any, Callable


class LangGraphRuntime:
    def __init__(self, graph: Any, *, schema_version: str = "langgraph-state/v1") -> None:
        if not callable(getattr(graph, "invoke", None)):
            raise TypeError("graph must provide invoke(input, config=...)")
        self.graph = graph
        self.schema_version = schema_version
        self.states: dict[str, Any] = {}
        self.restored: dict[str, Any] = {}
        self.lock = threading.RLock()

    def run(self, request: dict[str, Any], emit: Callable[[str, Any], None], stop_event: threading.Event) -> Any:
        execution_id = request["executionId"]
        if stop_event.is_set():
            raise RuntimeError("execution cancelled")
        emit("langgraph.started", {"executionId": execution_id})
        graph_input = self.restored.pop(execution_id, request["input"])
        config = {
            "configurable": {
                "thread_id": execution_id,
                "agent_version_ref": request["agentVersionRef"],
            }
        }
        output = self.graph.invoke(graph_input, config=config)
        with self.lock:
            self.states[execution_id] = output
        emit("langgraph.completed", {"executionId": execution_id})
        return output

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        from time import strftime, gmtime

        with self.lock:
            if execution_id not in self.states:
                raise KeyError("LangGraph execution has no checkpointable state")
            state = self.states[execution_id]
        return {
            "schemaVersion": self.schema_version,
            "state": state,
            "createdAt": strftime("%Y-%m-%dT%H:%M:%SZ", gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint["schemaVersion"] != self.schema_version:
            raise ValueError("incompatible LangGraph checkpoint schema")
        with self.lock:
            self.restored[execution_id] = checkpoint["state"]
