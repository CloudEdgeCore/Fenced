"""A2A client adapter for the Agent OS Runtime Interface.

The Kernel depends only on the Runtime Interface. This adapter translates that
stable lifecycle into the small A2A client surface (send/get/cancel), keeping
A2A task and artifact shapes outside Kernel packages.
"""

from __future__ import annotations

import threading
from time import gmtime, strftime
from typing import Any, Callable


class A2ARuntime:
    def __init__(self, client: Any, *, schema_version: str = "a2a-task/v1") -> None:
        if not callable(getattr(client, "send_message", None)):
            raise TypeError("A2A client must provide send_message(message)")
        self.client = client
        self.schema_version = schema_version
        self.tasks: dict[str, dict[str, Any]] = {}
        self.restored: dict[str, dict[str, Any]] = {}
        self.lock = threading.RLock()

    def run(self, request: dict[str, Any], emit: Callable[[str, Any], None], stop_event: threading.Event) -> Any:
        execution_id = request["executionId"]
        if stop_event.is_set():
            raise RuntimeError("execution cancelled")
        previous = self.restored.pop(execution_id, None)
        message = {
            "messageId": execution_id,
            "contextId": execution_id,
            "role": "user",
            "parts": [{"kind": "data", "data": request["input"]}, {"kind": "text", "text": request["goal"]}],
            "metadata": {
                "agentVersionRef": request["agentVersionRef"],
                "capabilities": request["capabilities"],
                "restoredTask": previous,
            },
        }
        emit("a2a.submitted", {"contextId": execution_id})
        task = self.client.send_message(message)
        if not isinstance(task, dict):
            raise TypeError("A2A client returned a non-object task")
        if stop_event.is_set():
            cancel = getattr(self.client, "cancel_task", None)
            if callable(cancel):
                cancel(task.get("id", execution_id))
            raise RuntimeError("execution cancelled")
        state = str(task.get("status", {}).get("state", "completed")).lower()
        if state in {"failed", "rejected", "canceled", "cancelled"}:
            raise RuntimeError(f"A2A task terminated in state {state}")
        with self.lock:
            self.tasks[execution_id] = task
        emit("a2a.completed", {"contextId": execution_id, "state": state})
        return {"task": task}

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        with self.lock:
            if execution_id not in self.tasks:
                raise KeyError("A2A execution has no checkpointable task")
            task = self.tasks[execution_id]
        return {
            "schemaVersion": self.schema_version,
            "state": task,
            "createdAt": strftime("%Y-%m-%dT%H:%M:%SZ", gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint["schemaVersion"] != self.schema_version:
            raise ValueError("incompatible A2A checkpoint schema")
        if not isinstance(checkpoint["state"], dict):
            raise ValueError("A2A checkpoint state must be an object")
        with self.lock:
            self.restored[execution_id] = checkpoint["state"]
