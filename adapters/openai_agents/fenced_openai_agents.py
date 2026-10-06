"""OpenAI Agents SDK adapter for the Agent OS Runtime Interface.

The adapter encapsulates OpenAI Agents SDK execution patterns (Agent, Runner, Handoffs, Tools).
Importing the OpenAI SDK itself remains the application's responsibility, preserving
kernel and protocol independence from framework internals.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class OpenAIAgentsRuntime:
    """Adapts OpenAI Agents SDK runners to Fenced Runtime Interface v1."""

    def __init__(self, agent_or_runner: Any, *, schema_version: str = "openai-agents-state/v1") -> None:
        self.agent_or_runner = agent_or_runner
        self.schema_version = schema_version
        self.states: dict[str, Any] = {}
        self.restored: dict[str, Any] = {}
        self.lock = threading.RLock()

    def run(
        self,
        request: dict[str, Any],
        emit: Callable[[str, Any], None],
        stop_event: threading.Event,
    ) -> Any:
        execution_id = request["executionId"]
        if stop_event.is_set():
            raise RuntimeError("execution cancelled")

        emit("openai_agents.started", {"executionId": execution_id, "agentVersionRef": request.get("agentVersionRef", "")})

        thread_input = self.restored.pop(execution_id, request["input"])

        output: Any = None
        if callable(getattr(self.agent_or_runner, "run", None)):
            emit("openai_agents.runner_invoked", {"executionId": execution_id})
            output = self.agent_or_runner.run(thread_input, thread_id=execution_id)
        elif callable(getattr(self.agent_or_runner, "invoke", None)):
            output = self.agent_or_runner.invoke(thread_input)
        else:
            raise TypeError("agent_or_runner must provide run or invoke")

        with self.lock:
            self.states[execution_id] = {
                "thread_messages": getattr(self.agent_or_runner, "thread_messages", {}).get(execution_id, []),
                "final_output": output,
            }

        emit("openai_agents.completed", {"executionId": execution_id})
        return output

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        with self.lock:
            if execution_id not in self.states:
                raise KeyError(f"OpenAI Agents execution {execution_id} has no checkpointable state")
            state = self.states[execution_id]

        return {
            "schemaVersion": self.schema_version,
            "state": state,
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint.get("schemaVersion") != self.schema_version:
            raise ValueError(f"incompatible OpenAI Agents checkpoint schema: {checkpoint.get('schemaVersion')}")
        with self.lock:
            self.restored[execution_id] = checkpoint["state"]
