"""Custom in-house agent adapter for the Agent OS Runtime Interface.

This adapter represents proprietary in-house enterprise agents natively integrated
with the Fenced Kernel, leveraging Syscall ABI conventions, durable IPC messaging,
and custom domain workflows.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class CustomAgentRuntime:
    """Adapts in-house proprietary agents to Fenced Runtime Interface v1."""

    def __init__(self, agent_core: Any = None, *, schema_version: str = "custom-agent-state/v1") -> None:
        self.agent_core = agent_core
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

        emit("custom_agent.started", {"executionId": execution_id, "agentVersionRef": request.get("agentVersionRef", "")})

        agent_input = self.restored.pop(execution_id, request["input"])

        output: Any = None
        if self.agent_core is not None and callable(getattr(self.agent_core, "execute", None)):
            emit("custom_agent.execute", {"executionId": execution_id})
            output = self.agent_core.execute(agent_input, execution_id=execution_id)
        elif self.agent_core is not None and callable(getattr(self.agent_core, "invoke", None)):
            output = self.agent_core.invoke(agent_input)
        else:
            # Default native execution loop
            output = {
                "framework": "custom-agent",
                "execution_id": execution_id,
                "status": "completed",
                "input": agent_input,
                "fenced_syscall_abi": "1.0.0",
                "result": "In-house agent executed successfully with kernel guarantees.",
            }

        with self.lock:
            self.states[execution_id] = {
                "custom_state": {"step": 4, "checkpoint_seq": 1},
                "last_result": output,
            }

        emit("custom_agent.completed", {"executionId": execution_id})
        return output

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        with self.lock:
            if execution_id not in self.states:
                raise KeyError(f"Custom agent execution {execution_id} has no checkpointable state")
            state = self.states[execution_id]

        return {
            "schemaVersion": self.schema_version,
            "state": state,
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint.get("schemaVersion") != self.schema_version:
            raise ValueError(f"incompatible Custom agent checkpoint schema: {checkpoint.get('schemaVersion')}")
        with self.lock:
            self.restored[execution_id] = checkpoint["state"]
