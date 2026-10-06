"""AutoGen adapter for the Agent OS Runtime Interface.

The adapter encapsulates Microsoft AutoGen multi-agent conversational patterns
(ConversableAgent, AssistantAgent, UserProxyAgent, GroupChat).
Importing AutoGen itself remains the application's responsibility, preserving
kernel and protocol independence from framework internals.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class AutoGenRuntime:
    """Adapts AutoGen conversational agents to Fenced Runtime Interface v1."""

    def __init__(self, agent_or_group: Any, *, schema_version: str = "autogen-state/v1") -> None:
        self.agent_or_group = agent_or_group
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

        emit("autogen.started", {"executionId": execution_id, "agentVersionRef": request.get("agentVersionRef", "")})

        # Use restored conversation history if recovering from checkpoint
        chat_input = self.restored.pop(execution_id, request["input"])

        # AutoGen pattern: initiate_chat or generate_reply or invoke
        output: Any = None
        if callable(getattr(self.agent_or_group, "initiate_chat", None)):
            emit("autogen.chat_initiated", {"executionId": execution_id})
            output = self.agent_or_group.initiate_chat(chat_input, execution_id=execution_id)
        elif callable(getattr(self.agent_or_group, "generate_reply", None)):
            emit("autogen.reply_generated", {"executionId": execution_id})
            output = self.agent_or_group.generate_reply(messages=[{"role": "user", "content": str(chat_input)}])
        elif callable(getattr(self.agent_or_group, "invoke", None)):
            output = self.agent_or_group.invoke(chat_input)
        else:
            raise TypeError("agent_or_group must provide initiate_chat, generate_reply, or invoke")

        with self.lock:
            self.states[execution_id] = {
                "chat_history": getattr(self.agent_or_group, "chat_messages", {}),
                "last_output": output,
            }

        emit("autogen.completed", {"executionId": execution_id})
        return output

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        with self.lock:
            if execution_id not in self.states:
                raise KeyError(f"AutoGen execution {execution_id} has no checkpointable state")
            state = self.states[execution_id]

        return {
            "schemaVersion": self.schema_version,
            "state": state,
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint.get("schemaVersion") != self.schema_version:
            raise ValueError(f"incompatible AutoGen checkpoint schema: {checkpoint.get('schemaVersion')}")
        with self.lock:
            self.restored[execution_id] = checkpoint["state"]
