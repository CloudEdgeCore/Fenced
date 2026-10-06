"""CrewAI adapter for the Agent OS Runtime Interface.

The adapter encapsulates CrewAI role-playing multi-agent crew execution (Crew, Agent, Task).
Importing CrewAI itself remains the application's responsibility, preserving
kernel and protocol independence from framework internals.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class CrewAIRuntime:
    """Adapts CrewAI multi-agent crews to Fenced Runtime Interface v1."""

    def __init__(self, crew: Any, *, schema_version: str = "crewai-state/v1") -> None:
        self.crew = crew
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

        emit("crewai.started", {"executionId": execution_id, "agentVersionRef": request.get("agentVersionRef", "")})

        crew_inputs = self.restored.pop(execution_id, request["input"])
        if not isinstance(crew_inputs, dict):
            crew_inputs = {"topic": str(crew_inputs)}

        output: Any = None
        if callable(getattr(self.crew, "kickoff", None)):
            emit("crewai.kickoff", {"executionId": execution_id})
            output = self.crew.kickoff(inputs=crew_inputs)
        elif callable(getattr(self.crew, "run", None)):
            output = self.crew.run(inputs=crew_inputs)
        elif callable(getattr(self.crew, "invoke", None)):
            output = self.crew.invoke(crew_inputs)
        else:
            raise TypeError("crew must provide kickoff, run, or invoke")

        with self.lock:
            self.states[execution_id] = {
                "crew_output": output,
                "tasks_output": getattr(self.crew, "tasks_output", []),
            }

        emit("crewai.completed", {"executionId": execution_id})
        return output

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        with self.lock:
            if execution_id not in self.states:
                raise KeyError(f"CrewAI execution {execution_id} has no checkpointable state")
            state = self.states[execution_id]

        return {
            "schemaVersion": self.schema_version,
            "state": state,
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint.get("schemaVersion") != self.schema_version:
            raise ValueError(f"incompatible CrewAI checkpoint schema: {checkpoint.get('schemaVersion')}")
        with self.lock:
            self.restored[execution_id] = checkpoint["state"]
