#!/usr/bin/env python3
"""
Official Fenced Python Runtime Adapter.
Implements the 7-method lifecycle over HTTP conforming to fenced.runtime.interface/v1:
- health
- start
- event
- result
- checkpoint
- restore
- stop
"""

import argparse
import json
import threading
import time
from typing import Any, Dict
from fenced_runtime import AgentRuntime, apply_legacy_compat, serve


class PythonStandaloneRuntime(AgentRuntime):
    """Clean reference implementation of an Fenced Python Runtime."""

    def __init__(self):
        self.states: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def run(self, request: Dict[str, Any], emit, stop_event: threading.Event) -> Dict[str, Any]:
        execution_id = request["executionId"]
        emit("agent.started", {"executionId": execution_id, "timestamp": time.time()})

        # Check cooperative cancellation
        if stop_event.is_set():
            emit("agent.cancelled", {"executionId": execution_id})
            raise RuntimeError("execution cancelled")

        output = {
            "executionId": execution_id,
            "goal": request.get("goal", ""),
            "input": request.get("input", {}),
            "status": "COMPLETED",
            "runtime": "python-standalone",
        }

        with self._lock:
            self.states[execution_id] = output

        emit("agent.completed", output)
        return output

    def checkpoint(self, execution_id: str) -> Dict[str, Any]:
        with self._lock:
            state = self.states.get(execution_id, {})
        return {
            "schemaVersion": "agent/v1",
            "state": state,
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def restore(self, execution_id: str, checkpoint: Dict[str, Any]) -> None:
        with self._lock:
            self.states[execution_id] = checkpoint.get("state", {})


def main():
    apply_legacy_compat()
    parser = argparse.ArgumentParser(description="Fenced Python Runtime Adapter")
    parser.add_argument("--port", type=int, default=8087, help="Listen port")
    args = parser.parse_args()

    runtime = PythonStandaloneRuntime()
    server = serve(runtime, adapter="python-runtime", port=args.port)
    print(f"Starting Python Runtime Adapter on port {args.port}...")
    server.serve_forever()


if __name__ == "__main__":
    main()
