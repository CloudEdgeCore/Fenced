from __future__ import annotations

import argparse
import threading
import time
from typing import Any, Callable

from fenced_runtime import apply_legacy_compat, serve


class EchoAgent:
    def __init__(self) -> None:
        self.states: dict[str, Any] = {}

    def run(self, request: dict[str, Any], emit: Callable[[str, Any], None], stop_event: threading.Event) -> Any:
        emit("echo.received", {"goal": request["goal"]})
        if stop_event.is_set():
            raise RuntimeError("execution cancelled")
        output = {"echo": request["input"], "goal": request["goal"]}
        self.states[request["executionId"]] = output
        return output

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        return {"schemaVersion": "echo/v1", "state": self.states[execution_id], "createdAt": _timestamp()}

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None:
        if checkpoint["schemaVersion"] != "echo/v1":
            raise ValueError("incompatible checkpoint")
        self.states[execution_id] = checkpoint["state"]


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    args = parser.parse_args()
    server = serve(EchoAgent(), "python-remote", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
