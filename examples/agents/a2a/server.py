from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from fenced_runtime import apply_legacy_compat, serve

REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "adapters", "a2a"))

from fenced_a2a import A2ARuntime  # noqa: E402


class ConformanceA2AClient:
    def send_message(self, message: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": message["messageId"],
            "contextId": message["contextId"],
            "status": {"state": "completed"},
            "artifacts": [{"artifactId": "result", "parts": message["parts"]}],
        }

    def cancel_task(self, _task_id: str) -> None:
        return


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()
    server = serve(A2ARuntime(ConformanceA2AClient()), "a2a", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
