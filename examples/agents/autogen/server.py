from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from fenced_runtime import apply_legacy_compat, serve

REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "adapters", "autogen"))

from fenced_autogen import AutoGenRuntime  # noqa: E402


class MinimalAutoGenGroup:
    """Dependency-free conformance group with AutoGen's initiate_chat contract."""

    def __init__(self) -> None:
        self.chat_messages: dict[str, list[dict[str, Any]]] = {}

    def initiate_chat(self, message: Any, *, execution_id: str) -> dict[str, Any]:
        self.chat_messages[execution_id] = [
            {"role": "user", "content": message},
            {"role": "assistant", "name": "coder", "content": "Code executed successfully."},
            {"role": "assistant", "name": "reviewer", "content": "Review approved."},
        ]
        return {
            "framework": "autogen",
            "execution_id": execution_id,
            "turns": 2,
            "summary": "AutoGen multi-agent conversation completed successfully.",
        }


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()
    server = serve(AutoGenRuntime(MinimalAutoGenGroup()), "autogen", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
