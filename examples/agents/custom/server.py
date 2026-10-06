from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from fenced_runtime import apply_legacy_compat, serve

REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "adapters", "custom_agent"))

from fenced_custom import CustomAgentRuntime  # noqa: E402


class MinimalCustomAgent:
    """Enterprise in-house agent core with custom execution lifecycle."""

    def execute(self, task_input: Any, *, execution_id: str) -> dict[str, Any]:
        return {
            "framework": "custom-agent",
            "execution_id": execution_id,
            "processed": True,
            "kernel_syscall_boundary": "1.0.0",
            "ipc_mailbox": "active",
            "effect_isolation": "fenced",
            "output": f"Enterprise task processed: {task_input}",
        }


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8093)
    args = parser.parse_args()
    server = serve(CustomAgentRuntime(MinimalCustomAgent()), "custom-agent", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
