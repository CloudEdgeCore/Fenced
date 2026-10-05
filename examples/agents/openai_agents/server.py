from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from fenced_runtime import apply_legacy_compat, serve

REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "adapters", "openai_agents"))

from fenced_openai_agents import OpenAIAgentsRuntime  # noqa: E402


class MinimalOpenAIAgentRunner:
    """Dependency-free conformance runner with OpenAI Agents SDK run contract."""

    def __init__(self) -> None:
        self.thread_messages: dict[str, list[dict[str, Any]]] = {}

    def run(self, user_prompt: Any, *, thread_id: str) -> dict[str, Any]:
        self.thread_messages[thread_id] = [
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": "Executing task with tool calling and handoffs."},
            {"role": "tool", "name": "web_search", "content": "Search result returned."},
            {"role": "assistant", "content": "Completed user prompt analysis."},
        ]
        return {
            "framework": "openai-agents",
            "thread_id": thread_id,
            "status": "completed",
            "model": "gpt-4o",
            "response": "OpenAI Agents SDK task finished with 1 tool call.",
        }


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8092)
    args = parser.parse_args()
    server = serve(OpenAIAgentsRuntime(MinimalOpenAIAgentRunner()), "openai-agents", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
