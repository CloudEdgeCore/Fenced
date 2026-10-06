from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from fenced_runtime import apply_legacy_compat, serve

REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "adapters", "langgraph"))

from fenced_langgraph import LangGraphRuntime  # noqa: E402


class MinimalGraph:
    """Dependency-free conformance graph with LangGraph's invoke contract."""

    def invoke(self, graph_input: Any, *, config: dict[str, Any]) -> Any:
        return {"framework": "langgraph", "input": graph_input, "thread": config["configurable"]["thread_id"]}


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8089)
    args = parser.parse_args()
    server = serve(LangGraphRuntime(MinimalGraph()), "langgraph", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
