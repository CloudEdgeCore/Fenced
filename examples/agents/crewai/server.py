from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from fenced_runtime import apply_legacy_compat, serve

REPOSITORY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPOSITORY_ROOT, "adapters", "crewai"))

from fenced_crewai import CrewAIRuntime  # noqa: E402


class MinimalCrew:
    """Dependency-free conformance crew with CrewAI's kickoff contract."""

    def __init__(self) -> None:
        self.tasks_output: list[dict[str, Any]] = []

    def kickoff(self, *, inputs: dict[str, Any]) -> dict[str, Any]:
        self.tasks_output = [
            {"task": "research", "status": "completed", "output": "Gathered data for " + str(inputs)},
            {"task": "synthesis", "status": "completed", "output": "Synthesized final report."},
        ]
        return {
            "framework": "crewai",
            "crew_status": "success",
            "tasks_count": 2,
            "raw": "Crew execution completed across all defined roles.",
        }


if __name__ == "__main__":
    apply_legacy_compat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    args = parser.parse_args()
    server = serve(CrewAIRuntime(MinimalCrew()), "crewai", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()
