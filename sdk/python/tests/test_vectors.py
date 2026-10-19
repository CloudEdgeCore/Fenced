"""Run the shared golden wire vectors against the Python Runtime Interface host.

The same document is run by the Go SDK and checked against the OpenAPI
contract, so the three implementations of the interface cannot drift apart.
"""

from __future__ import annotations

import json
import os
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from fenced_runtime import RuntimeHost, serve
from fenced_runtime.host import AgentRuntime

VECTORS_PATH = (
    Path(__file__).resolve().parents[2] / "conformance" / "fixtures" / "runtime-interface-v1.json"
)


def load_vectors() -> dict[str, Any]:
    with open(VECTORS_PATH, "r", encoding="utf-8") as handle:
        vectors = json.load(handle)
    assert vectors["schema"] == "fenced.runtime-interface.vectors/v1", vectors["schema"]
    return vectors


class VectorRuntime(AgentRuntime):
    """Deterministic runtime: one event, a fixed checkpoint, waits for cancel."""

    def __init__(self) -> None:
        self.released = threading.Event()

    def run(self, request: dict[str, Any], emit: Any, stop_event: threading.Event) -> Any:
        emit("vector.started", {"probe": "vector"})
        while not stop_event.is_set() and not self.released.is_set():
            stop_event.wait(0.05)
        return {"done": True}

    def checkpoint(self, execution_id: str) -> dict[str, Any]:
        return {
            "schemaVersion": "vector/v1",
            "state": {"step": "vector"},
            "createdAt": "2026-01-01T00:00:00Z",
        }

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> bool:
        return True


class RuntimeInterfaceVectors(unittest.TestCase):
    """Every vector must hold on the Python host too."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.vectors = load_vectors()
        cls.runtime = VectorRuntime()
        cls.server = serve(cls.runtime, "vector", "127.0.0.1", 0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.server.server_address[0], cls.server.server_address[1]
        cls.base_url = f"http://{host}:{port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.runtime.released.set()
        cls.server.shutdown()
        cls.server.server_close()

    def _call(self, method: str, path: str, body: Any = None) -> tuple[int, dict[str, Any]]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(self.base_url + path, data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_vectors(self) -> None:
        execution_id = self.vectors["executionId"]
        for vector in self.vectors["endpoints"]:
            with self.subTest(vector=vector["name"]):
                path = vector["path"].replace("{executionId}", execution_id)
                status, document = self._call(vector["method"], path, vector.get("request"))
                self.assertEqual(status, vector["status"], f"{vector['name']}: {document}")
                present = set(document)
                for field in vector["requiredFields"]:
                    self.assertIn(field, present, f"{vector['name']}: {document}")
                known = set(vector["requiredFields"]) | set(vector["optionalFields"])
                unknown = present - known
                self.assertFalse(unknown, f"{vector['name']}: unknown fields {sorted(unknown)}")
                for key, want in vector["values"].items():
                    self.assertIn(key, document, f"{vector['name']}: {document}")
                    self.assertEqual(document[key], want, f"{vector['name']}: {key}")

    def test_execution_status_vocabulary(self) -> None:
        """The Python host must not invent a status outside the vocabulary."""
        from fenced_runtime import host as host_module

        declared = {
            getattr(host_module, name)
            for name in dir(host_module)
            if name.isupper() and isinstance(getattr(host_module, name), str)
        }
        declared &= {
            "ACCEPTED",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        }
        self.assertTrue(declared, "the Python host must declare the execution statuses")
        self.assertEqual(
            sorted(declared), sorted(self.vectors["enums"]["executionStatus"])
        )


if __name__ == "__main__":
    unittest.main()
