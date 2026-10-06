"""Tests for framework ecosystem wrappers."""

import threading
import unittest

from fenced_runtime.ecosystem import FrameworkAdapterRuntime


class EcosystemWrapperTests(unittest.TestCase):
    def test_framework_adapter_runtime_execution(self):
        called = False

        def mock_runner(req, client, stop_event):
            nonlocal called
            called = True
            return {"greeting": "hello " + req.get("name", "world")}

        runtime = FrameworkAdapterRuntime("mock_framework", mock_runner)

        events = []
        def emit(event, data):
            events.append((event, data))

        stop_event = threading.Event()
        result = runtime.run({"executionId": "test-exec", "name": "agent"}, emit, stop_event)

        self.assertTrue(called)
        self.assertEqual(result.get("greeting"), "hello agent")
        self.assertTrue(any(e[0] == "mock_framework.starting" for e in events))
        self.assertTrue(any(e[0] == "mock_framework.completed" for e in events))

    def test_framework_adapter_with_checkpoint(self):
        saved_state = None

        class MockCheckpoint:
            def restore(self):
                return {"restored_step": 2}
            def save(self, state):
                nonlocal saved_state
                saved_state = state
                return {"saved": True}

        class MockClient:
            def __init__(self):
                self.checkpoint = MockCheckpoint()

        restored_step = None
        def mock_restorer(cp):
            nonlocal restored_step
            restored_step = cp.get("restored_step")

        def mock_serializer():
            return {"final_step": 3}

        def mock_runner(req, client, stop_event):
            return {"step": 3}

        runtime = FrameworkAdapterRuntime(
            "mock_framework",
            mock_runner,
            state_serializer=mock_serializer,
            state_restorer=mock_restorer,
            client=MockClient(),
        )

        events = []
        result = runtime.run({"executionId": "test-exec"}, lambda e, d: events.append(e), threading.Event())
        self.assertEqual(result.get("step"), 3)
        self.assertEqual(restored_step, 2)
        self.assertEqual(saved_state, {"final_step": 3})
        self.assertIn("mock_framework.restoring", events)


if __name__ == "__main__":
    unittest.main()

