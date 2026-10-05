import http.client
import json
import threading
import time
import unittest
from http.server import ThreadingHTTPServer

from fenced_runtime.host import RuntimeHost, _Execution


def start_request(execution_id: str) -> dict:
    return {
        "executionId": execution_id,
        "agentVersionRef": "fixture@1.0.0",
        "goal": "test",
        "input": {},
        "capabilities": {"tools": [], "models": [], "memory": [], "secrets": []},
    }


class BlockingRuntime:
    def run(self, _request, _emit, stop_event):
        stop_event.wait(1)
        return {"late": True}

    def checkpoint(self, _execution_id):
        return {"schemaVersion": "fixture/v1", "state": {}, "createdAt": "now"}

    def restore(self, _execution_id, _checkpoint):
        return None


class FailingRuntime(BlockingRuntime):
    def run(self, _request, _emit, _stop_event):
        raise RuntimeError("secret-provider-token")


class StubbornRuntime(BlockingRuntime):
    def run(self, _request, _emit, _stop_event):
        time.sleep(0.1)
        return {"late": True}


class StuckForeverRuntime(BlockingRuntime):
    """Ignores stop_event entirely; only the test's release event ends it."""

    def __init__(self) -> None:
        self.release = threading.Event()

    def run(self, _request, _emit, _stop_event):
        self.release.wait(30)
        return {"too": "late"}


class RuntimeHostTests(unittest.TestCase):
    def run_execution(self, host: RuntimeHost, execution_id: str) -> _Execution:
        execution = _Execution(digest=b"digest")
        with host.lock:
            host.executions[execution_id] = execution
            host.active += 1
        worker = threading.Thread(
            target=host._run,
            args=(start_request(execution_id), execution),
            daemon=True,
        )
        worker.start()
        worker.join(1)
        self.assertFalse(worker.is_alive())
        return execution

    def test_execution_watchdog_is_terminal_and_capacity_safe(self):
        host = RuntimeHost(BlockingRuntime(), "fixture", execution_timeout=0.02)
        execution = self.run_execution(host, "timeout")
        self.assertEqual(execution.result["status"], "FAILED")
        self.assertEqual(execution.result["errorCode"], "EXECUTION_TIMEOUT")
        self.assertEqual(host.active, 0)

    def test_adapter_exception_is_not_disclosed(self):
        host = RuntimeHost(FailingRuntime(), "fixture")
        execution = self.run_execution(host, "failure")
        self.assertEqual(execution.result["errorCode"], "ADAPTER_FAILED")
        self.assertNotIn("secret-provider-token", execution.result["error"])

    def test_stuck_execution_escalates_to_force_terminator(self):
        called = threading.Event()
        host = RuntimeHost(
            StubbornRuntime(), "isolated", execution_timeout=0.01,
            termination_grace=0.01, force_terminate=lambda _execution_id: called.set(),
        )
        execution = _Execution(digest=b"digest")
        with host.lock:
            host.executions["stuck"] = execution
            host.active += 1
        worker = threading.Thread(target=host._run, args=(start_request("stuck"), execution), daemon=True)
        worker.start()
        self.assertTrue(called.wait(0.2))
        worker.join(0.3)

    def test_stuck_agent_is_terminal_forced_and_capacity_bounded(self):
        # A stuck agent (ignores stop_event forever) still gets: a terminal
        # FAILED/EXECUTION_TIMEOUT result, the force_terminate escalation
        # within the bounded grace, and its ledger capacity back — even
        # though the thread itself cannot be killed from inside the
        # process (the documented protocol-host boundary).
        runtime = StuckForeverRuntime()
        force_terminated = threading.Event()
        host = RuntimeHost(
            runtime, "isolated", execution_timeout=0.02,
            termination_grace=0.05, force_terminate=lambda _id: force_terminated.set(),
        )
        execution = _Execution(digest=b"digest")
        with host.lock:
            host.executions["stuck"] = execution
            host.active += 1
        worker = threading.Thread(target=host._run, args=(start_request("stuck"), execution), daemon=True)
        try:
            worker.start()
            deadline = time.monotonic() + 1
            while execution.result is None and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertEqual(execution.result["status"], "FAILED")
            self.assertEqual(execution.result["errorCode"], "EXECUTION_TIMEOUT")
            self.assertTrue(force_terminated.wait(1))
            deadline = time.monotonic() + 1
            while host.active != 0 and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertEqual(host.active, 0)
            self.assertTrue(worker.is_alive())
        finally:
            runtime.release.set()
            worker.join(1)

    def test_stuck_agent_without_terminator_still_releases_capacity(self):
        runtime = StuckForeverRuntime()
        host = RuntimeHost(runtime, "isolated", execution_timeout=0.02, termination_grace=0.05)
        execution = _Execution(digest=b"digest")
        with host.lock:
            host.executions["stuck"] = execution
            host.active += 1
        worker = threading.Thread(target=host._run, args=(start_request("stuck"), execution), daemon=True)
        try:
            worker.start()
            deadline = time.monotonic() + 1
            while host.active != 0 and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertEqual(host.active, 0)
            self.assertIsNotNone(execution.result)
        finally:
            runtime.release.set()
            worker.join(1)

    def test_events_stream_pushes_events_and_terminal_result(self):
        host = RuntimeHost(BlockingRuntime(), "fixture")
        server = ThreadingHTTPServer(("127.0.0.1", 0), host.handler())
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            body = json.dumps(start_request("stream"))
            connection.request("POST", "/v1/executions:start", body=body, headers={"Content-Type": "application/json"})
            reply = connection.getresponse()
            self.assertEqual(reply.status, 202)
            reply.read()

            stream = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            stream.request("GET", "/v1/executions/stream/events/stream?after=0", headers={"Accept": "text/event-stream"})
            response = stream.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.getheader("Content-Type"), "text/event-stream")
            frames = []
            while True:
                line = response.readline().decode()
                if line == "":
                    # Server closed the connection without a result frame.
                    self.fail("stream ended without a terminal result frame")
                line = line.strip()
                if not line:
                    if frames and frames[-1][1] == "result":
                        break
                    continue
                if line.startswith(":"):
                    continue
                name, _, value = line.partition(": ")
                if name == "event":
                    frames.append(["", value, ""])
                elif name == "data" and frames:
                    frames[-1][2] = value
            self.assertEqual(frames[-1][1], "result")
            result = json.loads(frames[-1][2])
            self.assertEqual(result["status"], "SUCCEEDED")
            self.assertEqual(result["output"], {"late": True})

            # A resumed stream after the terminal frame replays nothing.
            resumed = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            resumed.request("GET", "/v1/executions/stream/events/stream?after=1000")
            response = resumed.getresponse()
            line = response.readline().decode().strip()
            self.assertEqual(line, "event: result")
            data = json.loads(response.readline().decode().strip().partition("data: ")[2])
            self.assertEqual(data["status"], "SUCCEEDED")
        finally:
            server.shutdown()
            server.server_close()

    def test_control_operation_has_deadline(self):
        host = RuntimeHost(BlockingRuntime(), "fixture", control_timeout=0.01)
        with self.assertRaises(TimeoutError):
            host._call_with_timeout(time.sleep, 0.2)

    def test_forward_capability_fields_are_accepted(self):
        body = start_request("future")
        body["futureField"] = {"ignored": True}
        body["capabilities"]["memorySensitivities"] = ["confidential"]
        RuntimeHost._validate_start(body)


if __name__ == "__main__":
    unittest.main()
