"""Standard-library host for stable fenced.runtime.interface/v1.

Framework adapters implement AgentRuntime; this module owns transport,
idempotency, bounded events, cancellation, checkpoint/restore and results.
No framework or model provider is imported by the protocol layer.

Isolation boundary: RuntimeHost is a *protocol host*, not a security or
isolation boundary. Agent code runs as Python threads inside this process,
and Python threads cannot be forcibly killed — an agent that ignores
``stop_event`` keeps its thread alive. The host therefore bounds what it
can bound: the watchdog finalizes a timed-out execution terminally, blocks
further event emission, releases its ledger capacity after the termination
grace, and escalates to the deployment-provided ``force_terminate`` hook.
A true hard kill must come from outside the process: run production agents
in a killable sandbox (separate process supervised by the adapter, OCI
container, gVisor sandbox, or microVM) and map ``force_terminate`` to the
corresponding PID/cgroup/container kill.
"""

from __future__ import annotations

import hashlib
import json
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Protocol
from urllib.parse import parse_qs, urlparse

PROTOCOL_VERSION = "fenced.runtime.interface/v1"
LEGACY_PROTOCOL_VERSION = "fenced.runtime.interface/v1alpha1"
MAX_BODY = 2 << 20
MAX_EVENT_PAYLOAD = 256 << 10
MAX_EVENT_PAGE = 1 << 20
MAX_EVENTS_PER_PAGE = 256
MAX_RESULT = 2 << 20
DEFAULT_EXECUTION_TIMEOUT = 60 * 60
DEFAULT_CONTROL_TIMEOUT = 10.0
SOCKET_TIMEOUT = 30.0


class AgentRuntime(Protocol):
    def run(
        self,
        request: dict[str, Any],
        emit: Callable[[str, Any], None],
        stop_event: threading.Event,
    ) -> Any: ...

    def checkpoint(self, execution_id: str) -> dict[str, Any]: ...

    def restore(self, execution_id: str, checkpoint: dict[str, Any]) -> None: ...


@dataclass
class _Execution:
    digest: bytes
    created_at: float = field(default_factory=time.monotonic)
    stop_event: threading.Event = field(default_factory=threading.Event)
    status: str = "ACCEPTED"
    events: list[dict[str, Any]] = field(default_factory=list)
    next_sequence: int = 1
    result: dict[str, Any] | None = None
    finalized: bool = False
    capacity_released: bool = False


class RuntimeHost:
    """Thread-safe black-box host shared by Python and framework adapters.

    This host terminates executions at the *protocol* level only; it is not
    a security or isolation boundary (see the module docstring). The
    optional ``force_terminate`` hook receives the execution id once a
    finalized execution is still occupying capacity after
    ``termination_grace`` — deployments that manage agent subprocesses map
    it to a real process/cgroup/container kill.
    """

    def __init__(
        self,
        runtime: AgentRuntime,
        adapter: str,
        *,
        max_concurrent: int = 16,
        event_limit: int = 1024,
        execution_limit: int = 4096,
        execution_timeout: float = DEFAULT_EXECUTION_TIMEOUT,
        control_timeout: float = DEFAULT_CONTROL_TIMEOUT,
        termination_grace: float = 5.0,
        force_terminate: Callable[[str], None] | None = None,
    ) -> None:
        if not adapter.strip():
            raise ValueError("adapter identity is required")
        if (
            max_concurrent <= 0
            or event_limit <= 0
            or execution_limit < max_concurrent
            or execution_timeout <= 0
            or control_timeout <= 0
            or termination_grace <= 0
        ):
            raise ValueError("host limits must be positive")
        self.runtime = runtime
        self.adapter = adapter
        self.max_concurrent = max_concurrent
        self.event_limit = event_limit
        self.execution_limit = execution_limit
        self.execution_timeout = execution_timeout
        self.control_timeout = control_timeout
        self.termination_grace = termination_grace
        self.force_terminate = force_terminate
        self.lock = threading.RLock()
        self.executions: dict[str, _Execution] = {}
        self.active = 0
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent, thread_name_prefix="fenced-execution")
        self.control_slots = threading.BoundedSemaphore(max_concurrent)

    def handler(self) -> type[BaseHTTPRequestHandler]:
        host = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "FencedRuntime/1.0"

            def setup(self) -> None:
                super().setup()
                self.connection.settimeout(SOCKET_TIMEOUT)

            def do_GET(self) -> None:  # noqa: N802
                host._handle(self, "GET")

            def do_POST(self) -> None:  # noqa: N802
                host._handle(self, "POST")

            def log_message(self, _format: str, *_args: Any) -> None:
                return

        return Handler

    def _handle(self, handler: BaseHTTPRequestHandler, method: str) -> None:
        parsed = urlparse(handler.path)
        if parsed.path == "/v1" or parsed.path.startswith("/v1/"):
            prefix, protocol = "/v1", PROTOCOL_VERSION
        elif parsed.path == "/v1alpha1" or parsed.path.startswith("/v1alpha1/"):
            prefix, protocol = "/v1alpha1", LEGACY_PROTOCOL_VERSION
        else:
            self._problem(handler, 404, "ROUTE_NOT_FOUND", "runtime interface route not found")
            return
        handler.fenced_protocol = protocol
        if method == "GET" and parsed.path == f"{prefix}/health":
            with self.lock:
                active = self.active
            self._write(
                handler,
                200,
                {
                    "status": "SERVING",
                    "protocolVersions": [protocol],
                    "adapter": self.adapter,
                    "maxConcurrent": self.max_concurrent,
                    "activeExecutions": active,
                },
            )
            return
        if method == "POST" and parsed.path == f"{prefix}/executions:start":
            self._start(handler)
            return
        execution_prefix = f"{prefix}/executions/"
        if not parsed.path.startswith(execution_prefix):
            self._problem(handler, 404, "ROUTE_NOT_FOUND", "runtime interface route not found")
            return
        remainder = parsed.path[len(execution_prefix) :]
        actions = {
            ":stop": "stop",
            ":checkpoint": "checkpoint",
            ":restore": "restore",
            "/events/stream": "events_stream",
            "/events": "events",
            "/result": "result",
        }
        for suffix, action in actions.items():
            if remainder.endswith(suffix):
                execution_id = remainder[: -len(suffix)]
                if not execution_id or "/" in execution_id:
                    self._problem(handler, 400, "INVALID_EXECUTION_ID", "execution id is invalid")
                    return
                getattr(self, f"_{action}")(handler, execution_id, method, parsed.query)
                return
        self._problem(handler, 404, "ROUTE_NOT_FOUND", "runtime execution route not found")

    def _start(self, handler: BaseHTTPRequestHandler) -> None:
        body = self._read_json(handler)
        if body is None:
            return
        try:
            self._validate_start(body)
        except ValueError as error:
            self._problem(handler, 422, "INVALID_START", str(error))
            return
        canonical = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
        digest = hashlib.sha256(canonical).digest()
        execution_id = body["executionId"]
        with self.lock:
            existing = self.executions.get(execution_id)
            if existing is not None:
                if existing.digest != digest:
                    self._problem(handler, 409, "EXECUTION_CONFLICT", "execution id conflicts with another request")
                    return
                self._write(
                    handler,
                    200,
                    {"executionId": execution_id, "status": existing.status, "replayed": True},
                )
                return
            if self.active >= self.max_concurrent:
                self._problem(handler, 429, "CAPACITY_EXHAUSTED", "agent runtime capacity exhausted")
                return
            if len(self.executions) >= self.execution_limit:
                terminal = [
                    (key, value)
                    for key, value in self.executions.items()
                    if value.status not in {"ACCEPTED", "RUNNING"} and value.capacity_released
                ]
                if not terminal:
                    self._problem(handler, 429, "RETENTION_EXHAUSTED", "execution retention capacity exhausted")
                    return
                oldest, _ = min(terminal, key=lambda item: item[1].created_at)
                del self.executions[oldest]
            execution = _Execution(digest=digest)
            self.executions[execution_id] = execution
            self.active += 1
        self.executor.submit(self._run, body, execution)
        self._write(handler, 202, {"executionId": execution_id, "status": "ACCEPTED", "replayed": False})

    def _run(self, request: dict[str, Any], execution: _Execution) -> None:
        execution_id = request["executionId"]
        with self.lock:
            execution.status = "RUNNING"

        def emit(event_type: str, payload: Any) -> None:
            if not isinstance(event_type, str) or not event_type.strip() or len(event_type) > 128:
                raise ValueError("event type is required and bounded")
            encoded = json.dumps(payload, separators=(",", ":"))
            if len(encoded.encode()) > MAX_EVENT_PAYLOAD:
                raise ValueError("event payload exceeds 256 KiB")
            with self.lock:
                if execution.status != "RUNNING":
                    raise RuntimeError("events cannot be emitted after execution termination")
                if len(execution.events) >= self.event_limit:
                    raise RuntimeError("event history limit exceeded")
                execution.events.append(
                    {
                        "sequence": execution.next_sequence,
                        "type": event_type,
                        "payload": payload,
                        "occurredAt": _timestamp(),
                    }
                )
                execution.next_sequence += 1

        watchdog = threading.Timer(
            self.execution_timeout,
            self._execution_timed_out,
            args=(execution_id,),
        )
        watchdog.daemon = True
        watchdog.start()
        try:
            output = self.runtime.run(request, emit, execution.stop_event)
            if execution.stop_event.is_set():
                self._finish(execution_id, "CANCELLED", error_code="EXECUTION_CANCELLED", error="execution cancelled")
            else:
                encoded = json.dumps(output, separators=(",", ":")).encode()
                if len(encoded) > MAX_RESULT:
                    self._finish(execution_id, "FAILED", error_code="INVALID_RESULT", error="adapter result exceeds 2 MiB")
                else:
                    self._finish(execution_id, "SUCCEEDED", output=output)
        except Exception as exc:  # fail closed without disclosing adapter internals
            # Preserve an operational signal without logging the exception
            # message, request, result, credentials, or user-controlled data.
            print(f"agent runtime execution failed: {type(exc).__name__}", file=sys.stderr, flush=True)
            status = "CANCELLED" if execution.stop_event.is_set() else "FAILED"
            code = "EXECUTION_CANCELLED" if status == "CANCELLED" else "ADAPTER_FAILED"
            detail = "execution cancelled" if status == "CANCELLED" else "adapter execution failed"
            self._finish(execution_id, status, error_code=code, error=detail)
        finally:
            watchdog.cancel()
            self._release_capacity(execution_id)

    def _execution_timed_out(self, execution_id: str) -> None:
        with self.lock:
            execution = self.executions.get(execution_id)
            if execution is None or execution.finalized:
                return
            execution.stop_event.set()
        self._finish(
            execution_id,
            "FAILED",
            error_code="EXECUTION_TIMEOUT",
            error="adapter execution timed out",
            release_capacity=False,
        )
        self._schedule_force_termination(execution_id)

    def _schedule_force_termination(self, execution_id: str) -> None:
        def terminate_if_stuck() -> None:
            with self.lock:
                execution = self.executions.get(execution_id)
                stuck = execution is not None and not execution.capacity_released
            if not stuck:
                return
            if self.force_terminate is not None:
                try:
                    self.force_terminate(execution_id)
                except Exception:
                    # The hook is best-effort; the capacity release below is
                    # the hard guarantee.
                    pass
            # A stuck agent thread cannot be killed from inside this
            # process. Releasing the ledger capacity bounds the damage —
            # the host keeps accepting work instead of leaking every
            # concurrency slot to runaway agents. The thread itself stays
            # alive until the surrounding sandbox is killed; that is the
            # deployment's responsibility, not the protocol host's.
            self._release_capacity(execution_id)

        timer = threading.Timer(self.termination_grace, terminate_if_stuck)
        timer.daemon = True
        timer.start()

    def _finish(
        self,
        execution_id: str,
        status: str,
        *,
        output: Any | None = None,
        error_code: str = "",
        error: str = "",
        release_capacity: bool = True,
    ) -> None:
        result = self._terminal(execution_id, status, output=output, error_code=error_code, error=error)
        with self.lock:
            execution = self.executions.get(execution_id)
            if execution is None:
                return
            if not execution.finalized:
                execution.finalized = True
                execution.status = status
                execution.result = result
            if release_capacity and not execution.capacity_released:
                execution.capacity_released = True
                self.active -= 1

    def _release_capacity(self, execution_id: str) -> None:
        with self.lock:
            execution = self.executions.get(execution_id)
            if execution is not None and not execution.capacity_released:
                execution.capacity_released = True
                self.active -= 1

    def _stop(self, handler: BaseHTTPRequestHandler, execution_id: str, method: str, _query: str) -> None:
        if method != "POST":
            self._problem(handler, 405, "METHOD_NOT_ALLOWED", "POST is required")
            return
        with self.lock:
            execution = self.executions.get(execution_id)
            if execution is None:
                self._problem(handler, 404, "EXECUTION_NOT_FOUND", "agent execution not found")
                return
            execution.stop_event.set()
            status = execution.status
        self._schedule_force_termination(execution_id)
        self._write(handler, 202, {"executionId": execution_id, "status": status})

    def _checkpoint(self, handler: BaseHTTPRequestHandler, execution_id: str, method: str, _query: str) -> None:
        if method != "POST":
            self._problem(handler, 405, "METHOD_NOT_ALLOWED", "POST is required")
            return
        with self.lock:
            exists = execution_id in self.executions
        if not exists:
            self._problem(handler, 404, "EXECUTION_NOT_FOUND", "agent execution not found")
            return
        try:
            checkpoint = self._call_with_timeout(self.runtime.checkpoint, execution_id)
            self._validate_checkpoint(checkpoint)
        except TimeoutError:
            self._problem(handler, 504, "CHECKPOINT_TIMEOUT", "checkpoint operation timed out")
            return
        except Exception:
            self._problem(handler, 422, "CHECKPOINT_FAILED", "checkpoint operation failed")
            return
        self._write(handler, 200, {"executionId": execution_id, "checkpoint": checkpoint})

    def _restore(self, handler: BaseHTTPRequestHandler, execution_id: str, method: str, _query: str) -> None:
        if method != "POST":
            self._problem(handler, 405, "METHOD_NOT_ALLOWED", "POST is required")
            return
        body = self._read_json(handler)
        if body is None:
            return
        if set(body) != {"executionId", "checkpoint"} or body["executionId"] != execution_id:
            self._problem(handler, 422, "RESTORE_ID_MISMATCH", "path and body execution ids must match")
            return
        try:
            self._validate_checkpoint(body["checkpoint"])
            self._call_with_timeout(self.runtime.restore, execution_id, body["checkpoint"])
        except TimeoutError:
            self._problem(handler, 504, "RESTORE_TIMEOUT", "restore operation timed out")
            return
        except Exception:
            self._problem(handler, 422, "RESTORE_FAILED", "restore operation failed")
            return
        self._write(handler, 200, {"executionId": execution_id, "restored": True})

    def _events(self, handler: BaseHTTPRequestHandler, execution_id: str, method: str, query: str) -> None:
        if method != "GET":
            self._problem(handler, 405, "METHOD_NOT_ALLOWED", "GET is required")
            return
        try:
            after = int(parse_qs(query).get("after", ["0"])[0])
            if after < 0:
                raise ValueError
        except ValueError:
            self._problem(handler, 400, "INVALID_CURSOR", "after must be a non-negative integer")
            return
        with self.lock:
            execution = self.executions.get(execution_id)
            if execution is None:
                self._problem(handler, 404, "EXECUTION_NOT_FOUND", "agent execution not found")
                return
            events: list[dict[str, Any]] = []
            page_bytes = 0
            truncated = False
            for event in execution.events:
                if event["sequence"] <= after:
                    continue
                event_bytes = len(json.dumps(event, separators=(",", ":")).encode())
                if len(events) >= MAX_EVENTS_PER_PAGE or page_bytes + event_bytes > MAX_EVENT_PAGE:
                    truncated = True
                    break
                events.append(event)
                page_bytes += event_bytes
        next_after = events[-1]["sequence"] if events else after
        self._write(handler, 200, {"executionId": execution_id, "events": events, "nextAfter": next_after, "truncated": truncated})

    def _events_stream(self, handler: BaseHTTPRequestHandler, execution_id: str, method: str, query: str) -> None:
        """Server-sent-events extension of the Runtime Interface.

        One long-lived connection replaces the Events/Result poll cycle:
        events are pushed as SSE frames (``event: event``) and the terminal
        frame (``event: result``) carries the final result. The ``after``
        cursor makes reconnects lossless. A v1-only client keeps using the
        frozen polling endpoints; both coexist forever.
        """
        if method != "GET":
            self._problem(handler, 405, "METHOD_NOT_ALLOWED", "GET is required")
            return
        try:
            after = int(parse_qs(query).get("after", ["0"])[0])
            if after < 0:
                raise ValueError
        except ValueError:
            self._problem(handler, 400, "INVALID_CURSOR", "after must be a non-negative integer")
            return
        with self.lock:
            if execution_id not in self.executions:
                self._problem(handler, 404, "EXECUTION_NOT_FOUND", "agent execution not found")
                return
        handler.send_response(200)
        handler.send_header("Content-Type", "text/event-stream")
        handler.send_header("Cache-Control", "no-cache")
        handler.send_header("Fenced-Runtime-Interface", getattr(handler, "fenced_protocol", PROTOCOL_VERSION))
        handler.end_headers()
        NL = chr(10)
        cursor = after
        idle_since = time.monotonic()
        while True:
            with self.lock:
                execution = self.executions.get(execution_id)
                if execution is None:
                    return
                pending = [event for event in execution.events if event["sequence"] > cursor]
                terminal = execution.status not in {"ACCEPTED", "RUNNING"}
                result = execution.result
            try:
                for event in pending[:MAX_EVENTS_PER_PAGE]:
                    frame = json.dumps(event, separators=(",", ":"))
                    handler.wfile.write(("event: event" + NL + "data: " + frame + NL + NL).encode())
                    cursor = event["sequence"]
                    idle_since = time.monotonic()
                if terminal and not pending:
                    frame = json.dumps(result, separators=(",", ":"))
                    handler.wfile.write(("event: result" + NL + "data: " + frame + NL + NL).encode())
                    handler.wfile.flush()
                    return
                if time.monotonic() - idle_since >= 10.0:
                    handler.wfile.write((": keepalive" + NL + NL).encode())
                    idle_since = time.monotonic()
                handler.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                return
            time.sleep(0.05)

    def _result(self, handler: BaseHTTPRequestHandler, execution_id: str, method: str, _query: str) -> None:
        if method != "GET":
            self._problem(handler, 405, "METHOD_NOT_ALLOWED", "GET is required")
            return
        with self.lock:
            execution = self.executions.get(execution_id)
            if execution is None:
                self._problem(handler, 404, "EXECUTION_NOT_FOUND", "agent execution not found")
                return
            result = execution.result
            status = execution.status
        if result is None:
            self._write(handler, 202, {"executionId": execution_id, "status": status})
            return
        self._write(handler, 200, result)

    @staticmethod
    def _validate_start(body: dict[str, Any]) -> None:
        required = {"executionId", "agentVersionRef", "goal", "input", "capabilities"}
        if not required.issubset(body):
            raise ValueError("start request is missing required fields")
        for key in ("executionId", "agentVersionRef", "goal"):
            if not isinstance(body[key], str) or not body[key].strip():
                raise ValueError(f"{key} is required")
        capabilities = body["capabilities"]
        classes = {"tools", "models", "memory", "secrets"}
        if not isinstance(capabilities, dict) or not classes.issubset(capabilities):
            raise ValueError("all capability classes must be explicit")
        if any(not isinstance(capabilities[name], list) for name in classes):
            raise ValueError("capability classes must be arrays")
        for name in classes | {"childAgents", "peers", "memorySensitivities"}:
            values = capabilities.get(name, [])
            if not isinstance(values, list) or len(values) > 256:
                raise ValueError(f"capabilities.{name} must be a bounded array")
            if any(not isinstance(value, str) or not value.strip() or len(value) > 256 for value in values):
                raise ValueError(f"capabilities.{name} contains an invalid grant")
        sensitivities = capabilities.get("memorySensitivities", [])
        if any(value not in {"internal", "confidential", "restricted"} for value in sensitivities):
            raise ValueError("capabilities.memorySensitivities contains an invalid tier")
        if capabilities.get("spawnTasks") and not capabilities.get("childAgents"):
            raise ValueError("capabilities.childAgents is required when spawnTasks is enabled")

    @staticmethod
    def _validate_checkpoint(checkpoint: dict[str, Any]) -> None:
        if not isinstance(checkpoint, dict) or set(checkpoint) != {"schemaVersion", "state", "createdAt"}:
            raise ValueError("checkpoint shape is invalid")
        if not isinstance(checkpoint["schemaVersion"], str) or not checkpoint["schemaVersion"].strip():
            raise ValueError("checkpoint schemaVersion is required")
        if not isinstance(checkpoint["createdAt"], str) or not checkpoint["createdAt"].strip():
            raise ValueError("checkpoint createdAt is required")
        if len(json.dumps(checkpoint["state"]).encode()) > MAX_BODY:
            raise ValueError("checkpoint state exceeds 2 MiB")

    @staticmethod
    def _terminal(
        execution_id: str,
        status: str,
        *,
        output: Any | None = None,
        error_code: str = "",
        error: str = "",
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"executionId": execution_id, "status": status, "completedAt": _timestamp()}
        if output is not None:
            result["output"] = output
        if error_code:
            result["errorCode"] = error_code
        if error:
            result["error"] = error
        return result

    def _call_with_timeout(self, operation: Callable[..., Any], *args: Any) -> Any:
        if not self.control_slots.acquire(blocking=False):
            raise RuntimeError("runtime control operation capacity exhausted")
        completed: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                completed.put((True, operation(*args)))
            except BaseException as error:  # transported internally, never exposed
                completed.put((False, error))
            finally:
                self.control_slots.release()

        threading.Thread(target=invoke, daemon=True).start()
        try:
            succeeded, value = completed.get(timeout=self.control_timeout)
        except queue.Empty as error:
            raise TimeoutError from error
        if not succeeded:
            raise value
        return value

    @staticmethod
    def _read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any] | None:
        try:
            length = int(handler.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                raise ValueError("request body size is invalid")
            body = json.loads(handler.rfile.read(length), object_pairs_hook=_unique_object)
            if not isinstance(body, dict):
                raise ValueError("request body must be an object")
            return body
        except (ValueError, json.JSONDecodeError) as error:
            RuntimeHost._problem(handler, 400, "INVALID_JSON", str(error))
            return None

    @staticmethod
    def _write(handler: BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
        encoded = json.dumps(body, separators=(",", ":")).encode()
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(encoded)))
        handler.send_header("Fenced-Runtime-Interface", getattr(handler, "fenced_protocol", PROTOCOL_VERSION))
        handler.end_headers()
        handler.wfile.write(encoded)

    @staticmethod
    def _problem(handler: BaseHTTPRequestHandler, status: int, code: str, detail: str) -> None:
        RuntimeHost._write(handler, status, {"code": code, "detail": detail, "status": status})


def serve(
    runtime: AgentRuntime,
    adapter: str,
    host: str = "127.0.0.1",
    port: int = 8088,
) -> ThreadingHTTPServer:
    runtime_host = RuntimeHost(runtime, adapter)
    server = ThreadingHTTPServer((host, port), runtime_host.handler())
    server.daemon_threads = True
    return server


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field {key!r}")
        result[key] = value
    return result
