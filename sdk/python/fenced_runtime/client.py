"""High-level idiomatic Fenced Client SDK for Python agents.

Provides first-class, typed abstractions for:
- IPC: Durable Mailbox cross-agent messaging
- Effect: Guarded external mutations with monotonic lease fencing and idempotency
- Service: Heartbeat, topology discovery, and supervisor integration
- Checkpoint: State persistence and crash recovery
- Gateways: Tool invocation and model inference with quota and audit receipts
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


class FencedError(Exception):
    """Base exception for all Fenced Client operations."""
    def __init__(self, message: str, status_code: int = 0, error_code: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


class FencingViolationError(FencedError):
    """Raised when an attempt's fencing token is superseded (lease evicted)."""
    pass


class IdempotencyConflictError(FencedError):
    """Raised when an idempotency key is reused with mismatched payload."""
    pass


class AmbiguousEffectError(FencedError):
    """Raised when an external effect outcome is UNKNOWN. Auto-replay forbidden."""
    pass


class IPCSubsystem:
    """Client for the Durable IPC Mailbox subsystem."""

    def __init__(self, client: "FencedClient") -> None:
        self._client = client

    def send(
        self,
        target_agent_ref: str,
        payload: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Send a durable message to a target agent's mailbox."""
        data = {
            "tenantId": self._client.tenant_id,
            "sender": {
                "agentVersionRef": self._client.agent_version_ref,
                "instance": self._client.execution_id,
                "fencingToken": self._client.fencing_token,
            },
            "target": {
                "agentVersionRef": target_agent_ref,
            },
            "payload": payload,
            "idempotencyKey": idempotency_key or f"msg-{int(time.time() * 1000)}-{os.urandom(4).hex()}",
            "correlationId": correlation_id or "",
            "replyTo": reply_to or "",
            "headers": headers or {},
        }
        return self._client._post("/v1/ipc/messages", data)

    def receive(self, max_messages: int = 10, timeout_ms: int = 5000) -> List[Dict[str, Any]]:
        """Poll and receive pending messages from the local agent mailbox."""
        params = urllib.parse.urlencode({
            "tenantId": self._client.tenant_id,
            "agentVersionRef": self._client.agent_version_ref,
            "instance": self._client.execution_id,
            "maxMessages": str(max_messages),
            "timeoutMs": str(timeout_ms),
        })
        resp = self._client._get(f"/v1/ipc/messages?{params}")
        return resp.get("messages", [])

    def ack(self, message_ids: List[str]) -> bool:
        """Acknowledge processed messages for exactly-once application."""
        if not message_ids:
            return True
        data = {
            "tenantId": self._client.tenant_id,
            "agentVersionRef": self._client.agent_version_ref,
            "instance": self._client.execution_id,
            "messageIds": message_ids,
        }
        resp = self._client._post("/v1/ipc/messages/ack", data)
        return resp.get("acknowledged", True)


class EffectSubsystem:
    """Client for the Kernel Effect API (guarded external mutations)."""

    def __init__(self, client: "FencedClient") -> None:
        self._client = client

    def execute(
        self,
        effect_type: str,
        idempotency_key: str,
        payload: Dict[str, Any],
        timeout_ms: int = 30000,
    ) -> Dict[str, Any]:
        """Dispatch an external mutation under monotonic fencing and idempotency."""
        if not idempotency_key:
            raise ValueError("idempotency_key is required for all external effects")

        data = {
            "tenantId": self._client.tenant_id,
            "executionId": self._client.execution_id,
            "fencingToken": self._client.fencing_token,
            "effectType": effect_type,
            "idempotencyKey": idempotency_key,
            "payload": payload,
            "timeoutMs": timeout_ms,
        }
        try:
            return self._client._post("/v1/effects", data)
        except FencedError as err:
            if err.error_code == "EFENCE" or err.status_code == 409:
                raise FencingViolationError(f"Effect rejected: stale fencing token ({err})") from err
            if err.error_code == "EINVAL" and "idempotency" in str(err).lower():
                raise IdempotencyConflictError(f"Effect payload conflict on idempotency key: {err}") from err
            if err.error_code == "EUNKNOWN" or "unknown" in str(err).lower():
                raise AmbiguousEffectError(f"Effect reached UNKNOWN state; auto-replay forbidden: {err}") from err
            raise

    def get(self, effect_id: str) -> Dict[str, Any]:
        """Retrieve existing effect receipt by effect ID."""
        return self._client._get(f"/v1/effects/{urllib.parse.quote(effect_id)}")

    def get_by_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """Look up existing effect receipt by idempotency key."""
        params = urllib.parse.urlencode({"idempotencyKey": idempotency_key})
        try:
            return self._client._get(f"/v1/effects/lookup?{params}")
        except FencedError as err:
            if err.status_code == 404:
                return None
            raise


class ServiceSubsystem:
    """Client for AgentService and Supervisor lifecycle."""

    def __init__(self, client: "FencedClient") -> None:
        self._client = client

    def heartbeat(
        self,
        phase: str = "RUNNING",
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Send instance heartbeat to Supervisor to prevent eviction."""
        data = {
            "serviceName": self._client.service_name,
            "instanceId": self._client.instance_id,
            "phase": phase,
            "fencingToken": self._client.fencing_token,
            "metrics": metrics or {},
            "timestamp": time.time(),
        }
        return self._client._post("/v1/services/heartbeat", data)

    def query(self, service_name: Optional[str] = None) -> Dict[str, Any]:
        """Inspect service instance topology and active replicas."""
        target = service_name or self._client.service_name
        return self._client._get(f"/v1/services/{urllib.parse.quote(target)}")


class CheckpointSubsystem:
    """Client for durable state checkpointing and recovery."""

    def __init__(self, client: "FencedClient") -> None:
        self._client = client

    def save(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Persist state snapshot tied to current attempt."""
        data = {
            "executionId": self._client.execution_id,
            "fencingToken": self._client.fencing_token,
            "state": state,
            "timestamp": time.time(),
        }
        return self._client._post("/v1/checkpoints", data)

    def restore(self) -> Optional[Dict[str, Any]]:
        """Fetch latest valid checkpoint for resumption."""
        params = urllib.parse.urlencode({"executionId": self._client.execution_id})
        try:
            resp = self._client._get(f"/v1/checkpoints/latest?{params}")
            return resp.get("state")
        except FencedError as err:
            if err.status_code == 404:
                return None
            raise


class FencedClient:
    """Unified, developer-facing Fenced Client."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        tenant_id: Optional[str] = None,
        token: Optional[str] = None,
        agent_version_ref: Optional[str] = None,
        execution_id: Optional[str] = None,
        service_name: Optional[str] = None,
        instance_id: Optional[str] = None,
        fencing_token: Optional[int] = None,
    ) -> None:
        # In-container or remote environment detection:
        self.base_url = (base_url or os.environ.get("FENCED_CONTROL_URL") or os.environ.get("FENCED_BASE_URL") or "http://127.0.0.1:8080").rstrip("/")
        self.tenant_id = tenant_id or os.environ.get("FENCED_TENANT_ID") or "default"
        self.token = token or os.environ.get("FENCED_TOKEN") or ""
        self.agent_version_ref = agent_version_ref or os.environ.get("FENCED_AGENT_REF") or "agent@1.2.0"
        self.execution_id = execution_id or os.environ.get("FENCED_EXECUTION_ID") or "exec-dev"
        self.service_name = service_name or os.environ.get("FENCED_SERVICE_NAME") or "agent-service"
        self.instance_id = instance_id or os.environ.get("FENCED_INSTANCE_ID") or self.execution_id
        self.fencing_token = fencing_token if fencing_token is not None else int(os.environ.get("FENCED_FENCING_TOKEN", "1"))

        # Subsystems:
        self.ipc = IPCSubsystem(self)
        self.effect = EffectSubsystem(self)
        self.service = ServiceSubsystem(self)
        self.checkpoint = CheckpointSubsystem(self)

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Fenced-Tenant": self.tenant_id,
            "X-Fenced-Execution-ID": self.execution_id,
            "X-Fenced-Fencing-Token": str(self.fencing_token),
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        req_data = json.dumps(data).encode("utf-8") if data is not None else None
        req = urllib.request.Request(url, data=req_data, headers=self._headers(), method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as err:
            raw_err = err.read().decode("utf-8")
            err_code = ""
            msg = f"HTTP {err.code}: {err.reason}"
            try:
                parsed = json.loads(raw_err)
                msg = parsed.get("message", msg)
                err_code = parsed.get("code", "")
            except Exception:
                if raw_err:
                    msg = f"{msg} - {raw_err}"
            raise FencedError(msg, status_code=err.code, error_code=err_code) from err
        except urllib.error.URLError as err:
            raise FencedError(f"Network error communicating with Fenced: {err.reason}") from err

    def _get(self, path: str) -> Dict[str, Any]:
        return self._request("GET", path)

    def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", path, data)
