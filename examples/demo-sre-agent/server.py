"""Flagship Showcase: Fenced Autonomous SRE Self-Healing Agent.

Demonstrates production capabilities:
1. Long-running supervised daemon service with auto-heartbeat
2. Durable IPC Mailbox alert ingestion
3. Resilient multi-step diagnostic loop with checkpoints
4. Fenced external side-effect execution (Kubernetes remediation) with monotonic fencing
5. SHA-256 idempotency protection against duplicate cluster mutations
"""

import logging
import os
import sys
import time

from fenced_runtime import apply_legacy_compat
from fenced_runtime.client import (
    FencedClient,
    AmbiguousEffectError,
    FencingViolationError,
    IdempotencyConflictError,
)
from fenced_runtime.ecosystem import wrap_custom_agent

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("sre-agent")


def handle_sre_incident(request: dict, client: FencedClient) -> dict:
    """Core SRE diagnosis and remediation handler running under Fenced."""
    incident = request.get("payload") or {"service": "checkout-api", "alert": "HighLatencyP99", "severity": "P1"}
    service_name = incident.get("service", "unknown-service")
    logger.info("Received SRE Incident: %s (%s)", incident.get("alert"), service_name)

    # Step 1: Checkpoint initial diagnostic state
    client.checkpoint.save({"step": "DIAGNOSING", "incident": incident, "attempts": 1})
    logger.info("Saved diagnostic checkpoint for attempt")

    # Step 2: Inquire cluster state via tool / gateway
    logger.info("Analyzing telemetry metrics for %s...", service_name)
    time.sleep(0.1)  # Simulate metric evaluation

    # Step 3: Send IPC status update to incident commander agent
    client.ipc.send(
        target_agent_ref="incident-commander@1.2.0",
        payload={"status": "INVESTIGATING", "remediationPlan": f"Rolling restart for {service_name}"},
        correlation_id=request.get("executionId", "exec-001"),
    )
    logger.info("Dispatched IPC status message to incident-commander")

    # Step 4: Execute guarded external remediation (Kernel Effect API)
    # Guaranteed monotonic lease fencing: if this agent was killed or partitioned,
    # the stale attempt is rejected with FencingViolationError and CANNOT mutate cluster!
    idempotency_key = f"sre-remediation-{service_name}-{int(time.time() // 60)}"
    remediation_payload = {
        "action": "rollout_restart",
        "targetDeployment": service_name,
        "namespace": "production",
        "initiatedBy": "Fenced SRE Agent v1.2",
    }

    try:
        logger.info("Dispatching fenced remediation effect with key: %s", idempotency_key)
        receipt = client.effect.execute(
            effect_type="k8s.deployment.restart",
            idempotency_key=idempotency_key,
            payload=remediation_payload,
            timeout_ms=10000,
        )
        logger.info("Remediation committed successfully! Receipt: %s (replayed=%s)", receipt.get("effectId"), receipt.get("replayed"))
    except FencingViolationError as err:
        logger.error("CRITICAL: Stale attempt detected! Fencing token rejected: %s", err)
        raise
    except IdempotencyConflictError as err:
        logger.warning("Idempotency conflict: %s", err)
        raise
    except AmbiguousEffectError as err:
        logger.error("Ambiguous effect status: %s. Auto-replay forbidden by Fenced!", err)
        raise

    # Step 5: Save terminal state
    client.checkpoint.save({"step": "REMEDIATED", "receipt": receipt})

    return {
        "status": "INCIDENT_RESOLVED",
        "service": service_name,
        "remediationReceipt": receipt,
        "governance": {
            "fencingToken": client.fencing_token,
            "tenant": client.tenant_id,
        },
    }


if __name__ == "__main__":
    apply_legacy_compat()
    port = int(os.environ.get("PORT", "8095"))
    logger.info("Starting Autonomous SRE Agent on port %d...", port)
    wrap_custom_agent(handle_sre_incident, server_port=port, name="sre-agent")
