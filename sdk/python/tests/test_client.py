"""Tests for Fenced Python Client SDK."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fenced_runtime.client import (
    FencedClient,
    AmbiguousEffectError,
    FencingViolationError,
    IdempotencyConflictError,
)


class MockControlServerHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress server logs during test

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

        if self.path == "/v1/ipc/messages":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"messageId": "msg-123", "status": "QUEUED"}).encode("utf-8"))

        elif self.path == "/v1/ipc/messages/ack":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"acknowledged": True}).encode("utf-8"))

        elif self.path == "/v1/effects":
            # Test fencing error
            if body.get("fencingToken") == 999:
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"code": "EFENCE", "message": "stale fencing token"}).encode("utf-8"))
                return

            if body.get("idempotencyKey") == "conflict-key":
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"code": "EINVAL", "message": "idempotency payload conflict"}).encode("utf-8"))
                return

            if body.get("idempotencyKey") == "unknown-key":
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"code": "EUNKNOWN", "message": "external effect ambiguous"}).encode("utf-8"))
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "effectId": "eff-456",
                "status": "COMMITTED",
                "replayed": False,
            }).encode("utf-8"))

        elif self.path == "/v1/services/heartbeat":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "OK", "nextHeartbeatSeconds": 5}).encode("utf-8"))

        elif self.path == "/v1/checkpoints":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"checkpointId": "chk-789", "saved": True}).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path.startswith("/v1/ipc/messages"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "messages": [{"messageId": "msg-1", "payload": {"task": "analyze"}}],
            }).encode("utf-8"))

        elif self.path.startswith("/v1/checkpoints/latest"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"state": {"turn": 3, "step": "review"}}).encode("utf-8"))

        elif self.path.startswith("/v1/services/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "serviceName": "test-service",
                "desiredReplicas": 2,
                "readyReplicas": 2,
            }).encode("utf-8"))

        elif self.path.startswith("/v1/effects/eff-1"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"effectId": "eff-1", "status": "COMMITTED"}).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()


class FencedClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), MockControlServerHandler)
        cls.port = cls.server.server_port
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.client = FencedClient(
            base_url=f"http://127.0.0.1:{self.port}",
            tenant_id="test-tenant",
            execution_id="exec-001",
            fencing_token=42,
        )

    def test_ipc_workflow(self):
        # 1. Send
        resp = self.client.ipc.send("recipient@1.2.0", {"msg": "hello"}, correlation_id="corr-1")
        self.assertEqual(resp.get("status"), "QUEUED")

        # 2. Receive
        msgs = self.client.ipc.receive()
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["messageId"], "msg-1")

        # 3. Ack
        acked = self.client.ipc.ack(["msg-1"])
        self.assertTrue(acked)

    def test_effect_workflow_and_errors(self):
        # Successful effect
        res = self.client.effect.execute("payment.charge", "key-valid", {"amount": 100})
        self.assertEqual(res["status"], "COMMITTED")

        # Fencing violation
        stale_client = FencedClient(
            base_url=f"http://127.0.0.1:{self.port}",
            fencing_token=999,
        )
        with self.assertRaises(FencingViolationError):
            stale_client.effect.execute("payment.charge", "key-valid", {"amount": 100})

        # Idempotency conflict
        with self.assertRaises(IdempotencyConflictError):
            self.client.effect.execute("payment.charge", "conflict-key", {"amount": 200})

        # UNKNOWN non-replayable state
        with self.assertRaises(AmbiguousEffectError):
            self.client.effect.execute("payment.charge", "unknown-key", {"amount": 300})

    def test_service_and_supervisor_heartbeat(self):
        resp = self.client.service.heartbeat(phase="RUNNING")
        self.assertEqual(resp.get("status"), "OK")

        info = self.client.service.query("test-service")
        self.assertEqual(info.get("readyReplicas"), 2)

    def test_checkpoint_workflow(self):
        save_resp = self.client.checkpoint.save({"turn": 3, "step": "review"})
        self.assertTrue(save_resp.get("saved"))

        restored = self.client.checkpoint.restore()
        self.assertEqual(restored.get("turn"), 3)


if __name__ == "__main__":
    unittest.main()
