import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";

import { FencedError, ControlClient, RuntimeClient } from "../dist/index.js";

test("control client preserves idempotency, authorization, and CAS", async (t) => {
  const requests = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    requests.push({ path: request.url, headers: request.headers, body });
    response.setHeader("Content-Type", "application/json");
    response.setHeader("ETag", 'W/"4"');
    if (request.url.endsWith("/cancel")) response.statusCode = 202;
    else response.statusCode = 200;
    response.end('{"id":"00000000-0000-0000-0000-000000000001","namespace":"default","goal":"ship","status":"RUNNING","resourceVersion":4,"steps":[]}');
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => server.close());
  const address = server.address();
  const client = new ControlClient(`http://127.0.0.1:${address.port}`, { token: "secret" });
  const created = await client.createWorkflow({ goal: "ship", workflow: { apiVersion: "fenced.dev/workflow/v1", kind: "Workflow", steps: [] } }, "workflow-fixed");
  await client.cancelWorkflow(created.value.id, created.value.resourceVersion);
  assert.equal(created.etag, 'W/"4"');
  assert.equal(requests[0].headers.authorization, "Bearer secret");
  assert.equal(requests[0].headers["idempotency-key"], "workflow-fixed");
  assert.equal(requests[1].headers["if-match"], 'W/"4"');
});

test("client returns structured API errors", async (t) => {
  const server = createServer((_request, response) => {
    response.statusCode = 409;
    response.setHeader("Content-Type", "application/problem+json");
    response.end('{"code":"CONFLICT","detail":"version conflict","traceId":"trace-1"}');
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => server.close());
  const address = server.address();
  const client = new ControlClient(`http://127.0.0.1:${address.port}`);
  await assert.rejects(client.getTask("task-1"), (error) => error instanceof FencedError && error.status === 409 && error.code === "CONFLICT");
});

test("runtime client uses the stable interface route", async (t) => {
  const server = createServer((request, response) => {
    assert.equal(request.url, "/v1/health");
    assert.equal(request.headers["fenced-runtime-interface"], "fenced.runtime.interface/v1");
    response.setHeader("Content-Type", "application/json");
    response.end('{"status":"SERVING","adapter":"test","protocolVersions":["fenced.runtime.interface/v1"]}');
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => server.close());
  const address = server.address();
  const client = new RuntimeClient(`http://127.0.0.1:${address.port}`);
  assert.equal((await client.health()).value.status, "SERVING");
});
