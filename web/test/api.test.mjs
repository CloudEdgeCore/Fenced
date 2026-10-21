import test from 'node:test';
import assert from 'node:assert/strict';

import { ControlClient, ApiError } from '../build/lib.js';

function response(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => (body === undefined ? '' : JSON.stringify(body)),
  };
}

test('every call carries the bearer token of its tenant', async () => {
  const seen = [];
  const client = new ControlClient({
    token: 'token-a',
    fetchImpl: async (url, init) => {
      seen.push({ url, init });
      return response(200, { tasks: [] });
    },
  });

  await client.tasks(10);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].url, '/v1/tasks?limit=10');
  assert.equal(seen[0].init.headers.Authorization, 'Bearer token-a');
});

test('a body is sent as JSON', async () => {
  const seen = [];
  const client = new ControlClient({
    token: 'token-a',
    fetchImpl: async (url, init) => {
      seen.push(init);
      return response(202, { id: 'svc-1' });
    },
  });

  await client.rollout('svc-1', 'summarizer@2.0.0', { autoRollback: true });
  assert.equal(seen[0].method, 'POST');
  assert.equal(seen[0].headers['Content-Type'], 'application/json');
  assert.deepEqual(JSON.parse(seen[0].body), {
    newVersion: 'summarizer@2.0.0',
    config: { autoRollback: true },
  });
});

test('a problem document becomes a typed error', async () => {
  const client = new ControlClient({
    token: 'token-a',
    fetchImpl: async () =>
      response(409, {
        status: 409,
        reasonCode: 'ROLLOUT_IN_PROGRESS',
        detail: 'a rollout to v2 is still in progress',
      }),
  });

  await assert.rejects(
    () => client.rollout('svc-1', 'v2'),
    (error) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, 409);
      assert.equal(error.reasonCode, 'ROLLOUT_IN_PROGRESS');
      assert.match(error.message, /still in progress/);
      return true;
    },
  );
});

test('a response without a body is still an error', async () => {
  const client = new ControlClient({
    token: 'token-a',
    fetchImpl: async () => response(500, undefined),
  });
  await assert.rejects(() => client.services(), (error) => {
    assert.equal(error.status, 500);
    assert.equal(error.reasonCode, 'HTTP_ERROR');
    return true;
  });
});

test('a service identifier is escaped', async () => {
  const seen = [];
  const client = new ControlClient({
    token: 'token-a',
    fetchImpl: async (url) => {
      seen.push(url);
      return response(200, {});
    },
  });
  await client.rollback('svc/../other');
  assert.equal(seen[0], '/v1/services/svc%2F..%2Fother/rollback');
});
