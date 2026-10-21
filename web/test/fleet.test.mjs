import test from 'node:test';
import assert from 'node:assert/strict';

import { collectFleet, summarizeTenant, runningRollouts } from '../build/lib.js';

const payload = {
  tasks: [{ id: 'task-1' }, { id: 'task-2' }],
  services: [
    {
      id: 'svc-1',
      name: 'summarizer',
      status: { phase: 'Active', readyReplicas: 2, desiredReplicas: 2, rolloutPhase: 'PROGRESSING' },
    },
    {
      id: 'svc-2',
      name: 'planner',
      status: { phase: 'Active', readyReplicas: 1, desiredReplicas: 1 },
    },
  ],
  approvals: [
    { id: 'a-1', status: 'PENDING' },
    { id: 'a-2', status: 'DECIDED' },
  ],
  auditEvents: [{ seq: 1 }, { seq: 2 }, { seq: 3 }],
};

test('a tenant is summarized into the numbers the fleet view shows', () => {
  const summary = summarizeTenant('tenant-a', payload);
  assert.equal(summary.tenantId, 'tenant-a');
  assert.equal(summary.tasks, 2);
  assert.equal(summary.services, 2);
  assert.equal(summary.approvals, 1);
  assert.equal(summary.auditEvents, 3);
  assert.equal(summary.runningRollouts, 1);
});

test('only the rollouts that are moving are counted', () => {
  const rollouts = runningRollouts(payload.services);
  assert.equal(rollouts.length, 1);
  assert.equal(rollouts[0].serviceName, 'summarizer');
  assert.equal(rollouts[0].phase, 'PROGRESSING');
});

test('a tenant that fails does not take the fleet view down', async () => {
  const summaries = await collectFleet(
    ['tenant-a', 'tenant-b'],
    async (tenantId) => {
      if (tenantId === 'tenant-b') {
        throw new Error('token expired');
      }
      return payload;
    },
  );

  assert.equal(summaries.length, 2);
  assert.equal(summaries[0].error, undefined);
  assert.equal(summaries[0].tasks, 2);
  assert.equal(summaries[1].error, 'token expired');
  assert.equal(summaries[1].tasks, 0);
});

test('the fleet view asks every tenant it holds', async () => {
  const asked = [];
  await collectFleet(['tenant-a', 'tenant-b'], async (tenantId) => {
    asked.push(tenantId);
    return payload;
  });
  assert.deepEqual(asked.sort(), ['tenant-a', 'tenant-b']);
});
