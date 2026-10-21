import test from 'node:test';
import assert from 'node:assert/strict';
import { renderToString } from 'react-dom/server';
import { createElement } from 'react';

import { App } from '../build/app.mjs';
import { MemoryStorage } from '../build/lib.js';

// The console has to render. A crash in a view is a crash in the product,
// and the browser is not the place to find it out.
test('the console renders its shell', () => {
  const html = renderToString(createElement(App, { storage: new MemoryStorage() }));
  assert.match(html, /Fenced/);
  assert.match(html, /Management Console/);
  assert.match(html, /Fleet/);
  assert.match(html, /Tasks/);
  assert.match(html, /Governance/);
  assert.match(html, /Audit/);
});

test('the console shows the tenants it holds', () => {
  const storage = new MemoryStorage();
  storage.setItem(
    'fenced.operator.tenants',
    JSON.stringify({ activeId: 'tenant-a', tenants: [{ id: 'tenant-a', token: 'token-a' }] }),
  );
  const html = renderToString(createElement(App, { storage }));
  assert.match(html, /tenant-a/);
  assert.match(html, /No tenant connected/);
});
