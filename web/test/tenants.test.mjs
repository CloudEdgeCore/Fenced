import test from 'node:test';
import assert from 'node:assert/strict';

import { TenantRegistry, MemoryStorage } from '../build/lib.js';

test('the registry keeps the tenants apart and hands out their own token', () => {
  const registry = new TenantRegistry(new MemoryStorage());
  registry.add({ id: 'tenant-a', token: 'token-a' });
  registry.add({ id: 'tenant-b', token: 'token-b' });

  assert.deepEqual(
    registry.list().map((tenant) => tenant.id),
    ['tenant-a', 'tenant-b'],
  );
  assert.equal(registry.tokenFor('tenant-a'), 'token-a');
  assert.equal(registry.tokenFor('tenant-b'), 'token-b');
  assert.equal(registry.active().id, 'tenant-a');

  registry.setActive('tenant-b');
  assert.equal(registry.active().id, 'tenant-b');
  assert.equal(registry.tokenFor('tenant-a'), 'token-a');
});

test('a tenant without a token cannot be read', () => {
  const registry = new TenantRegistry(new MemoryStorage());
  registry.add({ id: 'tenant-a', token: 'token-a' });
  registry.add({ id: 'tenant-blank', token: '   ' });

  assert.deepEqual(
    registry.readable().map((tenant) => tenant.id),
    ['tenant-a'],
  );
});

test('the registry survives a reload of the session', () => {
  const storage = new MemoryStorage();
  const first = new TenantRegistry(storage);
  first.add({ id: 'tenant-a', token: 'token-a' });
  first.add({ id: 'tenant-b', token: 'token-b' });
  first.setActive('tenant-b');

  const second = new TenantRegistry(storage);
  assert.equal(second.active().id, 'tenant-b');
  assert.equal(second.tokenFor('tenant-a'), 'token-a');
});

test('a broken session does not take the console down', () => {
  const storage = new MemoryStorage();
  storage.setItem('fenced.operator.tenants', '{not json');
  const registry = new TenantRegistry(storage);
  assert.deepEqual(registry.list(), []);
  assert.equal(registry.active(), undefined);
});

test('removing the tenant in focus moves the focus', () => {
  const registry = new TenantRegistry(new MemoryStorage());
  registry.add({ id: 'tenant-a', token: 'token-a' });
  registry.add({ id: 'tenant-b', token: 'token-b' });
  registry.setActive('tenant-b');

  registry.remove('tenant-b');
  assert.equal(registry.active().id, 'tenant-a');
  assert.equal(registry.tokenFor('tenant-b'), undefined);
});

test('a tenant that is not known cannot be focused', () => {
  const registry = new TenantRegistry(new MemoryStorage());
  assert.throws(() => registry.setActive('tenant-x'), /unknown tenant/);
});
