import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import test from 'node:test';

import {
  credentials,
  loadAnvilPackage,
  metadataWithBearer,
  protoPath,
} from '../dist/index.js';

test('bundled proto path resolves to a packaged file', () => {
  assert.equal(protoPath().endsWith('/proto/anvil.proto'), true);
  assert.equal(existsSync(protoPath()), true);
});

test('dynamic gRPC package loader exposes Anvil services', () => {
  const anvil = loadAnvilPackage();
  assert.equal(typeof anvil.ObjectService, 'function');
  assert.equal(typeof anvil.BucketService, 'function');
  assert.equal(typeof anvil.AuthService, 'function');
});

test('auth metadata and channel credentials helpers are usable', () => {
  const metadata = metadataWithBearer('token-123');
  assert.deepEqual(metadata.get('authorization'), ['Bearer token-123']);

  const channelCredentials = credentials({ endpoint: 'localhost:50051' });
  assert.equal(typeof channelCredentials.compose, 'function');
});
