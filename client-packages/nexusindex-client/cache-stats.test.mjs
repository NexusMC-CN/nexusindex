import assert from 'node:assert/strict';
import test from 'node:test';
import { NexusIndexClient } from './dist/index.js';

test('getCacheStats unwraps the server response', async () => {
  const client = new NexusIndexClient({
    fetchImpl: async () => Response.json({cacheVersion: 42, stats: {l1Hits: 9, l1Misses: 2, l2Hits: 1, l2Misses: 1, sets: 3, entries: 2}}),
  });
  assert.deepEqual(await client.getCacheStats(), {l1Hits: 9, l1Misses: 2, l2Hits: 1, l2Misses: 1, sets: 3, entries: 2});
});
