import test from 'node:test';
import assert from 'node:assert/strict';

import {
  classifyQueueTier,
  shouldPauseLargeJobForSmallerQueue,
} from '../src/queuePriority.ts';

test('queue tiers are assigned by file size thresholds', () => {
  assert.equal(classifyQueueTier(2 * 1024 * 1024), 'q1');
  assert.equal(classifyQueueTier(4 * 1024 * 1024), 'q2');
  assert.equal(classifyQueueTier(8 * 1024 * 1024), 'q3');
  assert.equal(classifyQueueTier(12 * 1024 * 1024), 'q4');
});

test('large jobs are paused when a smaller queue entry appears', () => {
  const currentTier = 'q4';
  const queueCounts = { q1: 1, q2: 0, q3: 0, q4: 0 };

  assert.equal(shouldPauseLargeJobForSmallerQueue(currentTier, queueCounts), true);
  assert.equal(shouldPauseLargeJobForSmallerQueue('q3', { q1: 0, q2: 0, q3: 1, q4: 0 }), false);
});
