import test from 'node:test';
import assert from 'node:assert/strict';

import { getLinkCounterForUrl, resetLinkCounterStateForTests } from '../src/linkCounter.ts';

test('same link keeps the same counter within a day', () => {
  resetLinkCounterStateForTests();

  const url = 'https://terabox.com/s/abc123';
  const first = new Date('2026-09-24T10:00:00');
  const second = new Date('2026-09-24T18:30:00');

  assert.equal(getLinkCounterForUrl(url, first), 1);
  assert.equal(getLinkCounterForUrl(url, second), 1);
});

test('new unique links increase the counter within the same day', () => {
  resetLinkCounterStateForTests();

  const firstUrl = 'https://terabox.com/s/abc123';
  const secondUrl = 'https://terabox.com/s/def456';
  const now = new Date('2026-09-24T09:15:00');

  assert.equal(getLinkCounterForUrl(firstUrl, now), 1);
  assert.equal(getLinkCounterForUrl(secondUrl, now), 2);
});

test('counter resets at midnight for the next day', () => {
  resetLinkCounterStateForTests();

  const url = 'https://terabox.com/s/abc123';
  assert.equal(getLinkCounterForUrl(url, new Date('2026-09-24T23:59:00')), 1);
  assert.equal(getLinkCounterForUrl(url, new Date('2026-09-25T00:01:00')), 1);
  assert.equal(getLinkCounterForUrl('https://terabox.com/s/def456', new Date('2026-09-25T00:02:00')), 2);
});
