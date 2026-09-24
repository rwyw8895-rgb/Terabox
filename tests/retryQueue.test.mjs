import test from 'node:test';
import assert from 'node:assert/strict';
import { getRetryLabel, shouldRetryLink } from '../src/retryQueue.ts';
import { isDiskwalaUrl, extractDiskwalaId } from '../server/terabox.ts';

test('retry decisions disable retries and fail immediately', () => {
  assert.equal(shouldRetryLink(0, 1), false);
  assert.equal(shouldRetryLink(1, 1), false);
});

test('retry labels include the file name and retry number', () => {
  assert.equal(
    getRetryLabel({ fileNames: ['report.zip'], retryCount: 2, maxRetries: 5, url: 'https://example.com' }),
    'report.zip (retry 2/5)'
  );
});

test('Diskwala links are detected and normalized', () => {
  assert.equal(isDiskwalaUrl('https://www.diskwala.com/app/abc123xyz'), true);
  assert.equal(extractDiskwalaId('https://dw.link/abc123xyz'), 'abc123xyz');
  assert.equal(extractDiskwalaId('https://www.diskwala.com/file/demo-link'), 'demo-link');
});

