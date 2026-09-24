export type QueueTier = 'q1' | 'q2' | 'q3' | 'q4';

export type QueueCounts = {
  q1: number;
  q2: number;
  q3: number;
  q4: number;
};

const MB = 1024 * 1024;

export function classifyQueueTier(sizeBytes: number): QueueTier {
  if (sizeBytes < 2.5 * MB) return 'q1';
  if (sizeBytes < 5 * MB) return 'q2';
  if (sizeBytes < 10 * MB) return 'q3';
  return 'q4';
}

export function shouldPauseLargeJobForSmallerQueue(
  currentTier: QueueTier,
  queueCounts: QueueCounts
): boolean {
  if (currentTier === 'q4') {
    return queueCounts.q1 > 0 || queueCounts.q2 > 0 || queueCounts.q3 > 0;
  }
  if (currentTier === 'q3') {
    return queueCounts.q1 > 0 || queueCounts.q2 > 0;
  }
  if (currentTier === 'q2') {
    return queueCounts.q1 > 0;
  }
  return false;
}
