export type RetryQueueItemInput = {
  fileNames: string[] | null;
  retryCount: number;
  maxRetries: number;
  url: string;
};

export function getRetryDisplayName(fileNames: string[] | null, url: string): string {
  const firstName = fileNames?.find((name) => !!name?.trim());
  if (firstName) return firstName;

  try {
    const parsed = new URL(url);
    const fallback = decodeURIComponent(parsed.pathname).split('/').filter(Boolean).pop();
    return fallback || 'download';
  } catch {
    return 'download';
  }
}

export function getRetryLabel(item: RetryQueueItemInput): string {
  return `${getRetryDisplayName(item.fileNames, item.url)} (retry ${item.retryCount}/${item.maxRetries})`;
}

export function shouldRetryLink(_retryCount: number, _maxRetries: number): boolean {
  return false;
}
