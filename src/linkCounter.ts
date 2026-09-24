import { extractUrlFromText } from '../server/terabox.ts';

const dailyCounters = new Map<string, { dayKey: string; counter: number }>();
let activeDayKey = '';

export function resetLinkCounterStateForTests(): void {
  dailyCounters.clear();
  activeDayKey = '';
}

export function normalizeLink(rawLink: string): string {
  const trimmed = (rawLink || '').trim();
  if (!trimmed) return '';

  const extracted = extractUrlFromText(trimmed) || trimmed;

  try {
    const parsed = new URL(extracted);
    parsed.hash = '';
    return parsed.href.replace(/\/+$/, '');
  } catch {
    return extracted.replace(/\s+/g, '').replace(/\/+$/, '');
  }
}

export function getDayKey(date: Date = new Date()): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

export function getLinkCounterForUrl(rawLink: string, date: Date = new Date()): number {
  const normalized = normalizeLink(rawLink);
  if (!normalized) return 0;

  const dayKey = getDayKey(date);
  if (activeDayKey !== dayKey) {
    activeDayKey = dayKey;
    for (const [link, entry] of dailyCounters) {
      if (entry.dayKey !== dayKey) {
        dailyCounters.delete(link);
      }
    }
  }

  const existing = dailyCounters.get(normalized);
  if (existing && existing.dayKey === dayKey) {
    return existing.counter;
  }

  const nextCounter = dailyCounters.size + 1;
  dailyCounters.set(normalized, { dayKey, counter: nextCounter });
  return nextCounter;
}

export function formatLinkCounter(rawLink: string, date: Date = new Date()): string {
  const counter = getLinkCounterForUrl(rawLink, date);
  return `#${counter}`;
}
