import { useEffect, useState } from 'react';

import { formatDuration } from './api/format';
import { JobSummary } from './api/types';

/**
 * What a job in "Fetching NZB" is actually doing.
 *
 * A URL job has no bytes, no rate and no ETA until its NZB arrives, so the
 * generic "0 B / 0 B · 0 B/s · ETA —" line said nothing at all — including
 * for two jobs that sat for most of a day behind an indexer's 429 with a
 * 22-hour Retry-After (field report 2026-11-01). The server already reports
 * the retry state on `url_fetch`; the web UI showed it and the app did not.
 *
 * The daemon retries on HTTP 429/503 and on timeouts and network errors, so
 * only an HTTP status is called a refusal. While a retry is in flight the
 * daemon clears `nextRetryAtUnix`, which reads as "retrying now".
 *
 * Pure so the jest suite can pin it without rendering.
 */
export function urlFetchDetail(job: Pick<JobSummary, 'url_fetch'>, nowMs: number): string {
  const retry = job.url_fetch;
  if (!retry || !retry.attempts) return 'Requesting NZB from the indexer';
  const error = retry.lastError?.trim();
  const refused = !!error && error.startsWith('http: status');
  const parts = [`${refused ? 'Indexer refused' : 'Fetch failed'}${error ? `: ${error}` : ''}`];
  const wait = retry.nextRetryAtUnix != null ? retry.nextRetryAtUnix - nowMs / 1000 : 0;
  parts.push(
    wait > 0
      ? `retrying in ${formatDuration(wait)} (${new Date((retry.nextRetryAtUnix as number) * 1000).toLocaleString()})`
      : 'retrying now',
  );
  if (retry.blockedSinceUnix != null) {
    parts.push(`waiting ${formatDuration(Math.max(0, nowMs / 1000 - retry.blockedSinceUnix))}`);
  }
  parts.push(`attempt ${retry.attempts}`);
  return parts.join(' · ');
}

/**
 * Wall-clock time that advances on its own while `active`.
 *
 * A parked fetch does not change, and the event stream sends nothing for an
 * unchanged queue, so a countdown read from Date.now() at render time froze
 * on whatever it said first. The ticker re-renders only the cards that need it.
 */
export function useNow(intervalMs: number, active: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return undefined;
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs, active]);
  return now;
}
