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
 * Pure so the jest suite can pin it without rendering.
 */
export function urlFetchDetail(job: Pick<JobSummary, 'url_fetch'>, nowMs: number): string {
  const retry = job.url_fetch;
  if (!retry || !retry.attempts) return 'Requesting NZB from the indexer';
  const parts = [`Indexer refused${retry.lastError ? `: ${retry.lastError}` : ''}`];
  if (retry.nextRetryAtUnix != null) {
    const wait = retry.nextRetryAtUnix - nowMs / 1000;
    parts.push(
      wait > 0
        ? `retrying in ${formatDuration(wait)} (${new Date(retry.nextRetryAtUnix * 1000).toLocaleString()})`
        : 'retrying now',
    );
  }
  if (retry.blockedSinceUnix != null) {
    parts.push(`waiting ${formatDuration(Math.max(0, nowMs / 1000 - retry.blockedSinceUnix))}`);
  }
  parts.push(`attempt ${retry.attempts}`);
  return parts.join(' · ');
}
