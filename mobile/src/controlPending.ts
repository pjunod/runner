import { JobSummary } from './api/types';

// SSE has a minimal marker while GET supplies owner/retry observations. Keep
// those details only for the same accepted control, never after marker removal.
export function mergePendingJobs(previous: readonly JobSummary[], next: JobSummary[]): JobSummary[] {
  const byId = new Map(previous.map(job => [job.id, job]));
  return next.map(job => {
    const old = byId.get(job.id);
    const pending = job.controlPending;
    if (!pending || pending.jobId !== job.id || pending.state !== 'pending'
        || pending.owner != null || pending.nextRetryAtMs != null || !old?.controlPending
        || old.controlPending.jobId !== job.id || !old.control || !job.control
        || old.control.revision !== job.control.revision
        || old.control.instance !== job.control.instance) return job;
    return { ...job, controlPending: old.controlPending };
  });
}

export function controlPendingLabel(job: JobSummary): string | undefined {
  const pending = job.controlPending;
  if (!pending || pending.jobId !== job.id) return undefined;
  return pending.state === 'review_required' ? 'Source needs review'
    : pending.state === 'verified' ? 'Waiting for control acceptance'
    : pending.state === 'retry' ? 'Waiting to retry source check' : 'Waiting for source owner';
}
export function controlPendingText(job: JobSummary): string | undefined {
  const pending = job.controlPending;
  const label = controlPendingLabel(job);
  if (!pending || !label) return undefined;
  const date = pending.nextRetryAtMs == null ? undefined : new Date(pending.nextRetryAtMs);
  return [label, typeof pending.owner === 'string' && pending.owner ? `Owner: ${pending.owner.slice(0, 128)}` : undefined,
    typeof pending.error === 'string' ? pending.error.slice(0, 2048) : undefined, date && Number.isFinite(date.getTime()) ? `Next check: ${date.toLocaleString()}` : undefined,
    'Existing controls remain available.'].filter(Boolean).join(' · ');
}
