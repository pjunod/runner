import { JobSummary, SeedPolicy } from './api/types';
import { formatDuration } from './api/format';

export type TorrentDisplayPhase =
  | 'fetching_source' | 'fetching_metadata' | 'queued' | 'checking'
  | 'downloading' | 'seeding' | 'paused_download' | 'paused_seed'
  | 'missing_files' | 'failed' | 'storage_hold' | 'unknown';
const phases: readonly string[] = ['fetching_source', 'fetching_metadata', 'queued',
  'checking', 'downloading', 'seeding', 'paused_download', 'paused_seed', 'missing_files', 'failed'];

export function torrentDisplayPhase(job: JobSummary): TorrentDisplayPhase | null {
  if (job.kind !== 'torrent') return null;
  const phase = job.torrent_phase;
  if (job.status === 'failed') return 'failed';
  if (phase === 'missing_files' || phase === 'failed' || phase === 'checking') return phase;
  if (phase === 'paused_download' && job.torrent_control_intent === 'running'
      && job.torrent_error === 'storage full') return 'storage_hold';
  const paused = job.torrent_control_intent
    ? job.torrent_control_intent === 'paused' : job.status === 'paused';
  if (job.ready) return paused ? 'paused_seed' : 'seeding';
  if (paused) return 'paused_download';
  if (job.torrent_control_intent === 'running' && phase === 'paused_download') return 'queued';
  const fallback = phase ?? (job.status === 'fetching' ? 'fetching_metadata' : job.status);
  return typeof fallback === 'string' && phases.includes(fallback)
    ? fallback as TorrentDisplayPhase : 'unknown';
}

export function torrentStatus(job: JobSummary): string {
  const phase = torrentDisplayPhase(job);
  if (phase === 'seeding') return job.upload_rate_bps && job.upload_rate_bps > 0 ? 'Seeding' : 'Seeding · idle';
  return ({ fetching_source: 'Fetching torrent source', fetching_metadata: 'Fetching metadata',
    queued: 'Queued for download', checking: 'Checking files', downloading: 'Downloading',
    storage_hold: 'Waiting for disk space', paused_download: 'Download paused',
    paused_seed: 'Seeding stopped', missing_files: 'Missing files', failed: 'Torrent error',
    unknown: 'Torrent state unavailable' } as Record<string, string>)[phase ?? 'unknown'];
}

export function seedLimitReached(job: JobSummary): boolean {
  const phase = torrentDisplayPhase(job);
  const p = job.seed_policy;
  if (!job.ready || !p || (phase !== 'seeding' && phase !== 'paused_seed')) return false;
  return p.stop_on_complete || (p.ratio_limit != null && (job.ratio ?? 0) >= p.ratio_limit)
    || (p.time_limit_secs != null && (job.seeding_seconds ?? 0) >= p.time_limit_secs);
}

export function seedPolicyText(policy?: SeedPolicy | null): string {
  if (!policy) return 'Seeding policy unavailable';
  if (policy.stop_on_complete) return 'Stop when download completes';
  const limits = [];
  if (policy.ratio_limit != null) limits.push(`ratio ${policy.ratio_limit.toFixed(2)}`);
  if (policy.time_limit_secs != null) limits.push(`${formatDuration(policy.time_limit_secs)} seeding`);
  return limits.length ? `Stop at ${limits.join(' or ')}${limits.length > 1 ? ' · first limit reached' : ''}`
    : 'Keep seeding until stopped';
}

export function seedStopText(job: JobSummary): string {
  return ({ manual: 'Stopped manually', download_complete: 'Stopped after download',
    ratio_limit: 'Ratio limit reached', time_limit: 'Seeding time limit reached',
    storage_full: 'Stopped: storage full' } as Record<string, string>)[job.seed_stop_reason ?? ''] ?? 'Seeding stopped';
}

export interface TorrentAction { action: 'pause' | 'resume' | 'seed-options'; label: string }
export function torrentPrimaryAction(job: JobSummary): TorrentAction | null {
  const phase = torrentDisplayPhase(job);
  if (!phase || ['failed', 'storage_hold', 'unknown'].includes(phase)) return null;
  if (phase === 'missing_files') return job.status === 'paused'
    ? { action: 'resume', label: 'Re-download missing files' } : null;
  if (phase === 'paused_seed' && seedLimitReached(job)) return { action: 'seed-options', label: 'Seeding options' };
  if (job.status === 'paused') return { action: 'resume', label: phase === 'paused_seed' ? 'Start seeding' : 'Resume' };
  if (job.status === 'queued' || job.status === 'downloading') {
    return { action: 'pause', label: phase === 'seeding' ? 'Stop seeding' : 'Pause' };
  }
  return null;
}

export interface SeedDraft { mode: 'defaults' | 'stop' | 'unlimited' | 'limits'; ratio: string; hours: string }
export function seedDraft(policy?: SeedPolicy | null): SeedDraft {
  return { mode: policy?.stop_on_complete ? 'stop'
    : policy?.ratio_limit != null || policy?.time_limit_secs != null ? 'limits' : 'unlimited',
  ratio: policy?.ratio_limit == null ? '' : String(policy.ratio_limit),
  hours: policy?.time_limit_secs == null ? '' : String(policy.time_limit_secs / 3600) };
}
export function seedPolicyBody(draft: SeedDraft): SeedPolicy & { use_defaults: boolean } {
  const body: SeedPolicy & { use_defaults: boolean } = { use_defaults: draft.mode === 'defaults',
    stop_on_complete: draft.mode === 'stop', ratio_limit: null, time_limit_secs: null };
  if (draft.mode === 'limits') {
    body.ratio_limit = draft.ratio.trim() ? Number(draft.ratio) : null;
    body.time_limit_secs = draft.hours.trim() ? Math.round(Number(draft.hours) * 3600) : null;
    if (body.ratio_limit == null && body.time_limit_secs == null) throw new Error('Enter a limit or choose unlimited seeding.');
    if ((body.ratio_limit != null && (!Number.isFinite(body.ratio_limit) || body.ratio_limit <= 0))
      || (body.time_limit_secs != null && (!Number.isSafeInteger(body.time_limit_secs) || body.time_limit_secs <= 0))) {
      throw new Error('Seeding limits must be positive numbers.');
    }
  }
  return body;
}
