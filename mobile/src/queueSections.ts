import { torrentDisplayPhase } from './torrentPresentation';
import { JobStatus, JobSummary, PostStage, StageSpan } from './api/types';

export type QueueSectionKey =
  | 'downloading'
  | 'fetching'
  | 'torrent_metadata'
  | 'checking'
  | 'seeding'
  | 'completed'
  | 'post_queued'
  | 'renaming'
  | 'verifying'
  | 'repairing'
  | 'extracting'
  | 'cleaning'
  | 'moving'
  | 'scripting'
  | 'attention'
  | 'paused'
  | 'waiting';

export interface QueueSectionDefinition {
  key: QueueSectionKey;
  label: string;
  ordered: boolean;
  collapsible?: boolean;
}

export interface SectionedJob {
  job: JobSummary;
  index: number;
}

export interface QueueJobSection {
  definition: QueueSectionDefinition;
  jobs: SectionedJob[];
}

export const QUEUE_SECTIONS: readonly QueueSectionDefinition[] = [
  { key: 'downloading', label: 'Downloading', ordered: true },
  { key: 'fetching', label: 'Fetching NZB', ordered: true },
  { key: 'torrent_metadata', label: 'Fetching torrent metadata', ordered: true },
  { key: 'checking', label: 'Checking torrent files', ordered: false },
  { key: 'post_queued', label: 'Waiting to post-process', ordered: false },
  { key: 'renaming', label: 'Renaming', ordered: false },
  { key: 'verifying', label: 'Checking integrity', ordered: false },
  { key: 'repairing', label: 'Repairing', ordered: false },
  { key: 'extracting', label: 'Extracting', ordered: false },
  { key: 'cleaning', label: 'Cleaning up', ordered: false },
  { key: 'moving', label: 'Moving', ordered: false },
  { key: 'scripting', label: 'Running scripts', ordered: false },
  { key: 'seeding', label: 'Seeding', ordered: false, collapsible: true },
  { key: 'completed', label: 'Completed', ordered: false, collapsible: true },
  { key: 'attention', label: 'Needs attention', ordered: false, collapsible: true },
  { key: 'paused', label: 'Paused', ordered: false, collapsible: true },
  { key: 'waiting', label: 'Waiting', ordered: true, collapsible: true },
];

const POST_STAGE_SECTIONS: Record<PostStage, QueueSectionKey> = {
  par_rename: 'renaming',
  rar_rename: 'renaming',
  post_unpack_rename: 'renaming',
  par_verify: 'verifying',
  par_repair: 'repairing',
  unpack: 'extracting',
  cleanup: 'cleaning',
  move: 'moving',
  script: 'scripting',
};

export function currentPostStage(
  status: JobStatus,
  stages: readonly StageSpan[] = [],
): PostStage | null {
  if (typeof status !== 'string') {
    return status.post.stage;
  }
  const last = stages.length ? stages[stages.length - 1] : undefined;
  return last && last.ms == null ? last.stage : null;
}

export function queueSectionKey(job: JobSummary): QueueSectionKey {
  if (job.control?.lifecycle === 'held' || job.status === 'failed') return 'attention';
  const phase = torrentDisplayPhase(job);
  if (phase) {
    if (phase === 'seeding') return 'seeding';
    if (phase === 'paused_seed') return 'completed';
    if (phase === 'checking' || phase === 'downloading') return phase;
    if (phase === 'fetching_source' || phase === 'fetching_metadata') return 'torrent_metadata';
    if (['failed', 'missing_files', 'storage_hold', 'unknown'].includes(phase)) return 'attention';
    if (phase === 'paused_download') return 'paused';
    return 'waiting';
  }
  const { status, stages } = job;
  if (status === 'paused') return 'paused';
  if (job.pp_done || job.ready) return 'completed';
  const stage = currentPostStage(status, stages);
  if (stage) return POST_STAGE_SECTIONS[stage] ?? 'post_queued';
  if (status === 'downloading' || status === 'fetching' || status === 'post_queued') {
    return status;
  }
  if (status === 'completed') return 'post_queued';
  return status === 'queued' ? 'waiting' : 'attention';
}

export function sectionQueueJobs(jobs: readonly JobSummary[]): QueueJobSection[] {
  const grouped = new Map<QueueSectionKey, SectionedJob[]>();
  jobs.forEach((job, index) => {
    const key = queueSectionKey(job);
    const section = grouped.get(key);
    const entry = { job, index };
    if (section) section.push(entry);
    else grouped.set(key, [entry]);
  });

  return QUEUE_SECTIONS.flatMap((definition) => {
    const sectionJobs = grouped.get(definition.key);
    return sectionJobs ? [{ definition, jobs: sectionJobs }] : [];
  });
}

export function isPostProcessingSection(key: QueueSectionKey): boolean {
  return ['post_queued', 'renaming', 'verifying', 'repairing', 'extracting',
    'cleaning', 'moving', 'scripting'].includes(key);
}

export function sectionTotals(jobs: readonly SectionedJob[]) {
  const value = (n?: number) => n != null && Number.isFinite(n) && n > 0 ? n : 0;
  return jobs.reduce((totals, { job }) => ({
    size: totals.size + value(job.size_bytes),
    uploaded: totals.uploaded + value(job.uploaded_bytes),
    uploadRate: totals.uploadRate + value(job.upload_rate_bps),
  }), { size: 0, uploaded: 0, uploadRate: 0 });
}
