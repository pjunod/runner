import { JobStatus, JobSummary, StatusDto, StoragePath } from './types';

const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
export const DEFAULT_NZBD_PORT = 6789;

export function formatBytes(value: number, suffix = ''): string {
  if (!Number.isFinite(value) || value <= 0) return `0 B${suffix}`;
  let amount = value;
  let unit = 0;
  while (amount >= 1024 && unit < UNITS.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  const digits = amount >= 100 || unit === 0 ? 0 : amount >= 10 ? 1 : 2;
  return `${amount.toFixed(digits)} ${UNITS[unit]}${suffix}`;
}

export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return '—';
  if (seconds < 60) return `${Math.ceil(seconds)}s`;
  // Round once, on the total: rounding the minutes of the last hour on their
  // own turned 9h 59m 30s into "9h 60m".
  const totalMinutes = Math.ceil(seconds / 60);
  if (totalMinutes < 60) return `${totalMinutes}m`;
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
}

export function diskGuardMessage(status: StatusDto): string | null {
  if (!status.disk_low) return null;
  const hasMultiRootEvidence =
    status.disk_guard_free_bytes !== undefined ||
    status.disk_guard_label !== undefined ||
    status.disk_guard_path !== undefined ||
    status.disk_guard_write_latched !== undefined ||
    status.disk_guard_all_roots_known !== undefined;
  if (!hasMultiRootEvidence) {
    return 'Downloads are held because the destination volume is low on space.';
  }
  if (status.disk_guard_write_latched) {
    return `Downloads are held because a write ran out of space${
      status.enospc_where ? `: ${status.enospc_where}` : ''
    }.`;
  }
  if (status.disk_guard_all_roots_known === false) {
    const known = status.disk_guard_free_bytes;
    return `Downloads remain held because not every configured storage root could be checked${
      known != null ? `; the lowest known reading is ${formatBytes(known)} free` : ''
    }.`;
  }
  if (status.disk_guard_free_bytes == null) {
    return 'Downloads are held while configured storage is checked.';
  }
  const root = status.disk_guard_label ?? status.disk_guard_path;
  const free = status.disk_guard_free_bytes;
  const detail = root
    ? `${root}${free != null ? ` (${formatBytes(free)} free)` : ''}`
    : 'a configured write volume';
  return `Downloads are held because ${detail} is low on space.`;
}

export function storageEvidenceLabel(storage: StoragePath): string {
  const label = storage.name || storage.label || 'volume';
  return storage.current === false && storage.available_bytes != null
    ? `${label} · last known`
    : label;
}

export interface StorageUsage {
  availableBytes: number;
  totalBytes: number;
  usedBytes: number;
  usedPercent: number;
}

export function storageUsage(storage: StoragePath): StorageUsage | null {
  if (
    storage.total_bytes === null ||
    storage.available_bytes === null ||
    !Number.isFinite(storage.total_bytes) ||
    !Number.isFinite(storage.available_bytes) ||
    storage.total_bytes <= 0
  ) {
    return null;
  }
  const totalBytes = storage.total_bytes;
  const availableBytes = Math.max(0, Math.min(totalBytes, storage.available_bytes));
  const usedBytes = totalBytes - availableBytes;
  return {
    availableBytes,
    totalBytes,
    usedBytes,
    usedPercent: (usedBytes * 100) / totalBytes,
  };
}

export function criticalStorage(paths: readonly StoragePath[]): StoragePath | null {
  let critical: StoragePath | null = null;
  let highestUsed = -1;
  for (const path of paths) {
    const usage = storageUsage(path);
    if (usage && usage.usedPercent > highestUsed) {
      critical = path;
      highestUsed = usage.usedPercent;
    }
  }
  return critical ?? paths[0] ?? null;
}

export function jobProgress(job: JobSummary): number {
  if (job.size_bytes > 0) {
    return Math.max(0, Math.min(1, job.downloaded_bytes / job.size_bytes));
  }
  if (job.total_articles > 0) {
    return Math.max(0, Math.min(1, job.done_articles / job.total_articles));
  }
  return 0;
}

export function jobStatusKey(status: JobStatus): string {
  if (typeof status === 'string') return status;
  return status.post?.stage ? `post:${status.post.stage}` : 'post';
}

export function jobStatusLabel(status: JobStatus, ready = false): string {
  const key = jobStatusKey(status);
  if (key.startsWith('post:')) {
    return key.slice(5).replaceAll('_', ' ');
  }
  if (key === 'completed') return ready ? 'ready' : 'download complete';
  return key.replaceAll('_', ' ');
}

export function isJobPaused(status: JobStatus): boolean {
  return jobStatusKey(status) === 'paused';
}

/// Stopped by a person (`paused`) or by a hold (`held`).
export function isJobStopped(status: JobStatus): boolean {
  return isJobPaused(status) || jobStatusKey(status) === 'held';
}

export function normalizeServerUrl(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) throw new Error('Enter the address of your Runner server.');
  if (/\[[^\]]*%[^\]]*\]/.test(trimmed)) {
    throw new Error('This scoped IPv6 address is unsupported. Choose another discovered address or use the server’s .local hostname.');
  }
  const withScheme = /^https?:\/\//i.test(trimmed) ? trimmed : `http://${trimmed}`;
  let parsed: URL;
  try {
    parsed = new URL(withScheme);
  } catch {
    throw new Error('Use an address like http://192.168.1.20:6789 or http://[2001:db8::7]:6789.');
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error('The server address must use http:// or https://.');
  }
  if (parsed.username || parsed.password) {
    throw new Error('Put credentials in the fields below, not in the URL.');
  }
  if (parsed.pathname !== '/' || parsed.search || parsed.hash) {
    throw new Error('Use the Runner server origin without a path or query.');
  }
  if (!hasExplicitPort(withScheme)) {
    parsed.port = String(DEFAULT_NZBD_PORT);
  }
  return parsed.origin;
}

function hasExplicitPort(url: string): boolean {
  const authority = url.replace(/^https?:\/\//i, '').split(/[/?#]/, 1)[0];
  if (authority.startsWith('[')) {
    return /^\[[^\]]+\]:\d+$/.test(authority);
  }
  return /:\d+$/.test(authority);
}

export function jobEta(job: JobSummary): string {
  if (isJobStopped(job.status)) return 'paused';
  if (job.rate_bps <= 0 || job.remaining_bytes <= 0) return '—';
  return formatDuration(job.remaining_bytes / job.rate_bps);
}
