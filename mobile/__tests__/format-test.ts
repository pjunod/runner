import {
  criticalStorage,
  diskGuardMessage,
  formatBytes,
  formatDuration,
  jobProgress,
  jobStatusKey,
  jobStatusLabel,
  normalizeServerUrl,
  storageEvidenceLabel,
  storageUsage,
} from '../src/api/format';
import { JobSummary, StatusDto } from '../src/api/types';

const job = (changes: Partial<JobSummary> = {}): JobSummary => ({
  id: 1,
  name: 'example',
  status: 'downloading',
  category: null,
  priority: 0,
  size_bytes: 100,
  downloaded_bytes: 25,
  failed_bytes: 0,
  remaining_bytes: 75,
  total_articles: 10,
  done_articles: 2,
  failed_articles: 0,
  files_total: 1,
  files_done: 0,
  health: 1000,
  critical_health: 850,
  rate_bps: 10,
  retried_articles: 0,
  assigned_node: null,
  pp_done: false,
  ready: false,
  ready_at_unix: null,
  dupe_key: '',
  dupe_score: 0,
  params: [],
  stages: [],
  ...changes,
});

describe('API presentation helpers', () => {
  test('distinguishes forecast holds from observed write failures', () => {
    const base = {
      disk_low: true,
      disk_guard_label: 'category: tv',
      disk_guard_path: '/library/tv',
      disk_guard_free_bytes: 1024,
      enospc_observed: 0,
      enospc_where: null,
    } as StatusDto;
    expect(diskGuardMessage(base)).toContain('category: tv (1.00 KiB free)');
    expect(
      diskGuardMessage({
        ...base,
        disk_guard_write_latched: true,
        enospc_observed: 1,
        enospc_where: 'write /scratch/job.part',
      }),
    ).toBe('Downloads are held because a write ran out of space: write /scratch/job.part.');
  });

  test('labels only retained capacity as last known', () => {
    expect(
      storageEvidenceLabel({
        label: 'library',
        path: '/library',
        available_bytes: 10,
        total_bytes: 100,
        current: false,
      }),
    ).toBe('library · last known');
    expect(
      storageEvidenceLabel({
        label: 'library',
        path: '/library',
        available_bytes: null,
        total_bytes: null,
        current: false,
      }),
    ).toBe('library');
  });

  test('does not describe a high known reading as low during an incomplete hold', () => {
    expect(
      diskGuardMessage({
        disk_low: true,
        disk_guard_all_roots_known: false,
        disk_guard_free_bytes: 1024 * 1024 * 1024,
        disk_guard_write_latched: false,
      } as StatusDto),
    ).toBe(
      'Downloads remain held because not every configured storage root could be checked; the lowest known reading is 1.00 GiB free.',
    );
  });

  test('preserves the legacy low-space message for an older daemon response', () => {
    expect(diskGuardMessage({ disk_low: true } as StatusDto)).toBe(
      'Downloads are held because the destination volume is low on space.',
    );
  });

  test('normalizes a host and supplies the LAN-friendly HTTP scheme', () => {
    expect(normalizeServerUrl(' 192.168.1.20:6789 ')).toBe('http://192.168.1.20:6789');
    expect(normalizeServerUrl('https://downloads.example.test')).toBe(
      'https://downloads.example.test:6789',
    );
    expect(normalizeServerUrl('nuc3')).toBe('http://nuc3:6789');
    expect(normalizeServerUrl('nuc3.local')).toBe('http://nuc3.local:6789');
    expect(normalizeServerUrl('192.168.4.7')).toBe('http://192.168.4.7:6789');
    expect(normalizeServerUrl('[2001:db8::7]')).toBe('http://[2001:db8::7]:6789');
  });

  test('explains unsupported scoped IPv6 addresses', () => {
    expect(() => normalizeServerUrl('http://[fe80::8%25wlan0]:6789')).toThrow('Choose another discovered address');
  });

  test('preserves an explicitly selected port', () => {
    expect(normalizeServerUrl('nuc3:8080')).toBe('http://nuc3:8080');
    expect(normalizeServerUrl('https://nuc3:8443')).toBe('https://nuc3:8443');
  });

  test('rejects credentials and API paths embedded in the server URL', () => {
    expect(() => normalizeServerUrl('http://user:pass@example.test:6789')).toThrow(
      'credentials',
    );
    expect(() => normalizeServerUrl('http://example.test:6789/api')).toThrow(
      'without a path',
    );
  });

  test('reads both unit and structured Rust enum status shapes', () => {
    expect(jobStatusKey('paused')).toBe('paused');
    expect(jobStatusKey({ post: { stage: 'par_repair' } })).toBe('post:par_repair');
    expect(jobStatusLabel({ post: { stage: 'par_repair' } })).toBe('par repair');
    expect(jobStatusLabel('completed', false)).toBe('download complete');
    expect(jobStatusLabel('completed', true)).toBe('ready');
  });

  test('calculates bounded byte progress and formats binary units', () => {
    expect(jobProgress(job())).toBe(0.25);
    expect(jobProgress(job({ downloaded_bytes: 250 }))).toBe(1);
    expect(formatBytes(1_048_576)).toBe('1.00 MiB');
  });

  test('selects the most-full configured filesystem as critical storage', () => {
    const paths = [
      {
        label: 'working',
        path: '/data',
        available_bytes: 40,
        total_bytes: 100,
      },
      {
        label: 'complete',
        path: '/downloads',
        available_bytes: 5,
        total_bytes: 100,
      },
      {
        label: 'measuring',
        path: '/later',
        available_bytes: null,
        total_bytes: null,
      },
    ];

    const critical = criticalStorage(paths);
    expect(critical?.label).toBe('complete');
    expect(critical && storageUsage(critical)).toEqual({
      availableBytes: 5,
      totalBytes: 100,
      usedBytes: 95,
      usedPercent: 95,
    });
  });
});

test('durations round once, on the total, never to a 60-minute remainder', () => {
  expect(formatDuration(9 * 3600 + 59 * 60 + 30)).toBe('10h');
  expect(formatDuration(3599.5)).toBe('1h');
  expect(formatDuration(3540)).toBe('59m');
  expect(formatDuration(3600 + 61)).toBe('1h 2m');
});
