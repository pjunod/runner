import fixtures from '../../crates/nzbd-types/fixtures/mobile-queue-parity.json';
import { JobSummary } from '../src/api/types';
import { queueSectionKey, sectionQueueJobs, sectionTotals } from '../src/queueSections';
import { torrentDisplayPhase, torrentStatus, torrentPrimaryAction, seedPolicyBody, seedLimitReached } from '../src/torrentPresentation';

for (const fixture of fixtures) {
  test(fixture.name, () => {
    const job = fixture.job as JobSummary;
    expect(queueSectionKey(job)).toBe(fixture.section);
    expect(torrentDisplayPhase(job)).toBe(fixture.phase);
    if (job.kind === 'torrent') {
      expect(torrentStatus(job).toLowerCase()).toBe(fixture.statusLabel);
      expect(torrentPrimaryAction(job)?.action ?? null).toBe(fixture.action);
      expect(torrentPrimaryAction(job)?.label.toLowerCase() ?? null).toBe(fixture.actionLabel);
    }
  });
}
test('every mixed-queue job occurs once and retains its original action index', () => {
  const jobs = fixtures.map((f) => f.job as JobSummary);
  const entries = sectionQueueJobs(jobs).flatMap((s) => s.jobs);
  expect(entries).toHaveLength(jobs.length);
  expect(new Set(entries.map((e) => e.job.id)).size).toBe(jobs.length);
  for (const e of entries) expect(jobs[e.index]).toBe(e.job);
});
test('totals ignore absent and invalid counters', () => {
  expect(sectionTotals([{ index: 0, job: { size_bytes: 20, upload_rate_bps: Infinity, uploaded_bytes: NaN } as JobSummary }]))
    .toEqual({ size: 20, uploaded: 0, uploadRate: 0 });
});
test.each(['0', '-1', 'Infinity', 'NaN'])('rejects ratio %s', (ratio) => {
  expect(() => seedPolicyBody({ mode: 'limits', ratio, hours: '' })).toThrow();
});
test.each(['0', '-1', '1e99', '0.0000001'])('rejects hours %s', (hours) => {
  expect(() => seedPolicyBody({ mode: 'limits', ratio: '', hours })).toThrow();
});
test('policy modes and unit conversion', () => {
  expect(seedPolicyBody({ mode: 'limits', ratio: '2', hours: '1.5' })).toEqual({
    use_defaults: false, stop_on_complete: false, ratio_limit: 2, time_limit_secs: 5400,
  });
  expect(seedPolicyBody({ mode: 'defaults', ratio: '', hours: '' }).use_defaults).toBe(true);
  expect(seedPolicyBody({ mode: 'stop', ratio: '', hours: '' }).stop_on_complete).toBe(true);
  expect(seedPolicyBody({ mode: 'unlimited', ratio: '2', hours: '1' }).ratio_limit).toBeNull();
  expect(() => seedPolicyBody({ mode: 'limits', ratio: '', hours: '' })).toThrow();
});
test('thresholds, empty selection, and stale-ready missing-file recovery', () => {
  const seed = fixtures.find((f) => f.name === 'stopped seed')!.job as JobSummary;
  const p = { stop_on_complete: false, ratio_limit: 2, time_limit_secs: null };
  expect(seedLimitReached({ ...seed, ratio: 2, seed_policy: p })).toBe(true);
  expect(seedLimitReached({ ...seed, ratio: 0, size_bytes: 0, seed_policy: p })).toBe(false);
  expect(seedLimitReached({ ...seed, ratio: 0, size_bytes: 0, seed_policy: { ...p, stop_on_complete: true } })).toBe(true);
  expect(seedLimitReached({ ...seed, ratio: 0, size_bytes: 0, seeding_seconds: 10, seed_policy: { ...p, time_limit_secs: 10 } })).toBe(true);
  expect(seedLimitReached({ ...seed, torrent_phase: 'missing_files', seed_policy: { ...p, stop_on_complete: true } })).toBe(false);
});
