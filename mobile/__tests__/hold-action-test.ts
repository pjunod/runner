import { holdAction } from '../src/holdAction';
import { JobControl } from '../src/api/types';

const held = (patch: Partial<JobControl>): JobControl => ({
  version: 1, revision: '1', lifecycle: 'held', cause: 'identity_conflict', stage: 'download_write',
  retry_policy: 'review', message: 'why', instance: 'i', ...patch,
});

// Field report 2026-11-01: HELD jobs whose only action was delete.
test('a review hold offers retry', () => {
  expect(holdAction(held({}))).toEqual({ action: 'resume', label: 'Retry' });
});
test('a hold Runner is retrying offers pause, and once paused offers retry', () => {
  const storage = held({ cause: 'storage_unavailable', retry_policy: 'scheduled_retry' });
  expect(holdAction(storage)).toEqual({ action: 'pause', label: 'Pause retries' });
  expect(holdAction({ ...storage, manual_pause: true })).toEqual({ action: 'resume', label: 'Retry' });
});
test('every policy has an exit', () => {
  for (const policy of ['scheduled_retry', 'resume_same_job', 'reobserve_mount', 'review', 'from_a_newer_build']) {
    expect(holdAction(held({ retry_policy: policy }))).not.toBeNull();
  }
});
test('no control, no hold action', () => {
  expect(holdAction(undefined)).toBeNull();
  expect(holdAction(held({ lifecycle: 'running' }))).toBeNull();
});
