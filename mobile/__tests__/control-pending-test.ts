import { JobSummary } from '../src/api/types';
import { mergePendingJobs, controlPendingText } from '../src/controlPending';
import { acquisitionStatus } from '../src/torrentPresentation';
const job = { id: 7, control: { revision: '3', instance: 'leader-a' }, controlPending: {
  jobId: 7, owner: 'worker-a', state: 'retry', error: 'Source owner offline', nextRetryAtMs: 1e300,
} } as JobSummary;
test('minimal SSE preserves observations only for the same accepted control and marker', () => {
  const generic = { ...job, controlPending: { jobId: 7, owner: null, state: 'pending', error: 'Waiting for proof', nextRetryAtMs: null } } as JobSummary;
  expect(mergePendingJobs([job], [generic])[0].controlPending).toEqual(job.controlPending);
  expect(mergePendingJobs([job], [{ ...generic, controlPending: undefined }])[0].controlPending).toBeUndefined();
  expect(mergePendingJobs([job], [{ ...generic, control: { ...job.control!, revision: '4' } }])[0].controlPending).toEqual(generic.controlPending);
  expect(mergePendingJobs([job], [{ ...generic, control: { ...job.control!, instance: 'leader-b' } }])[0].controlPending).toEqual(generic.controlPending);
  expect(controlPendingText(job)).toContain('Waiting to retry source check');
  expect(controlPendingText(job)).toContain('worker-a');
  expect(controlPendingText(job)).not.toContain('Invalid Date');
});
test('native acquisition labels do not describe donor network activity as child transfer', () => {
  const child = { id: 9, kind: 'torrent', torrent_role: 'acquisition', seed_donor_job_id: 5 } as JobSummary;
  expect(acquisitionStatus({ ...child, status: 'queued' })).toBe('Waiting for seed source');
  expect(acquisitionStatus({ ...child, status: 'downloading' })).toBe('Copying seed files');
  expect(acquisitionStatus({ ...child, torrent_role: 'seed', status: 'downloading' })).toBeUndefined();
});
