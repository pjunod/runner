import { ApiError } from '../src/api/client';
import { runJobAction } from '../src/api/jobActions';
import { JobSummary } from '../src/api/types';
import fixtures from '../../crates/nzbd-types/fixtures/mobile-queue-parity.json';
jest.mock('expo/fetch', () => ({ fetch: jest.fn() }));
const seed = fixtures.find((f) => f.resume404)!.job as JobSummary;
const client = { jobAction: jest.fn(), getJobs: jest.fn() };
beforeEach(() => { jest.resetAllMocks(); client.jobAction.mockRejectedValue(new ApiError('job not found', 404)); });
test('refused seed resume refreshes and opens options without retrying or dropping the job', async () => {
  client.getJobs.mockResolvedValue([seed]);
  const accept = jest.fn();
  expect(await runJobAction(client, seed, seed.id, 'resume', accept)).toMatchObject({ ok: false, seedOptions: seed.id });
  expect(accept).toHaveBeenCalledWith([seed]);
  expect(client.jobAction).toHaveBeenCalledTimes(1);
});
test('only a successful fresh listing can confirm disappearance', async () => {
  client.getJobs.mockResolvedValue([]);
  expect(await runJobAction(client, seed, seed.id, 'resume', jest.fn())).toMatchObject({ ok: false, message: 'This torrent is no longer in the queue.' });
});
test('failed refresh retains prior state and gives a useful error', async () => {
  client.getJobs.mockRejectedValue(new Error('offline'));
  const accept = jest.fn();
  await expect(runJobAction(client, seed, seed.id, 'resume', accept)).rejects.toThrow('current state could not be checked');
  expect(accept).not.toHaveBeenCalled();
});
test('new missing-files state does not open seed options', async () => {
  client.getJobs.mockResolvedValue([{ ...seed, torrent_phase: 'missing_files' }]);
  expect((await runJobAction(client, seed, seed.id, 'resume', jest.fn())).seedOptions).toBeUndefined();
});
test('ordinary action failures are not swallowed', async () => {
  await expect(runJobAction(client, seed, seed.id, 'pause', jest.fn())).rejects.toBeInstanceOf(ApiError);
  expect(client.getJobs).not.toHaveBeenCalled();
});
