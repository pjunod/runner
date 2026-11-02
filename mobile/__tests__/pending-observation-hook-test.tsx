import React from 'react';
import { useNzbd } from '../src/hooks/useNzbd';
import { JobSummary } from '../src/api/types';
const { act, create } = require('react-test-renderer');
const mockClient = { getSnapshot: jest.fn(), getJobs: jest.fn(), openEventStream: jest.fn() };
jest.mock('../src/api/client', () => ({ NzbdClient: jest.fn(() => mockClient) }));
let state: ReturnType<typeof useNzbd>;
let tree: any;
let deliver: (value: unknown) => void;
function Probe() { state = useNzbd(config); return null; }
const config = { baseUrl: 'http://fixture', token: '', username: '', password: '' };
const job = { id: 7, status: 'queued', control: { revision: '3', instance: 'leader-a' },
  controlPending: { jobId: 7, state: 'pending', owner: null, error: 'Waiting for proof', nextRetryAtMs: null } } as JobSummary;
const detailed = { ...job, controlPending: { ...job.controlPending!, state: 'review_required', owner: 'worker-a', error: 'Exact source changed' } } as JobSummary;
async function tick(next: JobSummary) {
  await act(async () => { deliver({ done: false, value: new TextEncoder().encode(`event: tick\ndata: ${JSON.stringify({jobs:[next],status:{}})}\n\n`) }); });
}
beforeEach(async () => {
  jest.useFakeTimers();
  mockClient.getSnapshot.mockResolvedValue({jobs:[{...job,controlPending:undefined}],status:{}});
  mockClient.getJobs.mockResolvedValue([detailed]);
  mockClient.openEventStream.mockResolvedValue({ok:true,body:{getReader:()=>({read:()=>new Promise(resolve=>{deliver=resolve})})}});
  await act(async () => { tree = create(<Probe />); });
  await tick(job);
});
afterEach(async () => { await act(async () => tree.unmount()); jest.useRealTimers(); jest.clearAllMocks(); });
test('healthy SSE polls pending details once and updates changed observations', async () => {
  await act(async () => jest.advanceTimersByTime(5000));
  expect(state!.connectionState).toBe('live');
  expect(state!.snapshot!.jobs[0].controlPending).toEqual(detailed.controlPending);
  expect(mockClient.getJobs).toHaveBeenCalledTimes(1);
  mockClient.getJobs.mockResolvedValue([{...detailed,controlPending:{...detailed.controlPending!,error:'Owner returned; checking again'}}]);
  await tick(job);
  await act(async () => jest.advanceTimersByTime(5000));
  expect(state!.snapshot!.jobs[0].controlPending!.error).toBe('Owner returned; checking again');
  expect(mockClient.getSnapshot).toHaveBeenCalledTimes(1);
});
test.each(['ack','revision'])('delayed observation ignores current %s boundary', async boundary => {
  let resolve!: (value: JobSummary[]) => void;
  mockClient.getJobs.mockImplementation(()=>new Promise(done=>{resolve=done}));
  await act(async () => jest.advanceTimersByTime(5000));
  await tick(job);
  await act(async () => jest.advanceTimersByTime(5000));
  expect(mockClient.getJobs).toHaveBeenCalledTimes(1);
  const current = boundary === 'ack' ? {...job,controlPending:undefined} : {...job,control:{...job.control!,revision:'4'}};
  await tick(current);
  await act(async () => resolve([detailed]));
  expect(state!.snapshot!.jobs[0].controlPending).toEqual(current.controlPending);
  expect(state!.snapshot!.jobs[0].control!.revision).toBe(current.control!.revision);
});
