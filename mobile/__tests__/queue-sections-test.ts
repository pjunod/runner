import { JobSummary, JobStatus } from '../src/api/types';
import { queueSectionKey, sectionQueueJobs } from '../src/queueSections';

const statusCases: [JobStatus, string][] = [
  ['downloading', 'downloading'],
  ['fetching', 'fetching'],
  ['post_queued', 'post_queued'],
  [{ post: { stage: 'par_rename' } }, 'renaming'],
  [{ post: { stage: 'rar_rename' } }, 'renaming'],
  [{ post: { stage: 'post_unpack_rename' } }, 'renaming'],
  [{ post: { stage: 'par_verify' } }, 'verifying'],
  [{ post: { stage: 'par_repair' } }, 'repairing'],
  [{ post: { stage: 'unpack' } }, 'extracting'],
  [{ post: { stage: 'cleanup' } }, 'cleaning'],
  [{ post: { stage: 'move' } }, 'moving'],
  [{ post: { stage: 'script' } }, 'scripting'],
  ['queued', 'waiting'],
  ['paused', 'paused'],
  ['failed', 'attention'],
  ['completed', 'post_queued'],
];

test.each(statusCases)('maps %p to the %s queue section', (status, expected) => {
  expect(queueSectionKey(job(1, status))).toBe(expected);
});

test('an open stage keeps an inconsistent completed row in post-processing', () => {
  expect(queueSectionKey({ ...job(1, 'completed'), stages: [
    { stage: 'par_rename', started_at_unix: 1000, ms: 2000 },
    { stage: 'par_verify', started_at_unix: 1002 },
  ] })).toBe('verifying');
});

test('groups by activity while preserving queue positions within each section', () => {
  const jobs = [
    job(10, { post: { stage: 'unpack' } }),
    job(11, 'queued'),
    job(12, 'downloading'),
    job(13, 'paused'),
    job(14, { post: { stage: 'par_repair' } }),
  ];

  const sections = sectionQueueJobs(jobs);

  expect(sections.map((section) => section.definition.key)).toEqual([
    'downloading',
    'repairing',
    'extracting',
    'paused',
    'waiting',
  ]);
  expect(sections.flatMap((section) => section.jobs.map(({ job: item }) => item.id))).toEqual([
    12, 14, 10, 13, 11,
  ]);
  expect(sections.find((section) => section.definition.key === 'waiting')?.jobs).toMatchObject([
    { index: 1, job: { id: 11 } },
  ]);
});

test('grouping uses an open stage when delayed PAR completion left a stale status', () => {
  const stale = job(402, 'completed');
  stale.stages = [{ stage: 'par_verify', started_at_unix: 1002 }];

  const sections = sectionQueueJobs([stale]);

  expect(sections).toMatchObject([
    { definition: { key: 'verifying', label: 'Checking integrity' }, jobs: [{ job: { id: 402 } }] },
  ]);
});

function job(id: number, status: JobStatus): JobSummary {
  return { id, status } as JobSummary;
}

 test('holds and pauses override stale open stage and ready facts', () => {
   const stale = { ...job(1, 'paused'), ready: true, stages: [{ stage: 'unpack', started_at_unix: 1 }],
     control: { lifecycle: 'held' } } as JobSummary;
   expect(queueSectionKey(stale)).toBe('attention');
   expect(queueSectionKey({ ...stale, control: undefined, ready: false })).toBe('paused');
 });
