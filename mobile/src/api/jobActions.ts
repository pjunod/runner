import { ApiError, NzbdClient } from './client';
import { JobSummary } from './types';
import { torrentDisplayPhase } from '../torrentPresentation';

export interface JobActionResult {
  ok: boolean;
  parked?: boolean;
  seedOptions?: number;
  message?: string;
}
export async function runJobAction(
  client: Pick<NzbdClient, 'jobAction' | 'getJobs'>,
  job: JobSummary | undefined,
  id: number,
  action: Parameters<NzbdClient['jobAction']>[1],
  acceptJobs: (jobs: JobSummary[]) => void,
): Promise<JobActionResult> {
  try { return await client.jobAction(id, action); }
  catch (cause) {
    if (!(cause instanceof ApiError) || cause.status !== 404 || action !== 'resume'
      || job?.kind !== 'torrent' || !job.ready) throw cause;
    let jobs: JobSummary[];
    try { jobs = await client.getJobs(); }
    catch { throw new Error('Runner refused to start seeding. Its current state could not be checked.'); }
    acceptJobs(jobs);
    const current = jobs.find((item) => item.id === id);
    if (!current) return { ok: false, message: 'This torrent is no longer in the queue.' };
    const phase = torrentDisplayPhase(current);
    if (phase === 'seeding' || phase === 'paused_seed') {
      return { ok: false, seedOptions: id, message: 'Runner refused to start seeding. Check this torrent’s seeding policy.' };
    }
    return { ok: false, message: 'The torrent state changed. Check its current status before trying again.' };
  }
}
