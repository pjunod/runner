import { JobControl } from './api/types';

/**
 * The one control a held job's card offers. Mirrors the web queue row and
 * docs/HELD_JOBS_PLAN.md §4.6: every hold has an exit.
 *
 * - Runner is retrying it on its own (an automatic policy, not paused):
 *   "Pause retries" stops that.
 * - A person decides (`review`), or the retries were paused: "Retry" sends
 *   it back to the step it stopped in; a cause that is still there holds it
 *   again with the reason.
 */
export interface HoldAction {
  action: 'pause' | 'resume';
  label: string;
}

const AUTOMATIC = ['scheduled_retry', 'resume_same_job', 'reobserve_mount'];

export function holdAction(control: JobControl | null | undefined): HoldAction | null {
  if (!control || control.lifecycle !== 'held') return null;
  if (AUTOMATIC.includes(control.retry_policy) && !control.manual_pause) {
    return { action: 'pause', label: 'Pause retries' };
  }
  return { action: 'resume', label: 'Retry' };
}
