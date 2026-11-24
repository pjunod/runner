import { JobControl } from './api/types';

/**
 * The one control a held job's card offers. Mirrors the web queue row and
 * docs/archive/HELD_JOBS_PLAN.md §4.6: every hold has an exit.
 *
 * Contract v2: every hold is environmental and automatic, and Runner ends it
 * at a deadline. So there is no hold that waits for a person.
 *
 * - Runner is retrying it on its own (not paused): "Pause retries" stops that.
 * - The retries were paused: "Retry" sends it back to the step it stopped
 *   in; a cause that is still there holds it again with the reason.
 */
export interface HoldAction {
  action: 'pause' | 'resume';
  label: string;
}

export function holdAction(control: JobControl | null | undefined): HoldAction | null {
  if (!control || control.lifecycle !== 'held') return null;
  if (!control.manual_pause) {
    return { action: 'pause', label: 'Pause retries' };
  }
  return { action: 'resume', label: 'Retry' };
}
