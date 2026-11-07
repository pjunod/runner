import { JobControl } from './api/types';
import { formatDuration } from './api/format';

/**
 * Contract v2 §2.3: a hold is environmental, automatic, and ends at a
 * deadline Runner enforces. These are the causes that heal without a code
 * change; a cause a newer node wrote is shown as it came.
 */
const CAUSES: Record<string, string> = {
  storage_unavailable: 'storage unavailable',
  capacity: 'out of space',
  quota: 'quota reached',
  allocation: 'allocation failed',
  mount_unavailable: 'mount unavailable',
  missing_tool: 'tool missing',
  engine_unavailable: 'engine unavailable',
  password_required: 'password required',
};

export function holdCauseLabel(cause: string | null | undefined): string {
  return (cause && CAUSES[cause]) || cause || 'held';
}

/** "storage unavailable: write failed" — the cause, then Runner's message. */
export function holdHeadline(control: JobControl): string {
  return `${holdCauseLabel(control.cause)}: ${control.message || 'job on hold'}`;
}

/**
 * Seconds left before Runner ends the hold as failed/hold_expired.
 * `deadline_unix` counts down live against this device's clock;
 * `deadline_in_s` is Runner's own remaining time at its last control write
 * (hosts share no clock), used when no unix time came.
 */
export function holdDeadlineSeconds(control: JobControl, nowMs: number): number | null {
  if (control.deadline_unix != null) return Math.max(0, control.deadline_unix - nowMs / 1000);
  if (control.deadline_in_s != null) return Math.max(0, control.deadline_in_s);
  return null;
}

export function holdDeadlineText(control: JobControl, nowMs: number): string | null {
  const left = holdDeadlineSeconds(control, nowMs);
  if (left == null) return null;
  return `Fails as hold expired in ${formatDuration(left)} unless it recovers`;
}
