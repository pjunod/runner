import { StatusDto } from './api/types';

export type QueueLane = 'downloads' | 'post';
export type QueueLaneAction = 'pause' | 'resume' | 'pause-post' | 'resume-post';

export const QUEUE_LANES: { lane: QueueLane; name: string; noun: string }[] = [
  { lane: 'downloads', name: 'Downloads', noun: 'downloads' },
  { lane: 'post', name: 'Post-processing', noun: 'post-processing' },
];

/** Older daemons do not report post_paused; treat that as running. */
export function lanePaused(status: Pick<StatusDto, 'download_paused' | 'post_paused'>, lane: QueueLane): boolean {
  return lane === 'downloads' ? !!status.download_paused : !!status.post_paused;
}

/** The action a lane's toggle sends: running lanes pause, paused lanes resume. */
export function laneToggleAction(lane: QueueLane, paused: boolean): QueueLaneAction {
  if (lane === 'downloads') return paused ? 'resume' : 'pause';
  return paused ? 'resume-post' : 'pause-post';
}

export function laneToggleLabel(lane: QueueLane, paused: boolean): string {
  const noun = QUEUE_LANES.find(l => l.lane === lane)!.noun;
  return `${paused ? 'Resume' : 'Pause'} ${noun}`;
}

export function laneNotice(action: QueueLaneAction): string {
  switch (action) {
    case 'pause': return 'Downloads paused.';
    case 'resume': return 'Downloads resumed.';
    case 'pause-post': return 'Post-processing paused.';
    case 'resume-post': return 'Post-processing resumed.';
  }
}

/** Text for the paused indicator, or null when both lanes are running. */
export function pausedIndicator(status: Pick<StatusDto, 'download_paused' | 'post_paused'>): string | null {
  const parts = [
    lanePaused(status, 'downloads') ? 'downloads paused' : null,
    lanePaused(status, 'post') ? 'post-processing paused' : null,
  ].filter(Boolean);
  return parts.length ? parts.join(' · ') : null;
}
