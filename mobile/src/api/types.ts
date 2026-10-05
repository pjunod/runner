export type PostStage =
  | 'par_rename'
  | 'par_verify'
  | 'par_repair'
  | 'rar_rename'
  | 'unpack'
  | 'cleanup'
  | 'move'
  | 'post_unpack_rename'
  | 'script';

export type JobStatus =
  | 'queued'
  | 'downloading'
  | 'paused'
  | 'fetching'
  | 'post_queued'
  | 'completed'
  | 'failed'
  | 'deleted'
  | { post: { stage: PostStage } };

export interface StageSpan {
  stage: PostStage;
  started_at_unix: number;
  ms?: number;
}

export type TorrentPhase =
  | 'fetching_source' | 'fetching_metadata' | 'queued' | 'checking'
  | 'downloading' | 'seeding' | 'paused_download' | 'paused_seed'
  | 'missing_files' | 'failed';

export interface SeedPolicy {
  stop_on_complete: boolean;
  ratio_limit: number | null;
  time_limit_secs: number | null;
}

export interface JobControl {
  version: number;
  revision: string;
  lifecycle: string;
  cause: string;
  stage: string;
  retry_policy: string;
  message: string;
  instance: string;
  previous_status?: JobStatus | null;
  manual_pause?: boolean;
}

export interface JobSummary {
  control?: JobControl | null;
  id: number;
  kind?: 'nzb' | 'torrent';
  torrent_phase?: TorrentPhase | null;
  torrent_control_intent?: 'running' | 'paused' | null;
  seed_policy?: SeedPolicy | null;
  seed_stop_reason?: 'manual' | 'download_complete' | 'ratio_limit' | 'time_limit' | 'storage_full' | null;
  torrent_error?: string | null;
  name: string;
  status: JobStatus;
  category: string | null;
  priority: number;
  size_bytes: number;
  downloaded_bytes: number;
  failed_bytes: number;
  remaining_bytes: number;
  total_articles: number;
  done_articles: number;
  failed_articles: number;
  files_total: number;
  files_done: number;
  health: number;
  critical_health: number;
  critical_health_estimated?: boolean;
  rate_bps: number;
  retried_articles: number;
  assigned_node: string | null;
  pp_done: boolean;
  ready: boolean;
  ready_at_unix: number | null;
  uploaded_bytes?: number;
  upload_rate_bps?: number;
  ratio?: number;
  seeding_seconds?: number;
  useful_peers?: number;
  dupe_key: string;
  dupe_score: number;
  params: [string, string][];
  stages: StageSpan[];
}

export interface ServerVolume {
  server: number;
  name: string;
  total_bytes: number;
  day_bytes: number;
  month_bytes: number;
  rate_bps: number;
}

export interface StoragePath {
  label: string;
  path: string;
  available_bytes: number | null;
  total_bytes: number | null;
  current?: boolean;
}

export interface StatusDto {
  version: string;
  built: string;
  up_since_unix: number;
  download_rate_bps: number;
  remaining_bytes: number;
  session_downloaded_bytes: number;
  download_paused: boolean;
  disk_low: boolean;
  disk_guard_free_bytes?: number | null;
  disk_guard_label?: string | null;
  disk_guard_path?: string | null;
  disk_guard_write_latched?: boolean;
  disk_guard_all_roots_known?: boolean;
  enospc_observed: number;
  enospc_where: string | null;
  quota_reached: boolean;
  blocked_servers: number[];
  health_abort: boolean;
  speed_limit_bps: number | null;
  max_active_downloads: number;
  jobs_queued: number;
  jobs_downloading: number;
  jobs_post: number;
  jobs_finished: number;
  servers: ServerVolume[];
  storage: StoragePath[];
}

export interface QueueSnapshot {
  status: StatusDto;
  jobs: JobSummary[];
}

export interface ConnectionConfig {
  baseUrl: string;
  username: string;
  password: string;
  token: string;
}

export interface AddNzbOptions {
  name: string;
  category?: string;
  priority: number;
  paused: boolean;
}

export interface AddNzbResult {
  id: number;
}

export interface AddTorrentOptions {
  category?: string;
  priority: number;
  paused: boolean;
}

export interface AddTorrentResult {
  id: number;
  info_hash: string;
  created?: boolean;
}

export interface HistoryFile {
  name: string;
  size: number;
  segments_total: number;
  segments_done: number;
  segments_failed: number;
  par2: boolean;
}

export interface HistoryJobRecord {
  files: HistoryFile[];
  total_articles: number;
  success_articles: number;
  failed_articles: number;
  retried_articles: number;
  par_size: number;
  url: string | null;
  client: string | null;
  category: string | null;
  queued_at_unix: number | null;
  original_name: string | null;
  dir_name: string | null;
}

export interface HistoryEntry {
  job: number;
  name: string;
  category: string | null;
  final_dir: string | null;
  status: string;
  size: number;
  health: number;
  completed_at_unix: number;
  picked_up_by: string | null;
  removed_at_unix: number | null;
  can_requeue: boolean;
  record: HistoryJobRecord | null;
  stages: StageSpan[];
}

export interface HistoryPage {
  entries: HistoryEntry[];
  total: number;
  offset: number;
  limit: number;
}

export type LogScope = 'system' | 'job' | 'file';

export interface LogEntry {
  id: number;
  kind: 'INFO' | 'WARNING' | 'ERROR' | 'DETAIL' | string;
  time_unix: number;
  text: string;
  scope: LogScope;
  job: number | null;
}

export interface LogsPage {
  entries: LogEntry[];
}

export type ConnectionState =
  | 'connecting'
  | 'live'
  | 'polling'
  | 'reconnecting'
  | 'offline';
