import { fetch } from 'expo/fetch';
import { File } from 'expo-file-system';

import {
  AddNzbOptions,
  AddNzbResult,
  AddTorrentOptions,
  AddTorrentResult,
  ConnectionConfig,
  HistoryPage,
  JobSummary,
  LogsPage,
  QueueSnapshot,
  StatusDto,
  SeedPolicy,
} from './types';

const CLIENT_NAME = 'nzbd-mobile/1.0';

function utf8Base64(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export class NzbdClient {
  constructor(readonly config: ConnectionConfig) {}

  async getStatus(): Promise<StatusDto> {
    return this.json<StatusDto>('/api/v1/status');
  }

  async getJobs(): Promise<JobSummary[]> {
    const result = await this.json<{ jobs: JobSummary[] }>('/api/v1/jobs');
    return result.jobs;
  }

  async getSnapshot(): Promise<QueueSnapshot> {
    const [status, jobs] = await Promise.all([this.getStatus(), this.getJobs()]);
    return { status, jobs };
  }

  async getHistory(limit = 200, offset = 0): Promise<HistoryPage> {
    const query = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    return this.json<HistoryPage>(`/api/v1/history?${query.toString()}`);
  }

  async getLogs(includeFiles = false, limit = 1000): Promise<LogsPage> {
    const query = new URLSearchParams({
      limit: String(limit),
      scope: includeFiles ? 'system,job,file' : 'system,job',
    });
    return this.json<LogsPage>(`/api/v1/logs?${query.toString()}`);
  }

  async queueAction(action: 'pause' | 'resume'): Promise<void> {
    await this.json(`/api/v1/queue/actions/${action}`, { method: 'POST' });
  }

  async jobAction(
    id: number,
    action:
      | 'pause'
      | 'resume'
      | 'delete'
      | 'delete-files'
      | 'move-top'
      | 'move-up'
      | 'move-down'
      | 'move-bottom',
  ): Promise<{ ok: boolean; parked?: boolean }> {
    return this.json(`/api/v1/jobs/${id}/actions/${action}`, { method: 'POST' });
  }

  async setSeedPolicy(id: number, policy: SeedPolicy & { use_defaults: boolean }): Promise<void> {
    await this.json(`/api/v1/jobs/${id}/torrent/seed-policy`, {
      method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(policy),
    });
  }

  async setJobPriority(id: number, priority: number): Promise<void> {
    await this.json(`/api/v1/jobs/${id}/priority`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ priority }),
    });
  }

  async addNzb(file: File, options: AddNzbOptions): Promise<AddNzbResult> {
    const query = new URLSearchParams({
      name: options.name,
      priority: String(options.priority),
      paused: String(options.paused),
    });
    if (options.category?.trim()) query.set('category', options.category.trim());

    const response = await this.request(`/api/v1/jobs?${query.toString()}`, {
      method: 'POST',
      headers: { 'content-type': 'application/x-nzb' },
      body: file,
    });
    return this.readJson<AddNzbResult>(response);
  }

  async addTorrentFile(file: File, options: AddTorrentOptions): Promise<AddTorrentResult> {
    const query = new URLSearchParams({
      priority: String(options.priority),
      paused: String(options.paused),
    });
    if (options.category?.trim()) query.set('category', options.category.trim());
    const response = await this.request(`/api/v1/jobs?${query.toString()}`, {
      method: 'POST',
      headers: { 'content-type': 'application/x-bittorrent' },
      body: file,
    });
    return this.readJson<AddTorrentResult>(response);
  }

  async addTorrentSource(uri: string, options: AddTorrentOptions): Promise<AddTorrentResult> {
    const response = await this.request('/api/v1/jobs', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        source: { type: uri.trim().startsWith('magnet:') ? 'magnet' : 'torrent_url', uri },
        category: options.category,
        priority: options.priority,
        paused: options.paused,
      }),
    });
    return this.readJson<AddTorrentResult>(response);
  }

  async openEventStream(signal: AbortSignal, lastEventId?: string): Promise<Response> {
    return fetch(`${this.config.baseUrl}/api/v1/events`, {
      method: 'GET',
      headers: this.headers({
        accept: 'text/event-stream',
        ...(lastEventId ? { 'last-event-id': lastEventId } : {}),
      }),
      signal,
    });
  }

  private async json<T>(path: string, init: RequestInit = {}): Promise<T> {
    return this.readJson<T>(await this.request(path, init));
  }

  private async request(path: string, init: RequestInit = {}): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12_000);
    try {
      return await fetch(`${this.config.baseUrl}${path}`, {
        ...init,
        headers: this.headers(init.headers as Record<string, string> | undefined),
        signal: controller.signal,
      });
    } catch (error) {
      if (controller.signal.aborted) {
        throw new ApiError('The Runner server did not answer within 12 seconds.');
      }
      throw new ApiError(
        error instanceof Error ? error.message : 'Could not reach the Runner server.',
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  private async readJson<T>(response: Response): Promise<T> {
    const text = await response.text();
    let body: unknown = {};
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }
    if (!response.ok) {
      const message =
        typeof body === 'object' && body && 'error' in body
          ? String((body as { error: unknown }).error)
          : typeof body === 'string' && body
            ? body
            : `Runner returned HTTP ${response.status}.`;
      throw new ApiError(message, response.status);
    }
    return body as T;
  }

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    const headers: Record<string, string> = {
      accept: 'application/json',
      'cache-control': 'no-store',
      'x-nzbd-client': CLIENT_NAME,
      'x-nzbd-role': 'operator',
      ...extra,
    };
    if (this.config.token) {
      headers.authorization = `Bearer ${this.config.token}`;
    } else if (this.config.password) {
      headers.authorization = `Basic ${utf8Base64(
        `${this.config.username}:${this.config.password}`,
      )}`;
    }
    return headers;
  }
}
