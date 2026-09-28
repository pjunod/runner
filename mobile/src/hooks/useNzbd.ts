import { File } from 'expo-file-system';
import { AppState } from 'react-native';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { JobActionResult, runJobAction } from '../api/jobActions';
import { NzbdClient } from '../api/client';
import { SseParser } from '../api/sse';
import {
  AddNzbOptions,
  AddNzbResult,
  AddTorrentOptions,
  AddTorrentResult,
  ConnectionConfig,
  ConnectionState,
  QueueSnapshot,
  SeedPolicy,
} from '../api/types';

interface HookResult {
  snapshot: QueueSnapshot | null;
  connectionState: ConnectionState;
  error: string | null;
  busyKey: string | null;
  lastUpdated: number | null;
  refresh: () => Promise<void>;
  queueAction: (action: 'pause' | 'resume') => Promise<void>;
  jobAction: (
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
  ) => Promise<JobActionResult>;
  setSeedPolicy: (id: number, policy: SeedPolicy & { use_defaults: boolean }) => Promise<void>;
  setJobPriority: (id: number, priority: number) => Promise<void>;
  addNzb: (file: File, options: AddNzbOptions) => Promise<AddNzbResult>;
  addTorrentFile: (file: File, options: AddTorrentOptions) => Promise<AddTorrentResult>;
  addTorrentSource: (uri: string, options: AddTorrentOptions) => Promise<AddTorrentResult>;
}

export function useNzbd(config: ConnectionConfig): HookResult {
  const client = useMemo(() => new NzbdClient(config), [config]);
  const activeClient = useRef<NzbdClient | null>(client);
  activeClient.current = client;
  useEffect(() => { activeClient.current = client; return () => { activeClient.current = null; }; }, [client]);
  const [snapshot, setSnapshot] = useState<QueueSnapshot | null>(null);
  const [connectionState, setConnectionState] = useState<ConnectionState>('connecting');
  const [error, setError] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const refreshPromise = useRef<Promise<void> | null>(null);
  const lastFrameAt = useRef(0);

  const refresh = useCallback(async () => {
    if (activeClient.current !== client) return;
    if (refreshPromise.current) return refreshPromise.current;
    const request = client
      .getSnapshot()
      .then((next) => {
        if (activeClient.current !== client) return;
        setSnapshot(next);
        setLastUpdated(Date.now());
        setError(null);
        if (Date.now() - lastFrameAt.current > 7_000) setConnectionState('polling');
      })
      .catch((cause) => {
        if (activeClient.current !== client) throw cause;
        const message = cause instanceof Error ? cause.message : 'Could not refresh Runner.';
        setError(message);
        setConnectionState('offline');
        throw cause;
      })
      .finally(() => {
        refreshPromise.current = null;
      });
    refreshPromise.current = request;
    return request;
  }, [client]);

  useEffect(() => {
    let disposed = false;
    let lastEventId: string | undefined;
    const controller = new AbortController();

    const acceptFrame = (event: string, data: string, id?: string) => {
      if (disposed) return;
      if (id) lastEventId = id;
      lastFrameAt.current = Date.now();
      setConnectionState('live');
      setError(null);
      if (event === 'tick') {
        try {
          setSnapshot(JSON.parse(data) as QueueSnapshot);
          setLastUpdated(Date.now());
        } catch {
          setError('Runner sent a queue update the app could not read.');
        }
      } else if (event === 'reset' || event === 'lagged') {
        void refresh().catch(() => undefined);
      }
    };

    const stream = async () => {
      let backoff = 1_000;
      await refresh().catch(() => undefined);
      while (!disposed) {
        try {
          if (lastFrameAt.current > 0) setConnectionState('reconnecting');
          const response = await client.openEventStream(controller.signal, lastEventId);
          if (!response.ok) throw new Error(`Event stream returned HTTP ${response.status}.`);
          if (!response.body) throw new Error('Event stream has no response body.');
          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          const parser = new SseParser();
          backoff = 1_000;
          while (!disposed) {
            const { done, value } = await reader.read();
            if (done) break;
            for (const frame of parser.feed(decoder.decode(value, { stream: true }))) {
              acceptFrame(frame.event, frame.data, frame.id);
            }
          }
          if (!disposed) throw new Error('Live queue stream ended.');
        } catch (cause) {
          if (disposed || controller.signal.aborted) break;
          setConnectionState(lastFrameAt.current > 0 ? 'reconnecting' : 'offline');
          setError(cause instanceof Error ? cause.message : 'Live queue stream failed.');
          await new Promise((resolve) => setTimeout(resolve, backoff));
          backoff = Math.min(backoff * 2, 15_000);
        }
      }
    };

    void stream();
    const poll = setInterval(() => {
      if (Date.now() - lastFrameAt.current > 7_000) {
        void refresh().catch(() => undefined);
      }
    }, 5_000);
    const appState = AppState.addEventListener('change', (state) => {
      if (state === 'active') void refresh().catch(() => undefined);
    });

    return () => {
      disposed = true;
      controller.abort();
      clearInterval(poll);
      appState.remove();
    };
  }, [client, refresh]);

  const mutate = useCallback(
    async <T,>(key: string, operation: () => Promise<T>): Promise<T> => {
      setBusyKey(key);
      setError(null);
      try {
        const result = await operation();
        await refresh().catch(() => undefined);
        return result;
      } catch (cause) {
        if (activeClient.current === client) setError(cause instanceof Error ? cause.message : 'Runner rejected the action.');
        throw cause;
      } finally {
        if (activeClient.current === client) setBusyKey(null);
      }
    },
    [refresh],
  );

  return {
    snapshot,
    connectionState,
    error,
    busyKey,
    lastUpdated,
    refresh,
    queueAction: (action) => mutate(`queue:${action}`, () => client.queueAction(action)),
    jobAction: (id, action) =>
      mutate(`job:${id}:${action}`, () => runJobAction(client,
        snapshot?.jobs.find((job) => job.id === id), id, action, (jobs) => {
          if (activeClient.current === client) setSnapshot((current) => current ? { ...current, jobs } : current);
        })),
    setSeedPolicy: (id, policy) => mutate(`job:${id}:seed-policy`, () => client.setSeedPolicy(id, policy)),
    setJobPriority: (id, priority) =>
      mutate(`job:${id}:priority`, () => client.setJobPriority(id, priority)),
    addNzb: (file, options) => mutate('add', () => client.addNzb(file, options)),
    addTorrentFile: (file, options) =>
      mutate('add', () => client.addTorrentFile(file, options)),
    addTorrentSource: (uri, options) =>
      mutate('add', () => client.addTorrentSource(uri, options)),
  };
}
