import { formatBytes } from './api/format';
import { StorageIo, StoragePath } from './api/types';

/**
 * Per-filesystem read/write throughput for the Storage block, matching the
 * web dashboard: three minutes at one sample a second. The latest rates come
 * from the stream's `storage_io` frame (sent only when they change) or a
 * status poll; a one-second clock turns them into samples, so an unchanged
 * rate still advances the graph. A second with nothing trustworthy (no
 * sample yet, a stalled sampler, a dead stream with no recent poll) is a
 * gap, never a zero: the graph does not claim an idle disk it did not see.
 */
export const STORAGE_IO_WINDOW = 180;
export const STORAGE_IO_BARS = 60;

export interface StorageIoSeries {
  read: (number | null)[];
  write: (number | null)[];
}

export interface StorageIoState {
  /** Latest throughput per row key; null means the server cannot sample it. */
  latest: Record<string, StorageIo | null | undefined>;
  /** When `latest` was last fed, for judging freshness without a stream. */
  notedAt: number;
  /** When the clock last took a sample; seconds it missed become gaps. */
  sampledAt: number;
  history: Record<string, StorageIoSeries>;
}

export const emptyStorageIo: StorageIoState = { latest: {}, notedAt: 0, sampledAt: 0, history: {} };

export function storageKey(storage: Pick<StoragePath, 'path'>, index: number): string {
  return `storage:${storage.path || index}`;
}

/** Record the rates a status poll or `storage_io` frame carried. */
export function noteStorageIo(
  state: StorageIoState,
  rows: readonly Pick<StoragePath, 'path' | 'io'>[] | undefined,
  now: number,
): StorageIoState {
  const latest = { ...state.latest };
  (rows ?? []).forEach((row, index) => {
    if (Object.prototype.hasOwnProperty.call(row, 'io')) latest[storageKey(row, index)] = row.io;
  });
  return { ...state, latest, notedAt: now };
}

function measurable(io: StorageIo | null | undefined): io is StorageIo & { read_bps: number; write_bps: number } {
  return !!io && io.read_bps != null && io.write_bps != null
    && io.source !== 'stale' && io.source !== 'unsupported';
}

/** One sample per row. `live` says the stream is up, so an unrepeated value is still current. */
export function sampleStorageIo(
  state: StorageIoState,
  rows: readonly Pick<StoragePath, 'path'>[],
  live: boolean,
  now: number,
): StorageIoState {
  const fresh = live || now - state.notedAt < 6_500;
  // A suspended app or a stalled JS thread skips ticks; those seconds were
  // not measured, so they are gaps and the time axis stays three minutes.
  const missed = state.sampledAt > 0
    ? Math.min(STORAGE_IO_WINDOW, Math.max(0, Math.floor((now - state.sampledAt) / 1_000) - 1))
    : 0;
  const gaps: null[] = new Array(missed).fill(null);
  const history: Record<string, StorageIoSeries> = {};
  const latest: StorageIoState['latest'] = {};
  rows.forEach((row, index) => {
    const key = storageKey(row, index);
    const io = state.latest[key];
    if (key in state.latest) latest[key] = io;
    const ok = fresh && measurable(io);
    const previous = state.history[key] ?? { read: [], write: [] };
    history[key] = {
      read: [...previous.read, ...gaps, ok ? Math.max(0, io.read_bps) : null].slice(-STORAGE_IO_WINDOW),
      write: [...previous.write, ...gaps, ok ? Math.max(0, io.write_bps) : null].slice(-STORAGE_IO_WINDOW),
    };
  });
  return { latest, notedAt: state.notedAt, sampledAt: now, history };
}

/**
 * The history lives outside React state so its once-a-second update
 * re-renders only the Storage block that subscribes to it, not the whole
 * dashboard and queue list.
 */
export interface StorageIoStore {
  get: () => StorageIoState;
  subscribe: (listener: () => void) => () => void;
  note: (rows: readonly Pick<StoragePath, 'path' | 'io'>[] | undefined, now: number) => void;
  sample: (rows: readonly Pick<StoragePath, 'path'>[], live: boolean, now: number) => void;
}

export function createStorageIoStore(initial: StorageIoState = emptyStorageIo): StorageIoStore {
  let state = initial;
  const listeners = new Set<() => void>();
  const set = (next: StorageIoState) => {
    state = next;
    listeners.forEach((listener) => listener());
  };
  return {
    get: () => state,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
    note: (rows, now) => set(noteStorageIo(state, rows, now)),
    sample: (rows, live, now) => set(sampleStorageIo(state, rows, live, now)),
  };
}

export interface StorageIoBar {
  /** 0..1 of the graph's height, or null for an unmeasured stretch. */
  write: number | null;
  read: number | null;
}

/**
 * The series as STORAGE_IO_BARS columns, right-aligned so the newest sample
 * sits at the right edge. Each column shows its busiest second, and the
 * scale is the busiest second in the window; `peak` is 0 when every
 * measured second was idle and null when nothing was measured.
 */
export function storageIoBars(series: StorageIoSeries | undefined): { bars: StorageIoBar[]; peak: number | null } {
  const per = STORAGE_IO_WINDOW / STORAGE_IO_BARS;
  const write = series?.write ?? [];
  const read = series?.read ?? [];
  const offset = STORAGE_IO_WINDOW - write.length;
  let peak: number | null = null;
  const raw: { write: number | null; read: number | null }[] = [];
  for (let bar = 0; bar < STORAGE_IO_BARS; bar++) {
    let w: number | null = null;
    let r: number | null = null;
    for (let slot = bar * per; slot < (bar + 1) * per; slot++) {
      const i = slot - offset;
      if (i < 0) continue;
      const wv = write[i];
      const rv = read[i];
      if (wv != null) w = Math.max(w ?? 0, wv);
      if (rv != null) r = Math.max(r ?? 0, rv);
    }
    if (w != null || r != null) peak = Math.max(peak ?? 0, w ?? 0, r ?? 0);
    raw.push({ write: w, read: r });
  }
  const scale = Math.max(peak ?? 0, 1);
  return {
    peak,
    bars: raw.map((b) => ({
      write: b.write == null ? null : b.write / scale,
      read: b.read == null ? null : b.read / scale,
    })),
  };
}

export interface StorageIoText {
  quiet: boolean;
  write: string;
  read: string;
  /** What the numbers cover, for the accessibility label. */
  describe: string;
}

/** What the I/O half of a row says. The server reports where its numbers come from. */
export function storageIoText(io: StorageIo | null | undefined): StorageIoText {
  const quiet = (text: string, describe: string): StorageIoText => ({ quiet: true, write: text, read: '', describe });
  if (io === undefined) return quiet('measuring…', 'Waiting for the first I/O sample.');
  if (io === null) return quiet('no I/O data', 'Runner cannot identify this filesystem, so it cannot sample its I/O.');
  // 0.8.0 daemons report "unsupported" where 0.8.1 graphs Runner's own I/O.
  if (io.source === 'unsupported') return quiet('I/O n/a', `${io.detail} keeps no I/O counters.`);
  if (io.source === 'stale') return quiet('I/O stalled', 'The I/O sampler has not reported for more than 5 seconds.');
  const describe = io.source === 'nfs'
    ? `This device's traffic to ${io.detail}; other clients of that server are not included.`
    : io.source === 'runner'
      ? `Runner's own reads and writes; ${io.detail} keeps no I/O counters, so other programs are not included.`
      : `Device-wide reads and writes on ${io.detail}, including other programs.`;
  if (io.read_bps == null || io.write_bps == null) return quiet('measuring…', describe);
  return { quiet: false, write: `W ${formatBytes(io.write_bps, '/s')}`, read: `R ${formatBytes(io.read_bps, '/s')}`, describe };
}
