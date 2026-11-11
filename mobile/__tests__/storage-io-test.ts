import {
  STORAGE_IO_BARS,
  compactRates,
  createStorageIoStore,
  emptyStorageIo,
  noteStorageIo,
  sampleStorageIo,
  storageIoBars,
  storageIoText,
} from '../src/storageIo';
import { StorageIo } from '../src/api/types';

const live = (read: number, write: number): StorageIo => ({ source: 'block', detail: 'sda1', read_bps: read, write_bps: write });

describe('storage I/O history', () => {
  it('advances on the clock, keeps gaps as gaps, and keeps three minutes', () => {
    const rows = [{ path: '/p' }];
    let t = 1_000_000;
    let state = noteStorageIo(emptyStorageIo, [{ path: '/p', io: live(1, 2) }], t);
    state = sampleStorageIo(state, rows, true, (t += 1_000));
    state = sampleStorageIo(state, rows, true, (t += 1_000));
    expect(state.history['storage:/p'].write).toEqual([2, 2]);

    state = noteStorageIo(state, [{ path: '/p', io: { source: 'stale', detail: 'sda1', read_bps: null, write_bps: null } }], t);
    state = sampleStorageIo(state, rows, true, (t += 1_000));
    state = noteStorageIo(state, [{ path: '/p', io: live(3, 4) }], t);
    state = sampleStorageIo(state, rows, false, (t += 3_000));
    state = sampleStorageIo(state, rows, false, (t += 4_000));
    // stale -> gap; 3 s later: two missed seconds, then a fresh poll's 4;
    // 4 s later: three missed seconds, then nothing fresh -> gap.
    expect(state.history['storage:/p'].write).toEqual([2, 2, null, null, null, 4, null, null, null, null]);

    for (let i = 0; i < 400; i++) state = sampleStorageIo(state, rows, true, (t += 1_000));
    expect(state.history['storage:/p'].write).toHaveLength(180);

    state = sampleStorageIo(state, [], true, (t += 1_000));
    expect(state.history).toEqual({});
    expect(state.latest).toEqual({});
  });

  it('seconds the clock missed (app suspended) become gaps, so the axis stays three minutes', () => {
    const rows = [{ path: '/p' }];
    let state = noteStorageIo(emptyStorageIo, [{ path: '/p', io: live(1, 2) }], 0);
    state = sampleStorageIo(state, rows, true, 1_000);
    state = sampleStorageIo(state, rows, true, 5_000);
    expect(state.history['storage:/p'].write).toEqual([2, null, null, null, 2]);
    state = sampleStorageIo(state, rows, true, 10_000_000);
    expect(state.history['storage:/p'].write).toHaveLength(180);
    expect(state.history['storage:/p'].write.filter((v) => v !== null)).toEqual([2]);
  });

  it('the store notifies subscribers on every sample', () => {
    const store = createStorageIoStore();
    const seen: number[] = [];
    const stop = store.subscribe(() => seen.push(store.get().sampledAt));
    store.note([{ path: '/p', io: live(1, 2) }], 0);
    store.sample([{ path: '/p' }], true, 1_000);
    stop();
    store.sample([{ path: '/p' }], true, 2_000);
    expect(seen).toEqual([0, 1_000]);
    expect(store.get().history['storage:/p'].write).toEqual([2, 2]);
  });

  it('a tick row without io does not erase the rate a storage_io frame gave', () => {
    let state = noteStorageIo(emptyStorageIo, [{ path: '/p', io: live(1, 2) }], 0);
    state = noteStorageIo(state, [{ path: '/p' }], 10);
    expect(state.latest['storage:/p']).toEqual(live(1, 2));
  });
});

describe('storage I/O bars', () => {
  it('scales to the busiest second, right-aligns new samples, and leaves gaps empty', () => {
    const { bars, peak } = storageIoBars({ write: [null, 100, 50], read: [null, 10, 25] });
    expect(bars).toHaveLength(STORAGE_IO_BARS);
    expect(peak).toBe(100);
    const last = bars[bars.length - 1];
    expect(last.write).toBe(1);
    expect(last.read).toBe(0.25);
    expect(bars[0]).toEqual({ write: null, read: null });
  });

  it('reports idle as peak 0 and nothing measured as null', () => {
    expect(storageIoBars({ write: [0, 0], read: [0, 0] }).peak).toBe(0);
    expect(storageIoBars({ write: [null], read: [null] }).peak).toBeNull();
    expect(storageIoBars(undefined).peak).toBeNull();
  });
});

describe('storage I/O text', () => {
  it('states rates and says what they cover', () => {
    const text = storageIoText({ source: 'runner', detail: 'fuse.glusterfs', read_bps: 1024, write_bps: 2048 });
    expect(text.quiet).toBe(false);
    expect(text.write).toBe('W 2.00 KiB/s');
    expect(text.read).toBe('R 1.00 KiB/s');
    expect(text.compact).toBe('W 2.00 · R 1.00 KiB/s');
    expect(text.describe).toContain("Runner's own");
    expect(storageIoText({ source: 'nfs', detail: 'qnap:/working', read_bps: 1, write_bps: 1 }).describe)
      .toContain('other clients');
  });

  it('is quiet before a sample and when the sampler stalls', () => {
    expect(storageIoText(undefined).write).toBe('measuring…');
    expect(storageIoText(null).write).toBe('no I/O data');
    expect(storageIoText({ source: 'stale', detail: 'sda1', read_bps: null, write_bps: null }).write).toBe('I/O stalled');
    expect(storageIoText({ source: 'nfs', detail: 'x', read_bps: null, write_bps: null }).write).toBe('measuring…');
  });
});

describe('compact rates', () => {
  it('shares the unit of the larger rate', () => {
    expect(compactRates(94.1 * 1024 * 1024, 1.64 * 1024 * 1024)).toBe('W 94.1 · R 1.64 MiB/s');
    expect(compactRates(0, 110 * 1024 * 1024)).toBe('W 0 · R 110 MiB/s');
    expect(compactRates(0, 0)).toBe('W 0 · R 0 B/s');
  });
});
