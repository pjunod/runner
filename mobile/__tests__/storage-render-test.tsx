import React from 'react';
import { Text } from 'react-native';
import { DashboardScreen } from '../src/screens/DashboardScreen';
import { useNzbd } from '../src/hooks/useNzbd';
import { createStorageIoStore, emptyStorageIo, noteStorageIo, sampleStorageIo } from '../src/storageIo';
const { act, create } = require('react-test-renderer');

jest.mock('expo/fetch', () => ({ fetch: jest.fn() }));
jest.mock('../src/hooks/useNzbd');
jest.mock('expo-secure-store', () => ({ isAvailableAsync: jest.fn(async () => false), getItemAsync: jest.fn(), setItemAsync: jest.fn() }));
jest.mock('../src/theme', () => {
  const actual = jest.requireActual('../src/theme');
  return { ...actual, useTheme: () => actual.resolveTheme('dark', 'dark'),
    useDisplayPreferences: () => ({ layout: 'classic', preference: 'dark', palette: 'classic', setLayout: jest.fn(), setPreference: jest.fn(), setPalette: jest.fn() }) };
});
jest.mock('react-native-safe-area-context', () => ({ SafeAreaView: require('react-native').View }));

const props = { config: { baseUrl: 'http://test', username: '', password: '', token: '' }, onEditConnection: jest.fn() };
let tree: any;
afterEach(async () => { if (tree) await act(async () => tree.unmount()); tree = undefined; });
const text = () => tree.root.findAllByType(Text).map((n: any) => n.props.children).flat(Infinity).join(' ');
const { StyleSheet } = require('react-native');
// Host nodes only: a composite and the host it renders share props.
const labelled = (start: string) => tree.root.findAll((n: any) => typeof n.type === 'string'
  && typeof n.props.accessibilityLabel === 'string' && n.props.accessibilityLabel.startsWith(start));

// nuc3's layout: a NFS completed folder and a GlusterFS processing volume.
const storage = [
  { name: 'completed', label: 'downloads', path: '/working/monarr/completed', available_bytes: 25, total_bytes: 100, current: true },
  { name: 'processing', label: 'working · torrent payloads · state', path: '/processing/', available_bytes: 50, total_bytes: 100, current: true },
];
const MiB = 1024 * 1024;

test('both storage rows graph their I/O and say what it covers; both pause lanes are there', async () => {
  let io = noteStorageIo(emptyStorageIo, [
    { path: storage[0].path, io: { source: 'nfs', detail: 'qnap:/working', read_bps: 80 * MiB, write_bps: 0 } },
    { path: storage[1].path, io: { source: 'runner', detail: 'fuse.glusterfs', read_bps: 3 * MiB, write_bps: 90 * MiB } },
  ], 1_000);
  for (let t = 2_000; t < 12_000; t += 1_000) io = sampleStorageIo(io, storage, true, t);
  const status = { download_rate_bps: 100, remaining_bytes: 1000, jobs_downloading: 0, jobs_queued: 0, storage,
    blocked_servers: [], servers: [], quota_reached: false, disk_low: false, download_paused: false, post_paused: true };
  (useNzbd as jest.Mock).mockImplementation(() => ({ snapshot: { jobs: [], status }, storageIo: createStorageIoStore(io), connectionState: 'live', busyKey: null, queueAction: jest.fn() }));
  await act(async () => { tree = create(<DashboardScreen {...props} />); });

  const shown = text();
  expect(shown).toContain('completed');
  expect(shown).toContain('processing');
  expect(shown).not.toContain('working · torrent payloads');
  expect(shown).toContain('W 90.0 MiB/s');
  expect(shown).toContain('R 80.0 MiB/s');
  const graphs = labelled('I/O, last 3 minutes');
  expect(graphs).toHaveLength(2);
  expect(graphs[0].props.accessibilityLabel).toContain('other clients');
  expect(graphs[1].props.accessibilityLabel).toContain("Runner's own");
  // Ten measured seconds draw write bars (or a read mark) in the last few
  // columns of each graph; the columns before them are empty, not zero.
  for (const graph of graphs) {
    const columns = graph.findAll((n: any) => typeof n.type === 'string'
      && StyleSheet.flatten(n.props.style)?.justifyContent === 'flex-end'
      && StyleSheet.flatten(n.props.style)?.height === '100%');
    expect(columns).toHaveLength(60);
    const drawn = columns.filter((column: any) => column.findAll((n: any) => typeof n.type === 'string' && n !== column).length > 0);
    expect(drawn.length).toBeGreaterThanOrEqual(3);
    expect(drawn.length).toBeLessThanOrEqual(5);
    expect(columns.indexOf(drawn[drawn.length - 1])).toBe(59);
  }

  expect(labelled('Pause downloads').length).toBeGreaterThan(0);
  expect(labelled('Resume post-processing').length).toBeGreaterThan(0);
});

test('a row with no sample yet says measuring, not missing', async () => {
  const status = { download_rate_bps: 0, remaining_bytes: 0, jobs_downloading: 0, jobs_queued: 0, storage: [storage[0]],
    blocked_servers: [], servers: [], quota_reached: false, disk_low: false };
  (useNzbd as jest.Mock).mockImplementation(() => ({ snapshot: { jobs: [], status }, storageIo: createStorageIoStore(), connectionState: 'live', busyKey: null, queueAction: jest.fn() }));
  await act(async () => { tree = create(<DashboardScreen {...props} />); });
  expect(text()).toContain('measuring…');
});
