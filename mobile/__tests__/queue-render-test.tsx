import React from 'react';
import { Text, Pressable } from 'react-native';
import * as SecureStore from 'expo-secure-store';
import { DashboardScreen, JobCard, makeStyles } from '../src/screens/DashboardScreen';
import { SeedPolicyEditor } from '../src/components/SeedPolicyEditor';
import { useNzbd } from '../src/hooks/useNzbd';
import { resolveTheme } from '../src/theme';
import { JobSummary } from '../src/api/types';
import { queueSectionKey } from '../src/queueSections';
import fixtures from '../../crates/nzbd-types/fixtures/mobile-queue-parity.json';
const { act, create } = require('react-test-renderer');

jest.mock('../src/hooks/useNzbd');
jest.mock('expo-secure-store', () => ({ isAvailableAsync: jest.fn(async () => false), getItemAsync: jest.fn(), setItemAsync: jest.fn() }));
jest.mock('../src/theme', () => {
  const actual = jest.requireActual('../src/theme');
  return { ...actual, useTheme: () => actual.resolveTheme('dark', 'dark'),
    useDisplayPreferences: () => ({ layout: 'classic', preference: 'dark', palette: 'classic', setLayout: jest.fn(), setPreference: jest.fn(), setPalette: jest.fn() }) };
});
jest.mock('react-native-safe-area-context', () => ({ SafeAreaView: require('react-native').View }));
const theme = resolveTheme('dark', 'dark');
const styles = makeStyles(theme);
let tree: any;
afterEach(async () => { if (tree) await act(async () => tree.unmount()); tree = undefined; });
function text() { return tree.root.findAllByType(Text).map((n: any) => n.props.children).flat(Infinity).join(' '); }
function button(label: string) { return tree.root.findAllByType(Pressable).find((n: any) => n.props.accessibilityLabel === label); }

test.each(['seeding idle', 'stopped seed', 'checking overrides ready', 'failed with stale ready', 'missing files with stale ready'])('card renders %s without NZB timers or health', async (name) => {
  const job = fixtures.find((f) => f.name === name)!.job as JobSummary;
  const sectionKey = queueSectionKey(job);
  await act(async () => { tree = create(<JobCard job={job} index={0} total={1} expanded busy={false}
    onToggle={jest.fn()} onAction={jest.fn()} onDelete={jest.fn()} onPriorityChange={jest.fn()} onSeedOptions={jest.fn()}
    movable={false} sectionKey={sectionKey} sectionLabel="Test section" styles={styles} tone={{ accent: theme.accent, background: theme.panel }} />); });
  expect(text()).not.toContain('Health');
  expect(text()).not.toContain('ETA');
  expect(text()).not.toContain('in test section');
  expect(button('Top')).toBeUndefined();
  if (name === 'failed with stale ready') {
    expect(text()).not.toContain('Files ready');
    expect(button('Pause')).toBeUndefined();
    expect(button('Resume')).toBeUndefined();
    expect(button('Remove')).toBeDefined();
  }
  if (name === 'missing files with stale ready') expect(button('Re-download missing files')).toBeDefined();
});
test('collapsed groups retain heading and live totals while cards leave the tree', async () => {
  const job = fixtures.find((f) => f.name === 'seeding active')!.job as JobSummary;
  let jobs = [job];
  (useNzbd as jest.Mock).mockImplementation(() => ({ snapshot: { jobs, status: null }, connectionState: 'live', busyKey: null }));
  const props = { config: { baseUrl: 'http://test', username: '', password: '', token: '' }, onEditConnection: jest.fn() };
  await act(async () => { tree = create(<DashboardScreen {...props} />); });
  const heading = () => tree.root.findAllByType(Pressable).find((n: any) => n.props.accessibilityLabel?.startsWith('Seeding,'));
  expect(tree.root.findAllByType(JobCard)).toHaveLength(1);
  await act(async () => heading().props.onPress());
  expect(heading().props.accessibilityState.expanded).toBe(false);
  expect(tree.root.findAllByType(JobCard)).toHaveLength(0);
  jobs = [{ ...job, upload_rate_bps: 4096 }];
  await act(async () => tree.update(<DashboardScreen {...props} />));
  expect(heading().props.accessibilityLabel).toContain('4');
  await act(async () => heading().props.onPress());
  expect(tree.root.findAllByType(JobCard)).toHaveLength(1);
});
test('seed options preserves draft on save failure and sends no lifecycle action', async () => {
  const job = fixtures.find((f) => f.name === 'stopped seed')!.job as JobSummary;
  const save = jest.fn().mockRejectedValue(new Error('Server unavailable'));
  await act(async () => { tree = create(<SeedPolicyEditor job={job} busy={false} onSave={save} onClose={jest.fn()} />); });
  await act(async () => button('Stop after download').props.onPress());
  await act(async () => button('Save policy').props.onPress());
  expect(save).toHaveBeenCalledWith({ use_defaults: false, stop_on_complete: true, ratio_limit: null, time_limit_secs: null });
  expect(text()).toContain('Server unavailable');
  expect(button('✓ Stop after download')).toBeDefined();
});
