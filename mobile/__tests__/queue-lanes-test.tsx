import React from 'react';
import { Text } from 'react-native';
import { fetch } from 'expo/fetch';
import { NzbdClient } from '../src/api/client';
import { DashboardScreen } from '../src/screens/DashboardScreen';
import { useNzbd } from '../src/hooks/useNzbd';
import { lanePaused, laneToggleAction, laneToggleLabel, pausedIndicator } from '../src/queueLanes';
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

const mockedFetch = fetch as jest.MockedFunction<typeof fetch>;
const props = { config: { baseUrl: 'http://test', username: '', password: '', token: '' }, onEditConnection: jest.fn() };
let tree: any;
afterEach(async () => { if (tree) await act(async () => tree.unmount()); tree = undefined; });
const text = () => tree.root.findAllByType(Text).map((n: any) => n.props.children).flat(Infinity).join(' ');
const button = (label: string) => tree.root.findAll((n: any) => n.props.accessibilityLabel === label && typeof n.props.onPress === 'function')[0];

async function render(status: Record<string, unknown>, queueAction = jest.fn(async () => undefined)) {
  const full = { download_rate_bps: 100, remaining_bytes: 1000, jobs_downloading: 0, jobs_queued: 0, storage: [], blocked_servers: [], servers: [], quota_reached: false, disk_low: false, ...status };
  (useNzbd as jest.Mock).mockImplementation(() => ({ snapshot: { jobs: [], status: full }, connectionState: 'live', busyKey: null, queueAction }));
  await act(async () => { tree = create(<DashboardScreen {...props} />); });
  return queueAction;
}

describe('lane helpers', () => {
  it('treats a missing post_paused as false and keeps lanes independent', () => {
    expect(lanePaused({ download_paused: true }, 'post')).toBe(false);
    expect(lanePaused({ download_paused: true }, 'downloads')).toBe(true);
    expect(lanePaused({ download_paused: false, post_paused: true }, 'downloads')).toBe(false);
    expect(lanePaused({ download_paused: false, post_paused: true }, 'post')).toBe(true);
  });
  it('picks the endpoint action per lane and state', () => {
    expect(laneToggleAction('downloads', false)).toBe('pause');
    expect(laneToggleAction('downloads', true)).toBe('resume');
    expect(laneToggleAction('post', false)).toBe('pause-post');
    expect(laneToggleAction('post', true)).toBe('resume-post');
    expect(laneToggleLabel('post', true)).toBe('Resume post-processing');
    expect(laneToggleLabel('downloads', false)).toBe('Pause downloads');
  });
  it.each([
    [{ download_paused: false }, null],
    [{ download_paused: false, post_paused: false }, null],
    [{ download_paused: true }, 'downloads paused'],
    [{ download_paused: false, post_paused: true }, 'post-processing paused'],
    [{ download_paused: true, post_paused: true }, 'downloads paused · post-processing paused'],
  ])('indicator for %j', (status, expected) => expect(pausedIndicator(status)).toBe(expected));
});

describe('client endpoints', () => {
  beforeEach(() => { mockedFetch.mockReset(); mockedFetch.mockResolvedValue({ ok: true, status: 200, text: async () => '{}' } as any); });
  it.each(['pause', 'resume', 'pause-post', 'resume-post'] as const)('POSTs /queue/actions/%s', async (action) => {
    await new NzbdClient(props.config).queueAction(action);
    expect(mockedFetch.mock.calls[0][0]).toBe(`http://test/api/v1/queue/actions/${action}`);
    expect((mockedFetch.mock.calls[0][1] as any).method).toBe('POST');
  });
});

describe('dashboard lane toggles', () => {
  it('both lanes running: pause buttons send the right action each', async () => {
    const queueAction = await render({ download_paused: false });
    expect(text()).not.toContain('paused');
    await act(async () => button('Pause post-processing').props.onPress());
    expect(queueAction).toHaveBeenLastCalledWith('pause-post');
    await act(async () => button('Pause downloads').props.onPress());
    expect(queueAction).toHaveBeenLastCalledWith('pause');
  });
  it('only post-processing paused: downloads untouched, time left not paused', async () => {
    const queueAction = await render({ download_paused: false, post_paused: true });
    expect(text()).toContain('post-processing paused');
    expect(text()).not.toContain('downloads paused');
    expect(button('Pause downloads')).toBeDefined();
    expect(text()).not.toMatch(/Time left\s+paused/);
    await act(async () => button('Resume post-processing').props.onPress());
    expect(queueAction).toHaveBeenLastCalledWith('resume-post');
  });
  it('only downloads paused, post_paused missing: time left says paused', async () => {
    const queueAction = await render({ download_paused: true });
    expect(text()).toContain('downloads paused');
    expect(text()).not.toContain('post-processing paused');
    expect(text()).toMatch(/Time left\s+paused/);
    expect(button('Pause post-processing')).toBeDefined();
    await act(async () => button('Resume downloads').props.onPress());
    expect(queueAction).toHaveBeenLastCalledWith('resume');
  });
  it('both paused shows both indicators', async () => {
    await render({ download_paused: true, post_paused: true });
    expect(text()).toContain('downloads paused · post-processing paused');
  });
});
