import React from 'react';
import { Text, TextInput } from 'react-native';
import { fetch } from 'expo/fetch';

import { ConnectionScreen } from '../src/screens/ConnectionScreen';
import { toDiscoveredNzbd } from '../src/discovery/nzbdService';
import { useNzbdDiscovery } from '../src/discovery/useNzbdDiscovery';

const { act, create } = require('react-test-renderer');

jest.mock('expo/fetch', () => ({ fetch: jest.fn() }));
jest.mock('../src/discovery/useNzbdDiscovery', () => ({ useNzbdDiscovery: jest.fn() }));
jest.mock('../src/components/ThemeSwitcher', () => ({ ThemeSwitcher: () => null }));
jest.mock('../src/theme', () => {
  const actual = jest.requireActual('../src/theme');
  return { ...actual, useTheme: () => actual.resolveTheme('dark', 'dark') };
});
jest.mock('react-native-safe-area-context', () => ({ SafeAreaView: require('react-native').View }));

const service = toDiscoveredNzbd({
  name: 'Runner on NAS', type: '_nzbd._tcp.', domain: 'local.',
  hostName: 'nas.local.', addresses: ['fd12::8%wlan0', '192.168.1.42'],
  port: 6789, txt: { auth: 'none' },
})!;
const mockedFetch = fetch as jest.MockedFunction<typeof fetch>;
let tree: any;
const button = (label: string) => tree.root.findAll((node: any) =>
  node.props.accessibilityLabel === label && typeof node.props.onPress === 'function')[0];
const text = () => tree.root.findAllByType(Text).map((node: any) => node.props.children).flat(Infinity).join(' ');

beforeEach(() => {
  mockedFetch.mockReset();
  mockedFetch.mockResolvedValue({
    ok: true, status: 200, text: async () => JSON.stringify({ version: 'test' }),
  } as Awaited<ReturnType<typeof fetch>>);
  (useNzbdDiscovery as jest.Mock).mockReturnValue({
    services: [service], scanning: false, noResults: false, error: null, scanAgain: jest.fn(),
  });
});
afterEach(async () => { if (tree) await act(async () => tree.unmount()); tree = undefined; });

test.each(service.addresses)('tests and saves the selected $kind address', async (address) => {
  const onConnect = jest.fn(async () => undefined);
  await act(async () => { tree = create(<ConnectionScreen onConnect={onConnect} />); });
  await act(async () => button(`Use ${service.name} at ${service.baseUrl}`).props.onPress());
  const label = `Use ${address.kind} address ${address.baseUrl}`;
  await act(async () => button(label).props.onPress());
  expect(button(label).props.accessibilityState.checked).toBe(true);
  expect(tree.root.findAllByType(TextInput).find((node: any) => node.props.accessibilityLabel === 'Server address').props.value).toBe(address.baseUrl);
  await act(async () => button('Test connection').props.onPress());
  expect(mockedFetch.mock.calls[0][0]).toBe(`${address.baseUrl}/api/v1/status`);
  expect(text()).toContain(`Connection works — Runner test answered at ${address.baseUrl}`);
  await act(async () => button('Connect').props.onPress());
  expect(onConnect).toHaveBeenCalledWith({ baseUrl: address.baseUrl, username: '', password: '', token: '' });
});

test('can switch to IPv4 after IPv6 is unreachable without saving the failed address', async () => {
  const onConnect = jest.fn(async () => undefined);
  await act(async () => { tree = create(<ConnectionScreen onConnect={onConnect} />); });
  await act(async () => button(`Use ${service.name} at ${service.baseUrl}`).props.onPress());
  await act(async () => button('Use IPv6 address http://[fd12::8]:6789').props.onPress());
  mockedFetch.mockRejectedValueOnce(new Error('Network request failed'));
  await act(async () => button('Connect').props.onPress());
  expect(text()).toContain('Network request failed');
  expect(onConnect).not.toHaveBeenCalled();
  await act(async () => button('Use IPv4 address http://192.168.1.42:6789').props.onPress());
  expect(text()).not.toContain('Network request failed');
  await act(async () => button('Connect').props.onPress());
  expect(onConnect).toHaveBeenCalledWith(expect.objectContaining({ baseUrl: 'http://192.168.1.42:6789' }));
});
