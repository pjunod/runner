import React from 'react';
import * as SecureStore from 'expo-secure-store';
import { loadCollapsed, parseCollapsed, saveCollapsed, useCollapsedSections } from '../src/storage/queuePreferences';
const { act, create } = require('react-test-renderer');
jest.mock('expo-secure-store', () => ({ isAvailableAsync: jest.fn(async () => true), getItemAsync: jest.fn(), setItemAsync: jest.fn(async () => undefined) }));
const store = SecureStore as jest.Mocked<typeof SecureStore>;
beforeEach(() => { jest.clearAllMocks(); store.isAvailableAsync.mockResolvedValue(true); store.getItemAsync.mockResolvedValue(null); });
test('only supported keys survive invalid and old preferences', () => {
  expect(parseCollapsed('{')).toEqual([]);
  expect(parseCollapsed('{}')).toEqual([]);
  expect(parseCollapsed('["downloading","seeding","seeding","completed"]')).toEqual(['seeding', 'completed']);
});
test('unavailable or failing storage falls back without rejecting', async () => {
  store.isAvailableAsync.mockRejectedValue(new Error('locked'));
  expect(await loadCollapsed()).toEqual([]);
  await expect(saveCollapsed(['waiting'])).resolves.toBeUndefined();
});
test('serializes rapid writes in user order', async () => {
  let release!: () => void;
  store.setItemAsync.mockImplementationOnce(() => new Promise<void>((resolve) => { release = resolve; }));
  const first = saveCollapsed(['seeding']);
  const last = saveCollapsed(['waiting']);
  for (let i = 0; i < 6; i++) await Promise.resolve();
  expect(store.setItemAsync).toHaveBeenCalledTimes(1);
  release(); await first; await last;
  expect(store.setItemAsync.mock.calls.map((c) => JSON.parse(c[1]))).toEqual([['seeding'], ['waiting']]);
});
test('early user toggle wins over delayed hydration and persists across remount', async () => {
  let release!: (value: string) => void;
  store.getItemAsync.mockImplementationOnce(() => new Promise((resolve) => { release = resolve; }));
  let hook!: ReturnType<typeof useCollapsedSections>;
  function Harness() { hook = useCollapsedSections(); return null; }
  let tree: any;
  await act(async () => { tree = create(<Harness />); });
  await act(async () => { hook.toggle('waiting'); release('["seeding"]'); });
  expect(hook.collapsed).toEqual(['waiting']);
  await act(async () => tree.unmount());
  store.getItemAsync.mockResolvedValue('["waiting"]');
  await act(async () => { tree = create(<Harness />); });
  expect(hook.collapsed).toEqual(['waiting']);
  await act(async () => tree.unmount());
});
