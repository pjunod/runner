import React from 'react';
import { Text } from 'react-native';
import { FailureKeepControl } from '../src/components/FailureKeepControl';
import { NzbdClient } from '../src/api/client';
const { act, create } = require('react-test-renderer');
jest.mock('../src/theme', () => {
  const actual = jest.requireActual('../src/theme');
  return { ...actual, useTheme: () => actual.resolveTheme('dark', 'dark') };
});
let tree: any;
afterEach(async () => { if (tree) await act(async () => tree.unmount()); tree = undefined; });
const text = () => tree.root.findAllByType(Text).map((n: any) => n.props.children).flat(Infinity).join(' ');
const button = (label: string) => tree.root.findAll((n: any) => n.props.accessibilityLabel === label && typeof n.props.onPress === 'function')[0];
test('retired source uses current GET preference, confirms PUT and preserves actionable failure', async () => {
  let keep = false;
  const client = { getFailureKeep: jest.fn(async () => ({ available: true, keep })),
    setFailureKeep: jest.fn(async (_id, value) => { keep = value; }) } as unknown as NzbdClient;
  await act(async () => { tree = create(<FailureKeepControl client={client} jobId={42} />); });
  expect(button('Keep on failure: off')).toBeDefined();
  await act(async () => button('Keep on failure: off').props.onPress());
  expect(client.setFailureKeep).toHaveBeenCalledWith(42, true);
  expect(button('Keep on failure: on')).toBeDefined();
  expect(text()).toContain('on for this retained source');
  (client.setFailureKeep as jest.Mock).mockRejectedValueOnce(new Error('Source changed; refresh and review'));
  await act(async () => button('Keep on failure: on').props.onPress());
  expect(button('Keep on failure: on')).toBeDefined();
  expect(text()).toContain('Source changed; refresh and review');
});
test('missing or unconfirmed source cannot offer a toggle', async () => {
  const client = { getFailureKeep: jest.fn(async () => ({ available: false, keep: false })) } as unknown as NzbdClient;
  await act(async () => { tree = create(<FailureKeepControl client={client} jobId={43} />); });
  expect(text()).toContain('No retained source');
  expect(button('Keep on failure: off')).toBeUndefined();
  await act(async () => tree.unmount()); tree = undefined;
  (client.getFailureKeep as jest.Mock).mockRejectedValueOnce(new Error('Storage unavailable'));
  await act(async () => { tree = create(<FailureKeepControl client={client} jobId={44} />); });
  expect(text()).toContain('Storage unavailable');
  expect(text()).toContain('Refresh source retention');
  expect(button('Keep on failure: off')).toBeUndefined();
});

test('changing identity during PUT resets pending state and ignores old completion', async () => {
  let finishOld!: () => void;
  const old = { getFailureKeep: jest.fn(async () => ({ available: true, keep: false })),
    setFailureKeep: jest.fn(() => new Promise<void>(resolve => { finishOld = resolve; })) } as unknown as NzbdClient;
  let finishNew!: () => void;
  const current = { getFailureKeep: jest.fn(async () => ({ available: true, keep: true })),
    setFailureKeep: jest.fn(() => new Promise<void>(resolve => { finishNew = resolve; })) } as unknown as NzbdClient;
  await act(async () => { tree = create(<FailureKeepControl client={old} jobId={42} />); });
  await act(async () => button('Keep on failure: off').props.onPress());
  await act(async () => tree.update(<FailureKeepControl client={current} jobId={43} />));
  expect(button('Keep on failure: on').props.accessibilityState.busy).toBe(false);
  await act(async () => button('Keep on failure: on').props.onPress());
  expect(current.setFailureKeep).toHaveBeenCalledWith(43, false);
  expect(button('Keep on failure: on').props.accessibilityState.busy).toBe(true);
  await act(async () => finishOld());
  expect(button('Keep on failure: on').props.accessibilityState.busy).toBe(true);
  expect(text()).not.toContain('on for this retained source');
  await act(async () => finishNew());
  expect(button('Keep on failure: on').props.accessibilityState.busy).toBe(false);
  expect(current.getFailureKeep).toHaveBeenCalledTimes(2);
});

test('pending owner application survives GET/remount and never confirms restored protection', async () => {
  let keep = false;
  const client = { getFailureKeep: jest.fn(async () => ({ available: true, keep, applied: !keep, pendingOwners: keep ? ['worker-a'] : [] })),
    setFailureKeep: jest.fn(async (_id, desired) => { keep = desired; return { applied: false }; }) } as unknown as NzbdClient;
  await act(async () => { tree = create(<FailureKeepControl client={client} jobId={42} />); });
  await act(async () => button('Keep on failure: off').props.onPress());
  expect(button('Requested Keep on failure: on')).toBeDefined();
  expect(text()).toContain('Waiting for source owners (1)');
  expect(text()).not.toContain('on for this retained source');
  await act(async () => tree.unmount()); tree = undefined;
  await act(async () => { tree = create(<FailureKeepControl client={client} jobId={42} />); });
  expect(button('Requested Keep on failure: on')).toBeDefined();
  expect(text()).toContain('Protection is not confirmed yet');
  (client.getFailureKeep as jest.Mock).mockResolvedValue({ available: false, keep: true, applied: false });
  await act(async () => tree.update(<FailureKeepControl client={client} jobId={43} />));
  expect(text()).toContain('No retained source');
  expect(text()).not.toContain('Protection is not confirmed yet');
  expect(button('Requested Keep on failure: on')).toBeUndefined();
});
