import React from 'react';
import { Text } from 'react-native';
import { DeliveryLifecyclePanel } from '../src/components/DeliveryLifecyclePanel';
import { deliveryLanes, deliveryReference } from '../src/deliveryTiming';
import { NzbdClient } from '../src/api/client';
import { DeliveryTrace } from '../src/api/types';
const { act, create } = require('react-test-renderer');
jest.mock('../src/theme', () => { const actual = jest.requireActual('../src/theme'); return { ...actual, useTheme: () => actual.resolveTheme('dark', 'dark') }; });
let tree: any;
afterEach(async () => { if (tree) await act(async () => tree.unmount()); tree = undefined; });
const text = () => tree.root.findAllByType(Text).map((n: any) => n.props.children).flat(Infinity).join(' ');
const button = (label: string) => tree.root.findAll((n: any) => n.props.accessibilityLabel === label && typeof n.props.onPress === 'function')[0];
const interval = (stage: string, start: number, end: number, nodeId = 'worker-a', clockDomain = 'boot-a') => ({ stage, nodeId, clockDomain, state: 'completed', startUnixMs: 100000, startMonotonicMs: start, endMonotonicMs: end, durationMs: end - start });
const receipt: DeliveryTrace = { deliveryId: 'receipt-a', status: 'accepted', readyAt: 1, acceptedAt: 2,
  lifecycleTiming: { version: 1, intervals: [interval('download', 0, 2000), interval('post_processing', 1000, 3000), interval('queue', 0, 1000, 'worker-b'), { ...interval('url_fetch', 0, 9000), state: 'interrupted' }, { stage: 'queue', nodeId: 'worker-a', clockDomain: 'legacy', state: 'completed', startUnixMs: 1, endUnixMs: 99999, durationMs: 99998 }] },
  sourceCleanup: { status: 'retry', attempts: 2 }, sourceCopies: [{ id: 'long-copy-identity', ownerNode: 'worker-a', status: 'retry', attempts: 2, error: 'Mount unavailable' }] };
test('measured subtotals union only same-session monotonic endpoints', () => {
  const lanes = deliveryLanes(receipt);
  expect(lanes).toHaveLength(3); expect(lanes[0].measuredMs).toBe(3000); expect(lanes[0].unknown).toBe(1); expect(lanes[2].measuredMs).toBeUndefined();
  expect(deliveryReference([['Delivery:Id', 'receipt-a']])).toBe('receipt-a');
});
test('expanded exact receipt shows timings/copies and real retry request feedback', async () => {
  const client = { getDelivery: jest.fn(async () => receipt), retryDeliveryCleanup: jest.fn(async () => {}) } as unknown as NzbdClient;
  await act(async () => { tree = create(<DeliveryLifecyclePanel client={client} receiptId="receipt-a" />); });
  expect(client.getDelivery).toHaveBeenCalledWith('receipt-a'); expect(text()).toContain('overlapping steps counted once'); expect(text()).toContain('Mount unavailable'); expect(text()).toContain('Consumer acceptance recorded');
  expect(text()).not.toContain('long-copy-identity');
  expect(text().replace(/\s+/g, ' ')).toContain('queue · completed · duration unknown');
  await act(async () => button('Retry source cleanup').props.onPress());
  expect(client.retryDeliveryCleanup).toHaveBeenCalledWith('receipt-a'); expect(text()).toContain('Source cleanup retry requested');
  (client.retryDeliveryCleanup as jest.Mock).mockRejectedValueOnce(new Error('Mount still unavailable'));
  await act(async () => button('Retry source cleanup').props.onPress());
  expect(text()).toContain('Mount still unavailable');
});
