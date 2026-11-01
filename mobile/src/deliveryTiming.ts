import { DeliveryInterval, DeliveryTrace } from './api/types';
export function deliveryReference(params?: [string, string][]) { return params?.find(([key, value]) => key === 'Delivery:Id' && value)?.[1]; }
const measured = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
export function deliveryLanes(delivery: DeliveryTrace) {
  const groups = new Map<string, { node: string; clock: string; intervals: DeliveryInterval[] }>();
  for (const interval of (delivery.lifecycleTiming?.version === 1 && Array.isArray(delivery.lifecycleTiming.intervals) ? delivery.lifecycleTiming.intervals : []).slice(0, 128)) {
    if (!interval || typeof interval.nodeId !== 'string' || typeof interval.clockDomain !== 'string') continue;
    const key = JSON.stringify([interval.nodeId, interval.clockDomain]);
    if (!groups.has(key)) groups.set(key, { node: interval.nodeId, clock: interval.clockDomain, intervals: [] });
    groups.get(key)!.intervals.push(interval);
  }
  return [...groups.values()].map(group => {
    const valid = group.intervals.filter(i => i.state === 'completed' && measured(i.startMonotonicMs) && measured(i.endMonotonicMs) && measured(i.durationMs) && i.endMonotonicMs >= i.startMonotonicMs && i.durationMs === i.endMonotonicMs - i.startMonotonicMs);
    const ranges = valid.map(i => [i.startMonotonicMs!, i.endMonotonicMs!]).sort((a, b) => a[0] - b[0]);
    let total = 0, end: number | undefined;
    for (const [start, finish] of ranges) { total += end == null ? finish - start : Math.max(0, finish - Math.max(start, end)); end = Math.max(end ?? finish, finish); }
    return { ...group, measuredMs: valid.length ? total : undefined, unknown: group.intervals.length - valid.length };
  });
}
