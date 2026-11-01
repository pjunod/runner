import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';
import { NzbdClient } from '../api/client';
import { DeliveryTrace } from '../api/types';
import { formatDuration } from '../api/format';
import { deliveryLanes, measuredIntervalDuration } from '../deliveryTiming';
import { Theme, useTheme } from '../theme';
export function DeliveryLifecyclePanel({ client, receiptId }: { client: NzbdClient; receiptId: string }) {
  const theme = useTheme(), styles = useMemo(() => makeStyles(theme), [theme]);
  const [delivery, setDelivery] = useState<DeliveryTrace | null>(null);
  const [error, setError] = useState<string | null>(null), [notice, setNotice] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false), [references, setReferences] = useState(false);
  const epoch = useRef(0), pending = useRef(false);
  const refresh = useCallback(async () => {
    if (pending.current) return;
    const identity = epoch.current; pending.current = true;
    try { const value = await client.getDelivery(receiptId); if (identity === epoch.current) { setDelivery(value); setError(null); } }
    catch (cause) { if (identity === epoch.current) setError(cause instanceof Error ? cause.message : 'Could not load publication lifecycle.'); }
    finally { if (identity === epoch.current) pending.current = false; }
  }, [client, receiptId]);
  useEffect(() => {
    epoch.current++; pending.current = false; setDelivery(null); setError(null); setActionError(null); setNotice(null); setBusy(false); setReferences(false); void refresh();
    const timer = setInterval(() => void refresh(), 5000);
    return () => { epoch.current++; clearInterval(timer); };
  }, [refresh]);
  const retry = async () => {
    if (pending.current) return;
    const identity = epoch.current; pending.current = true; setBusy(true); setNotice(null); setActionError(null);
    try { await client.retryDeliveryCleanup(receiptId); if (identity === epoch.current) { setNotice('Source cleanup retry requested. The publication remains visible until cleanup completes.'); setError(null); } }
    catch (cause) { if (identity === epoch.current) setActionError(cause instanceof Error ? cause.message : 'Source cleanup retry failed.'); }
    finally { if (identity === epoch.current) { pending.current = false; setBusy(false); await refresh(); } }
  };
  const duration = (ms?: number | null) => typeof ms === 'number' && Number.isSafeInteger(ms) && ms >= 0 ? formatDuration(ms / 1000) : 'unknown';
  const time = (unix?: number | null) => typeof unix === 'number' && unix > 0 && Number.isFinite(new Date(unix * 1000).getTime()) ? new Date(unix * 1000).toLocaleString() : 'time unknown';
  const retryable = ['pending', 'retry', 'blocked'].includes(delivery?.sourceCleanup?.status || '') || delivery?.sourceCopies?.some(copy => ['pending', 'retry', 'blocked'].includes(copy.status));
  return <View style={styles.panel}>
    <Text accessibilityRole="header" style={styles.title}>Publication lifecycle</Text>
    {actionError ? <Text accessibilityRole="alert" style={styles.error}>{actionError}</Text> : null}
    {error ? <Text accessibilityRole="alert" style={styles.error}>{error}</Text> : null}
    {!delivery && !error ? <Text style={styles.note}>Loading lifecycle details…</Text> : null}
    {delivery ? <>
      <Text style={styles.note}>Times from different machines or restarts stay separate.</Text>
      {deliveryLanes(delivery).length ? deliveryLanes(delivery).map(lane => <View key={JSON.stringify([lane.node, lane.clock])} style={styles.lane}>
        <Text style={styles.title}>Runner · {lane.node || 'node unknown'}</Text>
        <Text style={styles.note}>Measured time (overlapping steps counted once): {duration(lane.measuredMs)}</Text>
        {lane.unknown ? <Text style={styles.note}>Some steps have no complete timing.</Text> : null}
        {lane.intervals.map((interval, index) => <Text key={index} style={styles.text}>{interval.stage.replaceAll('_', ' ')} · {interval.state} · {measuredIntervalDuration(interval) === undefined ? 'duration unknown' : `${duration(measuredIntervalDuration(interval))} measured time`}</Text>)}
        {references ? <Text selectable style={styles.note}>{lane.clock}</Text> : null}
      </View>) : <Text style={styles.note}>Earlier step timings were not recorded.</Text>}
      {delivery.runnerStages ? <View style={styles.lane}><Text style={styles.title}>Processing details · {delivery.runnerStages.nodeId}</Text><Text style={styles.note}>These details expand the processing steps above and are not added again.</Text>{delivery.runnerStages.spans.slice(0, 128).map((span, index) => <Text key={index} style={styles.text}>{span.stage.replaceAll('_', ' ')} · {duration(span.ms)} measured time</Text>)}</View> : null}
      <Text style={styles.title}>Publication and handoff</Text><Text style={styles.text}>Ready {time(delivery.readyAt)} · {delivery.acceptedAt ? `Consumer acceptance recorded ${time(delivery.acceptedAt)}` : 'Awaiting consumer acceptance'}</Text>
      {delivery.sourceCleanup ? <Text style={styles.text}>Source cleanup: {delivery.sourceCleanup.status.replaceAll('_', ' ')} · {delivery.sourceCleanup.attempts} attempts{delivery.sourceCleanup.error ? ` · ${delivery.sourceCleanup.error}` : ''}</Text> : null}
      {delivery.sourceCopies?.slice(0, 64).map(copy => <View key={copy.id} style={styles.lane}><Text style={styles.title}>{copy.ownerNode || 'node unknown'} · {copy.status.replaceAll('_', ' ')}</Text><Text style={styles.note}>{copy.attempts} cleanup attempts{copy.nextRetryAt ? ` · next ${time(copy.nextRetryAt)}` : ''}</Text>{copy.error ? <Text style={styles.error}>{copy.error}</Text> : null}{references ? <Text selectable style={styles.note}>{copy.id}</Text> : null}</View>)}
      <Pressable accessibilityRole="button" onPress={() => setReferences(!references)}><Text style={styles.buttonText}>{references ? 'Hide timing and copy references' : 'Show timing and copy references'}</Text></Pressable>
      {retryable ? <Pressable accessibilityRole="button" accessibilityLabel="Retry source cleanup" accessibilityState={{ disabled: busy, busy }} onPress={() => { if (!busy) void retry(); }} style={styles.button}><Text style={styles.buttonText}>{busy ? 'Retrying source cleanup…' : 'Retry source cleanup'}</Text></Pressable> : null}
    </> : null}
    <Pressable accessibilityRole="button" onPress={() => void refresh()}><Text style={styles.buttonText}>Refresh lifecycle details</Text></Pressable>
    {notice ? <Text accessibilityLiveRegion="polite" style={styles.note}>{notice}</Text> : null}
  </View>;
}
const makeStyles = (theme: Theme) => StyleSheet.create({ panel: { gap: 8 }, lane: { paddingLeft: 10, borderLeftWidth: 2, borderLeftColor: theme.border, gap: 6 }, title: { color: theme.text, fontSize: 12, fontWeight: '700' }, text: { color: theme.text, fontSize: 12, lineHeight: 17 }, note: { color: theme.textMuted, fontSize: 12, lineHeight: 17 }, error: { color: theme.danger, fontSize: 12 }, button: { minHeight: 44, justifyContent: 'center' }, buttonText: { color: theme.accent, fontSize: 12, fontWeight: '700' } });
