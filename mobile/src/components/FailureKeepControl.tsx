import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';
import { NzbdClient } from '../api/client';
import { FailureKeepPreference } from '../api/types';
import { Theme, useTheme } from '../theme';

// This is the current retained source preference, never an admission snapshot.
export function FailureKeepControl({ client, jobId }: { client: NzbdClient; jobId: number }) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const [preference, setPreference] = useState<FailureKeepPreference | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const sequence = useRef(0);
  const identity = useRef(0);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const saving = useRef(false);
  const active = useRef(false);
  const refresh = useCallback(async () => {
    if (saving.current) return;
    const request = ++sequence.current;
    try {
      const current = await client.getFailureKeep(jobId);
      if (typeof current.available !== 'boolean' || typeof current.keep !== 'boolean' || current.applied !== undefined && typeof current.applied !== 'boolean') throw new Error('Runner did not provide a current preference.');
      if (active.current && request === sequence.current) { setPreference(current); setError(null); if (!current.available || current.applied === false) setNotice(null); }
    } catch (cause) {
      if (active.current && request === sequence.current) { setPreference(null); setError(cause instanceof Error ? cause.message : 'Could not check source retention.'); }
    }
  }, [client, jobId]);
  useEffect(() => {
    active.current = true; identity.current++; saving.current = false; setBusy(false); setError(null); setPreference(null); setNotice(null); setActionError(null); void refresh();
    const timer = setInterval(() => void refresh(), 5000);
    return () => { active.current = false; identity.current++; sequence.current++; clearInterval(timer); };
  }, [refresh]);
  const toggle = async () => {
    if (!preference?.available || saving.current) return;
    const epoch = identity.current;
    saving.current = true; const request = ++sequence.current; setBusy(true); setNotice(null); setActionError(null);
    try {
      const result = await client.setFailureKeep(jobId, !preference.keep);
      if (active.current && request === sequence.current) { setNotice(result?.applied === false ? 'Keep on failure requested. Waiting for source owners to apply the change.' : `Keep on failure is ${preference.keep ? 'off. Runner’s configured failure policy applies.' : 'on for this retained source.'}`); setError(null); }
    } catch (cause) {
      if (active.current && request === sequence.current) setActionError(cause instanceof Error ? cause.message : 'Could not save source retention.');
    } finally {
      if (epoch === identity.current) {
        saving.current = false;
        if (active.current && request === sequence.current) { setBusy(false); await refresh(); }
      }
    }
  };
  const pending = preference?.available && preference.applied === false;
  const label = `${pending ? 'Requested Keep' : 'Keep'} on failure: ${preference?.keep ? 'on' : 'off'}`;
  const ownerCount = Array.isArray(preference?.pendingOwners) ? Math.min(64, preference.pendingOwners.length) : 0;
  return <View style={styles.wrap}>
    {preference?.available ? <Pressable accessibilityRole="button" accessibilityLabel={label}
      accessibilityState={{ disabled: busy, busy }} onPress={() => { if (!busy) void toggle(); }} style={styles.button}>
      <Text style={styles.buttonText}>{busy ? 'Saving source retention…' : label}</Text>
    </Pressable> : <Text style={styles.note}>{error ? `Could not check source retention: ${error}` : preference ? 'No retained source is available for this preference.' : 'Checking current source retention…'}</Text>}
    {pending ? <Text accessibilityLiveRegion="polite" style={styles.note}>{`Waiting for source owners${ownerCount ? ` (${ownerCount})` : ''} to apply this retention change. The change is not confirmed on every source owner yet.`}</Text> : null}
    {actionError ? <Text accessibilityRole="alert" style={styles.error}>{actionError}</Text> : null}
    {notice ? <Text accessibilityLiveRegion="polite" style={styles.note}>{notice}</Text> : null}
    {!preference || error ? <Pressable accessibilityRole="button" onPress={() => void refresh()}><Text style={styles.buttonText}>Refresh source retention</Text></Pressable> : null}
  </View>;
}
const makeStyles = (theme: Theme) => StyleSheet.create({
  wrap: { gap: 8 }, button: { minHeight: 42, borderWidth: 1, borderColor: theme.border, borderRadius: 10, padding: 10, justifyContent: 'center', alignSelf: 'flex-start' },
  buttonText: { color: theme.accent, fontSize: 12, fontWeight: '700' }, note: { color: theme.textMuted, fontSize: 12, lineHeight: 17 }, error: { color: theme.danger, fontSize: 12, lineHeight: 17 },
});
