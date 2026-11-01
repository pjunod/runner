import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ActivityIndicator,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from 'react-native';

import { NzbdClient } from '../api/client';
import { formatBytes } from '../api/format';
import { ConnectionConfig, HistoryEntry, HistoryPage } from '../api/types';
import { Theme, useTheme } from '../theme';
import { FailureKeepControl } from '../components/FailureKeepControl';

export function HistoryView({ config }: { config: ConnectionConfig }) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const client = useMemo(() => new NzbdClient(config), [config]);
  const [page, setPage] = useState<HistoryPage | null>(null);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    try {
      setPage(await client.getHistory());
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not load Runner history.');
    } finally {
      setRefreshing(false);
    }
  }, [client]);

  const loadMore = useCallback(async () => {
    if (!page || page.entries.length >= page.total || loadingMore) return;
    setLoadingMore(true);
    try {
      const next = await client.getHistory(200, page.entries.length);
      setPage({ ...next, offset: 0, entries: [...page.entries, ...next.entries] });
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not load older history.');
    } finally {
      setLoadingMore(false);
    }
  }, [client, loadingMore, page]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return (
    <ScrollView
      contentContainerStyle={styles.content}
      refreshControl={
        <RefreshControl onRefresh={() => void refresh()} refreshing={refreshing} tintColor={theme.accent} />
      }
    >
      <View style={styles.heading}>
        <View>
          <Text style={styles.eyebrow}>HISTORY</Text>
          <Text style={styles.title}>Completed downloads</Text>
        </View>
        {page ? (
          <Text style={styles.count}>
            {page.entries.length === page.total
              ? `${page.total} total`
              : `${page.entries.length} of ${page.total}`}
          </Text>
        ) : null}
      </View>

      {error ? (
        <Pressable accessibilityRole="button" onPress={() => void refresh()} style={styles.error}>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.retry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {!page && refreshing ? (
        <View style={styles.empty}>
          <ActivityIndicator color={theme.accent} />
          <Text style={styles.emptyText}>Loading history…</Text>
        </View>
      ) : page?.entries.length === 0 ? (
        <View style={styles.empty}>
          <Text style={styles.emptyTitle}>No history yet</Text>
          <Text style={styles.emptyText}>Finished, failed, and removed jobs will appear here.</Text>
        </View>
      ) : (
        <View style={styles.list}>
          {page?.entries.map((entry) => (
            <HistoryCard
              entry={entry}
              client={client}
              expanded={expanded === entry.job}
              key={`${entry.job}:${entry.completed_at_unix}`}
              onToggle={() => setExpanded(expanded === entry.job ? null : entry.job)}
              styles={styles}
            />
          ))}
          {page && page.entries.length < page.total ? (
            <Pressable
              accessibilityRole="button"
              disabled={loadingMore}
              onPress={() => void loadMore()}
              style={({ pressed }) => [styles.loadMore, pressed && styles.pressed]}
            >
              {loadingMore ? <ActivityIndicator color={theme.accent} size="small" /> : null}
              <Text style={styles.loadMoreText}>Load older history</Text>
            </Pressable>
          ) : null}
        </View>
      )}
    </ScrollView>
  );
}

function HistoryCard({
  client,
  entry,
  expanded,
  onToggle,
  styles,
}: {
  client: NzbdClient;
  entry: HistoryEntry;
  expanded: boolean;
  onToggle: () => void;
  styles: ReturnType<typeof makeStyles>;
}) {
  const record = entry.record;
  return (
    <View style={[styles.card, expanded && styles.cardExpanded]}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        onPress={onToggle}
        style={({ pressed }) => [styles.cardMain, pressed && styles.pressed]}
      >
        <View style={styles.topline}>
          <Text numberOfLines={2} style={styles.name}>
            {entry.name}
          </Text>
          <Text style={[styles.status, statusStyle(entry.status, styles)]}>
            {entry.status.replaceAll('_', ' ')}
          </Text>
        </View>
        <View style={styles.meta}>
          <Text style={styles.metaText}>{formatBytes(entry.size)}</Text>
          <Text style={styles.metaText}>{formatTimestamp(entry.completed_at_unix)}</Text>
          {entry.category ? <Text style={styles.metaText}>{entry.category}</Text> : null}
          <Text style={styles.metaText}>health {(entry.health / 10).toFixed(1)}%</Text>
        </View>
      </Pressable>

      {expanded ? (
        <View style={styles.details}>
          {entry.requeue_reason ? <Text style={styles.metaText}>{entry.requeue_reason}</Text> : null}
          <FailureKeepControl client={client} jobId={entry.job} />
          <Detail label="Job" value={`#${entry.job}`} styles={styles} />
          <Detail label="Folder" value={entry.final_dir || record?.dir_name || '—'} styles={styles} />
          <Detail
            label="Articles"
            value={
              record
                ? `${record.success_articles}/${record.total_articles} complete` +
                  (record.failed_articles ? ` · ${record.failed_articles} failed` : '')
                : '—'
            }
            styles={styles}
          />
          <Detail label="Files" value={record ? String(record.files.length) : '—'} styles={styles} />
          <Detail label="Submitted by" value={record?.client || '—'} styles={styles} />
          <Detail label="Picked up by" value={entry.picked_up_by || 'Not observed'} styles={styles} />
        </View>
      ) : null}
    </View>
  );
}

function Detail({
  label,
  value,
  styles,
}: {
  label: string;
  value: string;
  styles: ReturnType<typeof makeStyles>;
}) {
  return (
    <View style={styles.detail}>
      <Text style={styles.detailLabel}>{label}</Text>
      <Text selectable style={styles.detailValue}>
        {value}
      </Text>
    </View>
  );
}

function formatTimestamp(unix: number): string {
  if (!Number.isFinite(unix) || unix <= 0) return 'Unknown time';
  return new Date(unix * 1000).toLocaleString();
}

function statusStyle(status: string, styles: ReturnType<typeof makeStyles>) {
  const value = status.toLowerCase();
  if (value.includes('fail')) return styles.statusError;
  if (value.includes('delete')) return styles.statusMuted;
  return styles.statusOk;
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    content: { padding: 14, paddingBottom: 44, gap: 12, maxWidth: 1000, width: '100%', alignSelf: 'center' },
    heading: { flexDirection: 'row', alignItems: 'flex-end', justifyContent: 'space-between', gap: 12, paddingHorizontal: 2 },
    eyebrow: { color: theme.textMuted, fontSize: 10, fontWeight: '800', letterSpacing: 1.3 },
    title: { color: theme.text, fontSize: 23, fontWeight: '800', letterSpacing: -0.4 },
    count: { color: theme.textMuted, fontSize: 11 },
    list: { gap: 9 },
    card: { borderRadius: 16, borderWidth: 1, borderColor: theme.border, backgroundColor: theme.panel, overflow: 'hidden' },
    cardExpanded: { borderColor: theme.accent },
    cardMain: { padding: 14, gap: 9 },
    pressed: { opacity: 0.72 },
    topline: { flexDirection: 'row', alignItems: 'flex-start', gap: 12 },
    name: { color: theme.text, fontSize: 16, lineHeight: 21, fontWeight: '700', flex: 1 },
    status: { fontSize: 10, fontWeight: '900', textTransform: 'uppercase', paddingHorizontal: 8, paddingVertical: 4, borderRadius: 8, overflow: 'hidden' },
    statusOk: { color: theme.success, backgroundColor: theme.accentSoft },
    statusError: { color: theme.danger, backgroundColor: theme.dangerSoft },
    statusMuted: { color: theme.textMuted, backgroundColor: theme.panelAlt },
    meta: { flexDirection: 'row', flexWrap: 'wrap', gap: 9 },
    metaText: { color: theme.textMuted, fontSize: 11, fontVariant: ['tabular-nums'] },
    details: { padding: 13, paddingTop: 0, gap: 8 },
    detail: { padding: 10, borderRadius: 10, backgroundColor: theme.panelAlt },
    detailLabel: { color: theme.textMuted, fontSize: 9, fontWeight: '800', textTransform: 'uppercase' },
    detailValue: { color: theme.text, fontSize: 12, lineHeight: 17, marginTop: 2 },
    empty: { minHeight: 230, padding: 28, borderRadius: 18, borderWidth: 1, borderStyle: 'dashed', borderColor: theme.border, backgroundColor: theme.panel, alignItems: 'center', justifyContent: 'center', gap: 10 },
    emptyTitle: { color: theme.text, fontSize: 20, fontWeight: '800' },
    emptyText: { color: theme.textMuted, fontSize: 14, lineHeight: 20, textAlign: 'center' },
    error: { padding: 13, borderRadius: 12, backgroundColor: theme.dangerSoft, gap: 4 },
    errorText: { color: theme.danger, fontSize: 13, lineHeight: 18 },
    retry: { color: theme.danger, fontSize: 11, fontWeight: '800' },
    loadMore: { minHeight: 46, borderRadius: 12, borderWidth: 1, borderColor: theme.border, backgroundColor: theme.panel, alignItems: 'center', justifyContent: 'center', flexDirection: 'row', gap: 8 },
    loadMoreText: { color: theme.accent, fontSize: 12, fontWeight: '800' },
  });
