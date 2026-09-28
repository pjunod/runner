import { useState } from 'react';
import { Modal, ScrollView, Text, TextInput, View } from 'react-native';
import { JobSummary, SeedPolicy } from '../api/types';
import { seedDraft, SeedDraft, seedPolicyBody } from '../torrentPresentation';
import { useTheme } from '../theme';
import { ActionButton } from './ActionButton';

export function SeedPolicyEditor({ job, busy, onSave, onClose }: {
  job: JobSummary;
  busy: boolean;
  onSave: (body: SeedPolicy & { use_defaults: boolean }) => Promise<void>;
  onClose: () => void;
}) {
  const theme = useTheme();
  const [draft, setDraft] = useState(() => seedDraft(job.seed_policy));
  const [error, setError] = useState<string | null>(null);
  const modes: [SeedDraft['mode'], string][] = [['defaults', 'Use category / global defaults'],
    ['stop', 'Stop after download'], ['unlimited', 'Keep seeding until stopped'], ['limits', 'Ratio or time limits']];
  const save = async () => {
    try { setError(null); await onSave(seedPolicyBody(draft)); }
    catch (cause) { setError(cause instanceof Error ? cause.message : 'Could not save policy.'); }
  };
  return <Modal visible animationType="slide" presentationStyle="pageSheet" onRequestClose={onClose}>
    <ScrollView contentContainerStyle={{ padding: 24, paddingTop: 60, gap: 18, backgroundColor: theme.background, flexGrow: 1 }}>
      <Text accessibilityRole="header" style={{ color: theme.text, fontSize: 24 }}>Seeding options</Text>
      <Text style={{ color: theme.text }}>{job.name}</Text>
      {modes.map(([mode, label]) => <ActionButton key={mode} label={`${draft.mode === mode ? '✓ ' : ''}${label}`}
        disabled={busy} onPress={() => setDraft({ ...draft, mode })} />)}
      {draft.mode === 'limits' ? <View style={{ gap: 12 }}>
        {(['ratio', 'hours'] as const).map((field) => <View key={field}>
          <Text style={{ color: theme.text }}>{field === 'ratio' ? 'Share ratio' : 'Seeding hours'}</Text>
          <TextInput accessibilityLabel={field === 'ratio' ? 'Share ratio' : 'Seeding hours'}
            keyboardType="decimal-pad" editable={!busy} value={draft[field]} placeholder="No limit"
            placeholderTextColor={theme.textMuted} style={{ color: theme.text, borderColor: theme.border, borderWidth: 1, padding: 12 }}
            onChangeText={(value) => setDraft({ ...draft, [field]: value })} />
        </View>)}
        <Text style={{ color: theme.textMuted }}>Seeding stops at the first limit reached.</Text>
      </View> : null}
      <Text style={{ color: theme.textMuted }}>Saving does not start a stopped torrent. Start seeding separately after saving.</Text>
      {error ? <Text accessibilityRole="alert" style={{ color: theme.danger }}>{error}</Text> : null}
      <ActionButton label="Save policy" disabled={busy} onPress={() => void save()} variant="primary" />
      <ActionButton label="Close" onPress={onClose} />
    </ScrollView>
  </Modal>;
}
