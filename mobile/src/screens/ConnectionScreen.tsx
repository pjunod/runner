import { useMemo, useState } from 'react';
import {
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  useWindowDimensions,
  View,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import { NzbdClient } from '../api/client';
import { normalizeServerUrl } from '../api/format';
import { ConnectionConfig } from '../api/types';
import { ActionButton } from '../components/ActionButton';
import { ThemeSwitcher } from '../components/ThemeSwitcher';
import { DiscoveredNzbd } from '../discovery/nzbdService';
import { useNzbdDiscovery } from '../discovery/useNzbdDiscovery';
import { Theme, useTheme } from '../theme';

interface Props {
  initial?: ConnectionConfig;
  onConnect: (config: ConnectionConfig) => Promise<void>;
  onCancel?: () => void;
  onForget?: () => Promise<void>;
}

export function ConnectionScreen({ initial, onConnect, onCancel, onForget }: Props) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const { width } = useWindowDimensions();
  const [baseUrl, setBaseUrl] = useState(initial?.baseUrl ?? 'http://');
  const [username, setUsername] = useState(initial?.username ?? '');
  const [password, setPassword] = useState(initial?.password ?? '');
  const [token, setToken] = useState(initial?.token ?? '');
  const [busy, setBusy] = useState<'test' | 'connect' | null>(null);
  const [showPassword, setShowPassword] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageKind, setMessageKind] = useState<'ok' | 'error'>('ok');
  const nearby = useNzbdDiscovery();
  const [selectedNearby, setSelectedNearby] = useState<DiscoveredNzbd | null>(null);

  const selectNearby = (service: DiscoveredNzbd) => {
    setSelectedNearby(service);
    setBaseUrl(service.baseUrl);
    setMessageKind('ok');
    setMessage(
      service.auth === 'none'
        ? `Selected ${service.name}. It does not require authentication.`
        : `Selected ${service.name}. Add its credentials, then connect.`,
    );
  };

  const configFromFields = (): ConnectionConfig => ({
    baseUrl: normalizeServerUrl(baseUrl),
    username: username.trim(),
    password,
    token: token.trim(),
  });

  const testConnection = async () => {
    setBusy('test');
    setMessage(null);
    try {
      const config = configFromFields();
      const status = await new NzbdClient(config).getStatus();
      setMessageKind('ok');
      setMessage(`Connection works — Runner ${status.version} answered at ${config.baseUrl}.`);
    } catch (cause) {
      setMessageKind('error');
      setMessage(cause instanceof Error ? cause.message : 'Could not connect to Runner.');
    } finally {
      setBusy(null);
    }
  };

  const submit = async () => {
    setBusy('connect');
    setMessage(null);
    try {
      const config = configFromFields();
      const status = await new NzbdClient(config).getStatus();
      await onConnect(config);
      setMessageKind('ok');
      setMessage(`Connected to Runner ${status.version}.`);
    } catch (cause) {
      setMessageKind('error');
      setMessage(cause instanceof Error ? cause.message : 'Could not connect to Runner.');
    } finally {
      setBusy(null);
    }
  };

  return (
    <SafeAreaView style={styles.safe}>
      <KeyboardAvoidingView
        behavior={Platform.OS === 'ios' ? 'padding' : undefined}
        style={styles.safe}
      >
        <ScrollView
          contentContainerStyle={styles.scroll}
          keyboardShouldPersistTaps="handled"
        >
          <View style={[styles.card, width >= 700 && styles.cardWide]}>
            <View style={styles.cardTopline}>
              <View style={styles.brandMark}>
                <Text style={styles.brandMarkText}>n</Text>
              </View>
              <ThemeSwitcher />
            </View>
            <Text style={styles.title}>{initial ? 'Server settings' : 'Connect to Runner'}</Text>
            <Text style={styles.subtitle}>
              Use the address you open from this device. A phone cannot reach your computer at
              localhost.
            </Text>

            <View style={styles.nearbySection}>
              <View style={styles.nearbyHeading}>
                <View style={styles.nearbyTitleRow}>
                  <Text style={styles.label}>Nearby Runner</Text>
                  {nearby.scanning ? <View style={styles.liveDot} /> : null}
                </View>
                <Pressable
                  accessibilityRole="button"
                  accessibilityLabel="Scan again for nearby Runner servers"
                  onPress={() => void nearby.scanAgain()}
                  style={styles.scanButton}
                >
                  <Text style={styles.scanButtonText}>Scan again</Text>
                </Pressable>
              </View>

              {nearby.services.map((service) => (
                <Pressable
                  accessibilityRole="button"
                  accessibilityLabel={`Use ${service.name} at ${service.baseUrl}`}
                  disabled={busy !== null}
                  key={service.key}
                  onPress={() => selectNearby(service)}
                  style={({ pressed }) => [styles.nearbyServer, pressed && styles.nearbyPressed]}
                >
                  <View style={styles.nearbyServerTop}>
                    <Text numberOfLines={1} style={styles.nearbyServerName}>
                      {service.name}
                    </Text>
                    {service.version ? (
                      <Text style={styles.versionBadge}>v{service.version}</Text>
                    ) : null}
                  </View>
                  <Text numberOfLines={1} style={styles.nearbyAddress}>
                    {service.baseUrl}
                  </Text>
                  <Text style={styles.nearbyAuth}>{authDescription(service.auth)}</Text>
                </Pressable>
              ))}

              {nearby.services.length === 0 ? (
                <View style={styles.discoveryEmpty}>
                  {nearby.scanning ? (
                    <ActivityIndicator color={theme.accent} size="small" />
                  ) : null}
                  <Text style={[styles.discoveryEmptyText, nearby.error && styles.discoveryError]}>
                    {nearby.error ??
                      (nearby.noResults
                        ? 'No server found yet. Check Local Network permission and Wi-Fi. Docker installs need the host-network discovery companion.'
                        : 'Scanning this Wi-Fi network. You can still enter an address manually.')}
                  </Text>
                </View>
              ) : null}
            </View>

            {selectedNearby && selectedNearby.addresses.length > 1 ? (
              <View style={styles.addressChoices}>
                <Text style={styles.label}>Addresses for {selectedNearby.name}</Text>
                <Text style={styles.addressHint}>
                  Choose an address, then test it from this device.
                </Text>
                {selectedNearby.addresses.map((address) => (
                  <Pressable
                    key={address.baseUrl}
                    accessibilityRole="radio"
                    accessibilityLabel={`Use ${address.kind} address ${address.baseUrl}`}
                    accessibilityState={{ checked: baseUrl === address.baseUrl, disabled: busy !== null }}
                    disabled={busy !== null}
                    onPress={() => {
                      setBaseUrl(address.baseUrl);
                      setMessage(null);
                    }}
                    style={[
                      styles.addressChoice,
                      baseUrl === address.baseUrl && styles.addressSelected,
                    ]}
                  >
                    <Text style={styles.addressKind}>{address.kind}</Text>
                    <Text style={styles.addressValue}>{address.baseUrl}</Text>
                    {baseUrl === address.baseUrl ? <Text style={styles.addressKind}>✓</Text> : null}
                  </Pressable>
                ))}
              </View>
            ) : null}

            <Text style={styles.label}>Server address</Text>
            <TextInput
              accessibilityLabel="Server address"
              autoCapitalize="none"
              autoCorrect={false}
              keyboardType="url"
              editable={busy === null}
              onChangeText={(value) => { setBaseUrl(value); setMessage(null); }}
              placeholder="http://192.168.1.20:6789"
              placeholderTextColor={theme.textMuted}
              style={styles.input}
              value={baseUrl}
            />

            <View style={styles.authHeading}>
              <Text style={styles.label}>Authentication</Text>
              <Text style={styles.optional}>optional</Text>
            </View>
            <Text style={styles.hint}>
              Use an API token, or the username and password from Runner. A token takes precedence.
            </Text>

            <TextInput
              accessibilityLabel="API token"
              autoCapitalize="none"
              autoCorrect={false}
              onChangeText={setToken}
              placeholder="API token"
              placeholderTextColor={theme.textMuted}
              secureTextEntry
              style={styles.input}
              value={token}
            />
            <TextInput
              accessibilityLabel="Username"
              autoCapitalize="none"
              autoCorrect={false}
              onChangeText={setUsername}
              placeholder="Username"
              placeholderTextColor={theme.textMuted}
              style={styles.input}
              value={username}
            />
            <View style={styles.passwordRow}>
              <TextInput
                accessibilityLabel="Password"
                autoCapitalize="none"
                autoCorrect={false}
                onChangeText={setPassword}
                placeholder="Password"
                placeholderTextColor={theme.textMuted}
                secureTextEntry={!showPassword}
                style={[styles.input, styles.passwordInput]}
                value={password}
              />
              <Pressable
                accessibilityRole="button"
                accessibilityLabel={showPassword ? 'Hide password' : 'Show password'}
                onPress={() => setShowPassword((value) => !value)}
                style={styles.showButton}
              >
                <Text style={styles.showButtonText}>{showPassword ? 'Hide' : 'Show'}</Text>
              </Pressable>
            </View>

            {message ? (
              <View style={[styles.message, messageKind === 'error' && styles.messageError]}>
                <Text
                  accessibilityLiveRegion="polite"
                  style={[styles.messageText, messageKind === 'error' && styles.messageErrorText]}
                >
                  {message}
                </Text>
              </View>
            ) : null}

            <View style={styles.connectActions}>
              <ActionButton
                disabled={busy !== null}
                label="Test connection"
                loading={busy === 'test'}
                onPress={() => void testConnection()}
                style={styles.connectAction}
              />
              <ActionButton
                disabled={busy !== null}
                label="Connect"
                loading={busy === 'connect'}
                onPress={() => void submit()}
                style={styles.connectAction}
                variant="primary"
              />
            </View>
            {onCancel ? (
              <ActionButton label="Cancel" onPress={onCancel} variant="ghost" />
            ) : null}
            {onForget ? (
              <ActionButton
                label="Forget this server"
                onPress={() => void onForget()}
                variant="danger"
              />
            ) : null}

            {busy ? (
              <View style={styles.testing}>
                <ActivityIndicator color={theme.accent} size="small" />
                <Text style={styles.testingText}>
                  {busy === 'connect' ? 'Testing and saving…' : 'Testing the native API…'}
                </Text>
              </View>
            ) : null}
            <Text style={styles.securityNote}>
              Credentials are stored in Keychain or Android Keystore. Prefer HTTPS outside a
              trusted LAN; self-signed certificates must be trusted by the device first.
            </Text>
          </View>
        </ScrollView>
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    safe: { flex: 1, backgroundColor: theme.background },
    scroll: { flexGrow: 1, justifyContent: 'center', padding: 20 },
    card: {
      width: '100%',
      maxWidth: 560,
      alignSelf: 'center',
      padding: 22,
      borderRadius: 22,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      gap: 12,
    },
    cardWide: { padding: 30 },
    cardTopline: {
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 12,
    },
    brandMark: {
      width: 46,
      height: 46,
      borderRadius: 14,
      alignItems: 'center',
      justifyContent: 'center',
      backgroundColor: theme.accent,
    },
    brandMarkText: { color: theme.onAccent, fontSize: 30, fontWeight: '900' },
    title: { color: theme.text, fontSize: 29, fontWeight: '800', letterSpacing: -0.7 },
    subtitle: { color: theme.textMuted, fontSize: 15, lineHeight: 21, marginBottom: 7 },
    label: { color: theme.text, fontSize: 13, fontWeight: '700' },
    nearbySection: {
      borderRadius: 14,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panelAlt,
      padding: 12,
      gap: 9,
    },
    nearbyHeading: {
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
    },
    nearbyTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 7 },
    liveDot: { width: 7, height: 7, borderRadius: 4, backgroundColor: theme.success },
    scanButton: { paddingVertical: 4, paddingLeft: 10 },
    scanButtonText: { color: theme.accent, fontSize: 12, fontWeight: '700' },
    nearbyServer: {
      borderRadius: 11,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      padding: 11,
      gap: 3,
    },
    nearbyPressed: { opacity: 0.7 },
    nearbyServerTop: {
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 8,
    },
    nearbyServerName: { color: theme.text, flex: 1, fontSize: 14, fontWeight: '700' },
    versionBadge: {
      color: theme.accent,
      fontSize: 10,
      fontWeight: '700',
      backgroundColor: theme.accentSoft,
      borderRadius: 8,
      overflow: 'hidden',
      paddingHorizontal: 7,
      paddingVertical: 3,
    },
    nearbyAddress: { color: theme.textMuted, fontSize: 12 },
    nearbyAuth: { color: theme.textMuted, fontSize: 11 },
    discoveryEmpty: { flexDirection: 'row', alignItems: 'center', gap: 8, minHeight: 30 },
    discoveryEmptyText: { color: theme.textMuted, flex: 1, fontSize: 12, lineHeight: 16 },
    discoveryError: { color: theme.danger },
    addressChoices: { gap: 8 },
    addressHint: { color: theme.textMuted, fontSize: 12, lineHeight: 17 },
    addressChoice: {
      flexDirection: 'row',
      alignItems: 'center',
      gap: 8,
      padding: 12,
      borderWidth: 1,
      borderColor: theme.border,
      borderRadius: 11,
      backgroundColor: theme.panelAlt,
    },
    addressSelected: { borderColor: theme.accent, backgroundColor: theme.accentSoft },
    addressKind: { color: theme.accent, fontSize: 12, fontWeight: '700' },
    addressValue: { color: theme.text, flex: 1, fontSize: 12 },
    optional: { color: theme.textMuted, fontSize: 12 },
    authHeading: { flexDirection: 'row', alignItems: 'baseline', gap: 7, marginTop: 5 },
    hint: { color: theme.textMuted, fontSize: 13, lineHeight: 18, marginTop: -7 },
    input: {
      minHeight: 48,
      borderRadius: 12,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panelAlt,
      color: theme.text,
      fontSize: 16,
      paddingHorizontal: 14,
    },
    passwordRow: { flexDirection: 'row', alignItems: 'center' },
    passwordInput: { flex: 1, paddingRight: 66 },
    showButton: { position: 'absolute', right: 5, padding: 12 },
    showButtonText: { color: theme.accent, fontSize: 13, fontWeight: '700' },
    message: {
      borderRadius: 10,
      padding: 11,
      backgroundColor: theme.accentSoft,
    },
    messageError: { backgroundColor: theme.dangerSoft },
    messageText: { color: theme.accent, fontSize: 13, lineHeight: 18 },
    messageErrorText: { color: theme.danger },
    connectActions: { flexDirection: 'row', gap: 9 },
    connectAction: { flex: 1 },
    testing: { flexDirection: 'row', alignItems: 'center', justifyContent: 'center', gap: 8 },
    testingText: { color: theme.textMuted, fontSize: 12 },
    securityNote: { color: theme.textMuted, fontSize: 11, lineHeight: 16, marginTop: 2 },
  });

function authDescription(auth: string): string {
  switch (auth) {
    case 'none':
      return 'No authentication required';
    case 'basic':
      return 'Username and password';
    case 'bearer':
      return 'API token';
    case 'basic,bearer':
      return 'API token or username and password';
    default:
      return 'Authentication not advertised';
  }
}
