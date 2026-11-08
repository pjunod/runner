import { ActivityIndicator, Pressable, StyleSheet, Text, ViewStyle } from 'react-native';

import { Theme, useTheme } from '../theme';

interface Props {
  label: string;
  onPress: () => void;
  variant?: 'primary' | 'secondary' | 'danger' | 'ghost' | 'latched';
  compact?: boolean;
  disabled?: boolean;
  loading?: boolean;
  style?: ViewStyle;
  accessibilityLabel?: string;
}

export function ActionButton({
  label,
  onPress,
  variant = 'secondary',
  compact = false,
  disabled = false,
  loading = false,
  style,
  accessibilityLabel,
}: Props) {
  const theme = useTheme();
  const styles = makeStyles(theme);
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={accessibilityLabel ?? label}
      disabled={disabled || loading}
      onPress={onPress}
      style={({ pressed }) => [
        styles.base,
        compact && styles.compact,
        styles[variant],
        pressed && styles.pressed,
        (disabled || loading) && styles.disabled,
        style,
      ]}
    >
      {loading ? (
        <ActivityIndicator
          color={variant === 'primary' ? theme.onAccent : theme.text}
          size="small"
        />
      ) : (
        <Text style={[styles.label, styles[`${variant}Label`]]}>{label}</Text>
      )}
    </Pressable>
  );
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    base: {
      minHeight: 44,
      paddingHorizontal: 16,
      borderRadius: 12,
      borderWidth: 1,
      borderColor: theme.border,
      alignItems: 'center',
      justifyContent: 'center',
    },
    compact: { minHeight: 36, paddingHorizontal: 11, borderRadius: 9 },
    primary: { backgroundColor: theme.accent, borderColor: theme.accent },
    secondary: { backgroundColor: theme.panel, borderColor: theme.border },
    danger: { backgroundColor: theme.dangerSoft, borderColor: theme.danger },
    ghost: { backgroundColor: 'transparent', borderColor: 'transparent' },
    latched: { backgroundColor: theme.panel, borderColor: theme.warning },
    label: { color: theme.text, fontSize: 14, fontWeight: '700' },
    primaryLabel: { color: theme.onAccent },
    secondaryLabel: { color: theme.text },
    dangerLabel: { color: theme.danger },
    ghostLabel: { color: theme.accent },
    latchedLabel: { color: theme.warning },
    pressed: { opacity: 0.72 },
    disabled: { opacity: 0.45 },
  });
