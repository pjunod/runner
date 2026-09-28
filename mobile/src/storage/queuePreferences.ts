import * as SecureStore from 'expo-secure-store';
import { useEffect, useRef, useState } from 'react';

const KEY = 'nzbd.queue.collapsed.v1';
const keys = ['seeding', 'completed', 'waiting'];
export function parseCollapsed(raw: string | null): string[] {
  try {
    const value: unknown = JSON.parse(raw ?? '[]');
    return Array.isArray(value) ? keys.filter((key) => value.includes(key)) : [];
  } catch { return []; }
}
export async function loadCollapsed(): Promise<string[]> {
  try {
    return await SecureStore.isAvailableAsync() ? parseCollapsed(await SecureStore.getItemAsync(KEY)) : [];
  } catch { return []; }
}
// A single chain also orders writes across dashboard unmount/remount.
let writes: Promise<void> = Promise.resolve();
export function saveCollapsed(value: readonly string[]): Promise<void> {
  const raw = JSON.stringify(keys.filter((key) => value.includes(key)));
  writes = writes.catch(() => undefined).then(async () => {
    if (await SecureStore.isAvailableAsync()) {
      await SecureStore.setItemAsync(KEY, raw, { keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK });
    }
  }).catch(() => undefined);
  return writes;
}
export function useCollapsedSections() {
  const [collapsed, setCollapsed] = useState<string[]>([]);
  const current = useRef<string[]>([]);
  const touched = useRef(false);
  useEffect(() => {
    let alive = true;
    void writes.then(loadCollapsed).then((saved) => {
      if (alive && !touched.current) { current.current = saved; setCollapsed(saved); }
    });
    return () => { alive = false; };
  }, []);
  const toggle = (key: string) => {
    if (!keys.includes(key)) return;
    touched.current = true;
    const next = current.current.includes(key) ? current.current.filter((item) => item !== key) : [...current.current, key];
    current.current = next;
    setCollapsed(next);
    void saveCollapsed(next);
  };
  return { collapsed, toggle };
}
