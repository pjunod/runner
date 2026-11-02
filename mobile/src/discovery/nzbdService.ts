export interface NetworkService {
  readonly name: string;
  readonly type: string;
  readonly domain: string;
  readonly hostName: string | null;
  readonly addresses: string[];
  readonly port: number;
  readonly txt: Record<string, string>;
}

export interface DiscoveredAddress {
  host: string;
  baseUrl: string;
  kind: 'IPv4' | 'IPv6' | 'Hostname';
}

export interface DiscoveredNzbd {
  key: string;
  name: string;
  baseUrl: string;
  addresses: DiscoveredAddress[];
  host: string;
  port: number;
  auth: string;
  version?: string;
}

export function isNzbdServiceType(type: string): boolean {
  const normalized = type.trim().toLowerCase().replace(/\.local\.$/, '.');
  return normalized === 'nzbd' || normalized === '_nzbd._tcp' || normalized === '_nzbd._tcp.';
}

export function serviceKey(service: Pick<NetworkService, 'name' | 'type' | 'domain'>): string {
  return `${service.name}|${service.type}|${service.domain}`;
}

export function toDiscoveredNzbd(service: NetworkService): DiscoveredNzbd | null {
  if (!Number.isInteger(service.port) || service.port < 1 || service.port > 65535) {
    return null;
  }

  const hosts = selectHosts(service);
  const host = hosts[0];
  if (!host) {
    return null;
  }
  const tls = ['1', 'true', 'yes'].includes((service.txt.tls ?? '').toLowerCase());
  const addresses = hosts.map((host): DiscoveredAddress => ({
    host,
    baseUrl: `${tls ? 'https' : 'http'}://${host.includes(':') ? `[${host}]` : host}:${service.port}`,
    kind: host.includes(':') ? 'IPv6' : /^\d+\.\d+\.\d+\.\d+$/.test(host) ? 'IPv4' : 'Hostname',
  }));

  return {
    key: serviceKey(service),
    name: service.name || 'Runner',
    baseUrl: addresses[0].baseUrl,
    addresses,
    host,
    port: service.port,
    auth: service.txt.auth || 'unknown',
    version: service.txt.version || undefined,
  };
}

function selectHosts(service: NetworkService): string[] {
  const addresses = [...new Set(service.addresses.map(usableHost).filter(Boolean) as string[])];
  const ipv4 = addresses.filter((host) => !host.includes(':') && !host.startsWith('169.254.'));
  const ipv6 = addresses.filter((host) => host.includes(':'));
  const hostname = usableHost(service.hostName);
  return [...new Set([...ipv4, ...ipv6, ...(hostname ? [hostname] : []), ...addresses])];
}

function usableHost(value: string | null): string | null {
  let host = (value ?? '').trim().replace(/^\[/, '').replace(/\]$/, '').replace(/\.$/, '');
  if (!host) return null;

  // Android can attach an interface scope even to a global IPv6 address.
  // Link-local addresses require their scope, which URL/fetch cannot represent.
  // Fall back to another advertised address rather than dropping that scope.
  if (host.includes(':')) {
    if (/^fe[89ab][0-9a-f]:/i.test(host)) return null;
    host = host.split('%', 1)[0];
  }
  try {
    const parsed = new URL(`http://${host.includes(':') ? `[${host}]` : host}`);
    // Require a host only, even when it came from native discovery metadata.
    if (
      parsed.port || parsed.username || parsed.password ||
      parsed.pathname !== '/' || parsed.search || parsed.hash
    ) {
      return null;
    }
    const canonical = parsed.hostname.replace(/^\[/, '').replace(/\]$/, '');
    if (
      canonical === '0.0.0.0' || canonical.startsWith('127.') || canonical === 'localhost' ||
      canonical === '::' || canonical === '::1' || /^ff[0-9a-f]{2}:/i.test(canonical)
    ) {
      return null;
    }
    return canonical;
  } catch {
    return null;
  }
}
