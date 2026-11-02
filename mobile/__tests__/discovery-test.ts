import { normalizeServerUrl } from '../src/api/format';

import {
  isNzbdServiceType,
  serviceKey,
  toDiscoveredNzbd,
} from '../src/discovery/nzbdService';

const service = {
  name: 'nzbd on studio',
  type: '_nzbd._tcp.',
  domain: 'local.',
  hostName: 'studio.local.',
  addresses: ['fe80::1', '169.254.9.219', '192.168.1.42'],
  port: 6789,
  txt: { path: '/api/v1', tls: '0', auth: 'bearer', version: '0.2.0' },
};

test('turns an nzbd service into a reachable connection', () => {
  expect(toDiscoveredNzbd(service)).toEqual({
    key: 'nzbd on studio|_nzbd._tcp.|local.',
    name: 'nzbd on studio',
    baseUrl: 'http://192.168.1.42:6789',
    addresses: [
      { host: '192.168.1.42', baseUrl: 'http://192.168.1.42:6789', kind: 'IPv4' },
      { host: 'studio.local', baseUrl: 'http://studio.local:6789', kind: 'Hostname' },
      { host: '169.254.9.219', baseUrl: 'http://169.254.9.219:6789', kind: 'IPv4' },
    ],
    host: '192.168.1.42',
    port: 6789,
    auth: 'bearer',
    version: '0.2.0',
  });
});

test('uses TLS metadata and brackets IPv6 addresses', () => {
  expect(
    toDiscoveredNzbd({
      ...service,
      addresses: ['2001:db8::8'],
      txt: { tls: '1', auth: 'none' },
    })?.baseUrl,
  ).toBe('https://[2001:db8::8]:6789');
});

test('falls back to the advertised hostname and rejects invalid ports', () => {
  expect(toDiscoveredNzbd({ ...service, addresses: [] })?.host).toBe('studio.local');
  expect(toDiscoveredNzbd({ ...service, addresses: ['127.0.0.1', '::1'] })?.host).toBe(
    'studio.local',
  );
  expect(toDiscoveredNzbd({ ...service, port: 0 })).toBeNull();
  expect(serviceKey(service)).toBe('nzbd on studio|_nzbd._tcp.|local.');
});

test('accepts native DNS-SD type variants for nzbd', () => {
  expect(isNzbdServiceType('_nzbd._tcp.')).toBe(true);
  expect(isNzbdServiceType('_NZBD._tcp.local.')).toBe(true);
  expect(isNzbdServiceType('nzbd')).toBe(true);
  expect(isNzbdServiceType('_http._tcp.')).toBe(false);
});


test('offers both IP families and the hostname, deduplicated and ready to connect', () => {
  const result = toDiscoveredNzbd({
    ...service,
    addresses: ['fe80::1%wlan0', 'fd12::8%wlan0', '192.168.1.42', '192.168.1.42'],
  })!;
  expect(result.addresses).toEqual([
    { host: '192.168.1.42', baseUrl: 'http://192.168.1.42:6789', kind: 'IPv4' },
    { host: 'fd12::8', baseUrl: 'http://[fd12::8]:6789', kind: 'IPv6' },
    { host: 'studio.local', baseUrl: 'http://studio.local:6789', kind: 'Hostname' },
  ]);
  for (const address of result.addresses) {
    expect(normalizeServerUrl(address.baseUrl)).toBe(address.baseUrl);
  }
});

test.each(['2001:db8::8%wlan0', 'fd12::8%7'])('uses IPv6-only service %s without an unnecessary scope', (host) => {
  const result = toDiscoveredNzbd({ ...service, addresses: [host], hostName: null })!;
  expect(result.addresses).toHaveLength(1);
  expect(result.addresses[0].kind).toBe('IPv6');
  expect(result.baseUrl).not.toContain('%');
  expect(normalizeServerUrl(result.baseUrl)).toBe(result.baseUrl);
});

test.each(['fe80::1%wlan0', 'fe90::1%4', 'FEA0::1', 'febf::1%25wlan0'])('uses the hostname instead of an unsupported link-local URL for %s', (host) => {
  expect(toDiscoveredNzbd({ ...service, addresses: [host] })?.baseUrl).toBe('http://studio.local:6789');
  expect(toDiscoveredNzbd({ ...service, addresses: [host], hostName: host })).toBeNull();
});

test.each(['::', '0:0:0:0:0:0:0:1', '127.0.0.2', '0.0.0.0', 'ff02::1'])('never offers unusable server address %s even as a fallback', (host) => {
  expect(toDiscoveredNzbd({ ...service, addresses: [host], hostName: null })).toBeNull();
});


test('all offered URLs also work with the native Expo URL parser', () => {
  const NativeURL = require('whatwg-url-minimum').URL;
  const originalURL = global.URL;
  global.URL = NativeURL;
  try {
    const result = toDiscoveredNzbd({
      ...service,
      addresses: ['fe80::1%wlan0', 'fd12::8%wlan0', '2001:db8::8', '192.168.1.42'],
    })!;
    expect(result.addresses.map((address) => address.kind)).toEqual(['IPv4', 'IPv6', 'IPv6', 'Hostname']);
    for (const address of result.addresses) {
      expect(normalizeServerUrl(address.baseUrl)).toBe(address.baseUrl);
    }
  } finally {
    global.URL = originalURL;
  }
});
