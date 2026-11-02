import { fetch } from 'expo/fetch';

import { NzbdClient } from '../src/api/client';

jest.mock('expo/fetch', () => ({ fetch: jest.fn() }));

const mockedFetch = fetch as jest.MockedFunction<typeof fetch>;

function okResponse(): Awaited<ReturnType<typeof fetch>> {
  return {
    ok: true,
    status: 200,
    text: async () => '{}',
  } as Awaited<ReturnType<typeof fetch>>;
}

describe('NzbdClient authentication', () => {
  beforeEach(() => mockedFetch.mockReset());

  it('encodes non-ASCII Basic credentials as UTF-8 before base64', async () => {
    mockedFetch.mockResolvedValue(okResponse());
    const client = new NzbdClient({
      baseUrl: 'https://nzbd.example.test',
      username: 'møbiłe',
      password: 'päss—🔒',
      token: '',
    });

    await client.getStatus();

    const init = mockedFetch.mock.calls[0]?.[1];
    const headers = init?.headers as Record<string, string>;
    expect(headers.authorization).toBe('Basic bcO4YmnFgmU6cMOkc3PigJTwn5SS');
  });

  it('keeps bearer authentication ahead of stored Basic credentials', async () => {
    mockedFetch.mockResolvedValue(okResponse());
    const client = new NzbdClient({
      baseUrl: 'https://nzbd.example.test',
      username: 'ignored',
      password: 'ignored',
      token: 'operator-token',
    });

    await client.getStatus();

    const init = mockedFetch.mock.calls[0]?.[1];
    const headers = init?.headers as Record<string, string>;
    expect(headers.authorization).toBe('Bearer operator-token');
  });
});

test('seed policy uses the existing authenticated PUT endpoint with only accepted fields', async () => {
  mockedFetch.mockReset();
  mockedFetch.mockResolvedValue(okResponse());
  const client = new NzbdClient({ baseUrl: 'https://nzbd.example.test', username: '', password: '', token: 'operator-token' });
  const policy = { use_defaults: false, stop_on_complete: true, ratio_limit: null, time_limit_secs: null };
  await client.setSeedPolicy(7, policy);
  expect(mockedFetch.mock.calls[0][0]).toBe('https://nzbd.example.test/api/v1/jobs/7/torrent/seed-policy');
  expect(mockedFetch.mock.calls[0][1]).toMatchObject({ method: 'PUT', body: JSON.stringify(policy) });
});

test('current retained-source preference uses GET and authenticated PUT after queue retirement', async () => {
  mockedFetch.mockReset(); mockedFetch.mockResolvedValue(okResponse());
  const client = new NzbdClient({ baseUrl: 'https://nzbd.example.test', username: '', password: '', token: 'operator-token' });
  await client.getFailureKeep(42); await client.setFailureKeep(42, true);
  expect(mockedFetch.mock.calls[0][0]).toBe('https://nzbd.example.test/api/v1/jobs/42/failure-keep');
  expect(mockedFetch.mock.calls[1][1]).toMatchObject({ method: 'PUT', body: JSON.stringify({ keep: true }) });
});


test('exact lifecycle lookup bounds the body and refuses mismatched receipt', async () => {
  const client = new NzbdClient({ baseUrl: 'https://nzbd.example.test', username: '', password: '', token: '' });
  const encoder = new TextEncoder();
  const streamed = (data: Uint8Array) => { let read = false; return { ok: true, body: { getReader: () => ({ read: async () => read ? { done: true } : (read = true, { done: false, value: data }), cancel: jest.fn(), releaseLock: jest.fn() }) } }; };
  mockedFetch.mockReset(); mockedFetch.mockResolvedValueOnce(streamed(encoder.encode(JSON.stringify({ deliveryId: 'receipt-a' }))) as any);
  expect((await client.getDelivery('receipt-a')).deliveryId).toBe('receipt-a');
  expect(mockedFetch.mock.calls[0][0]).toBe('https://nzbd.example.test/api/v1/deliveries/receipt-a');
  mockedFetch.mockResolvedValueOnce(streamed(encoder.encode(JSON.stringify({ deliveryId: 'wrong' }))) as any);
  await expect(client.getDelivery('receipt-a')).rejects.toThrow('different publication');
  mockedFetch.mockResolvedValueOnce(streamed(new Uint8Array(8 * 1024 * 1024 + 1)) as any);
  await expect(client.getDelivery('receipt-a')).rejects.toThrow('exceeds supported display size');
});

test('202 Keep response remains pending even if an optional body omits application state', async () => {
  mockedFetch.mockReset(); mockedFetch.mockResolvedValue(Object.assign(okResponse(), { status: 202 }));
  const client = new NzbdClient({ baseUrl: 'https://nzbd.example.test', username: '', password: '', token: '' });
  expect((await client.setFailureKeep(42, true)).applied).toBe(false);
});

test.each(['timeout', 'external', 'already-aborted'])('request composes %s abort with its own deadline', async mode => {
  jest.useFakeTimers();
  try {
    const external = new AbortController();
    const remove = jest.spyOn(external.signal, 'removeEventListener');
    if (mode === 'already-aborted') external.abort();
    mockedFetch.mockReset();
    mockedFetch.mockImplementation(async (_url, init) => new Promise((_resolve, reject) => {
      const signal = init!.signal!;
      if (signal.aborted) reject(new Error('aborted'));
      else signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
    }));
    const client = new NzbdClient({ baseUrl: 'http://test', username: '', password: '', token: '' });
    const request = client.getJobs(external.signal);
    const rejected = expect(request).rejects.toThrow();
    if (mode === 'external') external.abort();
    if (mode === 'timeout') await jest.advanceTimersByTimeAsync(12_000);
    await rejected;
    expect(mockedFetch.mock.calls[0][1]!.signal).not.toBe(external.signal);
    expect(mockedFetch.mock.calls[0][1]!.signal!.aborted).toBe(true);
    expect(remove).toHaveBeenCalledWith('abort', expect.any(Function));
  } finally { jest.useRealTimers(); }
});

test('pending jobs body remains bounded after headers arrive', async () => {
  jest.useFakeTimers();
  try {
    mockedFetch.mockReset();
    mockedFetch.mockImplementation(async (_url, init) => ({ok:true,status:200,text:()=>new Promise((_resolve,reject)=>{
      init!.signal!.addEventListener('abort',()=>reject(new Error('aborted')),{once:true});
    })} as any));
    const client = new NzbdClient({ baseUrl: 'http://test', username: '', password: '', token: '' });
    const pending = client.getJobs(new AbortController().signal);
    const rejected = expect(pending).rejects.toThrow('12 seconds');
    await jest.advanceTimersByTimeAsync(12_000);
    await rejected;
  } finally { jest.useRealTimers(); }
});
test('bounded JSON parsing preserves provider HTTP error status', async () => {
  mockedFetch.mockReset(); mockedFetch.mockResolvedValue({ok:false,status:409,text:async()=>JSON.stringify({error:'Source changed'})} as any);
  const client = new NzbdClient({ baseUrl: 'http://test', username: '', password: '', token: '' });
  await expect(client.getJobs()).rejects.toMatchObject({status:409,message:'Source changed'});
});
