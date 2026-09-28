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
