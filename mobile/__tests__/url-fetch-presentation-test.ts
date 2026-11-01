import { urlFetchDetail } from '../src/urlFetchPresentation';

const now = 1_793_539_000_000; // 2026-11-01T13:16:40Z

test('a fetch that has not failed says what it is doing', () => {
  expect(urlFetchDetail({ url_fetch: null }, now)).toBe('Requesting NZB from the indexer');
  expect(urlFetchDetail({}, now)).toBe('Requesting NZB from the indexer');
  expect(urlFetchDetail({ url_fetch: { attempts: 0 } }, now)).toBe('Requesting NZB from the indexer');
});

// The field case: a 429 with a 22-hour Retry-After, ten hours in.
test('a rate-limited fetch names the refusal, the wait and the next attempt', () => {
  const detail = urlFetchDetail(
    {
      url_fetch: {
        attempts: 1,
        blockedSinceUnix: 1_793_503_026,
        lastError: 'http: status 429 Too Many Requests',
        nextRetryAtUnix: 1_793_581_999,
      },
    },
    now,
  );
  expect(detail).toContain('Indexer refused: http: status 429 Too Many Requests');
  expect(detail).toContain('retrying in 11h 57m');
  expect(detail).toContain('waiting 10h ·');
  expect(detail).toContain('attempt 1');
});

test('a retry whose deadline has passed is retrying now', () => {
  const detail = urlFetchDetail({ url_fetch: { attempts: 3, nextRetryAtUnix: now / 1000 - 5 } }, now);
  expect(detail).toBe('Indexer refused · retrying now · attempt 3');
});
