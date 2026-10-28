# Expose credential-safe per-torrent tracker and DHT health

### Problem

`ManagedTorrent` exposes progress and peer facts, but an embedded caller cannot
tell whether a torrent has no peers because discovery is healthy and quiet or
because its trackers/DHT are disabled, searching, rejected, or backing off.
Session-wide DHT statistics do not answer that question for one torrent.

I would like to add a read-only per-torrent discovery snapshot. I am opening
the design issue before the implementation because the current prototype
touches `librqbit`, `librqbit-dht`, and `librqbit-tracker-comms`, and I would
rather align on the public contract than ask for review of a large patch with
the wrong boundary.

### Required facts

For DHT, an embedded caller needs to distinguish:

- disabled by session configuration;
- suppressed because the torrent is private;
- inactive or not running;
- searching, with no completed request yet;
- working, with a successful request in the current run; and
- degraded, with the latest request failing.

For each effective tracker, the caller needs to distinguish:

- inactive;
- announcing;
- working, with the next announce delay;
- backing off, with a retry delay and safe failure category; and
- rejected before use, such as an unsupported scheme.

Peer count cannot substitute for these states. Zero peers is a valid outcome
for a healthy announce, and a nonzero peer cache can outlive a later tracker
failure.

### Privacy boundary

The public snapshot must never contain a tracker path, query, user info,
passkey, response body, or arbitrary transport error. A safe endpoint can keep
only scheme, host, and explicit port. Failures should be bounded and
categorized so an embedding application can explain timeout, transport, HTTP
status, invalid response, tracker rejection, or unsupported scheme without
persisting secrets.

### Shape for discussion

The prototype uses a synchronous snapshot on `ManagedTorrent`, with DHT status
and current-run counters plus one ordered entry per tracker. This is only a
discussion starting point. I would appreciate guidance on:

1. whether the public boundary belongs on `ManagedTorrent`, an existing stats
   type, or a separate observable stream;
2. whether DHT request health should remain in `librqbit-dht` or be summarized
   only inside `librqbit`;
3. which tracker states and failure categories are stable enough to expose;
4. whether retry timing should be an absolute instant, a duration, or omitted;
   and
5. whether current-run request/peer counters are useful or too implementation
   specific.

I have a tested prototype against rqbit `main` snapshot
`4e5f94cbcf1d57ec500885c77cf1e24d70232d89` and an exact v8.1.1 backport
used to validate the embedding requirements. I can rebase it onto the current
upstream head and revise it to the preferred shape before opening a PR.

### AI assistance disclosure

OpenAI Codex assisted with research, prototyping, tests, and this draft.
