## Summary
Add an optional session-wide cap on live peers, `SessionOptions::peer_limit_total`, alongside the existing per-torrent `peer_limit`. Default `None`: unchanged.

## Motivation
`peer_limit` bounds the live peers of one torrent, but each torrent owns its own semaphore, so the number of peer tasks in a session grows linearly with the number of torrents. An application that manages many torrents can't put a ceiling on the total.

## Change
- `SessionOptions::peer_limit_total: Option<usize>`. When set, one semaphore is shared by all torrents of the session.
- A live peer now holds a `PeerPermit`, which owns its per-torrent permit and, if configured, a session permit, until the peer manager exits.
- Incoming peers try both limits without waiting and are dropped, as before, if either is full. Outgoing peers wait for both; the per-torrent permit is taken first and a torrent's single peer-adder task holds at most one permit while it waits for a session slot, so there is no lock-order problem between torrents.
- The existing 128 per-torrent fallback is kept.
- The `rqbit` CLI's `SessionOptions` literal gets `peer_limit_total: None` (no CLI flag).
- The test module is at the end of the file.

## Compatibility and defaults
`None` keeps today's behaviour. The limit is read when the session is constructed; there is no live resizing. It doesn't bound handshake work before an incoming connection has been routed to a torrent.

## Tests
```
# upstream main f4ca7d91 + this commit; Linux x86_64, Rust 1.97.1
cargo fmt --all -- --check     # ok
cargo clippy --all-targets     # no warnings (also clean with Rust 1.94)
cargo test                     # 171 passed, 0 failed (main alone: 168 passed, 0 failed)
```
The new tests (`peer_semaphore_tests` in `librqbit`) are part of that run and pass.

What the new tests cover:

Three tests in `peer_semaphore_tests`: the per-torrent limit is exact and releases; two torrents share one session pool (the fourth peer is rejected and admitted after a release); a waiting outgoing acquisition resumes when the shared permit is dropped. The e2e tests also exercise the changed accept/connect paths with the default (unset) limit.

## Related upstream work
- No open PR touches this. A follow-up PR bounding retained peer *records* (`known_peer_limit`) is stacked on this one.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. Human review and editing remain pending. The submitter must understand each change and be able to answer for it before submission.
