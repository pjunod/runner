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
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                       # ok
cargo check                                      # ok
cargo check --workspace --exclude rqbit-desktop  # ok
cargo clippy --all-targets                       # no warnings
cargo test -p librqbit --lib                     # 48 passed, 4 failed, 5 ignored
cargo test -p librqbit peer_semaphore_tests      # 3 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                     # 52 passed, 0 failed, 5 ignored
cargo test -p librqbit peer_semaphore_tests      # 3 passed, 0 failed, 0 ignored
```

Without IPv6, `cargo test -p librqbit --lib` fails 4 tests: `test_e2e_download_duplicate_pad_filenames`, `test_e2e_download_tcp`, `test_e2e_download_utp`, `test_e2e_stream`. They fail while creating a `Session` / UDP tracker client with `error creating socket: Address family not supported by protocol (os error 97)`, because `UdpTrackerClient::new` binds `[::]` (see #707). The four e2e tests fail the same way on unmodified main (45 passed, 4 failed there).

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

What the new tests cover:

Three tests in `peer_semaphore_tests`: the per-torrent limit is exact and releases; two torrents share one session pool (the fourth peer is rejected and admitted after a release); a waiting outgoing acquisition resumes when the shared permit is dropped. The e2e tests also exercise the changed accept/connect paths with the default (unset) limit.

## Related upstream work
- No open PR touches this. A follow-up PR bounding retained peer *records* (`known_peer_limit`) is stacked on this one.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
