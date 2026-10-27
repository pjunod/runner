## Summary
Bound the number of retained peer records per torrent and per session (`known_peer_limit`, `known_peer_limit_total`), and fix two cases where records outlive their usefulness. Builds on the session-wide live-peer cap (`peer_limit_total`), which has to land first.

## Motivation
The live-peer semaphore limits connected peers, but every distinct address learned from trackers, DHT, PEX, explicit input or an incoming connection is inserted into the torrent's peer map (and the peer-adder queue) first. Queued, backing-off, dead and not-needed records hold no live-peer permit, so a long-running session can retain an unbounded number of records even with `peer_limit` set.

## Change
- `SessionOptions::{known_peer_limit, known_peer_limit_total}` and `AddTorrentOptions::known_peer_limit`. `None` is unlimited (today's behaviour) and `0` rejects every new record.
- A new record takes a permit from each configured limit and keeps it inside the `Peer` until the record leaves the map. Discovered/explicit outgoing addresses and new incoming addresses use the same admission path; an existing record that reconnects doesn't need another permit. A full limit means the address is ignored (like a duplicate) or, for an incoming connection, the new `AddIncomingPeerResult::KnownPeerLimitReached`.
- Two fixes that apply whether or not a limit is set, because otherwise a limit fills up with dead entries:
  - a record created by an incoming connection (it has no outgoing address to retry) is removed when its connection ends, instead of staying in `Dead`/`NotNeeded` forever;
  - `reconnect_not_needed_peer` now moves a `NotNeeded` record to `Queued` before returning it, also when its outgoing address differs from the record handle. Before, that case stayed `NotNeeded`, so every reconnect trigger enqueued the same peer again. The peer queue now carries `(record handle, outgoing address)`.
- The `rqbit` CLI's `SessionOptions` literal gets `None` for both new limits (no CLI flags).

## Compatibility and defaults
- Both limits default to `None`: no limit.
- Visible even without limits: incoming-only peer records disappear from the peer list when their connection closes, and the duplicate-enqueue in the alternate-address case is gone.
- The limits are read at session / torrent construction; no live resizing, and no eviction (a full map refuses new records).

## Tests
```
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                       # ok
cargo check                                      # ok
cargo check --workspace --exclude rqbit-desktop  # ok
cargo clippy --all-targets                       # no warnings
cargo test -p librqbit --lib                     # 53 passed, 4 failed, 5 ignored
cargo test -p librqbit known_peer_budget_tests   # 5 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                     # 56 passed, 1 failed, 5 ignored
cargo test -p librqbit known_peer_budget_tests   # 5 passed, 0 failed, 0 ignored
```

Without IPv6, `cargo test -p librqbit --lib` fails 4 tests: `test_e2e_download_duplicate_pad_filenames`, `test_e2e_download_tcp`, `test_e2e_download_utp`, `test_e2e_stream`. They fail while creating a `Session` / UDP tracker client with `error creating socket: Address family not supported by protocol (os error 97)`, because `UdpTrackerClient::new` binds `[::]` (see #707). The four e2e tests fail the same way on unmodified main (45 passed, 4 failed there).

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

This builds on the session-wide live-peer cap (`peer_limit_total`); the commands above were run on top of it.

What the new tests cover:

Five tests in `known_peer_budget_tests`: the per-torrent limit is exact and released; two torrents share one session limit; peer records hold their slots until removed; a failed shared acquisition releases the local slot; an alternate outgoing address is queued once with the record handle. The existing `peers` tests were adjusted for the new field in `PeerStates`.

One run of the full `librqbit` lib suite (with #707 applied locally) had `test_e2e_download_tcp` fail with `wait_until_i_am_the_last_task` seeing 7 alive tasks; it passed in the 11 following runs of the whole suite and in 3 isolated runs. I haven't found a cause, so I mention it for completeness.

## Related upstream work
- **#667** (bound peers-map growth) addresses part of the same problem a different way: it evicts dead / not-needed entries once a torrent tracks 1024 peers. This PR instead refuses new records beyond configurable limits and releases capacity when a record is removed, and it adds a session-wide total. They touch the same code (`add_if_not_seen`, `add_incoming_peer`, and the peer-map tests), so whichever lands second needs a rebase. I'm happy to adapt this to #667 (for example keep only the session-wide total, the incoming-record cleanup and the requeue fix) or to rebase onto it.
- Builds on the `peer_limit_total` PR.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
