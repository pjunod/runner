## Summary
Bound the number of retained peer records per torrent and per session (`known_peer_limit`, `known_peer_limit_total`), and fix two cases where records outlive their usefulness. Builds on the session-wide live-peer cap (`peer_limit_total`), which has to land first.

## Motivation
The live-peer semaphore limits connected peers, but every distinct address learned from trackers, DHT, PEX, explicit input or an incoming connection is inserted into the torrent's peer map (and the peer-adder queue) first. Queued, backing-off, dead and not-needed records hold no live-peer permit, so a long-running session can retain an unbounded number of records even with `peer_limit` set.

## Change
- `SessionOptions::{known_peer_limit, known_peer_limit_total}` and `AddTorrentOptions::known_peer_limit`. `None` is unlimited (today's behaviour) and `0` rejects every new record.
- A new record takes a permit from each configured limit and keeps it inside the `Peer` until the record leaves the map. Discovered/explicit outgoing addresses and new incoming addresses use the same admission path; an existing record that reconnects doesn't need another permit. A full limit means the address is ignored (like a duplicate) or, for an incoming connection, the new `AddIncomingPeerResult::KnownPeerLimitReached`.
- Two fixes that apply whether or not a limit is set, because otherwise a limit fills up with dead entries:
  - a record created by an incoming connection (it has no outgoing address to retry) is removed when its connection ends, instead of staying in `Dead`/`NotNeeded` forever. Removal conditionally matches a still-queued incoming-only record under the map lock, preserving a replacement connection admitted after the old handler releases its guard;
  - `reconnect_not_needed_peer` now moves a `NotNeeded` record to `Queued` before returning it, also when its outgoing address differs from the record handle. Before, that case stayed `NotNeeded`, so every reconnect trigger enqueued the same peer again. The peer queue now carries `(record handle, outgoing address)`.
- The `rqbit` CLI's `SessionOptions` literal gets `None` for both new limits (no CLI flags).

## Compatibility and defaults
- Both limits default to `None`: no limit.
- Visible even without limits: incoming-only peer records disappear from the peer list when their connection closes, and the duplicate-enqueue in the alternate-address case is gone.
- The limits are read at session / torrent construction; no live resizing, and no eviction (a full map refuses new records).

## Tests

New qualification on 2026-10-28 at commit `3d86ddf65c667e0e2bc65bd19011a48ba8c651b4`,
tree `6dacbddb8e3432c4860664ae2fed46598cc04255`, based on session-peer-limit
`0b5cae556103d23d425172beb60bbb118dbbf2f1` and upstream main `f4ca7d91`:

```bash
cargo fmt --all -- --check  # Passed on Rust 1.97.1.
cargo clippy --all-targets  # Passed without warnings.
cargo test                 # 177 passed, zero failed.
```

The new deterministic regression
`known_peer_budget_tests::old_incoming_disconnect_does_not_remove_replacement_connection`
failed with unconditional removal and passed after the atomic conditional
removal. It forces a replacement incoming connection between the old
handler's state release and record cleanup, then checks both preservation
and eventual slot release. The five original budget tests remain.

[The exact receipt](07-known-peer-limit-evidence.json) records source trees,
patch and log hashes. The earlier version's 176/0 suite, ten repeated
library passes and Rust 1.94 checks are inherited results; they were not
rerun for this correction and do not qualify its new bytes.

## Related upstream work
- **#667** (bound peers-map growth) addresses part of the same problem a different way: it evicts dead / not-needed entries once a torrent tracks 1024 peers. This PR instead refuses new records beyond configurable limits and releases capacity when a record is removed, and it adds a session-wide total. They touch the same code (`add_if_not_seen`, `add_incoming_peer`, and the peer-map tests), so whichever lands second needs a rebase. I'm happy to adapt this to #667 (for example keep only the session-wide total, the incoming-record cleanup and the requeue fix) or to rebase onto it.
- Builds on the `peer_limit_total` PR.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. Human review and editing remain pending. The submitter must understand each change and be able to answer for it before submission.
