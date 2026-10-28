## Summary
Add `SessionOptions::disable_auto_restore` (default `false`). With persistence still enabled, the session constructor then doesn't re-add the stored torrents; the application adds the ones it wants.

## Motivation
When persistence is configured, `Session::new_with_opts` reads every stored torrent and adds it back before returning. An application that keeps its own queue (and wants to decide which torrents come back, in which order and with which options) has no way to opt out while still using persistence and fast-resume; the choice is all-or-nothing, or no persistence.

## Change
- New `SessionOptions::disable_auto_restore`. When true, the restore loop is skipped, but persistence stays on, so torrents added afterwards are still stored and fast-resume data is still used.
- The caller re-adds what it wants with `add_torrent`; `AddTorrentOptions::preferred_id` already lets it keep the stored torrent id so the persisted record is picked up again.
- The `rqbit` CLI's `SessionOptions` literal gets `disable_auto_restore: false`. I didn't add a CLI flag.

## Compatibility and defaults
Default `false` keeps today's behaviour. The option is read when the session is constructed.

## Tests
```
# upstream main f4ca7d91 + this commit; Linux x86_64, Rust 1.97.1
cargo fmt --all -- --check     # ok
cargo clippy --all-targets     # no warnings (also clean with Rust 1.94)
cargo test                     # 169 passed, 0 failed (main alone: 168 passed, 0 failed)
```
The new tests (`persistence_can_skip_implicit_admission` in `librqbit`) are part of that run and pass.

What the new tests cover:

New test `persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset` creates two persisted torrents (one partially downloaded), then reopens the same persistence folder three times: with `disable_auto_restore` (no torrents are admitted); re-adding one torrent by `preferred_id` (it resumes from the stored piece state; the file on disk is changed in between, so a re-check would give a different result); and with the default (both come back). The test needs `Session` creation to work, so it needs IPv6 or #707.

## Related upstream work
- #707 (UDP tracker socket falls back to IPv4): the new test and the e2e tests need it on a machine without IPv6.
- #682 and #708 each touch a line next to my one-line change in `crates/rqbit/src/main.rs` / `crates/librqbit/src/tests/mod.rs`; trivial rebase if they land first.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. Human review and editing remain pending. The submitter must understand each change and be able to answer for it before submission.
