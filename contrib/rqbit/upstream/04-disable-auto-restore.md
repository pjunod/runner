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
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                                      # ok
cargo check                                                     # ok
cargo check --workspace --exclude rqbit-desktop                 # ok
cargo clippy --all-targets                                      # no warnings
cargo test -p librqbit --lib                                    # 45 passed, 5 failed, 5 ignored
cargo test -p librqbit persistence_can_skip_implicit_admission  # 0 passed, 1 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                                    # 50 passed, 0 failed, 5 ignored
cargo test -p librqbit persistence_can_skip_implicit_admission  # 1 passed, 0 failed, 0 ignored
```

Without IPv6, `cargo test -p librqbit --lib` fails 5 tests: `persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset`, `test_e2e_download_duplicate_pad_filenames`, `test_e2e_download_tcp`, `test_e2e_download_utp`, `test_e2e_stream`. They fail while creating a `Session` / UDP tracker client with `error creating socket: Address family not supported by protocol (os error 97)`, because `UdpTrackerClient::new` binds `[::]` (see #707). The four e2e tests fail the same way on unmodified main (45 passed, 4 failed there).

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

What the new tests cover:

New test `persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset` creates two persisted torrents (one partially downloaded), then reopens the same persistence folder three times: with `disable_auto_restore` (no torrents are admitted); re-adding one torrent by `preferred_id` (it resumes from the stored piece state; the file on disk is changed in between, so a re-check would give a different result); and with the default (both come back). The test needs `Session` creation to work, so it needs IPv6 or #707.

## Related upstream work
- #707 (UDP tracker socket falls back to IPv4): the new test and the e2e tests need it on a machine without IPv6.
- #682 and #708 each touch a line next to my one-line change in `crates/rqbit/src/main.rs` / `crates/librqbit/src/tests/mod.rs`; trivial rebase if they land first.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
