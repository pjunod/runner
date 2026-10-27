## Summary
Fail torrent initialization when a selected file cannot be sized, instead of logging a warning and carrying on. Also make the "pause during initial check" error handling distinguish a real pause from a real error.

## Motivation
During initialization rqbit calls `TorrentStorage::ensure_file_length` for each selected file. When that fails (ENOSPC, EDQUOT, read-only filesystem, ...) the error is logged with `warn!` and the loop continues, so the torrent still reaches the paused/live state as if sizing had worked. The only evidence is a log line; stats and the torrent state never show it, and an application can't tell that the torrent is not really usable.

## Change
- The sizing loop moves into a small fallible helper, `ensure_selected_file_lengths`. It keeps the existing file selection and skips padding files exactly as before, returns the first failure with the file name and requested length as context, and does not touch later files. The existing error path in `ManagedTorrent::start` then puts the torrent into the `Error` state.
- Interaction with "Allow pausing during the initial file check": `ManagedTorrent::start` treats *any* initialization error as a pause when a pause was requested (`if init.is_pause_requested()`). With the change above, a sizing failure that races with a pause would be swallowed there. The checksum loop now returns a typed `InitialCheckPaused` error instead of `bail!("initial check paused")`, and only that error is suppressed when a pause was requested; anything else still moves the torrent to `Error`.

## Compatibility and defaults
- Successful initialization is unchanged: same files, same order, same padding handling.
- Behaviour change for the failure case, which is the point of the fix: a torrent whose file cannot be sized now ends in the `Error` state with the cause in the message, rather than continuing after a warning.
- No new public API.

## Tests
```
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                                     # ok
cargo check                                                    # ok
cargo check --workspace --exclude rqbit-desktop                # ok
cargo clippy --all-targets                                     # no warnings
cargo test -p librqbit --lib                                   # 47 passed, 4 failed, 5 ignored
cargo test -p librqbit file_sizing_error_stops_initialization  # 1 passed, 0 failed, 0 ignored
cargo test -p librqbit pause_race_preserves_non_cancellation   # 1 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                                   # 51 passed, 0 failed, 5 ignored
cargo test -p librqbit file_sizing_error_stops_initialization  # 1 passed, 0 failed, 0 ignored
cargo test -p librqbit pause_race_preserves_non_cancellation   # 1 passed, 0 failed, 0 ignored
```

Without IPv6, `cargo test -p librqbit --lib` fails 4 tests: `test_e2e_download_duplicate_pad_filenames`, `test_e2e_download_tcp`, `test_e2e_download_utp`, `test_e2e_stream`. They fail while creating a `Session` / UDP tracker client with `error creating socket: Address family not supported by protocol (os error 97)`, because `UdpTrackerClient::new` binds `[::]` (see #707). The four e2e tests fail the same way on unmodified main (45 passed, 4 failed there).

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

What the new tests cover:

- `file_sizing_error_stops_initialization`: a storage whose `ensure_file_length` returns `StorageFull` for file 0. The helper returns an error mentioning the file name, the length and the cause, and the storage is called exactly once.
- `pause_race_preserves_non_cancellation_initialization_errors`: with a pause requested, a `StorageFull` error is not suppressed while `InitialCheckPaused` is; without a pause requested neither is.

## Related upstream work
- Builds on the behaviour of 5a52e43f ("Allow pausing during the initial file check"); see above.
- I'm not aware of an open PR for the sizing path.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
