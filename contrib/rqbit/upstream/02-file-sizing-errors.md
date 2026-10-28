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
# upstream main f4ca7d91 + this commit; Linux x86_64, Rust 1.97.1
cargo fmt --all -- --check     # ok
cargo clippy --all-targets     # no warnings (also clean with Rust 1.94)
cargo test                     # 170 passed, 0 failed (main alone: 168 passed, 0 failed)
```
The new tests (`file_sizing_error_stops_initialization` in `librqbit`, `pause_race_preserves_non_cancellation` in `librqbit`) are part of that run and pass.

What the new tests cover:

- `file_sizing_error_stops_initialization`: a storage whose `ensure_file_length` returns `StorageFull` for file 0. The helper returns an error mentioning the file name, the length and the cause, and the storage is called exactly once.
- `pause_race_preserves_non_cancellation_initialization_errors`: with a pause requested, a `StorageFull` error is not suppressed while `InitialCheckPaused` is; without a pause requested neither is.

## Related upstream work
- Builds on the behaviour of 5a52e43f ("Allow pausing during the initial file check"); see above.
- I'm not aware of an open PR for the sizing path.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. Human review and editing remain pending. The submitter must understand each change and be able to answer for it before submission.
