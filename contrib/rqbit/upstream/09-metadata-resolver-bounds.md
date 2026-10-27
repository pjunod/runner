## Summary
Bound the work retained while resolving a magnet's metadata: at most 128 connections running, 256 addresses waiting, and a 4096-entry de-duplication set.

## Motivation
`read_metainfo_from_peer_receiver` wraps each peer connection in a 128-permit semaphore, but it still creates and keeps one future per candidate address *before* the semaphore is taken, and keeps every unique address in `seen`. The semaphore limits the I/O in flight, not the number of queued futures or retained candidates, so a DHT/tracker stream that produces peers faster than they are tried grows without bound while a magnet is resolving.

## Change
- The semaphore is replaced by explicit queues: at most 128 metadata connections are running (a future is created only when a slot is free) and up to 256 further addresses wait in a pending queue. While both are full, the initial address list and the address stream are not polled, so producers see backpressure.
- `seen` stops growing at 4096 addresses. Beyond that, new addresses are still tried (they just aren't de-duplicated any more), so reaching the limit never ends or stalls the search; it is logged once at debug level.
- Initial addresses go through the same admission path as streamed ones.

## Compatibility and defaults
No API change. For ordinary magnets (a few hundred peers at most) the behaviour is the same as before; the difference shows only when more candidates arrive than can be tried.

## Tests
```
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                                                   # ok
cargo check                                                                  # ok
cargo check --workspace --exclude rqbit-desktop                              # ok
cargo clippy --all-targets                                                   # no warnings
cargo test -p librqbit --lib                                                 # 47 passed, 4 failed, 5 ignored
cargo test -p librqbit initial_and_streamed_candidates_are_connected_to      # 1 passed, 0 failed, 0 ignored
cargo test -p librqbit production_metadata_queues_close_at_exact_boundaries  # 1 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                                                 # 51 passed, 0 failed, 5 ignored
cargo test -p librqbit initial_and_streamed_candidates_are_connected_to      # 1 passed, 0 failed, 0 ignored
cargo test -p librqbit production_metadata_queues_close_at_exact_boundaries  # 1 passed, 0 failed, 0 ignored
```

Without IPv6, `cargo test -p librqbit --lib` fails 4 tests: `test_e2e_download_duplicate_pad_filenames`, `test_e2e_download_tcp`, `test_e2e_download_utp`, `test_e2e_stream`. They fail while creating a `Session` / UDP tracker client with `error creating socket: Address family not supported by protocol (os error 97)`, because `UdpTrackerClient::new` binds `[::]` (see #707). The four e2e tests fail the same way on unmodified main (45 passed, 4 failed there).

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

What the new tests cover:

- `production_metadata_queues_close_at_exact_boundaries`: the active/pending queue and the de-duplication set at their exact boundaries (a duplicate is reported as such, an address beyond the set limit is `Untracked`).
- `initial_and_streamed_candidates_are_connected_to`: with loopback listeners, both an initial and a streamed candidate are actually connected to. I added it because the first version of this change enqueued candidates inside a `debug_assert!`, which would silently do nothing in release builds; the test fails if the enqueue is removed.

## Related upstream work
- **#708** (retry failed metadata peers) changes the same function in `dht_utils.rs`, so there will be a conflict whichever lands second; the two are compatible in intent (this PR bounds what is retained, #708 retries peers that failed) and I'm happy to rebase.
- The DHT-queue part is a separate PR. The LSD part of the original work is dropped (see that PR).

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
