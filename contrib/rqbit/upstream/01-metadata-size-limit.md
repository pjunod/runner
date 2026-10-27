## Summary
Add `PeerConnectionOptions::max_metadata_size: Option<u32>` so an application can cap the BEP 9 metadata size it accepts from a peer. The default (unset) keeps the existing 32 MiB ceiling.

## Motivation
When resolving a magnet link, the peer tells us the metadata size in its extended handshake. rqbit accepts anything up to a hardcoded 32 MiB and allocates a zeroed buffer of that size before it has seen any of the metainfo. An application that wants a tighter limit (most info dictionaries are far smaller) cannot enforce it today.

## Change
- New `max_metadata_size` field on `PeerConnectionOptions`. It goes through the same session/per-add merge as the other fields in that struct.
- `HandlerLocked::new` takes the limit and checks it before allocating the buffer. The check now also happens before we send `Unchoke`/`Interested` and the piece requests, so an oversized advertisement is dropped without starting the exchange.
- The error message reports the configured maximum (`metadata size N exceeds configured maximum M`) instead of `metadata size N is too big`.

## Compatibility and defaults
- `None` keeps the current 32 MiB limit, so default behaviour is unchanged.
- The field is optional, so existing serialized `PeerConnectionOptions` still deserialize.
- Only the size of metadata fetched from peers is affected. Piece sizes and metainfo validation are untouched.
- The only visible difference for existing users is the wording of the error for an oversized advertisement.

## Tests
```
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                        # ok
cargo check                                       # ok
cargo check --workspace --exclude rqbit-desktop   # ok
cargo clippy --all-targets                        # no warnings
cargo test -p librqbit --lib                      # 46 passed, 4 failed, 5 ignored
cargo test -p librqbit configured_metadata_limit  # 1 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                      # 50 passed, 0 failed, 5 ignored
cargo test -p librqbit configured_metadata_limit  # 1 passed, 0 failed, 0 ignored
```

Without IPv6, `cargo test -p librqbit --lib` fails 4 tests: `test_e2e_download_duplicate_pad_filenames`, `test_e2e_download_tcp`, `test_e2e_download_utp`, `test_e2e_stream`. They fail while creating a `Session` / UDP tracker client with `error creating socket: Address family not supported by protocol (os error 97)`, because `UdpTrackerClient::new` binds `[::]` (see #707). The four e2e tests fail the same way on unmodified main (45 passed, 4 failed there).

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

What the new tests cover:

New unit test `configured_metadata_limit_is_checked_before_allocation`: a size equal to the limit is accepted and one byte more is rejected with the expected message before a buffer is created.

## Related upstream work
- #704 (off-by-one in the `ut_metadata` bounds check) is on the serving side (`send_metadata_piece`); this change is on the receiving side and the two don't overlap.
- #687 added `max_request_window` right next to the new field in `PeerConnectionOptions`, so that struct and its merge in `session.rs` will need a trivial rebase if either lands second.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
