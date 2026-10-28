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
# upstream main f4ca7d91 + this commit; Linux x86_64, Rust 1.97.1
cargo fmt --all -- --check     # ok
cargo clippy --all-targets     # no warnings (also clean with Rust 1.94)
cargo test                     # 169 passed, 0 failed (main alone: 168 passed, 0 failed)
```
The new tests (`configured_metadata_limit` in `librqbit`) are part of that run and pass.

What the new tests cover:

New unit test `configured_metadata_limit_is_checked_before_allocation`: a size equal to the limit is accepted and one byte more is rejected with the expected message before a buffer is created.

## Related upstream work
- #704 (off-by-one in the `ut_metadata` bounds check) is on the serving side (`send_metadata_piece`); this change is on the receiving side and the two don't overlap.
- #687 added `max_request_window` right next to the new field in `PeerConnectionOptions`, so that struct and its merge in `session.rs` will need a trivial rebase if either lands second.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. Human review and editing remain pending. The submitter must understand each change and be able to answer for it before submission.
