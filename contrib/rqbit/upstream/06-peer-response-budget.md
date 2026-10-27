## Summary
Bound the piece and metadata responses queued for one peer, using the per-peer request window (`max_request_window`) as the limit, and advertise it as `reqq` in our extended handshake.

## Motivation
When a peer sends a request, the response goes onto unbounded channels (the upload rate-limit scheduler, then the peer's writer) with nothing limiting how many can be outstanding. A peer that sends requests faster than its socket drains them, or never reads at all, makes us queue responses (each one a chunk to read from disk and write) without limit. We also don't tell peers how many requests we accept (`reqq`).

## Change
- `PeerResponseBudget`: a per-connection semaphore whose permits travel with each queued piece or metadata response and are released only after the writer has flushed it to the socket (or the response is dropped). `WriterRequest::{UtMetadata, ReadChunkRequest}` carry the permit.
- Admission uses `try_acquire`: a peer that already has more responses pending than the budget is disconnected rather than parking the reader task behind the torrent-wide upload limiter.
- The budget is the peer's request window, i.e. `max_request_window` from #687 (default 128, unchanged), and is advertised as `reqq` in the extended handshake so well-behaved clients stay inside it. rqbit didn't advertise `reqq` before.

## Compatibility and defaults
- With the default window the budget is 128 per peer.
- One thing to decide: this ties the number of requests we accept from a peer to the number we are willing to have outstanding towards a peer. If `max_request_window` is set very low (for example for seek responsiveness when streaming), the budget and the advertised `reqq` shrink with it, and a peer that ignores `reqq` and keeps more requests in flight than that is disconnected. If you would rather keep the two independent, the budget can be its own option with 128 as the default; the rest of the change stays the same.
- Peers now see a `reqq` in our handshake.

## Tests
```
# upstream main f4ca7d91 + this commit; Linux x86_64, Rust 1.97.1
cargo fmt --all -- --check     # ok
cargo clippy --all-targets     # no warnings (also clean with Rust 1.94)
cargo test                     # 170 passed, 0 failed (main alone: 168 passed, 0 failed)
```
The new tests (`peer_response_budget_tests` in `librqbit`, `peer_response_writer_tests` in `librqbit`) are part of that run and pass.

What the new tests cover:

- `production_admission_spans_scheduler_and_writer_queues`: the permit is held from admission through the scheduler and writer queues; the budget is exhausted at exactly the limit, and a metadata response takes a permit too; releasing the permit reopens it; the advertised `reqq` equals the limit.
- `production_writer_holds_permit_until_socket_write_finishes`: the writer keeps the permit while a write is pending and releases it when the write future is cancelled.

The e2e transfer tests (second form above) also exercise the upload path with the permits.

## Related upstream work
- #687 (merged): the budget is derived from the request window it made configurable.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. I have reviewed every line and can answer for it.
