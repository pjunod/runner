## Summary
Bound the DHT worker's queues and the per-lookup concurrency (outgoing datagram queue, recursive-node queue, delivered-peer queue, bucket-refresh and ping queues; at most 32 concurrent requests per worker).

## Motivation
Several DHT queues are unbounded channels, and a lookup starts every request queued on it, so a slow UDP writer or a slow consumer of discovered peers lets work and memory pile up without a limit:
- the datagram queue feeding the socket writer;
- the per-lookup queues of nodes still to be queried and of peers found;
- the `FuturesUnordered` sets that run recursive requests, bucket refreshes and pings.

## Change
- Outgoing datagrams: a bounded queue of 256. A locally initiated request awaits a free slot *before* it allocates a transaction id and registers itself as in-flight. Replies and untracked announces are best-effort: when the queue is full they are dropped, because awaiting in the receive path would stop reading the socket.
- Recursive nodes: queue of 256, with at most 32 requests running per lookup worker (`BoundedFuturesUnordered` refuses a 33rd push, and the select loops stop receiving while it is full). A node dropped because the queue is full is found again by the periodic requery of the routing table. A full root queue is reported as `Saturated` (normal requery interval) rather than `Empty`, which would make the worker retry every second.
- Delivered peers: queue of 256, best-effort (a full queue drops the peer).
- Bucket refresh and questionable-node ping: queues of 256 and 32 concurrent requests per worker.
- Bootstrap fan-out is unchanged (its input is the finite configured list).
- The test module is at the end of the file.

## Compatibility and defaults
No API change. Under overload the DHT now drops work (new nodes, delivered peers, replies) instead of queueing it; under normal load nothing is dropped. The limits are constants in `dht.rs`; they can become options if you prefer.

## Tests
```
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                       # ok
cargo check                                      # ok
cargo check --workspace --exclude rqbit-desktop  # ok
cargo clippy --all-targets                       # no warnings
cargo test -p librqbit-dht --lib                 # 29 passed, 0 failed, 0 ignored
cargo test -p librqbit-dht queue_budget_tests    # 1 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                     # 49 passed, 0 failed, 5 ignored
cargo test -p librqbit-dht queue_budget_tests    # 1 passed, 0 failed, 0 ignored
```

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

`cargo test -p librqbit --lib` was only run in the second form (with #707 applied locally): without IPv6 its four e2e tests can't create a Session, as on unmodified main (see #707). No automated test drives a real lookup through the changed `RecursiveRequest` loops: the DHT tests that need the network (`#[ignore]`) were not run, and the e2e tests run with `dht: None`. What is covered is each queue/concurrency limit at its exact boundary, compilation, and the existing `librqbit-dht` unit tests.

What the new tests cover:

`queue_budget_tests::queue_budgets_close_at_exact_boundaries` checks each queue and each concurrency limit at its exact boundary (accepts N, rejects N+1, accepts again after one is consumed) and the `Saturated` delay.

## Related upstream work
- **#706** (bound the `get_peers` queue and the rate-limiter wait) solves part of the same problem in `request_peers_forever` and `DhtState::request` (32 in flight per search; a timeout on the rate-limiter wait). This PR also bounds that loop (32 concurrent, queue of 256) and so overlaps with it; it does not add the rate-limiter wait timeout. Whichever lands second needs a rebase; if #706 goes in first I'll drop the duplicated part and keep the rest.
- The LSD half of the original work is not included: e0e9348 already cancels the announce task, and #713 covers the registration handling.
- The metadata-resolver part is a separate PR.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
