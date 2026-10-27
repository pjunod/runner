## Summary
Bound HTTP tracker responses (1 MiB, 30 s) and clamp the announce interval a tracker asks for to at least 60 s. `force_tracker_interval` keeps working as an override. This changes behaviour, so I would like your view on the constants.

## Motivation
A tracker is untrusted input, and each torrent runs a long-lived monitor task per tracker:
- the HTTP response body is read with `Response::bytes()`, so it is buffered without a size limit;
- the request has no deadline of its own, so a tracker that stalls mid-response holds the task forever;
- the announce interval comes from the response: HTTP accepts any value, including 0 (a tight announce loop), and UDP is only clamped to 5 s.

## Change
- `fetch_http_tracker_response` streams the body into a checked buffer with a 1 MiB limit (an oversized `Content-Length` is rejected up front and every chunk is re-checked, so a missing or false header doesn't bypass it) and wraps the whole request and body read in a 30 s timeout.
- `normalized_tracker_interval` clamps tracker-supplied intervals to at least 60 s for both HTTP and UDP. `force_tracker_interval` is not clamped. Retry backoff is unchanged.
- The constants live in `tracker_comms.rs`; I can turn them into options if you prefer.

## Compatibility and defaults
This is a behaviour change:
- a tracker that asks for an interval below a minute (UDP used to be allowed down to 5 s, HTTP to 0) is now announced to at most once per minute unless `force_tracker_interval` is set;
- a tracker response above 1 MiB, or a request taking longer than 30 s, is now an error.

Nothing changes for trackers that answer within those bounds with a normal interval.

## Tests
```
# on top of upstream main f4ca7d91; Rust 1.97.1, Linux, no IPv6
cargo fmt --all -- --check                                                       # ok
cargo check                                                                      # ok
cargo check --workspace --exclude rqbit-desktop                                  # ok
cargo clippy --all-targets                                                       # no warnings
cargo test -p librqbit-tracker-comms --lib                                       # 6 passed, 0 failed, 1 ignored
cargo test -p librqbit-tracker-comms http_tracker_response_is_bounded_and_timed  # 1 passed, 0 failed, 0 ignored
cargo test -p librqbit-tracker-comms hostile_tracker_intervals_are_clamped       # 1 passed, 0 failed, 0 ignored

# same machine, with the change from #707 applied locally (not part of this PR) so that a Session can be created without IPv6
cargo test -p librqbit --lib                                                     # 49 passed, 0 failed, 5 ignored
cargo test -p librqbit-tracker-comms http_tracker_response_is_bounded_and_timed  # 1 passed, 0 failed, 0 ignored
cargo test -p librqbit-tracker-comms hostile_tracker_intervals_are_clamped       # 1 passed, 0 failed, 0 ignored
```

`cargo +1.94 fmt --all -- --check`, `cargo +1.94 check` and `cargo +1.94 clippy --all-targets` (the other leg of the CI rust-compat matrix): clean.

`cargo test -p librqbit --lib` was only run in the second form (with #707 applied locally): without IPv6 its four e2e tests can't create a Session, as on unmodified main (see #707).

What the new tests cover:

- `http_tracker_response_is_bounded_and_timed` runs against a loopback TCP server: a body exactly at the limit is accepted; an oversized `Content-Length` and an oversized chunked body are rejected; a stalled response times out.
- `hostile_tracker_intervals_are_clamped` checks the 0 / 59 / 60 / 61 s boundaries.

## Related upstream work
- I'm not aware of an open PR for these paths.

## AI disclosure

Prepared with OpenAI Codex and Anthropic Claude Opus 5.5 (Claude Code); reviewed and submitted by Paul Junod.
