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
# upstream main f4ca7d91 + this commit; Linux x86_64, Rust 1.97.1
cargo fmt --all -- --check     # ok
cargo clippy --all-targets     # no warnings (also clean with Rust 1.94)
cargo test                     # 170 passed, 0 failed (main alone: 168 passed, 0 failed)
```
The new tests (`http_tracker_response_is_bounded_and_timed` in `librqbit-tracker-comms`, `hostile_tracker_intervals_are_clamped` in `librqbit-tracker-comms`) are part of that run and pass.

What the new tests cover:

- `http_tracker_response_is_bounded_and_timed` runs against a loopback TCP server: a body exactly at the limit is accepted; an oversized `Content-Length` and an oversized chunked body are rejected; a stalled response times out.
- `hostile_tracker_intervals_are_clamped` checks the 0 / 59 / 60 / 61 s boundaries.

## Related upstream work
- I'm not aware of an open PR for these paths.

## AI disclosure

The change, its tests and a first draft of this description were written with AI assistance: OpenAI Codex for the original patch, and Anthropic Claude Opus 5.5 in Claude Code for the port to current main, the tests and the runs above. I have reviewed every line and can answer for it.
