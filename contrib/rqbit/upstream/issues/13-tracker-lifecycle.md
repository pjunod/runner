# Coordinate small fixes for HTTP tracker announce lifecycle

At `main` `f4ca7d919638a7b8b031ab062a61861a9eb586e4` (inspected
2026-10-28), the HTTP announce loop in
`crates/tracker_comms/src/tracker_comms.rs` starts with `Started` and then sets
its event to `None`. The HTTP request construction sets `trackerid: None`.
`Session::stop` cancels tasks without awaiting a final HTTP tracker announce.
These are source observations, not a new packet-capture reproduction.

[BEP 3](https://www.bittorrent.org/beps/bep_0003.html) describes tracker events
for completion and stopping and returning a tracker-provided identifier on
later announces. I would like to offer separate, small fixes rather than one
large lifecycle rewrite:

1. Echo the identifier returned by an HTTP tracker on subsequent announces.
2. Send `completed` once when an initially incomplete torrent finishes;
   initial seeds must not emit a false completion event.
3. Provide a bounded shutdown path for `stopped`, with clear semantics for
   pause, removal and session shutdown and no indefinite network wait.

A downstream prototype combines these with client-identity configuration,
query preservation, accounting and backoff. That combined patch is over
2,000 lines and does not apply to current `main`; I am asking for preferred
boundaries before porting individual fixes. Custom client identity and
backoff policy can be discussed separately.

The proposed regression harness is a loopback HTTP tracker that records
queries and returns a synthetic tracker identifier. It should prove event
ordering, no duplicate completion, initial-seed behavior, identifier reuse,
retained authentication query parameters, and a strict shutdown deadline.
No new upstream tests for those changes have been run.

A related concern is whether transport-error diagnostics retain tracker URL
credentials. That deserves an isolated redaction regression before any claim
or separate report; it is not established here. Fixtures must use synthetic
credentials and must never contain a private tracker URL.

Would these be useful as separate PRs, and which API should own the awaited
shutdown operation?

## AI disclosure

Prepared with AI coding assistants (OpenAI Codex; Anthropic Claude via Claude
Code), including the downstream prototype and this issue draft.
