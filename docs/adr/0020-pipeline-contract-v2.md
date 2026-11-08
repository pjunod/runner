# ADR-20 — Pipeline contract v2: one terminal fact per job, one owner per boundary

**Status:** accepted 2026-11-07 (Paul: rewrite the seam; keep the manifest; a
post-processing failure is a failure unless the job is marked keep-on-failure)
· **Built:** runner M0, M1a, M1b, M3, M6a (M6b waits on deploy confirmation)
· **Contract:** [PIPELINE_CONTRACT.md](../PIPELINE_CONTRACT.md) · **Wire
reference:** [INTEGRATION.md](../INTEGRATION.md)

## Context

Runner's seam with curator had grown four overlapping mechanisms for "what
happened to this job": a history status string re-parsed downstream, a
review hold (`post_failure`, `io`, `identity_conflict`) whose only exit was a
person, a delivery acknowledgement curator had to send before runner retired
its source, and four retry clocks (the hold ladder, a second PP-side ladder,
a failed-cleanup param clock and the artifact operation ladder). On nuc3 that
meant terminals emitted only after a 930-line failure handler finished
(median 98 s, p90 14 min), full disks classified as bad releases by matching
words in an error message, and 24 of 29 `DELETED` history rows being jobs a
person removed while they sat in a review hold.

## Decision

1. **One authoritative terminal per job, first.** A `job_terminals` row is
   written with the artifact's salvage obligation in one transaction; the
   history row, queue markers and `job_pp_finished` are replayed from it.
   The event carries a closed `outcome` and `reason`, `blames_release` and
   `salvage`, and goes out before disposal, cleanup, salvage or retention
   touch the bytes.
2. **Reasons are typed at the producer** — owner (`health`, `io`), URL
   fetcher (`fetch{gone|indexer_unavailable}`), post-processor (`par`,
   `unpack`, `script`, `publish`, `identity`) — and environmental errors are
   classified by errno, never by message text (subprocess stderr excepted).
3. **Holds are environmental only** (`storage_unavailable`, `capacity`,
   `quota`, `allocation`, `mount_unavailable`, `missing_tool`,
   `engine_unavailable`, `password_required`), visible as `held`, retried on
   one ladder (`HOLD_RETRY_DELAYS_SECONDS`) and ended by runner as
   `failed/hold_expired` at a configured deadline.
4. **Publication is per file, journaled, manifest last**, and there is no
   acknowledgement: a directory with `.runner-manifest.json` is curator's.
5. **Disposal is the artifact record's**, gated by the terminal row, on the
   operation's own retry (`Operation.next_retry`); nothing downstream waits
   on it.
6. **History is cursor-safe**: `(incarnation, seq)` with `head_seq` and
   `retained_min_seq`.

## What it replaced

| Replaced | By |
|---|---|
| `HoldCause::{PostFailure, Io, IdentityConflict}`, `HoldPolicy::Review`, the first-cause lock | typed `FailureReason` terminals; newest cause wins; boot conversion of persisted legacy causes |
| terminal after `handle_failed_job_admitted`; health/fetch terminals through PP admission | `record_terminal` from the owner, the fetcher and PP; salvage and disposal after it |
| `POST /api/v1/deliveries/{id}/accept`, `Delivery.status`, `accepted_at`, the cluster accept path | `410 Gone`; the read-only `/api/v1/deliveries` feed |
| the "always stage privately" copy and its four to five hash passes | per-file `renameat2(RENAME_NOREPLACE)` (or one hashing copy across volumes) under a move journal with finish-or-unwind on restart |
| the PP-side `POST_RETRY_DELAYS_SECONDS` scheduler and the `*PP:failed-cleanup-*` param clock | the owner's hold ladder; the disposal operation's `Operation.next_retry` |
| `is_out_of_space` / `is_storage_unavailable` text matching | `errno_hold_cause` / `io_error_hold_cause` / `error_hold_cause`; `stderr_hold_cause` for subprocess output |
| `CURATOR_DELIVERY.md`, `HELD_JOBS_PLAN.md`, `INTEGRATION_PLAN.md` and the custody/regrab/lifecycle plans | this ADR, the contract and `INTEGRATION.md`; the old documents are in [`archive/`](../archive) |

## Consequences

- A failed post-processing job is reported in seconds and curator can pick
  the next release without waiting on runner's bytes.
- `publish`, `io`, `identity` and `hold_expired` always park, whatever
  `failure_action` says (decision for Paul, adopted by both reviewers).
- The legacy wire fields rode one release for mixed-version safety. M6b
  (0.6.0) removes `pp_status`, `final_dir`, `includeAccepted` and the v1
  delivery-row shims, and deploys only after every node runs curator M2 and
  runner 0.5.0. Still to go: `custodyState`/`sourceCleanupStatus` with the
  receipt custody tracking behind them, and the persisted `held` status
  (0.5.0 already reads one).
