# History loading implementation status

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [HISTORY_LOADING_PLAN.md](../HISTORY_LOADING_PLAN.md).

**Status:** first release merged; M6 implemented and reviewed, final CI in PR #238 ·
**Updated:** 2026-09-25

The [reviewed plan](../HISTORY_LOADING_PLAN.md) records the diagnosis, nuc3
measurements, and recovery contracts. This page tracks delivery of its first
release. Work uses an independent clone at
`/private/tmp/nzbd-history-loading`, branch `codex/history-loading`, based on
main `2f84dfb`. Delivery PR: [#237](https://github.com/pjunod/runner/pull/237)
The user's working repositories are not used for implementation.
The PR description is the live status page for CI results and merge completion.

| Step | State | Evidence / next action |
|------|-------|------------------------|
| 1. Remove replay from history reads | Complete | Native and compat listings use the indexed view; page and count share a read transaction. |
| 2. Own and bound reconciliation | Complete | Single worker and pass gate, local-only repair, mutation fencing, transactional batches, unchanged-log skips. |
| 3. Preserve storage and recovery | Complete | Optional local index relocation preserves cursor IDs, committed WAL state, observations, tombstones, and spool location. |
| 4. Browser and operator controls | Complete | Latest-request rendering, loading feedback, persisted pause/resume, Dev enable control with advisory readiness. |
| 5. Adversarial review | Addressed | Four findings: authority locking, toggle response handling, stale post-action reads, and unbounded tombstone transactions. |
| 6. Final verification | Local affected checks passed | Compat: 18 tests; UI: 620 assertions. [Live required CI checks](https://github.com/pjunod/runner/pull/237/checks). |
| 7. Merge combined PR | Tracked live | [PR #237](https://github.com/pjunod/runner/pull/237) records final validation and merge status. |

## Validation policy and evidence

Before the user's latest CI instructions, affected Rust tests, UI harnesses,
clippy, and a synthetic performance probe had passed on the earlier base.
Those results are preliminary, not qualification of this branch. An ongoing
full-workspace run was stopped when the new instructions arrived. Unit tests resumed only after the combined
adversarial review and remediation. Final results are recorded below.

The preliminary 1,000-row storage probe measured p95 1.575 ms while three
replays ran, with a maximum batch of 15 ms. This was a local Mac debug build,
not HTTP latency or post-fix production evidence. The nuc3 baseline remains
12.3–31.7 seconds for slow 20-row requests and 15–37 ms for warm reads.

## Decisions and remaining operational work

- Performance fixes are always active. Dev settings can pause or enable
  synchronization immediately; readiness never blocks enablement.
- M6 is implemented and reviewed in [HISTORY_INCREMENTAL.md](../HISTORY_INCREMENTAL.md),
  which tracks its implementation, equivalence review, final tests, and merge.
- Retain the original index during relocation. Rejecting a stale destination
  or missing active index protects cursor state; it is not a feature gate.
- No production configuration, data migration, or deployment has been done.
  Deployment qualification must measure nuc3 latency and shared-peer freshness
  against the plan before claiming production performance improvement.

## Final review and verification

The single adversarial review requested changes on four points, all addressed
before starting final tests:

1. Directory authority now uses an exclusive local-only lock or a shared
   shared-mode lock, acquired before log inspection. Per-tag locks still exclude
   duplicate writers. Regression coverage exercises both cross-mode open orders.
2. Pause/resume handles the UI's parsed POST response and renders the result.
   Tests assert the displayed state and the inverse operation on the next click.
3. Successful history mutations force a new page request. A delayed pre-delete
   response cannot restore the deleted row; job/requeue refreshes also force it.
4. Tombstone catch-up uses 16-key transactions, cancellation/generation checks,
   and batch timings. Live deletion retains its atomic transaction. Tests cover
   pause between batches and resumed catch-up without resurrection.

Final verification uses the PR's required Main promotion gate, including Rust
compile/lint, affected unit and cluster tests, UI harnesses, and mobile checks.
The compat library suite runs locally because that suite is absent from the
required fast lane. The UI boot check and all 620 DOM assertions passed. Compat passed all 18
tests: 17 in the sandbox and the socket-binding test on its isolated retry
outside the sandbox. An initial compilation failure in the new tombstone
query was fixed before the test run. Required CI results and the final merge
record are maintained in the [PR status page](https://github.com/pjunod/runner/pull/237),
so publishing a result does not retrigger the same unit suites.
