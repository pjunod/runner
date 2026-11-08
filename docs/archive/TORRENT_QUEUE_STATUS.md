# Torrent queue — implementation and merge status

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [BITTORRENT_PROPOSAL.md](../BITTORRENT_PROPOSAL.md).

**Status:** adversarial review approved; local UI/format checks passed.
Required CI and merge status: [PR #232](https://github.com/pjunod/nzbd/pull/232).
**Updated:** 2026-09-20 · **Branch:** `codex/torrent-queue-lifecycle`.
**PR:** [#232](https://github.com/pjunod/nzbd/pull/232) (combined candidate).

Companion to [USAGE.md](../USAGE.md) (operator behavior) and
[CONFIGURATION.md](../CONFIGURATION.md) (seeding defaults). This page records
progress, decisions, review findings, and final validation for the queue work.

## Delivery progress

- [x] Preserve the existing queue with collapsible Seeding, Completed, and
  Waiting sections; collapse state persists in the browser.
- [x] Expose upload totals/rates, ratio, peers, seeded duration, completion
  time, policy, and stop reason in the queue and torrent detail panel.
- [x] Add durable manual stopping and stop-after-download, ratio, and time
  policies, with global/category defaults and per-torrent overrides.
- [x] Add Settings → Dev enable controls with live advisory readiness.
- [x] Transfer work into an independent clone and restore the original
  checkout without changing its branch or existing commits.
- [x] Make focused backend and UI/validation commits.
- [x] Open one draft PR for the combined review.
- [x] Complete the adversarial agent review and address actionable findings.
- [x] Run final UI checks: boot and 599 DOM assertions; formatting/diff clean.
- Required unit/build/mobile/CI results and merge completion are recorded by
  [the PR checks and merge record](https://github.com/pjunod/nzbd/pull/232),
  so this committed page does not freeze a transient CI status.

## Decisions and scope

1. **Retain design A and B's instrumentation.** Detail panels expose metrics
   and policy controls without requiring a separate transfer workspace.
2. **Stopping preserves payload files.** Removal and payload deletion stay
   separate. Editing a policy never silently restarts a stopped seed.
3. **Defaults are copied when applied.** An existing torrent does not change
   its policy when global or category defaults change later.
4. **Advisories do not gate enabling.** Unverified readiness says `unknown`;
   configuration validation and runtime data-integrity rules remain intact.
5. **Preserve the latest main recovery fixes.** The independent clone starts
   at main's PR #231 and keeps its resume-phase and progress fixes.
6. **Web UI scope.** The additive API data remains available to other clients;
   this change does not redesign the native mobile app.

## Validation evidence

Before the new workflow instruction, the earlier checkout passed 589 DOM
assertions and 308 affected Rust library tests, plus formatting, workspace
compilation, and affected-package Clippy. Three unrelated connection-pool
shutdown tests stalled and were excluded from that preliminary run. These are
historical results, not certification of this final candidate.

After the workflow instruction, testing was deferred until the adversarial
review approved all fixes. Final local UI boot/DOM checks and formatting pass.
Draft CI was skipped by the repository's existing workflow.
The required main check is `Main promotion gate`; protection stays intact.
The final torrent lane includes affected API/config/types/state/compatibility
unit tests and owner policy persistence cases. Preflight runs both UI harnesses.

## Review and merge record

The independent adversarial review requested three corrections, all addressed:

- Storage-paused torrents show “waiting for disk space” and the storage error;
  automatic recovery clears their stop reason.
- A completed seed policy no longer rejects missing-file recovery. Resuming
  missing files invalidates historical readiness and requires fresh verification
  before the stop-after-download policy can apply again. The adapter rearms
  completion delivery when owner readiness is revoked.
- The readiness advisory matches the actual peer port range, 1–65534.

Focused Rust/DOM regressions cover the changes. The reviewer approved all
corrections before final tests. Required CI runs the
Rust unit tests after the draft becomes ready; avoiding a duplicate local
Rust run keeps validation resource use bounded.
