# Download incident — implementation and merge status

Companion to [the incident review](DOWNLOAD_INCIDENT_REVIEW.md), which records
causes, evidence, and design decisions. This page tracks execution.

**Updated:** 2026-10-01 (America/New_York). **Phase:** final validation.
**Branch:** `codex/download-incident-recovery`, based on `main` at `9d3288b`.
**Agent checkout:** `/private/tmp/nzbd-incident-agent`.
**PR:** [#247](https://github.com/pjunod/runner/pull/247), ready for merge once required checks pass. **Deployed:** no.

## Progress

- [x] Transfer all incident work out of the user's checkout; leave unrelated files intact.
- [x] Incorporate Opus reviews v1 and v2; correct the 1703 diagnosis.
- [x] File rename fallback, same-inode replay, PP error propagation, PAR2 magic discovery.
- [x] Allocation isolation and public revision-checked retry; URL admission parity.
- [x] Fresh extraction retry destinations; stop invented season-pack numbering.
- [x] Generation-bound deletion and conservative legacy resurrection repair.
- [x] Select-all and terminal recovery-history controls.
- [x] Directory publication and recovery use journaled mkdir/link fallback with interruption regressions.
- [x] Known-extension RAR stem normalization with proven order/membership.
- [x] Safe PP-hold retry and relocation abandonment through existing actions/UI.
- [x] Honor configured intermediate directory, preserving empty-value semantics.
- [x] Supported media extension recovery; exact PAR mappings win. Optional ffprobe deferred as a separate enhancement.
- [x] Finish implementation and regression coverage; reconcile final review document.
- [x] One adversarial agent review at merge readiness; five findings addressed and targeted verification completed.
- [ ] Run required unit/regression/CI checks once on the reviewed candidate; retry failed or affected checks only.
- [ ] Merge the combined PR after green checks; clean agent resources.

## Workflow and decisions

Use focused commits within one larger PR. Keep the PR draft during construction
so the repository's main fast lane does not run tests on every push. Request the
adversarial agent review only after implementation is merge-ready. Existing
pre-instruction local test results are historical, not the final merge gate.
No more unit tests will run during implementation.

Use the existing settings and architecture. Add no feature flags or periodic
watchdogs. Ordinary identity/collision validation remains part of correctness.
Decision: keep atomic directory rename where supported; otherwise use the
existing custody journal to checkpoint exclusive mkdir/link publication.
Target entries become visible incrementally, but completion is published only
after manifest verification and fsync. Uncheckpointed creations require explicit
review. No feature flag or coordinated-writer assumption is introduced.
Duplicate sources are retired only after the owning custody commit. Curator current source was checked in an independent clone: automatic imports
consume completed status with a final path, not directory existence. This is
source verification, not a claim about its deployed revision.

Make routine decisions autonomously and record consequential choices here and
in the final review document. Production data recovery is separate from merging
code; do not silently retry or rename live payloads.

## Validation ledger

Final adversarial review and follow-up are complete. Local validation passed:

| Check | Result |
|---|---|
| Engine unit tests outside CI's torrent filters | 134 passed |
| Engine integration tests outside CI's two torrent cases | 22 passed |
| Post-processing unit tests | All 67 passed across initial and targeted runs |
| Post-processing failure regressions | 6 passed |
| Post-processing pipeline regressions | All 34 passed across initial and targeted runs |
| Daemon unit tests | 22 passed |
| Workspace/all-target compile and Clippy | Passed |

Two regression fixtures failed initially. The synthetic split RAR omitted the
ENDARC next-volume flag; adding the flag made independent 7-Zip extract all
three bytes. The crash-after-move fixture created an unowned destination instead
of committing a custody relocation; it now executes the real move before
simulating interruption. The 15 affected naming tests and the one move-resume
test passed on targeted rerun. Production behavior was not relaxed.

GitHub required checks are authoritative on the PR's latest commit. UI/static,
mobile, and supply-chain checks passed on the preceding candidate; Rust CI was
still running when the fixture corrections were committed. Final required
checks must be green before merge. The repository automatically runs its full
selected CI lanes after each push; those required runs are not bypassed. Local
passed suites are not repeated for an unrelated fixture failure.

Historical pre-workflow results are excluded from this final validation ledger.

## Adversarial review disposition

| Finding | Resolution |
|---|---|
| Retirement request ID exceeds API limit | Execute the already-authorized delete; short digest key only for legacy fallback. Cleanup errors cannot block startup. |
| Retry revisits a succeeded extraction workspace | Record committed output identities and verify them before reusing a successful extraction. Legacy workspaces can start a fresh attempt. |
| Cluster history advertises generation root | Normal completion and takeover pass the actual published path into history; completion response carries it to worker fallback. |
| Final heuristic rename bypasses custody | Use owned rename path; regression covers rename then publication conflict. |
| Cleanup failure invalidates a published recovery | Publication remains claimable. Durable cleanup checkpoint is retried through existing reconciliation, including cancelled/partial/terminal handoffs. |

The reviewer confirmed fixes 1–4, then identified terminal cleanup omission in
fix 5. That omission was corrected and covered by the same restart regression.
No tests were run by the reviewer. Full-workspace Clippy passed before that
last query/test update; final checks follow below.
