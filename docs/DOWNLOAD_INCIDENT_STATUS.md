# Download incident — implementation and merge status

Companion to [the incident review](DOWNLOAD_INCIDENT_REVIEW.md), which records
causes, evidence, and design decisions. This page tracks execution.

**Updated:** 2026-10-01 (America/New_York). **Phase:** implementation.
**Branch:** `codex/download-incident-recovery`, based on `main` at `9d3288b`.
**Agent checkout:** `/private/tmp/nzbd-incident-agent`.
**PR:** pending creation as one combined draft. **Deployed:** no.

## Progress

- [x] Transfer all incident work out of the user's checkout; leave unrelated files intact.
- [x] Incorporate Opus reviews v1 and v2; correct the 1703 diagnosis.
- [x] File rename fallback, same-inode replay, PP error propagation, PAR2 magic discovery.
- [x] Allocation isolation and public revision-checked retry; URL admission parity.
- [x] Fresh extraction retry destinations; stop invented season-pack numbering.
- [x] Generation-bound deletion and conservative legacy resurrection repair.
- [x] Select-all and terminal recovery-history controls.
- [ ] Directory publication and recovery on filesystems without flagged rename.
- [ ] Known-extension RAR stem normalization with proven order/membership.
- [ ] Safe PP-hold retry and relocation abandonment through existing actions/UI.
- [ ] Honor configured intermediate directory, preserving empty-value semantics.
- [ ] Supported media extension recovery; assess optional ffprobe evidence without overriding exact mappings.
- [ ] Finish implementation and regression coverage; reconcile final review document.
- [ ] One adversarial agent review at merge readiness; address findings.
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
Make routine decisions autonomously and record consequential choices here and
in the final review document. Production data recovery is separate from merging
code; do not silently retry or rename live payloads.

## Validation ledger

Final candidate validation has not started. Historical local candidate: 418
Rust tests passed, two existing performance fixtures ignored; Clippy and 844 UI
assertions passed before this workflow instruction and transfer to current main.
Those results must not be presented as validation of future changes.
