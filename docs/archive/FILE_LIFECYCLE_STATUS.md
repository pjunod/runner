# File lifecycle — implementation status

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [FILE_LIFECYCLE_OPERATIONS.md](../FILE_LIFECYCLE_OPERATIONS.md).

**Status:** implementation and adversarial review complete; final CI in progress.
**Updated:** 2026-09-25.

Companion to [the implementation plan](FILE_LIFECYCLE_PLAN.md),
[design review](FILE_LIFECYCLE_REVIEW.md), and
[operations guide](../FILE_LIFECYCLE_OPERATIONS.md).

## 1. Delivery

Work uses dedicated sibling clones under `/private/tmp/file-lifecycle-20260925`.
User checkouts and live media are untouched. Focused commits are batched into
one PR per repository. No deployment or live-media cleanup is included.

| Milestone | State |
|---|---|
| F0: truthful deletion and consumer cleanup | Implemented and locally verified |
| F1: durable ownership and operation journal | Implemented and locally verified |
| F2: discovery and Files attention view | Implemented and locally verified |
| F3: checked deletion and retention | Implemented and locally verified |
| F4: recovery publication | Implemented and locally verified |
| F5: Curator recovery import | Implemented and locally verified |
| F6: settings, cleanup and operations | Implemented and locally verified |
| Final adversarial review | Three reviewers completed; findings addressed |
| PRs and merge | Runner [#239](https://github.com/pjunod/runner/pull/239) and Curator [#41](https://github.com/pjunod/curator/pull/41) in CI |

## 2. Decisions

- Full approved scope remains: the fix plus the robustness improvements.
- Dev enable controls show prerequisites and current readiness as advisory
  information; they always accept enablement. Individual file mutations still
  verify identity, ownership and quiescence.
- Select a recovery subset in Runner before staging. Curator imports the entire
  resulting immutable handoff, avoiding stranded files under a partial claim.
- Curator serializes filesystem publication across ordinary and recovery
  imports. Downloads remain concurrent.
- Unknown legacy directories remain unowned and Keep until explicitly reviewed.
  External script-selected paths do not inherit deletion authority.
- User instructions override repository preferences for working in user
  checkouts and repeatedly running tests during construction. All unit suites
  ran after adversarial review; only failures were rerun locally.
- Cluster payload authority remains generation-aware and node-local; Windows
  legacy/unowned payloads remain Keep. See the operations guide for limitations.

## 3. Review corrections

Review fixes preserve writer-stop acknowledgements across retries, fence retiring
jobs, invalidate stale expiry authorization, avoid per-segment inventory locks,
reconcile abandoned allocations, and atomically journal cross-volume source
retirement. Discovery includes category roots and runs periodically when enabled.

Curator rejects colliding destinations, verifies durable publication before
receipts, rolls back uncommitted placement before cancellation acknowledgement,
and journals ebook replacement cleanup. Ordinary reimports receive a fresh
placement generation. Recovery previews run as durable background operations;
receipt delivery runs independently of copying. Restored handoffs require review.

## 4. Validation

Runner local verification completed:

- Full Rust workspace suite, followed by targeted reruns of corrected engine
  restart and post-processing regressions. All originally failing cases now pass.
- UI boot and DOM harness: 620 assertions.
- Workspace Clippy with warnings denied; rustfmt; minimum Rust 1.95 compilation.
- Million-record inventory fixture: 1,000,001 rows, first-page p95 1.325 ms;
  database 1.238 GB, preparation 44.8 seconds on macOS arm64.

Curator local verification completed:

- Go race/coverage suite, followed by passing targeted recovery API regression.
- Coverage gate 86.0%; 88 web unit tests; typecheck; 112 desktop/mobile Playwright
  tests; zero lint issues; generated sources consistent; production build passed.
- CI browser, mobile, Docker, lint and fake-consumer contracts pass. Linux CI
  unit assertions pass, but aggregate coverage is 85.6% against the 86.0% floor;
  additional passing failure-path coverage raises the combined local measurement to 86.4%; Linux CI is running again.

Remaining acceptance: green CI on final PR heads and merge. Real two-process
recovery and 100,000-entry filesystem load checks are currently running against
disposable fixtures.
No deployment has been performed.
