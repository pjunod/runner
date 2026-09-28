# Mobile queue parity — implementation and delivery status

**Status:** final validation · **Updated:** 2026-09-28 · **Branch:**
`codex/mobile-queue-parity` · **Base:** `5b683d6`

Companion to [the implementation plan](MOBILE_QUEUE_PARITY_PLAN.md) and
[Fable's plan review](MOBILE_QUEUE_PARITY_REVIEW.md). Work uses an independent
clone at `/private/tmp/nzbd-mobile-queue-parity`; the original checkout is intact.

**PR:** [#241](https://github.com/pjunod/runner/pull/241), draft until review corrections land.

## Progress

- [x] Incorporate Fable's required corrections and verify server contracts.
- [x] Create independent clone and implementation branch.
- [x] Implement lifecycle presentation, grouping, and collapse preferences.
- [x] Implement policy controls and refused-resume recovery.
- [x] Correct matching web actions and add shared regression fixtures.
- [x] Complete one combined PR and adversarial agent review.
- [ ] Address review findings, then run the final affected checks.
- [ ] Merge after required checks pass.
- [ ] Record iOS and Android artifact/distribution evidence.

## Validation and decisions

Unit tests are deferred until the combined candidate has received adversarial
review, as requested. No feature gate is added: this uses existing daemon
capabilities. Existing Settings Dev enable controls are outside this UI port.
Native release evidence is separate from the PR merge; no device or store
release is claimed before it is verified.

## Implementation evidence

The native classifier, section definitions, persistence hook, card rendering,
policy editor, and action recovery are implemented. Failed, storage-held, and
missing-file web controls now follow the reviewed handler contract. Shared
fixtures live under `crates/nzbd-types/fixtures/`; native rendered/action/storage
checks and engine serialization/sentinel checks are written but not yet run.

No dependency or native configuration change was needed. The existing Jest
renderer is used. The source checkout remains untouched; only the reviewed
plan and review were copied into this clone.

## Adversarial review disposition

The final agent review requested three P2 corrections, all addressed before
unit tests: restrict the editor to the latest seed phase, show refused-resume
context inside the native modal, and retain collapse choices in a session cache
when secure storage fails across dashboard remounts. Added rendered regressions
for stale decisions, changed connections, modal explanations, and failed writes.

Release metadata is prepared as **1.1.2 (10)** after rechecking main at build 9.
Xcode and two signing identities are available. Android signing is absent from
the standard local environment and Gradle properties; signed Android delivery
remains unverified. No device/store distribution is claimed.

## Final validation

The adversarial reviewer approved the correction pass with no remaining
findings. Local TypeScript checking passes. The mobile suite initially found
four renderer-query failures; corrected the test queries and all ten tests in
the affected renderer suite now pass (the other 107 tests passed on the full
run). Web boot and **726 DOM assertions** pass. Expo exports both iOS and
Android Hermes bundles successfully. Required CI will validate the final SHA;
Rust validation is delegated to that CI run to avoid duplicate compilation.
