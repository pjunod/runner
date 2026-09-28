# Mobile queue parity — implementation and delivery status

**Status:** implementing · **Updated:** 2026-09-28 · **Branch:**
`codex/mobile-queue-parity` · **Base:** `5b683d6`

Companion to [the implementation plan](MOBILE_QUEUE_PARITY_PLAN.md) and
[Fable's plan review](MOBILE_QUEUE_PARITY_REVIEW.md). Work uses an independent
clone at `/private/tmp/nzbd-mobile-queue-parity`; the original checkout is intact.

## Progress

- [x] Incorporate Fable's required corrections and verify server contracts.
- [x] Create independent clone and implementation branch.
- [x] Implement lifecycle presentation, grouping, and collapse preferences.
- [x] Implement policy controls and refused-resume recovery.
- [x] Correct matching web actions and add shared regression fixtures.
- [ ] Complete one combined PR and adversarial agent review.
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
