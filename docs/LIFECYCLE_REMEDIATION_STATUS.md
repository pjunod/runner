# Lifecycle remediation — remaining fixes and delivery status

**Status:** implementation active; not reviewed, tested, merged or deployed.
**Started:** 2026-11-01.

Companion to the approved download lifecycle proposal and API documentation.
This continuation starts from current merged code. It preserves the later
obfuscation, duplicate cleanup, mobile retry display, scan feedback, priority
and coverage fixes. The current-code audit and original review are preserved
outside compiler caches in the task's durable evidence directory.

## 1. Baseline and constraints

- Runner baseline: a2535623af1a7a5623677350895b49e8c2a840ab.
- Curator baseline: dae74249195b97f7af0f0b649b3ea79109e8aae7.
- Lower-model agents implement isolated slices; primary reviews integration.
- Final Astra adversarial review precedes the full required test gates.
- Merge only after findings are resolved and required tests/checks pass.
- Preserve coverage floors and existing functionality; no blanket cleanup.
- Existing processing/failed/completed backlog remains separately gated.
- Pre-upgrade in-flight jobs may complete import; old backlog deletion stays
  protected. This policy is approved, not a pending decision.
- No production deployment or media operations are part of this continuation.

## 2. Implementation ledger

| Slice | State | Evidence |
|---|---|---|
| Runner publication retries, resource holds, custody, Keep and durable queue mutations | Assigned | None yet |
| Runner real partial repair, native books, safe sidecars and classification | Assigned | None yet |
| Curator discovery, atomic import ledger, replacement ownership and manual intent | Assigned | None yet |
| Curator upgrade distinction, stale batches, storage explanation and reviewed acceptance | Pending | None yet |
| Cluster durable Delivery, copy retirement, URL replay and RPC bounds | Pending | None yet |
| Native torrent independent publication and seeding custody | Pending | None yet |
| Joined timing, usable controls and desktop/mobile acceptance | Pending | None yet |
| Real Runner–Curator/cluster/torrent integration evidence | Pending | None yet |
| Primary integration review | Pending | None yet |
| Final Astra review | Pending | None yet |
| Full tests, coverage and required CI | Pending | None yet |
| PRs and merge | Pending | None yet |
| Workspace archival and retained receipts | Pending | Work remains active |

## 3. Validation and claims

No test result is claimed for this continuation yet. Narrow regressions may
establish a specific fix; they do not replace final gates. Final reports must
name exact tested commits, toolchains, commands, limitations and review outcome.

Curator gates include lint, Go tests, coverage, web tests/build, mobile checks,
browser acceptance and real paired-binary lifecycle tests. Runner gates include
the pinned toolchain's formatting, linting, complete workspace tests with
required archive tools, native/mobile UI checks, coverage, and relevant cluster
and torrent integration tests. Repository-defined required checks remain binding.

## 4. Durable work and recovery

Source and evidence live in a task-owned persistent directory, not /tmp:
/Users/pjunod/.codex/visualizations/2026/10/22/01a164b8-ab67-7422-9827-ae254feaf95d/lifecycle-implementation-20261101

Each completed slice is committed; review and validation receipts are retained
outside compiler/cache directories. Do not finish/archive active work simply
because a turn ends. This page must remain honest about unfinished slices.

