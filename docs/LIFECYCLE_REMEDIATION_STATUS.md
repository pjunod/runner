# Lifecycle remediation — remaining fixes and delivery status

**Status:** implementation and primary review active; final gates and merge pending.
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
| Runner publication retries, resource holds, custody, Keep and durable queue mutations | Core checkpoints integrated; URL and broader timing still active | State 3f47dce, d70609c9, 2ecfc5c and ce4cdd3 integrated; focused failure/race and actual writer recovery regressions pass |
| Runner real partial repair, native books, safe sidecars and classification | Integrated; focused regressions pass | POST a2aa622 and bf4c21ca; integration 78555830 and 8727681a |
| Curator discovery, atomic import ledger, replacement ownership and manual intent | Core and follow-up integrated; focused service fault tests pass | Curator 2665a8b and 21a9bec |
| Curator upgrade distinction, stale batches, storage explanation and reviewed acceptance | Committed; focused migration/service/storage tests pass | Curator 2665a8b, 21a9bec and 176fd58; rendered UI acceptance remains pending |
| Cluster durable Delivery, copy retirement, URL replay and RPC bounds | RPC bounds and exact local copy helper integrated; Delivery/copy implementation active | Runner 2e36b491 and bf539031; actual authority-change and complete-copy-drain evidence pending |
| Native torrent independent publication and seeding custody | Pending | None yet |
| Joined timing, usable controls and desktop/mobile acceptance | Pending | None yet |
| Real Runner–Curator/cluster/torrent integration evidence | Pending | None yet |
| Primary integration review | Active | Review found retry scheduling CAS, intact partial dependency, probe routing and discovery bounds gaps; fixes underway |
| Final Astra review | Pending | None yet |
| Full tests, coverage and required CI | Pending | None yet |
| PRs and merge | Pending | None yet |
| Workspace archival and retained receipts | Pending | Work remains active |

## 3. Validation and claims

Focused POST regressions pass for native book publication, failed-health repair,
retained dependencies, deletion replay, exact repair IDs, title classification,
junk-only completion and persistent retry scheduling against the integrated
state checkpoint. State compilation and focused owner/custody/resource fault regressions passed.
Cluster, URL, torrent and full lifecycle integration remain active. These checks establish specific fixes and do not replace final gates. Final reports must
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

