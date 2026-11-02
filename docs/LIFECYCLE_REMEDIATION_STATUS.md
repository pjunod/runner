# Lifecycle remediation — remaining fixes and delivery status

**Status:** implementation and primary review active; final gates and merge pending.
**Started:** 2026-11-01.

Companion to the approved download lifecycle proposal and API documentation.
This continuation starts from current merged code. It preserves the later
obfuscation, duplicate cleanup, mobile retry display, scan feedback, priority
and coverage fixes. The current-code audit and original review are preserved
outside compiler caches in the task's durable evidence directory.

## 1. Baseline and constraints

- Runner initially audited at a2535623; later PR #261 (368b5001) is preserved
  through merge 1bac4dd7, with typed hold policy and one authority recovery loop.
- Curator initially audited at dae7424; PR #76 (990e768) and PR #77 (17eb0f6)
  are preserved through merges 225cfd6 and 4935fab. PR #78 (ecee8fe) shutdown
  ownership is preserved in a916ded. PR #79 (74111769) is preserved in 640f799;
  its iOS build 33 is retained.
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
| Runner publication retries, resource holds, custody, Keep and durable queue mutations | Core, URL and off-actor journal preparation integrated; later main policy reconciled | State through 1530967 and merge 1bac4dd7; focused custody/retry, actual writer recovery, cached URL restart and moved-source recovery regressions pass |
| Runner real partial repair, native books, safe sidecars and classification | Integrated; focused regressions pass | POST a2aa622 and bf4c21ca; integration 78555830 and 8727681a |
| Curator discovery, atomic import ledger, replacement ownership and manual intent | Core and follow-up integrated; focused service fault tests pass | Curator 2665a8b and 21a9bec |
| Curator upgrade distinction, stale batches, storage explanation and reviewed acceptance | Committed; focused migration/service/storage tests pass | Curator 2665a8b, 21a9bec and 176fd58; desktop/iPhone action and geometry regressions pass |
| Cluster durable Delivery, copy retirement, URL replay and RPC bounds | Durable receipts, admission catalog and coordinator compatibility fence integrated; partial recovery, Keep and cancellation remain active | Runner 6f44f5cb/ca01a927; clean three-voter upgrade, terminal outcomes, token replay, physical copy drain and new-ID allocation pass |
| Native torrent independent publication and seeding custody | Runner draft active; Curator native client mode committed | Curator 4da2cef/6b93d5a: explicit mode, capability checks, pending admission and two-mode isolation tests pass; real native backend integration remains pending |
| Joined timing, usable controls and desktop/mobile acceptance | Major-stage capture and immutable publication timing integrated | Runner c4111ddc and Curator 76fc7e9; actual pair proves measured download/POST intervals survive restart; omitted history is explicit |
| Real Runner–Curator/cluster/torrent integration evidence | Strengthened Usenet pair passes; final candidate and torrent pending | Runner 38d43723 + Curator c24ad27 prove both exact processing sources and publications physically disappear, both library files remain intact, and permission-failure cleanup resumes after restart |
| Primary integration review | Active | Review found retry scheduling CAS, intact partial dependency, probe routing and discovery bounds gaps; fixes underway |
| Final Astra review | Pending | None yet |
| Full tests, coverage and required CI | Pending | None yet |
| PRs and merge | Pending | None yet |
| Workspace archival and retained receipts | Pending | Work remains active |

## 3. Validation and claims

The PR #261 merge passes affected crate checks and focused tests for repeated
ENOSPC, a source already renamed before move commit, review/deletion refusal,
probe singleflight and worker control persistence. Worker lease-driven recovery
still needs integration with that unified policy. Cluster partial-copy claims,
cross-node Keep and explicit cancellation disposition remain active work.
Resumed download capacity now accounts for verified existing progress, with a
real partial writer restart regression; final capacity review remains active.

The later cluster checkpoint e570dc5 (integration bc12c425) proves terminal
unusable copy disposal, exact raw-copy seal refusal, newer-Delete protection,
renewed completion and encoded-ID routes. Admission catalog engine contracts
cdd4a3e (integration 6d39a3a4) pass bounded-page, accepted-identity, rollback
and retired-ID checks. The Raft catalog is now integrated in 6f44f5cb, with
actual upgrade, different-authority replay, deleted/failed receipt survival and
no-resurrection tests. Native
and phone controls distinguish acquisition children from seed sources and show
pending owner application of Keep. Curator 6fd9d1e corrects the torrent_url wire
literal found by the actual local-swarm fixture. Ordinary seed download passes;
managed same-item repeated manual acquisition remains to be proven.

Focused POST regressions pass for native book publication, failed-health repair,
retained dependencies, deletion replay, exact repair IDs, title classification,
junk-only completion and persistent retry scheduling against the integrated
state checkpoint. State compilation and focused owner/custody/resource fault regressions passed.
Cluster, URL, torrent and full lifecycle integration remain active. Curator PR #76 cleanup workers and immediate-import optimization are preserved. Worker late-completion and phone identity-switch races have focused passing regressions. These checks establish specific fixes and do not replace final gates. Final reports must
name exact tested commits, toolchains, commands, limitations and review outcome.

Curator gates include lint, Go tests, coverage, web tests/build, mobile checks,
browser acceptance and real paired-binary lifecycle tests. Runner gates include
the pinned toolchain's formatting, linting, complete workspace tests with
required archive tools, native/mobile UI checks, coverage, and relevant cluster
and torrent integration tests. Repository-defined required checks remain binding.

Latest main refresh also preserves Curator PR #78 shutdown ownership. Its
focused shutdown and lifecycle checks pass. Runner Apple CI and release builds
now select the Xcode 27.2 fleet and verify that version before building, as
required by the current runner fleet. The local build host reports Xcode 27.2,
build 27B52. BitTorrent M0 uses the existing Rust 1.95 minimum and will be
dispatched against the reviewed branch before merge. Final CI has not been
dispatched.

Cluster partial-unlink replay now resumes its exact journal while preserving
selected-copy original-source obligations. Actual mixed-worker fairness passes
with real NNTP activity. Authority upgrade receipts and live-lease relocation
checks and cluster admission failover are integrated. Native acquisition remains
under construction; the copy helper and bounded per-file progress storage are
integrated in ccf9b3f7/f2ec5639, with 28 focused crash/cleanup checks passing.
Engine integration and the real paired native test remain pending.
Cancellation disposition must still prove physical cleanup
for explicit file deletion while preserving remove-job-and-keep-files behavior.

## 4. Durable work and recovery

Source and evidence live in a task-owned persistent directory, not /tmp:
/Users/pjunod/.codex/visualizations/2026/10/22/01a164b8-ab67-7422-9827-ae254feaf95d/lifecycle-implementation-20261101

Each completed slice is committed; review and validation receipts are retained
outside compiler/cache directories. Do not finish/archive active work simply
because a turn ends. This page must remain honest about unfinished slices.

