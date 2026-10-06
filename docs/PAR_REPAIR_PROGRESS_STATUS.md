# PAR repair status — design, build, and qualification

**Updated:** 2026-10-06 · **Status:** local qualification passed; required PR CI next

Companion to [PAR_REPAIR_PROGRESS_PLAN.md](PAR_REPAIR_PROGRESS_PLAN.md), which
owns the implementation contract. The originating chat owns design; GPT-6.1
Sol builds in its isolated clone. This page records evidence, not intentions
presented as completed work.

## Current position

| Workstream | State | Evidence / next boundary |
|---|---|---|
| Diagnosis | complete | Repeated candidate scans; real-tool reproduction of ignored obfuscated recovery volumes |
| Storage fallback | implemented by builder, pre-review tests passed | Missing/empty inter_dir selects main_dir; final qualification follows review |
| Workspace retirement | implemented; regression cases written | Generation custody, durable local/cluster receipts, bounded admission and replay; legacy ambiguity stays explicit |
| Repair scanning and retry reuse | implemented; compiling and linting | Cached single-pass fingerprints, streamed packets, normalized companions, useful-input retry |
| Failed payload policy | unchanged | Seven-day park retention; payload deletion not requested |
| Adversarial review | complete | Reported findings fixed; final disposition has no remaining blockers |
| Final qualification | local cohort passed | 501 local tests passed; required CI supplies its disjoint cohort |
| PR and merge | draft [PR #252](https://github.com/pjunod/runner/pull/252) | Required Main promotion gate; ordinary merge after review and qualification |
| Production | unchanged | No deployment, restart, config edit, or live deletion performed |

## Ownership and working locations

- Design: `/private/tmp/nzbd-repair-design`.
- Builder clone: `/private/tmp/nzbd-par-repair-progress`.
- Builder storage worktree: removed after consolidation onto `codex/par-repair-progress`.
- Builder chat: `01a11343-6842-77f2-9e80-2937f4bd2969`.
- The generated plan was removed from the user's `~/code/nzbd/docs` checkout
  on 2026-10-06 after a byte-verified copy to the design clone.

## Decisions requiring visibility

1. **Main directory fallback:** use main_dir directly; do not append a new
   intermediate folder or fall back to completed storage.
2. **Legacy Keep is ambiguous:** existing event pruning prevents proving that
   absent retention events mean no user Keep. Preserve ambiguous legacy data
   and expose exact cleanup reasons; new eligible scratch cleans automatically.
3. **Quiescence is structural:** a generation-scoped use guard protects actual
   workers until exit. Dropping an async future is not sufficient.
4. **Tests changed mid-task:** builder ran tests before the user's new workflow.
   Future review and final qualification follow plan §9.5.

## Delivery evidence to fill as work completes

Record cohesive commit IDs, PR URL, adversarial review findings/disposition,
final test outcomes with code revisions, merge commit, and removed temporary
artifacts. Do not claim the 122 GiB legacy workspace backlog is automatically
reclaimed where provenance remains unresolved.

## Builder evidence

The builder has committed storage fallback (`6f7149c`) and shared deletion /
internal scratch startup groundwork (`fd77d36`). Before the final workflow
release, 609 tests passed, with two existing ignored state tests and no failures.
These are historical results. Final qualification follows adversarial review;
changed behavior invalidates affected earlier passes. Logs and the preserved
exploratory patch live in `/private/tmp/nzbd-build-artifacts`.

The final release in plan §9 resolves the former pause and merge-review conflict.
Legacy absence of retention events remains ambiguous; new workspace provenance
and a generation-scoped attempt use guard are now approved implementation.

Implementation compilation checks pass across every workspace target. Core
clippy passes with warnings denied. New regression cases cover stable scan
reuse, padded slices, changed input identities, cancellation, extensionless
recovery naming, receipt protections, retirement replay and restart generations.
They have been compiled but not executed: the final adversarial review remains
before qualification. The embedded job detail displays phase/counters and labels
article delivery as download health. Files detail displays cleanup evidence.

Resource decision: the manual CI workspace test invocation already includes the
API crate. Its duplicate API invocation was removed and the workspace invocation
now uses the committed lockfile. No production feature flags were introduced.

Cohesive implementation commits: `897920b` (custody and retirement receipts),
`00fdfe6` (repair sessions and actual worker joins), and `28f776d` (progress and
cleanup evidence in the UI). Workspace clippy and all-target compilation pass
with the committed lockfile. No new unit tests have run before review.

Qualification will use disjoint cohorts: PR fast-lane checks provide their
existing cluster/API/config/state/type coverage; local strict tests cover the
remaining affected post/PAR and engine cases. Previously green tests will not
be rerun solely because one other test fails. Required CI remains ordinary.

The repository now redirects to `pjunod/runner`; the builder clone uses that
canonical remote. PR #252 is attached to the builder chat. Main requires the
Main promotion gate and an up-to-date base; it does not require another human
review. The draft PR's validation lanes are skipped while adversarial review
runs, preserving the required review-before-tests order.

## Final adversarial review

The reviewer requested changes for three concrete defects:

- P1: raw relocation workers could outlive cancellation without custody.
- P1: a crash after PP_DONE but before the workspace receipt leaked scratch.
- P2: remote repair progress never reached authority snapshots.

Fixes retain generation custody through tracked filesystem exit and add
64 KiB relocation checkpoints plus publication authority checks. Local history
is associated in a durable pending receipt before a checked durable terminal
stamp; confirmation and startup/rescan replay validate the exact history cursor
and outcome. Cluster acceptance is durably recorded before dropping the worker
lease, with maintenance replay of parent finalization. Heartbeats carry optional
repair progress, filtered by accepted lease/token and assigned node.

Focused regressions now cover cancelled cross-volume copy/publication, pending
history without a stamp, the post-stamp crash gap across restart, authority
rejection and acceptance replay, old heartbeat compatibility and replacement,
and manager-level duplicate/unusable recovery batches. Qualification has not
started; these cases have only been compiled so far.

Final review disposition: all reported findings are addressed. The reviewer also
found a terminal Delete lock-wait race during fix verification; authority is now
checked under mutation admission and immediately before deletion. An explicit
lock-wait cancellation regression covers that case. Workspace clippy passes
with warnings denied. Final test qualification started after this disposition.

## Final local qualification

The local cohort has 501 passing tests and three existing ignored torrent
fixtures. Post/PAR (126), nonduplicated engine unit/integration (163), the
remaining workspace targets (206), and configuration examples (6) were run
with the lockfile and required external tools. Two integration failures were
fixed and only those cases rerun: receipt replay now wraps an exhausted cursor
immediately within its 25-record bound; the delayed-recovery fixture budgets
initial verification as well as fetching. Passing tests were not rerun solely
because those cases failed. Core formatting, whitespace and focused clippy
checks remain clean.

PR CI will provide cluster, API/config/state/types/qbit, selected engine torrent,
embedded JavaScript, and mobile qualification. The Rust UI DOM wrapper locally
also verifies server-schema roundtripping; its boot-only wrapper is omitted
because PR CI executes the identical boot harness. Required CI and the ordinary
merge are still pending. Final CI/merge/cleanup evidence will be recorded on
the PR, preserving this tested code revision without a redundant CI-only push.
