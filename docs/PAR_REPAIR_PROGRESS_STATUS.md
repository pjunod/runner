# PAR repair progress — build status and qualification

**Updated:** 2026-10-06 · **Design owner:** originating chat · **Builder:** Sol

Companion to [CONFIGURATION.md](CONFIGURATION.md) and
[FILE_LIFECYCLE_PLAN.md](FILE_LIFECYCLE_PLAN.md). The authoritative build design
is in the designer's own clone at
`/private/tmp/nzbd-repair-design/docs/PAR_REPAIR_PROGRESS_PLAN.md` until the
final release is copied here. This status tracks implementation and evidence;
the designer's release determines remaining repair scope.

## Current build

Storage fallback is implemented. Missing/empty `inter_dir` selects expanded
`main_dir` directly. Explicit intermediate paths and successful global/category
publication retain their meanings; existing allocations retain custody.

Workspace groundwork recognizes transform-owned scratch at startup and extracts
the existing identity/manifest-checked deletion executor for shared use. It does
not yet admit retirement receipts or delete obsolete workspaces.

| Work | State | Evidence or next step |
|---|---|---|
| Reproduce obfuscated recovery ingestion | Reproduced | par2cmdline 1.3.0 ignored extensionless recovery volumes when passed only the index; one damaged slice remained unrecoverable despite eight blocks on disk. |
| Storage fallback and routing | Implemented | TOML, NZBGet conversion, home expansion, real NNTP processing, global/category publication, conflict/readiness, restart custody, capacity roots, and internal discovery coverage. |
| Shared deletion executor | Implemented | Extracted body matches the previous executor exactly; generation authorization, mutation coordination, retry, and review behavior remain at the caller. |
| Scratch startup classification | Implemented | Transform operation, generation, path, and recorded identity establish internal scratch custody. Regression preserves scratch bytes and protections across restart. |
| Workspace retirement authorization | Pending design decisions | Legacy event-history completeness and generation-scoped active-use fencing require final rules. Operator Keep and review holds remain protected. |
| Repair session and bounded scanning | Paused for designer release | Initial exploratory edits are preserved separately; they are unqualified and will be reconciled with the final design. |
| Recovery retries and explicit inputs | Paused for designer release | Reproduction is available; implementation awaits final interfaces. |
| Cancellation and repair subprogress | Paused for designer release | Must share the attempt fencing and worker-quiescence contract. |
| One coherent PR | Pending completed build | Proper scoped commits, then one larger PR. |
| Adversarial review | Pending merge readiness | Spawn a reviewer only when the PR is otherwise ready; address findings before final tests. |
| Final tests and merge | Pending review | Each affected test must pass on the code being merged. Rerun failures and tests affected by fixes, not the whole suite by reflex. Respect normal GitHub controls. |

## Qualification evidence already obtained

Before the user's review-first/test-once instruction, the storage build passed
608 tests with two existing ignored state tests. The startup regression and
shared-executor refactor subsequently passed 609 tests, with the same two
ignored tests and zero failures. These runs are development evidence; later
behavior changes invalidate the affected results. No further tests will run
during implementation under the new workflow.

The storage regression fails with the old fallback (`/complete` versus expected
`/processing`). Scoped clippy denied warnings successfully. Formatting and diff
whitespace checks passed. Real-tool tests ran with `NZBD_REQUIRE_TOOLS=1`.

| Artifact | Location |
|---|---|
| Storage qualification log | `/private/tmp/nzbd-build-artifacts/nzbd-storage-tests.log` |
| Latest qualification log | `/private/tmp/nzbd-build-artifacts/nzbd-storage-workspace-tests.log` |
| Scoped clippy log | `/private/tmp/nzbd-build-artifacts/nzbd-storage-clippy.log` |
| Build target directory | `/private/tmp/nzbd-storage-target` |

## Decisions needing reconciliation

The user's new workflow permits unattended implementation choices. The designer
has reconfirmed design ownership and will provide the final release from its
clone. The earlier pause and design/merge review requirement were disclosed to
the user as a conflict; implementation continues within released scope.

Inventory currently prunes events older than 90 days without a completeness
watermark. A missing retention event cannot alone disprove an old operator Keep.
Inventory also lacks a generation-scoped live PP-reader registry. The safe
retirement implementation needs an explicit active-use fence; a terminal stage
alone is insufficient.

Local finalization suppresses history-record failures and ignores terminal
import results. Retirement evidence must independently require successful
durable history and stamp commits. Remote pipeline return precedes authority
acceptance, so remote retirement must wait for that acceptance.

## Workspace and cleanup

All implementation occurs in the isolated clone
`/private/tmp/nzbd-par-repair-progress`, branch `codex/par-repair-progress`.
Storage and workspace groundwork are separate commits; the redundant worktree
and temporary fixture/scripts have been removed. The exploratory repair patch
is preserved at `/private/tmp/nzbd-build-artifacts/paused-repair-exploration.patch`
until the final design replaces it. The user's
`~/code` repositories are not build checkouts. No deployment, live job change,
production deletion, or failed-payload retention change is included.

Temporary fixtures, scripts, logs, and obsolete checkouts will be removed when
no longer needed; completed code, the status page, and required audit evidence
will be preserved through the PR.
