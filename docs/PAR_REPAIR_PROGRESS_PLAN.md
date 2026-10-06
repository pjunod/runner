# PAR repair progress — bounded scanning and useful recovery retries

**Status:** complete design released for build; final workflow below supersedes
earlier pause/review/test sequencing · **Written:**
2026-10-06 · **Design owner:** originating chat · **Builder:** GPT-6.1 Sol

Companion to [ARCHITECTURE.md](ARCHITECTURE.md), which describes the
post-processing pipeline, and [FILE_LIFECYCLE_PLAN.md](FILE_LIFECYCLE_PLAN.md),
which explains file custody. This document specifies the repair performance,
recovery correctness, and progress changes requested after job #1757 on nuc3.
Implement milestone by milestone, record actual validation here, and deliver
through a reviewed GitHub PR. If an optimization would weaken file custody,
final content verification, or cancellation fencing, change the optimization.

## 1. Objective and evidence

Make repairing a damaged multi-volume download proportional to its data size,
reuse established work when additional recovery files arrive, and let an
operator distinguish scanning, waiting, reconstruction, and failure.

**Source baseline:** inspected checkout commit
`534682e8fd52f7edcad6f7e5febe977b0c4fea12`. The running container reported
`0.2.0+unknown`, built 2026-10-05 21:10 UTC. Its exact commit is unverified;
do not equate the checkout with the deployed binary. Recheck all interfaces
against current `origin/main` before implementation.

### 1.1 Confirmed observations

Job #1757 contained about 13 GB of payload and roughly 60 equal-sized archive
volumes. Downloading finished at 16:38 EDT on 2026-10-06. The UI recorded
repair spans of 25 minutes, 22 minutes 58 seconds, and 23 minutes 5 seconds,
followed by another active span. Additional recovery downloads completed near
17:04, 17:27, and 17:50 EDT.

At 18:04:02 and 18:04:14 EDT, the daemon's `rchar` counter advanced from
5,848,128,338,592 to 5,862,830,840,731 bytes. An open file handle changed from
archive part02 to part37. This establishes activity, not successful repair.
The counter is process-wide and includes cached reads; it is not a measurement
of physical disk traffic or a precise job-specific benchmark.

The inspected [repair workspace](../crates/nzbd-post/src/repair_workspace.rs)
loops over catalog files and candidate files. For each equal-sized pair it
computes a full MD5, then reads block CRCs on a mismatch. It repeats this
matching on every recovery round, including when workspace copies exist.
For around 60 volumes of 200 MiB this implies approximately 1.5 TB of logical
reads per round, before other verification. This is a complexity estimate.

### 1.2 Hypotheses that must be reproduced

The recovery files downloaded later have obfuscated names without `.par2`.
[PAR discovery](../crates/nzbd-post/src/par2.rs) recognizes their packet magic,
but the [subprocess wrapper](../crates/nzbd-post/src/tools.rs) passes only the
main PAR filename to `par2 verify` and `par2 repair`. Reproduce whether the
installed tool discovers those companions; do not assume that internal PAR
discovery means the subprocess consumes them.

The workspace inspection during a scan showed an earlier recovery volume
and the main index. Copying recovery files happens after matching, so that
snapshot alone does not prove that later files are permanently ignored.

## 2. Scope and invariants

**Storage placement finding (2026-10-06):** the live configuration sets
`paths.main_dir = "/processing/"` and
`paths.dest_dir = "/working/monarr/completed"`, with no `paths.inter_dir`.
The inspected `Config::download_dir()` falls back to `dest_dir` when
`inter_dir` is absent or empty; `main_dir` controls state/default paths rather
than implicitly enabling intermediate downloads. Thus unfinished source files
are already in the directory named completed, while the repair workspace is
under `/processing/queue/transform-workspaces`. This is not evidence of an
early successful-completion notification. The user's subsequent decision is
to default absent/empty `inter_dir` to expanded `main_dir`, as specified below.
Existing jobs
retain their recorded custody paths; do not manually relocate an active job.
Include regression coverage for configured intermediate storage and document
the distinction consistently: CONFIGURATION.md's earlier category discussion
currently says the engine always writes under `paths.dest_dir`, contradicting
its later intermediate-path section. Fix that stale explanation in this PR.
Live configuration changes or deployment remain outside this build request.

**User requirement superseding legacy fallback (2026-10-06):** "completed is
completed." Treat the preceding description as diagnosis of existing behavior,
not acceptable product semantics. A destination for completed output must not
also serve as the implicit download/repair directory. The originating chat
owns the revised storage design. The default is now decided below; remaining
repair and lifecycle design is still paused. Do not preserve
the empty-intermediate fallback merely for compatibility, and do not silently
move active files outside the custody journal. Sol is the builder, not the
design owner. The original merge-through instructions are suspended until the
originating chat releases the finalized design and later reviews the PR.

**Approved fallback contract:** `Config::download_dir()` returns expanded
nonempty `paths.inter_dir`, otherwise expanded `paths.main_dir`. Both absent
and empty intermediate values select processing, never `dest_dir`. No new
setting or appended subdirectory is needed. On nuc3 the new allocation root
is `/processing/`; successful publication still targets
`/working/monarr/completed`. Existing allocated jobs retain their recorded
custody paths; changing the default is not permission to move active files.
Sol may build and test this narrow change now while other design is paused.

Test absent/empty TOML intermediate values, explicit overrides, home expansion,
NZBGet conversion, unchanged completion destinations, new allocation and
processing under `main_dir`, successful publication to global/category
destinations, failed processing remaining not-ready, and unchanged custody of
previously allocated jobs. Verify single-node/cluster call sites, capacity-root
coverage, and exclusion of internal queue/state directories from job discovery.
Update the old regression named
`intermediate_directory_is_explicit_and_empty_preserves_destination` and
contradictory configuration documentation. Do not change production settings.

**Retention evidence (2026-10-06):** read-only inspection found 20 transform
workspaces using about 122 GiB: 19 retained and one active. Seventeen retained
records still carry `keep=true` and `hold="transform workspace"`; two carry
an allocation/recovery review hold. The finish path marks scratch retained
without releasing its automatic keep/hold, and normal expiry eligibility only
covers `parked_failed` and `recovery_imported`. This requires a deliberate
workspace retirement design; do not remove files by age or name alone.

The failed directory uses about 69 GiB. Live settings enable retention with
`failed_retention_days=7`, and `post.failure_action="park"`. Twenty-four
parked failed records have seven-day retention, no keep/hold, and accumulated
eligible time below seven days (oldest about 4.7 days). That is a configured
retention window, not evidence that the failed-file sweeper is disabled.
Separate retained relocation remnants exist and require individual custody
checks. Do not change failed retention or delete existing files without a
specific user instruction. Include visible retention/hold reasons in the
remaining lifecycle design rather than treating all disk use as one defect.

### 2.1 Workspace retirement — approved design, 2026-10-06

**Scope:** fix automatic retention of obsolete transform scratch and reconcile
existing inventory-owned workspaces. This authorization does not change failed
payload retention, completed payload retention, or delete live production data.
Sol implements this design; the originating chat reviews the implementation
before PR merge or any production cleanup.

**Observed defect:** `workspace()` sets scratch `keep=true` and
`hold="transform workspace"`. `finish_workspace_output()` changes its state
to retained but never releases those automatic protections. `Artifact::eligible`
and `tick()` only expire failed/recovery-imported payloads. Seventeen legacy
retained workspaces still have the transform hold; two have generic recovery
holds. Two retention events exist on scratch records, so blindly clearing all
keep flags would overwrite actual operator decisions.

**Lifecycle decision:** preserve scratch during the entire live processing
attempt, including downstream script/move failures that can retry. Retire it
after durable job finalization and writer quiescence, not merely after PAR or
extraction returns. Failed scratch can retire after the originals have been
durably parked/retained or explicitly deleted and no retry owns the scratch.
Use the same generation and lease fences as publication. Retirement is a
separate internal resource lifecycle, independent of seven-day failed payload
retention and its enable/disable switch.

**Completion evidence:** new finalization records a durable retirement receipt
referencing source artifact ID/generation, transform operation ID, scratch
ID/generation, and the terminal outcome. Record it only after the local
durable completion boundary, or cluster-authority acceptance for remote work,
and after workers have stopped. A history name, elapsed age, directory prefix,
or missing source alone is not completion evidence. History matching for legacy
cleanup must use `Artifact:Id`/operation generation, not a possibly reused job ID.

**Existing-workspace reconciliation:** enumerate transform operations, not
arbitrary directories. A terminal transform plus a completed parent with
durable successful job evidence can retire. An explicitly deleted parent with
a successful deletion operation for that generation can retire its unused
scratch. A failed repair transform with a finalized parked-failed parent and
intact retained originals can retire. A `source_gone` record requires evidence
of successful finalization/import or explicit deletion; disappearance by
itself does not qualify. Running/review operations, transitioning parents,
unknown ownership, changed identities, missing evidence, and generic review
holds remain visible with specific reasons. Do not automatically clear the
two generic legacy review holds. For legacy scratch, an operator retention
event blocks automatic migration; absence of such events is usable only where
the event history is complete. Ambiguity stays held.

**Keep provenance:** introduce an explicit distinction between automatic
workspace protection and operator Keep. New scratch must not acquire an
operator Keep merely because it needs temporary protection. Record operator
retention changes durably. A retirement receipt authorizes clearing only the
automatic transform hold; later user Keep/recovery/review holds veto deletion.
Do not infer this distinction from `keep=true` alone on old records.

**Deletion:** enqueue a dedicated internal `retire_workspace` operation on the
existing inventory task runner. Its authorization is the retirement receipt,
not `automatic=false` passed to ordinary user deletion. Refactor the existing
identity/manifest-checked deletion executor for shared use rather than adding
recursive path deletion. Revalidate receipt, generation, ownership, live-use
fences, and operator holds immediately before deletion under the mutation
coordinator. Persist admission atomically with the receipt and protection
transition so a crash cannot strand unprotected scratch or authorize a reused
path. Preserve retry/backoff, changed-file review, and partial-deletion replay.
Keep operation metadata and manifests according to existing diagnostic
compaction policy; freeing payload bytes must not erase the audit record.

**Scheduling:** kick reconciliation on terminal job finalization and startup;
continue in bounded batches of at most 25 retirements per existing 30-second
maintenance tick. Cleanup I/O runs outside async engine workers. No extra
seven-day retention applies to eligible scratch. Files with active readers or
writers never qualify. Keep progress and cleanup errors visible in the Files
inventory/logs. A single held workspace must not starve the next batch.

**Restart semantics:** a retired workspace cannot be reused from an old
operation record. An explicit post-processing restart with valid retained
source inputs must allocate a fresh owned scratch generation before writing;
it must not resurrect a deleted identity or accidentally resume a previous
deletion authorization. Startup must distinguish scratch artifacts from
job-owned payload allocations so it does not blanket-reclassify valid internal
scratch as "allocation has no recovered job" solely because `job` is null.

**Required tests:** successful whole-job cleanup; scratch retained between
extraction and final job commit; failed-job scratch cleanup with original
payload untouched; active attempt/recovery/lease protection; explicit Keep
before and after retirement admission; legacy terminal cleanup; legacy
operator-retention event protection; source-gone without completion evidence;
generic review holds; symlink/directory replacement; changed/unowned entries;
crash before/after receipt and during deletion; delete retry; fresh workspace
generation on explicit restart; clustered completion acceptance; retained
scratch survives unrelated failed-retention policy changes; bounded batches
make forward progress past protected records.

**Failed payload evidence:** on 2026-10-06 the 24 parked-failed records contain
19 `FAILURE/HEALTH` jobs (13.62 GiB logical inventory size) and five
`PAR_FAILURE` jobs (45.38 GiB). The directory's approximately 69 GiB disk use
also includes retained relocation remnants and may differ from manifest sums.
Given the reproduced obfuscated-PAR discovery problem, do not assume these
PAR failures are irrecoverable. Preserve them while assessing repairability;
the user has not requested deleting failed downloads or changing their policy.

Deliver optimized discovery/matching, retry reuse, explicit recovery inputs,
bounded retry decisions, cancellation-aware scanning, and visible progress.

1. **Preserve original custody.** Only inventory-owned, eligible source files
   and engine-authorized partial checkpoints may become repair inputs.
2. **Preserve final verification.** Full expected length and MD5 remain
   mandatory before publishing repaired output. CRC matches identify
   candidates; they do not establish final correctness.
3. **Preserve ambiguity handling.** Duplicate or equally ranked candidates
   must not become an arbitrary first-match selection.
4. **Preserve attempt fencing.** Cancelled or superseded attempts cannot
   publish files or replace progress from a newer attempt.
5. **Bound I/O, memory, and retries.** Large payloads stream through bounded
   buffers. A retry requires new usable evidence, not merely elapsed time.
6. **Keep compatibility.** Existing stage names, history interpretation,
   external clients, and serialized state must continue to work.

**Non-goals:** no new PAR reconstruction algorithm, dependency replacement,
global scheduler redesign, torrent changes, automatic regrab policy, broad
file-lifecycle rewrite, or production deployment. Do not restart, cancel,
rename, or alter live job #1757 as part of implementation or qualification.
Use synthetic fixtures and disposable directories. Merging does not prove
that the deployed container has the fix.

## 3. Existing contracts and proposed boundaries

Re-verify these exact existing interfaces at build time:

```rust
// crates/nzbd-post/src/repair_workspace.rs
pub async fn repair(
    inventory: &nzbd_state::artifacts::Inventory,
    job: u32,
    set: &Par2Set,
    tool: &Par2Tool,
    partials: &[(std::path::PathBuf, String)],
    restored: &mut std::collections::HashSet<std::path::PathBuf>,
) -> Result<VerifyResult, PostError>;

// crates/nzbd-post/src/par2.rs
pub fn load_sets(dir: &Path) -> Result<Vec<Par2Set>, PostError>;

// crates/nzbd-post/src/tools.rs
pub async fn verify_full(&self, main_par2: &Path)
    -> Result<VerifyResult, PostError>;
pub async fn repair(&self, main_par2: &Path)
    -> Result<RepairResult, PostError>;
```

The current [manager](../crates/nzbd-post/src/manager.rs)
`repair_isolated_loop` permits eight rounds. It calls workspace repair,
unpauses recovery blocks after `NeedMoreBlocks`, waits for those downloads,
and reloads the PAR set. `PostConfig::par_fetch_timeout` defaults to 600
seconds. Preserve these existing outer bounds unless a reproduced defect
requires changing them; do not add a longer timeout as the remedy.

### 3.1 One attempt owns a reusable repair session

Introduce a small repair-session object, owned by the manager's repair loop.
It should contain the source identity/generation, PAR set identity and slice
size, candidate fingerprints, validated mappings, prepared workspace inputs,
recovery packet identities, round statistics, and cancellation/progress hooks.
Adapt names and signatures to repository conventions; document final types.

The session survives recovery rounds within one attempt. Persistence of hash
caches across daemon restarts is not required. After restart, reconstruct
custody from the existing journal and safely rebuild any missing evidence.
Never make a cache authoritative over the inventory or trust `target.exists()`
as proof that a workspace file is complete and belongs to this attempt.

### 3.2 Candidate fingerprints are tied to stable inputs

Use inventory identity/generation plus opened-file identity and relevant
metadata to invalidate replaced or modified inputs. Check stability before
and after a scan; reject or rescan changed input. Metadata is an invalidation
hint, not a cryptographic content proof. Validate inputs through existing
file-opening and containment helpers to preserve symlink protections.

Cache full MD5 once per unchanged candidate. Compute slice evidence once per
candidate and slice size, either in that pass or lazily when needed. Index by
size/full digest and use prefix hashes or existing validated rename evidence
to narrow matching. Comparing cached hashes may be quadratic for ambiguous
sets; repeated whole-file I/O must not be. Avoid retaining payload bytes.

An exact filename is a hint. Prove identity before treating it as intact.
Retain support for damaged first blocks, obfuscated names, authorized partials,
missing files, short final slices with PAR padding, and multiple PAR sets.
Do not broaden eligibility for arbitrary `.part` files.

### 3.3 Recovery inputs are explicit and set-aware

Deduplicate usable recovery packets by set identity and recovery exponent.
Count validated packets, not filenames or estimated sizes, when deciding
whether recovery evidence increased. Reject corrupt/mixed packets as today.
Do not let the same exponent in another file count as additional capacity.

Pass the complete validated recovery input set to the subprocess through an
argv-only interface, or assign safe deterministic PAR filenames inside the
owned workspace. Prove the selected approach with a real `par2` fixture whose
recovery volumes have unrelated extensionless names. Account for argument
length, leading-dash filenames, spaces, and unrelated PAR sets. Never rename
the user's source files solely to satisfy tool discovery.

For large recovery volumes, inspect the existing 64 MiB discovery limit and
fixture sizes. Avoid accidentally expanding memory use; if it prevents valid
recovery for the reproduced case, implement bounded packet scanning with
tests rather than deleting the bound without replacement.

### 3.4 Retry decisions record useful change

```text
inventory and map once -> prepare workspace -> verify
                                               |
                     +-------------------------+---------------------+
                     | intact                  | repairable          | needs blocks
                     v                         v                     v
               validate/publish         reconstruct/validate    fetch missing data
                                                                     |
                                           no new usable evidence <--+--> new evidence
                                                   |                        |
                                             explain failure       add only new inputs
                                                                            |
                                                                       verify again
```

Record missing blocks, usable recovery blocks, newly downloaded inputs,
mapping scans, bytes scanned, and round duration. More usable recovery blocks
can justify a retry even if the reported deficit has not decreased yet.
Duplicate, corrupt, or unreadable downloads must not trigger the same full
scan again. If alternative eligible volumes remain, fetch a different bounded
batch; if none remain, terminate with a specific reason. Preserve the outer
attempt and fetch-time limits. Do not fail merely because reconstruction is
slow while measurable useful work continues.

## 4. Cancellation and progress are part of completion

Blocking filesystem scans must not monopolize an async runtime worker. Use
the project's blocking-work convention, bounded buffers, and explicit
cancellation checks between reads and files. Moving work to `spawn_blocking`
alone is insufficient: dropping its handle does not stop the worker. Ensure
replacement/retirement waits for relevant writes to quiesce and that no
cancelled worker can publish. Thread cancellation through existing attempt
controls; preserve lease checks and atomic publication.

Expose an optional additive repair-progress record through the normal engine
snapshot/API/SSE path. Proposed fields, to align with existing types:

| Field | Meaning |
|---|---|
| `attempt_id` | Fences updates from superseded attempts |
| `phase` | matching, preparing, verifying, fetching_recovery, reconstructing, validating |
| `files_done`, `files_total` | Current phase's known file work, not download counters |
| `bytes_scanned` | Logical scanner bytes for this attempt; not disk throughput |
| `round` | Current recovery round |
| `recovery_blocks_available` | Unique validated recovery blocks for this set |
| `additional_blocks_needed` | Most recent tool-reported deficit; unknown is null |
| `last_progress_at` | Time of actual completed work or new evidence |

Do not invent a percent or ETA for subprocess phases that cannot report one.
If tool output supports progress, stream bounded parsing while retaining output
caps and timeout behavior; otherwise show the phase and an unknown amount.
Coalesce progress to at most one published update per second, plus phase
changes and completion. Clear it on terminal/cancelled states. Avoid a durable
database write for every buffer and do not create a new high-volume event log.

The web job detail must show the phase and available counters. A useful line
is `Matching files: 42/62 · recovery round 2`; another is `Waiting for 3 more
recovery blocks`. Explain health as download health. Retain the existing
timeline's stage identity; additive subprogress is preferable to changing
persisted stage enums. Ensure mobile/API consumers tolerate absent/new fields;
a mobile redesign is outside scope.

## 5. Milestones and acceptance checks

### 5.1 M1 — reproduce and establish measurable contracts

Build synthetic multi-volume PAR fixtures with one damaged input and delayed,
obfuscated recovery volumes. Reproduce the repeated candidate reads and test
whether the current subprocess discovers those recovery files. Prefer small
real fixtures plus an instrumented reader over a multi-gigabyte CI fixture.
Record which findings are reproduced and which remain hypotheses.

**Acceptance:** regression tests fail for the intended reason on the old
implementation; fixture creation and tool requirements are reproducible.

### 5.2 M2 — reuse candidate identity and workspace preparation

Implement the session/index and bounded fingerprint scans. Preserve ambiguity,
partial eligibility, custody, and output verification. Handle input replacement
and invalid workspace copies. Reuse unchanged evidence across recovery rounds.

**Acceptance:** instrumentation shows each unchanged candidate receives at
most one full-MD5 scan and one slice scan per slice size during matching,
independent of catalog count. A second recovery round performs zero matching
payload reads for unchanged candidates. Report copy, subprocess verification,
and final validation reads separately; do not hide them in a headline metric.

### 5.3 M3 — consume recovery volumes and bound unproductive retries

Implement explicit subprocess recovery inputs and packet-aware retry decisions.
Prove that adding valid blocks advances recovery without repeating matching.
Preserve useful fetch escalation when some downloaded volumes are unusable.

**Acceptance:** a real-tool test repairs the extensionless recovery fixture to
the expected full MD5. Duplicate blocks do not count twice. Exhausted recovery
terminates with a specific reason and leaves originals intact.

### 5.4 M4 — cancellation and operator-visible progress

Integrate cancellation-aware blocking scanning and additive progress through
engine, API, SSE, and web UI. Exercise cancellation during scanning, workspace
preparation, waiting for recovery, and an active subprocess.

**Acceptance:** deterministic tests stop at the next cooperative checkpoint,
no further output is published, a new attempt cannot receive stale updates,
and UI/API tests distinguish phases without equating elapsed time to progress.
On a local synthetic workload, demonstrate cancellation response within one
second when storage reads are responsive; document that a kernel-blocked I/O
operation cannot promise that bound.

### 5.5 M5 — review, qualification, PR, and merge

Update this document with implementation locations, actual test commands,
before/after read counts, remaining limitations, and final status. Add a short
README reading-path link and update affected API/architecture documentation
in the same PR. Do not claim production validation from synthetic tests.

**Acceptance:** focused tests and required CI pass, the complete diff has been
reviewed, actionable findings are fixed, and the PR is merged through normal
repository controls. Record PR URL and merge commit. Do not bypass checks or
branch protection and do not use administrator overrides.

## 6. Required regression matrix

| Case | Required result |
|---|---|
| Many equal-sized intact volumes plus one damaged file | Linear payload matching reads; correct reconstruction |
| Additional PAR file on round two | New recovery accepted; old candidates not rescanned |
| Changed/replaced source between rounds | Cache invalidated or safe explicit failure |
| Replaced/incomplete workspace target | Never trusted merely because it exists |
| Damaged first block and obfuscated name | Block evidence still maps the eligible candidate |
| Equal-ranked candidates or duplicate content | Existing ambiguity semantics preserved |
| Authorized partial versus unrelated `.part` | Only authorized checkpoint participates |
| Short final slice / missing whole file | Correct padding and final full identity check |
| Extensionless PAR volumes / spaces / leading dash | Real subprocess receives intended inputs safely |
| Duplicate recovery exponent / different set | No false increase in usable blocks |
| Corrupt recovery data / exhausted volumes | Bounded fetch/retry with actionable reason |
| Cancellation and replacement during scanning | No late writes or stale progress |
| Restart with existing workspace | Custody re-established; no unsafe cached trust |
| Symlink or traversal candidate | Existing containment rules enforced |
| Old serialized job / missing progress field | API and UI compatibility preserved |
| Tool timeout / missing tool / output cap | Existing error and cleanup contracts preserved |

## 7. Qualification commands and evidence

Inspect [Makefile](../Makefile),
[CI](../.github/workflows/ci.yml), and
[lint](../.github/workflows/lint.yml) before running current equivalents.

```bash
cargo fmt --all --check                         # Formatting gate
cargo test --locked -p nzbd-post                # Repair and pipeline regressions
cargo test --locked -p nzbd-state               # Custody/workspace changes, if touched
cargo test --locked -p nzbd-engine              # Attempt and progress changes
cargo test --locked -p nzbd-api                 # API compatibility and UI coverage
cargo clippy --workspace --all-targets -- -D warnings  # Repository lint gate
```

Use the strict tool-backed test lane from the Makefile so required PAR tests
cannot silently skip. Run relevant focused tests during development and one
complete qualification pass when the change is ready; rerun for meaningful
changes or failures. Required GitHub checks determine merge readiness. Report
pre-existing failures honestly and resolve or obtain an explicit disposition;
never silently waive them.

Read-count regression tests are the performance gate. Record wall-clock
measurements on a disposable representative fixture as supporting evidence,
including file count/size, recovery rounds, tool version, storage, and whether
the cache was warm. Do not promise a specific production completion time.

## 8. Execution and delivery instructions

The user requested a GPT-6.1 Sol session to implement this plan and deliver a
proper PR to merge it. Work on `codex/par-repair-progress` in an isolated clone
or suitable isolated worktree based on current `origin/main`. The originating
checkout has unrelated work on `codex/retry-nzb-rate-limits` and untracked
files; do not switch its branch, stage its unrelated files, or reset it. Copy
this plan into the implementation checkout and include it in the PR.

Use a cohesive PR title and description stating the original trigger, changed
behavior, relevant risks, and actual validation. Review the entire diff after
implementation, including state migration/cancellation implications and
failure-path file preservation. Address findings, push, attach the PR to the
implementation chat, wait for CI, and merge using ordinary repository policy
once qualified. If permissions, external reviews, or unresolved checks block
merge, leave a concrete reviewable PR and report the exact blocker.

The implementation session owns follow-through; creating a draft PR is not
completion. Keep the user informed of meaningful milestones. Do not deploy to
nuc3 or change the running queue as part of this request.


## 9. Final design decisions and execution release — 2026-10-06

This section supersedes earlier alternatives, implementation pauses, test
sequencing, and the requirement for a separate originating-chat merge approval.
The originating chat owns these design decisions. Sol implements them and may
complete the PR workflow after qualification without waiting for the user or
an additional parent approval. Report architectural deviations explicitly in
the status page; routine implementation choices do not need another approval.

### 9.1 Candidate scanning and retry reuse

Use one in-memory `RepairSession` per PAR set per PP attempt. Compute full MD5
and padded slice CRCs together in one streaming pass per candidate; index
intact identities by length and full MD5 and compare damaged candidates using
cached CRC vectors. Preserve existing positive-score and ambiguity semantics,
including duplicate intact candidates. Candidates with no slice catalog still
receive full hashing. Scan with a 64 KiB read buffer, not a payload-sized
allocation. Bound accumulated fingerprint metadata to 64 MiB per session;
fail with an actionable resource-limit error if exceeded. That bound is
metadata, not a limit on payload bytes. Do not add a feature flag.

Invalidate cached evidence on source generation, identity, size, modification
metadata, or slice-size changes. Validate stable descriptors before and after
scanning. Cache is attempt-local only. Store workspace-input identity and
expected digest after successful preparation, recheck it before reuse, and
rebuild invalid entries through existing owned publication primitives. On
restart re-establish custody and evidence; existing filenames alone prove
nothing. New recovery rounds add only new recovery inputs and reuse unchanged
candidate fingerprints and prepared payloads.

### 9.2 Recovery discovery and subprocess inputs

The originating chat independently reproduced the defect using local par2:
verification with extensionless recovery companions returned exit 2 and
`You need 1 more recovery blocks`; copying those companions to
`catalog.recovery-000000.par2` and analogous names made verification return
exit 1 with `Repair is possible`. Sol independently reproduced the old failure
with spaces and leading dashes. This is now a confirmed local reproduction,
not a claim that every production PAR failure has that cause.

Normalize names only in the owned scratch workspace. Choose a generated main
basename outside the source catalog namespace and use same-prefix `.par2`
companion filenames with persistent per-session ordinals. Check all generated
names for collision with source catalog entries before copying; choose another
prefix if needed. Keep that prefix stable across rounds. Names contain no
source-controlled leading dash or directory traversal. Call the tool with the
safe generated main path; real-tool tests must prove all companions are
consumed. Preserve original filenames on disk outside scratch. Copy metadata
and valid recovery files once, recording source/target identities.

Replace whole-recovery-file loading with streaming packet validation: 64 KiB
buffers for packet hashing, at most 64 MiB retained metadata per file, checked
packet lengths and truncation, mixed-set/digest validation, and deduplication
by set/exponent. Never allocate the recovery packet body to parse its exponent.
Keep the existing in-memory parser for small callers, but ensure both paths
agree on validation via shared parser logic or parity fixtures.

A verification round may repeat only when the set of validated recovery
exponents increases or a source fingerprint changes. Duplicate/unusable
volumes may trigger another download selection while files remain, but do
not trigger a payload rematch or unchanged verification. Bound selection by
available NZB recovery files and the existing eight-round/600-second limits.
Report exhausted recovery, invalid inputs, or a tool/discovery disagreement
explicitly. Do not treat zero new exponents as successful repair progress.

### 9.3 Cancellation, quiescence, and progress

Add a generation-scoped attempt-use guard, acquired under the inventory's
mutation coordinator before opening workspace inputs. Retirement and restart
admission consult the same registry. Use a cancellation token and tracked
blocking workers; workers own a use-guard reference until actual exit.
The async attempt supervisor cancels then awaits those workers before
releasing the job's replacement/retirement boundary. Apply this to local and
cluster call sites: dropping a pipeline future must not imply worker exit.
Keep existing lease authority checks immediately before publication and
cluster completion acceptance before retirement receipts. A node without
proof of worker quiescence cannot retire another node's scratch.

Cancellation checks run between 64 KiB reads/copy writes and immediately before
publication. Tests coordinate explicit checkpoints rather than sleeping to
infer exit. Progress uses a bounded latest-value channel from blocking workers,
then an engine-owned transient map keyed by job and attempt identity. Register
and close attempt identity explicitly. Reject stale updates after cancellation
or replacement. Persist no per-buffer progress. Use existing snapshot/API/SSE
paths at one update per second plus phase transitions. Keep persisted stage
enums unchanged. Subprocess phases show phase/time with unknown percentage;
streaming and interpreting tool percentages is outside this change.

### 9.4 Legacy retention history and workspace retirement

Select the conservative legacy rule: no event-history completeness watermark
means absence of retention events does not prove absence of operator Keep.
Do not automatically clear ambiguous legacy Keep. Add durable operator-keep
provenance for new workspace records, separate from automatic use protection.
For legacy records, compute and expose a cleanup assessment with source/op
completion evidence, current holds, identity result, bytes, and explicit
`operator_keep_provenance_unknown` reason where relevant. No global enable
flag or manual feature gate is added; new eligible scratch retires normally.
A later explicit per-record retirement authorization may resolve ambiguous
legacy provenance using the existing reviewed inventory action pattern.
Do not change user decisions by assuming that a young timestamp proves audit
completeness. Existing scratch with positively established automatic protection
and full completion/quiescence evidence may retire using §2.1.

Complete the generation-scoped attempt-use guard from §9.3 and share it with
retirement/restart admission. A pending delete owns its scratch generation;
a restarted job uses a new owned scratch generation and path. Keep source
payloads, operation receipts, and small diagnostics distinct from scratch.
No failed payload retention change is included.

### 9.5 Commits, review, qualification, and cleanup

The user requires proper cohesive commits batched into one substantial PR.
Do not use or leave files in the user's `~/code` checkouts. The design owner
works in `/private/tmp/nzbd-repair-design`; the builder uses its isolated clone
and worktrees. Copy the finalized plan into the implementation branch. Keep
`docs/PAR_REPAIR_PROGRESS_STATUS.md` updated at meaningful milestones with
branch/PR, implemented scope, decisions, blockers, review, and validation.

Build the complete change before requesting an adversarial agent review.
This is explicit authorization to use an adversarial review subagent at that
point. Address its actionable findings before running unit-test qualification.
Do not repeatedly run suites during development. Prior test runs already
occurred before this instruction; record them as historical, not compliance
with the new sequence.

Each required test must pass on code relevant to the final merge. Run the
necessary qualification once after review fixes. For failures, rerun the failed
test and any previously passed tests whose covered behavior changed in the
fix; do not rerun the entire suite reflexively. Required hosted CI still
applies; avoid empty commits and avoid triggering redundant CI pushes during
local iteration. Never bypass protection or claim an old pass validates
changed code. Once review findings are fixed and required checks are green,
merge through ordinary repository policy. If external approval is required,
report that concrete blocker rather than silently waiting.

No new optional feature switches. If an enable UI becomes absolutely necessary,
record why and follow the user's dev-settings advisory-only requirement; do
not convert data-integrity invariants into optional toggles. Use judgement
for unattended implementation decisions and record them for the final report.
Keep diagnostic/sample files in the isolated workspace; remove disposable
fixtures, build helpers, and obsolete worktrees after delivery, preserving
unmerged work until it is safely included in the PR. Clean up the design clone
only after its documents are committed and available in the delivered branch.
Production deployment and live deletion remain separate from this code PR.
