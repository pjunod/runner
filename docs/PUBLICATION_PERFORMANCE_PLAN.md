# Publication performance — root cause and proposed fix

**Status:** implementation candidate; Astra PR review pending; unit tests not
run; deployment not started · **Written:** 2026-11-03 (America/New_York) · **Branch:**
`codex/runner-publication-performance`

Companion to [the file lifecycle contract](FILE_LIFECYCLE_PLAN.md) and
[storage configuration](CONFIGURATION.md): this document explains the observed
slowdown, separates necessary integrity checks from redundant work, and defines
the correction and its qualification gates. Implement the milestones in order.
Preserve existing custody, collision, crash-recovery, and completion guarantees;
an optimization that cannot preserve them needs a revised design here first.

## 1. Finding — verification layers multiply payload reads

Runner turns namespace operations into repeated content scans. Each layer
independently proves the same bytes without carrying usable evidence to the
next layer. Network filesystems magnify the cost. A plain rename can therefore
read a large file five times, and completed-folder publication adds several
more scans after the actual copy.

The fix is to assign each proof to one operation boundary, carry it through
unchanged object identities, and verify again only when copying bytes, recovering
after interruption, or crossing a namespace transition that invalidates the
existing evidence. Increasing buffers or replacing the copy loop alone does
not remove the dominant repeated work.

### 1.1 Evidence collected on nuc3

Read-only inspection on 2026-11-03, approximately 15:34–15:40 EST, used the
container configuration, API, logs, `/proc` counters/file positions, and SQLite
opened with `mode=ro` and `PRAGMA query_only=ON`.

| Observation | Evidence and interpretation |
|---|---|
| Processing mount | `/processing` maps to `/mnt/processing`, GlusterFS FUSE. |
| Completed mount | `/working` maps to `/mnt/qnap/working`, NFS 4.1; destination is `/working/monarr/completed`. This is a real cross-filesystem copy. |
| Configuration | `main_dir=/processing/`; no configured `inter_dir`. Commit `6f7149ce` on October 6 changed the empty-intermediate fallback from destination to main directory. This introduced a transfer for allocations that previously started on completed storage. |
| Deployed checkout | `/opt/noirr/runner` was at `0a4f172d`. The API reported `0.2.0+unknown`; binary-to-checkout identity is therefore not independently proven. The observed journal behavior matches that checkout. |
| Large job | Job 2312, Maurice, was still in `par_rename` after about 56 minutes. Its media file is 88,683,137,204 bytes, approximately 89 GB. |
| Reads during that job | One worker's `/proc/.../io` showed `rchar=400869630465`, about 401 GB. Open payload descriptors showed the same source inode under original and restored names; a two-second sample advanced by 223,346,688 bytes, about 106.5 MiB/s. This identifies active reading before the completed-folder move. |
| Directory fallback | Five recent successful directory-publication journals had `linked=true`. The deployed filesystem path uses exclusive mkdir/link publication rather than atomic flagged directory rename. No new live filesystem capability probe was performed. |
| Hidden completion cost | Job 2309 had roughly 296 seconds between the end of its last stored stage and its history completion timestamp. Other successful jobs had 98–396 seconds. These are publication/finalization gaps, not isolated copy benchmarks. |
| Repeated blocked cleanup | Logs contained 51,141 source-retirement warnings in the preceding 24-hour query. A separate one-hour query contained 2,642 warnings across 25 operation IDs, about 106 repeats per operation. |

`rchar` counts application reads, including reads served from cache; it is not
network traffic or physical disk I/O. Descriptor positions are progress
observations, not a controlled throughput benchmark. The warning count proves
repeated attempts, not their percentage of the slowdown. No production payload
was renamed, retried, deleted, or benchmarked during this investigation.

The implementation baseline is main `ea8a402b`, which includes later repair and
history fixes. Re-verify the source interfaces below before implementation.

### 1.2 Same-filesystem rename performs five full reads

The ordinary intact PAR filename-restoration path is:

```text
PAR full MD5 match                         1 payload read
  -> restore_file: journal SHA-256         1 payload read
    -> rename_exclusive fallback:
       hash original                      1 payload read
       link without replacement
       hash linked name                   1 payload read
       hash original name                 1 payload read
       unlink original
```

The last two names refer to the same inode. Hard linking has not copied the
payload. The primitive repeats content verification already owned by the
restoration journal. An ordinary successful first attempt therefore performs
five full reads, before repair, extraction, or completed publication.

Owners: [PAR matching](../crates/nzbd-post/src/rename.rs),
[`restore_file`](../crates/nzbd-state/src/artifacts/transforms.rs), and
[`rename_exclusive`](../crates/nzbd-state/src/artifacts/fs.rs).

### 1.3 Copy and publication independently rescan the same payload

[`populate_relocation_scratch`](../crates/nzbd-state/src/artifacts/relocation.rs)
copies each file in 64 KiB chunks, flushes it, rereads the source to hash it,
and rereads the copy to compare hashes. Renaming the verified temporary file
can then take the three-scan regular-file fallback from §1.2.

[`publish_directory_unlocked_checked`](../crates/nzbd-state/src/artifacts/publication.rs)
then independently hashes each regular file while capturing its manifest,
checking it before rename, checking it for the link fallback, checking before
and after linking, and verifying the final directory. That is six directory
publication scans on the ordinary linked path, in addition to copy verification
and temporary-file rename. Staging cleanup can add another digest scan.

These are source-derived counts for the stated successful path, not measured
physical reads. Replays, native rename support, and cache residency differ.

### 1.4 Two cleanup schedulers disagree about blocked work

[`retire_delivery_source`](../crates/nzbd-state/src/artifacts/relocation.rs)
records blocked cleanup and exponential delays for transient errors. The
relocation reconciler separately selects succeeded moves with
`cleanup_done=false`, in batches of 25, without respecting the Delivery's
blocked state or next retry. Each sweep can revisit the same retained archive
or unknown file and emit the same warning. A fixed first batch can also delay
later cleanup obligations.

### 1.5 The move occurs after the job snapshot used for history

[`process_job_ctx_from_inner`](../crates/nzbd-post/src/manager.rs) closes the prior
stage timeline and exports `fin`. Selected publication then enters `Move` and
does its work, but terminal history is built from that earlier `fin` snapshot.
Later importing it can also overwrite the updated live timeline. The result is
real publication work with no corresponding stored move span.

## 2. Why any hashing remains

`mv` trusts a successful same-filesystem rename to change a name while retaining
the same inode. That operation does not need a checksum to prove a byte copy.
Rsync also trusts filesystem operations; it computes transfer checksums while
moving bytes. Its local-path transfers default to whole-file mode, including
mounted network paths. See the [official rsync manual](https://download.samba.org/pub/rsync/rsync.1).

Runner additionally owns durable recovery and source deletion across restarts,
multiple workers, external importers, and selected-file publication. Hashes
serve three distinct purposes there:

| Purpose | Required evidence |
|---|---|
| NZB/PAR integrity | The protocol/catalog CRC, MD5, or PAR evidence that the downloaded or repaired content is correct. Preserve this validation. |
| Cross-filesystem copy | A digest of the bytes actually read from the source and a matching digest from the flushed destination. Preserve one destination readback; `fsync` is durability, not checksum verification. |
| Interrupted or externally changed custody | Durable generation/object identity and content evidence when existing metadata cannot prove the transition. Preserve conservative revalidation on replay or ambiguous mutation. |

The problem is repeated proof, not distrust of `mv`. No checksum establishes
ownership by itself, and checksums do not solve arbitrary concurrent writers.

**Change time matters.** `Identity` includes mount/inode, size, mtime, and ctime.
Link, unlink, and rename can change ctime even when content is unchanged. An
external same-size write followed by restored mtime can also change ctime.
Blindly ignoring ctime, or updating a cached digest's stamp after a link, would
weaken current tamper detection. The optimized design must distinguish an
unchanged object from an ambiguous change and keep final content validation
where the distinction cannot be established from metadata.

## 3. Contract — operation-scoped evidence and cheap namespace primitives

Existing interfaces, copied from the implementation baseline:

```rust
pub fn rename_exclusive(from: &Path, to: &Path) -> Result<()>;
pub fn copy_publish_checked(
    source: &Path,
    target: &Path,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<()>;
pub fn restore_file(&self, job: u32, from: &Path, to: &Path) -> Result<()>;
```

Definitions live in [filesystem primitives](../crates/nzbd-state/src/artifacts/fs.rs),
[file publication](../crates/nzbd-state/src/fileops.rs), and
[transforms](../crates/nzbd-state/src/artifacts/transforms.rs).

### 3.1 Namespace operations prove object identity without independent scans

Keep native exclusive rename first. The regular-file fallback uses anchored
parent descriptors, exclusive `linkat`, exact inode checks at both names, and
unlink only after the target is proven to reference the captured source.
Never replace a different inode merely because its bytes match. Preserve
file and directory flush ordering, symlink rejection, and both-name replay.

Move content-proof policy out of the generic rename primitive into the owner
that knows whether content proof is required. Enumerate every caller before
changing the primitive: transforms/replay, temporary-copy publication, URL
resolution, torrent adoption, engine recovery, and post-processing renames.
No caller may silently lose a content check it depends on.

For PAR restoration, compute SHA-256 alongside the already-required MD5 in the
same streaming read. Return the identity captured and checked on that same
descriptor/read; do not recapture a newer identity afterward and attach it to
an older digest. Carry the digest and captured source identity into the
restore journal. Where a controlled ctime transition makes content identity
ambiguous, verify the final inode once after the namespace transition, rather
than hashing each alias. This targets two reads instead of five while keeping
the existing content comparison. Direct callers without prior evidence retain
a single capture pass plus the necessary final check.

Apply the owner's final digest check to retained-original moves and every
restore replay branch, which also depend on the primitive's checks today.
Correct both-name restore replay: a link changes ctime on both names, so exact
comparison to the pre-link ctime rejects a legitimate interrupted move.
Require both anchored names to refer to the journaled object, matching
size/mtime, and a fresh digest equal to the journal's digest, with exact
identity stability during the scan. Verify the final object and authority
again before success. Preserve the legacy tuple journal format and refuse
different-inode adoption.

### 3.2 Copy once and verify at the owning commit boundary

```text
source read + streaming digest
       |
       v
private destination copy -> flush -> journaled namespace publication
                                              |
                                              v
                            final destination readback + digest
                                              |
                                  exact identity/authority recheck
                                              |
                                              v
                                    owning custody commit
                                      /               \
                                     v                 v
                            READY / completion    eligible cleanup
                                                       |
                                              retire staging afterward
                                                       |
                                        required ctime proof/manifest refresh
```

Collect the source digest during the copy rather than rewinding the source.
Keep source identity checks before/after reading and before completion. Carry
the digest through private staging, including temporary-file publication, so
the directory layer does not independently rediscover it.

Place the first destination readback after the namespace work that precedes
the owning custody commit. Flush the written file and affected directories,
hash the final descriptor, then recheck its exact identity and the owning
generation. Keep the existing retirement-after-owner-commit ordering:
`retire_publication_source_unlocked` must not run early merely to eliminate
a scan. Original processing bytes remain protected until the applicable
durable completion/retirement authority permits disposal. Directory presence
remains insufficient evidence of completion.

Post-commit removal of a staging hard link can change ctime on the published
inode. Preserve a second content check when the existing cleanup/identity
refresh contract requires it. That is a justified boundary, unlike repeated
hashes of each alias at each layer. Do not set `published`, `succeeded`, or
`retirement_done` before their existing semantic conditions hold. An unverified
publication state and pre-commit scratch retirement would require a separate
crash/downgrade design and are outside this correction.

Completion and cleanup are independent after verified custody commit. Cleanup
failure never retroactively makes publication unsuccessful and never grants
permission for additional deletion. Stale or ambiguous evidence after an
unlink blocks further cleanup until its journal is reconciled; preserve the
published result and the unresolved obligation.

Use a small internal verified-file value tying digest, byte count, source and
destination identities, generation, and operation ID together. It must not be
a global pathname cache. Evidence is usable only by that owner and exact
captured objects. Reopen through anchored descriptors; path names alone are
not proof. Any unexpected identity change invalidates the fast path.

New journal fields must be additive with conservative defaults. After restart,
an old journal, missing proof, or interrupted namespace boundary takes the
existing conservative validation path. Never infer ownership from a leftover
temporary name. Do not increase the number of persistent state machines merely
to cache a digest.

**Acceptance budget:** a fresh cross-filesystem publication should read the
source payload once, write it once, and verify the destination once before
the owning commit. Permit one additional destination scan for the controlled
post-commit staging cleanup/ctime boundary. Count both scans in the measured
budget. Protocol/PAR checks and later independent consumer work are separate.
Any other additional pass needs a specific, tested justification here.

For nonselected relocation, avoid hashing a source tree before discovering an
already-known cross-filesystem boundary. Select destination-local staging
when captured mount/device evidence establishes that boundary; this is a
choice of safe copy path, never permission to replace a target. Selected and
Delivery publication already skip the direct attempt. For an uncertain
legacy/native-rename attempt that still reaches `EXDEV` after capture scans,
count and document those preliminary reads as an explicit exception rather
than claiming a source-once result.

### 3.3 Same-filesystem extraction should publish its existing inode

[`commit_staging`](../crates/nzbd-post/src/manager.rs) calls
[`copy_publish_checked`](../crates/nzbd-state/src/fileops.rs) even for extraction
output already staged on the job filesystem. Prefer an exclusive hard link or
rename appropriate to the workspace's retention contract. Keep the source if
its journal requires retention. This avoids a full second write of extracted
media. Retain the checked copy path for a cross-device result, unsupported
linking, or a workspace that requires independently writable output.
Existing-target copy replay must prove the operation's inode or verify content;
different pre-existing bytes remain a conflict. Existing verified equality
reuse in `copy_publish_checked` remains supported; it does not grant rename
adoption or source-deletion authority. A generic copy retry is a different
contract from resuming the two names of one journal-owned rename.

Hard linking changes isolation: retained workspace and published names share
one inode. Require the extractor and its children to have exited, no future
workspace writer, and a journal that permits shared immutable bytes. Test
that workspace cleanup unlinks only its owned name, preserves the published
file, and correctly refreshes the controlled ctime transition. If an output
must remain independently writable, retain a separate copy on that path.

### 3.4 One cleanup eligibility decision governs every entry point

Keep successful publication terminal even when cleanup is blocked. Store
cleanup disposition and scheduling separately from relocation success. Both
the Delivery entry point and relocation sweeper must consult the same durable
eligibility decision before walking payload directories.

Use a typed internal result distinguishing stable policy/review blocks,
transient I/O or active-custody contention, and success. Do not infer this from
error-message prefixes: existing Keep/hold errors do not begin `blocked:` and
are incorrectly scheduled as transient retries by the Delivery entry point.
The common eligibility/claim function must also govern
`retire_completed_relocations` and the cleanup immediately following a move.

| Cleanup outcome | Next action |
|---|---|
| Pending | Eligible once, through a single claimed cleanup attempt. |
| Transient I/O error | Retry after 30, 60, 120, then 300 seconds; persist the deadline and honor it across restart. |
| Retained files, Keep, or review hold | Blocked; log the state transition once and stop repeated payload walks. |
| Policy/assessment change or explicit retry/rescan | Re-evaluate the exact generation; a changed pathname alone never grants deletion authority. |
| Success | Terminal; all entry points become no-ops. |

For non-Delivery relocations, persist equivalent eligibility in the existing
relocation journal; do not assume every move has a Delivery row. Old journals
receive at most one classification attempt before a stable block is recorded.
Recover abandoned non-Delivery cleanup claims at startup, after the inventory
lock excludes the previous process, just as interrupted Delivery workers are
recovered today. Filter blocked/not-due obligations before applying the batch
limit and select due work in deterministic order. Test more than 25 blocked
items followed by eligible work. Do not mark retained data as deleted to quiet
warnings.

Wire exact-generation re-evaluation into supported changes in
[`set_keep_on_failure`, `set_failure_assessment`, and `release_review`](../crates/nzbd-state/src/artifacts/mod.rs),
plus explicit cleanup retry/rescan. Preserve Keep-off's existing distinction
between ordinary blocked work and review/identity conflicts: a policy change
cannot clear a review requirement or create new deletion authority. A new
assessment may trigger eligibility evaluation but cannot rewrite immutable
Delivery file outcomes into permission to delete retained recovery material.
If fresh per-file authorization is required, keep the obligation blocked for
the existing explicit recovery/removal flow. No automatic full walk is needed
merely to learn that the blocking policy revision is unchanged.

### 3.5 Final history must include publication work

Close Move immediately after the attempted publication finishes, including
its error exit; do not time unrelated finalization or source cleanup as Move.
Finish the actual final stage after selected publication, before capturing
the terminal timeline or freezing timing into a Delivery. Merge that timeline
into the terminal job without
overwriting newer control state or losing the outcome/Delivery parameters
accumulated in `fin`. Do not reintroduce an unsafe whole-job overwrite.

Cover ordinary local completion, selected salvage on failure, remote worker
completion, crash-after-publication replay, and publication errors. Persist one
Move span per executed attempt; a replay that only reuses an already-committed
publication must not invent a second copy duration. Time errors/cancellation
as the attempt actually ran. The remote worker import, Delivery timing, stored
history, and emitted completion must refer to the same closed final timeline.

## 4. Scope and decisions

1. **Keep native filesystem operations.** Implement the copy/rename correction
   in the existing Rust boundary, because an external rsync process would not
   remove the surrounding scans or own Runner's custody journal. Benchmark
   rsync as a comparison, not as an assumed speedup.
2. **Keep readback at the required proof boundaries.** Verify destination bytes
   before the owning commit and retain the ctime-bound cleanup check where
   needed. Hashing only write buffers would provide weaker evidence.
3. **Keep replay conservative.** A restarted process cannot trust an in-memory
   observation from its predecessor. Hash when proof is absent or ambiguous.
4. **Keep configuration semantics.** Do not move processing back into completed
   storage, migrate live jobs, relax completion visibility, or bypass the
   generation-bound deletion contract as a performance workaround.
5. **Bound the broader audit.** Rank further findings separately. Fold in
   same-filesystem extraction publication because it shares this boundary;
   unrelated engine, history, or cluster changes need their own scoped fixes.

## 5. Qualification — prove fewer reads and unchanged failure behavior

### 5.1 Deterministic regression and work-budget checks

Use test-only read/copy counters or injected readers at the operation boundary.
Wall-clock tests alone cannot distinguish a fixed algorithm from warm-cache
luck. Count actual passes, including fallbacks and cleanup.

| Case | Required result |
|---|---|
| Native same-filesystem rename | Exclusive namespace result; no new payload scan in the primitive. |
| Unsupported flagged rename | Same-inode link/unlink; no overwrite; owner's required final content proof retained. |
| PAR filename restoration | Catalog MD5 and journal SHA share one read; at most one final transition verification. |
| Fresh cross-device publication | One source read, one destination write, one pre-commit destination readback; at most one further scan for the post-commit staging cleanup/ctime boundary. |
| Eligible immutable same-filesystem extraction commit | No duplicate payload write; source retention follows its journal. Independently writable outputs keep the separate-copy exception in §3.3. |
| Different-inode existing target | Never overwrite. Rename replay cannot adopt it or authorize source deletion based on equal bytes; existing non-destructive copy replay may retain its verified-equality behavior. |
| Same-size source mutation with restored mtime | Refuse stale content proof, including a mutation interleaved with link-induced ctime change. |
| Both-name restore replay and retained-original move | Accept the journaled same-inode ctime transition only with stable digest proof; verify the final object before success. |
| Destination corruption or replacement | Refuse completion and source retirement. |
| Crash before/after link, unlink, flush, verification, and commit | Resume only the owned operation; retain ambiguous files; never duplicate completion or delete an unproven source. |
| Cancel/delete/lease loss during I/O | Check authority, await workers, and preserve the current custody contract. |
| Legacy/missing proof or changed mount | Conservative validation; no global equality or identity weakening. |
| Repeated blocked cleanup sweeps | One state-transition warning; zero repeated payload walks until eligible state changes. |
| Transient cleanup error and restart | Deadline respected by all entry points; later due items progress. |
| Cleanup policy wakeup and interrupted non-Delivery claim | Re-evaluate only the exact generation; preserve review and immutable retention decisions; recover the dead worker's claim after restart. |
| Local, salvage, remote, and replay timing | Final history includes Move exactly when executed, with measured duration and correct attempt identity. |

Run the affected suites and static checks using the workflow's existing build
slot and compiler cache. Resolve failures before broadening the run.

```bash
cargo test -p nzbd-state                         # Filesystem and journal regressions
cargo test -p nzbd-post                          # PAR, extraction, and pipeline behavior
cargo test -p nzbd-engine -p nzbd-cluster         # Affected owners and remote completion
cargo fmt --all -- --check                       # Formatting gate
cargo clippy --workspace --all-targets -- -D warnings # Cross-crate interface checks
```

Exact filters may narrow unaffected engine/cluster suites after the changed
callers are known. Required repository CI remains the merge gate. The commands
above are proposed checks, not results already obtained.

### 5.2 Controlled measurements and deployment

Use generated, task-owned files outside active download/import trees. Compare
the original and candidate release builds on identical files and filesystem
pairs. Include a file larger than the available cache when feasible; otherwise
label the run cache-resident. Record payload size, application bytes read and
written, copy/verify/publication durations, CPU time, mount types/options, and
whether the run is warm or cold. Never drop host caches or load-test live
payloads to improve a benchmark result.

Deployment follows passing tests and required CI, through the existing media
Ansible workflow described in [deployment](DEPLOY.md), scoped to Runner on
nuc3. Preserve the previous image/source revision and active work before
restart. Stamp and verify the build identity; `+unknown` cannot establish
which binary was qualified. Check the current playbook's variables rather
than assuming it forwards Runner's build argument.

After deployment, observe one normal completion and one existing blocked
cleanup obligation. Confirm the correct final path/content, complete Move
timing, stable blocked state, and absence of repeated full scans on unchanged
objects. Do not retry or delete retained user payload merely to exercise this
test. Roll back the binary on corruption, lost completion, custody regression,
or uncontrolled I/O; preserve new journals and payloads for diagnosis. Confirm
old-reader compatibility of additive journal fields before rollout.

## 6. Broader audit — additional costs and their scope

An independent agent audited the remaining paths against `ea8a402b`. The
following are source-confirmed algorithmic findings, not live timing
measurements. The primary investigation did not establish their share of
nuc3's wall-clock slowdown.

| Priority / path | Finding | Disposition and validation |
|---|---|---|
| P1 · [extraction commit](../crates/nzbd-post/src/manager.rs) | `commit_staging` always uses the three-read copy helper, adding an unnecessary complete write when workspace and payload are on the same filesystem. | Included in §3.3. Count physical copy calls and payload reads; retain collision/replay and cross-device tests. |
| P1 · [PAR output publication](../crates/nzbd-post/src/repair_workspace.rs) | Its per-chunk copy checkpoint calls `fingerprint::stamp`, which reopens/stats the full output path. Three 64 KiB passes over 89 GB trigger about 4.1 million complete path walks, with path depth multiplying descriptor opens. | Include in the shared copy-boundary correction: cancellation/authority checks per chunk, descriptor checks around the operation, anchored path revalidation before publication. Test metadata work stays bounded per file, including source replacement/mutation. |
| P1 · [writer finalization](../crates/nzbd-engine/src/writer.rs) and [range validation](../crates/nzbd-engine/src/recovery.rs) | Async finalization calls blocking full-file CRC validation directly on a Tokio worker. `SealPartial` similarly scans ranges synchronously. | Separate scoped fix after the publication work: tracked blocking worker, cooperative cancellation, and join before custody release. A single-thread runtime regression must prove timers/controls progress during a gated scan. Keep CRC verification. |
| P2 · [PAR preparation/publication](../crates/nzbd-post/src/repair_workspace.rs) | SHA copy verification is followed by additional MD5 reads; validated repaired output is copied and hashed again by the workspace journal. | Carry combined MD5/SHA evidence through the shared boundary where exact identity permits. Keep independent writable repair inputs: hard-linking them would let the PAR tool damage originals. Count preparation/publication bytes separately. |
| P2 · [torrent acquisition](../crates/nzbd-state/src/artifacts/torrent.rs) | A fresh private copy is fully compared to the donor before rename and again afterward, excluding independent torrent piece verification. | Separate follow-up using the reviewed copy proof. Preserve seed isolation and full replay verification; assert one fresh final readback. |
| P2 · [writer extent admission](../crates/nzbd-engine/src/writer.rs) | Each segment scans the accumulated range vector twice. At 20,000 unique segments this is roughly 400 million comparisons; out-of-order large direct media is especially exposed. | Separate follow-up: segment-number lookup plus ordered extent index. Compare against the current acceptance rules with randomized order/overlap tests and work counters. |

The audit also identified per-segment `sync_data` as expensive but necessary
for the present durable-acknowledgement contract. Do not remove it. A later
group-commit design would have to delay acknowledgements until a batch is
durable. Existing identity-bound PAR discovery caching and combined MD5/slice
CRC fingerprinting are useful optimizations to preserve.

## 7. Execution and independent review

| Milestone | State | Acceptance |
|---|---|---|
| Root cause and plan | Drafted | Source-linked evidence, explicit proof boundaries, and tests defined. |
| Astra design review | Complete; findings addressed | Seven corrections recorded below; follow-up review found no further material design blockers after the diagram/extraction qualifications recorded below. |
| Broader I/O audit | Complete | Six findings source-checked; scope and validation recorded in §6. |
| Rename/copy/publication correction | Unit regressions green | §5.1 integrity and read-budget checks pass. |
| Cleanup scheduling and Move timing | Unit regressions green | Blocked/due selection and all completion paths pass regressions. |
| Integration checks and controlled measurements | Required CI pending; live measurements pending | Results and remaining limits recorded; required CI green. |
| Deployment and observation | Not started | Qualified build identified on nuc3 and §5.2 checks recorded. |

### 7.1 Astra findings and dispositions

Astra reviewed the source and draft on 2026-11-03. The root-cause analysis was
confirmed; the following design corrections were required before building.

| Finding | Change to the proposal |
|---|---|
| P1 · Early staging retirement changes the custody protocol | §3.2 keeps retirement after owner commit and explicitly permits the additional digest-bound ctime refresh. No unverified publication phase is introduced. |
| P1 · Removing primitive checks affects restore replay and retained originals | §3.1 requires owner final verification for every affected branch, fixes legitimate both-name ctime replay, and binds combined digest capture to the same descriptor identity. |
| P1 · Cleanup needs typed reasons, wakeups, and claim recovery | §3.4 defines typed outcomes, all entry points, exact-generation policy/review wakeups, immutable outcome limits, interrupted non-Delivery claim recovery, and due filtering before `LIMIT`. |
| P1 · Delivery and remote timing are stale too | §3.5 closes Move at the publication boundary before every frozen sink and preserves attempt/control authority. |
| P2 · Equal-content copy reuse differs from rename adoption | §3.3 and §5.1 preserve verified-equality copy replay without granting same-inode rename or deletion authority. |
| P2 · Extraction hard links change retention and mutation isolation | §3.3 makes the optimization conditional on immutable workspace contracts and requires mutation, cleanup/ctime, and crash tests; otherwise retain an independent copy. |
| P2 · Direct nonselected publication may hash before `EXDEV` | §3.2 chooses destination-local staging for known cross-filesystem boundaries and records uncertain direct-attempt read exceptions explicitly. |

Astra's follow-up confirmed all seven substantive findings were addressed.
Its two final qualifications are incorporated: §3.2 now branches completion
and cleanup independently after verified custody commit, and §5.1 limits the
zero-copy extraction requirement to eligible immutable paths. With those
corrections Astra judged the plan ready to implement.

The implementation candidate carries the copy source digest into final
publication, makes journal-owned rename primitives metadata-only with owner
verification, combines PAR MD5/SHA capture for filename restoration, schedules
cleanup through durable claims, and closes Move before terminal snapshots.
Read-budget, corruption, restore replay, cleanup deadline/restart/fairness,
and final-history regressions are included. Compilation and focused Clippy
checks preceded review. Astra cleared the corrected implementation at
`78bcce3` before the first unit run, as requested.

**Extraction isolation exception:** current extraction workspaces retain
independent output while the job may perform subsequent naming/repair work.
The candidate keeps those copies rather than creating shared writable inodes
without a complete workspace/ctime contract. The shared copy helper now hashes
while copying and performs one final readback, reducing its three payload
reads to two. Automatic hard-link extraction publication is deferred under
§3.3's independent-output exception; this candidate does not claim zero writes
for that path. The broader writer/torrent follow-ups in §6 remain separate.

Controlled measurements, required CI, merge, and deployment remain pending.
Documentation validation and static checks are not unit-test results.

### 7.2 Implementation PR review before unit tests

Draft [PR #270](https://github.com/pjunod/runner/pull/270) was reviewed by Astra
at `ccb2194` before any unit tests ran. The review requested changes; the
following corrections and regressions are included in the next candidate.

| Finding | Correction and regression |
|---|---|
| P1 · Cleanup filtering broke the pending-move SQL query | Restore the original pending query; keep due/readiness predicates in the separate succeeded-cleanup query. Reconciliation/recovery tests exercise preparation. |
| P1 · Recovery copies dropped the streamed source digest | Pass copy evidence through both recovery branches and validate supplied proof against existing publication journals. Corrupt copies during both initial-scratch and resumed-scratch recovery and require refusal. |
| P1 · Earlier files could change during a later cleanup hash | Rebind the root and recheck the complete manifest before refreshing identities or permitting source retirement. Mutate file A while file B is being verified and require both originals to remain. |
| P1 · Verified descriptors were not rebound to final names | Reopen full anchored source/destination paths at the proof boundary, including rename owners. Replace a destination ancestor before readback and require refusal even though the old descriptor still contains valid bytes. |
| P1 · Transient delete journals became permanent blocks | Preserve typed retry/in-flight state and the deletion journal's deadline; report active deletion custody as contention. Inject deletion I/O failure and require a scheduled retry. |
| P2 · Policy wakeups could be lost during claim settlement | Persist a per-operation wake revision outside mutable relocation checkpoints, compare it before inner settlement, and reconcile Delivery settlement under the same policy guard. Explicitly retry during inner settlement and between both ledger settlements; require a due retry rather than a permanent block. |
| P2 · Deferred moves without handoff authority could starve due work | Require Delivery or activated cluster acceptance before batch selection. Place 25 unready moves before eligible work and require that work to complete. |

The wake revision is a counter in the existing metadata store, not a new
payload state machine or a digest cache. Eligibility changes still grant no
new byte authority. Static compilation/Clippy checks passed before Astra cleared the corrections
for unit testing. The first run passed the new corruption, recovery, and wake
regressions but exposed successful-cleanup and custody-release regressions;
these are being corrected and reviewed before rerunning tests.


### 7.3 First unit run and correction scope

The first reviewed run reported 279 state tests passed, seven failed, and two
ignored; post-processing reported 101 passed and 18 failed. Merge is held.
The state failures exposed three missing compatibility cases: refreshing the
published identity after Delivery transfers ownership, preserving the retained
cluster outcome with the new typed cleanup block, and waking exact-generation
custody retries in both cleanup ledgers when workers release the source.
Keep-off must also wake the same cleanup schedule. Added coverage exercises
Delivery-backed custody release, release between ledger settlements, and
preservation of genuine I/O backoff. Restart tests explicitly expire injected
I/O retry deadlines instead of assuming restart cancels durable backoff.

All 18 post-processing failures were reproduced unchanged at base `ea8a402b`
using an isolated source export and the same build slot/tools. Fixture repairs
create download roots before direct inventory allocation, feed delayed PAR
records through owner-issued processing custody, and update stale diagnostic
expectations. These test repairs do not broaden production import authority.
Final green results, required CI, and merge will be recorded after validation.


### 7.4 Corrected unit results

Astra reviewed the cleanup corrections and the explicit RetryNow mode before
unit reruns. State is green: **287 passed, two existing ignored**. Post is green:
**119 passed** with required PAR2/7z tools enabled. The latter includes one
closed Move span for each executed publication attempt, including a failed
attempt followed by a successful retry.

The older failed-source RetryNow path also had a real admission bug: replacing
the whole queue record to clear its deadline started asynchronous verification,
then immediately attempted processing while that verification blocked admission.
RetryNow now requests an explicit forced attempt through the existing admission
wrapper. Only the automatic deadline and debounce are bypassed. Existing gate,
processing custody, integrity checks, and durable outcome writes still apply;
denied admission reports NotReady and preserves the stored deadline. Regressions
cover that refusal, ordinary deadline preservation, an offline-source attempt,
and successful recovery. The retry calculation has a pure test for the
30-minute warning, first-block preservation, and attempt increment; normalized
existing-deadline controls retain their idempotence test.

Local read-budget results are deterministic byte counters, not storage-throughput
benchmarks. Fresh generic copies read the source once while copying and the
published destination once; journal-owned primitive renames read no payload.
A linked publication cleanup that changes ctime retains the additional verified
refresh described in §3.2. No live nuc3 performance claim is made by these tests.
Required CI and merge remain pending; production deployment is not yet performed.


### 7.5 Broader integration qualification limit

The additional `failure_regressions` integration target passed all six tests.
The full legacy `pp_pipeline` target is not green on base `ea8a402b`: 34 of 38
tests fail there, primarily on asynchronous import verification and processing
admission assumptions. The candidate reports three passed and 35 failed. Its
additional final-stamp failure is also reproduced on unchanged base code:
three isolated repeats pass twice and fail once, confirming an existing race
between history visibility, fault injection, and admission release. Baseline receipts are retained at
`/private/tmp/runner-publication-baseline-integration.log` and
`/private/tmp/runner-publication-baseline-final-stamp-{0,1,2}.log`. Candidate and base
were tested with the same required external tools and isolated build slot.

An experimental fixture-only correction resolved many of those failures but
exposed additional existing manager and receipt assumptions. That broader test
repair is excluded from this publication change; its source is retained at
`/private/tmp/runner-publication-integration-fixture-experiment.rs`. The explicit
RetryNow wrapper's additional gate checks are limited to forced requests so
normal automatic processing retains its original gate order. Astra cleared
that final correction before the final post-processing rerun.

These baseline failures do not establish a new publication integrity regression,
but they prevent claiming full integration qualification or deploying this
candidate. The final affected post unit rerun passes all 119 tests; the six targeted
failure regressions also pass. The requested merge still requires the
repository's required CI checks to pass. Live nuc3 measurements and deployment
remain separate pending work.
