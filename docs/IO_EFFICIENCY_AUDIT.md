# Runner I/O audit — redundant work beyond completed-folder publication

**Status:** reviewed source findings; follow-ups are not implemented · **Written:**
2026-11-04 UTC · **Baseline:** `ea8a402b` · **Rechecked:** `2c13010` and the
integration recovery candidate. Final qualification and rollout evidence is tracked
in [PR #272](https://github.com/pjunod/runner/pull/272).

Companion to [PUBLICATION_PERFORMANCE_PLAN.md](PUBLICATION_PERFORMANCE_PLAN.md)
(the completed-folder root cause and implementation contract). This document
preserves the independent audit of adjacent code. Use the fixed-work table to
avoid rediscovering work already done, then choose follow-ups by measured cost.
Do not treat estimated I/O amplification as a measured wall-clock benchmark.

## What this audit establishes

The code had several layers independently proving the same unchanged bytes.
Replacing a copy command with rsync would leave most of that surrounding work
in place. Same-filesystem namespace changes should retain exact inode/root and
exclusive-publication checks without rereading payload content. A new physical
copy, mutable repair workspace, foreign journal or resumed staging inode still
needs evidence before Runner advertises verified completion or deletes its
source. Independent copies used for mutable repair or torrent seeding cannot
be replaced indiscriminately by hard links.

The findings below come from source inspection and the existing regression
suite, not a production filesystem benchmark. For byte estimates, `S` means the
payload bytes handled by one successful fresh operation. Reads may hit cache;
remote metadata round trips and runtime starvation can dominate even when
physical disk traffic is lower.

## Fixed or already being qualified on this branch

| Work | Evidence and resulting behavior | Qualification status |
|---|---|---|
| Generic exclusive rename hashed unchanged regular files repeatedly | [artifacts/fs.rs](../crates/nzbd-state/src/artifacts/fs.rs): namespace publication uses anchored identity/exclusivity rather than duplicate payload digest passes. | Implemented; root owns final workspace qualification/deployment. |
| Cross-filesystem relocation and selected publication repeated full scans | [relocation.rs](../crates/nzbd-state/src/artifacts/relocation.rs) and [publication.rs](../crates/nzbd-state/src/artifacts/publication.rs): copy evidence is reused within its exact owned operation; restart/collision evidence remains conservative. | Implemented; see publication plan for guarantees and targeted tests. |
| `copy_publish_checked` prehashed the source to name its temporary file and reread it after copying | [fileops.rs:129](../crates/nzbd-state/src/fileops.rs#L129): fresh temporary name uses exact source identity, SHA-256 is computed during copying, final destination is independently read back once. Fresh path is now approximately `2S` reads plus `S` writes, rather than the earlier extra full source passes. | Implemented; replay/collision paths deliberately revalidate existing bytes. |
| PAR publication called filesystem fingerprint checks per 64 KiB chunk | [repair_workspace.rs:446](../crates/nzbd-post/src/repair_workspace.rs#L446): source stamp is checked before and after the copy; per-chunk checkpoint is cancellation/authority only. | Implemented. The original pattern could cause millions of anchored path walks for an 89 GB file. |
| Blocked source cleanup was retried without respecting its existing backoff/review state | Artifact cleanup scheduling and typed failed-cleanup replay now retain exact deletion intent, Keep and generation checks. Cleanup-only metadata never grants verified payload export, repair or publication authority. | Implemented; pending final combined qualification. |
| Move phases lacked useful timings | Post-processing records move wait/work timing; copy/rename work can be distinguished from scheduling delays. | Implemented; root owns final observation after deploy. |
| Lease expiry/cancellation can block unrelated heartbeat renewals while waiting for writer/post joins | [worker.rs](../crates/nzbd-cluster/src/worker.rs) and [EngineHandle::retire_worker_job](../crates/nzbd-engine/src/lib.rs): renewal is separated from tracked join/retirement work. Exact revoked-grant retirement preserves foreign resume journals; accepted completion remains a separate compaction decision. Durable stop receipts replace execution watches only after actual joins. | Implemented; final combined qualification and runtime evidence tracked in PR #272. |
| Ready Post results wait behind download completions | [worker.rs](../crates/nzbd-cluster/src/worker.rs): independent fresh/replay lane turns and within-kind rotation preserve progress for both classes, immutable requests and the single copy/verification lane. Outbox header work is bounded and full decoding is limited to the selected request. | Implemented candidate; final qualification tracked in PR #272. |
| Heartbeats repeatedly persist unchanged provider-budget state | [leader.rs](../crates/nzbd-cluster/src/leader.rs) advances semantic revisions and skips confirmed durable writes; [control.rs](../crates/nzbd-cluster/src/control.rs) requires exact current issuer authority and checks duplicate-write postconditions. Uncommitted, stale or superseded commands are withheld. | Implemented candidate; final qualification tracked in PR #272. |

## Ranked follow-ups

| Rank | Trigger | Remaining amplification or contention | Smallest useful fix |
|---|---|---|---|
| 1 | An accepted completion restores the authority queue | Projection replacement discards allocation/cache and verification tickets for all payload jobs; source reconciliation then waits for the entire verified queue. Unchanged jobs repeat recovery work after unrelated completions. | Design job-level readiness or conservative proof reuse with fresh exact source identity/generation checks. Every source fact must still use checked export and guarded intent CAS; metadata alone cannot grant payload authority. |
| 2 | Large extracted output committed from staging on the same filesystem | Extraction still copies every file through `copy_publish_checked`: approximately `2S` reads and `S` writes after this branch's generic helper improvement. Staging remains independently writable and retained through later naming/repair, so current copy semantics are required. | Investigate an exclusive consume boundary before adding a metadata move/link path. Prove no later independent writes or staging-retention requirement, and preserve collision/crash replay checks; otherwise retain the verified copy. |
| 3 | Writer finalizes a large file with a combined CRC | A synchronous full-file CRC scan runs inside an async writer future, occupying a Tokio runtime worker for the scan duration. | Move the necessary CRC validation to an owned bounded blocking/payload lane; keep actual worker completion and descriptor lifetime tracked. |
| 4 | PAR workspace prepares matched data or recovery companions | Cached PAR fingerprints are followed by SHA-256 copy/readback and then independent MD5 scans of the copied data; companion preparation also reads the source again for MD5. | Compute required MD5 alongside the existing copy/readback stream and return digest evidence bound to exact identities, instead of rescanning unchanged input/output. |
| 5 | Fresh torrent donor copy and publication | Copy is independently checked against donor before and after namespace publication; the fresh path can read donor about `3S` and destination about `2S`, excluding optional torrent piece verification. | Keep the independent donor copy, but carry the verified staged digest and anchored identity across the metadata-only publication boundary; independently revalidate on actual replay/mutation. |
| 6 | Very large number of segments in one file | Writer checks each new extent against a vector with both `contains` and an overlap scan. Work grows quadratically with range count. | Index segment identity separately and use an ordered interval structure for overlap checks, preserving duplicate acknowledgment and conflict behavior. |
| 7 | Many immutable generation files streamed between cluster workers | `copy_generation_tree` performs `std::fs::copy` and then a complete destination SHA-256 pass before syncing. | Fuse copy and manifest hashing while retaining a separate destination readback only where the custody contract requires it. Benchmark first: some second reads are intentional integrity evidence. |

### Authority restoration repeats queue-wide payload recovery

**Evidence:** [projection replacement](../crates/nzbd-engine/src/owner_transitions.rs)
invalidates epochs, allocation cache and verification tickets for payload rows.
[Source reconciliation](../crates/nzbd-cluster/src/source_facts.rs) requires a
checked export of the complete queue before scheduler admission. Consequently,
an unrelated pending verification can prevent already-verified work from being
scheduled. Accepted completions repeatedly restore that projection.

The local fairness trace at `2c13010` also showed a ready Post completion waiting
about 16.6 seconds behind seven download completions. Completion-lane ordering
can remove that starvation while retaining bounded download progress and one
copy/verification operation at a time. Queue-wide recovery amplification is a
separate follow-up; it is not a measured production filesystem benchmark.

**Integrity boundary:** preserve earned local verification only when exact
source binding, file/range/finalization state, generation and fresh anchored
identity still agree. A business-intent comparison alone is insufficient.
Alternatively reconcile checked job exports individually, skip only explicit
busy rows, and carry binding/readiness into grant eligibility so a skipped
unbound row cannot acquire authority later in the same scheduling pass. Retain
all existing guarded CAS and ownership checks.

**Validation:** count verification work and control round trips after one job
completes in a large settled queue. Block recovery for an unrelated row and
prove eligible work progresses without allowing that blocked row to start.
Exercise changed bytes/inodes/generations, operator revision changes, failed
persistence, cancellation and authority replacement before any proof reuse.

### Extraction staging still copies same-filesystem files

**Evidence:**
[commit_staging](../crates/nzbd-post/src/manager.rs#L6555)
recurses through extracted staging files and always calls
`fileops::copy_publish_checked`. Its comment mentions hard-link publication,
but that helper first writes a separate temporary copy and only links the
verified temporary inode to its final name. It is not a direct link from the
original staging file.

**Integrity boundary:** extraction staging remains independently writable and
retained through subsequent naming/repair work. Ownership by the same attempt
does not make hard links safe: later writes would affect both paths. Keep the
current independent copy until an exclusive consume boundary proves there are
no later independent writes or staging-retention obligations. A fast path must
also reject symlinks, changed source identity, unknown existing targets and
conflicting replay bytes; preserve source bytes until durable publication
records exist.
Directory-wide rename may be inapplicable when outputs merge into a nonempty
working directory, but per-file exclusive link/rename can still avoid copying.

**Validation:** instrument payload bytes read/written for a large fresh
same-filesystem extraction; test crash/retry between target creation and stage
retirement, an existing different target, source replacement, and cross-device
fallback. Successful same-filesystem publication should add no full payload
copy; collision/replay remains conservative.

### Writer completion performs blocking CRC reads on a runtime worker

**Evidence:**
[writer::finalize](../crates/nzbd-engine/src/writer.rs#L604)
is async but calls synchronous `require_valid`, which calls
`recovery::validate_range` over the whole file when `combined_crc` exists.
The same synchronous path can run during idempotent publication recovery.

**Integrity boundary:** the CRC check detects incorrect/truncated durable
bytes; removing it merely because writes succeeded would weaken completion.
The concrete problem is execution placement and potentially reusable evidence,
not distrust of rename itself.

**Validation:** hold the validation worker behind a real test barrier and
verify unrelated actor requests/timers continue, cancellation retains the
partial/source, and completion is emitted only after actual CRC/readback and
publication acknowledgment. Measure scan bytes and runtime scheduling delay.

### PAR preparation rereads bytes for separate digest algorithms

**Evidence:**
[RepairSession preparation](../crates/nzbd-post/src/repair_workspace.rs#L272)
has a cached full MD5 fingerprint, copies through the SHA-256 helper, then
calls `full_md5` on the prepared target. Recovery companion preparation around
line 332 hashes both target and source again after copying. Verified repaired
output scanning around line 386 adds a different evidence boundary.

**Integrity boundary:** mutable repair input must remain independent from the
original retained source. Preserve those copies. The safe reduction is to
compute multiple required digests while already reading and bind them to the
same file identity; do not reuse pre-repair digests after a tool mutates bytes.

**Validation:** count bytes scanned in matching, preparation and publication
separately. Exercise unchanged rounds, a same-length mutation, repair output
mutation and replay after a copied stage survives process restart. Existing
unchanged-round fingerprint tests are useful anchors but not sufficient for a
new digest-evidence API.

### Torrent copy validates an unchanged publication twice

**Evidence:**
[resume_torrent_copy](../crates/nzbd-state/src/artifacts/torrent.rs#L1055)
validates resumable prefixes, copies donor bytes into an independent stage,
calls `verified_prefix` on donor and stage, publishes the stage, and calls
`verified_prefix` on donor and published destination again. Optional piece
verification is an additional necessary boundary tied to torrent metadata.

**Integrity boundary:** a seeding donor must remain an independent inode;
hard-linking the acquisition would let later repair/publication changes affect
the seed. Keep donor identity, operation generation, destination collision and
restart checks. A successful metadata publication of the already verified
stage should not require re-proving identical bytes solely because ctime
changed during rename/link.

**Validation:** fresh-copy byte counters, independent-inode assertions,
consumer mutation after publication, donor replacement, interrupted partial
copy, interrupted publication and resumption with an existing journaled stage.
Do not collapse checks across independent retries without a durable receipt.

### Segment range conflict checks scale quadratically

**Evidence:**
[writer.rs:380](../crates/nzbd-engine/src/writer.rs#L380)
uses `ranges.contains(&range)` and then scans all prior ranges for segment-ID
or byte-overlap conflicts. Across `N` nonduplicate segments this is roughly
`N(N-1)/2` prior-range comparisons per linear scan. For 100,000 ranges, one
such scan series is about five billion comparisons.

**Integrity boundary:** duplicate writes with identical extent/CRC must retain
the existing acknowledgment semantics; same segment with different extent or
CRC and overlapping different segments must still fail closed. A map plus
ordered interval lookup changes lookup cost, not that policy.

**Validation:** preserve existing overlap/duplicate tests and add a large
out-of-order workload with an operation/comparison budget rather than a flaky
wall-clock assertion. Test adjacent intervals, overflow and a conflict late in
the sequence.

## Measurements to collect before expanding the implementation

Record bytes copied, source bytes hashed, destination bytes read back, metadata
checkpoint counts, phase wait/work time and cancellation-to-join duration.
These tell you whether a slow job is reading payload again, waiting on a remote
filesystem, or holding up unrelated scheduling. A warm-cache microbenchmark
alone cannot establish NFS performance.

The source audit did not establish a separate actionable SQLite N+1 or
quadratic-manifest defect outside the listed cases. Do not report those as
confirmed findings. Background cleanup and lease retirement issues found
during qualification are real lifecycle/scheduling defects and are tracked
separately from the remaining payload-pass follow-ups.

## Guardrails

- Do not add rsync as a new publication owner. It does not replace Runner's
  generation, collision, replay and source-retirement journal contracts.
- Do not remove readback or cross-node range verification without replacing
  its evidence. A successful syscall alone does not prove previously corrupt
  or unrelated bytes safe to publish.
- Do not use hard links for independently mutable repair inputs or torrent
  seed donors. A namespace optimization must not merge their mutation domains.
- Do not expand the current retirement fix to implement these follow-ups.
  Finish serialized qualification and deployment first, then profile the
  highest-cost remaining path with explicit byte/metadata counters.
