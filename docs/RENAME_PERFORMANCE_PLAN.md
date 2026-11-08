# Rename performance — reuse producer reads and keep verified commits

**Status:** implementation candidate; plan approved by Astra · **Written:** 2026-11-04
(America/New_York) · **Base:** `01750d85` · **Branch:** `codex/rename-performance`

Companion to [publication performance](archive/PUBLICATION_PERFORMANCE_PLAN.md) and
[the I/O audit](IO_EFFICIENCY_AUDIT.md). Follow these milestones in order.
Changes to the final verification or crash-recovery contract require another
design review before implementation.

## 1. Evidence and objective

Live job 2319 spent 774,762 ms in PAR naming, then 625 ms in PAR verification.
Its 134 successful `par_restore` journals cover 69,675,407,366 bytes. Each
actual restore reads source MD5/SHA-256, renames, then reads destination SHA.
That implies 139,350,814,732 logical payload read bytes, excluding metadata;
physical traffic and syscall-only time were not measured.

Read-only inspection confirms `fuse.glusterfs`, source `localhost:/processing`,
at `/mnt/processing`, and a container descriptor limit of 1,024. A native-only
metadata cache would miss this incident.

The preserved pre-deployment queue record has 144 files: 10 already marked
`is_par2` and 134 data files. Thus the producer eligibility hint is present
before naming for the measured job, including paused recovery volumes.

Collect naming digests during the writer's existing full CRC read. On an
eligible fresh file, naming then performs one destination read instead of
another source read plus destination read. Required reads across writer and
naming decrease from approximately `3S` to `2S`; naming decreases from `2S`
to `S`. Measure byte savings; do not promise an unmeasured wall-time gain.

## 2. Contract — expected content is not current-content authority

### 2.1 Preserve writer ordering and distinct identities

Re-verify these existing interfaces at implementation time:

- [Writer finalization](../crates/nzbd-engine/src/writer.rs) checks full CRC
  before link(final)/unlink(part), then captures a new identity.
- [Scoped writer acknowledgement](../crates/nzbd-engine/src/owner.rs)
  fences by generation and persists finalization before success.
- [Restore commit](../crates/nzbd-state/src/artifacts/transforms.rs)
  verifies full destination SHA and anchored identities before success.
- [Recovery range proofs](../crates/nzbd-engine/src/recovery.rs) tolerate
  namespace changes; they are not strict whole-content cache authority.

Keep CRC-before-publication ordering. In its existing sequential read,
optionally compute full MD5, first-16-KiB MD5 and SHA-256 alongside CRC.
Retain the stable original read identity and descriptor. Record the later
publication locator separately, requiring the same object, mount, length
and mtime through the writer's own namespace operations. Do not pair an old
digest with a new identity and call it a fresh `ContentProof`.

The new opaque evidence describes expected bytes from a CRC-validated
producer stream. It cannot be constructed from JSON, parameters, imported
jobs or worker transport. No complete CRC/coverage means no naming evidence
and no extra scan. Only data files in jobs with known PAR metadata collect
optional digests; other jobs retain CRC-only work. This moves digest CPU
into an existing required read; it does not remove all hashing CPU.

Implementation interfaces are in
[writer_naming](../crates/nzbd-state/src/artifacts/writer_naming.rs):
`verify_producer_read` issues an opaque `ProducerRead`; `published` checks a
separate locator; `remember_writer_naming` admits an owner allocation snapshot
after durable acknowledgement; `take_writer_naming` consumes current file
scopes. `restore_file_expected` retains the existing final SHA/journal
boundary. Installation performs no filesystem or database I/O on the owner;
fresh validation and retired-descriptor closure run on existing blocking lanes.

### 2.2 Exact admission and bounded descriptor lifetime

Install evidence only after the matching scoped finalization is durably
acknowledged. Bind inventory instance, installation, job, artifact/allocation
generation, file ID/writer generation, publication path and retained FD.
Processing still waits for actual writer retirement.

An inventory-owned cache uses a process-wide descriptor budget of at most
`min(256, soft_descriptor_limit / 4)`; unknown limits use a conservative bound.
Eviction/budget or clone failure causes a miss, not download failure. Cache
no payload bytes. Transferred tokens retain their permits until actual drop.

Invalidate on writer replacement, recovery/queue replacement, job removal,
source retirement/relocation and inventory shutdown. The PAR attempt takes
its eligible entries as a batch and drops unused entries on success, error
or cancellation. Prove descriptor closure under eviction and failure.

Consumption checks the current exported file generation and exact source,
root/mount, path, locator and held FD. Reject empty legacy installation,
generation, mount or timestamp fields; `Identity::eq` has an empty-ctime
wildcard and is insufficient. Missing/stale/evicted/replaced evidence uses
the existing source scan. Tokens cannot cross inventory instances or jobs.

Bind reuse to stable allocation/file generations and the leaf descriptor,
not artifact revision or directory ctime. Earlier successful sibling renames
change those latter values. Each restore must capture the current artifact
revision for its own transaction without invalidating the remaining entries.

### 2.3 Destination readback remains the GlusterFS commit boundary

Add a narrowly scoped expected-content restore entry point separate from
fresh-source `ContentProof`. Re-anchor current owned source and tentative
namespace identity, then retain the generation-bound journal and exclusive
rename. Journal the immutable expected SHA from the producer stream.
Only succeed after full destination SHA matches, descriptor/path identities
are stable, and source generation/journal checks still pass. Cached MD5 and
size must match PAR; matching SHA binds those expected bytes to the actual
verified destination. Metadata alone grants no content, export or cleanup
authority, including on GlusterFS/NFS/FUSE.

An intervening write hidden by remote metadata can select a tentative rename
before final SHA rejects it. Preserve bytes, custody and the original digest
journal; never refresh the digest, succeed the restore or publish READY.
Recovery/retry must refuse mismatching content at its actual retained name.
This failure-path namespace behavior differs from a fresh source MD5 mismatch
that leaves the old name untouched; no unchecked rollback is implied.
Keep legacy restore/replay strict and its journal format compatible. Stop
for review if it cannot safely retain/refuse this tentative failure.

### 2.4 No-op handling, cancellation and observable work

Skip a full scan only when every prefix-matching PAR catalog entry already
names the exact existing path. This asserts no content integrity. Any
alternate target retains full matching or validated expected-content work.

Run full producer scans on joined blocking workers with bounded chunks and
cancellation checks; writer-stopped acknowledgement follows actual return.
Readback checks attempt authority between chunks and before commit. Retain
unfinished journals and data on cancellation.

Record source/readback bytes, hits/misses, actual/no-op renames and elapsed
matching/hash, namespace, manifest and journal work. Existing attempt progress
must include destination reads, so hits do not show zero while hashing.
Keep external protocol stage enums stable.

## 3. Decisions and alternatives

1. **Expected producer bytes plus final readback:** removes a redundant pass
   while preserving the independent committed-content check on remote storage.
2. **Keep CRC ordering:** moving it after publication creates a crash window
   where current recovery could promote an unvalidated final name.
3. **No persisted cache:** restart/import conservatively scans; ownership and
   descriptor lifetime stay explicit.
4. **No metadata-only success:** inode identity cannot replace destination SHA.
5. **No guessed-name protocol:** prefix/size without producer commitment would
   require a larger verified rollback design.

Non-goals: weaken CRC/PAR/collision/ownership/replay checks; remove destination
hashing; change queue-wide recovery trust; discard per-file journals/manifests;
change unrelated copy/torrent/extraction APIs; benchmark by mutating live data.

## 4. Milestones and acceptance

1. Opaque evidence and bounded cache: reject cross-instance/generation, stale
   path and legacy identity; deterministically close evicted descriptors.
2. Fuse eligible digests into existing CRC read: one producer pass; no new
   scan for partial/no-CRC/no-PAR files; unchanged pre-publication CRC refusal;
   cancellation joins actual worker; failed/stale durable ack admits nothing.
3. PAR reuse and no-op fast path: 134-file real-rename fixture has no full
   naming-source reads on hits and still reads every renamed destination.
   Preserve same-prefix ambiguity and strict fallback behavior.
   Drive real writers, owner durable acknowledgement and PP admission rather
   than manually seeding cache tokens. Verify known PAR eligibility against
   the recorded obfuscated-job shape and test late PAR discovery as a miss.
4. Accurate bytes/timing/progress: measure real reads and separate hash,
   namespace, manifest and journal durations, including final readback.
5. Astra reviews the complete PR before unit execution; resolve findings,
   execute appropriate tests and qualify the exact final head.

Mandatory negatives: wrong CRC, size or MD5; same-prefix alternate targets;
existing target; same-size mutation with unchanged reported locator metadata
(injected only through test support, disabled in production); replaced
inode/generation/root; stale writer or failed persistence; cancellation in
both scans; crash/restart after rename; changed expected journal; retained
failed source; eviction and session teardown. Failed final SHA must never
yield successful restore, verified-source promotion or consumer publication.

## 5. Tests, merge and rollout

Astra's first complete implementation review on 2026-11-04 requested joined
descriptor disposal on every post-CRC error path and stronger async-boundary
coverage. Corrections also preindex naming parameters rather than repeatedly
scanning them. Qualification must cover a blocked real writer scan, cancellation
after CRC, publication collision, replacement while a durable acknowledgement
is pending, and conservative fallback when no producer evidence is available.
The real download fixture includes known paused PAR recovery volumes and a
multi-segment file. Review these corrections before executing modified units.

```bash
export CARGO_TARGET_DIR=/private/tmp/lifecycle-build-20261101/runner
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
NZBD_REQUIRE_TOOLS=1 cargo test --locked --workspace --exclude nzbd-cluster
NZBD_REQUIRE_TOOLS=1 cargo test --locked -p nzbd-cluster -- --test-threads=1
```

Use only one compiler/test sequence in this slot at a time. Run unit,
integration and doctest coverage and report existing opt-in probes. Exact-head
required merge, full Linux/mobile/MSRV and supply-chain CI must pass.

Merge normally against the reviewed protected base and verify equal tested
and merged trees. Deploy only Runner/discovery on nuc3 using the existing
Ansible workflow, exact source export/version, preserved refs/config/state
and rollback image tags. Binary rollback preserves active state. Verify the
exact runtime, ready backend, stable images/container identities and zero
restarts for 15 minutes; distinguish measured read savings from throughput.
