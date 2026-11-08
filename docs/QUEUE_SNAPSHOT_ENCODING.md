# Queue snapshot encoding

The owner still persists the complete queue document at each required durability
barrier. Large documents use a lossless storage encoding to reduce writes on
network volumes. This changes the bytes stored in `queue.json`; it does not
change the queue document schema, journal ordering, custody checks or the point
at which a continuation may acknowledge success.

## Decision and evidence

**Status:** Implemented; runtime throughput qualification pending.
**Date:** 2026-11-08.

On nuc3, a 227 MB queue contains about 1.93 million segment records. A steady
20-second trace measured 12 complete saves taking 16.38 seconds: writes took
10.57 seconds and fdatasync took 1.95 seconds. This is control-state write
amplification on GlusterFS, separate from article decoding and payload writes.
The per-file generation and publication barriers were introduced in `a630414`
on November 1. Those barriers preserve crash safety and remain in place.

A read-only, in-memory benchmark on the same host compressed a stable
226,988,198-byte snapshot to 59,533,252 bytes with zstd level 1 in 0.276 seconds.
Decoding reproduced the exact original bytes. The measured fast modes produced
83–85 MB and provided little speed improvement. This benchmark is not a claim
that downloads have reached 109 MiB/s.

| Option | Consequence |
|---|---|
| Compress complete snapshots | Retains the current authority and replay model; adds bounded codec work and explicit rollback conversion |
| Group file barriers | Can remove some repeated saves; still rewrites the large queue for remaining barriers |
| Add lifecycle WAL records or per-job overlays | Reduces write amplification further; requires new replay, compaction, identity and rollback protocols |
| Relocate queue state to local storage | Changes placement and host-failure recovery; not part of this change |

Compression is the narrowest measured change. More extensive persistence
changes remain separate work if sustained throughput still falls short.

## Stored format

Documents smaller than 1 MiB, larger than 1 GiB, or not reduced by compression
retain plain JSON. Larger plain documents remain supported; the encoding limit
bounds decompression rather than imposing a new queue size limit.

The compressed envelope is:

| Bytes | Meaning |
|---|---|
| 0–7 | ASCII `NZBDQZ01`, storage encoding version 1 |
| 8–15 | Exact decoded byte count, unsigned little-endian |
| 16–19 | CRC32 of the original JSON bytes, unsigned little-endian |
| 20 onward | One zstd level-1 frame with its own checksum and pledged source size |

Loading accepts legacy JSON or the recognized envelope. Unsupported versions,
truncated frames, excess declared size, incorrect decoded length/checksum and
trailing data are errors. The decoder reads at most the declared length plus
one byte, never preallocates a caller-declared large buffer, and bounds its
window. No stale alternate snapshot is used after a decode failure.

Encoding completes in memory before the existing single `write_all`, file
sync, fencing check, atomic rename and directory sync. Compression errors occur
before creating the temporary snapshot. A failed save remains unacknowledged;
the ordered persistence worker must retain its journal evidence.

`SnapshotStore::decode` is also used by initial cluster-control migration.
Migration fingerprints and backup files retain the original on-disk bytes.

## Inspection and rollback

Old binaries cannot read this envelope. An isolated test of the prior live
0.4 image left its snapshot and journal unchanged and reported a corrupt-state
startup failure; its public control listener remained available to report the
failed backend. This is refusal, not a successful downgrade.

Before downgrading:

1. Stop every daemon that can own the inventory and preserve the current state
   and image rollback material. Never restore an old snapshot over newer work.
2. Determine the highest **document schema** supported by the target binary.
   Codec conversion does not make an M6 document compatible with an older schema.
3. Run the **new** binary offline:

   ```sh
   nzbd snapshot-plain --state-dir /processing/queue \
     --inventory-dir /processing/queue --max-schema 5
   ```

4. Verify conversion succeeded, then start the chosen old binary with the same
   current state. Preserve journals and inventory; conversion does not replay,
   compact, rotate or migrate them.

Use the actual enrolled inventory directory for `--inventory-dir`. It defaults
to `--state-dir`, but installations with a separate artifact directory must
supply that directory. The existing `artifacts.lock` is opened and exclusively
locked; the tool does not create a replacement inventory or validate that an
arbitrary supplied lock belongs to an arbitrary state directory.

The command validates every authoritative and `workers/*/queue.json` document
before the first replacement, including mixed plain/compressed stores. It
preserves the exact decoded JSON bytes, whitespace and unknown fields. Each
replacement is atomic and durable. An interrupted conversion can be rerun;
already plain files remain unchanged. The command never starts an engine or
opens SQLite. On Windows and network filesystems, the normal inventory lock
must provide the same exclusion used by the daemon.

## Acceptance

Tests cover complete document equality, the production-shaped queue, legacy
JSON, unknown/truncated/corrupt frames, declared-size limits, trailing frames,
failed fencing/filesystem commits, journal preservation, offline lock exclusion,
worker stores, exact rollback bytes, mixed stores and resumable conversion.
First cluster enrollment must decode a compressed snapshot and retain its exact
original bytes in the migration backup.

Deployment needs separate source and binary-hash receipts for the qualified
0.4 backport and current-main PR. Repeat untraced, sustained measurements with
one active download and no speed cap, then check the snapshot syscall profile.
The 109 MiB/s runtime gate remains open until measured.

## Encoding reuse for unchanged jobs

After the version-1 rollout, a separate 20-second trace measured 16 saves taking
17.40 seconds, with writes down to 3.68 seconds and fdatasync 0.74 seconds. Most
remaining save time was CPU work rebuilding the same queue encoding. Initial
untraced windows were 57.2 MiB/s and later 18.0 MiB/s while recovery progressed;
these do not satisfy the target.

The bounded `SnapshotEncoder` consumes the already-owned snapshot document. It
reuses a job fragment only after **full derived equality of the current job**,
including every file, segment, proof, generation, Keep flag, status and stage.
Root fields, pending admissions and URL admission receipts are encoded from the
current document every time. Job ordering, first-element comma placement,
insertions and deletions are preserved; duplicate IDs are freshly encoded.
Torrent records are always encoded fresh because numeric floating-point equality
can consider `-0.0` and `0.0` equal although their serialized bytes differ.

Encoding version 2 (`NZBDQZ02`) adds a four-byte little-endian frame count after
the version-1 length/CRC header. Exactly that many independent zstd frames carry
the opening document fields, each complete job fragment, and closing fields.
Their concatenated decoded bytes must exactly equal ordinary serialization of
the current `QueueSnapshotDoc`. The file contains every byte needed for recovery;
there is no earlier-file reference, external dictionary, delta replay or second
authority. The checksum covers the complete reconstructed JSON in its current
order. The decoder enforces frame count (2–4098), total length, checksums and no
trailing frames/data. Plain JSON and version 1 remain readable.

The authoritative encoder's retention budget is 768 MiB of **estimated allocated
capacity**, and the independent worker encoder has 64 MiB. Accounting walks
nested Vec capacities, String capacities, boxed message IDs and frame capacities,
with double-capacity plus per-allocation headroom and a map reserve. Oversized
entries are not retained. This is not an RSS cap: the owned incoming document,
encoded output and temporary changed fragment also occupy memory. Staging is
bounded to 256 MiB of compressed fragments and 128 MiB per changed job fragment;
oversized documents fall back to the original complete encoding after releasing
staged/cache buffers. Queues beyond the 1 GiB compressed-document limit remain
supported as plain JSON. The read-only native benchmark reports actual process
RSS separately, including its comparison documents and buffers.

Each store keeps its own cache. Cache contents are an accelerator even after a
failed save: no callback succeeds until the same file-sync, fencing, rename and
directory-sync sequence completes. Journal compaction still follows that commit.
Full equality prevents a failed-save cache from substituting stale metadata.
Exhaustive field audits and differential tests require new persisted fields to
remain covered.

A version-1 binary also refuses version 2. Use the **new version-2 binary's**
`snapshot-plain` converter before rolling back to the prior codec image as well
as before rolling back to a plain-JSON-only image. All earlier rollback and
schema checks still apply.

Read-only CPU validation can run the `nzbd-state` example
`snapshot_encoding_bench` against a snapshot file. It reports whole-document,
cold-cache, warm-cache and largest-job-changed timings, exact byte round trips,
reuse counts, structural memory estimates and benchmark peak RSS. It starts no
engine and writes no state. A live deployment is justified only after measured
benefit, full durability qualification and review.
