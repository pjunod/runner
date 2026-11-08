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
