# Third review: nzbd startup availability and runtime verification

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [STARTUP_RECOVERY_BACKPRESSURE.md](../STARTUP_RECOVERY_BACKPRESSURE.md) and [STARTUP_VERIFICATION_PLAN.md](../STARTUP_VERIFICATION_PLAN.md).

**Status:** independent third review preserved; see disposition · **Received:** 2026-11-01

Companion to [STARTUP_VERIFICATION_PLAN.md](../STARTUP_VERIFICATION_PLAN.md)
and [STARTUP_VERIFICATION_REVIEW.md](STARTUP_VERIFICATION_REVIEW.md).
The user supplied this review; its body is preserved verbatim. Claims of
source-supported interleavings are not runtime reproduction results.

Checked against a253562 in a fresh read-only clone. Nothing built, run or deployed.

**Verdict:** B2, B4 and S1–S5 are resolved as specified. Three blockers remain:
- R1/R2: the proof-mutation rule is the right direction, but it is not enough as written.
- R3: one B3 gap. The Compose transition does not fix the failure that was actually observed on nuc3.
- R4: identity matching in B1 needs one ordering rule.

## Status of earlier findings

| Item | Status |
|---|---|
| B1 provenance | Accepted in principle. R1, R2 and R4 below are still needed. |
| B2 publication/shutdown | Resolved. One simplification in §3. |
| B3 probes | Kubernetes resolved. Compose is still open (R3). |
| B4 monitor bind | Resolved. One small addition in §2. |
| S1 PP paths | Resolved. `spawn_job` call sites at 455/507/575/634/2101 confirmed. |
| S2 listener loss | Resolved. |
| S3 stale writers | The amendment is right, and the hazard is worse than I said earlier (R2). |
| S4, S5 | Resolved. |

## 1. Remaining blockers

### R1. The overlap check must include unacknowledged writes, and the writer needs the proven set

Confirmed in source:
- The writer gets its offset from the decoded yEnc header (`pool.rs:437`, `result.offset`).
- `write_segment` (`writer.rs:299`) does not check for overlap at all.
- The writer is one serial task per file, but it is spawned with no knowledge of ranges proven before a restart.

As written, "reject conflicting overlaps against proven ranges" still allows this sequence:
1. A writes [0,100) and syncs.
2. B writes [50,150), overwrites [50,100), and syncs.
3. The owner applies A's ack. The proof now covers bytes that are B's, not A's.

Corrections (implementation requirements):
1. `spawn_writer` receives the proven `(offset, len, crc)` set for its file generation.
2. The writer tracks every range it has written in this generation, acknowledged or not.
3. The overlap check runs against proven ranges plus written ranges, before `seek`/`write_all`.
4. An exact duplicate means the same offset, the same length **and the same CRC**. The same range with a different CRC is a conflict.
5. During download, reject a conflicting overlap as a segment failure (it means the post is malformed). Do not invalidate and then overwrite. That keeps durable invalidation off the hot path entirely: `write_segment` never overwrites proven bytes, so test 5 becomes "a conflicting overlap is never written".
6. Finalize `set_len(file_size)` (`writer.rs:371`) refuses to trim below any proven range end. Hold the file as an identity conflict. Preallocation (`:321`) only extends the file and needs no proof change.

### R2. A removed job's writer stays alive and keeps writing to the reused path

`remove_job_silent` (`owner.rs:3046`) only removes the handle from `self.writers`. It does not cancel `stop`. In-flight leases still hold clones of the writer's sender (`pool.rs:435`, `lease.writer`). So after `import_job` → `remove_job_silent` → re-add, the old writer task stays alive with its fd open and keeps writing to the same `.runner-file-N.part`. That is a live disk writer, not just a stale ack.

Corrections:
- Use the existing retire pattern: `retiring_writers` plus the asynchronous `stopped.wait_for` at `owner.rs:4403`. Cancel `stop`, and spawn the new generation's writer only after `stopped` fires. The file sits in a "replacing" state until then.
- Don't wait for `stopped` inside the owner loop. A write stuck on the mount would freeze the owner, which breaks the availability requirement.
- Add `SegmentFailed` (`owner.rs:518`) to the stale-result fence list. A stale failure can spend attempts or fail a segment in the new generation. The full list is `SegmentWritten`, `SegmentFailed`, `WriterFinalized`, `WriterError`, `FileMetadata` and `JobNameInspected`.

### R3. The Compose transition does not fix what happened on nuc3

The observed failure was Compose, not Kubernetes: `nzbd-discovery` has `depends_on: nzbd: service_healthy` (`examples/docker-compose/docker-compose.yml:81-83`). With `start_period: 10s`, `interval: 30s`, `retries: 3`, a 183 s start marks nzbd unhealthy, and Compose leaves discovery in `Created` for good. Changing the probe command, route and deadlines changes none of that.

Correction (implementation requirement; choose one):
- **Option (a):** set `start_period: 600s` with `start_interval: 5s` (needs Docker Engine 25+, Compose 2.20.2+), matching the Kubernetes allowance; or
- **Option (b):** change discovery to `condition: service_started`, if advertising before the listener is up is acceptable. That's a decision for you, not a technical constraint.

Apply the same change in `dev/docker-compose.yml:49-54, 86-88`.

### R4. Identity: what to match, and the inode-reuse ordering

- Reuse the existing `nzbd_state::artifacts::Identity` (`artifacts/mod.rs:48`). Its `mount_id` already handles device renumbering across remounts. That shrinks the Gluster prerequisite to two checks: GFID-derived inode stability, and sync reaching the bricks.
- Match only `device` or `mount_id` plus `inode`. `modified` and `changed` change on every write, so whole-struct equality would never match.
- `bytes` proves nothing for preallocated files, because the size equals `file_size` from the first write. "Size sufficient for covered range ends" is not evidence there.
- Inode reuse: nzbd's own unlink-and-recreate of `.runner-file-N.part` (remove/import, then re-add) can get the same inode and the same preallocated size. **Required ordering:** durably retire the old generation (snapshot or journal) before unlinking or recreating its path, and have proof records carry the generation, so an old record can never match the new file.

## 2. Smaller corrections

- **Rollback of the exec probe:** a startup probe that calls `nzbd health` kills every pod of a rolled-back image that lacks the subcommand. Ship the manifest change and the image together, and roll them back together.
- **CLI compatibility:** the final `nzbd health [--ready] [--monitor-bind]` must keep accepting the transitional `--url` form. Otherwise moving to the final image breaks the manifests deployed before it.
- **Monitor bind failure:** if the monitor can't bind (port in use), exit nonzero at once with a clear error. Nothing has touched storage yet, so a fast restart costs nothing.
- **Initial TLS state:** separate an invalid initial TLS/security config (exit nonzero, because it's a config error) from a stalled read (stay at 503 and alert). S2's 30 s deadline covers only a listener that has already been ready, so without this split a bad cert sits at 503 forever.

## 3. Optional simplifications (nothing about availability or evidence gets weaker)

- **B2 reconcile without CRC:** `fileops::link` is a hard `linkat` and already checks identity before and after (`fileops.rs:87`). The published file has the part's inode, so a current-proof file reconciles with an `fstat` alone. Keep the CRC path for legacy evidence only.
- **PP repair invalidation:** don't build a second mechanism. Download proof retires at publication. After that, the existing Inventory `par_repair`/`extract` operation records (written as `running` before the tool mutates anything, `transforms.rs:1123`) are the "invalidate before mutation" step.
- **Transition probe TLS:** for a `127.0.0.1`/`::1` target, skip certificate verification. The probe sends no credentials and reads a nonsecret route, and this removes the trust-material reads and the probe failures that cert rotation would cause. Today's `nzbd status` client is plaintext TCP (`main.rs:1422`), so HTTPS trust handling would be all new code.
- **xattr allocation ID (only if the Gluster GFID check fails):** a `user.nzbd.alloc` xattr written at creation and read back with `fgetxattr` survives hard links and remounts, works across nodes, and handles inode reuse.

## 4. Answers

- **Remaining from B1–B4/S1–S5:** B1 (R1, R2, R4) and B3 (R3 for Compose). The rest are resolved.
- **Is the proof-mutation rule sufficient?** Not as written. It needs:
  - an overlap set that includes unacknowledged writes;
  - CRC-matched duplicates;
  - finalize refusing to trim below a proven range end;
  - live old writers quiesced asynchronously;
  - `SegmentFailed` added to the fence list.

  With R1, the "durably invalidate before mutation" branch is needed only for removing or recreating a generation. PP repair is covered by the existing operation journal.
- **Further simplification:** see §3. All of it is optional. R1–R4 are requirements.

## 5. Test additions

- Two in-flight overlapping writes in one generation: the second is rejected before it touches the disk.
- Same range with a different CRC is rejected. Same range with the same CRC is acknowledged with no write.
- Finalize trim below a proven range end goes to an identity hold.
- After import/re-add, segments still queued for the old writer never reach the new generation's file.
- Remove, recreate at the same path with the same inode and size: old proof does not match.
- Compose: a simulated 183 s start leaves discovery running with no manual `start`.
- A rolled-back image with the new manifests is caught by the deploy check, not by pod kills.