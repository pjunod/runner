# Re-review: nzbd startup availability and runtime verification

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [STARTUP_RECOVERY_BACKPRESSURE.md](../STARTUP_RECOVERY_BACKPRESSURE.md) and [STARTUP_VERIFICATION_PLAN.md](../STARTUP_VERIFICATION_PLAN.md).

**Status:** independent re-review preserved; disposition in the companion record · **Received:** 2026-11-01

Companion to [STARTUP_VERIFICATION_PLAN.md](../STARTUP_VERIFICATION_PLAN.md)
(proposal) and [STARTUP_VERIFICATION_REVIEW.md](STARTUP_VERIFICATION_REVIEW.md)
(findings and disposition). The user supplied this Opus re-review; its
body is preserved verbatim. Source-level findings are not runtime test
results or evidence that the failure occurred on nuc3.

I read the source in a fresh clone at 9e4d6c9 and a253562. Nothing was built, run or deployed.

**Verdict:** The revision fixes most of the original findings. Four blockers remain:
- B1: the durability audit has a definite result, and it should be the second correction, not an optional add-on.
- B2: a shutdown race that I confirmed by reading the source (not reproduced).
- B3: the Kubernetes example will kill a slow start today.
- B4: the probe and bind address are tied to the volume config.

Some of the added machinery isn't needed (§3 below).

## 0. Baseline

The implementation baseline should be **a253562**, not 9e4d6c9. a253562 is `origin/main` and the nuc3 checkout. It is 34 commits ahead of 9e4d6c9 and changes about 13.5k lines, including owner.rs (+1.2k), post/manager.rs (+5.2k) and queue schema 4 → 5.

I re-checked every seam this review relies on at a253562:
- These functions are byte-identical at both commits: `recover`, `fold_job_journals`, `adopt_authority`, `import_job`.
- `writer.rs` and `tls.rs` are unchanged.
- The shutdown order is unchanged.
- PP admission is still driven by snapshots.

Line numbers below are at a253562.

## 1. The original P1-1 premise was wrong

`write_segment` calls `sync_data` before it returns (`writer.rs:338`). `SegmentWritten` is sent only after that. At runtime, only `on_segment_written` (`owner.rs:3332`) sets `Done`. So every runtime Done record already has durable data behind it. Lost journal appends only cost a redownload, which is the safe direction.

Fix the stale comments that mislead readers (including me): `finalize()` says "per-segment writes stay relaxed (page cache)", the doc on `a_finalize_that_fails_reports_not_ok` says the same, and `owner.rs:908` says "Revalidate even old page-cache acknowledgements".

## 2. Blockers

### B1. The audit's answer: today's reread is mostly unnecessary, and one gap in what is recorded stops us skipping it

Given the per-segment sync, the boot reread only protects against four things:
- (a) Done ranges written by binaries older than 3f1efc2 (2026-09-30).
- (b) Journal records written by other cluster nodes, whose binary version is unknown.
- (c) A changed file identity at the path (replaced, relocated or swapped).
- (d) Outside tampering or storage corruption. That is PAR2's job, and a boot CRC pass is not a reliable defense against it anyway.

**The gap:** nothing records which writer produced a range.
- `JournalRecord` (`nzbd-state/src/lib.rs:92`) has no version field.
- Snapshot schema 4 dates from 2026-08-22, before the sync guarantee, and v5 only adds URL admission receipts.
- So there is no way to tell a synced range from a legacy one, or a local record from a foreign one.

**Minimum fix (no watermark needed):**
1. Per file, record durable-ack provenance (the writer protocol version) and the identity from `fstat` on the descriptor the writer opened. Use `(st_dev, st_ino)`, plus size ≥ the range end.
2. Put the same provenance in a journal header or in each record, so foreign journals carry it too.
3. At boot, a file with current provenance and a matching identity is trusted with no byte reads.
4. Legacy files, foreign files without provenance, and files whose identity doesn't match get scheduled verification. A legacy file is verified once and then stamped.
5. The order segments were downloaded in doesn't matter, because the proof is per write rather than per position.

On nuc3 this turns 9,496 range reads (6.8 GB) into about one `fstat` per file. That removes the cause of the incident.

Async verification is still needed for whatever is left (legacy, foreign and mismatched files), but at a much smaller scale. Treating this as an optimization kept separate from availability means building the async machinery to handle 6.8 GB instead of removing the work.

**To verify on nuc3:** confirm that GlusterFS FUSE inode numbers (derived from gfid) stay the same across remounts, and that `fsync` reaches the bricks with the current write-behind and flush-behind settings. Until both are measured, identity is supporting evidence only.

### B2. A finalize still in flight at shutdown demotes a complete file (I confirmed this by reading the source; not reproduced)

Sequence:
1. The owner loop exits.
2. The final `save_snapshot` runs, then `marker.disarm()` (`owner.rs:1133`), then `writers.clear()` (`:1136`).
3. `Engine::shutdown` then waits on the tracker. A writer that is mid-`finalize` completes `link(part → final)`, the directory sync and `remove(part)`, but its `WriterFinalized` goes to an owner that no longer exists.
4. On the next boot the file is `!finalized`. `recover` resolves to `.runner-file-N.part` (gone), then falls back to `{filename}.part` (never existed).
5. `validate_range` fails, and every Done segment becomes Pending.
6. The redownload's `finalize` then fails with EEXIST in `linkat` and becomes an identity/publication error.

This happens on a clean shutdown. A crash between `remove(part)` and the next snapshot gives the same result.

**Correction:**
- One resolver that checks the publication name. `WriteCmd::PublicationName` can change `final_path` away from `f.filename`, so that name has to be persisted.
- If the `.part` is missing but the final file is present, keep Done and let `startup_pass → finalize` take its existing "no part, final verified by combined CRC" path.
- At shutdown, wait for the writers and apply their results before the final snapshot.

This is the real point of the §9 ordering. Note that nothing trusts the clean marker: `was_unclean` is used only in a log line (`owner.rs:975`). Don't design a protocol to make the marker trustworthy; keep it as a diagnostic.

### B3. The Kubernetes example will restart-loop a slow start today

- `examples/kubernetes/deployment.yaml:47` sets liveness to `httpGet /healthz`, initialDelay 10 s, period 30 s, with the defaults failureThreshold 3 and timeoutSeconds 1. A 183 s pre-listener start is killed at about 100 s, so the pod never comes up.
- §8 covers readiness only. It also needs to specify:
  - liveness, as an exec probe against the bootstrap `/healthz`;
  - `timeoutSeconds` set explicitly, because the Kubernetes default is 1 s, not 5 s;
  - a `startupProbe`, or a guarantee that the bootstrap listener comes up before any storage I/O.
- Ship the probe changes now, independent of the redesign.

On Compose: §8 says to "use exec-style probes in Compose", but Compose already does. It runs `nzbd status --url 127.0.0.1:6789`, which hits `/api/v1/status` (not `/healthz`) over plaintext. Its client (`main.rs:1413`) has no connect timeout and a 30 s read timeout. The fix is the route and the client deadlines, not the probe style.

### B4. The probe and bind address can't come from the config on the volume

§8 says the bootstrap bind configuration "must be available before volume-backed config recovery". Spell out what that means:
- the address comes from a compiled-in default, overridable by an env var or CLI flag, never from `nzbd.toml`;
- the probe subcommand uses the same source.

Otherwise a config read blocked on Gluster brings back the barrier through the probe path.

## 3. Unnecessary complexity

- **P0-1 journal retention through the first save and a restart.** This isn't needed if outcomes that aren't proof never change `Done`. Let the snapshot keep Done with an "unresolved" flag, and normal compaction stays safe. The rule should simply be "only proof demotes".
- **§9's six-step marker protocol.** See B2. Keep the drain order because it gets the writers' acknowledgements into the final snapshot. Drop the framing that makes the marker load-bearing.
- **New root-UUID marker files for the first correction.** The failure to prevent is "unmounted state_dir reads as a fresh install". Existing evidence is enough:
  1. On first successful inventory open, record the Inventory's SQLite installation ID in the config (or its mirror).
  2. At boot, a config with an ID next to a state_dir that has none means the root is unavailable, not new.
  3. Enrolling existing installs means writing that ID the first time the inventory opens. This isn't destructive, rollback ignores the field, and it gives an explicit recovery path.

  Bring UUIDs in later only if this proves insufficient.
- **A new "custody lease" vocabulary for PP.** a253562 already has `Artifact:Custody` and `Delivery:Custody` parameters, plus `Inventory::verified_custody_evidence` (`artifacts/transforms.rs:654`) over the rename, extract and par_repair operation journals. Build the P0-3 lease on Inventory operations, or name it something else, such as an "admission lease". Two meanings of custody in one codebase invites mistakes.
- **Coalescing reads and the I/O budget** (my original P2). This is moot once B1 lands, because the remaining set is small.

## 4. Should fix

| # | Type | Finding | Correction |
|---|---|---|---|
| S1 | Confirmed in source | PP admission at a253562 has more snapshot-driven entry points: `manager.rs:451` (JobFinished), `:498`, `:569`, `:611`, `:2017` (startup/rescan). | List all five in P0-3's caller audit. The manager grew by 5.2k lines since 9e4d6c9, so the 9e4d6c9 line numbers in the proposal are stale. |
| S2 | Redesign-introduced | A public listener that dies after startup (a failed rebind on reload, a TLS reconfig) leaves `/healthz` at 200 and `/readyz` at 503 forever. Nothing ever restarts it. | After a deadline, losing the public listener is a liveness failure, or the process exits. Specify this with the listener reconfiguration cases. |
| S3 | Possible (not confirmed) | `SegmentWritten` carries no writer or file generation. `import_job` → `remove_job_silent` drops the writer handle, but a write already in flight still acknowledges into the re-added job. | Bind the durable-ack provenance from B1 to the file generation, and drop acknowledgements from a stale generation. |
| S4 | Optional | Once B1 exists, a CRC mismatch on a range that has current provenance and a matching identity is a storage fault, not a download gap. | Hold the file and alert instead of setting it Pending. This prevents mass redownload into a store that returns wrong bytes (for example Gluster split-brain). |
| S5 | Confirmed in source | The resolvers disagree. `recover` falls back to `{filename}.part` and `fold_job_journals` (`owner.rs:3080`) to `filename`, and neither knows the publication name. | This is covered by the §6 single-resolver rule. Add the publication name and the B2 case to its tests. |

## 5. Answers to §11

- **Original findings still not fully addressed:** P1-1, now reframed as B1; the shutdown-order prerequisite, now B2; and P1-4/§8, where Kubernetes liveness is still missing (B3).
- **Durability audit scope:** it is scoped correctly but left open-ended. B1 gives the result: the gap is about which writer produced a range, not about data that was never synced.
- **Paths that still launch work from unverified or stale state:** the five PP entry points (S1), the P0-2 takeover and import paths, and stale writer acknowledgements (S3, possible).
- **Can the barrier come back?** Yes, through probe or bind config read from the volume (B4) and permanent public-listener loss (S2). The global-lock and async-snapshot sections are adequate as written.
- **Legacy roots, persistence, shutdown and worker accounting:** the root-enrollment requirements are right but heavier than needed (§3 above). Shutdown is specific once it is grounded in B2. Worker accounting is fine.

## 6. Order of corrections

**First correction (fixes the incident and the live defects):**
1. P0-1: only proof demotes Done; typed outcomes; replace `exists()` with fallible path checks, including `tls.rs:73`.
2. B1: durable-ack provenance plus fd-bound identity; legacy ranges verified once.
3. B2: one resolver that includes the publication name; writers drained before the final snapshot.
4. B3: Kubernetes liveness/startup probes and probe client deadlines.
5. P0-2, P0-3 and P0-4 as specified. They are live today and don't depend on the redesign. B1 provenance on foreign journals reduces P0-2.

**Later:** the bootstrap listener and `/readyz`, async snapshots with revisions, owner and Inventory I/O moved off the locks, reload keeping the listener up, loading states in all clients, and the root-identity extension if the installation-ID approach proves insufficient.