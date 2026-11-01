# Independent review: nzbd startup availability and runtime verification

**Status:** independent review preserved; findings reconciled in the companion record · **Reviewed:** 2026-11-01

Companion to [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md)
(proposal) and [STARTUP_VERIFICATION_REVIEW.md](STARTUP_VERIFICATION_REVIEW.md)
(review disposition). The user supplied this Opus review. Its body is
preserved below; priorities and claims are the reviewer's, not evidence
that any described failure occurred on nuc3.

Reviewed against source at 9e4d6c9 (read-only; nothing built, run, or deployed).

**Verdict:** The direction is right: no global barrier, typed outcomes, results bound to epoch and generation. I do not agree that there are "no remaining blockers." Four paths at 9e4d6c9 already consume unverified evidence or destroy good evidence; some are live defects before any redesign. The proposal also never questions whether a full byte reread is needed, and that decides how much of §6 is worth building.

## P0: correctness holes the spec must close

**P0-1. I/O failure is treated as corruption today, and the evidence is then destroyed.**
`writer.rs:418` `validate_range` returns `bool`; any stat/open/seek/read error is indistinguishable from a CRC mismatch. `owner.rs:882` then demotes the segment to Pending and clears `finalized`. `stable.exists()` also returns false on EIO/ESTALE and silently picks a different path. `Owner::run` (`owner.rs:1065`) saves a snapshot immediately, and `save_snapshot_result` (~`5165`) compacts every job journal. Result: one boot against a stalled or erroring mount converts every Done segment to Pending and persists it irreversibly. That is the mass redownload §6 forbids, and it can happen now.
*Correction:* fix this as a standalone PR first. Typed outcome from `validate_range`; I/O error preserves Done; no journal compaction while any segment's outcome is unresolved.

**P0-2. Cluster takeover and lease import already trust saved Done bits.**
`adopt_authority` (`owner.rs:2729`, push at `2758`) adopts snapshot jobs with their Done bits unvalidated; `fold_job_journals` validates only journal records; `startup_pass` then finalizes. `import_job` (`2609`) is the same unless `fold_journals`. §6's "a saved Done bit alone cannot become trusted evidence" is already violated on these paths.
*Correction:* elevate from "audit" to explicit requirements. Takeover and import create verification tickets exactly like boot.

**P0-3. Post-processing admission is snapshot-driven, not owner-leased.**
`nzbd-post/src/manager.rs:416` (startup/rescan) and `:424` (JobFinished) start PP from `engine.snapshot()` status. PP subprocesses read the payload (and par2 repair writes it) before any owner commit, so "enforce at the committing authority" never sees them.
*Correction:* the owner issues a PP custody lease tied to the verification epoch before PP starts. Unverified jobs are never published as Completed, PostQueued, or Post. The same applies to the lifecycle rewrites inside `recover` (~`owner.rs:893–925`): identity_conflict hold → Queued, and rar_rename retry → PostQueued via `validate_post_retry`. Both consume evidence and must move into the verification-completion handler.

**P0-4. Torrent recovery reads the snapshot from disk independently, then deletes.**
`torrent_admission.rs:238–254` loads `SnapshotStore` directly (racing the owner's own save) and removes `PendingSourceStore` entries absent from that copy's `pending_admissions`. Once early admission is permitted, this deletes a newly admitted job's source as an "orphan."
*Correction:* torrent recovery takes its job set from the owner; orphan removal is an owner-ordered operation.

## P1: barriers and design gaps

**P1-1. The verification volume is never challenged.**
The reread defends against crash-lost page-cache acknowledgements: writers don't `sync_data` per segment and journal appends aren't fsynced. That risk exists only after an unclean exit, yet `recover` rereads everything on every boot and ignores `was_unclean`. A startup CRC pass doesn't reliably catch out-of-band corruption either; PAR2 is the designed defense there.
*Correction:* keep a per-file durable watermark (`sync_data` at a checkpoint, then journal the watermark) plus fd-bound identity. After a clean exit, an identity match suffices. After a crash, reread only segments beyond the watermark. That turns a 6.8 GB background reread on every boot into a small tail. The async architecture is still needed for that tail, but at a fraction of the scale.
*Prerequisite bug:* `marker.disarm()` (`owner.rs:1093`) runs before `writers.clear()` (`1096`), and writers break on cancel without syncing. Fix the shutdown order before relying on the clean marker.

**P1-2. Unlisted pre-listener barriers.**
- `open_history`, including a synchronous startup trim of SQLite on Gluster (`main.rs:452`, `:494`).
- `TorrentSession::start` and `recover_active`, whose `?` aborts startup (`main.rs:~870`, `:924`).
- Per-category `create_dir_all` + `canonicalize` on network mounts (`main.rs:887`).
- `Inventory::open` / `reconcile_startup` / `allocate` under the shared mutation mutex; `create_dir_all(dest_dir)`, `UncleanMarker::check_and_arm` (a write), and `VolumeBook::load` in `recover`.
- `tls::server_config` (`main.rs:1097`).

Torrent piece verification (librqbit's own payload check on restore) is the BitTorrent equivalent of the CRC reread. Put it in scope or exclude it explicitly.

**P1-3. Config and auth can depend on the storage volume.**
When the config file is missing, `run` recovers config from `<state_dir>/nzbd.toml.saved` (`main.rs:666–706`) before anything else. That conflicts with "apply configured auth from the first request" when state_dir is on Gluster.
*Correction:* define a minimal pre-config surface (`/healthz` and the static UI shell, no data) and put a deadline on config recovery.

**P1-4. TLS and health: take the probe out of the TLS/auth path.**
`tls.rs:73` regenerates the identity whenever `!cert_path.exists()`. A transient stat error, or a mount that isn't up yet, silently rotates a fingerprint the mobile apps pin. That is a live bug.
*Correction:* serve `/healthz` on a loopback-only plaintext listener, with an exec probe in the k8s example. That drops most of §7's probe requirements (HTTPS, self-signed trust, credentials); only the deadlines remain. The public listener can then wait for TLS material without being reported unhealthy.

**P1-5. Reload restarts everything.**
`RunOutcome::Reload` (`main.rs:135`) re-runs the whole of `run()`, including `Owner::recover`. Today, every reload-triggering settings save costs about 3 minutes of UI outage on nuc3.
*Correction:* the control listener and verification state must survive reload. §6 covers worker accounting across reload, but not this.

**P1-6. Missing verification-outcome transitions per job status.**
Today a PostQueued, Post, or retained job that loses a segment keeps its status with a Pending segment the scheduler never fetches.
*Correction:* specify a transition table per status. Scope verification to files whose bytes the download stage still owns; after PP handoff, PAR is the check. Keep verification as a separate field, not a `JobStatus` variant: `recover` already folds `held()` and `retained` into `Paused`, and a third axis on the same enum repeats that conflation.

**P1-7. Compose amplified this incident.**
`nzbd-discovery` only advertises a port over mDNS, yet it waits on `service_healthy` (`examples/docker-compose/docker-compose.yml:83`, `dev/docker-compose.yml:88`). Use `service_started`. Neither file sets `stop_grace_period`; Docker's default is 10 s, while nzbd's shutdown can take 3 s drain + 1 s + up to 10 s for subsystems + a Gluster snapshot save. A redeploy can therefore end in SIGKILL and an unclean boot, which matters once P1-1 lands.

## P2

- **qBittorrent-compat routes** are missing from §5's unavailable-response mappings. Sonarr/Radarr use them for torrents, and an empty `[]` is the false-removal hazard. The web UI (its reload poll at `index.html:6009` treats `/healthz` OK as "back") and the mobile app also need explicit loading states.
- **"Verified root" is undefined.** Require a root identity marker (UUID file plus `st_dev`). Otherwise an unmounted path reads as an empty or missing root, and a missing snapshot loads as an empty queue.
- **Bind identity to the opened fd.** Use `fstat` on the open descriptor, not path metadata, and compare size and ctime before and after the read. `validate_range` has a TOCTOU gap between `symlink_metadata` and `open`.
- **One path resolver.** `fold_job_journals` resolves `dest_dir.join(job_dir_name)` + `f.filename`; `recover` uses the inventory path with `.part` fallbacks. Relocated or legacy jobs can get spurious identity_conflict holds. Confirm this.
- **Add `/readyz` and an owner-loop heartbeat.** If health means only that the control service is responsive, an owner loop that is deadlocked (not waiting on storage) stays green forever and can't be alerted on.
- **I/O budget.** Background verification shares the Gluster volume with live downloads. Coalesce reads (one open per file, sorted ranges); 9,496 FUSE opens are a large part of the ~17 ms per segment.
- **Custody is simpler than §6 implies.** The owner is the only spawner of writers and, with P0-3, of PP. Custody can be owner-side ticket state rather than file locks; cross-process exclusion is needed only against librqbit and external actors.

## Additions to §8 validation

1. Boot with EIO/ESTALE on the payload root: every Done bit is preserved and journals survive the first save. (P0-1)
2. Takeover, and import of a lease with a corrupted Done range, never reaches finalize. (P0-2)
3. A PP rescan during verification cannot start PP. (P0-3)
4. Torrent recovery during an early admission preserves the new pending source. (P0-4)
5. A clean restart does no full-volume rereads; after SIGKILL, rereads are limited to the post-watermark tail. (P1-1)
6. A settings reload keeps the listener up. (P1-5)

**Suggested order:** P0-1 as a standalone fix now, then the watermark (P1-1), because it determines how much of §6's worker and custody machinery is worth building.