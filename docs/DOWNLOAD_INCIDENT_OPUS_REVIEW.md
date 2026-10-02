# Independent review — DOWNLOAD_INCIDENT_REVIEW.md (incl. §10–12)

**Reviewer:** Claude (Cowork session) · **Date:** 2026-10-01 ~21:20–21:45 UTC
**Method:** production reads on nuc3 (API, `docker logs`, `stat`/`ls`, RAR header
listings of the parked byte copies in throwaway `--network none` containers with
the payload mounted read-only); one 3-ms `renameat2` probe per mount using
dot-named scratch dirs (created and removed); source at `b4ed40e` / `9d3288b` /
`ab25d0c`; the §12 fix and a scheduler test built and run on nuc3 in `/tmp`
(since deleted). Production config, queue, and inventory were not changed.

## 0. Verdict

The doc's caution is good, and its §4.1 and §12 diagnoses hold up. It misses
the common cause of most of the incident, and it leaves open questions that
the evidence now answers:

1. **`renameat2(RENAME_NOREPLACE)` returns `EINVAL` on both payload
   filesystems.** `/working` is NFS 4.1 and `/processing` is a **GlusterFS FUSE**
   mount; the doc doesn't say what either filesystem is. Every exclusive rename
   in the product goes through `fs::rename_exclusive`, so it can never succeed
   here. This causes the parking `EINVAL` (every park since at least 09-28),
   the §11 recovery `EINVAL`, and the §10 zombie's precondition. It also causes
   a **live defect the doc doesn't mention**: since the 09-30 22:44 deploy,
   obfuscated downloads are reported **SUCCESS** without being renamed or
   extracted.
2. **Job 1691's extraction failure is identified.** `rar_rename` renumbered an
   old-style RAR4 set out of order: the head volume became `part56`, and the
   last volume became `part01`. All four UNPACK_FAILUREs (1691, 1692, 1695,
   1696) show the same pattern.
3. **The post-deletion stall is a missing transition, and it's proven.** A
   refused allocation makes `grant_work` return nothing for *every* job. The
   blocked job is never held. The artifact delete path never wakes the
   scheduler.
4. **Candidate §5.2 (payload under `main_dir`) must not ship.** On this
   deployment it would turn every successful download into a publication
   failure. It also contradicts the NZBGet `InterDir` contract this config
   surface exposes.
5. **§12 identity fix: verified, and it's the right owner.** Its 4 new tests
   fail on the baseline and pass with the fix (119 passed / 2 ignored, matching
   Astra). There are two follow-ups (§6).

## 1. Provenance (doc §3.1 left this open)

- `/opt/noirr/runner` (compose project `runner`, `build: context: .`) reflog:
  `pull: Fast-forward → 9d3288b` at **2026-09-30 22:38:44 UTC**. The image was
  created at 22:44:57, and tracked files are clean. **Deployed = `9d3288b`** (PR
  #246, GitHub `main`), which is 7 commits ahead of the doc's baseline
  `b4ed40e`.
- In `b4ed40e..9d3288b`, `owner.rs` changes only yEnc-name tolerance and a
  `job held` log line. `nzbd-api/lib.rs` changes the held-resume message.
  Nothing changes in `nzbd-state` or `nzbd-post`, so the baseline reasoning
  about allocation, scheduling, and relocation applies to production.
- **Jobs 1691/1692/1695/1696 did not run on that binary.** They finished
  18:40–18:52 UTC 09-30, before the 22:44 deploy. Their staging dirs are named
  `.runner-move-<id>-1`, the key format from before `3f1efc2` (the current key
  appends a destination hash). That build is consistent with `ab25d0c`
  (`/opt/noirr/nzbd` reflog). That build's PP renames used `std::fs::rename`
  and it had no relocation probe, and those differences matter below.

## 2. Root causes, one per defect

### D1 — Exclusive rename is unsupported on both payload mounts (new; highest impact)

**Observed:** `findmnt`: `/mnt/qnap/working` is `nfs4 vers=4.1`, and
`/mnt/processing` is `fuse.glusterfs` (`localhost:/processing`, glusterfs
11.2). A direct probe on nuc3 (`renameat2(AT_FDCWD, a, AT_FDCWD, b,
RENAME_NOREPLACE)`) gave:

| Root | dir NOREPLACE | file NOREPLACE | plain `rename` | `fsync(dirfd)` |
|---|---|---|---|---|
| `/mnt/processing` | **EINVAL** | **EINVAL** | ok | ok |
| `/mnt/processing/failed` | **EINVAL** | **EINVAL** | ok | ok |
| `/mnt/qnap/working/monarr/completed` | **EINVAL** | **EINVAL** | ok | ok |

This is kernel behaviour. NFS has no rename-flags support, and FUSE returns
`EINVAL` when the userspace server doesn't implement `RENAME2`. Copy, fsync,
and plain rename all work. The code even anticipates it: `probe_publication`
classifies `EINVAL|ENOSYS|EOPNOTSUPP` as "unsupported publication". The only
fallback, the registry policy, is unreachable in production ("The normal API
always passes None").

**Every caller is affected:** `relocation.rs` (park/category move/publish),
`recovery.rs:457/562` (recovery publish into `/processing/recovery`, which is
the §11 `EINVAL`), `transforms.rs`, and, since `3f1efc2`, the PP renames:
`rename.rs::safe_rename` (par/rar rename), `deobfuscate.rs:206`, and
`tools.rs:447` (unrar→7z retry).

**Live consequence (not in the doc): false SUCCESS since 22:44 09-30.**
`safe_rename` logs `rename failed … Invalid argument (os error 22)` and returns
`None`, and PP carries on. That has happened 22 times in the current
container's log. History and disk:

| Job | History | What's actually in the completed folder |
|---|---|---|
| 1699 Henry.Gambles… | SUCCESS, no unpack stage | 15 **RAR5 volumes** (14 × 50 MB + tail) (`Rar!\x1a\x07\x01`) + PAR2, all with obfuscated names and no extension; nothing extracted |
| 1700 South.Park.S29E02… | SUCCESS | an 858 MB **Matroska** file with no extension (deobfuscation failed) + PAR2 |
| 1702 The.Last.Match.2013… | SUCCESS, no unpack stage | 4 **RAR5 volumes** + PAR2, unextracted |

Mechanism: the renames fail, so `detect_archives` finds no `.rar`, the unpack
stage is skipped, and the outcome is SUCCESS. This is the doc's hypothetical
in §5.3 ("absence of a detected first volume"), and it is happening now.

**Violated invariant / owner:** the filesystem primitive (`artifacts::fs::rename_exclusive`)
promises a capability the supported roots don't have. Callers then either fail
hard (relocation, recovery) or swallow the error (the PP rename stages, which
also break the Processing → history invariant). Two owners:

- `fs::rename_exclusive` needs a mechanism that works on NFS/FUSE. Use
  `link`+`unlink` for files (atomic `EEXIST` on NFS). For directories, reserve
  with `mkdir(target)`, check the target's identity, then `rename(src, target)`
  over the runner's own empty directory. That fails closed with `ENOTEMPTY` if
  anyone writes into the reservation. Alternatively, Paul decides these roots
  get a documented non-exclusive mode. Either way it's one cohesive change at
  the primitive, not per-caller special cases.
- PP rename/deobfuscate stages: a rename that fails must fail the stage or
  hold the job, never fall through to SUCCESS.

**Discriminating test:** a filesystem fixture that returns `EINVAL` for flagged
renames (the existing `FAULTS` hook covers `capability`, but PP renames aren't
under it). Assert that the par-rename/rar-rename failure is reported as a
failure. The neighbouring case: on tmpfs/ext4 the same job still succeeds.

### D2 — Job 1691's UNPACK_FAILURE: misordered RAR4 volume renumbering (doc §3.6/§4.3: "not established")

**Observed:** the parked copy in `/processing/failed/.runner-move-f45132…-1`
holds all 59 entries. The loop verified each file's size and SHA-256 against
the source before the final rename failed, so this is a byte-identical copy.
`7z l -slt` on each volume:

- `part01.rar`: 79,983,964 B, **Volume Index 55**, Split Before `+`, Split After `-`. This is the *last* volume (`.r54`, `[57_59]`).
- `part02..part55`: Volume Index 1..54.
- `part56.rar`: **Volume Index 0**, Split Before `-`. This is the real head (`.rar`, `[58_59]`).
- `Type = Rar` (RAR4), not RAR5.

The same signature appears in all four failed jobs' copies. 3c15… has 50
volumes with head at `part50` and vol 29 at `part01`. 33be… and de98… have 8
volumes each, with head at `part08` and vol 4 or 6 at `part01`.

**Mechanism:** `rar_rename` collects unknown-extension RAR files.
`rar5_volume_number` returns `None` for RAR4 headers, so it falls back to
`files_of` sort order and numbers `part01..partNN`. In old-style naming the
head `.rar` sorts after `.rNN`. RAR4 *does* record volume order (the
first-volume flag and the volume number 7-Zip reads), but the renamer doesn't
parse it. The extractor then opens a continuation volume as volume 1 and fails.
The first parked byte was written at 18:41:56.99, 2.9 s after unpack began at
18:41:54. **The 64.7 s "unpack" duration is mostly the 5.5 GB park copy**, not
extraction.

**Owner / fix:** `nzbd-post::rename::rar_rename`. Order RAR4 volumes from
their headers, the way it already does for RAR5. If order can't be proven,
don't renumber (fail loudly, as `split_volume_ext`'s own comment argues).
This code is unchanged on current `main`; it's masked today only because D1
makes every rename fail.

**Test:** a 3-volume RAR4 old-style fixture with obfuscated names that sort
the head last. Assert `part01` is the head. The neighbouring case is an
already-correct RAR5 numbered set, which must stay unchanged.

### D3 — Parking `EINVAL` (doc §4.3: "premature to name a syscall")

Under the build that ran 1691 (pre-`3f1efc2`): `rename_exclusive(source,
dest)` across NFS→Gluster gives `EXDEV`, which falls into the staging copy.
All 59 files were copied and verified, and `sync_directory` succeeded. Then
`rename_exclusive(scratch, /processing/failed/<name>)` on Gluster gave
**EINVAL**. All of that matches the probe, and a full verified staging copy
with no published directory isn't consistent with any other step failing.

Scale: inventory lists 17 `scratch-move-*` generations since 09-28 14:27,
including 136 GB, 66 GB ×3, and 18 GB ×3 retries. Every one failed.
`/processing/failed` still holds 9.2 GB in four orphaned staging dirs.

Current build: `probe_publication(/processing/failed)` now hits the same
`EINVAL` *before* copying, so parks fail fast. No park has occurred since the
deploy, so this is source-only. **Fix = D1.** Nothing parking-specific is
needed.

### D4 — Queue stall (doc §4.1 "Still unresolved")

**Observed** (container log, 20:30–21:20 UTC):

- 20:40:55: 1704 added and queued, with no allocation attempt while 1703 is
  active (concurrency honoured).
- 20:41:05.54: the first `payload allocation unavailable … job=1704`. That's
  when 1703's slot freed, which matches 1703's `.part` mtime of 20:41:05. **So
  pausing worked**, and the scheduler moved to 1704 immediately.
- The 120 errors come in **four bursts of about 30**, one per connection task,
  at 20:41:05, 20:41:34, 20:43:31, and 20:44:01.96. The last burst follows a
  web-UI global pause (20:43:59) and resume (20:44:01).
- **No log line of any kind after 20:44:02**, including across the 21:05:22
  artifact deletion. As of 21:26, the 1704 pathname no longer exists under
  completed.

**Mechanism (source plus test):**

- `grant_work` handles an `ensure_allocation` error with `break`. That returns
  the partial batch, which is usually empty, and doesn't hold the job, change
  its state, or advance `rotate`. Selection is deterministic, so every later
  call picks 1704 again. **One refused allocation starves the whole queue,
  not just 1704.** The log text "download held" is false: the job stays
  `Queued` and is never held.
- `pool.rs`: an empty lease batch parks the connection task on
  `epoch.changed()`. `idle_hold` only retires the socket and then parks again.
- `DELETE /api/v1/artifacts/{id}` → `Inventory::request_delete`/`kick_after`
  never touches the Owner, so the epoch never changes. A path that frees an
  allocation has no route to the scheduler.

**Proof on baseline `b4ed40e`:** an owner unit test with three queued jobs and
a pre-existing `dest/job-1`. Five `grant_work` calls give `[]`, and all three
jobs stay `Queued`, `held=false`. After removing the directory out of band,
`epoch.has_changed() == false`. The next `grant_work` call leases `[1, 2, 3]`.

**Owner / fix:** the queue owner already has the right transition. In
`grant_work`, an allocation refusal should call
`hold_job(job, "storage_error", "download_write", <error>)` and `continue`.
`hold_job` sets `Paused`, re-releases leases, bumps the epoch, persists, and
publishes. The scheduler then moves to the next eligible job, and the UI shows
a real reason. Recovery is the existing resume: resume goes through the Owner,
which bumps the epoch, so it retries allocation (and holds again if the
conflict persists). **No timer, and no inventory→scheduler wake channel is
needed.** That path only matters for an unheld queued job, which this fix
removes. A legacy job 1704 needs nothing special either. Its directory is
gone, so the first `grant_work` after restart allocates it.

**Discriminating test:** the test above, with assertions flipped. Jobs 2 and 3
lease, job 1 is held with cause `storage_error`, and resuming job 1 after the
directory is removed leases it. Neighbouring case: a transient `writer
retirement in progress` refusal must not be converted into a review hold.
`ensure_allocation` returns that as a `Conflict` too, so distinguish the
variants.

**Live experiment (harmless, confirms the wake-up gap):** one global pause and
resume in the web UI. If D4 is right, 1704 starts immediately.

### D5 — URL admission parity (doc §4.1): confirmed

`AddParsed` suffixes and allocates at admission. `CompleteUrlFetch` does
neither, so URL jobs defer allocation to `grant_work`, where D4 makes it fatal
for the queue. The candidate's `allocate_new_payload` + `hold_job` at
`CompleteUrlFetch` is the right shape. With D4 fixed, D5 becomes "URL jobs
get a fresh name instead of a hold", which is a correctness improvement rather
than an outage fix.

### D6 — "Payload and completed are conflated" (doc §4.2): reframe

- nzbd exposes `MainDir`/`DestDir`/`InterDir` on the NZBGet API (`main.rs`
  ~360–380). NZBGet's contract: when `InterDir` is empty, downloads go directly
  to `DestDir`. With `inter_dir` unset, payload under `dest_dir` is the
  **documented contract**, not a mis-wiring.
- The real defect: **`paths.inter_dir` is accepted but ignored by the engine**
  (`EngineConfig::single_node(…, cfg.dest_dir(), …)`). Setting it changes
  nothing except Files-scan roots.
- Today's placement is load-bearing: a successful job's final path equals its
  allocation, so `relocate` short-circuits (`source.path == destination`) and
  never meets D1. That's the only reason successful downloads work at all.

## 3. Candidate patch (doc §6) — verdicts

| Part | Verdict | Why |
|---|---|---|
| `owner.rs` `allocate_new_payload` + URL parity | **Keep** (with D4) | Correct owner, and its test is sound. On its own it doesn't fix the stall. |
| `owner.rs` inventory-path cache in writers/recovery | Keep only with D6 | Only meaningful once the root can differ from `dest_dir`. |
| `config` `download_dir()` = `inter_dir` or **`main_dir`** | **Reject** | (a) It changes the NZBGet contract for every install with `InterDir` empty. (b) Job directories would become siblings of `queue/`, `failed/`, `recovery/`, `torrents/` in `/processing`. A job named `queue` would collide, and the Files scan would treat state dirs as payload. (c) Every success would then cross Gluster→NFS through D1, so **every successful job would fail publication** (`PostError "completed publication: …EINVAL"`). |
| `validate()` overlap rule | Reject for now | It rejects configs that were valid before, and the motivation goes away once the main_dir fallback is dropped. |
| `manager.rs` publish-after-scripts | **Defer** | Depends on D1, and it changes the scripts' `NZBPP_DIRECTORY` contract. Revisit together with honouring `inter_dir`. |
| `manager.rs` archive-only guard | **Reject as written** | It **doesn't catch the live false successes** (1699/1702 volumes have no extension). It's overbroad: with `unpack=false` it fails any `.zip` job, and `split_volume_ext` treats `.a52` as an archive. The fix belongs at D1/D2: don't swallow rename failures, and detect archives by signature, which `rar_rename` already reads. |
| `artifacts.rs` scan roots / `worker.rs` | Defer with D6 | |
| UI select-all / retention text | Keep | Independent and harmless. |

## 4. §10–12 (job 1688 / identity fix) — review

**Diagnosis:** agreed, and the trigger traces to D1. 1688's park failed (D1),
leaving a `review` relocate op. The 18:20:27 deletion didn't cancel it. The
next startup's `reconcile_relocations` then tried `commit_relocation`, which
failed. Its scratch `scratch-move-365ec8…-1` is `source_gone`, which explains
the stored `ENOENT`. That branch revived the artifact as `retained / review:
interrupted move` over job 1689's directory.

**Verified:** in my tree (`b4ed40e` + the three-file diff from Paul's working
tree, byte-identical), the 4 new tests **fail on baseline** (`retained` ≠
`deleted`, etc.), and with the fix `cargo test -p nzbd-state` gives **119
passed / 2 ignored**. An instrumented trace shows the op cancelled in the
delete transaction and the tombstone surviving `reconcile_startup`. (A first
run looked like a failure. It was my mistake: a shared target dir plus
`git archive` mtimes produced a stale binary. A clean rebuild passes.)

**Why it matters more than the doc says:** inventory records inode 3735572 for
**both** job 1685 and job 1689. NFS reuses inode numbers. If 1689's directory
had reused 3735597, `verify` would have *passed*, and the zombie, with owned
authority and a retention deadline, could have acted on 1689's payload. The
generation and terminal-deletion check in this fix is the real protection;
identity alone isn't enough.

**Follow-ups:**

1. `reconcile_deleted_artifacts` keys on artifact ID. Scratch IDs
   (`scratch-{key}`) and op IDs (`key`) are deterministic, not allocation
   identities. Scope the tombstone repair to the generation recorded with the
   successful delete, so a recreated ID can never be force-deleted on restart.
2. A `review` relocation whose source is *not* deleted still has no operator
   resolution. There's no route to abandon it, and every restart reapplies
   `review: interrupted move`. Add an explicit "abandon relocation" operation.
   Releasing a hold shouldn't be the way out.

## 5. Smallest justified scope, in order

1. **D1 primitive**: an NFS/FUSE-capable exclusive publish in
   `artifacts::fs::rename_exclusive` (or Paul's ruling on a non-exclusive mode
   for these roots), plus a PP rule that rename failures fail the stage. This
   stops the live false SUCCESS and unblocks parking and recovery.
2. **D4**: `grant_work` holds and continues.
3. **D5**: the candidate's admission helper.
4. **D2**: RAR4 volume ordering in `rar_rename`.
5. **§12** as written, plus follow-up 1.
6. **UI** select-all and the terminal-recovery filter (§11), as the doc
   proposes.
7. **Later, after Paul decides:** honour `inter_dir` and publish
   inter → dest via the D1 primitive.

## 6. Decisions for Paul

- **Production right now:** every obfuscated download since 09-30 22:44 is
  falsely SUCCESS (1699/1700/1702 so far, likely imported as nothing). Options:
  roll back to the pre-`3f1efc2` build, which renames with `std::fs::rename`
  but still has D2, or ship D1 first. (No deployment was done here.)
- **Unfinished payload in completed:** that's NZBGet-correct with `InterDir`
  empty. If you want it elsewhere, the clean route is to set `InterDir` once
  the engine honours it (D6), not to redefine `MainDir`.
- **D1 semantics:** reservation-based exclusive publish, or an explicitly
  non-exclusive mode on NFS/FUSE.

## 7. Still unresolved

- Why one later volume sorted first in each set (the original on-disk names
  are only in the gone container's logs). It doesn't change D2's fix.
- No park has run on the current build, so the D3 fast-fail path is
  source-only.
- The 13 historical `source_gone` scratch generations: who removed those
  staging directories isn't recorded.