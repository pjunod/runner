# Independent review v2 — DOWNLOAD_INCIDENT_REVIEW.md §13 + the in-progress patch

**Reviewer:** Claude (Cowork) · **Date:** 2026-10-01 ~22:40–23:00 UTC
**Supersedes nothing in v1; corrects v1 where noted.** Production reads only.
I also ran a 1 ms `link`/`mkdir` probe per mount, using dot-named files that I
created and removed. The working-tree patch was read at 22:42 UTC while still
being edited (`fs.rs`, `rename.rs`, `deobfuscate.rs`, `manager.rs`,
`owner.rs`, `relocation*.rs`).

## 0. Summary

- **§13.3 resume, accepted:** my v1 recovery path for D4 was wrong.
  `QueueCommand::Resume` refuses any held job, and `ReleaseResourceHold`
  only releases `capacity` and `quota` holds. §2.1 gives a concrete
  transition that uses the existing protocol.
- **§13.3 directory reservation race, accepted:** reservation plus rename is
  not strictly no-replace. §2.2 gives a design that is strict on every object
  using only primitives I verified on both mounts.
- **§13.4 generation-scoped deletion, accepted:** §2.3 gives a legacy rule
  that is sound without guessing.
- **§13.6 is mis-framed.** Job 1703's files are **not extracted media**, and
  no post-extraction pass is missing. They are raw MKV posts with a PAR2 index
  that names every episode exactly. The existing par-rename stage failed on D1
  and the failure was swallowed. ffprobe is not needed for this case. The
  lexical season-pack numbering would have labelled **0 of 6** episodes
  correctly (§3).
- **New live failure:** job 1704 finally downloaded at 21:39–21:40, then
  crashed in unpack on a *directory* `rename_exclusive` and is now in a hold
  that **cannot be resumed**. The in-progress file-only fallback doesn't cover
  that path, and the new RAR4 ordering doesn't cover 1704's naming (§4).

## 1. What changed in production since v1 (observed)

| UTC | Event |
|---|---|
| 20:44:02 → ~21:39 | No log lines at all, including through the 21:05:22 artifact deletion. |
| 21:39:56–21:40:50 | **1704 downloads all 59 files** (allocation now succeeds, since its pathname is gone). |
| 21:40:52.42 | `unrar did not deliver the whole archive; retrying with 7-Zip` (head `73c2…​.rar`) |
| 21:40:52.55 | `job held job=1704 cause="unknown" stage="unpack" subprocess failed: filesystem: Invalid argument (os error 22)` → `post-processing crashed` |
| 21:49:49.67 | 1703 PP: `rename failed from=…/m7r8y9311ordvfw01f9wx … os error 22` |
| 21:49:50.62 | **1703 `outcome="SUCCESS"`**, history `par_rename:540 ms`, no unpack stage |

**D4 corroborated:** after the deletion, 1704 sat idle with zero allocation
attempts until something advanced the epoch at about 21:39. Nothing in the
INFO log shows what that was; most likely an operator resume of 1703. It then
allocated cleanly. Deleting the folder alone never woke the scheduler.

## 2. Responses to §13.3 and §13.4

### 2.1 Allocation hold: a valid retry transition (corrects v1 D4)

I checked `9d3288b` `owner.rs`:

- `Resume` returns `false` whenever `j.held()` (line ~1570).
- `ReleaseResourceHold` filters `cause ∈ {capacity, quota}`. It checks the
  revision, sets `Queued` or `PostQueued` from `previous_status`, persists,
  **then `bump_epoch()`**, and publishes.

**Proposal:** `grant_work` holds with a dedicated cause, `allocation`. Add
`allocation` to `ReleaseResourceHold`'s releasable set, with the same
revision-checked, owner-mediated protocol and `retry_policy =
"resume_same_job"`.

Release *is* the validated retry. The epoch bump makes the next `grant_work`
call `ensure_allocation` again. If the conflict persists, the job is held
again with a new revision and message, and other jobs keep going. There's no
new mechanism, no timer, and no inventory→scheduler channel.

Keep these out of it:

- `retiring_writers`. That's transient and must keep its current `Conflict`
  without a hold, which needs a distinct error variant in `ensure_allocation`.
- Custody and identity holds.

Tests:

1. Three queued jobs, job 1 refused, assert jobs 2 and 3 lease. Turn v1's
   baseline test around for this.
2. Release while the conflict persists: held again, revision+1.
3. Release after the conflict is removed: job 1 leases.
4. A writer-retirement refusal never becomes a hold.

### 2.2 Strict no-replace publication on NFS and Gluster (answers §13.3)

**Verified on both mounts today:**

- `link(a, b)` works, and `link` onto an existing name gives `EEXIST` with
  nothing replaced.
- The link shares the inode.
- `mkdir` onto an existing name gives `EEXIST`.
- `renameat2(RENAME_NOREPLACE)` gives `EINVAL` (v1).

**Strict on every object, no directory rename:**

1. `mkdir(target)` claims the name atomically, or fails with `EEXIST`.
2. For each entry in the verified manifest, depth-first: `mkdir` each
   subdirectory, then `linkat(scratch/x, target/x, 0)`. That's atomic
   no-replace per file.
3. Fsync the directories bottom-up, then unlink the scratch names and remove
   the scratch dirs.

Crash recovery is deterministic. The target dir carries the operation's
identity, so for each file:

- If the target is the same inode as the scratch, finish the unlink.
- If the target is missing, link it.
- If the target is a different inode, the conflict goes to review.

**The trade-off is visibility, not safety:** files appear in `target/` one
link at a time, over milliseconds, not atomically. Importers that act on the
SUCCESS notification never see a partial directory. Only a watcher scanning
the completed root could. That trade-off is Paul's call. The other contract
in the doc (coordinated writer, reservation plus rename) can at worst replace
a *foreign empty* directory, so it can't lose data, but it isn't strict.

**Where a directory rename isn't needed at all:** `tools.rs:447` renames the
failed extraction dir to `*.first-attempt` before retrying with 7-Zip. Give
the retry a fresh exclusive `mkdir`ed destination instead, and leave the
first attempt where it is. That removes the rename that crashed 1704 today.

### 2.3 Generation-scoped deletion, including legacy entries (answers §13.4)

Confirmed: the delete request is `(id, revision, undo_seconds, automatic)`,
with no generation. New entries should record the generation and enforce it
at execute, retry, and reconcile.

**Legacy rule, which is sound without inference from the current record:**
treat a legacy successful delete as covering the current record only when
there is a `relocate` operation with all of the following:

- same artifact ID;
- `state ∈ {running, review}`;
- `created_at` < the delete's success;
- `request.source.generation` == the current generation.

A generation changes only when a relocation commits or a new allocation is
made. An uncommitted op that predates the delete and still names the current
generation therefore proves the deleted record had that generation. That's
exactly the zombie signature (1688: op `move-365ec8…-1`, snapshot generation
`71b074a8…` = current).

Scratch artifacts have no relocate op of their own, so a reused `scratch-{key}`
ID is never matched. Anything else is left alone and listed for review.

## 3. §13.6 — what the 1703 folder really is

Container signatures in `…/Heated.Rivalry.S01.2160p.AMZN.WEB-DL.DDP5.1.H.265-RAWR`:
six files start with `1a45dfa3` (EBML/**Matroska**), and one 83,616 B file
starts with `PAR2\0PKT`. History has **no unpack stage**. These were posted
as MKVs, not extracted from anything.

I parsed the PAR2 `FileDesc` packets read-only and matched them by 16 KiB MD5
plus exact size:

| On disk | PAR2 name (exact 16k-MD5 + size match) |
|---|---|
| `fcmhz09zn9fdjjsbadvlni` | `…S01E01.Rookies…​.mkv` |
| `io3c42nnuavua67xnczu` | `…S01E02.Olympians…​.mkv` |
| `1qb2p7xytozx6jr8w69cf` | `…S01E03.Hunter…​.mkv` |
| `l1vyg983kql2nanyhp2bd` | `…S01E04.Rose…​.mkv` |
| `z6w9a01kgafjge1mvc6blh` | `…S01E05.Ill.Believe.in.Anything…​.mkv` |
| `05eqsv1tvhc8q5estjl2vm9` | `…S01E06.The.Cottage…​.mkv` |

**Causal chain (`9d3288b` `par_rename_owned`):**

1. Step 1 renames the magic-detected PAR2 index to `*.par2`. That hit D1
   (`EINVAL`, logged at 21:49:49), and `safe_rename` swallowed it.
2. `par2::load_sets` only looks at `.par2` files, so it found no set and made
   no renames.
3. Deobfuscation's renames also hit D1.
4. SUCCESS.

The evidence was there the whole time, and the stage that should use it is
the existing pre-extraction one.

**Consequences for §13.6:**

- **D1 with error propagation fixes this case.** Additionally, make
  `load_sets` recognise PAR2 by magic, so name recovery never depends on first
  renaming the index.
- **A post-extraction PAR2 pass** is a reasonable separate improvement, for
  archives that contain PAR2 and media, but it's not this incident.
- **Magic-based extension recovery** (EBML, `ftyp`, RIFF, TS sync) is a
  sound fallback when PAR2 is absent.
- **ffprobe** isn't needed for any observed case. Embedded titles are weak
  naming evidence and conflict-prone. Treat it as optional and later.
- **Remove the lexical season-pack numbering.** On this exact folder,
  lexical order of the random names is `05eq, 1qb2, fcmh, io3c, l1vy, z6w9`,
  i.e. E06, E03, E01, E02, E04, E05. `<job> - 01..06` would have labelled
  **0 of 6** episodes correctly. Today only D1 stopped it from doing so.

**Live data:** 1703, 1699, 1700, and 1702 sit in completed under obfuscated
names, recorded as SUCCESS. 1703 also logged `job imported`. All of them are
recoverable from evidence on disk (PAR2 and signatures). Nothing was changed.

## 4. Job 1704 now, and gaps in the in-progress patch

**Current failure.** 1704 is a different post from 1691. Every volume has a
**different random stem with its real extension**: `73c2…​.rar`, `0cfc…​.r00`,
`ccd9…​.r01`, and so on.

1. `split_volume_ext` correctly refuses to touch `.rNN`, so `rar_rename` does
   nothing.
2. unrar opens `73c2…​.rar` and looks for `73c2…​.r00`, which doesn't exist,
   so it under-delivers.
3. The 7-Zip retry calls `rename_exclusive(dest, dest.first-attempt)` on a
   **directory**, which gives `EINVAL`.
4. PP crashes, and the job is held with `cause=unknown`,
   `retry_policy=review`, `status=paused`. Neither `Resume` nor
   `ReleaseResourceHold` can clear that. **1704 is stuck now.**

**Patch review (snapshot 22:42; still in flux):**

| Area | Assessment |
|---|---|
| `fs::rename_exclusive` `EINVAL`→`linkat` fallback for regular files | Right primitive, verified on both mounts. **Gaps:** (a) directories still fail, which covers `tools.rs:447` (today's 1704 crash), relocation, recovery, and dir-level transforms; use §2.2. (b) A crash between link and unlink leaves two names, and with errors now propagated a rerun fails on `EEXIST`. Add "target is the same inode as the source → just unlink the source". (c) It probes per call with no capability cache, which is fine, but log once per mount. |
| `safe_rename` no longer pre-skips existing targets; errors propagate | Right direction ("a failed required rename must not report success"). Define `EEXIST`: same inode means already done; a different file fails closed. Test duplicate posts, where the par2 entry matches one file and another copy already holds the name. |
| `rar4_volume_number` (walks headers, checks CRC, reads ENDARC `EARC_VOLNUMBER`) | Header offsets and flags check out against the RAR 2.x–4.x format. The 1691/1692/1695/1696 copies all report Volume Index from ENDARC, so this would have ordered them. `EARC_VOLNUMBER` is optional, so failing closed when it's absent is right. **Gap:** it only applies to *unknown-extension* RARs. 1704's known-extension, mismatched-stem set is untouched. When headers prove one contiguous set, normalise the stems while keeping each verified `.rar`/`.rNN` position. |
| Whole-set validation before any rename | Good. Note it now fails a directory that contains two unrelated hidden archives, which is acceptable but should be tested. |
| `deobfuscate` returns `Result` | Good. Pair it with removing the lexical season-pack numbering (§3). |
| PP crash → hold `cause=unknown, retry_policy=review` | Needs an operator transition. Give it either a retry-PP action or a `pp_failure` outcome. As it stands it's a non-resumable state, the same class as §13.3. |

## 5. Order (revised)

1. **D1 in the primitive**: link for files, the §2.2 mkdir+link publish for
   directories, and a fresh-dest extraction retry. PP rename errors propagate,
   `EEXIST` semantics are defined, and PAR2 is detected by magic.
2. **D4 with the `allocation` cause** in `ReleaseResourceHold`, plus a way out
   of the `unknown`/`review` PP-crash hold (1704 needs one).
3. **RAR ordering**: the new RAR4 parser, plus stem normalisation for
   known-extension sets.
4. **Generation-scoped deletion**, the §2.3 legacy rule, and relocation
   abandon.
5. **Remove the season-pack lexical numbering.** Add extension recovery by
   signature.
6. **UI**, then `inter_dir`.

## 6. Unresolved

- What advanced the epoch at about 21:39. A job-level resume is not logged at
  INFO. Logging owner control actions would close this kind of gap.
- Whether to re-run PP (or apply a manual PAR2-evidence rename) for 1699,
  1700, 1702, and 1703 once fixed. That's Paul's call; nothing was mutated.