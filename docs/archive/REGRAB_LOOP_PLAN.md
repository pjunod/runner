# The re-grab loop — diagnosis and fix plan

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [PIPELINE_CONTRACT.md](../PIPELINE_CONTRACT.md) §2.2 (typed reasons and `blames_release` decide the re-grab).

**Status:** nzbd F1–F3 complete; monarr and operator work remains ·
**Diagnosed:** live on nuc3 2026-07-31 against build `0.2.0+unknown`
(built 2026-07-29 18:49 UTC, includes `2453665`) · **Written:** 2026-07-31 ·
**Updated:** 2026-08-05 · **Spans:** nzbd (§6) · monarr (§7) · the nuc3 deploy
(§8)

Companion to [ARCHITECTURE.md](../ARCHITECTURE.md) (how post-processing and the
owner loop are built) and sibling of
[DEFECT_HISTORY_DELETE.md](DEFECT_HISTORY_DELETE.md) — this is *why the
completed dir filled with a terabyte of duplicate downloads, and the fixes,
in order*.

How to work this doc: read §1–§4 once before touching anything — the four
defects feed each other, and a fix that ignores the loop shape will fix a
symptom. Then execute milestone by milestone: the operator steps in §8
come first (the volume is full *right now*; code fixes deployed onto a full
disk will appear not to work), then nzbd F1→F2→F3 (§6), then monarr M1–M4
(§7). Every `file:line` anchor below was read at `2453665` — re-verify each
against HEAD before editing; this repo moves under parallel sessions. If a
fix seems to require changing the compat wire surface, the event contract
(ARCHITECTURE §10.1), or the history JSONL format, stop and flag it instead.

---

## 1. What the operator saw — and what each thing actually was

Field report 2026-07-31: `/working/monarr/completed` near a terabyte
"again". Five releases of True Lies (~336 GB). Two complete BCS S04 season
packs (95 G + 96 G) while monarr lists the second half of S04 missing. A
complete Bates Motel S01 pack (125 G) while monarr lists the second half of
S01 missing. "monarr is not cleaning up after itself, and it's not
processing content correctly."

Every item resolves to a different station of one loop:

| Observed | Actually |
|---|---|
| 5 × True Lies dirs | Five consecutive *failed* grabs (4 × `PAR_FAILURE`, 1 × health-abort), each corpse left on disk. The 6th grab succeeded and was imported. |
| 2 × BCS S04 packs | Grab 1 (FraMeSToR) `PAR_FAILURE` — corpse. Grab 2 (EPSiLON) SUCCESS, half-imported (§4.4). |
| Bates S01 on disk, episodes missing | SUCCESS, picked up by monarr, import died halfway (§4.4). |
| "monarr not cleaning up" | Mostly nzbd: failed jobs' files are never removed (§4.2). monarr also never cleans after import (§7). |
| "not processing content correctly" | nzbd fails every repairable download (§4.1); monarr half-imports and goes silent (§4.4). |

The first-half/second-half pattern on the season packs: the first halves
came from earlier single-episode grabs. The packs were grabbed to fill the
*missing* second halves, completed, and were never (BCS-FraMeSToR case) or
only partly (EPSiLON, Bates) imported — so exactly the episodes the packs
were for stayed missing, which is what keeps monarr searching.

## 2. The loop

```
            ┌──────────────────────────────────────────────────────┐
            │                                                      │
            ▼                                                      │
   monarr grabs a release ──▶ nzbd downloads (46 MiB/s)            │
                                     │                             │
                          1–4 articles fail (0.001–0.003%)         │
                          — provider gaps, or ENOSPC once          │
                            the volume filled (D3)                 │
                                     │                             │
                                     ▼                             │
                     verify: NeedMoreBlocks { 1..4 }               │
                     unpause_par_blocks() → freed == 0   ◀── D1    │
                     "nothing left to fetch" → PAR_FAILURE         │
                     (4–5 GB of recovery data sat paused,          │
                      unreachable by name-parse)                   │
                                     │                             │
                                     ▼                             │
                     ~90 GB corpse stays in completed/   ◀── D2    │
                     volume fills → writes start failing ◀── D3    │
                     disk_low stays false → intake continues       │
                                     │                             │
                                     ▼                             │
                     monarr sees Failed (correctly re-grabs) ──────┘
                     imports of *successful* packs die on
                     the same full disk, halfway          ◀── D4
```

Measured cost while the loop ran: **725 GB downloaded on 2026-07-31 alone,
1.82 TB since the 2026-07-29 boot**, nearly all of it burned. ~523 GB of
failure debris in the last 100 history rows.

## 3. The evidence

All read from the daemon's own API and log on nuc3 — nothing here is
inferred from monarr's behavior alone.

### 3.1 How it was probed (recipe for re-verification)

The dashboard page's renderer is too busy for CDP `Runtime.evaluate`
(45 s timeouts). Navigate the browser tab to a *JSON endpoint* —
`http://192.168.4.7:6789/api/v1/history?limit=100` — and run `fetch()`
aggregations from that page instead; it works every time. Two tool traps:
the desktop `Control_Chrome` bridge answers "Chrome is not running" from
`execute_javascript` while `list_tabs`/`open_url` work (use the
claude-in-chrome extension instead), and the extension's secret scrubber
eats log lines shaped `key=value` (`[BLOCKED: Cookie/query string data]`) —
`.replace(/=/g,' ⟹ ')` before returning them. `record.original_name`
comes back `[BLOCKED: Base64 encoded data]`; harmless.

Useful endpoints: `/api/v1/status` (version, `disk_low`, rates),
`/api/v1/history?limit=N`, `/api/v1/logs?limit=400`,
`/api/v1/logs?job=N&limit=150` (per-job log survives the global ring).

### 3.2 Repair never succeeds — the correlation

Last 100 history rows: 76 SUCCESS · 10 PAR_FAILURE · 12 FAILURE/HEALTH ·
2 DELETED. **Every** row with `failed_articles ≥ 1 ended PAR_FAILURE; every
SUCCESS has `failed_articles == 0`.** Repair has never worked on this
install. The recent big ones:

| job | release | failed / total articles | par2 on hand | outcome |
|---|---|---|---|---|
| 196 | BCS.S04 FraMeSToR (111 G) | 1 / 150,100 | 5.3 GB, 11 files | PAR_FAILURE |
| 210 | True.Lies TRiToN (86 G) | 2 / 116,245 | 4.1 GB | PAR_FAILURE |
| 212 | True.Lies playBD (86 G) | 3 / 116,090 | 4.1 GB | PAR_FAILURE |
| 213 | True.Lies Top10UK (86 G) | 4 / 115,968 | 4.1 GB | PAR_FAILURE |
| 214 | True.Lies HDS (101 G) | 2 / 137,130 | 4.8 GB | PAR_FAILURE |
| 211 | True.Lies RetailSub | 113,669 / 115,989 | — | FAILURE/HEALTH (correct — dead on provider; health-abort at 2.1% worked) |

~5% par redundancy was present and paused (the article deficit between
`success_articles` and `total_articles` matches `par_size` on every row —
deferred-par is active and the vols were never fetched).

### 3.3 The smoking gun — job 196, healthy disk, July 29

Per-job log (`/api/v1/logs?job=196`), three consecutive lines:

```
20:36:02  requesting delayed par blocks  job=196 blocks_needed=1 round=0
20:36:03  job ... status=Failed
20:36:04  post-processing finished  job=196 outcome="PAR_FAILURE"
```

One recovery block needed, 5.3 GB of recovery data in the queue, failed
one second after asking. Note what is *absent*: the
`"delayed par files unpaused"` INFO line (`owner.rs:2069`) never fired —
so `unpause_par_blocks` returned 0 and `repair_loop` took its
`freed == 0 → return Ok(false)` exit. The disk was healthy on July 29;
D1 predates and is independent of the full volume.

### 3.4 The full volume nobody noticed

Log shapes from the 26-minute global window (2026-07-31, counts as seen):

```
finalize failed job=232 … error=No space left on device (os error 28)
writer error; failing file … error=write …part05.rar.part: no storage space   ×2
job failed: the download completed but the files did not … could not write …  ×2
post-processing crashed job=230 error=io: No space left on device             ×4
unpack failed … password_error=false                                          (238, 239, 225)
unpack failed; forcing par repair + retry job=226
unrar did not deliver the whole archive; retrying with 7-Zip                  ×7
free-space probe (statvfs) on the destination is slow — volume saturated?
    the disk-low guard reacts up to one probe late  ms=3641                   ×42
```

Meanwhile `/api/v1/status` reported `disk_low: false` and intake continued
at wire speed. The prober's own warning fired 42 times naming the
saturation, but its *answer* still cleared the threshold — either the
threshold is inadequate or statvfs on this mount reports free space that
is not allocatable (gluster quota is the prime suspect — §9 Q1). Either
way: the daemon observed dozens of authoritative ENOSPC errors and no
component treated them as a disk-space signal (§4.3).

### 3.5 What is healthy (verified, so nobody re-litigates it)

The `2453665` naming fix is live: dirs get the `.jobid` uniquifier
(`… · a60904ae` vs `… · a60904ae.239`), no shared directories in any recent
row. monarr 0.13.0 talks the native API, sends `monarr-transfer:t-…`
add-time params (N4) on **100/100** rows, and set `picked_up_by` on
**100/100** rows (`hidden` stays false — it acknowledges without hiding).
The rename-breaks-matching hypothesis is dead; the handoff protocol works.
Placeholder names (`monarr/0.13.0 · drunkenslug · <8 chars>`) survive on
~20/100 rows — those are fully obfuscated NZBs with no evidence anywhere,
which is the fix behaving as designed (show what the client sent rather
than invent), though §6 F1's block-size work touches the same par2 index
data that could name some of them later.

## 4. The defects

### 4.1 D1 — repair asks for par blocks it can never receive (nzbd)

`crates/nzbd-post/src/manager.rs` (`repair_loop`, ~line 1130 at `2453665`;
re-verify):

```rust
VerifyResult::NeedMoreBlocks { blocks_needed } => {
    tracing::info!(job = job_id.0, blocks_needed, round,
        "requesting delayed par blocks");
    let freed = engine.unpause_par_blocks(job_id, blocks_needed)
        .await.unwrap_or(0);
    if freed == 0 {
        return Ok(false); // nothing left to fetch
    }
    if !wait_par_files(engine, job_id, cfg.par_fetch_timeout).await {
        return Ok(false);
    }
}
```

The wait machinery below it is sound (250 ms poll, finalized-check,
`par_fetch_timeout_secs` default 600) — it is simply never reached, because
`unpause_par_blocks` (`crates/nzbd-engine/src/owner.rs:2040`) filters
candidates through a *filename parse*:

```rust
let candidates: Vec<(FileId, u32)> = job.files.iter()
    .filter(|f| f.paused && f.is_par2)
    .filter_map(|f| vol_par_blocks(&f.filename).map(|b| (f.id, b)))
    .collect();
if candidates.is_empty() { return 0; }
```

`vol_par_blocks` (`crates/nzbd-engine/src/queue.rs:1095`) requires a
literal `.volXX+NN.` marker in the filename. Obfuscated posts — which is
everything this indexer serves — name their recovery volumes
`<32-hex-hash>.par2` / `<hash>.part01.par2` with no vol marker. Every
paused vol is filtered out, `freed == 0`, PAR_FAILURE. **The failure is
silent**: the "nothing left to fetch" branch logs nothing, so the operator
sees a repair that "ran and failed", not a repair that never started.

Note the asymmetry that proves the fix is possible: *admission* correctly
paused these same hash-named vols (the article deficit in §3.2 shows it),
so admission's par-classification does not depend on the vol marker. Find
that criterion and reuse it on the unpause side (§9 Q4 pins the open
detail; likely-relevant: the par2 index the native verifier already parsed
knows the set's block size, and `NeedMoreBlocks` could carry it).

### 4.2 D2 — a failed job's bytes are forever (nzbd)

There is no disposition step for the files of a job that ends
`PAR_FAILURE`, `FAILURE/UNPACK`, or (with `health_action = "none"`, the
default) `FAILURE/HEALTH`. The dir — under the *category destination*,
i.e. the tree monarr watches — just stays. Debris found in the last 100
rows alone (see §8 R2 for the actionable list): five True Lies corpses
(~347 GB), BCS-FraMeSToR (111 GB), the July-29 shared-dir relic
`monarr_0.11.0 · drunkenslug · monarr_0` (18 GB), three ~9 GB
placeholder-named corpses, plus health-abort partials ≈ **523 GB**.

Machinery that already exists and should be generalized rather than
duplicated (`crates/nzbd-post/src/manager.rs:33` `HealthAction
{None, Park, Delete}`, applied at `manager.rs:322` for the health gate
only; `delete_job(job_id, delete_files)` in `owner.rs`; the M3 parked-NZB
spool, which preserves requeue-ability at zero disk cost — deleting a
failure's files loses nothing that `requeue` can't get back).

### 4.3 D3 — ENOSPC is not a disk-low signal (nzbd)

The disk-low guard trusts one source: the cached statvfs prober
(ARCHITECTURE §8.5, the 2026-07-26 starvation fix). On this mount the
probe is slow (3.6 s, warned 42×) and its answer disagreed with reality
for hours: writers, finalize, and PP all received ENOSPC while
`disk_low: false` kept intake at wire speed. Whatever the statvfs lie
turns out to be (§9 Q1), the design lesson stands on its own: **an
observed ENOSPC from the write path is ground truth and must flip the
guard immediately; statvfs is only the forecast.** Every failed write
today became a failed article, which D1 turned into a failed job, which
D2 turned into more debris on the full volume.

### 4.4 D4 — monarr: half-imports, no cleanup, no dedup (monarr)

nzbd's history for BCS-S04-EPSiLON (job 205) and Bates-S01 (job 216):
SUCCESS, `failed_articles = 0`, 21 files each (11 par2 + 10 plain-mkv
payload — season remux packs, nothing to unpack), picked up by
monarr 0.13.0. The content was complete on disk; monarr imported the first
half of each and stopped — almost certainly mid-copy ENOSPC on the same
full volume (unconfirmed until monarr's log is read, §9 Q2) — recorded no
visible failure, never retried, and left both the completed dir *and* its
own "missing" state. Also observed: jobs 238/239 are the *same NZB*
grabbed 6 minutes apart (`a60904ae` and `a60904ae.239`) — monarr's
retry/search has no already-in-flight dedup. And nothing ever removes a
completed dir after a *successful* import, which is the other half of the
terabyte. These are monarr-repo work items (§7); nzbd's contract surface
for them already exists (`picked_up_by`, `history delete` with
`delete files`, the N1 `job_pp_finished` event carrying `final_dir`).

## 5. Guardrails — what NOT to do here

- **Do not touch the compat wire surface or the event contract** (golden
  tests, ARCHITECTURE §10.1). Nothing in this plan needs it.
- **Do not build the native Rust par2 *repair* engine** (STATUS.md phase 5
  item). F1 is plumbing around the existing `Par2Tool` subprocess boundary;
  the GF(2^16) work is explicitly out of scope.
- **Do not auto-delete SUCCESS dirs from nzbd.** Post-import cleanup is
  monarr's call (M2) — nzbd deleting content an importer may still be
  copying is how you corrupt an import. A retention feature for
  picked-up-and-aged entries is a separate future decision.
- **Do not weaken the resolved history-delete semantics in passing.**
  [DEFECT_HISTORY_DELETE.md](DEFECT_HISTORY_DELETE.md) records the decision:
  forget means everywhere, and portable tombstones must continue to beat
  stale peer entries and compaction.
- **Do not sweep pre-existing debris from boot repair.** The software
  prevents recurrence (F2); the one-time cleanup is an operator step (R2)
  because only the operator knows which corpses are still wanted for
  forensics.
- **Do not weaken health-abort** (job 211 shows it working). F2 changes
  what happens to the *files*, not the verdict.

## 6. nzbd milestones

Work them in order; each ends with its acceptance check. Standing rules:
STATUS.md updates in every feature commit; `cargo fmt` + clippy clean;
tests accompany every behavior change; commit messages in the repo style.

### F1 — repair fetches its blocks (fixes D1)

Make `unpause_par_blocks` able to price hash-named vols. Preferred design:
`vol_par_blocks` stays the fast path; when it yields nothing for a paused
par2 file, fall back to a size-derived estimate
`max(1, file_size / block_size)` with `block_size` carried in
`VerifyResult::NeedMoreBlocks` (the native verifier just parsed the par2
index; it knows). If block size is unavailable, last resort: unpause the
smallest paused par2 file and let the next round escalate. Requirements:

- `freed == 0` while paused par2 files exist must WARN, naming the job,
  the count of paused vols, and why each was unpriceable — this branch
  was silent through months of failures.
- Rounds bound *re-requests* (the `0..8` loop), never patience: the wait
  is `par_fetch_timeout_secs` (default 600) per round, already correct.
- Confirm at build time how admission classifies/pauses vols for
  obfuscated posts (§9 Q4) and reuse its criterion rather than inventing
  a parallel one; confirm whether `par_rename` updates queue
  `FileEntry.filename` (if it someday does, the fast path starts working
  mid-job — the fallback must not double-count).

Tests: unit — `vol_par_blocks` fallback pricing on hash-named vols;
`pick_par_files` unchanged. e2e (nserv) — an *obfuscated* job with damaged
articles and paused hash-named vols: unpauses, waits, repairs, ends
SUCCESS; the per-job log shows request → unpaused → repair. Guard test —
the silent branch now warns. Acceptance: the §3.3 sequence is impossible
to reproduce — a damaged job with paused vols either repairs or fails
*loudly* with the reason on the row.

### F2 — terminal failure disposes of its files (fixes D2)

One knob: `[post] failure_action = none | park | delete` (serde alias
`health_action` for back-compat, applied to ALL terminal failure
outcomes — PAR_FAILURE, unpack failure, health abort, post crash).
Recommended default: **`delete`** — the bytes are known-bad, the parked
NZB (M3 spool) keeps `requeue` working, and a default of `none` is how
this terabyte happened. `park` moves the job dir to
`<main_dir>/failed/<dir>` (off the category tree monarr watches;
cross-device falls back to the N6 copy+fsync+rename pattern). Flag the
default choice to Paul in the PR if unsure. The one-dir-one-job invariant
(`2453665`) is what makes deleting/moving the whole dir safe.

Tests: e2e — PAR_FAILURE job with `delete` leaves no dir and history says
so; `park` moves the tree intact; `requeue` of a deleted failure
re-downloads; SUCCESS dirs untouched; a part-written shared-dir job (the
`success_articles > 0` boot-repair carve-out) is never swept. Acceptance:
run the F1 e2e with repair forced to fail — the corpse is gone, the row
says where the files went.

### F3 — observed ENOSPC latches the disk-low guard (fixes D3)

Any `ENOSPC`/`EDQUOT` reaching the writer, finalize, or PP paths (the fsx
layer already stamps op+path on these) reports to the engine; the guard
latches `disk_low = true` immediately — same intake pause as the statvfs
threshold — with the UI banner naming the volume and "observed from a
write" as the source. The latch clears only when a fresh statvfs shows
free ≥ 2× threshold (hysteresis, so a quota-lying mount doesn't flap) or
on operator resume. Status DTO grows an `ENOSPC observed` counter so the
dashboard can say why intake stopped. statvfs stays as the forecast;
keep its slow-probe warning.

Tests: unit — latch state machine (set on error, hysteresis clear,
operator override). e2e — inject ENOSPC through the fsx seam mid-download:
intake pauses within one tick, banner present, resume-after-space works.
Acceptance: with F3 deployed, §3.4 cannot recur — the first failed write
stops the fleet, instead of the 725 GB/day burn.

## 7. monarr milestones (verify each against monarr's code — it was not
readable from this session)

- **M1 — import survives ENOSPC.** Copy failures abort the import
  *visibly*, mark the release as failed-import (not silently missing),
  and retry when space returns. Never leave "first half imported, second
  half absent, no error anywhere" (§4.4).
- **M2 — clean up after successful import.** Delete the completed dir (or
  call nzbd's history delete-with-files; the API exists) once every file
  is verified imported. This is the other half of the terabyte.
- **M3 — dedup grabs.** Before grabbing, check nzbd's queue/history for
  the same NZB/transfer already in flight (jobs 238/239 were the same URL
  6 minutes apart). Add backoff — five grabs of one movie in 10 hours
  should have tripped an alarm.
- **M4 — surface the pipeline.** monarr had `picked_up_by` on all 100
  rows and still showed nothing about five consecutive failures of the
  same movie. A per-release history of grab → nzbd outcome → import
  result would have named this loop weeks earlier.
- *(Optional)* **M5 — phase 2 of INTEGRATION_PLAN.md**: subscribe to
  `job_pp_finished` instead of polling. Not required by this fix — the
  poll seam demonstrably works.

## 8. nuc3 runbook — do this before deploying any code

- **R1** Pause the queue (UI, or `POST /api/v1/pause`— verify route name).
- **R2** Reclaim ~530 GB — these are confirmed-failure corpses, safe to
  remove; their NZBs are parked for requeue:

  ```bash
  cd /working/monarr/completed
  rm -rf 'True.Lies.1994.2160p.UHD.Blu-ray.Remux.DV.HEVC.TrueHD.7.1.Atmos-HDS'      # 101G j214
  rm -rf 'True.Lies.1994.2160p.BluRay.UHD.REMUX.DV.HDR.HEVC.Atmos.7.1-Top10UKSingle' # 86G j213
  rm -rf 'True.Lies.1994.2160p.UHD.Remux.HEVC.DoVi.TrueHD.Atmos.7.1-playBD'          # 86G j212
  rm -rf 'True.Lies.1994.2160p.UHD.BluRay.REMUX.DV.HDR.HEVC.Atmos-TRiToN'            # 86G j210
  rm -rf 'True.Lies.1994.BluRay.2160p.DV.HDR.TrueHD.Atmos.AC3.HEVC.NL-RetailSub.REMUX' # 1.6G j211
  rm -rf 'Better.Call.Saul.S04.BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR'       # 111G j196
  rm -rf 'monarr_0.11.0 · drunkenslug · monarr_0'                                    # 18G  j187
  rm -rf 'monarr_0.13.0 · drunkenslug · a60904ae' 'monarr_0.13.0 · drunkenslug · a60904ae.239' 'monarr_0.13.0 · drunkenslug · 39d14552'  # ~9G each
  ```

  Do **NOT** remove `Better.Call.Saul.S04.1080p…-EPSiLON` or
  `Bates.Motel.S01.2160p…-RH` — that is the good content monarr still
  needs to finish importing (R4). Older debris beyond the last 100 rows
  likely exists; anything whose history row is a failure is fair game.
- **R3** Answer §9 Q1 while there: `df -h /working/monarr /processing`,
  and whether a gluster quota is set on the dir
  (`gluster volume quota <vol> list`, if applicable). This decides
  whether F3's statvfs distrust is quota-specific or general.
- **R4** With space free: trigger monarr to rescan/re-import the EPSiLON
  and Bates dirs (second halves should import — confirms Q2), then let
  monarr (or you) remove those two dirs.
- **R5** Redeploy on current `main`, with build identity. The deploy
  checkout is stale — that is why `make docker-build` "doesn't exist"
  there (target added in `30e811b`, Makefile line 94) and plausibly why
  the footer says `+unknown` (build-arg never passed):

  ```bash
  cd /opt/noirr_/nzbd && git pull
  make docker-build            # or: NZBD_GIT_DESCRIBE=$(git describe --tags --always --dirty --abbrev=9) docker compose up -d --build
  ```

  Acceptance: the footer reads `0.2.0+g<hash>`, not `+unknown`
  (DEPLOY.md "Which build is this?").
- **R6** Stopgap until F1/F2 ship, optional: set
  `[post] health_action = "delete"` so health-gated failures at least
  stop leaving partials. PAR_FAILURE debris continues until F2.
- **R7** Resume, and watch the next `failed_articles ≥ 1` job: until F1
  ships it will still go PAR_FAILURE (expected); after F1 it must repair.

## 9. Open questions

1. **Why did statvfs clear the threshold while writes got ENOSPC?**
   Suspect gluster quota (brick free ≠ allocatable-under-quota). R3
   answers it. F3 is correct regardless of the answer.
2. **Did monarr's imports die on ENOSPC specifically?** Read monarr's log
   for the EPSiLON/Bates imports. If it was something else (permissions,
   path mapping), M1 changes shape.
3. **What does monarr actually match on?** It demonstrably works (§3.5),
   but the mechanism is undocumented; write it down in the monarr repo
   while in there (M3 needs to know it anyway).
4. **How does admission classify hash-named par2 vols for deferred
   pausing?** It works today (§4.1's asymmetry); F1 must reuse that
   criterion. Locate it in the admission path before writing the
   unpause fallback.
5. **Does `par_rename` update queue `FileEntry.filename`?** Determines
   whether F1's fast path can start winning mid-job (and whether the
   fallback risks double-counting).

## 10. Done when

- A damaged-but-repairable obfuscated download **repairs and imports** on
  nuc3 (the §3.2 correlation breaks: `failed_articles ≥ 1` rows start
  ending SUCCESS).
- A terminal failure leaves **no bytes** in the category tree (or a
  parked tree, per config), and its history row says where the files went.
- Filling the destination volume **pauses intake within one tick** of the
  first failed write, with the banner naming the volume — no more
  wire-speed downloads into a full disk.
- monarr **finishes or visibly fails** every import, cleans up after
  successful ones, and never re-grabs a release it already has in flight.
- `/working/monarr/completed` holds only: content mid-import, and nothing
  older than the importer's cycle. The terabyte does not come back.
