# Held jobs — why a hold can strand a job, and the fix

**Status:** proposed · revised after adversarial review (rev 2) · **Base:**
`a253562` (main, 2026-11-01) · **Written:** 2026-11-01 · **Field report:**
jobs #2181 and #2183 on nuc3

Companion to [DOWNLOAD_INCIDENT_REVIEW.md](DOWNLOAD_INCIDENT_REVIEW.md) and
[FILE_LIFECYCLE_PLAN.md](FILE_LIFECYCLE_PLAN.md), which introduced the job
control record (`*Control:v1`, `nzbd_types::JobControl`). This doc is *why a
held job can have no way out, and how to make that impossible*. Code anchors
are from `a253562`; re-verify them at build time in case they moved.

Read §2 and §3.1 first. The timeline proves the outage was short. The exit
matrix shows the gap is structural, not a one-off.

**Rev 2 (review by Fable, 2026-11-01).** Rev 1 guessed that #2181's retry
schedule was lost to the outage. It is worse: the schedule write has never
worked since it shipped (§3.3). The review also found that the `io` cause
mixes transient and permanent failures, that storage holds have no automatic
exit, and that rev 1's owner-tick release would have put filesystem calls on
the owner loop. §4 is rebuilt around those findings. §6 lists every finding
and where it landed.

## 1. Symptom

Two Usenet jobs in the queue's *Needs attention* section, badge `HELD`, and
the only action `delete`:

| Job | Cause · stage | Message on the row |
|---|---|---|
| #2183 | `io` · `download_write` | `write /processing/…/.runner-file-73916.part: Transport endpoint is not connected (os error 107)` · needs review |
| #2181 | `io` · `post_unpack_rename` | `io: Input/output error (os error 5)` · needs review |

Both carry `retry_policy: "review"`, `retry_attempt: 0`, and
`next_retry_at_unix: null`. Nothing will ever retry them, and nothing the
operator can click will either. The report: "this keeps happening".

## 2. What happened on nuc3

Read from the live daemon (`/api/v1/jobs/{id}`, `/api/v1/history`,
`/api/v1/logs`) on 2026-11-01. Times are UTC.

| Time | Evidence | Meaning |
|---|---|---|
| 15:34:08 | history: a job finishes `SUCCESS` | `/processing` healthy |
| 15:34:37 | #2183 `blocked_since_unix` | writer gets ENOTCONN on `/processing`: the FUSE/network mount lost its transport |
| 15:35:29 | #2181 `blocked_since_unix` | post-processing gets EIO from the same mount |
| 15:40:01 | history: a job finishes `SUCCESS` | `/processing` back |
| 16:27:34 | log ring starts (`state_dir=/processing/queue`) | daemon restarted |

The outage lasted **under six minutes**. A retry at any point after 15:40
would have succeeded. Instead both jobs are stranded until deleted.

## 3. Root cause — a hold's way out depends on who placed it

A hold is a fence. `Owner::hold_job` (`crates/nzbd-engine/src/owner.rs`
~3570) writes `JobControl { lifecycle: "held", cause, stage, retry_policy,
… }` and parks the job. Getting *out* is decided in six separate places,
each covering the causes its author had in mind:

```
 hold_job ── stamps retry_policy ──▶ resume_same_job · reobserve_mount · review
 EngineHandle::resume_job ── capacity/quota/allocation only (API resume)
 PostManager scan ── mount_unavailable; scheduled_retry (never armed, §3.3)
 PostManager Restart ── API post-restart-all, any post-side hold
 boot (Owner::new) ── two holds, matched by exact message string
 UI row / detail / restart menu / mobile ── each hides a different subset
```

Nobody owns the question *"does every hold have an exit?"*, so the answer is
no. The cause is a free string chosen at each call site, so the compiler
cannot help either.

### 3.1 The exit matrix at `a253562`

Every cause the code writes, by where the job was when it stopped. "API"
means a raw HTTP call; nothing in the UI issues it.

| Cause | Side | Policy | Automatic exit | Operator exit |
|---|---|---|---|---|
| `capacity` · `quota` | download | `resume_same_job` | **none** (disk-guard recovery releases torrents only) | row `resume` → probe |
| `allocation` | download | `resume_same_job` | none | row `resume` |
| `password_required` | post | `resume_same_job` | none | row button shown, but `resume` → 409; detail Retry → post retry |
| `mount_unavailable` | post | `reobserve_mount` | every post scan, lifecycle-eligible only | row button shown, but `resume` → 409; detail Retry |
| `io` · `missing_tool` | post | `review` — the `scheduled_retry` upgrade **never lands** (§3.3) | **none** | API `post-restart-all` only |
| **`io`** | **download** | **`review`** | **none** | **none** |
| `identity_conflict` | download | `review` | boot, for one exact message | none |
| `identity_conflict` · `post_failure` | post | `review` | boot, for one exact RAR message | detail post-restart menu (`post_failure` only); API `post-restart-all` |

Every row has a gap. #2183 is the bold row. #2181 is the fifth row.

### 3.2 Defect A — download-side I/O holds were never given a retry

`EngineMsg::WriterError` (owner.rs ~1281–1302) holds every non-ENOSPC writer
error as `hold_job(job, "io", "download_write", &error)`. Three more sites
hold `io` when the queue snapshot cannot be saved: owner.rs ~3561, ~3777
and ~3816. `hold_job` maps `io` to `review`, and nothing else ever touches
it:

- `EngineHandle::resume_job` (`crates/nzbd-engine/src/lib.rs` ~873)
  releases only `allocation`, `capacity` and `quota`. It returns `Ok(false)`
  for everything else, which the API turns into `409 job is held: …`.
- `RetryPostHold` requires a post-side `previous_status`.
- The post manager only handles post-processing.

### 3.3 Defect B — the post-side retry schedule has never worked

For post-processing errors, `manager.rs` (~2318) makes two writes:

```rust
let _ = engine.hold_job(job, cause, &stage, &detail).await;   // write 1: policy "review"
if matches!(cause, "io" | "missing_tool") {
    schedule_recoverable_post_retry(&engine, job).await;      // write 2: "scheduled_retry"
}
```

Write 2 exports the row, sets `retry_policy = "scheduled_retry"` and
`next_retry_at_unix`, and re-imports it with `import_job_if_present`. The
import goes through `Owner::import_job` (~2936), which starts with this:

```rust
if let Some(control) = self.state.job(job.id).and_then(|j| j.control())
    .filter(|c| c.lifecycle == "held") {
    // Remote/exported legacy snapshots cannot erase a newer local fence.
    job.set_control(&control);
```

The job is held, so the owner puts its own `review` control back on the
incoming row. The import then "succeeds" and returns `true`, so the
`recoverable retry schedule was not committed` warning never fires. The
fence guard landed in `3f1efc2` (2026-09-30). The scheduler landed in
`f53cdcb` (2026-10-22), on top of it, and no test asserts the schedule
survives. **Every post-side `io` or `missing_tool` hold since 2026-10-22 has
been stranded.** This is the "keeps happening". It is deterministic, not an
outage race.

### 3.4 Defect C — the cause names where it failed, not whether retrying helps

`io` today means "an I/O-ish error happened here". On the download side it
covers ENOTCONN (fixed by the mount coming back) and also
`complete checkpoint digest differs`, `segment exceeds declared size`, and
`bytes never reached the filesystem` (`crates/nzbd-engine/src/writer.rs`
~95–407), which fail the same way every time. On the post side,
`PostError::Io(_)` covers EIO and also ENOENT, EACCES and SQLite constraint
errors.

Meanwhile a dropped mount reached through any of the 46
`PostError::Subprocess(e.to_string())` sites in `nzbd-post` (custody
renames, `fileops::open`, …) classifies as `post_failure`. A recovery
policy keyed on `io` either strands transient faults (today) or retries
permanent ones forever (rev 1 of this plan). **The cause has to encode
recoverability before any table keyed on it can be right.**

### 3.5 Defect D — `review` is built to mean "no exit"

Each surface turns `review` into a dead end on its own:

- **Engine:** `resume_job` refuses it (§3.2).
- **Queue row:** `pauseHidden: reviewHold || …` (`crates/nzbd-api/ui/index.html`
  ~2105). The DOM harness enforces it: "review holds do not offer a resume
  that cannot work".
- **Job detail:** `lifecycleRetryHidden` requires a non-review policy and
  `lifecycleEligible`. `recoveryMax` returns nothing for a held job, because
  a held job's `status` is `paused` (~2567).
- **Mobile:** `canResume` (`mobile/src/screens/DashboardScreen.tsx` ~620)
  allows only `resume_same_job`.

The reverse failure exists too. The row *shows* its button for
`password_required` and `mount_unavailable` holds, and that button 409s.
The two message-matched boot releases in `Owner::new` (~929, ~952) are what
this design produces: one-off exits for specific symptoms, because there was
no general one.

### 3.6 Defect E — storage holds have no automatic exit

`BeginResourceProbe` → `ReleaseResourceHold` is reached only from
`EngineHandle::resume_job`, which means an operator click. When
`update_disk_guard` sees space return, it releases torrents
(`release_torrents_after_storage_recovery`, ~4588) but no Usenet job. After
a full disk, every job that was writing needs one click each.
`BeginResourceProbe` also stops probing after 8 attempts (`count < 8`,
~2693). A restart with the mount still down turns a released job into an
`allocation` hold (`ensure_allocation`, ~4173), which has no automatic exit
either.

### 3.7 Defect F — messages without a path

`restore_media_extensions_owned` (`crates/nzbd-post/src/deobfuscate.rs`)
has two bare `?` on `io::Error`. One is in `crate::namespace::files(dir)?`
(~264, a readdir). The other is in `.read_to_end(&mut bytes)?` (~280, the
media sniff). Both reach the operator as `io: Input/output error (os error
5)` with no path. This is the `nzbd-post` half of the latent bug class
recorded when `nzbd-state` got its `fsx` layer.

## 4. Proposed fix

One rule: **a hold's cause states whether retrying can help; one function
maps cause to recovery; the hold and its first retry time are one write;
one off-loop task fires every automatic recovery; and every hold answers an
operator retry.**

```
 hold site ── classify(error) ──▶ HoldCause (enum, closed)
                                     │ .policy()
 hold_job ── control{policy, next_attempt_at} ── ONE save_snapshot
                                     │
 HoldRecovery task (off owner loop, authority only, 5 s)
   due + not paused? ─▶ release(job) ─┬─ ok ──────▶ Queued / PostQueued
                                      └─ refused ─▶ RescheduleHold(reason, backoff)
 operator resume / retry ─────────────▶ release(job, Operator) ─ refused ─▶ 409 + reason
```

### 4.1 `HoldCause` — the closed set, enforced by the compiler

Add an enum in `nzbd-types`. The wire value stays a string, and `JobControl`
is unchanged.

```rust
pub enum HoldCause {
    StorageUnavailable,   // the volume stopped answering — retry until it does
    Capacity, Quota,      // out of space / quota — retry when space returns
    Allocation,           // payload directory could not be allocated
    MountUnavailable,     // configured root identity changed — re-observe
    MissingTool,          // extractor/script binary absent — retry
    PasswordRequired,     // needs an operator-supplied password
    Io,                   // an I/O failure retrying will not fix
    IdentityConflict,     // the evidence disagrees with itself
    PostFailure,          // post-processing failed on its input
    Unrecognized(String), // written by another build; never produced here
}
impl HoldCause {
    pub fn as_str(&self) -> &str;
    pub fn parse(s: &str) -> Self;
    pub fn policy(&self) -> HoldPolicy;   // exhaustive match: the one table
}
```

| Cause | Policy | Released by |
|---|---|---|
| `storage_unavailable` · `missing_tool` | `scheduled_retry` | recovery task, on backoff |
| `capacity` · `quota` | `resume_same_job` | recovery task, when the disk guard is clear, via the existing probe |
| `allocation` | `resume_same_job` | recovery task, on backoff |
| `mount_unavailable` | `reobserve_mount` | recovery task, every pass |
| `password_required` · `io` · `identity_conflict` · `post_failure` · unrecognized | `review` | operator only |

`Owner::hold_job`, `EngineHandle::hold_job`, `QueueCommand::HoldJob`, and
`post_hold_cause` all take or return `HoldCause`. Adding a variant fails to
compile until `policy()` handles it, which is the guarantee the operator
asked for ("there's a closed scope of possibilities"), checked by `rustc`
instead of a source-grepping test. `password_required` moves to `review`
because nothing can retry it until a person supplies a password; its old
`resume_same_job` label was what made the row show a button that 409s.

### 4.2 Classify by errno at every hold site

Add `nzbd_engine::is_storage_unavailable(msg)` next to `is_out_of_space`. It
matches only the std `(os error N)` form. That form is what every
`io::Error` Display ends with, and payload names and extractor text do not
plausibly contain it. The errnos:

`5 EIO · 6 ENXIO · 11 EAGAIN · 19 ENODEV · 67 ENOLINK · 103 ECONNABORTED ·
104 ECONNRESET · 107 ENOTCONN · 110 ETIMEDOUT · 111 ECONNREFUSED · 112
EHOSTDOWN · 113 EHOSTUNREACH · 116 ESTALE · 121 EREMOTEIO`

The parenthesised match keeps `(os error 5)` from matching `os error 57`.

- **Writer errors** (`EngineMsg::WriterError`): ENOSPC → `capacity`/`quota`
  (unchanged); storage errno → `storage_unavailable`; anything else → `io`
  (`review`). The checkpoint-digest and declared-size failures stay
  operator-only. They cannot loop a full-file CRC every five minutes.
- **Snapshot-save failures** (the three owner `io` sites): these mean the
  state volume refused a write → `storage_unavailable`.
- **Allocation failures** with a storage errno → `storage_unavailable`, so a
  released job that hits the still-dead mount keeps retrying.
- **Post** (`post_hold_cause`): after the `NothingPublishable` guard and
  ENOSPC, a storage errno → `storage_unavailable` whatever the variant;
  `PostError::Io` otherwise → `io`; the rest unchanged.

### 4.3 The schedule is part of the hold — `Owner::hold_job`

`hold_job` stamps `retry_policy = cause.policy()`. For every automatic
policy, it also stamps `next_retry_at_unix = now + delay(retry_attempt)` and
increments `retry_attempt`, all in the same `JobControl` and the same
`save_snapshot`. `blocked_since_unix` keeps carrying the first failure, so
a long outage keeps its start time. If that save fails, the in-memory hold
with its schedule is still the owner's state (already true: "failed
persistence leaves the local fence in place"), and the next successful
snapshot carries both.

Backoff is `[30, 60, 120, 300]` seconds, capped, never expiring. The
probe's 8-attempt cap in `BeginResourceProbe` goes; its own `probe_after`
backoff stays. A volume that stays dead is retried every five minutes for
as long as it stays held, visibly, and the operator can pause it (§4.6).

Deleted: `schedule_recoverable_post_retry` and both calls, and the post
scan's `scheduled_retry` and `mount_unavailable` branches. The `import_job`
fence guard stays exactly as it is, because it protects held state from
stale snapshots. A regression test asserts that a held control can only be
changed through owner methods.

### 4.4 One release path, owner state only — `Owner::retry_hold`

A new owner method ends a non-storage hold. It does no filesystem work.

```rust
pub(crate) fn retry_hold(&mut self, job: JobId, revision: &str,
                         by: RetryBy /* Operator | Recovery */,
                         custody_checked: bool) -> Result<(), String>
```

- **Refuses, with a reason:** job missing · not held · stale revision ·
  torrent · `PP_DONE` already stamped · writers still stopping ·
  `Recovery` on a manually paused hold · post side without
  `custody_checked` · not the authority (`!self.persist`).
- **Download side:** status → `Queued`, writer handles dropped,
  `after_file_change` per file. This is what `ReleaseResourceHold` already
  does for storage holds. Leased segments were returned to `Pending` by
  `hold_job`; journaled `Done` ranges are kept.
- **Post side:** status → `PostQueued`. The post scan claims it through the
  existing `claim`/`active` guards and runs post-processing from the
  beginning. `validate_post_retry` already refuses while an attempt lease
  exists, so a release during a winding-down attempt cannot double-run.
- **Control:** `revision + 1`, `lifecycle: "running"`, `next_retry_at_unix:
  None`. `Operator` also clears `manual_pause` and resets `retry_attempt`.
  If the save fails, the job is restored and the call errs.

New `QueueCommand::RescheduleHold { job, revision, reason }` moves the
next attempt out on the same backoff, with the refusal appended to the
message. `RetryPostHold` becomes `retry_hold(…, custody_checked: true)` and
loses its cause and stage allow-lists. The stage list already named every
`PostStage`, so it guarded nothing, and an unrecognised cause must not
strand a job. `ReleaseResourceHold` and `BeginResourceProbe` keep their
jobs for storage holds.

### 4.5 One recovery task, off the owner loop — `HoldRecovery`

A tokio task spawned with the engine. It runs only while this engine is the
queue authority (single node, or the cluster leader after
`adopt_authority`). Every 5 s it reads the published snapshot, and for each
held job where `!manual_pause` and the job is due (`next_retry_at_unix <=
now`, or every pass for `reobserve_mount`) it runs:

| Policy | Release |
|---|---|
| `scheduled_retry`, download side | `RetryHold` |
| `scheduled_retry` / `reobserve_mount`, post side | `spawn_blocking(validate_post_retry)`, then `RetryPostHold` with the revision as CAS |
| `resume_same_job`: `capacity` · `quota` | only while the disk guard is clear: the existing `resume_job` probe (`capacity::health` in `spawn_blocking`) → `ReleaseResourceHold` |
| `resume_same_job`: `allocation` | `ReleaseResourceHold`; the next lease re-allocates |

Any refusal becomes `RescheduleHold(reason)`. The custody check and the
capacity probe both touch the payload mount, and both run in
`spawn_blocking` here, never in `on_command`. That keeps the engine's rule
that a wedged FUSE/NFS mount cannot freeze the owner loop (lib.rs ~330),
which `RetryPostHold` breaks today by validating inside the owner. The post
manager keeps doing post-processing and stops releasing holds.

Lifecycle eligibility no longer gates automatic recovery. The post-side
custody check is the safety gate, and a legacy row that can never pass it
says so in its message every five minutes instead of sitting silently.
*Decision taken without the operator; flagged for review.*

### 4.6 Every hold has an operator exit, and every automatic one a stop

- `EngineHandle::resume_job` on a held job runs the same release the task
  would, as `Operator`, with no due-time and no pause check. A refusal
  returns `EngineError::Lifecycle(reason)`, and the API maps that to `409`
  with the reason (today `503`, as if the service were down). Compat
  `GroupResume` already goes through `resume_job`.
- **Queue row.** One button, now honest:
  - automatic hold, not paused → **pause** (stops the retries; sets
    `manual_pause`)
  - review hold, or paused → **retry** (`resume`)

  The tooltip says a cause that is still present holds the job again, with
  the reason. A scheduled hold shows `next retry in …` alongside the
  30-minute warning. `· needs review` appears only for `review`.
- **Detail.** *Retry now* shows for every hold. *Pause* is reachable
  through the row.
- **Mobile.** The same two states on the job card.
- **Pre-hold manual pause.** An operator retry runs the job, even if it was
  paused before the fault. Clicking *retry* is the intent to run.
  `ReleaseResourceHold`'s automatic path keeps restoring `Paused`.

A `review` hold still never retries *by itself*, because unchanged input
fails the same way. Retrying an `identity_conflict` hold re-fetches the
conflicting segments into the job's own `.part` (`writer_for` resolves
through this job's artifact), and that **overwrites the retained evidence**.
The tooltip says so.

### 4.7 Re-arm on boot *and* on authority adoption

`fn rearm_holds(state: &mut QueueState, now)` runs in `Owner::new` and in
`adopt_authority`. Cluster engines start with `persist_queue: false`, so
`Owner::new`'s recovery never runs for them. For each held control:

1. **Reclassify by evidence:** an `io` hold whose stored message carries a
   storage errno becomes `storage_unavailable`. #2183 (`os error 107`) and
   #2181 (`os error 5`) both qualify.
2. **Re-derive the policy** from `HoldCause::policy()`. If the hold is
   automatic but has no time, it is due `now`. Revision `+ 1`.

**#2181 and #2183 recover on the first recovery pass after deploy, with no
operator action.** The two message-matched boot releases are deleted. One
re-queues an `identity_conflict` finalize hold, which is now an operator
retry. The other retries one RAR message, which is now an operator retry as
well.

### 4.8 Move holds keep their abandon path

A `move` failure with a storage errno becomes `storage_unavailable`. While
a relocation is `running|review|queued|retry`, `validate_post_retry`
refuses, so the hold keeps rescheduling, and the abandon gate
(`AbandonRelocation`, ~2549) admits only `unknown|post_failure`. Extend that
gate to every cause at stage `move`. Otherwise reclassification would make
a move hold unabandonable.

### 4.9 Paths in messages

Both bare `?` in `restore_media_extensions_owned` map to
`io::Error::new(kind, "<op> <path>: <err>")`. Kind and errno are kept, so
classification is unaffected. #2181's message would have read `io: read
/processing/…/<file>: Input/output error (os error 5)`.

### 4.10 Tests

| Test | Proves |
|---|---|
| `every_hold_cause_has_an_exit` | for every `HoldCause` variant, download side and post side: the hold stamps `policy()`, automatic policies carry a time, and an operator release reaches `Queued` / `PostQueued` (storage causes via their probe) |
| `held_control_survives_only_owner_writes` | an `import_job_if_present` carrying a different control does not change a held job's control (#3.3 stays a non-bug), and `RescheduleHold` does |
| `storage_errno_writer_error_schedules_itself` | #2183: ENOTCONN → `storage_unavailable`, `next_retry_at` durable in the same snapshot, released when due, a repeat backs off to 60 s, `blocked_since` kept |
| `permanent_writer_error_waits_for_review` | `complete checkpoint digest differs` → `io` · `review`, never released automatically |
| `recovery_task_releases_due_holds_off_loop` | download and post side release; custody refusal reschedules with its reason; a paused hold is skipped; a follower (`!persist`) releases nothing |
| `capacity_hold_released_when_space_returns` | disk guard clears → the probe runs → `Queued`, with no click |
| `rearm_reclassifies_by_errno_on_boot_and_adoption` | #2181 exactly, through both `Owner::new` and `adopt_authority` |
| `is_storage_unavailable` | every listed errno in, `13`, `28`, `57`, `50` out |
| API | `resume` on `identity_conflict` → 200; a refusal → 409 with its reason |
| DOM harness + mobile jest | automatic hold → `pause`; review or paused hold → `retry`; no `needs review` on scheduled holds |

## 5. Non-goals

- **No queue-wide "storage unavailable" latch.** Every job on a dropped
  mount holds and retries individually. A queue-level pause tied to mount
  health, like the ENOSPC latch, is better UX. It is a separate change
  because it needs a probe that can tell "mount back" from "mount point now
  shows the empty underlying directory".
- **No wire change.** `JobControl` keeps its fields and its string cause.
  Old snapshots re-arm (§4.7).
- **No automatic retry of `review` holds.**
- **Torrents are excluded.** They have their own control intents and actions.
- **The 46 `Subprocess(e.to_string())` sites stay.** §4.2 classifies them
  correctly. Rewriting them as typed errors is the `nzbd-post` half of the
  fsx migration, and is separate.

## 6. Review findings and where they landed

| # | Finding (Fable, rev 1) | Verdict | Landed |
|---|---|---|---|
| 1 | `import_job` restores the held control, so the post schedule never lands | confirmed | §3.3 root cause; §4.3 one write; test |
| 2 | `io` mixes transient and permanent; rev 1 would retry permanent failures forever | confirmed | §3.4; §4.1–4.2 `storage_unavailable` vs `io` |
| 3 | rev 1's owner tick would run custody I/O on the owner loop | confirmed | §4.5 off-loop task; `RetryPostHold` stops validating in-loop |
| 4 | capacity/quota holds have no automatic exit; probe gives up at 8 | confirmed | §3.6; §4.5 task row; cap removed |
| 5 | reclassified move holds become unabandonable | confirmed | §4.8 |
| 6 | allocation hold after restart breaks the retry chain | confirmed | §4.2 errno; §4.5 allocation row |
| 7 | rev 1's matrix missed API `post-restart-all`; the row button 409s for two causes | confirmed | §3.1 corrected; §4.1 `password_required` → review |
| 8 | there is no UI way to pause a held job | confirmed | §4.6 pause/retry states |
| 9 | boot re-arm skipped on cluster adoption | confirmed | §4.7 shared `rearm_holds`; `!persist` refuses |
| 10 | operator retry drops a pre-hold manual pause | kept, with the reason | §4.6 |
| 11 | errno list incomplete; free-text matching can be steered | accepted | §4.2: `(os error N)` only, list extended |
| 12 | missed hold sites; the source scan can't see variable causes | confirmed | §3.2 sites; §4.1 enum replaces the scan |
| 13 | identity_conflict retry overwrites retained evidence | confirmed | §4.6, stated in the UI |

## 7. Acceptance

- Gates: `cargo test --workspace` · `cargo clippy --workspace --all-targets
  -- -D warnings` · `cargo +1.85 check --workspace --all-targets` (MSRV) ·
  both node UI harnesses · mobile `npm run typecheck && npm test`.
- After deploy to nuc3, within one recovery pass (≤ 10 s) of boot: #2181
  and #2183 show `cause: storage_unavailable`, then `lifecycle: running` and
  progress. If `/processing` is unhealthy, each instead carries a fresh
  `next retry in …` and the refusal reason, and **never** a new `allocation`
  or `review` hold. `/api/v1/logs` shows `hold released` lines for both.
- Next real mount blip: held rows read `next retry in 30s`, a `pause`
  button is present, and they clear without a click.
