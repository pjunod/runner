# Held jobs — why a hold can strand a job, and the fix

**Status:** proposed, under review · **Base:** `a253562` (main, 2026-11-01) ·
**Written:** 2026-11-01 · **Field report:** jobs #2181 and #2183 on nuc3

Companion to [DOWNLOAD_INCIDENT_REVIEW.md](DOWNLOAD_INCIDENT_REVIEW.md) and
[FILE_LIFECYCLE_PLAN.md](FILE_LIFECYCLE_PLAN.md), which introduced the job
control record (`*Control:v1`, `nzbd_types::JobControl`). This doc is *why a
held job can have no way out, and how to make that impossible*. Code anchors
are from `a253562`; re-verify them at build time in case they moved.

Read §2 and §3.1 first: the timeline proves the outage was short, and the
exit matrix shows the gap is structural, not a one-off.

## 1. Symptom

Two Usenet jobs in the queue's *Needs attention* section, badge `HELD`,
the only action `delete`:

| Job | Cause · stage | Message on the row |
|---|---|---|
| #2183 | `io` · `download_write` | `write /processing/…/.runner-file-73916.part: Transport endpoint is not connected (os error 107)` · needs review |
| #2181 | `io` · `post_unpack_rename` | `io: Input/output error (os error 5)` · needs review |

Both carry `retry_policy: "review"`, `retry_attempt: 0`,
`next_retry_at_unix: null`. Nothing will ever retry them, and nothing the
operator can click will either. The report: "this keeps happening".

## 2. What happened on nuc3

Read from the live daemon (`/api/v1/jobs/{id}`, `/api/v1/history`,
`/api/v1/logs`) on 2026-11-01. Times are UTC.

| Time | Evidence | Meaning |
|---|---|---|
| 15:34:08 | history: a job finishes `SUCCESS` | `/processing` healthy |
| 15:34:37 | #2183 `blocked_since_unix` | writer gets ENOTCONN on `/processing` — the FUSE/network mount lost its transport |
| 15:35:29 | #2181 `blocked_since_unix` | post-processing gets EIO from the same mount |
| 15:40:01 | history: a job finishes `SUCCESS` | `/processing` back |
| 16:27:34 | log ring starts (`resolved data directories state_dir=/processing/queue`) | daemon restarted; pre-restart log lines are gone |

The outage lasted **under six minutes**. A retry at any point after 15:40
would have succeeded. Instead both jobs are stranded until deleted.

`/processing` is also the **state volume** (`state_dir=/processing/queue`).
That matters for #2181 (§3.3).

## 3. Root cause — a hold's way out depends on who placed it

A hold is a fence: `Owner::hold_job` (`crates/nzbd-engine/src/owner.rs`)
writes `JobControl { lifecycle: "held", cause, stage, retry_policy, … }`
and parks the job. Getting *out* is decided in five separate places, each
handling the causes its author had in mind:

```
 hold_job ── stamps retry_policy ──▶ resume_same_job · reobserve_mount · review
                                         │                │              │
 EngineHandle::resume_job ── capacity/quota/allocation only ──┘           │
 PostManager scan ── mount_unavailable; scheduled_retry (2nd write) ──┘    │
 boot (Owner::new) ── two holds, matched by exact message string           │
 UI row button ── hidden when retry_policy is review ◀────────────────────┘
 UI detail Retry ── only lifecycle-eligible + non-review policy
 UI post-restart menu ── only post_failure/unknown at post stages
```

Nobody owns the question *"does every hold have an exit?"*, so the answer
is no.

### 3.1 The exit matrix at `a253562`

Every cause the code writes, by where the job was when it stopped:

| Cause | Side | Policy stamped | Automatic exit | Operator exit |
|---|---|---|---|---|
| `capacity` · `quota` | either | `resume_same_job` | disk guard + resource probe | row `resume` |
| `allocation` | download | `resume_same_job` | — | row `resume` |
| `password_required` | post | `resume_same_job` | — | detail Retry (lifecycle-eligible only) |
| `mount_unavailable` | post | `reobserve_mount` | every post scan | detail Retry |
| `io` · `missing_tool` | post | `review`, then `scheduled_retry` **if a second write lands** | post scan, lifecycle-eligible only | detail Retry, only once scheduled |
| **`io`** | **download** | **`review`** | **none** | **none** |
| `identity_conflict` | download | `review` | boot, for one exact message | none |
| `identity_conflict` | post | `review` | none | none |
| `post_failure` · `unknown` | post | `review` | boot, for one exact RAR message | post-restart menu |

Four rows have no exit at all. #2183 is the bold row. #2181 is the fifth
row with its second write lost.

### 3.2 Defect A — download-side I/O holds were never given a retry

`EngineMsg::WriterError` (`owner.rs` ~1302) holds every non-ENOSPC write
error as `hold_job(job, "io", "download_write", &error)`. `hold_job` maps
`io` to `review`. Nothing else ever touches it:

- `EngineHandle::resume_job` (`crates/nzbd-engine/src/lib.rs` ~873)
  releases only `allocation` / `capacity` / `quota` and returns `Ok(false)`
  for everything else, which the API turns into
  `409 job is held: …`.
- `RetryPostHold` requires a post-side `previous_status` and a post stage;
  `download_write` is neither.
- The post manager's scheduled retry only runs for post-processing errors.

A dropped mount mid-download is the most ordinary failure a network
download volume has, and it produces a hold with no way out.

### 3.3 Defect B — the post-side retry schedule is a separate write

For post-processing errors, `manager.rs` (~2318) does two things in
sequence:

```rust
let _ = engine.hold_job(job, cause, &stage, &detail).await;   // write 1: policy "review"
if matches!(cause, "io" | "missing_tool") {
    schedule_recoverable_post_retry(&engine, job).await;      // write 2: policy "scheduled_retry"
}
```

Write 2 re-exports the row, sets `retry_policy = "scheduled_retry"`, and
re-imports it with `import_job_if_present`. If write 2 fails or never runs,
write 1 stands and the job is in `review` forever. Write 2 is lost in exactly
the situation that causes the hold: the error is I/O on `/processing`, and
`/processing` holds the state snapshot. It is also skipped outright for jobs
that are not `lifecycle_eligible()`.

#2181 shows the signature: `cause: "io"` (only this path writes `io` at
`post_unpack_rename`), `lifecycle_eligible: true`, and still
`retry_policy: "review"`, `retry_attempt: 0`. The code always calls write 2
for that combination, so write 2 did not take. **Inferred, not proven:** the
pre-restart log that would show `recoverable retry schedule was not
committed` was lost in the 16:27 restart. The defect stands either way —
a recovery that needs a second durable write after a durability failure is
not a recovery.

Nothing re-arms the hold after a restart: boot only re-examines two holds,
matched by exact message string (§3.5).

### 3.4 Defect C — `review` is built to mean "no exit"

Each surface independently turns `review` into a dead end:

- **Engine:** `resume_job` refuses it (§3.2).
- **Queue row:** `pauseHidden: reviewHold || …` in `rowModel`
  (`crates/nzbd-api/ui/index.html` ~2105), and the DOM harness enforces it:
  `"review holds do not offer a resume that cannot work"`.
- **Job detail:** `lifecycleRetryHidden` requires a non-review policy *and*
  `lifecycleEligible`.
- **Post-restart menu:** `recoveryMax` offers it only for `post_failure` /
  `unknown`. `RetryPostHold` excludes `identity_conflict`.
- **Mobile:** `canResume` in `mobile/src/screens/DashboardScreen.tsx` (~620)
  allows only `resume_same_job`.

The intent behind `review` was "a person should look before Runner tries
again". In practice it became "a person may only delete".

### 3.5 Defect D — the cause loses the errno on the way in

`post_hold_cause` (`manager.rs` ~4413) classifies by error *variant*.
Forty-six call sites in `nzbd-post` turn inventory and filesystem errors
into `PostError::Subprocess(e.to_string())`. That includes `rename_owned`'s
custody rename and the `fileops::open` in
`deobfuscate::restore_media_extensions_owned`. An EIO or ENOTCONN there
classifies as `post_failure` → `review`, not `io`. The same outage produces
different holds depending on which call hit it first.

The two boot special cases in `Owner::new` (~929, ~952) are the symptom of
this design. Each releases one specific hold, matched on its exact message
text, because there was no general exit to fall back on.

### 3.6 Defect E — #2181's message names no path

`restore_media_extensions_owned` (`crates/nzbd-post/src/deobfuscate.rs`
~280) sniffs each extensionless file with
`.take(4096).read_to_end(&mut bytes)?`. The bare `?` converts the
`io::Error` through `PostError::Io(#[from])`, so the operator sees
`io: Input/output error (os error 5)` with no file. This is the latent bug
class recorded when `nzbd-state` got its `fsx` layer: `nzbd-post` still
has `Io(#[from] std::io::Error)`.

## 4. Proposed fix

One rule: **the cause decides the recovery, in one table, written in the
same commit as the hold, and every hold also answers an operator retry.**

```
 hold_job(cause) ──▶ hold_retry_policy(cause) ──▶ policy + schedule, ONE write
        │
        ├─ scheduled_retry ── owner tick: due? ──▶ retry_hold(Schedule)
        ├─ reobserve_mount ── post scan ─────────▶ retry_hold(PostManager)
        ├─ resume_same_job ── resource probe ────▶ ReleaseResourceHold (unchanged)
        └─ review ────────── (nothing automatic)
                                         operator resume ──▶ retry_hold(Operator)
 retry_hold: download side → Queued · post side → custody check → PostQueued
```

### 4.1 One policy table — `nzbd_types::hold_retry_policy`

```rust
pub fn hold_retry_policy(cause: &str) -> &'static str {
    match cause {
        "capacity" | "quota" | "allocation" | "password_required" => "resume_same_job",
        "mount_unavailable" => "reobserve_mount",
        "io" | "missing_tool" => "scheduled_retry",
        _ => "review",
    }
}
pub const HOLD_RETRY_DELAYS_SECONDS: [i64; 4] = [30, 60, 120, 300];
```

These are the policies the code already intends: today's `hold_job` stamps
the first two arms, and the post manager adds the third. The table only
makes it one place. An unknown cause falls to `review`, which (§4.5) still
has an operator exit. The backoff is today's `POST_RETRY_DELAYS_SECONDS`,
moved so both sides share it: capped at five minutes, never expiring. A
mount that stays dead is retried every five minutes for as long as it is
held, visibly.

### 4.2 The retry schedule is part of the hold — `Owner::hold_job`

`hold_job` stamps `retry_policy` from the table. For `scheduled_retry` it
also sets `next_retry_at_unix = now + delay(prior.retry_attempt)` and
increments `retry_attempt`, in the same `JobControl` and the same
`save_snapshot`. `blocked_since_unix` keeps carrying the first failure, so
a long outage keeps its start time and the row's 30-minute warning still
fires.

If that one save fails, the in-memory hold *with its schedule* is still the
owner's state (unchanged behaviour: "failed persistence leaves the local
fence in place"), and the next successful snapshot carries both.

Removed: `schedule_recoverable_post_retry`, its two call sites, and the
post scan's `scheduled_retry` branch. Nothing else needs to follow up.

### 4.3 One release path — `Owner::retry_hold`

A new owner method, the only code that ends a non-storage hold:

```rust
pub(crate) fn retry_hold(&mut self, job: JobId, revision: Option<&str>,
                         by: RetryBy /* Operator | Schedule | PostManager */,
                         post_only: bool) -> Result<(), String>
```

- **Refuses, with a reason:** job missing · not held · stale revision ·
  `capacity`/`quota`/`allocation` (those keep their probe path) · torrent ·
  `PP_DONE` already stamped · writers still stopping · `Schedule` on a
  manually paused hold.
- **Download side** (`previous_status` not `Post`/`PostQueued`/`Completed`):
  status → `Queued`, the job's writer handles are dropped, and
  `after_file_change` runs per file. This is exactly what
  `ReleaseResourceHold` already does for capacity holds. `hold_job` already
  returned leased segments to `Pending`; journaled `Done` ranges are kept.
- **Post side:** `artifacts.validate_post_retry(job)` must pass, then status
  → `PostQueued`. The post scan claims it and runs post-processing from the
  beginning, as every held post retry does today. A refused validation on
  a `Schedule` retry reschedules on the same backoff, with the refusal
  appended to the message.
- Control: `revision + 1`, `lifecycle: "running"`, `next_retry_at_unix:
  None`, `manual_pause: false`. `Operator` also resets `retry_attempt`.
  If the save fails, the job is restored and the call errs.

`QueueCommand::RetryPostHold` becomes `retry_hold(…, PostManager, true)`.
That drops its cause and stage allow-lists: a held post retry always
restarts from the beginning, so the stage named in the hold never made a
retry unsafe, and an unrecognised stage must not strand a job. New
`QueueCommand::RetryHold` is the operator entry. `ReleaseResourceHold` and
`BeginResourceProbe` are unchanged.

### 4.4 The owner fires due retries — `Owner::release_due_retries`

Every 5th owner tick, the owner scans for `held` + `scheduled_retry` +
`!manual_pause` + `next_retry_at_unix <= now` and calls
`retry_hold(Schedule)`. Any refusal the retry did not already reschedule
(writers stopping, state volume still down) is rescheduled one step out.
Owner-local means a single writer owns both the schedule and its release,
so there is no gap for a second write to fall into. It applies to legacy
rows as well as lifecycle-eligible ones; the custody check is the safety
gate on the post side.

### 4.5 Every hold has an operator exit

- `EngineHandle::resume_job`: `allocation` and `capacity`/`quota` keep
  their existing paths. Every other hold goes to `RetryHold`, and a refusal
  returns `EngineError::Lifecycle(reason)`.
- API `POST /api/v1/jobs/{id}/actions/resume`: `Lifecycle(reason)` → `409`
  with the reason. Today it is `503`, as if the service were down.
- Web UI queue row: the button is never hidden for a hold. It reads
  `retry`, posts `resume`, and has a tooltip saying a cause that is still
  present holds the job again. `reviewHold` (and the `· needs review`
  suffix) excludes `scheduled_retry`, which now shows `next retry in …` even
  past the 30-minute warning.
- Detail Retry (`ManagerCommand::RetryNow`): calls `engine.retry_hold`, and
  spawns post-processing only for a post-side hold.
- Mobile: `canResume` accepts any hold, and the label reads Retry.

A `review` hold still never retries *by itself*: identity conflicts and
failed unpacks fail the same way on unchanged input. What changes is that a
person can always say "try again", and if the cause is still there, the hold
comes back with the reason.

### 4.6 Classify by errno — `nzbd_engine::is_storage_unavailable`

Next to `is_out_of_space`, and in the same style (errno text, because the
error has crossed a `to_string()` by then):
`(os error 5|6|19|107|112|116|121)`, plus "transport endpoint is not
connected" · "stale file handle" · "input/output error". `post_hold_cause`
checks it after ENOSPC and before the variant checks. A dropped mount
becomes `io` (auto retry) whichever wrapper the error came through. That
fixes §3.5 without touching the forty-six call sites. The parenthesised
match keeps `(os error 5)` from matching `os error 57`.

### 4.7 Boot re-arms holds already on disk

In `Owner::new`, after the existing per-job recovery, each held control goes
through `rearmed_hold(control, now)`. If its policy disagrees with the
table, or it is `scheduled_retry` with no time, it gets the table's policy,
`next_retry_at_unix = now`, and `revision + 1`. **#2181 and #2183 recover on
the first owner tick after deploy, with no operator action.** The two
message-matched boot releases stay; they are now redundant with the
operator exit, and removing them is a separate change.

### 4.8 Op and path on the deobfuscate read

The sniffing read maps its error to
`io::Error::new(kind, "read <path>: <err>")`, so #2181's message becomes
`io: read /processing/…/<file>: Input/output error (os error 5)`. The
kind is kept, so classification is unaffected.

### 4.9 The closed set, enforced by tests

| Test | Proves |
|---|---|
| `every_hold_cause_has_an_exit_on_both_sides` | for every known cause plus an unknown one, download side and post side: the hold stamps the table's policy, and an operator retry returns it to `Queued` / `PostQueued` (storage causes excepted, covered by their probe tests) |
| `hold_causes_in_source_are_the_known_set` | scans every `hold_job(` call in `crates/*/src` and fails on a cause literal not in the exit test's list — a new cause cannot ship without an exit |
| `io_hold_on_download_schedules_its_own_retry_and_the_owner_fires_it` | #2183 end to end: the schedule is durable *with* the hold, the owner releases it when due, a repeat failure backs off to 60 s, and `blocked_since` is kept |
| `paused_scheduled_hold_waits_for_the_operator` | the schedule never overrides a manual pause; an operator retry clears it and resets the backoff |
| `boot_rearms_holds_whose_policy_their_cause_does_not_earn` | #2181 exactly: `io` + `review` → `scheduled_retry` due now; correct holds untouched |
| `storage_unavailable_matches_dropped_mounts_and_nothing_else` | the errno list, including `os error 57` and `13` staying out |
| `dropped_mount_errors_hold_as_transient_io_through_any_wrapper` | `Subprocess(… os error 107)` and `Io(… os error 5)` both classify `io` |
| API `resume_retries_a_review_hold_and_missing_job_is_not_found` | `resume` on an `identity_conflict` hold → 200 and released; a refused resume → 409 with a reason |
| DOM harness | held rows show `retry` for every policy; a scheduled hold shows `next retry in` and no `needs review` |

The existing `post_hold_retry_checks_revision_custody_and_stage_then_persists`
flips one assertion: an unrecognised stage now retries instead of refusing
(§4.3).

## 5. Non-goals

- **No global "storage unavailable" latch.** When `/processing` drops,
  every active job holds individually and retries on its own backoff. A
  queue-wide pause tied to mount health, like the ENOSPC latch, would be
  better UX. It is a separate change, because it needs a probe that can tell
  "mount back" from "mount point now shows the empty underlying directory"
  (`mount_unavailable` already solves that for post-processing only).
- **Storage holds are untouched.** `capacity`/`quota`/`allocation` keep
  `BeginResourceProbe` → `ReleaseResourceHold`. They work, and the probe
  does real admission work `retry_hold` should not duplicate.
- **No wire change.** `JobControl` keeps its fields. Only the values a hold
  carries change, and old snapshots re-arm on boot (§4.7).
- **No automatic retry of `review` holds.** Only the operator path changes.
- **Torrents excluded.** They have their own control intents and actions.
- **The forty-six `Subprocess(e.to_string())` sites stay.** §4.6 makes them
  classify correctly. Rewriting them to typed errors is the `nzbd-post` half
  of the fsx migration, and it is worth doing, but separately.

## 6. Risks and open questions

1. **Retrying a download-side `identity_conflict`.** The three sources:
   foreign journal bytes failing range validation, unsafe yEnc metadata,
   and conflicting declared size. Retry re-queues pending segments. The
   offending bytes were never marked `Done`, so they are fetched again and
   overwrite in place; a conflict that persists re-holds. Reviewer: confirm
   no path writes into another job's payload on that re-fetch.
2. **Permanent EIO retries every five minutes forever.** A real dying
   disk keeps a job cycling at the cap. It is visible (`retry N`, `blocked
   for …`, the 30-minute warning) and the operator can pause or delete. The
   alternative, giving up after N tries, is how jobs get stranded in the
   first place.
3. **`retry_attempt` never resets on success.** A job that recovers and
   later hits an unrelated I/O hold starts at the capped delay. Cosmetic;
   only an operator retry resets it.
4. **Legacy (non-lifecycle-eligible) rows now auto-retry.** The post side
   still requires `validate_post_retry`. If it never passes for legacy
   rows, they reschedule every five minutes with the refusal in the
   message, instead of sitting silently in `review`.
5. **Cluster mode.** The release is owner-local. A post-side release only
   sets `PostQueued`; which node runs it is still decided by the post gate's
   `claim`. Reviewer: confirm a follower's owner cannot release a hold the
   leader owns.
6. **Two retry triggers for `mount_unavailable`.** The post scan's
   re-observe calls `retry_hold(PostManager)`. `mount_unavailable` is not
   `scheduled_retry`, so the owner tick never touches it. One trigger per
   policy.

## 7. Acceptance

- `cargo test --workspace` green; `cargo clippy --workspace --all-targets
  -- -D warnings` clean; `cargo +1.85 check --workspace --all-targets`
  (MSRV); both node UI harnesses green; mobile `npm run typecheck && npm
  test`.
- After deploy to nuc3: within ~10 s of boot, #2181 and #2183 show
  `lifecycle: "running"` (or a fresh `scheduled_retry` with a reason, if
  `/processing` is still unhealthy). `/api/v1/logs` shows `hold released for
  retry` for both.
- Manual: `docker exec nzbd` cannot drop a FUSE mount, so the field check
  is the next real blip. Expected: held rows read `next retry in 30s`, they
  clear without a click, and any held row shows a working `retry` button.
