# Mobile queue parity — review of the port plan

**Status:** approved with four required changes · **Reviews:**
[MOBILE_QUEUE_PARITY_PLAN.md](MOBILE_QUEUE_PARITY_PLAN.md) · **Written:**
2026-09-27 · **Verified against:** `origin/main` at
`5b683d6a1f29101f13b7c8f853e6346693963d22`

Companion to [MOBILE_QUEUE_PARITY_PLAN.md](MOBILE_QUEUE_PARITY_PLAN.md) (the
plan) — this is *what the reviewer checked, what was wrong, and what is now
decided*. Every claim below was read out of the source at `5b683d6`, not
inferred from the plan; file and line references are to that commit.
Apply §2 to the plan before implementing, and copy §4 into the plan's §8.

## 1. What the plan gets right

The finding in plan §1 is accurate in every particular checked. `a4ef575`
touched no file under `mobile/`; `f13d2a5` is six lines of build metadata
across four files; `mobile/src/queueSections.ts` was last changed in
`bc30259` (2026-08-28) and still classifies on `status` and `stages` alone.

The wire contract in plan §3.1 matches `crates/nzbd-types/src/lib.rs`:
`TorrentPhase` has exactly the ten variants listed, `TorrentStopReason`
includes `storage_full`, `SeedPolicy` is `{stop_on_complete, ratio_limit,
time_limit_secs}`, and `JobSummary` in `crates/nzbd-engine/src/snapshot.rs`
carries all five optional fields (`torrent_phase`, `torrent_control_intent`,
`seed_policy`, `seed_stop_reason`, `torrent_error`) at lines 58–62.

The precedence in plan §3.2 is a faithful port of the web `torrentPhase`
(`crates/nzbd-api/ui/index.html:1640`), with the optimistic overlay
correctly removed. The section order and collapse capabilities in plan §3.3
match the web `SECTIONS` array (line 1585). The collapsed summaries in plan
§4.1 match the web `sectionModel` (line 2665) to the word.

The bug named in plan §4.2 is real: `DashboardScreen.tsx:580` reads
`const postProcessing = !['downloading', 'fetching', 'waiting'].includes(sectionKey)`,
so any new section key is rendered as post-processing with a stage timer.

`PUT /api/v1/jobs/{id}/torrent/seed-policy` (`crates/nzbd-api/src/lib.rs:833`)
is `#[serde(deny_unknown_fields)]` over exactly the four fields the plan
sends; it returns 422 for `ratio_limit ≤ 0` or non-finite and for
`time_limit_secs == 0`, 404 for a non-torrent job, 409 when the owner
refuses the save, and 503 when BitTorrent is disabled or the queue owner is
unavailable. The plan's client-side validation is a superset, which is
correct.

The DOM harness exports `torrentPhase`, `torrentStatus`, `sectionOf`,
`seedLimitReached`, `seedPolicyText`, `seedStopText`, and `SECTIONS`
(line 5312), so the cross-client fixture in plan §6 is feasible without an
adapter. `react-test-renderer` is already in `mobile/package-lock.json` via
`jest-expo`; the "narrow tooling" caveat in plan §7.4 most likely means
adding `@testing-library/react-native` only.

SecureStore is the app's existing preference store
(`mobile/src/storage/themePreference.ts`), so plan §4.1's choice is
consistent with precedent; three short keys are far under the keychain
value limit.

## 2. Required changes — all in plan §5

The action table in plan §5 was written against the web UI's buttons rather
than against the server's `resume` handler, and the two disagree. The
handler is `QueueCommand::Resume` in `crates/nzbd-engine/src/owner.rs`,
lines 1462–1510. Two facts about it drive every change below:

- It acts only when `job.status == JobStatus::Paused` (line 1481).
  Any other status returns `Ok(false)`, which the API maps to **404**.
- Before that, if the torrent is `ready` and
  `torrent_runtime::seed_policy_reached` is true, it replies `false`
  (line 1466–1475) — also **404**.

### 2.1 Failed torrents have no recovery action

`BackendFact::Failed` (`torrent_runtime.rs:585`) sets `phase = Failed` and
`job.status = JobStatus::Failed`. `resume` therefore 404s. There is no
recheck, retry, or re-announce action on `/api/v1/jobs/{id}/actions/*` —
the accepted set is `pause · resume · delete · delete-files · move-*`
(`lib.rs:1224`).

**Change:** the Failed row becomes *remove only* (keep files / delete
files, with the existing confirmation). No lifecycle button. Show the
`torrent_error` text so the user knows why.

**Record as a web defect, do not port:** the web row shows a "resume"
button for a failed torrent (the `(over || st) === "paused"` branch at
`index.html:1877` falls through to "pause", which also 404s). Fix it in the
same PR or file it; either way the fixture names Failed as *no action* on
both clients.

### 2.2 Missing-files recovery is `resume`, and it deserves its own label

`StopReason::MissingContent` (`torrent_runtime.rs:520`) sets
`phase = MissingFiles` and `job.status = Paused`. `resume` then clears
`ready_at_unix` and `content_path`, sets `phase = Queued`, and the torrent
re-downloads (owner.rs:1488–1496). The test
`torrent_missing_file_recovery_is_not_blocked_by_a_completed_seed_policy`
proves the policy check is bypassed for this case because `ready` is false.

**Change:** Missing files → primary action **"Re-download missing files"**,
sending `resume`. Not a generic "Resume" — the user is authorising a full
re-download of a job they thought was finished, and the label must say so.

### 2.3 The policy-satisfied refusal is a 404 the client must disambiguate

When `ready && seed_policy_reached`, `resume` returns the same
`not_found()` as a job that no longer exists. The plan's client-side
`seedLimitReached` mirror is the first defence, but it can disagree with the
server at the threshold:

- The server tests `uploaded_bytes >= selected_bytes * limit` on integers
  (`torrent_runtime.rs:46–48`); the client only has the float
  `ratio = uploaded / selected` (owner.rs:4011). At an exact threshold the
  two can differ by one ulp.
- Seed accounting checkpoints every 30 s or 8 MiB (`seed_checkpoint_due`),
  so the snapshot the client classified from may lag the value the server
  compares.

**Change:** specify that a 404 from `resume` on a job the client believes
is `ready` means *refused, not gone*: re-fetch the job, and if it is still
present, open Seeding options with a one-line explanation. Never remove the
row or show "job not found" on this path. Add the case to the §6 Actions
row and to the fixture.

### 2.4 Storage hold: the plan's behaviour diverges from the web — name it

During a hold (`StopReason::StorageFull`, `torrent_runtime.rs:497–513`):
`phase = PausedDownload`, `last_error = "storage full"`,
`stop_reason = StorageFull`, `job.status = Paused`, and `control_intent`
stays `Running`. Because status is `Paused`, the web shows **"resume"**.
Sending it succeeds — status → `Queued`, phase → `Queued` — but
`last_error` is not cleared and the torrent will hit ENOSPC again on its
next write. Recovery is automatic:
`release_torrents_after_storage_recovery` (owner.rs:3290) releases every
held torrent when the volume comes back.

The plan's "explain automatic disk recovery; do not offer resume" is the
better behaviour. But plan §6 requires intentional native differences to be
named in fixture expectations, and this one is not named.

**Change (pick one, record which):**

- Fix the web in the same PR so the storage-hold row hides pause/resume and
  shows the explanation, keeping the fixture symmetric. *Recommended.*
- Or keep the web as is and name `storage_hold → no action (native) /
  resume (web)` as an intentional difference in the fixture.

## 3. Smaller corrections

**Pause on fetching.** The web hides the pause button for any fetching job
(`pauseHidden: fetching`, `index.html:1875`). Plan §5's "queued /
downloading / fetching / checking: preserve pause" should either match that
or name the difference in the fixture.

**Fixture placement.** `scripts/ci-scope` sets `mobile=true` for
`crates/nzbd-api/*` and `crates/nzbd-types/*`, and `torrent=true` for
`crates/nzbd-types/*` but *not* for `crates/nzbd-api/ui/*`. Put the fixture
under `crates/nzbd-types/fixtures/` and all three lanes fire without editing
`ci-scope`. `JobSummary` is `Serialize`-only, so a Rust test cannot
round-trip the fixture, but one that serialises constructed summaries and
diffs against the file would make it a three-way check for one afternoon's
work.

**Storage-full sentinel.** Plan §3.2 rule 4 matches `torrent_error ===
'storage full'`. Server-side this is `STORAGE_FULL_ERROR` in
`torrent_runtime.rs:19`, compared by value in owner.rs:3296 and the web.
The fixture must carry the literal so a rename breaks a test rather than
silently reclassifying every hold as `paused_download`.

**Ratio semantics.** Plan §5 says "ratio is at least the non-null ratio
limit". Server-side the ratio is `uploaded / selected_bytes` and is `0.0`
when `selected_bytes == 0`; the server also never reports a limit reached
when `selected_bytes == 0`. The client mirror should short-circuit the same
way so a zero-byte selection is never shown as policy-satisfied.

**Working-tree state.** The checkout the plan was written from has a
modified `README.md` and five untracked docs including the plan itself
(`git status` at `f959cf9`, 2026-09-27). Plan §7.1's "preserve the
existing checkout's documentation edits" is load-bearing.

## 4. Decisions — copy into plan §8

- [x] **Precedence:** confirmed as written in §3.2, with §2 above applied to
  actions: Failed → remove only; Missing files → `resume` labelled
  "Re-download missing files"; Storage hold → no lifecycle action, with the
  web either fixed to match or the divergence named in the fixture.
- [x] **Per-job policy editor in scope:** confirmed. Without it a
  policy-satisfied Completed row has no honest button — "Start seeding"
  would 404 (§2.3).
- [x] **Device-wide collapse preferences, all-expanded default, SecureStore:**
  confirmed; matches `themePreference.ts`.
- [x] **No NZB-only timers/health/priority on torrent groups:** confirmed as
  a hard requirement. `DashboardScreen.tsx:580` is replaced with a positive
  NZB post-processing predicate, not extended. Completed-vs-History wording
  in plan §2 is adequate.
- [x] **Shared fixtures catch the original omission:** confirmed, subject to
  the `crates/nzbd-types/fixtures/` placement so both lanes trigger.
- [x] **Separate iOS and Android evidence before closing the gap:**
  confirmed. Build number is allocated from then-current main at release
  time, not assumed to be 10.

## 5. What this review did not do

No application code was changed. No phone or simulator was inspected; the
findings are from source and `git log` at `5b683d6`. The plan's claim that
both native apps render from one React Native bundle and never embed the
daemon HTML was not independently checked — it is consistent with
`mobile/` having no WebView dependency in `package.json`, and nothing here
depends on it.