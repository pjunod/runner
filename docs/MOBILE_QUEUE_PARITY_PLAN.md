# Mobile queue parity — carry torrent grouping into iOS and Android

**Status:** Fable approved with corrections incorporated; not implemented ·
**Written:** 2026-09-27  
**Reference:** locally available `origin/main` at
`5b683d6a1f29101f13b7c8f853e6346693963d22` (mobile build 9 metadata).

Companion to [MOBILE.md](MOBILE.md) (native app operation),
[Fable's review](MOBILE_QUEUE_PARITY_REVIEW.md) (findings and decisions), and
[the web queue delivery record](https://github.com/pjunod/nzbd/blob/a4ef575/docs/TORRENT_QUEUE_STATUS.md)
(the behavior being ported). Read §3 before changing rendering, then execute
the milestones in §7. Re-verify the referenced source against the implementation
base: the working checkout is on `f959cf9`, before the web queue changes.
If implementation requires changing daemon lifecycle semantics or deleting
payloads differently, flag that as a separate change instead of folding it
into this port. This document is the review deliverable; no application code
has been changed for it.

## 1. Finding — the native implementation stopped before the web grouping work

Both native apps run the same React Native code. They do not display the
daemon's HTML UI, so updating that UI cannot update native grouping.

| Evidence | Consequence |
|---|---|
| `d5d7040`, 2026-09-19, added mobile torrent intake, controls, and basic metrics | Mobile knows that a job is a torrent, but still groups it using the NZB status model |
| `a4ef575`, 2026-09-20, added phase-aware web grouping and seed controls | The commit changed the web UI and its tests, with no mobile implementation changes |
| The web delivery record explicitly scoped out native redesign | The omission was a scope decision that left a user-visible parity gap |
| [Native grouping](../mobile/src/queueSections.ts) last changed in `bc30259`, 2026-08-28 | It reads `status` and `stages`, never `torrent_phase` or durable control intent |
| `f13d2a5`, 2026-09-26, changed build numbers only | Build 9 could not acquire grouping code that was never ported |
| [Grouping tests](../mobile/__tests__/queue-sections-test.ts) cover the old status model | Passing tests do not establish torrent lifecycle parity |

The mobile CI scope already includes API and type changes. The prevention gap
is missing behavior assertions, not simply a missing CI trigger. These findings
come from source and commit inspection, not from inspecting an installed phone.

## 2. Outcome — the same job has the same activity on all three clients

On iOS, iPadOS, and Android, separate torrent metadata fetching, file checking,
seeding, and stopped completed torrents from the download waiting list. Keep
the existing NZB post-processing groups and ordering. Allow Seeding, Completed,
and Waiting to collapse, with counts and aggregate metrics visible when shut.

Cards must explain what the torrent is doing. A seed shows upload activity and
readiness; a storage hold explains the disk problem; a completed stopped seed
offers the appropriate next action. Neither a stale generic status nor a full
download bar may hide a failure or suggest that seeding is downloading.

**Included:** section classification, collapse persistence, card status and
metrics, compatible pause/resume labels, and a small per-torrent seeding-policy
editor. The editor is included because the web's policy-limited Completed rows
offer “Seeding options” rather than an ineffective restart button.
The same PR will correct the web's failed-torrent and storage-hold actions,
label missing-file recovery explicitly, and handle ambiguous seed-resume 404s
on both clients. These are bounded parity fixes to the existing controls.

**Outside this port:** global/category settings editors, new admission options,
History redesign, queue virtualization, TV apps, daemon changes, and changes to
removal semantics. Those do not determine queue grouping. Preserve the existing
keep-files versus delete-files confirmation flow. Completed is a group in the
live queue; it does not move jobs into History or assert library import.

## 3. Contract — derive activity from authoritative lifecycle fields

### 3.1 Extend the native snapshot types without breaking older daemons

Re-verify the wire definitions in
[nzbd-types](https://github.com/pjunod/nzbd/blob/5b683d6/crates/nzbd-types/src/lib.rs)
and the summary projection before implementing these additions to
[mobile API types](../mobile/src/api/types.ts):

```typescript
type TorrentPhase =
  | 'fetching_source' | 'fetching_metadata' | 'queued' | 'checking'
  | 'downloading' | 'seeding' | 'paused_download' | 'paused_seed'
  | 'missing_files' | 'failed';

interface SeedPolicy {
  stop_on_complete: boolean;
  ratio_limit: number | null;
  time_limit_secs: number | null;
}

// Add to JobSummary; absent/null values are valid for NZBs and older daemons.
// torrent_phase?: TorrentPhase | null;
// torrent_control_intent?: 'running' | 'paused' | null;
// seed_policy?: SeedPolicy | null;
// seed_stop_reason?: 'manual' | 'download_complete' | 'ratio_limit'
//   | 'time_limit' | 'storage_full' | null;
// torrent_error?: string | null;
```

The existing `ready`, `ready_at_unix`, upload counters, ratio, peer count, and
seed duration remain the source of their corresponding display values.
`storage_hold` is a derived UI state, not a new server enum. JSON parsing does
not enforce TypeScript unions; an unfamiliar phase must remain visible in
Waiting with “Torrent state unavailable” if no higher-precedence rule applies.
Do not infer verified completion from byte counts, percentage, or `pp_done`.

### 3.2 Use one phase resolver for grouping, labels, and controls

Introduce a pure mobile helper, proposed as
`mobile/src/torrentPresentation.ts`, and use it from both grouping and cards.
Port the precedence in the web's `torrentPhase` and `sectionOf` functions from
[the inspected web source](https://github.com/pjunod/nzbd/blob/5b683d6/crates/nzbd-api/ui/index.html).
The native hook has no optimistic status overlay; preserve that model.

Evaluate these rules in order:

1. A job whose `kind` is not `torrent` uses the existing NZB classifier.
2. Generic `status === 'failed'` resolves to torrent failure.
3. Explicit `missing_files`, `failed`, or `checking` takes precedence over
   readiness and pause intent. A stale `ready` flag must not hide these states.
4. `paused_download` plus running intent and `torrent_error === 'storage full'`
   resolves to `storage_hold`, displayed as “Waiting for disk space”.
5. Determine paused state from durable control intent when present; otherwise
   use generic `status === 'paused'` for older daemons.
6. If `ready`, resolve to `paused_seed` when paused, otherwise `seeding`.
7. If paused and not ready, resolve to `paused_download`.
8. Running intent with an old `paused_download` phase resolves to `queued`,
   pending backend acknowledgement.
9. Otherwise use the supplied phase; when absent, map generic `fetching` to
   `fetching_metadata`, and use the generic status for other cases.

Keep the helper independent of React, storage, network requests, and the clock.
Pin the literal `storage full` in the shared fixtures and a Rust regression
against `STORAGE_FULL_ERROR`; changing that sentinel must fail a test instead
of silently turning a disk hold into an ordinary paused download.
Classify the full `JobSummary`, rather than extending the old status-only
function with a growing list of positional arguments. Suggested interfaces:

```typescript
type TorrentDisplayPhase = TorrentPhase | 'storage_hold' | 'unknown';
function torrentDisplayPhase(job: JobSummary): TorrentDisplayPhase | null;
function queueSectionKey(job: JobSummary): QueueSectionKey;
function sectionQueueJobs(jobs: readonly JobSummary[]): QueueJobSection[];
```

Migrate every status-only call and its tests together. Preserve the NZB
open-stage fallback that keeps stale completed rows in their active
post-processing group.

### 3.3 Section order and capabilities are explicit

| Order | Key / label | Members | Reorder | Collapse |
|---|---|---|---|---|
| 1 | `downloading` / Downloading | NZB downloads and torrent downloading | Yes | No |
| 2 | `fetching` / Fetching NZB | NZB source retrieval | Yes | No |
| 3 | `torrent_metadata` / Fetching torrent metadata | Torrent source and metadata retrieval | Yes | No |
| 4 | `checking` / Checking torrent files | Torrent checking | No | No |
| 5–12 | Existing post-process groups | Post queued · renaming · verifying · repairing · extracting · cleaning · moving · scripting | No | No |
| 13 | `seeding` / Seeding | Ready torrents with running intent | No | Yes |
| 14 | `completed` / Completed | Ready torrents with paused intent | No | Yes |
| 15 | `waiting` / Waiting | Queued, paused downloads, storage holds, failed/missing-file torrents, unknown states, existing NZB fallback | Yes | Yes |

Every job occurs exactly once. Omit empty headings. Preserve input order within
each group and retain original queue indices for move actions. Do not sort by
name, progress, or upload rate. Seeding and Completed must expose neither move
buttons nor the download priority editor. The older daemon fallback follows
§3.2; do not guess that an unready completed torrent is a seed.

## 4. Presentation — collapse changes visibility, never queue membership

### 4.1 Persist only the three collapsible section keys

Add optional `collapsible` metadata to section definitions. Use an accessible
pressable heading with its label, full count, and expanded state. Hidden cards
must be absent from the accessibility tree. Keep all groups expanded by default.

Use the existing SecureStore dependency through a small proposed
`mobile/src/storage/queuePreferences.ts` module. Store a JSON array under
`nzbd.queue.collapsed.v1`; retain only `seeding`, `completed`, and `waiting` on
read. This is a device display preference shared across connections, matching
the native app's existing display preferences. It is not synchronized with the
browser or daemon.

Malformed JSON, unknown keys, unavailable storage, and rejected reads fall back
to expanded groups. Failed writes keep the in-memory choice for the session.
An asynchronous initial read must not overwrite a user's early toggle, and
rapid toggles must persist the last choice, not whichever write finishes last.
Serialize writes or otherwise enforce that ordering. Do not write on every SSE
tick. A temporarily empty group retains its stored choice when it returns.

Keep these summaries visible and live while collapsed:

| Group | Summary |
|---|---|
| Seeding | Sum of upload rates and cumulative uploaded bytes |
| Completed | Sum of payload sizes, followed by “files kept” |
| Waiting | Sum of payload sizes |

Totals cover all jobs in the group, never just expanded cards. Treat missing
optional counters as zero and avoid displaying `NaN` or infinity. Collapsing
does not change the server overview counters, job action targets, or queue order.

### 4.2 Torrent cards need explicit rendering modes

[DashboardScreen](../mobile/src/screens/DashboardScreen.tsx) currently treats
every section other than Downloading, Fetching, and Waiting as post-processing.
Replace that negative test with an explicit NZB post-processing predicate;
otherwise new torrent groups will incorrectly show post-processing timers.

| State | Card behavior |
|---|---|
| Source / metadata fetch | State label; hide the download percentage and bar; avoid a meaningless ETA |
| Checking | “Checking files”; any retained byte progress means downloaded payload, not verification progress; no download ETA |
| Downloading | Payload progress, download rate, ETA; upload rate, ratio, and peers when available |
| Seeding | “Files ready”; “Seeding” or “Seeding · idle”; upload rate, uploaded bytes, ratio, peers, seeded duration; no download bar or ETA |
| Completed | “Files ready”; “Seeding stopped”; stop reason and policy; no download bar or ETA |
| Storage hold | “Waiting for disk space” and the server error; retain payload progress without suggesting active downloading |
| Failed / missing files | Failure label and server error; no ready claim or misleading full bar |
| Queued / paused download / unknown | Explicit state label; retain sensible known payload progress; no active-download ETA |

Keep torrent status visible even when the section names the broad activity.
Use ratio rather than Usenet health on torrent cards. Show readiness time when
available in expanded details, distinguishing it from import/pickup. Preserve
NZB health, stage timers, and existing recovery behavior. Style new groups with
the existing palette and verify all three navigation layouts at phone/tablet
widths; long error text and large text sizes must wrap.

## 5. Actions — a stopped seed must have an honest next step

Continue using the existing authenticated action endpoints and mutation/refresh
path in [useNzbd](../mobile/src/hooks/useNzbd.ts). Do not locally move a row on
button press. Busy state lasts through the request; refreshed snapshots and SSE
provide the authoritative state. A failed refresh after a successful mutation
does not fabricate a lifecycle transition.

| Display state | Primary action |
|---|---|
| Seeding | “Stop seeding”, sending existing `pause` when generic status permits it |
| Stopped seed below its policy limits | “Start seeding”, sending existing `resume` when generic status is `paused` |
| Stopped seed whose policy is already satisfied | “Seeding options”, opening the per-job policy editor |
| Paused download | “Resume”, using existing `resume` when generic status is `paused` |
| Queued / downloading / checking | “Pause” only when generic status is `queued` or `downloading` |
| Generic status `fetching` | No pause button, matching the web and the handler; metadata phase alone does not imply generic fetching status |
| Storage hold | Explain automatic disk recovery; do not offer resume as a disk-space remedy |
| Failed | Remove only, using existing keep-files / delete-files confirmations; show `torrent_error`; no lifecycle or move/priority controls |
| Missing files | “Re-download missing files”, sending `resume` when generic status is `paused`; no seed-policy gate even if readiness is stale |
| Unknown phase | Keep removal available; do not invent a lifecycle action |

Phase chooses the meaning and label; generic status also gates whether the
server accepts the verb. These action restrictions override the section's
`ordered` capability. The inspected owner accepts pause only for `queued` or
`downloading`, and resume only for `paused`. A stale mismatched snapshot must
not enable a verb the handler rejects. Removal remains available independently.

For ready seeds, the client predicts policy satisfaction from
`stop_on_complete`, ratio at least the non-null ratio limit, or seeded seconds
at least the non-null time limit. Limits are alternatives: the first reached
stops seeding. A zero-byte selection cannot satisfy the **ratio** condition;
stop-after-download and time limits still apply. The server projects ratio as
zero for an empty selection, so do not synthesize infinity from uploaded bytes.
Missing policy data means unknown, not proof that a limit was reached.

The ratio comparison is advisory. The server compares floating-point casts of
cumulative uploaded bytes and selected bytes times the limit; the client uses
the projected quotient. Rounding at equality and stale accounting snapshots
can disagree. The existing checkpoint bounds are 30 seconds or 8 MiB. Do not
change the daemon or its accounting to make client-side predictions exact.

**Web corrections in the same PR:** remove lifecycle and move/priority controls
for failed torrents; hide pause/resume during `storage_hold` and explain
automatic recovery; label missing-file resume “Re-download missing files”.
The web currently offers an invalid pause action for Failed and an ineffective
resume during a storage hold. Match the status guards above on both clients;
do not record those defects as intentional platform differences.

### 5.1 Disambiguate a refused seed resume before reporting disappearance

The API maps `resume == false` to 404 both when the job is absent and when a
seed policy prevents restarting. The owner policy refusal specifically checks
`seeding`/`paused_seed` plus a readiness timestamp and a reached policy.
Missing-file recovery bypasses that phase check, then clears historical
readiness and content path before queueing the download again.

Catch `ApiError.status === 404` from resume when the initiating snapshot was a
ready torrent. Fetch the current jobs using the existing `getJobs()` endpoint
and look up the same ID; avoid depending on an unrelated status request:

1. If still present as a seed, retain the row and open Seeding options with
   “Runner refused to start seeding. Check this torrent's seeding policy.”
   Do this even if the local policy prediction still says the limit is unmet.
2. If present but now checking, missing files, or failed, use the new state
   and its valid actions. Never overwrite it with the old ready state or open
   an editor for the wrong job.
3. If a successful fresh listing omits the ID, accept that authoritative
   disappearance and close any editor for it. Do not remove on the initial 404.
4. If the follow-up fetch fails, retain the last known row and report that
   the restart was refused and its current state could not be checked.

Do not surface the generic “job not found” error before this recovery runs,
automatically retry resume, or automatically change policy. Scope the result
to the initiating connection and job; switching servers invalidates that
pending UI response. Apply equivalent error handling in the web action path.

### 5.2 Use the existing per-job policy API

Add a typed method in [NzbdClient](../mobile/src/api/client.ts) and a hook
mutation for the already implemented endpoint:

```text
PUT /api/v1/jobs/{id}/torrent/seed-policy
Content-Type: application/json

{
  "use_defaults": false,
  "stop_on_complete": false,
  "ratio_limit": null,
  "time_limit_secs": null
}
```

Offer category/global defaults, stop after download, unlimited seeding, and
ratio/time limits. Defaults sends `use_defaults: true`; the server resolves the
category. Unlimited uses null limits. Limits requires at least one finite
positive value; convert hours to a positive safe integer number of seconds,
rejecting values that round to zero. A ratio must be finite and positive.
Send only these four accepted fields.

Saving a policy must not resume the job. After a successful save, report that
stopped torrents stay stopped until explicitly started. Preserve edits on
failure, avoid overwriting unsaved edits with SSE updates, and bind the editor
to its job ID. If the job disappears, close/disable its editor without applying
the draft to another row. Older servers may reject the endpoint: show that
error and keep the queue usable. Do not require the endpoint to classify jobs.

## 6. Regression evidence — test behavior shared with the web

Extend [native grouping tests](../mobile/__tests__/queue-sections-test.ts), add
pure presentation/policy tests, and exercise actual rendered headings and
cards. Pure helper tests alone would miss the current post-processing
predicate bug and wrongly wired action buttons.

| Test area | Required cases |
|---|---|
| Precedence | Every torrent phase; failed generic status with stale ready; checking and missing files with stale ready; durable intent overriding stale generic status; storage hold; resumed paused-download phase |
| Compatibility | NZB without kind; missing/null lifecycle fields; unknown future phase; old generic fetching; no inference from 100% bytes; existing open-stage regressions |
| Grouping | Mixed NZB/torrent queue; every ID exactly once; section order; stable original indices; no empty headings |
| Collapse | Three allowed keys only; live totals while hidden; restore after restart; malformed/unavailable storage; early toggle versus hydration; rapid writes; empty group reappearing |
| Rendering | Real headings/counts; collapsed cards absent; no post timer on torrents; no seed bar/ETA; truthful checking/error display; accessible toggle state |
| Actions | Failed is remove-only; missing files resumes with explicit re-download label; storage hold has no lifecycle action; generic fetching hides pause; stale generic status guards; no seed move/priority controls; ready-seed resume 404 re-fetches before choosing options, new state, or disappearance; follow-up fetch failure and connection switch; policy save never resumes |
| Policy | Defaults, stop, unlimited, ratio-only, time-only, both limits; equality/rounding and stale snapshots at threshold; zero-byte selection excludes ratio only; stop/time still apply; empty/zero/negative/non-finite/overflow input; save errors and disappearing job |
| Stream and refresh | Download → seed → stopped → seed; storage hold/recovery; missing-file recovery; polling fallback produces the same classification as SSE |

Place representative wire-format summaries and expected section/status/action
results in `crates/nzbd-types/fixtures/mobile-queue-parity.json`. Include the
`storage full` sentinel, failed remove-only behavior, missing-file recovery,
storage holds without lifecycle actions, and a ready seed whose resume 404s.
The last case includes response/refresh expectations for action-path tests,
not just a static section result.

Run the fixture through mobile helpers/rendered tests and the web DOM harness
exports (`torrentPhase`, `torrentStatus`, `sectionOf`, `seedLimitReached`,
`seedPolicyText`, `seedStopText`, and `SECTIONS`, verified by Fable). Exercise
the rendered action model and mocked request/refresh path on both clients.
No broad harness rewrite is required. Make this a behavioral comparison,
not a source-text check.

[CI scope](../scripts/ci-scope) already triggers Rust, mobile, and torrent lanes
for this fixture path. Add a Rust serialization check in the engine test suite,
where `JobSummary` is defined: serialize constructed summaries and compare
their wire fields with fixture inputs. `JobSummary` is Serialize-only, so do
not introduce Deserialize merely to round-trip test fixtures. Assert the
storage-full constant against the fixture from a runtime test that can access
it. This keeps server projection and both UI consumers checked together.

Intentional native differences, such as waiting for authoritative snapshots
instead of optimistic browser overlays, must be named in fixture expectations.
Assertions should catch future divergence without importing the entire web UI
into the native production bundle.

## 7. Milestones — implement on a base that includes the web changes

### 7.1 Establish the implementation base and classification contract

Use an isolated checkout from then-current main, preserving the existing
checkout's documentation edits. Re-verify the server fields, action handlers,
web resolver, and build metadata. Add types, pure helpers, and the parity
fixtures. Keep the web as the reference for precedence, recording any discovered
defect rather than copying it silently. Preserve all existing tracked README
edits and untracked plans/reviews; do not reset, clean, or switch the original
checkout to obtain the implementation base.

**Acceptance:** the §6 phase and compatibility matrix passes, with each fixture
assigned exactly once by both clients. No native runtime behavior changes yet.

### 7.2 Wire the native queue, collapse persistence, and card rendering

Update section definitions and Dashboard rendering; add the storage module and
small components/helpers where they reduce duplicated logic. Keep original
queue indices intact. Wire accessible collapse headings and aggregate totals.

**Acceptance:** rendered tests prove all new sections, preserved NZB stages,
live collapsed summaries, persistence races, and correct card modes. Both
mobile platforms use the same implementation.

### 7.3 Finish controls and the policy editor

Add policy client/hook methods and the per-job editor. Use phase-derived labels
and status-gated action availability; preserve existing removal confirmations.
Apply the web fixes in §5 and the ambiguous-404 recovery in §5.1 on both clients.

**Acceptance:** policy validation and client request tests pass; rendered action
tests prove that stop/start targets the right job and policy save never sends
resume. An unsupported policy endpoint produces a recoverable error.
Failed and storage-held rows expose no invalid lifecycle action. Missing-file
recovery has the explicit label. A ready seed's 404 neither drops its row nor
reports it missing without a successful authoritative re-fetch.

### 7.4 Validate, document, and produce releasable native builds

Update [MOBILE.md](MOBILE.md) with actual delivered grouping, policy controls,
fallback behavior, and collapse preferences. Reconcile its stale NZB-only Add
description while describing the existing torrent support. Update this plan's
status only as milestones are achieved.

From the implementation checkout:

```bash
cd mobile
npm ci                         # install the committed dependency set
npm run typecheck              # validate the native data and component contracts
npm test                       # grouping, presentation, storage, client, UI tests
npm run export                 # prove both native JavaScript bundles compile
```

Run the repository's web boot/DOM checks when adding the cross-client fixture;
use the exact Make targets in the implementation base. Run dependency doctor
if dependencies or native configuration change. Existing dependencies should
be sufficient except for any narrowly required renderer test tooling.

Exercise an iOS simulator/device and Android emulator/device with a controlled
mixed queue: fetching, checking, download completion, seeding, stopped seed,
storage hold, and missing files. Include cold restart after collapse, narrow
screens, tablet layout, large text, and VoiceOver/TalkBack headings. Use fixture
data for otherwise hard-to-reproduce states and label it as such; do not change
real torrent policy or delete real payloads merely to create test states.

After code validation, allocate the next unused native build number from the
then-current release metadata. Update all tracked iOS/Android/app configuration
locations together and use the existing signed release process. Do not assume
10 remains available. Record source SHA, version/build numbers, artifact and
distribution status for both platforms. A merged JavaScript change or successful
export alone does not mean the installed apps received the fix.

**Acceptance:** required CI passes; native smoke evidence is recorded; both
release artifacts contain the reviewed source. Report any signing, device, or
distribution limitation explicitly instead of marking delivery complete.

## 8. Fable review — accepted decisions and source clarifications

[Fable's review](MOBILE_QUEUE_PARITY_REVIEW.md), supplied on 2026-09-27,
approved the plan with four required action changes. All four are incorporated
in §5 and the §6/§7 acceptance checks. The checkmarks below record design
decisions, not implementation or release completion.

- [x] **Precedence:** confirmed as written in §3.2. Failed is remove-only;
  Missing files sends `resume` labelled “Re-download missing files”; Storage
  hold has no lifecycle action. Correct the web in the same PR to match.
- [x] **Per-job policy editor in scope:** confirmed. A policy-satisfied
  Completed row opens Seeding options. A refused ready-seed resume is
  disambiguated by refresh before reporting absence (§5.1).
- [x] **Device-wide collapse preferences, all-expanded default, SecureStore:**
  confirmed, following the existing theme preferences.
- [x] **No NZB-only timers/health or misplaced priority controls:** replace
  the negative post-processing predicate with an explicit NZB predicate.
  Seed groups never offer download priority; Completed is not History or
  library import. Failed jobs expose only removal controls.
- [x] **Shared fixtures catch the original omission:** place them under
  `crates/nzbd-types/fixtures/`, exercising server serialization and both UIs
  with the existing CI path rules.
- [x] **Separate iOS and Android evidence before closing the gap:** confirmed.
  Allocate the next build number at release time; do not assume build 10.

**Clarifications after checking the source at `5b683d6`:** the review is
preserved verbatim in its companion document. Three explanations there need
precision; the required user-facing corrections remain accepted:

1. Zero selected bytes disables only the ratio condition in
   `seed_policy_stop_reason`; stop-on-complete and time checks still run.
   Do not short-circuit the entire policy evaluator for an empty selection.
2. Missing-file recovery bypasses the owner's seed-policy refusal because the
   phase is `MissingFiles`, not because historical readiness must already be
   absent. The regression constructs a retained readiness timestamp and
   confirms resume clears it. Fixtures must cover that stale-readiness case.
3. The server's ratio check casts byte counters to `f64` before multiplying
   and comparing. It is not integer arithmetic. The client quotient can still
   disagree at a boundary, so the 404 recovery remains necessary.

**Validation boundary:** these revisions were checked against source and for
document consistency. No application code, device build, or runtime test has
been changed or executed as part of applying this review.
