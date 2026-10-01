# Superseded investigation — historical evidence and proposals

**Not the current plan.** Read [the reconciled incident review](DOWNLOAD_INCIDENT_REVIEW.md).
This preserves earlier reasoning, including rejected diagnoses and stale implementation status.

# Download incident — evidence, root causes, and proposed fixes

**Status:** independent review received; rejected path/publication changes
removed; remaining candidates need the corrections in section 13; nothing
deployed. Section 12's identity fix is **not ready to ship** until successful
deletions are scoped to the generation they actually deleted.
**Written:** 2026-10-01. **Live observations:** approximately 21:15–21:17 UTC.

**Read [the independent review](DOWNLOAD_INCIDENT_OPUS_REVIEW.md) and section
13 first.** Sections 2–12 preserve the investigation and its earlier candidate
design. Their unsettled or incorrect claims about directory semantics,
filesystem failures, and extraction are superseded by the review and the
corrections below. In particular, an empty `InterDir` must not acquire a new
`MainDir` fallback. The original extension-only archive guard was inadequate.

Companion to [CONFIGURATION.md](CONFIGURATION.md). This document records the
incident at `http://nuc3:6789/`, corrects earlier overstatements, and proposes
work for review. Treat the working-tree patch as a candidate to challenge,
not an approved fix. Review baseline source separately from the patch.

## 1. What the user observed and requires

- With downloads-at-once set to 1, pausing job 1703 did not start job 1704.
  The runner subsequently showed no downloading activity.
- A folder associated with job 1691 contained archive volumes under the
  configured completed directory. The user correctly rejects archive-only,
  unsuccessfully processed media as completed output.
- Files showed retained/held data without an adequate explanation and
  required individually selecting 59 entries; no select-all was present.
- Job 1688's retained folder cannot leave review hold: releasing the hold
  reports `payload identity changed; review required`. Section 10 records
  the additional investigation.
- The working directory is configured as `/processing/`, bound to
  `/mnt/processing` on the host. The user sees recent files there.
- Honor the existing path settings. Do not hardcode `/processing`, invent
  `/processing/intermediate`, or change settings to compensate for code.
- Correct the causes within the application's architecture. Do not add
  incident-specific watchdogs, retries, folder exceptions, or background
  repair loops that hide broken state transitions or interfere with normal
  operation. A concrete causal explanation is required before accepting a
  solution.

## 2. Corrections to the earlier diagnosis

**“It is not using `/processing`” was too broad and incorrect.** The runner
uses it for persistent queue state, databases, and other configured/default
storage roles. Recent writes there are real. They do not, by themselves,
establish the download payload location for a particular job.

**The 59-file folder was not recorded as successful post-processing.** Live
history records job 1691 as `UNPACK_FAILURE`. Its failed parking operation
left the payload under the completed root. The directory placement and
presentation are problematic even though history reports failure.

**The allocation error explains a demonstrated blocker, not every part of
the incident.** It is not proof that pausing is broken, or a complete
explanation of why the queue stays idle after the conflicting folder was
deleted. That latter transition still needs reproduction.

**A new intermediate subdirectory was an unjustified proposal.** The
candidate patch now falls back to the configured `main_dir` itself when
`inter_dir` is absent. That behavior is a proposed contract to review, not
evidence of what the deployed binary currently does.

## 3. Evidence and its limits

### 3.1 Deployed identity and saved configuration

The live status endpoint reports version `0.2.0+unknown`, built
`2026-09-30 22:39 UTC`. Docker reports container start
`2026-09-30T22:44:59.114320767Z` and image:

```text
sha256:1d9bfb6cae5e53e94e8ceb32e24bdcb1651278ee3bca660ca655b95542f0f33a
```

The exact source commit of that image has **not** been verified. Local
baseline HEAD is `b4ed40e718b0d286a34f4db338c80589d8d57138`. Source findings
below refer to that baseline unless explicitly labeled candidate patch.

Filtered `GET /api/v1/config` agrees with the user's Settings screenshot:

```toml
[paths]
main_dir = "/processing/"
dest_dir = "/working/monarr/completed"
# inter_dir, queue_dir, temp_dir, torrent_dir: absent

[post]
enabled = true
unpack = true
cleanup = true
failure_action = "park"
# failed_dir: absent

[cluster]
enabled = false
```

There are no category overrides and `pending_restart` is empty. This is a
filtered observation, not a replacement configuration file.

Verified Docker bind mounts include:

| Container path | Host path |
|---|---|
| `/processing` | `/mnt/processing` |
| `/working` | `/mnt/qnap/working` |
| `/etc/nzbd` | `/opt/noirr/runner/config` |

### 3.2 `/processing` is being used

A read-only recursive listing of files modified within the preceding four
hours returned queue/state files, including these UTC modification times:

| Container path | Modification time on 2026-10-01 |
|---|---|
| `/processing/queue/history.sqlite-wal` | 21:15:04 |
| `/processing/queue/artifacts.sqlite-wal` | 21:06:36 |
| `/processing/queue/queue.json` | 20:44:03 |
| `/processing/queue/volumes.local.json` | 20:41:30 |
| `/processing/queue/artifact-identities/08ab681330d8af105d8518e23087f5d1/bf112b86c94b13b51775b4b340c06909.json` | 20:39:47 |

An older relocation staging directory also exists at
`/processing/failed/.runner-move-f45132ba85aec77eec143a73bd9f3f34-1`.
Inventory additionally remembers an older, now `source_gone` payload under
`/processing/Heated.Rivalry.S01.2160p.AMZN.WEB-DL.DDP5.1.H.265-RAWR.#22982`.
The current listing does not establish all historical uses or identify
which specific files the user had inspected.

### 3.3 Job 1703's current payload is under completed

`GET /api/v1/jobs/1703` reports paused, with 8,297,337,098 downloaded bytes.
Its active artifact `08ab681330d8af105d8518e23087f5d1` records:

```text
/working/monarr/completed/Heated.Rivalry.S01.2160p.AMZN.WEB-DL.DDP5.1.H.265-RAWR
```

A filesystem listing confirms a `.runner-file-62665.part` in that folder,
5,103,118,457 bytes, modified at `2026-10-01 20:41:05 UTC`, along with other
recent payload files. This is direct evidence that this unfinished job has
payload in the completed tree; it is independent of the `/processing`
state-file writes.

### 3.4 Job 1704 hit an allocation conflict

The live log repeatedly records, including at `2026-10-01T20:44:02Z`:

```text
payload allocation unavailable; download held job=1704
error=existing payload needs explicit ownership review
```

Its API record says `kind=url`, `status=queued`, zero downloaded bytes,
8,072 total articles, and this name:

```text
Heated.Rivalry.S01E05.MULTI.2160p.WEB.H265-BAWLS-xpost
```

The same basename belonged to job 1691's retained artifact under the
completed root. This supports a collision with the earlier payload.

At 21:15 UTC, global status reports download pause false, disk-low false,
quota-reached false, no blocked servers, and zero jobs downloading. These
values rule out those reported global blocks at that observation time;
they do not prove the scheduler is otherwise healthy.

### 3.5 The conflicting artifact changed during the investigation

Artifact `f45132ba85aec77eec143a73bd9f3f34`, job 1691, was retained with a
filesystem error in the user's screenshot and earlier reads. Its latest
record is `deleted`. Events contain:

```text
1790888713  delete_requested
1790888722  succeeded: deletion confirmed
```

Job 1704 remained queued at the later observation. The investigating agent
did not issue the deletion. Do not propose deleting that folder again, or
assume the old collision is still the current filesystem condition.

### 3.6 Job 1691 failed extraction and then failed parking

`GET /api/v1/history?limit=200`, filtered to job 1691, reports:

```text
status: UNPACK_FAILURE
final_dir: /working/monarr/completed/Heated.Rivalry.S01E05.MULTI.2160p.WEB.H265-BAWLS-xpost
Failure:Files: checked move pending:
  Ok(Err(Io(Os { code: 22, kind: InvalidInput, message: "Invalid argument" })))
stages: par_rename, rar_rename, unpack
unpack duration: 64734 ms
```

The original NZB history lists 59 entries, including archive volumes,
an NFO, an SFV, and a sample. The screenshot shows many renamed RAR
volumes. Neither the entry count nor the presence of a sample proves
successful media extraction. Do not describe all 59 original entries as
RARs, or the sample as the completed episode.

The exact extraction failure and the syscall that produced `EINVAL` have
not been established. No raw source URLs or credentials are included here.

## 4. Root-cause assessment

### 4.1 URL admission omits collision handling present in direct admission

In baseline [owner.rs](../crates/nzbd-engine/src/owner.rs), `AddParsed`
chooses a fresh suffixed directory when the preferred path already exists.
`CompleteUrlFetch` finalizes the name and queues the job without that step.
Later `ensure_allocation` attempts `self.dest_dir.join(job_dir_name(job))`.

[Inventory::allocate](../crates/nzbd-state/src/artifacts/mod.rs) rejects
an existing filesystem entry with the exact logged error. The scheduler
logs that error and breaks out of lease generation before setting the job
to downloading. It leaves the visible job queued.

**Assessment:** high-confidence cause of the observed allocation blocker.
The source asymmetry and live error agree. The intended ownership check
should remain; admission should avoid claiming an older job's payload.

**Still unresolved:** whether pausing independently prevents normal queue
advancement; why deleting the conflict did not restart job 1704; whether
workers need an explicit wake-up after allocation conditions change. An
empty lease response and missing wake-up are hypotheses to test.

### 4.2 Usenet payload and completed destination are conflated

Baseline [main.rs](../crates/nzbd/src/main.rs), lines 791–797 at the
baseline commit, supplies `cfg.dest_dir()` as the single-node engine's
payload root. Baseline `writer_for` joins that root with the job directory.
[Config::state_dir](../crates/nzbd-config/src/lib.rs) separately derives
`<main_dir>/queue` when `queue_dir` is absent.

**Assessment:** the local baseline explains both simultaneous observations:
recent state writes in `/processing`, and unfinished payload in completed.
Live artifact and filesystem evidence confirm the latter for job 1703.
The precise deployed commit still needs verification before treating the
baseline as an exact copy of production.

This explains directory placement. It does not explain extraction failure,
the failed relocation syscall, or all scheduler behavior.

### 4.3 Failed parking leaves failed material at its original location

The source starts in completed; extraction fails; configured failure action
is park; parking reports `EINVAL`; files remain at the original path and
receive retention/review state. History records failure and the actual
remaining path. That chain is consistent with the history and staging
directory evidence.

[relocation.rs](../crates/nzbd-state/src/artifacts/relocation.rs) contains
cross-device rename/copy, `std::io::copy`, file synchronization, and
directory synchronization. Any diagnosis naming a specific failing syscall
without further evidence is premature. Existing buffered-copy fixes in
[recovery.rs](../crates/nzbd-state/src/artifacts/recovery.rs) concern a
separate path; do not assume they cover relocation or prove this failure.

### 4.4 Files UI is missing a bulk-selection control

The screenshot and baseline [UI](../crates/nzbd-api/ui/index.html) show
individual file selection without select-all. Retention state also needs
to explain the failure/hold reason without implying successful processing.
These are usability defects distinct from payload placement.

## 5. Proposed fixes for independent review

These are candidate directions, not permission to implement all of them.
For each one, establish the causal chain and the responsible architectural
component first. A test showing that a workaround succeeds does not prove
that the underlying failure is understood.

### 5.1 Give every admission path a fresh, durable allocation

Share allocation/name-collision handling between parsed NZBs and completed
URL fetches. Resolve the final name first, allocate a unique job directory,
then persist it before download writes. Check existing filesystem entries,
including symlinks, and queued reservations. Preserve the refusal to
implicitly adopt retained bytes.

Expose allocation failures as actionable job state and allow other eligible
jobs to progress. Define how an already queued, zero-byte legacy URL job
recovers; fixing only future admissions does not repair job 1704. Determine
the wake-up/retry contract after an external artifact operation succeeds.

### 5.2 Resolve download and completion roots from existing settings

Proposed single-node Usenet contract, to re-verify against configuration
semantics before implementation:

| Purpose | Existing setting / resolution |
|---|---|
| Download and processing payload | Nonempty `paths.inter_dir`, otherwise expanded `paths.main_dir` |
| Completed publication | Category `dest_dir` override, otherwise `paths.dest_dir` |
| Persistent state | Existing `paths.queue_dir`, otherwise `<main_dir>/queue` |
| Failed parking | Existing `post.failed_dir`, otherwise `<main_dir>/failed` |
| Torrent payload | Existing separate torrent resolution; preserve its semantics |

For the observed settings, new Usenet jobs would work under `/processing/`
and publish under `/working/monarr/completed`. These are configuration
values, not literals to embed in application logic.

Persisted allocations must remain authoritative across restarts. An upgrade
must not silently create a second payload, lose completed segments, or
overwrite an existing folder. Decide explicitly how old unfinished jobs
already under completed will be moved safely; retaining their old paths
avoids data loss but does not satisfy the final placement requirement.

### 5.3 Publish only after the required processing succeeds

Keep unsuccessful extraction, failed verification, and required script
failures out of the completed destination. Successful publication needs a
verified result and durable inventory/history updates. Do not infer success
from archive presence, a sample file, or the absence of a detected first
volume. Honor extraction settings while defining what an intentionally
unpacked-disabled archive job may report and where it may live.

Review scripts' path contracts before changing their order relative to the
move. Cover category destinations, script-selected final paths, stage
retries, restart points, and crash recovery. A move failure must not become
a successful completed result. Cross-filesystem publication must not expose
partial output to importers; review staging placement as well as rename.

Separately diagnose the actual unpack and parking failures. Add operation
and path context to filesystem errors, reproduce with safe fixtures on the
relevant filesystem types, and only then select a syscall/copy fix. Changing
the payload root alone is not a repair for a broken relocation primitive.

### 5.4 Make retention and selection usable

Show why a folder is held, its failed job outcome, and the appropriate
recovery action. Add select-all across the folder's paginated file listing,
clear selection, selected count, and partial-selection indication. Check
the artifact revision throughout selection so a changing manifest cannot
silently produce an incomplete or stale selection. Selection itself must
not delete or publish files.

## 6. Candidate patch already present — not an approved implementation

Ten tracked files contain uncommitted changes from before implementation
was stopped. Section 12 separately identifies the later authorized identity
lifecycle fix. No production deployment, configuration change, or payload
move was performed by this investigation. The changes remain available for
review; they have not been silently discarded.

| Files | Candidate changes |
|---|---|
| `crates/nzbd-engine/src/owner.rs` | Shared admission allocation; inventory-based path resolution; allocation path cache |
| `crates/nzbd-config/src/lib.rs` | Proposed `download_dir()` resolution and overlap validation |
| `crates/nzbd/src/main.rs` | Pass proposed download root to engine/PP; pass existing completed root to PP |
| `crates/nzbd-post/src/manager.rs` | Completion publication after scripts; inventory paths; archive guard; failed parking retry handling |
| `crates/nzbd-cluster/src/worker.rs` | Clear the new internal completion target for private cluster work |
| `crates/nzbd-api/src/artifacts.rs` | Resolve scan paths from inventory/configured download root |
| `crates/nzbd-api/ui/index.html` | Bulk selection, retention explanations, selection revision checks |
| `crates/nzbd-engine/tests/e2e.rs` | Restart with changed default root while retaining persisted allocation |
| `crates/nzbd-post/tests/pp_pipeline.rs` | Archive-only and script/publication regression cases |
| `crates/nzbd/tests/ui_dom_harness.js` | Bulk-selection and presentation assertions |

Review concerns include the broad scope of moving the script/move stages,
the new overlap validation rejecting previously accepted configurations,
archive detection semantics, preservation of old allocations, and the
absence of a complete existing-job recovery solution. The candidate does
not fix the demonstrated parking `EINVAL` or establish its exact cause.
Cluster behavior has not received an equivalent end-to-end review.

Validation performed before this document:

- The admission change passed 170 engine unit tests and 22 end-to-end tests
  before subsequent directory/path-resolution changes. That result does
  **not** validate the final engine working tree.
- UI harnesses passed 842 assertions before a later explanatory-text edit.
- `cargo check -p nzbd --tests` passed on an earlier candidate revision.
- The latest `cargo test -p nzbd-config -p nzbd-post` passed: 35 config unit,
  6 config integration, 61 post unit, 6 post integration, and 33 pipeline
  tests. This does not reproduce production mounts or establish root cause.
- No final combined test/lint run or deployment verification was completed.

## 7. Acceptance checks before calling this fixed

1. Reproduce active A + queued B + pause A with independent fresh names;
   B starts under the configured concurrency limit. Also test resume,
   global pause, restart, and a third eligible job behind an allocation
   failure.
2. Reproduce a URL job whose resolved name collides with retained data.
   It receives a distinct durable allocation; prior bytes remain unchanged.
   Test the already-admitted zero-byte case and conflict deletion/wake-up.
3. With arbitrary test roots, prove new payload bytes and extraction scratch
   use configured working/intermediate storage and no unfinished job appears
   in completed. Repeat with explicit `inter_dir` and category destinations.
4. Reproduce extraction failure, absent archive head, script failure, and
   move failure. History and UI report the real failure and location; none
   is advertised as completed output.
5. Verify successful extraction/publication and crash recovery across two
   filesystems. Test destination collision, capacity exhaustion, copy,
   fsync, and rename failures without overwriting either generation.
6. Restart with preexisting partial allocations. Verify segment reuse and
   byte integrity; review and test migration out of completed explicitly.
7. Select 59 files with one action; repeat across multiple API pages and a
   mid-selection manifest change. Verify selection has no filesystem effect.

## 8. How to review independently

Start with the live observations in section 3, then compare baseline source
to the patch. Do not use the candidate implementation as evidence that its
diagnosis is correct. Confirm deployed source provenance where possible.

```bash
git show b4ed40e:crates/nzbd/src/main.rs          # baseline wiring
git show b4ed40e:crates/nzbd-engine/src/owner.rs # baseline admission/writers
git diff --stat                               # candidate scope
git diff -- crates/nzbd-engine/src/owner.rs    # allocation/path changes
git diff -- crates/nzbd-post/src/manager.rs    # publication/stage changes
git diff -- crates/nzbd-config/src/lib.rs      # proposed configuration contract
```

Re-read filtered live status/history/artifact endpoints if necessary;
inventory state is changing. History and configuration may contain secrets,
so omit passwords and source URLs from review output. Inspect current
files before proposing any recovery action. This review authorizes no
deletion, re-adoption, production restart, or deployment.

The immediate review deliverable is a verdict on each proposed fix, the
smallest justified implementation scope, and a list of unresolved evidence
needed for the extraction, relocation, and post-deletion queue behavior.

## 9. Architectural acceptance criteria — fix the owner of the behavior

For **each** diagnosed defect, the reviewer must provide:

1. **A reproducible trigger and observed failure.** Name the input, prior
   durable/in-memory state, failing operation, and resulting state. Separate
   observed evidence from inferred mechanisms. If the exact cause remains
   unknown, say so and specify the next diagnostic experiment.
2. **The violated invariant and its owner.** Identify the existing component
   that should enforce the rule, including the caller/callee boundary where
   the rule is lost. Explain why the correction belongs there.
3. **The causal correction.** Show how the change removes the failure path
   for all equivalent admissions or transitions, rather than recognizing
   one job, filename, mount, or error string after the fact.
4. **State and lifecycle compatibility.** Explain behavior during pause,
   resume, retries, restart, concurrent work, cancellation, and partial
   failure. Identify the authoritative durable record. Do not create a
   second competing owner or silently rewrite existing allocation identity.
5. **Discriminating tests.** Demonstrate failure on the baseline and success
   with the correction, plus a neighboring case that would expose an
   overbroad fix. Include integration coverage at the failing boundary;
   tests that merely restate helper logic are insufficient.
6. **What becomes unnecessary.** Identify any workaround or duplicated
   logic the fix replaces. Explain any new ongoing mechanism, its owner,
   and why existing scheduling/lifecycle mechanisms cannot satisfy the
   invariant. Do not add periodic activity by default.

Candidate invariants to validate against the existing design:

| Boundary | Invariant to establish |
|---|---|
| Admission → inventory → writer | Every payload writer uses a committed allocation belonging to its job; equivalent admission types enforce the same rule. |
| Queue owner → worker | Every eligible job can make progress; a blocked job has an explicit reason; eligibility changes reach the scheduler through its normal event/state mechanism. |
| Config → payload placement | One defined resolution policy maps existing settings to working and completed roles; callers do not invent alternate path rules. |
| Processing → publication → history | Successful completion corresponds to verified, durably published output; failed processing cannot be mistaken for successful output. |
| Relocation → inventory | Copy/publication/retirement preserve ownership and crash consistency across supported filesystems; failures identify the actual operation. |
| Inventory → Files UI | Retention and ownership state are distinct from processing success and are explained accurately. |

In particular, a timer that periodically unblocks job 1704 is not an
acceptable substitute for finding the missing or incorrect queue
transition. Moving every failure to a different folder is not a substitute
for diagnosing extraction or relocation failure. Changing a label alone
does not fix incorrect payload placement. Blanket retries or swallowed
`EINVAL` errors are not a filesystem diagnosis.

Use the existing architecture where it supports these invariants. If a
boundary is fundamentally wrong, propose a cohesive change to that boundary
with explicit compatibility and recovery behavior. Do not accumulate
special cases to avoid making that decision. No proposed patch is accepted
until its causal explanation and architectural fit withstand independent
review; unresolved portions of this incident remain unresolved.

## 10. Additional incident — job 1688 cannot leave review hold

**Reported:** 2026-10-01, screenshot timestamp 17:08:38 America/New_York.
**Investigation:** production reads only. No release, inspection mutation,
adoption, deletion, or identity rewrite was attempted on production. The
user subsequently authorized fixing this cause locally; see section 12.

### 10.1 The error corresponds to a real recorded identity mismatch

The screenshot shows `American.Horror.Story.S13E02.2160p.WEB.H265-CAKES`,
job 1688, retained, owned by Runner, with 17 files totaling about 2.5 GiB.
Its hold is `review: interrupted move`; the release action errors with:

```text
payload identity changed; review required
```

Live `GET /api/v1/artifacts/365ec8dda3bc5b5da0c36c9d58c1d2bc` reports
revision 10, generation `71b074a841a43f18650c881714191917`, state `retained`,
owned true, and the same hold. Its path is:

```text
/working/monarr/completed/American.Horror.Story.S13E02.2160p.WEB.H265-CAKES
```

Comparison with read-only `stat` inside the running container:

| Identity field | Artifact record | Current directory |
|---|---|---|
| Device | 70 | 70 |
| Inode | 3735597 | 3735572 |
| Type | Directory | Directory |

`Identity::same_object` compares device, inode, and directory type.
`Inventory::verify` returns the exact screenshot error when that comparison
fails. `release_review` calls `verify` before changing the hold.

**Confirmed immediate cause:** the release operation rejects the recorded
identity mismatch. This is not merely a stale revision or changed file
count. The evidence does not yet identify what changed the identity or
whether a filesystem identity characteristic contributed.

### 10.2 The offered review workflow cannot reconcile this mismatch

`Inventory::inspect` also calls `verify` before enumerating the current
folder. Therefore “inspect” or “re-inspect” cannot refresh an artifact with
this mismatch. `adopt` rejects already-owned artifacts and also verifies
identity. These checks protect against applying old ownership authority to
different bytes, but the offered actions do not provide a resolution path.

The UI offers release based on the hold string prefix (`review`), without
knowing whether the backend can actually release that hold. A clearer
message is needed, but changing the message alone is not the lifecycle fix.

### 10.3 A deleted artifact and an unresolved move coexist

The artifact's event endpoint reports:

```text
1790789013  finalized: retained
1790792419  delete_requested
1790792427  succeeded: deletion confirmed
1790888785  retention: Keep
1790888790  retention: fresh retention period
```

The live operation endpoint for
`move-365ec8dda3bc5b5da0c36c9d58c1d2bc-1` reports:

```text
kind: relocate
state: review
attempts: 1
error: interrupted move: filesystem: No such file or directory (os error 2)
destination: /processing/failed/American.Horror.Story.S13E02.2160p.WEB.H265-CAKES
source snapshot inode: 3735597
source snapshot generation: 71b074a841a43f18650c881714191917
```

In local [reconcile_relocations](../crates/nzbd-state/src/artifacts/relocation.rs),
an unsuccessful reconciliation of a running/review move retrieves the
current artifact and unconditionally sets its state to `retained`, sets
`review: interrupted move`, and increments its revision. That error branch
does not check for terminal deletion or whether the operation still has
authority over the current artifact generation/state.

**Reproduced cause:** an unresolved old move resurrects a deleted artifact
as retained. The isolated regression fails before the correction with
actual `retained`, expected `deleted`. The live event/state contradiction
and source transition agree. The exact deployed source commit remains
unverified, and the audit log lacks an event for the old reconciler's
overwrite; those limits do not negate the reproduced transition.

Further inventory/history reads identify the replacement as a separately
recorded allocation, rather than unexplained changes to the original:

| UTC on 2026-09-30 | Durable evidence |
|---|---|
| 18:20:27 | Deletion of job 1688's artifact confirmed; old inode 3735597. |
| 18:20:42 | Job 1689's artifact created at the same pathname. |
| 18:21:16 | Job 1689 finalized as completed, inode 3735572; history reports SUCCESS. |
| 22:45:01 | Deployed daemon's reported startup time, after those operations. |

Job 1689's artifact is `6eab74996dfbd0a4ab9318fd7b066b5d`, generation
`aba714f2c56f5dad9468eb8b8e3d43f7`. Its identity matches the directory found
by `stat`. An older `-xpost` record for job 1685 also records inode 3735572;
the evidence does not prove which actor performed every physical move or
whether the inode was reused. It does establish that the current allocation
is distinct from deleted job 1688. Forcing job 1688's identity to match it
would grant the wrong artifact authority over another allocation.

### 10.4 Proposed architectural correction and acceptance checks

Put the correction in inventory operation ownership and reconciliation:

1. Define how deletion terminalizes or supersedes an unresolved relocation
   and its scratch generation. Check that authority in both commit and
   error paths. An obsolete operation must not revive a deleted artifact
   or apply its old ownership to a new directory at the same name.
2. Reconcile filesystem observations with durable generation identity.
   A different identity must enter an explicit discovery/review workflow;
   it must not inherit the old generation's ownership simply because its
   pathname matches. Determine how the existing scan/adoption architecture
   should represent this case before introducing another mechanism.
3. Make API/UI actions express the actual next valid transition. Distinguish
   acknowledging a releasable hold from resolving changed identity and
   superseding an obsolete operation. Preserve the identity verification.
4. Reproduce: interrupted move → delete original artifact → replacement at
   same pathname → reconciliation/restart. Assert that deletion remains
   terminal, the replacement is not implicitly owned/deleted, and stale
   operations cannot mutate it. Also test the unchanged-source case so
   ordinary interrupted-move recovery still works.
5. Reproduce the current inspection/release dead end and demonstrate a
   supported, explicit review path without database edits, force-release,
   identity-check bypass, or a periodic unholding watchdog.

The earlier directory and bulk-selection changes do not address this
lifecycle defect. The subsequently authorized correction is in section 12.

## 11. Cancelled recovery handoff remains in the main view

The second screenshot shows recovery
`70a9015f567db76ce0183b149c371c84`, `cancelled`, with a historical
`filesystem: Invalid argument (os error 22)` error. The live recovery record
agrees and identifies a Fright Night source, artifact
`359d7ffe74ac01b4a399aa29c093a643`. It is unrelated to job 1688 despite
appearing beneath that folder in the screenshot.

Read-only checks find:

- Source artifact state `source_gone`, with historical
  `review: cancelled recovery` hold metadata.
- No consumer claim or receipt.
- No staging path at
  `/processing/recovery/.staging/70a9015f567db76ce0183b149c371c84`.
- No published path at
  `/processing/recovery/published/70a9015f567db76ce0183b149c371c84`.
- No artifact with ID `recovery-70a9015f567db76ce0183b149c371c84`.

The UI always requests `recoveries?include_terminal=true`. Its cancelled
rows have neither cancel nor prune actions. The backend already supports
excluding cancelled/imported records through `recoveries_visible` when
`include_terminal` is false.

**Finding:** this observed row is terminal history that the UI permanently
includes in its operational list without a history visibility control.
The inspected paths do not show a remaining copy to clear. Hiding this row
would not resolve the separate ownership/relocation defect.

**Proposed correction for review:** default the operational view to active
handoffs and expose an explicit terminal-history view/filter using the
existing API contract. Preserve audit records. If cancellation leaves owned
staging data in other cases, show that artifact's actual lifecycle action;
do not equate hiding a row with deleting files. This UI change has not been
implemented during the identity fix.

## 12. Authorized identity fix — deletion terminates move authority

After requesting independent review, the user explicitly asked to trace and
fix how the payload identity entered this zombie state. Work resumed only
on that lifecycle defect. No watchdog, periodic cleanup, path override,
force-release, or weakened identity check was added.

### 12.1 Reproduction and correction

The new regression uses the existing inventory APIs to allocate/write a
job, fault a relocation, retain the failed source, successfully delete it,
allocate a different job at that pathname, and reopen/reconcile inventory.
Before the fix the deleted record becomes retained. This matches the
observed old-artifact/new-allocation distinction.

The correction is confined to:

- [mod.rs](../crates/nzbd-state/src/artifacts/mod.rs): when deletion starts,
  atomically cancel outstanding relocations in the same transaction that
  transfers the artifact to deleting. At startup, reconcile committed
  deletion records before unfinished operations.
- [relocation.rs](../crates/nzbd-state/src/artifacts/relocation.rs): both
  publication commit and failure/reconciliation paths require the operation
  to still own its source generation, location, identity, and live state.
  Obsolete operations become cancelled without mutating the artifact or
  filesystem. Queued deletion authorization survives reconciliation during
  its undo window; cancelling deletion retains normal recovery behavior.
- Startup also restores terminal deletion state for records already revived
  by the old code, using the existing successful deletion journal as the
  authority. Artifact IDs are allocation identities, not reusable path IDs.
  This correction touches only inventory metadata and records an audit
  event; it does not examine or remove the replacement directory. Independent
  scratch artifacts retain their own review/ownership state.

The governing invariant is that a successful deletion is terminal for that
artifact, and an old relocation cannot recover authority over its pathname.
This is enforced at the lifecycle boundaries, with recovery of legacy
inconsistent state in the existing startup reconciliation sequence.

### 12.2 Verification and deployment status

New tests in
[relocation_failure_tests.rs](../crates/nzbd-state/src/artifacts/relocation_failure_tests.rs)
cover deleted sources followed by replacement, legacy revived records,
idempotent startup repair, valid publication whose source authority has
ended, and pending deletion/undo. Existing successful interrupted-move and
registry-publication tests also pass.

`cargo test -p nzbd-state`: **119 passed, 2 ignored**. The first sandboxed
run could not create an existing test's Unix socket; rerunning with the
required local fixture permissions passed. `cargo clippy -p nzbd-state
--all-targets -- -D warnings` passed.
`cargo test -p nzbd-post --test pp_pipeline` also passed all **33** tests
after the lifecycle correction, exercising its post-processing callers.

The code is local and uncommitted. Production still needs an isolated,
reviewed deployment of this fix; the broader directory/processing candidate
must not be implicitly included. The cancelled-handoff UI finding remains
documented for review. Neither the original extraction failure nor the
relocation `EINVAL` is explained or fixed by this lifecycle correction.

Review only this correction with:

```bash
git diff -- crates/nzbd-state/src/artifacts/mod.rs \
  crates/nzbd-state/src/artifacts/relocation.rs \
  crates/nzbd-state/src/artifacts/relocation_failure_tests.rs
```

## 13. Independent review — corrected diagnosis and remaining design work

The supplied [Opus review](DOWNLOAD_INCIDENT_OPUS_REVIEW.md) adds production
filesystem probes, deployed source provenance, archive-header observations,
and baseline reproductions. Those observations are attributed to that review;
this follow-up checked the relevant local source and public contracts rather
than repeating production mutations/probes.

### 13.1 Accepted corrections

- The deployed image corresponds to `9d3288b`, according to the review's
  build/reflog evidence. The failed RAR4 jobs predate it and require analysis
  against the earlier build. The relevant state/post code is unchanged
  between the local baseline and the deployed revision.
- The review measured unsupported flagged renames on both payload mounts:
  NFS 4.1 at `/working` and GlusterFS FUSE at `/processing`. The local
  `rename_exclusive` has no production fallback. Linux documents that
  `RENAME_NOREPLACE` requires filesystem support and that unsupported flags
  can return `EINVAL`: [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html).
- The review traces parking/recovery publication failures to that primitive.
  Local `safe_rename` also swallows rename errors, allowing downstream
  extension-based detection to miss obfuscated archives. The reviewer
  identifies false SUCCESS on jobs 1699 and 1702 and failed deobfuscation on
  1700. This is a live correctness issue, not merely a hypothetical archive
  guard requirement.
- RAR4 ordering is a separate defect: the reviewer inspected headers showing
  head/continuation volumes misnumbered after lexical sorting. Local
  `rar_rename` parses RAR5 volume numbers but has no equivalent RAR4 ordering.
  Unknown order must not be treated as lexical order merely to produce a
  filename. Fixing filesystem support will expose this defect again.
- Pausing freed job 1703's slot and the scheduler selected 1704. Allocation
  rejection then repeatedly stopped lease generation without changing the
  job state. Local `grant_work` confirms that `break` behavior. Sleeping
  workers have no epoch change from the artifact deletion. The immediate
  scheduler fix belongs in queue-owner eligibility/hold transitions.
- NZBGet explicitly defines empty `InterDir` as downloading into `DestDir`:
  [configuration source](https://github.com/nzbgetcom/nzbget/blob/develop/nzbget.conf).
  The actual path-setting defect is that a nonempty configured `inter_dir`
  is ignored by engine wiring. Reinterpreting `main_dir` was wrong.

### 13.2 Rejected candidate changes have been removed

The working tree no longer contains the proposed `download_dir()` fallback,
overlap validation, changed engine/PP roots, inventory path cache, altered
publication/script ordering, or extension-only archive guard and its tests.
The associated scan-root and cluster-worker changes were also removed.
This only reverses changes made in this investigation. The prior candidate
diff was saved at `/tmp/nzbd-pre-review-candidate.patch` for audit/recovery.

Remaining application changes are the admission helper and URL parity,
Files select-all/retention presentation, and the three-file identity
lifecycle candidate. These remain uncommitted and undeployed.
After removing the rejected changes, `cargo test -p nzbd-engine` passed
170 unit tests and 22 end-to-end tests. This verifies the retained admission
candidate; it does not resolve the scheduler or filesystem defects.

### 13.3 Two proposed fixes need stronger contracts

**Directory reservation is not strict exclusive rename.** A successful
`mkdir(target)` followed by an identity check and plain `rename(source,
target)` has a check/use race. Another actor can replace the reservation
with a different empty directory after the check. Ordinary rename can
replace that unrelated empty directory. `ENOTEMPTY` protects a populated
destination, but does not establish the promised no-replacement invariant
for every object. Do not silently implement this as an equivalent fallback.
Either retain strict semantics through a reviewed publication design, or
explicitly choose and document a coordinated-writer contract. The user has
been asked which contract to preserve. No fallback has been implemented.

File link/unlink publication has a different contract and needs its own
crash, source-replacement, filesystem-capability, and duplicate-name tests.
Evaluate the shared primitive and all callers together; do not add a
mount-name or job-specific exception.

**Held allocation errors cannot currently use generic resume.** The review
correctly diagnoses starvation, but its proposed resume path does not match
local source: `QueueCommand::Resume` returns false whenever `j.held()`;
`ReleaseResourceHold` only releases causes `capacity` or `quota`. Therefore
`hold_job(..., "storage_error", "download_write", ...)` plus `continue`
alone would create another non-resumable state. The correction must include
an explicit validated allocation-retry transition through the queue owner,
persist it, and wake workers there. Preserve the existing restrictions for
other resource/custody holds. Test both starvation prevention and actual
resume through the public action path. Writer retirement is transient and
must not be classified as an operator review hold.

### 13.4 Generation-scoped deletion is required before shipping section 12

The reviewer correctly flags deterministic scratch IDs. The current delete
request persists `(artifact_id, revision, undo_seconds, automatic)` and
does **not** record the deleted generation. Section 12's startup repair
therefore has insufficient evidence to apply a successful old deletion to
an arbitrary current record with the same ID.

Required correction: persist generation with deletion authorization and
enforce it on execution, retries, and reconciliation. A delete for generation
A must never cancel generation B's operations or mark B deleted, even when
the artifact ID or pathname is reused. Add that exact regression. Specify
how legacy journal entries without generation are handled conservatively;
do not infer their authority from the current record or silently claim the
legacy zombie repair remains safe for all IDs.

An unresolved relocation with a live source also needs an explicit abandon
transition, scoped to its generation and operation. It must settle the old
operation and describe retained source/scratch disposition. Merely releasing
a review hold lets the same operation reapply it at the next startup.

### 13.5 Implementation order and release boundary

1. Define and test filesystem publication semantics, and propagate PP rename
   failures so they cannot become SUCCESS. Preserve existing settings.
2. Implement allocation failure isolation and a valid retry transition in
   the queue owner; retain the shared URL/direct admission allocation rule.
3. Correct RAR4 ordering from reliable evidence, with real multipart fixtures
   and unchanged RAR5/already-numbered neighbor cases.
4. Complete generation-scoped deletion authority and explicit relocation
   abandonment; retain the reproduced no-resurrection invariant.
5. Finish independent UI improvements, including the terminal-handoff filter.
6. Honor explicit `inter_dir` with the established empty-value behavior only
   after publication works and script-path compatibility is resolved.

A production rollback or deployment remains a separate operator decision;
neither has been performed. Passing the existing local candidate tests does
not validate the unresolved filesystem contract or the generation omission.

### 13.6 Extracted media with obfuscated, extensionless filenames

The user reports that the seven extensionless files in
`/working/monarr/completed/Heated.Rivalry.S01.2160p.AMZN.WEB-DL.DDP5.1.H.265-RAWR`
are already extracted media. Curator's manual scan reports no media files.
Do not conflate this report with the separately observed unextracted RAR
false-success cases. Individual file containers and recoverable naming
metadata in this download have not yet been independently inspected.

Source inspection establishes two pipeline gaps. `par_rename_owned` matches
PAR2 FileDesc names using size and full MD5 after a first-16-KiB hash filter,
but the manager invokes it before extraction. The final deobfuscation pass
does not repeat that evidence-based recovery for newly extracted PAR2/media.
Its basename renames preserve suffixes, leaving extensionless media without
an extension. The comment claiming there is no recovery evidence left after
unpacking is therefore an assumption, not an established invariant.

SABnzbd's [deobfuscation implementation](https://github.com/sabnzbd/sabnzbd/blob/develop/sabnzbd/deobfuscate_filenames.py)
provides PAR2 content-to-name matching and a separate content-based extension
recovery pass. Its [extension detector](https://github.com/sabnzbd/sabnzbd/blob/develop/sabnzbd/utils/file_extension.py)
uses file signatures. These are distinct from using the release/job name as
a fallback for one dominant file; a season title alone cannot identify every
episode in a pack.

Proposed responsibility: Runner must preserve and use available download and
extraction naming evidence, recover names through validated content matches,
and restore supported file extensions from container contents before handing
off results. Any post-extraction PAR2 pass must stay within the validated
payload namespace and operation lifecycle, preserve collision protections,
and run before evidence retirement. Curator owns library matching and import.
Do not conclude that episode identities are unrecoverable until the available
metadata has been examined. Do not infer episode order from random filenames;
the existing lexical season-pack numbering heuristic needs removal or a
replacement supported by actual per-file evidence.

The user's additional proposal is to probe extracted media with `ffprobe`.
This belongs in Runner's existing post-processing pipeline, after exact
metadata-based filename recovery and before final naming/handoff. Collect
structured format, stream, and embedded tag evidence (for example via
`-v error -show_format -show_streams -of json`), with the existing subprocess
timeout/output limits and validated local-file access. The
[official ffprobe documentation](https://ffmpeg.org/ffprobe.html) describes
container/stream identification and embedded metadata reporting. No existing
ffprobe integration was found in Runner's crates during this inspection.

Use container evidence to recover an appropriate extension, distinguishing
ambiguous format families rather than blindly mapping a demuxer name to an
extension. Use meaningful embedded titles or episode tags as naming evidence
when consistent with the job and stronger exact mappings. Preserve conflicting
or incomplete evidence for Curator; never manufacture episode order. Record
the original name, resulting name, and evidence used so recovery is auditable.
A successful probe establishes recognizability and readable stream metadata,
not complete-file integrity or successful decoding of every frame. Retain
PAR verification and extraction checks as separate results. Missing tools,
timeouts, malformed files, absent tags, conflicting titles, and duplicate
targets require explicit outcomes and regression coverage.

This is a proposed addition to the fix scope, not an implemented or validated
repair. Tests must distinguish exact name recovery, extension-only recovery,
absent/ambiguous evidence, collisions, and extracted metadata discovered only
after unpacking. A failed required rename must not silently report success.
