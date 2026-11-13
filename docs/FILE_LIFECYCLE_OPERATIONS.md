# File lifecycle operations

**Status:** implementation under final preparation · **Updated:** 2026-11-07 (pipeline contract v2 M3: per-file publication, no acceptance)

Runner records payload ownership separately from History. Removing a History
row does not abandon its files. Unknown folders are retained for review, and
failed or pending deletion preserves the retry record. See the
[delivery status](archive/FILE_LIFECYCLE_STATUS.md) for review and test results.

## 1. Enable and inspect

Settings → Dev → **Enable file lifecycle automation** controls periodic
discovery scans. Failed-payload expiry is not behind it (0.9.0): a parked
failure expires at its deadline either way. Enablement always accepts the
operator's choice; readiness information is advisory. Manual inspection, Keep and recovery remain available.
A specific destructive action still requires verified ownership and no active
writer, recovery claim or Keep hold.

Expanded queue and History details show **Keep on failure** when Runner can
identify the current retained source. This reads and changes the live preference;
the choice recorded when a Delivery was admitted remains historical evidence.
The preference remains available after the queue entry retires. An unavailable
source or failed lookup explains why the toggle cannot be used. Save failures
remain visible and do not claim a changed preference.

Curator-owned history explains why requeue is unavailable: acquire again with a
fresh **Grab** in Curator, or retry the retained source. Removing such a queue
entry does not offer an Undo that would reuse its consumer request token.
Ordinary parked jobs retain their existing Undo/requeue behavior.

New owned failures use seven days by default. Expiry is the wall-clock
deadline alone: it is the same across restarts and downtime, and Keep or a
hold suspends it. Zero days means indefinitely. An owned allocation whose job
is not in the recovered queue at startup is a parked failure with the same
retention (it was a review hold before 0.9.0); importing the job reclaims
it. Removing Keep or applying
a changed policy starts a fresh period. Existing unknown folders never acquire
retroactive expiry merely because automation is enabled.

In **Files**, request a scan. Scans run as durable operations and start the
moment they are admitted (the 30-second maintenance tick is only the safety
net); a scan measures every folder it discovers, so a discovered row carries
its file count and size straight away. A row showing `—` has not been
measured — a walk failed and the reason sits on the row, or the folder was
recorded by an older build — and **inspect** walks it. A root that does not
exist yet (the failed directory before the first parked failure) is skipped,
not a scan failure. Open a folder's **details**; adopt unknown files only
after reviewing the file list. Adoption enables Keep. Newly added or replaced
content revokes ownership when inspected. A reviewed hold can be released
explicitly; active recovery claims cannot be released that way. The list is
filtered (**Needs attention** = live Runner rows with an error, a blocked
cleanup, an open intent, a hold with no automatic exit, or past their
registered exit; **Not Runner's** = live content Runner did not create,
informational only),
searched, sorted and paged on the server; the counts on the filter chips
describe the whole inventory, not the current search.

**Preview retention for existing failures** shows up to 1,000 exact IDs and
revisions. Apply checks the whole selection in one transaction. A changed row
requires another preview. Kept, held, unknown and unchanged rows are excluded.
Repeat the preview to process another batch if necessary.

Manual Files deletion has an eight-second Undo window, and the deletion runs
as soon as the window closes. Native and compatibility
callers that request terminal deletion do not add that delay. An existing UI
Undo window is preserved. A pending operation is never reported as HTTP success;
retry the same request. Transient deletion failures retry after 1 minute,
5 minutes, 30 minutes and then 6 hours. Identity conflicts require review.

Expanded queue and History details read the exact `Delivery:Id` reference stored
with the job. Files → **Published deliveries** loads timing and cleanup details
on demand. Oversized publication responses are refused with a review reason;
opening a job never scans the Delivery collection to guess its receipt.

Measured queue, fetch, download and processing steps remain separate for each
machine and process session. A subtotal counts overlapping completed steps once,
using recorded monotonic endpoints. The immutable publication snapshot measures
queue, URL fetch, download and processing through READY. There is no consumer
acceptance (pipeline contract v2 §2.5): curator owns a publication from the
moment its `.runner-manifest.json` exists, and Runner does not invent a
handoff duration ending after the snapshot. The wire contract also accepts a
future explicit `handoff` interval.

Evidence stays bounded at 128 major intervals. Omission counters are conservative
lower bounds: **at least N omitted**, and any subtotal describes the **recorded
subset**. Replay keeps stable observation identities so foreign completed worker
measurements survive late imports without being counted twice. StageSpan-based
processing details lack historical per-span machine/session identity; their
origin remains explicitly unrecorded, including after restart. Missing or interrupted measurements stay
unknown. Processing details expand the major steps and are not added again.
Publication and source-copy cleanup follow separately. Copy
status, retry attempts and errors lead; exact references can be expanded. Retry
source cleanup uses its existing independent action, and polling preserves its
button and focus. Phone queue and History details show the same receipt and
provide visible retry results.

## Failed deletion review

A changed manifest or legacy device-only root identity stays in review. Normal
inspection, expiry and deletion never relax those checks. To explicitly review
the *current* bytes, use `GET /api/v1/artifacts/{id}/cleanup-review`. It hashes
regular files through descriptors and returns old/current root and allocation
identities, the current manifest, revision, generation and a fingerprint. A
changed root or payload inode, missing allocation marker, active writer,
recovery or publication move refuses the preview. Legacy remount review also
requires a current stable mount identity; an unmounted fallback cannot pass.

After reviewing that exact root and content, POST to the same endpoint with
`revision`, `generation`, `fingerprint` and `acknowledged: true`. Any intervening
metadata/content change refuses acceptance. Present files receive a **new
generation**, fresh immutable allocation sidecar and **Keep**, with automatic
cleanup disabled. Old delete approvals are cancelled. Acceptance deletes
nothing; release Keep and submit a new ordinary guarded deletion separately.
An explicitly reviewed absent payload becomes `source_gone` after current-root
verification. A failed database commit leaves the old allocation sidecar intact.
Review acceptance is recorded in the artifact event ledger.

## 2. Recover media into Curator

1. Inspect and, if needed, adopt the source. Select regular media files.
2. Preview staging capacity, then stage the selection. Keep may remain enabled.
3. Mount only `<recovery-root>/published` in Curator as `/recovery:ro`.
   Never expose `.staging`, the entire processing directory or a library root
   through this mount. The default recovery root is `<main-dir>/recovery`.
4. Configure Curator's Dev settings with `/recovery`, Runner's exact published
   prefix and the same separate recovery credential configured in Runner.
   Normal Runner API authentication remains necessary too.
5. Curator Settings → Recovery selects the handoff, title, library copy, files
   and TV episode mappings. Inspect the destination preview and queue it.
6. Follow persisted activity, copied bytes and receipt delivery. Cancellation
   waits until the worker stops. Completed library files remain recorded.

Staging uses independent copied files, SHA-256 verification and an external
manifest. Digests prove equal bytes, not healthy media. Curator uses its bounded
native container parser; no external probe is launched for recovery. Recognized
but unverified media requires an explicit choice. Book recovery is outside this
initial media-recovery contract.

Peak storage may include the original, staging copy, library copy and an old
replacement. Runner requires full staging bytes plus 1 GiB free reserve. Curator
checks its library volume independently with the same reserve. Replacement
rollback links share the old inode on that volume until metadata commits.

Only a complete per-file receipt starts staging's 24-hour eligible-uptime
retention. Source retention restarts after receipt, respecting Keep and zero-day
policy. Unselected source files remain held. **Delete receipted source files**
removes only successfully imported/already-present files whose identities and
hashes still match; it has its own eight-second Undo window. Partial or silent
consumers never cause expiry. A missing heartbeat is diagnostic only.

## 3. Interrupted work

- Allocation intent precedes writer creation. Ambiguous allocation after a
  crash resumes without granting destructive ownership.
- Queue retirement waits for writer-stop acknowledgements. Startup reconciles
  interrupted retirement before resuming writers.
- Category moves and failed-file parking journal their source and destination.
  A destination root that cannot be created (for example a park volume that is
  not mounted) is journaled too: the move's operation waits on its retry
  ladder with the error (shown on the row), and the same move retries. Publication refuses an existing destination. Cross-volume whole-directory
  moves verify copies before exact-entry source cleanup. Incomplete scratch is
  retained for review.
- A selected publication moves **file by file** under a journaled intent
  (`relocation_intents`: generation, target, each file's identity, digest and
  destination), written before the first rename; `.runner-manifest.json` is
  written last. On restart each open intent is finished (every file found
  unchanged in the target or the source) or unwound (moved files put back by
  identity, the target removed, the job failed `io` and parked) — never both.
  While an intent is open, deletion, disposal and queue retirement refuse
  either side; the source shows in state `transitioning`. Same-volume files are renamed (no copy, one digest read at
  most); cross-volume files are streamed once, hashing as they are written. A
  published file that a retained PAR set still needs is linked instead, and
  its source name is retained as `retained_repair_dependency`.
  An unwind never removes the only copy: a cross-volume copy goes only while
  the source still holds the journaled original, a linked repair input's
  publication name only while the source name is the same inode; otherwise
  the move stops for review and its operation error is shown on the artifact.
  A stuck move (review or retry ladder) is abandoned from the Files detail —
  **Abandon move and retain original** — which unwinds it, or, where it
  cannot be put back safely, closes it without deleting anything and
  registers the publication directory (no manifest) for an explicit
  disposition; the job's next publication attempt fails `io` and parks the
  source.
  The processing directory then holds only unselected siblings, which Runner
  disposes of once the delivery row exists. See
  [INTEGRATION.md §2a](INTEGRATION.md#2a-publication-manifest-and-the-move-journal-pipeline-contract-v2-runner-040).
- A fully journaled recovery publication can finish after a crash by checking
  directory identity, manifest, sizes and hashes. Incomplete staging is retained
  and exposed for explicit review; it is not erased by matching a scratch name.
- Curator journals placement before filesystem publication. Library metadata,
  quality/provenance, episode links and each file receipt commit together.
  Outbox delivery retries independently of the browser or original HTTP request.

Do not manually erase SQLite journals, sidecars, rollback files, a directory
with an active claim, or a publication directory without its
`.runner-manifest.json` (reconciliation owns it until it finishes or unwinds). A review state is a request to inspect the recorded paths
and actual filesystem, not evidence that the files are disposable.

## 4. Backup and restore

Stop Runner before taking this offline inventory snapshot:

```bash
nzbd artifacts-backup --state-dir /state --output /backups/lifecycle-20260925
```

The output directory must not exist. `artifacts.sqlite` is copied using SQLite
`VACUUM INTO`, including committed WAL data, and external allocation identities
are copied with it. `backup.json` describes a completed snapshot. This command
does not back up media volumes, queue snapshots or History; include those in the
same operational backup window separately. Keep incomplete backup directories
for inspection until their failure has been understood.

Restore the matching inventory and `artifact-identities` while Runner is stopped,
then quarantine before starting the daemon:

```bash
nzbd artifacts-restore --state-dir /state
```

Quarantine enables Keep and review holds on surviving records, resets retention
clocks and invalidates pending operations. Never copy an old database into a
running daemon. Never restore old metadata while allowing pre-restore consumers
to keep modifying media. Raw rollback without quarantine is unsupported.
Curator's database and library must likewise be restored as a coordinated set;
keep Runner sources held while reviewing any recovered import/outbox state.

## 5. Platform and cluster boundaries

Linux and macOS use descriptor-relative, no-follow access and exclusive
publication. Windows keeps ordinary queue operation and inspection; destructive
ownership is refused where equivalent deletion primitives are unavailable.
This is an operation-level inability to prove safe deletion, not an enable gate.
The native Windows tests verify ACLs, queue/wire behavior, and that unsupported
descriptor-relative absence proofs and deletion refuse without touching payload.
Owned publication/retirement fixtures require Unix descriptors and run on Linux
and macOS; their exclusion does not certify those operations on Windows.

Each cluster node keeps its inventory in its local control directory. Imported
pre-existing worker payloads remain unowned. A remote writer lease must retire
before local queue deletion can quiesce writers. The initial operational rollout
is standalone: cluster enablement remains advisory, but it does not manufacture
cross-node deletion authority. Do not adopt a folder that another node can still
write. Shared-volume permission and device-identity behavior must be verified on
the deployment filesystem before using managed destructive operations there.

## 6. Diagnostics and limits

Runner `/metrics` includes `nzbd_artifacts`, `nzbd_artifact_operations` and
`nzbd_recoveries`, labeled only by state. Inventory events explain specific
operations. No metric label contains a path, job or recovery ID.

Lists are paged; file pages contain 200 entries. A manifest is bounded at 100,000
entries and depth 64; recovery selection is at most 1,000 files. Scans stop with
an explicit incomplete-result error after 2,000 directories. Terminal manifests
and detailed events compact after 90 days; minimal identity and idempotence
records remain. Files reported removed outside Runner are quiet tombstones.

During a filesystem outage, retry or review states remain visible. An unavailable
root is not proof that a payload is gone. Resolve mounts/permissions, then retry
or inspect the named operation. Keep is the immediate retention override.

## Resource holds and transform generations

Writer exhaustion preserves stable `.runner-file-<FileId>.part` storage and
pauses the same job. Each acknowledged segment has passed a data flush; restart
revalidates recorded ranges against CRCs rather than trusting sparse length.
Final media publication requires contiguous coverage, expected yEnc size and
full checkpoint CRC. Authenticated cluster segment leases may seal private
sparse checkpoints; the authoritative assembler validates their complete union.
These checkpoints carry no media-ready or successful post-processing stamp.

Ordinary files with missing articles or NZB coverage gaps also seal privately
once every listed article has finished. Received ranges must be nonoverlapping,
within the known yEnc size, and match their recorded CRCs. A durable
`*File:repair:<id>` parameter identifies the private checkpoint. PAR repair
copies matching blocks into its owned workspace and publishes only outputs
whose size and full MD5 match the PAR catalog. Unknown sizes, overlapping
ranges, and changed disk bytes still require review.

Insufficient or absent recovery data produces `PAR_FAILURE` in History under
the configured failed-file disposition. It does not leave a permanent queue
hold. Health reports article delivery, not file completeness; an NZB can omit
ranges despite 100% article health. When no usable PAR bytes are known, the UI
says **recovery data unconfirmed** instead of presenting the 85% fallback as a
repair guarantee.

A control-request timeout during failed-repair cleanup preserves the source.
Runner retries the live Keep read up to three times, with a one-second wait
between reads. If the owner still cannot answer, the job carries
`cause=engine_unavailable`, `retry_policy=scheduled_retry` and a retry time.
The normal hold backoff applies; a timeout does not establish bad payload data
or authorize deletion. Issued mutations are not replayed by the read retry.
The hold recovery task continues after request timeouts.

On upgrade, the exact legacy hold `subprocess failed: read live Keep setting:
engine is shutting down` is re-armed automatically. Manual pauses remain in
force. Once Runner can read the live job again, a failed repair completes as
`PAR_FAILURE` in History, preserving files when Keep is set. Other review holds
retain their existing policy. The
`legacy_repair_timeout_recovers_automatically_and_records_terminal_failure`
regression checks this recovery with Keep both set and clear.

Multipart posts may use a different obfuscated yEnc filename in every article.
Runner keeps the first safe confirmed filename for that NZB file, including any
collision suffix, while requiring all articles to declare the same total size.
Unsafe names, missing sizes and conflicting sizes still hold the job. The
`multipart_articles_with_different_yenc_names_download_bit_identically` engine
regression checks the assembled bytes with varying article names.

PAR restoration records source/target identities in the existing operation
journal. Damaged candidates repair in separate operation-owned workspaces.
Full size and MD5 are checked before output publication, and originals remain
retained. Extraction publishes individual verified files without replacing
shared parent directories. Original archives and transform workspaces remain
retained pending independently admitted retirement. A directory name alone
never grants stale-staging deletion authority.

Relocation probes exclusive publication before payload copying and fences
repeated running/reviewed attempts. The synthetic registry-generation path
returns its actual published path and retains source bytes under a durability
hold. Ordinary relocation cannot enable that fallback. Native mount/remount,
server power-loss durability and every consumer mapping remain release gates.

## Durable job control contract (v1)

Runner queue snapshots and `job_control_changed` events carry an additive
`control` object: integer `version`; decimal-string `revision`; `instance`;
`lifecycle`; `cause`; `stage`; `retry_policy`; bounded `message`. Optional
`previous_status` and `manual_pause` preserve Runner resume intent. Version 1
resource holds use `lifecycle=held`, `cause=capacity|quota` and
`retry_policy=resume_same_job`. Review holds use `retry_policy=review`. Stage
names identify the failed operation, including `download_write`, `finalize`,
`extract`, `par_repair` and existing post-stage strings.

Compatibility projection is `status=paused`, `pp_done=false`, `ready=false`.
The authoritative control is stored in the Runner queue snapshot under
`*Control:v1`; it is persisted before events. Monarr stores it atomically with
active acquisition state. Missing facts cannot clear a held row; unknown
versions remain nonterminal, stale or conflicting control revisions are rejected,
and instance changes require explicit reconciliation. Identical running control
revisions still accept fresh transfer progress and stage observations. The SSE cursor is independent
of the durable revision. Polling held facts take precedence over older history.

`POST /api/v1/queue/{id}/resume` in Monarr calls Runner's existing
`POST /api/v1/jobs/{id}/actions/resume`; it does not add an NZB. Runner requires
writer quiescence, capacity reservation and a bounded 64 KiB write/flush probe.
Attempts and exponential backoff survive restart (eight attempts; initial
30-second backoff). Quota release is explicitly operator-triggered because
`statvfs` supplies no authoritative quota headroom. Manual pause and review
holds survive resource release.

The web and mobile queues put held jobs under **Needs attention**, with the
recorded reason. Failed jobs and torrents with missing files or storage holds
also belong there. **Paused** contains manual pauses. **Waiting** contains
queued work, and downloaded jobs awaiting verification belong under
**Waiting to post-process**. Held jobs take precedence over stale stage spans
and are not presented as reorderable download work.

The web queue labels these jobs **HELD** and shows their recorded reason.
Resource holds keep **resume**, which requests the recovery probe. Review holds
show **needs review** without offering an ineffective resume. The native job
action API returns HTTP 409 with the hold reason when resume is refused; HTTP
404 means the job is actually absent. New holds also appear in Logs.

An upgrade preserves unrelated holds. If an older build stopped an obfuscated
post with `conflicting yEnc name or declared size`, inspect the article metadata
before retrying: that old message does not distinguish a harmless name change
from a real size conflict. After confirming the name-only case and upgrading,
delete the queue job and requeue it from History to start a fresh allocation.
This repeats the download; it does not reuse the held partial payload. Existing
partial files remain subject to the normal Files retention and review policy.

The identical `job-control-v1.json` fixtures preserve a revision larger than
JavaScript's exact integer range and include an unknown future field.

### Review regressions (2026-09-30)

The post-processing manager preserves a stage's already-persisted hold instead
of reclassifying its error wrapper. Extraction retries allocate new generations
in the ownership journal, verify prior directory identities after restart, and
retain previous outputs. Allocation interruptions and identity changes require
review; directory names alone never grant reuse or cleanup authority.

Isolated PAR repair requests missing delayed recovery blocks, waits for writers,
and refreshes the same set before retrying. Restored filenames and protected
catalog names remain relative to each set root, keeping nested episodes apart.
Terminal history and `job_pp_finished` params preserve `*Control:v1` so a
consumer can recover a missed resume transition after the queue row retires.
The review probes in `failure_regressions.rs`, `pp_pipeline.rs`, and the
workspace restart test in `transforms.rs` cover these paths.

### Recovery of legacy coverage and RAR holds

On startup Runner revalidates received ranges and retries the exact legacy
`file coverage or expected size is unverified; partial retained` hold using
the private repair path. Manual pauses and unrelated holds remain intact.
Invalid sizes or overlapping ranges are held again with their existing bytes.

RAR restoration accepts SFV archive names only after matching whole-file CRCs
and rejecting ambiguous mappings, unsafe paths, and occupied targets. This
supports old RAR4 sets that omit the optional volume number. Existing holds
with the exact missing order/membership message retry after custody validation.
Extraction still validates the restored archive; SFV naming is not a successful
extraction verdict.

Regression coverage lives in `missing_articles_finish_download_but_keep_partial_private`,
`private_partial_repairs_with_par_and_fails_without_par`, and
`sfv_restores_obfuscated_old_rar_without_volume_numbers`.

## Failed publications, failed transforms and rediscovery

- **A copy publication whose job then failed** (a `publish` failure after the
  bytes were copied out) never becomes a delivery, so the source retirement it
  deferred "until the delivery row exists" would never fire. Once the failure
  is durable, the processing copy follows the job's own disposition: under
  **Delete** (the published generation deleted) it retires by closed scope;
  under **Park** it is parked in place with the failed TTL — recovery input
  (PAR volumes, archives) is never wiped by a park. A blocked retirement is
  parked with the TTL too. A job or row that keeps its failure never
  qualifies.
- **A failed job's unfinished transforms** (`extract`/`par_repair` journals
  still `running`/`retry`, often with their workspace gone) end with the failed
  attempt: the journal becomes `failed`, a missing workspace `source_gone`, and
  the source leaves `transitioning`, so the disposition can move it. No file is
  touched.
- **Every incomplete disposal is bounded.** One that holds no operation ladder
  of its own retries on a durable ladder (1 min doubling to 6 h); after 12
  attempts (about a day) it settles and every live row of the job is parked
  with the failed TTL in the same transaction. Retry now bypasses the wait
  and does not spend the budget.
- **An interrupted allocation** at a name only Runner gives (a transform
  key, a `.job-<N>-` directory) stays Runner's: empty and orphaned it is
  removed (never recursively); with bytes it is parked with the failed TTL;
  for a live job it becomes its allocation.
- **A scan never records a publication still landing** (an open intent's
  target, an unresolved move's destination, a directory containing one,
  `.runner-*` staging). Runner publications an older build recorded as
  unknown/Keep/review are folded back: a duplicate of Runner's own row (same
  directory object) is closed with a pointer to it; an unregistered
  directory whose verifying `.runner-manifest.json` names one of this
  installation's generations becomes `transferred`. Anything else stays
  **Not Runner's**.
- **An interrupted writer retirement whose directory is already gone** is
  settled `source_gone` at startup instead of re-arming its hold every boot.
