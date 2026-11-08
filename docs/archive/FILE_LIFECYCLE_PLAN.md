# File lifecycle — retained failures, orphan detection, and recovery

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [FILE_LIFECYCLE_OPERATIONS.md](../FILE_LIFECYCLE_OPERATIONS.md).

**Status:** revised after agent and Opus review; full scope retained;
implementation in progress; see [delivery status](FILE_LIFECYCLE_STATUS.md) ·
**Written / reviewed:** 2026-09-25 ·
**Scope approved:** retention, durable ownership, attention view, and Curator
recovery handoff · **Existing directory cleanup:** completed by the user.

**Code inspected:** Runner `f959cf922b47c581713df9401003c7c6f96cf5f7`;
Curator `0da1c755da44fdb0cd7bf615997016c8c096dc5c`. These are local source
revisions, not proof of the deployed containers' revisions.

Companion to [CONFIGURATION.md](../CONFIGURATION.md) (failure policy),
[ARCHITECTURE.md](../ARCHITECTURE.md) (queue and post-processing), and
[INTEGRATION.md](../INTEGRATION.md) (Curator handoff). Read §§1–4 for the
problem and decisions, §§5–10 for implementation contracts, and §§11–14
for delivery and review. Re-verify source symbols before implementation.
If an implementation changes ordinary download readiness, History cursor
meaning, or cluster publication authority, review that change separately.

This document is the approved design contract. The [delivery status](FILE_LIFECYCLE_STATUS.md)
and [operations guide](../FILE_LIFECYCLE_OPERATIONS.md) distinguish implemented
behavior and verified results from remaining acceptance work. It does not request another cleanup of the user's files.
The independent findings and their disposition are recorded in
[FILE_LIFECYCLE_REVIEW.md](FILE_LIFECYCLE_REVIEW.md).
The user reaffirmed that this initiative includes both the immediate fix
and the proposed robustness improvements. Review-driven changes refine the
contracts; they do not remove ownership, retention, visibility, or recovery
from scope. F0 isolates the confirmed bug before F1–F6 deliver the full plan.

## 1. Problem — files outlive the records that explain them

### 1.1 The field report exposed two separate lifecycle gaps

The user found an apparently unmanaged `failed` directory and old media
folders under `/processing`. Inspection on 2026-09-25 found:

| Observation before manual cleanup | Explanation supported by evidence |
|---|---|
| Runner working directory `/processing/`; failed-file policy `park` | Runner intentionally retained failed payloads under `/processing/failed` |
| 64 failed folders, approximately 169 GiB | Parking had no automatic payload expiry |
| Blindspotting folder, approximately 23 GiB, file dates July 3 | No matching job in the active queue or 997 retained History entries |
| Heated Rivalry folder, approximately 29 GiB, file dates July 18 | Five named episodes and a temporary file; no matching active or retained job |
| Curator mounted completed downloads, but not `/processing` | It could not recover these files through its normal import path |

Directory ages are filesystem observations, not reliable failure timestamps.
The original event that stranded the two July folders was not established.
The user has since cleaned up the directory; these figures describe the
incident, not current disk usage or a remaining recovery task.

Two hypotheses require different evidence. The History `delete-files`
failure path demonstrably loses a record while leaving files; it is a
confirmed defect to fix independently, but no retained log links it to
these July folders. A temporary file *inside* Heated Rivalry does not prove
a leaked PP move: the current helper creates a sibling named
`<target>.pp-move.<tag>`, whereas the observed folder held `2236.out.tmp`.
F0 checks available historical logs/records and naming behavior. If cleanup
has removed decisive evidence, report the incident cause as unresolved;
do not delay the confirmed fix or manufacture an RCA from a similar symptom.

The same inspection saw four Curator downloads waiting for import for about
five days. Those used the completed-download tree. Their cause was not
diagnosed and is not attributed to this defect.

### 1.2 Existing implementation explains why the files remained

| Existing seam | Behavior and gap |
|---|---|
| [`PostSection`](../../crates/nzbd-config/src/lib.rs) | `failure_action` supports `none`, `park`, and `delete`; default is `delete`. `failed_dir` defaults to `<main_dir>/failed` through daemon wiring |
| [`dispose_failed`](../../crates/nzbd-post/src/manager.rs) | Moves, keeps, or deletes a failed job's directory and reports the resulting path; there is no parked-payload retention worker |
| [`handle_failed_job`](../../crates/nzbd-post/src/manager.rs) | Persists failure finalization state and retries disposition; stable errors eventually leave files with an explanatory History note |
| [`HistoryDb::prune`](../../crates/nzbd-state/src/history.rs) | Trims records and their spooled NZBs; it does not remove the corresponding media payloads |
| [`history_action`](../../crates/nzbd-api/src/lib.rs) | `delete-files` currently attempts recursive deletion, then deletes the History record even when file removal reports false |
| [`move_dir` / `copy_dir_all`](../../crates/nzbd-post/src/manager.rs) | Uses a scratch directory for cross-filesystem moves; some synchronization errors are discarded. Recovery needs checked durability and operation-specific ownership |
| [`storage_roots`](../../crates/nzbd-config/src/lib.rs) | Lists writable roots for capacity monitoring; this is not evidence that every descendant belongs to Runner |
| Curator `ScanImportPath` / `QueueManualImport` (curator `internal/app/acquisition/manualimport.go`) | Supports preview and asynchronous imports, but not a Runner recovery manifest or durable cross-service receipt |
| Curator `StartImporters` (curator `internal/app/acquisition/importers.go`) | Already separates copying from the control loop and recovers interrupted manual imports; reuse this execution model |

The present system has job execution and historical reporting, but lacks a
persistent lifecycle for bytes left behind after execution ends. A folder
name, a successful HTTP request, or a History pickup flag cannot fill that
gap. They do not prove ownership or successful library placement.

Existing Rust boundary to preserve while introducing the coordinator
(copied from the inspected PP manager; re-verify at build time):

```rust
pub async fn dispose_failed(
    cfg: &PostConfig,
    job_id: JobId,
    dir: &Path,
    dest_dir: &Path,
    dir_name: &str,
    tag: &str,
) -> Disposition;

pub struct Disposition {
    pub note: String,
    pub files_at: Option<PathBuf>,
}
```

`files_at: None` reports absent payload, not durable proof of the inventory
transition. The coordinator must persist its own intent/outcome while
continuing to supply accurate disposition and terminal-history behavior.

## 2. Intended behavior — each retained folder has an explanation

### 2.1 Four product changes

1. **Expire parked failures after a visible retention period.** Proposed
   policy is seven days, with per-folder **Keep indefinitely**. Show failure
   reason, size, expiry, and cleanup outcome. Existing installations must
   explicitly enable expiry; upgrading must not silently change `park`.
2. **Keep ownership independently of History.** Every newly created Usenet
   job directory, retained failure, recovery copy, and owned scratch area
   has a durable identity and operation state. History can be forgotten
   without losing control of the bytes.
3. **Show Files needing attention.** Reconcile disk, inventory, and live
   jobs. Explain parked failures, unknown folders, inaccessible paths,
   interrupted recovery, and cleanup failures. Unknown means review is
   needed; it does not mean garbage.
4. **Recover selected files through Curator.** Runner stages a durable,
   independent copy in a dedicated recovery area exposed read-only to
   Curator. Curator identifies the
   target and imports exact selected files. Runner reports success only
   after Curator durably confirms the selected results.

### 2.2 User journeys that must work

| Trigger | Result |
|---|---|
| A repair fails with parking enabled | Row reads “Parked failure · repair failed · 12 GiB · expires Oct 2”; Keep, Inspect, Recover, and Delete are available when eligible |
| User selects Keep indefinitely | Deadline is removed durably; restart and History trimming cannot re-enable deletion |
| An unknown folder appears in the working root | Attention row shows first observed time, measured size/freshness, and “No tracked job”; no expiry is assigned |
| A configured mount disappears | Show “Storage unavailable”; do not mark its contents deleted or automatically recreate the missing mount |
| User recovers three episodes from a failed pack | Preview identifies exact files; Curator asks for title/episode mapping; unselected files remain accounted for |
| Curator imports only two of three episodes | Show “Partial import: 2 of 3”; retain source and staging; retry the remaining file without repeating completed work |
| A deletion fails | The row remains with the error, remaining bytes, and retry state; History is not falsely reported as file removal |
| User cleans up a folder externally | A complete scan records “Removed outside Runner”; no repeated orphan alert and no invented deletion receipt |

## 3. Scope — preserve download and library responsibilities

Runner owns source retention, filesystem reconciliation, staging, and its
own cleanup. Curator owns media identity, episode mapping, quality policy,
library placement, and the evidence that placement committed.

Initial delivery covers all four features on **standalone Runner**. Cluster
mode may expose read-only observations; automatic expiry, adoption,
recovery, and new inventory-driven deletion are disabled there until they
use the replicated authority and output-generation contract in
[CLUSTERING_STATUS.md](../CLUSTERING_STATUS.md). A local SQLite lock is not a
cluster fence. This release boundary requires reviewer acceptance (§14).

Non-goals:

- Do not auto-import or auto-delete unknown folders. Age is not ownership.
- Do not recursively manage arbitrary library, NAS, watch, or state trees.
  Writable roots can contain files created by other applications.
- Do not apply failed-file retention to successful completed downloads,
  torrent payloads, or active jobs. Torrent deletion keeps its existing
  backend-owned file-inventory workflow.
- Do not turn a failed download into a normal `ready: true` job or emit a
  synthetic successful `job_pp_finished`. Recovery is a separate workflow.
- Do not mount all of `/processing` into Curator. Only published recovery
  copies need to be accessible to it.
- Do not guarantee that a failed source is playable merely because it
  hashes consistently or has a recognizable media header.
- Do not add an automatic library quality upgrade or broad re-download
  mechanism. Curator's existing selection and replacement policy remains.

## 4. Decisions — ownership must precede destructive work

1. **Use a separate durable artifact inventory.** History remains a user
   activity record. Ownership and unresolved work remain until bytes are
   accounted for, even when History records and NZB spools expire.
2. **Use opaque IDs and generations, never names as identity.** Two jobs
   can have the same release name, and a path can be reused. Every mutation
   addresses an artifact ID and expected revision, then rechecks disk.
3. **Persist intent before touching files and outcome after durability.**
   SQLite and filesystem changes cannot share a transaction. Recoverable
   operation records bridge crashes between them.
4. **Treat legacy data as observation until reviewed.** A History path can
   suggest an owner, but cannot prove the contents still belong to it.
   Migration must not give old files a retroactive deletion deadline.
5. **Stage copies and wait for receipts.** Source media stays intact through
   a failed or partial import. Reflink when supported, otherwise copy; do
   not use hardlinks that let consumers mutate retained source bytes.
6. **Keep large work off requests and engine ticks.** Scans, hashing, media
   probes, and copies use bounded workers and cached results. The new view
   must not recreate the failure in [HISTORY_LOADING_PLAN.md](../HISTORY_LOADING_PLAN.md).
7. **Ship cluster mutations only with cluster authority.** Standalone
   safety does not generalize to a stale worker on a shared volume.

## 5. Durable model — records survive History retention

### 5.1 Store, identity, and backup

Proposed standalone store: `<state_dir>/artifacts.sqlite`, separate from
`history.sqlite`, using checked transactions and `synchronous=FULL`.
Use the repository's SQLite conventions, a schema version, and startup
migration transactions. The store is authoritative, not a derived cache.
Confirm the underlying filesystem supports the selected SQLite durability
mode; do not enable destructive workers on an unsupported shared-state
configuration. Persistent state and backup health are release checks.

An installation UUID plus an artifact UUID identifies ownership across
restarts and job-ID reuse. Ownership sidecars live under
`<state_dir>/artifact-identities/<artifact-id>/<generation>.json`, outside
all payload trees. They store schema version, installation/artifact IDs,
generation, root/path binding, and observed filesystem identity; no secrets
or source URLs. Sync them and their parent directories before committing
the corresponding inventory binding. A sidecar corroborates the trusted
database, never grants authority by itself, and is restored with state.

No ownership marker is placed inside an ordinary job, failed payload, or
category destination. Consumers must see exactly the media/supporting files
they would otherwise see. Moving or importing media cannot strand a marker
that prevents empty-folder cleanup. The recovery bundle's explicit manifest
is control metadata beside its `payload/`, within a dedicated recovery root
that ordinary consumers do not scan (§8.2).

Back up the inventory with SQLite's consistent backup mechanism together
with the installation identity. Do not copy a live database file without
its transactional state. On missing/corrupt inventory, disable mutations,
surface a persistent error, and reconstruct only review candidates from
sidecars and History. Unknown schema versions also disable mutations.

Restoration must go through an explicit offline restore command. It assigns
a fresh restore epoch, disables destructive workers, invalidates restored
delete intents/deadlines, and review-holds every nonterminal artifact and
recovery. An old snapshot can predate Keep or an import hold without any
change to the directory identity or sidecar; reconciliation cannot recover these
lost decisions. Reconcile consumer claims and receipts before allowing any
new claim or releasing a source hold. Restored cleanup intents require
fresh authorization after reconciling partially removed files.

The supported restore procedure sets a durable `restore_review_required`
flag and resets retention elapsed-time budgets before workers start. It
does not require an external checkpoint service or pretend to detect every
filesystem rollback. Raw database replacement or whole-volume rollback
without running restore quarantine is unsupported: neither a marker nor an
extra file on the same restored volume can prove that policy is current.
No restored deadline resumes solely because a filesystem scan succeeded.

### 5.2 Proposed logical schema

These are new tables, not existing History schema. Timestamps are UTC Unix
seconds; IDs are opaque strings; revisions are increasing integers.

| Table | Required fields and purpose |
|---|---|
| `artifact_roots` | `root_id`, `configured_path`, canonical path, volume identity, role set, availability, last complete scan; binds relative paths to verified storage |
| `artifacts` | `artifact_id`, `installation_id`, optional `job_id` and job generation, `kind`, `ownership`, `root_id`, relative path, filesystem identity, `revision`, lifecycle state, failure status/reason, first observation, parked time, last observation, measured logical/allocated bytes, measurement completeness |
| `artifact_retention` | Artifact ID, `policy_origin`, `eligible_at`, nullable wall deadline, required retention seconds, persisted observed eligible uptime, `keep_forever`, `review_required`; no mtime-based expiry |
| `artifact_files` | Artifact ID, file ID, relative path, kind, size, identity/version evidence, optional digest and probe result; paginated inventory, not one unbounded JSON blob |
| `artifact_operations` | Operation ID, artifact ID, expected generation, action, state, idempotency key, immutable request digest, attempt count, next attempt, source/target references, owned scratch path, error, creation/update times |
| `recoveries` | Recovery ID, source artifact/revision, state, selection digest, manifest digest, staged artifact ID, consumer binding, claim generation, last contact, Curator import ID, cleanup preferences |
| `recovery_files` | Recovery ID, selected source file ID, staged relative name, bytes, content digest, per-file import result and receipt ID |
| `artifact_events` | Monotonic sequence, artifact/operation IDs, time, actor, transition and structured error; bounded audit distinct from History |

Enforce uniqueness for installation/artifact identity, current owned path,
operation idempotency keys, recovery/file identity, and receipt identity.
Mutations use revision compare-and-swap within a transaction. File detail
pages use stable file IDs; requests cannot substitute arbitrary paths.
Persist actor identity from authentication, not a client-supplied label.

Retain nonterminal ownership and receipt records without time pruning.
Terminal audit may be compacted after 90 days, but keep a minimal terminal
tombstone and consumed operation/receipt identities so delayed retries
cannot resurrect work. Normal successful payload removal is a common
terminal path, not an attention alert: show **Imported; source removed**
when a consumer receipt exists, otherwise **Completed; source absent** with
no invented import claim. Collapse these rows out of the default attention
view. After 90 days drop bulky file inventories, sidecars, and audit detail
only for verified terminal artifacts, preserving compact identity/operation
receipts and unresolved claims. Never turn a reused path into an old owner.

Measure SQLite storage at 365,000 terminal artifacts (1,000/day for one year)
and at one million. At an illustrative 512 bytes per compact record, one
year is about 178 MiB before indexes/pages; actual measurements govern the
budget. Track terminal-record count and DB bytes, and investigate growth
above 1 KiB per compact artifact excluding separate operation receipts.
This is an estimate to validate, not a claim about current storage. No
unbounded per-file payload survives merely to support one small tombstone.

### 5.3 Lifecycle and classification are separate

```text
new job allocation ──> owned active ──> completed retained
                              │
                              └─ failure ─> disposition pending
                                               │
                                 park committed│
                                               v
                                         parked failure
                                          │           │
                           retention/delete│           │recover
                                          v           v
                                    delete pending   recovery hold
                                          │           │
                                removed / error       └─> receipt / attention

disk discovery ──> untracked ── explicit adoption ──> retained, review held
```

`ownership` is `owned`, `candidate`, or `untracked`. Lifecycle describes
active, retained, staging, awaiting import, cleanup pending, removed, and
error states. Retention is an independent policy. UI classification is
derived from all three, so “recovery blocked” does not accidentally erase
the original failure reason or retention hold.

New directory allocation must reserve inventory identity before admission
can create payload files; sync its external identity sidecar before publishing
the bound directory for writes. If allocation is interrupted, reconcile the pending
record rather than inventing a second identity. Moves update location via
an operation record; duplicate source/target copies remain tracked until
the move is fully resolved. Successful jobs remain accounted for but do
not receive a failed-payload deadline.

Integrate at the real writer/allocation boundary as well as post-processing;
tracking only `dispose_failed` would miss crashes before failure handling.
Inventory persistence failure blocks the relevant new mutation and exposes
an error. It must not silently degrade to untracked file creation.

**Explicit availability decision:** ownership is a core persistence
dependency once F1 is enabled. An unavailable/corrupt inventory pauses new
payload allocation and affected destructive/finalization work, with
**Ownership storage unavailable** in status, logs, and the intake response.
Existing writers with already committed ownership may finish writes; they
cannot publish an unrecorded relocation or report an uncommitted lifecycle
transition. Retrying store initialization is bounded; no busy loop blocks
the API. Do not acknowledge a new job as admitted unless its required
durable state committed. This changes admission availability, not the
`ready: true` wire meaning. It is a deliberate design decision in the full
robustness scope and a required F1 failure-injection review, not an incidental
side effect of an optional UI. Failing open would recreate unowned files.

## 6. Retention — seven days with explicit holds

### 6.1 Proposed configuration and upgrade behavior

```toml
[post]
failure_action = "park"       # existing key; keep the operator's choice

[artifacts]                    # proposed section; not supported today
mode = "observe"              # observe | manage; controls lifecycle actions
scan_enabled = true           # discovery can stop; ownership remains core
failed_keep_days = 7          # 0 means no automatic expiry
scan_interval_secs = 900      # 15 minutes; cached reads between scans
cleanup_interval_secs = 3600  # hourly eligibility pass
recovery_root = "/processing/recovery"  # outside completed/import trees
```

`observe` is the upgrade default: track new ownership and report candidates,
but do not perform new retention/recovery mutations. `manage` enables the
new actions after compatibility checks; existing `failure_action` behavior
continues independently. There is no misleading `off` value. `observe`
plus `scan_enabled = false` stops discovery and new feature mutations;
minimal ownership tracking remains a core persistence responsibility.
Unresolved durable intents are surfaced. Switching
modes cannot discard a hold or cancel an in-flight filesystem operation
without its normal cancellation procedure.

The proposed `recovery_root` default is `<main_dir>/recovery`; the
explicit value above illustrates the inspected deployment. Root changes
require restart and keep the previous root registered until it has no
unresolved artifacts. Reject configurations overlapping state, torrent,
watch, failed, completed/category destination, or library roots in ways that
break role exclusion. Recovery must not be within a completed/import tree,
or an ancestor of one. Nesting within the working root is permitted only
as its explicit excluded recovery role. Mount only its `published/` subtree
read-only into Curator; private staging is not exposed there.

Intervals must be positive; scan minimum is 60 seconds and cleanup minimum
is 60 seconds. Negative retention values and unknown modes are errors.
All new settings are exposed in Settings and redacted config round trips.
Policy and interval changes apply on save through the manager; root changes
follow the application's restart-required mechanism.

On first enabling `manage`, only future parked failures receive automatic
deadlines. Previously parked files show **Review retention**; applying the
policy to them is an explicit previewed action with a fresh configured
retention period, never an immediate deletion based on old timestamps.

For new owned parked failures:

```text
eligible_at = time parking durably completed
required_seconds = failed_keep_days × 86,400
wall_deadline = eligible_at + required_seconds                 (if > 0)
automatic_expiry = wall_now >= wall_deadline
                   AND persisted_observed_uptime >= required_seconds
                   AND all ownership/policy/hold checks pass
automatic_expiry = disabled                                   (if days = 0)
```

Persist a conservative lower bound on elapsed eligible daemon uptime for
each artifact. Accumulate monotonic deltas only while the policy is active
and no Keep/recovery/review hold applies; checkpoint at most once per minute
in a bounded transaction. Never count an interval twice across retries or
restart. Crash/uncheckpointed time and downtime earn no elapsed credit.
Starting a fresh policy resets both deadline and accumulated time. This
needs no host clock-sync signal inside Docker: forward wall-clock changes
cannot bypass the uptime bound, and backward changes only delay deletion.

The trade-off is explicit: seven days means **at least seven days observed
while Runner is running and eligible**, so downtime extends retention. The
UI shows the earliest estimated expiry as the later of the wall deadline
and `now + remaining_observed_seconds`, with **Paused while Runner is
offline/held** where relevant. The hourly cleanup pass can delay actual
removal further. Do not display a guaranteed wall date that ignores the
monotonic lower bound. Use the same dual-clock rule for automatic staging
cleanup, with its 24-hour duration.

Do not start the clock while a cross-volume park is incomplete. A job kept
in place by `failure_action = "none"` is visible but receives no automatic
expiry. `delete` retains its immediate-failure semantics and still records
unresolved cleanup if deletion fails.

Changing `failed_keep_days` affects future failures. Existing deadlines
change only through a previewed **Apply to existing** action. Selecting
Keep clears the deadline; removing Keep starts a fresh retention period.
History hide/forget, restart, and failed cleanup never shorten it.

### 6.2 Deletion requires current ownership and an exclusive operation

Before each destructive operation, the standalone owner must establish:

1. The persisted policy permits deletion and no review, Keep, recovery,
   import, or active-job hold exists.
2. The configured root is available on the expected volume. Parent storage
   access and a completed scan distinguish absence from mount loss.
3. The folder identity and generation match the inventory and external sidecar;
   the target is strictly within an allowed payload root and is not a
   protected root or an ancestor of one.
4. No live job, torrent inventory, other artifact, or pending operation
   references this path or an overlapping subtree. If authoritative state
   cannot be read, deletion cannot proceed.
5. A fresh recursive inventory has no unknown additions or substitutions.
   A mismatch becomes **Contents changed — review required**.
6. An exclusive per-artifact operation is durably reserved using the same
   coordinator used by Keep, recovery, adoption, and delete actions.

Use handle-relative, no-follow traversal and identity checks through the
operation, not `canonicalize` followed by an unchecked `remove_dir_all`.
Reject symlinks, special files, unexpected mount boundaries, and replaced
parents. Unlink only the exact owned inventory and remove directories only
when empty. Recheck identity immediately before each destructive step.
Platforms lacking the required safe primitives must disable destructive
actions and explain the limitation. Threat model: exclusive Runner writes
to owned payloads; hostile administrators modifying the filesystem are
outside that guarantee. Ordinary concurrent external changes must fail
closed when detected.

Persist `delete_pending` and its exact target generation, then remove owned
files, check synchronization errors, and persist `removed`. A crash after
removal but before recording completion retries to a confirmed absent
target on a verified available volume. Partial deletion retains the exact
remaining inventory and error. It never deletes the explanatory row.

The Files UI explicitly requests an eight-second Undo grace period; this
is new for file deletion, whose existing History button arms in place.
The new artifact API defaults to immediate authorized deletion and accepts
only `undo_seconds = 0` or `8`. Legacy/API and compat deletion requests use
zero grace; they never inherit the UI delay. An existing UI operation with
an active grace period cannot be shortened by a concurrent legacy request.
The durable operation is pending during the chosen grace period, and bytes
are untouched until it ends. After destructive work starts, the API refuses cancellation and
the UI stops offering Undo. Keep racing with expiry either commits first
and blocks deletion, or receives an explicit conflict; it never falsely
reports protection after deletion has begun. Automatic expiry is the
previously configured policy, with a visible deadline and audit event.

Cleanup errors retry at 1 minute, 5 minutes, 30 minutes, then every 6 hours;
the schedule survives restart. Permission errors remain visible. Identity
conflicts require review rather than repeated deletion attempts. Storage
unavailability and ENOSPC/EDQUOT block the relevant operation until storage
recovers; integrate observed space errors with the existing disk guard.
Never delete extra files or bypass a hold because storage is full.

### 6.3 Existing delete actions must share the ownership checks

Route existing native Usenet `delete-files` requests through the inventory
coordinator, including queue and History routes. Preserve terminal success
semantics: **no `202` or other `2xx` until verified removal**. Curator's
existing native adapter treats any `2xx` as completion and immediately marks
the payload removed; returning admission would make it stop cleanup retries.
Use the new artifact-operation routes for asynchronous UI actions.

**Consumer retry contract, verified in source:** Curator's native `do`
returns an error for `503`; its adapter does not automatically retry the
HTTP request. The acquisition sweep selects outstanding payloads again,
but both `CleanupPayloads` and `cleanupAfterImport` first call
`removeImportedDir` after a client error. That direct-disk fallback can
bypass Runner's Keep or recovery hold. Returning `503` alone is not a fix.

For native Runner-managed payloads, eliminate this direct-disk fallback.
Use typed adapter errors/results so queue-to-History fallback is only for
an unknown queue job (`404`), not a pending deletion, ownership conflict,
unavailable authority, or auth error. Leave `payload_removed_at` unset on
pending/refused outcomes and retry the same durable operation on later
sweeps; expose blocked cleanup in Activity. Other clients retain their
existing policy until separately reviewed. Recovery sources and published
copies never use generic `removeImportedDir`, even if a future client type
or path mapping changes. Deploy this Curator correction before enabling
Runner management where Curator has writable access to payloads.

Existing routes may wait up to ten seconds for the durable operation. If it
is still pending, return `503` with a retry hint and operation ID; the
operation may continue safely in the background. Repeated requests resolve
the same operation by installation/job generation/action, even after the
queue row retires. Preserve a terminal removal receipt for subsequent
idempotent success. Existing History success fields remain, but failures
never delete the explanatory History row. A legacy path without ownership
returns conflict and requires review, not recursive deletion on faith.

Queue deletion must first cancel and await writer/PP quiescence, durably
retire active ownership, and only then authorize file removal. The current
fire-and-forget deletion in the engine is not a quiescence receipt. Plain
queue removal may retain its existing admission response, but `delete-files`
cannot use that response to claim bytes are gone. Add an explicit capability
for clients if future versions want asynchronous deletion; do not retrofit
`202` into the existing success path.

Plain History forget still removes History, not payloads. Existing terminal
failure handlers, queue deletion, category moves, and compat deletion paths
must be audited for bypasses and routed through the same owner where they
mutate tracked payloads. Preserve NZBGet wire semantics; unsupported async
changes to compat require a separate contract decision, not a fabricated
success. Retained torrents continue to use their existing restricted path.

## 7. Reconciliation — observe roots without adopting their contents

### 7.1 Discovery scope and classification rules

Register role-aware roots from configuration; do not use the capacity
probe's root list as a recursive cleanup allowlist. Discover top-level
entries under the working, Usenet destination, configured category, and
failed roots. Inventory owned subtrees individually. Deduplicate canonical
overlaps and exclude exact configured state, temporary, watch, torrent,
cluster-control/generation, and recovery subtrees before descending.
Unknown scratch-like names are still unknown; a `.pp-move` suffix alone
is not ownership proof. Known scratch belongs to a persisted operation.

The root itself is infrastructure, so `failed` is labeled **Failed download
storage** and its children are classified individually. Never display the
whole failed root as a disposable orphan. Loose files outside known
directories may be attention items; their paths are not executable input.

| Classification | Required evidence | Automatic deletion |
|---|---|---|
| Active job | Current queue/PP ownership or pending admission | Never |
| Torrent/state/system storage | Configured role or backend inventory | Never through this feature |
| Completed retained | Durable owned successful payload | Never under failed-file retention |
| Parked failure | Owned failed payload with committed parking | Only under enabled retention and all §6 checks |
| Legacy candidate | A History path/name suggests a match | Never before explicit adoption and retention decision |
| Untracked | No owner or credible candidate association | Never |
| Recovery in progress / awaiting import | Durable operation or consumer claim | Never while held |
| Cleanup failed | Pending owned cleanup with failure evidence | Retry only while original authorization remains valid |
| Storage unavailable / scan incomplete | Read, mount, or bounded scan failure | Never |
| Removed externally | Verified root available and target absent | Record observation; do not claim Runner removed it |

An untracked item appears on the first scan with **Still observing**.
Recovery/adoption requires unchanged identity and inventory across two
complete scans at least 15 minutes apart, plus an explicit user action.
Stability is a prerequisite, not proof of ownership or complete media.
Any current job overlap immediately excludes adoption.

### 7.2 Bounded scans and honest output

Proposed initial limits: one directory scan worker, one copy/delete worker,
pages of 100 results, at most 500 API results per request, and bounded
enumeration batches of 1,000 entries. Persist enumeration progress where
possible; cap each scan slice at 30 seconds and 10,000 entries. A slice
limit means **Incomplete**, not empty. Network filesystem calls can exceed
application deadlines; cap outstanding blocking work and never launch
replacement workers indefinitely when a syscall is stuck.

Measure sizes lazily and cache the result with timestamp and completeness.
Distinguish logical bytes from allocated bytes; sparse files and reflinks
mean neither value is a promise of exactly that much newly freed space.
List/detail requests read the inventory only; they do not traverse disk,
replay History, or hash files. Hashing and media probes are explicit jobs.

Use metadata checks in background scans. Full content digests belong to
recovery copy verification; they are too expensive for every periodic scan.
Root errors appear once with affected counts and last successful scan time.
Mark missing artifacts only after completing the relevant root traversal.

## 8. Recovery — durable copies and explicit import receipts

### 8.1 Preview selects files, not an entire failed directory

Runner **Inspect** lists exact file IDs, names, sizes, known completion
evidence, and uncertain files. `.tmp`, partial segments, archives, and par2
files are excluded from normal media selection. A user may explicitly stage
an obfuscated regular file for identification; it remains **Unidentified —
not importable** until Curator probes it. Staging bytes is not import
approval. Never rename a retained source to make discovery work.

Curator owns content identification after publication, reusing
`probe.File` (curator `internal/infra/probe/probe.go`) and its native
`mediainfo` (curator `internal/domain/mediainfo/mediainfo.go`) readers.
The stock Curator image is distroless; `MONARR_FFPROBE` is an optional
fallback, not a tool known to be installed. Keep native MKV/MP4 byte budgets
and the existing 30-second optional external timeout; use one bounded
recovery probe worker. No new media-probe executable is added to Runner.
Do not infer unsupported content is healthy. External probes, when
configured, accept only local manifest files with bounded output and no
remote/playlist resource loading. Unsupported or malformed files remain
blocked with a reason. Book recovery is outside initial scope.

There are two previews: Runner's byte-selection/capacity preview before
staging, and Curator's content/target preview before import. Curator shows
probe results, staged names, episode mapping, and verification limits and
requires the explicit content/target decision there. Runner can display
Curator's reported probe summary via recovery progress, attributed to it;
it does not pretend to have measured the content itself.

The UI says **Media recognized; completeness not proven** unless stronger
verification exists. A successful probe is not a full decode test; a digest
proves that the copy matches its source, not that the source is healthy.
Report available par2 or original completion evidence separately. A user
may explicitly choose recognized but unverified media; that choice is
recorded, never described as fully validated.

For an unknown/legacy folder, **Adopt for recovery** binds the observed
identity and reviewed file inventory to Runner. It defaults to Keep and
does not grant automatic expiry. Any subsequent change invalidates the
preview revision. Existing failed records also receive a recovery hold
before any staging work begins.

### 8.2 Stage outside ordinary completed-download trees

```text
retained source (held)
        │ copy or reflink selected files
        v
<recovery_root>/.staging/<recovery-id>/<generation>/
        │ checked sync + source/copy digest verification
        v
<recovery_root>/published/<recovery-id>/payload/ + manifest beside payload
        │ Curator claim + user-selected library target
        v
Curator import worker ──> durable per-file results ──> Runner receipt
```

Example deployment: Runner uses `/processing/recovery`; the host directory
`/mnt/processing/recovery/published` is mounted read-only at `/recovery` in
Curator. Configure and validate that exact prefix mapping. Curator cannot
see private staging, source files, or Runner state through this mount.
Neither published nor private recovery lives within completed downloads,
so old Curator and third-party completed-folder scanners cannot accidentally
discover a recovery copy. The explicit recovery reader still rejects paths
outside its mapped published root.

Reserve the recovery ID, selection, hold, operation, and scratch identity
before copying. Use a private directory with restrictive permissions and
no-clobber publication; never merge into a release-named directory. Source
files must remain the same identity/size/version throughout copying. Hash
the source and independently verify staged contents, checking all fsync
and parent-directory sync results before publication. Reflinks must be
independent on write; otherwise fall back to a checked copy. Cross-device
rename is not a shortcut that may consume the source.

Check target capacity for a full ordinary copy plus the configured free
space reserve. A reflink may save space but must not be assumed available.
The capacity preview shows retained source bytes, additional staging bytes,
and Curator's projected library/rollback space separately for each volume.
Without reflinks this can reach three full copies, plus an old library copy
temporarily retained during replacement. A 60 GiB source can require another
60 GiB in staging and 60 GiB at the library. Curator must check its own target
capacity before claiming; Runner cannot infer it from the source volume.
ENOSPC blocks work and retains the source; storage pressure does not override
Keep or authorize premature deletion.

The read-only failed-root alternative avoids staging, but exposes live source
content and only covers that root. The full recovery design also handles
reviewed unknown folders and provides a fixed verified snapshot. Retain
staging for this release; a direct-source transport can be evaluated later
with equivalent mutation/version checks. This is a stated space/reliability
trade-off, not a requirement to mount the whole working directory.
If a copy fails or the process dies, preserve the source and hold. Retry
only the owned scratch generation after reconciling its operation. Reuse
helper code only after fixing ignored sync errors and proving that scratch
cleanup cannot target another operation's files.

Publish the immutable manifest only after all selected files are durable.
It includes recovery ID, source artifact/revision, manifest version and
digest, and per-file IDs, staged relative paths, lengths, digests, and
verification limitations. It contains no credentials or unbounded raw job
params. Staging is visible to the filesystem but never import-eligible until
the inventory records `awaiting_import` and the manifest is published.

### 8.3 Curator consumes a separate recovery workflow

Extend the native Runner adapter and Curator's existing asynchronous manual
import path. Curator discovers recoveries from Runner's new API using its
configured client connection. Runner makes no outbound callback requests.
Normal history polling and compatibility clients do not see recovery copies
as successful downloads. Ordinary Curator scans must explicitly exclude
the reserved recovery subtree; do not rely solely on a leading dot.

Runner displays **Ready for Curator** with its recovery ID. Curator Activity
exposes a **Recoveries** view where the user chooses the media item, copy,
and episode mapping using existing quality rules. If a configured Curator
UI link is provided, it may open that recovery view; only an allowlisted
base URL can be used, and no credentials appear in the link.

Before accepting an import, Curator verifies:

- Its configured remote-path mapping resolves the recovery root to a
  readable local path. Do not assume `/processing` exists in Curator.
- The manifest ID/digest and exact selected files match Runner's response.
- Files resolve within the reserved root without symlink or mount escape.
- The target title/episodes exist and replacement decisions are previewed
  against the current library generation and exact existing file identities.

The preview binds item, copy, episode set, library revision, quality policy,
existing file IDs/content evidence, and proposed destinations. Before any
publication or replacement, Curator reserves those logical targets and
destination paths and revalidates the preview. All recovery and ordinary
imports must use the same target coordinator; the existing per-download
lock does not serialize different downloads for the same episode. A changed
target or policy returns **Library changed — review again**, preserving
source and staging. Cancellation releases reservations only after workers
are quiescent. Persist reservation/operation ownership through restart;
never steal a live writer's reservation on a timeout.

Curator durably reserves one import keyed by Runner installation ID,
recovery ID, and manifest digest. Repeated clicks, polling, and restart
return the same import ID. It claims the recovery before its worker reads
files; publication and claim are distinct states. Claims carry a generation
and the authenticated consumer binding. Stale generations cannot acknowledge
or replace a newer attempt.

Recovery imports use copy semantics and suppress Curator's ordinary source
cleanup for this subtree. Runner remains the only staging cleanup owner.
This remains in scope as an explicit Curator robustness milestone, along
with the shared target coordinator. A receipt sent only after the current
importer returns cannot account for publication before its database commit,
nor stop an ordinary import changing the selected target. Reuse the existing
importer's parsing/quality/copy components, but do not claim its current
completion signal already supplies the new crash contract. Implement the
common primitives in separately reviewable Curator changes; recovery is
enabled only after their acceptance tests pass.

Curator needs a durable per-file placement protocol before the outbox:

1. Commit a placement intent containing source digest, destination,
   expected target generation, import/file IDs, and any replacement plan.
2. Copy to an operation-owned temporary file, verify its content digest,
   check file sync, then publish while holding the target reservation.
   Check the destination directory sync. Existing replacement paths must
   preserve the old copy in an owned rollback location until commit; do not
   delete superseded files before the new metadata and receipt are durable.
3. In one database transaction, commit library file/episode links and
   required quality/provenance metadata, placement completion, per-file
   result, and receipt-outbox state. Ancillary logs may remain best effort;
   fields used to decide placement or completion may not.
4. On restart, reconcile the placement intent against published content.
   A matching publication finishes its missing metadata transaction without
   recopying or re-running quality selection. A conflicting destination
   holds for review; it is neither an automatic overwrite nor “already
   present.” Retain replacement backups until commit and track their cleanup.
5. Retry outbox delivery until Runner acknowledges it. A lost HTTP response
   does not cause a second import. Recovery source cleanup waits for the
   verified committed result, never merely a successful rename.

The existing `placeContext` rename and separate library updates do not supply
this protocol. Implement it explicitly in Curator rather than assuming the
current manual importer is already transactional across disk and SQLite.

### 8.4 Receipt meaning, partial imports, and cleanup

A receipt binds recovery ID, claim generation, manifest digest, consumer
identity, import ID, and every selected file's result. An `imported` result
requires durable library placement, confirmed length/content verification,
and committed library metadata. `already_present` must identify verified
equivalent content; same title or filename is insufficient. `skipped`,
`failed`, `cancelled`, and `unverified_duplicate` are unresolved outcomes.

Runner accepts a completion only when every selected file has a resolved
result. It persists the receipt before changing the UI to **Imported**.
Partial results remain visible per file and are retried idempotently.
The receipt is Curator's authenticated assertion; Runner cannot independently
read a library path it does not mount, and must not imply otherwise.

Hold behavior is explicit:

- A staged recovery waiting for a user or consumer remains held. After
  24 hours without progress it appears as **Needs attention**, not expired.
- Consumer silence or a missed heartbeat never releases a hold. Restart
  resumes the claim. A new consumer cannot steal it through a timeout.
- Cancellation while claimed requires Curator to stop and acknowledge that
  its worker is quiescent. An offline consumer leaves **Cancellation pending**.
- An unclaimed recovery can be cancelled immediately through a durable
  operation. Deleting its staging still follows ownership checks.
- Cancellation/failure returns the source to review-held retention; it
  cannot expose an already elapsed original deadline and delete immediately.

After a full receipt, default behavior keeps the source for a fresh configured
retention period (seven days by default; indefinitely if Keep was set or
`failed_keep_days = 0`). The recovery preview may explicitly
select **Remove recovered source files after confirmed import**. This
authorizes only the selected, unchanged files, not unselected pack contents.
Unknown adopted folders remain Keep by default. Unselected retained files
stay visible with a review hold until the user chooses their policy.

Successfully imported staging receives a 24-hour cleanup deadline; the UI
shows it separately from source retention. Failed/partial/cancel-pending
staging has no automatic expiry. Staging cleanup must never traverse library
receipt paths. A cleanup failure changes cleanup status, not the historical
fact that the import succeeded.

## 9. API and UI — expose durable state, not inferred success

### 9.1 Proposed Runner API

All routes below are new, under `/api/v1`. Reuse existing authentication
and request limits. They are not available through the NZBGet compatibility
surface. Requests that mutate require control authority; consumer claim and
receipt routes require a configured consumer identity. A caller-supplied
display name such as `X-Client-Name` is not an authentication identity.
Define a per-consumer credential or binding in Runner's auth model before
shipping receipts; do not expose it through config reads or URLs.

| Method and route | Contract |
|---|---|
| `GET /artifacts?state=attention&limit=100&cursor=...` | Cached records with stable ID cursor, scan freshness, byte completeness, and counts |
| `GET /artifacts/{id}` | Identity, reason, retention/holds, allowed actions with blockers, and operation summary |
| `GET /artifacts/{id}/files?limit=100&cursor=...` | Cached, paginated file inventory; no synchronous traversal |
| `GET /artifacts/{id}/events?after=...` | Artifact audit with a separate event cursor |
| `POST /artifacts/scan` | Enqueue/coalesce a bounded scan; `202` operation ID |
| `POST /artifacts/{id}/inspect` | Queue file inventory work; `202` operation ID; Curator probes published selections |
| `POST /artifacts/{id}/adopt` | Bind an unchanged reviewed inventory; defaults to Keep |
| `PATCH /artifacts/{id}/retention` | Set Keep or an explicitly previewed policy, with expected revision |
| `POST /artifacts/retention-preview` | Return exact candidate IDs/revisions and deadlines; no mutation |
| `POST /artifacts/retention-apply` | Apply the unchanged preview selection atomically; changed items return conflict |
| `POST /artifacts/{id}/delete` | Queue owned deletion; explicit UI `undo_seconds=8`, otherwise zero grace; `202` operation ID |
| `GET /artifact-operations/{id}` | Durable state, progress, error, retry time, and whether cancellation is possible |
| `POST /artifact-operations/{id}/cancel` | Cancel only at a safe boundary; return conflict after destructive work starts |
| `POST /artifacts/{id}/recoveries` | Reserve hold and exact file selection, then stage asynchronously |
| `GET /recoveries?state=awaiting_import&cursor=...` | Consumer discovery; stable ID paging and manifest metadata |
| `GET /recoveries/{id}` | Full recovery state and paginated manifest references |
| `POST /recoveries/{id}/claim` | Bind an authenticated consumer/import ID and issue a claim generation |
| `POST /recoveries/{id}/progress` | Record contact/progress for the current claim; does not release holds |
| `POST /recoveries/{id}/receipt` | Validate and durably record idempotent per-file results |
| `POST /recoveries/{id}/cancel` | Persist cancellation intent; await worker quiescence if claimed |
| `POST /recoveries/{id}/cancel-ack` | Current consumer confirms worker stop; permits owned staging cleanup |

All mutations carry an idempotency key; artifact mutations also carry an
expected revision (`If-Match` or the typed field below, standardized before
implementation). Reusing a key with a different request returns `409`.
Store successful results and unresolved intents durably before returning.
Use `409` for stale revisions, active claims, or ownership conflict; `422`
for invalid selection/policy; `503` for unavailable inventory/authority.
`404` means unknown ID, not an unavailable storage root.

Recovery request example, with proposed fields:

```json
{
  "expected_revision": 12,
  "idempotency_key": "recover-selection-83",
  "file_ids": ["file-a", "file-b"],
  "staged_names": {
    "file-a": "Example.S01E01.mkv",
    "file-b": "Example.S01E02.mkv"
  },
  "accept_unverified_media": true,
  "remove_selected_source_after_import": false
}
```

`staged_names` accepts plain relative file names under the assigned payload
directory; reject traversal, absolute paths, separators that escape the
layout, reserved names, and collisions. `accept_unverified_media` records
an explicit choice from the preview; it cannot override ownership checks,
source changes or an active job. Failed media recognition blocks the
subsequent Curator import; staging unidentified bytes does not override it.

The accepted response contains `recovery_id`, `operation_id`, `state`, and
source revision. `202` means admitted work, never completed recovery.
The list row's `allowed_actions` is authoritative for UI rendering, but the
server rechecks the same predicates on every mutation.

Receipt example:

```json
{
  "idempotency_key": "curator-import-417-receipt-1",
  "claim_generation": 1,
  "manifest_digest": "sha256:<manifest-digest>",
  "import_id": "417",
  "files": [
    {
      "file_id": "file-a",
      "result": "imported",
      "bytes": 5368709120,
      "digest": "sha256:<file-digest>",
      "library_file_id": "curator-file-9001"
    }
  ]
}
```

Every received file ID must belong to that manifest; sizes/digests must
agree. Conflicting duplicate receipts return conflict and leave the hold.
Missing files keep the recovery partial. Consumer identity comes from
authentication; library file IDs are informational, never cleanup paths.
Expose receipts as **Confirmed by Curator**, not independently verified by
Runner. Receipt transport uses the configured trusted connection and must
not log bearer credentials or raw authorization headers.

### 9.2 Curator contract additions

Existing endpoints in
`acquisition_handlers.go` (curator `internal/api/acquisition_handlers.go`)
include `GET /import/scan`, `GET /import/default-path`, and
`POST /import/manual`; their exact mounted API prefix is determined by
Curator's router. Existing manual-import admission returns `202` with a
download ID. Extend the typed API in
`openapi.yaml` (curator `internal/api/openapi.yaml`) and regenerate
bindings; do not hand-edit generated Go types.

Add a recovery-specific admission route, proposed `POST /import/recovery`,
accepting configured Runner client ID, recovery ID, manifest digest,
selected target item/copy/episode mapping, and an idempotency key. Resolve
the source from the authenticated Runner manifest and configured mapping;
do not accept an arbitrary caller-supplied filesystem path in this route.
Persist its identity, per-file results, claim state, and receipt outbox
separately from Activity's short display retention. Activity cleanup must
not erase a receipt still awaiting delivery.

Add recovery discovery/status to the native Runner adapter, the existing
30-second reconciliation loop, and Curator Activity. Coalesce discovery
while a previous request is pending and cap per-cycle pages. Polling is
authoritative for this first version; optional new SSE hints may trigger
refresh, but must use a separate artifact sequence and must not alter
existing History cursors or event semantics.

### 9.3 Runner attention view

Add **Files** beside Queue/History/Logs/Settings, with **Needs attention**
as its default filter and **All tracked files** available. Each row shows:

- Release/folder name and location, with infrastructure roots labeled by
  their role rather than presented as orphan payloads.
- Reason, original outcome when known, source of ownership evidence,
  logical/allocated bytes, and measurement freshness.
- Expiry or exact hold reason, recovery/import state, and last error.
- Applicable Inspect, Keep, Recover, Review retention, Retry cleanup,
  Delete, and Undo controls; disabled controls explain their blocker.

Use the existing in-place rendering conventions in
[`index.html`](../../crates/nzbd-api/ui/index.html). Polls must preserve expanded
rows, selection, filters, and focused controls. Batch actions preview exact
counts and sizes; stale selections conflict rather than silently broadening
the action. Do not require a generic confirmation popup for each action;
use the preview for retention/recovery and existing Undo for manual delete.

**How to read it:** “Untracked” means Runner cannot prove ownership;
“Parked” means retained by policy; “Imported” means a complete committed
consumer receipt; “Partial” means unresolved selected files; “Unavailable”
means observation failed. A blank or incomplete size is unknown, not zero.

## 10. Crash, concurrency, and compatibility contracts

| Failure boundary | Required restart/retry behavior |
|---|---|
| Intent committed before mkdir/copy/delete | Resume the same operation ID after checking disk; no duplicate target |
| Directory created before sidecar/binding committed | Keep allocation pending; do not start payload writes or automatically adopt other contents |
| Park copied but source removal incomplete | Track both locations and hold expiry until disposition is resolved |
| Copy complete but fsync/manifest publication failed | Not importable; keep source and retry owned staging |
| Published copy exists but DB transition missing | Verify operation ID, external binding, manifest, and digests before finishing publication |
| Curator library publication before metadata/outbox commit | Reconcile the placement intent and content, then finish metadata; do not blindly recopy |
| Curator metadata/outbox committed before receipt delivery | Durable outbox replays receipt; do not reimport confirmed files |
| Receipt accepted but response lost | Same receipt returns the stored result; cleanup occurs at most once per generation |
| Delete partially completed | Persist/observe remaining inventory, retain error, and retry only unchanged owned files |
| Keep/recovery races with expiry | One transactional operation reservation wins; loser gets an explicit conflict |
| Root disappears or resolves to a different volume | Block actions and report root identity mismatch; no empty-directory success |
| Unknown files appear in an owned subtree | Freeze automatic cleanup and require renewed review |
| History forget/prune runs concurrently | Ownership, holds, deadlines, and outbox are unaffected |
| Inventory restored from backup | Quarantine restored nonterminal state; reauthorize retention and reconcile claims; scans alone cannot resume expiry |
| Runner or Curator downgrades | Quiesce work, export old-compatible config/mirror, retain source/holds until compatible recovery resumes |
| Cluster mode enabled | New mutations unavailable until replicated implementation exists; preserve standalone inventory for review |

The failure protocol must be exercised with injected termination at each
filesystem/transaction boundary. Startup inventory reconciliation precedes
new destructive workers. Stop workers on shutdown, await safe boundaries,
and leave durable pending state when a filesystem call cannot finish.

Do not rewrite a failed History entry's original status or `final_dir` into
a recovery success. Link it to the artifact/recovery as supplemental data.
History records can show **Files removed** from inventory even if the old
path remains in the record for audit. If History is forgotten, Files still
shows the retained payload.

Before downgrading to a binary unaware of recovery state, disable new
actions and quiesce both sides. Export a configuration accepted by the exact
target binary: remove the new `[artifacts]` section and new consumer-auth
fields, or restore the pre-upgrade configuration while preserving subsequent
compatible settings. The current parser rejects unknown fields; setting
Disabling actions in new TOML is not sufficient. Handle the saved-config mirror through
the documented recovery path too, so fallback does not reload incompatible
TOML. Validate startup with that prior binary against disposable copies
before documenting downgrade as supported. Back up the new configuration
for re-upgrade without exposing credentials. Before downgrading Curator,
quiesce recovery claims and remove its recovery mount or disable all recovery
readers. The sibling recovery root remains outside ordinary completed scans;
do not reconfigure an old consumer to scan it as a download directory. Preserve
inventory, sidecars, and staging; removing the new database is not rollback.
Restoring automatic behavior requires compatible binaries and reconciliation.

## 11. Implementation — deliver in independently reviewable milestones

The milestone IDs below are proposed PR boundaries. All remain unstarted.
F0 is a small independent fix; F1–F6 remain the full approved initiative.
Separating commits makes review and rollback practical without reducing
the desired product or deferring its robustness guarantees indefinitely.
Update this document and the runtime reference docs in the same changes
that implement behavior. No live cleanup or configuration change belongs
to the documentation-only proposal.

### 11.1 F0 — stop losing History on failed deletion

**Work:** Fix `history_action` so failed stat/unlink or unavailable storage
does not delete the History entry or return a misleading successful removal.
Keep the row/path/error and return a non-success response; distinguish a
verified already-absent payload from an inaccessible root. Add one reliable
failure fixture and one success/verified-absence fixture. Do not await the
entire inventory implementation to fix this confirmed record-loss bug.

Trace any still-available July records/logs and the observed temporary-file
naming against the applicable old code, recording evidence and uncertainty.
Cleaned files and missing history may make attribution impossible. The
confirmed deletion bug remains independently actionable even then.

**Acceptance:** Inject removal failure through the public History action;
the record and path remain queryable, response is non-success, and a later
successful retry removes the files before dropping History. Capture the
actual error rather than asserting a directory is merely nonempty. The RCA
states whether historical attribution is proven or unavailable; it never
calls an internal `.tmp` a PP move scratch directory without evidence.

### 11.2 F1 — establish inventory and allocation ownership

**Work:** Add the store/schema and typed model in
[`nzbd-state`](../../crates/nzbd-state/src), with a dedicated module such as
`artifacts.rs`. Wire lifecycle service startup in
[`main.rs`](../../crates/nzbd/src/main.rs). Audit engine allocation/admission,
writer startup, PP moves/failures, and queue/History deletion. Add UUID
identity, external sidecars, operation journal, startup reconciliation, and protected
root resolution. Keep new retention actions disabled.

**Acceptance:** A fixture job retains a resolvable artifact after History
prune/forget and daemon restart. Killing between allocation intent and
payload write cannot create a writable unowned job directory. Same-name
jobs and reused paths cannot inherit each other's authority.
Restoring a backup taken before Keep or a recovery claim must never resume
its old deletion deadline, even if all filesystem identities still match.
Successful import/empty-directory cleanup leaves no control file behind.
Inventory write failure yields the documented admission pause/error and
bounded recovery while existing correctly owned writes and API reads behave
as specified. Test `observe` and disabled scanning separately from core
ownership, rather than labeling either “off.”

### 11.3 F2 — reconciliation and read-only Files view

**Work:** Add bounded background discovery, root availability and role
exclusion, cached size/file enumeration, stable pagination, and attention
classification. Introduce read endpoints and the Files UI. Legacy History
matching only creates candidates. Show support limits in cluster mode.

**Acceptance:** A synthetic working root containing state, torrent payloads,
failed children, active jobs, legacy folders, symlinks, and an unavailable
mount produces the expected classifications without modifying payloads.
Repeated page reads do no filesystem scans and preserve UI selection.

### 11.4 F3 — checked operations and retention

**Work:** Implement intent/reservation, no-follow ownership checks, delete
inventory, retry scheduling, checked synchronization, Undo, Keep, deadlines,
policy preview/apply, and observe/manage configuration. Fix the existing
History record loss after failed deletion. Audit all deletion bypasses.
Integrate disk guards and expose structured errors.

**Acceptance:** Virtual-clock tests prove seven-day expiry, indefinite Keep,
fresh deadlines after review, and safe races. Deletion cannot affect active
jobs, torrents, unknown additions, or substituted paths. Injected unlink
failure leaves the remaining files and explanatory records visible.
The updated Curator cleanup client receiving `503` keeps cleanup pending;
after verified removal, its retry receives terminal success and only then
marks the payload removed. Test both queue and History routes and their
transition. No `202` response may escape through either legacy route.
Spy on the direct-disk fallback: pending/refused native Runner deletion must
never call `removeImportedDir`. Queue-to-History fallback happens only on
`404`. Test recovery/Keep hold refusal, auth failure, unavailable Runner,
and `RemoveCompleted` settings. API/legacy requests have no UI Undo delay.
Install this consumer fix before enabling managed operations, not at F5.

### 11.5 F4 — recovery inspection and durable publication

**Work:** Add adoption preview, exact file selection for staging,
independent copy/reflink, content digest verification, safe staging names,
manifest publication, source holds, operation progress, and cancellation.
Expose only a dedicated published root read-only to Curator; document the
mount mapping and per-volume peak capacity preview. Runner gains no probe
dependency; unidentified content is staged for later Curator inspection.

**Acceptance:** Cross-device and interrupted-copy fixtures never expose a
partial recovery as importable or remove the source. `.tmp` files are not
automatically selected. Changed source, insufficient space,
and failed fsync have distinct visible outcomes. Ordinary imports ignore
all recovery scratch and published copies until explicitly selected.

### 11.6 F5 — Curator claim, import, and receipt

**Work:** In Curator, extend
`internal/adapters/nzbd` (curator `internal/adapters/nzbd`),
`manualimport.go` (curator `internal/app/acquisition/manualimport.go`),
`importers.go` (curator `internal/app/acquisition/importers.go`),
`internal/infra/sqlite` (curator `internal/infra/sqlite`), and Activity.
Add the recovery admission route, existing native media probe integration,
target preview, path mapping check,
per-file progress, durable idempotency/outbox, consumer credentials, and
source-cleanup exclusion. Implement claim/receipt routes in Runner.

**Acceptance:** A three-file recovery with one injected placement failure
reports 2/3, retains source and staging, then completes only the remaining
file after restart. Lost receipt responses and repeated discovery create
one import. An incorrect consumer, generation, digest, or file list cannot
release a hold. Curator Activity retention cannot discard pending receipts.
Inject termination after filesystem publication but before each metadata
boundary. Resume from the placement intent without overwriting a changed
destination or losing the previous library copy. An ordinary import that
changes the same episode after recovery preview invalidates that preview.
Native MKV/MP4 probing works in the distroless image without ffprobe;
unsupported/malformed content and a failed optional fallback remain explicit.

### 11.7 F6 — cleanup after import and operational rollout

**Work:** Add receipt-driven staging expiry, optional selected-source
cleanup, partial-selection holds, cancellation acknowledgement, metrics,
upgrade/downgrade checks, and operator documentation. Exercise the actual
two-container path mapping with disposable fixtures, then enable management
only after observing correct classification.

**Acceptance:** All four product journeys work end-to-end on a standalone
deployment. Curator lacks access to Runner state/source directories yet can
import a recovery. An imported source is not removed before receipt, and
unselected files survive selected-source cleanup. No existing folder receives
a deadline merely because the feature was installed.
Start the actual prior Runner binary against an exported compatible config
and mirror in a disposable fixture; disabling the feature in new TOML alone
does not satisfy downgrade acceptance.

## 12. Validation — test ownership loss and ambiguous outcomes

| Test group | Minimum required cases |
|---|---|
| Storage/model | Atomic schema install; unknown schema; missing DB/admission behavior; consistent backup/restore; backup before Keep/claim then restore after old deadline; idempotency conflict; job-ID reuse; History prune isolation; terminal-record compaction and size |
| Allocation/move | Crash before/after intent, external sidecar, directory publication, cross-volume copy, source unlink, and inventory outcome |
| Deletion | Symlink and parent substitution; root/ancestor target; volume change; extra files; partial unlink; sync failure; unavailable queue; competing operation |
| Retention | Exact expiry boundary; zero days; Keep; mode change; policy preview race; restart; recovery hold; runtime and startup clock jumps, including not-yet-overdue deadlines; no legacy retroactive expiry |
| Discovery | Nested roots; aliases; custom failed/category roots; loose files; reserved roots; pagination under changes; incomplete scan; inaccessible NAS |
| Recovery | Obfuscated media; partial/temp exclusions; Curator native/optional probe timeout and unsupported content; source mutation; copy corruption; EXDEV; full disk; no-clobber collision |
| Curator | Mapping mismatch; partial pack; verified duplicate versus skipped quality; repeated click; crash between library publication/metadata/outbox; ordinary import invalidates recovery target; outbox retry; stale claim |
| UI/API | Auth/consumer scope; request size bounds; escaped hostile filenames; stale revision; durable Undo; selection/focus preservation; cached reads |
| Compatibility | Existing History cursors and readiness; native/compat deletion behavior; Curator pending/refusal never invokes disk fallback; zero legacy Undo; no marker left in payload; paused torrent payload untouched; cluster mutation guard; old-binary config/mirror startup and separate recovery mount |

Use temporary fixture roots and fake clocks; never target real retained
media. Proposed test symbols should be added with the implementation, for
example `artifacts_keep_survives_history_prune`,
`artifacts_rejects_replaced_parent`, and
`recovery_partial_receipt_keeps_source`. Filtering tests by a new name is
not proof they ran; CI must assert discovered tests and failures normally.

Existing Runner commands to run after the relevant implementation:

```bash
cargo test -p nzbd-state -p nzbd-post -p nzbd-api  # store, PP, API regression
cargo test -p nzbd-engine                       # allocation and queue contracts
make ui-test                                   # existing browser harness
make check                                     # repository core release gates
```

Curator's implementation PR must run its documented repository checks and
API generation checks against the actual target revision, plus the new
cross-service fixture. Add a deterministic integration test that launches
Runner and Curator with different container paths and proves claims,
partial receipts, recovery after restart, and cleanup ordering.

Proposed performance acceptance on a declared local test filesystem:
10,000 attention rows, 100,000 file records, and one ongoing copy. Cached
100-row list p95 stays below 200 ms and queue/status latency does not regress
by more than 10% versus the same load without scanning. These are review
targets, not measured results or promises for a stalled NAS. Record machine,
filesystem, fixture size, baseline, CPU, memory, and timing with the result.

Retention tests exercise the dual-clock rule in §6.1 rather than depending
on a platform synchronization signal. Test Docker-style restart with no
clock service, forward/backward wall jumps, just-before/past deadlines,
uncheckpointed crashes, repeated checkpoint retries, holds, zero retention,
and a restore resetting observed uptime. No elapsed interval may be counted
twice. Automatic expiry must work unattended after enough observed runtime;
a full offline week earns no credit and extends the displayed estimate.

## 13. Rollout and operations — first observe, then enable

1. **Deploy ownership and observe mode.** Back up persistent state; verify
   configured root roles, actual mounts, and scan results. Existing manual
   cleanup is already done; establish a fresh baseline rather than using
   the incident's old directory list.
2. **Deploy compatible Curator.** Verify native-deletion fallback suppression,
   separate read-only recovery mount,
   consumer identity, and remote-path mapping before allowing staging.
   If Curator is not compatible, disable Recover with that exact reason and
   do not enable managed retention where that consumer can bypass holds.
3. **Run a disposable recovery.** Prove three selected files, one forced
   failure, resume, receipt, and cleanup. Confirm Curator cannot access
   Runner's private state and does not delete retained source files.
4. **Enable `manage`, with seven days for future parks.** The setting change
   shows eligible scope and no retroactive deadlines. Existing candidates
   remain review-held. Observe a virtual-clock or disposable short-policy
   expiry; do not shorten retention on real files to test it.
5. **Publish evidence and update docs.** Add the shipped contract to
   [CONFIGURATION.md](../CONFIGURATION.md), [USAGE.md](../USAGE.md),
   [INTEGRATION.md](../INTEGRATION.md), [DEPLOY.md](../DEPLOY.md), and
   [STATUS.md](../../STATUS.md), including supported platforms and cluster limits.

Proposed bounded metrics: artifact count and allocated bytes by
classification; scan age/completeness; cleanup attempts/errors; oldest
pending operation; held recoveries; receipt backlog; recovery copied bytes.
Do not label metrics by release, path, job, or artifact ID. Logs and detail
views carry those identifiers with credential-bearing params omitted.

**How to read it:** rising parked bytes with future deadlines is retention
working; rising overdue eligible bytes with errors is cleanup blocked;
untracked counts require review; a stale/incomplete scan means the totals
cannot be trusted; a receipt backlog means imported results have not yet
been reconciled. “No attention rows” only means healthy when scans are
complete and fresh.

Rollback stops admission of new recovery/deletion work, waits for safe
worker boundaries, and leaves holds and operation records intact. Disable
expiry first; do not erase sidecars or inventory and do not revert Curator
to unrestricted scanning while recovery copies exist. If storage or
inventory integrity is uncertain, leave management disabled and report the
blocking artifact/root. A restart is not authorization to finish unknown
deletions.

## 14. Reviewer decisions and completion criteria

The four capabilities are approved in principle. The following are proposed
implementation choices requiring review, not additional requests to the
user before writing this document:

| Decision | Recommended resolution | Trade-off |
|---|---|---|
| Initial deployment boundary | Standalone mutations; cluster read-only until replicated authority integration | Delays cluster parity rather than risking shared-volume deletion |
| Retention defaults | Seven days of observed eligible uptime plus elapsed wall deadline; legacy data review-held | Downtime/holds extend retention without an operator clock gate |
| Recovery copy ownership | Runner owns staging; Curator copies and sends durable receipts | Extra I/O and potentially an additional full payload copy |
| Recovery after confirmed import | Source gets a fresh configured retention period, honoring Keep/zero; selected-source removal is explicit; staging expires after at least 24 observed hours | Recovery does not immediately maximize reclaimed space |
| Media verification | Existing Curator native probe, optional existing external fallback; recognition and copy integrity distinguished from completeness | Identification follows staging; initial scope excludes books |
| Durable store | Separate authoritative SQLite inventory for standalone; core ownership dependency with explicit admission error | Store failure pauses new allocations; preserves ownership rather than failing open |
| Consumer identity | Explicit authenticated consumer binding for claims/receipts | Requires auth/API work beyond a client-name header |
| Supported filesystem operations | Checked durability and safe handle-relative traversal; unsupported platforms disable mutations | More implementation work than recursive path deletion |

Review specifically whether the ownership proof covers every current
allocation/move/delete path; whether a source change can escape validation;
whether Curator can ever emit a premature receipt; whether cancellation
can release a live worker's hold; and whether upgrading, downgrading, or
restoring a backup can enable unintended expiry. These are release blockers.

Implementation is complete for the initial standalone release only when:

- [ ] New failed payloads have durable ownership, visible deadlines, and Keep.
- [ ] History deletion/retention cannot orphan ownership or pending work.
- [ ] The Files view explains unknown folders and infrastructure accurately.
- [ ] All destructive operations enforce current ownership and preserve errors.
- [ ] Recovery publishes exact durable copies without changing original outcomes.
- [ ] Curator imports with durable per-file results and retryable receipts.
- [ ] Partial/cancelled/offline imports cannot trigger premature cleanup.
- [ ] Cross-service, crash, concurrency, and path-substitution tests pass.
- [ ] Observe-first rollout and downgrade procedures are verified with fixtures.
- [ ] Runtime documentation and supported-mode limits match shipped behavior.

Cluster mutation support and book recovery must remain visibly unsupported
until separately implemented and tested. They must not be counted as done
by this proposal or hidden behind a best-effort fallback.
