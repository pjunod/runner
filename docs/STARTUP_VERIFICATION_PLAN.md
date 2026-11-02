# Startup verification — root cause and proposed runtime recovery

**Status:** proposed; implementation has not started · **Written:** 2026-11-01
· **Incident:** `nuc3`, 2026-11-01 · **Review:** revised after
[Opus's third review](STARTUP_VERIFICATION_OPUS_THIRD_REVIEW.md); these
amendments supersede the previously approved draft and were approved by
Astra on scoped re-review against `a253562`.
Implementation has not started. See
[findings and disposition](STARTUP_VERIFICATION_REVIEW.md).

Companion to [DEPLOY.md](DEPLOY.md) (container operation),
[ARCHITECTURE.md](ARCHITECTURE.md) (engine and persistence), and
[STARTUP_RECOVERY_REVIEW.md](STARTUP_RECOVERY_REVIEW.md) (the earlier pending
magnet startup defect). This proposal addresses local recovery work that
still keeps the service offline. Read the evidence and operation contract
before implementing the milestones. Preserve the existing durable ownership
and publication rules; flag any required change to those rules separately.

## 1. Finding — recovery is incorrectly a condition of service startup

The daemon should start serving its UI, API, health checks, and controls
while recovery proceeds. The running application decides which operations
are available from the evidence each operation requires. Neither resuming
downloads nor completing all recovery is a prerequisite for service startup.

Moving the listener earlier and then globally refusing useful requests
until all checks finish would preserve the same underlying design problem.
Verification must become scheduled runtime work, with dependencies scoped
to affected jobs, files, and storage roots.

### 1.1 Evidence from the live container

Read-only SSH inspection on `nuc3` established the following:

| Evidence | Observation |
|---|---|
| Deployment | Compose project `runner`, `/opt/noirr/runner/docker-compose.yml`; checkout HEAD `a2535623af1a7a5623677350895b49e8c2a840ab`. |
| Container start | `2026-11-01T16:27:33.839Z` (11:27:33 a.m. EST). |
| First daemon log | Resolved data directories at `16:27:34.005Z`. |
| Recovery work | Relocation/source-retirement warnings through `16:27:44.279Z`. |
| Engine progress | Recovered URL fetches respawned at `16:30:35.095Z`. |
| Listener available | `nzbd listening` at `16:30:36.827Z`, about 183 seconds after container start. |
| Health settings | `nzbd status --url 127.0.0.1:6789`; 10-second start period, 30-second interval, 5-second timeout, 3 failures. |
| Compose symptom | User-reported failure after 72.9 seconds; discovery depends on `service_healthy`. |
| Subsequent state | `nzbd` healthy with zero restarts; `nzbd-discovery` still `Created`. |
| Storage | `/processing` maps to `/mnt/processing`, mounted as `fuse.glusterfs`. |
| Queue sample after startup | 4 jobs; 2 fetching and 2 paused; 9,496 completed segments totaling 6,806,056,476 bytes. |
| Bounded read-only CRC sample | 160 ranges, 114,688,000 bytes, 2.731 seconds; all CRCs matched. Linear full-scan estimate: 162.1 seconds. |

**Evidence boundary:** the listener delay and later healthy state are directly
observed. Source inspection confirms synchronous range validation before
serving. The sample strongly supports CRC reads as the dominant cost, but
162.1 seconds is an extrapolation from a warm, post-startup sample, not a
recorded startup phase duration. Lifecycle reconciliation, database access,
and other startup work contribute to the total. Add phase timings before
claiming an exact breakdown. The deployed binary reports `0.2.0+unknown`;
the checkout revision is not independently proven to be the image revision.
The relevant deployed recovery source was inspected and matched the loop
described below. No restart or production state mutation was performed.

Authentication was not the cause of this incident: the live service
advertised `auth="none"`, and the existing probe later succeeded unchanged.
The pending-magnet warnings occurred after the listener opened; they are
not the explanation for this pre-listener delay.

### 1.2 The call chain puts payload reads before the listener

Implementation baseline: `a2535623af1a7a5623677350895b49e8c2a840ab` (queue
schema 5), the observed nuc3 checkout. Initial investigation used `9e4d6c9`
(schema 4). The baseline is 34 commits ahead; owner and PP manager changes
make the old line numbers unsuitable for implementation. Read-only Git
object comparison confirms that `recover`, `fold_job_journals`,
`adopt_authority`, and `import_job` are byte-identical at both revisions,
as are the complete writer and TLS source files. PP admission paths and
shutdown were rechecked at `a253562`. Source links point to symbols in the
local checkout; line numbers stated below refer to `a253562` unless marked
otherwise. Re-verify all callers against the eventual implementation head.

| Existing interface or path | Consequence |
|---|---|
| `Engine::spawn(cfg: EngineConfig) -> Result<EngineHandle, EngineError>` in [engine lib.rs](../crates/nzbd-engine/src/lib.rs) | Calls synchronous `Owner::recover` before returning a handle. |
| `Owner::recover` in [owner.rs](../crates/nzbd-engine/src/owner.rs) | Loads queue state, reconciles lifecycle records, replays journals, then validates every saved `Done` range, including paused jobs. |
| `validate_range(path, offset, len, crc) -> bool` in [writer.rs](../crates/nzbd-engine/src/writer.rs) | Checks metadata, opens and seeks the file, reads the range in 64 KiB buffers, and compares CRC. Repeats file access per segment. |
| `run()` in [main.rs](../crates/nzbd/src/main.rs) | Awaits engine construction before building and serving the API. Other storage initialization also precedes serving. |
| `startup_pass`, `send_finalize`, `check_job_complete` in [owner.rs](../crates/nzbd-engine/src/owner.rs) | Depend on recovered completion evidence already being trustworthy. Moving the scan alone would break this assumption. |

```text
Current:
  open stores → reconcile/replay → reread saved payloads → start engine
       → bind/serve API → Docker probe passes

Proposed:
  start control service → serve health, status, UI, and available controls
              │
              └─ background metadata recovery → queue available
                       └─ scheduled verification → release eligible work
```

The synchronous order simplified recovery by making uncertain state
invisible to the running engine. That convenience couples availability to
the amount and responsiveness of stored data.

### 1.3 Source review found correctness defects beyond the outage

Opus identified the following mechanisms at `9e4d6c9`; source inspection
confirmed them and rechecked them at `a253562`. These are code-level defects or redesign hazards, not
claims that corruption or evidence loss occurred during this incident.

| Path | Finding and required correction |
|---|---|
| Boolean range validation and first snapshot save | Any stat/open/seek/read error returns `false`; recovery demotes `Done`, clears finalization, then startup saves and compacts nondelegated job journals. Only proven invalid data may be demoted. Preserve unresolved evidence in the snapshot; compaction is safe only when that snapshot faithfully includes it. |
| `adopt_authority`, `import_job`, `fold_job_journals` | Saved `Done` ranges are not all revalidated; journal folding checks journal records only. Takeover/import must establish the same evidence requirements as boot before dependent work. |
| Post-manager startup/rescan and `JobFinished` handlers | Snapshot statuses initiate subprocess work. A final owner commit check comes too late to prevent those reads or PAR writes. Require an owner-issued PP admission lease before tools start, using existing Inventory authority. |
| Hold/retry rewrites inside `Owner::recover` | Identity-conflict retry becomes `Queued`; a RAR retry can become `PostQueued`. Move evidence-dependent rewrites into the accepted verification-result transition. |
| Torrent recovery's direct `SnapshotStore::load` and orphan deletion | Recovery makes sidecar deletion decisions from a separate snapshot. Early admission would race stale orphan decisions. Use owner-provided restore work and owner-ordered conditional cleanup, including pending recovery. This concurrency hazard does not prove an existing startup admission race. |
| Clean marker and writer shutdown | `marker.disarm()` precedes `writers.clear()` and completion of all workers. Marker absence alone is not a durable payload guarantee. However, acknowledged segment writes already call `sync_data` before reporting success; Opus's contrary premise is incorrect at this baseline. |
| Finalization racing shutdown or a crash | A writer may publish and remove its partial after the owner's final snapshot, losing `WriterFinalized`. Next boot looks only for partial names and may demote intact published data. Persist publication intent, resolve the final name, and drain/apply results before the last snapshot. This interleaving is source-confirmed, not reproduced. |
| Path selection and TLS generation | `exists()` hides errors. Range/journal recovery resolves different paths; generated TLS identity may be recreated after a failed existence check. Propagate uncertainty, centralize payload resolution, and create certificates only on proven first initialization. |

The current code does a full CRC pass regardless of `was_unclean`, which
only affects a diagnostic log. The audit now identifies the missing piece:
saved ranges do not record durable-writer provenance or fd-bound identity.
Add that proof as the second correction (§3.4), without a watermark system.
Read/open overhead
also contributes: the live sample does not separate FUSE metadata costs
from byte reads and hashing.

## 2. Decision — let the running app manage incomplete recovery

1. **Start the control service independently of engine construction.**
   Serve embedded UI, service status, diagnostics, and shutdown without
   requiring the full engine handle or opening payload/history stores.
   Metadata loading runs outside HTTP execution and the queue-owner loop.
   A process-local bootstrap monitor starts without state-volume access.
   Sensitive public routes require known configuration, authentication,
   and TLS material; unknown security settings never become permissive
   defaults. A missing config can itself require a data-volume mirror
   read, so bootstrap behavior is specified separately in §3.3. The
   storage-backed `artifacts.lock` acquired by `Inventory::open` is a
   prerequisite for data-store mutation, not for serving the control
   listener. Pending or conflicting data ownership keeps the relevant
   component unavailable without authorizing a second writer.
2. **Represent unavailable evidence explicitly.** Queue loading, verified
   payload state, storage availability, and user pause are separate facts.
   Do not replace an unavailable queue with an empty writable queue, report
   recovery as a user pause, or require one global recovery-complete flag.
3. **Gate operations before side effects and again at commit.** The queue owner
   decides whether required ownership and verification evidence permits
   writes, leases, finalization, or completion. API-only checks are
   insufficient because background tasks and compatibility routes also
   initiate work. In particular, PP must acquire an owner-issued custody
   lease before launching any reader, repair tool, or extraction task.
4. **Verify strategically.** Prioritize jobs requested for resume, then
   runnable recovered jobs; paused jobs have lower priority. Unrelated new
   or verified work may proceed once its own prerequisites are satisfied.
   Per-job gating is an acceptable first implementation; it must not grow
   into a daemon-wide payload-verification barrier.
5. **Treat results as conditional evidence.** A worker reports its result
   to the owner; it never mutates queue state or enables work directly.
   The owner rejects stale results and applies accepted changes durably
   before dependent work is admitted.
6. **Avoid rereading proven durable ranges.** Add writer-protocol provenance
   and fd-bound file identity to saved/journaled completion evidence.
   Trust matching, fully covered current evidence without byte rereads;
   schedule remaining legacy or uncertain scopes. This is the second
   correction, not optional watermark research. Neither it nor the
   remaining checks replaces the runtime-availability requirement.

The trade-off is explicit recovery state and asynchronous result handling.
We accept that complexity to keep storage delays confined to the work
that depends on that storage.

## 3. Contract — availability follows each operation's dependencies

The following is proposed behavior, not an existing API guarantee. Exact
DTO and message names are implementation choices; preserve existing native
NZBGet, and qBittorrent compatibility contracts when extending them.

| Operation | Behavior while recovery or verification is incomplete |
|---|---|
| Embedded UI, service status, health, logs | Available without waiting for payload scans. Clearly identify loading, checking, or degraded components. |
| Queue listing before metadata loads | Report queue loading/unavailable explicitly. Never fabricate an empty successful listing that consumers could interpret as job removal. |
| New admission before authoritative metadata loads | Return a retryable unavailable response, or accept only through an existing durable idempotent intent mechanism proven safe in this state. Never acknowledge an undurable job. |
| New work after queue metadata is ready | Proceed if its storage, ownership, duplicate/admission checks, and other existing requirements permit it. Old unrelated verification does not block it. |
| Pause/resume | Preserve user intent independently of verification. Before metadata is available, persist supported global intent safely or return a retryable response. Resume prioritizes checks; it never bypasses them. |
| Writes or transfer leases for recovered work | Wait for the affected job/file's ownership and verification prerequisites. A conservative job-level hold is sufficient initially. |
| Finalize, PAR, unpack, publish, history success, consumer handoff | Require the relevant verified inputs and existing lifecycle authority. Recovery status must be checked by every entry path, including startup and post-processing replay. |
| Cluster import or authority takeover | Establish evidence tickets for saved ranges as well as journal records. Never finalize or delegate from an unverified snapshot status. |
| Torrent restore and pending-source cleanup | Restore plans come from the owner. Before sidecar removal, recheck current linkage and generation in owner order; a stale disk snapshot cannot authorize deletion. |
| Remove or destructive retry | Invalidate verification tickets, fence the affected operation, and wait for relevant readers/writers to release custody before destructive file changes. Never wait for unrelated jobs. |
| History or settings backed by unavailable storage | Return component-specific unavailable/degraded state promptly. Reading service status must not await those stores; existing safe settings writes may retain their own prerequisites. |
| Shutdown | Stop new work, cancel recovery, fence late results, and retain durable recovery evidence. A blocked storage read must not cause an unbounded application drain. |

### 3.1 Verification state and results must preserve uncertainty

Track `pending`, `checking`, `verified`, and `blocked` verification states,
with reason and progress, separately from queue lifecycle and user pause.
A CRC mismatch is a validation result, not a new queue lifecycle state.

Use typed worker outcomes: verified bytes, proven mismatch/truncation or
missing file on a verified root, unavailable storage/I/O error, identity
change, and cancellation. The current boolean validator collapses these
cases; background recovery must not treat an inaccessible GlusterFS mount
as proof that all its segments need redownloading.

For a proven invalid legacy/untrusted range, the owner marks only the affected ranges pending,
invalidates affected finalization evidence, recomputes totals, and admits
repair/redownload only for work whose bytes are still owned by the download
stage, through the explicit transitions below. For uncertain storage or changed
identity, retain evidence, hold affected work, and retry or request review.
A contradiction of current matching durable proof is a storage/integrity
hold, not an automatic redownload transition.

| Saved job situation | Accepted result and next action |
|---|---|
| Queued/downloading, download stage still owns bytes | Verified ranges remain done. Proven invalid legacy ranges can become pending; corruption despite current matching durable proof is a storage/integrity hold. Recompute totals and schedule only when relevant evidence is resolved. |
| User-paused or deliberately held | Preserve pause/hold and progress evidence. Verification cannot resume the job. A requested resume changes priority and intent but remains subject to evidence. |
| Download-stage completion recorded, PP has not acquired custody | Treat saved completion as provisional. Revalidate or consume a valid checkpoint proof; mismatch returns affected download-owned work through an explicit repair/redownload transition. Do not merely leave `PostQueued` with unfetchable pending segments. |
| `PostQueued`/`Post` with durable PP custody or a retained transformed workspace | Reconcile the PP operation and its generation first. Use existing PAR/manifest and workspace evidence for the current inputs; never CRC-check a transformed path against obsolete pre-PP ranges or automatically rewrite those ranges as pending. |
| Published, transferred, deleted, or consumer-owned payload | Preserve terminal receipts and handoff authority. A startup download scan cannot reacquire custody or trigger redownload. Integrity/repair follows the owning lifecycle's explicit policy. |
| Imported/takeover job | Apply the corresponding row above after acquiring current authority and validating supplied evidence; old `Done` bits and status alone grant no permission. |

The owner issues a PP admission lease binding job/allocation generation, verification
epoch, and current input scope before either rescan or event handling can
launch a subprocess. Revocation and removal coordinate with live PP
custody, just as with writers. Unverified download completion must not be
published as actionable `Completed`, `PostQueued`, or `Post`; retain the
historical stage separately when reconstructing already-owned PP work.
Build the admission token on the existing Inventory operation/generation
model and respect `Artifact:Custody`, `Delivery:Custody`, and
`Inventory::verified_custody_evidence`; do not introduce a second meaning
of custody. At `a253562`, audit all five manager launch paths: `JobFinished`,
pending restart after attempt completion, held-job restart, direct operator
restart, and startup/periodic rescan (`spawn_job` calls at lines 455, 507,
575, 634, and 2101). Centralize the admission check so a new caller cannot
bypass it.

A verification ticket must bind to the process/owner epoch, job generation,
file revision, allocation generation, and opened file identity plus the
exact ranges/expected CRCs. Prevent writes and rename/delete against that
verification scope while reads run. An identity check alone cannot detect
all in-place writes. Apply the result only if the ticket still matches and
the operation still exists. Pause remains effective even when verification
succeeds; deletion, replacement, retry, or takeover invalidates old results.

Keep unverified evidence distinguishable through snapshot saves and restart.
No saved `Done` bit alone may bypass verification on a new process. On
unresolved I/O preserve the original range/CRC/finalization evidence and
mark the scope unresolved. Compaction can proceed when the durable snapshot
faithfully preserves that evidence and its uncertainty/provenance through
the captured journal cutoff. Otherwise retain the uncovered records.
There is no unconditional keep-journals-through-another-restart rule.
Unrelated jobs need not stop saving.

Use the existing inventory installation ID as the first root-enrollment
anchor. Persist the expected ID in durable configuration or an independent
local bootstrap anchor after a successful, verified inventory enrollment.
The current config schema needs an optional backward-compatible field;
this is a proposed addition, not an existing setting. A mirror on the same
missing volume alone cannot establish the expected identity when both are
unavailable. A read-only config/Secret needs an explicit external update or
independent writable anchor; do not silently write somewhere ephemeral.

Before a database-creating `Inventory::open`, an enrolled installation must
check the expected ID and existing store. Missing or mismatched state is
unavailable storage, not permission to create a new installation. Initial
legacy enrollment is explicit and non-destructive: record the existing ID
only after validating that this is the intended inventory/root. Old binaries
must safely ignore the added metadata or have an explicit rollback plan.
An absent config/anchor with uncertain storage is not proof of first run.

Combine that expected installation with current per-artifact root identities,
sidecars, expected mount information, and opened directory handles. `st_dev`
is supporting evidence, not a portable root identifier. The installation ID
anchors the state inventory; it does not by itself prove every payload root.
Use existing allocation/mount evidence for those roots and add a new root
UUID format only if a specific gap remains. Enrollment never gates the
control service.

Resolve all recovery paths from one inventory/allocation resolver,
including boot, journal folding, import, relocation, and final publication.
Use fallible existence checks and descriptor-based `fstat`, checking
identity/size/change metadata before and after reads where relevant.
These checks support ownership and provenance; they do not prove content
integrity against arbitrary external writers.

### 3.2 Background I/O must be bounded in fact

Run blocking reads and lifecycle work outside Tokio's request handlers and
the queue owner. Set finite worker and queue limits, initially one payload
verification worker per storage root with a finite process-wide cap. Use
bounded byte/range batches, yield between batches, and allow urgent work
to preempt at those boundaries. Open each file once per bounded batch to
reduce repeated metadata/open overhead without weakening identity checks.

Cancellation and timeouts do not interrupt a blocked filesystem syscall.
Keep that worker's slot and custody accounted for until it actually exits;
do not accumulate replacement workers on repeated timeouts. Stale results
cannot commit. Shutdown may abandon a blocked read only with authority
revoked and without authorizing destructive changes against its live scope.
Use existing storage-worker patterns where they satisfy these requirements.

Moving `Owner::recover` to a worker is insufficient. The owner's
`save_snapshot`, journal sync, allocation checks, and other storage-backed
calls must not stall its message loop; `Inventory` also has a mutation
mutex that spans filesystem work. Audit these paths and separate in-memory
decisions from bounded I/O requests and durable-commit acknowledgements.
Preserve serialized commit ordering and per-operation custody. Do not hold
the owner or a shared inventory mutex while waiting for a filesystem call;
conflicting work can report pending/unavailable while unrelated controls
continue. A timeout cannot be reported as a failed mutation if its commit
may still land: keep the durable operation identity and let retries query
or reconcile that same outcome.

Asynchronous persistence needs explicit revision and journal boundaries.
Capture a snapshot revision and per-journal replay/compaction watermarks;
acknowledge only that revision after durable publication. A delayed older
snapshot must not overwrite a newer snapshot, clear newer dirty state, or
compact records appended after its capture. Preserve the atomic snapshot
and replay ordering in `save_snapshot_result`; test interruption and
out-of-order completion before moving this I/O off the owner.

Existing shutdown is also part of the change: task-tracker waits,
`Inventory::close`, and Tokio runtime drop can wait on workers or storage.
Use an explicit finite drain deadline, report any remaining storage work,
and retain leases, process-lock custody, and actual worker-slot accounting
across same-process configuration reloads. Never unlock a live worker's
scope and initialize a replacement writer as though cancellation stopped
the syscall. Process termination may use a bounded exit path after fencing
late results; it must preserve the next boot's recovery evidence.

Keep the control listener and component/verification state alive across
ordinary settings reloads. Replace the current `RunOutcome::Reload` loop's
whole-daemon restart with a process-lifetime controller and fenced backend
reconfiguration. Do not rerun a full recovery pass for an unrelated setting.
If an operator changes the bind address or TLS identity, report that
listener reconfiguration explicitly; prepare the replacement securely
before switching where possible. Ordinary reload must not close the UI.

Schedule fairly across roots and eligible jobs, reserve capacity for
foreground control work, and prevent paused-job scans from starving active
work. Give runnable recovery work aging so repeated resume requests cannot
starve it indefinitely. Deferred paused work remains visibly deferred.

### 3.3 Status and health must mean what they report

Expose service responsiveness separately from queue loading, pending and
checked bytes/ranges, affected jobs, last progress time, blocked roots,
retry timing, and reasons an operation is unavailable. Report phase timings
for configuration, store loading, journal replay, lifecycle reconciliation,
and payload verification. Do not promise a precise ETA from stale storage
throughput.

Health means the control service is responsive and can report its state;
it does not imply downloads can resume or every backend is healthy.
Start a loopback-only plaintext bootstrap monitor in the daemon's network
namespace before any volume-backed config or storage I/O. Proposed launch
interface: compiled default `127.0.0.1:6790`, overridden by
`NZBD_MONITOR_BIND`, then by `--monitor-bind` (CLI wins). Accept only numeric
loopback addresses; no DNS lookup and no `nzbd.toml` access. `run` and the
proposed `nzbd health [--ready] [--monitor-bind ADDR]` probe use the same
resolver and precedence. Multiple local instances explicitly select separate
ports; container instances have separate network namespaces. Expose only `/healthz` and a minimal
nonsecret `/readyz` summary there, never data, settings, logs, shutdown, or
an unauthenticated management API. Compose already uses an exec-style probe;
change its subcommand, route, and deadlines, not its execution style.
Kubernetes switches from HTTP probes to exec for container loopback.
The proposed binary probe has a 2-second total deadline with each phase
bounded by the remaining budget, inside explicit Kubernetes
`timeoutSeconds: 3` and Compose `timeout: 5s`. Keep `status` intact.
Exec probes are separate processes: a daemon-only CLI override is not
inherited. Prefer shared `NZBD_MONITOR_BIND` in the container environment
or pass the same explicit flag in both daemon and probe commands. Test
custom-bind propagation, not just default resolution. The transitional
numeric-loopback probe in §5.4 performs no DNS or trust-material read;
connect, TLS handshake where used, and HTTP response all share its total
budget.
Retain transitional `nzbd health --url URL` in the final image; its behavior
must not silently change to probing the monitor. Monitor bind failure
(including a port conflict) is an immediate clear nonzero exit before
storage access, not an indefinitely loading component.

For deployment traffic, `/readyz` returns HTTP 200 only when the configured
secure public control service is responding; a bound socket alone is not
enough. It returns HTTP 503 while public configuration/security/TLS is
unknown or that listener is unavailable. Queue loading, owner starting or
stalled, and an unavailable storage root do not change that HTTP 200 while
the control service remains responsive. Report those component states in
the nonsecret summary and authenticated status/alerting. A Kubernetes
readiness failure withdraws the entire Pod from routing, so using backend
readiness here would recreate the global UI availability barrier.
The exec liveness/readiness modes exit 0 for their respective route's HTTP
200 and nonzero for any other result or deadline expiry. Liveness uses
`/healthz`; traffic readiness uses `/readyz` with the semantics above.
Track a monotonic owner-loop heartbeat with a configurable/testable stall
deadline once the owner exists, separately from intentionally blocked
jobs. Publish failure/readiness transitions for alerting and use a separate
owner-stall liveness policy if automated restart is desired. A green
bootstrap liveness probe must not conceal an unresponsive queue owner.

After the public listener has been ready once, unexpected loss or failed
reconfiguration starts a 30-second recovery deadline. Preserve a functioning
old listener while preparing a replacement where security policy permits;
if no secure control listener returns by the deadline, `/healthz` becomes
503 and the process takes its bounded nonzero-exit path. Report the cause
immediately. This prevents permanent green-monitor/503-public-listener
limbo. Initial config/TLS loading is a distinct visible bootstrap state,
not a false claim that a public listener has already recovered.

Config may be recovered from a mirror on GlusterFS. Put the attempt under
a finite deadline with real worker accounting. Before trusted config is
available, serve only the nonsecret bootstrap surface; never treat an
unreadable mirror as a new installation or expose a setup wizard with
unknown credentials. A static UI shell may be served only through a known
safe public transport with no protected state. Full UI/data/control routes
require known authentication and TLS settings. Preserve TLS identity: use
fallible file probes, never regenerate after EIO/ESTALE, and require proven
first initialization plus atomic/exclusive certificate-pair provisioning.
No plaintext public fallback or identity rotation on delayed storage.
Invalid initial security configuration, including malformed certificate/key
material, exits nonzero with a clear error. A stalled or transient storage
read remains retryable, leaves public readiness at 503, and raises visible
diagnostics while local liveness continues. Do not conflate configuration
invalidity with transient unavailability or apply the post-ready 30-second
listener-loss deadline before any listener has been ready.

Native, NZBGet, qBittorrent-compatible, web, and mobile clients need explicit
loading/unavailable behavior. Successful empty queues/torrent arrays can
cause false job-removal decisions. Replace the web reload poll's assumption
that `/healthz` success means all UI data is ready; consume component status.
The existing public `/healthz` route may remain for compatibility,
but deployment liveness uses the independent local monitor. Source edits
do not update nuc3's untracked Compose file.

### 3.4 Record durable acknowledgements instead of rereading every byte

`write_segment` awaits `sync_data` before `SegmentWritten` at both inspected
revisions (`writer.rs:336–339`, introduced by `3f1efc21`). A successful
current-writer acknowledgement already proves data sync under the storage
contract. `JournalRecord` has no writer-protocol/identity field, and neither
snapshot schema 4 nor 5 identifies which writer supplied a saved range.
The missing information is persisted provenance, not a missing watermark.

Add a versioned durable-ack proof bound to installation/root identity,
allocation and file generation, writer generation/protocol, exact completed
range coverage, and identity from `fstat` on the opened writer descriptor.
Persist equivalent proof in the snapshot and journal records or a
versioned journal header. A header must not retroactively bless older
records or a differently generated writer. Missing or unknown provenance
is untrusted, including foreign journals.

A per-file proof may cover all completed ranges only when every one is
known to come from durable acknowledgements or a valid verification pass.
Writing one new synced segment cannot stamp old unverified `Done` ranges
in the same file as trusted. Bind writer acknowledgements themselves to
file/allocation/writer generation and discard stale messages after import,
replacement, or retry; the current `SegmentWritten` has no such generation.
The stale-message hazard is not limited to result delivery. At this
baseline `remove_job_silent` drops writer handles without canceling `stop`,
while leases retain sender clones. The old task and fd can therefore remain
live after import/re-add. Extend the existing `retiring_writers` / asynchronous
`stopped.wait_for` pattern: cancel the old writer, keep the scope visibly
`replacing`, and wait outside the owner loop before starting any replacement
writer. A closed watch, canceled wait, or timeout is not proof of stopped
I/O; leave the scope unavailable and accounted for. Never freeze the owner
on that wait, and never reuse the path while an old writer can access it.

Fence all stale results before state changes: `SegmentWritten`,
`SegmentFailed`, `WriterFinalized`, `WriterError`, `FileMetadata`, and
`JobNameInspected`. Bind network-produced outcomes to the applicable
lease/writer/file generation so stale failure cannot spend the replacement's
attempts or fail its segment. Rejecting a message alone does not fence disk
writes. These mechanisms are source-supported, not reproduced on nuc3.

Give `spawn_writer` the proven `(offset, len, crc)` ranges for its generation.
The serial writer also tracks every range it has written during that
generation, whether or not the owner has acknowledged it. Check against
both sets before `seek` or `write_all`. This prevents A from syncing
`[0,100)`, B from overwriting `[50,100)`, and A's delayed acknowledgement
then blessing B's bytes. Include successful-but-not-yet-synced writes in
the reservation/accounting path; a failed or uncertain write holds that
scope rather than allowing a conflicting successor to overwrite it.

An exact duplicate has identical offset, length, and CRC; acknowledge its
existing evidence without rewriting only after the prior write's sync is
known to have succeeded (owner acknowledgement may still be pending).
Replay is idempotent for the same logical segment; do not double-count
completed bytes or use one range proof to complete a distinct overlapping
segment ID. Treat such distinct overlapping claims as malformed.
The same extent with a different CRC,
or any conflicting overlap, is a rejected segment/malformed-post outcome.
Download writes must not use invalidate-then-overwrite as an alternative.
Finalize refuses `set_len` below any proven or written range end and holds
an identity conflict. Extend-only preallocation does not revoke proof.

Before nzbd unlinks/recreates or otherwise reuses a partial path, durably
retire its old generation in snapshot/journal authority and await actual old
writer quiescence. Allocate a distinct new generation before new writes;
proof and journal records carry that generation. A reused inode and the
same preallocated size cannot make retired proof valid for the new file.
This ordering, rather than a new allocation-xattr system, is the first
implementation's inode-reuse protection.

Download write authority retires at publication. Retain its immutable
content proof in publication evidence for interrupted-finalize recovery;
it grants no future download writes. At PP admission that old proof must
stop authorizing the current contents before any transformation. Subsequent
repair or extraction uses existing Inventory generation/operation authority: the
relevant `par_repair`/`extract` operation must be durably `running` before
tools mutate its scope. Do not build a parallel proof-invalidation protocol
for PP, and do not reuse a pre-transform download proof after such mutation.
The admission lease must preserve this existing transition ordering.

On boot, open each applicable file through the common resolver. Reuse
`nzbd_state::artifacts::Identity` and its descriptor-backed capture and
`same_object` semantics: equal nonempty `mount_id` plus inode (and regular
file type), or device plus inode when mount identity is unavailable/legacy.
Do not compare the entire `Identity`: modified/changed timestamps and size
legitimately change during writing or publication. Size covering each range
end is only a bounds check, not byte evidence; preallocation already gives
holes their final apparent size.

Trust requires compatible durable provenance, exact covered ranges, a
current nonretired generation, established root identity, and matching file
object identity together. Legacy/foreign ranges without proof get scheduled
verification once, then gain proof only for the stable, durable verified
scope. Changed identity is held for reconciliation or verification, never
automatically adopted. Do not add a second sync to every current segment.

For GlusterFS, reuse existing mount source/type/root identity handling for
device renumbering rather than inventing a second remount identity system.
Confirm GFID-derived inode stability and that successful sync reaches the
bricks under deployed write-behind/flush-behind settings. If cross-node or
remount object identity cannot be established safely, leave that scope
untrusted. Metadata is not proof against arbitrary external mutation. A CRC
contradiction on current matching durable evidence is a storage/integrity
hold and alert, not blind redownload into the suspect store. Legacy proven
mismatches keep the explicit lifecycle-aware repair/redownload policy.

On proven, enrolled files this changes range rereads into bounded per-file
metadata checks. Existing nuc3 records lack this proof and can still need
one full validation pass at migration; no immediate production speedup is
claimed. Keep background verification bounded because the legacy set or
an unavailable mount can still be large or indefinitely slow.

Fix stale comments in `finalize`, its failure test, and `Owner::recover`
that describe current segment writes as page-cache-only. Specify backward
compatibility for old snapshots/journals and mixed-version cluster writers;
old or unsupported proof versions take the conservative validation path.
Do not introduce new watermark or clean-exit trust protocols.

### 3.5 Reconcile publication when acknowledgement or shutdown is interrupted

Persist publication intent before dispatching `WriteCmd::PublicationName`:
the current caller sends `f.filename`, which can differ from the writer's
initial target or the last durable snapshot's name. Bind it to the same
file/allocation generation and retain it through journal/snapshot recovery.
The common resolver considers the stable partial, legacy partial, and the
recorded final publication. It never chooses a different path because a
stat failed and never treats a conflicting existing file as permission to
overwrite it.

If a matching final file exists and the partial is absent, preserve `Done`
and route through idempotent completion reconciliation. The current
`finalize` already has a no-part/final-file combined-CRC validation path;
recovery must not erase its inputs before reaching it. Final-file presence
alone is insufficient: validate the expected identity/generation and
completion proof. With current immutable proof and the recorded hard-link
publication, fd object identity can reconcile without a new CRC read;
keep the CRC path for legacy/unknown evidence. A conflict stays held.
Cover publication-name changes and crash
points around link, directory sync, partial removal, and acknowledgement.

During shutdown stop new admission/leases, quiesce writers and PP, and
continue servicing their acknowledgements while draining. Apply accepted
`SegmentWritten` and `WriterFinalized` results before the final durable
snapshot and journal cutoff. Keep the receiver live and handle full channels
so waiting for writers cannot deadlock against their result sends. On a
finite drain timeout, preserve pending publication/evidence for next boot;
never fabricate a finished drain. Include live name inspections and other
result-producing tasks in the same accounting.

`was_unclean` remains diagnostic. Clearing the marker neither grants trust
nor substitutes for data/provenance checks. Clear it after an actually
completed drain if retained, but do not build a new marker-based correctness
protocol. A crash or timed-out drain must still recover an intact publication
without demoting all ranges or colliding with it on redownload.

## 4. Guardrails and alternatives

This is a documentation proposal. It does not authorize a production
restart, migration, payload deletion, or deployment.

- Preserve CRC validation, ownership fencing, journaling, and publication
  durability. Removing checks would trade an availability bug for data loss.
- Preserve real user pause and existing failure/retry policy. A recovery
  worker may not silently resume paused downloads or reset deliberate holds.
- Keep fatal listener/configuration/local control-service ownership errors
  explicit.
  A live service with a corrupt store reports that component unavailable;
  it does not invent replacement state or loop forever without diagnostics.
- Cover single-node operation first. Shared cluster ownership/leases need
  the same invariant; never let an unverified record escape via delegation.
  Define cluster behavior and tests before enabling the new path there.
- A longer Compose start period is a temporary mitigation only. It leaves
  the UI offline and cannot bound a stalled filesystem.
- A listener that serves only a green probe while all controls hang is
  insufficient. Moving all recovery into one blocking task or holding one
  global mutex across it also fails the contract.
- Local SQLite placement and faster CRC scans may reduce cost. They do
  not substitute for availability under slow or inaccessible storage.

## 5. Corrections, availability work, and observable acceptance

### 5.1 Only proof may demote saved completion evidence

Ship typed validation and fallible path selection, including TLS generation
checks, as a standalone correction. Unresolved outcomes never demote `Done`
or erase finalization/provenance evidence. Snapshot that evidence together
with unresolved state; ordinary compaction remains valid when the durable
snapshot covers the records and preserves all information needed for replay.
Otherwise keep the uncovered records. Correct the misleading page-cache
comments alongside the changed behavior.

**Acceptance:** injected EIO/ESTALE survives first save, ordinary safe
compaction, and another restart with progress/evidence intact. No mass
redownload, false completion, or PP launch occurs. A proven mismatch follows
lifecycle policy; a failure on current durable proof is surfaced as a
possible storage fault. TLS probing failures do not rotate identity.

### 5.2 Add durable-ack provenance and fd-bound identity

Implement §3.4 for local writes, snapshots, and foreign journals. Establish
current protocol coverage for all claimed ranges without blessing mixed
legacy ranges. Reject stale-generation acknowledgements. Test and document
storage assumptions; use conservative validation where identity or sync
semantics cannot be established. The clean marker is never the trust bit.

**Acceptance:** enrolled current-protocol files restart with no payload CRC
rereads; mixed/unknown legacy and foreign fixtures are validated once and
stamped only for the established scope. A new write cannot bless an old
unverified range. Crash each data-sync/acknowledgement/journal/snapshot
boundary. Send A `[0,100)` and B `[50,150)` while A's owner acknowledgement
is delayed: B is rejected before changing bytes. Seed a restarted writer
with proven ranges and repeat. Same offset/length with different CRC is
rejected; the exact tuple replays without a disk write only after successful
sync of the original. Finalize cannot trim below any proven/written end.
Duplicate replay does not double-count completion, and a different segment
ID claiming the same extent is rejected as malformed.
During remove/import/re-add, cancel the old task and await its actual stop
asynchronously; a stuck writer leaves the scope replacing while controls
respond. Queued old leases never reach a replacement file. Recreate the
same inode and apparent size after durable retirement and prove old records
cannot match the new generation. Check stale `SegmentFailed` as well as
all other result types. PP repair uses its existing operation transition.
Validate protocol compatibility, cross-node identity behavior, and
Gluster remount/sync semantics before enabling its fast path. Expected
metadata-only timing is measured, not inferred from the old CRC sample.

### 5.3 Recover final publications and drain writer results correctly

Persist publication intent and unify partial/final path resolution. During
shutdown keep the owner receiver processing results until writers finish
or the finite deadline expires, then save accepted state. Keep publication
reconciliation safe even after an abrupt crash before acknowledgement.

**Acceptance:** deterministic barriers after link, directory sync, partial
removal, and before `WriterFinalized` reproduce the shutdown/crash boundaries.
Restart preserves a complete final file without redownloading or EEXIST.
Repeat with a changed `PublicationName`, both partial/final names present,
conflicting identity, missing expected size, and I/O failure. A full result
channel during drain cannot deadlock owner and writers. Marker presence or
absence does not change which evidence is trusted.

### 5.4 Correct probe deadlines and the Kubernetes restart hazard early

At `a253562`, Kubernetes uses public HTTP liveness with a 10-second initial
delay and 30-second period, with default threshold 3 and timeout 1 second.
With no startup probe, a roughly 183-second pre-listener start can be killed
around 100 seconds. This restart-loop risk is source-confirmed; no live
Kubernetes failure was observed.

Do not wait for the full runtime redesign to fix this manifest hazard.
The early probe release adds bounded `nzbd health --url URL` transition
mode against `/healthz`, retaining this form in later images. This local
transition accepts only numeric loopback HTTP/HTTPS targets, sends no
credentials/cookies, disables redirects and environment proxies, and never
reads `nzbd.toml`. For HTTPS, this narrowly scoped nonsecret probe skips
server certificate verification only for the accepted literal loopback
target and exact health route; it must not weaken any public server,
normal API client, or nonloopback connection. The final monitor remains
plain loopback with no trust-material read. Keep the 2-second total bound.
The example HTTP command is `nzbd health --url http://127.0.0.1:6789`.

Resolve the actual Compose dependency failure in this early correction:
choose `condition: service_started` for `nzbd-discovery` in both shipped
recipes. Advertising an endpoint before it serves is acceptable; discovery
is endpoint discovery, not a download-readiness promise. Preserve truthful
TLS/auth metadata and client loading behavior. Apply the equivalent change
to nuc3's untracked recipe only during an authorized rollout. Do not leave
this dependency change until the later runtime milestone. The main service
can still report unhealthy during old pre-listener recovery; this early
change prevents discovery being stranded and does not claim the control
service is already available. Test ordinary `compose up -d` behavior and
state explicitly that `--wait` retains its own health-wait semantics.

Do not deploy a probe command before its supporting image exists. Treat
manifest/probe and image as a versioned rollout pair: rolling back to a
binary without `health` also restores a compatible probe manifest. A
pre-deployment compatibility check must catch mixed old-image/new-probe
combinations before they cause pod restart loops.

For the transition, use an exec `startupProbe` with 5-second period,
`timeoutSeconds: 3`, and `failureThreshold: 120` (about 600 seconds), so
liveness cannot kill the observed 183-second startup. Set readiness and
liveness timeouts explicitly to 3 seconds; the client total is 2 seconds.
This is a bounded interim mitigation, not completion of the availability
requirement, and larger datasets may still exceed its allowance.

Once the bootstrap monitor starts before storage I/O, use exec liveness
against local `/healthz`, readiness against local `/readyz`, and an exec
startup probe against local `/healthz` (5-second period, 3-second timeout,
6 failures: about 30 seconds). Preserve the routing semantics in §3.3.
Compose keeps its CMD execution style and 5-second timeout while switching
to the bounded health subcommand. Test run/probe flag/env/default parity
with config reads deliberately blocked.

**Acceptance:** the Kubernetes manifests and matching image tolerate a
simulated 183-second backend startup without a liveness restart. Both
Compose recipes leave discovery running after that startup without a
manual `start`. Validate image/probe rollback pairing, retained `--url`
compatibility, immediate monitor bind failure, and invalid-versus-stalled
initial TLS behavior. Stalled connects and
responses finish within the probe budget. Final bootstrap probes succeed
with every storage/config barrier closed; sensitive public routes remain
unavailable until security is known. Never use an unsupported probe command
or rely on Kubernetes' 1-second timeout default.

### 5.5 Enforce import, PP admission, and torrent cleanup authority

Apply boot-equivalent proof requirements to cluster import/takeover.
Centralize the PP admission lease across all five manager launch paths in
§3.1 using existing Inventory generations/operations. Defer hold/retry
rewrites until their evidence is accepted. Restore torrents from owner
plans and condition sidecar cleanup on current owner linkage/generation.
These safety corrections do not require asynchronous snapshots first.

**Acceptance:** corrupt or unknown imported `Done` ranges never finalize or
launch PP. Exercise each manager entry path while verification is blocked.
Concurrent early torrent admission survives stale recovery cleanup plans.
Transformed and handed-off payloads follow their own lifecycle authority.
An old writer acknowledgement cannot complete a replacement generation.

### 5.6 Complete independent control startup and persistent runtime recovery

The preceding correctness and reduced-reread release is not the final
availability fix. Preserve the original requirement even for a large
one-time legacy verification pass or an indefinitely slow mount.

Keep the control listener and verification/component state alive across
ordinary settings reload. Move expensive backend work behind component
states: config mirrors, history opening/trim, Inventory open/reconcile/
allocate, destination/category mkdir and canonicalize, marker I/O,
`VolumeBook::load`, journal replay, torrent session/restore and librqbit
piece checking, TLS, and router construction. Preserve librqbit integrity
checks without duplicating Usenet CRC machinery.

Move owner/global-lock filesystem waits behind bounded asynchronous work
with serialized commit acknowledgements and snapshot revisions/journal
cutoffs. Keep slots and scope ownership across reload when a syscall is
still live. Coalesce remaining reads opportunistically; throughput tuning
is not an extra prerequisite once provenance eliminates the normal scan.

Wire native, NZBGet, qBittorrent, web, and mobile loading semantics. Track
owner heartbeat and component failure without withdrawing functioning UI
routing. Set `service_started` for independent discovery with truthful
metadata. Initial targets are 20-second application drain and 30-second
Compose/Kubernetes termination grace, validated against the whole teardown.
Unexpected loss of a previously ready public listener gets the explicit
30-second recovery/liveness policy in §3.3.

**Acceptance:** with metadata/payload barriers closed, known-secure UI,
status, and available controls remain reachable through deployment routing.
Before security is known only the local nonsecret monitor responds. Root
or owner failure is visible without falsely empty queues. Ordinary reload
keeps the listener up without rerunning full recovery. Failed public rebind
cannot leave permanent healthy-monitor/unavailable-public limbo. Exercise
snapshot reorder/crash boundaries, mutation timeouts, worker caps, repeated
reload with a stuck syscall, and finite drain. No test releases a recovery
barrier merely to make readiness pass.

### 5.7 Validate production behavior and compatibility separately

After implementation review and authorized rollout, preserve nuc3's
untracked configuration/Compose file and rollback image. Record exact
source/image identity and phase timings. Verify discovery startup, stable
pause intent, visible recovery progress, and availability before any long
legacy pass finishes. New proof fields, expected installation ID enrollment,
and mixed-version cluster behavior need explicit rollback/compatibility
coverage. A read-only deployment config must have a durable enrollment path.

**Acceptance:** measured service availability and metadata-only fast-path
behavior match their separate contracts. Legacy verification can take time
without holding the service offline. No correctness claim depends on the
original warm-sample CRC estimate or on checkout identity proving image
identity. Source inspection, deterministic tests, and live observations
remain labeled separately.

## 6. Independent review request

Review the diagnosis and proposed design; do not implement or deploy it.
Challenge the evidence boundary, early control-service feasibility, every
operation's dependency scope, stale-result and ownership races, durability
across restart, typed storage failures, real worker bounds, shutdown, and
native/NZBGet/qBittorrent compatibility behavior. Check the existing segment
`sync_data` guarantee before recommending new watermark machinery. Identify hidden global barriers and
paths that could consume unverified evidence. Return prioritized findings
with concrete corrections, and distinguish blockers from later optimization.
