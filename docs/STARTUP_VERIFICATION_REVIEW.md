# Startup verification review — findings and disposition

**Status:** Opus third review incorporated; Astra approved the amended specification;
implementation has not started · **Updated:** 2026-11-01 ·
**Reviewers:** GPT-6 Astra, high reasoning, and user-supplied Opus review

Companion to [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md)
(incident evidence, runtime recovery contract, and acceptance criteria).
This record preserves the independent reviews and their disposition.
The initial Astra review was read-only against local source
baseline `9e4d6c9`; Astra accepted the recorded production observations and
did not independently inspect or alter nuc3.

## Initial review required four corrections

| Priority | Finding | Correction in the revised plan |
|---|---|---|
| P1 | `Inventory::open` opens and locks `artifacts.lock` on the state volume. Calling process ownership a startup prerequisite could recreate the storage barrier. | Section 2 separates local control-listener ownership from storage-backed writer authority. Serve the control service while acquisition is pending; never allow a second data writer. |
| P1 | Backgrounding `Owner::recover` leaves synchronous owner snapshot/journal/store calls and inventory mutation locks spanning filesystem work. Asynchronous persistence could also reorder snapshots or compact newer journal records. | Section 3.2 explicitly requires bounded asynchronous I/O, operation-scoped custody, commit acknowledgements, snapshot revisions, and journal capture/compaction watermarks. Section 5.2 requires stalled-storage, ambiguous-outcome, crash, and reordered-completion tests. |
| P1 | Tracker waits, `Inventory::close`, and Tokio runtime drop can still block shutdown. Same-process reload can create new workers or release custody while old syscalls remain live. | Sections 3.2 and 5.3 cover the complete drain/teardown path, retained live-worker capacity and custody, fenced late results, and repeated reload tests with a blocked syscall. |
| P2 | The current plain-TCP helper has a 30-second read timeout and lacks complete deadlines or HTTPS support. | Section 3.3 requires transport-correct HTTP/HTTPS probing, explicit trust, stable certificate identity, and connect/write/read/overall bounds shorter than the Compose timeout. Section 5.3 requires authenticated HTTP and HTTPS daemon tests. |

Astra also identified post-manager startup sweeps and cluster journal
folding/authority adoption as explicit audit targets. The revised milestone
names them because these paths can consume recovered completion evidence
independently of initial startup. The editorial ambiguity about process
ownership in the guardrails was corrected to local control-service ownership.

## Astra approved the earlier draft before Opus's review

After re-reading the initial corrections, Astra approved that draft
with no remaining substantive blockers at specification level. All four
initial findings were resolved. The reviewer agreed with the causal
diagnosis and with the distinction between observed startup duration and
the warm-sample CRC estimate. Checkout identity remains qualified separately
from the deployed image's unknown source identity.

That approval covered the earlier diagnosis and design. It does not establish that
code, migration, rollback, or production behavior is correct. Concrete
persistence sequencing, custody across reload, client unavailable-response
mappings, and TLS bootstrap configuration still require implementation
review and the plan's tests. File-read batching, finer-than-job gating, and
CRC throughput tuning are later optimizations; they must not delay the
availability and safety contract.

The section numbers above refer to the earlier draft. Opus subsequently
identified concrete gaps, so the earlier no-blockers verdict must not be
read as approval of this amendment or proof that the original requirements
were sufficient.

## Opus findings were checked against source

The [supplied review](STARTUP_VERIFICATION_OPUS_REVIEW.md) is preserved
separately. The following table distinguishes accepted mechanisms from
claims needing qualification; no listed failure was reproduced on nuc3.

| Opus finding | Disposition and proposal change |
|---|---|
| P0-1: I/O becomes corruption, then evidence is compacted | Confirmed mechanism. Add a standalone typed-outcome/path-resolution fix that preserves original progress and affected journals; require fault-injected first-save/restart tests. This corrects a present defect, not only future concurrency. |
| P0-2: takeover/import trusts snapshot `Done` | Confirmed. Require boot-equivalent evidence tickets on takeover/import before finalize/delegation; journal-only validation is insufficient. |
| P0-3: PP reads snapshot status before owner commit | Confirmed. Add mandatory owner-issued PP custody before subprocess launch, including rescan/event paths, and defer evidence-dependent boot hold/retry rewrites to accepted-result handling. |
| P0-4: torrent recovery independently deletes orphans | Confirmed stale-snapshot deletion mechanism. Early concurrent admission introduces the described race; inspection does not prove it occurs in the existing pre-listener startup sequence. Require owner-provided restore plans and conditional owner-ordered cleanup. |
| P1-1: no per-segment data sync; add watermarks first | Incorrect premise at `9e4d6c9`: `write_segment` awaits `sync_data` at lines 336–339 before reporting success, introduced in `3f1efc21`. Retain the goal of avoiding redundant reads, audit current proof and legacy/import provenance, and add checkpoint format only for demonstrated gaps. Availability must not depend on this optimization. |
| P1-1: clean marker precedes writer quiescence | Confirmed ordering; marker absence alone is insufficient. Add an explicit quiesce/flush/durable-snapshot/marker-clear order. Do not infer loss of already-synced segment bytes merely from this ordering. |
| P1-2: remaining startup barriers and torrent checks | Confirmed inspection targets. Name history trim, destination/category mkdir, marker I/O, volume loading, session/restore, and librqbit initialization/piece checks; put them behind component state while retaining integrity requirements. |
| P1-3/P1-4: config/TLS/bootstrap dependencies | Accept restricted local bootstrap monitoring while security configuration is unknown. No public data/controls or permissive first-run fallback; preserve TLS identity and distinguish real first initialization from I/O errors. Adopt loopback-only liveness/readiness with exec probes instead of coupling liveness to public HTTPS. |
| P1-5: ordinary settings reload repeats full startup | Confirmed. Require process-lifetime control listener and verification state across ordinary reloads, not just retained worker accounting. |
| P1-6: missing lifecycle transitions | Accept explicit download-owned, user-held, pre-PP, PP-owned/transformed, terminal/consumer-owned, and imported/takeover transitions. Post-owned data cannot be reset from obsolete download CRCs. |
| P1-7: discovery dependency and termination grace | Accept `service_started` for independent advertising with truthful metadata. Add finite application drain and deployment grace with margin; proposed 20-second/30-second targets require validation. |
| P2: compatibility, mount identity, descriptor identity, resolver | Explicitly add qBittorrent and web/mobile loading behavior, provisioned root UUID plus mount evidence, fd-bound checks, and one inventory-aware path resolver. `st_dev`/ctime support custody but are not durable content proofs. |
| P2: readiness/heartbeat and I/O budget | Add nonsecret `/readyz`, owner heartbeat/stall reporting, per-root limits and coalesced batches. Health and full backend availability remain separate. |
| P2: custody implementation complexity | Accept owner-side tickets/reservations as the default for internal writers and PP. They still require real cross-process exclusion where external tools or librqbit own a scope; no new per-file filesystem lock system is mandated. |

Root provisioning and durable-proof formats still need explicit migration
and backward-compatibility decisions at implementation time. Missing
markers on legacy installations must not silently create fresh authority
on an unmounted path.

## Astra approved the amendments after two additional corrections

The amended design was returned to Astra for source review. Astra confirmed
the segment-sync correction and accepted the explicit PP, cluster, torrent,
bootstrap, reload, and lifecycle requirements, then requested two changes:

1. **Keep traffic readiness independent of backend recovery.** Kubernetes
   readiness can remove a whole Pod from Service routing. The plan now
   defines `/readyz` as HTTP 200 when the securely configured public control
   service responds, even while metadata loads, an owner stalls, or a root
   is unavailable. Initial public security/TLS unavailability returns 503.
   Exec probe exit semantics and routing-level fault tests are explicit.
   Backend/owner failure stays visible through status, heartbeat, and alerts.
2. **Provide a legacy-root migration contract.** The new root UUID must not
   be mistaken for existing inventory fields or permanently block existing
   installs. The plan now requires non-destructive enrollment, independently
   trusted expected-root binding, equivalent legacy evidence where proven,
   and rollback. Enrollment never gates control-service startup.

Astra rechecked both corrections and approved the amended proposal with no
remaining specification blockers. That was the design verdict for that draft; the subsequent re-review
and amendments below supersede it. Neither approval proves
the implementation, migration, or deployment safe. Opus then supplied the second re-review recorded below. No Opus approval
is claimed.

## Opus second re-review resolved the concrete correction sequence

The [second supplied review](STARTUP_VERIFICATION_OPUS_REREVIEW.md) moves
implementation to `a253562` and identifies B1–B4. Read-only inspection of
that Git object confirms it is 34 commits ahead of `9e4d6c9`; the four
recovery/import functions, writer, and TLS code are unchanged. PP has five
launch paths and the baseline uses queue schema 5. The working checkout
has not been rebased or its source edited; the plan explicitly distinguishes
its linked local files from the immutable implementation baseline.

| Finding | Disposition in this amendment |
|---|---|
| B1: missing durable-ack provenance | Accept as the concrete second correction. Current snapshots/journals have no protocol and descriptor identity proof. Add versioned writer/file/allocation generation and exact covered ranges, including foreign journals; do not bless mixed legacy ranges after a new write. Legacy data can need one verification pass. Gluster inode mapping and actual sync semantics must be established before enabling metadata-only trust there. No watermark or extra per-segment sync is required. |
| B2: finalize completes after owner stops receiving | Confirmed source interleaving, not reproduced. Persist publication intent, resolve stable/legacy/final paths, retain `Done` when an intact final publication can be reconciled, and drain/apply writer results before the last snapshot. The marker stays diagnostic. At this baseline the command uses current `f.filename`; it can differ from the writer's initial target or older durable metadata, rather than being an arbitrary independent current filename. |
| B3: Kubernetes kills a slow pre-listener start | Confirmed manifest hazard; no live Kubernetes failure observed. Specify an early bounded public-health probe and startup grace with explicit Kubernetes timeouts, then switch to bootstrap exec probes once supported. Do not configure a monitor command before its image implements it. Compose already uses CMD: change route, client deadline, and command. |
| B4: bind/probe can depend on volume config | Specify compiled `127.0.0.1:6790`, environment `NZBD_MONITOR_BIND`, and CLI `--monitor-bind`, with shared run/probe precedence, numeric loopback validation, and no `nzbd.toml` or DNS dependency. |
| Simpler journal policy | Accept. Only proven invalidity demotes `Done`. Compaction can proceed when the durable snapshot preserves original evidence, unresolved/provenance state, and the captured journal cutoff; otherwise retain uncovered records. No mandatory extra-restart journal retention. |
| Simpler root enrollment | Use existing inventory installation ID anchored independently in configuration/local bootstrap metadata, with explicit read-only config enrollment. Check expected identity before database creation. It does not replace per-payload mount/allocation evidence. New root UUIDs are deferred unless a gap remains. |
| Existing PP custody vocabulary | Name the new token a PP admission lease, built on Inventory operations/generations and existing custody evidence. Audit five manager launch paths at the newer baseline. |
| S2: permanent public-listener loss | Add a 30-second recovery deadline after losing a previously ready public listener; report immediately, then fail monitor liveness and take bounded nonzero exit if unrecovered. Initial security loading is a separate state. |
| S3: stale writer acknowledgements | The message lacks a generation; the hazardous interleaving is not reproduced. Bind acknowledgements to writer/file/allocation generation and quiesce old writers before replacement. Dropping stale messages alone cannot prevent old disk writes. |
| S4/S5: current-proof corruption and path resolution | Hold and alert on contradictory current durable proof; do not reflexively redownload into suspect storage. Resolve and test recorded publication names as well as both partial conventions. |

The early correctness/provenance release does not finish the user's
availability requirement. Large legacy verification or inaccessible storage
still requires independent controls, bounded runtime recovery, and a
listener that survives ordinary reload. Those remain mandatory planned
work, rather than being dismissed because the normal reread set shrinks.

## Astra review identified an additional durable-proof mutation hazard

Astra confirmed that historical sync plus matching inode/size cannot
preserve trust across later same-inode writes. The writer can receive
conflicting offsets; rejecting overlap only at finalize is too late.
A second write could change previously proven bytes and crash before its
acknowledgement, leaving old snapshot proof apparently valid.

The plan now makes proven scopes immutable: reject conflicting overlap
before write, avoid rewriting proven duplicates, or durably invalidate
proof/advance the mutation generation before overwrite, truncate, resize,
or repair. Quiesce stale workers before replacement and test crash after
mutation but before acknowledgement. This is a fast-path design hazard,
not an observed production failure.

Astra rechecked the saved mutation rule and crash-test requirements and
approved the revised design specification with no remaining blockers. The
final check also confirmed generation fencing for `WriterFinalized`,
`WriterError`, `FileMetadata`, and name inspections, plus shared monitor-bind
configuration for separately executed probes and total probe deadlines
including resolution/trust loading. Approval covers the specification;
implementation crash safety, Gluster assumptions, and deployment behavior
remain untested requirements. That verdict applied to the second-review
amendment; the third review and changes below supersede it.

## Opus third review closes earlier findings and tightens proof ordering

The [third supplied review](STARTUP_VERIFICATION_OPUS_THIRD_REVIEW.md)
accepts B2/B4 and S1–S5 as specified, while requiring R1–R4. Read-only
inspection at `a253562` confirms the unseeded writer range state, decoded
yEnc offsets, retained lease sender clones after silent removal, existing
async retirement pattern, and `Identity::same_object` comparison semantics.
These remain source-level observations, not runtime reproduction results.

| Finding | Disposition in this amendment |
|---|---|
| R1: proof must include unacknowledged writes | Seed each writer with the proven offset/length/CRC set and track all writes it has performed, even before owner acknowledgement. Check both sets before seek/write; exact duplicates require all three fields and successful prior sync. Conflicts become rejected segment/malformed-post outcomes, with no download overwrite/invalidation alternative. Refuse finalize trim below proven or written range ends; extension alone does not invalidate proof. |
| R2: old writer retains a live fd and sender clones | Extend `retiring_writers`/`stop`/`stopped.wait_for`: cancel, keep the scope replacing, wait asynchronously outside the owner, and block replacement until actual stop. Failed/closed waits do not imply stopped I/O. Add `SegmentFailed` to generation fencing alongside all writer/metadata/name outcomes. No stale disk writer may share a replacement path. |
| R3: early Compose transition still stranded discovery | Choose option (b), `service_started`, in both shipped recipes during the early probe correction. Advertising an endpoint before it serves is acceptable; it conveys discovery, not readiness. This change prevents discovery staying Created but does not itself make the old pre-listener daemon healthy or change `compose --wait` semantics. The nuc3 recipe is untracked and needs the same change at rollout. |
| R4: identity and inode reuse | Reuse `artifacts::Identity` and descriptor-backed `same_object`: mount identity plus inode where available, else device plus inode, with regular-file type. Do not use full-struct timestamp/size equality; preallocated size is only a bounds check. Durably retire old generation before unlink/recreate, then allocate a distinct new generation. Old proof cannot match a reused inode and equal size. |

### Smaller corrections are explicit rollout and failure contracts

Image and exec-probe manifests deploy and roll back together; preflight
rejects an old binary lacking the new command. The final image keeps
transitional `health --url` compatibility. Monitor bind failure exits
immediately before storage access. Invalid initial TLS/security config
exits clearly; stalled material reads remain retryable, traffic-unready,
and visible. The post-ready listener-loss deadline remains a separate case.

### Optional simplifications retain existing authority

Current immutable proof plus expected hard-link publication identity can
reconcile finalization without CRC rereads; legacy evidence keeps CRC.
Publication retires download write authority, while retained content proof
can still establish that immutable publication. PP mutation transfers to
existing Inventory operation authority and a durable running operation;
there is no second invalidation journal.

The numeric-loopback transition health probe requests only nonsecret
`/healthz`, without credentials, cookies, proxies, redirects, DNS, or trust
file reads. Its HTTPS path can skip certificate validation only in that
narrow scope; ordinary/public clients and servers retain their security
contracts. New allocation xattrs remain deferred unless existing identity
and generation ordering prove insufficient; unsupported identity can still
fall back to verification.

Tests now explicitly cover two writes whose owner acknowledgements are
pending, differing-CRC duplicate extents, forbidden final trim, live old
leases during import/re-add, same-inode recreation, automatic Compose
discovery startup after a simulated 183-second boot, and rollback
image/probe mismatch detection.

## Astra approved the third-review amendments

Astra checked the saved R1–R4 changes against `a253562` and approved the
amended design specification with no remaining blockers from that scoped
review. It confirmed the overlap/written-range contract, actual asynchronous
writer termination, generation retirement before path reuse, early Compose
dependency decision, existing identity semantics, and probe/rollback/TLS
failure distinctions.

One optional clarification was incorporated: same-tuple duplicate replay is
idempotent for the same logical segment and cannot double-count completed
bytes or complete a distinct overlapping segment ID. Distinct overlapping
logical claims are malformed. This does not change the no-overwrite rule.

**How to read this result:** all supplied review bodies remain preserved.
The latest amendment has Astra's specification approval. Opus has not yet
reviewed this response to R1–R4; no Opus approval of it is claimed.
Implementation remains proposed. No production change, restart, schema
migration, runtime test, or benchmark was performed in this review.
