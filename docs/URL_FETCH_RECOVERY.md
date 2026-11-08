# Fetching NZB stalls — root cause and recovery contract

**Status:** implemented; validation and delivery recorded in the PR. **Written:**
2026-11-08 (America/New_York). **Incident baseline:** `b5ef661`.

Read this document for the URL acquisition ownership defect and the correction.
The [download performance investigation](DOWNLOAD_SPEED_ROOT_CAUSE_PLAN.md)
covers measured inventory contention. Receipt-model performance changes belong
to pipeline v2; this change does not replace that work or weaken custody checks.

## 1. Evidence and limits

Read-only inspection of nuc3 found the same 23 durable `Fetching` jobs across
repeated observations. They were about 16–79 minutes old, all admitted during
the running process. Each exact source identity had no cached NZB, resolution,
acknowledgement, cancellation, or storage-retry file. Their parameters had no
URL retry metadata. Other jobs were downloading; pause and disk guards were clear.

Affected job IDs: 2345, 2382, 2383, 2391, 2394, 2395, 2398, 2400, 2411, 2419,
2422, 2450, 2488, 2489, 2490, 2491, 2492, 2493, 2494, 2505, 2512, 2513, 2571.

Runner logged repeated owner-reply timeouts but no provider fetch retries in
the inspected window. A subsequent read of Curator's logs found submission
replay `context deadline exceeded` messages matching the transfer tokens of
jobs 2382, 2398, 2489, 2490, and 2491. This supports caller-side timeout as a
real incident condition; it does not establish where every handler stopped.

**Two co-equal manifestations explain the empty fetching rows:** the submitting
future disappeared before consuming the committed admission reply, or its
fetch worker started and timed out obtaining the source descriptor. Neither
exit logged enough information to distinguish all historical cases. The
controlled reproduction proves the latter; do not attribute all 23 to it.

The daemon started with the M1b merge. No comparable pre-M1b timeout-rate
measurement was collected, so this investigation does not establish whether
M1b introduced or amplified the latency. Synchronous per-receipt inventory
refresh in `owner_persistence::persist_then` is a concrete latency path, not
an exclusive measured explanation of every timeout. Share this evidence with
the pipeline v2 work rather than building a separate receipt-refresh redesign.

## 2. Root cause: durable work outlives its executor

On the incident baseline, the owner commits and publishes the `Fetching` row,
bumps the epoch, and sends the admission reply in the same successful branch.
`EngineHandle::add_url` waits on that reply without a reply deadline, then
starts a worker only when `created` is true. Admission therefore survives a
dropped caller, but executor creation does not.

The worker's initial export has a two-second enqueue and ten-second reply
limit. Its catch-all branch silently returns on timeout. `UrlFetchGuard`
removes the in-memory execution claim without arranging another attempt.
Duplicate submission returns the existing ID without creating another worker.

Fable's source review identified two additional manifestations:

| Path | Failure before correction |
|---|---|
| Admission reply abandoned | Committed row exists; no worker starts. |
| Initial source lookup times out | Worker exits; no retry is arranged. |
| `CompleteUrlFetch` enqueue times out | NZB is already cached and a resolution may be staged, but the worker exits with the row still fetching. |
| Takeover's sequential export loop times out | Resumption returns early; remaining fetching rows never receive workers. |

These violate one invariant: an eligible accepted fetching row must remain
executable after a transient coordination failure. Raising a timeout or
periodically retrying old rows would leave that ownership gap intact.

## 3. Correction: derive execution from the existing read model

`url_dispatch::run` is one engine-tracked task. It scans the local immutable
snapshot initially, then reconciles on the existing queue epoch watch,
`UrlFetchGuard` exit notification, or a pending dispatch backoff deadline.
The snapshot retains the work; notifications are coalesced hints.

The dispatcher uses the existing `url_fetches` set to acquire/release a single
local executor slot. It adds no lease, durable dispatch record, status schema,
periodic age scan, or worker-disposition protocol. On exit, the guard releases
the slot and notifies the dispatcher. A still-fetching row is retried after
30, 60, 120, then 300 seconds, capped there. Counters are volatile and reset
on restart; provider/storage deadlines remain durable in their existing stores.
Queue hints cannot reset or bypass an outstanding dispatch backoff.

`add_url` no longer starts execution. `resume_url_fetches` only wakes the
same dispatcher, so one slow row cannot abort takeover's remaining work.
The worker takes an ID and reads the URL and retry state from its fresh owner
export. Existing source identity and completion checks remain authoritative.
Cluster dispatch also checks the current queue-authority snapshot and guard.

Startup retains `plan_url_refetches_scoped` as a one-time legacy duplicate
policy. Its duplicate dispositions use the same executor claim and retry path.
Normal reconciliation never re-runs deduplication against accepted admissions.

### Admission durability and lifecycle races

An unrelated snapshot publication can expose a newly inserted row while its
acceptance save is still pending. The owner now uses its existing
`pending_control_revisions` export barrier for that interval. The callback
clears the matching barrier, publishes commit or rollback, and bumps the epoch.
The dispatcher may see the row early, but its worker cannot obtain a source
descriptor and contact the provider before acceptance commits.

Workers recheck `Fetching` and hold state after export. A row deleted between
snapshot read and export yields no acquisition; a resolved row is not fetched
again. A lifecycle hold prevents dispatch. `paused: true` remains different:
Runner acquires the NZB and leaves the resulting download paused, preserving
the former behavior.

### Provider, storage, and control-plane failures

The dispatcher does not interpret provider or storage deadlines. An active
worker continues to honor `fetch_retry_state` and its storage-retry file.
Cached source and staged outbox records survive worker replacement.

Progress publication and preparation timeouts are coordination failures. Their
retry waits retain the in-memory operation and its provider deadline instead
of writing a misleading storage failure. Actual storage failures keep the
existing durable storage retry behavior. The issued `CompleteUrlFetch` mutation
still waits for its actual acknowledgement: this correction does not introduce
a deadline that could overlap an unresolved commit with a replacement worker.

Worker exits record job, source identity when available, and execution phase.
Unexpected pending exits and coordination failures produce warnings. Poisoned
execution-set locks are reported and their set is recovered. New diagnostics
do not include credential-bearing provider URLs.

## 4. Validation and delivery

The diagnostic baseline test reproduced a permanent orphan after a delayed
owner reply. The implementation regressions assert progress instead and run
in `make fast-check-rust`, including:

- Abandoned committed admission reply, including an add-paused submission.
- Actual ten-second lookup timeout; other takeover rows progress meanwhile.
- Full owner channel at completion enqueue; recovery reuses cached NZB bytes
  against an unreachable provider URL.
- Pre-commit export barrier and failed-save rollback publication.
- Repeated worker exits with many coalesced hints; backoff prevents respawn spin.
- Deletion between snapshot read and lookup; no subsequent acquisition.
- Authority refusal followed by takeover wakeup.
- Production backoff schedule, including saturation.
- Lifecycle hold release and control-plane progress failures without false storage retry records.

Broader engine and URL restart/idempotency suites cover existing acquisition
and custody behavior. CI and final PR results are recorded in the PR. No live
recovery, restart, or deployment was performed during diagnosis or local work.
Merging this correction does not itself deploy it. A later deployment can
recover the incident rows through the ordinary startup dispatcher, preserving
their job IDs, transfer tokens, and admission generations.

## 5. Fable review disposition

| Finding | Resolution |
|---|---|
| Completion enqueue orphan omitted | Added to diagnosis and tested with cached source plus a saturated channel. |
| Takeover can abort mid-list | Replaced sequential exports with a dispatcher wakeup; tested with a stalled first lookup. |
| Coordinator overbuilt | Reused snapshot, watch, execution set, and tracker; added only dispatch bookkeeping and exit notification. |
| Lease/disposition protocol unnecessary | Kept existing claim and completion guards; dispositions are diagnostic context only. |
| Historical manifestation overstated | Caller loss and worker timeout are co-equal; Curator timeout evidence added with limits. |
| Receipt performance overlaps v2 | Deferred redesign to v2; linked existing measured performance work. |
| Coordination errors labelled storage | Separated timeout waits from durable storage retries. |
| Poisoned execution set silent | Added diagnostics and safe recovery of the in-memory set. |
