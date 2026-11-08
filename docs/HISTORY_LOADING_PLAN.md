# History loading — root cause and proposed performance fix

**Status:** first release implemented; final qualification in progress · **Updated:** 2026-09-25 ·
**Diagnosis base:** `f959cf922b47c581713df9401003c7c6f96cf5f7` ·
**Delivery status:** [HISTORY_LOADING_STATUS.md](archive/HISTORY_LOADING_STATUS.md)

Companion to [ARCHITECTURE.md](ARCHITECTURE.md#86-crash-recovery) §8.6
(Crash recovery, including history storage),
[DEFECT_HISTORY_DELETE.md](archive/DEFECT_HISTORY_DELETE.md) (durable deletion),
and [INTEGRATION_PLAN.md](archive/INTEGRATION_PLAN.md) (consumer cursors). This
document explains why browsing history can stall, proposes a staged fix,
and identifies the evidence needed before shipping it. Read the deletion
contract before changing ingestion. If an optimization requires changing
merge precedence or cursor meaning, stop and review that change separately.

## Implementation status — first release on `codex/history-loading`

The historical diagnosis and proposed milestones below remain the review
record. The implemented behavior is:

- Explicit `LocalOnly` daemon wiring, portable-directory writer locking,
  startup replay, and dirty-state repair for failed local publication.
- One owned OS-thread worker per store, whole-pass gate, completion-based
  throttle, persistent pause/resume, and status/I/O/placement in the native API
  and History tab. Dev settings provide a live enable control with advisory
  readiness. Healthy local-only worker ticks perform no replay.
- Independent SQLite reader connection; page and total share one transaction.
  Consumer observations enqueue without storage I/O and retry even while paused.
- Replay uses stable opened-file snapshots, conservative fingerprints with a
  60-second verification scan, and transactions bounded to 16 entries or
  128 KiB of source text; tombstones use 16-key batches with the same
  cancellation and timing contract. Existing-row updates avoid AUTOINCREMENT churn and
  preserve the original merge rules. Local mutation generations fence stale
  replay publication. These batch caps are conservative implementation values;
  only local storage has been benchmarked, not GlusterFS after this change.
- `history.index_dir` copies the active index at startup through SQLite, keeps
  the spool in its original location, and tracks the active index path. Missing
  registered indices and existing different destinations fail explicitly.
- Ordering index, latest-request browser rendering, same-page request
  coalescing, and loading/error/synchronization controls.

The first release shipped in PR #237. M6 is now implemented on the follow-up
branch, with merge-equivalence review and final verification tracked in
[HISTORY_INCREMENTAL.md](HISTORY_INCREMENTAL.md). It retains full recovery and
periodic verification, while ordinary shared-log appends consume only suffixes.
No production deployment, index migration, or post-fix production timing has
been performed.

A repeatable local debug-build probe uses 1,000 synthetic entries containing
16 KiB records and alternates 20-row pages at offsets 0, 500, and 980 during
three full replays. Initial results: startup 603 ms; three replays 1,558 ms;
storage-read p50 1.459 ms, p95 1.575 ms, p99 1.614 ms; maximum batch 15 ms.
These are storage-layer observations on this Mac, excluding HTTP/JSON/browser
costs, and cannot be presented as nuc3's latency or a GlusterFS batch-size study.
Run it with:

```bash
cargo test -p nzbd-state history_loading_probe -- --ignored --nocapture
```

[CONFIGURATION.md](CONFIGURATION.md) documents restart, migration, rollback,
and operator controls. Final review and verification results are tracked in
[HISTORY_LOADING_STATUS.md](archive/HISTORY_LOADING_STATUS.md).

## 1. Finding — page reads pay for replaying the entire history

The principal code-level defect is that a small history read can synchronously
reconcile the entire portable history log into SQLite. Server-side pagination
limits the returned rows, but does not limit this reconciliation work.

An eligible request reads every matching JSONL file twice, deserializes every
entry on the second pass, and attempts an upsert plus a cursor lookup for each
surviving entry. Existing entries still take the conflict-update path. The
request waits for all of this before fetching its page.

The throttle permits starts five seconds apart, even while a previous pass is
still running. Fable measured 12.3–31.7-second page requests on nuc3, versus
15–37 ms for warm reads. The slow path lasts longer than the throttle window.
The web UI also polls the active history tab every five seconds, including
when its event stream is healthy. An idle history view can therefore keep
causing full replays even when the logs have not changed.

**Evidence boundary:** source inspection confirms the mechanism. Fable supplied
live HTTP measurements during review (§2.3); subsequent read-only API and SSH
checks confirmed single-node operation and SQLite placement on GlusterFS
(§2.4). Refresh-associated work dominates the measured delay. The split between
log reads, write/sync costs, and contention still needs phase timings; neither
the exact number of overlapping passes nor per-entry fsync latency was directly
measured. This section records the pre-implementation diagnosis; current
implementation and preliminary local timings are listed above.

**Recommendation:** start with an explicit single-writer fast path for nuc3,
subject to the recovery conditions in §4.0. For shared history, combine a
one-pass gate with background reconciliation so listings never await a pass.
Prioritize batching and persistent local SQLite placement over the ordering
index. Unchanged-file detection must not block the request-path fix. Incremental
ingestion remains a later stage with merge-equivalence tests.

## 2. Evidence — the request and its amplification points

Source links name the relevant symbols; line anchors refer to the inspected
revision and must be rechecked at implementation time.

| Finding | Code evidence | Consequence |
|---|---|---|
| The web UI defaults to 20 rows and sends `limit` and `offset`. | [`setHistoryPage`, `refreshHistory`](../crates/nzbd-api/ui/index.html#L2983) | Small pages already exist; reducing page size cannot bound replay cost. |
| Native history awaits refresh before listing rows. | [`get_history`](../crates/nzbd-api/src/lib.rs#L1870) | The request that triggers refresh waits for all reconciliation. |
| The throttle records the start time and releases its mutex before replay. | [`HistoryDb::refresh`](../crates/nzbd-state/src/history.rs#L249) | A pass taking more than five seconds can overlap another pass. This is not a one-worker guarantee. |
| Replay sorts all matching paths, scans tombstones, then scans entries. | [`rebuild_from_jsonl`](../crates/nzbd-state/src/history.rs#L281) | Two full content passes, including over unchanged files. |
| Each entry calls `insert`, which calls `insert_seq`. | [`insert_seq`](../crates/nzbd-state/src/history.rs#L688) | Per-entry upsert, serialization, and `SELECT id`; replay does not use the returned cursor. |
| There is no replay-wide or batch transaction around those upserts. | [`rebuild_from_jsonl`](../crates/nzbd-state/src/history.rs#L330) | Repeated statement preparation and autocommit overhead. Actual sync cost depends on SQLite and storage settings. |
| The only history key index declared by the schema is the unique completion key, alongside the primary key. | [`open_tagged`](../crates/nzbd-state/src/history.rs#L137) | Neither matches the page ordering by completion time. Verify the actual database plan during measurement. |
| Pages order by `completed_at DESC, id DESC`. | [`list_page`](../crates/nzbd-state/src/history.rs#L887) | Without a suitable index, SQLite must do extra sorting work before returning a page. |
| The page query parses `record` and `stages` while holding the connection mutex. | [`query`](../crates/nzbd-state/src/history.rs#L929) | Large retained job records increase page CPU, lock duration, and response size. |
| Requeue decoration already uses a cached directory listing. | [`spooled_ids`](../crates/nzbd-state/src/history.rs#L1229) | The old per-row filesystem-stat problem has already been reduced; do not present it as the current primary defect. |
| Compat history and duplicate detection also call refresh. | [`history`, `is_duplicate`](../crates/nzbd-compat/src/lib.rs#L641) | The same cost affects consumers and admission, not only browsing. |
| History requests have no request-generation guard. | [`refreshHistory`](../crates/nzbd-api/ui/index.html#L3310) | A slow older request can overwrite the result of a newer page selection. |
| History polls continue every five seconds. | [`pollPass`](../crates/nzbd-api/ui/index.html#L4726) | Polls can overlap navigation and event-triggered fetches. |
| Mobile requests 200 entries and appends older pages to a `ScrollView`. | [`HistoryView`](../mobile/src/screens/HistoryView.tsx), [`getHistory`](../mobile/src/api/client.ts#L52) | Mobile shares the server defect and has separate potential payload/rendering costs. It does not use the web UI's 20-row default. |

### 2.1 The expensive path runs before pagination

```text
Page click / history poll
  │
  ▼
GET /api/v1/history?limit=20&offset=N
  │
  ▼
spawn_blocking                 HTTP handler still awaits the result
  │
  ├─ refresh eligible?
  │    ├─ enumerate and sort history*.jsonl
  │    ├─ pass 1: read all files; collect tombstones
  │    ├─ apply tombstones
  │    └─ pass 2: read all files; for each surviving entry
  │         ├─ deserialize entry and embedded record
  │         ├─ serialize fields for SQLite
  │         ├─ INSERT ... ON CONFLICT DO UPDATE
  │         └─ SELECT id by completion key
  │
  ├─ SELECT page + COUNT(*)
  ├─ mark seen when the caller is a consumer
  ├─ cached spool lookup + JSON construction
  ▼
HTTP response → browser JSON parsing → render
```

`spawn_blocking` keeps synchronous I/O off the async runtime's worker threads.
It does not detach that work from the response or remove its cost.

### 2.2 Log length matters more than the number of displayed rows

Let `B` be the total JSONL bytes, `L` the number of entry lines, `H` the number
of indexed rows, `P` the page size, and `O` the offset. On a refresh-triggering
request, work includes approximately:

```text
two scans of B
+ decoding L entries
+ up to L upserts and L cursor lookups
+ page selection / sorting over H
+ exact count
+ P records decoded and serialized
+ filesystem decoration, transfer, and rendering
```

This is a work model, not a timing formula. Multiple appended versions of one
completion increase `L` without increasing `H`. Retention can reduce retained
content, but using retention to compensate for the read path changes how much
user history is kept and does not correct the underlying design.

The [retention comment](../crates/nzbd-state/src/history.rs#L488) records an
earlier observation of 3.1 seconds for 179 indexed rows on `nuc3`. The
[API comment](../crates/nzbd-api/src/lib.rs#L1905) records an earlier 250 ms
spool-lookup cost. Those are historical annotations, not measurements made
for this proposal or proof of the deployed version's behavior.

### 2.3 Live review evidence isolates the expensive path

Fable reported these Chrome measurements on nuc3 on 2026-09-25: build
`0.2.0+unknown`, reported build time 16:27 UTC, 997 indexed rows. They are
reviewer-supplied observations, not a new benchmark run by this document's
author. The deployed `+unknown` version does not prove an exact commit match
with the inspected checkout.

| Request / condition | Wall time | Response body |
|---|---|---|
| `/api/v1/status` | 6 ms | 1 KB |
| History, limit 1, offset 0, after 35 seconds quiet | 15.4 s | 3 KB |
| History, limit 20, offset 0; six samples spaced at least 5 seconds | 12.3–31.7 s | 324 KB |
| History, limit 200, offset 0 | 12.8 s | 1.6 MB |
| History, limit 1, inside the throttle window | 15 ms | Not reported |
| History, limit 20, offset 500, inside the window | 32 ms | 131 KB |
| History, limit 20, offset 980, inside the window | 37 ms | 19 KB |

Warm reads remain fast even for deeper pages. Response construction, counting,
and pagination are not the dominant explanation for these multi-second stalls.
The newest 20-row response averages approximately 16 KB per entry; mobile's
200-row fetch was 1.6 MB. Those sizes matter for later transfer/rendering work,
but do not explain a 15.4-second request returning only one 3 KB entry.

Fable also reported 868 native monarr calls over approximately 1.5 hours
(roughly one call per six seconds), plus a browser with two event subscriptions.
Together with the start-time throttle, this makes sustained overlapping replay
plausible and a priority, rather than an exotic edge case. The client counters
do not identify every call's route or measure concurrent passes. The review's
estimate of two to four simultaneous replays must be verified with an active-
pass counter; it is not an observed concurrency measurement.

### 2.4 nuc3's history index is confirmed to be on GlusterFS

Read-only follow-up checks on 2026-09-25 answered the review's configuration
questions and inspected the running container's mount namespace:

| Evidence | Confirmed value |
|---|---|
| `GET /api/v1/config`: `paths.main_dir` | `/processing/` |
| `paths.queue_dir` | Unset (`null`); `state_dir()` resolves to `/processing/queue` |
| `cluster.enabled` | `false` |
| Pending restart settings | Empty list |
| Container processing bind | Host `/mnt/processing` → container `/processing` |
| Covering filesystem in container mount table | `fuse.glusterfs`, source `localhost:/processing` |
| Resolved SQLite path | `/processing/queue/history.sqlite`; 8,171,520 bytes at inspection |
| Portable logs found in that history directory | Only `history.jsonl`; 7,381,309 bytes at inspection |
| Existing persistent local mounts | `/data` and `/etc/nzbd` resolve to host ext4; neither was changed |

The single-node [daemon wiring](../crates/nzbd/src/main.rs#L760) puts the index
in `state_dir` and logs in its `history` subdirectory. The
[cluster wiring](../crates/nzbd/src/main.rs#L1185) also uses `cfg.state_dir()`
for the index. [State-directory resolution](../crates/nzbd-config/src/lib.rs#L612)
does not enforce local-disk placement. These are neighboring directories on
the same mount, not literally the same directory.

**Leading cost explanation:** repeated autocommit upserts and cursor lookups
are hitting network-mounted SQLite. Gluster/FUSE round trips, commit syncs,
and overlapping passes amplify the work. The implementation enables WAL and
does not set `synchronous`; confirm the daemon connection's effective setting
and sync costs rather than asserting a measured fsync per row. A separate
SQLite CLI connection cannot establish the daemon's connection-local PRAGMA.

The [durability helper](../crates/nzbd-config/src/durable.rs#L166) classifies
persistent versus ephemeral container storage. Its mount-prefix parser is
reusable, but `Persistent` does not mean local: Gluster and NFS mounts qualify
as persistent too. A placement check must inspect filesystem type/source and
resolve the actual database path in the serving process's mount namespace.

## 3. Contracts — preserve storage and consumer meaning

These interfaces and SQL are copied from the inspected implementation.
Re-verify them against the linked source before building.

```rust
pub fn refresh(&self) -> Result<(), StateError>;
pub fn list_page(
    &self,
    limit: usize,
    offset: usize,
    include_hidden: bool,
) -> Result<Vec<HistoryEntry>, StateError>;
pub fn list_since(
    &self,
    since_seq: i64,
    limit: usize,
) -> Result<Vec<HistoryEntry>, StateError>;
```

The native API has two distinct read orders:

| Form | Required behavior |
|---|---|
| `?limit=P&offset=O` | Newest first: `completed_at DESC, id DESC`; native reads include hidden rows. |
| `?since_seq=S&limit=P` | `id > S`, oldest first: `id ASC`; includes hidden rows; offset does not affect selection. |
| Response | Preserve `entries`, `total`, `offset`, `limit`, existing entry fields, and `can_requeue`. |
| Defaults | Native endpoint defaults to 200 and caps at 10,000; web UI defaults to 20; mobile defaults to 200. |

**Durable identity:** `(job_id, completed_at)` identifies a completion. The
SQLite row ID is a node-local cursor; it is not portable log state. Existing
rows must keep their IDs during reconciliation. Do not rebuild by deleting
and reinserting all rows in a running index.

**Deletion:** tombstones win over all entry copies, regardless of file order.
Keep the insertion-time tombstone guard. A refresh must not expose an entry
whose tombstone it has already ingested. A failed durable delete append must
not remove the indexed row. See [the deletion contract](archive/DEFECT_HISTORY_DELETE.md).

**Retention:** ingestion must honor the node's monotone retention floor, even
when a peer still retains old entries. Compaction uses atomic replacement and
keeps tombstones; synchronization must detect that replacement.

**Merge order:** full replay sorts filenames, then reads each file in line
order. Conflict updates overwrite `hidden` and preserve prior non-null values
when `removed_at`, `picked_up_by`, or `record` is absent. Other stored fields
are not generally overwritten by the current conflict clause:

```sql
ON CONFLICT(job_id, completed_at) DO UPDATE SET
  hidden = excluded.hidden,
  removed_at = COALESCE(excluded.removed_at, history.removed_at),
  picked_up_by = COALESCE(excluded.picked_up_by, history.picked_up_by),
  record = COALESCE(excluded.record, history.record)
```

Do not replace this with timestamp-based last-writer-wins or retain only the
last physical line. Neither is equivalent. Preserve index-local observation
fields (`first_seen`, `last_seen`, `seen_count`) and durability markers.

**Writes and observations:** local completion writes and acknowledged actions
must remain immediately visible in the local index. Consumers must still mark
the entries they actually read; operator/browser reads must retain their
existing exclusion from pickup observations.

## 4. Proposed first release — remove replay from ordinary page reads

### 4.0 M0 — stop routine replay for an explicit single-writer store

nuc3 has clustering disabled and only the untagged log in its history
directory. In the supported single-process layout, this process writes both
the log and index, and `open_tagged` already replays at startup. Replaying its
unchanged log on every eligible listing adds no new successful local writes.

Pass an explicit local-only versus shared-history policy from daemon wiring.
Do not infer that policy solely from `tag == None`: the generic `HistoryDb::open`
API and tests can read a union of peer files without assigning a writer tag.
Verify exclusive ownership of this state directory; a network mount does not
by itself imply another writer, and this inspection did not inventory every
process on other hosts. Local-only mode must reject unsupported concurrent
writers rather than silently treating their changes as local.

In healthy local-only mode, `refresh` performs no routine replay after startup.
Keep an explicit repair path for failed local index updates and operator
recovery. In particular,
[`record_seq_durable_with`](../crates/nzbd-state/src/history.rs#L827) can append
portable evidence before an index insert fails. Audit every log/index ordering
and preserve retries or mark the store dirty for recovery; deleting replay
without addressing that case would delay recovery until restart. Do not
disable startup replay or the tombstone guard.

**Acceptance:** startup restores the same entries; successful local writes,
hide/restore/delete, requeue, and consumer observations remain visible without
replay; injected append-success/index-failure recovers; clustered or explicitly
shared stores continue ingesting peers. This small fix can ship before the
shared-history worker work, once those preconditions and tests pass.

### 4.1 Own one reconciliation worker per history store

For shared-history stores, keep the initial load before serving history, then
run one lifecycle-owned worker per `Arc<HistoryDb>`, including a store exposed
through a cluster API. Local-only stores use §4.0 rather than an idle replay
worker. Use five seconds after the previous pass completes as the initial
periodic scheduling policy; coalesce wakeups and never queue one full pass per
request. Do not put a Tokio runtime dependency into the synchronous state
crate just to schedule this worker.

Replace the timestamp-only throttle with explicit synchronization: one active
pass, last attempted time, last successful time, and last error. A synchronous
caller and the worker must enter the same gate. Advance successful state only
after successful reconciliation. A failed or incomplete read must remain
eligible for retry; avoid silently treating a skipped unreadable file as a
successful full observation.

Native and compat history listings read the local index without awaiting log
reconciliation. They may request a coalesced wakeup. Initial loading remains
a startup cost and must be measured separately.

**Freshness change requiring review:** a listing can now return the previously
indexed view while peer changes are being ingested. With healthy storage and
no backlog, the expected delay is one scheduling interval plus reconciliation
time and filesystem visibility delay. Five seconds is not a hard upper bound.
On failure, retain the last indexed view and report degraded freshness in the
product as well as diagnostics. Do not report an unsuccessful pass as fresh.

Expose a history-synchronization status surface: running/idle/paused/failed,
last pass duration and bytes read, age of last success, last error, and store
placement classification. Provide authenticated Pause and Resume controls.
Pause blocks scheduled and request-triggered reconciliation, including admission
refreshes, and requests cooperative stopping between safe batch boundaries;
it does not promise to interrupt a blocked filesystem syscall. Let a transaction
finish atomically, retain retry progress correctly, and show "stopping" until
the pass exits. Resume coalesces one catch-up pass. Local writes continue.
Make the stale-view consequence explicit while paused; admission retains its
existing best-effort error policy rather than silently bypassing Pause.
The paused state should survive restarts; mandatory startup recovery remains
separate and must be described as such. Worker shutdown follows the same safe
boundary rules. Exact API/UI placement is an implementation review item.

Keep compat duplicate admission on an explicit synchronous refresh path in
this release, sharing the same gate. It must wait for a due pass rather than
silently changing its admission policy to a possibly older background view.
The existing admission behavior is already throttled and ignores refresh
errors; changing that error policy is a separate review item. This proposal
does not claim global, strongly consistent duplicate detection.

Its current history check also decodes up to 1,000 visible entries through
`list(1000)` and linearly scans `dupe_key`. The gate protects against overlapping
replay, not missed duplicates, unbounded staleness on failure, or the 1,000-row
search limit. Follow up with a targeted SQL predicate and measured index for
the duplicate key/status/score policy. Decide separately whether searching
older rows changes admission behavior; do not silently expand that contract.

### 4.2 Skip unchanged content, with a conservative fallback

This is an independent optimization after gating and decoupling; it must not
delay M2. It benefits shared stores and recovery work, not healthy local-only
stores that no longer replay at all.

Maintain in-memory fingerprints of the successfully consumed file set:
pathname, file identity where available, length, and high-resolution
modification metadata. Enumerate the directory so new peer files are found.
Use metadata from opened handles when validating a consumed snapshot.

If the file set and all reliable fingerprints are unchanged, skip content
reads and replay writes. When any file changes, the first release conservatively
replays the whole set in the existing deterministic order. This is deliberately
less ambitious than tailing each file: it preserves merge precedence while
eliminating repeated idle work.

Only cache a fingerprint for bytes actually consumed successfully. If a file
grows or is replaced during the pass, retain the observed older boundary and
schedule another pass; never mark the newer, unread length as consumed. Treat
truncation, rename replacement, new paths, and uncertain identity as requiring
reconciliation. Disappearing files do not implicitly delete indexed entries.

Metadata is a hint, not a portable proof of equal contents. Supported writes
are append and atomic replacement; arbitrary in-place log edits are outside
the normal writer contract. Use a conservative full scan when identity or
metadata is unreliable, plus a periodic verification pass. An initial
60-second verification interval is a proposed tunable implementation constant,
not an existing setting or guaranteed detection bound on a partitioned mount.
Keep this optimization independently disableable during rollout.

### 4.3 Batch replay work without creating one long reader lock

Introduce a replay-specific insertion path. Live `record_seq` callers still
need a cursor; replay does not need a `SELECT id` after every line. Prepare
statements once per batch and execute writes in bounded transactions.

Read and decode JSONL outside the SQLite mutex. Keep the tombstone pass before
entry replay and retain the SQL tombstone guard for concurrent deletions.
Measure statement/commit latency and connection hold time on the actual mount
before choosing a batch limit. The previous 256-entry/1 MiB suggestion was
ungrounded for GlusterFS and is withdrawn. Sweep small and larger row/byte
limits on a disposable representative copy, allow one oversized entry to make
progress, and choose the limit from reader wait-time and throughput results.
Use a time budget checked between statements as well as row/byte bounds;
neither can preempt a blocking statement or commit.

Today `insert_seq` releases the mutex after each entry's upsert and ID lookup
(not after each individual statement). Batching lengthens those lock holds;
warm reads are already 15–37 ms under the observed load, so reducing commit
count must not turn the worker into a new source of page stalls. Batching has
greater expected benefit than adding an ordering index on this deployment,
but only with measured contention control.

Release the database lock between batches and allow readers to proceed. One
transaction over the entire history would reduce commits but could make every
page wait behind the background worker through the existing single connection.
WAL does not bypass an application mutex around that connection.

Skip conflict updates whose effective mutable values are unchanged. Do not
claim that an upsert predicate eliminates all writes: `AUTOINCREMENT` bookkeeping
changes even when an update predicate rejects an identical row in a local
SQLite 3.53.4 in-memory probe: sequence 1 → 2 → 3, while the existing row ID
stayed 1. This establishes the behavior for that probe, not the deployed
daemon's SQLite build. Include before/after `sqlite_sequence` and write counters
in the disposable replay benchmark. A later existing-
row update/new-row insert split can remove that work, but must preserve the
tombstone guard, stable IDs, and existing live-write durability behavior.

If bounded batches still miss latency targets, evaluate a separate read
connection to the local WAL database. Treat this as a measured follow-up with
explicit transaction-snapshot tests, not as an assumption that moving work
to another task makes reads independent.

**Placement:** give the derived index a persistent local directory independent
of the authoritative log directory. Do not merely point `queue_dir` elsewhere:
that also relocates the queue journal, snapshots, config mirror, and currently
the single-node history log. Add an explicit index-directory contract or a
reviewed wiring change; select a persistent local bind, not a container layer.
Use filesystem-type/source checks as described in §2.4, and expose unexpected
network or unknown placement in the operator surface.

Migration must preserve the existing database, row IDs, tombstones, retention
floor, observation fields, and durability markers. `spool` is derived from
the database parent today, so preserve or explicitly migrate the parked NZBs
too. Quiesce writers and checkpoint/close SQLite before a filesystem move,
or use a supported consistent backup procedure. Never copy only the live
main database file while WAL may contain committed changes. Verify counts,
IDs, metadata, and spool availability before switching paths. Keep rollback
consistent with writes accepted after cutover; an old abandoned copy is not
a valid rollback database. No placement or service change was made during
this review.

### 4.4 Add the index matching the page order

This is a scaling improvement after the request-path defect, not the primary
fix for nuc3's measured latency.

Create this additive index when opening or upgrading the local schema:

```sql
CREATE INDEX IF NOT EXISTS history_completed_at_id
ON history(completed_at DESC, id DESC);
```

Verify with `EXPLAIN QUERY PLAN` that the native page query uses the index and
does not build a temporary ordering B-tree. This also matches retention's
newest-first ordering. Only add a hidden-filter index if the compat query plan
and timings justify its write/storage cost.

The index does not make deep `OFFSET` pagination constant-time, and it does
not eliminate `COUNT(*)` or large record decoding. Preserve exact totals for
this release; measure their remaining cost before proposing count caching or
a new keyset-pagination contract. Fetch page and total under one read snapshot
so background ingestion cannot make them disagree within a response.

### 4.5 Prevent older browser responses from replacing newer pages

Capture the requested page, size, and a monotonically increasing generation
at dispatch. Apply a response only if that generation is still current.
Abort superseded fetches where practical, but retain the generation check:
aborting a browser request does not guarantee cancellation of server work.

Coalesce automatic refreshes for the same requested page. A user selecting a
different page must immediately supersede the old request. Apply the same
generation rule to fallback pagination after the last page disappears, and
to loading/error indicators. Keep the prior rows visible with an explicit
loading state; do not pair old rows with a pager claiming the new page has
already loaded.

Preserve the existing API response and embedded job records. A summary/detail
split, prefetch cache, or mobile list virtualization can follow if profiling
shows a remaining client bottleneck. They do not remove full log replay.

## 5. Incremental ingestion — the second stage needs merge equivalence

The first release removes routine replay for healthy local-only stores and
removes shared replay from the HTTP dependency chain. Optional unchanged-file
detection cuts idle shared-store work, but changing shared history still
triggers full content work. Incremental ingestion should make that work
proportional to new bytes and affected completion keys.

**Why offsets alone are wrong:** suppose sorted files are `history.a.jsonl`
and `history.b.jsonl`. Both contain the same completion; B's last version sets
`hidden=true`. A later append to A sets `hidden=false`. Full replay still
finishes with B's `true`. Applying only A's new tail would incorrectly finish
with `false`. Non-null merge fields have analogous precedence problems.

The recommended direction is per-file derived contributions plus per-file
committed byte offsets. A contribution must preserve the first immutable
payload needed for an unseen key, the last hidden value, and the last non-null
values of mergeable fields; a single last-line copy is insufficient. Recompute
affected keys in the same sorted-file order as replay, preserving existing
row IDs and index-local metadata. Persist derived progress with the associated
updates in one transaction, or keep both ephemeral and reconstruct them on
restart. Never persist an offset ahead of its applied data.

| Case | Required behavior before incremental mode is enabled |
|---|---|
| Append to an existing file | Consume new complete lines; reevaluate only affected keys with all relevant file contributions. |
| New peer file | Ingest from zero and apply its actual position in sorted-file precedence. |
| Partial final line | Keep the offset at the last complete newline; retry when the writer finishes. |
| Malformed complete line | Skip with bounded diagnostics, matching the existing tolerance for unknown formats. |
| Rename replacement or truncation | Invalidate the file's derived contribution and reconcile conservatively; preserve tombstones and retention. |
| Crash between read and commit | Replay the uncommitted suffix; do not skip its entries or assign new IDs to existing rows. |
| Peer disappears or mount fails | Do not infer deletes or remove monotone tombstone knowledge. |
| Local record/hide/restore/delete during ingestion | Serialize or version publication so stale work cannot undo an acknowledged local mutation; compare later convergence with the full-replay reference. |
| Compaction removes earlier versions | Preserve the existing `COALESCE` behavior and node-local retained values; do not clear fields solely because the compacted contribution lacks them. |

The implementation and its concurrency review are tracked in
[HISTORY_INCREMENTAL.md](HISTORY_INCREMENTAL.md). It preserves the existing
conflict-resolution protocol and uses ephemeral offsets and contributions. Keep full replay
as the recovery path and test oracle. Cross-file conflicts, disappearing files,
and local mutation races must pass equivalence tests before rollout. A future
versioned log format could simplify ordering, but that is outside this fix.

## 6. Validation — distinguish diagnosis from shipping evidence

### 6.1 Establish a baseline on a copy and on the affected deployment

The first live baseline is recorded in §2.3, with configuration and mount
confirmation in §2.4. Extend it with phase and concurrency instrumentation;
do not repeat heavy production polling just to recreate an already observed
stall. Use a disposable copy for batch-size and write-amplification experiments.

Record the running build ID, row count, log file count, bytes and line count,
retention bounds, page size/offset, record payload sizes, filesystem mounts,
and whether the request triggered a refresh. Do not capture sensitive names,
URLs, or job parameters in performance logs.

Measure initial startup, warm reads inside the throttle, the first read after
the throttle, idle periodic polling, and reads concurrent with appended history.
Collect at least 100 requests per warm/active scenario and report p50, p95,
p99, and maximum, alongside CPU, bytes read, writes, and lock waits. Keep cold
filesystem-cache experiments separate; do not flush a production host's cache.

Proposed temporary tracing fields, not existing metrics:

| Measurement | How to interpret it |
|---|---|
| `history_refresh_ms`, files, bytes, lines | Growth with total log length confirms the replay amplification. |
| Refresh attempts, active passes, skipped-unchanged passes | An active count above one is a scheduling defect; unchanged passes should do no content replay outside verification. |
| `history_db_wait_ms`, batch hold time, committed batches | High wait despite fast SQL means contention survived the move to a worker. |
| Page query, count, record decode, spool, JSON times | Identifies the remaining request cost after replay is removed. |
| Last successful reconciliation age and last error | Distinguishes a fast stale response from a healthy fresh index. |
| Operator-visible running/paused/error state, last bytes and duration | Shows who owns background I/O and confirms Pause actually stops new work. |
| Browser request duration and render duration | Separates backend delay from transfer/parsing/rendering. |

Use fixtures with 1,000, 10,000, and 100,000 unique entries; also hold the
indexed count fixed while increasing duplicate/version lines. Test one and
multiple peer files, tiny and large job records, local storage, and the actual
supported shared-volume configuration. Repeat first, middle, and last pages
at 20 and 200 rows, with concurrent clients.

### 6.2 Preserve existing regression coverage

Run the history, API, compatibility, and UI tests after implementation:

```bash
cargo test -p nzbd-state history::tests  # Log/index/retention contracts
cargo test -p nzbd-api                  # Native paging and observations
cargo test -p nzbd-compat               # Consumer and admission behavior
make ui-test                           # Embedded browser harness
make check                             # Repository Rust quality gates
```

The [state tests](../crates/nzbd-state/src/history.rs#L1307) already cover
stable cursors, portable tombstones, peer copies, compaction, retention,
record preservation, and spool availability. Extend them rather than replacing
them with tests that only assert the new code path was called. These commands
are proposed implementation checks; they were not run for this documentation
change.

### 6.3 Add regressions that reproduce the failure modes

| Test | Required assertion |
|---|---|
| Explicit local-only store after successful startup | Ordinary refresh triggers read no log content; local actions still appear immediately. |
| Local log append succeeds, SQLite publication fails | Repair/retry recovers the durable entry without restarting or losing its identity. |
| Untagged shared store | Writer tag alone cannot disable ingestion of peer files. |
| Many simultaneous triggers; reconciliation delayed beyond five seconds | Exactly one pass is active; pending triggers are bounded and coalesced. |
| Warm unchanged history | Zero content bytes replayed and zero replay writes, except scheduled verification. |
| Slow shared-log I/O | Native and compat listings do not await it; database wait remains separately bounded and measured. |
| Slow duplicate admission refresh | Admission still observes the intended synchronous refresh policy. |
| Startup and first HTTP read | Startup seeds history; the first ordinary read does not immediately repeat that load. |
| Failure, then recovery | Successful age/fingerprints do not advance on failure; recovery ingests missed changes. |
| New file, append during scan, same-length rename, truncate and regrow | Changes are eventually discovered; unread bytes are never marked consumed. |
| Delete during replay; entry in an earlier file, tombstone in a later file | No resurrection after the tombstone is applied; existing cursor guarantees hold. |
| Hide/restore and records spread over files | Final values match the reference replay, including null-preservation behavior. |
| Large replay batch and concurrent page readers | Batch limits work; lock hold/wait measurements satisfy the target. |
| Pause during a pass, admission trigger while paused, resume, restart | No new pass bypasses Pause; safe stop is visible; restart preserves pause; resume coalesces catch-up. |
| Local-index migration with committed WAL and parked NZBs | Committed rows, cursor IDs, metadata, tombstones, and requeue sources survive; rollback does not select a stale abandoned copy. |
| Page/total concurrent with ingestion | Each response describes a consistent read snapshot. |
| Page A slow, page B fast, A resolves last | B stays displayed; loading/error state belongs to B. |
| Timer poll while navigation is pending | It does not restore an older page or create unbounded requests. |
| Incremental crash boundaries and partial lines | No omitted complete entries, changed existing IDs, or duplicate cursor delivery. |

Use injected barriers or fake clocks for concurrency tests rather than sleeps.
Use deterministic byte/write counters for complexity assertions; keep wall-clock
performance thresholds in a repeatable benchmark rather than flaky unit tests.

### 6.4 Proposed performance acceptance targets

The primary first-release requirement is that no ordinary native or compat
history listing waits for reconciliation to complete, directly or indirectly
through an unbounded database lock. The 15–37 ms warm samples already establish
that a sub-100 ms read is possible on nuc3; a warm-only benchmark would miss
this bug entirely. Six slow samples do not establish a production percentile.

- Delay a shared-store pass for 30 seconds and request pages throughout it.
  Reads continue from the last indexed view without waiting for the pass.
  Include batch commit delays and connection contention, not only log reads.
- On a comparable warmed nuc3 dataset, target page p95 below 100 ms and p99
  below 250 ms **during** reconciliation, with no multi-second replay-correlated
  stalls. These are proposed non-regression thresholds, not achieved results.
  Compare actual browser and server timings separately.
- Healthy local-only requests cause zero post-startup log replay; deliberately
  failed local publication still invokes the tested repair path.
- No reconciliation runs concurrently with another for the same store.
- When the optional fingerprint optimization ships, an unchanged ordinary
  shared-worker tick replays zero content bytes. Report verification separately.
- Measure 100,000-row fixtures as a scaling target, separately from nuc3's
  approximately 1,000-row baseline; do not delay the urgent fix on adding an
  ordering index that warm production reads do not presently need.
- Existing-row cursor IDs survive all optimization paths, and all deletion,
  retention, record, and observation regressions pass.
- Healthy peer convergence is measured as scheduling delay plus pass duration;
  it meets an agreed deployment target. Failures expose increasing freshness
  age rather than a misleading five-second guarantee.
- Incremental mode, when delivered, reads work proportional to appended bytes
  and affected contributions in the normal append case; recovery scans remain
  explicitly measured exceptions.

## 7. Delivery — separate reviewable changes and their acceptance checks

| Milestone | Change | Acceptance check |
|---|---|---|
| M0: single-writer fast path | Use confirmed non-cluster wiring to skip healthy post-startup replay; preserve startup and failed-publication repair. | Exclusive ownership and explicit mode verified; local action/recovery tests pass; no routine replay from nuc3 listings. |
| M1: extend evidence | Retain Fable's measurements and confirmed mounts; add phase, concurrency, and batch benchmarks on a disposable copy. | Distinguish measured active passes and sync costs from estimates; establish batch lock-wait baseline. |
| M2: gate and decouple together | One shared-store pass at a time; lifecycle-owned background worker; native/compat listings never await it; admission shares the gate; visible status and pause/resume. | A delayed 30-second pass cannot stall listings; no overlap or hidden pause bypass; lifecycle/freshness tests pass. |
| M3: reduce shared replay cost | Measured bounded batches and cursor-free replay path; persistent local-index placement with migration coverage. | Commit/write cost falls without degrading warm reader waits; local placement, IDs, WAL, and spool migration verified. |
| M3b: optional independent optimizations | Simple opened-handle fingerprints with conservative fallback; ordering index after query-plan verification. | Unchanged content is skipped safely; replacement/recovery tests pass; neither item blocks M0 or M2. |
| M4: correct browser races | Add latest-request ownership, automatic-request coalescing, and loading/error behavior. | Controlled out-of-order response tests and `make ui-test` pass. |
| M5: deployment verification | Benchmark the affected environment and canary the first release. | §6.4 targets and agreed freshness behavior met; no row/cursor regressions. |
| M6: incremental ingestion | Review contribution schema and offset transaction design, then implement. | Differential replay, crash, replacement, mutation-race, and scaling tests pass before enabling it. |

Expected code locations are the [history store](../crates/nzbd-state/src/history.rs),
[native API](../crates/nzbd-api/src/lib.rs),
[compat API](../crates/nzbd-compat/src/lib.rs),
[daemon lifecycle](../crates/nzbd/src/main.rs),
[cluster lifecycle](../crates/nzbd-cluster/src/lib.rs), and
[embedded UI](../crates/nzbd-api/ui/index.html). Keep the JSONL wire format
unchanged for the first release. The added index is local and additive.

Canary M0 on the verified single-node layout; canary shared-worker changes in
a shared-history environment separately. Use retained history and active
completions. Compare
latency and freshness with the baseline; verify counts, sampled completion
keys, hidden state, tombstones, and existing cursor IDs. Test a restart and
peer failover before broad rollout. Mixed-version peers must still exchange
the same portable log records.

Performance fixes are always active; there are no rollout flags. Settings →
Dev provides an Enable history synchronization section with current readiness
and requirements as advice only. Its persisted live control uses the same
Pause/Resume contract as §4.1 and never blocks enabling on readiness.
Rollback restores the prior reconciliation path or binary while retaining
SQLite and JSONL. Do not delete the index as a routine rollback step, because
that changes local cursor state. Any later incremental tables must be additive
and ignorable by the previous binary; offsets are derived data, never authority.

## 8. Scope and reviewer decisions

**Non-goals:** changing retention defaults, shortening history to hide latency,
redesigning portable mutation ordering, removing job records from existing API
responses, introducing a new pagination protocol, or rewriting mobile UI in
the first release. Each changes an independent user or compatibility contract.

The reviewer should decide:

1. **Approve background freshness for listing endpoints.** Local writes remain
   immediately visible; peer changes become visible after worker ingestion.
   Fable approved this trade-off in review. Preserve native cursor ordering;
   measure peer convergence for native and compat consumers before rollout.
2. **Approve preserving synchronous duplicate admission initially.** This avoids
   changing admission semantics while fixing browsing; admission may retain
   latency until reconciliation itself becomes incremental.
3. **Approve the revised order.** M0 handles healthy local-only stores; M2 gates
   and decouples shared reads; M3 addresses batches and placement. Fingerprints
   and the ordering index do not block the urgent fix. Shared stores still
   replay full content until incremental ingestion is delivered.
4. **Approve merge equivalence as a release gate.** Faster tailing must not undo
   durable deletes, lose non-null record fields, or invent a new cross-node order.
5. **Confirm performance and convergence targets.** Use the affected deployment's
   measurements to ratify or revise §6.4 before implementation sign-off.
6. **Approve local-only repair and placement migration contracts.** Do not
   replace the explicit source policy with a tag check or move only a live
   SQLite main file. Preserve the failed-publication recovery path and spools.
7. **Approve operator ownership of background work.** Status, freshness, pause,
   resume, safe stopping, and paused admission behavior must be reviewable in
   the product, not available only in tracing output.

This review record incorporates Fable's measurements and independently
confirmed nuc3 configuration/mount placement. See the implementation status
above for delivered work. Phase-level profiling on the target mount and
post-fix production validation remain outstanding.
