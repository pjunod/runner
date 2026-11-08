# Download speed regression — measured causes and proposed correction

**Status:** index correction implemented; Fable review requirements covered.
Validation and merge status are recorded in the PR; live throughput comparison
remains pending and production configuration is unchanged.
**Written:** 2026-11-07 (America/New_York). **Source baseline:**
`b5ef661292d61b65477926d0966ed6515aa1d969`. **Branch:**
`codex/download-inventory-performance`.

Companion to [the I/O audit](IO_EFFICIENCY_AUDIT.md) and
[publication performance](PUBLICATION_PERFORMANCE_PLAN.md). This plan addresses
slow active Usenet downloads on nuc3. Review the evidence and scope before
implementing the milestones. If an optimization requires weakening durability,
identity, generation, retention or deletion checks, revise this plan first.

## 1. Finding — lifecycle queries repeatedly scan the operation journal

The user reports a fall from a previously consistent 109 MiB/s to approximately
30–40 MiB/s, beginning at least several weeks before November 7. Live inspection
identified repeated full-table inventory queries while downloads were active.
The dominant workspace lookup spent 15.215 seconds executing across 33 calls
in one 30-second sample. Other inventory lookups sometimes took 0.5–2 seconds.

**Confirmed defect:** lifecycle lookups evaluate JSON predicates over retained
operation records without suitable indexes. The shared inventory connection is
protected by a mutex, held for the query. The operations table occupies about
277 MiB, mostly historical PAR filename-restoration records unrelated to the
lookup. Repeated scans read and parse that unrelated history.

**Two co-equal explanations for the speed regression:** inventory mutex
contention and owner tick/persistence stalls can each leave provider connections
idle. The inventory scans may also aggravate tick stalls through shared state
volume I/O, rather than through the inventory mutex. Live profiles confirm the
scans and long inventory calls; logs separately show long snapshot saves. Neither
mechanism has yet been assigned a measured share of the 109 to 30–40 MiB/s loss.
Capture both on the same timeline before attributing the download regression.

**GlusterFS is not established as the primary cause.** It stores both payload
and inventory, and can amplify metadata and sync costs. Most observed database
reads were cache-served. No local-versus-GlusterFS payload comparison has been
performed. The October 6 directory-default change alone is not a diagnosis.

## 2. Evidence — live profiles and an isolated database snapshot

### 2.1 Deployment and measurement boundaries

Read-only inspection on November 7, approximately 23:03–23:16 EST, used status,
logs, process descriptors, syscall summaries, bounded BPF probes and SQLite
read-only queries. Additional traffic was requested by the user through
Curator. Measurements span active downloading, post-processing and maintenance;
the individual samples below identify their scope.

| Item | Observed value and interpretation |
|---|---|
| Deployed checkout | `/opt/noirr/runner` at `b5ef661`; merges PR #282. |
| Running binary | API reports `0.3.0+unknown`, built November 7 at 22:55 EST. Checkout identity is not independently embedded in that version string; measured symbols and queries are from the running executable. |
| Storage | `/processing` maps to host `/mnt/processing`, GlusterFS FUSE. `/working` maps to `/mnt/qnap/working`, NFS 4.1. |
| Actual payload path | Open active `.runner-file-*.part` descriptors were under `/processing/<job>/`. This was observed during receiving, before extraction/publication. |
| Configuration | `main_dir=/processing/`, destination `/working/monarr/completed`, no explicit intermediate directory, Newshosting connections 30, pipeline depth 2, configured active-download limit 1. |
| Rate controls | Live `speed_limit_bps=null`; no blocked provider, quota refusal or low-space latch in the inspected statuses. |
| Container CPU | No CPU quota, no cpuset restriction, zero cgroup throttled periods. This excludes configured CPU throttling, not every possible CPU bottleneck. |
| First active rate | 29,424,515 B/s, about 28.1 MiB/s, before syscall tracing. |
| Inventory size | SQLite file 305,512,448 bytes, about 291.4 MiB. `dbstat` attributes 290,189,312 bytes, about 276.7 MiB, to `operations`. |
| Operation history | 12,546 succeeded operations; 6,992 `par_restore` records contain 263,155,374 bytes of JSON text. Row counts are snapshots during ongoing work. |

`/proc/.../io` application-byte counters include cache hits. They are not disk
or network throughput. Strace adds substantial overhead at hundreds of thousands
of reads per second; rates observed under tracing must not be compared directly
with the earlier 109 MiB/s baseline. BPF probes also have overhead. No claimed
throughput improvement is derived from either instrumented rate.

### 2.2 The repeated reads belong to the inventory database

A one-second trace of positional reads, with raw syscall arguments rather than
payload contents, attributed 59,497 calls to descriptor 38. Of these, 58,431
requested 4,096 bytes. Descriptor 38 was `/processing/queue/artifacts.sqlite`.
A separate eight-second sample counted 603,390 `pread64` calls.

A 30-second SQL-correlated BPF sample attributed the following page reads to
specific SQLite statements. It overlapped ongoing downloading and lifecycle
work; it was not a controlled single-job benchmark.

| Query family | Page-read calls | Time in completed `sqlite3_step` calls |
|---|---:|---:|
| Transform scratch lookup | 2,337,621 | 7.626 s |
| Completed relocation cleanup candidates | 422,448 | 1.487 s |
| Discovery status | 70,829 | 0.237 s |
| Torrent-copy compaction candidates | 70,372 | 0.228 s |

A subsequent 30-second sample during the new active traffic measured 33 scratch
lookup steps taking 15.215 s, 12 completed-relocation cleanup steps taking
5.151 s, two discovery-status steps taking 1.119 s, and two torrent-compaction
steps taking 1.110 s. These are elapsed times inside SQLite calls, not
percentages of physical storage utilization or complete operation durations.

A separate 20-second function profile measured 288 completed `Inventory::get`
calls. Twenty-six took at least 256 ms, including six in the 1,024–2,048 ms
bucket. That measures complete lookup calls, including any mutex wait and
row decoding. It does not isolate mutex wait by itself.

**Counter-evidence to an overbroad claim:** twelve sampled `Owner::grant_work`
calls completed in roughly 1–16 ms. No long `for_job` calls were captured in
that interval. We have not proved that the scheduler function itself waits
seconds on every request, nor identified the exact affected caller for every
long inventory lookup. Trace that dependency in milestone 1.

### 2.3 Payload syncs have a cost, but do not explain the whole profile

A 20-second syscall summary recorded 46 `fdatasync` calls totaling 0.495 s,
about 10.8 ms per call. A separate 12-second summary recorded 57 totaling
0.394 s, about 6.9 ms per call. These summaries include all traced sync calls;
they do not distinguish payload, queue journal and other file descriptors.

Provider snapshots showed all 30 TLS connections established, with empty
receive queues and periods of approximately 0.9–3 seconds since the last data
on some sockets. This is consistent with request gaps; it does not establish
whether the application or provider caused each gap.

Logs reported snapshot saves blocking engine ticks for 1.6–3.2 seconds.
This is a co-equal causal hypothesis, not a secondary storage footnote.
[rate.rs](../crates/nzbd-engine/src/rate.rs) documents the same failure mode: a
late tick can reflect journal fsync and snapshot work on a slow state volume.
[owner_persistence.rs](../crates/nzbd-engine/src/owner_persistence.rs) already
includes `elapsed_ms` in `PersistenceFinished`. Queue journals, snapshots and
inventory share GlusterFS state storage. The observed socket idle intervals fit
a stalled owner at least as well as an inventory mutex wait. A common timeline
of tick duration, persistence elapsed time, lock waits and wire traffic is
required; the earlier logs alone do not establish the later download cause.

### 2.4 A snapshot proves the lookup can avoid scanning unrelated history

A consistent SQLite online backup was made through a read-only source connection
into memory on nuc3, then streamed to the required external drive. Index
experiments changed only that isolated copy. SQLite on the comparison host was
3.54.1; the timings below are not a prediction of nuc3's payload throughput.

| Query | Before | After targeted index | Query-plan change |
|---|---:|---:|---|
| Missing transform scratch ID | 112.69 ms | 0.0424 ms | `SCAN operations` to indexed equality search |
| Latest discovery status | 102.32 ms | 0.0105 ms | Full table scan and temporary sort to ordered partial-index scan |

These were single before/after observations, with different cache conditions.
The durable result is the change in query plan; the latency ratios are
illustrative, not a benchmark confidence interval. The missing-ID query returned
zero rows both times; discovery returned one row. Exhaustive result equivalence
and migration tests are still required.

Local query-comparison receipts and the private snapshot are retained outside
the Git checkout. The measurements are summarized here; database contents must
not be committed or included in a PR.


### 2.5 Review follow-up — candidate selectivity and all nine query plans

The snapshot's kind × state histogram is below. It contains no `sqlite_stat1`.
The new candidate index must win against `operations_state` without relying on
ANALYZE having run. All counts are from the consistent snapshot, not an estimate.

| Kind | Succeeded | Failed | Cancelled | Review | Running | Preview |
|---|---:|---:|---:|---:|---:|---:|
| delete | 419 | 0 | 0 | 101 | 0 | 0 |
| directory_publish | 448 | 0 | 122 | 0 | 0 | 0 |
| extract | 176 | 5 | 0 | 0 | 0 | 0 |
| inspect | 14 | 2 | 0 | 0 | 0 | 0 |
| par_repair | 13 | 32 | 0 | 0 | 1 | 0 |
| par_restore | 6,992 | 0 | 0 | 0 | 0 | 0 |
| relocate | 448 | 0 | 16 | 0 | 0 | 0 |
| retention_preview | 1 | 0 | 0 | 0 | 0 | 1 |
| retire_workspace | 199 | 0 | 0 | 0 | 0 | 0 |
| scan | 3,836 | 0 | 0 | 0 | 0 | 0 |
| stage | 0 | 1 | 0 | 0 | 0 | 0 |
| torrent-copy | 0 | 0 | 0 | 0 | 0 | 0 |

The succeeded-only index spans 12,546 rows. Kind selection narrows relocation
work to 464 rows, of which 448 succeeded. There are zero torrent-copy rows in
this snapshot: the empty-kind lookup is useful evidence for avoiding unrelated
history, but is not a populated torrent workload benchmark. Add populated,
mixed-state torrent fixtures in the Rust regression suite.

Checking the complete production queries exposed two defects in the original
three-index proposal:

- With `operations_kind_state` installed, SQLite chose it for discovery and
  added `USE TEMP B-TREE FOR ORDER BY`, bypassing the original discovery index.
- The stage-cleanup query at `torrent.rs:1731` constrains kind but not state.
  `(kind,state)` groups rows by state before its implicit rowid tiebreaker, so
  this query still needed a temporary sort. The compaction query at line 1773
  constrains both columns and did not need a sort.

The revised candidate in §4 adds kind as the first discovery-index expression
and replaces `(kind,state)` with a kind-only index. In the same snapshot all
nine exact query plans select the intended index, neither torrent cursor query
sorts, and both show `rowid>?` in the index search. Relocation queries choose
`operations_kind` over `operations_state` and retain all state/authority filters.
The trade-off is testing state on 464 relocation candidates rather than 448;
this avoids adding a fourth index solely for the state-unconstrained torrent
query. Revalidate this choice with the application's bundled SQLite and larger
mixed-state journals before implementation is accepted.

Full SQL, histogram and before/after plan receipts are retained outside Git.
The permanent tests in
[operation_index_tests.rs](../crates/nzbd-state/src/artifacts/operation_index_tests.rs)
now exercise all nine production statements with the bundled SQLite.

## 3. Code history — recent changes are credible contributors

| Change | Source evidence | What it establishes |
|---|---|---|
| September 25, `b7c2822` | Adds `Inventory::discovery_status` in [tasks.rs](../crates/nzbd-state/src/artifacts/tasks.rs). | Introduced the unindexed JSON kind filter and latest-created sort. |
| September 30, `3f1efc2` | Adds per-segment `sync_data` in [writer.rs](../crates/nzbd-engine/src/writer.rs). | Adds a durability cost on the receive/write path; not the dominant measured database scan. |
| October 6, `897920b` | Adds `scratch_operation` in [workspace_lifecycle.rs](../crates/nzbd-state/src/artifacts/workspace_lifecycle.rs). | Introduced the dominant measured workspace lookup and its broad uses in custody checks. |
| October 6, `6f7149c` | Changes empty-intermediate fallback in [configuration](../crates/nzbd-config/src/lib.rs). | Changes allocation location; distinct from `897920b` and not independently proven to explain the rate loss. |
| November 1, `a630414` | Introduces the completed-relocation candidate query shape in [relocation.rs](../crates/nzbd-state/src/artifacts/relocation.rs). | Adds another measured scan over succeeded operation history. Later cleanup changes also affect its final predicate. |

These dates fit a regression developing over weeks as operation history grows.
They do not locate the user's last known fast deployment or prove that the
November 7 pipeline changes introduced the problem. Compare the actual deployed
binary revisions when evaluating that narrower question.

### 3.1 The dominant lookup scans JSON and holds the shared connection

The exact query in `scratch_operation` is:

```sql
SELECT data FROM operations
WHERE json_extract(data,'$.kind') IN ('extract','par_repair')
  AND json_extract(json_extract(data,'$.request'),'$.scratch.id')=?1
LIMIT 1;
```

`Operation.request` is a JSON-encoded string inside the operation JSON. The
nested extraction is intentional. The existing primary key and `state` and
`artifact` indexes do not serve this scratch-ID predicate.

[Inventory](../crates/nzbd-state/src/artifacts/mod.rs) uses a shared
`Mutex<Connection>`. `scratch_operation` holds that mutex while executing the
query. Its callers include `artifact_in_use` and `workspace_assessment`; the
former participates in custody/admission checks, not just the Files UI.
The normal `artifact_in_use` call checks a source artifact ID, which does not
match a transform scratch ID. Its normal outcome is therefore a miss that scans
the complete table. Start caller attribution with `reserve_reconciliation` in
[startup.rs](../crates/nzbd-state/src/artifacts/startup.rs), which has roughly a
dozen reconciliation call sites; it is a candidate for the observed cadence,
not yet a measured attribution. Other source-ID callers include deletion,
worker retry/cancellation, materialization and seed-retirement admission.

```text
custody / workspace lookup
          |
          v
shared inventory connection locked
          |
          v
scan operation JSON, including unrelated historical PAR restores
          |
          +---- other inventory users can wait for the connection
          |
          v
return matching workspace or absence
```

The completed-relocation cleanup query additionally filters nested request
fields while selecting `state='succeeded'`. The existing state index includes
12,546 succeeded records. Reconciliation repeats the query for storage roots.
Read the complete predicate in [relocation.rs](../crates/nzbd-state/src/artifacts/relocation.rs)
before changing it; it contains authority, delivery, retry and cleanup guards.

## 4. Proposed contract — change access paths without changing authority

Re-verify all SQL against baseline `b5ef661` at implementation time. The first
candidate correction is additive, non-unique SQLite expression indexes, created
idempotently by inventory initialization for new and existing installations:

```sql
CREATE INDEX IF NOT EXISTS operations_transform_scratch
ON operations(
  json_extract(json_extract(data,'$.request'),'$.scratch.id'),
  json_extract(json_extract(data,'$.request'),'$.scratch.generation')
)
WHERE json_extract(data,'$.kind') IN ('extract','par_repair');

CREATE INDEX IF NOT EXISTS operations_scan_recent
ON operations(
  json_extract(data,'$.kind'),
  json_extract(data,'$.created_at') DESC
)
WHERE json_extract(data,'$.kind')='scan';

CREATE INDEX IF NOT EXISTS operations_kind
ON operations(json_extract(data,'$.kind'));
```

The first index serves scratch ID lookups and the generation-qualified variant
in [transforms.rs](../crates/nzbd-state/src/artifacts/transforms.rs). The second
serves the discovery equality filter and its order, even with the generic kind
index present. The third serves relocation and torrent-kind filtering while
retaining rowid order when kind is fixed; state remains a residual predicate.
All three revised shapes were checked together on the snapshot against the nine
queries in §6.2. Bundled-SQLite regression tests and write/migration cost checks
remain acceptance requirements. Place the statements in the existing schema
batch in [mod.rs](../crates/nzbd-state/src/artifacts/mod.rs), beside
`operations_state` and `operations_artifact`, following the `artifacts_bytes`
upgrade precedent.

1. **Keep query predicates and authority checks intact.** Indexing changes the
   candidate access path; it must not waive generation, identity, Keep, hold,
   active-use, retry, delivery or exact-operation requirements.
2. **Keep indexes non-unique and test the producer invariant.**
   [workspace()](../crates/nzbd-state/src/artifacts/transforms.rs) derives
   `scratch.id = format!("scratch-{key}")` from the operation primary key,
   returns the existing workspace for a known key, and `save_operation` upserts
   on that key. Supported producer calls cannot create two operations with one
   scratch ID. The traversal-order ambiguity is closed for that path. Test
   repeated same-key admission and distinct keys, asserting one operation per
   derived scratch ID. This is a construction invariant, not a new database
   UNIQUE constraint or permission to adopt arbitrary malformed historical rows.
3. **Let SQLite maintain derived keys transactionally.** Do not introduce a
   separately updated cache or manually populated side table in this fix.
4. **Measure migration and writes.** Index construction parses existing JSON
   and holds write locks. Measure startup cost, added database size and
   `save_operation` latency on a realistic journal. Operation data and transform
   requests are serde-written, so malformed JSON is not expected in normal
   records. Include a non-transform operation with a non-JSON request in the
   upgrade fixture: the partial WHERE must shield it from nested extraction.
   A genuine CREATE INDEX failure must fail inventory open loudly through the
   existing schema batch; never add a fallback that hides the failure.
5. **Keep connection/thread redesign separate unless measurements require it.**
   First remove proven scans. Additional connections and moving callers off
   the actor introduce distinct consistency and lifecycle considerations.

## 5. Guardrails — preserve data and isolate the hypothesis

Do not disable segment sync, journal durability, CRC checks, verified publication
or deletion custody to obtain a higher rate. Do not delete operation history or
recovery receipts to shrink the table. Do not move existing payloads or change
intermediate-directory settings during the first comparison: doing so changes
two variables and obscures whether the code correction helped.

The branch implements the three indexes and regression tests. Production
database/index state and download configuration have not been modified.
Deployment and active-throughput qualification remain separate from PR merge.

## 6. Milestones — prove correctness before claiming speed restoration

### 6.1 Capture the affected caller and a repeatable active baseline

Use bounded profiles during a substantial normal Usenet download. Attribute
scratch-query calls starting with `reserve_reconciliation` → `artifact_in_use`
and check other source-ID callers and workspace assessment. Capture owner tick
latency and `PersistenceFinished.elapsed_ms` on the same timeline as inventory
mutex owner/waiter intervals, actor responsiveness, wire bytes, payload bytes
and file completion. Distinguish direct connection-lock contention from state
I/O that delays persistence or a tick. Include maintenance and post work because
those were present in the observed workload.

**Acceptance:** a retained receipt identifies the slow query, calling path,
mutex owner/waiter, owner tick and persistence elapsed time within the active
download interval. Explicitly report if scheduler latency remains small, if
tick/persistence stalls dominate, or if scans precede stalls through shared I/O;
do not infer the missing dependency from scan volume alone.

### 6.2 Implement indexes and protect upgrades and result semantics

Add the reviewed indexes in inventory schema initialization. Regression tests
must reopen an existing installation without those indexes, preserve all rows
and allow repeated startup. Exercise real and normal-miss source IDs, both
transform kinds, generation mismatch, the one-operation-per-derived-scratch-ID
invariant, non-transform history with a non-JSON request, and latest-discovery
ordering. Test updates to operation kind,
request and state so index entries cannot become stale.

Permanent `cargo test` tests must execute `EXPLAIN QUERY PLAN` for every exact
production statement below, with all proposed indexes present, on fixtures large
enough to exercise unrelated history and mixed operation states. Share production
SQL constants with tests or otherwise guard against test SQL drifting from the
real call site. A one-time snapshot check is not sufficient. Partial-index
predicate implication is syntactically sensitive: preserve the production
`IN ('extract','par_repair')` form and order in the partial WHERE.

| Baseline statement | Required plan assertion |
|---|---|
| [workspace_lifecycle.rs:634](../crates/nzbd-state/src/artifacts/workspace_lifecycle.rs#L634), `scratch_operation` | `USING INDEX operations_transform_scratch` |
| [transforms.rs:486](../crates/nzbd-state/src/artifacts/transforms.rs#L486), generation-qualified scratch | `USING INDEX operations_transform_scratch`; both equality keys |
| [torrent.rs:1868](../crates/nzbd-state/src/artifacts/torrent.rs#L1868), artifact/state/scratch | `USING INDEX operations_transform_scratch`; preserve residual artifact/state predicates |
| [tasks.rs:10](../crates/nzbd-state/src/artifacts/tasks.rs#L10), discovery | `USING INDEX operations_scan_recent`; no temporary sort |
| [relocation.rs:1521](../crates/nzbd-state/src/artifacts/relocation.rs#L1521), completed source relocations | `USING INDEX operations_kind`; no rowid sort |
| [relocation.rs:1701](../crates/nzbd-state/src/artifacts/relocation.rs#L1701), cleanup wake | `USING INDEX operations_kind` |
| [relocation.rs:3093](../crates/nzbd-state/src/artifacts/relocation.rs#L3093), full cleanup-candidate predicate | `USING INDEX operations_kind`; keep correlated authority subqueries |
| [torrent.rs:1731](../crates/nzbd-state/src/artifacts/torrent.rs#L1731), stage cleanup | `USING INDEX operations_kind`; rowid lower bound; no temporary sort |
| [torrent.rs:1773](../crates/nzbd-state/src/artifacts/torrent.rs#L1773), compaction | `USING INDEX operations_kind`; rowid lower bound; no temporary sort |

Line numbers identify `b5ef661`; relocate the complete statement if edits move
it. Test with no `sqlite_stat1`, as in production, and with populated torrent
history. Retain the existing custody, concurrent-use, Keep and cleanup-retry
tests. Do not force planner choice with `INDEXED BY` just to make tests pass.

```bash
cargo test -p nzbd-state --lib  # Inventory and lifecycle regression suite.
cargo fmt --all -- --check     # Formatting check for the reviewed changes.
```

**Acceptance:** fresh/open-upgrade/idempotent migration tests pass; fixture
results agree with the original queries; access costs scale with matching
candidates rather than unrelated PAR history. Required repository CI is green.
Builds, caches and test scratch stay on the external drive.

### 6.3 Compare the corrected binary on nuc3 with storage unchanged

After review and an approved rollout, retain the previous executable/image and
record an identifiable candidate build. Use the same provider settings, storage
paths and comparable large downloads. Collect multiple sustained intervals
without syscall tracing; keep traced samples separate. Record post-processing,
maintenance, competing traffic, owner tick latency and persistence elapsed time
for both versions. Check whether any remaining production statement still scans
`operations`: its roughly 277 MiB size exceeds SQLite's usual roughly 2 MiB
connection page-cache default, so scans can churn the cache even on OS cache
hits. Measure the actual cache setting rather than assuming the default. The
`par_restore` full scans in `writer_naming.rs` are test-only, not production
follow-ups.

**Acceptance:** full-table metadata reads and long inventory waits fall sharply,
no correctness/retention behavior changes, and measured wire/payload throughput
improves. Do not require exactly 109 MiB/s from a single uncontrolled release.
If inventory work falls but throughput does not improve, reject it as the
complete explanation. Test owner tick and persistence stalls next, including
shared-state I/O coupling; also check network/provider and writer sync. Only
then proceed to the payload-storage comparison in §6.4.

### 6.4 Evaluate storage only if the corrected download remains limited

Compare equivalent new allocations on local storage and GlusterFS without
changing queue state placement and provider settings simultaneously. Measure
per-file write/sync latency and idle request time. Separate the later completed
publication copy from active receiving.

**Acceptance:** a controlled comparison quantifies the storage contribution.
The outcome, rather than the mere presence of a GlusterFS mount, determines
whether another storage or batching change is justified.

## 7. Fable review — disposition and remaining decisions

Fable approved the direction and preservation of authority, with four required
corrections. This revision closes the scratch-ID ambiguity using the producer
invariant, names permanent query-plan tests for all nine production statements,
adds the kind × state histogram and measured planner choices, and makes owner
tick/persistence stalls a co-equal causal hypothesis throughout qualification.
Caller attribution starts with normal source-ID misses in reconciliation.
Upgrade tests now explicitly cover a non-transform non-JSON request; schema
failures remain fatal rather than hidden.

**New evidence requiring review:** the original `(kind,state)` index caused a
sort in the state-unconstrained torrent cursor query, and competed with the
original discovery partial index. The revised set keeps three indexes by using
kind-only selection and a discovery equality/order prefix. All nine exact plans
now choose the intended index without a temporary sort on SQLite 3.54.1. Review
this trade-off before implementation; permanent tests must verify it on the
bundled SQLite and realistic populated fixtures.

The review's approval is of the indexing direction, not proof of the complete
109 MiB/s regression cause or approval of an unmeasured production rollout.
The user authorized implementation, PR creation and merge after review. The
implementation changes access paths only; production deployment and the
active-throughput comparison remain outstanding.

## 8. Implementation — additive indexes and permanent regression checks

The three index definitions in §4 are in the existing inventory schema batch.
[operation_queries.rs](../crates/nzbd-state/src/artifacts/operation_queries.rs)
shares all nine exact SQL statements with their production callers and tests;
comparison with `b5ef661` confirmed their SQL text is unchanged.

[Four regression tests](../crates/nzbd-state/src/artifacts/operation_index_tests.rs)
cover existing-inventory upgrade and repeated open, query/result equivalence and
index selection for all nine statements without ANALYZE, ordered torrent rowid
cursors with mixed states, non-JSON requests in non-transform history, kind and
request/state/order updates, workspace scratch-ID construction, and fatal schema
creation errors. The fixture includes 512 unrelated PAR records and 96 mixed-state
torrent records so empty production torrent history cannot hide a bad plan.

A local warm-snapshot cost check on SQLite 3.54.1 added about 332 KiB of indexes.
Individual creation took 322 ms (kind), 137 ms (discovery), and 147 ms (scratch),
including local commits. Updating the same 32 large PAR records (5.6 MB JSON)
inside rollback-only trials took 1.4–2.2 ms before indexing and 4.4–4.9 ms after.
Those update timings exclude commit/fsync and are not nuc3 predictions. The
added write cost is real and remains part of runtime qualification.

Validation results and PR merge status are recorded with the PR. The remaining
root-cause attribution and live comparison gates in §6 remain necessary before
claiming the earlier 109 MiB/s rate has been restored.
