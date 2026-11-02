# Usenet clustering

| | |
|---|---|
| Status | Implemented; merge-candidate validation tracked in [CLUSTERING_STATUS.md](CLUSTERING_STATUS.md) |
| Scope | Trusted-LAN nzbd nodes, fixed control voters, shared POSIX payload volume |
| Decision record | Supersedes the file-election limits recorded by ADR-13–16 |
| Delivery plan | [CLUSTERING_COMPLETION_PLAN.md](CLUSTERING_COMPLETION_PLAN.md) |

nzbd can distribute Usenet downloads and post-processing while presenting one
queue and one API authority. Hiqlite orders control state. The existing shared
volume carries article data, immutable result generations, journals, and the
leader-discovery projection.

BitTorrent remains single-node and cannot run in cluster mode. WAN clustering,
dynamic voter membership, and object-storage payloads are out of scope.

## Architecture

```text
client ──▶ any node ──proxy──▶ current API authority
                                │
                    Hiqlite fixed-voter quorum
                    jobs · revisions · exact leases
                    receipts · budgets · migration
                                │
       ┌────────────────────────┼────────────────────────┐
       ▼                        ▼                        ▼
 download worker          range worker              PP worker
 private attempt          private range             private tree
       └──────── sealed immutable generations ──────────┘
                                │
                     fenced authority publication
                                ▼
                       selected final output
```

The control and data planes have different jobs:

- The replicated control store is the authority for job identity and revision,
  exact work leases, command/publication receipts, provider-budget handoffs,
  and the one-time legacy migration marker.
- The shared payload volume is not a lock service. It stores large data,
  per-node article journals, private attempts, and immutable manifests.
- `leader.json` is a discovery projection. Losing or corrupting it does not
  create authority; a node must hold the replicated election lease.
- Each node keeps its Hiqlite directory locally. Never place `control_dir` on
  the shared payload mount.

## Control authority

Every mutable job has a stable incarnation and monotonically increasing
revision. One API request's changed rows commit as one revision-checked
Hiqlite transaction before success is returned; a failed commit restores the
exact replicated projection. URL admission has one narrower asynchronous
transition: its already-durable `Fetching` row may resolve once to queued or
failed under the same revision check. A worker completion names the exact job
revision it executed; a newer pause, delete, file edit, or other intent makes
that completion conflict.

Election and queue adoption are distinct steps. The native mutation API
returns a retryable `503` while an elected process is still adopting the
replicated projection, so a successful write cannot be erased by the tail of
takeover recovery.

Coordinator priority is a bounded cold-start bias, not a deterministic claim:
the delay remains below one election interval so failover cannot consume a
worker's lease deadline, and the replicated compare-and-set decides the winner.

The elected authority schedules and projects state but performs no local
download or post-processing. A coordinator's configured executor capacity is
available whenever it is a worker; while elected, work goes to another node so
every mutating attempt has the same durable lease and private-generation path.

A lease token is exact, not merely “owned by this node”:

```text
resource + owner node + owner process incarnation + fence + lease revision + expiry
```

Acquire, renew, release, and publication compare every field. Renewal returns a
successor token. Replaying an earlier token cannot release or publish. A
heartbeat may resynchronize only to a newer durable token with the same
resource, owner process, and fence; this covers a committed renewal whose
response was lost with the old leader without crossing a takeover. Workers also
keep a conservative monotonic deadline; failed or hung HTTP never extends it.
Cluster JSON request and response bodies are capped at 32 MiB; individual
work descriptors have the tighter limits described below. Ordinary control
calls have a 3-second connect / 10-second total deadline. Bulk completion and
exact-copy cleanup are owned background operations: a client timeout does not
cancel the operation or authorize a duplicate hash/deletion. Streaming API
proxy traffic keeps its separate streaming behavior.

## Migration and rollback

The first start of the new control plane is a stopped-cluster conversion:

1. Back up the legacy queue and history inputs under the node-local control
   directory.
2. Hash the queue plus retained shared-history evidence and transactionally
   import job/file IDs, job intent, and counters. History remains in its
   existing shared JSONL location and is included in the migration backup.
3. Commit the source fingerprint and target `cluster_id`.
4. Concurrent retries for the same cluster resolve idempotently. After the
   marker commits, later queue/history changes do not create another migration;
   a different `cluster_id` is rejected.

Coordinator authority requires lifecycle protocol 2 in the replicated
`cluster/leader` lease scope. The database rejects a missing, nonnumeric or
older capability on both claim and renewal, including a lease committed by an
older binary before the upgrade. Safe relinquishment remains available. Job
execution leases keep their own worker admission checks; a coordinator
capability never authorizes payload cleanup.

Stop and upgrade every coordinator and worker together. Once the new schema
is installed, an old coordinator cannot reclaim or renew authority. This is
an intentional compatibility fence, not a rolling mixed-version mode. Preserve
the control-store backup and exact owner storage identity; reverting only the
executable does not undo the schema or pending lifecycle obligations.

Do not run file-election and replicated-control binaries against the same
state tree concurrently. Rollback means stopping the new cluster and restoring
the pre-conversion backup, which loses later control changes, or deliberately
exporting the newer state. It is not a live downgrade.

## Scheduling

Download and post-processing placement uses stable weighted load:

```text
assigned or leased work / configured node weight
```

Assigned-but-not-polled work consumes capacity. Low-disk or stale nodes receive
no new writes. PP prefers a node that is not downloading, then applies the same
weighted comparison. These are placement weights, not feature qualification.

### Provider connection budgets

Server name is the initial provider-account key. All nodes that use one account
must configure the same name and account-wide connection ceiling.

Budget transfers are generation based. A reduction is published first. Each
NNTP connection task acknowledges only after reaching a batch boundary; a task
above the new allowance closes its socket before acknowledging. The leader
withholds increases until every old holder confirms the shrink. A stopped or
unreachable holder therefore leaves capacity conservatively reserved and the
diagnostic snapshot reports it as `uncertain_reserved_capacity` until all of
that holder's durable work leases expire. At the same bounded deadline the
worker independently drains its local connection budgets; only then may the
leader retire the dead holder and reassign the capacity.

Desired, commanded, acknowledged, and pending transfer state is replicated so
a leader change does not forget an uncertain socket. Zero-connection shares are
valid when an account has fewer sockets than executors.

## Download execution

Small and multi-file jobs retain whole-job leases. A one-file job with at least
64 articles is divided into fixed 32-article ranges when at least two download
workers are available. Range owners are chosen deterministically from node
weights.

Each range grant contains one file and only its inclusive article interval.
Articles outside the interval are absent—not marked failed or complete. The
worker writes to a fence-specific private directory and seals:

- the partial job state and exact article offsets/lengths/CRCs;
- payload hashes and byte counts;
- a hash of the exact sealed `job.json`;
- job incarnation, owner node, fence, and result identity.

Accepted ranges are durable control rows. Restart or reassignment reuses them;
only missing ranges return to NNTP. When all ranges exist, one exact assembly
lease reads the accepted manifests, requires the exact non-overlapping range
set and continuous byte coverage, validates every article CRC and offset,
writes one private output, checks an advertised whole-file CRC when present,
and returns one sealed result. No range worker renames the public file.

## Post-processing and publication

Remote post-processing begins by copying the completed input into a bounded,
fence-specific private tree. PAR rename/verify/repair, unpack, cleanup,
deobfuscation, and scripts operate there. Category publication is deferred to
the authority. A lease check runs before every local commit and before final
stamping.

The authority accepts a result only when:

- authentication succeeds;
- the exact lease token is live;
- job incarnation and expected revision still match;
- the immutable generation stays within the cluster namespace;
- every manifest path is safe and every length/hash matches;
- the hashed `job.json` exactly matches the completion request.

The publication receipt is committed before selection. Selection copies the
verified generation to a temporary sibling, fsyncs it, preserves any prior
directory under a uniquely named `.superseded-*` sibling, then atomically
renames the new directory. The result marker makes retries idempotent. PP
history uses the publication receipt timestamp, so a lost response retries the
same logical row rather than inventing a second completion.

When the authority has a history index it records that logical row directly.
Otherwise the PP executor records it only after the authority returns the
durable receipt timestamp; a lost response leaves the lease active and the
retry reuses the same receipt and history key.

The durable job carries `*Cluster:result-ref`, and history's `final_dir` points
at that immutable generation. The familiar completed-directory path is a
recoverable publication alias for compatibility, not the result identity.
Cluster scripts receive job-incarnation, lease/resource/fence/revision, and
exact script-receipt environment variables. Lease expiry cancels the owned
subprocess future immediately; the process wrapper kills children on drop.

Superseded directories are deliberately recoverable; inspect and remove them
only after confirming the selected output. In-progress `.building-*` and
private attempt directories are never selected as results.

## API and diagnostics

Every node serves the native API and NZBGet-compatible shim. A non-leader
proxies client traffic to the discovered authority. Peer endpoints under
`/cluster/v1/*` use the independent cluster secret, checked before JSON/body
extraction so malformed unauthenticated requests cannot expose peer schemas.

Authenticated `GET /api/v1/cluster` serves a two-second background snapshot,
so an unavailable shared mount does not turn the HTTP request into an
unbounded filesystem operation. It includes:

- local role, current authority, epoch, and sampled time;
- control mode and quorum-commit health (`true`, `false`, or `null` when the
  node is not a voter);
- registry rows and their disk/capacity observations;
- exact live lease resource, kind, owner, fence, revision, age, and expiry;
- desired/commanded provider budgets, pending shrink acknowledgements, and
  uncertain reserved capacity.

Settings → **Dev · Usenet cluster** contains the enable switch and common
fields. The safe-enable list labels evidence as met, unmet, or unknown. It is
advisory only: the operator can save `cluster.enabled = true` regardless.
Authentication, stale-token checks, invalid configuration, and transaction
conflicts still reject the operation they protect; they are correctness
boundaries, not feature gates.

## Configuration checklist

Before enabling, normally verify:

- an odd fixed voter roster (three voters is the normal minimum);
- unique stable node names and control IDs;
- node-local durable `control_dir` values;
- identical shared-volume and completed-download mounts;
- matching provider account names and account-wide caps;
- working cluster secret distribution and trusted-LAN transport;
- NTP/clock monitoring (leases use durable expiry plus local conservative
  deadlines; wildly wrong clocks remain operationally harmful);
- sufficient temporary capacity for private PP copies and range generations.
- at least one eligible non-authority executor whenever queued work should
  advance (a one-voter/one-process control-only cluster preserves the queue but
  intentionally does not execute unfenced local work).

These checks are advice, not an activation receipt.

## Failure behavior

| Failure | Result |
|---|---|
| Minority loses quorum | Cannot acquire/renew/publish control authority; cached diagnostics remain available |
| Worker or leader resumes after expiry | Old exact token is rejected; committed ranges remain reusable |
| Heartbeat hangs or response is lost | Worker deadline expires; a publication retry resolves by receipt ID |
| Pause/delete races completion | Reconciled job revision wins; stale result changes no queue/history state |
| Worker dies during a range | Accepted ranges remain; incomplete range alone is retried |
| PP attempts overlap | Each has private bytes; only the accepted fence is selected |
| Budget holder disappears | Its uncertain sockets remain reserved through durable lease expiry; the worker drains locally at its deadline, then capacity can move |
| Shared volume stalls | Control and cached diagnostics stay bounded; payload publication reports incomplete |

## Limits

- Fixed voters only; changing membership is a stopped-cluster operation.
- Trusted LAN only. Use network policy or a private overlay; this is not a WAN
  protocol.
- One shared POSIX payload namespace is required.
- Range splitting intentionally starts with one large active file and fixed
  range size; no live resharding or adaptive range migration.
- Provider accounts are keyed by server name in this release.
- Deployment and node restarts are separate from merging the software.

## Work descriptor limits

A serialized job plus its grant scope may occupy at most 16 MiB. Larger
work is held with `descriptor_oversize` and an instruction to split the NZB
or reduce its metadata; it is not repeatedly leased to workers. Each poll
returns at most one grant, so metadata cannot multiply past the transport
budget when a worker advertises many slots. Later polls fill remaining slots.
Cluster JSON request and response bodies share a 32 MiB cap. Completion
metadata above the descriptor cap receives an explicit 413 response rather
than relying on Axum's smaller default JSON limit. These limits cover both
whole-job and bounded range/assembly grants.

The focused `work_rpc_round_trips_descriptors_above_one_mib_and_bounds_both_directions`
test sends a real 2 MiB loopback HTTP request and response, then checks refusal
of oversized request and response bodies. The cluster peer router applies the
same request cap before JSON extraction.

## Durable Delivery and exact copy cleanup

A cluster has one replicated consumer identity. Node storage identities remain
local and identify the owner of a particular artifact generation; a successor
leader must not reinterpret another node's device or inode as local ownership.
Upgrade migration preserves the established consumer identity and existing
Delivery sequence high-water before accepting new publications. Ambiguous
historical admission identity is retained with an actionable review diagnostic.

```mermaid
sequenceDiagram
    participant W as Worker
    participant L as Current authority
    participant C as Curator
    participant O as Exact source owners
    W->>W: Seal payload and persist completion outbox
    W->>L: Complete exact grant/result
    L->>L: Commit selection and durable materialization
    L->>L: Commit immutable READY and copy obligations
    C->>L: Discover READY; accept exact manifest
    L->>L: Commit consumer receipt
    L->>O: Retry exact owned copies independently
    O-->>L: Actual removed, retained, or retry result
```

A committed READY remains authoritative after Curator removes its consumer
path. Replay uses the durable receipt rather than requiring an impossible
rehash of that removed path. Each mutable copy result is merged by exact
revision/claim CAS; an old leader cannot overwrite a newer consumer receipt.
Completion and cleanup operations own their bulk work independently of a
short HTTP timeout, so retries join existing work instead of launching
concurrent hashes or unlinks. Heartbeats run on a separate control lane.

Every new receiver advertises its storage identity and start-ordering
capability. The original lease transaction records these facts. The receiver
journals start before private copying/import/writer creation; removal fencing
serializes against that journal. Missing legacy capability is not upgraded
implicitly. Cleanup requires actual writer/task stop acknowledgments; lease
expiry or an absent queue row alone cannot prove quiescence.

Explicit DeleteFiles records its original mode with the exact current job
incarnation/tombstone before physical deletion. It may override Keep only for
those exact owned copies. Remove/Park/Trash preserve files. Consumer handoff,
active use, recoverable dependencies and unknown members remain separate
custody guards. A rejected sealed result gets a durable private cleanup or
retention receipt; it never produces an empty public READY. Source-owner
seal/cleanup replay remains bound to the original generation.

Keep on failure is mutable desired policy, separate from immutable Delivery
admission metadata. HTTP202 and `applied:false` mean owners have not all
acknowledged the requested policy. An offline owner stays pending; a removed
source reports unavailable. Successful-source retirement remains eligible
regardless of this failure-only preference. Partial raw copies without proof
that all recoverable dependencies are safe remain visibly retained. Automatic
retirement of redundant partial raw copies is deferred.

### Bounds and upgrade order

- Delivery discovery is bounded to 8 MiB per public row and 16 MiB per page. An
  invalid/oversized row becomes a trusted sequence/ID `discoveryError` stub,
  allowing the next cursor to advance while retaining the source for review.
- Public `sourceCopies` is bounded to 64 small progress records. Private file
  deletion manifests are not part of that public projection.
- Descriptors are 16 MiB, peer request/response bodies 32 MiB, private Delivery
  records 32 MiB and serialized transaction parameters 64 MiB. Raft WAL segments
  are 128 MiB; existing smaller WAL configuration upgrades on reopen.
- Upgrade Curator first so it understands discovery stubs, then upgrade Runner
  coordinators and workers together. The coordinator lease scope and receiver
  capability fence reject incompatible lifecycle participants. Downgrading
  while new lifecycle obligations exist is unsupported.
- This upgrade migrates receipts and metadata only. It does not sweep legacy
  or unowned payload paths or authorize backlog cleanup.
