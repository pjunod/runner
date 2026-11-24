# BitTorrent M1b report — one queue contract, still no peer session

**Status:** implemented; merged in PR #9 · **Date:** 2026-08-05 ·
**Historical branch:** `codex/bittorrent-engine-unblock` ·
**Decision owner:** ADR-19 in [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md)

M1b added the protocol-neutral state and message boundary needed by any future
embedded BitTorrent engine. At that milestone the daemon had no torrent
configuration, admission route, listener, DHT node, tracker task, or dependency
on `nzbd-torrent`. Later M2a work added dormant `[torrent]` configuration and a
named guard that rejects `enabled = true`; M2b then added dormant
selective-session and queue-adjacent runtime ownership without wiring the
production daemon. BitTorrent remains unusable, with no production admission
route, listener, daemon session, DHT node, or tracker task.

This milestone was originally blocked with M2 after stable `librqbit` failed
the authoritative-resume and discovery-health gates. A re-check found the same
gaps in rqbit's unreleased 9.0 line, but also made the dependency distinction
clear: the queue/backend seam is engine-independent and testable with no
network or payload I/O. ADR-19 now authorizes that dormant seam while keeping
production wiring blocked.

---

## 1. Delivered contracts

### 1.1 Schema 3 owns the torrent job representation

`JobKind` now reserves `torrent`, and a defaulted `Job.torrent` record carries
only durable control facts: v1 info hash, non-secret source class, relative
metadata path, phase, selected file layout, cumulative accounting, readiness,
seed policy, canonical content path, recent activity, and a redacted bounded
error.

The queue writer now emits schema 3. Reads retain the compatibility rules:

- an unversioned document is version 1;
- schema 2 loads existing `nzb` and `url` jobs with `torrent = None` and no
  semantic change;
- schema 3 round-trips the torrent control record; and
- a future version still wins over a nested unknown-enum error and fails by
  version name.

This is intentionally a one-way downgrade boundary. A schema-2 binary sees
`schema_version: 3` and refuses the whole queue rather than silently skipping a
torrent job and serving a partial truth. The writer emits schema 3 even when
the queue contains only Usenet jobs, so merely running this build through one
snapshot save makes an older schema-2 binary refuse rollback. That collateral
cost is deliberate: conditional schema emission would make the downgrade
boundary depend on queue history and crash timing rather than one durable
format version.

### 1.2 Native read models gain additive neutral facts

`JobSummary` now includes:

```json
{
  "kind": "nzb | url | torrent",
  "ready": false,
  "ready_at_unix": null
}
```

Existing fields and `JobStatus` spellings are unchanged. Usenet readiness
continues to derive from its durable `*PP:done` stamp; torrent readiness comes
only from `TorrentRecord.ready_at_unix`. No torrent-only lifecycle value is
inserted into `JobStatus`, so older consumers do not receive a new status enum
case.

### 1.3 Backend traffic cannot starve control

The new backend channel has three independent paths:

| Path | Direction | Delivery rule |
|---|---|---|
| Commands | owner → adapter | Bounded FIFO for start, pause, resume, remove, priority, and limit changes |
| Structural facts | adapter → owner | Bounded reliable FIFO for metadata, ready, stopped, resumed, and failed facts |
| Progress | adapter → owner | Watched latest-value map, one replaceable value per job |

A progress flood therefore cannot sit ahead of a delete command or displace a
ready/failure fact. The tests send 50,000 progress updates into a one-command
channel and observe the remove command next; the owner sees only the latest
progress value for the job.

For M2 control, `TorrentRecord.control_intent` is the queue owner's durable
pause/resume request, distinct from the backend-observed `TorrentPhase`.
The owner snapshots a changed intent before placing its matching pause, resume,
or priority command on the bounded FIFO. A full FIFO retains that command and
retries it in order on the next owner tick. Retention is capped at 64 commands:
once full, a new control request is rejected before its intent is persisted,
rather than allowing an unattached executor to grow owner memory without
bound. Records written before this field decode as `running`.

`SafeError` accepts only an already-redacted message from the adapter and
enforces the proposal's 2 KiB UTF-8-safe persistence bound. Engine-specific
redaction remains an M2 adapter responsibility.

The M2e pause/resume executor emits `Stopped { Paused }` after it has applied
a pause, and `Resumed` after it has applied a resume. These facts are emitted
only on a state change: repeat controls leave the registry state and the
structural FIFO unchanged. A `Resumed` fact restores `downloading` or
`seeding` from the durable readiness checkpoint, so a paused seed cannot be
mistaken for a new download.

### 1.4 One active set spans both protocols

The existing priority, queue order, global pause, force priority, and
`max_active_downloads` rules now compute eligibility across Usenet work and a
fake torrent record. With the default cap of one, a higher-priority active
torrent owns the slot and NNTP does not borrow it. With two slots, both appear
in the active set and NNTP may progress in its own slot.

A torrent in `downloading` yields after 60 seconds with neither payload
progress nor a useful-peer fact. It remains live; a later activity fact makes
it eligible again. Checking/metadata work remains eligible, while seeding does
not consume a download slot.

---

## 2. Safety boundary

M1b is dormant by construction:

- `nzbd` still does not depend on or start `nzbd-torrent`;
- no config parser, API handler, watch directory, UI control, qBittorrent
  compatibility route, or peer listener was added;
- all mixed-protocol scheduling uses constructed fake records; and
- production recovery refuses a persisted torrent row with a named error and
  leaves the queue unchanged.

That last refusal is deliberate. Schema support proves durability and
downgrade behavior, but the daemon may not infer that durable representation
means an engine exists. M2 will replace the refusal only after ADR-19 names a
stable engine/API that passes M0 gates 7 and 8 plus the remaining platform
matrix.

---

## 3. Acceptance evidence

| Requirement | Evidence |
|---|---|
| Schema-2 Usenet migration | Fixture removes the new field, loads as schema 2, and preserves kind/status with `torrent = None` |
| Schema-3 torrent round trip | Durable record including file selection and accounting saves and reloads |
| Downgrade guard | Emitted document carries `schema_version: 3`; future-version fallback remains named |
| Shared priority/pause/cap | Mixed fake-torrent and real Usenet queue-selection tests |
| Stalled yield/reacquire | Boundary tests at 59/60 seconds and mixed active-set test after new activity |
| Progress backpressure | 50,000 updates coalesce while remove remains immediately receivable |
| Existing behavior | Workspace formatting, lint, tests, and Rust 1.85 checks are required before publication |

---

## 4. What remains before M2

M1b did not soften any M0 stop condition. The two conditions were:

1. ADR-19 selects the maintained rqbit v8.1.1 derivation and proves all eleven
   M0 gates on the native matrix. Required transfer facts remain public and
   bounded, while unavailable detailed tracker/DHT diagnostics stay explicit
   `unknown` rather than inferred;
2. all eleven M0 gates must pass on native macOS, Linux glibc/musl, and
   Windows, including private-mode capture and dependency review.

Both conditions and independent review completed on 2026-08-14; the status was
reconciled here on 2026-08-22. M2a and M2b have since merged; remaining M2
slices are tracked by #154–#160,
and #163 retains sole activation ownership; this report still authorizes no
production networking by itself.

The two repository prerequisites named in the original report are complete:
[`REGRAB_LOOP_PLAN.md`](archive/REGRAB_LOOP_PLAN.md) F1–F3 landed on 2026-07-31, and
[`DEFECT_HISTORY_DELETE.md`](archive/DEFECT_HISTORY_DELETE.md) now makes forget durable
across the shared JSONL. They no longer block M2. The engine decision completed
separately and is now accepted.

No production BitTorrent networking should appear in review of this milestone.
If it does, that is a scope and safety defect, not an optional follow-up.

---

## 5. Review focus

Reviewers should concentrate on four questions:

1. Does schema 3 preserve every schema-2 Usenet meaning while producing a
   loud downgrade boundary?
2. Can any progress volume delay a control command or erase a structural
   fact?
3. Does the mixed active set preserve current Usenet behavior at the default
   cap while preventing either protocol from stealing the other's slot?
4. Is every path that could start production peer traffic still absent or
   explicitly refused?

The engine choice, tracker API shape, and end-user torrent API remain M0/M2
review questions; they are not smuggled into this seam.

---

## 6. Post-merge Fable review reconciliation

Fable reviewed merged PR #9 at `f8ab647` and found one blocking recovery gap
plus two non-blocking design risks. The follow-up disposition is:

1. **Accepted and strengthened — cluster authority adoption.** Startup already
   used `QueueState::from_runtime_doc`, but leader takeover used the generic
   document converter. Takeover now validates through the same production
   boundary before enabling persistence or touching local state. An
   unsupported torrent row, future schema, or corrupt snapshot returns its
   named error, leaves `queue.json` and the worker queue byte-for-byte
   unchanged, and keeps leader scheduling disabled. The suggested
   journals-only fallback was not used because saving that reconstructed state
   could erase the authoritative queue. A leader whose adoption was refused
   retries on each lease tick, so repairing the snapshot restores scheduling
   without a daemon restart or election flap. Engine and cluster end-to-end
   tests pin the refusal, preservation, retry, and recovery claims.
2. **Accepted — pre-download starvation.** Fetching source, fetching magnet
   metadata, checking, and downloading now share the 60-second no-activity
   yield. `Queued` deliberately does not: a job that has not started cannot
   generate the activity needed to reacquire a slot. Pre-download work with no
   activity stamp is likewise treated as fresh rather than falling back to the
   possibly old queue time, so it gets one chance to run and produce a backend
   fact. M2 must stamp activity once work starts so a genuinely stalled phase
   eventually yields.
3. **Adapted — schema-3 rollback warning.** The one-way boundary remains, but
   §1.1 now states the easily missed consequence: a pure-Usenet queue is
   rewritten as schema 3 on its first save, so rollback to a schema-2 binary
   fails even when no torrent job has ever existed.
