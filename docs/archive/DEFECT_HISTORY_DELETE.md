# Defect — "forget" doesn't forget, and the history cursor pays for it

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [HISTORY_INCREMENTAL.md](../HISTORY_INCREMENTAL.md).

**Status:** resolved with shared JSONL tombstones · **Resolved:** 2026-08-05 ·
**Confirmed:** probe 2026-07-26 · **Found:** during the post-build review of
integration phase 1 ([INTEGRATION_PLAN.md](INTEGRATION_PLAN.md)) ·
**Present since:** `1fdad15` (2026-07-17), when delete and the JSONL rebuild
first shipped together — though until `9f402d8` (same day, cluster C2) added
the throttled `refresh()` on the read path, the entry only came back at the
next daemon start, which is rare enough to look like something else

Companion to [ARCHITECTURE.md](../ARCHITECTURE.md) §8.6 (how history is
stored) — this is *one specific thing that store got wrong, the decision that
fixed it, and the tests that keep it fixed*.

The fix chooses “forget it everywhere.” `HistoryDb::delete` now appends one
portable tombstone for every matching `(job, completed_at)` key before it
removes the derived SQLite row. Every node unions entry lines and tombstones;
a tombstone wins regardless of file order, so a peer's old copy cannot
resurrect the row. The cursor never publishes the forgotten completion again.

Before the fix, `HistoryDb::delete` removed the SQLite index row and left the
append-only JSONL line in place, so the next `refresh()` imported the entry
straight back with a fresh, higher rowid. That had two consequences, one for
operators and one for the integration surface:

- **The UI's "forget" was undone within one history poll.** Clicking it again
  repeated the cycle. There was no way to remove a history entry.
- **`GET /api/v1/history?since_seq=` delivered that entry twice**, under two
  different cursor values, because the resurrected row was new as far as the
  index was concerned.

The second is why this was found: phase 1 made the rowid a public cursor,
which turned a latent storage bug into a wrong answer on a documented API.
The first is the one users actually hit, and it predates all of that.

---

## 1. Root cause — delete was the only transition that was not durable

The JSONL is the authoritative, mergeable log; the SQLite index is a derived
read model rebuilt from it (ARCHITECTURE §8.6 / ADR-16). Before this fix,
every state change to a history entry was written back to the log so it
survived a rebuild — except one:

| Transition | Written before the fix? | Survived a rebuild? |
|---|---|---|
| `record` (a job finished) | yes — the entry is appended | yes |
| `hide` (a client imported it) | yes — `set_hidden` re-appends the updated entry | yes |
| `restore` (un-hide) | yes — same path | yes |
| `mark_seen` (a client's poll listed it) | no — index-local by design, and cheap to lose | n/a, advisory |
| **`delete` (forget this entry)** | **no** | **no — it came back** |

`hide` already re-appended deliberately, with a comment saying why: *"so the
hidden state survives an index rebuild (the upsert makes the last JSONL line
win)"*. Before tombstones, `delete` had no equivalent, so the rebuild's
`INSERT … ON CONFLICT` put the row back with nothing to say it should not.

The rebuild is not a rare recovery path. `refresh()` runs on **every**
`GET /api/v1/history`, on the compat `history` method, and in the
duplicate check `append` performs — throttled to one re-union per 5 s, and
`open()` seeds the index without touching the throttle, so the first read
after a daemon start rebuilds immediately rather than waiting out a
window. An *arr polling history every 30 s is, incidentally, a machine for
triggering this.

```
 client: POST /api/v1/history/2/actions/delete
            │
            ▼
   delete()  ── DELETE FROM history WHERE job_id = 2 ──▶ index: row gone
            └─ drop_spool(2) ─────────────────────────▶ NZB spool: gone
               (JSONL untouched — the line for job 2 is still there)
            │
 client: GET /api/v1/history            (the first poll past the 5 s
            │                                    refresh throttle — usually
            ▼                                    the very next one)
            │
            ▼
   refresh() ── rebuild_from_jsonl ── re-reads every history*.jsonl
            └─ insert(job 2) ─▶ no conflicting row ─▶ NEW rowid ─▶ back
```

---

## 2. Reproduction — confirmed, not inferred

Probe against `HistoryDb` directly on 2026-07-26, single-node layout
(`jsonl_dir = Some(state_dir/history)`, which is what the daemon uses).
Four entries recorded, then:

```
initial                       [(job 1, seq 1), (job 2, seq 2), (job 3, seq 3), (job 4, seq 4)]
delete(job 2)                 [(job 1, seq 1),                 (job 3, seq 3), (job 4, seq 4)]
refresh()                     [(job 1, seq 1), (job 3, seq 3), (job 4, seq 4), (job 2, seq 6)]
                                                                              ^^^^^^^^^^^^^^^ back
delete(job 2); refresh()      job 2 returns at seq 15      ← deleting again does not help
delete(job 4) ×3; refresh()   job 4 returns at seq 32      ← unbounded, one new rowid per cycle
```

**A consumer's view.** A consumer that had walked the cursor to `seq 4`
and asks `?since_seq=4` is handed job 2 at `seq 6` — an entry it already
imported, with a different cursor value, so no amount of remembering
"the last seq I saw" prevents it.

**The hidden case is different and worth stating separately.** An entry a
client hid (the "imported" signal) and someone then forgot comes back
**with `hidden = true` and `picked_up_by` intact**, because `hide` did
append. So it stays invisible to compat clients — Sonarr will not re-import
it — but it *is* visible to `?since_seq=` (hidden-inclusive by design,
§3.3 of the plan) and to the UI's own hidden-inclusive view. The
integration duplicate happens either way; the *arr re-import risk does not.

**Untouched rows keep their `seq`.** Job 1 stayed at 1 and job 3 at 3
across every cycle above. The cursor is stable and monotone for rows that
are never deleted, which is why this shows up as a duplicate rather than
as wholesale renumbering.

**The `seq` jump is a red herring, but explain it once so nobody chases
it.** Job 2 went 2 → 6, not 2 → 5, because SQLite's `AUTOINCREMENT`
advances `sqlite_sequence` on the whole re-import pass, not just on rows
that were actually inserted. On a real history of a few thousand entries,
each rebuild burns a few thousand ids. It is ugly and it is harmless:
`i64` has room for centuries of it, existing rows keep their ids, and
ordering is unaffected.

---

## 3. What it broke, concretely

**Operators.** The UI's per-entry **forget** ("remove the history record,
keep files") and **delete files** both call `HistoryDb::delete`. Neither
stuck before tombstones. The entry returned on the next render, and clicking
again repeated the cycle — with the spool already dropped, so the row that
came back had `can_requeue: false` and had lost its Undo. This was very likely
one of the two mechanisms behind the *"deleted items came back after
refresh"* field report of 2026-07-26 (STATUS.md, round 5); that
investigation found and fixed a queue-side cause, and this history-side
cause was never in scope.

**The undo-a-delete flow.** `history_requeue` re-adds the job and then deletes
the parked `DELETED` record, on the reasoning that *"the job is queued again,
so a `DELETED` record for it would be a lie"*. That delete did not stick
either, so after an Undo the operator had both a live job and a `DELETED`
history row claiming otherwise — exactly the lie the code set out to avoid.

**Compat clients.** `HistoryFinalDelete` (nzbget's permanent-delete verb)
mapped to the same call and was equally ineffective. Low blast radius: the
*arrs use `HistoryDelete` (which hides, and which works), not this.

**Native consumers on the cursor.** Duplicate delivery, as above. monarr's
importer is idempotent per download row, so the practical damage today is
a redundant scan notification rather than a double import — but "the
duplicate happens to be harmless downstream" is not a property this
repo gets to assume about every future consumer.

**Disk.** The JSONL still does not need a synchronous rewrite for a delete.
Later retention compaction drops obsolete entry lines but keeps tombstones;
the mutation is one small line per forgotten completion key.

---

## 4. Retired workarounds

- **Consumers previously had to dedupe on `(job, completed_at)`.** The key
  remains useful for consumer idempotency, but nzbd no longer emits a second
  cursor row because of its own delete/rebuild cycle.
- **Operators previously had no workaround.** Deleting again did not help;
  stopping the daemon and hand-editing `history*.jsonl` did. The tombstone is
  now the supported durable form of that intent.

---

## 5. Decision — forget means everywhere

The design question was:

> **When an operator forgets a history entry, do they mean "forget it
> here" or "forget it everywhere"?**

The answer is **everywhere**. History JSONL is the portable authority; an
operator action that disappears when another node rebuilds from that authority
is not a delete. The implementation combines the durable Option A record with
an Option B-shaped *derived* SQLite table so concurrent refreshes cannot expose
a deleted row between log replay steps.

### Accepted — durable tombstone in the JSONL

`delete` appends this backward-skippable record before changing SQLite:

```json
{"op":"tombstone","job":2,"completed_at_unix":1721952000}
```

- **Semantics:** forget means everywhere. Tombstones ride the shared
  volume like any other record and every node converges.
- **Compatibility:** old binaries fail to parse the mutation as a
  `HistoryEntry` and skip it, exactly as they skip an unknown or torn line.
  A rolling cluster must therefore finish upgrading before it relies on a
  forget issued by the new version; upgraded nodes converge immediately.
- **Ordering:** file names and clocks are not a cross-node total order, so
  tombstones are monotone for their immutable completion key. A new completion
  has a different `completed_at`; rewriting the same key does not revive it.
- **Compaction:** entry lines for a forgotten key may disappear, but the
  tombstone remains. A peer can retain its old entry indefinitely, so no node
  can prove the tombstone is globally safe to drop.

### Declined as authority — local-only tombstones

A local-only `deleted (job_id, completed_at)` table would make refresh work on
the serving node but leave another node free to show the entry. The new
`history_tombstones` table is instead a read-optimized projection of the JSONL
mutations. Wiping SQLite reconstructs the same tombstone set from authority.

That alternative was smaller—one table and one `NOT EXISTS`—but its
consequence was unacceptable: forgetting an entry on node A would leave it on
node B, and wiping A's derived index would resurrect it there too.

### Rejected

- **Delete the JSONL line.** Rewriting an append-only log on a shared
  volume, under concurrent appends from other nodes, to service a UI
  click. No.
- **Make `seq` a stored counter instead of the rowid**, so a resurrected
  row keeps its old cursor value. It cannot: the JSONL deliberately does
  not carry `seq` (a rowid is index-local; publishing one node's numbering
  would publish a number that is wrong everywhere else), so there is
  nothing to restore it from. And it fixes only the duplicate, leaving
  "forget doesn't forget" untouched — the smaller half of the defect.
- **Dedupe inside `list_since`.** Requires remembering what each consumer
  has already been handed. The cursor exists so that the server doesn't
  have to.

---

## 6. Acceptance — the defect stays fixed across every rebuild path

The implementation carries runnable evidence for every original requirement:

1. `delete_survives_refresh_and_never_replays_on_the_cursor` runs three full
   replay cycles and proves both the full view and a pre-delete cursor stay
   clear.
2. `tombstone_beats_hidden_and_peer_copies_regardless_of_file_order` puts the
   original and hidden re-append in a peer file and rebuilds a fresh index.
3. `durable_delete_converges_on_a_fresh_index` proves a second node reads the
   shared decision, not the locally deleted row.
4. `compaction_keeps_tombstones_that_guard_against_peer_resurrection` proves a
   later retention rewrite cannot discard the only durable delete evidence.
5. `failed_tombstone_append_does_not_delete_the_index_row` proves a failed
   authority write leaves both the visible row and its Undo spool intact.
6. `requeue_durably_removes_the_deleted_history_record` drives the native API
   delete → Undo path and proves the obsolete `DELETED` row stays absent on a
   fresh node.
7. `crates/nzbd-compat/tests/golden.rs` remains byte-identical; the wire
   surface did not change.
