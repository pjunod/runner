# Integration plan — nzbd's side of the monarr pipeline

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [INTEGRATION.md](../INTEGRATION.md), written from [PIPELINE_CONTRACT.md](../PIPELINE_CONTRACT.md).

**Status:** ✅ N1–N7 built and tested (2026-07-26) · **Written:** 2026-07-26 ·
**Master plan:** `monarr/docs/plan-integration.md` (contract §3, phasing §7)

> **Built.** Every milestone below is implemented; its acceptance check
> lives in `crates/nzbd/tests/integration_events.rs` (daemon-level, N1–N5),
> `crates/nzbd-post/tests/pp_pipeline.rs` (N6),
> `crates/nzbd-state/src/history.rs` (N3 cursor properties) and
> `crates/nzbd-api/src/eventhub.rs` (N2 ring eviction, where 1024 events
> are cheap to produce). Two deviations from the text below, both
> deliberate, both narrowing the contract rather than widening it:
>
> 1. `pp_status` carries the history status string verbatim. For a job
>    that reached post-processing that is exactly the `PpFinal` set; the
>    terminal paths that never reach PP (health gate, failed fetch) emit
>    their existing `FAILURE/HEALTH` / `FAILURE/FETCH` rather than staying
>    silent. Nothing is invented for the event, and consumers switch on
>    the `SUCCESS`/`FAILURE` prefix the master plan §5.1 already assumes.
> 2. `history_seq` is `0` when the history write itself failed. The event
>    still fires — "PP ended and history did not record it" is precisely
>    the moment a consumer must not be left waiting on nothing.
>
> Two silences N4's acceptance check turned up, both fixed here:
> `JobSummary` carried no params (so a tracking id was invisible in the
> queue view it exists to be visible in), and compat `listgroups`
> hardcoded `"Parameters": []` (NZBGet populates it).
>
> **Contract amendment (from the post-build review).** The SSE `id:` is
> `<boot>-<seq>`, not a bare `<seq>`. A bare sequence number cannot
> distinguish "you missed events 51-60" from "we restarted and these are a
> *different* events 51-60" — the second served as a clean resume is
> silent corruption of the consumer's view, and the plan's own `reset`
> rule ("or the server restarted — seq resets") is unimplementable
> without an epoch. `Last-Event-ID` is opaque to SSE, so carrying it costs
> nothing; the plain monotone `seq` still rides in the body. Consumers
> should treat the id as opaque and echo it back verbatim.
>
> **Resolved 2026-08-05 —
> [DEFECT_HISTORY_DELETE.md](DEFECT_HISTORY_DELETE.md).** `HistoryDb::delete`
> now appends a portable tombstone for `(job, completed_at)` before removing
> the derived index row. Refresh, a rebuilt index, and a peer's stale entry
> cannot publish the completion again under a fresh `?since_seq=` cursor.
> Consumers should still treat `(job, completed_at)` as the stable identity
> of a completion, but no longer need that key to mask delete resurrection.
>
> Not done, and deliberately: cluster-mode PP stage timings are not wired
> to a node's `/metrics`. Leases run wherever the scheduler puts them, so
> a per-node summary would describe an arbitrary slice of the work;
> cluster-wide PP metrics need their own design.

This document is self-contained: everything nzbd must build, with the
exact contract slice it implements, so a session opened in this repo
alone can do the work. The master plan in the monarr repo holds the
cross-app rationale, the monarr/plurx packages, and the phasing; if this
file and the master disagree, the master wins and this copy is the bug.

The shape of the whole thing: monarr will subscribe to nzbd's existing
SSE stream instead of only polling the nzbget-compat API. nzbd's job is
to make that stream complete (post-processing events exist today only
as log lines), resumable (ids + replay), and reconcilable (a history
cursor) — and to stay honest about paths and about who is consuming it.
**nzbd gains no outbound HTTP and never learns monarr's address.**
Everything here is additive; the compat surface must not change.

Work milestone by milestone, in order — later ones lean on earlier
ones. Each ends with an acceptance check that is a runnable command or
an observable fact. If a step seems to require changing the compat
layer's behavior or adding outbound HTTP, stop and flag it instead.

---

## 1. Contract slice (copied from master §3.2–§3.4 on 2026-07-26)

Re-verify every identifier against the named file before building on
it, in case it moved.

**New events** — extend `Event` in `crates/nzbd-engine/src/events.rs`
(wire: `#[serde(tag = "event", rename_all = "snake_case")]`):

```jsonc
{ "event": "job_pp_stage", "job": 7, "name": "Show.S01E01…",
  "stage": "par_verify" }
// stage = snake_case of PostStage (nzbd-types):
//   par_rename | par_verify | par_repair | rar_rename | unpack |
//   cleanup | move | post_unpack_rename | script

{ "event": "job_pp_finished", "job": 7, "name": "Show.S01E01…",
  "category": "tv", "pp_status": "SUCCESS",
  "final_dir": "/downloads/complete/Show.S01E01…",
  "size_bytes": 1234567890, "health": 1000,
  "params": [["monarr-transfer","t-42-a3f9c1"]],
  "history_seq": 913 }
// pp_status = existing PpFinal::as_str() values, verbatim:
//   SUCCESS | PAR_FAILURE | UNPACK_FAILURE | SCRIPT_FAILURE
```

**Ordering guarantee (normative):** `job_pp_finished` is emitted only
after `HistoryDb::record` has returned, so a consumer reacting to it
can immediately `GET /api/v1/history?since_seq=<history_seq-1>` and see
the row.

**SSE resume:** every engine-event frame gains `id: <seq>` (monotone
u64, process lifetime; also `"seq"` inside the JSON). Ring buffer of the
last 1024 events; `Last-Event-ID: <seq>` on reconnect replays the tail;
seq unknown/too old (or daemon restarted) → first frame is
`event: reset` `{"reason":"gap"}` and the client must poll-reconcile.
Stream-local frames (`tick`, `hb`, `log`) carry no `id:`.

**History cursor:** `HistoryEntry` JSON gains `"seq"` (the SQLite
rowid). `GET /api/v1/history?since_seq=N&limit=M` returns rows with
`seq > N` in ascending order. The existing `?limit=` (descending, UI)
form is unchanged.

**Add-time params:** `POST /api/v1/jobs` gains optional query field
`params` = URL-encoded JSON object (string→string). Keys starting with
`*` are rejected 422 (reserved for internals like `*PP:done`). Params
land in `Job.params` and flow to history + compat `Parameters` through
the existing plumbing.

**Attribution:** consumers send `X-Nzbd-Client: monarr/<version>`; nzbd
records native-API and SSE callers in the client registry, same as
compat callers today.

---

## 2. Milestones

### N1 — post-processing events

The gap: `Event::JobFinished` fires at *download* completion; PP ends
with `tracing::info!`, a history write, and `remove_job_silent`. Nothing
event-shaped carries `final_dir`, ever.

- Add the two variants from §1 to `events.rs`.
- Give the PP manager a way to publish: the broadcast sender lives in
  the engine, and `nzbd_post::manager` holds an `EngineHandle` — add
  `EngineHandle::emit(&self, Event)` (or equivalent) rather than a
  second bus, so SSE and any existing subscribers see one stream.
- Emit `job_pp_stage` wherever PP transitions stages (the
  `set_job_status(job, JobStatus::Post{stage})` call sites in
  `crates/nzbd-post/src/manager.rs` — today silent by design; that
  design is what changes).
- Emit `job_pp_finished` in the finalize block of `process_job_ctx`,
  strictly **after** `HistoryDb::record` returns, carrying the fields
  from §1 (`final_dir` is already computed there; `history_seq` is the
  rowid the record call must now return; `params` = the job's non-`*`
  params via the existing `user_params` filter).
- Failed fetches and health-aborted jobs also write history rows today;
  make sure those paths emit `job_pp_finished` with the matching
  failure `pp_status` (or document precisely which terminal paths do
  not emit, in ARCHITECTURE — an event that *usually* fires is worse
  than none).

**Accept:** `cargo test -p nzbd-post -p nzbd-engine` green; a new
daemon test (see `crates/nzbd/tests/daemon.rs` patterns) downloads a
tiny job from `nzbd-nserv`, collects SSE, and asserts the order:
`job_added → job_finished → job_pp_stage+ → job_pp_finished`, and that
an immediate `GET /api/v1/history?since_seq=<history_seq-1>` returns
the row.

### N2 — SSE ids and replay

The gap: `broadcast::channel(512)` is lossy and the stream has no `id:`
lines — a consumer that blinks misses events with no way to tell.

- Wrap event fan-out in a seq-stamping layer (AtomicU64 in `ApiState`)
  plus a 1024-entry ring (`VecDeque` behind a lock is fine at this
  rate).
- `sse_events` (`crates/nzbd-api/src/lib.rs`): write `id:` on engine
  events; parse `Last-Event-ID`; replay from the ring before switching
  to live; emit `reset` per §1 when the gap can't be covered. Keep
  `lagged{skipped}` for mid-stream overflow.

**Accept:** daemon test: open SSE, note a seq, disconnect, cause 3
events, reconnect with `Last-Event-ID` → exactly the 3 missed events
replay, in order, before live resumes. Second case: reconnect with a
seq older than the ring → first frame is `reset`.

### N3 — history cursor

- Include `seq` in the JSON of `GET /api/v1/history`
  (`crates/nzbd-state/src/history.rs` — the SQLite `id` is currently
  not selected; select it).
- Add `since_seq` to the query: ascending, `seq > N`, respects `limit`,
  combinable with nothing else (keep it dumb).

**Accept:** test inserts 5 history rows, `?since_seq=` from each
midpoint returns exactly the later rows ascending; `?limit=`-only
behavior byte-identical to before (compat golden tests untouched).

### N4 — add-time params

- Extend `AddJobQuery` with `params: Option<String>`; parse as JSON
  object of strings; reject `*`-prefixed keys with 422 and a message
  naming the offending key.
- Apply to the job exactly as `GroupSetParameter` does today, so
  history/compat propagation is free.

**Accept:** test adds a job with
`params={"monarr-transfer":"t-1-abc123"}`, asserts the param visible in
`GET /api/v1/jobs/{id}`, in the compat `listgroups` `Parameters`, and —
after completion — in the history entry. A `*bad` key → 422.

### N5 — attribution for native + SSE consumers

The gap: `ClientRegistry` only hears from `nzbd_compat::note_client`,
so a native-API monarr is invisible in `GET /api/v1/clients` and
history `picked_up_by`.

- Note clients on authenticated `/api/v1` calls too, keyed by
  `X-Nzbd-Client` when present (fall back to User-Agent).
- Count open SSE subscriptions per client and expose them in
  `GET /api/v1/clients` (e.g. `"events": true` / subscriber count), and
  surface in the UI ("1 event subscriber: monarr/0.6.1") — the operator
  should be able to *see* that push is attached, not infer it.
- History hide via the native API attributes `picked_up_by` the same
  way compat `HistoryDelete` does.

**Accept:** daemon test: call native API + open SSE with
`X-Nzbd-Client: monarr/9.9.9`; `GET /api/v1/clients` lists it with the
subscription flag; hide a history row natively → `picked_up_by`
recorded.

### N6 — category destination honesty

The gap (pre-existing, load-bearing now): `[[category]]
dest_dir/unpack/extensions` are parsed and *advertised* to compat
clients as `CategoryN.DestDir`, but `nzbd-post` always writes to
`dest_dir/<sanitize_name(job.name)>`. Monarr path-maps off reported
paths; advertised ≠ actual is exactly the silent import failure this
whole plan exists to kill.

- Honor `[[category]] dest_dir` in the PP move step: final dir =
  `<category dest_dir>/<sanitized name>` when set, else the global.
  Honor `unpack = false` per category by skipping the unpack stage.
  (`extensions` filtering: implement or delete the key — decide, don't
  leave it half.)
- This is a behavior change for configs that set the key expecting
  nothing to happen; call it out in the release notes / STATUS entry.

**Accept:** pp_pipeline test with a category dest_dir asserts files
land there and `job_pp_finished.final_dir` + history + compat
`FinalDir` all agree on the real path.

### N7 — metrics and docs

- `/metrics` additions: `nzbd_events_emitted_total{event=…}`,
  `nzbd_sse_clients`, `nzbd_pp_stage_seconds{stage=…}` (histogram or
  summary, cheap buckets).
- Docs in the same commits as behavior: `docs/ARCHITECTURE.md` (events
  section + resume protocol), `docs/USAGE.md` — the "*arr handoff,
  demystified" section currently says *"There is no push."* That
  sentence changes to describe the subscribe flow and the fallback.
  `docs/CONFIGURATION.md` only if any knob is added (target: none —
  the ring size may be a constant). `STATUS.md` checklist entry.

**Accept:** `curl /metrics | grep nzbd_events_emitted_total` after one
download shows nonzero; docs grep: `grep -n "There is no push"
docs/USAGE.md` returns nothing.

---

## 3. Guardrails (stop and flag rather than violate)

1. **Compat surface frozen** — `nzbd-compat/tests/golden.rs` and the
   XML-RPC codec pass unmodified. Third-party arrs depend on it.
2. **No outbound HTTP.** No webhook client, no monarr URL in
   `nzbd.toml`. The direction decision (monarr subscribes) is settled
   in the master plan §3.
3. **Don't rename existing event variants, statuses, or the
   `PpFinal` strings** — they're load-bearing for the UI, compat
   mapping, and now the monarr adapter.
4. **`*`-prefixed params stay internal** — the new `params` field must
   not become a write path into `*PP:done` etc.
5. **SSE stays optional** — a consumer that never connects loses
   nothing that `?since_seq=` polling can't recover. If a design makes
   the cursor path weaker to make SSE stronger, it's backwards.

## 4. Order and independence

N1 → N2 → N3 can land as one PR each or together; N4/N5/N6/N7 are
independent of each other but all assume N1's `EngineHandle::emit`.
Ship this repo's work before monarr's phase 2 (master §7) — current
monarr keeps working against every intermediate state because it only
speaks compat until its native adapter exists.
