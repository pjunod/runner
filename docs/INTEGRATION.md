# Integration — runner's seam with curator and cinema

**Status:** current for runner 0.5.0 (pipeline contract v2, M6a) · **Written
from:** [PIPELINE_CONTRACT.md](PIPELINE_CONTRACT.md) and the shipped code
(`crates/nzbd-api/src/lib.rs` `event_json`, the handlers named per section) ·
**Updated:** 2026-11-08

This page is the reference for what runner (nzbd) promises the two
applications downstream of it — curator (monarr) and cinema (plurx) — in wire
terms, where you look at each seam, and the command that proves it. The
contract is the design and its rationale; this page is what the build does.
Every event name, field name and status string below was read from the code;
`crates/nzbd-api/src/lib.rs` `integration_doc_names_every_sse_event` fails
when the event tables here and `event_json` disagree.

Fields marked **deprecated, removed in M6b** are still sent so a curator or
consumer that has not moved to contract v2 keeps working; runner M6b drops
them once every node runs curator M2 and runner M1a/M3 (see `STATUS.md`).

For connecting the classic \*arr apps over the NZBGet-compatible shim, see
[USAGE.md](USAGE.md); for every config key, [CONFIGURATION.md](CONFIGURATION.md);
for retention, parking and recovery of bytes, [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md).

The contract in three sentences: **runner emits exactly one terminal fact per
job, before it does anything else with the bytes**; curator drives its
download row from runner's event stream and uses the poll only to catch what
the stream missed; curator tells cinema what landed. Runner never calls
curator or cinema.

## The pipeline

```
   ┌───────────────┐   POST /api/v1/jobs (params: monarr-transfer)   ┌──────────────┐
   │               │  ─────────────────────────────────────────────▶ │              │
   │    curator    │                                                 │    runner    │
   │  (decides)    │  ◀───────────────────────────────────────────── │ (downloads)  │
   │               │   SSE /api/v1/events · /history · /deliveries   │              │
   └───────┬───────┘                                                 └──────────────┘
           │ reads <completed>/<publication>/.runner-manifest.json     no outbound
           │ POST /api/v1/scan (correlation_id = transfer)             HTTP. ever.
           ▼
   ┌───────────────┐
   │    cinema     │        runner and cinema never speak.
   │   (plays it)  │
   └───────────────┘
```

Default ports: **runner 6789** · curator 7676 · cinema 32400.

| Path | Owner | Exit |
|---|---|---|
| `/processing/<job>` | runner | a publication, or a terminal failure → disposal (§5), after any salvage obligation settles |
| `/processing/failed/<job>` (`[post] failed_dir`) | runner | retention (`failed_retention_days`) or explicit delete; never gates curator |
| `<completed>/<publication>` | curator, from the moment `.runner-manifest.json` exists | per file, by curator; runner never deletes under `<completed>` |

## Wiring the three together in Docker

**One shared user-defined network, and address everything by container name.**
That is the recommended setup, not merely one that works.

```bash
docker network create media          # once, on the host
```

Then in **each** compose file — nzbd's, monarr's, plurx's — attach the service
and declare the network as external:

```yaml
services:
  nzbd:
    container_name: nzbd             # this name IS the hostname
    # ...the rest of your service...
    networks:
      - media

networks:
  media:
    external: true                   # created above; compose must not own it
```

`docker compose up -d` in each directory. Curator's nzbd client then points at
host `nzbd`, port `6789` — no `host.docker.internal`, no published-port
arithmetic.

**Get the names from Docker, not from memory.** One command answers both "what
hostname does my consumer use" and "are they on the same network":

```bash
docker ps --format '{{.Names}}\t{{.Networks}}'
```

```
plurxd	media
nzbd	media
monarr	media
```

Column 1 is the hostname. Column 2 must contain the shared network for every
container that has to reach another; `bridge` or `host` there means it cannot
be resolved by name from the others.

Read column 1 literally — a container called `plurxd` is not reachable as
`plurx`. Docker's embedded resolver answers a name that does not exist on the
network with `lookup <name> on 127.0.0.11:53: server misbehaving`, which reads
like the far side is broken. It is not: nothing was dialled, and the port in
that message is irrelevant.

Two consequences worth knowing:

**Published ports are irrelevant on this path.** `ports: - "6789:6789"` maps
host→container; container→container traffic goes straight to the *internal*
port and works even with none published. Keep the published one for your own
browser.

**`nzbd` must be reachable from the container doing the reaching.** nzbd never
dials out (see the non-goals below), so it is always the callee here — but
`extra_hosts: ["host.docker.internal:host-gateway"]` on nzbd's own compose
does nothing for anyone else. That setting belongs on the *caller*, which is
why the shared network is the cleaner answer: it is one fact configured once
instead of the same fact configured per-container and easy to get half-right.

## The seams runner has

| § | Seam | Direction | Transport |
|---|---|---|---|
| 1 | Native job API — submit, control, admission lookup | curator → runner | HTTP `/api/v1/jobs`, `/api/v1/request-admissions/{token}` |
| 2 | Event stream — terminals, status, salvage | runner → curator (held open by curator) | SSE `/api/v1/events` |
| 3 | History cursor — the terminal reconciliation feed | curator reads | HTTP `/api/v1/history` |
| 4 | Publication, manifest, reconciliation feed | runner writes on disk, curator reads | `.runner-manifest.json`, HTTP `/api/v1/deliveries` |
| 5 | Failure disposal and the artifact inventory | observation | HTTP `/api/v1/artifacts` |
| 6 | Transfer-id passthrough | curator → runner → curator | job `params` |
| 7 | Status / capacity | curator reads | HTTP `/api/v1/status` |
| 8 | Client registry | observation | HTTP `/api/v1/clients` |
| 9 | Handoff column | observation | HTTP `/api/v1/history` |
| 10 | NZBGet compat shim | Sonarr/Radarr/Lidarr → runner | JSON-RPC / XML-RPC |

**Runner makes no outbound HTTP calls to curator or cinema, and gains none.**
A downloader that reaches out to its consumers can hang on their outages;
everything below is something done *to* runner, or something runner wrote
down while it was being done.

**Auth.** One `Authorization` header: `Bearer <api.token>`, or HTTP Basic
with `api.password`. Enforced only when `api.token` or `api.password` is set
in `nzbd.toml`. `/healthz` is always exempt; `/metrics` is not. Every curator
call carries `X-Nzbd-Client: monarr/<version>`, which names it in the client
registry (§8).

```bash
NZBD=http://127.0.0.1:6789
TOK=<api.token from nzbd.toml>
```

---

## 1. Native job API

| Purpose | Method | Path |
|---|---|---|
| Add (NZB body, or `?url=`) | `POST` | `/api/v1/jobs?url=…&name=…&category=…&priority=…&paused=…&dupe_key=…&dupe_score=…&params=…` |
| Queue | `GET` | `/api/v1/jobs` |
| One job | `GET` | `/api/v1/jobs/{id}` |
| Action (`pause`, `resume`, `delete`, `delete-files`, …) | `POST` | `/api/v1/jobs/{id}/actions/{action}` |
| Retry a hold now | `POST` | `/api/v1/jobs/{id}/retry` |
| Keep the source if it fails | `GET`/`PUT` | `/api/v1/jobs/{id}/failure-keep` (`{"keep": true}`) |
| Admission lookup by transfer | `GET` | `/api/v1/request-admissions/{token}` |
| Status | `GET` | `/api/v1/status` |

**`params`** is a URL-encoded JSON object of string → string. The keys
curator uses:

| Param | Meaning |
|---|---|
| `monarr-transfer` | the transfer token — opaque, compared by exact equality; also the request token that makes `POST /api/v1/jobs?url=` idempotent and keys `/request-admissions/{token}` |
| `monarr-delivery` | `1`: the job is published for curator (a publication + manifest + feed row) |
| `monarr-manual-additional` | `1`: an operator's extra grab; carried through, not interpreted |

Keys starting with `*` are runner's own and are refused.

**Queue rows** (`GET /api/v1/jobs`) carry `status` (the persisted status:
`queued` · `fetching` · `downloading` · `paused` · `post_queued` ·
`post_processing` · `completed` · `failed` · `deleted`), `status_projected`
(the same, except `held` while a job is held on an environmental cause —
§2.3), `transfer`, `control` (§2.3) and `ready`. `completed` alone means the
articles finished downloading, not that a payload is importable: import on
`job_pp_finished{outcome: completed}` (§2.1) or the history row, never on the
queue status.

**`GET /api/v1/request-admissions/{token}`** answers the durable receipt of a
token-scoped admission (it outlives the queue and history rows). Its two
jobs: "did my `POST /jobs` with this token land, and as which job id", and
"what happened to this transfer" after a history continuity loss (§3). It is
never consulted to settle a terminal in normal operation.

| Receipt field | Meaning |
|---|---|
| `protocolVersion`, `requestToken`, `jobId`, `generation`, `runnerInstanceId`, `submissionSha256` | identity of the admission |
| `outcome` | the admission's outcome (the continuity-loss lookup) |
| `terminalOutcome`, `failureStage`, `failureCause` | the terminal's history status string and failure detail |
| `failureReason`, `fetchCause`, `blamesRelease`, `salvage` | the typed terminal (§2.2), from the `Failure:Reason` / `Failure:FetchCause` / `Failure:BlamesRelease` / `Failure:Salvage` params |
| `publicationPath`, `sourceArtifactId`, `sourceGeneration`, `keepSourceOnFailure` | where the bytes went |
| `custodyState`, `sourceCleanupStatus` | **deprecated, removed in M6b** (OpenAPI `deprecated: true`) — runner never holds bytes for curator, and source cleanup gates nothing |

```bash
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/status"        # the probe curator runs
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/request-admissions/<transfer>"
curl -sS "$NZBD/healthz"                                               # -> the literal: ok
```

`/api/v1/status` answering with a `version` field is the whole of "runner is
really there"; curator treats a 200 without `version` as a hard failure.

---

## 2. Event stream

`GET /api/v1/events` is a Server-Sent Events stream. Each engine fact carries
`id: <boot>-<seq>`; a reconnect sends `Last-Event-ID: <boot>-<seq>` and runner
replays from its ring (`RING = 1024` frames), or answers `reset` when the
boot id changed or the cursor fell out of the ring. The stream is never the
only path: the history cursor (§3) and the deliveries feed (§4) recover
everything it can miss, and curator's cursor is owned by the history reader,
never advanced by an SSE frame.

### Event reference

Engine facts — exactly the names `event_json` emits, each with an `id:`
(checked by `integration_doc_names_every_sse_event`):

| Event | Payload | Meaning |
|---|---|---|
| `job_pp_finished` | §2.1 | **the terminal** — once per job, after it is durable, before anything touches the bytes |
| `job_status_changed` | `{job, transfer, status, stage, control}` | every status transition, including into and out of `held` (§2.4) |
| `job_publication_ready` | `{job, transfer, publication}` | a salvage publication of a failed job (§2.2); not a terminal |
| `job_control_changed` | `{job, control}` | every write of the job's hold/control record (§2.3) |
| `job_added` | `{job, name}` | a job entered the queue |
| `job_finished` | `{job, name, status, health}` | the download phase ended; post-processing may follow. **Advisory** — never import on it |
| `job_deleted` | `{job}` | the queue row is gone. **Advisory** — the cancel terminal is `job_pp_finished{outcome: cancelled}` |
| `job_pp_stage` | `{job, name, stage}` | a post-processing stage started (`stage` is a `PostStage` name, below) |
| `job_assigned` | `{job, node}` | cluster: a job was leased to a node |
| `url_fetch_resolved` | `{job}` | a URL job's NZB was fetched and the job resolved |
| `file_finished` | `{job, file, filename, ok}` | one file of a job finished |
| `segment_exhausted` | `{job, file, segment}` | an article ran out of servers |
| `server_blocked` | `{server, seconds}` | a news server was blocked for `seconds` |
| `queue_pause_changed` | `{paused, source}` | the whole queue was paused or resumed |
| `speed_limit_changed` | `{bytes_per_sec}` | the speed limit changed |
| `max_active_downloads_changed` | `{n}` | the concurrency limit changed |

Stream frames — local to one connection, no `id:`, never replayed:

| Frame | Payload | Meaning |
|---|---|---|
| `tick` | `{status, jobs}` | 1 Hz progress snapshot, sent only when it changed |
| `hb` | `{now_unix}` | heartbeat when an idle stream sent nothing for a while |
| `log` | `{entries, dropped}` | new log lines |
| `reset` | `{reason}` | the resume cursor could not be honoured; reconcile from §3 |
| `lagged` | `{skipped}` | this connection fell behind; frames were skipped |

`stage` values (`PostStage`): `par_rename` · `par_verify` · `par_repair` ·
`rar_rename` · `unpack` · `cleanup` · `move` · `post_unpack_rename` ·
`script` · `publication_verify`.

### 2.1 The terminal — `job_pp_finished`

Every job ends in exactly one terminal, and it is durable before it is
announced: runner writes one `job_terminals` row (with any salvage obligation
on the artifact, in the same transaction), appends the history row, stamps
the queue row, and only then emits the event. A crash anywhere in that chain
is replayed at boot and every 30 s, so a consumer sees the event at least
once and every copy carries the same `history_seq`. If the terminal or
history write fails, the job is held `storage_unavailable` on stage
`persistence` and the event goes out when the write lands.

Three producers: the owner at the end of the download (`health`, `io`), the
URL fetcher (`fetch`), the post-processor (the PP reasons and `completed`);
plus the operator delete path, which records `outcome: cancelled` with the
transfer from the same record as its `DELETED` history row.

| Field | Meaning |
|---|---|
| `job`, `name`, `category` | the job |
| `transfer` | `params["monarr-transfer"]`, or null for a job curator did not add |
| `outcome` | `completed` · `failed` · `cancelled` |
| `reason` | `failed` only: `health` · `fetch` · `par` · `unpack` · `script` · `publish` · `io` · `identity` · `hold_expired` (§2.2) |
| `fetch_cause` | `fetch` only: `gone` · `indexer_unavailable` |
| `stage` | `failed` only: a `PostStage` name, or `download` · `health` · `fetch` |
| `message` | `failed` only: human text, at most 2048 characters |
| `blames_release` | whether the consumer should blocklist the release (§2.2) |
| `salvage` | `pending` when runner will assess the failed job's bytes and may publish the usable part (`job_publication_ready`); otherwise `none` |
| `keep_on_failure` | the job's own Keep flag, echoed; it changes what happens to the bytes, never whether the failure is reported |
| `fetched` | `{success_articles, total_articles, bytes}` |
| `publication` | `completed` only: `{path, generation, manifest_digest, files, cross_volume}` (§4); null otherwise |
| `history_seq`, `history_incarnation` | the durable history row; never 0. The history cursor is the pair (§3) |
| `params` | the job's params (including `*Control:v1`) |
| `size_bytes`, `health` | the job's size and download health (per mille) |
| `pp_status` | **deprecated, removed in M6b** — the history status string (`SUCCESS`, `FAILURE/HEALTH`, …); read `outcome` and `reason` |
| `final_dir` | **deprecated, removed in M6b** — equals `publication.path` on success; read `publication` |

```json
{"job": 2333, "name": "Dry.Wind.2020…", "category": "movies",
 "transfer": "t-591-a3f9c1…", "outcome": "failed", "reason": "unpack",
 "fetch_cause": null, "stage": "unpack", "message": "…", "blames_release": true,
 "salvage": "pending", "keep_on_failure": false,
 "fetched": {"success_articles": 1204, "total_articles": 1204, "bytes": 8123456789},
 "publication": null, "pp_status": "UNPACK_FAILURE", "final_dir": null,
 "size_bytes": 8123456789, "health": 1000, "params": [["monarr-transfer", "t-591-a3f9c1…"]],
 "history_seq": 4412, "history_incarnation": "7f3a…"}
```

### 2.2 Failure reasons

| `reason` | `stage` | Set when (by) | `blames_release` | `salvage` may be `pending` | Bytes after (§5) |
|---|---|---|---|---|---|
| `health` | `health` | articles missing below critical (owner) | yes | yes | `failure_action`, after salvage settles |
| `fetch` | `fetch` | NZB/URL fetch failed (URL fetcher); `fetch_cause` `gone` (the indexer answered 404/410/403 or another non-retried status for this item) or `indexer_unavailable` (retries exhausted on 429/503, timeouts, transport) | only for `gone` | no | nothing to dispose |
| `par` | `par_verify`/`par_repair` | repair impossible (PP) | yes | yes | `failure_action`, after salvage settles |
| `unpack` | `unpack` | the extractor failed on the bytes (PP) | yes | yes | `failure_action`, after salvage settles |
| `script` | `script` | post script exited non-zero (PP) | no | yes | `failure_action` |
| `publish` | `move`/`publication_verify` | nothing passes selection (PP) | no | yes | **park, always** |
| `io` | any | a write or filesystem step failed in a way retrying cannot fix (owner or PP) | no | yes | **park, always** |
| `identity` | any | the release's evidence contradicts itself (PP) | yes | no | **park, always** |
| `hold_expired` | the hold's stage | an environmental hold passed its deadline (§2.3) | no | yes | **park, always** |

History status strings, per reason: `FAILURE/HEALTH` · `FAILURE/FETCH` ·
`PAR_FAILURE` · `UNPACK_FAILURE` · `SCRIPT_FAILURE` · `FAILURE/PUBLISH` ·
`FAILURE/IO` · `FAILURE/IDENTITY` · `FAILURE/HOLD_EXPIRED`; success is
`SUCCESS…`, an operator delete `DELETED`.

**Which failures hold instead.** An error is an environmental hold, not a
failure, only when its errno says the environment will heal: `ENOSPC` →
`capacity`, `EDQUOT` → `quota`, a dropped volume (`EIO`, `ENXIO`, `EAGAIN`,
`ENODEV`, `ENOLINK`, `ECONNABORTED`, `ECONNRESET`, `ENOTCONN`, `ETIMEDOUT`,
`ECONNREFUSED`, `EHOSTDOWN`, `EHOSTUNREACH`, `ENETDOWN`, `ENETUNREACH`,
`ESTALE`, `EREMOTEIO`, or SQLite `SQLITE_IOERR`) or a volume that turned
read-only (`EROFS` — ext4 `errors=remount-ro`, an export flipped read-only;
both heal on remount) → `storage_unavailable` (SQLite `SQLITE_FULL` →
`capacity`); a missing tool → `missing_tool`; an unavailable engine →
`engine_unavailable`; a wrong or missing archive password →
`password_required`; a replaced or absent mount → `mount_unavailable`. The
errno is read from the `io::Error` (`raw_os_error()`, or the error it wraps)
wherever one exists, and from its kind when only the kind survived; only a
subprocess's stderr is read as text (`nzbd_engine::stderr_hold_cause`).
Everything else is the job's terminal failure — `failed/io`, parked, for a
filesystem error. That deliberately includes `EACCES`/`EPERM`/`ENOENT` (a
retry repeats them) and `EMFILE`/`ENFILE`/`ENOMEM`: those are a fault in the
daemon or the host, not the volume, and a hold would hide it.

**Salvage** is a second fact, not part of the terminal: with `salvage:
pending`, runner assesses the failed job's bytes after the terminal and
announces a usable partial with `job_publication_ready {job, transfer,
publication}` (also a `/api/v1/deliveries` row with `kind: "salvage"`), or
settles the obligation as nothing usable. The job stays failed for re-grab
purposes.

### 2.3 Holds

A held job is waiting on something environmental: `storage_unavailable` ·
`capacity` · `quota` · `allocation` · `mount_unavailable` · `missing_tool` ·
`engine_unavailable` · `password_required`. It is visible as
`status_projected: "held"` on `/api/v1/jobs` and `status: "held"` on
`job_status_changed`; the persisted `status` stays `paused` with
`control.lifecycle == "held"` until M6b. The NZBGet-compat shim shows
`paused`.

`control` (`*Control:v1`): `version`, `revision`, `lifecycle`, `cause`,
`stage`, `retry_policy` (`scheduled_retry` · `resume_same_job` ·
`reobserve_mount`), `message`, `instance`, `previous_status`,
`manual_pause`, `retry_attempt`, `next_retry_at_unix`, `blocked_since_unix`,
`released_at_unix`, `deadline_in_s`, `deadline_unix`.

- **One retry clock.** A timed hold is retried at 30 s, 1 m, 2 m, then every
  5 m (`HOLD_RETRY_DELAYS_SECONDS`, jittered), by the owner; nothing else
  schedules a held job.
- **A deadline runner enforces.** `[post] hold_deadline_secs` (default
  `86400`; `password_hold_deadline_secs`, default `604800`) after the episode
  began, runner ends the job `failed/hold_expired`, bytes parked. Curator
  never authors that terminal. `deadline_in_s` is the time remaining as of
  the control's last write (hosts share no clock); `deadline_unix` is for
  display.
- **The newest cause wins**: a new hold on a held job replaces the control
  (revision + 1).
- **`manual_pause`**: an operator who pauses a held job suspends its
  automatic recovery until they resume it; the deadline keeps running.

### 2.4 Status and progress

`job_status_changed {job, transfer, status, stage, control}` fires on every
transition of the projected status, including each `PostStage`; `tick` is
the 1 Hz progress snapshot. A consumer does not need to diff ticks to find
transitions.

```bash
curl -sSN -H "Authorization: Bearer $TOK" -H "Accept: text/event-stream" \
     -H "X-Nzbd-Client: curl/manual" "$NZBD/api/v1/events"
# pause and resume the queue in the UI: queue_pause_changed appears
```

---

## 3. History cursor — the terminal reconciliation feed

`GET /api/v1/history?since_seq=<n>&limit=<n>` (default `limit` 200, at most
10000; or `?limit=&offset=` for the page view) answers:

| Key | Meaning |
|---|---|
| `entries` | history rows, ascending by `id` on the cursor shape |
| `total`, `offset`, `limit`, `sync` | paging and portable-log sync state |
| `incarnation` | the history index's identity, minted whenever the index is rebuilt from the portable log (which reassigns ids) |
| `head_seq` | the newest row id |
| `retained_min_seq` | the oldest row id still kept |

The cursor is **`(incarnation, seq)`**. A consumer whose stored incarnation
differs, or whose cursor is below `retained_min_seq` or above `head_seq`, has
lost continuity: it reconciles every outstanding transfer through
`/api/v1/request-admissions/{token}` (§1), then resumes from
`retained_min_seq`. Ids have gaps; advance to the last id of a committed
page.

---

## 4. Publication, manifest and the reconciliation feed

A publication is a directory `<completed>/<name>.job-<id>.runner-<gen>/`
holding **only the selected files** and `.runner-manifest.json`. The
manifest is written **last** and is the commit marker: a directory without
it is not a publication — curator ignores it and runner's restart
reconciliation finishes or unwinds it. From the moment the manifest exists
the directory is curator's; runner never deletes anything under
`<completed>`.

**How the files get there.** Before the first file moves, runner journals
the move intent — generation, target directory, and for every selected file
its source path, destination, inode/device/size/mtime identity and its
digest if one is already journaled — in the `relocation_intents` table, in
the same transaction as the relocation operation. Then, file by file:

- **Same volume:** `renameat2(RENAME_NOREPLACE)`, falling back to
  `linkat` + `unlink` on mounts that refuse it (NFS, FUSE — the PR #247
  chain). A file without a journaled digest gets exactly one read pass, to
  compute it. Nothing is copied.
- **Different volume:** one streaming copy per file into `<dst>.part` that
  computes the SHA-256 as it writes, `fsync`, then an exclusive rename.
- One exception: a selected file that is still an input of a PAR set needed
  to repair the rest of the source (`repairDependency` in its file outcome)
  is *linked* into the publication on the same volume instead of renamed, so
  its source name stays and the set remains repairable after curator removes
  the publication.
- Each file is marked moved in the journal as it lands. Then
  `.runner-manifest.json` is written (`.part`, `fsync`, exclusive rename).

The processing directory keeps only the unselected siblings (PAR sets,
samples, consumed archives, NFOs); runner disposes of it once the delivery
row exists. The delivery record reuses the journaled digests — the old
private staging tree and its four-to-five hash passes are gone.

In a cluster, the leader lands the worker's sealed payload in its own
publication directory and writes the manifest there last, through the same
writer; its `generation` is the cluster result id, `cross_volume` is `true`
(the payload was copied) and its digest is the cluster delivery row's
`manifestDigest`.

**`.runner-manifest.json`:**

```json
{
  "generation": "5f0c6b1e9a2d4c8e8b7a6d5c4b3a2918",
  "job": 2333,
  "transfer": "t-2333-00112233445566778899aabbccddeeff",
  "created_at": 1794076138,
  "cross_volume": false,
  "files": [{"path": "Movie.2020.mkv", "bytes": 8123456789, "sha256": "…"}],
  "digest": "<bare lowercase hex>"
}
```

`files` is in bytewise path order. `digest` is the deployed
`runner-manifest-v1` algorithm, unchanged: SHA-256 over
`runner-manifest-v1\n` and then, per file, `<len(path)>:<path>\n<bytes>\n<sha256>\n`
— the same value as `Delivery:ManifestDigest`, the delivery row's
`manifestDigest` and `job_pp_finished.publication.manifest_digest`.
`crates/nzbd-state/tests/fixtures/runner_manifest_v1.json` holds a manifest
and the digest curator's own `ports.ManifestDigest` computes for it; the test
`manifest_digest_matches_curator_fixture` keeps the two languages equal.
Curator re-verifies the digest once, at import, only when `cross_volume` is
true.

**Crash safety.** On every start (and on each 30 s tick, and before a
post-processing retry) runner reconciles each intent that is still open:

- the manifest exists → the publication committed; record it;
- every selected file is accounted for, unchanged, either in the target
  (same object, or same size and digest across volumes) or in the source
  (same identity, and same digest when one was journaled) → **finish**:
  move the rest, write the manifest;
- any file missing from both sides, or changed → **unwind**: move the moved
  files back by identity, remove the target directory, and fail the job
  `failed/io` (parked, no blame) on its next publication attempt.

Exactly one of finish or unwind runs, and the decision is logged with the
per-file evidence. A restart that had already decided to finish and stopped
again re-locates every file the journal does not yet record as moved (a
rename can land before its journal update), so finishing is idempotent too.
While an intent is open, neither the source nor the target can be disposed
of, deleted or retired.

**What identity proves.** A file with a journaled digest is checked by its
bytes. A same-volume file the journal has no digest for yet (the crash came
before its one read pass) is matched by **inode, device, size, mtime and
ctime only** — the same check a rename preserves. That detects replacement
and ordinary writes (both change ctime) but not bit rot under unchanged
metadata; such a file's digest is computed as it is published, and curator
re-verifies only cross-volume publications. Legacy records without a ctime
are matched on inode, size and mtime.

**An unwind never deletes the only copy.** A cross-volume copy is removed
only while the source still holds the journaled original, and a linked PAR
repair input loses its publication name only while the source name is the
same inode. Otherwise the unwind stops for review (the operation's error is
mirrored onto the artifact, so Files and Attention show it).

**Operator exit.** A move stuck in review or on its retry ladder can be
abandoned from the Files detail (*Abandon move and retain original*), i.e.
`POST /api/v1/artifact-operations/<op>/abandon-relocation` with the
artifact's `revision` and `generation`. Runner unwinds the move — the source
is complete and stays runner's — and the job's next publication attempt fails
`failed/io`, parking the source. If the move cannot be put back safely,
abandoning closes it without deleting anything: the files stay where they are
and the publication directory (no manifest, so never curator's) is registered
in the inventory for an explicit disposition.

**The reconciliation feed.** `GET /api/v1/deliveries?after=<cursor>&limit=<1..100>`
(default 50) is a read-only feed of immutable publication rows in sequence
order: `{protocolVersion, deliveries, nextCursor, cursorRule}`. Each row
carries `sequence`, `jobId`, `requestToken` (the transfer),
`publicationPath`, `publicationGeneration`, `manifestDigest`, `readyAt`,
`kind` (`completed` | `salvage`), `terminalOutcome`, `failureStage`,
`failureCause`, `keepSourceOnFailure`, `fileOutcomes`, `sourceCleanup`
(runner's own retirement of its processing source — informational, it gates
nothing), and the manifest body:
`files`, plus `manifest` (the `.runner-manifest.json` as written, for
publications since 0.4.0). `GET /api/v1/deliveries/{id}` answers one row.
Curator reads the feed on the poll to catch a `completed` or
`job_publication_ready` it missed; every row is returned.

Deprecated, **removed in M6b**: the `includeAccepted` query parameter
(accepted and ignored — there is no acceptance). Not on the wire but kept for
rollback until M6b: the stored row's v1 `status: "ready"` and the
`deliveries.accepted_at` / `consumer_receipt_id` columns, so an M1a build
rolled back onto this state can still read it. `POST
/api/v1/deliveries/{id}/accept` answers `410 Gone`.

```bash
# no open intents after a restart (post-deploy check)
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/artifacts?filter=attention" \
  | python3 -m json.tool | grep -c '"state": "transitioning"'
# a publication's manifest and its feed row agree
jq -r .digest "<completed>/<publication>/.runner-manifest.json"
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/deliveries?after=0&limit=100" \
  | jq -r '.deliveries[] | [.publicationPath, .manifestDigest, .kind] | @tsv'
```

---

## 5. Failure disposal and the artifact inventory

After the terminal, runner disposes of a failed job's processing source per
`[post] failure_action` (`delete` by default, `park`, `none`) — overridden by
the job's Keep flag, and by the park-always reasons (§2.2) — and only once
any salvage obligation has settled. Disposal is owned by the artifact record
and gated by the `job_terminals` row, not the queue row; it runs on the
artifact operation's own retry ladder (`Operation.next_retry`: 30 s, 1 m,
2 m, then every 5 m) and, in a cluster, under the operation's lease, so one
node disposes. **Nothing downstream waits on it.**

`GET /api/v1/artifacts` (and `/api/v1/artifacts/{id}`) rows for a failed
source carry `failure_reason` (the same wire string as
`job_pp_finished.reason`), `salvage` (`pending` · `published` ·
`settled_none`) and the operation's error; the queue row's
`failed_cleanup.attempts` counts disposal attempts. Parked sources follow
`failed_retention_days`; Files → Attention shows a stuck operation. Details
in [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md).

---

## 6. Transfer-id passthrough — one id from grab to playable

Curator mints the transfer token at grab time and passes it as the
`monarr-transfer` job param. It is **opaque**: runner compares it by exact
equality and never parses it. Runner stores it on the job, echoes it as
`transfer` on every fact (`job_pp_finished`, `job_status_changed`,
`job_publication_ready`, the queue row), writes it into the manifest
(`transfer`) and the feed row (`requestToken`), keys the admission receipt by
it, and re-attaches it when a history entry is requeued. Curator sends the
same value to cinema as `correlation_id`. A fact whose transfer does not
match curator's row is dropped by curator.

```bash
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/history?limit=5" \
  | grep -o 'monarr-transfer[^]]*'
```

---

## 7. Status / capacity — "up" is not the same as "working"

**What it does.** monarr's health check reads `/api/v1/status` on its own
timer and turns four booleans into plain-language problems. A downloader that
answers HTTP while its disk is full is *up* and *not working*, and the
Connections panel is supposed to tell those apart.

**Wire.** monarr reads six fields and ignores the rest: `version`,
`download_paused`, `disk_low`, `quota_reached`, `blocked_servers`,
`health_abort`.

**Where you see it.** **monarr → System → Connections**, state `degraded`,
with the detail being one or more of these literal strings, joined by `; `:

- `the destination disk is low on space`
- `the download quota is used up`
- `N news server(s) are blocked`
- `the queue is paused`

The same text appears in **monarr → System → Health** as the check
`client:<name>:capacity`, worded `<name> is up but not downloading: …`.

**How to verify.**

```bash
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/status" \
  | python3 -m json.tool | grep -E 'version|paused|disk_low|quota|blocked'
```

**How to read it.** `health_abort` is reported by nzbd and **deliberately not
surfaced by monarr**. It reflects `[post] health_action` being `park` or
`delete` — an operator policy that is on by default and correct to leave on.
Surfacing it produced a red badge that could never be cleared, which trains
you to ignore the panel. A signal that is always on is not a signal.

---

## 8. Client registry — who is talking to nzbd right now

**What it does.** nzbd keeps an in-memory registry of every API caller: name,
first seen, last seen, call count, last method, which API (native vs
nzbget-compat), and how many event streams it holds open. This is nzbd's
answer to *"is monarr actually connected, or have I been staring at a stale
config?"*

**Where you see it.** **nzbd → History tab**, the strip above the table.
Empty state: the chip reads `no API clients yet`, with the tooltip *"a
connected Sonarr/Radarr polls every ~30 s; a subscriber holds /api/v1/events
open"*. Otherwise one chip per client, reading
`<label> · <how> · <last seen>`:

| Chip reads | Means |
|---|---|
| `subscribed` / `subscribed ×N` | N event streams open — push is working |
| `polling` | called within the last 120 s |
| `quiet` | seen before, nothing lately |

Hover for the raw agent string, then
`<calls> calls since <time> — last: <method> · native API`, plus
`· receiving events live` when a stream is open.

The label is not the raw User-Agent. Anything that is not pretending to be
Mozilla is already a product token — `monarr/0.9.0`, `Sonarr/4.0.0` — and is
shown verbatim; a browser is collapsed to `Chrome 120 (macOS)` rather than
dumped in full, because a chip is a glance. The tooltip always carries the
unabridged agent.

**How to verify.**

```bash
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/clients"
```

**How to read it.** Entries are pruned after `CLIENT_TTL_SECS = 300` of
silence — **except** clients holding an open subscription, which are exempt.
So a monarr in push mode that has gone completely silent still shows, which is
what you want: the chip disappearing means the process is gone, not merely
idle. The registry is in-memory and resets on restart; an empty strip right
after a restart is not evidence of anything.

---

## 9. Handoff column — did the consumer actually take the files?

**What it does.** nzbd records, per history entry, whether a client has seen
it and whether it removed it after importing. This closes the loop from the
downloader's side: the files finished *and someone collected them*.

**Where you see it.** **nzbd → History tab**, the `Handoff` column:

| Text | Means |
|---|---|
| `✓ imported by <who> <ago>` | the client deleted the entry after importing |
| `seen by <who> <ago> ×<n>` | listed in a consumer's history poll n times; import not yet observed |
| `⏳ awaiting pickup <ago>` | no consumer has polled history since this finished |
| `hidden` | you hid it from clients |

**A consumer, not a browser.** Opening this tab does not count: a history read
records the pull only when the caller sends `X-Nzbd-Client`, or a User-Agent
that is a product token rather than a browser's `Mozilla/…`. If the browser
counted, every row would flip to `seen` the moment you looked at the page, and
the column would be answering a different question than the one it asks.

> **Fixed 2026-07.** Until then only the nzbget-compat `history` RPC recorded
> the pull — the native `GET /api/v1/history` did not. A native consumer
> polling every 30 seconds therefore left every finished job reading
> `⏳ awaiting pickup` forever, which inverts the meaning of the one column
> that answers "did my *arr take these?". If you are on an older build, read
> this column as compat-clients-only.

**How to verify.**

```bash
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/history?limit=20" \
  | python3 -m json.tool | grep -E 'picked_up_by|seen_count|last_seen_at_unix'
```

**How to read it.** `⏳ awaiting pickup` climbing on a finished job is the
single clearest sign the *consumer* is down, not nzbd — the download worked,
nobody came for it. It will resolve itself when monarr returns; nzbd does not
need to be told. Persistent `seen ×n` with no `✓` means the consumer is
listing the entry and refusing to import it — look at monarr's Activity trace,
not at nzbd. That is the more interesting failure of the two, and the one this
column exists to make visible: a downloader can only report that it finished,
never that the files were used.

---

## 10. NZBGet compat shim — the other consumers

**What it does.** Sonarr, Radarr and Lidarr connect to nzbd as an "NZBGet"
download client with no changes: JSON-RPC 1.1 (`append`, `history`,
`editqueue`, the `*Lo/*Hi/*MB` triplets), XML-RPC with `system.multicall`, and
NZBGet's extension-script protocol byte-for-byte. Full guide in
[USAGE.md](USAGE.md).

**Where you see it.** Same client strip as §8 — the tooltip reads
`· nzbget-compat API` instead of `· native API`, which is how you tell at a
glance whether a consumer is on the rich path or the compatibility path.

**How to read it.** Compat clients get no event stream, no job params and no
capacity reporting. If a consumer shows `nzbget-compat API` when you expected
`native API`, it is configured as an NZBGet client rather than an nzbd one,
and the transfer id (§6) is not flowing.

---

## Metrics

`GET /metrics` (Prometheus text format, **requires auth**). Integration-
relevant names:

| Metric | Type | Reads |
|---|---|---|
| `nzbd_events_emitted_total{event="…"}` | counter | engine events published, by name (§2) |
| `nzbd_sse_clients` | gauge | event streams open right now |

`nzbd_sse_clients` at 0 while curator is configured for push means the
stream is not established — check §8 first.

---

## Compatibility

| Runner | Curator | Works because |
|---|---|---|
| this build | curator M2+ | the contract |
| this build | curator before M2 | `pp_status`/`final_dir` still on the event, `accept` refused with `410` (an old curator's ack retries but import completes), `/request-admissions` still answers. Runner M3 must not reach a node before curator M2 is deployed there |
| M6b | curator before M2 | **not supported** — M6b merges only after every node runs curator M2 and runner M1a/M3 |

## What runner deliberately does not do

- **No outbound HTTP.** Runner never calls curator, cinema, or anything else.
- **No deletion under `<completed>`.** Runner publishes there and never
  removes anything there; disposal and source retirement operate on
  `/processing` only.
- **No acknowledgement.** A publication is curator's the moment its manifest
  exists.
- **No knowledge of libraries or titles.** A job param is an opaque string;
  no seam matches titles.

## Keeping this honest

Every literal in this document — event names, field names, status strings,
config keys — was read from the code on the branch that ships it, and the
event tables are checked against `event_json` by a test. When a seam changes,
this file changes in the same commit.
