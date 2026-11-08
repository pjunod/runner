# Integration — nzbd's seams with monarr and plurx, and how to prove they work

> **Being replaced by [PIPELINE_CONTRACT.md](PIPELINE_CONTRACT.md) — 2026-11-07.** The runner ↔ curator ↔ cinema seam is being rewritten as contract v2: one typed terminal `job_pp_finished` per job emitted before any disposal, holds only for environmental causes with a runner-enforced deadline, and no delivery acknowledgement. Where this page and the contract disagree, the contract describes the target; this page describes what is deployed until each milestone ships.

Companion to [INTEGRATION_PLAN.md](INTEGRATION_PLAN.md) (the event/cursor
contract and how it was built) — this is *what each seam does, where you look
at it, and the exact command that proves it*. For connecting the classic *arr
apps over the NZBGet-compatible shim, see
[USAGE.md](USAGE.md); for every config key, [CONFIGURATION.md](CONFIGURATION.md).

nzbd is the **first stage** of a three-application pipeline. It downloads and
post-processes; it never decides what to download and never touches the
library. Everything it knows about the other two applications, it learned by
being called.

## The pipeline

```
   ┌───────────────┐     grab (params: monarr-transfer)     ┌──────────────┐
   │               │  ────────────────────────────────────▶ │              │
   │    monarr     │                                        │     nzbd     │
   │  (decides)    │  ◀──────────────────────────────────── │ (downloads)  │
   │               │   SSE /api/v1/events  ·  30s poll      │              │
   └───────┬───────┘                                        └──────────────┘
           │                                                  no outbound
           │ POST /api/v1/scan (correlation_id = transfer)     HTTP. ever.
           ▼
   ┌───────────────┐
   │     plurx     │        nzbd and plurx never speak.
   │   (plays it)  │        There is no seam between them, by design.
   └───────────────┘
```

Default ports: **nzbd 6789** · monarr 7676 · plurx 32400.

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

`docker compose up -d` in each directory. Monarr's nzbd client then points at
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

## The seams nzbd has

| # | Seam | Direction | Transport | Who starts it |
|---|---|---|---|---|
| 1 | Native job API | inbound | HTTP `/api/v1/*` | monarr |
| 2 | Event stream | inbound (held open) | SSE `/api/v1/events` | monarr |
| 3 | Transfer-id passthrough | inbound → echoed back | job `params` | monarr |
| 4 | Status / capacity | inbound | HTTP `/api/v1/status` | monarr |
| 5 | Client registry | observation | HTTP `/api/v1/clients` | you |
| 6 | Handoff column | observation | HTTP `/api/v1/history` | you |
| 7 | NZBGet compat shim | inbound | JSON-RPC / XML-RPC | Sonarr/Radarr/Lidarr |

**nzbd makes no outbound HTTP calls to monarr or plurx, and gains none.**
That is a standing guardrail, not an omission. A downloader that reaches out
to its consumers is a downloader that can hang on their outages, and nzbd's
job is to keep the disk moving. Everything below is either something done
*to* nzbd, or something nzbd wrote down while it was being done.

---

## 1. Native job API — "download this, and let me manage it"

**What it does.** monarr's `nzbd (native)` download client drives the queue
over nzbd's own REST API rather than the NZBGet-compatible shim. Same
downloads, richer surface: job params (§3), a real event stream (§2), and a
status endpoint monarr reads for capacity (§4).

**Wire.** Base URL is the client's configured URL, trailing slash trimmed.

| Purpose | Method | Path |
|---|---|---|
| Add | `POST` | `/api/v1/jobs?url=…&category=…&params=…` |
| Queue | `GET` | `/api/v1/jobs` |
| History | `GET` | `/api/v1/history?limit=100` |
| Remove from queue | `POST` | `/api/v1/jobs/{id}/actions/delete`, `…/delete-files` |
| Remove from history | `POST` | `/api/v1/history/{id}/actions/delete`, `…/delete-files` |
| Status | `GET` | `/api/v1/status` |

**Auth.** One `Authorization` header, two shapes. A bearer token
(`Authorization: Bearer <api.token>`) when monarr's client has a password and
no username; HTTP Basic otherwise. Auth is enforced **only** when `api.token`
or `api.password` is set in `nzbd.toml` — with neither, every endpoint is
open. `/healthz` is always exempt; **`/metrics` is not.**

**Identity.** Every call carries `X-Nzbd-Client: monarr/<version>`. nzbd
prefers that header over the User-Agent when naming a client (§5), which is
why monarr shows up by name rather than as `Go-http-client/1.1`.

**Where you see it.**

- **nzbd → History tab → the client strip** above the table. A working monarr
  is a chip reading `monarr/<version> · polling · 12s ago`, or
  `· subscribed · 3s ago` when push is on.
- **monarr → Settings → Download clients**, per-row **Test** button →
  `✓ reachable` or `✕ <message>`.
- **monarr → System → Connections**, row kind `download client`, state
  `polling` or `live`.

**How to verify.**

```bash
NZBD=http://127.0.0.1:6789
TOK=<api.token from nzbd.toml>          # or use  -u nzbd:<api.password>

curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/status"   # the probe monarr runs
curl -sS "$NZBD/healthz"                                          # -> the literal: ok
```

> **Extractors lie, and one of them shipped in this image.** `unrar-free` —
> the GPL clone Debian installs for `unrar` — cannot handle multi-volume
> archives and does not say so: given volume 1 of a five-volume set with every
> volume present, it extracts volume 1, prints `All OK`, and exits 0. Nearly
> every release above a few hundred megabytes is multi-volume, so the result
> was a 48 GiB film delivered as its first 500 MiB and reported as a completed
> download. Fixed 2026-07: the image no longer installs it, the RAR path falls
> back to 7-Zip when the configured extractor under-delivers, and the unpack
> is checked against the volume set's own size before anything is committed.
> If you built an older image, `docker compose up -d --build` is the fix.

**How to read it.** `/api/v1/status` answering with a `version` field is the
whole of "nzbd is really there" — monarr treats a 200 with no `version` as a
hard failure (`nzbd: … answered, but not like nzbd`), because a reverse proxy
serving an index page will otherwise pass a naive reachability test. A 401
means the token is wrong; a connection refused means the URL or port is.

---

## 2. Event stream — the reason an import is instant instead of 30s late

**What it does.** `GET /api/v1/events` is a Server-Sent Events stream. monarr
holds it open when the client's **Live updates** checkbox is on
(`download_clients.mode = 'push'`), and imports the moment post-processing
finishes instead of on the next poll.

**Wire.** Each engine frame carries `id: <boot>-<seq>`; a reconnect sends
`Last-Event-ID: <boot>-<seq>` and nzbd replays from the `RING = 1024` buffer,
or answers `reset` if the boot id changed or the cursor fell out of the ring.

Event names nzbd emits: `job_added` · `job_finished` · `job_deleted` ·
`file_finished` · `segment_exhausted` · `server_blocked` ·
`queue_pause_changed` · `speed_limit_changed` · `job_assigned` ·
`job_pp_stage` · `job_pp_finished` · `job_status_changed` ·
`job_publication_ready`, plus the stream-level frames `reset` · `lagged` ·
`tick` · `hb` · `log`.

monarr acts on exactly six: `job_pp_stage` (step trace), `job_pp_finished`
(completed or failed, carrying `final_dir`), `job_deleted` (failed, "removed
in nzbd"), `reset` and `lagged` (both → full queue refresh), and `tick` (one
progress update per job). **`job_finished` is deliberately ignored** — it
fires when the download ends, before post-processing, and importing then would
import an unrepaired, unextracted directory.

For NZB and URL jobs, the queue's `status: "completed"` establishes only
**article download complete**. The row keeps that status after post-processing,
so the status alone never establishes payload readiness. `ready` is the
authoritative boundary. A native consumer may import only after `ready: true`,
using `final_dir` from `job_pp_finished` or the durable history row; a queue row
does not carry an import path. Failed post-processing is terminal (`pp_done:
true`) but never ready; its history row may still name a parked directory, which
is evidence for recovery, not an importable payload.

### Terminal events (pipeline contract v2, runner 0.3.0)

Every job ends in exactly one terminal, and the terminal is durable before it
is announced. Runner writes one `job_terminals` row (with any salvage
obligation, in the same transaction), appends the history row, stamps the
queue row, and only then emits `job_pp_finished`. A crash anywhere in that
chain is replayed at boot and every 30 s, so a consumer sees the event at
least once and every copy carries the same `history_seq`. Operator delete is a
terminal too: `job_pp_finished{outcome:"cancelled"}` with the transfer, from
the same record as its `DELETED` history row; `job_deleted` is still sent but
is advisory.

`job_pp_finished` fields:

| Field | Meaning |
|---|---|
| `job`, `name`, `category` | as before |
| `transfer` | the `monarr-transfer` param, or null |
| `outcome` | `completed` · `failed` · `cancelled` |
| `reason` | `failed` only: `health` · `fetch` · `par` · `unpack` · `script` · `publish` · `io` · `identity` · `hold_expired` |
| `fetch_cause` | `fetch` only: `gone` (404/410 — blames the release) or `indexer_unavailable` |
| `stage` | `failed` only: a post stage name, or `download` · `health` · `fetch` |
| `message` | `failed` only: human text, at most 2048 characters |
| `blames_release` | whether the consumer should blocklist the release: true for `health` · `par` · `unpack` · `identity` and for `fetch{gone}` |
| `salvage` | `pending` when Runner will assess the failed job's bytes and may publish the usable part later; otherwise `none` |
| `keep_on_failure` | the job asked for its source to be kept; retention only, it never changes the terminal |
| `fetched` | `{success_articles, total_articles, bytes}` |
| `publication` | `completed` only: `{path, generation, manifest_digest, files, cross_volume}` |
| `history_seq`, `history_incarnation` | the durable history row; the history cursor is the pair |
| `pp_status`, `final_dir`, `size_bytes`, `health`, `params` | legacy names kept for old consumers (removed in M6); `params` carries `*Control:v1` |

`job_status_changed{job, transfer, status, stage, control}` fires on every
status transition, including into and out of `held`. `status` is the
projected native status: a job held on an environmental cause reads `held`
(the persisted status stays `Paused` until M6), and `control.deadline_in_s` /
`deadline_unix` say when the hold ends on its own. The queue listing carries
the same projection as `status_projected`, plus `transfer`. `deadline_in_s` is
the time remaining as of the control's last write, not a live countdown; use
`deadline_unix` for one.

`GET /api/v1/artifacts` (and `/artifacts/{id}`) rows for a failed source
carry `failure_reason` (the same wire string as `job_pp_finished.reason`, set
when the terminal is recorded) and `salvage` (`pending`, `published` or
`settled_none`); `failure_assessment.reason` is the assessment's own text.
The Files tab reads these to say why a source is parked.

`job_publication_ready{job, transfer, publication}` announces a salvage
publication of a failed job. It is not a terminal; the job stays failed. The
matching `/api/v1/deliveries` row has `kind: "salvage"` (completed jobs are
`kind: "completed"`), with the same manifest body. `POST …/accept` is a 200
no-op since 0.3.0 — see [CURATOR_DELIVERY.md](CURATOR_DELIVERY.md).

`GET /api/v1/history` now answers `incarnation`, `head_seq` and
`retained_min_seq` beside the rows. A consumer that stores `since_seq` must
store the incarnation with it: a new incarnation means the index was rebuilt
and the stored cursor no longer names the same rows, so rescan from
`retained_min_seq`.

**Where you see it.**

- **nzbd → History tab → client strip**: the chip reads `subscribed` (or
  `subscribed ×N`) and gets the `live` class. Its tooltip ends
  `· receiving events live`.
- **monarr → System → Connections**: state `live`, tooltip *"A push stream is
  open — completions arrive the moment they happen"*. If push is configured
  but the stream is down, the state falls back to `polling` with the detail
  `push is configured but the stream is not connected`.
- **monarr → Settings → Download clients**: the per-row `live` / `poll` pill.
  That pill is an **echo of stored config**, not a probe — it says what you
  asked for, Connections says what you got.

**How to verify.**

```bash
curl -sSN -H "Authorization: Bearer $TOK" \
     -H "Accept: text/event-stream" \
     -H "X-Nzbd-Client: curl/manual" \
     "$NZBD/api/v1/events"
# then, in the UI, pause/resume the queue — you should see queue_pause_changed
```

Your `curl/manual` chip will appear in the client strip as `subscribed` within
a second, which also proves the registry (§5) is live.

**How to read it.** The stream is **never load-bearing**. The 30 s
`queue.refresh` poll runs in push mode too, so a dead stream costs you latency
and nothing else. monarr reconnects forever with `1s → 60s` jittered backoff
and injects a synthetic reset on every reconnect, because a gap in the stream
is a gap in knowledge regardless of why it happened. `live` going to `polling`
is a performance regression, not an outage.

---

## 3. Transfer-id passthrough — one id from grab to playable

**What it does.** monarr mints `t-<downloadID>-<6 hex>` at grab time and hands
it to nzbd as a job parameter. nzbd stores it, returns it on history entries
and in the `job_pp_finished` payload, and **re-attaches it when a history
entry is requeued** — the id that names a transfer has to survive an undo.

**Wire.** `POST /api/v1/jobs?…&params={"monarr-transfer":"t-42-a3f9c1"}` —
a URL-encoded JSON object of string→string. `parse_add_params` rejects
`*`-prefixed keys; that namespace is nzbd's own (`*URL`). The value lands in
`HistoryEntry.params`.

The same id continues past nzbd: monarr writes it to `downloads.transfer` and
sends it to plurx as `correlation_id` on `POST /api/v1/scan`. One grep across
three applications reconstructs a transfer.

**Where you see it.** nzbd's UI does not render `params`. Read it from the
API, or from monarr's **Activity** page, where the handoff detail shows the
step trace and the id.

**How to verify.**

```bash
curl -sS -H "Authorization: Bearer $TOK" "$NZBD/api/v1/history?limit=5" \
  | grep -o 'monarr-transfer[^]]*'
```

**How to read it.** An empty `params` on a monarr-grabbed job means monarr
fell back to the untagged `Add` path — the id exists in monarr's database but
does not follow the job through nzbd, so a failure mid-pipeline is harder to
join up. It is a diagnostic loss, not a functional one; the download still
completes and still imports.

---

## 4. Status / capacity — "up" is not the same as "working"

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

## 5. Client registry — who is talking to nzbd right now

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

## 6. Handoff column — did the consumer actually take the files?

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

## 7. NZBGet compat shim — the other consumers

**What it does.** Sonarr, Radarr and Lidarr connect to nzbd as an "NZBGet"
download client with no changes: JSON-RPC 1.1 (`append`, `history`,
`editqueue`, the `*Lo/*Hi/*MB` triplets), XML-RPC with `system.multicall`, and
NZBGet's extension-script protocol byte-for-byte. Full guide in
[USAGE.md](USAGE.md).

**Where you see it.** Same client strip as §5 — the tooltip reads
`· nzbget-compat API` instead of `· native API`, which is how you tell at a
glance whether a consumer is on the rich path or the compatibility path.

**How to read it.** Compat clients get no event stream, no job params and no
capacity reporting. If a consumer shows `nzbget-compat API` when you expected
`native API`, it is configured as an NZBGet client rather than an nzbd one,
and the transfer id (§3) is not flowing.

---

## Metrics

`GET /metrics` (Prometheus text format, **requires auth**). Integration-
relevant names:

| Metric | Type | Reads |
|---|---|---|
| `nzbd_events_emitted_total{event="…"}` | counter | events published, by name |
| `nzbd_sse_clients` | gauge | streams open right now |

`nzbd_sse_clients` at 0 while monarr is configured for push means the stream
is not established — check §5 before anything else. There is no per-client
metric; the registry is JSON-only.

---

## What nzbd deliberately does not do

- **No outbound HTTP.** nzbd never calls monarr, plurx, or anything else. It
  cannot be blocked by their outages and cannot leak into them.
- **No knowledge of libraries, media items, or metadata.** A job param is an
  opaque string to nzbd; it stores and returns it without ever interpreting it.
- **No push to plurx.** nzbd and plurx have no seam. plurx learns about new
  files from monarr, after the import, when the files are actually in place —
  telling it earlier would only announce files that are not there yet.
- **No connection test button.** nzbd tests nothing; it observes. Everything in
  §5 and §6 is a record of what was done to it, which cannot go stale in the
  way a cached probe result can.

## Keeping this honest

Every literal string in this document — event names, chip text, problem
strings, config keys — was read out of the code on the branch that ships it.
When a seam changes, this file changes in the same commit; a doc that is
confidently wrong about a wire format costs more than no doc at all.
