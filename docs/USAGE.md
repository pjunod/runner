# Using nzbd

## The CLI

```sh
nzbd run [--config nzbd.toml] [--bind 0.0.0.0:6789]   # the daemon
nzbd add show.nzb [--url 127.0.0.1:6789] [--name N] [--category tv] [--priority 50]
nzbd status [--url 127.0.0.1:6789]                    # queue/rate/remaining as JSON
nzbd import-config nzbget.conf [--out nzbd.toml]      # migrate from NZBGet
```

**First-run setup:** if the `--config` path doesn't exist yet, the daemon
boots anyway and the web UI serves a setup form — paths, one news server,
optional UI password. Submitting writes the config file to that path and
restarts the daemon with it, no manual restart needed. (Everything the
wizard writes is ordinary `nzbd.toml`; edit it by hand afterwards.)

`add` and `status` are thin API clients — they talk to a running daemon,
local or remote. Logs go to stderr; set `RUST_LOG` for verbosity
(`RUST_LOG=debug nzbd run …`), and the same stream feeds the in-daemon
log ring visible in the UI and API.

Other ways to queue work: drop `.nzb` files into `paths.nzb_watch_dir`, drop
`.torrent` files into `paths.torrent_watch_dir`, let a feed rule accept items
([CONFIGURATION.md](CONFIGURATION.md) `[[feed]]`), POST to the native API, or
let Sonarr/Radarr do it.

## The web UI

Open `http://<host>:6789/`. One embedded page (no separate frontend to
deploy): live queue with per-job and per-file actions, pause/resume,
speed limit control, history, log tail, settings, layouts and color schemes. It updates
over SSE at 1 Hz — progress bars, rates and the sparkline move without
you touching anything.

**Adding downloads.** **+ add nzb** uploads one or more local NZB files.
**+ add torrent** opens an inline form for a magnet link, remote `.torrent`
URL, or local `.torrent` file, with category, priority, and add-paused options.
BitTorrent intake requires `[torrent].enabled = true` and a restart; the
button remains visible when disabled so configuration is advisory rather than
a second UI gate, and the daemon's rejection says when the running session is
still disabled. Torrent rows stay in the same queue while downloading and
seeding; their protocol chip, ratio, upload rate, peers, seeding time,
pause/resume, and keep/delete-payload controls identify the different
lifecycle.

**Feature enable controls.** Open **Settings → Dev → Enable features** for
BitTorrent and Usenet clustering. The readiness list updates from your form
values, marks unmet conditions, and labels disk, port, and cross-node checks
that still require operator verification. Advisories never disable an enable
switch. Save and restart to apply enable changes; normal configuration
validation still rejects unsupported combinations such as torrent and cluster
execution together. Seeding controls and queue sections have no extra gate.

**Files.** One table of every folder under the download roots — the
folders Runner wrote and the ones it found — filtered by **All / Needs
review / Owned / Cleared** (each chip carries the server's count), searched
by name, sorted by recent change, size, file count or name, and paged on the
server (25–200 rows; the pager sits above and below the table). **Scan
folders** walks the configured roots for folders Runner does not know about
and measures each one as it is found; the button holds and the pill counts
up until the scan finishes, then the result is announced. A row whose files
and size read `—` has not been measured yet — inspect walks it — and a row
never shows a zero it did not measure. **details** opens the folder's panel
as a row directly beneath it: full path, state, ownership, hold, the file
list (paged, with checkboxes for a Curator recovery copy), and the actions
its state allows — inspect, adopt & keep, release review hold, keep /
remove keep, delete (8-second Undo). Every background walk, delete and
staging run is watched to completion and its outcome toasted; nothing
waits for the daemon's 30-second maintenance tick. A recovery copy for
Curator can be staged only from an owned, unheld folder in a settled state;
when that is not the case the panel says which precondition is missing
instead of offering the button, and lists the folder's own handoffs (with
**cancel handoff**) — a folder with an open handoff is held until Curator's
import receipt arrives or the handoff is cancelled. See
[FILE_LIFECYCLE_OPERATIONS.md §2](FILE_LIFECYCLE_OPERATIONS.md) for the
whole recovery flow.

**Torrent lifecycle.** Ready torrents share in **Seeding** and move to
**Completed** when seeding stops. An idle seed is still available to peers;
zero upload speed does not put it back in Waiting. Metadata retrieval and
piece checking have their own sections. Missing files and errors remain
visible as failures rather than claiming the payload is ready.

Fold **Seeding**, **Completed**, or **Waiting** using its heading. Your browser
remembers each choice. Counts cover the entire section, including off-page
jobs; Seeding keeps its combined upload rate and uploaded bytes visible.
Collapsed jobs consume no pagination slots, so a large seed collection cannot
hide downloads or waiting jobs behind pages of folded rows.

Open a torrent's name for files, upload speed and total, share ratio, useful
peers, cumulative seeding time, completion timestamp, stopping reason, and
the **After download** editor. Choose category/global defaults, stop after
download, keep seeding, or a ratio/time limit. The first configured limit
reached stops seeding. Blank limits mean unlimited; time excludes stopped
periods. Applying defaults copies the active settings rather than subscribing
the torrent to future configuration changes.

**Stop seeding** retains files and survives restart. **Start seeding** resumes
sharing; if a saved stop condition is already met, **seeding options** opens
the editor so you can change it first. Editing a policy never starts a stopped
torrent. **Remove torrent** keeps payload files, while **delete data** is the
separate destructive action. Stop after download still permits uploading
pieces while downloading; it stops seeding after verification completes.

**Display** in the control row keeps three browser-local choices. **Layout**
offers Classic, Plex, and Theater; Classic is the exact nzbd layout from before
the selector and remains the default. Plex moves primary tabs into a pinned
navigation rail (and bottom tabs on a phone), while Theater opens the dashboard
into a wider top-deck. **Color scheme** matches plurx and monarr: Classic,
Terminal, noirr, Amber, Giallo, Silver, Void, VHS, Paper, and Tide. **Appearance**
independently follows the system or forces Light/Dark. Void and VHS are
midnight-only and say so in the menu. Switching any choice leaves live rows,
the active tab, and open detail panels in place.

**Deleting is one click, and undoable.** There is no confirmation dialog
anywhere in this UI. Click *delete* and the row is gone immediately; a
toast in the corner offers **Undo** for 8 seconds. That works because the
daemon *parks* the job rather than dropping it: it regenerates the NZB
from queue state, spools it beside the history index, and writes a
`DELETED` history entry. Undo re-queues from that spool, so a misclick on
a 60 GiB download costs one more click instead of a re-download. The
parked entry stays in History with a **requeue** button long after the
toast has faded — until you forget it or delete its files.

Two things are *not* undoable, and behave differently on purpose:

- **delete files** in History removes the downloaded files from disk. It
  arms in place — the button becomes `sure?` for three seconds, and
  clicking anywhere else cancels it. Two clicks, both on the button you
  already aimed at.
- Deleting when no history store is configured. The toast says so rather
  than offering an Undo that would fail.

**How to read the header.** The connection indicator is the thing to
check first when the page looks stuck: `● live updates` means the event
stream is delivering; `◌ updates delayed` means the stream is fine but
the daemon itself has stopped publishing fresh data — its engine is busy
or its state disk is slow; the page shows the last data it sent and
clears the moment it catches up (the daemon logs `engine tick ran long`
with timings when this happens — that log line names the culprit);
`◌ polling — reconnecting…` means the stream dropped and 5-second polls
are carrying the page while it is rebuilt automatically; a red banner
across the top means the daemon is not answering at all and every number
below it is the last state seen. The rate tile's sparkline is the last three minutes,
one point per second — a provider that dies for ten seconds a minute is
invisible in the number and obvious in the shape. The chips beside the
badges are your news servers with their current share of the wire rate;
they add up to the header rate exactly — the tile IS their sum, the
same bytes counted per server — and one turns red when that server is
blocked after connection failures. The browser tab title tickers
`▼ 93 MiB/s · 12m — nzbd` while downloading, so you can watch it from
another tab.

**If an action fails, the page says so** — with the daemon's own error
text — and the row springs back to where it was. An action that gets no
answer within five seconds reverts the same way. Nothing is ever shown as
done merely because the click happened.

**Long queues are paged** — 20 rows at a time by default, with 50, 100
and *all* in the picker under the table; your choice is remembered in the
browser. The controls only appear once there is more than one page (or
once you have changed the setting, so you can get back). Paging is
display only: the move arrows still move a job through the whole queue,
so the first row of page 2 can move up into page 1, and the page index
follows along as jobs finish and the queue shrinks.

## On your phone (PWA)

The UI is a progressive web app: responsive on small screens, installable
to the home screen with its own icon, standalone (no browser chrome).
Browsers only grant the full install + offline shell to origins they
consider **secure**, which gives three tiers:

1. **Plain HTTP on the LAN** — works fine as a responsive site; on iOS,
   Safari's Share → *Add to Home Screen* still gives an icon and
   full-screen launch. No service worker, no Android install prompt.
2. **nzbd's built-in HTTPS** — set `[api] tls = true` (nothing else): a
   self-signed certificate is generated once, persisted under the state
   dir, and its sha256 fingerprint is printed at startup. Then trust that
   cert on the phone — *clicking through the browser warning is not
   enough for Chrome to enable service workers*: download `cert.pem` to
   the device and install it (Android: Settings → Security → More →
   Install certificates → CA certificate; iOS: open the file → install
   the profile → Settings → General → About → Certificate Trust Settings
   → enable). After that the origin is secure and install works.
   Custom certs: `tls_cert`/`tls_key`; extra hostnames/IPs for the
   generated one: `tls_sans`.
3. **A real certificate** — any TLS reverse proxy (Caddy, Traefik,
   Tailscale `tailscale serve`) in front of nzbd. Zero warnings, nothing
   to install on devices; the best option when you have a domain or a
   tailnet.

## Settings

The **Settings** tab edits the running configuration as a normal form:
paths, news servers (add/remove), speed & queue, web UI & API,
post-processing, and categories. Passwords stay stored — type a new one
only to change it. Saving applies what a running daemon can absorb
immediately (the speed limit today); anything else flags a **restart
required** banner listing the affected sections, with a *Restart nzbd*
button that bounces the daemon in place (downloads resume from the
journal). Feeds and cluster settings live in the collapsible raw-TOML
editor at the bottom, which edits the same file.

## Connecting Sonarr / Radarr / Lidarr

Add a download client of type **NZBGet** (not SABnzbd):

- Host: where nzbd runs · Port: `6789` · SSL: off (or your reverse proxy)
- Username/password: whatever `[api]` has (empty if auth is off)
- Category: e.g. `tv` — create a matching `[[category]]`

Everything the *arr apps use is implemented against NZBGet's real wire
behavior and locked with golden tests: `version`, `append` (v13+ and
legacy call forms), `listgroups`, `history`, `editqueue`
(`Group*`/`File*`/`History*` verbs), `status`, `config`, `rate`, pause
family, `listfiles`, `log`/`writelog`, `scan`, `servervolumes`,
`sysinfo`, `testserver`. Duplicate handling (dupe key/score/mode) and
per-job passwords (`*Unpack:Password`) behave like NZBGet. XML-RPC
(including `system.multicall`) is served on `/xmlrpc` for older tooling.

## The *arr handoff, demystified

The handoff is a **pull, and it still works if nothing else does**:
Sonarr/Radarr poll nzbd's history (every ~30 s) for entries they queued,
import the files themselves, and then delete the history entry (NZBGet
`HistoryDelete` — which *hides*, not erases). If the *arr is down when a
download finishes, nothing is lost — the entry waits in history and gets
picked up on the next poll after it returns. Downloads only rot on disk
when an import fails silently on the *arr side, which is exactly the
case nzbd makes visible.

A consumer that would rather **not wait up to 30 s** can subscribe
instead of polling — see "Subscribing to the handoff" below. Push is an
optimization on top of this pull, never a replacement for it: nzbd makes
no outbound connections and does not know any consumer's address, so a
subscriber that never connects loses nothing the poll cannot recover.

The **History** tab shows each stage of that pull:

- **connected clients** strip — every API consumer seen (User-Agent),
  whether it's actively polling, and when it last called. If your *arr
  isn't listed or shows "quiet", it isn't talking to nzbd at all.
- **⏳ awaiting pickup** — finished, but no client history poll has
  listed it yet (normal for ~30 s; suspicious after hours).
- **seen by <client> ×N** — the client's polls have returned this entry
  N times. It knows. If this state persists, the *arr saw the download
  but hasn't imported it — check its Activity queue for import errors.
- **✓ imported by <client>** — the client deleted the entry after
  import (shown dimmed, kept for the record).

Manual controls per entry: **restore** re-exposes a hidden entry so a
connected *arr re-imports it on its next poll (the fix for "imported
but the files went missing"); **hide** does the reverse; **forget**
drops the record and keeps files; **delete files** removes both. Forget is
durable cluster-wide: nzbd writes a shared history tombstone before the local
row disappears, so a refresh, restart, or peer rebuild cannot bring it back.
Finish a rolling upgrade before relying on that guarantee; older binaries skip
the new mutation record and cannot enforce a forget they do not understand.

## Subscribing to the handoff

`GET /api/v1/events` is a Server-Sent Events stream carrying everything
the engine does. Two events cover the handoff:

- `job_pp_stage` — post-processing entered a stage (`par_verify`,
  `unpack`, `move`, …). This is what turns "stuck on something" into
  "repairing, since four minutes ago".
- `job_pp_finished` — post-processing ended. Carries `pp_status`, the
  job's params, and **`final_dir`: where the files actually are**. Note
  that the older `job_finished` fires when the *download* ends, before
  any of this.

For NZB and URL jobs, live `status: "completed"` establishes only that the
article-download phase ended. The row keeps that status after post-processing,
so the status alone is not permission to use the payload. Native consumers
must wait for `ready: true` and obtain the path from `job_pp_finished` or the
corresponding history row's `final_dir`. A failed job can be `pp_done: true` and
have a parked `final_dir`, but remains `ready: false`; that directory is for
diagnosis or requeue, not import.

```bash
curl -N -H 'Authorization: Bearer <token>' \
     -H 'X-Nzbd-Client: myapp/1.0' \
     http://localhost:6789/api/v1/events
```

Send `X-Nzbd-Client` and your subscription shows up in the connected
clients strip, so "is it attached right now" is a glance rather than a
guess. `EventSource` in a browser cannot set headers; a server-side
consumer using a plain HTTP client can.

**Resuming.** Every engine event carries an `id:` — treat it as opaque
and echo it back verbatim. Reconnect with
`Last-Event-ID: <the last id you handled>` and nzbd replays exactly what
you missed. Two things to handle:

- `event: reset` with `{"reason":"gap"}` means the gap could not be
  covered — you were away longer than the 1024-event buffer, or the
  daemon restarted (ids are process-scoped and a restart invalidates
  them). Do a full reconcile before trusting the stream again.
- `event: lagged` with `{"skipped":N}` means events were dropped.
  Reconnect with your `Last-Event-ID` to fill the hole; if the loss
  happened upstream of the numbering, this event arrives *in* the stream
  with an id of its own, so it survives a replay too.

**The reconcile that always works.** `GET /api/v1/history?since_seq=N`
returns entries newer than cursor `N`, oldest first, including entries
another client has hidden. Take the last row's `seq` as your next cursor.
`job_pp_finished` carries the `history_seq` of the row it refers to, and
is emitted only after that row is durably written — so reacting to the
event by reading `?since_seq=<history_seq - 1>` is guaranteed to find it,
with no retry loop. A consumer that was offline for a week catches up
from the cursor alone; the event stream is the fast path, the cursor is
the correct one.

**Tagging your downloads.** Add jobs with your own tracking id:

```bash
curl -X POST --data-binary @release.nzb \
     -H 'X-Nzbd-Client: myapp/1.0' \
     'http://localhost:6789/api/v1/jobs?name=Show.S01E01&category=tv&params=%7B%22myapp-id%22%3A%22t-42%22%7D'
```

`params` is a URL-encoded JSON object of strings. The id then appears on
the job in the queue, on the `job_pp_finished` event, in the history
entry and in compat `Parameters` — so grepping any log for it finds the
whole story. Keys beginning with `*` are reserved for nzbd's internals
and are rejected with a 422 naming the key.


## Native API

The compat shim is for NZBGet clients; automation you write yourself
should prefer the native JSON API (self-describing at
`/api/v1/openapi.json`):

```
GET  /api/v1/status                 queue totals, rate, health
GET  /api/v1/jobs                   the queue
POST /api/v1/jobs                   add a job (NZB content or URL)
GET  /api/v1/jobs/{id}
GET  /api/v1/jobs/{id}/files        per-file segment progress
GET  /api/v1/jobs/{id}/nzb          the job's NZB, regenerated from queue state
GET  /api/v1/jobs/{id}/torrent      torrent lifecycle and effective policy
PUT  /api/v1/jobs/{id}/torrent/seed-policy   edit the effective policy
POST /api/v1/jobs/{id}/actions/{action}     pause|resume|delete|delete-files|move-*
POST /api/v1/queue/actions/{action}
PUT  /api/v1/queue/speed-limit
GET  /api/v1/history
POST /api/v1/history/{id}/actions/{action}  hide|restore|delete|delete-files|requeue
GET  /api/v1/events                 SSE stream of queue changes
GET  /api/v1/logs                   recent daemon log
GET  /api/v1/artifacts              folder inventory: ?filter=live|attention|owned|cleared|all
                                    &sort=updated|size|files|name &q=<name> &offset &limit(≤200)
                                    → entries[{artifact,files,bytes,measured,earliest_expiry}],
                                      total, counts{live,attention,owned,cleared,all,live_bytes}, discovery
POST /api/v1/artifacts/scan         walk the configured roots (202, runs immediately)
GET  /api/v1/artifacts/{id}/files   manifest page (200 entries) + file_count, bytes, measured
POST /api/v1/artifacts/{id}/inspect|adopt|release-review|retention|delete
GET  /api/v1/artifact-operations/{id}   state of a scan/inspect/delete/stage operation
GET  /metrics                       Prometheus metrics
GET  /healthz                       liveness (always unauthenticated)
```

With `[api] password` set, authenticate with HTTP Basic or
`Authorization: Bearer <token>`.

**Torrent policy.** The policy PUT accepts `stop_on_complete` (boolean),
`ratio_limit` (positive number or null), and `time_limit_secs` (positive integer
or null). For example, `{"ratio_limit":2,"time_limit_secs":172800}` stops at
ratio 2 or 48 hours; `{"stop_on_complete":true}` stops after download.
`{"use_defaults":true}` copies the active category/global defaults. Invalid
limits return 422. Zero remains the unlimited spelling at admission and in
configuration; the policy PUT uses null so it cannot be confused with stopping
immediately. Native admission accepts `stop_seeding_on_complete` alongside
`seed_ratio_limit` and `seed_time_limit_secs` in typed JSON or raw-upload query
parameters. Omitted values inherit defaults.

Queue/SSE rows include `torrent_phase`, `torrent_control_intent`, `seed_policy`,
`seed_stop_reason`, and `torrent_error`, alongside the existing transfer
counters. The generic `status` remains unchanged for compatibility. Use the
torrent fields and `ready` to present the lifecycle, rather than inferring
seeding from a 100% progress bar.

**Delete and requeue.** `actions/delete` answers
`{"ok":true,"parked":true|false}`. When `parked` is true the daemon has
spooled the job's regenerated NZB and written a `DELETED` history entry,
and `POST /api/v1/history/{id}/actions/requeue` will put it back —
`200 {"id":<new job id>}` on success, `404` if the entry or its spooled
NZB is gone, `501` with no history store configured. A successful requeue
consumes the entry and its spool: the job is queued again, so a `DELETED`
record for it would be a lie. Entries in `GET /api/v1/history` carry
`can_requeue`, which is derived at read time rather than stored — it
answers "is the requeue source still on *this* node?", and the spool is
local, not shared cluster state.

**The event stream** (`/api/v1/events`) carries every engine event under
its own `event:` name, plus three of its own: `tick` (`{status, jobs}` at
1 Hz, the whole read model from one snapshot, suppressed while nothing
changes); `hb` (`{now_unix}`, sent when a tick was suppressed and nothing
has gone out for 5 s — that is how a client tells an idle queue from a
dead stream, since `EventSource` cannot see keep-alive comments); and
`log` (`{entries, dropped}` at 1 Hz, capped at 200 lines per frame, with
`dropped` reporting what the cap cut). A new connection tails the log
from the newest id rather than replaying the ring — use `GET
/api/v1/logs` for backfill.

## RSS feeds and the filter language

Feeds poll on an interval, run each item through the filter, and queue
whatever is accepted (once — a persistent guid ledger dedupes across
restarts and cluster failovers). `fetchfeeds` forces a poll;
`viewfeed(id)` previews what a feed's filter would do — each item comes
back flagged ACCEPTED/REJECTED and NEW/BACKLOG.

The filter is a line-oriented subset of NZBGet's language:

```
# comments start with '#'
Require: expression        # every Require must pass, or the item is rejected
Accept(options): expression
Reject: expression         # first matching Accept/Reject decides
expression                 # bare line = Accept
# Short forms: Q: (require), A: (accept), R: (reject)
```

An expression is space-separated terms, ALL of which must match:

| Term | Meaning |
|---|---|
| `pattern` or `title:pattern` | wildcard match on the title (`*` any run, `?` one char, case-insensitive) |
| `category:pattern` / `url:pattern` | same matching on those fields |
| `size:>4GB` · `size:<900MB` · `size:500MB-2GB` | decoded-size window (K/M/G/T suffixes) |
| `age:>3d` · `age:<30d` | item age in days |
| `-term` | negates any term (`-*x265*`, `-category:foreign`) |

Accept options are carried onto the queued job: `category`, `priority`,
`pause` (yes/no), `dupekey`, `dupescore` — e.g.
`Accept(category:tv-hd, priority:100, dupescore:10): *2160p*`.

If no Accept rule exists at all, everything passing the Requires is
accepted (pure Reject-filtering works).

## Post-processing

The per-job pipeline and its knobs are described in
[CONFIGURATION.md](CONFIGURATION.md) `[post]`. Operational notes:

- **Verification is usually free.** nzbd records CRCs while downloading,
  so an intact par2 set is proven without re-reading data. Repair spawns
  `par2` only when something is actually damaged.
- **Deobfuscation is layered.** Evidence first: par2 16k-hashes recover
  real names even for fully-hex posts (including obfuscated `.par2` files
  found by magic bytes), and archive signatures fix mislabeled volumes.
  Then, post-unpack, a heuristic pass renames what evidence couldn't:
  a dominant file gets the job name (SABnzbd's rule, its heuristics
  ported); a fully-obfuscated season pack gets stable `<job> - NN`
  numbers. Names the par2 set vouches for are never overridden. The
  queue shows the `post_unpack_rename` stage while it runs, each rename
  is logged, and the applied list persists in history as
  `Deobfuscate:Count` / `Deobfuscate:Files` parameters.
- **Extension scripts** are NZBGet's: a directory of scripts (legacy
  header or v2 `manifest.json`), `NZBPP_*`/`NZBPR_*` environment,
  `[NZB] FINALDIR=…` and friends on stdout, exit codes 92–95. Point
  `post.scripts_dir` at your existing NZBGet scripts.
- **Health actions**: failed-health jobs can be left (`none`), parked, or
  deleted from disk (`delete`), mirroring NZBGet's HealthCheck.

## History, duplicates, quotas

Finished jobs retire from the queue into history (SQLite, with an
append-only JSONL mirror per node in cluster mode). Duplicate handling
follows NZBGet: dupe key/score/mode on jobs, checked against queue and
history on `append`, with `DELETED/DUPE` history records for rejects.
Daily/monthly quotas soft-hold the queue when exhausted and release on
rollover; `servervolumes` exposes per-server counters.

## Clustering, day to day

Point everything (the *arr apps, your browser, `nzbd add`) at **any**
node — every node serves the full API and transparently proxies to the
current leader. `GET /api/v1/cluster` shows nodes, roles, and the
leader. Feeds, the watch dir, and PP scheduling are leader-gated;
downloads and PP run wherever leases land (PP prefers nodes that aren't
downloading). Nothing needs draining for a rolling restart — leases
expire and are adopted. Deployment: [DEPLOY.md](DEPLOY.md); semantics
and failure matrix: [CLUSTERING.md](CLUSTERING.md).
