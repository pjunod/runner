# Configuration reference — `nzbd.toml`

nzbd reads one TOML file (`nzbd run --config nzbd.toml`). Every section
and key is optional — omitted keys take the defaults shown here, and a
bare `nzbd run` with no file at all works with the defaults. Unknown keys
are rejected at startup (typos fail loudly rather than silently doing
nothing). Paths accept a leading `~`.

Converting from NZBGet? `nzbd import-config nzbget.conf --out nzbd.toml`
maps an existing configuration onto this format and reports what mapped,
what was recognized but not applicable, and what needs review by hand.

## `[paths]`

```toml
[paths]
main_dir = "~/downloads"            # default Usenet processing root + state
dest_dir = "~/downloads/complete"   # finished downloads (per-category overrides below)
# inter_dir = "~/downloads/inter"   # processing override; absent/empty uses main_dir
# nzb_watch_dir = "~/downloads/nzb" # drop .nzb files here to auto-queue them
# queue_dir = "~/downloads/queue"   # journal + queue snapshots (default: <main_dir>/queue)
# temp_dir = "/tmp/nzbd"            # scratch space
```

The watch dir is polled by the daemon; a dropped `.nzb` is queued and the
file removed. In cluster mode only the current leader watches it.

## `[torrent]` — single-node BitTorrent

```toml
[paths]
# torrent_dir = "/data/torrents"       # default: <main_dir>/torrents
# torrent_watch_dir = "/data/watch-torrent"

[torrent]
enabled = false
listen_port = 6881
dht = false
pex = true
local_discovery = false
upnp_port_forwarding = false
# socks_proxy_url = "socks5://127.0.0.1:1080"
# socks_proxy_username = "alice"
# socks_proxy_password = "secret"
max_peers_per_torrent = 80
max_peers_total = 400
max_known_peers_per_torrent = 1024
max_known_peers_total = 4096
upload_limit_kib = 0
default_seed_ratio = 0
default_seed_minutes = 0
stop_seeding_on_complete = false
metainfo_max_mib = 10
source_redirects = 5
```

| Key | Default | Security and operational implication |
|---|---|---|
| `enabled` | `false` | `true` starts one torrent session after restart. Disabled mode opens no peer listener and refuses startup if live torrent records still need an owner. |
| `listen_port` | `6881` | One explicit TCP/IPv4 port from 1–65534; ranges and ephemeral port 0 are unsupported. |
| `dht` / `pex` | `false` / `true` | DHT finds peers for trackerless magnets but can expose an unknown magnet hash before metadata reveals private status. Private torrents require DHT off, and DHT is incompatible with the SOCKS proxy. |
| `local_discovery` / `upnp_port_forwarding` | `false` / `false` | Avoids LAN disclosure and router mutation. Either unsupported value is rejected. |
| `socks_proxy_*` | absent | URL must be a credential-free SOCKS5 origin. Username/password are paired; the username must be non-empty, and both fields may contain only ASCII letters, digits, `-`, `.`, `_`, or `~` (so characters such as `@` and `!` are rejected). The password is masked by Settings. |
| peer ceilings | `80`, `400`, `1024`, `4096` | Separate live and retained per-torrent/session budgets. |
| `upload_limit_kib` | `0` | Unlimited; this is the only torrent setting designed for live application. |
| seed ratio/minutes | `0` | Unlimited globally; optional category values override these and per-add values override categories. |
| `stop_seeding_on_complete` | `false` | Stop after the selected payload is verified. Uploading pieces during download is still allowed. Category and per-add values can override this default. |
| `metainfo_max_mib` / `source_redirects` | `10` / `5` | Bounds hostile metadata and source fetches; metainfo accepts 1–100 MiB. |

Omitting `[torrent]` is identical to its disabled defaults. `torrent_dir`
never falls back to Usenet `dest_dir`; when omitted it derives as
`<main_dir>/torrents`. Optional category `torrent_dir`, `seed_ratio`, and
`seed_minutes` follow per-add → category → global precedence. Torrent roots do
not enter disk probes while disabled. Cluster mode and BitTorrent cannot be
enabled together. Unknown keys fail closed.

Category `stop_seeding_on_complete` is optional: omit it to inherit, or set
`true`/`false` explicitly. Seed defaults take effect for new admissions after
restart; they do not rewrite existing torrent policies. Use the queue's
After download editor to change an existing torrent immediately. Ratio and
time limits stop on the first boundary reached, retaining all payload files.

Before enabling, publish the configured TCP peer port only where intended,
confirm the payload and state volumes have durable free space, and decide
whether unlimited seeding is acceptable. DHT is public discovery: it makes
trackerless magnets usable, but may query their hashes before downloaded
metadata reveals that a torrent is private. nzbd rejects that private metadata
before admission; it cannot retract the earlier lookup. Operators requiring no
pre-metadata public lookup must leave DHT off and use a trusted tracker-bearing
magnet or `.torrent` file. A SOCKS proxy is not a VPN kill switch; proxy
deployments must keep DHT off and enforce required routing in the host or
container firewall. The Developer settings tab shows these prerequisites and
their current form-derived state as advice; it never disables the operator's
explicit enable control.

**Client identity.** Trackers and peers see Runner, never the embedded
engine: peer ID prefix `-RN<major><minor><patch>0-` (`-RN0200-` for 0.2.0,
twelve random bytes after it, new each daemon start), HTTP `User-Agent:
Runner/<version>` on tracker announces and `.torrent` downloads, and
`Runner <version>` in the BEP 10 handshake. Announces follow BEP 3/15:
passkeys in the announce URL's query are kept, `key` is stable per tracker
session, `tracker id` is echoed, `downloaded` counts only payload fetched
since `started`, `completed` is sent once within about five seconds of a
download finishing, failed announces back off from one minute to thirty, and
pausing, removing, or shutting down sends one `stopped` (best effort, at most
two seconds; shutdown waits up to three). Private trackers that allow only listed clients will not know
`RN` until Runner is added to their allowlist.

Sonarr and Radarr can use nzbd as a qBittorrent client at the normal nzbd API
URL. Use the existing nzbd username/password, or put `[api].token` in the
client's API-key field. The compatibility surface reports Web API `2.8.1` and
implements only the download-client routes; search, RSS, plugins, tracker
editing, torrent creation, alternate Web UI, and remote shutdown are absent.

## `[[server]]` — one block per news server

```toml
[[server]]
name = "primary"          # unique label; same name on several nodes of a
                          # cluster means "one shared account" (budget is split)
host = "news.example.com"
port = 563                # default 563
tls = true                # default true
username = "user"
password = "pass"
active = true
tier = 0                  # failover ladder level: 0 = main, 1+ = backups
group = 0                 # servers in the same group never run concurrently
fill = false              # true = fill server (tried only for missing articles)
connections = 8           # concurrent NNTP connections
pipeline_depth = 2        # commands in flight per connection (adaptive AIMD
                          # raises/lowers the effective depth at runtime)
retention_days = 0        # 0 = unlimited; skips articles older than this
cert_verification = "strict"   # strict | minimal | none
```

Tiers implement NZBGet's ladder: every article is tried on tier 0 first,
then tier 1, and so on. `fill` servers are consulted only after the
regular servers of their tier miss an article.

## `[[category]]`

```toml
[[category]]
name = "tv"
dest_dir = "/data/complete/tv"   # optional override of paths.dest_dir
torrent_dir = "/data/torrents/tv" # optional immutable seed root
seed_ratio = 1.5                 # optional torrent override; 0 = unlimited
seed_minutes = 4320              # optional torrent override; 0 = unlimited
unpack = true                    # optional per-category unpack override
extensions = []                  # extension scripts to run for this category
```

`name` is matched against the job's category case-insensitively, so an
*arr sending `TV` lands on `name = "tv"`.

`dest_dir` is a **move at the end of post-processing**, not a different
download target: the engine writes under nonempty `paths.inter_dir`, otherwise
`paths.main_dir`, and the finished folder is relocated to
`<category dest_dir>/<job name>` before extension scripts run and before any
path is reported. Cross-filesystem
destinations work (the move falls back to copy-then-remove), which is the
usual homelab shape — download on the SSD, library on the NAS. If the
move fails, the failure is logged loudly and every reported path names
where the files actually are, not where they were meant to go.

`unpack` overrides `[post] unpack` for this category only. `extensions`
names the post-processing scripts this category runs, by file name or
stem (`"Clean.py"` and `"clean"` both select `Clean.py`); an empty list —
the default — runs every discovered script, which is the global behavior.

The daemon accepts at most 64 distinct configured write roots across state,
cluster state, queue paths, watch paths, and category destinations. Exact path
duplicates share one root. This per-configuration bound is paired with a
process-wide 64-call probe ceiling that survives hot reloads until each
filesystem syscall returns, so category/path churn cannot accumulate unbounded
wedged threads. Startup names the count and limit when a config exceeds it.
Where stable filesystem identity is available, distinct paths on one mount are
reported as one failure domain. Windows conservatively reports them as separate
rows while still enforcing the lowest free-space reading across all of them.

> **Behavior change (integration phase 1).** These three keys were parsed
> and advertised to compat clients as `CategoryN.DestDir` / `.Unpack` /
> `.Extensions` for a long time while post-processing ignored all of
> them. A config that set `dest_dir` "expecting nothing to happen" will
> now see files move there. This was fixed rather than documented as a
> quirk because an *arr path-maps off the advertised value: advertised
> paths that are not actual paths are a silent import failure with
> nothing in any log to explain it.

## `[queue]`

```toml
[queue]
article_retries = 3          # per-article retry attempts
retry_interval_secs = 10
article_timeout_secs = 60
article_cache_mb = 0         # reserved; DirectWrite keeps this at 0
direct_write = true          # positional writes straight into sparse files
crc_check = true             # verify per-article CRC32 while downloading
continue_partial = true      # resume partially-downloaded files on restart
propagation_delay_mins = 0   # ignore posts younger than this
min_free_disk_mb = 250       # pause grabbing new work below this free space
# speed_limit_kib = 10240    # global rate cap (KiB/s); absent = unlimited
max_active_downloads = 1     # how many jobs download at once (1..=100)
daily_quota_mb = 0           # 0 = unlimited (NZBGet DailyQuota)
monthly_quota_mb = 0         # NZBGet MonthlyQuota
quota_start_day = 1          # day of month the monthly quota resets
```

`min_free_disk_mb` applies to the lowest reading across every configured write
root, not only `paths.dest_dir`. A zero value disables the hold but keeps the
same cached inventory visible in native status.

`max_active_downloads` decides how many jobs are worked on at the same
time. At `1` — the default, and what nzbd has always done — the top job
takes every connection until it has no segments left to hand out. Raising
it splits the connection pool evenly between that many jobs; priority
still decides *which* jobs are in the set, this decides how many.

It does not make anything faster. The same connections move the same
bytes either way; they simply arrive spread across several jobs instead
of completing one at a time, so the first job finishes later and the last
finishes at about the same moment. Raise it when you want several things
moving at once — a small job not stuck behind a 60 GB remux — not when
you want more throughput.

Both this and the speed limit can be changed while nzbd runs, from the
box on the Queue page or from Settings; the value in the file is the
starting position after a restart.

Per-server `connections` also applies without a restart when you *lower*
it. Raising it above the number nzbd started with needs a restart, since
the sockets are opened at boot — the settings page says so when it
happens rather than pretending the new number is in force.

When a quota is exhausted the queue soft-holds (downloads pause, the API
stays up, the queue keeps accepting jobs); it releases automatically when
the day/month rolls over. Volume accounting is per server and survives
restarts (`servervolumes` in the compat API shows it).

## `[api]`

```toml
[api]
bind = "127.0.0.1:6789"     # use 0.0.0.0:6789 to serve the LAN
discovery = true             # advertise reachable listeners as _nzbd._tcp
tls = false                 # true = serve HTTPS (NZBGet SecureControl).
                            # With no cert configured, a self-signed cert is
                            # generated once under the state dir and reused;
                            # the startup log prints its sha256 fingerprint.
# tls_cert = "/etc/nzbd/cert.pem"   # bring your own PEM chain + key instead
# tls_key  = "/etc/nzbd/key.pem"    # (NZBGet SecureCert / SecureKey)
# tls_sans = ["nas.lan", "192.168.1.10"]  # extra names for the generated cert
compat_version = "26.2"     # version string the NZBGet shim reports
username = "nzbd"           # HTTP Basic user (compat ControlUsername)
# password = "secret"       # setting a password ENABLES auth everywhere
#
# Secrets and the settings editor: the web UI never shows a real password —
# it displays `***unchanged***` and swaps the real value back in when you
# save. That means the TOML you see (or Download) in the Settings tab is a
# DISPLAY, NOT A BACKUP. A file restored from that copy would carry the
# placeholder as your password; nzbd refuses to start on such a config and
# tells you which field to fix, and the Download button names the masked
# copy `nzbd-masked.toml` so it cannot be mistaken for the real file. For a
# real backup, copy `nzbd.toml` off the config volume itself.
# token = "long-random"     # optional Bearer token alternative
allow_legacy_default_credentials = false   # opt-in nzbget/tegbzn migration aid
```

With no password set the API is open (bind to localhost!). With one set,
every endpoint except `/healthz` requires HTTP Basic (or the Bearer
token). The *arr apps pass username/password in their NZBGet client
settings unchanged.

With discovery enabled, nzbd advertises `_nzbd._tcp.local.` over mDNS after
the API listener starts. The TXT record contains only `path`, `tls`, `auth`,
and the nzbd version; credentials and queue state are never advertised. A
loopback listener is skipped even when discovery is enabled because another
device cannot connect to it. Set `bind = "0.0.0.0:6789"` (or a specific LAN
address) for the mobile app to find it, and set `discovery = false` to opt out.
Multicast startup errors are logged but do not stop the API.

A normal Docker bridge is a multicast boundary. The daemon can advertise to
other containers on that bridge, but phones on the physical LAN will not see
it. Run the image's advertiser companion with host networking while leaving
the downloader on its application network:

```bash
docker run -d --name nzbd-discovery --restart unless-stopped \
  --network host ghcr.io/pjunod/nzbd:latest \
  advertise --name nuc3 --port 6789
```

The companion publishes the API already mapped to host port 6789; it does not
proxy traffic or read the downloader's configuration. Pass `--tls` and
`--auth basic`, `bearer`, or `basic,bearer` when those metadata should appear
in discovery results. The Compose example includes the same companion as the
`nzbd-discovery` service.

## `[post]` — post-processing

```toml
[post]
enabled = true
par2_cmd = "par2"           # external tools; names or absolute paths
unrar_cmd = "unrar"
sevenzip_cmd = "7z"
# scripts_dir = "~/nzbd-scripts"   # NZBGet extension scripts live here
unpack = true
cleanup = true              # delete archives/par2/sfv after successful unpack
deobfuscate_final = true    # rename still-obfuscated files to the job name
                            # (season packs get "<job> - NN"); par2-proven
                            # names are never touched
strategy = "balanced"       # sequential | balanced | aggressive | rocket
                            # (1 / 2 / 3 / 6 concurrent PP jobs)
failure_action = "delete"   # none | park | delete — what happens to the
                            # FILES of a job that ended in a terminal
                            # failure: par failure, unpack failure, script
                            # failure, health abort, post crash. Deleting
                            # loses nothing: the job's NZB is parked with
                            # its history row, so requeue re-downloads it.
                            # "none" leaves ~90 GB per failed grab in the
                            # tree your importer watches — that is how a
                            # terabyte of duplicates happened. Anything but
                            # "none" also aborts a download the moment its
                            # health drops below critical health (the point
                            # where even all par2 blocks can't repair it),
                            # instead of finishing a doomed download.
                            # Accepts the old name `health_action`, which
                            # only ever governed the health gate
failed_dir = "/data/usenet/failed"   # where "park" puts them; defaults to
                            # <main_dir>/failed — deliberately off the
                            # category tree
tool_timeout_secs = 3600
script_timeout_secs = 3600
par_fetch_timeout_secs = 600   # wait for delayed par files during repair
```

Failed-file disposition is retried before the terminal history row is written.
A stable local error such as permission denied is attempted three times, then
the job is retired with the last `delete failed: ...` or `park failed: ...`
note and the observed path to any files left for operator cleanup. With an
uninterrupted daemon, the first attempt is immediate and the next two follow
the 30-second rescans, so exhaustion takes about 60 seconds. A restart may make
the next attempt happen sooner, but the attempt count lives in the durable
queue row beside the failure timestamp and never resets. Disk-full/quota errors
and a closed cluster admission or authority gate do not consume that budget;
they remain retryable until storage or ownership recovers.

The PP pipeline per job: par-rename → rar-rename → par verify (native
quick-verify from download CRCs; repair only on damage) → unpack (with a
repair-and-retry loop for archives that fail) → cleanup → deobfuscate →
extension scripts. Scripts get NZBGet's exact `NZBPP_*` environment and
`[NZB] KEY=value` command channel; exit codes 92–95 mean what they mean
in NZBGet.

## `[history]` — retention and local index placement

```toml
[history]
# index_dir = "/local/nzbd-history" # optional persistent local bind; restart required
keep_max = 1000    # keep at most this many entries (0 = unlimited)
keep_days = 90     # drop entries finished longer ago than this (0 = forever)
```

Both bounds apply and whichever is reached first wins. They answer
different questions, which is why neither one alone is enough: `keep_max`
answers *how big may this get* and holds when a week's backlog lands in a
day; `keep_days` answers *how far back do I care* and holds when the
daemon sits quiet for months.

History pages read the indexed SQLite view directly. A standalone daemon
replays its portable log at startup and when a failed local publication needs
repair; shared stores reconcile in a background worker. Trimming still bounds
stored data and recovery work: it deletes index rows, compacts the portable
log, and raises the watermark that prevents old peer entries from returning.

**Local index placement.** Without `index_dir`, the database remains
`<state_dir>/history.sqlite`. On a network-backed state directory, configure
`index_dir` to an empty directory on a persistent local bind. Do not use a
container's writable layer. The logs remain in their existing directory and
parked NZBs remain under `<state_dir>/nzbs`; changing `queue_dir` would move
other queue state as well and is not a substitute.

The change takes effect after restart. Stop the old process first: upgraded
processes hold a portable-directory writer lock, but an older binary does not
honor that lock. Startup uses SQLite `VACUUM INTO` to copy committed data,
including WAL, cursor IDs, observations, tombstones, and metadata. It then
records the active index path in a hidden marker beside the portable logs.
The old copy remains for inspection. An existing different destination is
rejected rather than overwritten or silently selected.

Do not delete only the registered index or switch back to its abandoned old
copy: startup rejects a missing registered database and an existing stale
migration target. To relocate again, choose a new empty directory so the
active database is copied. An interrupted copy/cutover can require explicit
operator recovery; preserve both files and inspect the active-path marker
before removing anything. A legacy-binary rollback needs a quiesced copy of
the latest database into its expected location and removal of the unsupported
`index_dir` setting. Never copy only a live main database file while its WAL
may contain committed writes.

**Synchronization controls.** Settings → Dev → Enable history synchronization
provides a live, persisted control with storage and recovery readiness advice.
Readiness never blocks enabling, and the control works even if the config file
is read-only. The performance fixes have no rollout flags. The History tab shows local/shared mode, worker
state, last duration and bytes read, last successful reconciliation age,
repair state, and index placement. Pause persists across restarts and stops
new reconciliation passes, including admission-triggered refreshes. It stops
an active pass at a safe boundary; a blocked filesystem call can delay that
stop. Local writes and consumer observations continue. Startup recovery still
runs before serving history, even when background reconciliation is paused.
Resume permits catch-up on the next worker tick (normally within five seconds).

The authenticated native API exposes the same surface:

```text
GET  /api/v1/history-sync
POST /api/v1/history-sync/pause
POST /api/v1/history-sync/resume
```

History status also reports the last scan kind, affected entry count, rebuilt
file count, incomplete tails, and unrecognized complete lines. Normal shared
appends use incremental ingestion automatically; startup and periodic full
verification remain recovery safeguards. See
[HISTORY_INCREMENTAL.md](HISTORY_INCREMENTAL.md) for interpretation and limits.

History responses also include `sync`. Shared history can lag peer changes by
one worker interval plus reconciliation/storage visibility time; there is no
hard five-second freshness guarantee. Consumer observations enqueue without request-time storage writes and are
flushed by the worker even while reconciliation is paused. The queue is bounded
to 10,000 job keys plus at most 256 in-flight keys; overflow reports an internal
error and can drop advisory observations without delaying history reads. They remain advisory, index-local data.

See [HISTORY_LOADING_PLAN.md](HISTORY_LOADING_PLAN.md) for the measured defect,
implementation status, and performance validation boundaries.

Trimming runs at startup and on a 60-second throttle as jobs finish, so
lowering a bound takes effect when you restart, not when the next job
happens to complete. Nothing else changes: an entry's parked NZB is
reaped with it, exactly as `forget` already does.

Importing an `nzbget.conf` maps `KeepHistory` onto `keep_days` — the units
already agree, so your existing retention window comes across rather than
being replaced by nzbd's default.

## `[[feed]]` — RSS/Atom indexer feeds

```toml
[[feed]]
name = "indexer-tv"
url = "https://indexer.example/rss?apikey=…&t=5000"
interval_mins = 15
category = "tv"       # default category for accepted items
priority = 0
pause = false         # queue items paused
filter = """
# NZBGet-style filter: first matching Accept/Reject wins;
# Require lines must ALL pass first. See USAGE.md for the language.
Require: size:>200MB -age:>30d
Accept(category:tv-hd, priority:50): *1080p* -*x265*
Reject: *cam* *telesync*
Accept: *
"""
```

Feed state (a guid ledger with 90-day retention) prevents re-downloading
items across restarts — and across failovers in cluster mode, where only
the leader polls. `fetchfeeds`/`viewfeed` in the compat API trigger and
preview feeds on demand.

## `[cluster]` — multi-node mode

Off by default; a single-node daemon needs none of this. Full semantics:
[CLUSTERING.md](CLUSTERING.md). Deployment recipes: [DEPLOY.md](DEPLOY.md).

```toml
[cluster]
enabled = true
cluster_id = "media-home"                 # identical and stable on all nodes
node_name = "node-a"                      # unique + stable per node
shared_dir = "/mnt/work"                  # the shared POSIX volume (all nodes)
advertise_url = "http://10.0.0.11:6789"   # how PEERS reach this node
secret_file = "/etc/nzbd/cluster.secret"  # same secret on every node
# secret = "inline-secret"                # alternative to secret_file
coordinator = true          # eligible for leader election
priority = 10               # lower = preferred leader
download = true             # takes leases while this node is a worker
max_download_jobs = 2
post_process = true         # PP executor (anti-affinity prefers idle nodes)
pp_slots = 1
lease_interval_secs = 5     # heartbeat cadence
takeover_after_secs = 20    # leader considered dead after this silence
worker_ttl_secs = 30        # work lease expiry (another node then adopts)
control_dir = "/var/lib/nzbd/control"      # local durable disk, never shared_dir
control_node_id = 1                        # unique + stable voter identity
control_raft_bind = "10.0.0.11:8810"
control_api_bind = "10.0.0.11:8820"
download_weight = 2         # positive relative placement weight
pp_weight = 1

[[cluster.control_peers]]    # identical fixed roster on every voter
id = 1
raft_addr = "10.0.0.11:8810"
api_addr = "10.0.0.11:8820"

[[cluster.control_peers]]
id = 2
raft_addr = "10.0.0.12:8810"
api_addr = "10.0.0.12:8820"

[[cluster.control_peers]]
id = 3
raft_addr = "10.0.0.13:8810"
api_addr = "10.0.0.13:8820"
```

An empty `control_peers` list is a supported single-voter configuration. A
production HA cluster normally uses a fixed odd roster of at least three.
Membership changes are stopped-cluster operations.

The elected authority never consumes its own download or PP capacity. Keep at
least one other eligible executor online when work must advance; a coordinator
becomes an executor again automatically after it is no longer authority.

Settings → **Dev · Usenet cluster** exposes the enable switch and common
fields. Its met/unmet/unknown requirements are advisory only. The form never
requires an approval receipt or readiness score. Configuration validation
still rejects malformed identities, missing authentication, and invalid bind
or voter definitions because those values cannot execute safely.

Provider account budgets use the `[[server]].name` as the shared account key in
this release. Nodes using the same account must use the same name and the same
account-wide `connections` ceiling. See [CLUSTERING.md](CLUSTERING.md) for
acknowledged budget transfer and migration/rollback behavior.

## Complete minimal example

```toml
[paths]
main_dir = "/data"
dest_dir = "/data/complete"

[[server]]
name = "primary"
host = "news.example.com"
port = 563
tls = true
username = "user"
password = "pass"
connections = 20

[[category]]
name = "tv"

[[category]]
name = "movies"

[api]
bind = "0.0.0.0:6789"
password = "change-me"
```

### Usenet download and completion paths

`paths.inter_dir`, when nonempty, is the Usenet download and processing root.
Successful post-processing publishes to `paths.dest_dir`, or the matching
category's destination, before extension scripts and completion notification.
A failed move does not report success. Absent or empty `paths.inter_dir` uses
expanded `paths.main_dir` directly, without an appended subdirectory. This also
applies to imported NZBGet configurations with absent or empty `InterDir`.
`paths.dest_dir` is the successful publication destination, not the implicit
processing root. `paths.main_dir` also supplies state and other default paths.
Existing allocated jobs retain their recorded custody path when settings change;
this default change does not relocate them. Path changes take effect after daemon
restart. Configure separate processing and completed roots for the intended
separation; overlap validation is not introduced by this fallback change.
