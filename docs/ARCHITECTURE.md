# nzbd — Architecture Design

**A ground-up Rust reimplementation of the NZBGet Usenet downloader.**

| | |
|---|---|
| Status | Draft for review (v0.1) |
| Date | 2026-07-17 |
| Working name | `nzbd` ("NZB daemon" — placeholder; trivially renameable, check crates.io/GitHub availability before publishing) |
| Reference | nzbget v26.2/26.3 (nzbgetcom fork), source analyzed at commit of 2026-07 |

---

## 1. Context and motivation

NZBGet is a binary Usenet downloader with a 20+ year lineage: a C++17 daemon (revived and actively maintained by the nzbgetcom fork since 2023, currently v26.2) known for a tiny footprint and best-in-class throughput (~1.4–1.8 GB/s measured, vs ~0.9 GB/s for a maximally patched SABnzbd). It is the workhorse behind countless Sonarr/Radarr/Prowlarr setups.

The codebase, however, carries its age:

- **Blocking I/O with one OS thread per in-flight article** (`ArticleDownloader : Thread`), plus a thread per HTTP connection in a hand-rolled web server. Concurrency is capped at `2 + Σ MaxConnections`.
- **One coarse global mutex** (`DownloadQueue::m_lockMutex`) serializes essentially all queue mutation across every subsystem.
- **Global mutable singletons** (`g_Options`, `g_QueueCoordinator`, `g_WorkState`, …) wired in `main`, coordinated via an ad-hoc Observer pattern; cooperative busy-wait throttling (`Sleep(10ms)` loops against a shared speed meter).
- **Hand-rolled everything**: HTTP server, XML-RPC/JSON-RPC serialization (4,200-line `XmlRpc.cpp`), versioned line-oriented text persistence (`DiskState.cpp`, 2,616 lines), a from-scratch config parser.
- A jQuery + Bootstrap 2.3 web UI with no build system (server-side JS file concatenation as the "bundler").
- C++ memory-safety exposure in a daemon that parses hostile inputs from the network (articles, NZBs, RSS feeds, archives).

Notably, the fork's own modernization converged on exactly the components a rewrite would choose — SIMD yEnc via **rapidyenc** (v26.1), **par2cmdline-turbo** statically linked (v24.4+), OpenSSL-only TLS, `std::filesystem` — which validates the component strategy below.

**What must be preserved** (the product's soul): the lightweight/high-throughput ethos, direct-to-disk writing, unpack-during-download, crash-safe resume, and — critically — the ecosystem: Sonarr/Radarr integration and the post-processing script protocol.

## 2. Goals and non-goals

### Goals

1. **Throughput ≥ nzbget** on the same hardware (target: saturate 10 Gbit with headroom; decode is not the bottleneck — rapidyenc does >4.5 GB/s/core).
2. **Memory safety and fearless concurrency**: no data races by construction; hostile-input parsing in safe Rust.
3. **Drop-in for the *arr ecosystem**: an nzbget-compatible JSON-RPC/XML-RPC shim, byte-shape-faithful for the methods clients actually call.
4. **Post-processing script compatibility**: existing NZBGet extensions run unmodified (same env-var protocol, exit codes, `[NZB]` commands).
5. **Crash-safe resume**: kill -9 at any moment loses at most a few seconds of progress.
6. **Single static binary** per platform (Linux x86-64/aarch64 incl. musl for NAS, macOS, Windows), embedded web UI, no runtime deps beyond optional `unrar`/`par2` executables.
7. **Modern operability**: structured logs, Prometheus metrics, typed TOML config, OpenAPI-documented native API, SSE event stream.
8. **A testable architecture**: every subsystem behind a trait; an in-tree mock NNTP server (nzbget's `nserv` equivalent) for end-to-end tests in CI.
9. **Optional clustering**: multiple nodes sharing a work volume distribute download and post-processing work with automatic leader failover — full design in [`CLUSTERING.md`](CLUSTERING.md) (ADR-13…16). Single-node remains the default and pays nothing for it.

The [2026-09-19 cluster completion plan](CLUSTERING_COMPLETION_PLAN.md)
reassesses ADR-13/15/16 against plurx's current implementation. It recommends
a narrow replicated control store and private output generations before C3;
the implemented architecture below still uses shared-file election. The
recommendation includes an explicit compiler-floor change and migration,
and is not a claim that either has shipped. Progress lives in
[CLUSTERING_STATUS.md](CLUSTERING_STATUS.md).

### Non-goals (initial releases)

- Usenet **posting** (nzbget doesn't do it either).
- The legacy **BinRPC** binary protocol and `nzbget -C` remote-CLI modes (native CLI talks REST instead).
- Serving the **stock nzbget web UI assets** unmodified (we build our own UI; the shim targets programmatic clients, not `webui/`).
- Importing nzbget's on-disk **DiskState queue files** (we import `nzbget.conf` and accept re-queuing in-flight downloads at migration time).
- GnuTLS, pre-TLS-1.2 protocols, Windows XP-era targets.
- Streaming-mount/WebDAV modes (the altmount/nzbdav direction) — noted as a future direction the architecture should not preclude, not a v1 feature.

## 3. The reference: how nzbget v26 actually works

Condensed from source analysis; this is the behavioral contract the rewrite preserves (or deliberately breaks, with rationale).

### 3.1 Structure

| Subsystem | LOC (cpp+h) | Core files |
|---|---|---|
| queue (model, coordinator, persistence, dupes, history) | 13,114 | QueueCoordinator, DiskState, DownloadInfo, DupeCoordinator |
| remote (RPC, web server) | 8,288 | XmlRpc (4,200), WebServer, RemoteServer |
| postprocess | 8,555 | PrePostProcessor, ParChecker, UnpackController, DirectUnpack |
| nntp (download engine) | 4,301 | ArticleDownloader, ArticleWriter, ServerPool, Decoder, StatMeter |
| extension | 4,091 | ExtensionManager, ScriptController protocols |
| main / util / connect / feed / frontend | ~22,000 | Options (152 option names), Scheduler, Connection/TlsSocket, FeedFilter |

External components: **rapidyenc** (SIMD yEnc decode), **par2cmdline-turbo** (linked as C++ lib, driven via `Par2Repairer`), **Boost.Json**, libxml2 (SAX NZB parse), OpenSSL, external `unrar`/`7z` binaries.

### 3.2 Semantics that must be reproduced exactly

These encode two decades of Usenet-reality hardening. They are the spec; file references point into the C++ source for the port.

**Server failover ladder** (`ArticleDownloader::Run`, `ServerPool`): servers have config `Level` (normalized to contiguous tiers 0..N), `Group` (same-level servers treated as interchangeable — failing one skips the whole group), and `Optional` (fill servers whose blockage never stalls progress). Per attempt: connect/transfer errors → retry same server indefinitely (server blocked for `RetryInterval`=10 s); NNTP 4xx "no such article" or CRC error → mark server failed for this article, move on; other failures → decrement retries (`ArticleRetries`=3) then mark failed. All servers at a tier exhausted → next tier. All tiers exhausted → article failed. Per-server `Retention` (days) pre-fails old articles. Connections are pooled, held idle 5 s, selected randomly among free candidates at a tier.

**Health model** (`DownloadInfo.cpp`): per-mille values. `Health = (size − parSize − (failed − parFailed)) × 1000 / (size − parSize)`, clamped to 999 if any non-par bytes failed. `CriticalHealth = (size − 2·goodParSize) × 1000 / (size − goodParSize)` (0 when par ≥ half; 850 as estimation fallback). Health < critical → par-check is pointless → fail/delete/park per `HealthCheck` option.

**Delayed par download**: on NZB add, extra `.volNNN+MM.par2` files are queued **paused**. When repair needs blocks, the smallest set of paused par files covering the needed block count is unpaused with force priority (three widening name-match passes; block counts parsed from `volXX+NN` filenames), and the checker waits for their arrival to continue.

**Disk strategy**: `DirectWrite=yes` default — preallocate the output file sparse (POSIX `truncate`), seek-write each decoded segment at `yEnc begin−1`; gaps zero-filled at assembly. Optional RAM `ArticleCache` (default off) with background flush at 90% budget. Per-segment temp files only as fallback. Final commit = atomic rename. Per-segment CRC32s are stored and combined (`crc32_combine`) into whole-file CRCs — these power par2 **quick verification** without re-reading data.

**Post-processing state machine** (`PrePostProcessor`): not a fixed sequence — a status-flag-driven loop: par-rename → par-check/repair → rar-rename → (unpack | cleanup | move | post-unpack-rename | scripts), with unpack success re-triggering par-rename on extracted files, and unpack failure re-triggering par-repair (`RequestParCheck`, force repair, second unpack attempt). Parallelism strategies: `sequential` (1 job), `balanced`, `aggressive` (3 jobs, 1 par), `rocket` (6 jobs, 2 par). Direct unpack streams `unrar x -vp` volume-by-volume during download via stdin feed, aborts on any failed article, and its results are used only if no archive remains unprocessed.

**Script protocol** (`ScriptController`): spawn via fork/exec with env-var interface — `NZBOP_*` (all options), `NZBPO_*` (extension's own options), `NZBPR_*` (NZB params), `NZBPP_*`/`NZBNP_*`/`NZBNA_*`/`NZBSP_*`/`NZBFP_*`/`NZBCP_*` per script type. Stdout lines `[INFO]`/`[WARNING]`/`[ERROR]`/`[DETAIL]`/`[DEBUG]`; command channel via `[NZB] KEY=value` lines. Exit codes (post-processing): **93**=success, **94**=error, **95**=skip, **92**=request par-check. Extension metadata: v2 `manifest.json` or legacy `### NZBGET ... SCRIPT ###` comment headers.

**RPC surface** (`XmlRpc.cpp`): 56 method names over XML-RPC (`/xmlrpc`), JSON-RPC 1.1-flavored (`/jsonrpc`), and JSON-P (`/jsonprpc`); GET allowed only for "safe" read methods; `system.multicall` on XML-RPC. 64-bit sizes serialized as `*Lo`/`*Hi`/`*MB` triplets. `editqueue` takes ~50 string action names. Auth: HTTP Basic, URL-embedded `user:pass` path segment, or `X-Auth-Token` cookie; three tiers (Control / Restricted / Add-only); infamous defaults `nzbget`/`tegbzn6789`.

**What Sonarr/Radarr actually call** (verified in live Sonarr source, `NzbgetProxy.cs`): exactly seven methods — `version` (gates: ≥12 required, ≥16 for modern append), `append` (v16 form, returns int NZBID; tracked via a `drone`=hex-id PP-parameter, priorities −100…900), `status`, `listgroups`, `history`, `config`, `editqueue` (only `GroupSetParameter`, `GroupFinalDelete`, `HistoryDelete`, `HistoryRedownload`). JSON-RPC over POST to `/jsonrpc`, HTTP Basic. **This is the adoption-critical surface.**

### 3.3 Defaults worth keeping (users' mental model)

`ArticleRetries=3` · `RetryInterval=10s` · `ArticleTimeout=60s` · `DirectWrite=yes` · `ArticleCache=0` · `WriteBuffer=0` · speed window 30×1 s · connection idle hold 5 s · `UrlConnections=4` · `CrcCheck=yes` · `ContinuePartial=yes` · `PropagationDelay=0` · `DiskSpace=250MB` min-free · `ParQuick=yes` · `PostStrategy=sequential` · control port 6789.

## 4. Design principles

1. **Async task-per-connection, not thread-per-article.** One tokio task owns each NNTP connection's protocol state; article work is dispatched to connections, not connections to articles. Scales to hundreds of connections in one process at near-zero idle cost.
2. **Single-writer state, message passing.** The engine's queue state has exactly one owner task ("actors without a framework": plain tokio tasks + `mpsc` commands + `oneshot` replies + `watch`/broadcast events, per the canonical Ryhl pattern). No global mutex; readers get cheap immutable snapshots via `arc-swap`.
3. **The hot path is a pipeline, not a lock party.** bytes → TLS → decoder (in-place, SIMD) → disk writer are connected by ownership transfer of buffers, with backpressure via bounded channels.
4. **Every boundary is a trait.** `NntpTransport`, `ParEngine`, `Extractor`, `ScriptRunner`, `Clock` — swappable for tests, subprocess-vs-FFI-vs-native swappable in production.
5. **Compat is quarantined.** All bug-for-bug nzbget emulation lives in one crate (`nzbd-compat`) that adapts the native API. The core never learns what a `SizeLo` is.
6. **Crash-only design.** Every state mutation is either recoverable from the journal or explicitly ephemeral. Clean shutdown is just a fast crash with a flush.
7. **Cancellation-safe by construction**: `CancellationToken` trees + `TaskTracker`; every subprocess has a timeout, an output cap, and a kill-on-drop guard.

## 5. System overview

```
                                ┌────────────────────────────────────────────────┐
                                │                  nzbd daemon                   │
  Sonarr/Radarr ── /jsonrpc ──▶ │ ┌──────────┐   commands (mpsc)  ┌────────────┐ │
  curl/UI ──────── /api/v1 ───▶ │ │ nzbd-api │ ──────────────────▶│            │ │
  Web UI ───────── /  (SPA) ──▶ │ │  (axum)  │ ◀────────────────  │   engine   │ │
  scripts/watch dir ──────────▶ │ └──────────┘  snapshots (arc-   │ (queue own-│ │
                                │      │        swap) + events    │  er task)  │ │
                                │      ▼ SSE/WS  (broadcast)      └─────┬──────┘ │
                                │                                       │ per-   │
                                │ ┌───────────┐  ┌────────────┐         │ server │
                                │ │ state     │  │ post-proc  │   ┌─────▼──────┐ │
                                │ │ journal + │◀─│ orchestr.  │◀──│ conn pools │ │
                                │ │ SQLite    │  │ par/unpack/│   │ (tasks per │ │
                                │ │ history   │  │ scripts    │   │ connection)│ │
                                │ └───────────┘  └─────┬──────┘   └─────┬──────┘ │
                                │                      ▼                ▼        │
                                │              subprocesses      decoder→writer  │
                                │              (par2, unrar, 7z, (yEnc, CRC,     │
                                │               extensions)       DirectWrite)   │
                                └────────────────────────────────────────────────┘
```

Data flow for one segment: engine leases `(segment, tier)` to a server pool → an idle connection task issues `BODY <msgid>` (optionally pipelined) → response bytes stream through the incremental yEnc decoder (in-place, dot-unstuffing aware) → decoded chunks with `(file, offset)` go to that file's disk-writer → writer seek-writes (or caches) → segment completion event → engine updates counts/health, journal appends, broadcast notifies API subscribers. Article failures walk the failover ladder inside the engine's lease bookkeeping — identical semantics to §3.2, different mechanics.

## 6. Crate map

A Cargo workspace. Fine-grained crates keep compile times sane, make ownership boundaries physical, and let the parser/decoder be reused (or fuzzed) standalone.

| Crate | Purpose | Key deps |
|---|---|---|
| `nzbd-types` | Domain model: IDs, jobs/files/segments, server defs, statuses, health math, per-mille types. No I/O. | serde |
| `nzbd-nzb` | Streaming NZB parser (+ password/category meta, filename deobfuscation heuristics, content hashing for dupes) | quick-xml |
| `nzbd-yenc` | Incremental yEnc/UU decoder: scalar reference impl now; `rapidyenc-sys` FFI feature later; CRC32 + combine | crc32fast |
| `nzbd-nntp` | NNTP protocol: command/response codec, capabilities, AUTHINFO, BODY streaming, dot-unstuffing, COMPRESS DEFLATE (RFC 8054) | tokio, tokio-rustls |
| `nzbd-engine` | Queue owner task, scheduler (failover ladder), per-server connection pools, pipelining, rate limiter, quotas, disk writers, article cache, crash journal | tokio, arc-swap |
| `nzbd-post` | Post-processing orchestrator state machine; `ParEngine` + `Extractor` + `ScriptRunner` traits and their subprocess impls; direct unpack | tokio (process) |
| `nzbd-state` | Persistence: queue snapshot + segment journal, SQLite history, config store | rusqlite (bundled) |
| `nzbd-api` | Native REST `/api/v1` + SSE events + OpenAPI | axum, tower-http |
| `nzbd-compat` | nzbget JSON-RPC 1.1 / XML-RPC / JSON-P shim, auth tiers, field-shape fidelity | axum (router merge), quick-xml |
| `nzbd-config` | TOML config model + validation + `nzbget.conf` importer | toml, serde |
| `nzbd-cluster` | Clustering (CLUSTERING.md): shared-volume election + node registry, epoch fencing, HTTP work-lease protocol, leader scheduler, worker executor runtime, any-node→leader API proxy | axum, tokio |
| `nzbd` (bin) | Daemon: wiring, CLI (`nzbd run/import/status`), signals, embedded UI assets | clap, rust-embed |
| `nzbd-nserv` (dev) | Mock NNTP server for integration tests & benchmarks (nzbget's `nserv` equivalent): scripted articles, injected failures, latency shaping | tokio |

Dependency policy (2026 survey, verified maintenance status): **tokio** (only runtime; io_uring still unstable/file-only — do not architect around it) · **rustls 0.23 + aws-lc-rs + rustls-platform-verifier** (OS trust stores; per-server cipher/verification config parity) · **axum 0.8** (tokio-team; tower ecosystem) · **quick-xml** (pin — fast-moving; event API, not serde-derive, for malformed-NZB tolerance) · **rusqlite bundled** behind a dedicated DB thread (sled is dead; redb viable but hand-rolled indexes) · **crc32fast** (correct ISO-HDLC polynomial — beware `crc32c`, wrong polynomial for yEnc) · **tracing + metrics** · **sevenz-rust2** (pure-Rust 7z; original sevenz-rust is deleted) · unrar/par2 via **subprocess only**. Hand-roll the JSON-RPC envelope: nzbget's dialect is 1.1-flavored (no `"jsonrpc":"2.0"`, positional params, `{version:"1.1", result}` envelope) and jsonrpsee's strict 2.0 server would fight compat at every turn.

## 7. Domain model (`nzbd-types`)

```rust
pub struct JobId(u32);      // dense, monotonic; == NZBID in the compat shim
pub struct FileId(u32);
pub struct SegmentId { pub file: FileId, pub number: u32 }

pub struct Job {                       // == NzbInfo
    pub id: JobId,
    pub kind: JobKind,                 // Nzb | Url
    pub name: String,
    pub category: Option<String>,
    pub priority: i32,                 // -100..=900; >= FORCE (900) ignores pause
    pub dupe: DupeInfo,                // key, score, mode (Score|All|Force)
    pub params: Vec<(String, String)>, // PP-parameters (incl. Sonarr's "drone")
    pub files: Vec<FileEntry>,
    pub totals: JobTotals,             // sizes/articles: total, success, failed, par…
    pub status: JobStatus,             // queued/downloading/post{stage}/history{…}
}

pub struct FileEntry {                 // == FileInfo
    pub id: FileId,
    pub subject: String,
    pub filename: String,              // deobfuscated; confirmed after direct-rename
    pub is_par2: bool,
    pub paused: bool,                  // delayed-par mechanism uses this
    pub segments: Vec<Segment>,
    pub crc32: Option<u32>,            // combined from segment CRCs
}

pub struct Segment {                   // == ArticleInfo
    pub message_id: Box<str>,
    pub number: u32,
    pub size: u32,
    pub state: SegmentState,           // Pending | Leased{server} | Done{crc,offset,len} | Failed
    pub tried: ServerBitmap,           // failed-server set for the failover ladder
}

pub struct ServerDef {
    pub id: ServerId,
    pub host: String, pub port: u16, pub tls: TlsMode,
    pub credentials: Option<Credentials>,
    pub tier: u8,                      // == normalized Level
    pub group: u8,                     // 0 = none; same tier+group = interchangeable
    pub fill: bool,                    // == Optional
    pub max_connections: u16,
    pub pipeline_depth: u8,            // first-class (see §8.3); default 2
    pub retention_days: u32,
    pub cert_verification: CertLevel,  // None | Minimal | Strict
}

pub struct Health(u16);                // per-mille, 0..=1000; formulas from §3.2 as methods
```

Everything is `serde`-serializable; the journal and API reuse these types. Compat translation (Lo/Hi splits, status strings like `PP_QUEUED`, `SUCCESS/ALL`) lives exclusively in `nzbd-compat` mappers.

## 8. Download engine (`nzbd-engine`)

### 8.1 Queue owner

One task owns `QueueState` (jobs, files, segments, lease table). Inputs: a bounded `mpsc<Command>` (Add, Edit, Pause, SetRate, …, each with an optional `oneshot` reply), completion/failure events from connection tasks, and a 1 Hz tick (journal flush, health/quota checks, `PropagationDelay` re-evaluation). Outputs: leases pushed to per-server pool queues, a debounced `arc_swap::ArcSwap<QueueSnapshot>` for lock-free API reads, and a `broadcast<Event>` stream (job added/progress/completed, server state, log). This replaces nzbget's global mutex + observer web with one serialization point that never blocks readers.

**URL jobs** register instantly as `Fetching` placeholders (named through the same junk-stripper the post-fetch metadata pass uses — never the raw URL tail with its glued query string and API key) while a cancel-aware background task downloads the NZB. Adding a URL that is already `Fetching` returns the existing job instead of a duplicate (clients re-add when a fetch looks stalled). On recovery, jobs found in `Fetching` get their fetch re-spawned — the task died with the old process; without this they sit at "0 B of 0 B" forever — one per unique URL, with same-URL pile-ups failed as duplicates of the survivor (field report 2026-07-25).

### 8.2 Scheduling and the failover ladder

Selection preserves §3.2 semantics: highest-priority non-paused job → highest-priority file → next pending segment, with force-priority bypassing pause and quota, `PropagationDelay` filtering young files, and (later phase) direct-rename-first article ordering. The ladder is a pure function — `fn next_action(outcome, segment, servers) -> Lease | Retry{server, after} | Escalate | Fail` — table-driven from the C++ behavior (connect-error vs 430-not-found vs CRC-error vs incomplete), unit-tested against scenario fixtures. Server blocking (10 s), group skip, fill-server semantics, and retention pre-fail are all in this function, not scattered across tasks.

### 8.3 Connection pools and pipelining

Per server: a pool task + up to `max_connections` connection tasks (spawned on demand, retired after 5 s idle, hot/cold distinction so reconnect storms don't hammer providers). Each connection task runs the NNTP state machine: greeting → CAPABILITIES → AUTHINFO → optional COMPRESS DEFLATE → loop { take lease(s), issue `BODY`, stream-decode }. **Pipelining depth is first-class per-server config** — SABnzbd's provider benchmarks show depth 1→2 is the single biggest throughput win, and high-latency links want 10–30; nzbget has none (its thread-per-article model can't). Same-host account dedup: a shared per-endpoint 430-miss cache avoids re-asking a provider that already said "no such article" on another account (idea proven in javi11/nntppool).

### 8.4 Decode and disk

Response bytes are decoded **incrementally in the connection task** (no extra hop): the `nzbd-yenc` streaming decoder handles yEnc header/trailer parse, escape sequences and CRLF/dot-unstuffing across arbitrary chunk boundaries, in-place, with running CRC32. Phase 1 ships a scalar decoder (correct, ~0.5–1 GB/s/core, plenty behind TLS); the `rapidyenc-sys` feature (vendored CC0 sources, `cc` build — the binding doesn't exist in the ecosystem yet, we write it) restores the >4.5 GB/s ceiling and serves as differential-fuzzing oracle for the scalar impl.

Decoded chunks go to a **per-file writer task** (bounded channel = backpressure): DirectWrite default (sparse preallocate via `truncate`, positional writes at `yenc_begin − 1`; on Linux `pwrite` needs no seek serialization), optional RAM article cache with a global budget and 90%-flush policy, temp-file fallback, gap zero-fill + atomic rename at assembly, `fsync` policy configurable (default: dirs on rename, data relaxed). Per-segment CRCs recorded for par2 quick-verify.

### 8.5 Rate, quota, stats

A global token-bucket rate limiter (replacing the C++ sleep-loop): connection tasks `acquire(n)` before each read; changing the limit is instant, fairness is inherent, and per-server buckets compose. Speed metering is **one measurement, attributed twice, normalized by time**: connection tasks record wire bytes per job *and* per server at the single read site; the owner tick drains those counters stamped with the measured wall time they cover and folds them into per-entity EMAs (α = 1/5) at bytes ÷ measured seconds — so a tick that fsync work delayed cannot inflate a rate (2.5 s of bytes counted as "one second" was field report 2026-07-26: a header claiming 24.8 MiB/s over a ~9.9 MiB/s wire). The header `download_rate_bps` is the **sum of the per-server EMAs**, so the tile, the provider chips and the row rates agree to integer rounding by construction rather than by hope; nzbget's 30×1 s window average is retired — two windows over the same counters is two numbers claiming to be the same thing. Per-server volume counters (sec/min/hour/day arrays) and daily/monthly quota with `QuotaStartDay` reproduce nzbget's accounting; quota-reached behaves as pause-except-force. The min-free-disk guard reads a **cached** lowest free-space value across every configured write root. An independent prober starts immediately without delaying daemon startup, runs each admitted statvfs on a detached OS thread with its own two-second response deadline, and starts the next cycle 10 s after the bounded prior cycle finishes; the owner loop itself never touches those volumes. A degraded process-wide permit pool can serialize responsive roots, so recovery evidence can take longer than two seconds without making any individual filesystem call unbounded. A statvfs against a write-saturated FUSE/network destination can block for seconds, and running it inline every 10th tick starved lease handout, sawtoothing throughput ~30% at >100 MiB/s (field report 2026-07-26). Timed-out roots retain conservative last-known data and are not rescheduled until their prior syscall returns. A process-wide 64-call permit ceiling stays held for each detached syscall across hot reload and path churn, so incomplete evidence cannot clear an existing hold and wedged mounts cannot grow blocking work without bound. Every section of the owner tick is now individually timed, and the `engine tick ran long` warning carries the full breakdown (`guards/volumes/publish/journal_sync/finalize/snapshot_save`) — nothing in the tick is unmeasured.

**Storage throughput.** The dashboard's per-filesystem read/write rates come from `nzbd-engine::disk_io`, a sampler on its own OS thread (`nzbd-disk-io`) that wakes once a second, takes the filesystems from the capacity probe's latest reading (their `st_dev`), and reads only procfs: `/proc/self/mountinfo` to find each device's mounts, `/proc/diskstats` for block devices (by major:minor, or by the mount source's device name when `st_dev` is anonymous, as on btrfs), and `/proc/self/mountstats` for NFS (the server-byte columns). A filesystem with none of these (GlusterFS and other FUSE mounts, SMB, overlay) falls back to `nzbd_state::io_meter`, which counts Runner's own bytes per `st_dev` at four points: the article writer, external tools (par2, 7z, unrar) from their `/proc/<pid>/io` `rchar`/`wchar` sampled every 100 ms while they run and charged to the directory they run in (on kernel 7.0, as on the nucs, an exited child's counters are unreadable even before it is reaped, so the last interval of each run is lost; copies made with `splice()`/`copy_file_range()` never reach these counters, which par2, 7z and unrar do not use), the cross-filesystem publication copy and the PAR fingerprint pass, and torrent payload transfer (verified bytes written, uploaded bytes read) on the torrent root's filesystem as the capacity probe last saw it. Nothing it reads touches the storage, so a wedged mount cannot stall it; the owner reads its latest value from an `ArcSwap` when publishing and withholds rates older than five seconds as `stale`. Each row reports its `source` (`block`, `nfs`, `runner`, `stale`) so the UI can say whether a rate is device-wide or this machine's share. The rate is the counter delta over measured wall time; a counter that goes backwards (device replaced, remount) yields no rate for that sample. History is kept in the browser, the same three minutes as the rate sparkline.

### 8.6 Crash recovery

The queue snapshot contains jobs, segment outcomes, unresolved recovery evidence and durable file proofs. The append-only per-job journal retains segment ranges and optional provenance; older records remain readable and require verification. A clean-shutdown marker is diagnostic, never proof that payload data is intact. History uses SQLite WAL and its append-only JSONL source of truth.

The process starts a local monitor before reading configuration or storage. Once
authentication and TLS are known, a persistent public control listener serves
the UI, logs, shutdown and component status while backend metadata loads.
Metadata-dependent routes return retryable 503 responses during that interval.
Missing configuration or mirrors preserve an established secure identity;
default no-TLS/no-auth startup is allowed only when first install is proven.
Readiness means the secure control listener responds; it does not require every
download to be runnable. See [deployment probes](DEPLOY.md#container-health-and-discovery-startup)
and the [reviewed recovery plan](STARTUP_VERIFICATION_PLAN.md).

Recovery loads queue metadata before touching payload roots. Root-scoped workers
then reconcile lifecycle state and establish allocation capabilities
independently, so a blocked mount does not hold engine construction or another
root's metadata progress. The owner captures startup artifact generations,
revisions and existing journal fingerprints before admitting runtime work.
Root replay is limited to that snapshot; delayed startup work cannot classify
new runtime allocations or operations as crash leftovers. Typed article-range
workspaces allocate below their fenced nested parent. A subsequent range uses
a fresh allocation generation, preserving the previous workspace's bytes and
custody; preparation remains inside the engine's bounded payload lane.
Verification uses bounded batches (up to 64 ranges,
32 MiB, or 250 ms), retaining a cursor bound to the observed file identity
between batches. The owner publishes checked-byte progress and can admit newly
eligible higher-priority work before a yielded job resumes; manual pauses
remain effective. Current writer proofs bind installation, allocation and file
generation, descriptor identity, and exact immutable ranges. Legacy ranges
require CRC reads and a successful descriptor sync before receiving such a
proof. Protocol 1 permits metadata-only recovery on recognized local filesystem
identities; unqualified network/FUSE mounts, including GlusterFS, continue
background range verification because deployment-specific identity and flush
guarantees have not been established. Missing data, mismatches, identity
conflicts and unavailable storage remain distinct outcomes; unavailable I/O
never erases Done evidence.

The queue owner captures immutable documents and sends persistence requests to
one bounded, ordered filesystem lane, independent from bounded payload-I/O
workers. Durability-sensitive transitions resume only after a completion
acknowledgement; enqueueing a request is not a successful save. Snapshot commit
compacts only the exact journal incarnation and cutoff captured by that
document, before later appends. Non-authority cluster workers write a private
node checkpoint for generation admission; they never overwrite the shared
queue or compact foreign journals. A fenced or failed save retains journals.
Late writer, verifier and post-processing results are checked against current
owner generations before they can change state.

Post-processing updates retain their `PostAdmission` scope while deferred and
apply only to the matching job lifetime. Replacement imports persist as one
acknowledged document, without a durable remove/insert gap. Resume and torrent
Start set an exact pending control-revision fence; lease and backend scheduling
wait for the matching durable save acknowledgement, so an older completion
cannot release a newer Pause or Resume. Inventory discovery and review actions
reserve the exact artifact generation, perform payload probes outside the
global mutation coordinator, then recheck revision before committing.
Runtime transforms, relocation, deletion and interrupted-operation replay use
this same boundary. Their generation reservations retain namespace custody
until actual worker completion; new scratch joins that reservation in the
transaction that records it. After a local completion is durably stamped,
post-processing joins its workers and releases the source reservation before
retrying that exact generation's journaled publication cleanup. Ownership,
identity, retention and delivery guards still decide whether deletion may
proceed. A failed snapshot restores prior values while preserving only operator
intent changed after the provisional record was installed. Accepted cluster publications use the same owner
payload lane: a durable intent binds the verified sealed result, old directory,
building directory and backup identities. Selection commits a fresh inventory
generation while retaining prior custody. History and the canonical queue
replacement complete before the owner releases operational fences. Replay
requires the accepted control result and current authority checkpoint, and
never adopts a replacement inode merely because a marker exists.
See [startup verification status](archive/STARTUP_VERIFICATION_STATUS.md).

Shutdown stops intake, cancels workers, consumes their remaining results, and waits for actual worker termination before saving the final document and clearing the diagnostic marker. A failed drain or final save prevents a replacement backend from being started in the same process.


**Naming an obfuscated job.** A job's name has three possible sources, and they are consulted in order of how much they actually know. (1) **The hint** — what the client called it. It wins whenever it carries information; `queue::is_uninformative_name` rejects hashes, opaque 16+ character tokens with no separators, and posting furniture like `yEnc`. (2) **The NZB's own evidence** — `<meta name>` title, par2 recovery-set base, common payload stem. (3) **The requestor** — who asked and from where (`monarr · drunkenslug · cc310b99`), which is a *provisional* name: it says why the job is here, not what it is. Field report 2026-07-29, job #182: a 4.8 GiB download whose title, every payload filename and every par2 filename were random tokens, so (1) and (2) had nothing and the queue showed a 40-character hash — "that is not useful at all".

The real answer arrives during the download. **par2 FileDesc packets carry the true source filenames**, and the recovery index is small and lands early — #182's was 20.5 KiB and finished one minute into a multi-hour download. So `Owner::on_writer_finalized` content-sniffs every completed file for the `PAR2\0PKT` magic (never the `.par2` extension: an obfuscated post hides its par2 files exactly like everything else, which is why `FileEntry::is_par2` — a guess off the NZB subject — is `false` for precisely the files that matter), reads it on a blocking thread, and renames the job from `nzbd_par2::scan`. The parser is the leaf crate `nzbd-par2` because `nzbd-post` depends on `nzbd-engine`, so the engine cannot reach the PP copy; one parser, two callers.

Two invariants make that rename safe. **`Job::name` and `Job::dir_name` are different things**: the display name may improve, the storage directory is fixed at admission and never moves, because `Owner::writer_for` recomputes its target directory from the job every time a writer spawns — a rename that moved it would split a job's files across two directories. Post-processing reads its *input* directory from `dir_name` and moves the finished job to a destination named from the *display* name, so the good name still reaches the disk. And **`Job::name_provisional` is a flag, not an inference**: a good placeholder is indistinguishable from a real title by inspection — `monarr · drunkenslug · cc310b99` reads as informative, which is the point of it — so a gate that asked "does this look like junk?" would refuse the real name when the par2 index finally supplied it.

**Durable history transitions.** JSONL is authoritative in both directions:
bare `HistoryEntry` lines add/update a completion, while tagged tombstones
forget one immutable `(job, completed_at)` key. SQLite is only the local read
model; its `history_tombstones` table is rebuilt from those portable mutations.

| Transition | Portable JSONL fact | Rebuild result |
|---|---|---|
| `record` | Append entry | Completion exists |
| `hide` / `restore` | Re-append changed entry | Latest visibility wins |
| `delete` | Append `{ "op": "tombstone", … }` before deleting SQLite | Completion stays absent on every upgraded node |
| `mark_seen` | None; advisory and node-local | Observation may reset |

Tombstones are monotone because per-node file order and wall clocks cannot
provide a trustworthy cluster-wide “last line.” A later completion has a new
`completed_at`; the same key cannot be revived. Old binaries skip the tagged
mutation as an unknown line, so a rolling cluster must finish upgrading before
it relies on a new forget. See
[DEFECT_HISTORY_DELETE.md](archive/DEFECT_HISTORY_DELETE.md) for the defect and decision.

**History loading and retention.** Native and compat listings read a local
SQLite WAL snapshot; native page rows and total share one read transaction.
Standalone stores explicitly opt into one-writer ownership and skip routine
post-startup replay, retaining repair after failed local publication. Shared
stores have one runtime-owned worker, a whole-pass synchronization gate,
completion-based throttling, per-file incremental contributions and complete-line
offsets, conservative fingerprints with periodic full verification, and bounded
replay transactions. Offsets publish only after associated commits; restart
reconstructs the cache. See [HISTORY_INCREMENTAL.md](HISTORY_INCREMENTAL.md). Readers use a separate SQLite
connection, and consumer observations defer when replay owns the writer.
Pause/resume and worker I/O/freshness/placement are visible in the History tab.
`history.index_dir` permits an index copy to persistent local storage without
moving logs or parked NZBs; a registered active path prevents selecting an
abandoned copy. The actual mount classification is reported rather than assuming
`state_dir` is local. See [HISTORY_LOADING_PLAN.md](HISTORY_LOADING_PLAN.md).

Retention remains a storage and recovery bound (defaults 1000 entries / 90
days; `0` disables a bound, both apply and the first to bite wins; NZBGet's
`KeepHistory` imports onto `keep_days`). A trim does three things together, and any one alone leaves the entry half-deleted: it deletes the index rows, **compacts this node's own JSONL** to the surviving keys, and raises a monotone `retention_floor` watermark that ingest honours — without the watermark the next refresh re-imports what the trim just dropped. A node rewrites only the file it alone appends to (`history.<node>.jsonl` in a cluster), never a peer's; the swap is tmp+rename so a concurrent reader sees the whole old file or the whole new one; and an in-process lock serializes it against this node's own appends. Every surviving entry line is kept, not just the newest per key — the reader merges `removed_at`/`picked_up_by` with `COALESCE`, so collapsing a key's lines would change what a rebuild reconstructs. Tombstones are also kept even when their covered entry lines disappear: a peer may retain an old copy indefinitely, so compaction cannot prove the delete evidence is globally safe to drop.

**What history keeps.** A history entry used to be a summary — name, status, size, health, params — and everything else the queue knew was dropped when the job left it: the file table, the article counts, which indexer it came from, who asked for it, how long it took. Those are exactly the questions asked *afterwards* ("which file was short?", "was this the release I think it was?", "where did I get this?"), so a record that answers them only while the job is still in the queue answers them at the one time nobody is asking (field report 2026-07-29). `HistoryEntry::record` is an optional `JobRecord` captured from the `Job` as it leaves: per-file rows (name, size, segments done/failed, par2 flag), article totals, par size, source URL, requesting client, queue-entry time, the storage directory, and the `original_name` the job was admitted under when it later renamed itself — so an *arr that added `cc310b99…` can still find the row. `None` on entries written before it existed, and the upsert merges it with `COALESCE`, so a later `hide` re-append (which carries no record) cannot erase one.

**Re-downloading from history.** `spool_nzb` had exactly one caller — the delete-park path — so a job that *finished* kept no requeue source and reported `can_requeue: false` forever. Post-processing now parks every finished job's regenerated NZB alongside the history entry, **gzipped**: an NZB is XML and compresses about ten to one, which is what makes one per job affordable (a thousand entries costs a couple of hundred MB rather than a couple of GB on the state volume). `read_spool` sniffs the gzip magic rather than trusting the name, so spools written before compression still requeue with no migration. The spool is reaped with its entry, so history retention bounds it for free. `queue::job_to_nzb` moved down into `nzbd-engine` for the same reason `nzbd-par2` exists: `nzbd-post` needs it and cannot reach `nzbd-api`.

`GET /api/v1/history` pages server-side (`?limit=`/`?offset=`, with `total` in the response) because the cost paging bounds is server-side: the per-row work to derive `can_requeue` is a spool lookup each, which on a network mount was the bulk of a warm read (250 ms / 179 rows). That lookup is now ONE directory listing per request, cached briefly; the single-row `has_spool` remains exact for the requeue path itself, because a display snapshot and an answer someone acts on are not the same claim. The `?since_seq=` cursor shape is untouched and ignores `offset` — a cursor already carries a position.

## 9. Post-processing (`nzbd-post`)

The orchestrator is an explicit state machine — same effective stage graph as §3.2 (par-rename → par-check/repair → rar-rename → unpack ↔ repair retry loop → cleanup → move → post-unpack-rename → scripts), driven by typed stage results instead of status-flag polling. Job admission reproduces the four `PostStrategy` levels. Queue-pause-during-PP options carry over.

**Failed-file disposition — bounded only when local failure is stable.** A health-failed job persists its logical completion timestamp before Park/Delete, then records history and stamps `*PP:done` only after the configured file action is externally observable. A stable local error gets three attempts: immediately, then on the next two 30-second rescans, so an uninterrupted daemon exhausts the bound in about 60 seconds. The count shares the queue row's durable snapshot with `*PP:failure-at`, so a restart resumes rather than resets it. One occasion spends one attempt: the finish event and the queue scan routinely observe the same still-retryable row — it carries no `*PP:done` stamp for the overlap guard to skip on — so the counter is debounced to one increment per rescan period. Counting each observation instead spent two of the three attempts in the same instant, which made the bound nondeterministic and retired jobs in half the documented window. The debounce period begins at the *durable* increment, never at the attempt to make one: a counter commit that does not reach the queue snapshot restores the previous row and has spent nothing, so the next observation retries immediately rather than losing an attempt to a write that never happened. Exhaustion records the observed `delete failed: ...` or `park failed: ...` note and leftover-file path without claiming that the configured action succeeded, then ends the rescan loop. ENOSPC/EDQUOT instead raises the disk hold and remains unbounded, while a lost cluster authority or admission gate returns without spending an attempt — recovery, not a misleading terminal success, is the only valid outcome for those transient states.

**par2 — `ParEngine` trait, subprocess-first.** nzbget links par2cmdline-turbo as a C++ library and drives `Par2Repairer` directly; there is **no stable C API**, so FFI means maintaining a C++ shim — the worst option. SABnzbd's model (shell out to `par2cmdline-turbo`, which publishes per-platform binaries) is battle-tested and crash-isolated; we ship that first, bundling/locating the binary like `UnrarCmd`. The trait boundary is designed for the one thing subprocess can't do: **quick verification** stays native — we already hold per-segment CRC32s and combined file CRCs from download, so "is this file intact?" is a CRC-combine against par2 packet checksums (pure Rust, we parse par2 packets ourselves — packet parsing is simple; GF(2^16) repair math is the hard part we delegate). Delayed-par fetching (§3.2) is an engine command (`UnpauseParBlocks{count}`) issued by the orchestrator. A native-Rust repair engine is a swap-in later (four young 2025–26 pure-Rust par2 projects exist; none yet trustworthy).

**Unpack — subprocess, hardened.** `unrar x -y -p… -o+ <archives> <dir>/` and `7z x …` with nzbget's exit-code maps (unrar 5=disk-space, 11=wrong-password; 7z requires "Everything is Ok"), password-file retry loop, `.001` split joining, cleanup rules, temp-unpack-dir + move-back. Hardening beyond nzbget: argv-only (never shell), non-inherited env, kill-on-drop with timeout, bounded captured output, and unpack into a same-filesystem staging dir with symlink-escape checks (nzbget had a symlink TOCTOU fix as recently as v26.2 — we design it out). Direct unpack ports the `-vp` volume-pause trick: feed `\n` on stdin as each volume completes; abort on first failed article; PP validates coverage before trusting results. 7z can later switch to in-process `sevenz-rust2` behind the same `Extractor` trait.

**Scripts — protocol-compatible.** `ScriptRunner` reproduces the env-var interface, `[LEVEL]` stdout parsing, `[NZB] KEY=value` command channel, exit codes 92–95, per-type prefixes (`NZBPP_`, `NZBNP_`, `NZBNA_`, `NZBSP_`, `NZBFP_`, `NZBCP_`), option export (`NZBOP_*`/`NZBPO_*`), and queue-event filtering with `EventInterval` throttling. Discovery reads both v2 `manifest.json` and legacy `###`-header formats. Result: the existing extension ecosystem (VideoSort, notification scripts, …) runs unmodified. Native additions (webhooks, an SSE event feed for sidecar processes) come later without breaking this.

## 10. API layer

### 10.1 Native API (`nzbd-api`)

`/api/v1` REST + `/api/v1/events` SSE, OpenAPI-documented, JSON in domain-model terms (real 64-bit integers — no Lo/Hi). Sketch:

```
GET  /api/v1/status                    queue+speed+disk summary
GET  /api/v1/jobs?state=queued         list (paged)         POST /api/v1/jobs   (multipart NZB | {url})
GET  /api/v1/jobs/{id}                 detail w/ files      PATCH /api/v1/jobs/{id}  (priority, category, pause…)
POST /api/v1/jobs/{id}/actions/{a}     retry|park|reprocess|move-top|…
GET  /api/v1/history?since=…           SQLite-backed        DELETE /api/v1/jobs/{id}?final=true
GET  /api/v1/servers · POST /api/v1/servers/{id}/test       per-server config + live probe
GET  /api/v1/config · PUT /api/v1/config                    typed, validated, atomic apply-or-reject
GET  /api/v1/logs?level=…&job=…        ring buffer + per-job
GET  /metrics                          Prometheus
```

SSE events mirror the engine broadcast — every variant crosses under its own `event:` name (`job_added`, `queue_pause_changed`, `speed_limit_changed`, …), never an opaque catch-all, because UI refresh nudges key on those names. Three events are the stream's own rather than the engine's:

| Event | Payload | Why it exists |
|---|---|---|
| `tick` | `{status, jobs}` at 1 Hz | Progress, rates and ETAs have no discrete event. Serialized from **one** snapshot, so the header rate and the per-job rows can never disagree. Deduplicated while the queue is idle, so a quiet stream costs nothing but keep-alives (battery and radio on phone PWAs). |
| `hb` | `{now_unix}` | That dedup makes client-side staleness detection impossible: an idle queue and a dead stream look identical. When a tick is suppressed as a duplicate and nothing has gone out for 5 s, `hb` says "still here" without shipping a read model. Axum's `KeepAlive` comment lines stay for proxies; `EventSource` cannot see them by design, which is exactly why `hb` is needed. |
| `log` | `{entries, dropped}` at 1 Hz | Daemon log lines batched onto the same loop — a `tail -f` that updates once a second is the right cost/fidelity trade, and batching keeps per-connection state to a single cursor. Capped at 200 lines per frame; `dropped` reports what the cap cut, and a fresh stream starts from the newest id rather than replaying the ring (`GET /api/v1/logs` is the backfill). Records carry `scope` and `job` already, so the client filters and the server grows no per-connection filter state. |

**Post-processing events.** `job_finished` fires when the *download* completes; everything after it — verify, repair, unpack, move, scripts — used to exist only as log lines, so a consumer could see that a job had stopped downloading and nothing else. `job_pp_stage {job, name, stage}` marks each stage transition, and `job_pp_finished {job, name, category, pp_status, final_dir, size_bytes, health, params, history_seq}` reports the outcome and, critically, **where the files are**. Post-processing publishes through `EngineHandle::emit` onto the engine's own broadcast rather than a second bus: two buses would mean two orderings, and the ordering is the point.

That ordering is normative. `job_pp_finished` is emitted only *after* `HistoryDb::record` returns, so a consumer may react to the event and immediately `GET /api/v1/history?since_seq=<history_seq - 1>` and be certain the row is there. Emitting before the write would hand every consumer an invisible race against ours. The terminal paths that never reach post-processing at all — the critical-health gate, a failed URL fetch — emit it too, carrying their existing `FAILURE/HEALTH` / `FAILURE/FETCH` status strings: an event that *usually* fires is worse than no event, because it teaches consumers to add a timeout instead of trusting it.

**Resume protocol.** The engine's `broadcast` channel is lossy by design — a slow subscriber is dropped rather than allowed to stall the queue owner — which is right for a browser tab and wrong for a consumer that imports files. Every engine frame therefore carries `id: <boot>-<seq>`, stamped by a single pump task in `nzbd-api::eventhub` so that all consumers see the same numbers in the same order; the monotone `seq` also rides in the body. The epoch is not decoration: `seq` is process-lifetime and restarts at 1, so a bare number cannot distinguish "you missed events 51-60" from "we restarted and these are a *different* events 51-60" — and the second one served as a clean resume is silent corruption of the consumer's view rather than a visible gap. `Last-Event-ID` is opaque to SSE, so carrying the epoch in it costs nothing. The last 1024 frames are kept; a client reconnecting with `Last-Event-ID` gets exactly the missed tail replayed before live frames resume, and a gap the ring cannot cover — or an id from any other process — answers `event: reset {"reason":"gap"}`. `tick`, `hb` and `log` carry no `id:`: they are the stream's own views, not engine facts, and resuming from one would mean nothing.

The one loss the design could otherwise not signal is the pump itself falling behind the engine: those events are dropped *before* they are numbered, so no seq gap appears to betray them. The pump therefore publishes its own `lagged {skipped}` **as an event** — it takes a seq, lands in the ring, and reaches live and replaying clients alike, so the invisible failure becomes the loudest one. (A per-connection `lagged`, meaning that one SSE stream fell behind the hub, is separate and unnumbered; the client's `Last-Event-ID` covers it on reconnect.)

The ring is a convenience, never the recovery path. `GET /api/v1/history?since_seq=N` (oldest-first, hidden rows included, `seq` = the local SQLite rowid) is what makes loss harmless: a consumer that was away for a week reconstructs everything from the cursor alone. The rowid is deliberately **not** written into the append-only JSONL — that log is portable and gets re-imported into indices that assign their own rowids, so publishing one node's numbering would be publishing a number that is wrong everywhere else. `seq` is an opaque "newer than this" token against the node you are talking to.

**Add-time params.** `POST /api/v1/jobs?params=<url-encoded JSON object>` sets job parameters in the same owner-loop turn as the add, so a consumer's tracking id is on the job from its first appearance in the queue and reaches the queue view, history and the compat `Parameters` array through the plumbing that already existed — one write, not a second path. Keys starting with `*` are rejected `422` by name: that prefix is nzbd's internal namespace (`*PP:done`, `*URL`, `*Unpack:Password`), and a client able to write there could forge post-processing state.

Two response shapes exist for the UI's undoable delete. `POST /api/v1/jobs/{id}/actions/delete` (and `delete-files`) answers `{"ok":true,"parked":bool}`: before deleting, the API exports the job and spools its regenerated NZB beside the history index, then writes a `DELETED` history entry — so `POST /api/v1/history/{id}/actions/requeue` can put it back (`200 {"id":u32}`, `404` if the entry or its spool is gone). History entries carry a derived `can_requeue`, which answers "is the requeue source still on *this* node?" — the spool is local, not cluster state. The engine's `delete_job` is untouched by all of this; single-writer authority is not renegotiated by a UI feature. Caching discipline: every live JSON response is `Cache-Control: no-store` (a router-wide default layer — some browsers heuristically cache header-less `fetch()` GETs, which froze the dashboard until a manual reload; field report 2026-07-25), the UI shell is `no-cache` (revalidate), and only the immutable PWA icons carry `max-age`. Auth: bearer tokens (argon2-hashed at rest) with role scopes replacing the three-tier model; the UI uses a session cookie. TLS via rustls with the same cert options as the compat surface.

### 10.2 Compat shim (`nzbd-compat`)

An axum sub-router mounted at `/jsonrpc`, `/xmlrpc`, `/jsonprpc` (plus GET-style `/{proto}/{method}/{params}` for safe methods), speaking nzbget's JSON-RPC 1.1 dialect (`{"version":"1.1","result":…}`), XML-RPC with `system.multicall`, HTTP Basic + URL-embedded `user:pass` + `X-Auth-Token` cookie, and the Control/Restricted/Add tiers mapped onto native roles. `version` reports a configurable compat string (default `"26.2"` — Sonarr gates on ≥12/≥16 and parses `-testing` suffixes; honesty lives in `sysinfo` and the native API).

Implementation phases:

| Phase | Methods | Unblocks |
|---|---|---|
| C1 | `version`, `append` (all three historical arg-order forms, incl. v25.3 `AutoCategory`; returns int NZBID), `status`, `listgroups`, `history`, `config`, `editqueue` {GroupSetParameter, GroupFinalDelete, HistoryDelete, HistoryRedownload, GroupPause/Resume, GroupSetPriority/Category/Name, HistoryReturn/Redownload/RetryFailed} | **Sonarr, Radarr, Prowlarr, Lidarr, Readarr, Mylar** |
| C2 | `listfiles`, `pause*/resume*` family, `rate`, `scheduleresume`, `writelog`/`log`/`loadlog`, `scan`, `saveconfig`/`loadconfig`/`configtemplates`, `shutdown`/`reload`, remaining `editqueue` actions | nzb360, NZBGet mobile apps, scripts |
| C3 | `servervolumes`/`resetservervolume`, `editserver`, `testserver*`, `sysinfo`, `systemhealth`, history dup entries, extension RPCs | stat dashboards, full webui-class clients |

Field fidelity is enforced by **golden tests**: recorded responses from a real nzbget 26.2 instance (running against `nzbd-nserv` fixtures) are diffed structurally against shim output — every field name, `*Lo/*Hi/*MB` triplet, deprecated alias (`FirstID`/`LastID`), and status string (`PP_QUEUED`, `SUCCESS/ALL`, `DELETED/DUPE`, …) must match. The shim translates; it never computes.

## 11. Configuration (`nzbd-config`)

Native config is **TOML** (`nzbd.toml`): typed, serde-validated, sectioned (`[paths]`, `[[server]]`, `[[category]]`, `[queue]`, `[post]`, `[security]`, `[[task]]`), env overrides (`NZBD__SERVER_1__HOST`-style), secrets referencable from files. Applied via the native API atomically (validate → swap → notify subsystems through their command channels; no restart for most options — same live-apply set as nzbget or better).

**Secrets are plaintext in the file, and the mask is not one (2026-07-26).** Server passwords live in `nzbd.toml` as written; there is no file or env indirection for them (only `[cluster]` has `secret_file`). The settings editor never shows them — `GET /api/v1/config` returns `mask_secrets()`, which substitutes `SECRET_MASK` (`***unchanged***`), and a save runs `merge_masked_secrets()` to put the real values back from the running config, matching servers by name then index. That round trip had two silent holes, both closed. A mask the merge could not resolve — a renamed server, a newly added one, a running config that never held that secret — was written straight through as `None`: the password was *deleted* and the save returned 200. Unresolvable masks are now left in place, `merge_masked_secrets` returns the list of them, and `PUT /api/v1/config` answers 422 naming the field to retype. And nothing on the boot path objected to a config that still contained the mask, so a file restored from a copy taken out of the editor left the daemon authenticating with the literal string `***unchanged***` against a healthy account — the symptom (connection failures) pointing three days away from the cause. `Config::validate()` now rejects any secret equal to the mask and says where the file came from; `Config::parse_toml_unvalidated()` is the one seam that skips it, for the editor's merge path only. The masked text is a display, not a backup: the editor's Download hands it back as `nzbd-masked.toml` with a header saying so, rather than as the `nzbd.toml` you would drop into place.

**Config durability (2026-07-26).** The configuration is a *replica*, not a singleton: every successful write — first-run wizard and settings editor alike — also drops a copy at `<state_dir>/nzbd.toml.saved`, and a boot that finds no config file recovers from that copy before it will serve the first-run wizard. The reason is that a container's config directory is writable whether or not anything is mounted at it. When nothing is, saves succeed into the image's writable layer and are destroyed by the next recreate, so a configured, downloading install reverted to an unconfigured one on every deploy — and the page blamed the config path rather than the missing mount. The state directory does not have this failure: it is on the data volume, the mount whose contents the queue itself proves persist. Recovery restores the file, keeps running, and says loudly — boot log and a UI banner — that the config directory is not keeping what is written to it. `NZBD_MAIN_DIR` (set by our image to the declared `VOLUME`) is how the search finds the data volume with no config to read `paths.main_dir` from. When set, it is authoritative: recovery does not probe the user's default directory or `/data`, which prevents one explicitly isolated process or test from adopting another installation's mirror. `/proc/self/mountinfo` is how `durability()` distinguishes "on a volume" from "on the container's own layer" and warns *before* the first loss. Deliberate resets delete the mirror too — the boot log names the file.

`nzbd import-config /path/nzbget.conf` maps the 117 scalar options + `ServerN.*`/`CategoryN.*`/`TaskN.*` blocks onto the native model with a printed report (imported / renamed / dropped-with-reason). Scheduler tasks (`TaskX.Time/WeekDays/Command/Param`, cron-like with the same commands: pause windows, rate windows, server activation, script runs) become `[[task]]` entries executed by a small scheduler service in the daemon. The shim's `config`/`saveconfig` (phase C2) operate on a **projection**: native options exposed under their nzbget names for clients that edit config remotely, with unmappable writes rejected loudly rather than silently dropped.

## 12. Web UI (as built)

**One self-contained page, no build step.** `crates/nzbd-api/ui/index.html` is compiled into the binary with `include_str!` and served at `/`; the PWA manifest, service worker and icons ride along as `include_bytes!`. The originally planned Svelte 5 + Vite SPA was dropped in phase 4 and the decision re-confirmed in the v2 rebuild (2026-07-25): a Node build chain in CI is a real and permanent cost, and the two things it would have bought — component structure and a diffing runtime — turned out to be ~150 lines of keyed reconciler for one table plus a handful of tiles. Vendoring preact+htm was rejected on the same arithmetic. This is a deliberate simplification and it has limits: past roughly 120 KB the single file should split into several embedded assets. The other scaling worry — nzbget's `fasttable` lesson, that large queues must not freeze the UI — is answered for now by paging the queue (20 rows by default, up to *all* on one page) rather than virtualizing it: paging is ~40 lines against a windowed-scroll implementation's several hundred, and it bounds the row count the reconciler and the browser have to touch each tick, which is the actual cost. Virtualizing is the answer if someone routinely runs *all* on a four-figure queue.

**The rendering laws.** v1 rebuilt the queue with `tbody.innerHTML` on every 1 Hz tick. A tick landing between mousedown and mouseup destroys the pressed button, so the browser never fires `click` — at 1 Hz, deleting a job was a coin flip (field report 2026-07-25 #6). The same assignment reset the detail panel's scroll every second, restarted every progress transition, and made selecting a job name impossible. **A UI that rebuilds the world every second cannot be interacted with.** v2 is organised around not doing that:

1. After boot, `innerHTML` is never assigned to a container holding live rows. Updates are targeted: `textContent`, `style.width`, `classList.toggle`, `disabled`, `title`.
2. Row identity is `tr.dataset.jobId`. The reconciler keeps `Map<key, node>`: update mutates in place, add inserts at position, remove drops the node, reorder is `insertBefore` of the *existing* node. An untouched job keeps the same DOM node across ticks — the property that clicks, hover, selection and CSS transitions all depend on.
3. A cell is written only when its rendered value changed, compared against a `prev` model cached on the row. A 1 Hz progress tick writes two cells, not six.
4. One delegated `click` listener on `document`, dispatching on `closest("[data-action]")`. No generated `onclick` strings — which also retires the escape-job-names-into-JavaScript quoting v1 needed.
5. The job-detail panel is a stable subtree: files reconciled by file id, activity tail appended incrementally, scroll position never reset by an update.

**Structure.** A plain `store` object (`{status, jobs, history, clients, logs, conn}`) written only by the SSE handlers, the poll fallback, and pending-op resolution. Render never fetches; fetch never renders — v1 interleaved both freely, which is how a failed POST became indistinguishable from a successful one. `rowModel(job, ctx)` is pure (a flat view model of strings and flags, no DOM); `reconcileRows(parent, models, dom)` does the keyed diff through a narrow `dom` adapter that is the only DOM surface the renderer may touch. That adapter exists so the CI harness can substitute an ~100-line fake and count every write: [`crates/nzbd/tests/ui_dom.rs`](../crates/nzbd/tests/ui_dom.rs) asserts the five laws above, and `ui_boot.rs` catches the "parses but dies at load" class that large single-file refactors produce.

**Actions are pending-until-confirmed.** Every mutating click registers an op keyed `jobId:kind` and applies its effect to the view immediately — delete hides the row, pause flips the chip, a move reorders locally — while the POST is still in flight. It resolves one of three ways: server state reflects the intent (a tick or an event) → drop the op; the POST answers `!r.ok` or throws → revert now, with the daemon's own `error` string in a toast; nothing confirms it within 5 s → revert, "…didn't take". The overlay is consulted on every render, so a tick landing mid-flight cannot flash the old state back.

**No blocking dialogs, anywhere.** `confirm()` interrupts before anything happens and then says nothing afterwards — the worst of both ends. Delete is one click: the row goes, and a toast offers `Undo` for 8 s, backed by the server-side park in §10.1 — so a misclick on a 60 GiB job costs one more click, not a re-download. The one genuinely irreversible action, history's *delete files*, arms in place instead (the button becomes `sure?` for 3 s), because there is nothing to undo it with. A test fails the build if `confirm(` or `alert(` reappears.

**Liveness.** Connection state is `live` · `stalled` · `reconnecting` · `unreachable`, the last raising a page-wide banner rather than a gray dot — after two consecutive failed polls, since one alone is a race with a restart. The two clocks behind the verdict are deliberately separate: `tick` receipt is *freshness*, `hb` receipt is *liveness*, and conflating them was field report 2026-07-26 — a wedged engine (slow state volume) stops publishing snapshots, the stream dedups the unchanged payload down to heartbeats, and a client that counts `hb` as fresh shows a frozen page under a green dot with its poll fallback disarmed. Now: ticks stale + hb fresh + the last-seen wire rate > 0 ⇒ `stalled` ("updates delayed" — the engine, not the connection), and the poll fallback engages whenever ticks are stale for any reason but a healthy idle stream. The page also **owns stream retry**: `EventSource`'s built-in reconnect only covers clean network failures — any non-200 answer (a 503 mid-restart, a proxy error page) closes it permanently, which used to leave "polling — reconnecting…" up forever; a fatally-closed stream is rebuilt with capped backoff (1 s → 15 s), plus a belt in the 5 s pass and on tab-visibility return. The rate tile carries a 180-sample sparkline fed by ticks *and* poll results through a ~1 Hz sampler (a number says what is happening now; the shape says whether that is normal), the tab title tickers `▼ 93 MiB/s · 12m — nzbd` and resets when idle, per-provider chips show each server's share of the wire rate — summing exactly to the header tile, §8.5 — and go red when it is blocked, and the Logs tab is a `tail -f` over the `log` stream with a 500-line client ring that marks any gap the server's batch cap created.

**Paging is a view concern, not a wire one.** The queue is paged client-side (20 · 50 · 100 · all, remembered in `localStorage`) out of the read model the tick already delivered. Paging server-side would mean the `tick` takes page parameters, and the property that header stats and job rows come from *one* snapshot is worth more than the bytes that would save. Two consequences the code has to honour: a row's move-up/move-down arrows reflect its **global** queue position, not its position on the page — moving is a queue operation and page 2's first row can still move up — and the page index clamps when the queue shrinks under you, which is the normal case as jobs finish.

**Deliberately absent:** i18n (the structure allows it; nzbget's 20 languages are a parity goal, not a phase-4 one), virtualized tables, drag-and-drop reordering, feed preview, extension-manager UI. The settings and first-run forms are still template-built — they re-render on user intent, never on a tick, and hold no live rows, so the laws above do not apply to them.

## 13. Observability and security

**Observability.** `tracing` spans: `job(id)` → `file(id)` → `segment(msgid)/server(id)` — a failed download is diagnosable from one filtered trace. JSON or pretty logs; per-job ring buffers backing the `loadlog` compat call. `metrics` → Prometheus: per-server bytes/connections/article-success-rate/430-rate, decoder throughput, writer queue depth, repair durations, PP stage timings. `/healthz` liveness + a `systemhealth`-style self-check (disk space, cert expiry, server reachability).

**Security.** Memory-safe parsing of all network inputs (`forbid(unsafe_code)` outside `nzbd-yenc`'s FFI feature). rustls everywhere; per-server `CertVerification` None/Minimal/Strict parity (users need Minimal for some providers). No default credentials — first-run generates a token and prints it (the shim can opt into legacy `nzbget`/`tegbzn6789` for migration, off by default, warned loudly). Subprocesses: argv-only, scrubbed env, timeouts, output caps, staging-dir confinement, symlink-escape checks. Config redacts secrets in API reads for restricted roles (parity with `Restricted()` masking). systemd unit ships with `ProtectSystem=strict`, `NoNewPrivileges`, RW only on configured dirs.

## 14. Testing strategy

| Layer | Approach |
|---|---|
| Parsers (NZB, yEnc, NNTP, par2 packets) | Unit + property tests (proptest) + fuzzing (cargo-fuzz); yEnc differentially tested against rapidyenc once the FFI lands; NZB corpus cribbed from SABnzbd/nzb-rs edge cases (obfuscated subjects, broken XML, weird encodings) |
| Failover ladder & scheduler | Table-driven scenario tests: scripted server outcomes → expected lease sequences (the §3.2 semantics as executable spec) |
| Engine end-to-end | `nzbd-nserv` mock NNTP server: serves generated posts with injected 430s, CRC corruption, stalls, disconnects, per-server latency; asserts assembled files bit-identical + resume-after-kill works (kill -9 mid-download in CI) |
| Post-processing | Fixture archive sets (par2 damage matrices, multi-volume rar, passworded, split files); asserts stage transitions + repair outcomes with a pinned par2cmdline-turbo |
| Compat shim | Golden structural diffs vs recorded nzbget 26.2 responses; plus live Sonarr/Radarr containers in a nightly integration workflow pointed at nzbd (the real certification) |
| Performance | criterion micro-benches (decode, CRC, journal append); throughput harness vs nserv over TLS on loopback with regression tracking; reference target: ≥ nzbget on same box, headroom to 10 Gbit |

## 15. Key decisions (ADR summary)

| # | Decision | Alternatives rejected — why |
|---|---|---|
| 1 | **Rust + tokio**, task-per-connection | Go (simpler, but GC + no rapidyenc-class native decode story without cgo); modern C++ (keeps the safety exposure this rewrite exists to remove); thread-per-article port (doesn't scale, can't pipeline) |
| 2 | **rustls + aws-lc-rs** + platform-verifier | OpenSSL (C, global-state pain); ring (post-maintenance-scare, security-fix-only); native-tls (maintenance mode) |
| 3 | **Single-owner queue task + arc-swap snapshots** | Global `RwLock` (nzbget's coarse-mutex problem reborn); actor frameworks (kameo/ractor: churn + dependency risk for what channels do fine) |
| 4 | **yEnc: scalar Rust now, vendored rapidyenc FFI as feature** | Pure-Rust SIMD now (std::simd still nightly-only in 2026; `std::arch` port of 8 years of edge-case hardening is real risk); existing crates (dead, non-incremental, or unaudited AI-generated) |
| 5 | **par2: subprocess par2cmdline-turbo behind `ParEngine`; native quick-verify from download CRCs** | FFI (no C API; C++ shim maintenance); pure-Rust repair today (4 projects, all <18 months old, none differentially validated); skipping quick-verify (loses nzbget's biggest PP speed win) |
| 6 | **unrar: subprocess only** | `unrar` crate static-links RARLAB freeware-licensed code (not OSI, GPL-incompatible in the common reading) and forfeits crash isolation against hostile archives — nzbget and SABnzbd both subprocess for good reason |
| 7 | **State: snapshot + append-only journal for queue; SQLite (rusqlite bundled) for history** | SQLite for hot segment state (write amplification vs journal appends); sled (dead); redb (viable, revisit); nzbget's text DiskState (versioned line-format archaeology — the thing we're escaping) |
| 8 | **axum + hand-rolled RPC envelope in `nzbd-compat`** | jsonrpsee (strict 2.0 — fights nzbget's 1.1 dialect); actix-web (own runtime layer, less ecosystem); emulating at HTTP-server level like nzbget (hand-rolled HTTP in 2026 — no) |
| 9 | **Native TOML config + one-shot importer + shim projection** | Adopting `nzbget.conf` as primary store (perpetuates write-back of an ordered flat format; blocks typed validation); no importer (migration cliff) |
| 10 | **Compat scope: *arr-first (7 methods), then breadth; stock webui assets out of scope** | Full 56-method day-one parity (months of shim work before any user value; nzbdav's 1k★ proves targeted API emulation is the adoption cheat code) |
| 11 | **SSE for events** (WS available via axum if needed) | Polling (nzbget's model — wasteful); WS-only (proxy/reconnect friction for zero benefit here) |
| 12 | **Extension protocol preserved as-is** | New plugin API first (would orphan the existing script ecosystem — its env/exit-code protocol is crude but proven and language-agnostic) |
| 13 | **Cluster topology: elected leader + workers, work leased over HTTP; shared volume = data plane + election lease only** (CLUSTERING.md §2.1) | Symmetric peers via FS locks (distributed locking on a network FS — fencing bugs become queue corruption); external etcd/redis (runtime dependency); embedded Raft (consensus machinery to guard a download queue) |
| 14 | **Work units: whole-job download leases + stage-level PP leases** (CLUSTERING.md §2.2) | PP-only offload (leader still congested); segment-split downloads day one (cross-node fan-in complexity — deferred to C3, protocol doesn't preclude it) |
| 15 | **HA: automatic election (monotonic staleness observation, write–wait–verify, epoch fencing); every node serves the API and proxies to the leader** (CLUSTERING.md §4) | Workers-idle-when-leader-down (rejected by owner); VIP/keepalived as the only path (now optional); wall-clock lease expiry (home-lab clocks lie) |
| 16 | **Cluster state: per-job/per-node journals + queue snapshot on the shared volume; journal replay unions completion evidence; snapshot has a pre-rename epoch check; SQLite never on the network FS** (CLUSTERING.md §6.4 documents the remaining authority race) | Global journal (can't reclaim jobs independently); FS locks per job (see 13); SQLite/WAL on Gluster (shared-memory WAL doesn't span nodes; known corruption class) |
| 17 | **Web UI: one embedded no-build page with a hand-rolled keyed reconciler** (§12) | Svelte 5 + Vite SPA (a Node build chain in CI, permanently, for component structure we don't need at this size); vendoring preact+htm (13 KB of third-party JS in-tree to diff one `<table>` that ~150 lines diff fine). Trade-off accepted knowingly: no virtualized tables and no i18n scaffolding, revisit past ~500 rows or ~120 KB |
| 18 | **Delete parks the regenerated NZB to history and is undoable; no `confirm()` anywhere** (§12) | Confirmation dialogs (interrupt before anything happens, then say nothing afterwards — the worst of both ends); bare instant delete (a misclick on a 60 GiB job costs a full re-download); two-step inline arming for everything (still two clicks, the thing being complained about). Arming is kept for the one action nothing can undo: deleting downloaded files |
| 19 | **BitTorrent: one protocol-neutral queue/backend seam; immutable seed tree; readiness is durable state** ([proposal](BITTORRENT_PROPOSAL.md), [M0 report](BITTORRENT_M0_REPORT.md), [M1b report](BITTORRENT_M1B_REPORT.md)). **Amended:** the maintained twelve-patch rqbit v8.1.1 engine retains the accepted M0 boundaries, adds feature-gated loopback DHT test support, and announces as Runner with a tracker-compliant lifecycle. Single-node activation owns one session, durable admission/recovery, shared scheduling/bandwidth/quota/disk policy, native web/mobile surfaces, and a narrow qBittorrent Web API 2.8.1 projection. DHT-on may query a privacy-unknown magnet hash before list-only metadata validation; public metadata proceeds and private metadata is rejected before managed admission. Disabled mode opens no peer listener and refuses orphaned live torrent state. M6 cluster ownership still requires its own ADR; proxy use remains fail-closed for DHT and UDP trackers. | A second daemon (two queue authorities and lifecycle stores); pretending pieces/peers are NNTP articles/servers (dishonest state); pinning a release candidate or entering `libtorrent` FFI before an explicit engine decision. First release is deliberately v1 TCP/IPv4 and single-node. |
| 20 | **Pipeline contract v2: one terminal fact per job, typed at the producer, environmental holds only, per-file journaled publication with no acknowledgement** ([ADR-20](adr/0020-pipeline-contract-v2.md), [contract](PIPELINE_CONTRACT.md)) | Patching the v1 seam (review holds, acknowledgement, four retry clocks and text-matched causes each fixed a symptom and left the next); a broker or new transport (SSE + REST + SQLite already carry it) |

## 16. Roadmap

| Phase | Scope | Exit criteria |
|---|---|---|
| **0. Scaffold** (this session) | Workspace, domain types + health math, NZB parser, scalar incremental yEnc + CRC combine, NNTP codec skeleton, API/shim stubs, CI-ready tests | `cargo test` green; design reviewed |
| **1. Core engine** | Queue owner, scheduler + failover ladder, pools + pipelining, rustls transport, DirectWrite writer, journal + resume, rate limiter, `nzbd-nserv`, CLI (`nzbd run`, `add`) | Downloads real NZBs from real providers at line rate; kill -9 resume proven; ladder scenario suite green |
| **C1. Cluster foundation** | `nzbd-cluster` per CLUSTERING.md: shared-volume election + registry, epoch-fenced per-job journals, HTTP work-lease protocol, distributed whole-job downloads, any-node API proxy, `[cluster]` config | Multi-node harness green: single-leader invariant, leader-kill failover with lease adoption, worker-kill reclaim with zero re-fetch, single-node parity |
| **2. Post-processing** | PP orchestrator, par quick-verify (native) + repair (subprocess), unpack + direct unpack, script protocol, health gates, dupe handling, SQLite history; PP stages are cluster-claimable work units (CLUSTERING.md phase C2) | Damaged-set fixtures repair correctly; existing NZBGet PP scripts run unmodified; a job downloaded on one node repairs on another |
| **3. API + compat** | Native REST/SSE + auth, compat C1→C2, config importer, `rapidyenc-sys` | Sonarr + Radarr run a full grab→import cycle against nzbd in CI; nzbget-migration guide works |
| **4. Web UI + ecosystem** | SPA (queue/history/config/stats), extension manager, RSS feeds + filter language, compat C3, packaging (static musl, Docker, Homebrew, Windows) | Daily-drivable replacement; linuxserver-style container published |
| **5. Beyond parity** | Native par2 repair swap-in, io_uring file I/O when tokio stabilizes it, article-level streaming APIs (mount-mode groundwork), per-provider adaptive pipelining, cluster C3 (segment-split downloads, weighted scheduling) | — |

Rough sizing honesty: phases 1–3 are each multiple weeks of focused work; nzbget is ~62k LOC of accumulated behavior. The phasing is designed so every phase ends with something independently useful (phase 1 alone = a fast, resumable CLI NZB downloader).

## 17. Risks and open questions

- **Compat drift** is the top product risk — mitigated by golden tests + nightly live-*arr CI, and by keeping the shim a pure translator.
- **rapidyenc build friction** on exotic NAS targets → scalar decoder is always available; feature-gated FFI.
- **par2cmdline-turbo availability** per platform → bundle checksummed binaries in release artifacts, `ParCmd` override like nzbget.
- **Behavioral unknowns** (deobfuscation heuristics, dupe corner cases) will surface in beta — the C++ source stays the arbiter; port with tests, not vibes.
- **Scope**: RSS filter language and full config-editing webui are large; deliberately late phases. Streaming-mount demand (nzbdav's traction) may reorder phase 5 — the segment-addressed engine design keeps that door open.
- **Clustering rests on the shared volume behaving like a consistent POSIX FS.** Gluster must run with quorum (replica 3 or arbiter); a volume that split-brains gives the cluster two truths — no application protocol survives that. Election fencing is practical (epoch + verify-before-commit + union journals), not linearizable consensus; residual zombie-writer windows are analyzed in CLUSTERING.md §6.4. SQLite is never placed on the network FS (ADR-16).
- Open: adopt `redb` for the journal if fsync patterns disappoint? Expose native API under gRPC too? Windows service story timing? Name the thing properly.

## Appendix A — behavioral cheat sheet carried from nzbget

Failover: retries=3, retry-interval=10 s, timeout=60 s, idle-hold=5 s, random pick among free same-tier connections, groups fail together, fill servers never stall, retention pre-fails. Health = per-mille formulas §3.2. Speed window 30×1 s. Cache flush at 90%. Downloader cap concept (`2 + Σ connections`) replaced by pool-native limits. PP strategies sequential/balanced/aggressive(3,1)/rocket(6,2). Script exits 92/93/94/95. Priorities −100/−50/0/50/100/900-force. Sonarr `drone` param is sacred. Sizes cross the shim as Lo/Hi/MB triplets. Default port 6789.

*Sources: full source analysis of nzbgetcom/nzbget v26.2/26.3 (July 2026); Sonarr develop-branch NzbgetProxy.cs; SABnzbd performance discussions (#2352, #3366); 2026 crates.io/GitHub ecosystem survey (tokio, rustls, axum, rapidyenc, par2cmdline-turbo, prior-art projects).*
