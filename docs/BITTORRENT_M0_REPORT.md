# BitTorrent M0 report — maintained rqbit v8.1.1 closes the engine boundary

**Security amendment, 2026-11-13:** maintained patch
[`0023-upgrade-upnp-xml-parser.patch`](../contrib/rqbit/0023-upgrade-upnp-xml-parser.patch)
upgrades the compiled UPnP helper to fixed quick-xml 0.41 and removes both
XML advisory exceptions. UPnP remains unavailable. Earlier exception tables
and native matrix measurements below record historical evidence; the
[gate 9 advisory scope](BITTORRENT_GATE9_REVIEW.md#2-advisory-scope--fixed-parser-no-rustsec-exceptions)
is authoritative for the updated dependency policy.

**Status:** all eleven gates pass locally and on the native matrix; independent
review complete; remaining M2 slices tracked by #154–#160 with activation owned
solely by #163; daemon wiring remains disabled ·
**Original run:** 2026-08-05 · **Amended:** 2026-08-14 ·
**Accepted:** 2026-08-14 · **Status reconciled:** 2026-08-22 ·
**Engine:** rqbit v8.1.1 archive plus the ordered nine-patch maintained series,
`default-features = false`, `rust-tls` ·
**Host:** macOS 26.6 arm64 · **Decision owner:** ADR-19 in
[BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md)

The original isolated spike proved that stable `librqbit` could download and
seed nzbd’s first-release v1/TCP/IPv4 workload but left ownership and resource
gaps. ADR-19 now selects a reproducibly derived v8.1.1 engine with exactly nine
maintained patches. The combined local proof closes those M0 gaps without
adding a production session to the daemon.

The earlier blockers were:

1. enabling fast-resume persistence automatically restores every library
   record during session construction, before nzbd can reconcile its durable
   job store; and
2. public per-torrent stats do not expose detailed tracker/DHT diagnostics; and
3. several tracker, peer, handshake, discovery, metadata, and storage paths
   lacked the reviewed runtime boundaries.

The 2026-08-14 decision separates required operational facts from optional
diagnostics. Transfer phase, verified progress, completion, peer counts,
admission facts, rates/ETA inputs, and a bounded credential-safe last error are
required. Tracker/DHT detail is diagnostic only and is explicitly `unknown`
when unavailable; peer availability is never used to invent discovery health
or a health percentage.

The maintained-engine implementation stopped at the isolated `nzbd-torrent`
boundary and queue schema-version groundwork. Later M2a work added dormant
`[torrent]` configuration, and M2b added dormant selective-session and
queue-adjacent runtime ownership, but validation still rejects `enabled = true`;
no torrent admission API, production daemon session dependency, peer listener,
or production torrent admission path has been added.

The exact maintained-engine head received
[independent approval on PR #101](https://github.com/pjunod/nzbd/pull/101#issuecomment-5298914349)
and merged on 2026-08-14. This report's status was reconciled on 2026-08-22;
that later date is not a second M0 acceptance event.

The [pre-release operations review](BITTORRENT_RELEASE_REVIEW.md) collects the
current traffic, port, path, seeding, deletion, evidence, and sign-off contract
without presenting any of those proposed settings as usable production
behavior.

A later hostile-input review found another stable-engine prerequisite outside
the two failed ownership/observability gates: magnet metadata is allocated up
to rqbit's fixed 32 MiB ceiling before nzbd can apply its proposed 10 MiB
default. Section 3.5 records the maintained change that moves a caller's limit
ahead of that allocation. This still does not authorize production wiring.

---

## 1. Result against the eleven gates

| Gate | Result | Evidence and consequence |
|---:|---|---|
| 1. Rust 1.85 and platform packaging | **Pass** | A real Rust 1.85.1 macOS build passes, and the release harness links only macOS system libraries, not OpenSSL. The first checked-in `BitTorrent M0` run caught Tokio 1.53.0 using Rust 1.86's `OnceLock::wait` on Windows. After the workspace pinned Tokio 1.52.4, the [corrected run completed successfully](https://github.com/pjunod/nzbd/actions/runs/31060326800) on 2026-08-06 UTC: the isolated adapter suite passed on Linux glibc, macOS arm64, Windows MSVC, and x86-64/aarch64 musl under Rust 1.85, and the exact-engine/Rust-TLS dependency policy also passed. |
| 2. v1 `.torrent`, magnet, TCP/IPv4, then seed | **Pass** | Deterministic generated payloads download through both admission paths; a local seeder accounts for both uploads and exact bytes match. The adapter checks nonzero piece length and total payload, checked aggregate length, whole SHA-1 hash width, exact hash count, and rqbit's `u32` absolute chunk-index boundary before the engine constructs its length table. |
| 3. Controls and live limits | **Pass** | Pause/resume is exercised before completion, the live download limit is removed during transfer, and idempotent keep-data delete, idempotent delete-data, and unrelated-sibling retention pass. |
| 4. Kill/restart never trusts partial data | **Pass** | The process-level contract persists two paused torrents, kills the child without a clean stop, changes one payload to complete valid bytes, starts a session with automatic restore disabled, and explicitly restores only the selected durable ID. The new session starts empty; exactly one torrent is admitted; and its progress remains the persisted 16 KiB and incomplete rather than trusting the 64 KiB now on disk. A full recheck remains a safe degradation path, not the normal ownership model. |
| 5. Private-torrent discovery | **Pass** | A one-tracker private torrent downloads through a loopback HTTP tracker, and a deterministic peer-wire control proves public torrents consume an injected PEX peer while private torrents ignore it. A Linux capture harness starts live DHT, redirects both stable-8.1.1 bootstrap ports to a local KRPC probe, and observes separate public control hashes before and during a 15-second private canary window. It scans all captured UDP—not just redirected DHT—for the private hash in binary or LSD-style text form. The [first successful capture run](https://github.com/pjunod/nzbd/actions/runs/31128106994) passed on 2026-08-06 UTC: both DHT controls were observed and no private DHT/LSD hash appeared. Tracker order is still lost through a hash set before truncation, so the adapter rejects private metainfo unless it has exactly one unique tracker. Empty tracker slots are treated as absent; every non-empty tracker URL now fails closed before rqbit can silently discard malformed/non-UTF-8 input, unsupported schemes, missing hosts, or UDP without a port. A magnet does not expose the private bit before resolution; because stable rqbit has no per-add DHT suppression, the adapter rejects every magnet in a DHT-enabled session before calling the engine. A known-private `.torrent` is likewise rejected before engine admission when session DHT is live. Loopback and policy tests prove both guards fail before contact while DHT-disabled private admission remains available. |
| 6. Path and delete safety | **Pass** | The adapter now repeats the portable lexical path invariant before rqbit admission: unnamed payloads, empty/dot/parent components, slash or backslash, absolute/UNC forms, Windows drive prefixes and device aliases, alternate-data-stream and other reserved characters, trailing dot/space aliases, malformed UTF-8, empty multi-file paths, missing/unsafe multi-file roots, metainfo-declared symlinks, exact duplicate paths, file-versus-directory-prefix overlaps, and portable Unicode NFC/full-case-fold collisions fail with safe nzbd-owned errors. Greek sigma, long-s, Windows dotless-i, compatibility-ligature, and sharp-s/full-fold aliases are covered without collapsing compatibility-only width pairs. rqbit's shared `torrent-content` fallback is therefore never used for an unnamed single-file torrent. Magnet input first resolves through rqbit's list-only path, which returns before storage construction; nzbd then applies the same metainfo contract and admits only the validated bytes. A fake BEP 9 peer proves an unsafe resolved path leaves the destination empty, and the real-admission corpus still proves rejection before an escape file exists. The session canonicalizes its output root and preflights every existing payload prefix with no-follow metadata: symlinks fail, prefixes must be directories, and leaves must be regular files while an existing regular leaf remains valid resume input. A Unix test proves a symlink is rejected before storage and its external target remains empty. The [first cross-platform filesystem-probe run](https://github.com/pjunod/nzbd/actions/runs/31240896567) passed on 2026-08-08 UTC for ASCII-case and Unicode NFC/NFD pairs. The [review-correction native run](https://github.com/pjunod/nzbd/actions/runs/31264422471) passed on 2026-08-08 UTC after proving default macOS storage also aliases compatibility ligatures and full-case-fold pairs; the adapter now rejects those pairs. The native matrix records all four rejected classes and requires the remaining compatibility-width pair that the adapter admits to stay distinct. The exact observations are recorded in §2.2. They describe hosted temporary volumes, not every operator payload mount. This is defense in depth, not a claim to close the check/write race. The importer-safe content inventory omits BEP 47 padding entries while raw engine-indexed progress remains available only as diagnostics. Delete-data removes only parsed torrent content; an unrelated sibling survives. Higher layers must still prove persisted delete-root authority, descriptor-relative containment across writes, and empirical filesystem-specific behavior across supported operator payload mounts in M5/M2. |
| 7. Public observability | **Pass** | The adapter exposes phase, total/progress/upload bytes, verified file progress, rates/ETA inputs, peer counts, completion, admission facts, and its existing 2 KiB credential-safe error. Detailed tracker/DHT state is explicitly `unknown` because this engine does not provide it. Tests pin that default and prevent inference from peer count; no torrent-health percentage exists. |
| 8. nzbd-authoritative persistence | **Pass** | The maintained `disable_auto_restore` option lets session construction admit nothing while retaining rqbit persistence and explicit selective restore. The gate-4 kill/restart test proves the chosen record and persisted verified progress remain subordinate to nzbd's durable selection. Upstream acceptance is not required for the pinned maintained engine. |
| 9. Resource, package, and license delta | **Pass** | The accepted measurements and dependency/license exceptions remain recorded in §4 and the [gate 9 review brief §4.1](BITTORRENT_GATE9_REVIEW.md#41-recorded-disposition--accepted-2026-08-14). The maintained engine now enforces tracker request size/deadline/cadence, 80/400 live peers, 1,024/4,096 retained peers, 256 pending handshakes, a 128-request per-peer response window, bounded DHT/metadata discovery, a 10 MiB pre-allocation metadata ceiling, and fail-closed file sizing. CI verifies the pinned upstream SHA-256, exact nine-patch order/membership, clean application, byte-identical vendor, and every focused behavior test. The [2026-08-14 maintained-engine M0 matrix at `771cf5b`](https://github.com/pjunod/nzbd/actions/runs/31846437027) refreshed the resource measurements and passed every native M0 target. |
| 10. One explicit rustls provider | **Pass** | The process starts without a provider, explicitly installs aws-lc, and constructs librqbit’s rustls client without the mixed-provider panic. |
| 11. v1-only boundary | **Pass** | Stable input uses v1 pieces/`btih`; v2-only and hybrid `.torrent` files and magnets return separate named errors before managed-torrent admission. Magnet classification reads decoded `xt` query parameters rather than searching the whole URI, so version-looking text in a display name or tracker URL cannot create a false v2/hybrid result. The adapter accepts one valid 40-hex or 32-base32 `btih`, rejects missing, malformed, or duplicate v1 topics by name, and rechecks the resolved info dictionary before storage exists. |

The local proof and native rerun pass every gate. Independent review completed
on the exact maintained-engine head in PR #101. This result authorized M2
planning; each later milestone still requires its own exact-head review, and
none of this M0 evidence adds daemon networking or production payload I/O.

---

## 2. What was built

### 2.1 Isolated engine adapter

`crates/nzbd-torrent` pins stable 8.1.1 and contains the only librqbit-facing
code. The daemon does not depend on it. The boundary currently provides:

- explicit process-wide aws-lc provider installation;
- session construction with DHT off by default, UPnP unavailable, DHT
  persistence disabled unconditionally, no library fast-resume/session
  persistence, global trackers, remote blocklist, or deferred-write buffer,
  and proxy+DHT rejected because DHT bypasses the engine's SOCKS path. Every
  stable 8.1.1 session option is assigned explicitly so a new upstream option
  becomes a compile-time review event instead of silently inheriting a default;
- per-torrent admission options assigned explicitly for metadata resolution
  and managed torrents, with selective files, alternate output roots, custom
  trackers/storage, and deferred writes unavailable. Newly added upstream add
  options likewise require an explicit adapter review;
- raw v1 metainfo and two-stage v1 magnet admission, with an engine-compatible
  exact-topic grammar, eager `so=` expansion rejected before rqbit parsing,
  and resolved metadata returned by list-only mode and revalidated before
  storage construction;
- bounded bootstrap peers: caller order is retained, duplicates are removed,
  non-unicast/port-zero/IPv6 endpoints fail by name, and resolved magnet peers
  cannot grow the handoff beyond 80. This bounds admission input only; it does
  not cap peers later discovered from trackers, PEX, or DHT;
- one concurrent torrent initialization: stable rqbit's unset default permits
  three payload integrity scans at once, while the dormant session explicitly
  serializes that disk-heavy work. This is an initialization-I/O guard, not the
  future shared active-download scheduler or a runtime peer cap;
- one DHT-disabled session pressure probe that admits 100 distinct one-byte
  torrents within 30 seconds, keeps an exact 10-active/90-paused mix, and
  requires session shutdown within 10 seconds. The torrents have no trackers
  or initial peers, so the test exercises adapter and engine bookkeeping
  without public discovery or payload transfer. A passing wall-clock deadline
  is a regression guard, not a memory or throughput measurement. The
  [first native evidence run](https://github.com/pjunod/nzbd/actions/runs/31330178035)
  passed the isolated adapter suite on 2026-08-09 UTC for Linux x86_64 GNU,
  Linux x86_64 musl, Linux aarch64 musl, macOS aarch64, and Windows x86_64;
- one optimized-process memory probe repeats that exact trackerless, peerless
  100-torrent mix and samples resident memory before session construction,
  after construction, after every ten admissions, and after all handles have
  completed initialization into the exact 10-live/90-paused state. It fails
  above a preliminary 32 MiB growth ceiling on each native runner. The probe
  first proves exact-ceiling acceptance and first-byte-excess rejection
  through the same guard used for the measurement. This is a sampled retained-
  memory regression guard, not an allocator peak, active-swarm, or exhaustion-
  resistance claim;
- a second optimized-process probe constructs and validates the accepted
  100,000-file inventory while sampling resident memory before fixture
  construction, after construction, and after validation. It fails above a
  preliminary 64 MiB growth ceiling on each native runner. The same guard's
  exact-ceiling and first-byte-excess negative control runs before the
  measurement. This is retained memory evidence, not the parser's transient
  allocation peak or a concurrent hostile-submission test;
- an ignored, crate-private local-swarm unit probe injects
  `ErrorKind::StorageFull` from `pwrite_all` after one 16 KiB write call returns
  successfully. The affected 256 KiB torrent must become `Error`, remain incomplete,
  retain a display-safe fault fact, and answer an independently scheduled
  stats request within one second. Before the fault is admitted, a separate
  64 MiB torrent must already be live with nonzero incomplete progress through
  normal filesystem storage; it must remain live at the fault boundary and
  then complete in the same engine session. The seeder's aggregate upload is
  capped at 8 MiB/s so the boundary assertion does not depend on loopback or
  filesystem speed, and rqbit selects its own listen port from a test-only
  multi-port range rather than inheriting a bind/drop/rebind race. The fault
  state and exact write accounting must remain unchanged afterward, and both
  sessions must stop within ten seconds. The custom-storage and broad-listen
  helpers are compiled only into crate unit tests and are absent from every
  non-test build; normal admission still forces `storage_factory: None` and
  requires one explicit non-zero listen port. This proves containment of an
  injected write-time fault and a future interception point, not general
  ENOSPC behavior. The daemon now has a protocol-neutral multi-root guard,
  fail-safe incomplete-probe recovery, cluster placement enforcement, and
  local/per-node limiting-volume status, but no torrent path reports this
  injected signal into that guard. There is still no torrent API-responsiveness
  proof, new-request pause, or continued-upload proof, and stable rqbit
  currently reports the affected torrent as an error rather than a paused
  download;
- a second ignored, crate-private unit probe injects `ErrorKind::StorageFull`
  from `ensure_file_length` while adding a paused, peerless 256 KiB torrent.
  With maintained patch `0018`, the probe fails unless admission or
  initialization exposes a bounded error retaining the file name, requested
  length, and storage cause. If scheduling returns a handle before the failure,
  stats must enter `Error`; `Paused` or `Live` is a failure. The requested file
  must remain zero bytes after exactly one 262,144-byte sizing request and zero
  piece writes. Reverting `0018` restores the historical `Paused`-then-`Live`
  result and makes this proof fail. This closes the engine fail-open boundary;
  it does not satisfy the M2 disk-full contract.
  The [2026-08-13 replacement run](https://github.com/pjunod/nzbd/actions/runs/31654021866)
  records the superseded fail-open behavior on every native M0 target. The
  [maintained-engine M0 matrix at `771cf5b`](https://github.com/pjunod/nzbd/actions/runs/31846437027)
  proves the corrected fail-closed result on those same targets;
- explicit peer lifetimes: the dormant session pins stable 8.1.1's effective
  10-second connect and read/write timeouts and 120-second keepalive interval;
  per-add options inherit this reviewed session policy instead of introducing
  another override;
- bytes-only managed admission: the private engine handoff accepts only
  metainfo bytes that already passed nzbd preflight. Stable rqbit's URL variant
  cannot bypass nzbd's source-fetch size, redirect, timeout, or redaction
  policy through this helper. A separate dormant HTTP(S) helper follows at
  most five manually validated redirects, strips authentication across origin
  changes, ignores ambient proxy variables, retains no cookies, uses the
  process-wide aws-lc provider with OS trust roots, bounds declared and
  streamed bodies, applies one end-to-end timeout, and returns only
  preflighted bytes. Its private-CA loopback and oversized chunked-body tests
  pin the TLS and while-streaming properties directly;
- named v2-only and hybrid rejection for both metainfo and magnets;
- fail-closed HTTP/HTTPS/UDP tracker URL validation for metainfo and magnets,
  with at most 64 unique non-empty trackers and 2 KiB per decoded URL, plus
  exact-one-tracker validation for private metainfo;
- pause, resume, idempotent forget, and bounded delete-data delegation;
- live session upload/download rate changes;
- info hash, phase, progress, upload, per-file progress, rates, derived ETA,
  peer aggregates, completion, display-safe error facts, and an importer-safe
  content-file inventory that omits BEP 47 padding entries;
- a 2 KiB display-safe boundary for rqbit operation/stat errors that removes
  magnet URIs, URL credentials/queries, inline, colon-delimited, JSON-style,
  and whitespace-separated secret assignments, peer addresses, absolute
  paths, and control characters before returning adapter values;
- split SOCKS URL/username/password input with password redaction, a strict
  credential form, named rejection of UDP trackers, and a recording relay
  proving the peer path did not also connect directly; and
- an idle release harness used only for M0 measurements.

The crate forbids unsafe code. It is intentionally not a complete production
backend: there is no queue owner channel, durable torrent record, config/API,
URL-fetch route, watch folder, seed policy, or session lifecycle in the daemon.
The source-fetch helper is not reachable from production daemon input.

The dependency graph also required a production-daemon TLS correction outside
the isolated crate: `crates/nzbd/src/main.rs` and `crates/nzbd/src/tls.rs`
install the process-wide aws-lc provider before any daemon rustls client or
server is constructed. That idempotent provider selection is the only runtime
daemon behavior changed by the spike.

### 2.2 Deterministic tests

The isolated suite covers:

- `.torrent` and magnet download from one local TCP seeder;
- a fake BEP 9 metadata peer proving unsafe magnet-resolved paths fail while
  the destination directory remains empty;
- exact-byte verification and upload accounting;
- a live 16 KiB/s limit followed by an unlimited transfer;
- pause and resume before download completion;
- keep-data and delete-data behavior, including idempotence;
- named rejection of zero, empty, and multi-port TCP listen ranges so the
  adapter cannot silently request an ephemeral port or probe a range;
- deterministic deduplication and an 80-peer bound for explicit bootstrap
  peers, with rejection of port-zero, non-unicast, and IPv6 endpoints before
  the IPv4-only v1 engine path sees them;
- authenticated SOCKS5 peer routing through a loopback proxy, with a
  recording relay proving no parallel direct connection;
- origin-only SOCKS proxy validation, including rejection of paths, queries,
  and fragments before rqbit's peer and tracker clients can interpret the
  same setting differently;
- named rejection of proxy+DHT and proxy+UDP-tracker leak paths;
- named rejection of tracker port zero rather than deferring an invalid
  endpoint to rqbit's retry/error path;
- named rejection of privacy-unknown magnets and known-private metainfo in a
  DHT-enabled session before rqbit can contact even an explicitly supplied
  peer;
- password redaction and invalid proxy combinations, including embedded proxy
  credentials before validation, multi-token authorization/cookie values,
  embedded JSON/query assignments, private-tracker credential names, and
  alternate assignment separators;
- engine/stat error redaction and UTF-8-safe 2 KiB truncation with an explicit
  marker;
- authenticated HTTP source handling that preserves credentials only across
  same-origin redirects, strips them across origin changes, does not replay
  response cookies, exposes only origins in errors, rejects non-HTTP targets,
  accepts exactly five redirects and rejects the sixth, bounds both declared
  and chunked bodies, enforces one end-to-end timeout, and preflights the
  returned metainfo before it leaves the helper;
- one-tracker private-torrent discovery and multi-tracker rejection;
- a positive-control PEX peer that public torrents contact and private torrents
  ignore; the canary address is available only through the peer-wire message;
- traversal rejection before filesystem escape;
- canonical-root and existing-symlink preflight whose external target remains
  untouched, with the remaining check/write race called out explicitly;
- adapter-owned portable path preflight for single- and multi-file torrents,
  including safe named errors, Windows device/character/alias rejection,
  metainfo-declared symlink rejection, and exact or portable full-case-fold
  duplicate path rejection;
- parsed BEP 47 padding metainfo whose padding entry stays out of the
  importer-safe content inventory without shifting later file progress;
- centralized proposal limits for raw metainfo, magnet length, file count, one
  path component, one projected relative path, and aggregate projected path
  bytes, with exact boundary and first-excess tests;
- one session retaining 100 distinct torrents at once, split into ten
  live-but-peerless and ninety paused handles, with bounded admission and
  shutdown deadlines and no tracker, DHT, initial-peer, or payload traffic;
- deterministic, socket-free preflight mutations covering every truncation and
  bounded single-byte replacement, deletion, and insertion around valid v1,
  v2-only, and hybrid seeds, with exact accepted/rejected outcome counts plus
  nesting, length-overflow, duplicate-info, trailing-byte, and framed-marker
  invariants;
- checked v1 payload/piece/hash geometry, including zero values, aggregate
  length overflow, malformed hash width, mismatched hash count, and valid
  zero-length sidecars;
- the explicit aws-lc provider invariant;
- v2-only and hybrid named rejection for metainfo and magnets;
- query-aware magnet validation for hex/base32 `btih` with lowercase base32
  normalized to rqbit's accepted spelling, valid `btmh`,
  duplicate/missing/unknown exact topics, engine-incompatible casing, false
  version markers outside `xt`, and pre-engine rejection of unbounded `so=`
  file-range expansion; and
- the persistence auto-restore behavior that fails gate 8.

The
[review-correction native run](https://github.com/pjunod/nzbd/actions/runs/31264422471)
passed on 2026-08-08 UTC and recorded these hosted temporary-volume results:

| Runner | ASCII case | NFC/NFD | Ligature/ASCII | Sharp-s/SS | Width pair |
|---|---|---|---|---|---|
| Linux | Distinct | Distinct | Distinct | Distinct | Distinct |
| macOS | Aliased | Aliased | Aliased | Aliased | Distinct |
| Windows | Aliased | Distinct | Distinct | Distinct | Distinct |

The collision classes from ASCII case through sharp-s/SS are adapter-rejected.
The width pair is still admitted, so every native job requires it to remain
distinct. These values describe the hosted runner temp volumes, not operator
payload roots.

All peers, trackers, proxies, and DHT responders in the suite are local and
generated by the tests. The ordinary suite requires permission to bind
loopback ports but does not need the internet. The Linux-only capture harness
resolves rqbit's two bootstrap names, redirects their IPv4 UDP traffic to its
local DHT probe, and blocks matching IPv6 bootstrap traffic before the capture
point. Separate public lookups prove the probe is live before and throughout a
15-second private canary window. The harness scans every captured UDP packet,
not only redirected DHT traffic, for the private binary hash and the ASCII hash
used by LSD.

The bounded mutation corpus is a fast ordinary-test regression layer, not a
claim of coverage-guided fuzzing completeness. Two separate, feature-gated
`cargo-fuzz` targets exercise the adapter-owned complete metadata-only file
admission wrapper and exact magnet preflight without creating a session or
touching the network or filesystem. The metainfo target runs across all
proxy/DHT combinations. Its nine reviewed seeds cover valid v1, private v1,
v2-only, hybrid, UDP tracker, announce-list, multifile,
private-multiple-tracker, and unsafe-path outcomes. The magnet target runs
every valid UTF-8 input in normal and proxy modes from reviewed valid-v1,
lowercase-base32, v2-only, hybrid, eager-selection, and proxy+UDP-tracker
seeds. Every reviewed seed has a deterministic named-outcome contract and
lives separately from its target's ignored evolving corpus. The targets use
bencode and URI dictionaries and cap generated input at 1 MiB and 32 KiB,
respectively; the latter permits campaigns to cross the exact 16 KiB magnet
limit. Each target runs 20,000 cases on relevant pull requests and a separate
five-minute weekly campaign. The main workspace suite passes a valid magnet at
exactly 16 KiB and a valid v1 document at exactly the 10 MiB default metainfo
ceiling, then requires the first excess byte to fail by the respective named
size boundary. It also accepts a valid 100,000-file v1 inventory below the
metainfo ceiling and requires file 100,001 to fail by the named file-count
boundary. CI does not persist either evolved corpus, prove a peak-memory
ceiling or path writes, or complete M5's symlink, mounted-filesystem,
sustained-campaign, and broader resource-exhaustion work.

The two-stage magnet guard originally closed the storage-ordering gap but not
the allocation gap inside rqbit. Maintained patch `0016` now enforces the
adapter's 10 MiB maximum before peer-buffer allocation or metadata requests.

Stable 8.1.1 also creates a private 128-permit live-peer semaphore for every
torrent. Its public session/add options cannot apply the proposal's default of
80 live peers per torrent, and there is no shared session semaphore for the
proposed 400-peer total. The adapter's 80-peer guard bounds only the explicit
and resolved bootstrap vector handed to rqbit; tracker, PEX, and DHT discovery
can still fill the engine's 128 live slots. The pinned rqbit-main snapshot
`4e5f94cbcf1d57ec500885c77cf1e24d70232d89` exposes a per-torrent
`peer_limit`, but still no session-total limit. The maintained stable series
now carries matching
stable and main candidates: every outgoing or routed incoming peer holds one
per-torrent permit and, when configured, one session-shared permit until its
manager exits. Exact-boundary tests prove two torrents share one aggregate
ceiling. The adapter pins 80/400 explicitly. This does not authorize production
networking.

Neither stable 8.1.1 nor the pinned main snapshot bounds retained peer records.
Every unique tracker, DHT, PEX, explicit, or incoming address enters a map and
unbounded peer-adder channel before a live permit is acquired; queued,
backoff, dead, and not-needed records do not count against the 80/400 manager
limits. The contribution kit now carries stable and main candidates with
caller-selected per-torrent and shared-session record permits. The permits
live inside each map record, failed shared acquisition returns the local slot,
incoming-only records are removed when their manager exits, and
alternate-address reconnects carry the retained record handle and transition
to queued before enqueue. Exact-limit and one-entry retry tests pass on both
source lines. On exact stable 8.1.1, `Peer` measured 296
bytes on macOS arm64; 4,096 raw structs are 1,212,416 bytes (1.16 MiB) before
map, allocator, and live-bitfield overhead. The accepted 1,024/4,096 limits are
now pinned by the adapter and enforced by maintained patch `0010`.

Stable 8.1.1's TCP listener accepts every socket into an unbounded set of
pending handshake checks before it can route a connection to a torrent or
acquire either live-peer permit. Each incomplete check can retain its socket,
buffers, and task for the 10-second read timeout, so a fixed timeout does not
bound concurrent pre-routing work. Current rqbit main now exposes a
per-listener `max_pending_incoming_handshake_checks` option with a default of
256. The contribution kit carries an exact stable-only 256-check backport and
tests its 255/256 boundary; its verifier checks main's native equivalent
without applying a redundant patch. Maintained patch `0009` enforces the
accepted 256 TCP-only ceiling. This does not authorize a production listener.

Stable 8.1.1 and the pinned rqbit-main snapshot also share three tracker-side
resource gaps. HTTP announces have no tracker-owned deadline, call
`Response::bytes()` without a body limit, and accept a tracker-provided zero
announce interval; UDP is clamped only to five seconds. The adapter now limits
one source to 64 tracker URLs, but input fan-out is not a per-request memory,
lifetime, or request-rate budget. Maintained patch `0005` streams at most 1 MiB
within 30 seconds and clamps unforced HTTP/UDP intervals to at least 60
seconds. Current-main remains contribution material; production wiring remains
disabled.

The asynchronous-queue audit found a separate post-handshake boundary. A live
peer can pipeline valid piece requests into an unbounded torrent upload
scheduler and then an unbounded per-peer writer while upload rate limits or a
slow socket delay draining. Valid BEP 9 metadata requests bypass the scheduler
and enter that same writer directly. A socket timeout bounds one write's
lifetime, not the number of queued response records. The contribution kit now
carries stable and main candidates with one 128-permit response window per
peer and advertises that window as BEP 10 `reqq`. Admission is non-blocking: a
peer that exceeds the advertised outstanding-response window is disconnected,
so its socket reader never waits behind torrent-global upload throttling. Each
permit follows a piece response through scheduler and writer or a metadata
response through the writer and is released only after the socket write
completes or is cancelled. Production-path admission and blocked-write tests
fail when those guards are intentionally bypassed. Maintained patch `0012`
enforces the accepted 128-request window and over-window disconnect behavior.

The same audit followed discovery work back before live-peer admission. Both
source lines use unbounded channels for outgoing DHT datagrams, recursive
nodes, and delivered peers; recursive futures can also grow without a fixed
active-work limit. Bucket refreshes and questionable-node pings add unbounded
maintenance channels/future sets, while bootstrap starts every configured
hostname concurrently. That finite configuration fan-out is intentionally
unchanged because an eight-host concurrency window can let one hostname's
24-hour retry budget starve later entries. The magnet metadata resolver's
semaphore limits active I/O but not queued futures or its retained
unique-address set. Current main adds an unbounded LSD result stream and leaves
its periodic announcer alive
after the stream is dropped. The contribution kit now carries stable and main
candidates with 256-record DHT send, recursive-node, and delivered-peer queues;
32 active recursive requests per worker; 256 queued and 32 active DHT
maintenance requests per worker; 128 active metadata attempts; 256 pending
metadata candidates; and a 4,096-entry deduplication set that does not
terminate discovery. The main candidate also bounds LSD results at 256, ties
the announce task to the stream
lifecycle, protects replacement registrations, and keeps protocol replies
independent of local result saturation. Both candidates process recursive
nodes before best-effort peer delivery, use the
normal requery delay when the node queue is saturated, and reuse one DHT
response per recursive step instead of issuing the same request twice.
Exact-boundary proofs pass and deliberate DHT/LSD guard regressions fail.
Maintained patch `0014` enforces the stable-line DHT and metadata limits;
current-main LSD changes remain optional drift material. Production discovery
remains disabled.

### 2.3 Queue schema version fallback

The independent M1a groundwork adds a versioned queue envelope:

- missing `schema_version` reads as legacy version 1;
- new writes use version 2;
- a future version fails by name. Healthy current snapshots parse once; if a
  typed parse fails, a header-only fallback distinguishes a future version
  from ordinary corruption so a future enum cannot masquerade as a generic
  parse failure;
- versions older than the supported floor fail by name; and
- the writer refuses to emit a non-current schema.

This is useful even while the engine decision is blocked. No torrent enum
variant or scheduler path was added.

### 2.4 Executable platform and dependency gate

`.github/workflows/bittorrent-m0.yml` turns gate 1 from a workstation claim
into a repeatable native matrix. Under Rust 1.85 it runs the real isolated
adapter suite — local seeder, tracker, authenticated SOCKS relay, persistence
contract, provider check, and input validation — on:

- Linux x86-64 glibc;
- macOS arm64;
- Windows x86-64 MSVC;
- Linux x86-64 musl; and
- Linux arm64 musl.

The two musl jobs run on their native CPU architectures and link and execute
the tests; they are not `cargo check` substitutes. The workflow is path-gated
to the adapter, lockfile, workspace dependency declaration, daemon TLS
provider initialization, its own workflow, and the policy script, so unrelated
documentation changes do not burn five native builders.

The first matrix run did its job before this report claimed success: Windows
failed while compiling Tokio 1.53.0 because that target's signal path calls
`OnceLock::wait`, which is unstable on Rust 1.85. The Linux-only workspace MSRV
check could not see the target-specific code. The workspace therefore pins
Tokio 1.52.4: upstream declares Rust 1.71 and its Windows signal path uses
`OnceLock::get_or_init` instead. The
[corrected native run](https://github.com/pjunod/nzbd/actions/runs/31060326800)
completed successfully on 2026-08-06 UTC. All five platform jobs executed the
isolated adapter suite under Rust 1.85, including the corrected Windows path,
and the dependency-policy job passed.

Before those builders run, `scripts/check-bittorrent-deps.sh` checks the normal
and feature graphs under the MSRV toolchain. It requires exactly
`librqbit 8.1.1`, requires `rust-tls`, rejects librqbit's default feature set,
and rejects `openssl`, `openssl-sys`, or `native-tls` anywhere in the adapter's
normal dependency closure. This guards the accepted dependency shape; it does
not replace binary linkage inspection or the gate 9 license/vulnerability
review.

Adding the gate starts no session in the daemon and changes no config, API,
listener, admission, or recovery behavior. The successful corrected run makes
gate 1 a pass; the maintained-engine rerun refreshes that evidence without
changing daemon reachability.

---

## 3. Historical gaps and the selected maintained fixes

### 3.1 Historical gap — automatic restore outranked nzbd state

The proposal requires this startup order:

1. load and validate nzbd’s queue snapshot;
2. determine the authoritative durable torrent jobs;
3. create the library session without starting anything else; and
4. explicitly restore only those jobs, using library resume data as an
   accelerator.

Stable 8.1.1 does something different whenever `SessionOptions.persistence`
is set: `Session::new_with_opts` opens the store, streams every stored record,
adds them all, and only then returns the session. `fastresume` shares that
persistence path. The public option selects JSON or Postgres storage, but it
does not provide an `auto_restore = false` switch, an authoritative ID filter,
or a public custom-store constructor. The `session_persistence` module itself
is private.

The contract test creates one persisted paused torrent, stops the session, and
constructs a second session without re-adding any nzbd job. The second session
already contains the torrent. That is a second queue authority, not merely a
cache.

Rejected workarounds:

- parsing or rewriting librqbit’s private `session.json` schema before boot;
- deleting resume records opportunistically and hoping crash order agrees;
- auto-restoring first, then stopping “ghost” torrents after they may already
  have announced or opened payload files; and
- copying the private persistence module into nzbd.

The acceptable engine API is small: either separate resume storage from
automatic admission, add an authoritative restore predicate/list, or accept a
public persistence store whose enumeration nzbd controls before session start.

### 3.2 Required fact boundary — detailed discovery remains optional

`ManagedTorrent::stats()` is useful and the adapter now projects its stable
transfer/peer facts. Tracker monitors, however, keep their last errors inside
private tasks and tracing output. The public torrent stats model does not
answer:

- which tracker is working, backing off, or rejected;
- the last redacted tracker failure;
- whether DHT is healthy, disabled, or degraded for this job; or
- whether a trackerless magnet is stalled on discovery rather than peers.

Inferring these states from a zero peer count would make the API lie: a healthy
tracker can return no peers, and a failed tracker can coexist with live peers.
The amended observability contract requires those details to remain explicit
diagnostics, not inferred facts. The maintained candidate returns `unknown`
for unavailable discovery detail while still exposing the required transfer,
verified-progress, completion, peer, admission, and bounded-error facts.

### 3.3 The authoritative-restore patch is selected and shipped locally

[`contrib/rqbit/0001-allow-persistence-without-auto-restore.patch`](../contrib/rqbit/0001-allow-persistence-without-auto-restore.patch)
targets rqbit v8.1.1 commit `00b97485160ff5b5aa2b379ea0815d568ec665f0`.
It adds one backwards-compatible `SessionOptions::disable_auto_restore` flag.
The default remains false, so existing rqbit callers still restore every
record. Setting it true keeps JSON persistence and fast resume active while
skipping only the constructor's implicit admission loop.

Both the exact v8.1.1 backport and the contribution patch for rqbit main at
`4e5f94cbcf1d57ec500885c77cf1e24d70232d89` carry the same contract. The test
persists two paused torrents with deterministic four-piece payloads after the
authoritative torrent validates exactly one 16 KiB piece. It then replaces that
file with the complete valid 64 KiB payload before restart, constructs a
session that starts empty, and explicitly restores only the authoritative
torrent with its persisted `preferred_id`. The restored torrent still reports
only the persisted 16 KiB and remains incomplete; a full disk recheck would
report 64 KiB and complete. Finally, a legacy session still restores both
records. That verifies the ownership and non-empty fast-resume seams without
exposing the private persistence schema or deleting library state before
startup.

[`scripts/check-rqbit-authoritative-restore-patch.sh`](../scripts/check-rqbit-authoritative-restore-patch.sh)
clones a supplied clean v8.1.1 or rqbit-main tree locally, requires the exact
stable tag or a descendant of the documented main base, reports the verified
SHA, applies the matching patch directly or by three-way fallback on main,
verifies formatting, and runs the isolated Rust-TLS test. The dedicated
`rqbit contribution patches` workflow checks both variants on relevant changes
and weekly for upstream-main drift. Both variants passed locally on 2026-08-06.
The stable patch is now part of the selected maintained series and the adapter
sets `disable_auto_restore = true`; no daemon session consumes the option.

### 3.4 Detailed discovery-health patch is optional contribution material

[`contrib/rqbit/0003-expose-per-torrent-discovery-health.patch`](../contrib/rqbit/0003-expose-per-torrent-discovery-health.patch)
targets the exact v8.1.1 commit. Its public `ManagedTorrent::discovery_health`
snapshot distinguishes disabled, private-suppressed, inactive, searching,
working, and degraded DHT states, with current-run request and peer counters.
Each tracker reports a stable state, next-announcement delay, and bounded last
failure category. Public endpoints retain only scheme, host, and port; path,
query, user information, response body, and credentials never enter the
snapshot.

The corresponding rqbit-main patch starts at the same documented main base as
the authoritative-restore contribution. Tests prove endpoint and serialized
snapshot redaction, private-torrent DHT suppression, DHT success/failure
transitions, HTTP failure classification, tracker backoff, and rejected
tracker retention. The focused stable suites pass 37 tests with three
pre-existing network tests ignored; the focused main suites pass 69 tests
with six upstream integration tests ignored.

[`scripts/check-rqbit-discovery-health-patch.sh`](../scripts/check-rqbit-discovery-health-patch.sh)
uses the same exact-stable and descendant-of-main rules as the restore
verifier, reports the tested SHA, allows a three-way fallback only for main,
checks formatting, and runs all three affected crate suites. The dedicated
workflow tests all contribution candidates against stable and main on relevant
changes and weekly for upstream drift. Patch `0003` is intentionally absent
from the maintained dependency: detailed tracker/DHT state may be added later,
but gate 7 passes by reporting unavailable diagnostics as `unknown`.

### 3.5 The peer-metadata allocation ceiling is selected and shipped locally

Stable rqbit rejects BEP 9 metadata above 32 MiB, but the ceiling is hardcoded
inside its peer metadata reader. An embedding application can reject resolved
metainfo afterward, but that is too late to enforce nzbd's proposed 10 MiB
hostile-input allocation limit for magnets.

[`contrib/rqbit/0016-limit-peer-metadata-before-allocation.patch`](../contrib/rqbit/0016-limit-peer-metadata-before-allocation.patch)
targets the exact v8.1.1 commit, and `0017` carries the same change for the
documented rqbit-main base. They add an optional
`PeerConnectionOptions::max_metadata_size`, merge it through the existing
session/per-add peer-option path, retain 32 MiB when it is unset, and reject
an oversized extended handshake before allocating a buffer or sending
metadata requests. The focused test accepts the exact configured boundary and
rejects one byte above it.

[`scripts/check-rqbit-metadata-size-limit-patch.sh`](../scripts/check-rqbit-metadata-size-limit-patch.sh)
applies the matching patch to a clean stable or main tree, checks formatting,
and runs that test. The contribution workflow exercises both variants on
relevant changes and weekly for upstream drift. The exact-stable patch is part
of the maintained series, and the adapter pins `max_metadata_size` to 10 MiB.

### 3.6 The file-sizing error fix is selected and shipped locally

The five-platform storage witness proves stable rqbit 8.1.1 treats an
`ensure_file_length` failure as warning-log-only and continues initialization.
That behavior is not usable as nzbd's ENOSPC control contract.

[`contrib/rqbit/0018-propagate-file-sizing-errors.patch`](../contrib/rqbit/0018-propagate-file-sizing-errors.patch)
targets the exact v8.1.1 commit, and `0019` carries the equivalent current-main
candidate. Both extract the selected-file sizing loop into a fallible helper,
preserve selection and padding behavior, attach the file name and requested
length as error context, and return the first storage failure through the
existing initialization state transition.

Current rqbit main also treats any initialization error as a successful pause
when a pause request overlaps the check. The `0019` candidate replaces the
checksum loop's string-only cancellation with a typed marker and suppresses
only that marker. Its focused race classifier proves an injected storage error
remains non-suppressible while the pause flag is set, while genuine checksum
cancellation is suppressed only with that flag.

The exact unit proof supplies a storage implementation that returns injected
`StorageFull` for file 0 at 262,144 bytes. It requires one call and an error
chain that retains the file name, byte count, and cause. A sensitivity mutation
inside the helper made the test fail because `unwrap_err()` received `Ok(())`;
restoring propagation made it pass. Because that focused test calls the helper
directly, the verifier separately pins each source line's blocking
initialization call through its trailing `?`. A mutation that discarded the
helper result at that boundary left the unit test green but failed this
structural assertion. The verifier also requires the exact test in Cargo's
list, requires an exact one-test pass summary for every applicable test, pins
the current-main pause classifier at the real error branch, checks formatting,
and compiles the Rust-TLS library. Pull requests have blocking exact-stable and
documented-main-base legs plus an advisory moving-main drift leg; pushes and
the weekly schedule require all three.

The exact-stable patch is part of the maintained series. Current-main remains
contribution material. M2a defines dormant torrent storage paths and M2b adds a
dormant runtime boundary, but the production daemon does not consume those
config paths while `enabled = true` is rejected; the M2 storage-full row remains
dependency-gated, and production wiring remains disabled.

---

## 4. Measurements and dependency review

Measurements are from one run of the isolated optimized `m0_idle` harness on
macOS 26.6 arm64. They are not the final daemon delta because the daemon
intentionally does not link the blocked adapter.

| Measurement | Result |
|---|---:|
| Unstripped optimized harness | 10,111,360 bytes (9.64 MiB) |
| Maximum resident set while starting/stopping one idle session | 8,814,592 bytes (8.41 MiB) |
| Normal dependency closure of `nzbd-torrent` | 222 unique package/version identities |
| New package/version identities in the workspace lockfile | 178 |
| OpenSSL dynamic link | none; only CoreFoundation, libiconv, and libSystem were listed on this host |

The
[maintained-engine cross-platform M0 matrix at `771cf5b`](https://github.com/pjunod/nzbd/actions/runs/31846437027)
passed both optimized probes on 2026-08-14 UTC after deriving the exact
nine-patch engine. Values are process resident-set samples in bytes; the time
column is admission plus shutdown for the session probe and validation for the
metainfo probe.

| Platform | Probe | Baseline RSS | Maximum sampled RSS | Sampled growth | Time |
|---|---|---:|---:|---:|---:|
| Linux aarch64 musl | 100-torrent session | 1,859,584 | 6,107,136 | 4,247,552 | 51 ms + 1,001 ms |
| Linux aarch64 musl | 100,000-file preflight | 745,472 | 4,591,616 | 3,846,144 | 84 ms |
| Linux x86_64 GNU | 100-torrent session | 4,739,072 | 9,883,648 | 5,144,576 | 78 ms + 1,000 ms |
| Linux x86_64 GNU | 100,000-file preflight | 2,633,728 | 18,108,416 | 15,474,688 | 85 ms |
| Linux x86_64 musl | 100-torrent session | 4,919,296 | 9,560,064 | 4,640,768 | 58 ms + 1,000 ms |
| Linux x86_64 musl | 100,000-file preflight | 831,488 | 4,751,360 | 3,919,872 | 117 ms |
| macOS aarch64 | 100-torrent session | 6,979,584 | 10,502,144 | 3,522,560 | 64 ms + 1,003 ms |
| macOS aarch64 | 100,000-file preflight | 5,980,160 | 33,275,904 | 27,295,744 | 76 ms |
| Windows x86_64 MSVC | 100-torrent session | 8,343,552 | 11,206,656 | 2,863,104 | 2,608 ms + 1,003 ms |
| Windows x86_64 MSVC | 100,000-file preflight | 4,722,688 | 8,491,008 | 3,768,320 | 141 ms |

All session growth results stayed below 5 MiB and all metainfo growth
results stayed below 27 MiB, below the preliminary 32 MiB and 64 MiB
regression ceilings. Those ceilings retain headroom for hosted-runner noise
without permitting the previous order-of-magnitude regressions. The wide
platform spread is why these remain
platform-specific sampled-growth guards, not portable peak-memory promises.

The
[hardened storage-fault run](https://github.com/pjunod/nzbd/actions/runs/31345445318)
passed the behavioral probe and its fail-closed discovery guard on all five
native targets on 2026-08-10 UTC. Every target injected `StorageFull` on the
second write after one 16,384-byte write call returned successfully, kept the
faulted 262,144-byte torrent incomplete in `Error` with zero engine progress,
and preserved exactly two write attempts and one successful write after the
already-live 67,108,864-byte control torrent completed. The control was still
incomplete at the fault boundary on every target. The response deadline
measures the independently scheduled stats reply rather than synchronous lock
acquisition on the test task. The shared runner also proved that it discovered
and executed exactly the one ignored test, after a discriminating negative
control demonstrated that the discovery guard rejects a non-matching line.

| Platform | Fault transition | Stats response | Control progress at fault | Control completion |
|---|---:|---:|---:|---:|
| Linux aarch64 musl | 52 ms | 0 ms | 8,847,360 bytes | 7,098 ms |
| Linux x86_64 GNU | 77 ms | 0 ms | 8,962,048 bytes | 7,097 ms |
| Linux x86_64 musl | 52 ms | 0 ms | 8,814,592 bytes | 7,098 ms |
| macOS aarch64 | 26 ms | 0 ms | 8,323,072 bytes | 7,020 ms |
| Windows x86_64 MSVC | 29 ms | 0 ms | 16,384 bytes | 7,315 ms |

This evidence remains deliberately narrow: it proves the injected write-time
path and an already-active sibling's containment, not durable persistence of
the accepted chunk.

The historical initialization-time proof injected `StorageFull` into
`ensure_file_length` for a paused, peerless 262,144-byte torrent. The
[replacement storage-fault run](https://github.com/pjunod/nzbd/actions/runs/31654021866)
passed the two then-current ignored proofs and the shared runner's
discriminating discovery negative control on all five native targets on
2026-08-13 UTC.
Every target observed exactly one 262,144-byte sizing request, a zero-byte
filesystem object, zero piece writes, successful initialization into `Paused`,
a subsequent transition to `Live`, no stats-visible error, and no sizing retry
before that observation. Every target returned the stats response in 0 ms and
completed the sizing witness in about one second. This is adverse evidence:
stable rqbit emits a warning log but continues without durable control-plane
fault state after the allocation failure.

Maintained patch `0018` deliberately invalidates that adverse witness. The
current exact ignored proof requires the same request and filesystem facts but
also requires a bounded file/length/cause error during admission or
initialization; an asynchronously returned handle must enter `Error` and may
never report `Paused` or `Live`. The
[maintained-engine native M0 matrix at `771cf5b`](https://github.com/pjunod/nzbd/actions/runs/31846437027)
passed on all five targets: each recorded one 262,144-byte sizing request, a
zero-byte file, zero writes, and `control_plane=torrent_error`. The M2
storage-full row remains blocked because this engine result is not yet routed
into daemon pause, API, recovery, or existing-upload policy. Neither injected
path simulates every real-filesystem ENOSPC or allocation policy.

The 2026-08-07 review remediation refreshed these measurements after adding
`icu_casemap 2.1.1` and `icu_casemap_data 2.1.1` for Unicode simple case
folding. The change added two package identities, 36,400 binary bytes, and
16,384 bytes to the one-sample idle RSS measurement.

The normal closure reports permissive licenses: MIT, Apache-2.0, BSD, ISC,
Unicode-3.0, Zlib, Unlicense, CDLA-Permissive-2.0, and related compatible
expressions. `option-ext 0.2.0` is MPL-2.0. No GPL, AGPL, LGPL, SSPL, or BUSL
identifier appeared in the normal closure. This is an inventory, not legal
advice or final acceptance.

The initial resolver selected 2026 transitive releases requiring Rust 1.86 or
1.88. The lockfile now pins compatible releases in the allowed semver ranges,
including `serde_with 3.14.1`, `idna_adapter 1.2.1`, and ICU4X 2.1.x. With the
then-existing `time 0.3.41` MSRV pin restored, the actual Rust 1.85.1
toolchain passed `cargo check --workspace --all-targets` on macOS. The
2026-09-19 clustering compiler-floor upgrade moved the repository to Rust
1.95 and fixed `time 0.3.51`; that historical pin and exception are gone.

The checked-in [`deny.toml`](../deny.toml) policy passes locally with
cargo-deny 0.20.2 across all features and the locked graph. It fails on an
unapproved license, registry, Git source, wildcard dependency, OpenSSL/native
TLS dependency, vulnerable or yanked crate, or unmaintained direct workspace
dependency. `option-ext 0.2.0` is the only license exception; its MPL-2.0
file-level copyleft is accepted at that exact version.

The first advisory run found and removed the daemon's direct
`rustls-pemfile 2.2.0` dependency. The daemon now parses certificates and
private keys through the maintained `rustls-pki-types` API already re-exported
by rustls. Two exact advisory exceptions remain:

- [`RUSTSEC-2026-0194`](https://rustsec.org/advisories/RUSTSEC-2026-0194)
  and
  [`RUSTSEC-2026-0195`](https://rustsec.org/advisories/RUSTSEC-2026-0195)
  affect quick-xml 0.37 through librqbit 8.1.1's unconditional UPnP helper.
  The path is reachable only when UPnP port forwarding is enabled. The adapter
  no longer exposes an UPnP input and unconditionally constructs rqbit with
  port forwarding false. A production gate must remove both exceptions or
  keep UPnP unavailable.

The former `RUSTSEC-2026-0009` allowance was removed when the Rust 1.95
compiler floor selected fixed `time 0.3.51`.

[`supply-chain.yml`](../.github/workflows/supply-chain.yml) pins
`cargo-deny-action` to an immutable revision and runs separate blocking policy
and RustSec jobs on every pull request, main or release-tag push, a daily
schedule, and manual dispatch. The
[first blocking run](https://github.com/pjunod/nzbd/actions/runs/31064446916)
passed both jobs on 2026-08-06 UTC. The recorded delta and the narrow advisory
dispositions were accepted on 2026-08-14. The maintained series now enforces
the runtime boundaries that made gate 9 Partial. Its combined verifier proves
the upstream checksum, exact series, generated vendor, focused tests, and
affected crate suites. The 2026-08-14 maintained-engine run refreshed the
native measurements on all five targets. A green exception alone is not
evidence that the
underlying code became safe to enable.

[`scripts/check-reviewed-dependency-exceptions.sh`](../scripts/check-reviewed-dependency-exceptions.sh)
now makes the disposition executable in the repository-wide Supply chain
workflow. It requires the reviewed quick-xml package set, the exact `time`
package and `alloc`/`std` feature sets, the MPL-2.0 `option-ext` path, and only
the three named RustSec ignores. Workspace-member versions are normalized, and
an obsolete package pin fails with the exception that must be reconsidered.
The adapter unit test independently proves that its constructed rqbit options
cannot enable UPnP. The [gate 9 review brief](BITTORRENT_GATE9_REVIEW.md)
collects the measurements, exceptions, renewal rules, and the exact decision
that was put to the reviewer; its §4.1 records the accepted disposition. This
report states the outcome and does not widen it.

---

## 5. Recorded acceptance path and result

The accepted path was:

1. derive the exact vendor from the checked v8.1.1 archive and nine-patch
   series, then reject any checksum, membership, order, application, vendor, or
   focused-test drift;
2. run all eleven M0 gates on native macOS, Linux glibc/musl, and Windows;
3. run the packet-capture private-mode test and confirm the maintained engine
   enforces every boundary accepted in
   [gate 9 review brief §4.1](BITTORRENT_GATE9_REVIEW.md#41-recorded-disposition--accepted-2026-08-14),
   including the final-daemon and per-platform memory remeasurements that
   disposition requires; and
4. obtain independent review, then decompose M2 without adding daemon wiring
   to this M0 change.

**Result (2026-08-14):** all four steps are complete for the maintained-engine
baseline. M2 was dependency-decomposed from current main on 2026-08-22 and its
contracts were corrected after adversarial review. This result does not add a
daemon dependency, listener, admission route, or production feature switch.

The human-review checklist, submission order, issue draft, PR draft, exact
patch mapping, and reproduction commands are collected in the
[`contrib/rqbit` contribution kit](../contrib/rqbit/README.md). Nothing in that
kit has been posted upstream, and rqbit's AI policy requires human review and
editing before it is.

The stable changes may still be proposed upstream, but upstream acceptance is
not an M0 gate. Patch `0003` detailed discovery health and all current-main
variants remain optional contribution material. Removing a required stable
patch or switching to full-recheck-only recovery requires an ADR-19 amendment;
full recheck is a degradation path, not the chosen resume model. The
`libtorrent-rasterbar` fallback remains available if the maintained series
stops being small, reviewable, reproducible, or portable.

The later ADR-19 re-check separated M1b from this engine gate: a fake-only,
protocol-neutral queue/backend seam is useful for either engine and starts no
networking, so it may proceed under the dormant limits recorded in
[BITTORRENT_M1B_REPORT.md](BITTORRENT_M1B_REPORT.md). Until the native M0
evidence and independent review completed, `nzbd-torrent` remained a tested
spike boundary. They are now accepted; M2 still must land as complete,
reviewed milestone slices rather than exposing a half-wired feature flag,
admission route, or peer listener.

---

## 6. Reproduction summary

The key local gates are:

```sh
scripts/check-bittorrent-deps.sh
scripts/check-rqbit-maintained-patch-series.sh
scripts/check-bittorrent-fuzz-deps.sh
scripts/check-reviewed-dependency-exceptions.sh
cargo deny --all-features --locked check bans licenses sources
cargo deny --all-features --locked check advisories
cargo test -p nzbd-torrent -- --nocapture
cargo test -p nzbd-state snapshot
cargo check -p nzbd-torrent

# Requires nightly-2026-08-01 and cargo-fuzz 0.13.2.
make fuzz-test
make fuzz-metainfo
make fuzz-metainfo FUZZ_SECONDS=300
make fuzz-magnet
make fuzz-magnet FUZZ_SECONDS=300
make gate
make gate-fuzz

# Linux only; requires passwordless sudo, iptables/ip6tables, and tcpdump.
scripts/check-private-discovery-leaks.sh

# Against a clean local checkout of rqbit v8.1.1 or main at the documented SHA.
scripts/check-rqbit-authoritative-restore-patch.sh /path/to/rqbit
scripts/check-rqbit-discovery-health-patch.sh /path/to/rqbit
scripts/check-rqbit-tracker-request-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-session-peer-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-pending-handshake-budget.sh /path/to/rqbit
scripts/check-rqbit-known-peer-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-metadata-size-limit-patch.sh /path/to/rqbit
scripts/check-rqbit-file-sizing-error-patch.sh /path/to/rqbit
scripts/check-rqbit-peer-response-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-discovery-pressure-patch.sh /path/to/rqbit
```

`make gate` is the deterministic local release entry point: formatting,
strict lint, the whole workspace suite, Rust 1.85 checking, the dormant
adapter dependency boundary, the absence of `nzbd-torrent` and every
`librqbit*` package from the production daemon's normal dependency graph,
reviewed dependency exceptions, and committed fuzz seed contracts. The
`make gate-fuzz` target runs that gate first and then both bounded 20,000-case
libFuzzer campaigns by default. It requires the pinned nightly toolchain and
cargo-fuzz used by the BitTorrent fuzz workflow. The scheduled five-minute
campaigns remain separate CI evidence.

The Rust 1.85 check must select both the 1.85 Cargo and `rustc`; this host also
has a newer Homebrew compiler on `PATH`. The native platform matrix should run
in CI rather than treating missing cross-C compilers on macOS as a code result.

For public API evidence, see the pinned
[`SessionOptions`](https://docs.rs/librqbit/8.1.1/librqbit/struct.SessionOptions.html),
[`Session` construction source](https://docs.rs/librqbit/8.1.1/src/librqbit/session.rs.html#695-726),
and [`TorrentStats`](https://docs.rs/librqbit/8.1.1/librqbit/struct.TorrentStats.html).
