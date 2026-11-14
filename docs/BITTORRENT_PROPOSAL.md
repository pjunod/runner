# BitTorrent support — one queue, two transfer protocols

**Security amendment, 2026-11-13:** maintained patch
[`0023-upgrade-upnp-xml-parser.patch`](../contrib/rqbit/0023-upgrade-upnp-xml-parser.patch)
upgrades the compiled UPnP helper to fixed quick-xml 0.41 and removes both
XML advisory exceptions. UPnP remains unavailable. Earlier exception tables
and native matrix measurements below record historical evidence; the
[gate 9 advisory scope](BITTORRENT_GATE9_REVIEW.md#2-advisory-scope--fixed-parser-no-rustsec-exceptions)
is authoritative for the updated dependency policy.

**Status:** ADR-19 implemented for single-node v1; M2 lifecycle/policy/activation,
M3 native surfaces, and M4 qBittorrent compatibility are implemented; the
magnet DHT amendment is locally qualified and awaits PR publication ·
**Decision:** pin the reproducibly derived twelve-patch rqbit v8.1.1 engine;
complete single-node M2–M5 before separately approving M6 ·
**Written:** 2026-08-05 · **Revised:** 2026-09-20 ·
**Verified against:** rqbit v8.1.1 (`00b97485160ff5b5aa2b379ea0815d568ec665f0`) ·
**Cluster reuse baseline:** plurx `7c781e5f5e28ac8114bacb1919a463a6a18e2680` ·
**Post-baseline cluster audit:** plurx `64e96aa71f58ebb3ec2c7b71d5c36a537b50aba6` ·
**Scope:** architecture, contracts, milestones, and review questions; M6
cluster ownership remains separately gated

Companion to [ARCHITECTURE.md](ARCHITECTURE.md) (how nzbd is built),
[INTEGRATION.md](INTEGRATION.md) (the current consumer seams), and
[CLUSTERING.md](CLUSTERING.md) (whole-job leases and fencing) — this is the
proposal for making BitTorrent a first-class transfer protocol without
turning the Usenet engine into a pile of torrent-shaped exceptions.

Read §1–§4 before reviewing individual interfaces. The central constraint is
in §3.4: a completed torrent may still be uploading, so “download finished”
cannot mean “move, rename, clean up, and forget the job” as it does for
Usenet. The implementation plan is §15. Work milestone by milestone; every
milestone preserves the existing NZBGet surface and the current Usenet test
suite. Re-verify every source anchor against HEAD before building. If the
maintained derivation or a future dependency change stops proving the gates in
§4.3, stop and return to the engine decision instead of coding around a
missing capability.

The first 2026-08-05 review accepted the architecture and found that the original
draft had attributed uTP and IPv6 support from the 9.x development line to
the stable 8.1.1 release. This revision deliberately chooses the stable
release and narrows v1 to TCP and IPv4. It also registers the decision as
ADR-19, describes downgrade behavior as a whole-queue startup failure, makes
schema versioning part of M1a, removes a premature `nzbd-usenet` extraction,
and treats the enforcing disk guard, qBittorrent login throttling, and torrent
watch path as construction rather than reuse. The first M0 run is now recorded
in [BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md). It proved the core data
path but also found two stable-API gaps: fast-resume persistence restores its
own complete torrent list before nzbd can reconcile it, and tracker/DHT health
is not available through the public per-torrent stats model. The isolated
adapter and schema groundwork remained; production session wiring stopped here
until ADR-19 was amended with a maintained fix or a different engine. The
2026-08-14 amendment in §4.3.3 makes that decision: nzbd derives a pinned local
rqbit v8.1.1 vendor from the immutable release archive plus a small ordered
patch series. The 2026-09-20 magnet amendment adds the eleventh patch: a
feature-gated loopback-test constructor, not an operator bootstrap setting.
The 2026-09-22 tracker-compliance amendment adds the twelfth: Runner's client
identity and a BEP 3/15-conformant announce lifecycle (§4.3.3 item 12).
This is an explicit dependency boundary, not permission to wire another
daemon session.

A same-day re-check of stable 8.1.1 and rqbit's unreleased 9.0 branch found
that both original M0 blockers remained. That result changed the milestone
dependency, not the production gate: M1b's queue schema, scheduler boundary,
and fake backend were useful for any embedded engine and started no peer
session, so they proceeded independently. The historical maintained-engine
series and its independent review accepted all eleven M0 gates. The later
test-support patch for deterministic DHT bootstrap passed the maintained
derivation, focused upstream suites, release DHT assertion, and upstream
workspace check; it does not alter the production engine path. M2 was
decomposed on 2026-08-22 and corrected after an adversarial plan review. M2a's dormant
`[torrent]` config and fail-closed activation guard and M2b's dormant selective
runtime ownership have since merged, but no production admission route,
listener, or daemon session is present merely because those slices cleared.

The Fable review later that day found a real proxy leak boundary and several
plan inconsistencies. That revision rejects proxy+DHT and proxy+UDP trackers,
originally rejected privacy-unknown magnets in a DHT-enabled session, disables
process-global DHT persistence, completes v2/hybrid admission checks
for metainfo, reserves schema version 3 for torrent job records, defines how a
stalled torrent yields the shared slot, and originally split completed M1a
schema work from then-blocked M1b routing. The §4.3.2 amendment then permitted
that dormant routing seam while gates 7 and 8 still blocked production wiring.
The maintained-engine amendment in §4.3.3 supersedes that gate state.

The 2026-09-20 amendment supersedes only the blanket magnet rejection. DHT-on
sessions may query an unresolved v1 hash, resolve metadata in list-only mode,
and admit it only after full validation proves it public. A private result is
rejected before managed storage, but the earlier public hash query cannot be
retracted. DHT remains off by default; known-private metainfo and proxy rules
retain their previous fail-closed boundaries.

> BitTorrent publishes the client’s IP address to peers and may upload data
> after the download completes. nzbd can provide controls and honest status;
> it cannot make public peer-to-peer traffic private or decide whether the
> operator has the right to share particular content.

---

## 1. Recommendation — embed `librqbit`, keep nzbd authoritative

Add BitTorrent as a second transfer backend under nzbd’s existing durable
queue, API, event stream, UI, history, and category model. Use
[`librqbit`](https://docs.rs/librqbit/8.1.1/librqbit/) as an embedded library,
wrapped by a new `nzbd-torrent` crate. Do not run a second daemon and do not
reimplement the peer protocol.

The user-visible result is one nzbd process with:

- `.torrent` and magnet admission through the native API, web UI, mobile app,
  and a torrent watch directory;
- downloads, uploads, peers, ratio, ETA, and protocol shown in the same queue;
- pause, resume, priority, delete, and delete-with-data using the existing
  control vocabulary;
- a small qBittorrent Web API compatibility shim so Sonarr and Radarr can add
  and monitor torrents without changes;
- verified payloads left in place while they seed;
- the current Usenet behavior and NZBGet compatibility left unchanged.

The first production release is single-node and BitTorrent v1 only unless the
M0 interop spike proves more. Cluster torrent leases, torrent post-processing,
streaming, and broad qBittorrent API compatibility are later work. This is a
boundary, not an apology: the first release should be a trustworthy download
client before it becomes a feature catalogue.

### 1.1 The decisions in one page

| Question | Proposed answer | Why |
|---|---|---|
| Torrent engine | Embed the maintained rqbit v8.1.1 source derivation with Rust TLS; TCP/IPv4 in v1 | The immutable upstream archive, checksum, ordered patch series, generated vendor, and CI drift proof form one reviewed dependency. uTP and IPv6 remain outside the first release. |
| Queue ownership | nzbd remains the source of truth | A library resume file may accelerate recovery; it must not become a second job database. |
| Domain model | Add a protocol-specific transfer record; do not fake pieces as NNTP articles | A peer, piece, tracker, and upload ratio have no honest mapping to server, segment, health, or par2. |
| *arr integration | Emulate only the qBittorrent endpoints current Sonarr/Radarr use | The existing NZBGet shim cannot receive torrent grabs, and a full qBittorrent clone would be unrelated scope. |
| Completion | Add durable `ready` state; keep seeding jobs live | A torrent is importable before it is removable. |
| Post-processing | Off for torrents in the first release | nzbd’s PP pipeline renames and deletes inputs, which would break seeding. |
| Seeding default | Unlimited until the caller or operator sets a limit | Guessing a ratio can cause tracker penalties; silently deleting at a limit can race import. |
| Port mapping | Unavailable in the first release | Stable rqbit's UPnP helper carries advisory-affected XML code. A future explicit opt-in requires a patched dependency and a fresh security review because it mutates the router and exposes an inbound service. |
| Authentication | Reuse `[api]` credentials and token | One process should not grow a second credential database for a compatibility route. |
| Cluster | Single-node first; whole-torrent leases later | Two sessions writing one payload is not an acceptable approximation of distribution. |

### 1.2 Success means this exact workflow works

```
 Sonarr/Radarr ── qBittorrent Web API ──▶ nzbd
       │                                  │
       │ magnet or .torrent               ▼
       │                         durable unified queue
       │                                  │
       │                          librqbit session
       │                         peers · pieces · hash
       │                                  │
       │                    verified payload becomes READY
       │                                  │
       └──── polls content_path ◀─────────┤
                                          │
                              remains live and SEEDING
                                          │
                       ratio/time reached or caller removes
                                          ▼
                                  terminal history
```

How to read it: `READY` is the handoff point; `SEEDING` is continuing transfer
work. They commonly coexist. Treating either one as the other causes one of
two failures: import waits forever for seeding to end, or post-processing
destroys the files being seeded.

---

## 2. Current architecture — what can be reused and what cannot

The proposal is based on the code at `83efc9da7`, not on an abstract download
client. These are the seams an implementation will actually encounter.

### 2.1 Existing foundations and the work each still requires

Calling a component reusable does not make its current contract
protocol-neutral. This table distinguishes a foundation that can stay from a
surface that must be extended:

| Existing component | Keep | New BitTorrent work |
|---|---|---|
| Queue authority and commands (`nzbd-engine/src/owner.rs`) | One owner, admission order, priority, pause/resume/delete, durable event ordering | Backend command routing and cross-protocol scheduling semantics |
| Arc-swap snapshot (`nzbd-engine/src/snapshot.rs`) | Lock-free immutable publication, existing 1 Hz/structural cadence | `kind` and readiness fields; active torrent download progress; volatile seed counters stay out of list ticks |
| Native REST, SSE, and replay (`nzbd-api`) | Routing/auth shell, event ring, `Last-Event-ID` recovery | Content-type admission, torrent detail/export, additive snapshot projection |
| Basic/Bearer auth and TLS (`nzbd-api`, `nzbd/src/tls.rs`) | Credential comparison, existing token, rustls serving policy | qBittorrent `SID` sessions and per-IP failed-login throttling; no such limiter exists today |
| Configured categories (`nzbd-config`, `nzbd-post`) | Existing labels and Usenet post-processing rules | Torrent paths/seed policy plus a restart-free runtime overlay for *arr-created categories |
| History store (`nzbd-state/src/history.rs`) | Terminal record format, cursor ordering, and cluster-wide durable tombstones | Torrent terminal records and payload-aware delete outcomes |
| Client registry (`nzbd-api`) | Consumer attribution model | qBittorrent polling attribution |
| UI and mobile shells (`nzbd-api/ui`, `mobile/`) | Existing navigation, controls, and additive JSON parsing | Protocol presentation and old-client tests for new values, not just new fields |
| Enforcing disk guard (`nzbd-engine`) | ENOSPC latch plus a multi-root forecast over the same configured storage inventory the API displays; the limiting roles, path, and free bytes are published | Route torrent-adapter storage faults and pause/upload recovery policy into the guard after the upstream gates pass |
| NZB watch task (`nzbd/src/main.rs`) | Directory scan and rename-on-result pattern | Parameterized extension/admission callback or a distinct torrent watch task |
| Cluster control plane (`nzbd-cluster`) | Leader fencing and whole-job lease model | Later exclusive torrent lease; coordinate with the already-planned `Segment` lease variant |

### 2.2 Usenet-specific structures that must stay Usenet-specific

`Job` currently contains `files: Vec<FileEntry>`, and each `FileEntry`
contains yEnc article `Segment`s. `JobTotals` reports par bytes and article
success/failure. The owner leases a segment to a configured `ServerId`, the
writer positional-writes it, and health asks whether the remaining par2 data
can repair missing articles.

None of those words describes BitTorrent correctly:

| Usenet concept | Why it must not be reused for torrents |
|---|---|
| Article segment | A torrent piece can span files, arrives in blocks from several peers, and is accepted only after a piece hash passes. |
| News server | A swarm may contain thousands of transient peers plus trackers and DHT; peers are not configured failover providers. |
| `Health` / critical health | Piece availability is dynamic. No peers now is not proof that a torrent is unrepairable. |
| par2 recovery | Piece hashes detect corruption and the swarm redownloads bad pieces; there is no delayed recovery-volume analogue. |
| `Completed` terminal status | A torrent normally uploads after reaching 100%; completion is not terminal. |
| Category move | Moving or renaming the payload breaks the paths advertised to peers unless the torrent engine is stopped and retargeted. |

The required refactor is therefore a backend seam, not an extra case in
`next_for_server()`.

### 2.3 Current compatibility has a protocol boundary

The NZBGet shim makes nzbd a Usenet download client to Sonarr/Radarr. Those
applications do not send torrent grabs to an NZBGet-configured client; they
select a torrent client integration and use that client’s API. Keeping the
NZBGet version string while accepting a magnet on `append` would be invisible
to the workflow that needs it and would make the shim lie about its contract.

The proposed qBittorrent shim is parallel to `nzbd-compat`:

```
 /jsonrpc, /xmlrpc ──▶ nzbd-compat ───────▶ Usenet jobs only

 /api/v2/*          ──▶ nzbd-qbit-compat ─▶ BitTorrent jobs only

 /api/v1/*          ──▶ native API ───────▶ both protocols
```

The native surface is the product API. Both compatibility surfaces are
projections for a named ecosystem client; neither gets to redefine the
domain model.

### 2.4 Plurx is the cluster reuse baseline, not a dependency

Cluster work must begin by reusing the merged contracts already proven in
plurx, pinned here to `7c781e5f5e28ac8114bacb1919a463a6a18e2680`.
Re-verify these anchors with `git show`, not a developer's checked-out plurx
branch. The baseline is semantic: nzbd must not take a direct `plurx-core`
dependency. Extracting a shared crate or protocol requires its own ADR and
compatibility boundary.

| Concern | Merged plurx source | nzbd reuse rule |
|---|---|---|
| Monotone lease authority | `crates/plurx-core/src/cluster/coordination.rs` and both store implementations at `d0a83eb0da10fe352da2176aa19701d95eb6c2c4` | Port the token and exact-CAS semantics behind nzbd's store boundary; do not invent a second lock protocol. |
| Fenced publication | `crates/plurx-core/src/store/publication.rs` and both transactional implementations at `61951db0945e1a9874c8a1742154bba5da5d11bf` | Validate the live fence in the same transaction that publishes durable state. A pre-check is not fencing. |
| Renewal loss and self-fencing | `crates/plurxd/src/state.rs` at `038f154a1c35f1cb0afe496eddf9bd4cb6778dc0` | Cancellation and the local expiry deadline both stop the worker; renewal returns a replacement token. |
| Membership and failover operations | `crates/plurx-core/src/cluster/membership.rs` and `crates/plurx-cluster-check` on the pinned baseline | Reuse identity, tombstone, degraded-quorum, and real-process test contracts. Do not copy the implementation: it depends on plurx's patched Hiqlite/WAL stack. |
| Architecture and validation matrix | `docs/CLUSTER-MEDIA-POOL-PLAN.md` on the pinned baseline | Use its facts-versus-bytes split, transactional fence rules, and failure matrix as the starting review checklist. |

Later merged plurx work hardens that baseline without changing nzbd's
dependency boundary. The audit was performed against the accepted main tree at
`64e96aa71f58ebb3ec2c7b71d5c36a537b50aba6`; the implementation commits below
are subordinate anchors, not a replacement baseline. They are prior art for
the M6 ADR, not an accepted nzbd topology or permission to implement M6 early:

| Concern | Accepted plurx tree and implementation anchors | Additional M6 reuse rule |
|---|---|---|
| Removal and publication fencing | failover-hardening merge `0662f3768694e0bb633c4159c592702d72d328cc`; `c764d6f54320547e57d9a4e3ab8301cf1ce24b18`, `5b8019f50b5ec30784d4ca7d1cbb9854db13f9df`; harness correction `2f77fcfad54ae5e54fa24857c17b85b0ed154a05` | If the ADR allows dynamic removal, make removal intent durable before changing membership, stop new authority, invalidate the removed owner's existing tokens, and leave ambiguous outcomes fenced; otherwise reject dynamic removal for torrent-capable clusters. Check both the live lease and member eligibility in the transaction for every owner-derived authoritative mutation. The harness correction is evidence maintenance, not a new production guarantee. |
| Topology evidence | topology-evidence merge `bec1f803c36d4cd31b7f589d67f5412cf8c7c154`; `8596336d0bf66cf432dcafc071c66e817823c031` | Make the ADR name a schema-validated, reproducible semantic workload for the chosen topology. Named-runner resource evidence is required only before making quantitative capacity claims or selecting a default because of them. |
| Passive cluster observation | passive-metrics merge `6c187ff8bb694513fb8344bd308c547dedefe775`; `3be87ae6ed4eeff908577ba522252d5cbed19feb`, `a209533368acb2fe1a7a5c0fe4b861cfadc61952` | Metrics scrapes read a passive snapshot and perform no coordination-store work; a separate bounded background sampler may refresh that snapshot. |
| Quorum-backed follower freshness | quorum-watermark merge `64e96aa71f58ebb3ec2c7b71d5c36a537b50aba6`; `9aad04c45711abc00487df0fe997d31411148b3b` | Only if the ADR admits bounded follower reads, require a term/leader/commit proof with a local monotonic deadline and applied-index budget. An invalid proof falls back to authority or refuses the read; it never proves payload validity or torrent readiness. |

Only merged plurx code in the pinned trees counts as prior art. In particular,
`origin/agent/builder/issue-417-cluster-activity-transport` is unmerged and is
not an accepted dependency or design premise. Plurx also does not yet prove
every torrent-session takeover or remote-placement behavior; those remain
nzbd acceptance work in §12.

---

## 3. Requirements and guardrails

### 3.1 Functional requirements

The first release must:

1. Accept a raw v1 `.torrent`, a magnet URI with a v1 info hash, and an
   authenticated HTTP(S) URL that resolves to a `.torrent`.
2. Resolve magnet metadata without blocking the queue owner or an API request.
3. Download all selected files, verify every completed piece, recover after a
   process crash, and never report readiness before verification completes.
4. Upload verified pieces while the job is allowed to seed.
5. Pause, resume, reprioritize, forget, and forget-with-data idempotently.
6. Enforce global download and torrent upload limits without a restart.
7. Expose total size, downloaded and uploaded bytes, rates, ETA, ratio, peer
   counts, file progress, tracker/DHT state, content path, and last error.
8. Keep `.torrent` metadata and magnet tracker passkeys out of logs, events,
   metrics labels, and ordinary API list responses.
9. Preserve add-time category and caller parameters through readiness and
   terminal history.
10. Work with current Sonarr and Radarr when configured as a qBittorrent
    download client, including category creation and post-import category
    changes.

### 3.2 Non-functional requirements

| Requirement | Gate |
|---|---|
| Existing behavior | The NZBGet golden suite and all Usenet engine tests remain byte-for-byte green. |
| Crash correctness | Kill during metadata fetch, piece write, verification, readiness commit, and seeding; restart never promotes unverified bytes. |
| Memory | Torrent feature disabled adds no live session; enabled idle target is measured and documented before merge. |
| Packaging | Linux glibc/musl, macOS, Windows, Docker, and Rust 1.85 gates still pass. |
| Backpressure | Torrent stats enter the queue owner over a bounded, coalescing channel; a chatty swarm cannot starve queue commands. |
| Security | Metainfo size/path limits, URL limits, private-torrent rules, redaction, auth, and delete scope have negative tests. |
| Observability | A stalled torrent says why it is stalled; “no peers” is not translated into “failed.” |
| Upgrade | A pre-torrent `queue.json` loads unchanged; disabling `[torrent]` leaves all existing behavior unchanged. |

### 3.3 Compatibility requirements

- The NZBGet JSON-RPC/XML-RPC response shapes do not gain torrent rows. Sonarr
  configured with both nzbd client types must not see one download twice.
- Existing `/api/v1/jobs` clients keep their current fields. Additions are
  optional fields or new endpoints; existing field meanings do not change.
- Existing `job_finished` and `job_pp_finished` events keep their order and
  meaning. Readiness is an additive field in the authoritative snapshot, not
  a second event for the same Usenet durable moment.
- Existing config files remain valid under `deny_unknown_fields`; all new
  sections have defaults and are absent from masked output when disabled only
  if the current settings serializer can round-trip that shape safely.
- A torrent’s external download id is its lowercase v1 info hash. `JobId`
  remains nzbd’s internal identity.

### 3.4 Non-goals for the first production release

- BitTorrent v2-only and hybrid swarms (BEP 52). M0 now rejects both input
  forms by name; a later release needs a new engine gate and durable v2 hash
  identity.
- uTP and IPv6 peer transport. Stable `librqbit` 8.1.1 is TCP/IPv4; revisit
  both when a stable 9.x release passes the M0 build, resume, and interop
  gates.
- Public force-recheck. Stable 8.1.1 exposes no verified public recheck API;
  restart recovery may revalidate data internally, but the UI makes no
  operator-triggered recheck promise.
- Creating `.torrent` files or acting as a tracker.
- Search, RSS torrent discovery, WebTorrent, streaming, sequential playback,
  or UPnP media-server behavior.
- A general qBittorrent replacement UI or full WebUI API implementation.
- Torrent post-processing, unpack, cleanup, deobfuscation, or extension
  scripts. See §9 for the safe future design.
- Cross-seeding, automatic tracker injection, automatic torrent repair, or
  tracker-specific rules.
- Anonymous torrenting. A SOCKS proxy is not a complete privacy boundary, and
  the first release will not label it as one.
- Multi-node torrent execution. Cluster admission is rejected clearly until
  §12 is implemented.

These guardrails keep the first release reviewable. Each omitted feature has a
place in the architecture; none needs to be smuggled into the first download
path.

---

## 4. ADR-19 — embed a torrent library behind a backend adapter

**Status:** Amended 2026-08-14; maintained rqbit v8.1.1 candidate selected,
production wiring still disabled

**Deciders:** maintainer · security reviewer · operator representative

### 4.1 Context

nzbd’s defining constraints are a Rust/Tokio core, a small operational
footprint, a single distributable daemon, safe parsing of hostile input, and a
durable queue controlled by one owner. A BitTorrent implementation needs peer
discovery, tracker protocols, DHT, PEX, peer sessions, bencode,
piece selection, piece hashing, disk layout, fast resume, uploads, and NAT
behavior. Rebuilding those protocols would be a new project, not a feature.

### 4.2 Options considered

| Option | Complexity | Runtime dependency | Protocol maturity | Fit with nzbd | Decision |
|---|---:|---:|---:|---:|---|
| Embed `librqbit` | Medium | none | Good v1 feature set; eleven bounded fixes close the reviewed M0 and deterministic-DHT-test gaps | Rust, Tokio, library API, Apache-2.0, single binary | **Choose the reproducibly derived v8.1.1 patch series, subject to complete M0 evidence** |
| Bind `libtorrent-rasterbar` | High | C++ library/FFI | Broad and mature, including extensive BEP coverage | Adds unsafe FFI, C++ packaging, and static-build complexity | Fallback if M0 fails a blocker |
| Delegate to Transmission/qBittorrent/rqbit daemon | Low initial, high product | second daemon | Mature according to chosen daemon | Split queue, auth, persistence, paths, logs, lifecycle, and support boundary | Reject |
| Implement BitTorrent in nzbd | Extreme | none | Years behind mature engines on day one | Maximum control, unacceptable protocol/security burden | Reject |

### 4.3 `librqbit` acceptance gates

As of 2026-08-05, docs.rs and crates.io list stable `librqbit` 8.1.1. Its
public API provides `Session`, `.torrent` and magnet admission,
pause/unpause/delete, file selection, session persistence, fast resume, DHT,
private torrents, TCP over IPv4, SOCKS, and live upload/download rate setters.
It does **not** provide uTP, IPv6 peer transport, BEP 52, or a verified public
force-recheck API. uTP and IPv6 appear in the pre-release 9.x line; this
proposal does not pin a release candidate for a production data path. The
project is Apache-2.0 licensed and its stable manifest offers a `rust-tls`
feature.

Those are promising inputs, not proof. M0 must build this exact dependency
shape first:

```toml
librqbit = { version = "=8.1.1", path = "contrib/rqbit/vendor/crates/librqbit", default-features = false, features = ["rust-tls"] }
```

Pin the first accepted version because this adapter becomes a correctness
boundary. Upgrade deliberately after interop tests, not because a broad
semver range moved underneath the daemon.

M0 passes only if it proves all of the following:

1. Rust 1.85, musl, macOS, and Windows builds pass with no OpenSSL dependency.
2. A local v1 `.torrent` and magnet complete through TCP/IPv4, then seed.
3. Pause, resume, delete/keep-data, delete/data, and live rate changes behave
   deterministically through the library API.
4. A killed process resumes without trusting unverified data.
5. Private torrents do not use DHT, PEX, or local discovery. Stable 8.1.1
   converts trackers to a hash set before truncating private torrents to one,
   so M0 must prove a one-tracker private torrent and the adapter must reject
   zero or multiple unique trackers rather than pretending a primary tracker
   is deterministic. A magnet's private bit is unknown until metadata arrives,
   so a DHT-enabled lookup may expose its hash before nzbd can reject private
   metadata. The release contract must state that limit rather than invent a
   pre-metadata privacy oracle.
6. File paths are either rejected safely by the library or can be validated
   before any file is created.
7. Torrent stats expose the required public facts without reading
   library-private state: transfer phase, total and verified bytes, verified
   per-file progress, completion, rates/ETA inputs, peer counts, admission
   facts, and a bounded credential-safe last error. Detailed tracker/DHT state
   is diagnostic only; an unavailable diagnostic is reported as `unknown` and
   is never inferred from peer count or converted into a health percentage.
8. Session persistence can be used as a resume accelerator without letting it
   auto-restore torrents absent from nzbd’s durable job store.
9. The binary and idle-memory growth, dependency/package-count delta, and
   transitive license delta are measured and accepted; every reviewed runtime
   resource boundary is enforced; and the exact upstream checksum, patch
   membership/order, application, generated vendor, and behavior proofs are
   protected against drift.
10. The process installs one explicit rustls 0.23 `CryptoProvider` before
    either nzbd TLS or `librqbit` constructs a client, and a test proves the
    mixed aws-lc/ring dependency graph cannot reach an ambiguous-provider
    panic.
11. Stable 8.1.1 has no versioned supported-BEP list and its v1 add path
    requires `btih`; v2-only and hybrid metainfo and magnets are rejected
    with distinct named errors unless an explicit test proves otherwise.

If gates 1–8 fail and cannot be fixed by a small maintainable engine change,
evaluate `libtorrent-rasterbar`. Do not hide a library gap behind polling,
unsafe file operations, or a second source of truth.

#### 4.3.1 Historical M0 result — stop before daemon integration

The 2026-08-05 spike is a **no-go for production wiring on stable 8.1.1**.
The detailed commands, measurements, and evidence are in
[BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md).

- The deterministic TCP/IPv4 data path, `.torrent` and magnet admission,
  seeding, pause/resume/delete, live limits, authenticated SOCKS, path
  rejection, explicit rustls provider, v1-only format boundary, one-tracker
  private mode, and public progress/rate/peer facts work.
- The private-mode evidence now includes a peer-wire positive control: a
  public torrent contacts a canary learned only through PEX, while the same
  message cannot make a private torrent contact it. A Linux packet-capture
  harness also runs live DHT behind a local redirect and requires separate
  public info-hash controls before and during a 15-second private canary
  window. It rejects the private hash across all captured UDP, including raw
  DHT and LSD-style text payloads. The
  [first successful capture run](https://github.com/pjunod/nzbd/actions/runs/31128106994)
  observed both DHT controls and no private DHT/LSD hash on 2026-08-06 UTC, so
  gate 5 passes.
- Rust 1.85 works after pinning compatible transitive versions. The first
  checked-in matrix caught an additional target-specific incompatibility:
  Tokio 1.53.0's Windows signal path uses an API stabilized after Rust 1.85.
  The workspace now pins Tokio 1.52.4, and the `BitTorrent M0` workflow makes
  Linux glibc, macOS arm64, Windows MSVC, and x86-64/aarch64 musl execute the
  isolated adapter suite under that toolchain. The
  [corrected native run](https://github.com/pjunod/nzbd/actions/runs/31060326800)
  passed on all five platforms, along with the exact-engine/Rust-TLS dependency
  policy, so gate 1 passes. Gates 4, 7, and 8 still block production wiring.
- Gate 7 fails the full observability contract: public stats provide transfer
  and peer facts, but not a durable per-torrent tracker/DHT status or last
  tracker error.
- Gate 8 fails the ownership contract: enabling JSON persistence causes
  `Session::new_with_opts` to restore every library record before it returns.
  The persistence trait/store and constructor hook are private, so nzbd cannot
  filter against its durable job store first.

The preferred next move is a small upstreamable 8.x API change: allow session
persistence/fast resume without automatic restore, or accept an authoritative
restore filter, and expose a public per-torrent discovery-health snapshot.
Then rerun all eleven gates. Vendoring the private JSON schema, deleting
library files before boot, or inferring tracker failures from “no peers” is
explicitly rejected. A full hash recheck with library persistence disabled is
safe but operationally different from the accepted fast-resume requirement;
adopting it requires an explicit ADR change. If the upstream surface cannot be
made stable and small, evaluate `libtorrent-rasterbar` as already specified.

#### 4.3.2 Historical upstream re-check and M1b amendment

The 2026-08-05 follow-up checked crates.io's current stable release and rqbit
main at `4e5f94c`. Stable remains 8.1.1. The unreleased tree identifies itself
as 9.0.0-rc.0, but it still constructs persistence and then streams every
stored torrent through `add_torrent` before `Session::new_with_opts` returns.
Its tracker statistics provider carries transfer counters into announce
requests; it is not a public tracker-health snapshot and does not retain a
redacted last announce failure. Its public DHT statistics are session-wide,
not a per-job discovery result.

That evidence keeps gates 7 and 8 failed. nzbd will not pin a release
candidate, vendor rqbit's private persistence format, or interpret an internal
tracker counter as health. The preferred route remains two small upstream
APIs followed by a stable release and a complete M0 rerun; the existing
libtorrent fallback remains the next engine evaluation if those APIs cannot
be made stable.

The first of those changes is now concrete. The tested
[`disable_auto_restore` patch](../contrib/rqbit/0001-allow-persistence-without-auto-restore.patch)
targets the exact v8.1.1 tag and preserves rqbit's default behavior. Its
contract proves that persistence can retain two records while the constructor
admits none, after which nzbd can explicitly restore only its authoritative ID
and select the matching persistence identity. Its focused test also proves
that the selected torrent reports only the persisted 16 KiB and remains
incomplete even though its complete valid 64 KiB payload is on disk; a full
recheck would report 64 KiB and complete. The reproducible verifier, scheduled
drift check, and full result are recorded in
[BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md#33-the-authoritative-restore-patch-is-ready-not-released).
This preparation does not make gate 8 pass: the API must survive upstream
review and ship in a stable release before nzbd can consume it.

The second API is now concrete as well. The tested
[`discovery_health` patch](../contrib/rqbit/0003-expose-per-torrent-discovery-health.patch)
adds a public, per-torrent snapshot with explicit DHT states, current-run DHT
counters, tracker states, next-announcement delays, and bounded last-failure
categories. Tracker paths, queries, user information, response bodies, and
credentials are excluded; only scheme, host, and port reach the public
endpoint label. Exact-stable and rqbit-main variants, focused tests, and the
scheduled drift verifier are recorded in
[BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md#34-the-discovery-health-patch-is-ready-not-released).
This preparation does not make gate 7 pass: the API must survive upstream
review and ship in a stable release before nzbd can consume it.

Because this contribution spans `librqbit`, `librqbit-dht`, and
`librqbit-tracker-comms`, submit its public contract as an upstream design
issue before asking maintainers to review the full patch. The two rqbit APIs
may move through upstream independently, but M2 does not ship on the restore
API alone: a degraded discovery view still fails gate 7 and cannot distinguish
an idle swarm from a tracker or DHT failure.

It does not follow that M1b must wait. A serializable transfer record, one
owner-controlled active set, reliable structural facts, and coalesced progress
are required whichever embedded engine wins. M1b is therefore authorized with
these hard limits:

- no torrent config, admission endpoint, daemon dependency, listener, DHT, or
  tracker task;
- fake backend only, with production recovery refusing a persisted torrent
  row rather than guessing how to run it; and
- no M2 work until ADR-19 records an engine that passes every stop gate.

Primary evidence: [crates.io stable release](https://crates.io/crates/librqbit),
[unreleased session construction](https://github.com/ikatson/rqbit/blob/4e5f94cbcf1d57ec500885c77cf1e24d70232d89/crates/librqbit/src/session.rs),
[tracker provider state](https://github.com/ikatson/rqbit/blob/4e5f94cbcf1d57ec500885c77cf1e24d70232d89/crates/tracker_comms/src/tracker_comms.rs),
and [public torrent statistics](https://docs.rs/librqbit/8.1.1/librqbit/struct.TorrentStats.html).

#### 4.3.3 Maintained-engine amendment — selected 2026-08-14

This amendment supersedes the release-only conclusions in §§4.3.1–4.3.2.
ADR-19 selects rqbit v8.1.1 plus exactly these twelve stable patches:

1. disable automatic restore while retaining explicit restore;
2. bound tracker response size, request duration, and announce cadence;
3. enforce per-torrent and shared live-peer ceilings;
4. bound pre-routing incoming handshakes;
5. enforce per-torrent and shared retained-known-peer ceilings;
6. bound peer piece/metadata response backlog;
7. bound DHT and metadata-discovery work;
8. enforce the 10 MiB peer-metadata ceiling before allocation; and
9. propagate file-sizing failures instead of logging and continuing;
10. make the session PEX toggle authoritative for inbound and outgoing PEX;
    and
11. expose a feature-gated, test-only DHT bootstrap constructor so loopback
    discovery tests never depend on public bootstrap hosts; and
12. let nzbd present one client identity (tracker `User-Agent`, BEP 10 `v`,
    with the BEP 20 peer ID it already controls) and announce the way private
    trackers account for ratio: the tracker URL's own query survives, `key`
    is stable per tracker session, `tracker id` is echoed, `downloaded`
    counts only payload fetched in the session, `completed` goes out once
    when an incomplete download finishes, and `stopped` is sent best-effort
    when the tracker session ends. Stable 8.1.1 dropped query-string
    passkeys, never sent `completed`/`stopped` over HTTP, repeated
    `started`/`completed` on every UDP announce, and reported
    already-present data as downloaded. Failed announces now back off
    from 60 s to 30 minutes.

The machine-readable order is
[`contrib/rqbit/maintained-series.txt`](../contrib/rqbit/maintained-series.txt).
The upstream input is the immutable v8.1.1 archive with the checked SHA-256 in
[`upstream-v8.1.1.sha256`](../contrib/rqbit/upstream-v8.1.1.sha256). CI rebuilds
the vendor from that archive, applies every patch in order, compares the result
byte-for-byte with `contrib/rqbit/vendor`, and runs the exact affected upstream
tests. The checked-in vendor is generated output and must never be hand-edited.

The series is about 3,500 changed lines (patch 0021 is roughly half of that,
most of it tests) and stays within the “small,
maintainable engine change” branch of this ADR. It does not add a second
runtime, change the queue owner, or adopt rqbit's private persistence format.
The heavier `libtorrent-rasterbar` FFI/packaging fallback is therefore not
triggered. Current-main variants and the optional detailed
`discovery_health` patch remain upstream contribution material; they are not
part of the production dependency.

The resume decision is nzbd-authoritative selective restore. The engine starts
with automatic restore disabled, nzbd admits only durable queue records, and
the persisted engine state can accelerate only those selected restores. A full
recheck is a safe degradation path, not the normal ownership model. The gate-4
proof kills the writer process, starts an empty session, restores one selected
record, and verifies that complete bytes placed on disk after the persisted
16 KiB checkpoint do not become trusted progress.

Gate 7 requires operational facts, not a synthetic swarm score. The adapter
publishes transfer phase, total and verified bytes, per-file verified progress,
completion, rates/ETA inputs, peer counts, admission facts, and a bounded
credential-safe error. Detailed tracker and DHT state is diagnostic only; this
candidate reports it as `unknown` when unavailable and never guesses from peer
availability. No torrent-health percentage is defined.

Gate 8 may pass on the maintained local patch; upstream acceptance is not a
condition of this ADR. Gate 9 additionally requires patch integrity, every
reviewed runtime limit, dependency/license evidence, and current native
measurements. Passing M0 authorizes decomposition and review of M2; it does not
add daemon config, admission, a listener, DHT, tracker work, or payload I/O.

### 4.4 Consequences

**What gets easier:** peer protocol correctness, DHT/tracker behavior, piece
verification, fast resume, rate limiting, and upload behavior are delegated to
a maintained engine. nzbd focuses on policy, durability, integration, and an
honest user model.

**What gets harder:** nzbd inherits an important dependency’s release cadence
and gaps. The adapter requires pinned interop tests. Stable 8.1.1 is
TCP/IPv4-only, offers sequential download as its only piece-order mode, reduces
a private torrent to one nondeterministically ordered tracker, and has no BEP
52 path. The adapter therefore restricts private metainfo to exactly one
unique tracker, and the first release must say those limits plainly.

**What must be revisited:** v2/hybrid torrents · interface binding/VPN
enforcement · cluster portability of resume state · safe copy-on-write torrent
post-processing · whether a future library version removes the pin.

---

## 5. Target architecture — protocol adapters report facts to one owner

```
                         ┌───────────────────────────────┐
 native API / UI / PWA ─▶│                               │
 NZBGet compat ─────────▶│  queue owner                  │
 qBittorrent compat ────▶│  durable jobs · policy       │
                         │  priority · events · snapshot │
                         └──────────┬─────────┬──────────┘
                                    │         │
                         commands   │         │ commands
                                    ▼         ▼
                         ┌────────────────┐ ┌────────────────┐
                         │ Usenet backend │ │ Torrent backend│
                         │ NNTP pools     │ │ librqbit       │
                         │ article writer │ │ session        │
                         │ par health     │ │ peers + pieces │
                         └───────┬────────┘ └───────┬────────┘
                                 │ facts            │ facts
                                 └────────┬─────────┘
                                          ▼
                               bounded/coalesced updates
```

The arrow back to the owner carries facts: downloaded bytes, verified bytes,
rates, phase, peers, errors, metadata resolved, and ready. It does not carry a
second queue. The owner decides whether a job is allowed to run, persists the
policy state, publishes snapshots, and orders events.

### 5.1 Crate boundaries

| Crate | Responsibility | Must not own |
|---|---|---|
| `nzbd-types` | Protocol-neutral job fields plus serializable Usenet/Torrent transfer records | Sockets, library handles, API compatibility strings |
| `nzbd-engine` | Queue authority, scheduling, combined snapshots/events, backend command routing; current Usenet implementation stays in place behind the seam | BitTorrent peer protocol or qBittorrent wire shapes |
| `nzbd-torrent` (new) | `librqbit` wrapper, handle map, source validation, stats coalescing, resume integration | Job ids, history policy, qBittorrent compatibility |
| `nzbd-qbit-compat` (new) | Minimal `/api/v2` request/response projection | Torrent session handles or independent auth |
| `nzbd-api` | Native typed API, SSE, combined status | Compatibility-specific state strings |
| `nzbd-post` | Existing Usenet PP; later, derivative-worktree PP | Mutating a live seed payload |

Do not extract a `nzbd-usenet` crate in M1b. `owner.rs`, article leasing, and
the current snapshot are too interwoven for that to be a mechanical move.
Introduce the backend seam around the implementation in place; a later crate
split needs its own review and is not required by BitTorrent. The M1b
acceptance gate is existing Usenet behavior behind a new boundary, not moved
ownership or a redesigned engine.

### 5.2 Adapter contract

The exact Rust details may change in M0, but the ownership contract must not:

```rust
pub enum BackendCommand {
    Start { job: JobId },
    Pause { job: JobId },
    Resume { job: JobId },
    Remove { job: JobId, delete_data: bool },
    SetPriority { job: JobId, priority: i32 },
    SetDownloadLimit { bytes_per_sec: Option<u64> },
}

pub enum BackendFact {
    MetadataReady { job: JobId, torrent: TorrentMetadata },
    Ready { job: JobId, content_path: PathBuf },
    Stopped { job: JobId, reason: StopReason },
    Failed { job: JobId, error: SafeError },
}

adapter.progress(job, TransferProgress { /* latest counters */ });
```

Progress is a watched latest-value map: only the newest value per job matters.
`MetadataReady`, `Ready`, `Stopped`, and `Failed` use a separate bounded
structural channel and are never replaced by progress. Commands have their own
bounded channel in the opposite direction. One FIFO for thousands of
peer-stat updates, facts, and delete commands would recreate the queue
starvation the owner design exists to prevent.

### 5.3 One session, many torrents

Run one `librqbit::Session` per nzbd process, not one session per job. A single
session owns the listen port, DHT node, rate limits, and peer id. The adapter
holds engine handles keyed only by engine identity/info hash and never owns an
nzbd `JobId`. A daemon/backend runtime module holds the
`JobId ↔ engine identity` association and translates protocol-neutral M1b
commands/facts. Raw library handles do not cross that boundary; only `JobId`
and the serializable torrent record reach the queue owner.

The intended contract is for library persistence to be an optimization beneath
that map. On boot, nzbd must enumerate its durable torrent jobs and explicitly
restore only those jobs. Stable 8.1.1 cannot implement that contract: session
construction auto-restores every library record before returning. Production
wiring therefore remains blocked by §4.3.1. Two lists that merge by hope are
two queue authorities.

---

## 6. Domain and persistence contract

### 6.1 Keep existing serialized fields and add a tagged transfer record

Do not rewrite `queue.json` into a new schema in the same milestone that adds a
new engine. Extend `JobKind` with `Torrent`, leave `Nzb` and `Url` spellings
unchanged, and add a defaulted protocol record:

```rust
pub enum JobKind {
    Nzb,
    Url,
    Torrent,
}

pub struct Job {
    // Existing named fields keep their serialized names and meanings.
    // Field order is not a serde compatibility contract.
    // ... current fields from nzbd-types/src/lib.rs ...

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent: Option<TorrentRecord>,
}
```

This makes old snapshots load with `torrent = None` and leaves compat code’s
existing field reads intact. A later schema cleanup may introduce a fully
tagged `TransferRecord`; it is not required to ship the feature safely.

### 6.2 Version 2 is the envelope; M1b uses 3 and M2 admission uses 4

An additive `Job` field is backward-readable, but `JobKind::Torrent` is not.
Today one unknown enum value makes the whole `queue.json` fail to deserialize
and aborts daemon startup, including every Usenet job. M1a has added this
version-2 envelope without adding a torrent representation. M1b now writes
version 3:

```rust
pub const QUEUE_SCHEMA_VERSION: u32 = 3;

pub struct QueueSnapshotDoc {
    #[serde(default = "legacy_queue_schema_version")]
    pub schema_version: u32, // missing = v1
    // ... existing fields unchanged ...
}
```

Version 2 is spent: it means “the legacy Usenet job shape inside an explicit
envelope.” Version 3 adds `JobKind::Torrent` and the defaulted `torrent`
record. A version-2 document loads with the same Usenet meaning and is rewritten
as version 3 on the next owner snapshot. Reusing version 2 would have let the
old binary treat the new enum as ordinary corruption instead of a version
mismatch.

`SnapshotStore::load` parses a healthy typed document once, then validates its
version. If the typed parse fails, it decodes a tiny header that ignores the
remaining JSON values so a future enum still produces a named
`StateError::Corrupt`. Version 1 loads through the existing defaults. A version
greater than the running binary supports reports, for example when a
schema-2 binary sees the M1b document:

```text
queue.json schema version 3 is newer than this nzbd supports (2);
upgrade nzbd or restore a compatible queue snapshot
```

This makes a future *version* fail by name; it cannot retrofit an older binary.
A pre-torrent nzbd still fails its entire startup when it sees a live torrent
row. The drain/export procedure in §17.2 is therefore mandatory before a
downgrade, not optional release-note advice.

M2 cannot add its top-level pending-admission collection while continuing to
write version 3: the existing schema-3 reader accepts unknown fields and could
rewrite the queue without those intents. Issue #154 therefore advances the
writer to version 4, treats version 3 as an empty-pending migration input, and
retains unconditional version emission. A captured exact schema-3 reader is a
required negative fixture: it must reject a version-4 document by version
before it can serve or save.

### 6.3 Proposed durable torrent record

```rust
pub struct TorrentRecord {
    pub info_hash_v1: String,          // 40 lowercase hex characters
    pub source: TorrentSource,
    pub metadata_file: PathBuf,        // relative to torrent state root
    pub phase: TorrentPhase,
    pub files: Vec<TorrentFileRecord>,
    pub total_bytes: u64,
    pub selected_bytes: u64,
    pub downloaded_bytes: u64,         // advisory checkpoint
    pub uploaded_bytes: u64,           // cumulative across restarts
    pub seeding_seconds: u64,           // cumulative across restarts
    pub ready_at_unix: Option<i64>,
    pub content_path: Option<PathBuf>,  // resolved, canonical payload path
    pub seed_policy: SeedPolicy,
    pub last_activity_unix: Option<i64>,
    pub last_error: Option<String>,     // already redacted and bounded
}

pub enum TorrentSource {
    Metainfo,
    Magnet,
    Url,                               // original URL is stored as a secret
}

pub enum TorrentPhase {
    FetchingSource,
    FetchingMetadata,
    Queued,
    Checking,
    Downloading,
    Seeding,
    PausedDownload,
    PausedSeed,
    MissingFiles,
    Failed,
}

pub struct SeedPolicy {
    pub ratio_limit: Option<f64>,       // None = unlimited
    pub time_limit_secs: Option<u64>,   // None = unlimited
}
```

Magnet and HTTP(S) sources also need a durable pre-descriptor state because a
crash may occur before validated metainfo exists. M2 therefore bumps the queue
to schema 4 and adds a defaulted queue-owned pending-admission collection whose
record contains only the reserved `JobId`, non-secret source class, phase, and
an opaque relative secret reference. Schema 3 migrates forward with an empty
collection; every later save emits schema 4 even when the collection is empty.
The exact pre-M2 schema-3 reader must reject schema 4 by future-version name
before serving or writing, so it cannot silently drop an unknown field. The
original magnet or URL is atomically written and fsynced at
`queue/torrents/pending/<job-id>.source` with mode 0600 or an equivalent
platform-secret ACL. On Unix the post-rename directory flush commits the
rename entry; Windows exposes no supported directory fsync, so there only the
sidecar file itself is flushed (before the rename). It never enters
`queue.json`, history, logs, events, errors, debug output, or API snapshots.

Admission recovers as an ordered state machine: protected source artifact →
durable pending intent → bounded fetch/list-only metadata resolution → durable
validated `sources/<infohash>.torrent` → structural replacement with the full
`TorrentRecord` preserving `JobId` → pending-secret removal → managed payload
start. Restart reconciles every edge idempotently, including an artifact left
before its queue intent. The secret is removed only after descriptor plus queue
transition are durable or the pending admission has a confirmed terminal
outcome.

Re-verify field types against `librqbit` in M0. The contract is what each
field means:

- `downloaded_bytes` never exceeds selected content bytes and never counts
  duplicate/corrupt wire bytes as completed data;
- `uploaded_bytes` and `seeding_seconds` are monotone across restarts;
- readiness is durable and means every selected byte in `content_path` passed
  the torrent’s piece hashes;
- a seed limit pauses the torrent; it never deletes data automatically;
- `last_error` is display-safe, bounded to 2 KiB, and contains no URL query,
  tracker passkey, peer address list, or filesystem data outside the job root.

### 6.4 High-frequency state is not queue truth

Piece bitfields and peer lists belong to the torrent engine’s resume state.
Writing them through `queue.json` every second would enlarge snapshots and
turn peer churn into state-volume I/O. nzbd persists only control state,
metadata, stable file layout, readiness, and cumulative accounting.

Checkpoint upload bytes and seed time at most every 30 seconds or every 8 MiB
of additional upload, whichever comes first. On an unclean shutdown, lost
accounting makes the torrent seed slightly longer, never shorter. That is the
safe direction for tracker obligations.

### 6.5 Identity and duplicate admission

The v1 info hash is the transfer identity. Admission follows this sequence:

```
 source syntax valid? ── no ──▶ reject without creating an intent
       │ yes
 persist secret + pending intent ──▶ fetch/resolve bounded metainfo
       │
 validate and durably persist v1 descriptor
       │
 hash already live? ── yes ──▶ return existing JobId, apply safe new params
       │ no
 hash in terminal history? ── yes ──▶ follow dupe mode / explicit force
       │ no
 persist descriptor + queue job ────▶ start only after durable commit
```

A magnet’s `btih` gives the identity before metadata arrives; an HTTP source
may not reveal it until bounded fetch and parse. A `.torrent` gives it after
bounded bencode parsing. Pending-intent reconciliation and final hash
deduplication must not create two sessions writing the same payload. The
existing dupe key/score may still be carried for *arr semantics; it does not
replace the info-hash invariant.

### 6.6 Crash recovery order

1. Load the nzbd queue snapshot, structural journal, and protected pending
   source inventory; safely reconcile queue-linked and orphan artifacts.
2. Resume each pending source idempotently without disclosing it or starting
   payload work before its durable descriptor transition.
3. Validate every torrent descriptor and ensure its paths remain under the
   configured torrent root.
4. Start the library session paused.
5. Explicitly restore each durable torrent job, using fast resume only when
   the library validates it against the stored metainfo and paths. Restore
   consults the durable control intent before the transient queue status: a
   persisted pause remains paused and a persisted resume is eligible to run
   without discarding verified progress. Any durable removal intent excludes
   the job from restore before engine recovery, regardless of whether its
   requested disposition keeps or deletes payload data. Ready and seeding jobs
   remain live restores even without an active-download slot; only an explicit
   removal or terminal failure retires them.
6. Recheck any job whose ready stamp, file state, and resume state disagree.
7. Publish the first combined snapshot.
8. Resume only jobs allowed by queue policy.

No API response may show `Seeding` merely because an old snapshot said so.
The process must first re-establish that the payload is present and trusted.

---

## 7. API and ecosystem contracts

### 7.1 Native admission — content type makes the protocol explicit

Keep the current raw-NZB request behavior. Extend `POST /api/v1/jobs` by
content type:

| Content type | Body | Result |
|---|---|---|
| `application/x-nzb` or legacy/unspecified | raw NZB XML | Existing Usenet admission |
| `application/x-bittorrent` | raw bencoded `.torrent` | Torrent admission |
| `application/json` | typed `source` object | Magnet or URL admission |

Proposed typed request:

```json
{
  "source": {
    "type": "magnet",
    "uri": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
  },
  "category": "tv",
  "priority": 0,
  "paused": false,
  "params": {
    "monarr-transfer": "t-42-a3f9c1"
  },
  "seed": {
    "ratio": 1.5,
    "minutes": 4320
  }
}
```

`source.type` is `magnet` or `torrent_url`; raw bytes use the torrent content
type. Do not overload the existing `?url=` query: it currently means “fetch an
NZB,” and guessing the fetched document’s protocol after creating a URL job
would make error and compatibility behavior depend on remote content.

Success remains `201 {"id": <JobId>}` and may add `info_hash`. A duplicate
live hash returns `200` with the existing id and `created: false`. Malformed or
unsupported metainfo returns `422`; a magnet metadata deadline returns `504`
and cancels an explicit request's still-pending reservation. Recovery keeps
the same transient timeout intent durable for a later retry. Other source
fetch failures become a visible failed job only after a valid request was
durably admitted.

For magnet/URL input, “durably admitted” means the protected source artifact
and queue-owned pending intent above both exist. The source can then resume
after a kill during metadata resolution or HTTP fetch without putting its
query, tracker passkey, or credentials in the public queue snapshot. The
production route and watch task remain unmounted until final M2 activation.
The dormant API module and its engine dependency are likewise feature-gated so
the default daemon graph does not link the peer engine, DHT, or UPnP before
#163 activates the production entry points. The configured `make test` quality
command and CI test/lint workflows compile and exercise `nzbd-api` with the
`torrent-admission` feature explicitly while the production graph stays clean.

### 7.2 Native read model

Existing job fields stay present. Torrent jobs add:

```json
{
  "id": 42,
  "kind": "torrent",
  "name": "Example.Release",
  "status": "completed",
  "ready": true,
  "ready_at_unix": 1785945600,
  "size_bytes": 12884901888,
  "downloaded_bytes": 12884901888,
  "remaining_bytes": 0,
  "rate_bps": 0,
  "torrent": {
    "info_hash_v1": "0123456789abcdef0123456789abcdef01234567",
    "phase": "seeding",
    "content_path": "/data/torrents/tv/Example.Release",
    "seed_ratio_limit": 1.5,
    "seed_time_limit_secs": 259200
  }
}
```

`kind`, `ready`, and `ready_at_unix` are new `JobSummary` fields. Keep the
top-level `status` inside the existing `JobStatus` vocabulary so an old mobile
build does not receive an enum value its switches cannot handle: metadata and
checking project as `fetching`, an active payload as `downloading`, and a
ready seed as `completed`. `torrent.phase` carries the precise protocol state.

The list projection intentionally omits volatile seed counters. During an
active download, downloaded/remaining bytes still change on the existing
tick. Once ready, peer count, upload bytes/rate, ratio, tracker timing, and
seeder/leecher estimates live in `GET /api/v1/jobs/{id}/torrent`. This lets an
idle seeding daemon resume suppressing byte-identical ticks instead of sending
every SSE client a full queue once per second forever. The UI polls torrent
detail only while that job's panel is visible.

`GET /api/v1/jobs/{id}/files` returns one neutral file shape for both
protocols. Usenet keeps segment counts; torrent files add `selected`,
`downloaded_bytes`, and `progress`. Protocol-inapplicable fields are absent,
not zeroed into a misleading story.

Add `GET /api/v1/jobs/{id}/torrent` for tracker-safe, live detail and
`GET /api/v1/jobs/{id}/torrent-file` to export retained metainfo. The latter
requires normal auth, returns no magnet URI, and sets `Cache-Control: no-store`.

### 7.3 Readiness is authoritative state, not a duplicate event

Add `ready: bool` and `ready_at_unix: Option<i64>` to `JobSummary`. For
Usenet, derive readiness from the durable `*PP:done` stamp at the same point
the existing `job_pp_finished` event fires. That event keeps its
`history_seq` cursor and remains the integration handoff for current Usenet
consumers; emitting a second, less informative event at the same moment would
create vocabulary without information.

For BitTorrent, persist `TorrentRecord.ready_at_unix` only after every
selected byte passes piece verification, then structurally publish a snapshot
with `ready=true`. The torrent remains in `/jobs` while seeding. Native
consumers reconcile from the snapshot, while Sonarr/Radarr poll the
qBittorrent projection as they do with other torrent clients.

Do not emit `job_pp_finished` for a torrent that did no post-processing. If
field evidence later proves that a named torrent-ready event is necessary,
design it with the same replay/cursor guarantees as the existing event rather
than adding it speculatively.

### 7.4 qBittorrent compatibility — implement the *arr surface, no more

Current Sonarr’s qBittorrent client uses the endpoints below. Radarr shares
the same lineage but must be tested separately. Advertise Web API `2.8.1` in
the first release: it is new enough for `content_path` and add-time share
limits, while keeping the older `paused` add field current Sonarr chooses for
API versions below 2.11.

| Method | Path | Required behavior |
|---|---|---|
| `POST` | `/api/v2/auth/login` | Validate existing Basic credentials, or return `Ok.` for an already-valid Bearer request; issue `SID` only for form login |
| `GET` | `/api/v2/app/webapiVersion` | Plain `2.8.1` |
| `GET` | `/api/v2/app/version` | A parseable compatibility version documented as nzbd-backed |
| `GET` | `/api/v2/app/preferences` | Save path, DHT, queueing, and global seed-limit fields |
| `GET` | `/api/v2/torrents/info` | Torrent jobs only; category/hash filters used by *arr |
| `GET` | `/api/v2/torrents/properties` | Save path and seeding time for one hash |
| `GET` | `/api/v2/torrents/files` | Relative file names for import fallback |
| `POST` | `/api/v2/torrents/add` | Multipart `.torrent` or `urls` magnet/HTTP URL; category, paused, content layout, seed limits |
| `POST` | `/api/v2/torrents/delete` | Forget, with optional `deleteFiles=true` |
| `POST` | `/api/v2/torrents/setCategory` | Change the label only; never move a live payload implicitly |
| `GET` | `/api/v2/torrents/categories` | Project `[[category]]` names and torrent save paths |
| `POST` | `/api/v2/torrents/createCategory` | Add an in-memory compatibility category or reject per §7.6 |
| `POST` | `/api/v2/torrents/setShareLimits` | Update per-job ratio/time policy |
| `POST` | `/api/v2/torrents/topPrio` | Move job to the top of its priority band |
| `POST` | `/api/v2/torrents/setForceStart` | Map to force priority without bypassing the disk guard |

Do not implement qBittorrent search, RSS, plugins, log, transfer-global
settings, tracker editing, torrent creation, alternate Web UI, or shutdown.
An unsupported endpoint returns `404` with a log at debug, not a fabricated
success.

Sonarr calls `auth/login` even when it is configured with an API key, using
empty form credentials plus the Bearer header. Its proxy selector falls back
to legacy qBittorrent V1 routes if V2 probing fails. Contract tests therefore
pin successful login and `webapiVersion` probing; nzbd does not implement a
legacy V1 surface and must never fail those probes accidentally.

### 7.5 qBittorrent state projection

| nzbd torrent phase | qBittorrent `state` | How *arr reads it |
|---|---|---|
| `FetchingMetadata` | `metaDL` | Queued; metadata is not content |
| `Queued` | `queuedDL` | Waiting for a download slot |
| `Checking` incomplete | `checkingDL` | Queued while hashes are checked |
| `Downloading` with peers | `downloading` | Active download |
| `Downloading` without useful peers | `stalledDL` | Warning, not terminal failure |
| `PausedDownload` | `pausedDL` | Paused incomplete download |
| `Seeding` with upload traffic | `uploading` | Completed and importable |
| `Seeding` idle | `stalledUP` | Completed and importable |
| `PausedSeed` | `pausedUP` | Completed; may be removable after seed limits |
| `MissingFiles` | `missingFiles` | Warning; path/data needs operator action |
| `Failed` | `error` | Warning/failure handling, with detail in nzbd |

The `/torrents/info` fields current *arr code consumes are `hash` · `name` ·
`size` · `progress` · `eta` · `state` · `category` · `save_path` ·
`content_path` · `ratio` · `ratio_limit` · `seeding_time_limit` ·
`last_activity`. Return all of them with their qBittorrent units and sentinel
values; missing fields often deserialize to a plausible zero, which is a more
dangerous failure than an explicit error. Do not return
`inactive_seeding_time_limit`: qBittorrent introduced it in Web API 2.9.2,
later than the advertised 2.8.1 contract.

`content_path` must name the file for a single-file torrent or the root folder
for a multi-file torrent. `save_path` names its containing torrent root. They
must not be equal for a completed job: current Sonarr treats equality as a path
error and will not import it.

### 7.6 Categories and the settings file

Sonarr’s connection test creates a missing category through
`createCategory`. nzbd’s durable configuration currently changes only through
the settings API, with validation and mirrored persistence, and any category
change is restart-required. A Sonarr connection test cannot restart the daemon
between `createCategory` and the immediate category re-fetch, and the
compatibility route must not silently rewrite `nzbd.toml` behind that test.

Use a durable runtime category overlay under the state directory:

- configured `[[category]]` entries win and expose their torrent path;
- compatibility-created categories persist in `torrent-categories.json` and
  default to `<torrent_dir>/<sanitized category>`;
- the Settings UI lists overlay categories and offers “promote to config”;
- deleting an overlay category never moves or deletes existing torrents;
- invalid, empty, traversal, or case-colliding names return qBittorrent’s
  documented conflict response.

This preserves a successful *arr test without giving a compatibility shim
permission to edit the operator’s configuration file.

### 7.7 Authentication

Support both routes current *arr clients can use:

1. Put `[api].token` in the qBittorrent **API Key** field. The existing Bearer
   token authenticates every `/api/v2` call; no cookie is involved.
2. Use `[api].username` and `[api].password`. Implement
   `POST /api/v2/auth/login` and return a short-lived, HttpOnly, SameSite=Lax
   `SID` cookie after validating those same credentials.

There is no qBittorrent-specific password. nzbd has no failed-login limiter
today, so this shim must add a bounded per-IP limiter rather than claiming to
reuse one: extract the remote socket address, rate-limit failed form logins,
cap and expire the `SID` store, and never log form fields. `/api/v2` is not
exempt merely because a real qBittorrent installation can trust localhost.

---

## 8. Scheduling, bandwidth, quotas, and disk guards

### 8.1 One active-download limit across protocols

`[queue].max_active_downloads` becomes the number of active *download jobs*
across Usenet and BitTorrent. Seeding jobs do not consume a download slot;
they consume upload and peer resources controlled by `[torrent]`.

Metadata fetching and payload downloading consume a slot only while the owner
has granted backend run permission. If neither metadata nor verified payload
progresses for 60 seconds and the backend reports no useful peer, the job
projects as `stalledDL`, the owner revokes run permission, and the job yields
its slot. A yielded torrent may keep discovery-only activity only if the
engine can prove that payload transfer remains gated; otherwise nzbd pauses it
and probes again only when a slot would otherwise be idle. A newly useful peer
or an explicit retry returns the job to its priority band, but it must reacquire
a slot before requesting metadata or pieces.

This policy means one dead magnet cannot hold the default single slot forever
and starve Usenet. It also depends on the tracker/peer facts missing from the
8.1.1 public API: until gate 7 is resolved, M1b cannot claim to implement slot
yielding honestly. Its fake-backend acceptance test must prove a stalled
torrent yields, a following Usenet job runs, and the torrent later reacquires
capacity without transferring while unselected.

The owner selects the highest-priority schedulable jobs regardless of
protocol, then grants/revokes backend run permission. Within the same priority,
the existing queue order remains the tiebreaker. Force priority may bypass a
soft pause/quota as it does now; it never bypasses low-disk, path validation,
or private-torrent network rules.

The default remains `1`. Raising it in the feature commit would change Usenet
concurrency even for operators who leave BitTorrent disabled. When an
operator enables torrents, the settings validation and UI state plainly that
the one slot is shared and offer the existing queue setting as the single
knob. There is no contradictory torrent-specific download-slot setting.

The current cap is a scheduling bound, not a byte-in-flight invariant: a
Usenet job whose remaining articles are already leased can overlap briefly
with another job granted new work. M1b preserves that behavior and documents
the same eventual bound across protocols rather than promising a hard network
concurrency guarantee the current engine does not enforce.

### 8.2 The download limit is aggregate

Keep `[queue].speed_limit_kib` as the operator’s one download ceiling. The
budget coordinator divides that ceiling among active backends and reapportions
it once per second from demand:

```
 no global limit ──▶ both backends unlimited

 one backend active ──▶ it receives the full limit

 both active ──▶ proportional share with a 1 MiB/s floor when available
                 unused share returns on the next 1 s allocation tick
```

The current NNTP token bucket already changes live. `librqbit::limits::Limits`
exposes live `set_download_bps` and `set_upload_bps`. The combined rate may
overshoot by one allocation interval; document and test the maximum. If M0
finds the library setter is not process-global or not prompt, make separate
limits explicit instead of advertising an aggregate control that is not one.

### 8.3 Upload has its own limit

Add `[torrent].upload_limit_kib`, default `0` (unlimited). Download and upload
must not share one token bucket: limiting downloads to preserve interactive
bandwidth while honoring a seed obligation is normal. The UI shows both rates
and controls them separately.

### 8.4 Existing quotas count all downloaded payload bytes

Daily/monthly quotas are named download quotas, not Usenet-provider quotas.
After this feature, they count successful incoming torrent payload bytes as
well as decoded payload bytes from successfully written NNTP segments. Failed
articles and NNTP protocol overhead do not count toward the quota; wire bytes
remain the separate speed-display measurement. Per-news-server volume counters
remain available; add a separate BitTorrent source row so their sum explains
the global quota.

Do not count uploaded bytes against a download quota. Expose torrent upload
totals separately. This is a documented behavior expansion for operators who
enable BitTorrent; Usenet-only totals are unchanged.

### 8.5 The disk guard covers every configured write volume

The enforcing task in `nzbd-engine` and the API storage panel derive their
inventory from the same config method. Node-local state, shared cluster state
when enabled, downloads, working, failed, intermediate, temporary, watch, and
category roots are expanded. Exact path duplicates retain all of their role
labels while sharing one probe. On platforms that expose stable filesystem
identity, distinct paths are grouped by containing filesystem so several roles
on one mount remain one failure domain. Windows conservatively keeps distinct
path rows; the minimum is still enforced across every row.

Each configured root is measured independently on a detached OS thread with a
two-second response deadline after admission. A process-wide 64-call permit pool remains held
until each filesystem syscall actually returns, including across hot reloads,
so path churn cannot accumulate unbounded wedged work. The first cycle starts
immediately but is not awaited by daemon startup; later cycles begin ten seconds
after the prior bounded cycle finishes. Permit loss to an older wedged config
can serialize the current roots, so a degraded recovery cycle may exceed two
seconds even though each admitted filesystem call keeps its own response bound. A wedged mount
therefore cannot block the owner/API or suppress current readings from other
volumes while probe capacity remains. A timeout or
permission error retains that root's last-known reading, marks the cycle
incomplete, and cannot clear an existing threshold hold or observed-write
latch. When a nonzero floor is configured, startup holds fail-safe until the
first complete asynchronous reading; a zero floor keeps observability enabled
without holding intake.

The lowest reading gates local engine leasing and publishes its joined roles,
common configured path, free bytes, and active write-latch cause through the
engine snapshot and native status API. Cluster workers publish the hold and
request zero free download and post-processing slots while it is active; the
leader independently excludes those workers from new placement and releases an
unleased delegation if its target becomes held. `/api/v1/cluster` publishes the same limiting facts
for each node, while the ordinary status response describes the node serving
that engine snapshot.

[`REGRAB_LOOP_PLAN.md`](archive/REGRAB_LOOP_PLAN.md) F1–F3 landed before this expansion.
Observed ENOSPC/EDQUOT from existing writers and post-processing still latches
the same immediate guard even when a mounted filesystem's forecast lies. The
dormant torrent adapter does not yet report its `StorageFull` signal into that
daemon path; the guard is ready to receive it, not evidence that production
torrent I/O is enabled.

A full disk pauses new piece requests and metadata writes but keeps the API
and existing uploads alive where the library can serve already-present data.
If the library cannot separate download pause from upload, M0 records that
limitation and the safe fallback pauses the whole torrent.

---

## 9. Storage and post-processing — seed data is immutable

### 9.1 Proposed layout

```text
<main_dir>/
  queue/                              existing queue + history state
    torrents/
      sources/<infohash>.torrent      retained metainfo, mode 0600
      pending/<job-id>.source         transient secret source, mode 0600
      resume/                          library fast-resume data
      torrent-categories.json         compat-created category overlay

<torrent_dir>/
  <category>/
    <torrent content root>             exact paths the swarm expects
```

`torrent_dir` defaults to `<main_dir>/torrents`, not `dest_dir`. Usenet’s
`dest_dir` is a post-processing handoff tree where cleanup and moves are
expected. A torrent directory is a live protocol store where paths and bytes
must remain stable while seeding.

The metainfo filename uses the info hash, never a user-controlled torrent
name. File mode is 0600 where supported because tracker URLs commonly contain
account passkeys. Directory mode follows the state directory’s existing
policy.

### 9.2 Path rules

Before starting a torrent, validate every metadata path:

- relative only; no absolute path, drive prefix, UNC prefix, `..`, empty
  component, NUL, or platform separator ambiguity;
- an explicit payload name is required; do not accept an engine-generated
  fallback shared by unrelated unnamed torrents;
- resolved output remains under the canonical category torrent root;
- no existing symlink may redirect a write outside that root, every existing
  intermediate payload component is a directory, and every existing leaf is
  a regular file;
- exact, Unicode-NFC-equivalent, simple-case-fold-equivalent, and
  file-versus-directory-prefix collisions are rejected before storage on every
  platform, conservatively covering common normalization-insensitive and
  case-insensitive filesystems;
- total file count, total path bytes, and metainfo bytes stay under fixed
  limits from §10.3;
- padding files are not exposed as user content or importer paths.

The library may perform its own checks. nzbd repeats the invariant at the
boundary because delete-with-data and reported `content_path` trust it later.
The dormant M0 adapter implements the metadata-only portion itself before
rqbit admission: portable root/components, UTF-8, empty/dot/parent names,
cross-platform separators, Windows drive/device names, reserved characters and
trailing dot/space aliases, metainfo-declared symlinks, exact duplicate paths,
file-versus-directory-prefix overlaps, and collisions under Unicode simple
case folding plus NFC. The collision key also includes Windows' dotless-i
alias, while deliberately retaining compatibility-only ligature and width
distinctions. It also checks v1 piece
geometry before rqbit can construct its length table: piece length is nonzero,
aggregate file length cannot overflow and is nonzero, the hash table is made of
whole SHA-1 values, its count exactly covers the declared payload, and the
derived 16 KiB chunk count fits rqbit's `u32` absolute-index representation. It
projects a separate importer-safe content inventory from validated rqbit
metadata and omits every BEP 47 padding entry while preserving engine file
indices for low-level diagnostics. A parsed padding-metainfo test proves the
padding path never appears in that content inventory. It
applies that same contract to magnets by resolving them through rqbit's
list-only mode, which returns metadata before storage construction, and then
admitting only the validated returned bytes. Before rqbit parses a magnet, the
adapter requires the exact parameter and namespace spelling supported by the
pinned engine, rejects unknown or malformed exact topics, normalizes lowercase
base32 `btih` values to the uppercase spelling rqbit accepts, and rejects
`so=`. Indexed and case-varied keys remain unsupported because pinned rqbit
ignores them; tests pin that engine behavior so they cannot silently become a
selection or tracker bypass after an upgrade.
Stable 8.1.1 eagerly expands every `so=` range into a vector, so even a short
URI can otherwise trigger an attacker-sized allocation; selective file
admission must instead use a bounded, explicit nzbd-owned input. A fake BEP 9
peer proves an unsafe resolved path leaves the destination empty. This closes
write ordering but not
the stable engine's internal metadata allocation: rqbit may allocate up to
32 MiB before nzbd can enforce its 10 MiB returned-metainfo limit, so production
magnet wiring still needs a reviewed pre-allocation answer. The adapter now
canonicalizes the session output root and rejects every payload prefix that is
an existing symlink before rqbit constructs storage; a Unix test proves the
external target stays empty. The
[first native filesystem-probe run](https://github.com/pjunod/nzbd/actions/runs/31240896567)
passed on 2026-08-08 UTC for pairs the adapter already rejects: the hosted
Linux volume distinguished both the ASCII-case and Unicode NFC/NFD pairs,
macOS aliased both pairs, and Windows aliased case but distinguished NFC/NFD.
Review-directed probing added compatibility-ligature, compatibility-width, and
full-case-fold pairs that the adapter then admitted. Default macOS storage
aliased the ligature and full-fold pairs, so the portable collision key now
rejects them with Unicode full case folding; the width pair remains admitted
and must stay distinct in every native job. The
[review-correction native run](https://github.com/pjunod/nzbd/actions/runs/31264422471)
passed on 2026-08-08 UTC: Linux distinguished every pair; macOS aliased case,
NFC/NFD, ligature, and full-fold pairs but distinguished width; and Windows
aliased case but distinguished NFC/NFD, ligature, full-fold, and width pairs.
These observations cover hosted temporary volumes, not every operator payload
mount. The preflight remains defense in depth and does not close the check/write
race. Descriptor-relative containment, persisted delete-root authority, and
empirical filesystem-specific behavior across supported operator payload mounts
remain M5/M2 work; no production path is wired.

The Windows name restrictions are deliberately unconditional. `?`, `:`, and
device basenames such as `aux.c` are legal on Linux, but accepting them would
make the same persisted job impossible to restore on a supported Windows
node. The first release has no sanitize-and-rename mode because changing a
torrent's payload mapping without an engine-owned storage abstraction can
break piece verification and seeding. If field data justifies a native-only
mode, it needs an explicit storage-policy design and separate compatibility
contract; it must not appear as a hidden platform default.

Before M2 wires production admission, ADR-19 must explicitly decide whether
an operator-visible native-paths opt-in exists. Until that decision is recorded
with its restore and migration contract, Linux-only names such as `aux.c` and
`Movie: The Sequel (2020).mkv` continue to fail closed on every platform; a
runtime platform check or undocumented override is not an acceptable shortcut.

### 9.3 First release: no torrent post-processing

On piece completion, the torrent engine’s hash verification *is* the transfer
verification. Mark the job ready and report the seed content path directly.
Do not run par rename, archive rename, unpack, cleanup, deobfuscation, category
move, or NZBGet extension scripts.

This matches the normal *arr torrent workflow: the media manager imports by
copy or hardlink while the download client keeps seeding the original. It also
avoids doubling every payload without the operator asking.

### 9.4 Future safe post-processing: derive, never mutate

If torrent PP is approved later, materialize a separate worktree under
`dest_dir`:

```
 immutable seed tree
        │
        ├── reflink clone when the filesystem proves CoW support
        └── full copy otherwise
                │
                ▼
       existing PP pipeline mutates the derivative
                │
                ▼
       ready=true, final_dir=derivative
```

Do not default to hardlinks. Rename/delete of one hardlink is safe, but an
extension script that modifies a file in place changes the shared inode and
corrupts the seed. A reflink is copy-on-write; a copy is expensive but honest.
The UI must estimate required space before starting derivative PP and the disk
guard must include both roots.

---

## 10. Configuration contract

Add one section and two path/watch keys. Defaults keep the feature disabled,
so existing installs do not open a peer port or start DHT after an upgrade.

```toml
[paths]
main_dir = "/data"
dest_dir = "/data/complete"             # existing Usenet/PP handoff
torrent_dir = "/data/torrents"          # immutable seed payloads
# torrent_watch_dir = "/data/watch-torrent"

[torrent]
enabled = false
listen_port = 6881                       # one TCP/IPv4 peer port in v1
dht = false                              # public discovery; opt in after reviewing hash exposure
pex = true
local_discovery = false
upnp_port_forwarding = false
# socks_proxy_url = "socks5://127.0.0.1:1080"
# socks_proxy_username = "proxy-user"
# socks_proxy_password = "proxy-secret" # masked by Settings API
max_peers_per_torrent = 80
max_peers_total = 400
max_known_peers_per_torrent = 1024
max_known_peers_total = 4096
upload_limit_kib = 0                     # 0 = unlimited
default_seed_ratio = 0                   # 0 = unlimited
default_seed_minutes = 0                 # 0 = unlimited
metainfo_max_mib = 10
source_redirects = 5
```

### 10.1 Why each default is where it is

- `enabled = false`: upgrades must not begin peer-to-peer traffic.
- `listen_port = 6881`: conventional and easy to map; only one session owns
  it. The adapter admits exactly one explicit non-zero port, rather than
  exposing rqbit's raw range probe or allowing port `0`; failure to bind is a
  startup error when the feature is enabled.
- `dht = false`, `pex = true`: DHT remains opt-in because it publicly queries
  hashes. When enabled it resolves trackerless magnets before their private
  bit is known; public validated metadata proceeds and private metadata is
  rejected before managed admission. That rejection cannot retract the
  earlier lookup. With DHT disabled, a magnet needs a supported tracker or an
  explicitly supplied peer source. The engine suppresses DHT and PEX after
  private metainfo is known.
  When `socks_proxy_url` is set, settings validation requires `dht = false`
  because librqbit 8.1.1 does not proxy DHT UDP traffic.
- `local_discovery = false`: LAN multicast reveals torrent participation and
  is unnecessary for the normal server deployment.
- `upnp_port_forwarding = false`: router mutation requires explicit consent.
- unlimited seed defaults: the operator or indexer’s seed policy is more
  authoritative than a project guess. The UI warns on an unlimited active
  seed; it does not silently invent a ratio.
- fixed metainfo and redirect limits: torrent sources are hostile input and
  an authenticated caller still should not make the daemon allocate without
  bound. The maintained engine carries the adapter's 10 MiB
  `max_metadata_size` through peer options and rejects the declared BEP 9 size
  before allocating a peer buffer or sending metadata requests.
- the `max_peers_per_torrent = 80` and `max_peers_total = 400` values are
  enforced runtime budgets in the maintained engine, not merely bootstrap
  input caps. Every incoming and outgoing live peer holds both applicable
  permits through manager release.
- the retained-peer limits are distinct from the 80/400 live-manager limits.
  The maintained engine holds per-torrent and shared-session permits for the
  entire record lifetime,
  removes incoming-only records after their manager exits, and prevents
  alternate-address reconnects from being queued repeatedly while preserving
  the retained record handle separately from the connection address. The
  accepted 1,024/4,096 values cap the exact-8.1.1 296-byte raw `Peer` structs at
  1,212,416 bytes (1.16 MiB) session-wide before map, allocator, and live
  bitfield overhead. The size sample is not a complete memory measurement.
- live-peer permits do not cover incomplete incoming handshakes before torrent
  routing, so the maintained stable backport caps that set at 256. The limit is
  TCP-only for the first release; a timeout by itself is not treated as a
  concurrency boundary.
- live-peer admission, upload rate limits, and read/write timeouts also do not
  bound response records queued after a peer is established. Valid piece
  requests cross an unbounded upload scheduler and peer writer, while valid
  BEP 9 metadata requests enter the writer directly. A tested stable/main
  candidate gives each peer one 128-permit window spanning those stages,
  advertises it as BEP 10 `reqq`, and holds each permit through the socket
  write. Admission never awaits: a peer that exceeds the advertised window is
  disconnected so its reader cannot stall behind torrent-global upload
  throttling. The maintained engine enforces the accepted 128-request window
  and over-window disconnect policy. This does not authorize a production
  writer.
- live/retained-peer permits also do not bound discovery work before admission.
  Stable 8.1.1 and current main leave outgoing DHT datagrams, recursive nodes,
  delivered peers, and metadata candidates on unbounded retained-work paths;
  current main adds an unbounded LSD result stream. A tested stable/main
  candidate bounds DHT queues at 256, recursive work at 32 active requests per
  worker, delivered peers at 256, maintenance at 256 queued/32 active requests,
  metadata work at 128 active attempts plus 256 pending addresses, a
  4,096-entry deduplication set that does not terminate discovery, and
  current-main LSD results at 256. Configured bootstrap fan-out is left
  unchanged because a small concurrency window can let a long hostname retry
  starve later entries. The candidate also ties
  the LSD task to its stream, protects replacement registrations, and reuses
  one response instead of sending every recursive DHT request twice. The fixed
  values and UDP drop policy are enforced by the maintained stable series;
  current-main LSD changes remain drift material because v8.1.1 has no LSD
  result stream. This evidence does not authorize production discovery.
- dormant torrent initialization is serialized explicitly. Stable rqbit's
  unset default permits three concurrent initialization integrity scans, which
  can multiply disk I/O before torrents become live. The one-scan guard does
  not replace §8.1's future cross-protocol active-download scheduler.
- stable rqbit's effective peer fallbacks are pinned explicitly: 10 seconds to
  connect, 10 seconds for each read/write operation, and a 120-second keepalive
  interval. Per-add options inherit that session policy, so engine-default
  drift cannot silently lengthen peer connection lifetime.
- proxy credentials are split fields: `socks_proxy_url` must contain no URL
  userinfo, `socks_proxy_username` is ordinary config, and
  `socks_proxy_password` uses the existing whole-field secret-mask and restore
  path. The adapter constructs an authenticated URL only in memory. This
  avoids exposing a password inside a partly masked URL.
- `socks_proxy_url` is an origin only: `socks5://host:port`, with an optional
  trailing slash normalized away. Paths, queries, and fragments are rejected
  before rqbit sees them. Stable rqbit otherwise ignores those components for
  peer TCP while handing the full string to its HTTP tracker client, creating
  two interpretations of one security-sensitive setting.
- proxy routing is fail-closed: proxy+DHT is invalid, and metainfo or magnets
  containing `udp://` trackers are rejected because 8.1.1 does not proxy UDP
  announces. HTTP(S) tracker announces and TCP peers remain eligible. DHT
  persistence stays disabled until nzbd can place it under its own state
  directory instead of rqbit's process-global cache.
- the dormant adapter assigns every stable 8.1.1 session option explicitly.
  Library fast-resume/session persistence, global trackers, remote blocklist
  fetching, deferred-write buffering, and UPnP remain off or absent. A future
  upstream field must therefore fail compilation and receive an explicit
  network/storage review instead of silently inheriting a new default.
- the same compile-time boundary applies to every stable 8.1.1 per-torrent add
  option. Metadata resolution and managed admission cannot silently gain file
  selection, alternate output roots, injected trackers, custom storage, or
  deferred writes through an upstream default.
- the M0 adapter accepts only URL-unreserved ASCII in proxy credentials. M2
  must either percent-encode punctuation with redaction fixtures or expose
  this as a named validation limit; it may not silently broaden the accepted
  secret form while returning partly encoded URLs in errors.

### 10.2 Category seed overrides

Extend `[[category]]` only with optional torrent policy:

```toml
[[category]]
name = "tv"
torrent_dir = "/data/torrents/tv"
seed_ratio = 2.0
seed_minutes = 10080
```

Per-add settings from Sonarr/Radarr win, then category, then `[torrent]`
defaults. `dest_dir`, `unpack`, and `extensions` retain their current Usenet
meaning until derivative torrent PP exists. Reusing them early would make a
category silently mutate seed content.

### 10.3 Validation limits

The exact constants should be centralized and tested:

| Input | Proposed limit | Failure |
|---|---:|---|
| Raw/fetched metainfo | 10 MiB default, 1–100 MiB config range | `422 metainfo_too_large` |
| Redirects | 5 | `422 too_many_redirects` |
| Files per torrent | 100,000 | `422 too_many_files` |
| One path component | 255 encoded bytes | `422 path_component_too_long` |
| One relative path | 4 KiB encoded | `422 path_too_long` |
| All path bytes | 16 MiB | `422 metadata_too_large` |
| Magnet URI | 16 KiB | `422 magnet_too_long` |
| Unique non-empty trackers | 64 per source | `422 too_many_trackers` |
| One decoded tracker URL | 2 KiB | `422 tracker_url_too_long` |
| Explicit initial peers | 80 unique unicast IPv4 endpoints | `422 invalid_initial_peer` / `too_many_initial_peers` |
| Display-safe error | 2 KiB | truncate with an explicit marker |

Limits are guards, not tuning lore. If valid field data needs a higher value,
raise the bound with a regression fixture and measured memory impact.
For magnets, `metainfo_max_mib` must reach the engine before BEP 9 buffer
allocation; enforcing it only after metadata resolution does not satisfy this
table.

The dormant adapter centralizes and enforces the limits it can own before
daemon integration: 10 MiB raw metainfo, 16 KiB magnet URIs, 100,000 files,
255 encoded bytes per component, 4 KiB per projected relative payload path,
16 MiB across projected paths, 64 unique non-empty trackers, 2 KiB per decoded
tracker URL, and 2 KiB for every rqbit operation or live-stat error that
crosses the adapter. It also deduplicates explicit peers in caller order, caps
that bootstrap vector at 80, and rejects port zero, non-unicast, and IPv6
endpoints before rqbit sees them. This is not the proposed runtime
peer cap: stable 8.1.1 can still discover and connect up to its hard-coded 128
live peers per torrent and has no session-total budget. Matching stable/main
contribution candidates enforce caller-selected per-torrent and session-total
limits across routed incoming peers and all outgoing discovery sources, but
remain review evidence rather than shipped capability. Projected paths include
the multi-file root and platform separator bytes, so the accounting bounds the
paths later passed to storage rather than only the raw bencode component
payloads. Exact-limit tests pass and the first excess byte or file returns a
stable named error. Error truncation is UTF-8 safe and ends with an explicit
marker. The dormant source-fetch boundary now validates the 1–100 MiB fetch
range, follows at most five manually checked redirects, and enforces the
selected limit while the body streams. The other dormant admission paths
still use the 10 MiB default. Daemon integration must carry one configured
limit through both fetching and later engine admission; a successful fetch
must never return bytes that the next boundary rejects only because it used a
different limit. Daemon configuration and API plumbing remain future work; no
production input is wired by these constants or helpers.
The dormant managed-admission helper accepts preflighted metainfo bytes only;
it cannot receive rqbit's URL variant. Authenticated HTTP(S) fetching remains
an nzbd-owned boundary because stable rqbit buffers the response before nzbd
can enforce its size limit and does not expose this proposal's redirect,
timeout, or redaction policy.

Tracker input limits do not bound tracker responses. Stable 8.1.1 and the
pinned rqbit-main snapshot issue HTTP announces without a tracker-owned
deadline, buffer the complete response, and accept zero-second HTTP intervals;
UDP accepts five seconds. The selected maintained engine closes that boundary
with patch 2 in §4.3.3: it bounds the complete HTTP request and decoded body
and enforces a polite minimum unforced HTTP/UDP interval. The accepted values
are 30 seconds, 1 MiB, and 60 seconds respectively. Upstream acceptance remains
optional follow-through, not a production prerequisite.

---

## 11. Security, privacy, and abuse boundaries

### 11.1 Network exposure is visible configuration

When enabled, startup logs one redacted summary:

```
BitTorrent enabled: listen=:6881 dht=off pex=on lsd=off upnp=off proxy=none
```

The Settings UI repeats that state and warns when the API is LAN-exposed but
has no password/token. It also states that a proxy setting does not prove a
VPN kill switch. In stable 8.1.1, SOCKS covers peer TCP and HTTP tracker
announces but does not cover DHT or UDP trackers. nzbd therefore rejects
proxy+DHT and proxy+`udp://` tracker combinations instead of leaking through a
second transport. The public `librqbit::SessionOptions` exposes a SOCKS URL
and listen port range; it does not prove interface-bound leak prevention. M0
must not turn “traffic probably uses the VPN” into a supported claim.

For deployments requiring a hard privacy boundary, document container/network
namespace routing through the VPN and firewall rules that reject non-VPN
egress. Application settings are defense in depth, not the kernel boundary.

### 11.2 Private torrents override discovery settings

For metainfo marked private:

- disable DHT, PEX, and local peer discovery for that torrent;
- use only trackers embedded in the metainfo or explicitly supplied by the
  authenticated add request;
- do not merge global public trackers;
- preserve the private flag through resume and cluster handoff;
- add an interop test that observes no DHT announce or PEX messages.

An unresolved magnet is privacy-unknown input. With session DHT enabled, nzbd
permits public lookup of that unknown hash, resolves BEP 9 metadata in
list-only mode, and applies the complete metainfo and private-torrent contract
before storage or managed peer handoff. A private result is rejected, but the
lookup exposure has already happened. With DHT disabled, magnets may resolve
through validated HTTP(S)/UDP trackers or explicitly supplied peers; a
trackerless magnet with no other source fails immediately with actionable
guidance.

If the selected library cannot prove this per torrent, M0 fails. Private
tracker rules are not a UI preference.

Stable 8.1.1 also loses tracker tier order before selecting the first private
tracker. The v1 adapter therefore accepts exactly one unique private tracker
and returns a named `422 private_tracker_count` failure for metainfo with
backup announce tiers. The qBittorrent shim projects that as an add failure;
Sonarr/Radarr may retry another release, but nzbd does not rewrite or discard
trackers silently. Stable rqbit also silently drops malformed, non-UTF-8, and
unsupported tracker URLs. The adapter instead treats empty tracker slots as
absent and validates every non-empty `.torrent` and magnet tracker before
admission: HTTP and HTTPS require a host, UDP requires a host and explicit
port, and every other scheme fails by name.

Many private trackers whitelist client peer IDs or approved client families;
rqbit is not assumed to be accepted. Before M4, the operator documentation and
compatibility test matrix must name each supported tracker policy. An
unapproved client response is a tracker/account-policy failure, not a network
retry, and nzbd must warn that using the qBittorrent shim does not make the
wire client identify as qBittorrent. No private-tracker compatibility claim is
made until that disclosure and an allowed-account fixture exist.

### 11.3 Secret redaction

Treat these as secrets: full magnet URIs · HTTP source query strings · tracker
announce URLs · tracker response bodies · proxy credentials · auth forms.

`socks_proxy_password` is added to configuration validation,
`mask_secrets`, and `merge_masked_secrets` together. A URL containing
userinfo is rejected, so no second hidden credential representation can leak
through `GET /api/v1/config`. A password without a proxy URL/username is also
invalid; a credential-free local proxy remains supported. The M0 boundary
accepts URL-unreserved ASCII credentials only; punctuation support is an M2
contract item with percent-encoding and round-trip redaction tests.

Logs and events may include scheme + host and a stable short digest for
correlation, for example `https://tracker.example/<redacted>#a91c03`. They may
include the info hash, which is the public swarm identifier and required for
operations. Metrics labels never include names, URLs, hashes, or peer IPs.

The native torrent-detail endpoint returns tracker hosts and status, not full
announce URLs. Exporting the retained `.torrent` is an explicit authenticated
operation and is never embedded in a normal queue response.

The dormant adapter already sanitizes rqbit operation errors and live
`stats.error` values before returning them. It removes complete magnet URIs,
URL userinfo/path/query data, recognized inline, colon-delimited, JSON-style,
whitespace-separated, and arrow-delimited secret assignments, multi-token
authorization and cookie values, peer addresses, absolute paths, and control
characters, then applies the 2 KiB display bound.
The dormant source-fetch helper applies an origin-only error contract. The
later daemon boundary must keep that invariant rather than treating either
adapter guard as a replacement for job/API redaction.

### 11.4 Source fetching

Authenticated HTTP(S) `.torrent` URLs are necessary because *arr clients may
send short-lived indexer URLs. Fetch with:

- HTTP and HTTPS only; reject `file:`, `ftp:`, `data:`, and custom schemes;
- bounded body, redirects, connect timeout, and total timeout;
- TLS verification through the existing rustls policy;
- credentials/query redacted before any error crosses into a job;
- no cookies persisted after the fetch;
- redirect target revalidated on every hop.

The dormant adapter implements that fetch contract without exposing it through
the daemon. It defaults to a 10 MiB body, five redirects, a 10-second connect
timeout, and a 30-second end-to-end timeout spanning the redirect chain and
body. URL basic-auth credentials are decoded once, survive only same-origin
redirects, and are stripped on every cross-origin hop; credentials supplied by
a redirect target are ignored. Redirects are manual, ambient proxy environment
variables are disabled, the client has no cookie store, and TLS uses the
process-wide aws-lc provider plus `rustls-platform-verifier` so operator-added
OS trust roots continue to work for private indexers. Declared and streamed
body sizes fail at the first excess byte, and returned bytes must pass the same
metainfo, geometry, path, privacy, and tracker preflight used before engine
admission. The fetch helper accepts a 1–100 MiB limit, but the other dormant
admission helpers still use the 10 MiB default; production wiring must carry
one configured value through both boundaries. Loopback tests cover both
redirect sides, status and timeout redaction, the exact five-hop boundary,
cookie non-replay, private-CA HTTPS, header and mutation-discriminating
chunked-body ceilings, and proxy-unsafe tracker rejection. This is a dormant
seam only: it creates no API route, queue job, engine session, listener,
discovery traffic, or payload I/O.

Private/LAN indexers are a legitimate deployment, so an unconditional
private-IP ban would break the intended workflow. Authentication is the
authority boundary. The UI warns that accepting a URL lets an authenticated
client make the daemon connect outbound. That authority also includes every
manually validated redirect target: a trusted or compromised indexer can pivot
the daemon's blind GET to an internal or link-local address. The response still
must parse as admitted metainfo and errors expose only the target origin, but
those guards do not remove the outbound-request risk.

### 11.5 Deletion is resolved, bounded, and idempotent

`deleteFiles=true` resolves the stored canonical content root, proves it is
beneath `torrent_dir`, stops the torrent, waits for library file handles to
close, and deletes only that root. A mismatch stops with an error; it never
falls back to deleting a parent or recomputed display name.

Single-file and multi-file torrents have different content roots. Test both,
plus duplicate names, symlinks, moved files, already-absent data, and a path
whose prefix merely resembles the configured root. The existing job
`dir_name` scar is the lesson: display names do not own storage.

### 11.6 Supply-chain boundary

- Pin the accepted `librqbit` version and review its feature graph. Keep
  default features off, use Rust TLS, and do not embed its HTTP API or Web UI.
- Run [`deny.toml`](../deny.toml) with cargo-deny 0.20.2 across all features
  and the locked graph. The policy allow-lists licenses and crates.io, denies
  wildcard and unknown-source dependencies, and forbids native-tls/OpenSSL.
- Run separate blocking license/bans/source and RustSec jobs on every pull
  request, main or release-tag push, and a daily schedule. Pin the
  cargo-deny action by immutable commit so the gate cannot change underneath
  an otherwise identical revision.
- Treat advisory exceptions as production capability restrictions, not
  waivers. The quick-xml exceptions inherited through librqbit 8.1.1 mean UPnP
  remains unavailable until the dependency is patched or replaced. The
  `time` exception is acceptable only while its vulnerable parsing feature is
  absent and Rust 1.85 remains the verified MSRV.
- Run
  [`check-reviewed-dependency-exceptions.sh`](../scripts/check-reviewed-dependency-exceptions.sh)
  in the repository-wide Supply chain workflow. It freezes the reviewed
  quick-xml, `time`, and MPL-2.0 package/feature sets plus the three permitted
  RustSec ignores without pinning nzbd's own release version; the adapter
  separately tests that no input can enable rqbit UPnP.
- Treat [the gate 9 review brief](BITTORRENT_GATE9_REVIEW.md) as the human
  decision record. CI can prove the reviewed boundary has not changed, but it
  cannot decide whether 9.64 MiB of binary growth, 8.41 MiB of idle RSS, or a
  222-package closure is acceptable. Its §4.1 records the accepted disposition
  of 2026-08-14: the measured prototype cost and two capability-restricted
  dependency exceptions are accepted, and the peer, tracker, handshake,
  retained-record, response-window, and discovery boundaries are accepted as
  requirements a stable engine must enforce before production use. Under that
  disposition's renewal rule, any engine, dependency-path, feature, advisory,
  MSRV, or relevant policy change reopens it.
- Subscribe to upstream releases and security notices, then test upgrades
  against the local swarm harness before changing the pin.
- Keep the adapter's deterministic preflight mutation corpus in ordinary CI:
  every truncation plus bounded byte replacement, deletion, and insertion
  around v1, v2-only, and hybrid seeds, with structural-limit invariants. It
  complements rather than replaces the M5 coverage-guided targets. The
  isolated `cargo-fuzz` crate pins its two test-only additions against the
  reviewed product lock, checks that every reviewed seed reaches its named
  outcome, and keeps those seeds separate from each target's ignored evolving
  corpus. The metainfo target calls the complete adapter-owned metadata-only
  file-admission wrapper across all proxy/DHT combinations without sessions or
  I/O; its nine reviewed seed classes cover version, tracker, privacy, path,
  and multifile behavior. Its 1 MiB campaign cap keeps CI bounded while main
  workspace tests retain the exact 10 MiB product boundary. The magnet target
  passes valid UTF-8 to both normal and proxy preflight from valid-v1,
  lowercase-base32, v2-only, hybrid, eager-selection, and proxy+UDP-tracker
  seeds. Its 32 KiB campaign cap permits generated inputs to cross the exact
  16 KiB product limit. Each target runs a 20,000-case pull-request smoke and a
  separate five-minute weekly campaign. Exercise path rejection through real
  engine admission even if upstream also fuzzes bencode; M5 still owns
  sustained campaigns, corpus retention, symlink, mounted-filesystem, and
  normalized case-collision probes.

---

## 12. Clustering — exclusive whole-torrent leases, after single-node

**Reference refresh, 2026-09-19:** the
[Usenet cluster completion plan](CLUSTERING_COMPLETION_PLAN.md) compares
plurx `8663d6c0` and recommends a shared control-store/lease foundation that
M6 can reuse. It does not implement torrent leases or prove torrent-specific
payload, peer-session, resume, or upload ownership. The older pinned baselines
elsewhere in this proposal remain historical evidence; consult the new plan
before selecting another coordinator or copying an older transport version.

### 12.1 First release behavior

If `[cluster].enabled = true` and `[torrent].enabled = true`, startup fails
with a validation error naming this document and the unsupported combination.
Do not accept a torrent on the leader and leave operators to discover during
failover that the job was local-only.

This is deliberately stricter than “leader only.” Existing cluster safety
relies on fenced per-job journals and idempotent article writes. A torrent
engine also has a listen identity, active peer connections, resume bitfields,
and upload accounting. Those need an explicit lease design.

### 12.2 Later cluster contract

M6 begins with a storage/topology ADR, reviewed before implementation work is
decomposed. The current nzbd shared-filesystem lease has a documented residual
fencing window that is acceptable for idempotent article writes and journal
union. It is not sufficient for a stateful torrent session. The ADR must choose
a coordination store that can perform a linearizable fence comparison in the
same transaction as publication, and must state its quorum-loss and rollback
behavior. It must also decide which bytes are shared, replicated, or local.
Whenever that store cannot establish linearizable authority, acquire, renew,
and every owner-derived authoritative mutation fail closed.
Raft majority-loss behavior is required only if the ADR chooses Raft or Hiqlite.

Add `LeaseKind::Torrent`, distinct from NNTP `Download` and `Post`:

```rust
pub enum LeaseKind {
    Download,   // existing Usenet whole-job execution
    Post,
    Torrent,    // exclusive peer session + payload write authority
}
```

[`STATUS.md`](../STATUS.md) already reserves a future `Segment` variant for
cluster C3. These are independent string-serialized variants, not competing
numeric slots:
whichever roadmap lands second must preserve the first and add its own named
variant. M6 contract fixtures include both names if C3 has landed by then.

Port plurx's monotone lease token and exact-CAS behavior rather than extending
the current lease with an ad hoc flag:

```rust
pub struct TorrentLease {
    pub resource: String,
    pub owner_node_id: String,
    pub fence: u64,
    pub revision: u64,
    pub expires_at_unix_ms: i64,
}
```

The durable row is never deleted while its resource identity can be reused.
Acquiring an expired or explicitly released lease increments the fence. Renew
and release compare the owner, fence, revision, and previous expiry exactly; a
successful renew returns a replacement token. Delayed, duplicated, and
reordered requests therefore cannot revive an older authority or create an ABA
hole. The ADR must choose either a stable per-job-incarnation resource key or a
per-info-hash tombstone, then define forget/re-add behavior, safe retirement or
retention, counter exhaustion, migration, and rollback. A retired row may be
garbage-collected only if its resource identity can never be issued again.

The ADR must decide whether torrent-capable clusters support dynamic member
removal. If they do not, remove and leave requests are refused while torrent
capability is enabled, without a partial state change. If they do, removal is a
separate authority transition: durable intent first freezes new work and
renewal for that identity and invalidates its live tokens; owned work is then
drained, transferred, or the removal is refused; only then may membership
change and the final member tombstone be materialized. A proven rejection may
roll the intent back. A timeout, lost response, crash, or otherwise ambiguous
commit outcome stays fenced until the new configuration is proved.
Member-removal tombstones, retained lease-resource rows used for ABA
prevention, and stale resume-generation retirement use separate keys and
lifecycles.

One node holds the torrent lease and runs its peer session. Durable queue
facts, the live lease, and the pointer to the current resume generation live
in the linearizable coordination store. Payload and checkpoint bytes may live
on a shared volume or a named node, as selected by the ADR, but they are not a
transactional database mutation. Each owner writes a fence-scoped generation;
only a transaction that checks the exact live fence and current member
eligibility can publish that generation as current. The same rule covers
readiness, durable progress and upload accounting, seed-policy transitions,
and every other authoritative fact derived from owner work; a prior eligibility
or lease check is not fencing. A stale generation may remain on disk for
recovery or cleanup, but must never become authoritative. The adapter cancels
and self-fences no later than its local lease deadline when renewal fails or
cannot reach the selected store's authority.

A successor may adopt reported progress or readiness only after proving that
the exact published resume/payload generation is accessible and validating it
under the new fence. Otherwise it starts a clean isolated generation with zero
inherited readiness or progress and re-verifies or redownloads the bytes. A
replicated path string, heartbeat, or stale local file is not proof of access.

The worker heartbeat reports protocol-neutral bytes plus torrent upload and
peer facts. Provider connection budgets do not apply; torrent peer and upload
caps do. A heartbeat is observability, not write authority.

The ADR also names the schema and acceptance use of a reproducible topology
artifact. Semantic CI proves cardinality, quorum, workload identity, durable
results, and failure behavior. Named-runner CPU, memory, storage, network, and
latency samples are required only for quantitative comparisons or a
performance-based default. If the chosen design later admits bounded follower
reads, freshness comes from a quorum-backed term/leader/commit watermark with a
local monotonic deadline and applied-index limit. Missing or expired proof
falls back to authority or refuses the bounded read according to the caller's
contract. That proof says nothing about payload readability, piece validity,
or job readiness. Metrics scrapes remain store-free; bounded background
sampling may perform the coordination work.

### 12.3 Failure sequence

```
 worker stops renewing or lease expires
        │
        ▼
 durable lease CAS advances the fence
        │
        ├── old worker sees cancel/deadline ──────▶ stops and self-fences
        │
        ▼
 new worker receives descriptor + new lease token
        │
        ▼
 validates payload + selected resume generation
        │
        ▼
 writes a new generation; fenced transaction publishes its pointer
        │
        ▼
 reconnects to swarm; never trusts old unverified pieces
```

Database fencing does not magically fence filesystem writes, open sockets, or
peer uploads. Piece writes of hash-identical bytes may be idempotent in theory;
the design does not rely on that theory. A resumed process that was stopped
past its TTL must not mutate the authoritative/current namespace or publish a
current pointer. A late physical write is tolerable only inside its isolated
generation, which must remain permanently unreachable after the fence changes.
The adapter's storage guard rejects work after cancellation or the local
deadline. If the ADR instead requires zero physical writes or packets after
resume, it must select and prove a storage/network-level fencing primitive;
plurx's user-space cancellation contract does not provide that guarantee.

### 12.4 Cluster acceptance gate

Use real processes and a controllable coordination store. The gate must cover:

- delayed and reordered renew/release calls, lost acknowledgements, repeated
  acquire, and ABA attempts against a durable row whose fence never resets;
- two takeover candidates, leader loss, worker loss, loss of linearizable
  authority, and recovery without granting authority while the store cannot
  prove a current writer; include majority loss when Raft or Hiqlite is chosen;
- if dynamic removal is supported, removal intent before membership change;
  crashes at every removal boundary; concurrent admission and removal; late
  work; a restarted old identity; heartbeats that try to clear a tombstone;
  rollback after a proven rejection; and timeout or unknown outcomes that
  remain fenced; otherwise, stable refusal of remove and leave requests for a
  torrent-capable cluster with no partial state change;
- an exact published resume/payload generation that can be adopted only by a
  survivor that proves access and validates it, plus an unavailable generation
  that restarts clean with zero inherited readiness/progress and re-verifies or
  redownloads instead of trusting stale state;
- independent retention and cleanup tests for the member-removal tombstone,
  lease-resource row, and stale resume generation so one lifecycle cannot
  reopen another;
- `SIGSTOP` beyond TTL followed by `SIGCONT` during metadata fetch, piece
  download, hash check, readiness publication, and seeding, proving the stale
  process cannot mutate the current namespace or publish a current pointer;
  any late write must stay in a permanently unreachable stale generation;
- stale resume generations, stale payload writes, peer termination, monotone
  upload accounting, fenced seed-policy transitions, and no false-ready event;
- mixed-version startup, migration rollback, and parity of data before and
  after enabling the new coordination topology;
- a schema-validated topology artifact for the chosen design, with
  named-runner resource evidence before any quantitative topology claim; and
- if bounded follower reads are enabled, invalid, expired, wrong-term, and
  lagged watermark cases that fall back to authority or refuse, plus a metrics
  scrape test that performs no coordination-store work.

Every case must converge to one live session and correct payload. Until the
matrix is green, cluster+torrent stays a config error. A unit test of a lease
guard or an in-process task cancellation is not a substitute for this harness.

---

## 13. UI, mobile, events, and metrics

### 13.1 Queue presentation

Every row gains a protocol chip: `USENET` or `TORRENT`. Reuse the current
progress and rate columns, then show protocol facts in the detail panel:

| Surface | Usenet | Torrent |
|---|---|---|
| Progress | articles/files/health | selected bytes/files/piece verification |
| Sources | configured providers | peers · seeders · leechers · tracker/DHT status |
| Transfer | download rate/retries | download + upload rates · ratio · ETA |
| Completion | PP stage and final dir | ready path + seeding policy/progress |
| Failure | health/server/write reason | source/metadata/tracker/path/library reason |

Do not show torrent “health” as a percentage. Availability, connected peers,
and last useful activity are facts; a made-up health score would tell the user
a stalled private torrent is doomed when its tracker is merely between
announces.

### 13.2 Controls

Existing pause/resume, priority, move, delete, and delete-data controls work
for both protocols. Torrent detail adds:

- upload limit and current upload rate;
- seed ratio/time policy and progress;
- tracker host/status and next announce, redacted;
- peer counts, never the full IP list in the ordinary UI;
- “recheck” only if the library exposes a safe public API and M0 proves it;
- “copy magnet” only when doing so will not expose a private tracker passkey
  without an explicit warning.

### 13.3 Mobile

Extend `mobile/src/api/types.ts`, formatting, queue sections, and detail views
with optional torrent fields. Existing app versions ignore added JSON fields
and continue controlling Usenet jobs. A new app shows the protocol chip,
upload rate, ratio, peers, readiness, and seed limit. `.torrent` submission
uses the existing document picker with a new MIME/extension path; magnet add
gets a text/paste action.

### 13.4 Events

Add these structural events:

- `job_metadata_ready` — provisional magnet became a named/filed torrent;
- `torrent_seed_limit_reached` — torrent paused, data retained;
- `torrent_tracker_error` — only on state change and with redacted tracker;
- `torrent_missing_files` — payload no longer matches stored layout.

The structural snapshot publication carries readiness (§7.3). During an
active download, the 1 Hz `tick` carries payload progress. Volatile seed
bytes/rate/peer counters stay in the on-demand torrent-detail response, so an
idle seed does not defeat byte-identical tick suppression. Do not emit one
SSE frame per piece, block, peer connect, or tracker retry. The event ring is
for things a consumer acts on, not a packet trace.

### 13.5 Metrics

Add low-cardinality process metrics:

```text
nzbd_transfer_download_bytes_total{protocol="usenet|bittorrent"}
nzbd_transfer_download_rate_bytes_per_second{protocol="usenet|bittorrent"}
nzbd_torrent_upload_bytes_total
nzbd_torrent_upload_rate_bytes_per_second
nzbd_torrent_jobs{phase="metadata|downloading|seeding|paused|error"}
nzbd_torrent_peers_connected
nzbd_torrent_tracker_errors_total
nzbd_torrent_piece_hash_failures_total
nzbd_torrent_resume_rechecks_total
```

Never label by job, torrent hash, name, tracker, category, or peer. Per-job
detail lives in the API and logs; Prometheus label cardinality is not a debug
database.

Keep the existing `nzbd_download_rate_bytes_per_second` as the aggregate,
unlabelled alias. Existing dashboards must not go blank when the labelled
per-protocol metric arrives; deprecation, if any, is a later release-note and
dashboard migration.

---

## 14. Failure and recovery matrix

| Failure | Required state | Automatic action | Operator sees |
|---|---|---|---|
| Invalid bencode/path | No live job for raw add | Reject before filesystem write | `422` with safe named reason |
| HTTP source timeout | Admitted source job, no payload | Retry bounded fetch or fail source stage | `fetching_source` then explicit error |
| Magnet has no metadata peers | Live, not ready | Stay queued/stalled; do not call failed | Metadata age, DHT/tracker facts |
| Tracker error | Live | Back off per engine; continue DHT/other trackers when allowed | Redacted tracker host + last error |
| No peers | Live | Stay stalled | `stalledDL`, availability/last activity |
| No metadata/payload progress for 60 s and no useful peer | Live, slot yielded | Revoke run permission; discovery-only or idle-capacity probe per §8.1 | `stalledDL`, last progress, next probe |
| Proxy configured with DHT or UDP tracker | No live job/session for invalid input | Reject before traffic starts; never downgrade to direct UDP | Named config or `422` privacy error |
| Private metainfo has backup announce tiers | No live job | Reject rather than choose a nondeterministic tracker | `422 private_tracker_count`; *arr add fails visibly |
| Bad piece | Not counted complete | Discard and redownload | Hash-failure counter; no false progress |
| ENOSPC/EDQUOT | Live, download paused | Latch disk guard; keep API, seed existing data when possible | Full path/root signal and observed error |
| Process kill mid-piece | Piece untrusted | Resume/recheck; never ready from partial write | `checking` after restart when needed |
| Process kill after ready commit | Ready survives | Restore and verify resume before seeding | Ready remains once trust is re-established |
| Payload moved/deleted externally | Live, not silently redownloaded | Pause as `missing_files` pending operator/caller action | Missing path and safe recovery actions |
| Listen port unavailable | Feature cannot start safely | Fail startup if enabled | Exact bind address/port, no fallback random port |
| DHT unavailable | Tracker torrents may continue | Mark DHT degraded | Session status, magnet warning when trackerless |
| Seed limit reached | Ready + paused seed | Persist counters, pause, retain data | `pausedUP`, limit and achieved ratio/time |
| Forget, keep data | Terminal history | Stop session, remove metadata/control, retain payload | History says data retained and where |
| Forget, delete data | Terminal history | Stop handles, validate root, remove exact content | History says deleted; any refusal is loud |

Silence is never evidence of swarm health. Equally, temporary silence from a
swarm is not evidence of terminal failure.

---

## 15. Implementation plan — each milestone leaves a usable truth

### 15.1 M0 — maintained dependency and interoperability spike (accepted)

The checked-in adapter remains isolated from the daemon; there is no production
config, API, or peer listener.

**Work:** pin `librqbit` 8.1.1 with Rust TLS · compile every target · generate
a local v1 torrent · run loopback seeder/downloader through `.torrent` and
magnet paths over TCP/IPv4 · test private mode/first-tracker behavior with PEX
and DHT/LSD capture controls · kill/resume · live limits · path rejection ·
install/assert the process rustls provider · measure
binary/memory/package/license deltas · record the public API needed by §5.2.

**Stop conditions:** any §4.3 gate fails without a small maintainable fix ·
Rust 1.85 or musl cannot build · private mode leaks discovery · safe delete
root cannot be established · resume can promote unverified data.

**Acceptance:** a checked-in `nzbd-torrent` integration test completes and
seeds deterministic generated content with the internet disabled; the build
matrix and a short spike report state pass/fail for all eleven §4.3 gates.

**Result:** ADR-19 now selects the maintained v8.1.1 derivation. The local and
native combined proofs pass all eleven gates, including process-kill selective
restore, explicit `unknown` discovery diagnostics, patch integrity, and
reviewed runtime limits. Independent review remains the completion evidence.
Do not start M2 session integration from this change. See
[BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md).

### 15.2 M1a — queue schema envelope (complete)

**Work:** add queue schema version 2 · treat a missing version as legacy v1 ·
name future/retired version failures · refuse non-current writes · avoid a
second parse on healthy production snapshots.

**Acceptance:** `cargo test --workspace` and the NZBGet golden suite remain
unchanged; a v1 queue fixture round-trips, a future-version document containing
an unknown job kind fails with the named version error, the v2 snapshot cannot
be written as schema 1, and a production-sized current snapshot round-trips.

**Result:** complete. Version 2 contains no torrent job representation and is
useful hardening on its own.

### 15.3 M1b — protocol-neutral backend seam (merged in PR #9)

**Work:** atomically bump the queue writer to schema version 3 · add
`JobKind::Torrent` and the defaulted `torrent` field · add `kind`/readiness to
`JobSummary` · introduce backend commands/facts around the current Usenet
executor in place · keep one queue owner and snapshot · add schema migration
and old-mobile projection tests. Do not extract a `nzbd-usenet` crate.

Do not start `librqbit` from the daemon in this milestone. A fake torrent
backend proves queue command routing, stalled-slot yield, and coalesced
progress without network or disk behavior. The amendment in §4.3.2 permits
this engine-independent boundary while keeping all M2 networking blocked.

**Acceptance:** v2 fixtures migrate to v3 without semantic change; a v3
torrent row makes the current v2 binary fail by schema version; fake Usenet
and torrent jobs obey one priority/pause/max-active schedule; a stalled
torrent yields so a following Usenet job completes and later reacquires only
when eligible; a progress flood cannot delay a delete command beyond the
bounded test threshold.

**Result:** implemented for review. The queue writer emits schema 3, schema-2
Usenet rows retain their meaning, the combined active set accounts for fake
torrent work, and backend progress cannot consume control/structural FIFO
capacity. At M1b merge the daemon had no torrent config or admission path and
rejected a persisted torrent row with a named M1b error before starting any
peer session. M2a later added dormant config and retained the fail-closed
activation boundary. See [BITTORRENT_M1B_REPORT.md](BITTORRENT_M1B_REPORT.md).

### 15.4 M2 — single-node torrent download, resume, and seeding

M2b keeps activation dormant while establishing ownership: `nzbd-torrent`
owns one maintained session and indexes raw handles only by rqbit identity and
v1 info hash. The protocol-neutral runtime beside the queue owner alone holds
`JobId -> engine identity`. Session persistence starts with automatic restore
disabled; the durable queue supplies the selected metainfo descriptors,
preferred engine IDs, file selection, and paused-first resume order. A resume
checkpoint or readiness disagreement forces recheck, and later bytes on disk
cannot advance the durable trusted checkpoint by themselves. The production
`nzbd` dependency graph remains isolated until final M2 activation.

M2d folds maintained-engine observations through the M1b backend contract.
Rate and useful-peer samples remain replaceable progress; only verified and
uploaded checkpoints plus useful activity reach durable queue state. A
`Ready` fact is accepted only after every selected byte is reported verified,
and its canonical content path means the adapter has completed the payload and
directory durability barrier. The queue persists readiness while the job stays
live in `seeding`. ENOSPC/EDQUOT, missing payload, and transient discovery are
classified as recoverable `Stopped` facts; only an explicitly classified,
display-safe unrecoverable error may become `Failed`.

**Work:** add `[torrent]` and paths · start one session only when enabled · raw
metainfo/magnet/URL admission · durable descriptor before start · handle map ·
stats coalescing · readiness · pause/resume/delete · seed policy · download and
upload limits · disk guard · watch directory · terminal history.

**Prerequisites already met:** [`REGRAB_LOOP_PLAN.md`](archive/REGRAB_LOOP_PLAN.md)
F1–F3 landed on 2026-07-31, and
[`DEFECT_HISTORY_DELETE.md`](archive/DEFECT_HISTORY_DELETE.md) is resolved with shared
JSONL tombstones. M2 may rely on the enforcing disk guard and terminal-history
delete semantics; neither is still a reason to start production networking.
The complete M0 matrix and independent review of the maintained engine are
accepted. M2a #152 and M2b #153 merged via PRs #161/#181; remaining M2 slices
are tracked by #154–#160 under the corrected dependency graph, and final
activation #163 retains sole activation ownership. That graph authorizes
milestone work and review in dependency order, not an incomplete production switch.

Keep native UI changes minimal in this milestone; the API and logs must make
every state observable.

**Acceptance:** a daemon e2e test adds local `.torrent` and magnet jobs,
downloads exact generated bytes, durably publishes `ready=true`, seeds to a
second client, survives kill/restart, honors limits, and deletes only the
requested payload. With `[torrent].enabled=false`, it opens no peer listener.

### 15.5 M3 — native API, web UI, and mobile

**Work:** typed native admission · torrent detail/export · file projection ·
protocol chip · upload/ratio/peer/seed views · controls · metrics · mobile
types/screens/document picker/magnet add · configuration editor and warnings.

**Prerequisite:** the mobile P0 release-signing and non-Latin-1 Basic-auth
fixes from [`MOBILE_REVIEW.md`](MOBILE_REVIEW.md) are implemented and guarded
in CI. M3 remains downstream of completing the tracked M2 slices #154–#160 and
final activation #163, but no longer inherits those two defects.

**Acceptance:** DOM and mobile unit tests cover every new state; an old mobile
client fixture receives only existing top-level status values; an idle seed
produces no repeated full-queue tick; UI controls revert loudly on backend
failure; no displayed/logged value contains a tracker passkey fixture.

### 15.6 M4 — qBittorrent compatibility for Sonarr and Radarr

**Work:** new `nzbd-qbit-compat` crate · endpoint set in §7.4 · Bearer and
cookie auth · category overlay · state and field projections · request/client
attribution · contract fixtures captured from current Sonarr and Radarr.

Run real application integration tests against pinned Sonarr and Radarr
containers: connection test · category create · magnet add · `.torrent` add ·
progress poll · import path · imported-category change · seed limit · remove
keep/delete data.

**Acceptance:** both applications complete that workflow without patches;
unsupported qBittorrent endpoints remain absent; the NZBGet golden suite is
byte-identical and torrent rows never appear through it.

### 15.7 M5 — adversarial hardening and release gate

**Work:** fuzz/preflight malformed metainfo · traversal/symlink/case collision ·
source redirect/size/time bounds · auth failure limiting · private torrent
network capture · full-disk behavior · 100k-file metadata · 100 concurrent
torrents idle/active mix · shutdown deadline · supply-chain/license review ·
operator docs and examples.

**Acceptance:** the failure matrix in §14 is executable and green; `make gate`
passes; Linux glibc/musl, macOS, Windows, Docker, and MSRV artifacts build; the
reviewer can verify public traffic, ports, paths, seed policy, and deletion
from docs without reading code.

The checked-in release entry points separate deterministic proof from bounded
coverage-guided work. `make gate` runs formatting, strict lint, the whole
workspace suite, Rust 1.85 checking, the dormant adapter dependency policy,
the production daemon's continued isolation from `nzbd-torrent` and every
`librqbit*` package, reviewed dependency exceptions, and the committed fuzz
seed contracts.
`make gate-fuzz` first runs that deterministic gate and then both default
20,000-case libFuzzer campaigns. The latter requires the pinned nightly
toolchain and cargo-fuzz used by CI; weekly five-minute campaigns remain
separate evidence rather than silently extending every local gate.

The checked-in
[pre-release operations review](BITTORRENT_RELEASE_REVIEW.md) gives reviewers
one short surface for public traffic, ports, paths, seeding, deletion, current
evidence, and stop conditions. Its policy check pins the selected engine
boundary, the eleven-gate M0 state, and continued daemon isolation. Updating
that check requires the same reviewed gate-state change as this proposal and
the M0 report; it cannot be bypassed to expose production wiring.

**Current groundwork:** adapter-owned metadata-only file admission and magnet
preflight have separate feature-gated libFuzzer targets. Nine metainfo and seven
magnet seed classes are reviewed and contract checked, separate from each
target's ignored evolving corpus; each target has its own dictionary, bounded
pull-request smoke, and weekly five-minute campaign. The main workspace suite
also accepts a valid magnet URI at the exact 16 KiB product limit and a valid
v1 document at the exact 10 MiB default metainfo ceiling, naming the first
excess byte for each. It accepts a valid 100,000-file v1 inventory below the
metainfo ceiling and names file 100,001 as the first rejected inventory. A
DHT-disabled session probe also retains 100 distinct torrents at once, with an
exact ten live-but-peerless and ninety paused mix, and fails if admission takes
more than 30 seconds or shutdown takes more than 10 seconds. Its sources have
no trackers or initial peers, so it starts no public discovery or payload
transfer. The
[first native evidence run](https://github.com/pjunod/nzbd/actions/runs/31330178035)
passed the isolated adapter suite on 2026-08-09 UTC across Linux x86_64 GNU,
Linux x86_64 musl, Linux aarch64 musl, macOS aarch64, and Windows x86_64. The
[maintained-engine sampled-memory run](https://github.com/pjunod/nzbd/actions/runs/31837867629)
passed the same five targets on 2026-08-14 UTC after deriving the exact
then-current nine-patch engine: 100-torrent sampled RSS growth ranged from 2,863,104 to
5,144,576 bytes, and 100,000-file preflight growth ranged from 3,768,320 to
27,295,744 bytes. Exact
baselines, maxima, and timings are recorded in the M0 report. This is useful
continuous coverage plus
wall-clock and retained-memory regression evidence, not M5 completion:
crash-free bounded runs, functional limits, periodic RSS samples, and a
deadline on shared CI do not establish peak-memory exhaustion resistance,
filesystem containment, production shutdown orchestration, or sustained
fuzzing adequacy.

An ignored crate-private local-swarm unit probe now injects `StorageFull` from
`pwrite_all` after one successful 16 KiB piece write. Before admitting the
faulted 256 KiB torrent, it requires a separate 64 MiB filesystem-backed
control torrent to be live with nonzero incomplete progress. The target must
then remain incomplete in `Error` with an independently bounded stats response
while the already-active control remains live, completes, and leaves the
fault state and write accounting unchanged. The seeder is capped at 8 MiB/s
so the fault boundary cannot race the control's completion, and rqbit binds
directly from a test-only multi-port range so no temporary-listener handoff is
involved. The custom-storage and broad-listen helpers are absent from non-test
builds; the normal adapter still forbids custom storage and accepts only one
explicit non-zero listen port. This pins a write-time engine seam and current
containment behavior, but deliberately does not call the §14 ENOSPC row green:
rqbit transitions the write-faulted torrent to `Error`, and M2 must route that
signal into nzbd's existing multi-root guard, stop new piece requests, and
choose the documented pause/upload fallback. The guard already exposes its
limiting volume; no daemon torrent API or production torrent writer exists to
prove the remaining outcomes yet.

A second ignored crate-private probe now covers the initialization-time path.
It injects `StorageFull` from `ensure_file_length` while adding a paused,
peerless 262,144-byte torrent and requires exactly one sizing request, zero
piece writes, and a zero-byte filesystem object. The maintained test requires
admission or initialization to return a bounded error retaining the file name,
requested length, and injected storage cause. If scheduling returns a handle
first, it must enter `Error`; `Paused` or `Live` is a failure. Reverting patch
`0018` restores stable rqbit's warning-only behavior and makes this test fail.
The shared runner must discover and execute exactly one instance of both
ignored storage proofs. The
[2026-08-13 replacement run](https://github.com/pjunod/nzbd/actions/runs/31654021866)
records the superseded fail-open result on Linux x86_64 GNU, Linux x86_64
musl, Linux aarch64 musl, macOS aarch64, and Windows x86_64. The
[maintained fail-closed run](https://github.com/pjunod/nzbd/actions/runs/31837867629)
passes on those same targets.

The maintained dependency includes the exact-stable change that makes this
failure stop initialization. The stable and current-main variants preserve selected-file and padding
behavior, add file/length context, and return the first `ensure_file_length`
error through rqbit's existing initialization error path. A sensitivity
mutation inside the helper makes the exact unit test fail, while a separate
fail-closed verifier assertion rejects either source line if its blocking
initialization boundary discards the helper result. The current-main candidate
also types checksum cancellation and proves a concurrent pause suppresses only
that marker, not a storage failure. The exact stable derivation and focused
tests block pull requests; documented and moving-main variants remain upstream
drift material. This fix does not satisfy the daemon storage policy by itself:
M2 still needs torrent fault routing, pause, API, and recovery behavior before
production wiring.

The
[hardened native run](https://github.com/pjunod/nzbd/actions/runs/31345445318)
passed the behavioral probe and its fail-closed exact-test discovery guard on
Linux x86_64 GNU, Linux x86_64 musl, Linux aarch64 musl, macOS aarch64, and
Windows x86_64 on 2026-08-10 UTC. Each target injected the fault on write
attempt two after one 16,384-byte write call returned successfully, returned
the independently scheduled fault stats inside one second, kept the
already-live control active and incomplete at the boundary, completed all
67,108,864 control bytes under the deterministic seed cap, and reasserted
unchanged fault state and write accounting afterward. The runner first proved
that its discovery guard rejects a benchmark-classified non-test line and then
required discovery and execution of exactly the one ignored write-time proof
test. This historical run predates the sizing-fault proof and does not broaden
the proof beyond the limitations above.

### 15.8 M6 — cluster torrent leases (separate approval)

**Entry gate:** single-node field data exists and a storage/topology ADR has
selected the linearizable coordination store, facts-versus-bytes placement,
quorum-loss behavior, migration, and rollback. A different-model adversarial
reviewer posts numbered findings on that architecture change; the lead records
Accept, Reject, or Defer with evidence and resolves every critical/high finding
before implementation children are created.

**Work:** port the pinned plurx lease and publication contract behind nzbd's
store interfaces · add the lease kind and durable monotone fence · add
generation-scoped resume/payload writes plus same-transaction fencing of every
owner-derived authoritative mutation · add the durable removal-intent sequence
and distinct member/resource/generation retirement rules, or reject dynamic
removal for torrent-capable clusters · add bounded self-fencing on renewal loss
· expose passive heartbeat facts and leader proxying · emit the
schema-validated topology artifact · run the real-process failover harness ·
remove the config compatibility error only after the complete gate passes.

**Acceptance:** the §12.4 kill matrix converges with one session and correct
bytes; delayed same-token operations, acknowledgement loss, ABA, loss of
linearizable authority, two-candidate takeover, ambiguous removal or explicit
removal refusal, stale generations, mixed versions, and rollback are covered;
and enabling torrent+cluster no longer weakens any existing single-node or
Usenet cluster invariant. In particular, `SIGCONT` after a TTL cannot let the
stale owner mutate the authoritative namespace or publish a current generation;
any late bytes remain permanently unreachable. Majority loss and
quorum-watermark cases apply when the selected ADR uses those mechanisms; loss
of linearizable authority always fails closed.

### 15.9 Milestone commit rule

One milestone may take several commits, but no commit may expose a config/API
switch that starts a half-wired backend. Behavior, tests, docs, configuration
reference, API summary, and STATUS update land together when the behavior
becomes reachable.

---

## 16. Test strategy — deterministic local swarms, real consumer contracts

### 16.1 Unit and property tests

- Source parsing: v1 magnet forms, base32/hex `btih`, duplicate parameters,
  oversize input, missing hash, and named v2-only/hybrid rejection for both
  magnets and metainfo.
- Magnet preflight: only decoded `xt` parameters determine the info-hash
  version. One valid 40-hex or 32-base32 `btih` is required; lowercase base32
  is normalized before rqbit parsing. Duplicate v1,
  malformed v1, v2-only, and hybrid topics fail by stable name. Version-looking
  text in display names and tracker URLs is an explicit negative fixture. A
  fake BEP 9 peer also proves resolved metainfo is revalidated in list-only
  mode before any payload storage exists. A loopback DHT node proves a
  trackerless public magnet sends `get_peers`, resolves metadata without
  `announce_peer`, and leaves no managed handle. Paired private fixtures prove
  both adapter entry points reject after metadata inspection, while the
  DHT-disabled explicit-peer path remains available. A separate libFuzzer target passes
  valid UTF-8 to the exact preflight in normal and proxy modes. Its seven
  contract-checked seeds cover valid v1, lowercase-base32 normalization,
  v2-only, hybrid, authority-form rejection, eager-selection rejection, and
  proxy+UDP-tracker rejection. Every accepted normalized URI is also parsed by
  rqbit inside the target, so preflight/parser divergence becomes a reproducer.
  Pull requests run 20,000 cases and the weekly job runs for five minutes;
  generated input is capped at 32 KiB, failing reproducers are uploaded, and
  the evolved corpus is not retained. An adjacent contract passes a valid URI
  at exactly the 16 KiB product limit through the full preflight and requires
  the first excess byte to fail by name.
- Private metainfo discovery: a known-private `.torrent` is rejected before
  engine admission when session DHT is live; the same one-tracker metainfo is
  accepted by policy when DHT is disabled.
- Tracker preflight: malformed/non-UTF-8 URLs, missing hosts, UDP without an
  explicit port, and unsupported schemes fail by name for both metainfo and
  magnets; HTTP, HTTPS, and explicit-port UDP remain accepted when proxy policy
  permits them.
- Path validation: every platform prefix/separator, traversal, normalization,
  symlinks, case collisions, padding files, exact-root delete proof.
- Adapter path preflight: the portable metadata-only subset and declared
  symlink, Windows device/character/alias, exact-duplicate, and
  lowercase-collision rejection run as socket-free unit tests before
  real-admission path tests. The session root is canonicalized, and an
  existing-symlink integration case must leave its external target untouched;
  a dedicated native test target records hosted filesystem case, Unicode
  NFC/NFD, compatibility-ligature, and full-fold aliasing. It requires the
  compatibility-width pair still admitted by the adapter to remain distinct.
  Descriptor-relative containment and filesystem-specific behavior on
  supported operator payload mounts remain M5 probes.
- Metainfo preflight: a deterministic mutation corpus runs in ordinary CI and
  pins exact accepted/rejected counts for v1, v2-only, and hybrid seeds; a
  validator replaced by `Ok(())` therefore fails rather than turning the
  corpus into a no-panic-only test. Checked v1 payload/piece/hash geometry
  prevents zero-divide, length-wrap, and inconsistent hash-table input from
  reaching rqbit. A separate libFuzzer target exercises the complete
  metadata-only file-admission wrapper across all proxy/DHT combinations from
  nine contract-checked version, tracker, privacy, path, and multifile seeds.
  Pull requests run 20,000 cases and the weekly job runs for five minutes,
  with failing reproducers uploaded. Reviewed seeds are kept apart from the
  ignored evolving corpus. The campaign caps generated input at 1 MiB and does
  not retain an evolved corpus. Main-workspace contracts accept a valid v1
  document at the exact 10 MiB default and prove the first excess byte fails by
  name; they separately construct a valid 100,000-file v1 inventory under that
  ceiling and prove the first excess file fails by name. They deliberately do
  not infer a stable peak-memory budget from shared CI runners. Sustained
  coverage-guided fuzzing and the remaining M5 resource/filesystem probes
  remain a release gate.
- State projection: every `TorrentPhase` maps to the expected native and
  qBittorrent status, with units/sentinel values pinned.
- Seed policy: add/category/global precedence, ratio precision, time across
  restarts, limits pause but never delete.
- Scheduler: mixed priorities/protocols, force semantics, stalled-slot yield
  and later reacquisition, slot release on readiness, seeding excluded from
  download slots.
- Session pressure: retain 100 distinct torrents in one DHT-disabled session,
  require an exact ten-active/ninety-paused mix, and fail when admission or
  shutdown exceeds the documented 30-second or 10-second deadline. Keep every
  source trackerless and peerless so this remains bookkeeping evidence rather
  than a public-network or throughput test.
- Session memory pressure: run the same trackerless, peerless 100-torrent mix
  in an optimized process, sample resident memory before session construction,
  after construction, after every ten admissions, and after all handles have
  initialized into the exact 10-live/90-paused state; fail if sampled growth
  exceeds the preliminary 32 MiB regression ceiling. Record every native
  platform separately, and exercise exact-ceiling acceptance plus first-byte-
  excess rejection through the same guard before measuring. This catches large
  retained-memory regressions; periodic working-set samples do not prove the
  true allocation peak, active swarm behavior, or exhaustion resistance.
- Metainfo memory pressure: construct and validate the accepted 100,000-file
  inventory in an optimized process, sample resident memory before fixture
  construction, after construction, and after validation, and fail if sampled
  growth exceeds the preliminary 64 MiB regression ceiling. Record every
  native platform separately, and exercise exact-ceiling acceptance plus
  first-byte-excess rejection through the same guard before measuring. This
  catches large retained-memory regressions; three working-set samples do not
  prove the parser's transient allocation peak or resistance to concurrent
  hostile submissions.
- Redaction: adapter engine/stat errors prove magnet, tracker/query/proxy,
  secret-assignment, embedded JSON/query assignments, private-tracker
  `torrent_pass`/`authkey`, API keys, signatures, session identifiers,
  peer-address, absolute-path, control-character, and UTF-8 truncation
  fixtures; later source/API tests prove the same invariant through logs,
  events, and metrics.

Every new safety guard must have a discriminating negative case: remove or
bypass the guard once, observe the named test fail, and record that evidence in
the PR. A test-name filter must first prove it matched an exact test; an exit-0
run with zero selected tests is not evidence.

### 16.2 Local swarm e2e

Generate content and metainfo during the test. Run a local seed session and
nzbd downloader with DHT disabled and an explicit loopback peer or local
tracker. This avoids copyrighted fixtures, public trackers, internet
flakiness, and dependency on a third-party swarm.

Cover: single/multi-file · empty file · boundary piece sizes · magnet metadata ·
bad-piece peer · disconnect/reconnect · pause/resume · selective file fixture ·
rate changes · seed upload · restart · delete keep/data · ENOSPC fault injection.

The SOCKS fixture binds the proxy's outbound TCP source and places a recording
relay before the seeder, then asserts that every observed connection came from
that source. Separate admission tests reject proxy+DHT and proxy+UDP-tracker
inputs before any session or torrent traffic. Proxy-use evidence without the
absence-of-direct-path assertion is not leak-prevention evidence.

### 16.3 Consumer integration

Pin tested Sonarr and Radarr image versions in an opt-in/CI job and make their
HTTP traffic available as sanitized fixtures for fast contract tests. The
real-container test is authoritative; fixtures make regressions cheap to
locate. Upgrade the pins deliberately and compare new calls before claiming a
new supported consumer version.

### 16.4 Compatibility matrix at first release

| Input/client | Supported | Evidence required |
|---|---|---|
| v1 `.torrent` over TCP/IPv4 | Yes | local + cross-client fixture |
| v1 magnet (`btih`) over TCP/IPv4 | Yes | metadata exchange + restart |
| HTTP(S) `.torrent` URL | Yes | redirect/auth/redaction/limit tests |
| Private v1 torrent, primary tracker | Yes | network capture proves no DHT/PEX/LSD; first-tracker behavior recorded |
| Private v1 torrent, backup announce tiers | No in 8.1.1 | named `private_tracker_count` rejection; no nondeterministic tracker choice |
| uTP peer transport | No | named stable-8.1.1 limitation |
| IPv6 peer transport | No | named stable-8.1.1 limitation |
| v2-only (`btmh`) | No | named rejection |
| Hybrid v1/v2 | No | named rejection for metainfo and magnets |
| Sonarr qBittorrent client | Yes | real-container workflow |
| Radarr qBittorrent client | Yes | real-container workflow |
| General qBittorrent Web UI/API clients | No claim | unsupported routes are explicit |
| Cluster mode | No in first release | startup validation error |

### 16.5 Performance checks

Measure rather than inherit upstream anecdotes:

- download/upload throughput on loopback and 1/10 Gbit-capable hosts;
- CPU per GiB hashed and served;
- memory for 1 · 10 · 100 active torrents and 100 idle seeds;
- startup/recheck time with 1 TiB of ready payload and valid/invalid resume;
- queue-owner command latency during peer churn;
- snapshot/resume write volume on local and network state storage;
- binary/image size delta on each target.

No fixed throughput promise belongs in README until nzbd’s adapter has
measurements on its own build.

---

## 17. Rollout and rollback

### 17.1 Rollout sequence

1. Merge the dormant backend seam with feature disabled.
2. Ship an experimental single-node flag to test installs only.
3. Validate one public v1 swarm and one private-tracker fixture with redacted
   operational evidence.
4. Validate Sonarr and Radarr independently.
5. Mark single-node v1 support stable; keep v2, PP, and cluster claims absent.
6. Collect field data before approving M6 or derivative PP.

### 17.2 Upgrade behavior

- Existing config: torrent disabled, no new port, no DHT, and no transfer or
  disk-probe behavior change. The queue-format boundary below is still real.
- Enabling torrent: startup validates path, port, library state, and cluster
  exclusion before serving the API as healthy. The settings UI also warns
  that the existing default `max_active_downloads = 1` is shared by Usenet
  and BitTorrent; nzbd does not silently raise it.
- Disabling with live torrents: startup refuses and names the live job count,
  unless an explicit `allow_paused_state = true` migration option is designed.
  Silently forgetting seeds is not a safe default.
- M2 writes schema 4 unconditionally once #154 lands. The exact pre-M2
  schema-3 binary rejects it before serving or writing; direct downgrade is
  unsupported. Pause/export and drain every live torrent and pending
  admission, stop the daemon, preserve payload/metainfo, restore a known-good
  schema-3 backup captured before M2, reconcile or re-import newer Usenet work,
  and only then install that binary. No schema-4-to-3 converter exists; without
  the backup, use a rollback build that still reads schema 4.
- Downgrade from schema 3 to this schema-2 binary: it reports that version 3
  is newer than supported and aborts the entire queue load before serving.
  Schema 3 is emitted for every queue snapshot, including a pure-Usenet queue,
  so one save by the newer daemon is enough to make this rollback fail even if
  BitTorrent was never enabled and no torrent row ever existed.
  Downgrade to a pre-envelope binary: its single typed `queue.json` decode
  sees unknown `JobKind::Torrent`, reports an unknown-variant error, and also
  aborts startup. Neither binary isolates or skips the torrent row; Usenet
  jobs are unavailable too. Direct downgrade after any schema-3 save is
  unsupported because the current writer cannot emit schema 2 and no in-place
  converter exists. The supported old-binary procedure is to pause/export and
  drain every live torrent, stop the daemon, preserve payload/metainfo, restore
  a known-good schema-2 backup captured before upgrade, reconcile or re-import
  newer Usenet work, and only then install the old binary. Without that backup,
  use a rollback build that still reads schema 3. This procedure is mandatory;
  no migration may silently drop live jobs. The version fallback added in M1a
  makes future-version failures clearer in new binaries but cannot change
  already released old binaries.

### 17.3 Rollback

If the torrent backend is unhealthy but the daemon is running, pause all
torrent jobs without pausing Usenet, export retained metainfo, and keep
payloads. A rollback release must still understand and display torrent records
even if it cannot run them; removing the code that parses persisted jobs is a
data-loss change, not a feature toggle.

---

## 18. Risks and mitigations

| Risk | Impact | Mitigation / decision gate |
|---|---|---|
| `librqbit` lacks a required control or platform | Feature cannot meet nzbd’s contract | M0 gates before production architecture; `libtorrent` is the named fallback. |
| Stable library lacks BEP 52 | Some modern torrents reject | Honest v1 scope and named rejection; revisit on proven stable support. |
| Stable library is TCP/IPv4 and private torrents use one tracker | Some peers/networks and tracker failover are unavailable | Publish the exact v1 matrix, verify primary-tracker private downloads, revisit only on a stable 9.x gate. |
| Maintained live-peer budgets regress | Tracker/PEX/DHT discovery can exceed configured resource expectations | Pin 80 per torrent and 400 per session in adapter options; exact boundary/release tests and the derivation drift check fail on guard removal. |
| Retained known-peer records regress | Tracker/DHT/PEX or repeated incoming addresses can grow queued, backoff, dead, and not-needed state outside live-peer permits | Pin 1,024 per torrent and 4,096 per session; hold permits for the record lifetime and run exact refusal/release tests. |
| Pending incoming handshakes regress | Remote clients can retain sockets, buffers, and tasks before live-peer permits apply | Keep the maintained 256 pre-routing ceiling and its exact boundary test separate from live-peer limits. |
| Tracker response or interval is hostile | Memory/request tasks remain unbounded or a tracker is hammered | Maintain the request deadline, streamed 1 MiB body cap, and 60-second minimum unforced interval; tracker-count preflight alone is insufficient. |
| Established peer floods piece or metadata requests | The upload scheduler and peer writer retain unbounded response records behind rate limits or a slow socket | Maintain the 128-request per-peer window spanning scheduler and writer, non-blocking admission, and over-window disconnect policy. |
| DHT or magnet discovery outruns its consumer | Datagram, recursive-node, maintenance, delivered-peer, metadata-future, or candidate work retains without a fixed bound before peer admission | Maintain the reviewed stable queue/worker/candidate ceilings and best-effort overload policy. Keep current-main LSD lifecycle changes as optional upstream material until a future engine upgrade. |
| Private tracker rejects or bans an unapproved rqbit client | Grab fails or tracker account is penalized | No qBittorrent wire-identity claim; disclose whitelist requirements and gate each supported tracker policy before M4. |
| Torrent PP corrupts seeds | Tracker hash failures and broken uploads | No torrent PP in v1; future reflink-or-copy derivative only. |
| Passkeys leak in logs/UI | Tracker account compromise | Secret classification, boundary redaction, fixtures in every output path. |
| qBittorrent shim drifts from *arr | Adds/imports fail after upgrades | Minimal surface, current-source contract table, pinned real-container tests. |
| Two persistence systems disagree | Ghost or duplicate torrents | nzbd authoritative; library persistence only accelerates explicit restore. |
| VPN/proxy claim is false | Public IP exposure | Reject proxy+DHT and proxy+UDP trackers; prove peer TCP has no direct path; still make no anonymity claim and document the network namespace/firewall boundary. |
| Unlimited seeding surprises operator | Ongoing bandwidth/storage use | Visible status/warning and explicit upload control; do not guess a potentially harmful default. |
| Automatic seed limit races import | Data removed before consumer copies it | Limit pauses only; caller performs explicit removal. |
| Torrent stats flood owner/API | Queue actions lag | Coalesced watched progress; structural bounded events; latency test. |
| Cluster failover starts two sessions | Shared payload/resume corruption | Cluster rejected until exclusive fenced lease/write tests pass. |
| Full disk affects a second root | Partial writes and stalled daemon | Multi-root probe plus observed ENOSPC latch; keep API responsive. |
| Huge hostile metainfo exhausts memory | Denial of service | Bounded fetch/parser/path/file limits before session start. |
| Delete escapes torrent root | Data loss | Persisted canonical content root, symlink/path proof, refuse on mismatch. |

---

## 19. Review disposition — maintained rqbit selected; single-node activated

The first 2026-08-05 review approved the architecture and rejected the
original engine capability claim. Fable's follow-up review found the proxy
leak boundary and plan inconsistencies described above. This revision resolves
both review passes as follows:

| Decision | Disposition |
|---|---|
| Engine | Pin the exact rqbit v8.1.1 archive plus the ordered twelve-patch maintained series, generated vendor, and integrity CI. Evaluate `libtorrent-rasterbar` only if that bounded maintenance model cannot pass M0. |
| Transport | Accept TCP/IPv4-only for the first release; uTP and IPv6 wait for a stable 9.x line and repeatable resume/interop proof. |
| Torrent format | Accept v1 `.torrent`/`btih` magnets only; the M0 adapter rejects v2-only and hybrid metainfo/magnets with distinct named errors. |
| Queue schema | Version 2 is the torrent-free envelope; M1b writes version 3 with the torrent variant and defaulted record; M2 writes version 4 for protected pending admissions, with schema-3 migration and exact old-reader rejection. |
| Compatibility | qBittorrent Web API 2.8.1 is the one supported *arr surface; do not also build Transmission RPC. |
| Post-processing | No torrent PP in v1; future PP operates on a reflink-or-copy derivative, never a hardlinked seed tree. |
| Seeding | Unlimited default; per-add/category limits pause and retain data rather than deleting it. |
| Paths | Use a separate immutable `torrent_dir`; display names never own storage. |
| Categories | Use a durable runtime overlay so Sonarr's connection test does not edit config or require restart. |
| Concurrency | Keep one shared `max_active_downloads` knob and its existing default of 1; a torrent with no progress/useful peer for 60 s yields so it cannot starve Usenet. |
| Privacy | SOCKS is supported with split/masked credentials only when DHT is off and no UDP tracker is present; peer TCP must prove no direct path. v1 still makes no interface-binding, kill-switch, VPN, or anonymity claim. |
| Private trackers | Exactly one unique tracker only in 8.1.1; backup tiers fail visibly, and tracker client-whitelist compatibility must be disclosed and tested before M4. |
| Readiness | Add authoritative snapshot fields, not a duplicate Usenet event; keep `job_pp_finished` unchanged and do not emit it for unprocessed torrents. |
| Quota | Count verified torrent payload and successfully written decoded NNTP payload; do not count upload or NNTP protocol overhead. |
| UPnP | Unavailable in the first release; a future opt-in requires a patched dependency and fresh security review. |
| Cluster | Keep torrent+cluster as a startup error. M6 still requires separate approval after single-node evidence. |

ADR-19 records the maintained engine/API resolution, and the local and native
proofs pass all eleven M0 gates. The consolidated implementation composes the
single-node session, durable queue ownership, native web/mobile surfaces, and
the narrow qBittorrent Web API projection. Cluster ownership remains rejected
at startup until M6 receives its own ADR and evidence.

The 2026-08-22 cross-project adversarial review also rejected transplanting
plurx's cluster implementation or treating nzbd's residual shared-filesystem
window as sufficient for torrents. Its findings are dispositioned in §2.4,
§12, and §15.8: use a pinned merged baseline, exclude unmerged activity work,
require same-transaction publication fencing and generation-scoped bytes,
prove stale-owner self-fencing with `SIGSTOP`/`SIGCONT`, and land a separately
reviewed storage/topology ADR before M6 implementation. No product runtime code
is authorized by this documentation change.

The final 2026-08-22 adversarial pass raised five numbered findings; all were
accepted and resolved: stale M0 statements were reconciled to the 2026-08-14
approval, the stale-owner guarantee was narrowed from impossible zero physical
I/O to authoritative-namespace exclusion, the exact PR #101 approval is cited,
lease-resource retirement and forget/re-add are now ADR decisions, and the
SwarmDeck contract opens the standard review item so the supervisor—not the
lead—selects an eligible different-model reviewer.

The subsequent M2 decomposition review raised eight more findings and blocked
dispatch until their contracts were corrected. Admission remains unmounted
until final activation; magnet resolution has a durable staged boundary before
payload start; delete/history recovery is an idempotent state machine rather
than a false cross-store atomic claim; inactive torrent paths do not affect
Usenet disk probes; existing stopped/error facts and disk roles remain the sole
owners; explicit dependency markers match the graph; and #163 owns the stated
schema-4-to-3 rollback boundary plus HTTP and mixed-protocol daemon evidence.

The final ownership/recovery re-review found two remaining gaps and blocked
dispatch again. They are resolved above: `nzbd-torrent` owns only
engine-identity handles while the daemon/backend runtime owns the JobId
association, and protected pending source artifacts make magnet/HTTP admission
resumable without leaking secrets or starting payload work before durable
validated metainfo.

One final durability pass found that a top-level pending collection could not
safely reuse schema 3 because its existing reader ignores unknown fields. M2
now bumps the queue to schema 4, migrates schema 3 forward with no pending
intents, and requires the captured schema-3 reader to reject schema 4 before
serving or rewriting it.

---

## 20. External evidence — re-check before implementation

All links were reviewed 2026-08-05. Software APIs move; M0 must re-check the
selected version and current consumer branches.

| Source | What this proposal uses it for |
|---|---|
| [`librqbit` 8.1.1 API](https://docs.rs/librqbit/8.1.1/librqbit/) | Embedded session, admission, handles, and stats |
| [`SessionOptions`](https://docs.rs/librqbit/8.1.1/librqbit/struct.SessionOptions.html) | DHT, fast resume, persistence, listen range, UPnP, SOCKS, limits |
| [`Session` construction source](https://docs.rs/librqbit/8.1.1/src/librqbit/session.rs.html#695-726) | Persistence enumerates and restores every stored torrent before construction returns |
| [`AddTorrentOptions`](https://docs.rs/librqbit/8.1.1/librqbit/struct.AddTorrentOptions.html) | Pause, file selection, output root, trackers, per-torrent limits |
| [`TorrentStats`](https://docs.rs/librqbit/8.1.1/librqbit/struct.TorrentStats.html) | Public progress, file, rate, ETA, and peer facts; no public tracker/DHT health snapshot |
| [`Limits`](https://docs.rs/librqbit/8.1.1/librqbit/limits/struct.Limits.html) | Live upload/download rate setters |
| [rqbit README at v8.1.1](https://github.com/ikatson/rqbit/blob/v8.1.1/README.md) | Stable feature claims, TCP/IPv4 scope, and sequential-only behavior |
| [rqbit README on `main`](https://github.com/ikatson/rqbit/blob/main/README.md) | 9.x uTP/dual-stack direction; not evidence for the stable pin |
| [rqbit license](https://github.com/ikatson/rqbit/blob/main/LICENSE) | Apache-2.0 compatibility |
| [qBittorrent Web API](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-4.1%29) | Endpoint methods, form fields, response units, and status vocabulary |
| [Sonarr `QBittorrentProxyV2`](https://github.com/Sonarr/Sonarr/blob/develop/src/NzbDrone.Core/Download/Clients/QBittorrent/QBittorrentProxyV2.cs) | Exact qBittorrent endpoints current Sonarr calls |
| [Sonarr `QBittorrent`](https://github.com/Sonarr/Sonarr/blob/develop/src/NzbDrone.Core/Download/Clients/QBittorrent/QBittorrent.cs) | State interpretation, import-path rule, seed-limit/removal behavior |
| [libtorrent features](https://www.libtorrent.org/features.html) | Fallback engine capability comparison |
| [Transmission RPC specification](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md) | Rejected alternative compatibility/delegation surface |

The source list is evidence for a proposal, not a dependency lockfile. The M0
report records the exact versions and commits actually tested.
