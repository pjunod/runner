# BitTorrent gate 9 review — accept measured cost, not silent risk drift

**Status:** all eleven dispositions recorded; maintained implementation and
refreshed native measurements pass; independent review complete ·
**Date:** 2026-08-07 · **Disposition and amendment recorded:** 2026-08-14 ·
**Accepted:** 2026-08-14 · **Status reconciled:** 2026-08-22 ·
**Engine:** rqbit v8.1.1 archive plus the canonical sixteen-patch maintained series ·
**Decision owner:** ADR-19 in
[BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md)

Companion to
[BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md) (the complete spike result)
— this is the shortest review surface for gate 9's resource, package, license,
and advisory decision. Review the table and §4; do not mark the gate Pass
merely because CI is green. The decision requested was whether the recorded
cost and three constrained exceptions are acceptable for the eventual first
release, and whether the missing live-peer, retained-peer, tracker-request,
pre-routing handshake, and established-peer response-backlog controls must be
resolved before this gate can pass. DHT, metadata-resolution, and current-main
LSD queues also needed explicit discovery-pressure boundaries.

That decision has been taken: §4.1 records the accepted disposition for all
eleven items. ADR-19 now selects the maintained v8.1.1 series that implements
items 6–11. The immutable archive checksum, exact patch membership/order,
clean application, byte-identical vendor, and focused behavior tests pass. The
[2026-08-14 maintained-engine run](https://github.com/pjunod/nzbd/actions/runs/31837867629)
also refreshes the measurements on all five native targets.

The 2026-09-20 amendment adds patch `0020`, which exposes a constructor only
under the engine's `test-support` feature so nzbd can inject loopback DHT
bootstrap nodes. It adds no production option, socket, discovery source, or
resource budget. The prior measurements remain historical evidence. The
amended eleven-patch derivation, vendor drift check, focused upstream suites,
release-mode DHT dispatch proof, and complete upstream workspace check passed
locally on 2026-09-20; PR CI is the publication record for that evidence.

The 2026-09-22 amendment adds patch `0021`. It adds two embedder options (the
session HTTP `User-Agent` and the BEP 10 `v` string) and changes tracker
announce content and timing. Each torrent's tracker session polls its own
statistics every 5 s until the download is complete, then every 30 s (the
statistics provider now caches the torrent handle instead of scanning the
session). `completed` goes out once, promptly after a successful announce;
failed announces back off from 60 s to 30 minutes instead of retrying every
minute. When a tracker session ends, one `stopped` per tracker that was sent
`started` is spawned, bounded to 2 s; a restarted torrent waits for it before
its next `started`, and `Session::stop` waits up to 3 s for these before
cancelling the session's network tasks. Magnet peer lookups send no events.
It adds no dependency, feature, listener, or discovery source. Under the
renewal rule the twelve-patch derivation, vendor drift check, focused upstream
suites (including the new tracker lifecycle, backoff, UDP and identity
proofs), release-mode DHT dispatch proof, and upstream workspace check passed
locally on 2026-09-22; PR CI is the publication record.

The 2026-11-13 security renewal is recorded in
[ADR-19 §4.3.4](BITTORRENT_PROPOSAL.md#434-security-maintenance-renewal--2026-11-13).
The immutable archive and resource/admission boundaries remain unchanged.
Patch 0024 removes unmaintained generic retry/clock and optional hash
libraries while preserving retry timing, BitTorrent SHA-1 outputs and the
production AWS-LC selection. Native timing/hash/wire proofs are added to M0;
its resource, storage, discovery and fuzz gates remain required. The subsequent [serialization renewal](BINCODE_COMPATIBILITY.md) adds patch 0025
and replaces real bincode consumers with maintained forks using independent
legacy byte/error, mixed-version and recovery proofs. Unused provenance
snapshots and the deliberate legacy validation oracle stay explicit. Current
source derivation, scoped graph audits and protected checks renew the evidence;
historical measurements above remain historical, rather than being relabeled.

No production BitTorrent path is enabled by this review or by its recorded
disposition. Gate 8 uses the maintained selective-restore option. Gate 7
requires operational facts and explicit `unknown` discovery diagnostics, not
a detailed upstream tracker/DHT API or a health percentage.

## 1. Decision requested — measured costs, exact exceptions, and resource gaps

The isolated M0 adapter measured the following in one macOS 26.6 arm64 run:

| Measurement | Recorded result | Reviewer question |
|---|---:|---|
| Unstripped optimized `m0_idle` harness | 10,111,360 bytes (9.64 MiB) | Is this binary delta acceptable for the single-binary release? |
| Maximum resident set for one idle session | 8,814,592 bytes (8.41 MiB) | Is this idle memory cost acceptable before real swarm load? |
| Sampled growth for 100 initialized dormant torrents | 2,863,104–5,144,576 bytes across five native targets | Are the preliminary 32 MiB guard and sampled-growth method acceptable until active-swarm tests exist? |
| Sampled growth for 100,000-file preflight | 3,768,320–27,295,744 bytes across five native targets | Are the preliminary 64 MiB guard and sampled-growth method acceptable until concurrent hostile-input tests exist? |
| Normal `nzbd-torrent` dependency closure | 222 package/version identities | Is the maintenance and audit surface acceptable? |
| New workspace lockfile identities | 178 package/version identities | Is the dependency expansion proportionate to not implementing BitTorrent ourselves? |

These are spike measurements, not permanent limits. The complete M0 rerun
measured the maintained dependency on every native target, and M2 must later
measure the final daemon when it is linked. A
reviewer can accept this preliminary cost without claiming the eventual daemon
has the same size or resident set. The exact cross-platform baselines, maxima,
growth, and timings are in the
[M0 report](BITTORRENT_M0_REPORT.md#4-measurements-and-dependency-review),
backed by the
[successful maintained-engine run on 2026-08-14 UTC](https://github.com/pjunod/nzbd/actions/runs/31837867629).

The proposed `max_peers_per_torrent = 80` and `max_peers_total = 400` are not
implemented by stable 8.1.1. Stable hard-codes 128 live peer permits per
torrent and exposes no shared session cap. The adapter's 80-entry guard applies
only to explicit/resolved bootstrap peers; later tracker, PEX, or DHT discovery
can fill all 128 engine slots. The pinned rqbit-main snapshot exposes a
per-torrent `peer_limit`, but still no session-total budget. Preliminary
binary/dependency acceptance therefore cannot authorize the advertised 80/400
runtime contract. The contribution kit proves a candidate per-torrent default,
per-add override, and shared session semaphore on both source lines. Incoming
and outgoing peer managers hold both applicable permits through release, and
two-torrent boundary tests exercise the aggregate ceiling. Until equivalent
Maintained patch `0007` now enforces both budgets, and the adapter pins 80/400
explicitly. Production networking remains disabled.

The live-peer ceiling also does not bound retained peer records. Stable 8.1.1
and current main insert every unique tracker, DHT, PEX, explicit, or incoming
address into a map and unbounded peer-adder channel before a live permit is
acquired. Queued, backoff, dead, and not-needed records can therefore grow
without the 80/400 live-manager budget. A contribution candidate adds
caller-selected per-torrent and shared-session record permits, holds them for
the map lifetime, removes incoming-only records when their manager exits, and
prevents alternate-address reconnects from being queued repeatedly. The
proposal's preliminary 1,024/4,096 policy is informed by an exact-8.1.1 macOS
arm64 measurement: the 296-byte `Peer` struct makes 4,096 raw records
1,212,416 bytes (1.16 MiB), excluding map, allocator, and live-bitfield
overhead. Maintained patch `0010` now enforces the accepted 1,024/4,096 values,
and the refreshed native measurements pass.

The tracker-count preflight is also not a tracker-request budget. Stable 8.1.1
and the pinned rqbit-main snapshot issue HTTP tracker requests without a
tracker-owned deadline, buffer the full decoded response, and accept a
tracker-provided zero-second HTTP announce interval; UDP accepts five seconds.
The contribution kit proves a candidate 30-second complete-request deadline,
1 MiB streamed response cap, and 60-second minimum unforced HTTP/UDP interval
on both source lines. Maintained patch `0005` provides those stable-line
controls; preliminary resource acceptance still does not authorize tracker
networking.

The live-peer permits also start after an incoming client has completed enough
of its handshake to be routed to a torrent. Stable 8.1.1 accepts every TCP
socket into an unbounded pending-check set, where each incomplete client can
retain a socket, buffers, and a task for the 10-second handshake read timeout.
Current rqbit main has a configurable per-listener ceiling of 256 pending
checks. The contribution kit carries a stable-only 256-check backport and a
verifier for main's native boundary. This is a distinct pre-routing resource
budget: neither the 80/400 live-peer candidate nor the fixed timeout closes an
unbounded concurrency set. Maintained patch `0009` provides the accepted
stable 256-check boundary; production listener activation remains
unauthorized.

Established live-peer work has another independent gap. Valid BEP 3 piece
requests enter an unbounded upload-scheduler channel and then an unbounded
per-peer writer; valid BEP 9 metadata requests enter the writer directly. A
rate limit or socket timeout slows draining but does not cap the number of
queued records. The contribution kit proves one candidate 128-permit response
window on both source lines and advertises it as BEP 10 `reqq`. Admission is
non-blocking: a peer that exceeds the advertised outstanding-response window
is disconnected rather than parking its socket reader behind torrent-global
upload throttling. The permit follows a piece response across both queues and
a metadata response through the writer, and is released only after the socket
write completes or is cancelled. Production-path admission and blocked-write
tests fail under intentional guard/lifetime bypasses. Maintained patch `0012`
provides the accepted window and overload policy; established peer traffic
remains unauthorized.

Discovery has its own retained-work chain before a peer becomes live. Stable
8.1.1 and the pinned rqbit-main snapshot use unbounded channels for outgoing
DHT datagrams, recursive nodes, and delivered peers, and can grow the active
recursive future set without a fixed ceiling. Magnet metadata resolution uses
a 128-permit semaphore around I/O but can retain an unbounded future queue and
unique-address set before those permits. DHT maintenance uses unbounded
refresher/pinger channels and active future sets. Bootstrap starts every
configured hostname, but that finite configuration fan-out is intentionally
unchanged: an eight-host window can let one hostname's 24-hour retry budget
starve all later entries. Current main adds an unbounded LSD result channel
whose periodic announce task survives result-stream drop. The
contribution kit now proves candidate 256-record DHT send, recursive-node,
delivered-peer, DHT-maintenance, and LSD queues; 32 active recursive and 32
active maintenance requests per worker; 128 active metadata attempts; 256
pending metadata candidates; and a 4,096-entry metadata deduplication set that
does not terminate discovery. It also
cancels LSD work with its owning stream, protects replacement registrations,
and removes an existing duplicate DHT request per recursive step. Maintained
patch `0014` provides the stable-line DHT and metadata boundaries. The
current-main LSD lifecycle work remains optional contribution material because
v8.1.1 does not have that result stream.

The normal closure is permissively licensed except for exact
`option-ext 0.2.0`, which is MPL-2.0 file-level copyleft. The checked-in
`deny.toml` accepts only that package/version under MPL-2.0; another MPL package
or version fails the blocking policy job.

## 2. Advisory scope — fixed parser, no RustSec exceptions

As of 2026-11-13, maintained patch
[`0023-upgrade-upnp-xml-parser.patch`](../contrib/rqbit/0023-upgrade-upnp-xml-parser.patch)
requires fixed `quick-xml 0.41` in the UPnP helper. The former exceptions for
RUSTSEC-2026-0194 and RUSTSEC-2026-0195 are removed from
[`deny.toml`](../deny.toml) and the audit invocation. The older
RUSTSEC-2026-0009 `time` exception was removed on 2026-09-19.

[`scripts/check-reviewed-dependency-exceptions.sh`](../scripts/check-reviewed-dependency-exceptions.sh)
rejects older affected XML releases, any RustSec ignore, and drift in the
exact MPL-2.0 `option-ext 0.2.0` dependency path. Workspace versions are
normalized so release bumps do not masquerade as dependency changes.

UPnP remains disabled. Fixing the compiled dependency does not authorize
router changes or change the accepted production capability boundary. The
recorded August dispositions below remain historical review evidence.

## 3. Reproduction — inspect the same evidence CI enforces

Run the exact dependency and exception checks from the repository root:

```bash
scripts/check-bittorrent-deps.sh                    # Exact rqbit/Rust-TLS graph; no OpenSSL.
scripts/check-rqbit-maintained-patch-series.sh       # Exact archive, series, vendor, and proofs.
scripts/check-reviewed-dependency-exceptions.sh    # Exact exception package and feature sets.
cargo test -p nzbd-torrent --lib                    # Includes the no-UPnP construction invariant.
cargo deny --all-features --locked check bans licenses sources
cargo deny --all-features --locked check advisories

# Against clean rqbit v8.1.1 and current-main trees, respectively.
scripts/check-rqbit-tracker-request-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-session-peer-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-pending-handshake-budget.sh /path/to/rqbit
scripts/check-rqbit-known-peer-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-peer-response-budget-patch.sh /path/to/rqbit
scripts/check-rqbit-discovery-pressure-patch.sh /path/to/rqbit
```

Reproduce the macOS harness measurements rather than comparing a debug binary:

```bash
cargo build --release -p nzbd-torrent --example m0_idle
wc -c target/release/examples/m0_idle
/usr/bin/time -l target/release/examples/m0_idle
```

`maximum resident set size` is bytes on macOS. Linux reports different units
and allocator behavior, so cross-platform numbers are useful context but not
byte-for-byte acceptance comparisons.

## 4. Reviewer disposition — accept, reject, or require another boundary

Gate 9 can pass only if the recorded dispositions and their implementation
evidence all hold:

1. **Binary and idle-memory cost.** The one-sample 9.64 MiB harness and
   8.41 MiB idle RSS are acceptable preliminary costs, subject to
   final-daemon remeasurement.
2. **Dependency and license cost.** The 222-package closure, 178 new lockfile
   identities, and exact MPL-2.0 exception are acceptable.
3. **UPnP restriction.** Compiling the affected `quick-xml` is acceptable only
   because the first release cannot enable UPnP and CI guards that boundary.
4. **`time` restriction.** Resolved: the Rust 1.95 clustering upgrade permits
   fixed `time 0.3.51`; no exception remains.
5. **Renewal rule.** Any engine, feature, advisory, dependency-path, or MSRV
   change reopens this decision; a previous green run is not a waiver.
6. **Runtime peer budgets.** The maintained engine must enforce the proposed
   80/400 peer budgets or the proposal must be explicitly amended with new
   measured and reviewed limits.
7. **Tracker request budgets.** The maintained engine must bound the
   complete HTTP tracker request, decoded response body, and hostile unforced
   HTTP/UDP announce intervals. The prepared 30-second, 1 MiB, and 60-second
   candidate values are evidence for review, not shipped capability.
8. **Pending incoming handshakes.** The maintained engine must bound
   incomplete pre-routing handshake work separately from routed live peers.
   The prepared stable 256-check backport and main's native per-listener
   boundary are evidence for review, not shipped capability. A reviewer must
   explicitly accept the preliminary value and the TCP-only first-release
   scope or require a different measured ceiling.
9. **Retained peer records.** The maintained engine must separately bound
   queued, backoff, dead, and not-needed peer records per torrent and across
   the session. The prepared permit candidate and 296-byte raw-struct
   measurement are evidence for review, not shipped capability. A reviewer
   must accept or revise the preliminary 1,024/4,096 limits and require
   additional target-specific memory evidence if the raw measurement is not
   sufficient.
10. **Established-peer response backlog.** The maintained engine must
    bound remote-triggered piece and metadata responses across the upload
    scheduler and socket writer. The prepared 128-permit candidate and its
    failing negative control are evidence for review, not shipped capability.
    A reviewer must accept or revise the advertised window and over-window
    disconnect behavior.
11. **Discovery-pressure budgets.** The maintained engine must bound DHT
    datagrams, recursive work, delivered peers, and retained metadata
    candidates. Any stable line that includes LSD must also tie its bounded
    result stream and announce task to one lifecycle. The prepared
    256-record queue, 32-request worker, 128-active/256-pending metadata, and
    4,096-entry deduplication limits, UDP overload policy, exact-boundary proofs, and
    failing negative controls are evidence for review, not shipped capability.
    A reviewer must accept or revise each boundary.

If any item is rejected, gate 9 remains Partial and the remedy must be named:
upgrade or patch the dependency, remove the capability, change engines, or set
an accepted resource budget. Silence is not acceptance.

### 4.1 Recorded disposition — accepted 2026-08-14

The decision owner accepted the recommended package for all eleven §4 items,
with no exceptions, on
[issue #83](https://github.com/pjunod/nzbd/issues/83#issuecomment-5287959702)
(reply `APPROVE RECOMMENDED DEFAULTS`, 2026-08-14T00:30:03Z). This section is
the disposition of record; §4 remains the statement of what was asked.

| # | Disposition | Recorded scope of the acceptance |
|---:|---|---|
| 1 | **Accepted provisionally** | The one-sample 9.64 MiB harness, 8.41 MiB idle RSS, and the preliminary 32 MiB / 64 MiB sampled-growth guards are acceptable prototype costs. The final daemon must be remeasured on every supported native target; a materially larger result reopens this item. |
| 2 | **Accepted as locked** | The 222-package normal closure, 178 new lockfile identities, and exactly one MPL-2.0 package (`option-ext 0.2.0`) are acceptable. CI must continue to reject dependency and license drift. |
| 3 | **Accepted as a capability restriction** | Compiling `quick-xml 0.37.5` is acceptable only while UPnP cannot be enabled and CI proves that boundary. The first release ships without UPnP; enabling it requires a patched or replaced dependency and a new review. |
| 4 | **Accepted; resolved 2026-09-19** | The compiler-floor upgrade selected fixed `time 0.3.51`; CI removed the obsolete exception and its feature-path allowance. |
| 5 | **Accepted** | Renewal is automatic: any engine, dependency-path, feature, advisory, MSRV, or relevant policy change reopens this decision. A previously green run is not a waiver. |
| 6 | **Accepted as a required boundary** | A stable engine limit of **80 live peers per torrent** and **400 across the nzbd session** is required. Stable 8.1.1's hard-coded 128-per-torrent-only behavior is explicitly not accepted. Production stays disabled until both limits ship and are tested. |
| 7 | **Accepted as a required boundary** | A **30-second** complete tracker-request deadline, **1 MiB** decoded response cap, and **60-second** minimum unforced HTTP/UDP reannounce interval are required before tracker networking is authorized. |
| 8 | **Accepted as a required boundary** | A **256-connection** pending-handshake ceiling is required for the first TCP-only release, as a budget distinct from live-peer permits. Excess incoming work waits rather than growing without bound. |
| 9 | **Accepted as a required boundary** | **1,024** retained peer records per torrent and **4,096** per session are required, covering queued, backoff, dead, and not-needed records. The 296-byte raw-struct measurement is not sufficient on its own: final per-platform memory measurements are required before release and may revise these values. |
| 10 | **Accepted as a required boundary** | A **128-permit** advertised response window per established peer is required, with over-window peers disconnected rather than parked behind torrent-global upload throttling. |
| 11 | **Accepted as a required boundary** | The prepared conservative discovery bounds are required: 256-record DHT send, recursive-node, delivered-peer, maintenance, and LSD queues; 32 active recursive and 32 active maintenance requests per worker; 128 active and 256 pending metadata attempts; and a 4,096-entry deduplication set that does not terminate discovery. Excess datagrams and work are dropped or backpressured, and LSD work is cancelled with its owning stream. Stable engine support and the failing negative controls are still required. |

What the disposition does **not** do:

- It does not make old upstream evidence sufficient by itself. Items 6–11 are
  enforced by the maintained stable series and must keep their exact
  revert-sensitive tests green.
- It does not enable any production BitTorrent configuration, listener,
  tracker, DHT, admission, payload I/O, or UI path.
- It does not make detailed tracker/DHT state a required public fact. Missing
  detail remains `unknown`, never inferred from peers.
- It does not give rqbit authority over the durable queue. Automatic restore is
  disabled; nzbd selects every restore.
- It does not accept an unexplained private fork. The immutable upstream input,
  exact ordered patch set, generated vendor, and CI drift proof are the
  reviewed maintained dependency.

Gate 9 passes because the maintained engine enforces every applicable boundary
in this table, the combined proof is green, and the documented native matrix
has refreshed the measurements. Independent review approved the exact
maintained-engine head on 2026-08-14; the final daemon must still be remeasured
during M2. Under item 5, any input change reopens this disposition rather than
inheriting it.

## 5. Non-goals — this review cannot authorize M2

- It does not wire rqbit persistence into the daemon. The maintained API makes
  selective restore possible, but M2 must integrate it with the durable queue.
- It does not expose detailed tracker or DHT health. Gate 7 reports the
  diagnostic as `unknown` while preserving required facts.
- It does not add torrent configuration, admission, listeners, trackers, DHT,
  payload I/O, qBittorrent compatibility, or UI.
- It does not treat the 80-entry bootstrap-input guard as a live-peer resource
  cap. The adapter separately pins the maintained engine's 80/400 runtime
  limits.
- It does not treat the 64-tracker input cap as a request lifetime, body, or
  announce-rate budget.
- It does not treat the 10-second read timeout or live-peer permits as a limit
  on the number of incomplete incoming handshake tasks.
- It does not treat the 80/400 live-manager limits as caps on retained queued,
  backoff, dead, or not-needed peer records.
- It does not treat upload rate limiting or a socket timeout as a bound on
  queued piece and metadata responses from an established peer.
- It does not treat live-peer or retained-peer permits as bounds on DHT, LSD,
  or magnet-metadata discovery queues and candidate sets.
- It does not submit any prepared patch upstream.

Gate 9 is one required decision among eleven, not permission to skip native M0
evidence, independent review, or the separate M2 implementation.
