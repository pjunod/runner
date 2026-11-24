# rqbit maintained engine — reproducible v8.1.1 boundary for nzbd

**Status:** active single-node BitTorrent engine · **Upstream base:**
rqbit v8.1.1, commit `00b97485160ff5b5aa2b379ea0815d568ec665f0` ·
**Stable patch delta:** exactly fifteen ordered patches ·
**Production daemon:** starts it only when `[torrent].enabled = true`

Companion to
[BITTORRENT_PROPOSAL.md](../../docs/BITTORRENT_PROPOSAL.md) (the decision),
[BITTORRENT_M0_REPORT.md](../../docs/BITTORRENT_M0_REPORT.md) (the gate evidence),
and [BITTORRENT_GATE9_REVIEW.md](../../docs/BITTORRENT_GATE9_REVIEW.md)
(accepted limits and renewal rules).

This directory has two distinct jobs:

1. define the exact rqbit source derivation used by nzbd's single-node adapter;
2. retain optional upstream contribution material without silently adding it
   to that dependency.

The first job is release-blocking. The second is useful maintenance work, but
upstream acceptance is not a condition of the selected M0 engine.

## 1. Maintained dependency contract

The dependency is derived from three reviewed inputs:

- the checksum-pinned rqbit `v8.1.1` source archive;
- its SHA-256 in [`upstream-v8.1.1.sha256`](upstream-v8.1.1.sha256); and
- the exact order in [`maintained-series.txt`](maintained-series.txt).

The maintained series is:

| Order | Patch | Contract |
|---:|---|---|
| 1 | `0001-allow-persistence-without-auto-restore.patch` | Keep persistence available while disabling implicit admission, so nzbd selects every restore. |
| 2 | `0005-bound-tracker-requests.patch` | Cap decoded responses at 1 MiB, complete requests within 30 seconds, and clamp unforced announces to at least 60 seconds. |
| 3 | `0007-bound-session-peers.patch` | Enforce 80 live peers per torrent and 400 across the session. |
| 4 | `0009-bound-pending-incoming-handshakes.patch` | Bound pre-routing TCP handshake checks at 256. |
| 5 | `0010-bound-known-peer-records.patch` | Enforce 1,024 retained peer records per torrent and 4,096 across the session. |
| 6 | `0012-bound-peer-response-backlog.patch` | Bound each established peer to 128 queued piece/metadata responses through socket write. |
| 7 | `0014-bound-discovery-pressure.patch` | Bound stable-line DHT and magnet-metadata queues, active work, and retained candidates. |
| 8 | `0016-limit-peer-metadata-before-allocation.patch` | Enforce the adapter's 10 MiB BEP 9 ceiling before allocation or requests. |
| 9 | `0018-propagate-file-sizing-errors.patch` | Stop initialization on the first selected-file sizing failure and preserve useful error context. |
| 10 | `0019-disable-peer-exchange.patch` | Make the daemon's PEX setting authoritative for inbound messages and outgoing advertisement. |
| 11 | `0020-expose-test-only-dht-bootstrap.patch` | Expose a feature-gated constructor for deterministic loopback DHT tests and a typed invalid-resolved-metadata marker without adding an nzbd runtime bootstrap setting. |
| 12 | `0021-configurable-client-identity-and-announce-lifecycle.patch` | Let the embedder set the tracker HTTP `User-Agent` and BEP 10 `v`, and make announces tracker-compliant: keep the tracker URL's own query (passkeys), send a stable `key`, echo `tracker id`, report session-relative `downloaded`, send `completed` once and promptly, back off failed announces, and send a bounded `stopped` when the torrent's tracker session ends (ordered before a restart's `started`; awaited by `Session::stop`). Peer lookups send no events. |
| 13 | `0022-read-only-piece-verifier.patch` | Reuse the existing piece verifier against an FD-bound read-only snapshot before native processing copies; verify boundary bytes and avoid rescanning preceding files for every piece. |
| 14 | `0023-upgrade-upnp-xml-parser.patch` | Require fixed XML parsing in both UPnP crates without enabling UPnP; lock compatible security fixes for maintained upstream proofs. |

| 15 | `0024-replace-unmaintained-retry-and-sha1-backends.patch` | Replace generic unmaintained retry/clock dependencies with owned peer/bootstrap policies preserving symmetric jitter, caps, reset and elapsed deadlines; replace the optional legacy hash backend with RustCrypto SHA-1 while keeping the historical feature interface and production AWS-LC backend. |

The checked-in [`vendor/`](vendor/) tree is generated from those inputs. It
contains the derived upstream `LICENSE`, `README.md`, and `crates/` tree needed
by the workspace path dependency. Do not edit it directly. Change the source
patch, rerun the derivation check, and replace the generated result together.

Changing the archive, checksum, patch membership, patch order, generated
vendor, engine features, or a reviewed dependency path reopens ADR-19 and the
gate-9 disposition. A broad semver update is not permitted.

## 2. Why this remains rqbit, not libtorrent

The stable series applies cleanly to one immutable source archive and has
focused tests at every changed behavior boundary. It preserves
the Rust/Tokio single-binary architecture and adds no FFI or second daemon.
That stays within ADR-19's small-maintainable-fix branch, so the heavier
`libtorrent-rasterbar` C++/FFI and packaging fallback is not triggered.

The fallback must be reconsidered if this series stops being small,
reproducible, portable, independently reviewable, or economical to keep current.
Do not hide growing engine maintenance by weakening the drift check.

## 3. Reproduction and drift proof

Run the complete check from the repository root:

```sh
scripts/check-rqbit-maintained-patch-series.sh
```

The script:

1. checks exact series membership and order;
2. reuses a checksum-verified archive from the existing build cache, downloads
   it once when absent, or uses `RQBIT_UPSTREAM_ARCHIVE`;
3. verifies the SHA-256 before extraction;
4. applies each patch with Git's conflict and whitespace checks;
5. compares the derived `LICENSE`, `README.md`, and `crates/` tree byte for
   byte with `vendor/`;
6. requires every named behavior proof to exist;
7. runs the affected `librqbit`, tracker, and DHT suites; and
8. checks the derived upstream workspace, excluding the desktop package.

By default the script caches the verified archive under
`SWARM_BUILD_CACHE_DIR` when the launcher provides it, then `CARGO_TARGET_DIR`,
then the ordinary workspace `target/` directory. Set `RQBIT_UPSTREAM_CACHE` to
choose another reusable cache path. A matching cached archive makes later
policy-gate runs network-independent; a mismatched cache fails closed instead
of silently replacing reviewed source bytes.

For a cold offline checkout, provide the reviewed archive explicitly:

```sh
RQBIT_UPSTREAM_ARCHIVE=/path/to/rqbit-v8.1.1.tar.gz \
  RQBIT_SERIES_DERIVE_ONLY=1 \
  scripts/check-rqbit-maintained-patch-series.sh
```

If GitHub regenerates its tag archive with different bytes, the download fails
the checked-in checksum. Keep the current vendor and patches unchanged, obtain
the previously reviewed archive from a trusted cache, and pass it through
`RQBIT_UPSTREAM_ARCHIVE`. Changing the checksum is an ADR-19 and gate-9 review,
not a recovery shortcut.

CI runs the complete maintained library proof with Rust 1.95 and `--locked`. The
ordinary workspace resolves `librqbit` through
`contrib/rqbit/vendor/crates/librqbit`, keeps default features off, and enables
`rust-tls`. [`check-bittorrent-deps.sh`](../../scripts/check-bittorrent-deps.sh)
checks the active daemon's maintained engine, Rust TLS, and narrow qBittorrent
shim. The upstream lock also fixes the selected proof graph; optional upstream
CLI and desktop snapshots are not the shipped daemon dependency graph.
The compatibility check excludes the unshipped `rqbit` CLI and desktop; it
checks the remaining library workspace without running the stock CLI web UI's
`npm install` build hook. The stock CLI retains `parse_duration`'s
RUSTSEC-2021-0041 in operator-supplied argument parsing. The default-TLS library
compatibility graph also retains the `crypto-hash` unmaintained notice. Neither
package appears in the three executed maintained test graphs or the shipped
daemon. These findings are reported, not suppressed.

Every maintained behavior has a test that fails when its guard, permit
lifetime, propagation, or admission boundary is reverted. A patch applying is
not sufficient evidence.

## 4. Optional upstream material

The following files are not part of `maintained-series.txt` and therefore do
not affect the nzbd dependency:

- `0003-expose-per-torrent-discovery-health.patch` and its `0004` main variant;
- every even-numbered/current-main counterpart (`0002`, `0006`, `0008`,
  `0011`, `0013`, `0015`, `0017`, and
  `0019-propagate-file-sizing-errors-main.patch`); and
- the issue/PR drafts next to those patches.

Detailed tracker/DHT health is intentionally optional. The M0 public contract
requires transfer phase, verified progress, completion, peer counts, admission
facts, rates/ETA inputs, and a bounded credential-safe last error. When the
engine cannot supply detailed discovery diagnostics, nzbd reports `unknown`.
It never infers health from peer availability and never publishes a synthetic
torrent-health percentage.

Current-main variants are compatibility and possible upstream contribution
material only. Submit them from fresh human-reviewed branches, follow rqbit's
contribution policy, and reconcile upstream feedback without changing the
checked stable dependency implicitly.

## 5. Ownership and non-goals

The maintained restore model is nzbd-authoritative selective restore:

1. load and validate nzbd's durable queue;
2. construct rqbit with automatic restore disabled;
3. explicitly restore only the selected durable records; and
4. use rqbit resume state only as an accelerator for those records.

A full hash recheck is a safe degradation path, not the normal ownership model.
rqbit's private persistence schema is not copied into nzbd, and auto-restoring
then removing ghost jobs is not accepted.

This directory owns the derived engine boundary, not operator policy. Runtime
configuration, admission, lifecycle, storage-fault routing, native clients and
qBittorrent compatibility are implemented in the surrounding nzbd crates and
documented in the project BitTorrent proposal and configuration guide.

## Dependency maintenance boundaries

The owned `librqbit-core::retry::RetryPolicy` has two fixed policies, with
10-second initial delays, factors 6 (peer) and 1.5 (bootstrap), base caps of
one hour and one minute, symmetric 50% jitter, and a 24-hour elapsed-time
budget that includes the proposed sleep. Reset restarts both the interval
and elapsed budget. The project maintains this small policy directly;
backoff/instant are removed rather than renamed or suppressed. Upstream's
backon migration changes jitter and elapsed-time accounting, so it cannot
be imported unchanged. Arithmetic attribution remains in the source.

The historical `sha1-crypto-hash` feature selects maintained RustCrypto
`sha1`; its SHA-1 output remains mandated by BitTorrent v1. Native Linux,
macOS and Windows M0 jobs run the retry regressions and known SHA-1 vectors
with both this backend and AWS-LC. The daemon continues to select AWS-LC.

The unused bincode 2 declaration in librqbit is removed from the manifest and
derived lockfile. Its library/all-target compilation and full tests pass
without that dependency, and the fuzz graph no longer includes bincode 2.
This does not replace a codec: peer serialization still uses bincode 1, and
the product's Hiqlite persistence still uses bincode 2.

Bincode 1.3.3 and 2.0.1 still carry RustSec's unmaintained notice. They encode
peer protocol data and persisted Raft/WAL/state records using distinct fixed
and variable integer configurations. The retired upstream package has no
patched release; bincode 3.0.0 is a compile-error retirement marker. Community
[bincode-next](https://github.com/Apich-Organization/bincode) and
[bincode_reloaded](https://github.com/butlergroup/bincode_reloaded) 3.x forks
exist. The former adds default SIMD and new formats/configurations; the latter
changes Serde/error/derive dependencies. Neither is an independently validated
replacement for this project's persisted and peer data. Wincode likewise
promises compatibility only for matching schemas/configurations. A replacement
must prove byte-for-byte encoding, decoding of existing records, malformed-input
behavior and cross-version recovery; any differing format needs a migration.
This patch preserves those bytes and keeps the notices visible instead of
renaming a codec to silence them. Optional stock rqbit CLI/UI and unselected
Hiqlite feature graphs remain provenance snapshots, separately inventoried
from the shipped product and maintained compatibility-test graphs. Optional
contribution probes still test separately fetched upstream sources; their CLI
compile checks retain every other upstream default integration feature (including
TLS/Postgres and current-main Prometheus) while disabling the unrelated
stock web UI/npm build hook. These probes do not establish a clean audit of
every upstream feature or replace the maintained engine proofs.
