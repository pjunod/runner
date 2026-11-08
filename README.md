# runner

[![Tests](https://github.com/pjunod/nzbd/actions/workflows/ci.yml/badge.svg)](https://github.com/pjunod/nzbd/actions/workflows/ci.yml)
[![Lint](https://github.com/pjunod/nzbd/actions/workflows/lint.yml/badge.svg)](https://github.com/pjunod/nzbd/actions/workflows/lint.yml)
[![Supply chain](https://github.com/pjunod/nzbd/actions/workflows/supply-chain.yml/badge.svg)](https://github.com/pjunod/nzbd/actions/workflows/supply-chain.yml)
[![Coverage](https://raw.githubusercontent.com/pjunod/nzbd/badges/coverage.svg)](https://github.com/pjunod/nzbd/actions/workflows/coverage.yml)
[![Test count](https://raw.githubusercontent.com/pjunod/nzbd/badges/tests.svg)](https://github.com/pjunod/nzbd/actions/workflows/ci.yml)

A ground-up Rust reimplementation of the [NZBGet](https://nzbget.com) Usenet
downloader — modern architecture, same soul: tiny footprint, line-rate
throughput, direct-to-disk writing, and drop-in compatibility with the
Sonarr/Radarr ecosystem and NZBGet's post-processing script protocol.
Optionally runs as a **multi-node cluster** over a shared work volume.

See [file lifecycle operations](docs/FILE_LIFECYCLE_OPERATIONS.md) for retention,
recovery mounts, cancellation and backup/restore, and
[INTEGRATION.md](docs/INTEGRATION.md) for the seam with curator and cinema
(pipeline contract v2).

## Highlights

- **Drop-in for the *arr apps** — Sonarr/Radarr/Lidarr connect to it as an
  "NZBGet" download client, unchanged: JSON-RPC 1.1 dialect (`append`,
  `history`, `editqueue`, the `*Lo/*Hi/*MB` triplets), XML-RPC with
  `system.multicall`, and NZBGet's extension-script protocol byte-for-byte.
- **Fast, careful engine** — async single-owner queue, per-server connection
  pools with NNTP pipelining (plus AIMD adaptive depth), rustls TLS, the
  NZBGet server-failover ladder (tiers/groups/fill servers), DirectWrite
  disk assembly, crash-safe journal with kill-9 resume, token-bucket rate
  limiting, daily/monthly quotas and low-disk guards.
- **Post-processing, natively verified** — par2 quick-verify uses the CRCs
  gathered *during download* (an intact set is proven with zero data
  re-reads), subprocess repair only when needed, hardened unrar/7z
  extraction, cleanup, NZBGet extension scripts.
- **Three-layer deobfuscation** — par2 16k-hash renames, archive-signature
  renames, then a final job-name pass (SABnzbd-style dominant-file rule,
  plus numbered season packs, which SABnzbd skips). Evidence always beats
  heuristics; every rename is logged and recorded in history.
- **A handoff you can watch, not infer** — the event stream reports every
  post-processing stage and a completion carrying `final_dir`, so a
  consumer learns where the files landed instead of polling history and
  guessing. Frames are numbered and resumable (`Last-Event-ID`), a gap
  too big to replay says so rather than looking contiguous, and
  `GET /api/v1/history?since_seq=N` reconstructs anything the stream
  dropped. Tag a download with your own id at add time and grep for it
  across the whole pipeline. runner makes no outbound connections: push is
  an optimization over the poll, never a replacement.
- **RSS/Atom feeds** with the NZBGet filter language
  (`Accept`/`Reject`/`Require`, wildcards, size/age windows, per-rule
  category/priority/dupe options).
- **Embedded web UI** at `/` — live queue, history, live log tail, speed
  controls, rate sparkline, per-provider chips, three opt-in layouts, ten
  color schemes with independent light/dark appearance, first-run
  setup wizard. Rendered in place from a 1 Hz SSE stream, so clicking,
  selecting and scrolling survive updates; every action applies instantly
  and reverts — loudly — if the daemon disagrees; delete is one click
  with an 8-second Undo and **no confirmation dialogs anywhere**. One
  self-contained page, zero build toolchain — and an **installable PWA**
  on phones, with built-in HTTPS (`[api] tls = true` self-generates a
  persistent certificate) to make browsers treat it as a secure origin.
- **Native mobile remote** in [`mobile/`](mobile/) — one TypeScript client
  for iPhone, iPad, and Android: live queue state, whole-queue and per-job
  controls, provider status, secure saved credentials, and `.nzb` submission
  through the system document picker. Classic preserves its shipped layout;
  Plex and Theater change the native navigation, and the same ten color
  schemes remain independent from Auto/Light/Dark appearance. It finds nearby
  daemons through local DNS-SD, uses the native API directly rather than a web
  view, and keeps manual addresses available; [docs/MOBILE.md](docs/MOBILE.md)
  covers display choices, builds, and LAN/TLS rules.
- **Clustering** — nodes sharing a POSIX volume (Gluster is the reference)
  elect a leader, distribute downloads and post-processing with
  anti-affinity, partition provider connection budgets, and fail over
  automatically without re-fetching. No extra services: the volume is the
  coordinator. Design: [docs/CLUSTERING.md](docs/CLUSTERING.md).

## Quickstart

```sh
# Docker — mount the config DIRECTORY read-write (empty is fine: the
# first-run wizard writes nzbd.toml into it)
mkdir -p config && sudo chown -R 1000:1000 config
docker run -d --name nzbd -p 6789:6789 \
  -v /data/usenet:/data \
  -v $PWD/config:/etc/nzbd \
  ghcr.io/pjunod/nzbd:latest

# …or a release binary / source build (see docs/INSTALL.md)
nzbd run --config nzbd.toml
```

Minimal `nzbd.toml`:

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
```

Usenet downloads process under `main_dir` unless a nonempty `inter_dir` is
configured. Successful post-processing publishes to `dest_dir` or the category
destination; existing jobs keep their recorded processing paths.

Then open `http://localhost:6789/` for the UI, or point Sonarr/Radarr at
host `localhost`, port `6789`, client type **NZBGet**.

Migrating? `nzbd import-config /path/to/nzbget.conf` converts an existing
NZBGet configuration and prints a mapping report.

## Documentation

Every document is indexed in [docs/README.md](docs/README.md); the ones most
people need:

| Doc | What it covers |
|---|---|
| [docs/INSTALL.md](docs/INSTALL.md) | Release binaries, Docker, Homebrew, building from source, musl static builds |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | The complete annotated `nzbd.toml` reference |
| [docs/USAGE.md](docs/USAGE.md) | CLI, web UI, connecting the *arr apps, RSS feeds + filter language, extension scripts, deobfuscation |
| [docs/DEPLOY.md](docs/DEPLOY.md) | systemd, Docker Compose, Kubernetes, multi-node cluster deployment |
| [docs/MOBILE.md](docs/MOBILE.md) | Building the iPhone/iPad/Android app, connecting it to runner, and its exact control/security boundaries |
| [docs/INTEGRATION.md](docs/INTEGRATION.md) | The seam with curator and cinema: job API, events, history cursor, publications, disposal |
| [docs/FILE_LIFECYCLE_OPERATIONS.md](docs/FILE_LIFECYCLE_OPERATIONS.md) | What happens to payload bytes: retention, parking, recovery |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design: the whole system, phase by phase, and the ADR summary |
| [docs/CLUSTERING.md](docs/CLUSTERING.md) | Cluster design (ADR-13…16), failure matrix, operations |
| [docs/BITTORRENT_RELEASE_REVIEW.md](docs/BITTORRENT_RELEASE_REVIEW.md) | BitTorrent pre-release operations review: current no-go and the sign-off conditions |
| [STATUS.md](STATUS.md) | What's done, what's next, with commit evidence |

Deployable examples live under [`examples/`](examples/):
[`docker-compose/`](examples/docker-compose/) (compose file + example
config), [`kubernetes/`](examples/kubernetes/) (full manifest set) and
[`systemd/`](examples/systemd/) (hardened unit file).

## Development

Three GitHub Actions workflows gate every push/PR — **Tests** (unit +
engine e2e + multi-node cluster tests + the whole-daemon test, plus an
MSRV 1.95 check), **Lint** (`cargo fmt --check`, `clippy -D warnings`),
and **Coverage** (`cargo llvm-cov`, self-hosted badges). Coverage runs every
workspace test target even after a failure, then fails the job before reports
or badges can be published unless the whole instrumented suite passed. It also
rejects line-coverage regressions below the single `MINIMUM_LINE_COVERAGE`
constant in [`.github/workflows/coverage.yml`](.github/workflows/coverage.yml);
raise that value only after a complete `main` run proves the new floor.

The `Makefile` wraps the whole workflow — `make` (or `make help`) lists
every target:

```sh
make setup    # one-shot: toolchain (+ MSRV + llvm-cov), par2/7z, git hooks
make run      # build + run the daemon (first-run setup UI on :6789)
make check    # everything CI enforces: fmt + clippy + tests + MSRV
make test     # the workspace suite; `make coverage` for line coverage
```

`make setup` installs the post-processing tools (`par2`, `7z`) the tests
exercise and wires the committed git hooks (pre-commit: fmt check;
pre-push: `clippy -D warnings` + `cargo test`). Tests that need those
tools self-skip with a notice when they're missing; CI installs them and
sets `NZBD_REQUIRE_TOOLS=1` so a skip there is a failure (`make
test-strict` reproduces that locally).

To hack on the *container* rather than the engine, [`dev/`](dev/) has a
compose file that builds the image locally from the Dockerfile
(`cd dev && docker compose up --build`) — see [`dev/README.md`](dev/README.md).

## License

MIT OR Apache-2.0. Written from scratch against a behavioral spec
([docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §3); no NZBGet (GPL) code is
ported.
