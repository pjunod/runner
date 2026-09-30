# Dev container

Build the image from the working tree and run it — for exercising the
*container* (packaging, PP tools, paths, UI) rather than the engine; the
faster inner loop for engine work is `cargo run -p nzbd -- run`.

```sh
cd dev
docker compose up --build          # foreground with logs; Ctrl-C stops
# → open http://localhost:6789/ — with no config present, the first-run
#   setup UI appears and writes config/nzbd.toml for you.
# Prefer a file? mkdir -p config; cp nzbd.toml.example config/nzbd.toml
# before `up`.
```

The `nzbd-init` service sets the config and data directory owners to
`1000:1000` before the daemon starts, including when Docker creates those
directories as root. It also fixes ownership of an existing `nzbd.toml`.
It does not recursively change downloaded files. The daemon runs as
UID/GID `1000:1000`; an initialization failure prevents startup.

The loop:

```sh
docker compose up --build -d       # rebuild + restart after a code change
docker compose watch               # …or let Compose rebuild on save (v2.22+)
docker compose logs -f
docker compose exec nzbd nzbd status --url 127.0.0.1:6789

# Queue something: the watch dir is wired to ./data/nzb
mkdir -p data/nzb && cp ~/some.nzb data/nzb/

docker compose down                # stop (data/ persists)
rm -rf data                       # full reset
```

Web UI: <http://localhost:6789/> (no auth in the dev config).

Notes:

- Downloads and state land in `dev/data/` on the host (bind mount) —
  inspect them directly. `dev/data/` and `dev/config/` are gitignored.
- The repo's `.dockerignore` keeps `target/` out of the build context;
  the image build compiles from scratch inside Docker (release profile),
  so expect the first build to take a few minutes and rebuilds to reuse
  cargo's layer only when dependencies didn't change.
- The production-shaped deployment examples live in
  `examples/docker-compose/` — this folder is only for hacking.
