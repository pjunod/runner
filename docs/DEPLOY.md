# Deploying nzbd

Every recipe below is complete — start from a blank machine, copy the
blocks in order, end with a running daemon. Adjust paths and passwords;
nothing else should need editing.

## Directories nzbd needs

nzbd touches exactly three kinds of places, all set in `nzbd.toml`:

| Directory | Config key | What lives there |
|---|---|---|
| Working root | `paths.main_dir` | in-progress downloads, the crash-safe queue journal (`<main_dir>/queue`), history DB |
| Completed | `paths.dest_dir` | finished, post-processed jobs (per-category subdirs/overrides) |
| Config | — | `nzbd.toml` itself (+ optional extension scripts dir, watch dir) |

In containers the convention is one volume, `/data`, holding both:
`main_dir = "/data"`, `dest_dir = "/data/complete"`, with the config
directory mounted read-write at `/etc/nzbd`.

**The path-alignment rule (the one people trip on):** Sonarr/Radarr must
see finished downloads at the *same path* nzbd reports. Mount the same
host directory at the same container path in both containers — e.g.
`-v /data/usenet:/data` on nzbd *and* on Sonarr. If the paths differ,
imports fail with "path does not exist".

## Docker, by hand

**Zero-config path:** skip writing a config entirely — mount an empty
config *directory* and let the first-run setup UI create the file:

```sh
sudo mkdir -p /data/usenet /opt/nzbd/config
sudo chown -R 1000:1000 /data/usenet /opt/nzbd/config
docker run -d --name nzbd --restart unless-stopped -p 6789:6789 \
  -v /data/usenet:/data \
  -v /opt/nzbd/config:/etc/nzbd \
  ghcr.io/pjunod/nzbd:latest
# → http://localhost:6789/ shows the setup form; it writes
#   /opt/nzbd/config/nzbd.toml and restarts the daemon with it.

# Advertise the host-published API to iPhone, iPad, and Android clients.
# Host networking is required because mDNS does not cross Docker's bridge.
docker run -d --name nzbd-discovery --restart unless-stopped \
  --network host ghcr.io/pjunod/nzbd:latest \
  advertise --name "$(hostname -s)" --port 6789
```

Where the wizard's write actually lands, by deployment shape:

- **Config directory mounted (above)** — written to the host, survives
  anything. This is the recommended shape.
- **No config volume** — the write succeeds into the container's own
  filesystem layer and is destroyed when the container is recreated
  (`docker rm`, `compose up --build`, image update). This used to start
  setup over from scratch. Since 2026-07-26 it does not: every config
  write also lands a copy at `<main_dir>/queue/nzbd.toml.saved` on the
  **data** volume, and a boot that finds no config file recovers from
  that copy, restores the file and carries on configured. You get a
  warning banner in the UI and a `warn` in the log both before the first
  loss (the daemon can see that the directory is not a mount) and after
  a recovery. Fix the mount anyway — recovery is a safety net, not a
  deployment.
- **Read-only config (`:ro` bind, compose `configs:`, Kubernetes
  ConfigMap)** — the daemon can't write at all: the wizard *and* the
  Settings tab both fail their save with `Read-only file system
  (os error 30)`. The setup page detects this at boot and says so;
  fill the form anyway and use **Show config** to copy or download the
  generated `nzbd.toml`, place it yourself, and restart. (The same
  button works in every deployment if you'd rather manage the file by
  hand.)
- **File bind mount of a missing file** (`-v ./nzbd.toml:/etc/nzbd/nzbd.toml`
  before the host file exists) — Docker invents a *directory* at that
  path; nzbd refuses to start with a message explaining the fix. Mount
  the directory instead.

**"Where do I put the copied config?"** The path the setup page shows
(`/etc/nzbd/nzbd.toml`) is the path *inside the container*. The file
belongs on the **host side** of the volume mounted there — find it on
the machine running the container (the setup page prints this command
with your actual container ID filled in):

```sh
docker container inspect -f \
  '{{range .Mounts}}{{if eq .Destination "/etc/nzbd"}}{{.Source}}{{end}}{{end}}' nzbd
# → e.g. /opt/nzbd/config  →  save it as /opt/nzbd/config/nzbd.toml
docker restart nzbd
```

Use `docker container inspect` (not bare `docker inspect`): if an
*image* shares the name, bare inspect matches it and fails with
`map has no entry for key "Mounts"`. Wrong name? `docker ps` lists the
real one (compose names containers `<project>-<service>-1` unless
`container_name` is set) — or just read the `volumes:` line in your
compose file, which is the same answer.

If that prints nothing, no volume is mounted: either recreate the
container with one (recommended, see above), or copy the file straight
into the container — this survives restarts but not re-creation:

```sh
docker cp nzbd.toml nzbd:/etc/nzbd/nzbd.toml && docker restart nzbd
```

On Kubernetes with a ConfigMap, the config lives in the ConfigMap
itself: `kubectl create configmap nzbd-config --from-file=nzbd.toml
--dry-run=client -o yaml | kubectl apply -f -`, then restart the pod.
And if the wizard's save failed with *permission denied* on a mounted
volume, the mount exists but the host directory isn't writable by the
container user — `sudo chown -R 1000:1000 <host dir>` and the wizard's
own Save works.

Or fully declarative, config-first:

```sh
# 1. Host directories
sudo mkdir -p /data/usenet/complete /opt/nzbd/config
sudo chown -R 1000:1000 /data/usenet /opt/nzbd/config

# 2. Config
sudo tee /opt/nzbd/config/nzbd.toml >/dev/null <<'EOF'
[paths]
main_dir = "/data"
dest_dir = "/data/complete"

[[server]]
name = "primary"
host = "news.example.com"
port = 563
tls = true
username = "CHANGE-ME"
password = "CHANGE-ME"
connections = 20

[[category]]
name = "tv"

[[category]]
name = "movies"

[api]
bind = "0.0.0.0:6789"
password = "CHANGE-ME"
EOF

# 3. Create + start the container
docker run -d \
  --name nzbd \
  --restart unless-stopped \
  -p 6789:6789 \
  -v /data/usenet:/data \
  -v /opt/nzbd/config:/etc/nzbd \
  -e TZ=Etc/UTC \
  ghcr.io/pjunod/nzbd:latest

# 4. Verify
docker logs -f nzbd            # Ctrl-C to stop following
curl -s localhost:6789/healthz # -> ok
# Web UI: http://localhost:6789/  (user "nzbd", the [api] password)
```

The host volume is owned by the container's UID 1000; if your host dir
belongs to someone else: `sudo chown -R 1000:1000 /data/usenet`.

**Mount the directory, not the file.** Bind-mounting
`/opt/nzbd/nzbd.toml` directly is the classic trap: if the host file
doesn't exist yet Docker invents an empty *directory* in its place, and
the daemon refuses to start with a "config path is a DIRECTORY" error
(fix: `rmdir` it, write the file, recreate the container). Mounting
`/opt/nzbd/config` at `/etc/nzbd` sidesteps that entirely — a missing
config just means the first-run wizard writes one. Keep the mount
read-write, or the Settings tab can never save.

Useful lifecycle commands:

```sh
docker exec -it nzbd nzbd status --url 127.0.0.1:6789   # queue as JSON
docker cp show.nzb nzbd:/tmp/ && docker exec nzbd nzbd add /tmp/show.nzb

# Upgrade to the latest image
docker pull ghcr.io/pjunod/nzbd:latest
docker stop nzbd && docker rm nzbd
# …then re-run the `docker run` block above (state is on the volumes)

# Build the image from a checkout instead of pulling
make docker-build && docker run -d --name nzbd ... nzbd
```

Extension scripts: add `-v /opt/nzbd/scripts:/scripts:ro` and set
`post.scripts_dir = "/scripts"` in the config.

### Which build is this?

The footer of the web UI, and `version` on `/api/v1/status`, name the
running build: `0.2.0-7-g798b1a691` is seven commits past the `v0.2.0`
tag, a `-dirty` suffix means it came from an unclean tree, and the
`built` stamp beside it is the compile time.

Building the image by hand needs one extra flag, because `.dockerignore`
excludes `.git` (a build context carrying the whole history is slow) and
the binary therefore cannot work out its own commit:

```sh
docker build -t nzbd \
  --build-arg NZBD_GIT_DESCRIBE="$(git describe --tags --always --dirty --abbrev=9)" .

# Compose, same idea:
NZBD_GIT_DESCRIBE="$(git describe --tags --always --dirty --abbrev=9)" \
  docker compose up -d --build
```

`make docker-build` and `make docker` fill it in for you, and `make
version` prints what they would stamp. **A build that skips it reports
`0.2.0+unknown`** — deliberately, so an anonymous image is visible at a
glance instead of impersonating a release.

### Cutting a release

Versions come from git tags, so a release is a tag:

```sh
$EDITOR Cargo.toml                # bump [workspace.package] version
cargo check --workspace           # refresh Cargo.lock
git commit -am "release: v0.3.0"
git tag -a v0.3.0 -m "v0.3.0"
git push origin main --follow-tags
```

Tags must be `vMAJOR.MINOR.PATCH` and must match the `Cargo.toml`
version — the build script only matches `v[0-9]*`, and a tag behind the
crate version renders as `0.3.0+v0.2.0-4-gabc123def` rather than quietly
claiming the wrong release. Between tags the version carries the commit
count and hash, so it changes on **every** commit.

## Docker Compose

A ready deployment ships in
[`examples/docker-compose/`](../examples/docker-compose/) — the compose
file (with an optional Sonarr companion commented in) plus an example
config to copy:

```sh
git clone https://github.com/pjunod/nzbd.git
cd nzbd/examples/docker-compose
mkdir -p config
cp nzbd.toml.example config/nzbd.toml
$EDITOR config/nzbd.toml     # server credentials + [api] password

docker compose up -d
docker compose logs -f nzbd
curl -s localhost:6789/healthz   # -> ok
```

Edit the `volumes:` in the compose file if your downloads live somewhere
other than `/data/usenet`. `config/` is bind-mounted read-write, so the
Settings tab writes straight back to `config/nzbd.toml`; skip the `cp`
and the first-run wizard creates it instead.

The `nzbd-init` service runs as root and sets the config and data directory
owners, plus an existing regular `nzbd.toml`, to `1000:1000`. The daemon
waits for this service to finish successfully and runs unprivileged. This
also repairs directories Docker created as `root:root` when the bind sources
did not exist. Initialization does not recurse into downloads or follow a
config-file symlink. Read-only mounts or filesystems that reject `chown`
cause initialization to fail; inspect `docker compose logs nzbd-init`.

The helper shares the daemon's volume list through a YAML anchor. Change
host paths in that list so both services see the same directories. If you
maintain your own Compose file, copy both `nzbd-init` and the daemon's
`depends_on` entry from the example. Updating the image alone does not add
the helper to an existing Compose deployment.

For an existing root-owned config directory, you can repair it immediately
from the directory containing your Compose file (replace `nzbd` if your
service has another name):

```sh
docker compose run --rm --no-deps --user 0:0 --entrypoint /bin/sh nzbd -ec '
  chown -h 1000:1000 /etc/nzbd
  if [ -f /etc/nzbd/nzbd.toml ] && [ ! -L /etc/nzbd/nzbd.toml ]; then
    chown -h 1000:1000 /etc/nzbd/nzbd.toml
  fi'
```

Verify the initialization and write access with a locally built image:

```sh
make docker-build
python3 scripts/check-compose-permissions.py
```

Compose applies a config-file change on `docker compose up -d`, not on
`restart`.

## Kubernetes

Complete manifests in [`examples/kubernetes/`](../examples/kubernetes/):
namespace, config Secret, PVC, Deployment (probes, non-root, Recreate
strategy), Service, kustomization.

```sh
cd examples/kubernetes
$EDITOR secret.yaml          # put your real nzbd.toml in stringData
$EDITOR pvc.yaml             # size + storageClassName
kubectl apply -k .

kubectl -n nzbd get pods
kubectl -n nzbd port-forward svc/nzbd 6789:6789
# http://localhost:6789/ — in-cluster clients use nzbd.nzbd.svc:6789
```

The config arrives as a Secret mounted read-only, so the Settings tab is
view-and-copy there: edit `secret.yaml` and re-apply, or use **Show
config** to copy what the UI would have written.

Keep `replicas: 1` — the queue journal lives on the RWO volume. Scaling
out means nzbd clustering (next section), not more replicas; the
Kubernetes shape for that is described in the examples'
[README](../examples/kubernetes/README.md).

## systemd (bare metal)

Unit file in [`examples/systemd/nzbd.service`](../examples/systemd/nzbd.service):

```sh
sudo useradd -r -m -d /var/lib/nzbd nzbd
sudo install -m 755 nzbd /usr/local/bin/          # binary from INSTALL.md
sudo mkdir -p /etc/nzbd /data && sudo chown nzbd /data
sudo cp nzbd.toml /etc/nzbd/
sudo cp examples/systemd/nzbd.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nzbd
systemctl status nzbd && journalctl -u nzbd -f
```

The unit is hardened (`ProtectSystem=strict`); if your download dirs are
not under `/data`, extend `ReadWritePaths=` accordingly.

## Multi-node cluster (shared volume)

Design + failure matrix: [CLUSTERING.md](CLUSTERING.md). Requirements: a
shared POSIX filesystem mounted at the same path on every node (Gluster
with quorum is the reference; NFSv4/CephFS also qualify — see
CLUSTERING.md §12), and one shared secret.

```sh
# On ONE machine: mint the cluster secret, then copy it to every node
openssl rand -hex 32 | sudo tee /etc/nzbd/cluster.secret >/dev/null
sudo chmod 600 /etc/nzbd/cluster.secret
```

Each node runs the normal single-node setup (any recipe above) plus a
`[cluster]` block — identical everywhere except `node_name` and
`advertise_url`:

```toml
# node-a (10.0.0.11)
[cluster]
enabled = true
node_name = "node-a"
shared_dir = "/mnt/work"                  # the shared mount, same path everywhere
advertise_url = "http://10.0.0.11:6789"
secret_file = "/etc/nzbd/cluster.secret"
```

```toml
# node-b (10.0.0.12) — e.g. a box that should post-process but not download
[cluster]
enabled = true
node_name = "node-b"
shared_dir = "/mnt/work"
advertise_url = "http://10.0.0.12:6789"
secret_file = "/etc/nzbd/cluster.secret"
download = false          # role knobs: download / post_process /
post_process = true       # coordinator / priority — CONFIGURATION.md
```

Start the nodes in any order. Verify:

```sh
curl -s http://10.0.0.11:6789/api/v1/cluster | jq
# nodes, roles, and the current leader; run it against any node
```

Operationally: point the *arr apps at any node (each proxies to the
leader), restart nodes freely (leases expire and are adopted — nothing
already downloaded is re-fetched), and keep Gluster quorum on so the
volume itself never splits.
