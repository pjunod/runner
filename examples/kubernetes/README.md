# nzbd on Kubernetes

A minimal, production-shaped single-node deployment: one replica, config
from a Secret (it contains news-server credentials), downloads on a PVC,
and local exec probes against nzbd's bootstrap monitor.

```sh
# 1. Edit secret.yaml — put your real nzbd.toml in it
# 2. Size the PVC in pvc.yaml for your storage class
kubectl apply -k .
kubectl -n nzbd port-forward svc/nzbd 6789:6789   # then open http://localhost:6789/
```

Files:

| File | Purpose |
|---|---|
| `namespace.yaml` | the `nzbd` namespace |
| `secret.yaml` | the full `nzbd.toml` (Secret, not ConfigMap — it holds server passwords) |
| `pvc.yaml` | `/data` volume for downloads + state |
| `deployment.yaml` | the daemon: probes, resources, non-root security context |
| `service.yaml` | ClusterIP on 6789 |
| `kustomization.yaml` | ties it together (`kubectl apply -k .`) |

## Probe behavior and image rollouts

The startup and liveness probes run `nzbd health` inside the container.
They check the loopback monitor on `127.0.0.1:6790`, which reports whether
the control service can answer health requests while config, queue, or
payload recovery continues. Readiness runs `nzbd health --ready`; it
removes the Pod from Service routing until the configured secure public
control listener responds. A ready Pod may still report queue loading or
storage trouble in application status. These component states do not make
the UI unreachable.

The startup probe checks every 5 seconds with a 3-second timeout and allows
6 failures (about 30 seconds). Liveness and readiness each have an explicit
3-second timeout; the CLI bounds its own request to 2 seconds. The Pod has
30 seconds for graceful termination.

The probe command is part of the image interface. Roll out this manifest
with an image that contains `nzbd health`; rolling the manifest back also
requires rolling the image back to a version with the matching interface.
An older binary exits on the unknown `health` command, and Kubernetes will
restart it. Pin an image version for controlled rollouts, check that the
target image supports the probe commands before applying, and keep the
previous image reference with the manifest rollback. Do not apply this
manifest first and update the image later.

If the monitor uses a custom port, set `NZBD_MONITOR_BIND` on the container
so both the daemon and its exec probes resolve the same address. A CLI
override given only to the daemon is not inherited by probe processes. The
environment value must be a numeric loopback address; the monitor is
plaintext and exposes only nonsecret health summaries. A bind error such as
a port conflict prevents startup with a clear failure before storage I/O.

Notes:

- **One replica.** nzbd keeps its queue journal under `paths.main_dir`;
  two pods over the same RWO volume would fight. Scale *out* with nzbd's
  own clustering instead (below), not with `replicas: 2`.
- Point Sonarr/Radarr (in-cluster) at `nzbd.nzbd.svc:6789` as an NZBGet
  client. For outside access add an Ingress/LoadBalancer in front of the
  Service — and set an `[api] password` first.
- The *arr apps must see the same paths as nzbd to import finished
  downloads: mount this PVC (or the same underlying storage) into them at
  the same mount point (`/data`).

## Multi-node nzbd cluster on Kubernetes

nzbd clustering needs a shared **RWX POSIX volume** (CephFS, NFS,
GlusterFS — see `docs/CLUSTERING.md` §12 for what the volume must
guarantee). The shape: one Deployment **per nzbd node**, each with

- the shared RWX PVC mounted at the same path (e.g. `/mnt/work`),
- its own `nzbd.toml` differing only in `node_name` (unique + stable) and
  `advertise_url` (its own Service DNS name),
- the same `[cluster] secret` (one shared Secret),
- its own ClusterIP Service, so peers can reach it by name.

Any node's Service can be the target for the *arr apps — every node
proxies to the current leader. `deployment.yaml` has commented markers at
the two places that change per node.
