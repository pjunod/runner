# Startup recovery defect — pending magnets hold the API offline

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [STARTUP_RECOVERY_BACKPRESSURE.md](../STARTUP_RECOVERY_BACKPRESSURE.md) and [STARTUP_VERIFICATION_PLAN.md](../STARTUP_VERIFICATION_PLAN.md).

**Status:** root cause confirmed on `nuc3` 2026-09-25 · immediate service
restored · review findings addressed in
[PR #236](https://github.com/pjunod/runner/pull/236) · not yet deployed

Companion to [DEPLOY.md](../DEPLOY.md) (container operation) and
[TORRENT_QUEUE_STATUS.md](TORRENT_QUEUE_STATUS.md) (torrent queue authority).
This document records one startup failure, the proposed code change, and the
checks a reviewer should require before deployment.

## Decision for review

Accept the split recovery in PR #236: restore authorized local torrent state
before serving, then retry pending magnet and URL admissions after the API
listener is bound. A source that cannot be reached may remain pending for a
later retry; it must not keep the API and Docker health check offline.

The code is committed, the focused tests pass, and required PR checks
passed on 2026-09-25. The image on `nuc3` still runs the earlier code.
Merge and deployment validation remain.

## What happened on `nuc3`

`docker compose up -d --build` built the image, created the `nzbd` container,
then reported `dependency failed to start: container nzbd is unhealthy`.
The dependent `nzbd-discovery` service requires `nzbd: service_healthy`, so
Compose left it in `Created` state. The daemon process itself was still
running.

| UTC on 2026-09-25 | Observation |
|---|---|
| 13:36:22 | `nzbd` logged its resolved data directories. |
| 13:36:33 | BitTorrent session started; two durable torrents restored. |
| 13:38:33 | Pending magnet job 1616 timed out after 120 s. |
| 13:40:33 | Pending magnet job 1617 timed out after 120 s. |
| 13:42:33 | Pending magnet job 1618 timed out; `nzbd listening` followed immediately on `0.0.0.0:6789`. |
| After 13:42:33 | The health check became green. `docker compose start nzbd-discovery` started the dependent service. Both services were verified running. |

During the wait, Docker's health probe (`nzbd status --url
127.0.0.1:6789`) returned `Connection refused (os error 111)`. The live
health configuration allows 10 s for startup, probes every 30 s, times out
each probe after 5 s, and marks the container unhealthy after 3 failures.
The three source timeouts totaled about 360 s, far beyond that allowance.

**Observed impact:** the API and web UI were unavailable until recovery
finished, and Compose did not start discovery. The three unresolved
admissions remained durable for a later retry. No data loss was observed.

## Root cause — network recovery ran before the listener

Before PR #236, [`run()`](../../crates/nzbd/src/main.rs) awaited
`TorrentAdmissionService::recover()` before it reached
`TcpListener::bind()`. That recovery method restored local descriptors,
then walked `snapshot.pending_admissions` sequentially. For each magnet it
awaited `resolve_magnet_metadata()`, which can take 120 s when metadata is
unavailable. Only after the last admission attempt did the daemon log
`nzbd listening`.

```
old startup
  restore saved torrents
       │
       ▼
  magnet 1616 (120 s) → magnet 1617 (120 s) → magnet 1618 (120 s)
       │
       ▼
  bind :6789 → health probe succeeds → discovery can start
```

The container image built successfully. This was a readiness ordering
defect, not a compiler or Docker engine failure. Lengthening the health
grace period would hide the Compose error but leave the API unavailable for
the same six minutes, with no fixed upper bound as pending jobs accumulate.

## Proposed fix — serve while remote sources retry

PR #236 separates `recover_active()` from `recover_pending()` in
[`torrent_admission.rs`](../../crates/nzbd-api/src/torrent_admission.rs).
The daemon still restores saved local torrents before accepting requests.
After binding the API listener it spawns one background pending-admission
recovery task. Shutdown signals that task to cancel, waits up to 1 s, and
aborts a straggler before stopping the torrent session. If interruption lands
after the queue owner commits a descriptor but before in-memory registry
attachment, the next boot restores it from the durable descriptor. Pending
source sidecars and queue records keep their existing durability rules; a
transient resolution failure remains pending for a later restart.

```
new startup
  restore saved torrents → bind :6789 → health probe succeeds
                               │
                               └─▶ retry pending magnets and URLs
                                    (may take minutes; API stays available)
```

**Scope:** this change moves network sourced pending admission retries. It
does not change magnet lookup deadlines, erase the three pending jobs, or
make an unreachable magnet resolve. Retries remain sequential: N unreachable
magnets can still take N × 120 s in the background, so the last one waits
for its predecessors. Bounded concurrency and a surfaced attempt count are
follow-up work. A different failure during local descriptor restore can
still stop startup; that is outside this defect.

**Concurrent removal:** the background task loads its pending snapshot
after the listener is bound. The queue owner commits only if a reservation
is still pending; `finish()` reports `MissingPending` if a user removed it.
That case now logs removal instead of claiming the job is durable. A newly
submitted admission follows the normal request path and is not in this
startup snapshot.

**Recovery errors:** a failed source read or resolution logs the job and
allows later jobs in the snapshot to run. A failed cancellation of a
deterministically rejected magnet also logs and continues. If opening the
snapshot or pending source store fails, the background task logs the error
and retries the pass after 30 s. The API stays available, but these failures
are visible only in logs; persistent storage failure still needs operator
attention.

## Evidence and acceptance

The change is isolated on branch `codex/fix-pending-torrent-startup` in
PR #236. Local verification completed on 2026-09-25:

| Check | Result | What it establishes |
|---|---|---|
| `cargo fmt --all --check` | Pass | Patch is formatted. |
| `cargo check -p nzbd --locked --offline` | Pass | Daemon and changed API compile. |
| `cargo test -p nzbd-api --locked --offline recover_resumes_a_durable_http_intent_and_reaps_orphans` | Pass | Local recovery leaves a pending source durable; a later retry completes it and reaps the orphan. |
| `cargo test -p nzbd-api --locked --offline recover_removes_a_deterministically_unusable_pending_magnet` | Pass | Existing deterministic rejection behavior remains. |
| `cargo test -p nzbd-api --locked --offline recovery_keeps_transient_magnet_failures_and_reaps_policy_failures` | Pass | A timed-out magnet remains pending; policy failures are deterministic. |
| `cargo test -p nzbd --test daemon --locked --offline pending_magnet_does_not_block_api_startup` | Pass in 2.96 s | The real daemon serves `/healthz` with a saved unresolved magnet. Restoring the old `.recover()` call makes it fail at the 15 s deadline. |
| Pre-push `cargo check --workspace --all-targets` | Pass after review changes | Workspace targets type-check. |
| Required PR checks, including Rust merge candidate and main promotion gate | Pass on 2026-09-25 | CI accepted the reviewed code; production ordering remains to be checked after deployment. |

The loopback HTTP test needs local socket permission. Its first run in the
restricted sandbox failed at socket bind with `Operation not permitted`; it
passed when rerun with that permission. That failure was environmental, not
an assertion failure.

**Deployment acceptance:** first record the pending IDs on `nuc3`; they were
`[1616, 1617, 1618]` on 2026-09-25. If they have since been removed, do not
claim the live three-job ordering check passed. After review and merge,
rebuild with those jobs still durable. The `nzbd listening` log line and a
successful health probe must appear before any 120 s magnet timeout; `docker compose up -d --build` must start `nzbd-discovery` without
the dependency error. Later timeout warnings may still appear, and those
jobs must remain pending. Check the queue and service state rather than
treating a green health probe as proof that magnet metadata resolved.

## Rollout and rollback

Use the live Compose project at `/opt/noirr/nzbd` after review. Its checkout
was on `main` at `8a501ce` on 2026-09-25; its `config/`, `nzbd.toml`, and
Compose file are untracked deployment files. Preserve them and use a
fast-forward update. Before building, tag the working image so rollback does
not depend on rebuilding an old Git tree:

```bash
cd /opt/noirr/nzbd
docker exec nzbd cat /processing/queue/queue.json \
  | python3 -c 'import json,sys; print([p["job_id"] for p in json.load(sys.stdin)["pending_admissions"]])'
docker image tag nzbd:latest nzbd:pre-startup-recovery
git pull --ff-only
docker compose up -d --build
docker compose ps
docker inspect nzbd --format '{{.State.Health.Status}}'
docker logs --tail 40 nzbd
```

**How to read it:** the first command prints pending IDs only, never source
secrets. Record the IDs before updating; an empty list cannot demonstrate
the original failure on this deployment. `nzbd` should become `healthy`
promptly after local restore, and `nzbd-discovery` should be `Up`. The API
must be reachable while pending magnet retries are still running. The exact
elapsed time for local
restore depends on the saved torrents, so the ordering of the listener and
120 s warnings is the decisive check.

If the new image fails an unrelated startup check, restore the tagged image
and recreate the services without rebuilding:

```bash
cd /opt/noirr/nzbd
docker image tag nzbd:pre-startup-recovery nzbd:latest
docker compose up -d --no-build --force-recreate
docker compose ps
```

Rollback restores the old behavior: pending magnets may again delay API
readiness and block discovery. It is a service recovery path, not a repair
for the underlying readiness defect.
