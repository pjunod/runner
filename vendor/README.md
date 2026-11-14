# Vendored coordination dependencies — provenance and update boundary

Companion to [CLUSTERING.md](../docs/CLUSTERING.md) (runtime behavior) and
[CLUSTERING_COMPLETION_PLAN.md](../docs/CLUSTERING_COMPLETION_PLAN.md) (the
decision that introduced this source) — this file records why these crates are
in-tree and how to refresh them without losing the maintained fixes.

| Directory | Source | Revision | License |
|---|---|---|---|
| `hiqlite/` | `pjunod/plurx/vendor/hiqlite` | `8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176` | Apache-2.0 |
| `hiqlite-wal/` | `pjunod/plurx/vendor/hiqlite-wal` | `8663d6c0e7d4a8242cd1b6c6d385cecd72c7d176` | Apache-2.0 |

The pair is copied together because plurx maintains transport flush, terminal
writer ownership, bounded snapshot recovery, and WAL auto-repair as one tested
durability contract. Updating only one directory can compile while changing
recovery behavior, so every refresh records one source revision and reviews
both directory diffs. nzbd uses the SQL/Raft store only for bounded cluster
control metadata; article bodies and post-processing payloads remain on the
configured shared data volume.

The recorded source has one local transport fix in `hiqlite/src/network/api.rs`:
established API streams observe their Raft group's terminal metrics and close
when that group stops. Without it, an upgraded connection can outlive the
listener and keep returning stopped-leader errors to surviving voters.
Preserve this fix during refresh until it is present in the maintained source.
The standalone `api_stream_stop_watch_ignores_live_updates_and_observes_terminal_states`
test covers live updates, shutdown, fatal stop, and a closed metrics channel.

The recorded source also carries one local dependency-security delta in
`hiqlite/Cargo.toml`: `rusqlite` 0.40 → 0.41 and `serde_rusqlite` 0.43 → 0.44,
so the bundled `libsqlite3-sys` is ≥ 0.39 (RUSTSEC-2026-0459). rusqlite 0.41's
`Statement::columns()` returns an iterator, so the three
`mapping_cols_from_stmt(stmt.columns())` call sites collect it into the `Vec`
the helper takes; behavior is unchanged. plurx's maintained copy still pins
0.40 at the time of writing — carry this delta forward on refresh until the
maintained source makes the same bump.

Do not edit the vendored source to add nzbd application behavior. Put adapter
logic in `crates/nzbd-cluster`; keeping the dependency delta identical to its
maintained source makes later provenance and security review finite.

As of 2026-11-13, the standalone `hiqlite/Cargo.lock` used by CI's terminal
stream regression also selects fixed crossbeam-epoch 0.9.20, h2 0.4.16,
rustix 1.1.6, rustls 0.23.46, rustls-webpki 0.103.17, Tokio 1.53.3 and
webpki-root-certs 1.0.10. The WAL lock independently selects fixed rustix and
Tokio. These compatible lockfile deltas preserve the source/API and licenses.
The product uses the root workspace lockfile; the standalone CI feature tree
is `--no-default-features --features sqlite`. Old optional upstream tooling
entries in these snapshots are not evidence that those features are shipped.
