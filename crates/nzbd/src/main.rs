//! nzbd daemon binary, phase 1: boots the download engine, serves the
//! native API and the compat shim, and offers a small control CLI
//! (`add`, `status`) that talks to a running daemon over the native API.

use clap::{Parser, Subcommand};

mod discovery;
mod tls;
use nzbd_engine::{Engine, EngineConfig, Tuning};
use nzbd_types::CertLevel;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "nzbd",
    version,
    about = "Usenet downloader daemon (NZBGet reimplemented in Rust)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Quarantine file operations after restoring a backup. Stop the daemon first.
    ArtifactsRestore {
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Snapshot lifecycle inventory and identities. Stop the daemon first.
    ArtifactsBackup {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Run the daemon.
    Run {
        /// Path to nzbd.toml (defaults are used if absent).
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Override the listen address, e.g. 0.0.0.0:6789.
        #[arg(short, long)]
        bind: Option<String>,
    },
    /// Add an NZB file to a running daemon.
    Add {
        /// Path to the .nzb file.
        file: PathBuf,
        /// Daemon address.
        #[arg(long, default_value = "127.0.0.1:6789")]
        url: String,
        /// Job name (defaults to the file name).
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        category: Option<String>,
        #[arg(long, default_value_t = 0)]
        priority: i32,
    },
    /// Show queue status of a running daemon.
    Status {
        #[arg(long, default_value = "127.0.0.1:6789")]
        url: String,
    },
    /// Advertise a host-published nzbd API over DNS-SD/mDNS.
    ///
    /// Run this companion on the host network when the daemon itself is in
    /// a bridged container whose multicast traffic cannot reach the LAN.
    Advertise {
        /// API port published on the host.
        #[arg(long, default_value_t = 6789)]
        port: u16,
        /// LAN-visible node name, e.g. nuc3.
        #[arg(long)]
        name: Option<String>,
        /// Advertise an HTTPS endpoint instead of HTTP.
        #[arg(long)]
        tls: bool,
        /// Authentication metadata only; no credential is advertised.
        #[arg(long, default_value = "unknown")]
        auth: String,
    },
    /// Import an nzbget.conf into nzbd.toml with a mapping report.
    ImportConfig {
        /// Path to the nzbget.conf to import.
        path: PathBuf,
        /// Where to write the converted config.
        #[arg(short, long, default_value = "nzbd.toml")]
        out: PathBuf,
    },
}

fn main() -> anyhow_lite::Result<()> {
    // Must precede every rustls client/server construction. The workspace can
    // compile both aws-lc and ring through independent dependencies, and
    // rustls 0.23 intentionally will not choose between them implicitly.
    tls::install_process_crypto_provider()?;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    // The daemon log ring backs `GET /api/v1/logs` and the compat `log`
    // method; the fmt layer keeps stderr behavior unchanged.
    let logbuf = nzbd_api::LogBuffer::new(1000);
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(nzbd_api::LogBufferLayer(logbuf.clone()))
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::ArtifactsBackup { state_dir, output } => {
            let inventory = nzbd_state::artifacts::Inventory::open(&state_dir)
                .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
            inventory
                .backup(&output)
                .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
            println!("Lifecycle backup written to {}", output.display());
            Ok(())
        }
        Command::ArtifactsRestore { state_dir } => {
            let inventory = nzbd_state::artifacts::Inventory::open(&state_dir)
                .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
            inventory
                .quarantine_restore()
                .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
            println!("Restored inventory quarantined. Review all held payloads before resuming retention.");
            Ok(())
        }
        Command::Run { config, bind } => loop {
            match run(config.clone(), bind.clone(), logbuf.clone())? {
                RunOutcome::Exit => break Ok(()),
                RunOutcome::Reload => {
                    tracing::info!("restarting with the new configuration");
                }
            }
        },
        Command::Add {
            file,
            url,
            name,
            category,
            priority,
        } => client_add(file, url, name, category, priority),
        Command::Status { url } => client_status(url),
        Command::Advertise {
            port,
            name,
            tls,
            auth,
        } => advertise(port, name, tls, auth),
        Command::ImportConfig { path, out } => {
            let content = std::fs::read_to_string(&path)?;
            match nzbd_config::import_nzbget_conf(&content) {
                Ok((cfg, report)) => {
                    let toml_text = nzbd_config::to_toml(&cfg)
                        .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
                    std::fs::write(&out, toml_text)?;
                    println!("wrote {}", out.display());
                    println!(
                        "mapped {} options, skipped {} (recognized), {} unknown",
                        report.mapped.len(),
                        report.skipped.len(),
                        report.unknown.len()
                    );
                    for w in &report.warnings {
                        println!("warning: {w}");
                    }
                    if !report.unknown.is_empty() {
                        println!("review by hand: {}", report.unknown.join(", "));
                    }
                    Ok(())
                }
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }
    }
}

fn advertise(port: u16, name: Option<String>, tls: bool, auth: String) -> anyhow_lite::Result<()> {
    let advertiser =
        discovery::Advertiser::start_standalone(port, name.as_deref(), tls, auth.trim())
            .ok_or_else(|| anyhow_lite::Error::msg("could not start local API advertisement"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(shutdown_signal());
    drop(advertiser);
    Ok(())
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

/// Map `[post]` config onto the PP manager's runtime config.
fn post_config(
    cfg: &nzbd_config::Config,
    slots: usize,
    stats: Option<Arc<nzbd_types::metrics::PpStageStats>>,
) -> nzbd_post::manager::PostConfig {
    nzbd_post::manager::PostConfig {
        completed_dir: Some(cfg.dest_dir()),
        par2_cmd: cfg.post.par2_cmd.clone(),
        unrar_cmd: cfg.post.unrar_cmd.clone(),
        sevenzip_cmd: cfg.post.sevenzip_cmd.clone(),
        scripts_dir: cfg
            .post
            .scripts_dir
            .as_ref()
            .map(|p| nzbd_config::expand_home(p)),
        unpack: cfg.post.unpack,
        cleanup: cfg.post.cleanup,
        deobfuscate_final: cfg.post.deobfuscate_final,
        failure_action: nzbd_post::manager::FailureAction::parse(&cfg.post.failure_action),
        failed_dir: Some(
            cfg.post
                .failed_dir
                .clone()
                .map(|p| nzbd_config::expand_home(&p))
                .unwrap_or_else(|| nzbd_config::expand_home(&cfg.paths.main_dir).join("failed")),
        ),
        slots,
        tool_timeout: Duration::from_secs(cfg.post.tool_timeout_secs.max(1)),
        script_timeout: Duration::from_secs(cfg.post.script_timeout_secs.max(1)),
        par_fetch_timeout: Duration::from_secs(cfg.post.par_fetch_timeout_secs.max(1)),
        categories: category_rules(cfg),
        stats,
    }
}

/// `[[category]]` blocks as post-processing rules.
///
/// These keys were parsed and advertised to compat clients as
/// `CategoryN.*` long before anything applied them: an operator who set
/// `dest_dir` got files in the global destination and an *arr that
/// path-mapped off the advertised value found nothing there. Same values,
/// one source, now actually used — see `compat_options`, which projects
/// the identical set.
fn category_rules(cfg: &nzbd_config::Config) -> Vec<nzbd_post::manager::CategoryRule> {
    cfg.categories
        .iter()
        .map(|c| nzbd_post::manager::CategoryRule {
            name: c.name.clone(),
            dest_dir: c.dest_dir.as_ref().map(|p| nzbd_config::expand_home(p)),
            unpack: c.unpack,
            extensions: c.extensions.clone(),
        })
        .collect()
}

/// The engine and API derive their storage inventory from the same config
/// method. That keeps a newly configured write root from appearing on the
/// dashboard while silently escaping the enforcing low-disk guard.
fn disk_guard_roots(cfg: &nzbd_config::Config) -> Vec<nzbd_engine::volumes::DiskGuardRoot> {
    cfg.storage_roots()
        .into_iter()
        .map(|root| nzbd_engine::volumes::DiskGuardRoot {
            label: root.label,
            path: root.path,
        })
        .collect()
}

fn torrent_payload_roots(cfg: &nzbd_config::Config) -> Vec<PathBuf> {
    let mut roots = vec![cfg.torrent_dir()];
    roots.extend(
        cfg.categories
            .iter()
            .filter_map(|category| category.torrent_dir.as_ref())
            .map(|path| nzbd_config::expand_home(path)),
    );
    roots.dedup();
    roots
}

/// Map daemon queue settings onto the engine's runtime units and bounds.
fn engine_tuning(cfg: &nzbd_config::Config) -> Tuning {
    Tuning {
        article_retries: cfg.queue.article_retries,
        retry_interval: Duration::from_secs(cfg.queue.retry_interval_secs),
        article_timeout: Duration::from_secs(cfg.queue.article_timeout_secs),
        propagation_delay: Duration::from_secs(cfg.queue.propagation_delay_mins as u64 * 60),
        min_free_disk_bytes: cfg.queue.min_free_disk_mb * 1024 * 1024,
        daily_quota_bytes: cfg.queue.daily_quota_mb * 1024 * 1024,
        monthly_quota_bytes: cfg.queue.monthly_quota_mb * 1024 * 1024,
        quota_start_day: cfg.queue.quota_start_day.clamp(1, 28),
        health_abort: nzbd_post::manager::FailureAction::parse(&cfg.post.failure_action)
            != nzbd_post::manager::FailureAction::None,
        ..Tuning::default()
    }
}

/// Resolve and bound the daemon's cluster settings before any cluster task is
/// started. Returning the shared path alongside the runtime config keeps every
/// cluster subsystem on the same expanded volume root.
fn cluster_runtime_config(
    cfg: &nzbd_config::Config,
) -> anyhow_lite::Result<(nzbd_cluster::ClusterConfig, PathBuf)> {
    let c = &cfg.cluster;
    let secret = c
        .resolve_secret()
        .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
    let shared_dir =
        nzbd_config::expand_home(c.shared_dir.as_ref().expect("validated: shared_dir set"));
    let default_control_dir = cfg.paths.main_dir.join(".nzbd-control").join(&c.node_name);
    let control_dir =
        nzbd_config::expand_home(c.control_dir.as_ref().unwrap_or(&default_control_dir));
    let runtime = nzbd_cluster::ClusterConfig {
        cluster_id: c.cluster_id.clone(),
        node_name: c.node_name.clone(),
        shared_dir: shared_dir.clone(),
        advertise_url: c.advertise_url.clone(),
        secret,
        coordinator: c.coordinator,
        priority: c.priority,
        download: c.download,
        max_download_jobs: c.max_download_jobs,
        post_process: c.post_process,
        pp_slots: c.pp_slots.max(1),
        lease_interval: Duration::from_secs(c.lease_interval_secs.max(1)),
        takeover_after: Duration::from_secs(c.takeover_after_secs.max(2)),
        worker_ttl: Duration::from_secs(c.worker_ttl_secs.max(3)),
        control_dir,
        control_node_id: c.control_node_id,
        control_raft_bind: c.control_raft_bind.clone(),
        control_api_bind: c.control_api_bind.clone(),
        control_peers: c
            .control_peers
            .iter()
            .map(|peer| nzbd_cluster::ControlPeer {
                id: peer.id,
                raft_addr: peer.raft_addr.clone(),
                api_addr: peer.api_addr.clone(),
            })
            .collect(),
        download_weight: c.download_weight.max(1),
        pp_weight: c.pp_weight.max(1),
        disk_guard_roots: disk_guard_roots(cfg),
        torrent_payload_roots: torrent_payload_roots(cfg),
    };
    Ok((runtime, shared_dir))
}

/// NZBGet-style option projection for the compat shim's `config` method
/// (*arr clients read categories and paths from here).
fn compat_options(cfg: &nzbd_config::Config, bind: &str) -> Vec<(String, String)> {
    let port = bind.rsplit(':').next().unwrap_or("6789").to_string();
    let mut o = vec![
        ("Version".into(), cfg.api.compat_version.clone()),
        ("ControlPort".into(), port),
        ("ControlIP".into(), "0.0.0.0".into()),
        (
            "MainDir".into(),
            nzbd_config::expand_home(&cfg.paths.main_dir)
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "DestDir".into(),
            cfg.dest_dir().to_string_lossy().into_owned(),
        ),
        (
            "InterDir".into(),
            cfg.paths
                .inter_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        (
            "NzbDir".into(),
            cfg.paths
                .nzb_watch_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        (
            "ScriptDir".into(),
            cfg.post
                .scripts_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        (
            "Unpack".into(),
            if cfg.post.unpack { "yes" } else { "no" }.into(),
        ),
        ("PostStrategy".into(), cfg.post.strategy.clone()),
    ];
    for (i, c) in cfg.categories.iter().enumerate() {
        let n = i + 1;
        o.push((format!("Category{n}.Name"), c.name.clone()));
        o.push((
            format!("Category{n}.DestDir"),
            c.dest_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ));
        o.push((
            format!("Category{n}.Unpack"),
            if c.unpack.unwrap_or(cfg.post.unpack) {
                "yes"
            } else {
                "no"
            }
            .into(),
        ));
    }
    o
}

/// `[[feed]]` config → feed engine definitions.
fn feed_defs(cfg: &nzbd_config::Config) -> Vec<nzbd_feed::FeedDef> {
    cfg.feeds
        .iter()
        .enumerate()
        .map(|(i, f)| nzbd_feed::FeedDef {
            id: i as u32 + 1,
            name: f.name.clone(),
            url: f.url.clone(),
            interval: Duration::from_secs(f.interval_mins.max(1) * 60),
            filter: f.filter.clone(),
            category: f.category.clone(),
            priority: f.priority,
            pause: f.pause,
        })
        .collect()
}

/// The `[history]` bounds, in the form the store takes them.
fn history_retention(cfg: &nzbd_config::Config) -> nzbd_state::history::Retention {
    nzbd_state::history::Retention {
        keep_max: cfg.history.keep_max,
        keep_days: cfg.history.keep_days,
    }
}

/// Open the history store: SQLite index in a node-local dir, authoritative
/// JSONL wherever `jsonl_dir` points (shared volume in cluster mode).
fn open_history(
    local_dir: &std::path::Path,
    jsonl_dir: &std::path::Path,
    node_tag: Option<&str>,
    retention: nzbd_state::history::Retention,
    index_dir: Option<&std::path::Path>,
) -> anyhow_lite::Result<Arc<nzbd_state::history::HistoryDb>> {
    for dir in [local_dir, jsonl_dir] {
        std::fs::create_dir_all(dir).map_err(|source| {
            anyhow_lite::Error::msg(format!(
                "history db: {}",
                with_fs_hint(nzbd_state::StateError::Io {
                    op: "create directory",
                    path: dir.to_path_buf(),
                    source,
                })
            ))
        })?;
    }
    let index_dir = index_dir.map(nzbd_config::expand_home);
    let db = nzbd_state::history::HistoryDb::open_configured(
        &index_dir
            .as_deref()
            .unwrap_or(local_dir)
            .join("history.sqlite"),
        Some(jsonl_dir),
        node_tag,
        if node_tag.is_some() {
            nzbd_state::history::HistoryMode::Shared
        } else {
            nzbd_state::history::HistoryMode::LocalOnly
        },
        Some(&local_dir.join("nzbs")),
    )
    .map(Arc::new)
    .map_err(|e| anyhow_lite::Error::msg(format!("history db: {}", with_fs_hint(e))))?;
    // Trim at boot, not just when the next job finishes: lowering the
    // bound on a running install should take effect when you restart it,
    // and an install upgrading into retention for the first time has its
    // whole backlog to work through before it serves a single page.
    match db.set_retention(retention) {
        Ok(0) => {}
        Ok(n) => tracing::info!(dropped = n, "history trimmed at startup"),
        // Retention is a housekeeping bound, not a correctness one. A
        // daemon that cannot trim still has every entry it ever had.
        Err(e) => tracing::warn!(error = %with_fs_hint(e), "history retention trim failed"),
    }
    let status = db.sync_status();
    if status.placement != "local" {
        tracing::warn!(path = %status.index_path.display(), placement = status.placement,
            "history index is not verified local; configure history.index_dir on persistent local storage");
    }
    Ok(db)
}

/// Watch-dir scanner: `.nzb` files dropped into `NzbDir` are added and
/// renamed `.queued` (`.error` on a parse failure). Runs every 30 s and on
/// a `scan` RPC nudge; in cluster mode only the authority scans.
fn spawn_watch_dir(
    engine: nzbd_engine::EngineHandle,
    dir: PathBuf,
    notify: Arc<tokio::sync::Notify>,
    is_authority: Arc<dyn Fn() -> bool + Send + Sync>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                _ = notify.notified() => {}
            }
            if !is_authority() {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                let is_nzb = p
                    .extension()
                    .map(|x| x.eq_ignore_ascii_case("nzb"))
                    .unwrap_or(false);
                if !is_nzb || !p.is_file() {
                    continue;
                }
                let name = p
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let Ok(content) = std::fs::read(&p) else {
                    continue;
                };
                match engine.add_nzb(&name, &content, None, 0).await {
                    Ok(id) => {
                        tracing::info!(job = id.0, file = %p.display(), "watch dir: queued");
                        let _ = std::fs::rename(&p, p.with_extension("nzb.queued"));
                    }
                    Err(err) => {
                        tracing::warn!(file = %p.display(), error = %err, "watch dir: rejected");
                        let _ = std::fs::rename(&p, p.with_extension("nzb.error"));
                    }
                }
            }
        }
    });
}

fn spawn_torrent_watch_dir(
    service: nzbd_api::torrent_admission::TorrentAdmissionService,
    dir: PathBuf,
    cancel: tokio_util::sync::CancellationToken,
    tracker: &tokio_util::task::TaskTracker,
) {
    tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
            if let Err(error) = service.scan_watch_once(&dir).await {
                tracing::warn!(error = %error, "torrent watch scan failed");
            }
        }
    });
}

/// Resolves on SIGINT (ctrl-c) or SIGTERM — the latter is what
/// `docker stop`, tini and systemd send. Both mean the same thing:
/// finish in-flight writes, sync journals, exit clean (no unclean
/// marker, no recovery pass on next boot).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// How a `run()` pass ended: a real shutdown, or a first-run setup that
/// wrote a config and wants the daemon to come back up with it.
#[derive(PartialEq)]
enum RunOutcome {
    Exit,
    Reload,
}

/// Turn a startup failure into an operator-actionable message.
///
/// A daemon that can't write its state directory dies with EACCES and,
/// historically, no clue which path was at fault. `nzbd-state` now carries
/// the path on the error; this walks the source chain to find it and spells
/// out the fix.
fn with_fs_hint<E: std::error::Error + 'static>(e: E) -> anyhow_lite::Error {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(&e);
    while let Some(err) = cur {
        if let Some(st) = err.downcast_ref::<nzbd_state::StateError>() {
            if st.is_permission_denied() {
                if let Some(path) = st.path() {
                    return anyhow_lite::Error::msg(format!(
                        "{e}\n\
                         hint: the daemon cannot write {}. Check the owner and mode of that \
                         path and its parents (a directory created by an earlier `sudo` run \
                         or by Docker is the usual cause), or set paths.queue_dir to a \
                         directory this user owns.",
                        path.display()
                    ));
                }
            }
        }
        cur = err.source();
    }
    anyhow_lite::Error::msg(e.to_string())
}

/// Put a recovered config back at the path the daemon was pointed at.
///
/// Best-effort by design: recovery already has the bytes in hand, so a
/// read-only or root-owned config directory must not turn a successful
/// boot into a failed one — it just means the mirror stays the source of
/// truth until the mount is fixed.
fn restore_config_file(path: &std::path::Path, toml: &str) -> bool {
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return false;
        }
    }
    match std::fs::write(path, toml.as_bytes()) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "could not write the recovered config back to its file; \
                 running from the saved copy instead"
            );
            false
        }
    }
}

fn run(
    config: Option<PathBuf>,
    bind: Option<String>,
    logbuf: Arc<nzbd_api::LogBuffer>,
) -> anyhow_lite::Result<RunOutcome> {
    let mut setup_path: Option<PathBuf> = None;
    let mut recovered_from: Option<PathBuf> = None;
    let cfg = match &config {
        Some(path) => {
            // Actionable errors for the two classic container mistakes.
            if path.is_dir() {
                return Err(anyhow_lite::Error::msg(format!(
                    "config path {} is a DIRECTORY, not a file — if this is a \
                     Docker bind mount, the host file didn't exist when the \
                     container was created, so Docker made a directory in its \
                     place. Remove it on the host (rmdir), create the real \
                     config file, and recreate the container.",
                    path.display()
                )));
            }
            if !path.exists() {
                // A missing config file is not proof of a first run. In a
                // container it usually means nothing is mounted at the
                // config directory, so the config written last time went
                // into the image's writable layer and died with the
                // container — and serving the wizard here would throw away
                // a working install on every deploy. Look for the mirror
                // we keep on the data volume first.
                match nzbd_config::durable::find_mirror() {
                    Some(rec) => {
                        // Put the file back if we can: the operator's next
                        // `cat`/hand-edit should find a real config, and a
                        // config dir that IS durable self-heals for good.
                        let restored = restore_config_file(path, &rec.toml);
                        tracing::warn!(
                            path = %path.display(),
                            recovered_from = %rec.from.display(),
                            restored,
                            "config file is MISSING — recovered the last saved \
                             configuration from the data volume. The config \
                             directory did not keep what was written to it: in \
                             Docker this means no volume is mounted at it, so \
                             every container recreate loses the file. Mount one \
                             (see docs/DEPLOY.md); until then this copy is what \
                             keeps the daemon configured."
                        );
                        recovered_from = Some(rec.from);
                        rec.config
                    }
                    None => {
                        // Genuinely nothing to run: first-run setup, boot
                        // with defaults (no servers) and let the web UI
                        // write this file, then reload.
                        tracing::warn!(
                            path = %path.display(),
                            "no config file — first-run setup is live in the web UI"
                        );
                        setup_path = Some(path.clone());
                        nzbd_config::Config::default()
                    }
                }
            } else {
                let text = std::fs::read_to_string(path).map_err(|e| {
                    anyhow_lite::Error::msg(format!("cannot read config {}: {e}", path.display()))
                })?;
                nzbd_config::Config::from_toml(&text)
                    .map_err(|e| anyhow_lite::Error::msg(format!("{}: {e}", path.display())))?
            }
        }
        None => nzbd_config::Config::default(),
    };
    let bind = bind.unwrap_or_else(|| cfg.api.bind.clone());
    // Always present: in setup mode it powers the wizard; in normal runs
    // it powers the Settings tab (view/edit config + hot reload).
    let setup = Some(match setup_path {
        Some(p) => Arc::new(nzbd_api::SetupHandle::new(p, bind.clone())),
        None => Arc::new(
            nzbd_api::SetupHandle::for_running(config.clone(), bind.clone(), cfg.clone())
                .recovered_from(recovered_from),
        ),
    });
    // Say it at boot, not only when something already went wrong: a config
    // directory on the container's writable layer looks perfectly healthy
    // — saves succeed — right up until the container is recreated.
    if let Some(path) = &config {
        if nzbd_config::durable::durability(path) == nzbd_config::durable::Durability::Ephemeral {
            tracing::warn!(
                path = %path.display(),
                "the config directory is INSIDE THE CONTAINER, not on a mounted \
                 volume — saves will succeed and then be destroyed the next time \
                 this container is recreated (compose up, image pull). Mount a \
                 volume at it (see docs/DEPLOY.md). nzbd keeps a copy beside its \
                 state and restores it automatically, but fix the mount."
            );
        }
    }

    let servers = cfg.server_defs();
    for s in &servers {
        if s.cert_verification == CertLevel::None {
            tracing::warn!(
                server = %s.name,
                "TLS certificate verification is DISABLED for this server"
            );
        }
    }
    if servers.is_empty() {
        tracing::warn!(
            "no [[server]] configured — the queue will accept jobs but nothing can download"
        );
    }

    let tuning = engine_tuning(&cfg);

    // Log the *resolved* directories before touching them: `~` expansion
    // and the `<main_dir>/queue` default mean the effective paths are not
    // always obvious from the config file.
    tracing::info!(
        state_dir = %cfg.state_dir().display(),
        dest_dir = %cfg.dest_dir().display(),
        "resolved data directories"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    if cfg.cluster.enabled {
        runtime.block_on(run_cluster(cfg, servers, tuning, bind, logbuf))?;
        return Ok(RunOutcome::Exit);
    }

    let mut engine_cfg = EngineConfig::single_node(
        servers,
        cfg.state_dir(),
        cfg.download_dir(),
        tuning,
        cfg.speed_limit_bps(),
    );
    engine_cfg.disk_guard_roots = disk_guard_roots(&cfg);
    engine_cfg.torrent_payload_roots = torrent_payload_roots(&cfg);
    engine_cfg.max_active_downloads = cfg.max_active_downloads();

    runtime.block_on(async move {
        // History is a queue-terminal dependency for torrents even when
        // Usenet post-processing is disabled: payload disposition must be
        // durable before a confirmed removal can leave the active queue.
        let history_db = if cfg.post.enabled || cfg.torrent.enabled {
            let state_dir = cfg.state_dir();
            Some(open_history(
                &state_dir,
                &state_dir.join("history"),
                None,
                history_retention(&cfg),
                cfg.history.index_dir.as_deref(),
            )?)
        } else {
            None
        };
        let _history_worker = history_db.as_ref().map(|db| db.start_worker()).transpose()
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
        engine_cfg.history = history_db.clone();
        let engine = Engine::spawn(engine_cfg).await.map_err(with_fs_hint)?;
        if !cfg.torrent.enabled {
            let live_torrents = engine
                .snapshot()
                .jobs
                .iter()
                .filter(|job| job.kind == nzbd_types::JobKind::Torrent)
                .count();
            if live_torrents > 0 {
                engine.shutdown().await;
                return Err(anyhow_lite::Error::msg(format!(
                    "[torrent] is disabled but {live_torrents} live torrent queue record(s) remain; re-enable BitTorrent and explicitly remove or drain them before disabling"
                )));
            }
        }

        let torrent_cancel = tokio_util::sync::CancellationToken::new();
        let torrent_tracker = tokio_util::task::TaskTracker::new();
        let mut torrent_executor = None;
        let torrent_service = if cfg.torrent.enabled {
            let proxy = cfg.torrent.socks_proxy_url.as_ref().map(|url| {
                nzbd_torrent::TorrentProxyConfig {
                    url: url.clone(),
                    username: cfg.torrent.socks_proxy_username.clone(),
                    password: cfg.torrent.socks_proxy_password.clone(),
                }
            });
            let listen_end = cfg
                .torrent
                .listen_port
                .checked_add(1)
                .expect("validated torrent listen port");
            let session = nzbd_torrent::TorrentSession::start(
                cfg.torrent_dir(),
                nzbd_torrent::TorrentSessionConfig {
                    dht: cfg.torrent.dht,
                    pex: cfg.torrent.pex,
                    listen_port_range: Some(cfg.torrent.listen_port..listen_end),
                    proxy,
                    persistence_dir: Some(cfg.state_dir().join("torrents/session")),
                    max_peers_per_torrent: Some(cfg.torrent.max_peers_per_torrent as usize),
                    max_peers_total: Some(cfg.torrent.max_peers_total as usize),
                    max_known_peers_per_torrent: Some(
                        cfg.torrent.max_known_peers_per_torrent as usize,
                    ),
                    max_known_peers_total: Some(cfg.torrent.max_known_peers_total as usize),
                    metainfo_max_bytes: Some(cfg.torrent.metainfo_max_mib * 1024 * 1024),
                },
            )
            .await
            .map_err(|error| anyhow_lite::Error::msg(format!("torrent session: {error}")))?;

            let mut category_seed_policies = std::collections::HashMap::new();
            let mut category_payload_roots = std::collections::HashMap::new();
            for category in &cfg.categories {
                category_seed_policies.insert(
                    category.name.clone(),
                    nzbd_types::SeedPolicy {
                        stop_on_complete: category.stop_seeding_on_complete.unwrap_or(cfg.torrent.stop_seeding_on_complete),
                        ratio_limit: category.seed_ratio,
                        time_limit_secs: category.seed_minutes.map(|minutes| minutes * 60),
                    },
                );
                if let Some(root) = &category.torrent_dir {
                    let root = nzbd_config::expand_home(root);
                    std::fs::create_dir_all(&root)?;
                    category_payload_roots.insert(category.name.clone(), std::fs::canonicalize(root)?);
                }
            }
            for (name, root) in nzbd_qbit_compat::load_overlay_category_roots(
                &cfg.state_dir(),
                &cfg.torrent_dir(),
            ) {
                category_payload_roots.entry(name).or_insert(root);
            }
            let upload_limit_bps = (cfg.torrent.upload_limit_kib > 0)
                .then_some(cfg.torrent.upload_limit_kib * 1024);
            let service = nzbd_api::torrent_admission::TorrentAdmissionService::new(
                engine.clone(),
                session,
                cfg.state_dir(),
                cfg.torrent.socks_proxy_url.is_some(),
                cfg.torrent.dht,
            )
            .with_transfer_policy(
                nzbd_types::SeedPolicy {
                    stop_on_complete: cfg.torrent.stop_seeding_on_complete,
                    ratio_limit: (cfg.torrent.default_seed_ratio > 0.0)
                        .then_some(cfg.torrent.default_seed_ratio),
                    time_limit_secs: (cfg.torrent.default_seed_minutes > 0)
                        .then_some(cfg.torrent.default_seed_minutes * 60),
                },
                category_seed_policies,
                upload_limit_bps,
            )
            .with_category_payload_roots(category_payload_roots)
            .with_source_fetch_limits(nzbd_torrent::TorrentSourceFetchLimits {
                max_metainfo_bytes: cfg.torrent.metainfo_max_mib as usize * 1024 * 1024,
                max_redirects: cfg.torrent.source_redirects as usize,
                ..Default::default()
            });
            service
                .recover_active()
                .await
                .map_err(|error| anyhow_lite::Error::msg(format!("torrent recovery: {error}")))?;
            torrent_executor = Some(service.spawn_backend_executor().map_err(|error| {
                anyhow_lite::Error::msg(format!("torrent backend executor: {error}"))
            })?);
            if let Some(watch) = &cfg.paths.torrent_watch_dir {
                let dir = nzbd_config::expand_home(watch);
                std::fs::create_dir_all(&dir)?;
                spawn_torrent_watch_dir(
                    service.clone(),
                    dir,
                    torrent_cancel.clone(),
                    &torrent_tracker,
                );
            }
            tracing::info!(
                port = cfg.torrent.listen_port,
                "single-node BitTorrent backend active"
            );
            Some(service)
        } else {
            None
        };
        torrent_tracker.close();

        // Post-processing manager (par verify/repair → unpack → cleanup →
        // scripts), watching the engine's finish events.
        let pp_cancel = tokio_util::sync::CancellationToken::new();
        let pp_tracker = tokio_util::task::TaskTracker::new();
        let history = history_db.clone();
        // Shared with the API so `/metrics` can report stage durations
        // measured where they actually happen.
        let mut pp_stats = None;
        let mut pp_manager = None;
        if cfg.post.enabled {
            let slots = nzbd_post::manager::strategy_slots(&cfg.post.strategy);
            let stats = Arc::new(nzbd_types::metrics::PpStageStats::new());
            pp_stats = Some(stats.clone());
            pp_manager = Some(nzbd_post::manager::spawn_post_manager(
                engine.clone(),
                post_config(&cfg, slots, Some(stats)),
                history_db.expect("post-processing history opened above"),
                cfg.download_dir(),
                None, // single node: always the authority
                pp_cancel.clone(),
                &pp_tracker,
            ));
        }
        pp_tracker.close();

        let scan_notify = Arc::new(tokio::sync::Notify::new());
        if let Some(watch) = &cfg.paths.nzb_watch_dir {
            let dir = nzbd_config::expand_home(watch);
            let _ = std::fs::create_dir_all(&dir);
            spawn_watch_dir(engine.clone(), dir, scan_notify.clone(), Arc::new(|| true));
        }
        let feed_cancel = tokio_util::sync::CancellationToken::new();
        let feed_tracker = tokio_util::task::TaskTracker::new();
        let feeds_handle = (!cfg.feeds.is_empty()).then(|| {
            nzbd_feed::spawn_feeds(
                engine.clone(),
                feed_defs(&cfg),
                cfg.state_dir(),
                Arc::new(|| true),
                feed_cancel.clone(),
                &feed_tracker,
            )
        });
        feed_tracker.close();
        let clients_registry = Arc::new(nzbd_api::ClientRegistry::default());
        let compat_state = nzbd_compat::CompatState {
            config: Arc::new(nzbd_compat::CompatConfig {
                version: cfg.api.compat_version.clone(),
            }),
            engine: engine.clone(),
            history: history.clone(),
            options: Arc::new(compat_options(&cfg, &bind)),
            log: Some(logbuf.clone()),
            scan_notify: Some(scan_notify),
            feeds: feeds_handle,
            clients: Some(clients_registry.clone()),
        };
        // Signals in-flight SSE streams to end when we start shutting down,
        // so graceful shutdown can drain and a restart isn't held open by an
        // open `/api/v1/events` connection.
        let (sse_shutdown_tx, sse_shutdown_rx) = tokio::sync::watch::channel(false);
        let auth = nzbd_api::AuthConfig {
            username: cfg.api.username.clone(),
            password: cfg.api.password.clone(),
            token: cfg.api.token.clone(),
        };
        let mut app = nzbd_api::require_auth(
            nzbd_api::router_with(nzbd_api::ApiState {
                engine: engine.clone(),
                torrent: torrent_service.clone(),
                history,
                log: Some(logbuf.clone()),
                setup: setup.clone(),
                clients: Some(clients_registry.clone()),
                shutdown: Some(sse_shutdown_rx),
                pp_stats,
                pp_manager,
                events: None, // router_with starts the hub
            })
            .merge(nzbd_compat::router(compat_state)),
            auth.clone(),
        );
        if let Some(torrent) = torrent_service.clone() {
            let qbit_save_path = torrent.output_root().to_path_buf();
            let configured_categories = cfg
                .categories
                .iter()
                .map(|category| {
                    let path = category
                        .torrent_dir
                        .as_ref()
                        .map(|path| nzbd_config::expand_home(path))
                        .unwrap_or_else(|| cfg.torrent_dir());
                    (category.name.clone(), path)
                })
                .collect();
            app = app.merge(nzbd_qbit_compat::router(nzbd_qbit_compat::QbitState::new(
                engine.clone(),
                torrent,
                nzbd_qbit_compat::QbitAuth {
                    username: auth.username,
                    password: auth.password,
                    token: auth.token,
                },
                cfg.state_dir(),
                qbit_save_path,
                configured_categories,
                cfg.torrent.dht,
                true,
                cfg.torrent.default_seed_ratio,
                cfg.torrent.default_seed_minutes,
                Some(clients_registry.clone()),
            )));
        }

        // A second view of the shutdown signal for the drain deadline below.
        let mut force_rx = sse_shutdown_tx.subscribe();
        let shutdown_setup = setup.clone();
        let shutdown = async move {
            match &shutdown_setup {
                Some(s) => {
                    tokio::select! {
                        _ = shutdown_signal() => tracing::info!("shutting down"),
                        _ = s.reload.notified() => {
                            tracing::info!("configuration changed; restarting with it")
                        }
                    }
                }
                None => {
                    shutdown_signal().await;
                    tracing::info!("shutting down");
                }
            }
            // Tell open SSE streams to end so graceful shutdown can drain
            // (otherwise a live `/api/v1/events` connection blocks forever).
            let _ = sse_shutdown_tx.send(true);
        };

        // Hard deadline on the drain: once shutdown is triggered, a client
        // holding a connection open must never stall a restart. If graceful
        // shutdown hasn't finished a few seconds after the trigger, proceed
        // anyway — dropping `serve` closes whatever is left.
        let force = async move {
            let _ = force_rx.wait_for(|triggered| *triggered).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
        };

        let tls_setup = tls::server_config(&cfg, &cfg.state_dir())
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
        let listener = tokio::net::TcpListener::bind(&bind).await?;
        // Pending magnets and source URLs can be unreachable for minutes.
        // The listener and restored torrents are ready before retrying them.
        let pending_cancel = torrent_cancel.clone();
        let pending_recovery = torrent_service.clone().map(|service| {
            tokio::spawn(async move {
                loop {
                    let result = tokio::select! {
                        _ = pending_cancel.cancelled() => return,
                        result = service.recover_pending() => result,
                    };
                    match result {
                        Ok(_) => return,
                        Err(error) => tracing::error!(
                            %error,
                            "pending torrent recovery failed; retrying in 30 seconds"
                        ),
                    }
                    tokio::select! {
                        _ = pending_cancel.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    }
                }
            })
        });
        let listener_address = listener.local_addr()?;
        let _advertiser =
            discovery::Advertiser::start(&cfg.api, listener_address, None, tls_setup.is_some());
        match tls_setup {
            None => {
                tracing::info!(%bind, "nzbd listening");
                let serve = std::future::IntoFuture::into_future(
                    axum::serve(
                        listener,
                        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                    )
                    .with_graceful_shutdown(shutdown),
                );
                tokio::pin!(serve, force);
                tokio::select! {
                    r = &mut serve => r?,
                    _ = &mut force => {
                        tracing::warn!("graceful shutdown exceeded its deadline; forcing restart");
                    }
                }
            }
            Some(t) => {
                tracing::info!(%bind, %t.note, "nzbd listening (https)");
                serve_tls(listener, t.config, app, shutdown).await?;
            }
        }

        // Subsystem teardown is BOUNDED. This sequence runs between "the
        // listener is down" and "the listener is back" — every second here
        // is user-visible dead air on a restart. A PP job or feed poll that
        // outlives its cancel must not hold the daemon offline (field
        // report 2026-07-25: a restart hung for minutes behind one PP job;
        // the page never came back). Stragglers die with this pass's
        // runtime; PP is crash-safe and re-runs on the next pass. Filesystem
        // probes use detached OS threads, so a wedged syscall cannot make
        // Tokio runtime teardown wait for it.
        if let Some(worker) = &_history_worker { worker.stop(); }
        feed_cancel.cancel();
        pp_cancel.cancel();
        torrent_cancel.cancel();
        if let Some(mut recovery) = pending_recovery {
            if tokio::time::timeout(Duration::from_secs(1), &mut recovery)
                .await
                .is_err()
            {
                // A retry interrupted after the queue owner commits but before
                // registry attachment is reconstructed by the next restore.
                recovery.abort();
            }
        }
        if let Some(service) = &torrent_service {
            service.shutdown().await;
        }
        if let Some(executor) = torrent_executor.take() {
            executor.abort();
        }
        let subsystems = async {
            feed_tracker.wait().await;
            pp_tracker.wait().await;
            torrent_tracker.wait().await;
        };
        if tokio::time::timeout(Duration::from_secs(10), subsystems)
            .await
            .is_err()
        {
            tracing::warn!(
                "feed/post-processing tasks ignored shutdown for 10s — restarting anyway \
                 (an interrupted PP job re-runs on the next pass)"
            );
        }
        engine.shutdown().await;
        let reload = setup
            .as_ref()
            .is_some_and(|s| s.applied.load(std::sync::atomic::Ordering::Relaxed));
        Ok(if reload {
            RunOutcome::Reload
        } else {
            RunOutcome::Exit
        })
    })
}

/// Serve the router over TLS: hand-rolled accept loop (tokio-rustls +
/// hyper-util's auto builder) so we stay on the workspace's existing
/// rustls stack. Per-connection tasks die with the runtime when a run
/// pass ends (shutdown or setup reload).
async fn serve_tls(
    listener: tokio::net::TcpListener,
    config: std::sync::Arc<rustls::ServerConfig>,
    app: axum::Router,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow_lite::Result<()> {
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((stream, peer)) = accepted else { continue };
                let acceptor = acceptor.clone();
                let app = app
                    .clone()
                    .layer(axum::Extension(axum::extract::ConnectInfo(peer)));
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(stream).await else {
                        return; // handshake failure (scanner, plain HTTP, …)
                    };
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection_with_upgrades(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
                });
            }
        }
    }
    Ok(())
}

/// Cluster mode (docs/CLUSTERING.md): shared-volume state, elected leader,
/// distributed download work; this node serves the full API either way.
async fn run_cluster(
    cfg: nzbd_config::Config,
    servers: Vec<nzbd_types::ServerDef>,
    tuning: Tuning,
    bind: String,
    logbuf: Arc<nzbd_api::LogBuffer>,
) -> anyhow_lite::Result<()> {
    let c = &cfg.cluster;
    let (cluster_cfg, shared_dir) = cluster_runtime_config(&cfg)?;
    // Job data must be visible to every node: default dest to the shared
    // volume unless the operator pointed it there (or elsewhere) already.
    let dest_dir = cfg.download_dir();
    if !dest_dir.starts_with(&shared_dir) {
        tracing::warn!(
            dest = %dest_dir.display(),
            shared = %shared_dir.display(),
            "dest_dir is outside the shared volume; remote post-processing (phase C2) will not see the files"
        );
    }

    // Post-processing wiring (C2): PP runs wherever the leader's
    // anti-affinity scheduler assigns it — as a work lease on an idle node
    // when one exists. History: SQLite index stays node-local, the
    // authoritative JSONL lives on the shared volume.
    let pp = if cfg.post.enabled && cfg.cluster.post_process {
        let jsonl_dir = shared_dir.join(".nzbd-cluster/history");
        let history = open_history(
            &cfg.state_dir(),
            &jsonl_dir,
            Some(&c.node_name),
            history_retention(&cfg),
            cfg.history.index_dir.as_deref(),
        )?;
        Some(nzbd_cluster::PpSetup {
            // Cluster PP stage timings are not wired to this node's
            // /metrics: the leases run wherever the scheduler puts them,
            // so a per-node summary would describe an arbitrary slice of
            // the work. Cluster-wide PP metrics need their own design.
            post: post_config(&cfg, cfg.cluster.pp_slots.max(1) as usize, None),
            history,
        })
    } else {
        None
    };

    let runtime = nzbd_cluster::ClusterRuntime::start(
        cluster_cfg,
        servers,
        tuning,
        dest_dir,
        cfg.speed_limit_bps(),
        cfg.max_active_downloads(),
        pp,
    )
    .await
    .map_err(with_fs_hint)?;

    let scan_notify = Arc::new(tokio::sync::Notify::new());
    if let Some(watch) = &cfg.paths.nzb_watch_dir {
        let dir = nzbd_config::expand_home(watch);
        let _ = std::fs::create_dir_all(&dir);
        let view = runtime.leader_gate();
        spawn_watch_dir(
            runtime.engine.clone(),
            dir,
            scan_notify.clone(),
            Arc::new(view),
        );
    }
    let feed_cancel = tokio_util::sync::CancellationToken::new();
    let feed_tracker = tokio_util::task::TaskTracker::new();
    let feeds_handle = (!cfg.feeds.is_empty()).then(|| {
        // Seen-store on the shared volume: a failover must not re-download
        // a feed's whole backlog.
        nzbd_feed::spawn_feeds(
            runtime.engine.clone(),
            feed_defs(&cfg),
            shared_dir.join(".nzbd-cluster"),
            Arc::new(runtime.leader_gate()),
            feed_cancel.clone(),
            &feed_tracker,
        )
    });
    feed_tracker.close();
    let app = runtime.router_full(
        &cfg.api.compat_version,
        compat_options(&cfg, &bind),
        nzbd_api::AuthConfig {
            username: cfg.api.username.clone(),
            password: cfg.api.password.clone(),
            token: cfg.api.token.clone(),
        },
        Some(logbuf),
        Some(scan_notify),
        feeds_handle,
    );
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let listener_address = listener.local_addr()?;
    let _advertiser = discovery::Advertiser::start(
        &cfg.api,
        listener_address,
        Some(&cfg.cluster.node_name),
        false,
    );
    tracing::info!(
        %bind,
        node = %cfg.cluster.node_name,
        "nzbd listening (cluster mode: C2 distributed post-processing)"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    feed_cancel.cancel();
    feed_tracker.wait().await;
    runtime.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// control-client commands (minimal HTTP/1.1 over loopback; the full native
// CLI arrives with the phase-3 API work)
// ---------------------------------------------------------------------------

fn client_add(
    file: PathBuf,
    url: String,
    name: Option<String>,
    category: Option<String>,
    priority: i32,
) -> anyhow_lite::Result<()> {
    let content = std::fs::read(&file)?;
    let name = name.unwrap_or_else(|| {
        file.file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "download".into())
    });
    let mut path = format!("/api/v1/jobs?name={}&priority={priority}", urlenc(&name));
    if let Some(c) = &category {
        path.push_str(&format!("&category={}", urlenc(c)));
    }
    let (status, body) = http_request(&url, "POST", &path, Some(content))?;
    if status == 201 {
        println!("{body}");
        Ok(())
    } else {
        eprintln!("add failed ({status}): {body}");
        std::process::exit(1);
    }
}

fn client_status(url: String) -> anyhow_lite::Result<()> {
    let (status, body) = http_request(&url, "GET", "/api/v1/status", None)?;
    if status == 200 {
        println!("{body}");
        Ok(())
    } else {
        eprintln!("status failed ({status}): {body}");
        std::process::exit(1);
    }
}

/// One-shot HTTP/1.1 request over TCP (loopback control traffic only).
fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> anyhow_lite::Result<(u16, String)> {
    use std::io::{Read, Write};
    let addr = addr.trim_start_matches("http://").trim_end_matches('/');
    let mut sock = std::net::TcpStream::connect(addr)?;
    sock.set_read_timeout(Some(Duration::from_secs(30)))?;
    let body = body.unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes())?;
    sock.write_all(&body)?;
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp)?;
    let text = String::from_utf8_lossy(&resp);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow_lite::Error::msg("malformed HTTP response"))?;
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.trim().to_string())
        .unwrap_or_default();
    Ok((status, payload))
}

fn urlenc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Tiny stand-in for `anyhow` to keep deps lean.
mod anyhow_lite {
    pub type Result<T> = std::result::Result<T, Error>;

    pub struct Error(String);

    impl Error {
        pub fn msg(s: impl Into<String>) -> Self {
            Error(s.into())
        }
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// `fn main() -> Result<_, E>` prints `E` with `Debug`, so the derived
    /// form would render a multi-line hint as literal `\n` escapes inside
    /// quotes. Delegate to `Display` (same as `anyhow`) and the operator
    /// gets a readable message.
    impl std::fmt::Debug for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(self, f)
        }
    }

    impl<E: std::error::Error> From<E> for Error {
        fn from(e: E) -> Self {
            Error(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(extra: &str) -> nzbd_config::Config {
        let toml = format!("[paths]\nmain_dir = \"/data\"\ndest_dir = \"/data/complete\"\n{extra}");
        nzbd_config::Config::from_toml(&toml).unwrap()
    }

    #[test]
    fn compat_options_project_nzbget_vocabulary() {
        let cfg = cfg_with(
            "[[category]]\nname = \"tv\"\ndest_dir = \"/data/tv\"\n\n[post]\nunpack = false\n",
        );
        let opts = compat_options(&cfg, "0.0.0.0:6789");
        let get = |k: &str| {
            opts.iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        assert_eq!(get("ControlPort"), "6789");
        assert_eq!(get("MainDir"), "/data");
        assert_eq!(get("DestDir"), "/data/complete");
        assert_eq!(get("Unpack"), "no");
        assert_eq!(get("Category1.Name"), "tv");
        assert_eq!(get("Category1.DestDir"), "/data/tv");
        assert!(!get("Version").is_empty());
    }

    #[test]
    fn post_config_maps_health_and_timeouts() {
        let cfg = cfg_with(
            "[post]\nhealth_action = \"park\"\ntool_timeout_secs = 0\nscripts_dir = \"~/scripts\"\n",
        );
        let pc = post_config(&cfg, 3, None);
        assert_eq!(
            pc.failure_action,
            nzbd_post::manager::FailureAction::Park,
            "the old `health_action` key still parses"
        );
        assert_eq!(pc.slots, 3);
        // Zero timeout is clamped to something sane rather than "instant".
        assert!(pc.tool_timeout >= Duration::from_secs(1));
        assert!(pc.scripts_dir.is_some());
    }

    #[test]
    fn engine_tuning_preserves_units_and_safety_bounds() {
        let mut cfg = cfg_with(
            "[queue]\narticle_retries = 7\nretry_interval_secs = 11\n\
             article_timeout_secs = 13\npropagation_delay_mins = 3\n\
             min_free_disk_mb = 4\ndaily_quota_mb = 5\nmonthly_quota_mb = 6\n\
             quota_start_day = 99\n\n[post]\nfailure_action = \"none\"\n",
        );
        let tuning = engine_tuning(&cfg);
        assert_eq!(tuning.article_retries, 7);
        assert_eq!(tuning.retry_interval, Duration::from_secs(11));
        assert_eq!(tuning.article_timeout, Duration::from_secs(13));
        assert_eq!(tuning.propagation_delay, Duration::from_secs(180));
        assert_eq!(tuning.min_free_disk_bytes, 4 * 1024 * 1024);
        assert_eq!(tuning.daily_quota_bytes, 5 * 1024 * 1024);
        assert_eq!(tuning.monthly_quota_bytes, 6 * 1024 * 1024);
        assert_eq!(tuning.quota_start_day, 28);
        assert!(!tuning.health_abort);

        cfg.queue.quota_start_day = 0;
        cfg.post.failure_action = "park".into();
        let bounded = engine_tuning(&cfg);
        assert_eq!(bounded.quota_start_day, 1);
        assert!(bounded.health_abort);
    }

    #[test]
    fn cluster_runtime_config_resolves_secret_volume_and_liveness_bounds() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("downloads");
        cfg.paths.dest_dir = tmp.path().join("shared/complete");
        cfg.cluster.enabled = true;
        cfg.cluster.node_name = "node-a".into();
        cfg.cluster.shared_dir = Some(tmp.path().join("shared"));
        cfg.cluster.advertise_url = "https://node-a.test:6789".into();
        cfg.cluster.secret = Some("cluster-secret".into());
        cfg.cluster.coordinator = false;
        cfg.cluster.priority = 4;
        cfg.cluster.download = false;
        cfg.cluster.max_download_jobs = 6;
        cfg.cluster.post_process = true;
        cfg.cluster.pp_slots = 0;
        cfg.cluster.lease_interval_secs = 0;
        cfg.cluster.takeover_after_secs = 0;
        cfg.cluster.worker_ttl_secs = 0;

        let (runtime, shared_dir) = cluster_runtime_config(&cfg).unwrap();
        assert_eq!(shared_dir, tmp.path().join("shared"));
        assert_eq!(runtime.node_name, "node-a");
        assert_eq!(runtime.shared_dir, shared_dir);
        assert_eq!(runtime.advertise_url, "https://node-a.test:6789");
        assert_eq!(runtime.secret, "cluster-secret");
        assert!(!runtime.coordinator);
        assert_eq!(runtime.priority, 4);
        assert!(!runtime.download);
        assert_eq!(runtime.max_download_jobs, 6);
        assert!(runtime.post_process);
        assert_eq!(runtime.pp_slots, 1);
        assert_eq!(runtime.lease_interval, Duration::from_secs(1));
        assert_eq!(runtime.takeover_after, Duration::from_secs(2));
        assert_eq!(runtime.worker_ttl, Duration::from_secs(3));
        assert!(
            runtime
                .disk_guard_roots
                .iter()
                .any(|root| root.path == cfg.paths.dest_dir),
            "the cluster engine enforces every configured write root"
        );

        cfg.cluster.secret = None;
        let err = match cluster_runtime_config(&cfg) {
            Err(err) => err,
            Ok(_) => panic!("cluster startup without a secret must fail closed"),
        };
        assert!(err.to_string().contains("requires secret"), "{err}");
    }

    #[test]
    fn cluster_startup_fails_before_spawning_when_the_shared_volume_is_unusable() {
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("not-a-directory");
        std::fs::write(&blocker, "file").unwrap();

        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("downloads");
        cfg.paths.dest_dir = tmp.path().join("complete");
        cfg.post.enabled = false;
        cfg.cluster.enabled = true;
        cfg.cluster.node_name = "node-a".into();
        let shared_root = blocker.join("shared");
        cfg.cluster.shared_dir = Some(shared_root.clone());
        cfg.cluster.advertise_url = "http://127.0.0.1:6789".into();
        cfg.cluster.secret = Some("cluster-secret".into());
        let tuning = engine_tuning(&cfg);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let result = runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(2),
                    run_cluster(
                        cfg,
                        Vec::new(),
                        tuning,
                        "127.0.0.1:0".into(),
                        nzbd_api::LogBuffer::new(1),
                    ),
                )
                .await
            })
            .expect("cluster startup must terminate instead of serving after the guard regresses");
        let err = result
            .expect_err("cluster startup must fail when its shared root is below a regular file");
        let shared_root_error =
            std::fs::create_dir_all(shared_root.join(".nzbd-cluster").join("nodes"))
                .expect_err("the test's shared root must remain unusable");
        assert_eq!(
            err.to_string(),
            format!("io: {shared_root_error}"),
            "startup must stop on the configured shared root {}",
            shared_root.display()
        );
        assert!(
            blocker.is_file(),
            "startup must not replace the blocking file"
        );
    }

    #[test]
    fn feeds_keep_ids_defaults_and_operator_options() {
        let cfg = cfg_with(
            "[[feed]]\nname = \"daily\"\nurl = \"https://indexer.test/daily\"\n\
             interval_mins = 0\nfilter = \"Require: *1080p*\"\ncategory = \"tv\"\n\
             priority = 25\npause = true\n\n\
             [[feed]]\nname = \"manual\"\nurl = \"https://indexer.test/manual\"\n\
             interval_mins = 30\n",
        );
        let feeds = feed_defs(&cfg);

        assert_eq!(feeds.len(), 2);
        assert_eq!(feeds[0].id, 1);
        assert_eq!(feeds[0].name, "daily");
        assert_eq!(feeds[0].url, "https://indexer.test/daily");
        assert_eq!(feeds[0].interval, Duration::from_secs(60));
        assert_eq!(feeds[0].filter, "Require: *1080p*");
        assert_eq!(feeds[0].category.as_deref(), Some("tv"));
        assert_eq!(feeds[0].priority, 25);
        assert!(feeds[0].pause);
        assert_eq!(feeds[1].id, 2);
        assert_eq!(feeds[1].interval, Duration::from_secs(30 * 60));
        assert!(!feeds[1].pause);
    }

    #[test]
    fn recovered_config_restore_is_best_effort_and_exact() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("config/nzbd.toml");
        assert!(restore_config_file(&target, "[api]\ndiscovery = false\n"));
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "[api]\ndiscovery = false\n"
        );

        let blocker = tmp.path().join("not-a-directory");
        std::fs::write(&blocker, "file").unwrap();
        assert!(!restore_config_file(
            &blocker.join("config/nzbd.toml"),
            "ignored"
        ));

        let directory_target = tmp.path().join("directory-target");
        std::fs::create_dir(&directory_target).unwrap();
        assert!(!restore_config_file(&directory_target, "ignored"));
    }

    #[test]
    fn history_directory_failure_names_the_unusable_path() {
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("not-a-directory");
        std::fs::write(&blocker, "file").unwrap();
        let local = blocker.join("history-local");
        let err = match open_history(
            &local,
            &tmp.path().join("history-jsonl"),
            None,
            nzbd_state::history::Retention::UNLIMITED,
            None,
        ) {
            Err(err) => err,
            Ok(_) => panic!("a history directory below a regular file must fail"),
        };
        let message = err.to_string();
        assert!(
            message.contains("history db:") && message.contains("create directory"),
            "{message}"
        );
        assert!(message.contains(&local.display().to_string()), "{message}");
    }

    #[test]
    fn directory_config_path_explains_the_docker_bind_mount_mistake() {
        let tmp = tempfile::tempdir().unwrap();
        let err = match run(
            Some(tmp.path().to_path_buf()),
            Some("127.0.0.1:0".into()),
            nzbd_api::LogBuffer::new(1),
        ) {
            Err(err) => err,
            Ok(_) => panic!("a directory cannot be parsed as nzbd.toml"),
        };
        let message = err.to_string();
        assert!(message.contains("is a DIRECTORY, not a file"), "{message}");
        assert!(message.contains("Docker bind mount"), "{message}");
        assert!(
            message.contains(&tmp.path().display().to_string()),
            "{message}"
        );
    }

    #[test]
    fn anyhow_lite_error_wraps_and_displays() {
        let e = anyhow_lite::Error::msg("boom");
        assert_eq!(format!("{e}"), "boom");
        let io: anyhow_lite::Error = std::io::Error::other("disk on fire").into();
        assert!(format!("{io}").contains("disk on fire"));
        // `main` prints with Debug — it must not quote-and-escape the text.
        let multi = anyhow_lite::Error::msg("first line\nsecond line");
        assert_eq!(format!("{multi:?}"), "first line\nsecond line");
    }

    fn denied(path: &str) -> nzbd_state::StateError {
        nzbd_state::StateError::Io {
            op: "create directory",
            path: PathBuf::from(path),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        }
    }

    /// The reported failure: three lines of bare EACCES with no path. The
    /// message must now carry both the path and what to do about it.
    #[test]
    fn startup_permission_error_names_the_path_and_the_fix() {
        let engine_err = nzbd_engine::EngineError::State(denied("/data/usenet/queue"));
        let msg = with_fs_hint(engine_err).to_string();

        assert!(msg.contains("/data/usenet/queue"), "{msg}");
        assert!(msg.contains("hint:"), "{msg}");
        assert!(msg.contains("paths.queue_dir"), "{msg}");
    }

    /// The hint has to survive being wrapped by the cluster layer, which
    /// is where a shared-volume mount most often has the wrong owner.
    #[test]
    fn fs_hint_reaches_through_the_cluster_error_chain() {
        let err = nzbd_cluster::ClusterError::Engine(nzbd_engine::EngineError::State(denied(
            "/mnt/gluster/nzbd/queue",
        )));
        let msg = with_fs_hint(err).to_string();
        assert!(msg.contains("/mnt/gluster/nzbd/queue"), "{msg}");
        assert!(msg.contains("hint:"), "{msg}");
    }

    /// Errors that aren't permission problems pass through unchanged — no
    /// misleading chmod advice on a corrupt queue.json or a missing file.
    #[test]
    fn fs_hint_only_fires_on_permission_errors() {
        let corrupt = nzbd_engine::EngineError::State(nzbd_state::StateError::Corrupt(
            "queue.json: trailing comma".into(),
        ));
        let msg = with_fs_hint(corrupt).to_string();
        assert!(msg.contains("trailing comma"), "{msg}");
        assert!(!msg.contains("hint:"), "{msg}");

        let missing = nzbd_engine::EngineError::State(nzbd_state::StateError::Io {
            op: "open",
            path: PathBuf::from("/data/queue/queue.json"),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        });
        let msg = with_fs_hint(missing).to_string();
        assert!(msg.contains("/data/queue/queue.json"), "{msg}");
        assert!(!msg.contains("hint:"), "{msg}");
    }
}
