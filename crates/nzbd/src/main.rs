//! nzbd daemon binary, phase 1: boots the download engine, serves the
//! native API and the compat shim, and offers a small control CLI
//! (`add`, `status`) that talks to a running daemon over the native API.

use clap::{Parser, Subcommand};

mod control;
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
        /// Numeric loopback bootstrap monitor address (overrides NZBD_MONITOR_BIND).
        #[arg(long)]
        monitor_bind: Option<String>,
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
    /// Probe process liveness, or public control readiness, within two seconds.
    Health {
        #[arg(long)]
        ready: bool,
        #[arg(long)]
        monitor_bind: Option<String>,
        /// Transitional literal-loopback HTTP/HTTPS /healthz probe.
        #[arg(long, conflicts_with = "monitor_bind")]
        url: Option<String>,
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
        Command::Run {
            config,
            bind,
            monitor_bind,
        } => {
            // No config, mirror, TLS, or state-volume access precedes this bind.
            let control = control::Controller::start(
                control::monitor_bind(monitor_bind.as_deref())?,
                logbuf.clone(),
            )?;
            loop {
                match run(config.clone(), bind.clone(), logbuf.clone(), &control) {
                    Ok(RunOutcome::Exit) => break Ok(()),
                    Ok(RunOutcome::Reload) => {
                        tracing::info!("reloading backend; control service remains available")
                    }
                    Err(error) => {
                        // Unknown initial security must never fall through to defaults.
                        if !control.security_known() || control.is_stopping() {
                            break Err(error);
                        }
                        tracing::error!(%error, "backend unavailable; retrying without closing control service");
                        control.loading();
                        control.reason("backend", error.to_string());
                        for _ in 0..50 {
                            if control.is_stopping() {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(100));
                        }
                        if control.is_stopping() {
                            break Ok(());
                        }
                    }
                }
            }
        }
        Command::Health {
            ready,
            monitor_bind,
            url,
        } => control::health(ready, monitor_bind.as_deref(), url.as_deref()),
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
        hold_deadline: Duration::from_secs(cfg.post.hold_deadline_secs.max(1)),
        password_hold_deadline: Duration::from_secs(cfg.post.password_hold_deadline_secs.max(1)),
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
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let pending = parent.join(format!(
        ".nzbd-recovered-config-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(parent)?;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&pending)?;
        file.write_all(toml.as_bytes())?;
        file.sync_all()?;
        // Never replace a config that appeared after the missing-file read.
        std::fs::hard_link(&pending, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&pending);
    if let Err(error) = &result {
        tracing::warn!(path = %path.display(), %error,
            "could not restore missing config exclusively; running from saved copy without replacing local data");
    }
    result.is_ok()
}

/// Bootstrap has one real OS worker at a time. A timed-out filesystem call
/// keeps its slot; retries never accumulate detached copies of that call.
enum BootstrapError {
    Unavailable(String),
    Invalid(String),
}

type BootstrapSecurity = (
    nzbd_config::Config,
    Option<PathBuf>,
    Option<PathBuf>,
    Option<tls::TlsSetup>,
    Option<String>,
);

fn load_bootstrap(
    path: Option<PathBuf>,
    control: &control::Controller,
) -> anyhow_lite::Result<BootstrapSecurity> {
    loop {
        let worker_path = path.clone();
        let trusted_configuration = control.trusted_configuration();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let started = std::time::Instant::now();
        let worker = std::thread::Builder::new()
            .name("nzbd-bootstrap-security".into())
            .spawn(move || {
                let result = read_bootstrap(worker_path, trusted_configuration);
                let _ = tx.send(result);
            })?;
        let result = loop {
            if control.is_stopping() {
                return Err(anyhow_lite::Error::msg(
                    "shutdown while configuration/security storage is unavailable",
                ));
            }
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(result) => break result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if started.elapsed() >= Duration::from_secs(10) {
                        control.reason("configuration", "configuration/security storage deadline exceeded; waiting for the existing worker, no duplicate reads started");
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow_lite::Error::msg("bootstrap security worker failed"))
                }
            }
        };
        let _ = worker.join();
        match result {
            Ok(result) => {
                tracing::info!(
                    elapsed_ms = started.elapsed().as_millis(),
                    "configuration and security loaded"
                );
                return Ok(result);
            }
            Err(BootstrapError::Invalid(message)) => return Err(anyhow_lite::Error::msg(message)),
            Err(BootstrapError::Unavailable(message)) => {
                tracing::warn!(%message, "configuration/security storage unavailable; retrying");
                control.reason("configuration", message);
                for _ in 0..50 {
                    if control.is_stopping() {
                        return Err(anyhow_lite::Error::msg("shutdown during bootstrap retry"));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}

fn lifecycle_inventory_dir(cfg: &nzbd_config::Config) -> PathBuf {
    if cfg.cluster.enabled {
        let default = cfg
            .paths
            .main_dir
            .join(".nzbd-control")
            .join(&cfg.cluster.node_name);
        nzbd_config::expand_home(cfg.cluster.control_dir.as_ref().unwrap_or(&default))
            .join("file-lifecycle")
    } else {
        cfg.state_dir()
    }
}
fn validate_installation_before_mutation(
    cfg: &nzbd_config::Config,
    config: Option<&std::path::Path>,
) -> anyhow_lite::Result<Option<String>> {
    let expected = expected_installation_id(config)?;
    nzbd_state::artifacts::Inventory::validate_expected(
        &lifecycle_inventory_dir(cfg),
        expected.as_deref(),
    )
    .map_err(|error| {
        anyhow_lite::Error::msg(format!("installation identity preflight: {error}"))
    })?;
    Ok(expected)
}

const SECURITY_RECEIPT: &str = ".nzbd-security-established";

fn security_receipt(config: &std::path::Path) -> PathBuf {
    config
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(SECURITY_RECEIPT)
}

/// Missing configuration is not positive evidence of a new installation.
/// Any enrolled identity, historical state, security material, or uncertain
/// probe keeps bootstrap nonpermissive until trusted settings are recovered.
fn prove_first_installation(config: Option<&std::path::Path>) -> Result<(), String> {
    if expected_installation_id(config)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("configuration and mirrors are missing for an enrolled installation; refusing unauthenticated first-run defaults".into());
    }
    if let Some(config) = config {
        let receipt = security_receipt(config);
        match std::fs::symlink_metadata(&receipt) {
            Ok(_) => return Err(format!("configuration is missing but established security was recorded in {}; refusing first-run defaults", receipt.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(error) => return Err(format!("cannot inspect established security {}: {error}", receipt.display())),
        }
    }
    for directory in nzbd_config::durable::candidate_state_dirs() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "cannot establish first installation from {}: {error}",
                    directory.display()
                ))
            }
        };
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "cannot inspect prior installation in {}: {error}",
                    directory.display()
                )
            })?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if matches!(
                name.as_ref(),
                "artifacts.sqlite"
                    | "artifacts.lock"
                    | "queue.json"
                    | "queue.json.bak"
                    | "tls"
                    | "jobs"
                    | "workers"
                    | "history"
                    | "nzbd.toml.saved"
                    | SECURITY_RECEIPT
            ) || name.starts_with("unclean.")
                || name.ends_with(".journal")
            {
                return Err(format!("configuration is missing but prior installation evidence exists at {}; refusing first-run defaults", entry.path().display()));
            }
        }
    }
    Ok(())
}

fn remember_established_security(config: &std::path::Path) {
    use std::io::Write;
    let receipt = security_receipt(config);
    let result = (|| -> std::io::Result<()> {
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&receipt)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(error) => return Err(error),
        };
        file.write_all(b"nzbd established security v1\n")?;
        file.sync_all()?;
        #[cfg(unix)]
        std::fs::File::open(receipt.parent().unwrap())?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::warn!(path = %receipt.display(), %error, "cannot retain independent security history; keep the configuration or expected installation identity on durable storage");
    }
}

fn read_bootstrap(
    path: Option<PathBuf>,
    trusted_configuration: Option<nzbd_config::Config>,
) -> Result<BootstrapSecurity, BootstrapError> {
    use BootstrapError::{Invalid, Unavailable};
    let config_path = path.clone();
    let mut setup_path = None;
    let mut recovered_from = None;
    let mut recovered_text = None;
    let cfg = match path {
        None => {
            prove_first_installation(None).map_err(Unavailable)?;
            nzbd_config::Config::default()
        }
        Some(path) => {
            match std::fs::metadata(&path) {
                Ok(metadata) if metadata.is_dir() => return Err(Invalid(format!("config path {} is a DIRECTORY, not a file — check the Docker bind mount and create the host config file", path.display()))),
                Ok(_) => {},
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
                Err(error) => return Err(Unavailable(format!("cannot inspect config {}: {error}", path.display()))),
            }
            match std::fs::read_to_string(&path) {
                Ok(text) => nzbd_config::Config::from_toml(&text)
                    .map_err(|error| Invalid(format!("{}: {error}", path.display())))?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if trusted_configuration.is_some() {
                        return Err(Unavailable(format!("configuration {} disappeared; retaining the process's last trusted security and public listener until an explicit configuration returns", path.display())));
                    }
                    let mut recovered = None;
                    // Do not use find_mirror(): its historical Option API hides
                    // unreadable mirrors and could expose a permissive wizard.
                    for dir in nzbd_config::durable::candidate_state_dirs() {
                        let mirror = nzbd_config::durable::mirror_path(&dir);
                        match std::fs::read_to_string(&mirror) {
                            Ok(text) => {
                                let cfg =
                                    nzbd_config::Config::from_toml(&text).map_err(|error| {
                                        Invalid(format!(
                                            "saved config {}: {error}",
                                            mirror.display()
                                        ))
                                    })?;
                                recovered = Some(cfg);
                                recovered_from = Some(mirror);
                                recovered_text = Some(text);
                                break;
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => {
                                return Err(Unavailable(format!(
                                    "cannot read saved config {}: {error}",
                                    mirror.display()
                                )))
                            }
                        }
                    }
                    match recovered {
                        Some(cfg) => cfg,
                        None => {
                            prove_first_installation(Some(&path)).map_err(Unavailable)?;
                            setup_path = Some(path);
                            nzbd_config::Config::default()
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                    return Err(Invalid(format!(
                        "invalid config text {}: {error}",
                        path.display()
                    )))
                }
                Err(error) => {
                    return Err(Unavailable(format!(
                        "cannot read config {}: {error}",
                        path.display()
                    )))
                }
            }
        }
    };
    let tls = tls::server_config_checked(&cfg, &cfg.state_dir(), || {
        validate_installation_before_mutation(&cfg, config_path.as_deref())
            .map(|_| ())
            .map_err(|error| anyhow_lite::Error::msg(format!("storage unavailable: {error}")))
    })
    .map_err(|error| {
        let message = error.to_string();
        if message.starts_with("storage unavailable:") {
            Unavailable(message)
        } else {
            Invalid(message)
        }
    })?;
    if setup_path.is_none() {
        if let Some(path) = config_path.as_deref() {
            remember_established_security(path);
        }
    }
    Ok((cfg, setup_path, recovered_from, tls, recovered_text))
}

const INSTALLATION_ID_ENV: &str = "NZBD_EXPECTED_INSTALLATION_ID";

fn identity_sidecar(config: &std::path::Path) -> PathBuf {
    config
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(".nzbd-installation-id")
}
fn validate_installation_id(text: &str) -> anyhow_lite::Result<String> {
    let id = text.trim();
    if id.len() != 32 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow_lite::Error::msg(
            "installation identity must be 32 hexadecimal characters",
        ));
    }
    Ok(id.to_ascii_lowercase())
}
fn expected_installation_id(
    config: Option<&std::path::Path>,
) -> anyhow_lite::Result<Option<String>> {
    let environment = match std::env::var(INSTALLATION_ID_ENV) {
        Ok(value) => Some(validate_installation_id(&value)?),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let stored = if let Some(config) = config {
        let path = identity_sidecar(config);
        match std::fs::read_to_string(&path) {
            Ok(value) => Some(validate_installation_id(&value).map_err(|error| {
                anyhow_lite::Error::msg(format!("{}: {error}", path.display()))
            })?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(anyhow_lite::Error::msg(format!(
                    "cannot read installation anchor {}: {error}",
                    path.display()
                )))
            }
        }
    } else {
        None
    };
    if let (Some(environment), Some(stored)) = (&environment, &stored) {
        if environment != stored {
            return Err(anyhow_lite::Error::msg(
                "launch installation identity conflicts with the durable config sidecar",
            ));
        }
    }
    Ok(environment.or(stored))
}

fn enroll_installation_id(
    config: Option<&std::path::Path>,
    accepted: &str,
) -> anyhow_lite::Result<bool> {
    use std::io::Write;
    if expected_installation_id(config)?.is_some() {
        return Ok(true);
    }
    let Some(config) = config else {
        return Ok(false);
    };
    // An image-layer config or absent first-run config is not an independent
    // durable anchor. Readonly mounts require an explicit launch identity.
    if nzbd_config::durable::durability(config) != nzbd_config::durable::Durability::Persistent
        || !config.is_file()
    {
        return Ok(false);
    }
    let path = identity_sidecar(config);
    let parent = path.parent().unwrap();
    let pending = parent.join(format!(
        ".nzbd-installation-id.pending-{}",
        std::process::id()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = match options.open(&pending) {
        Ok(file) => file,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
            ) =>
        {
            return Ok(false)
        }
        Err(error) => return Err(error.into()),
    };
    let result = (|| -> anyhow_lite::Result<()> {
        writeln!(file, "{accepted}")?;
        file.sync_all()?;
        match std::fs::hard_link(&pending, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if expected_installation_id(Some(config))?.as_deref() != Some(accepted) {
                    return Err(anyhow_lite::Error::msg(
                        "concurrent installation enrollment conflicts with accepted inventory",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&pending);
    result?;
    Ok(true)
}

fn run(
    config: Option<PathBuf>,
    bind: Option<String>,
    logbuf: Arc<nzbd_api::LogBuffer>,
    control: &control::Controller,
) -> anyhow_lite::Result<RunOutcome> {
    let (cfg, setup_path, recovered_from, tls_setup, recovered_text) =
        load_bootstrap(config.clone(), control)?;
    let bind = bind.unwrap_or_else(|| cfg.api.bind.clone());
    // Start the known-secure public control service before SetupHandle's
    // writable probe, history opening, Engine::spawn, or other backend I/O.
    control.public(&cfg, &bind, tls_setup)?;
    let expected_identity = validate_installation_before_mutation(&cfg, config.as_deref())?;
    if let (Some(path), Some(text)) = (&config, &recovered_text) {
        let restored = restore_config_file(path, text);
        tracing::warn!(path = %path.display(), recovered_from = ?recovered_from, restored,
            "missing config recovered from saved data-volume mirror; check config persistence");
    }
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
        let result = runtime.block_on(run_cluster(
            cfg, servers, tuning, bind, logbuf, control, config,
        ));
        runtime.shutdown_timeout(Duration::from_secs(1));
        result?;
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
    engine_cfg.expected_installation_id = expected_identity;

    // Keep a shutdown handle outside the startup future: any later failure
    // (torrent restore, watch roots, router setup) must drain the accepted
    // engine before a retry can create another owner/writer scope.
    let startup_engine = Arc::new(std::sync::Mutex::new(None::<nzbd_engine::EngineHandle>));
    let cleanup_engine = startup_engine.clone();
    let result = runtime.block_on(async move {
        // History is part of every job's terminal (pipeline contract v2
        // §2.1): health and fetch failures are recorded by the queue owner
        // even with post-processing disabled, and `job_pp_finished` is only
        // sent once its history row is durable. The daemon does not start
        // without it — an unopenable history store is a startup error.
        let history_db = {
            let state_dir = cfg.state_dir();
            Some(open_history(
                &state_dir,
                &state_dir.join("history"),
                None,
                history_retention(&cfg),
                cfg.history.index_dir.as_deref(),
            )?)
        };
        let _history_worker = history_db.as_ref().map(|db| db.start_worker()).transpose()
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
        engine_cfg.history = history_db.clone();
        let engine = Engine::spawn(engine_cfg).await.map_err(with_fs_hint)?;
        *startup_engine.lock().unwrap() = Some(engine.clone());
        match enroll_installation_id(config.as_deref(), &engine.artifacts().installation) {
            Ok(true) => control.identity(true, None),
            Ok(false) => {
                let reason = format!("installation identity is not independently enrolled; persist {} beside the config or set {INSTALLATION_ID_ENV} to {} in durable deployment configuration", ".nzbd-installation-id", engine.artifacts().installation);
                tracing::warn!(%reason);
                control.identity(false, Some(reason));
            }
            Err(error) => {
                tracing::error!(%error, "installation enrollment unavailable; preserving accepted inventory without replacing anchor");
                control.identity(false, Some(error.to_string()));
            }
        }
        if !cfg.torrent.enabled {
            let live_torrents = engine
                .snapshot()
                .jobs
                .iter()
                .filter(|job| job.kind == nzbd_types::JobKind::Torrent)
                .count();
            if live_torrents > 0 {
                if !matches!(tokio::time::timeout(Duration::from_secs(8), engine.shutdown()).await, Ok(true)) {
                    tracing::error!("disabled torrent backend did not drain durably; refusing replacement");
                    std::process::exit(1);
                }
                *startup_engine.lock().unwrap() = None;
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

        control.install(app, engine.clone());
        let shutdown_setup = setup.clone();
        let shutdown = async {
            match &shutdown_setup {
                Some(s) => tokio::select! {
                    _ = control.stopped() => tracing::info!("shutting down"),
                    _ = s.reload.notified() => tracing::info!("configuration changed; reloading backend"),
                },
                None => control.stopped().await,
            }
            control.loading();
            let _ = sse_shutdown_tx.send(true);
        };

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
        shutdown.await;

        // The control listener remains up during backend teardown. A stuck
        // old worker must never be replaced by a second writer/PP scope.
        let drain = async {
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
            tracing::error!("feed/post-processing tasks ignored shutdown for 10s; refusing overlapping replacement scope");
            std::process::exit(1);
        }
        if !matches!(tokio::time::timeout(Duration::from_secs(8), engine.shutdown()).await, Ok(true)) {
            // A live writer scope cannot be inherited by a replacement backend.
            tracing::error!("backend drain timed out; refusing replacement while old writer scope may be live");
            std::process::exit(1);
        }
        };
        if tokio::time::timeout(Duration::from_secs(20), drain).await.is_err() {
            tracing::error!("backend teardown exceeded 20 seconds; process exit preserves recovery evidence and prevents overlapping scopes");
            std::process::exit(1);
        }
        let reload = setup
            .as_ref()
            .is_some_and(|s| s.applied.load(std::sync::atomic::Ordering::Relaxed));
        Ok(if reload {
            RunOutcome::Reload
        } else {
            RunOutcome::Exit
        })
    });
    if result.is_err() {
        let engine = cleanup_engine.lock().unwrap().take();
        if let Some(engine) = engine {
            let drained = runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(8), engine.shutdown()).await
            });
            if !matches!(drained, Ok(true)) {
                tracing::error!("failed backend did not drain; refusing replacement while writer scope remains live");
                std::process::exit(1);
            }
        }
    }
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

/// Cluster mode (docs/CLUSTERING.md): shared-volume state, elected leader,
/// distributed download work; this node serves the full API either way.
async fn run_cluster(
    cfg: nzbd_config::Config,
    servers: Vec<nzbd_types::ServerDef>,
    tuning: Tuning,
    bind: String,
    logbuf: Arc<nzbd_api::LogBuffer>,
    control: &control::Controller,
    config_path: Option<PathBuf>,
) -> anyhow_lite::Result<()> {
    let c = &cfg.cluster;
    let (cluster_cfg, shared_dir) = cluster_runtime_config(&cfg)?;
    let expected_identity = validate_installation_before_mutation(&cfg, config_path.as_deref())?;
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

    let runtime = nzbd_cluster::ClusterRuntime::start_expected(
        cluster_cfg,
        servers,
        tuning,
        dest_dir,
        cfg.speed_limit_bps(),
        cfg.max_active_downloads(),
        pp,
        expected_identity,
    )
    .await
    .map_err(with_fs_hint)?;

    match enroll_installation_id(
        config_path.as_deref(),
        &runtime.engine.artifacts().installation,
    ) {
        Ok(true) => control.identity(true, None),
        Ok(false) => {
            let reason = format!("cluster lifecycle installation identity is not independently enrolled; persist .nzbd-installation-id beside the config or set {INSTALLATION_ID_ENV} to {} in durable deployment configuration", runtime.engine.artifacts().installation);
            tracing::warn!(%reason);
            control.identity(false, Some(reason));
        }
        Err(error) => {
            tracing::error!(%error, "cluster installation enrollment unavailable; preserving accepted inventory and existing anchor");
            control.identity(false, Some(error.to_string()));
        }
    }

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
    control.install(app, runtime.engine.clone());
    control.stopped().await;
    control.loading();

    feed_cancel.cancel();
    feed_tracker.wait().await;
    if !runtime.shutdown().await {
        return Err(anyhow_lite::Error::msg(
            "cluster shutdown did not complete durably; refusing replacement",
        ));
    }
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

    #[test]
    fn missing_primary_after_known_security_never_selects_default_or_stale_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("lost.toml");
        let mut trusted = nzbd_config::Config::default();
        trusted.api.password = Some("known-secret".into());
        trusted.api.tls = true;
        match read_bootstrap(Some(missing), Some(trusted)) {
            Err(BootstrapError::Unavailable(message)) => {
                assert!(message.contains("last trusted security"))
            }
            _ => panic!(
                "lost primary configuration must retain established security and remain retryable"
            ),
        }
        assert!(
            !tmp.path()
                .join(".nzbd-security-established")
                .try_exists()
                .unwrap(),
            "a missing primary must not manufacture a new security receipt or TLS identity"
        );
    }

    fn cfg_with(extra: &str) -> nzbd_config::Config {
        let toml = format!("[paths]\nmain_dir = \"/data\"\ndest_dir = \"/data/complete\"\n{extra}");
        nzbd_config::Config::from_toml(&toml).unwrap()
    }

    #[tokio::test]
    async fn default_processing_allocation_publishes_only_after_success() {
        use nzbd_engine::{AddOpts, Event};
        use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
        use nzbd_types::JobStatus;

        for (category, conflict) in [(false, false), (true, false), (false, true), (true, true)] {
            let tmp = tempfile::tempdir().unwrap();
            let data = prng_bytes(17, 32768);
            let post = build_post("storage-contract", &[("payload.mkv", data.clone())], 8192);
            let server = NservBuilder::new().with_post(&post).start().await.unwrap();
            let mut cfg = nzbd_config::Config::default();
            cfg.paths.main_dir = tmp.path().join("processing");
            cfg.paths.dest_dir = tmp.path().join("complete");
            cfg.servers = vec![nzbd_config::ServerConfig {
                name: "mock".into(),
                host: "127.0.0.1".into(),
                port: server.port(),
                tls: false,
                ..Default::default()
            }];
            cfg.queue.min_free_disk_mb = 0;
            cfg.post.unpack = false;
            cfg.post.deobfuscate_final = false;
            cfg.categories = vec![nzbd_config::CategoryConfig {
                name: "tv".into(),
                dest_dir: Some(tmp.path().join("library/tv")),
                ..Default::default()
            }];
            let mut engine_cfg = EngineConfig::single_node(
                cfg.server_defs(),
                cfg.state_dir(),
                cfg.download_dir(),
                engine_tuning(&cfg),
                None,
            );
            engine_cfg.disk_guard_roots = disk_guard_roots(&cfg);
            let engine = Engine::spawn(engine_cfg).await.unwrap();
            let mut events = engine.subscribe();
            let job = engine
                .add_nzb_opts(
                    "storage-contract",
                    post.nzb.as_bytes(),
                    AddOpts {
                        category: category.then(|| "TV".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let allocation = engine.artifacts().for_job(job.0).unwrap().unwrap();
            assert_eq!(allocation.root, cfg.paths.main_dir);
            assert!(!cfg.dest_dir().join("storage-contract").exists());
            if tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Event::JobFinished {
                        job: finished,
                        status,
                        ..
                    } = events.recv().await.unwrap()
                    {
                        if finished == job {
                            assert_eq!(status, JobStatus::Completed);
                            break;
                        }
                    }
                }
            })
            .await
            .is_err()
            {
                eprintln!("download timeout snapshot: {:#?}", engine.snapshot());
                eprintln!("download timeout job: {:?}", engine.export_job(job).await);
                eprintln!(
                    "download timeout allocation: {:?}",
                    engine.artifacts().for_job(job.0)
                );
                panic!("download must finish");
            }
            assert_eq!(
                std::fs::read(allocation.path.join("payload.mkv")).unwrap(),
                data
            );
            assert!(!engine.export_job(job).await.unwrap().unwrap().ready());
            let destination = if category {
                tmp.path().join("library/tv")
            } else {
                cfg.dest_dir()
            };
            let published_dir =
                destination.join(format!("storage-contract.runner-{}", allocation.generation));
            assert!(!published_dir.exists());
            if conflict {
                std::fs::create_dir_all(&published_dir).unwrap();
            }
            let history = Arc::new(
                nzbd_state::history::HistoryDb::open(&cfg.state_dir().join("history.sqlite"), None)
                    .unwrap(),
            );
            let mut pp_events = engine.subscribe();
            let result = nzbd_post::manager::process_job(
                &engine,
                &post_config(&cfg, 1, None),
                &history,
                &cfg.download_dir(),
                job,
            )
            .await;
            if conflict {
                assert!(result.is_err());
                assert!(!engine.export_job(job).await.unwrap().unwrap().ready());
                assert_eq!(
                    std::fs::read(allocation.path.join("payload.mkv")).unwrap(),
                    data
                );
                assert!(!published_dir.join("payload.mkv").exists());
            } else {
                assert_eq!(result.unwrap(), nzbd_post::manager::PpFinal::Success);
                let (pp_status, final_dir) = tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        match pp_events
                            .recv()
                            .await
                            .expect("post-processing event stream")
                        {
                            Event::JobPpFinished {
                                job: finished,
                                pp_status,
                                final_dir,
                                ..
                            } if finished == job => break (pp_status, final_dir),
                            _ => continue,
                        }
                    }
                })
                .await
                .expect("durable post-processing completion event");
                assert_eq!(pp_status, "SUCCESS");
                assert_eq!(final_dir.as_deref(), Some(published_dir.to_str().unwrap()));
                assert!(engine.export_job(job).await.unwrap().unwrap().ready());
                assert_eq!(
                    std::fs::read(published_dir.join("payload.mkv")).unwrap(),
                    data
                );
                assert!(!allocation.path.exists());
                assert_eq!(
                    history.list(10).unwrap()[0].final_dir.as_deref(),
                    published_dir.to_str()
                );
            }
            engine.shutdown().await;
        }
    }

    #[tokio::test]
    async fn changed_processing_default_preserves_existing_custody_after_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("processing");
        cfg.paths.dest_dir = tmp.path().join("complete");
        cfg.paths.inter_dir = Some(cfg.dest_dir()); // Reproduce the old allocation root.
        let post = nzbd_nserv::build_post("existing-job", &[("payload.bin", vec![7; 100])], 100);
        let spawn = |cfg: &nzbd_config::Config| {
            Engine::spawn(EngineConfig::single_node(
                Vec::new(),
                cfg.state_dir(),
                cfg.download_dir(),
                Tuning::default(),
                None,
            ))
        };
        let engine = spawn(&cfg).await.unwrap();
        let job = engine
            .add_nzb_opts(
                "existing-job",
                post.nzb.as_bytes(),
                nzbd_engine::AddOpts {
                    paused: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let old = engine.artifacts().for_job(job.0).unwrap().unwrap();
        assert_eq!(old.root, cfg.dest_dir());
        std::fs::write(old.path.join("sentinel"), b"preserve").unwrap();
        engine.shutdown().await;
        drop(engine);

        cfg.paths.inter_dir = None;
        let engine = spawn(&cfg).await.unwrap();
        let existing = engine.artifacts().for_job(job.0).unwrap().unwrap();
        assert_eq!(existing.path, old.path);
        assert_eq!(existing.generation, old.generation);
        assert_eq!(
            std::fs::read(existing.path.join("sentinel")).unwrap(),
            b"preserve"
        );
        assert!(!cfg.download_dir().join("existing-job").exists());
        let new = engine
            .add_nzb_opts(
                "new-job",
                post.nzb.as_bytes(),
                nzbd_engine::AddOpts {
                    paused: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            engine.artifacts().for_job(new.0).unwrap().unwrap().root,
            cfg.paths.main_dir
        );
        engine.shutdown().await;
    }

    #[test]
    fn processing_root_is_covered_by_single_node_and_cluster_capacity_wiring() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("shared/processing");
        cfg.paths.dest_dir = tmp.path().join("shared/complete");
        cfg.cluster.shared_dir = Some(tmp.path().join("shared"));
        cfg.cluster.secret = Some("fixture-secret".into());
        for intermediate in [
            None,
            Some(PathBuf::new()),
            Some(tmp.path().join("shared/intermediate")),
        ] {
            cfg.paths.inter_dir = intermediate;
            assert!(disk_guard_roots(&cfg)
                .iter()
                .any(|r| r.path == cfg.download_dir()));
            cfg.cluster.enabled = true;
            let (cluster, _) = cluster_runtime_config(&cfg).unwrap();
            assert!(cluster
                .disk_guard_roots
                .iter()
                .any(|r| r.path == cfg.download_dir()));
            assert!(cluster
                .disk_guard_roots
                .iter()
                .any(|r| r.path == cfg.dest_dir()));
            assert_eq!(
                post_config(&cfg, 1, None).completed_dir,
                Some(cfg.dest_dir())
            );
            assert!(cluster
                .disk_guard_roots
                .iter()
                .all(|r| !r.path.as_os_str().is_empty()));
            cfg.cluster.enabled = false;
        }
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

        let control =
            control::Controller::start("127.0.0.1:0".parse().unwrap(), nzbd_api::LogBuffer::new(1))
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
                        &control,
                        None,
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
        let control =
            control::Controller::start("127.0.0.1:0".parse().unwrap(), nzbd_api::LogBuffer::new(1))
                .unwrap();
        let err = match run(
            Some(tmp.path().to_path_buf()),
            Some("127.0.0.1:0".into()),
            nzbd_api::LogBuffer::new(1),
            &control,
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
