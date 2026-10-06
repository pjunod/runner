//! Typed TOML configuration + (phase 3) `nzbget.conf` importer.

pub mod durable;

use nzbd_types::{CertLevel, ServerDef, ServerId, TlsMode};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub paths: Paths,
    #[serde(default, rename = "server")]
    pub servers: Vec<ServerConfig>,
    #[serde(default, rename = "category")]
    pub categories: Vec<CategoryConfig>,
    #[serde(default, rename = "feed")]
    pub feeds: Vec<FeedConfig>,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub post: PostSection,
    #[serde(default)]
    pub history: HistorySection,
    #[serde(default)]
    pub cluster: ClusterConfig,
    #[serde(default, skip_serializing_if = "TorrentConfig::is_default")]
    pub torrent: TorrentConfig,
}

/// `[torrent]` — opt-in BitTorrent session settings.
///
/// The daemon must treat this section as inert unless [`TorrentConfig::enabled`]
/// is explicitly enabled. Keeping the gate in the typed configuration makes an
/// omitted section and `enabled = false` equivalent for all future wiring.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct TorrentConfig {
    /// Master feature gate. The safe default performs no torrent networking.
    pub enabled: bool,
    /// The one explicit TCP/IPv4 peer listener port.
    pub listen_port: u16,
    /// Enable distributed hash table peer discovery.
    pub dht: bool,
    pub pex: bool,
    pub local_discovery: bool,
    pub upnp_port_forwarding: bool,
    /// Credential-free `socks5://host:port` origin.
    pub socks_proxy_url: Option<String>,
    pub socks_proxy_username: Option<String>,
    pub socks_proxy_password: Option<String>,
    pub max_peers_per_torrent: u32,
    pub max_peers_total: u32,
    pub max_known_peers_per_torrent: u32,
    pub max_known_peers_total: u32,
    pub upload_limit_kib: u64,
    pub default_seed_ratio: f64,
    pub default_seed_minutes: u64,
    pub stop_seeding_on_complete: bool,
    pub metainfo_max_mib: u32,
    pub source_redirects: u32,
}

impl Default for TorrentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_port: 6881,
            dht: false,
            pex: true,
            local_discovery: false,
            upnp_port_forwarding: false,
            socks_proxy_url: None,
            socks_proxy_username: None,
            socks_proxy_password: None,
            max_peers_per_torrent: 80,
            max_peers_total: 400,
            max_known_peers_per_torrent: 1024,
            max_known_peers_total: 4096,
            upload_limit_kib: 0,
            default_seed_ratio: 0.0,
            default_seed_minutes: 0,
            stop_seeding_on_complete: false,
            metainfo_max_mib: 10,
            source_redirects: 5,
        }
    }
}

impl std::fmt::Debug for TorrentConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TorrentConfig")
            .field("enabled", &self.enabled)
            .field("listen_port", &self.listen_port)
            .field("dht", &self.dht)
            .field("pex", &self.pex)
            .field("local_discovery", &self.local_discovery)
            .field("upnp_port_forwarding", &self.upnp_port_forwarding)
            .field(
                "socks_proxy_url",
                &self.socks_proxy_url.as_ref().map(|_| "<redacted>"),
            )
            .field("socks_proxy_username", &self.socks_proxy_username)
            .field(
                "socks_proxy_password",
                &self.socks_proxy_password.as_ref().map(|_| "***"),
            )
            .field("max_peers_per_torrent", &self.max_peers_per_torrent)
            .field("max_peers_total", &self.max_peers_total)
            .field(
                "max_known_peers_per_torrent",
                &self.max_known_peers_per_torrent,
            )
            .field("max_known_peers_total", &self.max_known_peers_total)
            .field("upload_limit_kib", &self.upload_limit_kib)
            .field("default_seed_ratio", &self.default_seed_ratio)
            .field("stop_seeding_on_complete", &self.stop_seeding_on_complete)
            .field("default_seed_minutes", &self.default_seed_minutes)
            .field("metainfo_max_mib", &self.metainfo_max_mib)
            .field("source_redirects", &self.source_redirects)
            .finish()
    }
}

impl TorrentConfig {
    fn is_default(value: &Self) -> bool {
        value == &Self::default()
    }
}

/// One configured filesystem root the daemon writes to.
///
/// Labels are operator-facing roles (for example `state` or `category: tv`),
/// while paths are already expanded. The engine uses the same inventory as
/// the API storage panel so a displayed filesystem cannot be omitted from the
/// enforcing disk guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRoot {
    pub label: String,
    pub path: PathBuf,
}

/// Bound the number of independent filesystem probes and their detached OS
/// threads. A config with more write roots is almost certainly generated or
/// malformed and is refused before the daemon starts.
pub const MAX_STORAGE_ROOTS: usize = 64;

/// `[history]` — retention and optional persistent local index placement.
/// Listings read SQLite directly; shared portable logs reconcile in the
/// background. Retention bounds stored data and recovery/reconciliation work.
///
/// Two bounds, because they answer different questions and either one
/// alone leaves a hole. A count bound answers "how big may this get" and
/// holds even when a burst arrives in a day; an age bound answers "how far
/// back do I care" and holds even when the daemon is quiet for months.
/// Whichever bites first wins. `0` disables that bound.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct HistorySection {
    /// Persistent local index directory. Logs and parked NZBs stay in state_dir.
    /// Changing this copies the active index at startup, preserving cursors.
    pub index_dir: Option<PathBuf>,
    /// Keep at most this many entries (0 = unlimited).
    pub keep_max: u32,
    /// Drop entries finished more than this many days ago (0 = forever).
    /// NZBGet `KeepHistory`, which is days and means the same thing.
    pub keep_days: u32,
}

impl Default for HistorySection {
    fn default() -> Self {
        // A thousand entries is more than a year of a busy *arr setup and
        // still a JSONL small enough to re-read on a slow mount; ninety
        // days is the window in which anyone asks "did that ever come in?"
        HistorySection {
            index_dir: None,
            keep_max: 1000,
            keep_days: 90,
        }
    }
}

/// `[post]` — post-processing (ARCHITECTURE.md §9): par verify/repair,
/// unpack, cleanup, extension scripts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct PostSection {
    pub enabled: bool,
    pub par2_cmd: String,
    pub unrar_cmd: String,
    pub sevenzip_cmd: String,
    /// Directory holding NZBGet-style extension scripts (legacy header or
    /// v2 `manifest.json`). None = no scripts.
    pub scripts_dir: Option<PathBuf>,
    pub unpack: bool,
    /// Delete archives/par2/sfv after a successful unpack.
    pub cleanup: bool,
    /// Rename still-obfuscated files to the job name after unpack
    /// (SABnzbd-style; fully obfuscated season packs get `<job> - NN`).
    pub deobfuscate_final: bool,
    /// NZBGet `PostStrategy`: sequential | balanced | aggressive | rocket.
    pub strategy: String,
    /// What to do with the FILES of a job that ended in a terminal
    /// failure — par failure, unpack failure, health abort, post crash:
    /// none | park | delete.
    ///
    /// Was `health_action` (NZBGet `HealthCheck`) and still parses under
    /// that name. It only ever governed the health gate, so a par failure
    /// left its whole directory in the category tree forever — which is
    /// how ~523 GB of known-bad downloads accumulated under an importer's
    /// watch folder (docs/REGRAB_LOOP_PLAN.md D2). The default is
    /// `delete`: the bytes are known-bad and the job's NZB is parked with
    /// its history entry, so `requeue` gets them back.
    #[serde(alias = "health_action")]
    pub failure_action: String,
    /// Where `failure_action = "park"` puts a failed job's directory.
    /// Defaults to `<main_dir>/failed` — deliberately off the category
    /// tree an importer watches.
    pub failed_dir: Option<PathBuf>,
    pub tool_timeout_secs: u64,
    pub script_timeout_secs: u64,
    /// How long to wait for delayed par-block downloads during repair.
    pub par_fetch_timeout_secs: u64,
}

impl Default for PostSection {
    fn default() -> Self {
        PostSection {
            enabled: true,
            par2_cmd: "par2".into(),
            unrar_cmd: "unrar".into(),
            sevenzip_cmd: "7z".into(),
            scripts_dir: None,
            unpack: true,
            cleanup: true,
            deobfuscate_final: true,
            strategy: "balanced".into(),
            failure_action: "delete".into(),
            failed_dir: None,
            tool_timeout_secs: 3600,
            script_timeout_secs: 3600,
            par_fetch_timeout_secs: 600,
        }
    }
}

/// `[cluster]` — multi-node work distribution (docs/CLUSTERING.md).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    pub enabled: bool,
    /// Stable identity shared by every node in this cluster.
    pub cluster_id: String,
    /// Unique, stable node name (journal fencing suffix, registry key).
    pub node_name: String,
    /// The shared work volume mount (Gluster).
    pub shared_dir: Option<PathBuf>,
    /// How peers reach this node's API, e.g. "http://10.0.0.11:6789".
    pub advertise_url: String,
    pub secret: Option<String>,
    pub secret_file: Option<PathBuf>,
    /// Eligible for leader election.
    pub coordinator: bool,
    /// Lower = preferred leader (staggers candidacy).
    pub priority: u32,
    pub download: bool,
    pub max_download_jobs: u32,
    /// PP executor role (effective from phase 2 / cluster C2).
    pub post_process: bool,
    pub pp_slots: u32,
    pub lease_interval_secs: u64,
    pub takeover_after_secs: u64,
    pub worker_ttl_secs: u64,
    /// Local durable Raft state. This must not be on the shared payload mount.
    pub control_dir: Option<PathBuf>,
    /// Stable Hiqlite voter id. Every configured peer list must contain it.
    pub control_node_id: u64,
    pub control_raft_bind: String,
    pub control_api_bind: String,
    /// Explicit fixed voter roster. Empty means a single voter using the local
    /// bind addresses; online membership changes are deliberately unsupported.
    pub control_peers: Vec<ClusterControlPeer>,
    /// Positive scheduler weights. Capacity is divided by assigned/weight.
    pub download_weight: u32,
    pub pp_weight: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterControlPeer {
    pub id: u64,
    pub raft_addr: String,
    pub api_addr: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            enabled: false,
            cluster_id: "default".into(),
            node_name: String::new(),
            shared_dir: None,
            advertise_url: String::new(),
            secret: None,
            secret_file: None,
            coordinator: true,
            priority: 10,
            download: true,
            max_download_jobs: 2,
            post_process: true,
            pp_slots: 1,
            lease_interval_secs: 5,
            takeover_after_secs: 20,
            worker_ttl_secs: 30,
            control_dir: None,
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:8810".into(),
            control_api_bind: "127.0.0.1:8820".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
        }
    }
}

impl ClusterConfig {
    /// Resolve the shared secret (inline beats file).
    pub fn resolve_secret(&self) -> Result<String, ConfigError> {
        if let Some(s) = &self.secret {
            return Ok(s.clone());
        }
        if let Some(f) = &self.secret_file {
            return std::fs::read_to_string(expand_home(f))
                .map(|s| s.trim().to_string())
                .map_err(|e| ConfigError::Invalid(format!("secret_file: {e}")));
        }
        Err(ConfigError::Invalid(
            "[cluster] requires secret or secret_file".into(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct Paths {
    pub main_dir: PathBuf,
    pub dest_dir: PathBuf,
    pub inter_dir: Option<PathBuf>,
    pub nzb_watch_dir: Option<PathBuf>,
    pub queue_dir: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
    pub torrent_dir: Option<PathBuf>,
    pub torrent_watch_dir: Option<PathBuf>,
}

impl Default for Paths {
    fn default() -> Self {
        Paths {
            main_dir: PathBuf::from("~/downloads"),
            dest_dir: PathBuf::from("~/downloads/complete"),
            inter_dir: None,
            nzb_watch_dir: None,
            queue_dir: None,
            temp_dir: None,
            torrent_dir: None,
            torrent_watch_dir: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub active: bool,
    pub tier: u8,
    pub group: u8,
    pub fill: bool,
    pub connections: u16,
    pub pipeline_depth: u8,
    pub retention_days: u32,
    pub cert_verification: CertVerification,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            name: String::new(),
            host: String::new(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            connections: 8,
            pipeline_depth: 2,
            retention_days: 0,
            cert_verification: CertVerification::Strict,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CertVerification {
    None,
    Minimal,
    Strict,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct CategoryConfig {
    pub name: String,
    pub dest_dir: Option<PathBuf>,
    pub unpack: Option<bool>,
    pub extensions: Vec<String>,
    pub torrent_dir: Option<PathBuf>,
    pub seed_ratio: Option<f64>,
    pub seed_minutes: Option<u64>,
    pub stop_seeding_on_complete: Option<bool>,
}

/// `[[feed]]` — an RSS/Atom indexer feed with an NZBGet-style filter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct FeedConfig {
    pub name: String,
    pub url: String,
    pub interval_mins: u64,
    /// Filter script (Accept/Reject/Require lines); empty = accept all.
    pub filter: String,
    pub category: Option<String>,
    pub priority: i32,
    pub pause: bool,
}

impl Default for FeedConfig {
    fn default() -> Self {
        FeedConfig {
            name: String::new(),
            url: String::new(),
            interval_mins: 15,
            filter: String::new(),
            category: None,
            priority: 0,
            pause: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct QueueConfig {
    pub article_retries: u8,
    pub retry_interval_secs: u64,
    pub article_timeout_secs: u64,
    pub article_cache_mb: u64,
    pub direct_write: bool,
    pub crc_check: bool,
    pub continue_partial: bool,
    pub propagation_delay_mins: u32,
    pub min_free_disk_mb: u64,
    pub speed_limit_kib: Option<u64>,
    /// How many jobs may download at the same time (1..=100).
    ///
    /// `1` is nzbd's historical behavior and the default: the top job
    /// takes every connection until it runs out of segments to hand out.
    /// Raising it splits the connection pool evenly across that many
    /// jobs — priority still decides WHICH jobs, this decides how many.
    ///
    /// It does not make anything faster. The same connections move the
    /// same bytes; they arrive spread over several jobs instead of
    /// finishing one at a time, so first-completion gets slower and
    /// everything-completes stays put. Raise it when you want several
    /// things moving, not when you want more throughput.
    pub max_active_downloads: u32,
    /// Daily/monthly download quotas in MB (0 = unlimited); NZBGet
    /// `DailyQuota` / `MonthlyQuota` / `QuotaStartDay`.
    pub daily_quota_mb: u64,
    pub monthly_quota_mb: u64,
    pub quota_start_day: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        // NZBGet-compatible defaults (ARCHITECTURE.md §3.3)
        QueueConfig {
            article_retries: 3,
            retry_interval_secs: 10,
            article_timeout_secs: 60,
            article_cache_mb: 0,
            direct_write: true,
            crc_check: true,
            continue_partial: true,
            propagation_delay_mins: 0,
            min_free_disk_mb: 250,
            speed_limit_kib: None,
            max_active_downloads: 1,
            daily_quota_mb: 0,
            monthly_quota_mb: 0,
            quota_start_day: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ApiConfig {
    pub bind: String,
    /// Advertise this API on the local network using DNS-SD/mDNS.
    /// Loopback-only listeners are never advertised because other devices
    /// cannot reach them.
    pub discovery: bool,
    /// Serve HTTPS instead of HTTP. With no cert configured, a
    /// self-signed certificate is generated on first boot (under the
    /// state dir) and reused after that — trust it on your devices for
    /// full PWA install. NZBGet `SecureControl`.
    pub tls: bool,
    /// PEM certificate chain / private key (NZBGet `SecureCert`/`SecureKey`).
    /// Both empty + `tls = true` = self-signed auto-generation.
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// Extra subject-alt-names for the generated certificate (hostnames
    /// and/or IPs you'll browse to). `localhost` is always included.
    pub tls_sans: Vec<String>,
    /// Report this version string on the compat shim's `version` method.
    pub compat_version: String,
    /// Opt-in legacy default credentials for migration (off by default).
    pub allow_legacy_default_credentials: bool,
    /// HTTP Basic auth (NZBGet `ControlUsername`/`ControlPassword`).
    /// Auth is enforced when a password is set; `/healthz` stays open.
    pub username: String,
    pub password: Option<String>,
    /// Bearer token accepted as an alternative to Basic auth.
    pub token: Option<String>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        ApiConfig {
            bind: "127.0.0.1:6789".into(),
            discovery: true,
            tls: false,
            tls_cert: None,
            tls_key: None,
            tls_sans: Vec::new(),
            compat_version: "26.2".into(),
            allow_legacy_default_credentials: false,
            username: "nzbd".into(),
            password: None,
            token: None,
        }
    }
}

/// Expand a leading `~`/`~/` to `$HOME` (config-file ergonomics).
pub fn expand_home(p: &std::path::Path) -> PathBuf {
    let Some(s) = p.to_str() else {
        return p.to_path_buf();
    };
    if s == "~" || s.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            if s == "~" {
                return PathBuf::from(home);
            }
            return PathBuf::from(home).join(&s[2..]);
        }
    }
    p.to_path_buf()
}

impl Config {
    pub fn from_toml(s: &str) -> Result<Config, ConfigError> {
        let cfg = Config::parse_toml_unvalidated(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse without validating.
    ///
    /// Exactly one caller should want this: the settings editor, which
    /// receives a config whose secrets are still [`SECRET_MASK`] and has
    /// to parse it *before* [`merge_masked_secrets`] can put the real
    /// ones back. Validation rejects the mask (see [`Config::validate`]),
    /// so the normal `from_toml` cannot be used there. Everything else —
    /// boot, imports, round-trip checks — must keep using `from_toml`.
    pub fn parse_toml_unvalidated(s: &str) -> Result<Config, ConfigError> {
        Ok(toml::from_str(s)?)
    }

    /// Journal + queue snapshots directory (NZBGet `QueueDir` equivalent):
    /// `paths.queue_dir`, defaulting to `<main_dir>/queue`.
    pub fn state_dir(&self) -> PathBuf {
        match &self.paths.queue_dir {
            Some(d) => expand_home(d),
            None => expand_home(&self.paths.main_dir).join("queue"),
        }
    }

    pub fn dest_dir(&self) -> PathBuf {
        expand_home(&self.paths.dest_dir)
    }

    /// Usenet processing root: nonempty InterDir, otherwise MainDir.
    pub fn download_dir(&self) -> PathBuf {
        self.paths
            .inter_dir
            .as_ref()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| expand_home(p))
            .unwrap_or_else(|| expand_home(&self.paths.main_dir))
    }

    /// Immutable torrent payload root, independent from the Usenet handoff.
    pub fn torrent_dir(&self) -> PathBuf {
        self.paths
            .torrent_dir
            .as_ref()
            .map(|path| expand_home(path))
            .unwrap_or_else(|| expand_home(&self.paths.main_dir).join("torrents"))
    }

    /// Resolve seed policy in per-add → category → global order.
    pub fn torrent_seed_limits(
        &self,
        category_name: Option<&str>,
        per_add_ratio: Option<f64>,
        per_add_minutes: Option<u64>,
    ) -> (f64, u64) {
        let category = category_name.and_then(|name| {
            self.categories
                .iter()
                .find(|category| category.name == name)
        });
        (
            per_add_ratio
                .or_else(|| category.and_then(|category| category.seed_ratio))
                .unwrap_or(self.torrent.default_seed_ratio),
            per_add_minutes
                .or_else(|| category.and_then(|category| category.seed_minutes))
                .unwrap_or(self.torrent.default_seed_minutes),
        )
    }

    /// Every configured filesystem root the daemon can write.
    ///
    /// Exact duplicate paths are kept once with their role labels joined: they
    /// are one failure domain and probing them repeatedly would add no safety.
    /// Filesystem-level grouping still happens at measurement time where the
    /// platform exposes a stable filesystem identity because distinct
    /// configured paths can live on the same mounted volume. Windows keeps
    /// distinct paths as conservative separate rows.
    pub fn storage_roots(&self) -> Vec<StorageRoot> {
        let mut roots = Vec::<StorageRoot>::new();
        let mut push = |label: String, path: PathBuf| {
            if let Some(root) = roots.iter_mut().find(|root| root.path == path) {
                root.label.push_str(" · ");
                root.label.push_str(&label);
            } else {
                roots.push(StorageRoot { label, path });
            }
        };
        push("state".into(), self.state_dir());
        if self.cluster.enabled {
            if let Some(path) = &self.cluster.shared_dir {
                push(
                    "cluster state".into(),
                    expand_home(path).join(".nzbd-cluster"),
                );
            }
        }
        push("downloads".into(), self.dest_dir());
        push("working".into(), expand_home(&self.paths.main_dir));
        push(
            "failed".into(),
            self.post
                .failed_dir
                .as_ref()
                .map(|path| expand_home(path))
                .unwrap_or_else(|| expand_home(&self.paths.main_dir).join("failed")),
        );
        if let Some(path) = self
            .paths
            .inter_dir
            .as_ref()
            .filter(|p| !p.as_os_str().is_empty())
        {
            push("intermediate".into(), expand_home(path));
        }
        if let Some(path) = &self.paths.temp_dir {
            push("temporary".into(), expand_home(path));
        }
        if let Some(path) = &self.paths.nzb_watch_dir {
            push("watch".into(), expand_home(path));
        }
        for category in &self.categories {
            if let Some(path) = &category.dest_dir {
                push(format!("category: {}", category.name), expand_home(path));
            }
        }
        if self.torrent.enabled {
            push("torrent payloads".into(), self.torrent_dir());
            push("torrent control".into(), self.state_dir().join("torrents"));
            if let Some(path) = &self.paths.torrent_watch_dir {
                push("torrent watch".into(), expand_home(path));
            }
            for category in &self.categories {
                if let Some(path) = &category.torrent_dir {
                    push(
                        format!("torrent category: {}", category.name),
                        expand_home(path),
                    );
                }
            }
        }
        roots
    }

    /// Configured speed limit in bytes/sec.
    pub fn speed_limit_bps(&self) -> Option<u64> {
        self.queue.speed_limit_kib.map(|k| k * 1024)
    }

    /// The configured concurrent-download cap, clamped into range.
    ///
    /// Always `Some`: unlike the speed limit there is no "unset" — the
    /// default of 1 is a real policy, so the file always outranks a
    /// value left in the snapshot by a runtime nudge.
    pub fn max_active_downloads(&self) -> Option<u32> {
        Some(self.queue.max_active_downloads.clamp(1, 100))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        // A secret that is still the display mask is not a secret. The
        // settings editor shows every password as SECRET_MASK, so the TOML
        // you copy out of it is NOT a backup — and a config restored from
        // that copy used to be accepted verbatim, leaving the daemon
        // authenticating with the literal string "***unchanged***" against
        // a provider account that was perfectly fine (field report
        // 2026-07-26: "it imported the config file but lost my password").
        // Refuse it, and say which field and why.
        let masked = |v: &Option<String>| v.as_deref() == Some(SECRET_MASK);
        let mask_hit = |field: String| -> Result<(), ConfigError> {
            Err(ConfigError::Invalid(format!(
                "{field} is still the placeholder {SECRET_MASK:?} — this config was \
                 copied from the settings editor, which masks every secret. The \
                 masked text is a display, not a backup: put the real value back \
                 (or delete the line to run without one)."
            )))
        };
        for s in &self.servers {
            if masked(&s.password) {
                mask_hit(format!("server '{}' password", s.name))?;
            }
        }
        if masked(&self.api.password) {
            mask_hit("[api] password".into())?;
        }
        if masked(&self.api.token) {
            mask_hit("[api] token".into())?;
        }
        if masked(&self.cluster.secret) {
            mask_hit("[cluster] secret".into())?;
        }
        if masked(&self.torrent.socks_proxy_password) {
            mask_hit("[torrent] socks_proxy_password".into())?;
        }
        for s in &self.servers {
            if s.host.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "server '{}' has no host",
                    s.name
                )));
            }
            if s.connections == 0 {
                return Err(ConfigError::Invalid(format!(
                    "server '{}' has zero connections",
                    s.name
                )));
            }
        }
        for f in &self.feeds {
            if f.name.trim().is_empty() || f.url.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "[[feed]] requires name and url".into(),
                ));
            }
        }
        let storage_roots = self.storage_roots();
        if storage_roots.len() > MAX_STORAGE_ROOTS {
            return Err(ConfigError::Invalid(format!(
                "configured write-root count {} exceeds the safety limit {}",
                storage_roots.len(),
                MAX_STORAGE_ROOTS
            )));
        }
        if self.cluster.enabled {
            if self.cluster.cluster_id.trim().is_empty() {
                return Err(ConfigError::Invalid("[cluster] requires cluster_id".into()));
            }
            if self.cluster.node_name.trim().is_empty() {
                return Err(ConfigError::Invalid("[cluster] requires node_name".into()));
            }
            if self.cluster.shared_dir.is_none() {
                return Err(ConfigError::Invalid("[cluster] requires shared_dir".into()));
            }
            if self.cluster.advertise_url.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "[cluster] requires advertise_url".into(),
                ));
            }
            if self.cluster.secret.is_none() && self.cluster.secret_file.is_none() {
                return Err(ConfigError::Invalid(
                    "[cluster] requires secret or secret_file".into(),
                ));
            }
            if self.cluster.control_node_id == 0 {
                return Err(ConfigError::Invalid(
                    "[cluster] control_node_id must be greater than zero".into(),
                ));
            }
            if self.cluster.download_weight == 0 || self.cluster.pp_weight == 0 {
                return Err(ConfigError::Invalid(
                    "[cluster] download_weight and pp_weight must be greater than zero".into(),
                ));
            }
            if !self.cluster.control_peers.is_empty() {
                let mut ids = std::collections::HashSet::new();
                for peer in &self.cluster.control_peers {
                    if peer.id == 0
                        || peer.raft_addr.trim().is_empty()
                        || peer.api_addr.trim().is_empty()
                        || !ids.insert(peer.id)
                    {
                        return Err(ConfigError::Invalid(
                            "[cluster] control_peers require unique positive ids and both addresses"
                                .into(),
                        ));
                    }
                }
                if !ids.contains(&self.cluster.control_node_id) {
                    return Err(ConfigError::Invalid(
                        "[cluster] control_peers must contain control_node_id".into(),
                    ));
                }
            }
        }
        self.validate_torrent()?;
        Ok(())
    }

    fn validate_torrent(&self) -> Result<(), ConfigError> {
        let torrent = &self.torrent;
        if torrent.listen_port == 0 || torrent.listen_port == u16::MAX {
            return Err(ConfigError::Invalid(
                "[torrent] listen_port must be one explicit port between 1 and 65534".into(),
            ));
        }
        if torrent.upnp_port_forwarding {
            return Err(ConfigError::Invalid(
                "[torrent] upnp_port_forwarding is unavailable in the maintained backend".into(),
            ));
        }
        if torrent.local_discovery {
            return Err(ConfigError::Invalid(
                "[torrent] local_discovery is unavailable in the maintained backend".into(),
            ));
        }
        if torrent.max_peers_per_torrent == 0
            || torrent.max_peers_total == 0
            || torrent.max_known_peers_per_torrent == 0
            || torrent.max_known_peers_total == 0
        {
            return Err(ConfigError::Invalid(
                "[torrent] peer ceilings must be greater than zero".into(),
            ));
        }
        if torrent.max_peers_per_torrent > torrent.max_peers_total
            || torrent.max_known_peers_per_torrent > torrent.max_known_peers_total
            || torrent.max_peers_per_torrent > torrent.max_known_peers_per_torrent
            || torrent.max_peers_total > torrent.max_known_peers_total
        {
            return Err(ConfigError::Invalid(
                "[torrent] peer ceilings must satisfy live-per-torrent <= live-total and live <= retained".into(),
            ));
        }
        if !torrent.default_seed_ratio.is_finite() || torrent.default_seed_ratio < 0.0 {
            return Err(ConfigError::Invalid(
                "[torrent] default_seed_ratio must be a finite non-negative number".into(),
            ));
        }
        let rqbit_max_kib = u64::from(u32::MAX) / 1024;
        if torrent.upload_limit_kib > rqbit_max_kib {
            return Err(ConfigError::Invalid(format!(
                "[torrent] upload_limit_kib must not exceed {rqbit_max_kib}"
            )));
        }
        if torrent.enabled
            && self
                .queue
                .speed_limit_kib
                .is_some_and(|limit| limit > rqbit_max_kib)
        {
            return Err(ConfigError::Invalid(format!(
                "[queue] speed_limit_kib must not exceed {rqbit_max_kib} while the torrent backend is configured"
            )));
        }
        if !(1..=100).contains(&torrent.metainfo_max_mib) {
            return Err(ConfigError::Invalid(
                "[torrent] metainfo_max_mib must be between 1 and 100".into(),
            ));
        }
        if torrent.source_redirects > 5 {
            return Err(ConfigError::Invalid(
                "[torrent] source_redirects must be between 0 and 5".into(),
            ));
        }
        if torrent.default_seed_minutes > u64::MAX / 60 {
            return Err(ConfigError::Invalid(
                "[torrent] default_seed_minutes is too large".into(),
            ));
        }
        match (
            &torrent.socks_proxy_url,
            &torrent.socks_proxy_username,
            &torrent.socks_proxy_password,
        ) {
            (None, None, None) => {}
            (Some(origin), None, None) => validate_proxy_origin(origin)?,
            (Some(origin), Some(username), Some(password)) => {
                validate_proxy_origin(origin)?;
                if username.is_empty()
                    || !username.bytes().all(is_url_unreserved)
                    || !password.bytes().all(is_url_unreserved)
                {
                    return Err(ConfigError::Invalid(
                        "[torrent] proxy credentials must be non-empty URL-unreserved ASCII".into(),
                    ));
                }
            }
            _ => {
                return Err(ConfigError::Invalid(
                    "[torrent] proxy username and password must be set together with socks_proxy_url".into(),
                ));
            }
        }
        if torrent.socks_proxy_url.is_some() && torrent.dht {
            return Err(ConfigError::Invalid(
                "[torrent] dht must be false when a SOCKS proxy is configured".into(),
            ));
        }
        for category in &self.categories {
            if let Some(ratio) = category.seed_ratio {
                if !ratio.is_finite() || ratio < 0.0 {
                    return Err(ConfigError::Invalid(format!(
                        "category '{}' seed_ratio must be a finite non-negative number",
                        category.name
                    )));
                }
            }
            if category
                .seed_minutes
                .is_some_and(|minutes| minutes > u64::MAX / 60)
            {
                return Err(ConfigError::Invalid(format!(
                    "category '{}' seed_minutes is too large",
                    category.name
                )));
            }
        }
        if torrent.enabled && self.cluster.enabled {
            return Err(ConfigError::Invalid(
                "[torrent] cannot be enabled while [cluster] is enabled; M6 is not approved".into(),
            ));
        }
        Ok(())
    }

    pub fn server_defs(&self) -> Vec<ServerDef> {
        self.servers
            .iter()
            .enumerate()
            .map(|(i, s)| ServerDef {
                id: ServerId(i as u32 + 1),
                name: s.name.clone(),
                host: s.host.clone(),
                port: s.port,
                tls: if s.tls { TlsMode::Tls } else { TlsMode::None },
                username: s.username.clone(),
                password: s.password.clone(),
                active: s.active,
                tier: s.tier,
                group: s.group,
                fill: s.fill,
                max_connections: s.connections,
                pipeline_depth: s.pipeline_depth.max(1),
                retention_days: s.retention_days,
                cert_verification: match s.cert_verification {
                    CertVerification::None => CertLevel::None,
                    CertVerification::Minimal => CertLevel::Minimal,
                    CertVerification::Strict => CertLevel::Strict,
                },
            })
            .collect()
    }
}

fn is_url_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn validate_proxy_origin(origin: &str) -> Result<(), ConfigError> {
    let url = url::Url::parse(origin)
        .map_err(|_| ConfigError::Invalid("[torrent] socks_proxy_url is not valid".into()))?;
    if url.scheme() != "socks5"
        || url.host_str().is_none()
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::Invalid(
            "[torrent] socks_proxy_url must be a credential-free socks5://host:port origin".into(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// nzbget.conf importer (ARCHITECTURE.md §11)
// ---------------------------------------------------------------------------

/// What the importer did with each nzbget.conf option.
#[derive(Debug, Default)]
pub struct ImportReport {
    /// Options mapped onto nzbd config (nzbget key → nzbd setting).
    pub mapped: Vec<(String, String)>,
    /// Recognized-but-intentionally-unmapped options (defaults differ or
    /// the feature is built-in) — safe to ignore.
    pub skipped: Vec<String>,
    /// Options nzbd does not know (yet) — review these by hand.
    pub unknown: Vec<String>,
    /// Anything suspicious (unparsable values, missing hosts, …).
    pub warnings: Vec<String>,
}

/// Map `nzbget.conf` (KEY=value lines with `${Var}` substitution plus
/// `ServerN.*`/`CategoryN.*` blocks) onto [`Config`] with a full report.
pub fn import_nzbget_conf(content: &str) -> Result<(Config, ImportReport), ConfigError> {
    // Pass 1: raw key/value pairs (last one wins, like NZBGet).
    let mut raw: Vec<(String, String)> = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            raw.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    // ${Var} substitution against earlier keys (NZBGet semantics).
    let lookup: std::collections::HashMap<String, String> = raw
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .collect();
    let expand_once = |v: &str| -> String {
        let mut out = String::with_capacity(v.len());
        let mut rest = v;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            match rest[start + 2..].find('}') {
                Some(end) => {
                    let var = &rest[start + 2..start + 2 + end];
                    match lookup.get(&var.to_lowercase()) {
                        Some(val) => out.push_str(val),
                        None => {
                            out.push_str("${");
                            out.push_str(var);
                            out.push('}');
                        }
                    }
                    rest = &rest[start + 2 + end + 1..];
                }
                None => {
                    out.push_str(&rest[start..]);
                    rest = "";
                }
            }
        }
        out.push_str(rest);
        out
    };
    // Nested references (`Category1.DestDir=${DestDir}/movies` where
    // DestDir itself uses ${MainDir}) expand to a fixpoint, cycle-bounded.
    let expand = |v: &str| -> String {
        let mut cur = v.to_string();
        for _ in 0..10 {
            let next = expand_once(&cur);
            if next == cur {
                break;
            }
            cur = next;
        }
        cur
    };

    let mut cfg = Config::default();
    let mut report = ImportReport::default();
    let mut servers: std::collections::BTreeMap<u32, ServerConfig> = Default::default();
    let mut categories: std::collections::BTreeMap<u32, CategoryConfig> = Default::default();
    let mut feeds: std::collections::BTreeMap<u32, FeedConfig> = Default::default();
    let mut control_ip = "127.0.0.1".to_string();
    let mut control_port = "6789".to_string();

    let yes = |v: &str| v.eq_ignore_ascii_case("yes");
    for (key, rawv) in &raw {
        let v = expand(rawv);
        let lk = key.to_lowercase();

        // ServerN.* / CategoryN.* blocks
        if let Some(rest) = lk.strip_prefix("server") {
            if let Some((n, field)) = rest.split_once('.') {
                if let Ok(n) = n.parse::<u32>() {
                    let s = servers.entry(n).or_default();
                    let mapped = match field {
                        "name" => {
                            s.name = v.clone();
                            true
                        }
                        "host" => {
                            s.host = v.clone();
                            true
                        }
                        "port" => {
                            s.port = v.parse().unwrap_or(119);
                            true
                        }
                        "username" => {
                            s.username = (!v.is_empty()).then(|| v.clone());
                            true
                        }
                        "password" => {
                            s.password = (!v.is_empty()).then(|| v.clone());
                            true
                        }
                        "encryption" => {
                            s.tls = yes(&v);
                            true
                        }
                        "connections" => {
                            s.connections = v.parse().unwrap_or(4);
                            true
                        }
                        "level" => {
                            s.tier = v.parse().unwrap_or(0);
                            true
                        }
                        "group" => {
                            s.group = v.parse().unwrap_or(0);
                            true
                        }
                        "optional" => {
                            s.fill = yes(&v);
                            true
                        }
                        "retention" => {
                            s.retention_days = v.parse().unwrap_or(0);
                            true
                        }
                        "active" => {
                            s.active = yes(&v);
                            true
                        }
                        "certverification" => {
                            s.cert_verification = match v.to_lowercase().as_str() {
                                "none" => CertVerification::None,
                                "minimal" => CertVerification::Minimal,
                                _ => CertVerification::Strict,
                            };
                            true
                        }
                        "jointgroup" | "cipher" | "ipversion" | "notes" => {
                            report.skipped.push(key.clone());
                            false
                        }
                        _ => {
                            report.unknown.push(key.clone());
                            false
                        }
                    };
                    if mapped {
                        report
                            .mapped
                            .push((key.clone(), format!("server[{n}].{field}")));
                    }
                    continue;
                }
            }
        }
        if let Some(rest) = lk.strip_prefix("feed") {
            if let Some((n, field)) = rest.split_once('.') {
                if let Ok(n) = n.parse::<u32>() {
                    let f = feeds.entry(n).or_default();
                    let mapped = match field {
                        "name" => {
                            f.name = v.clone();
                            true
                        }
                        "url" => {
                            f.url = v.clone();
                            true
                        }
                        "interval" => {
                            f.interval_mins = v.parse().unwrap_or(15);
                            true
                        }
                        "filter" => {
                            f.filter = v.replace('%', "\n");
                            true
                        }
                        "category" => {
                            f.category = (!v.is_empty()).then(|| v.clone());
                            true
                        }
                        "priority" => {
                            f.priority = v.parse().unwrap_or(0);
                            true
                        }
                        "pausenzb" => {
                            f.pause = yes(&v);
                            true
                        }
                        "backlog" | "extensions" => {
                            report.skipped.push(key.clone());
                            false
                        }
                        _ => {
                            report.unknown.push(key.clone());
                            false
                        }
                    };
                    if mapped {
                        report
                            .mapped
                            .push((key.clone(), format!("feed[{n}].{field}")));
                    }
                    continue;
                }
            }
        }
        if let Some(rest) = lk.strip_prefix("category") {
            if let Some((n, field)) = rest.split_once('.') {
                if let Ok(n) = n.parse::<u32>() {
                    let c = categories.entry(n).or_default();
                    let mapped = match field {
                        "name" => {
                            c.name = v.clone();
                            true
                        }
                        "destdir" => {
                            c.dest_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                            true
                        }
                        "unpack" => {
                            c.unpack = Some(yes(&v));
                            true
                        }
                        "extensions" => {
                            c.extensions = v
                                .split(',')
                                .map(|e| e.trim().to_string())
                                .filter(|e| !e.is_empty())
                                .collect();
                            true
                        }
                        "aliases" => {
                            report.skipped.push(key.clone());
                            false
                        }
                        _ => {
                            report.unknown.push(key.clone());
                            false
                        }
                    };
                    if mapped {
                        report
                            .mapped
                            .push((key.clone(), format!("category[{n}].{field}")));
                    }
                    continue;
                }
            }
        }

        // Scalar options
        let mapped_to: Option<String> = match lk.as_str() {
            "maindir" => {
                cfg.paths.main_dir = PathBuf::from(&v);
                Some("paths.main_dir".into())
            }
            "destdir" => {
                cfg.paths.dest_dir = PathBuf::from(&v);
                Some("paths.dest_dir".into())
            }
            "interdir" => {
                cfg.paths.inter_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.inter_dir".into())
            }
            "nzbdir" => {
                cfg.paths.nzb_watch_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.nzb_watch_dir".into())
            }
            "queuedir" => {
                cfg.paths.queue_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.queue_dir".into())
            }
            "tempdir" => {
                cfg.paths.temp_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.temp_dir".into())
            }
            "controlip" => {
                control_ip = if v == "0.0.0.0" || v.is_empty() {
                    "0.0.0.0".into()
                } else {
                    v.clone()
                };
                Some("api.bind (ip)".into())
            }
            "controlport" => {
                control_port = v.clone();
                Some("api.bind (port)".into())
            }
            "articleretries" | "retries" => {
                cfg.queue.article_retries = v.parse().unwrap_or(3);
                Some("queue.article_retries".into())
            }
            "articleinterval" | "retryinterval" => {
                cfg.queue.retry_interval_secs = v.parse().unwrap_or(10);
                Some("queue.retry_interval_secs".into())
            }
            "articletimeout" => {
                cfg.queue.article_timeout_secs = v.parse().unwrap_or(60);
                Some("queue.article_timeout_secs".into())
            }
            "articlecache" => {
                cfg.queue.article_cache_mb = v.parse().unwrap_or(0);
                Some("queue.article_cache_mb".into())
            }
            "directwrite" => {
                cfg.queue.direct_write = yes(&v);
                Some("queue.direct_write".into())
            }
            "crccheck" => {
                cfg.queue.crc_check = yes(&v);
                Some("queue.crc_check".into())
            }
            "continuepartial" => {
                cfg.queue.continue_partial = yes(&v);
                Some("queue.continue_partial".into())
            }
            "propagationdelay" => {
                cfg.queue.propagation_delay_mins = v.parse().unwrap_or(0);
                Some("queue.propagation_delay_mins".into())
            }
            "diskspace" => {
                cfg.queue.min_free_disk_mb = v.parse().unwrap_or(250);
                Some("queue.min_free_disk_mb".into())
            }
            "dailyquota" => {
                cfg.queue.daily_quota_mb = v.parse().unwrap_or(0);
                Some("queue.daily_quota_mb".into())
            }
            "monthlyquota" => {
                cfg.queue.monthly_quota_mb = v.parse().unwrap_or(0);
                Some("queue.monthly_quota_mb".into())
            }
            "quotastartday" => {
                cfg.queue.quota_start_day = v.parse().unwrap_or(1);
                Some("queue.quota_start_day".into())
            }
            // Was on the "recognized but skipped" list because nzbd kept
            // history forever and had nothing to map it onto. It does now,
            // and the units already agree: NZBGet's KeepHistory is days.
            "keephistory" => {
                cfg.history.keep_days = v.parse().unwrap_or(0);
                Some("history.keep_days".into())
            }
            "downloadrate" => {
                let kib: u64 = v.parse().unwrap_or(0);
                cfg.queue.speed_limit_kib = (kib > 0).then_some(kib);
                Some("queue.speed_limit_kib".into())
            }
            "unrarcmd" => {
                cfg.post.unrar_cmd = v.clone();
                Some("post.unrar_cmd".into())
            }
            "sevenzipcmd" => {
                cfg.post.sevenzip_cmd = v.clone();
                Some("post.sevenzip_cmd".into())
            }
            "scriptdir" => {
                cfg.post.scripts_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("post.scripts_dir".into())
            }
            "unpack" => {
                cfg.post.unpack = yes(&v);
                Some("post.unpack".into())
            }
            "healthcheck" => {
                cfg.post.failure_action = v.to_lowercase();
                Some("post.failure_action".into())
            }
            "unpackcleanupdisk" => {
                cfg.post.cleanup = yes(&v);
                Some("post.cleanup".into())
            }
            "poststrategy" => {
                cfg.post.strategy = v.to_lowercase();
                Some("post.strategy".into())
            }
            "securecontrol" => {
                cfg.api.tls = yes(&v);
                Some("api.tls".into())
            }
            "securecert" => {
                cfg.api.tls_cert = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("api.tls_cert".into())
            }
            "securekey" => {
                cfg.api.tls_key = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("api.tls_key".into())
            }
            // Recognized, intentionally unmapped (built-in, obsolete, or a
            // policy nzbd handles differently).
            "parcheck"
            | "parrepair"
            | "parscan"
            | "parbuffer"
            | "parthreads"
            | "parquick"
            | "parrename"
            | "rarrename"
            | "directunpack"
            | "scriptorder"
            | "extensions"
            | "shelloverride"
            | "eventinterval"
            | "umask"
            | "daemonusername"
            | "lockfile"
            | "logfile"
            | "writelog"
            | "rotatelog"
            | "errortarget"
            | "warningtarget"
            | "infotarget"
            | "detailtarget"
            | "debugtarget"
            | "nzblog"
            | "crashtrace"
            | "crashdump"
            | "timecorrection"
            | "outputmode"
            | "curses"
            | "updatecheck"
            | "appbin"
            | "appdir"
            | "version"
            | "configfile"
            | "webdir"
            | "confighome"
            | "secureport"
            | "certstore"
            | "certcheck"
            | "authorizedip"
            | "controlusername"
            | "controlpassword"
            | "restrictedusername"
            | "restrictedpassword"
            | "addusername"
            | "addpassword"
            | "formauth"
            | "urlconnections"
            | "urlforce"
            | "urlinterval"
            | "urltimeout"
            | "remotetimeout"
            | "downloadqueue"
            | "reloadqueue"
            | "flushqueue"
            | "dupecheck"
            | "tempdircleanup"
            | "feedhistory"
            | "skipwrite"
            | "rawarticle"
            | "articlereadchunksize"
            | "nzbdirinterval"
            | "nzbdirfilesage"
            // DirectRename's during-download variant is covered by the PP
            // rename stage (par/rar rename are always-on in nzbd).
            | "directrename"
            | "dupescope" => {
                report.skipped.push(key.clone());
                None
            }
            _ => {
                report.unknown.push(key.clone());
                None
            }
        };
        if let Some(target) = mapped_to {
            report.mapped.push((key.clone(), target));
        }
    }

    cfg.api.bind = format!("{control_ip}:{control_port}");
    cfg.servers = servers.into_values().collect();
    cfg.categories = categories
        .into_values()
        .filter(|c| !c.name.is_empty())
        .collect();
    cfg.feeds = feeds
        .into_values()
        .filter(|f| !f.name.is_empty() && !f.url.is_empty())
        .collect();

    for (i, s) in cfg.servers.iter().enumerate() {
        if s.host.is_empty() {
            report
                .warnings
                .push(format!("server #{} has no host — dropped", i + 1));
        }
    }
    cfg.servers.retain(|s| !s.host.is_empty());
    for s in &mut cfg.servers {
        if s.connections == 0 {
            report
                .warnings
                .push(format!("server '{}': Connections=0 raised to 1", s.name));
            s.connections = 1;
        }
    }

    cfg.validate()?;
    Ok((cfg, report))
}

/// Render a [`Config`] as nzbd.toml text.
pub fn to_toml(cfg: &Config) -> Result<String, ConfigError> {
    let text =
        toml::to_string_pretty(cfg).map_err(|e| ConfigError::Invalid(format!("serialize: {e}")))?;
    // Empty top-level arrays ("category = []") are just noise in a file
    // people read and edit; the parser defaults them anyway.
    Ok(text
        .lines()
        .filter(|l| !matches!(l.trim(), "category = []" | "feed = []" | "server = []"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim_start()
        .to_string()
        + "\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[paths]
main_dir = "/data/usenet"
dest_dir = "/data/usenet/complete"

[[server]]
name = "primary"
host = "news.provider.example"
port = 563
tls = true
username = "u"
password = "p"
connections = 30
pipeline_depth = 4

[[server]]
name = "block"
host = "fill.provider.example"
tier = 1
fill = true
connections = 8

[queue]
article_cache_mb = 512

[api]
bind = "0.0.0.0:6789"
"#;

    #[test]
    fn intermediate_directory_defaults_to_main_and_preserves_completion_destination() {
        for (intermediate, expected) in [
            ("", "/processing"),
            ("inter_dir = \"\"", "/processing"),
            (
                "inter_dir = \"/configured-intermediate\"",
                "/configured-intermediate",
            ),
        ] {
            let cfg = Config::from_toml(&format!(
                "[paths]\nmain_dir = \"/processing\"\ndest_dir = \"/complete\"\n{intermediate}"
            ))
            .unwrap();
            assert_eq!(cfg.download_dir(), PathBuf::from(expected));
            assert_eq!(cfg.dest_dir(), PathBuf::from("/complete"));
            let roots = cfg.storage_roots();
            assert!(roots.iter().any(|r| r.path == cfg.download_dir()));
            assert!(roots.iter().any(|r| r.path == cfg.dest_dir()));
            assert!(roots.iter().all(|r| !r.path.as_os_str().is_empty()));
        }
    }

    #[test]
    fn processing_and_completion_paths_expand_home_independently() {
        let home = PathBuf::from(std::env::var("HOME").unwrap());
        let mut cfg =
            Config::from_toml("[paths]\nmain_dir = \"~/processing\"\ndest_dir = \"~/complete\"")
                .unwrap();
        for intermediate in [None, Some(PathBuf::new())] {
            cfg.paths.inter_dir = intermediate;
            assert_eq!(cfg.download_dir(), home.join("processing"));
            assert_eq!(cfg.dest_dir(), home.join("complete"));
        }
        cfg.paths.inter_dir = Some("~/intermediate".into());
        assert_eq!(cfg.download_dir(), home.join("intermediate"));
        assert_eq!(cfg.dest_dir(), home.join("complete"));
    }

    #[test]
    fn nzbget_missing_and_empty_intermediate_default_to_main() {
        for intermediate in ["", "InterDir=", "InterDir=${MainDir}/intermediate"] {
            let (cfg, _) = import_nzbget_conf(&format!(
                "MainDir=/processing\nDestDir=/complete\n{intermediate}\nCategory1.Name=tv\nCategory1.DestDir=${{DestDir}}/tv"
            )).unwrap();
            assert_eq!(
                cfg.download_dir(),
                PathBuf::from(if intermediate.ends_with("/intermediate") {
                    "/processing/intermediate"
                } else {
                    "/processing"
                })
            );
            assert_eq!(cfg.dest_dir(), PathBuf::from("/complete"));
            assert_eq!(
                cfg.categories[0].dest_dir,
                Some(PathBuf::from("/complete/tv"))
            );
        }
    }

    #[test]
    fn history_index_location_is_optional_and_requires_restart() {
        let old = Config::default();
        let mut new = old.clone();
        new.history.index_dir = Some("/local/history".into());
        let text = to_toml(&new).unwrap();
        assert_eq!(
            Config::from_toml(&text).unwrap().history.index_dir,
            new.history.index_dir
        );
        assert!(diff_sections(&old, &new).1.contains(&"history"));
        assert!(old.history.index_dir.is_none());
    }

    #[test]
    fn parses_and_maps() {
        let cfg = Config::from_toml(SAMPLE).unwrap();
        assert_eq!(cfg.servers.len(), 2);
        assert_eq!(cfg.queue.article_cache_mb, 512);
        assert_eq!(cfg.queue.article_retries, 3); // default preserved
        let defs = cfg.server_defs();
        assert_eq!(defs[0].max_connections, 30);
        assert_eq!(defs[0].pipeline_depth, 4);
        assert_eq!(defs[1].tier, 1);
        assert!(defs[1].fill);
        assert_eq!(cfg.api.compat_version, "26.2");
        assert!(cfg.api.discovery); // omitted field keeps LAN discovery on
    }

    #[test]
    fn rejects_bad_config() {
        assert!(Config::from_toml("[[server]]\nname = \"x\"").is_err()); // no host
        assert!(Config::from_toml("nonsense_key = 1").is_err()); // unknown field
    }

    #[test]
    fn torrent_section_parses_with_safe_defaults_and_rejects_unknown_keys() {
        let disabled = Config::from_toml("[torrent]\nenabled = false").unwrap();
        assert!(!disabled.torrent.enabled);
        assert!(!disabled.torrent.dht);
        assert_eq!(disabled.torrent.listen_port, 6881);
        assert!(disabled.torrent.pex);

        let configured = Config::from_toml(
            r#"
[paths]
main_dir = "/data"
torrent_dir = "/data/torrents"
torrent_watch_dir = "/data/watch-torrent"

[torrent]
enabled = false
listen_port = 6881
dht = false
pex = true
local_discovery = false
upnp_port_forwarding = false
socks_proxy_url = "socks5://127.0.0.1:1080"
socks_proxy_username = "alice"
socks_proxy_password = "secret"
max_peers_per_torrent = 80
max_peers_total = 400
max_known_peers_per_torrent = 1024
max_known_peers_total = 4096
upload_limit_kib = 0
default_seed_ratio = 2.5
default_seed_minutes = 10080
metainfo_max_mib = 10
source_redirects = 5

[[category]]
name = "tv"
torrent_dir = "/data/torrents/tv"
seed_ratio = 2.0
seed_minutes = 1440
"#,
        )
        .unwrap();
        assert!(!configured.torrent.enabled);
        assert!(!configured.torrent.dht);
        assert_eq!(configured.torrent.default_seed_ratio, 2.5);
        assert_eq!(
            configured.torrent.socks_proxy_password.as_deref(),
            Some("secret")
        );
        assert_eq!(configured.categories[0].seed_minutes, Some(1440));
        assert_eq!(configured.torrent_dir(), PathBuf::from("/data/torrents"));
        let rendered = to_toml(&configured).unwrap();
        assert_eq!(Config::from_toml(&rendered).unwrap(), configured);

        assert!(Config::from_toml("[torrent]\nenabled = false\ntyop = true").is_err());
        assert!(Config::from_toml("[paths]\ntorrent_typo = \"/tmp\"").is_err());
        assert!(Config::from_toml("[[category]]\nname = \"tv\"\ntorrent_typo = 1").is_err());

        let exact_defaults = r#"
[torrent]
enabled = false
listen_port = 6881
dht = false
pex = true
local_discovery = false
upnp_port_forwarding = false
max_peers_per_torrent = 80
max_peers_total = 400
max_known_peers_per_torrent = 1024
max_known_peers_total = 4096
upload_limit_kib = 0
default_seed_ratio = 0
default_seed_minutes = 0
metainfo_max_mib = 10
source_redirects = 5
"#;
        assert_eq!(
            Config::from_toml(exact_defaults).unwrap().torrent,
            TorrentConfig::default()
        );
    }

    #[test]
    fn empty_config_with_torrent_defaults_round_trips() {
        let config = Config::from_toml("").unwrap();
        assert_eq!(config.torrent, TorrentConfig::default());

        let rendered = to_toml(&config).unwrap();
        assert!(!rendered.contains("[torrent]"));
        let reparsed = Config::from_toml(&rendered).unwrap();
        assert_eq!(reparsed, config);
    }

    #[test]
    fn torrent_negative_contract_rejects_unsupported_or_unsafe_settings() {
        let invalid = [
            ("listen_port = 0", "listen_port"),
            ("upnp_port_forwarding = true", "upnp_port_forwarding"),
            ("metainfo_max_mib = 0", "metainfo_max_mib"),
            ("metainfo_max_mib = 101", "metainfo_max_mib"),
            ("default_seed_ratio = -1", "default_seed_ratio"),
            ("default_seed_ratio = nan", "default_seed_ratio"),
            ("max_peers_per_torrent = 0", "peer ceilings"),
            ("max_peers_per_torrent = 401", "peer ceilings"),
            (
                "listen_port_range = { start = 6881, end = 6882 }",
                "listen_port_range",
            ),
            ("upload_limit_kib = -1", "upload_limit_kib"),
            ("source_redirects = -1", "source_redirects"),
            ("default_seed_minutes = -1", "default_seed_minutes"),
            ("local_discovery = true", "local_discovery"),
            (
                "socks_proxy_url = \"socks5://user:pw@localhost:1080\"",
                "credential-free",
            ),
            ("socks_proxy_password = \"secret\"", "username and password"),
            (
                "socks_proxy_url = \"socks5://localhost:1080\"\ndht = true",
                "dht must be false",
            ),
        ];
        for (body, expected) in invalid {
            let error = Config::from_toml(&format!("[torrent]\n{body}\n"))
                .expect_err(body)
                .to_string();
            assert!(error.contains(expected), "{body}: {error}");
        }

        let cluster_error = Config::from_toml(
            "[torrent]\nenabled = true\n\
             [cluster]\nenabled = true\nnode_name = \"a\"\nshared_dir = \"/shared\"\n\
             advertise_url = \"http://a\"\nsecret = \"secret\"\n",
        )
        .unwrap_err()
        .to_string();
        assert!(cluster_error.contains("cannot be enabled while [cluster]"));
    }

    #[test]
    fn torrent_debug_redacts_proxy_material() {
        let torrent = TorrentConfig {
            socks_proxy_url: Some("socks5://private.example:1080".into()),
            socks_proxy_username: Some("alice".into()),
            socks_proxy_password: Some("hunter2".into()),
            ..Default::default()
        };
        let shown = format!("{torrent:?}");
        assert!(!shown.contains("private.example"), "{shown}");
        assert!(!shown.contains("hunter2"), "{shown}");
    }

    #[test]
    fn defaults_match_nzbget() {
        let q = QueueConfig::default();
        assert!(q.direct_write);
        assert_eq!(q.article_retries, 3);
        assert_eq!(q.retry_interval_secs, 10);
        assert_eq!(q.article_timeout_secs, 60);
        assert_eq!(q.min_free_disk_mb, 250);
    }

    #[test]
    fn cluster_section_parses_and_validates() {
        let toml = r#"
[cluster]
enabled = true
node_name = "node-a"
shared_dir = "/mnt/work"
advertise_url = "http://10.0.0.11:6789"
secret = "hunter2"
priority = 3
download = true
max_download_jobs = 4
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.node_name, "node-a");
        assert_eq!(cfg.cluster.priority, 3);
        assert_eq!(cfg.cluster.max_download_jobs, 4);
        assert_eq!(cfg.cluster.lease_interval_secs, 5); // default preserved
        assert_eq!(cfg.cluster.resolve_secret().unwrap(), "hunter2");

        // Missing requirements are rejected loudly.
        for broken in [
            "[cluster]\nenabled = true",
            "[cluster]\nenabled = true\nnode_name = \"a\"\nshared_dir = \"/x\"\nadvertise_url = \"http://a\"",
        ] {
            assert!(Config::from_toml(broken).is_err(), "{broken}");
        }
        // Disabled cluster needs nothing.
        assert!(Config::from_toml("[cluster]\nenabled = false").is_ok());
    }

    #[test]
    fn post_section_parses_with_defaults() {
        let cfg = Config::from_toml(
            "[post]\nstrategy = \"rocket\"\nscripts_dir = \"/opt/scripts\"\nunpack = false",
        )
        .unwrap();
        assert!(cfg.post.enabled);
        assert_eq!(cfg.post.strategy, "rocket");
        assert!(!cfg.post.unpack);
        assert!(cfg.post.cleanup);
        assert_eq!(cfg.post.par2_cmd, "par2");
        assert_eq!(cfg.post.scripts_dir, Some(PathBuf::from("/opt/scripts")));
        // Absent section = NZBGet-flavored defaults.
        let def = Config::from_toml("").unwrap();
        assert_eq!(def.post.strategy, "balanced");
        assert_eq!(def.post.tool_timeout_secs, 3600);
    }

    const NZBGET_CONF: &str = r#"
# Typical nzbget.conf excerpt
MainDir=/data/usenet
DestDir=${MainDir}/dst
InterDir=${MainDir}/inter
NzbDir=${MainDir}/nzb
QueueDir=${MainDir}/queue
TempDir=${MainDir}/tmp
ControlIP=0.0.0.0
ControlPort=6789
ControlUsername=nzbget
ControlPassword=tegbzn6789

Server1.Name=main
Server1.Level=0
Server1.Host=news.example.com
Server1.Port=563
Server1.Username=user1
Server1.Password=pass1
Server1.Encryption=yes
Server1.Connections=30
Server1.Retention=4500
Server1.Active=yes
Server1.CertVerification=strict

Server2.Name=fill
Server2.Level=1
Server2.Optional=yes
Server2.Host=fill.example.com
Server2.Port=119
Server2.Encryption=no
Server2.Connections=8
Server2.Active=yes

Category1.Name=movies
Category1.DestDir=${DestDir}/movies
Category2.Name=tv
Category2.Unpack=no

ArticleCache=700
DirectWrite=yes
CrcCheck=yes
ContinuePartial=yes
ArticleRetries=3
ArticleInterval=10
ArticleTimeout=60
DownloadRate=8000
DiskSpace=250
PropagationDelay=0

Unpack=yes
UnpackCleanupDisk=yes
UnrarCmd=unrar
SevenZipCmd=7z
ScriptDir=${MainDir}/scripts
PostStrategy=aggressive
ParCheck=auto
ParRepair=yes
KeepHistory=30
FutureOption=whatever
"#;

    #[test]
    fn nzbget_conf_import_maps_everything() {
        let (cfg, report) = import_nzbget_conf(NZBGET_CONF).unwrap();

        // ${Var} substitution + paths
        assert_eq!(cfg.paths.main_dir, PathBuf::from("/data/usenet"));
        assert_eq!(cfg.paths.dest_dir, PathBuf::from("/data/usenet/dst"));
        assert_eq!(
            cfg.paths.inter_dir,
            Some(PathBuf::from("/data/usenet/inter"))
        );
        assert_eq!(
            cfg.paths.queue_dir,
            Some(PathBuf::from("/data/usenet/queue"))
        );
        assert_eq!(cfg.api.bind, "0.0.0.0:6789");

        // Servers with NZBGet vocabulary translated (Level→tier,
        // Optional→fill, Encryption→tls)
        assert_eq!(cfg.servers.len(), 2);
        let s1 = &cfg.servers[0];
        assert_eq!(s1.name, "main");
        assert_eq!(s1.host, "news.example.com");
        assert_eq!(s1.port, 563);
        assert!(s1.tls);
        assert_eq!(s1.connections, 30);
        assert_eq!(s1.tier, 0);
        assert_eq!(s1.retention_days, 4500);
        assert_eq!(s1.username.as_deref(), Some("user1"));
        let s2 = &cfg.servers[1];
        assert_eq!(s2.tier, 1);
        assert!(s2.fill, "Optional=yes becomes a fill server");
        assert!(!s2.tls);

        // Categories
        assert_eq!(cfg.categories.len(), 2);
        assert_eq!(cfg.categories[0].name, "movies");
        assert_eq!(
            cfg.categories[0].dest_dir,
            Some(PathBuf::from("/data/usenet/dst/movies"))
        );
        assert_eq!(cfg.categories[1].unpack, Some(false));

        // Queue + post
        assert_eq!(cfg.queue.article_cache_mb, 700);
        assert_eq!(cfg.queue.speed_limit_kib, Some(8000));
        assert!(cfg.post.unpack);
        assert_eq!(cfg.post.strategy, "aggressive");
        assert_eq!(
            cfg.post.scripts_dir,
            Some(PathBuf::from("/data/usenet/scripts"))
        );

        // Report: mapped entries exist; auth options are recognized-skipped;
        // unknown future options surface for review.
        assert!(report.mapped.iter().any(|(k, _)| k == "MainDir"));
        assert!(report
            .mapped
            .iter()
            .any(|(k, t)| k == "Server1.Host" && t == "server[1].host"));
        assert!(report.skipped.iter().any(|k| k == "ControlPassword"));
        assert!(report.skipped.iter().any(|k| k == "ParCheck"));

        // KeepHistory used to land on the skipped list because nzbd kept
        // history forever and had nothing to map it onto. It has bounds
        // now, and NZBGet's units are already days — so an import carries
        // the operator's retention window across instead of silently
        // handing them ours.
        assert_eq!(cfg.history.keep_days, 30);
        assert!(
            !report.skipped.iter().any(|k| k == "KeepHistory"),
            "KeepHistory is mapped now, not skipped"
        );
        assert!(report
            .mapped
            .iter()
            .any(|(k, v)| k == "KeepHistory" && v == "history.keep_days"));
        assert!(report.unknown.iter().any(|k| k == "FutureOption"));
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);

        // The imported config round-trips through nzbd.toml.
        let toml_text = to_toml(&cfg).unwrap();
        let re = Config::from_toml(&toml_text).unwrap();
        assert_eq!(re.servers.len(), 2);
        assert_eq!(re.queue.article_cache_mb, 700);
    }

    #[test]
    fn nzbget_conf_import_drops_hostless_servers() {
        let (cfg, report) = import_nzbget_conf(
            "MainDir=/x
Server1.Name=ghost
Server1.Connections=4
             Server2.Host=ok.example
Server2.Connections=0
",
        )
        .unwrap();
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].host, "ok.example");
        assert_eq!(cfg.servers[0].connections, 1, "zero raised to one");
        assert_eq!(report.warnings.len(), 2);
    }

    #[test]
    fn feed_sections_parse_and_import() {
        let cfg = Config::from_toml(
            "[[feed]]\nname = \"idx\"\nurl = \"https://idx.example/rss\"\n\
             interval_mins = 30\nfilter = \"Accept: *1080p*\"\ncategory = \"tv\"",
        )
        .unwrap();
        assert_eq!(cfg.feeds.len(), 1);
        assert_eq!(cfg.feeds[0].interval_mins, 30);
        assert_eq!(cfg.feeds[0].category.as_deref(), Some("tv"));
        // name+url required
        assert!(Config::from_toml("[[feed]]\nname = \"x\"").is_err());

        // nzbget.conf FeedN.* import (% is NZBGet's newline in filters).
        let (cfg, report) = import_nzbget_conf(
            "Feed1.Name=idx\nFeed1.URL=https://idx.example/rss\n\
             Feed1.Interval=45\nFeed1.Filter=Accept: *1080p* % Reject: *x265*\n\
             Feed1.Category=tv\nFeed1.PauseNzb=no\nFeed1.Backlog=yes\n",
        )
        .unwrap();
        assert_eq!(cfg.feeds.len(), 1);
        assert_eq!(cfg.feeds[0].interval_mins, 45);
        assert!(cfg.feeds[0].filter.contains('\n'), "% becomes newline");
        assert!(report.mapped.iter().any(|(k, _)| k == "Feed1.URL"));
        assert!(report.skipped.iter().any(|k| k == "Feed1.Backlog"));
    }

    #[test]
    fn path_helpers() {
        let cfg = Config::from_toml(SAMPLE).unwrap();
        assert_eq!(cfg.state_dir(), PathBuf::from("/data/usenet/queue"));
        assert_eq!(cfg.dest_dir(), PathBuf::from("/data/usenet/complete"));
        assert_eq!(cfg.speed_limit_bps(), None);

        let home = std::env::var("HOME").unwrap();
        let def = Config::default();
        assert_eq!(
            def.state_dir(),
            PathBuf::from(&home).join("downloads/queue")
        );
    }

    #[test]
    fn storage_roots_are_complete_expanded_and_deduplicated() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.paths.main_dir = tmp.path().join("working");
        cfg.paths.queue_dir = Some(tmp.path().join("state"));
        cfg.paths.dest_dir = tmp.path().join("downloads");
        cfg.paths.inter_dir = Some(tmp.path().join("intermediate"));
        cfg.paths.temp_dir = Some(tmp.path().join("temporary"));
        cfg.paths.nzb_watch_dir = Some(tmp.path().join("watch"));
        cfg.post.failed_dir = Some(tmp.path().join("failed"));
        cfg.categories = vec![
            CategoryConfig {
                name: "tv".into(),
                dest_dir: Some(tmp.path().join("library/tv")),
                ..Default::default()
            },
            CategoryConfig {
                name: "same-as-downloads".into(),
                dest_dir: Some(tmp.path().join("downloads")),
                ..Default::default()
            },
        ];
        let roots = cfg.storage_roots();
        let labels: Vec<_> = roots.iter().map(|root| root.label.as_str()).collect();
        assert_eq!(
            labels,
            [
                "state",
                "downloads · category: same-as-downloads",
                "working",
                "failed",
                "intermediate",
                "temporary",
                "watch",
                "category: tv",
            ]
        );
        assert!(roots.iter().all(|root| root.path.starts_with(tmp.path())));

        cfg.cluster.enabled = true;
        cfg.cluster.shared_dir = Some(tmp.path().join("shared"));
        let cluster_roots = cfg.storage_roots();
        assert_eq!(cluster_roots[0].path, tmp.path().join("state"));
        assert_eq!(
            cluster_roots[1].path,
            tmp.path().join("shared/.nzbd-cluster")
        );
        assert_eq!(cluster_roots[1].label, "cluster state");
    }

    #[test]
    fn excessive_storage_roots_are_rejected_before_probe_threads_start() {
        let cfg = Config {
            categories: (0..MAX_STORAGE_ROOTS)
                .map(|index| CategoryConfig {
                    name: format!("category-{index}"),
                    dest_dir: Some(PathBuf::from(format!("/storage/{index}"))),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let error = cfg.validate().unwrap_err().to_string();
        assert!(error.contains("write-root count"), "{error}");
        assert!(error.contains(&MAX_STORAGE_ROOTS.to_string()), "{error}");
    }

    #[test]
    fn torrent_storage_is_conditional_complete_and_deduplicated() {
        let mut cfg = Config::default();
        cfg.paths.main_dir = PathBuf::from("/data");
        cfg.paths.torrent_watch_dir = Some(PathBuf::from("/watch"));
        cfg.categories.push(CategoryConfig {
            name: "tv".into(),
            torrent_dir: Some(PathBuf::from("/data/torrents/tv")),
            ..Default::default()
        });
        assert_eq!(cfg.torrent_dir(), PathBuf::from("/data/torrents"));
        assert!(cfg
            .storage_roots()
            .iter()
            .all(|root| !root.label.contains("torrent")));

        cfg.torrent.enabled = true;
        let roots = cfg.storage_roots();
        for expected in [
            "torrent payloads",
            "torrent control",
            "torrent watch",
            "torrent category: tv",
        ] {
            assert!(roots.iter().any(|root| root.label.contains(expected)));
        }
    }

    #[test]
    fn torrent_seed_policy_uses_per_add_then_category_then_global() {
        let mut cfg = Config::default();
        cfg.torrent.default_seed_ratio = 1.0;
        cfg.torrent.default_seed_minutes = 60;
        cfg.categories.push(CategoryConfig {
            name: "tv".into(),
            seed_ratio: Some(2.0),
            seed_minutes: Some(120),
            ..Default::default()
        });
        assert_eq!(cfg.torrent_seed_limits(None, None, None), (1.0, 60));
        assert_eq!(cfg.torrent_seed_limits(Some("tv"), None, None), (2.0, 120));
        assert_eq!(
            cfg.torrent_seed_limits(Some("tv"), Some(3.0), Some(180)),
            (3.0, 180)
        );
    }
}

/// Did any server's `connections` count move?
fn server_conns_changed(old: &Config, new: &Config) -> bool {
    if old.servers.len() != new.servers.len() {
        return false; // adding or removing a server is a restart anyway
    }
    old.servers
        .iter()
        .zip(new.servers.iter())
        .any(|(o, n)| o.connections != n.connections)
}

/// Are the server lists the same apart from their connection counts?
fn servers_equal_ignoring_connections(old: &Config, new: &Config) -> bool {
    if old.servers.len() != new.servers.len() {
        return false;
    }
    old.servers.iter().zip(new.servers.iter()).all(|(o, n)| {
        let mut o = o.clone();
        let mut n = n.clone();
        o.connections = 0;
        n.connections = 0;
        o == n
    })
}

#[cfg(test)]
mod live_settings_tests {
    use super::*;

    fn base() -> Config {
        let mut c = Config::default();
        c.servers.push(ServerConfig {
            name: "eweka".into(),
            host: "news.example".into(),
            connections: 8,
            ..Default::default()
        });
        c
    }

    /// A live key must not also flag its section as restart-required.
    /// `diff_sections` compares whole sections, so every key added to the
    /// live list needs holding equal before that compare — forget it and
    /// every save raises a restart banner nobody can clear.
    #[test]
    fn a_live_key_does_not_also_demand_a_restart() {
        let old = base();
        let mut new = base();
        new.queue.max_active_downloads = 4;
        let (live, restart) = diff_sections(&old, &new);
        assert!(live.contains(&"max active downloads"));
        assert!(
            !restart.contains(&"queue"),
            "changing only a live key must not ask for a bounce: {restart:?}"
        );
    }

    /// Connection counts apply live; everything else about a server is
    /// baked into a socket that already exists.
    #[test]
    fn connection_counts_are_live_but_the_rest_of_a_server_is_not() {
        let old = base();
        let mut new = base();
        new.servers[0].connections = 4;
        let (live, restart) = diff_sections(&old, &new);
        assert!(live.contains(&"connections"));
        assert!(
            !restart.contains(&"servers"),
            "only the count moved: {restart:?}"
        );

        let mut moved_host = base();
        moved_host.servers[0].host = "other.example".into();
        let (live, restart) = diff_sections(&old, &moved_host);
        assert!(restart.contains(&"servers"), "a new host needs a reconnect");
        assert!(!live.contains(&"connections"));

        // Both at once: the host change still wins a restart.
        let mut both = base();
        both.servers[0].host = "other.example".into();
        both.servers[0].connections = 2;
        let (live, restart) = diff_sections(&old, &both);
        assert!(live.contains(&"connections"));
        assert!(restart.contains(&"servers"));
    }

    /// Adding or removing a server is structural, never live.
    #[test]
    fn adding_a_server_is_a_restart() {
        let old = base();
        let mut new = base();
        new.servers.push(ServerConfig {
            name: "block".into(),
            host: "block.example".into(),
            connections: 4,
            ..Default::default()
        });
        let (live, restart) = diff_sections(&old, &new);
        assert!(restart.contains(&"servers"));
        assert!(!live.contains(&"connections"));
    }

    #[test]
    fn an_unchanged_config_asks_for_nothing() {
        let (live, restart) = diff_sections(&base(), &base());
        assert!(live.is_empty(), "{live:?}");
        assert!(restart.is_empty(), "{restart:?}");
    }

    #[test]
    fn torrent_upload_limit_is_live_and_every_other_torrent_key_restarts() {
        let old = base();
        let mut upload = old.clone();
        upload.torrent.upload_limit_kib = 1024;
        let (live, restart) = diff_sections(&old, &upload);
        assert_eq!(live, ["torrent upload limit"]);
        assert!(!restart.contains(&"torrent"));

        let mut discovery = old.clone();
        discovery.torrent.pex = false;
        let (live, restart) = diff_sections(&old, &discovery);
        assert!(live.is_empty());
        assert!(restart.contains(&"torrent"));
    }

    #[test]
    fn the_configured_cap_is_clamped() {
        let mut c = Config::default();
        c.queue.max_active_downloads = 0;
        assert_eq!(c.max_active_downloads(), Some(1), "zero is not a pause");
        c.queue.max_active_downloads = 9_999;
        assert_eq!(c.max_active_downloads(), Some(100));
        assert_eq!(Config::default().queue.max_active_downloads, 1);
    }
}

// ---------------------------------------------------------------------------
// Settings-editor support: masked secrets round-trip
// ---------------------------------------------------------------------------

/// Compare two configs section by section for the settings UI:
/// returns (live_appliable, restart_required) section names.
///
/// A section listed as live must have a corresponding branch in
/// `put_config`; the name is the contract between the two. Anything not
/// named here needs a restart, which is the safe default.
pub fn diff_sections(old: &Config, new: &Config) -> (Vec<&'static str>, Vec<&'static str>) {
    let mut live = Vec::new();
    let mut restart = Vec::new();
    if old.paths != new.paths {
        restart.push("paths");
    }
    // Connection counts apply live; everything else about a server (host,
    // credentials, TLS, tier) is baked into a socket that already exists.
    // Compared with the counts held equal, so changing only those does
    // not also flag the whole section as needing a bounce.
    if server_conns_changed(old, new) {
        live.push("connections");
    }
    if !servers_equal_ignoring_connections(old, new) {
        restart.push("servers");
    }
    if old.categories != new.categories {
        restart.push("categories");
    }
    if old.feeds != new.feeds {
        restart.push("feeds");
    }
    if old.queue.speed_limit_kib != new.queue.speed_limit_kib {
        live.push("speed limit");
    }
    if old.queue.max_active_downloads != new.queue.max_active_downloads {
        live.push("max active downloads");
    }
    // Held equal before the rest of the section is compared, so a change
    // to a live key does not also flag `queue` as restart-required. Every
    // live key added above needs a line here or every save will claim a
    // restart is pending.
    let mut oq = old.queue.clone();
    let mut nq = new.queue.clone();
    oq.speed_limit_kib = None;
    nq.speed_limit_kib = None;
    oq.max_active_downloads = 0;
    nq.max_active_downloads = 0;
    if oq != nq {
        restart.push("queue");
    }
    if old.api != new.api {
        restart.push("api");
    }
    if old.history != new.history {
        restart.push("history");
    }
    if old.post != new.post {
        restart.push("post-processing");
    }
    if old.cluster != new.cluster {
        restart.push("cluster");
    }
    if old.torrent.upload_limit_kib != new.torrent.upload_limit_kib {
        live.push("torrent upload limit");
    }
    let mut old_torrent = old.torrent.clone();
    let mut new_torrent = new.torrent.clone();
    old_torrent.upload_limit_kib = 0;
    new_torrent.upload_limit_kib = 0;
    if old_torrent != new_torrent {
        restart.push("torrent");
    }
    (live, restart)
}

/// Placeholder the settings UI shows instead of stored secrets. A saved
/// config carrying this exact value keeps the existing secret.
pub const SECRET_MASK: &str = "***unchanged***";

/// Clone the config with every secret replaced by [`SECRET_MASK`], for
/// display/editing. Feed URLs (which may embed API keys) are left as-is —
/// they're the feed's identity and NZBGet shows them too.
pub fn mask_secrets(cfg: &Config) -> Config {
    let mut c = cfg.clone();
    for s in &mut c.servers {
        if s.password.is_some() {
            s.password = Some(SECRET_MASK.into());
        }
    }
    if c.api.password.is_some() {
        c.api.password = Some(SECRET_MASK.into());
    }
    if c.api.token.is_some() {
        c.api.token = Some(SECRET_MASK.into());
    }
    if c.cluster.secret.is_some() {
        c.cluster.secret = Some(SECRET_MASK.into());
    }
    if c.torrent.socks_proxy_password.is_some() {
        c.torrent.socks_proxy_password = Some(SECRET_MASK.into());
    }
    c
}

/// Replace [`SECRET_MASK`] values in an edited config with the secrets
/// from the previous config, so a masked display round-trips without the
/// user retyping passwords. Servers are matched by name, then by index.
/// Returns the fields whose mask could NOT be resolved — an empty vec
/// means every secret came back.
///
/// The old version wrote `prev.and_then(...)` straight into the field, so
/// a mask with no previous value to restore from (a renamed server, a
/// newly added one, a running config that never had a password) silently
/// became `None`: the password was deleted and the save reported success.
/// Unresolvable masks are now LEFT IN PLACE, which makes `validate()`
/// reject the config and forces the caller to say so out loud.
#[must_use]
pub fn merge_masked_secrets(new: &mut Config, old: &Config) -> Vec<String> {
    let is_mask = |v: &Option<String>| v.as_deref() == Some(SECRET_MASK);
    let mut unresolved = Vec::new();
    for (i, s) in new.servers.iter_mut().enumerate() {
        if is_mask(&s.password) {
            let prev = old
                .servers
                .iter()
                .find(|o| !o.name.is_empty() && o.name == s.name)
                .or_else(|| old.servers.get(i));
            match prev.and_then(|p| p.password.clone()) {
                Some(pw) => s.password = Some(pw),
                None => unresolved.push(format!("server '{}' password", s.name)),
            }
        }
    }
    let mut restore = |cur: &mut Option<String>, prev: &Option<String>, what: &str| {
        if is_mask(cur) {
            match prev {
                Some(v) => *cur = Some(v.clone()),
                None => unresolved.push(what.to_string()),
            }
        }
    };
    restore(&mut new.api.password, &old.api.password, "[api] password");
    restore(&mut new.api.token, &old.api.token, "[api] token");
    restore(
        &mut new.cluster.secret,
        &old.cluster.secret,
        "[cluster] secret",
    );
    restore(
        &mut new.torrent.socks_proxy_password,
        &old.torrent.socks_proxy_password,
        "[torrent] socks_proxy_password",
    );
    unresolved
}

#[cfg(test)]
mod mask_tests {
    use super::*;

    #[test]
    fn secrets_mask_and_merge_round_trip() {
        let toml = r#"
[paths]
main_dir = "/data"
dest_dir = "/data/complete"

[[server]]
name = "prime"
host = "news.example.com"
username = "u"
password = "real-secret"

[api]
password = "api-secret"

[cluster]
secret = "cluster-secret"

[torrent]
socks_proxy_url = "socks5://127.0.0.1:1080"
socks_proxy_username = "alice"
socks_proxy_password = "proxy-secret"
"#;
        let cfg = Config::from_toml(toml).unwrap();
        let masked = mask_secrets(&cfg);
        let shown = to_toml(&masked).unwrap();
        assert!(!shown.contains("real-secret"));
        assert!(!shown.contains("api-secret"));
        assert!(!shown.contains("cluster-secret"));
        assert!(!shown.contains("proxy-secret"));
        assert!(shown.contains(SECRET_MASK));

        // User edits something unrelated and saves the masked text back.
        // The editor's own path parses WITHOUT validating (the mask is not
        // a legal secret) and merges the real values back in.
        let edited = shown.replace("main_dir = \"/data\"", "main_dir = \"/mnt/big\"");
        let mut new_cfg = Config::parse_toml_unvalidated(&edited).unwrap();
        assert!(merge_masked_secrets(&mut new_cfg, &cfg).is_empty());
        assert_eq!(new_cfg.servers[0].password.as_deref(), Some("real-secret"));
        assert_eq!(new_cfg.api.password.as_deref(), Some("api-secret"));
        assert_eq!(new_cfg.cluster.secret.as_deref(), Some("cluster-secret"));
        assert_eq!(
            new_cfg.torrent.socks_proxy_password.as_deref(),
            Some("proxy-secret")
        );
        assert_eq!(new_cfg.paths.main_dir, PathBuf::from("/mnt/big"));
        // …and the merged result is a config the strict validator accepts.
        new_cfg.validate().unwrap();

        // Typing a NEW password replaces rather than restores.
        let repw = edited.replace(
            &format!("password = \"{SECRET_MASK}\""),
            "password = \"fresh\"",
        );
        let mut new_cfg = Config::parse_toml_unvalidated(&repw).unwrap();
        assert!(merge_masked_secrets(&mut new_cfg, &cfg).is_empty());
        assert_eq!(new_cfg.servers[0].password.as_deref(), Some("fresh"));
    }

    /// The masked text is a DISPLAY, not a backup.
    ///
    /// Field report 2026-07-26: "it imported the config file but lost my
    /// password" — a config restored from a copy taken out of the settings
    /// editor carried `password = "***unchanged***"`, and nothing on the
    /// boot path objected, so the daemon authenticated with that literal
    /// string against a provider account that was perfectly healthy. The
    /// symptom (connection failures) pointed at the provider; the cause was
    /// three days earlier in a text editor.
    #[test]
    fn a_masked_config_is_refused_on_the_boot_path() {
        let masked = format!(
            "[paths]\nmain_dir = \"/data\"\ndest_dir = \"/data/complete\"\n\n\
             [[server]]\nname = \"prime\"\nhost = \"news.example.com\"\n\
             password = \"{SECRET_MASK}\"\n"
        );
        let err = Config::from_toml(&masked).expect_err("the mask must not boot");
        let msg = err.to_string();
        assert!(msg.contains("prime"), "names the server: {msg}");
        assert!(
            msg.contains("settings editor"),
            "explains where it came from: {msg}"
        );
        assert!(
            msg.contains("not a backup"),
            "and that the copy was not a backup: {msg}"
        );

        // Every other masked secret is caught the same way.
        for (section, body) in [
            (
                "[api] password",
                format!("[api]\npassword = \"{SECRET_MASK}\"\n"),
            ),
            ("[api] token", format!("[api]\ntoken = \"{SECRET_MASK}\"\n")),
        ] {
            let e = Config::from_toml(&body).expect_err("must reject");
            assert!(e.to_string().contains(section), "{section}: {e}");
        }

        // A real secret still parses — the check is exact, not fuzzy.
        let real = masked.replace(SECRET_MASK, "***unchanged***-but-mine");
        Config::from_toml(&real).expect("a real password that merely looks similar is fine");
    }

    /// A mask with nothing behind it must not silently delete a password.
    #[test]
    fn an_unresolvable_mask_is_reported_not_blanked() {
        let old = Config::from_toml(
            "[paths]\nmain_dir = \"/data\"\ndest_dir = \"/d\"\n\n\
             [[server]]\nname = \"prime\"\nhost = \"h\"\npassword = \"real\"\n",
        )
        .unwrap();

        // The operator renames the server AND leaves the mask in place, so
        // neither the name nor the index finds a previous secret.
        let mut renamed = Config::parse_toml_unvalidated(&format!(
            "[paths]\nmain_dir = \"/data\"\ndest_dir = \"/d\"\n\n\
             [[server]]\nname = \"second\"\nhost = \"h2\"\npassword = \"{SECRET_MASK}\"\n\n\
             [[server]]\nname = \"third\"\nhost = \"h3\"\npassword = \"{SECRET_MASK}\"\n"
        ))
        .unwrap();
        let unresolved = merge_masked_secrets(&mut renamed, &old);

        // Server 1 falls back to the index and recovers. Server 2 has
        // nothing behind it at all.
        assert_eq!(renamed.servers[0].password.as_deref(), Some("real"));
        assert_eq!(unresolved, vec!["server 'third' password".to_string()]);
        // The mask is LEFT IN PLACE — the old code wrote None here, which
        // is how a save could delete a password and report success.
        assert_eq!(renamed.servers[1].password.as_deref(), Some(SECRET_MASK));
        // …so the strict validator refuses it too, belt and braces.
        assert!(renamed.validate().is_err());
    }
}
