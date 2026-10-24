//! BitTorrent engine boundary for nzbd.
//!
//! M0 deliberately keeps this crate disconnected from the daemon. It proves
//! the pinned engine and the process-wide TLS provider before queue admission,
//! persistence, API, or configuration can start peer-to-peer traffic.

#![forbid(unsafe_code)]

pub mod identity;
mod source_fetch;

#[cfg(test)]
mod m0_storage_full_probe_tests;

pub use source_fetch::{fetch_torrent_source, TorrentSourceFetchLimits};

use librqbit::api::TorrentIdOrHash;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ManagedTorrent, PeerConnectionOptions,
    Session, SessionOptions, TorrentStats as EngineTorrentStats, TorrentStatsState,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::ops::Range;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The exact engine release the M0 contract and interop tests describe.
pub const ENGINE_VERSION: &str = "8.1.1";
/// Default raw or fetched `.torrent` admission limit from proposal §10.3.
pub const DEFAULT_MAX_METAINFO_BYTES: usize = 10 * 1024 * 1024;
/// Minimum configurable raw or fetched `.torrent` admission limit.
pub const MIN_CONFIGURED_MAX_METAINFO_BYTES: usize = 1024 * 1024;
/// Maximum configurable raw or fetched `.torrent` admission limit.
pub const MAX_CONFIGURED_MAX_METAINFO_BYTES: usize = 100 * 1024 * 1024;
/// Maximum HTTP redirects followed while fetching a `.torrent` source.
pub const MAX_TORRENT_SOURCE_REDIRECTS: usize = 5;
/// Default timeout for establishing a torrent-source HTTP connection.
pub const DEFAULT_TORRENT_SOURCE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default end-to-end timeout for a torrent-source HTTP fetch.
pub const DEFAULT_TORRENT_SOURCE_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum magnet URI length from proposal §10.3.
pub const MAX_MAGNET_URI_BYTES: usize = 16 * 1024;
/// Maximum number of payload files described by one torrent.
pub const MAX_TORRENT_FILES: usize = 100_000;
/// Maximum encoded length of one projected relative payload path.
pub const MAX_TORRENT_RELATIVE_PATH_BYTES: usize = 4 * 1024;
/// Portable maximum encoded length of one payload path component.
pub const MAX_TORRENT_PATH_COMPONENT_BYTES: usize = 255;
/// Maximum aggregate encoded length of all projected relative payload paths.
pub const MAX_TORRENT_PATH_BYTES: usize = 16 * 1024 * 1024;
/// Maximum unique explicit bootstrap peers accepted for one torrent.
pub const MAX_INITIAL_PEERS: usize = 80;
/// Maximum live peers managed for one torrent.
pub const MAX_LIVE_PEERS_PER_TORRENT: usize = 80;
/// Maximum live peers managed across the embedded session.
pub const MAX_LIVE_PEERS_TOTAL: usize = 400;
/// Maximum retained peer records for one torrent.
pub const MAX_KNOWN_PEERS_PER_TORRENT: usize = 1_024;
/// Maximum retained peer records across the embedded session.
pub const MAX_KNOWN_PEERS_TOTAL: usize = 4_096;
/// Maximum unique non-empty trackers accepted from one torrent source.
pub const MAX_TRACKERS_PER_TORRENT: usize = 64;
/// Maximum decoded byte length of one tracker URL.
pub const MAX_TRACKER_URL_BYTES: usize = 2 * 1024;
/// Maximum concurrent engine integrity checks during torrent initialization.
pub const MAX_CONCURRENT_TORRENT_INITIALIZATIONS: usize = 1;
/// Timeout for establishing an outgoing peer connection.
pub const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Timeout for one peer-protocol read or write operation.
pub const PEER_READ_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Interval between idle peer keepalive messages.
pub const PEER_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(120);
/// End-to-end deadline for resolving magnet metadata from DHT, trackers, or
/// explicitly supplied peers.
pub const MAGNET_METADATA_TIMEOUT: Duration = Duration::from_secs(120);
/// Maximum display-safe engine error length from proposal §10.3.
pub const DISPLAY_SAFE_ERROR_MAX_BYTES: usize = 2 * 1024;

// librqbit-core 5.0.0 divides pieces into 16 KiB chunks and stores absolute
// chunk indices in u32. Keep this version-pinned representation check at the
// adapter boundary until the engine makes its arithmetic checked.
const RQBIT_CHUNK_BYTES: u64 = 16 * 1024;
const ERROR_TRUNCATION_MARKER: &str = "... [truncated]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoProviderInstall {
    InstalledAwsLc,
    AlreadyInstalled,
}

#[derive(Debug, thiserror::Error)]
pub enum TorrentError {
    #[error("could not install the process rustls crypto provider")]
    CryptoProvider,
    #[error("torrent engine: {0}")]
    Engine(String),
    #[error("torrent engine returned no managed handle")]
    MissingHandle,
    #[error("torrent engine did not return resolved magnet metadata")]
    MissingResolvedMagnet,
    #[error("BitTorrent v2-only magnets are not supported by librqbit 8.1.1")]
    UnsupportedV2Magnet,
    #[error("hybrid BitTorrent v1/v2 magnets are not supported in the first release")]
    UnsupportedHybridMagnet,
    #[error("invalid BitTorrent magnet URI: {0}")]
    InvalidMagnet(&'static str),
    #[error("BitTorrent v2-only metainfo is not supported by librqbit 8.1.1")]
    UnsupportedV2Metainfo,
    #[error("hybrid BitTorrent v1/v2 metainfo is not supported in the first release")]
    UnsupportedHybridMetainfo,
    #[error("invalid SOCKS proxy configuration: {0}")]
    InvalidProxy(&'static str),
    #[error("invalid TCP listen range: expected exactly one non-zero port")]
    InvalidListenPortRange,
    #[error("invalid initial BitTorrent peer: {0}")]
    InvalidInitialPeer(&'static str),
    #[error("too many unique initial BitTorrent peers ({count}; maximum {limit})")]
    TooManyInitialPeers { count: usize, limit: usize },
    #[error(
        "SOCKS proxy cannot be combined with DHT because librqbit 8.1.1 sends DHT traffic outside the proxy"
    )]
    ProxyWithDht,
    #[error(
        "This magnet has no usable peer source while DHT is disabled. Enable DHT without a SOCKS proxy, or use a tracker-bearing magnet or .torrent file."
    )]
    MagnetDiscoveryUnavailable,
    #[error(
        "Magnet metadata could not be resolved within 120 seconds. Check peer availability and DHT or tracker connectivity, then retry."
    )]
    MagnetMetadataTimeout,
    #[error("Resolved magnet metadata is invalid. Use a valid magnet or a trusted .torrent file.")]
    InvalidResolvedMagnetMetadata,
    #[error(
        "This torrent is private and cannot be added while DHT is enabled. Disable DHT and use the private tracker's .torrent file."
    )]
    PrivateMetainfoWithDht,
    #[error(
        "SOCKS proxy cannot be used with UDP trackers because librqbit 8.1.1 sends UDP announces outside the proxy"
    )]
    ProxyWithUdpTracker,
    #[error(
        "private torrents must declare exactly one unique tracker in the first release (found {0})"
    )]
    PrivateTrackerCount(usize),
    #[error("invalid BitTorrent tracker URL: {0}")]
    InvalidTracker(&'static str),
    #[error("BitTorrent tracker URL is too long ({size} bytes; maximum {limit})")]
    TrackerUrlTooLong { size: usize, limit: usize },
    #[error("too many unique BitTorrent trackers ({count}; maximum {limit})")]
    TooManyTrackers { count: usize, limit: usize },
    #[error("unsafe torrent metainfo path: {0}")]
    UnsafeMetainfoPath(&'static str),
    #[error("torrent metainfo is too large ({size} bytes; maximum {limit})")]
    MetainfoTooLarge { size: usize, limit: usize },
    #[error("invalid torrent source URL: {0}")]
    InvalidTorrentSource(&'static str),
    #[error("invalid torrent source limits: {0}")]
    InvalidTorrentSourceLimits(&'static str),
    #[error("torrent source request failed for {origin}")]
    TorrentSourceRequestFailed { origin: String },
    #[error("torrent source request timed out for {origin}")]
    TorrentSourceTimeout { origin: String },
    #[error("torrent source returned HTTP {status} from {origin}")]
    TorrentSourceHttpStatus { status: u16, origin: String },
    #[error("torrent source exceeded the redirect limit ({limit})")]
    TooManyTorrentSourceRedirects { limit: usize },
    #[error("torrent source returned an invalid redirect from {origin}: {reason}")]
    InvalidTorrentSourceRedirect {
        origin: String,
        reason: &'static str,
    },
    #[error("magnet URI is too long ({size} bytes; maximum {limit})")]
    MagnetTooLong { size: usize, limit: usize },
    #[error("torrent contains too many files ({count}; maximum {limit})")]
    TooManyFiles { count: usize, limit: usize },
    #[error("torrent relative path is too long ({size} bytes; maximum {limit})")]
    PathTooLong { size: usize, limit: usize },
    #[error("torrent path component is too long ({size} bytes; maximum {limit})")]
    PathComponentTooLong { size: usize, limit: usize },
    #[error("torrent path metadata is too large ({size} bytes; maximum {limit})")]
    PathMetadataTooLarge { size: usize, limit: usize },
    #[error("torrent contains payload paths that collide or overlap under portable comparison")]
    PathCollision,
    #[error("unsafe existing torrent path: a payload component is a symbolic link")]
    ExistingPathSymlink,
    #[error("unsafe existing torrent path: {0}")]
    ExistingPathType(&'static str),
    #[error("invalid torrent piece geometry: {0}")]
    InvalidMetainfoGeometry(&'static str),
    #[error("duplicate engine identity in the authorized restore set")]
    DuplicateEngineIdentity,
    #[error("restored torrent info hash does not match its durable descriptor")]
    RestoreHashMismatch,
}

fn engine_error(error: impl std::fmt::Display) -> TorrentError {
    TorrentError::Engine(display_safe_error(&error.to_string()))
}

fn display_safe_error(message: &str) -> String {
    let mut output = String::new();
    let mut redact_next_secret_value = false;
    for raw_token in message.split_whitespace() {
        let normalized = if raw_token.chars().any(char::is_control) {
            Cow::Owned(
                raw_token
                    .chars()
                    .filter(|character| !character.is_control())
                    .collect::<String>(),
            )
        } else {
            Cow::Borrowed(raw_token)
        };
        if normalized.is_empty() {
            continue;
        }
        let redacted = if redact_next_secret_value {
            if is_secret_assignment_separator(&normalized) {
                Cow::Borrowed(normalized.as_ref())
            } else {
                redact_next_secret_value = secret_value_may_continue(&normalized);
                Cow::Borrowed("<redacted>")
            }
        } else {
            redact_next_secret_value = secret_value_follows(&normalized);
            redact_error_token(&normalized)
        };
        let separator = usize::from(!output.is_empty());
        if output
            .len()
            .saturating_add(separator)
            .saturating_add(redacted.len())
            > DISPLAY_SAFE_ERROR_MAX_BYTES
        {
            if separator != 0 && output.len() < DISPLAY_SAFE_ERROR_MAX_BYTES {
                output.push(' ');
            }
            let remaining = DISPLAY_SAFE_ERROR_MAX_BYTES.saturating_sub(output.len());
            let mut boundary = remaining.min(redacted.len());
            while !redacted.is_char_boundary(boundary) {
                boundary -= 1;
            }
            output.push_str(&redacted[..boundary]);
            return mark_display_truncated(output);
        }
        if separator != 0 {
            output.push(' ');
        }
        output.push_str(&redacted);
    }
    output
}

fn redact_error_token(token: &str) -> Cow<'_, str> {
    if let Some(assignments) = redact_secret_assignments(token) {
        return Cow::Owned(redact_error_token_values(&assignments).into_owned());
    }
    redact_error_token_values(token)
}

fn redact_error_token_values(token: &str) -> Cow<'_, str> {
    let whole = redact_error_value(token);
    if whole.as_ref() != token {
        return whole;
    }
    for separator in ['=', ':'] {
        if let Some((key, value)) = token.split_once(separator) {
            let redacted = redact_error_value(value);
            if redacted.as_ref() != value {
                return Cow::Owned(format!("{key}{separator}{redacted}"));
            }
        }
    }
    whole
}

fn redact_secret_assignments(token: &str) -> Option<String> {
    let bytes = token.as_bytes();
    let mut ranges = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if !matches!(bytes[index], b'=' | b':') {
            index += 1;
            continue;
        }

        let mut key_end = index;
        while key_end > 0 && matches!(bytes[key_end - 1], b'"' | b'\'') {
            key_end -= 1;
        }
        let mut key_start = key_end;
        while key_start > 0
            && (bytes[key_start - 1].is_ascii_alphanumeric()
                || matches!(bytes[key_start - 1], b'_' | b'-' | b'.'))
        {
            key_start -= 1;
        }
        if key_start == key_end || !is_secret_key(&token[key_start..key_end]) {
            index += 1;
            continue;
        }

        let mut value_start = index + 1;
        while value_start < bytes.len() && matches!(bytes[value_start], b'"' | b'\'' | b'=' | b'>')
        {
            value_start += 1;
        }
        let mut value_end = value_start;
        while value_end < bytes.len()
            && !matches!(
                bytes[value_end],
                b'&' | b',' | b';' | b'}' | b']' | b')' | b'"' | b'\''
            )
        {
            value_end += 1;
        }
        if value_start != value_end {
            ranges.push(value_start..value_end);
            index = value_end;
        } else {
            index += 1;
        }
    }

    if ranges.is_empty() {
        return None;
    }
    let redacted_bytes = ranges
        .iter()
        .map(|range| range.end.saturating_sub(range.start))
        .sum::<usize>();
    let mut output = String::with_capacity(
        token
            .len()
            .saturating_sub(redacted_bytes)
            .saturating_add(ranges.len() * "<redacted>".len()),
    );
    let mut copied = 0usize;
    for range in ranges {
        output.push_str(&token[copied..range.start]);
        output.push_str("<redacted>");
        copied = range.end;
    }
    output.push_str(&token[copied..]);
    Some(output)
}

fn secret_value_follows(token: &str) -> bool {
    let wrapper = |character: char| {
        matches!(
            character,
            ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
        )
    };
    let token = token.trim_matches(wrapper);
    let (key, inline_value) =
        if let Some(key) = token.strip_suffix('=').or_else(|| token.strip_suffix(':')) {
            (key, None)
        } else if let Some((key, value)) = token.split_once(['=', ':']) {
            (key, Some(value))
        } else {
            (token, None)
        };
    let key = key.trim_matches(wrapper);
    if key.is_empty() || !is_secret_key(key) {
        return false;
    }
    inline_value.is_none_or(|value| {
        let value = value.trim_matches(wrapper);
        value.is_empty() || is_authorization_scheme(value) || value.ends_with([',', ';'])
    })
}

fn is_secret_assignment_separator(token: &str) -> bool {
    matches!(token, "=" | ":" | "=>" | ":=" | "->")
}

fn secret_value_may_continue(token: &str) -> bool {
    let token = token.trim_matches(|character: char| {
        matches!(character, '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\'')
    });
    is_authorization_scheme(token.trim_end_matches([',', ';'])) || token.ends_with([',', ';'])
}

fn is_authorization_scheme(value: &str) -> bool {
    [
        "basic",
        "bearer",
        "digest",
        "negotiate",
        "token",
        "aws4-hmac-sha256",
    ]
    .iter()
    .any(|scheme| value.eq_ignore_ascii_case(scheme))
}

fn redact_error_value(value: &str) -> Cow<'_, str> {
    if value
        .as_bytes()
        .windows(b"magnet:?".len())
        .any(|window| window.eq_ignore_ascii_case(b"magnet:?"))
    {
        return Cow::Borrowed("<redacted-magnet>");
    }
    if let Some(scheme_end) = value.find("://") {
        if value.len() > DISPLAY_SAFE_ERROR_MAX_BYTES * 2 {
            return Cow::Borrowed("<redacted-url>");
        }
        let scheme_start = value[..scheme_end]
            .rfind(|character: char| {
                !character.is_ascii_alphanumeric() && !matches!(character, '+' | '-' | '.')
            })
            .map_or(0, |index| index + 1);
        let prefix = &value[..scheme_start];
        let candidate = value[scheme_start..].trim_end_matches(|character: char| {
            matches!(character, ',' | ';' | ')' | ']' | '}' | '"' | '\'')
        });
        let safe = url::Url::parse(candidate)
            .ok()
            .and_then(|url| {
                // `host_str` returns the URL serialization, so an IPv6 literal
                // already arrives bracketed. Re-bracketing it here produced a
                // `[[::1]]` authority that no URL parser accepts.
                let host = url.host_str()?;
                let port = url
                    .port()
                    .map(|port| format!(":{port}"))
                    .unwrap_or_default();
                Some(format!("{}://{host}{port}/<redacted>", url.scheme()))
            })
            .unwrap_or_else(|| "<redacted-url>".into());
        return Cow::Owned(format!("{prefix}{safe}"));
    }
    if value
        .find('?')
        .is_some_and(|query| value[query + 1..].contains(['=', '&']))
    {
        return Cow::Borrowed("<redacted-query>");
    }

    let trimmed = value.trim_matches(|character: char| {
        matches!(character, ',' | ';' | '(' | ')' | '{' | '}' | '"' | '\'')
    });
    let address = trimmed.parse::<SocketAddr>().is_ok()
        || trimmed.parse::<std::net::IpAddr>().is_ok()
        || trimmed
            .trim_matches(['[', ']'])
            .parse::<SocketAddr>()
            .is_ok();
    if address {
        return Cow::Borrowed("<redacted-peer>");
    }
    let bytes = trimmed.as_bytes();
    let windows_absolute = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\');
    if trimmed.starts_with('/') || trimmed.starts_with("\\\\") || windows_absolute {
        return Cow::Borrowed("<redacted-path>");
    }
    Cow::Borrowed(value)
}

fn is_secret_key(key: &str) -> bool {
    let key = key
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    // Match conservative substrings intentionally: engine error fields are
    // untrusted, and over-redacting an innocent key such as `design` is safer
    // than leaking tracker passkeys through a novel compound field name.
    // Do not narrow this to exact names without equivalent leak coverage.
    [
        "auth",
        "cookie",
        "credential",
        "key",
        "pass",
        "secret",
        "session",
        "sig",
        "token",
    ]
    .iter()
    .any(|secret| key.contains(secret))
}

fn mark_display_truncated(mut message: String) -> String {
    let mut boundary = DISPLAY_SAFE_ERROR_MAX_BYTES - ERROR_TRUNCATION_MARKER.len();
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    message.truncate(boundary);
    message.push_str(ERROR_TRUNCATION_MARKER);
    message
}

/// Install nzbd's aws-lc provider before librqbit constructs its reqwest
/// rustls client.
///
/// `librqbit`'s Rust-TLS feature brings ring-backed hashing/TLS features into
/// a workspace that already enables aws-lc. rustls 0.23 cannot infer a process
/// default when both providers are compiled, so daemon startup must call this
/// before starting either network stack.
pub fn install_process_crypto_provider() -> Result<CryptoProviderInstall, TorrentError> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(CryptoProviderInstall::AlreadyInstalled);
    }

    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) => Ok(CryptoProviderInstall::InstalledAwsLc),
        Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => {
            Ok(CryptoProviderInstall::AlreadyInstalled)
        }
        Err(_) => Err(TorrentError::CryptoProvider),
    }
}

#[derive(Debug, Clone)]
pub struct TorrentSessionConfig {
    pub dht: bool,
    pub pex: bool,
    pub listen_port_range: Option<Range<u16>>,
    pub proxy: Option<TorrentProxyConfig>,
    /// Engine resume state is an accelerator; construction never restores it.
    pub persistence_dir: Option<PathBuf>,
    pub max_peers_per_torrent: Option<usize>,
    pub max_peers_total: Option<usize>,
    pub max_known_peers_per_torrent: Option<usize>,
    pub max_known_peers_total: Option<usize>,
    pub metainfo_max_bytes: Option<u32>,
}

impl Default for TorrentSessionConfig {
    fn default() -> Self {
        Self {
            dht: false,
            pex: true,
            listen_port_range: None,
            proxy: None,
            persistence_dir: None,
            max_peers_per_torrent: None,
            max_peers_total: None,
            max_known_peers_per_torrent: None,
            max_known_peers_total: None,
            metainfo_max_bytes: None,
        }
    }
}

#[derive(Clone, Default)]
pub struct TorrentProxyConfig {
    /// A credential-free `socks5://host:port` URL.
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for TorrentProxyConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TorrentProxyConfig")
            .field("url", &"<redacted>")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl TorrentProxyConfig {
    fn engine_url(&self) -> Result<String, TorrentError> {
        let mut url = url::Url::parse(&self.url)
            .map_err(|_| TorrentError::InvalidProxy("URL is not valid"))?;
        if url.scheme() != "socks5" {
            return Err(TorrentError::InvalidProxy("URL scheme must be socks5"));
        }
        if url.host_str().is_none() || url.port().is_none() {
            return Err(TorrentError::InvalidProxy(
                "URL must contain a host and port",
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(TorrentError::InvalidProxy(
                "put credentials in the separate username and password fields",
            ));
        }
        if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
            return Err(TorrentError::InvalidProxy(
                "URL must not contain a path, query, or fragment",
            ));
        }
        // Treat a trailing slash as the same origin, but never pass alternate
        // URL syntax to rqbit's peer and HTTP tracker clients. Stable rqbit
        // parses these two paths differently.
        url.set_path("");
        match (&self.username, &self.password) {
            (None, None) => {}
            (Some(username), Some(password)) if !username.is_empty() => {
                if !username.bytes().all(is_url_unreserved)
                    || !password.bytes().all(is_url_unreserved)
                {
                    return Err(TorrentError::InvalidProxy(
                        "credentials may contain only URL-unreserved ASCII characters",
                    ));
                }
                url.set_username(username)
                    .map_err(|_| TorrentError::InvalidProxy("username is not URL-safe"))?;
                url.set_password(Some(password))
                    .map_err(|_| TorrentError::InvalidProxy("password is not URL-safe"))?;
            }
            _ => {
                return Err(TorrentError::InvalidProxy(
                    "username and password must be set together",
                ));
            }
        }
        Ok(url.into())
    }
}

fn is_url_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

#[derive(Debug, Clone, Default)]
pub struct TorrentAddConfig {
    pub paused: bool,
    pub overwrite: bool,
    pub initial_peers: Vec<SocketAddr>,
    pub only_files: Option<Vec<usize>>,
    pub preferred_id: Option<usize>,
    pub output_root: Option<PathBuf>,
}

#[derive(Clone)]
pub struct TorrentSession {
    inner: Arc<Session>,
    dht_enabled: bool,
    output_root: PathBuf,
    proxy_enabled: bool,
    magnet_metadata_timeout: Duration,
    #[cfg(any(test, feature = "test-support"))]
    magnet_metadata_timeout_gate: Option<Arc<tokio::sync::Notify>>,
    #[cfg(any(test, feature = "test-support"))]
    magnet_metadata_timeout_gate_used: Arc<AtomicBool>,
}

struct ResolvedMagnet {
    torrent_bytes: bytes::Bytes,
    seen_peers: Vec<SocketAddr>,
}

impl TorrentSession {
    /// Resolve magnet metadata in list-only mode. No managed torrent or
    /// payload storage exists when this returns.
    pub async fn resolve_magnet_metadata(&self, magnet: String) -> Result<Vec<u8>, TorrentError> {
        let resolved = self.resolve_validated_magnet(magnet, Vec::new()).await?;
        Ok(resolved.torrent_bytes.to_vec())
    }

    /// Validate resolved metainfo against the payload root selected by the
    /// durable queue owner. This must run before admission is committed.
    pub fn validate_metainfo_filesystem(
        bytes: &[u8],
        output_root: &Path,
    ) -> Result<(), TorrentError> {
        validate_existing_filesystem_paths(bytes, output_root)
    }

    /// Reject a magnet that cannot reach any implemented discovery source.
    /// Higher layers call this before creating a durable reservation.
    pub fn validate_magnet_discovery(&self, magnet: &str) -> Result<(), TorrentError> {
        let magnet = validate_magnet_contract(magnet, self.proxy_enabled)?;
        validate_magnet_discovery_available(&magnet, self.dht_enabled, false)
    }
    pub async fn start(
        output_root: PathBuf,
        config: TorrentSessionConfig,
    ) -> Result<Self, TorrentError> {
        Self::start_internal(output_root, config, None).await
    }

    /// Start a DHT session against only the supplied bootstrap nodes.
    /// Available solely to deterministic downstream test builds.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn start_with_dht_bootstrap_for_test(
        output_root: PathBuf,
        config: TorrentSessionConfig,
        dht_bootstrap_addrs: Vec<SocketAddr>,
    ) -> Result<Self, TorrentError> {
        Self::start_internal(output_root, config, Some(dht_bootstrap_addrs)).await
    }

    async fn start_internal(
        output_root: PathBuf,
        config: TorrentSessionConfig,
        test_dht_bootstrap_addrs: Option<Vec<SocketAddr>>,
    ) -> Result<Self, TorrentError> {
        if test_dht_bootstrap_addrs.is_some() {
            if config
                .listen_port_range
                .as_ref()
                .is_some_and(|range| range.start == 0 || range.start >= range.end)
            {
                return Err(TorrentError::InvalidListenPortRange);
            }
        } else {
            validate_listen_port_range(config.listen_port_range.as_ref())?;
        }
        install_process_crypto_provider()?;
        let proxy_enabled = config.proxy.is_some();
        if proxy_enabled && config.dht {
            return Err(TorrentError::ProxyWithDht);
        }
        let socks_proxy_url = config
            .proxy
            .as_ref()
            .map(TorrentProxyConfig::engine_url)
            .transpose()?;
        std::fs::create_dir_all(&output_root).map_err(engine_error)?;
        let output_root = std::fs::canonicalize(output_root).map_err(engine_error)?;
        let options = session_options(
            config.dht,
            config.pex,
            config.listen_port_range,
            socks_proxy_url,
            config.persistence_dir,
            config.max_peers_per_torrent,
            config.max_peers_total,
            config.max_known_peers_per_torrent,
            config.max_known_peers_total,
            config.metainfo_max_bytes,
        );
        #[cfg(any(test, feature = "test-support"))]
        let inner = match test_dht_bootstrap_addrs {
            Some(addresses) => {
                Session::new_with_opts_for_test(output_root.clone(), options, addresses).await
            }
            None => Session::new_with_opts(output_root.clone(), options).await,
        }
        .map_err(engine_error)?;
        #[cfg(not(any(test, feature = "test-support")))]
        let inner = {
            debug_assert!(test_dht_bootstrap_addrs.is_none());
            Session::new_with_opts(output_root.clone(), options)
                .await
                .map_err(engine_error)?
        };
        Ok(Self {
            inner,
            dht_enabled: config.dht,
            output_root,
            proxy_enabled,
            magnet_metadata_timeout: MAGNET_METADATA_TIMEOUT,
            #[cfg(any(test, feature = "test-support"))]
            magnet_metadata_timeout_gate: None,
            #[cfg(any(test, feature = "test-support"))]
            magnet_metadata_timeout_gate_used: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Override the metadata deadline solely in downstream deterministic
    /// tests. Production sessions always use [`MAGNET_METADATA_TIMEOUT`].
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn with_magnet_metadata_timeout_for_test(mut self, timeout: Duration) -> Self {
        self.magnet_metadata_timeout = timeout;
        self
    }

    /// Hold the test-only metadata deadline until a test-controlled readiness
    /// event, allowing discovery tests to prove the timeout cancels an active
    /// lookup rather than racing the first network request.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn with_magnet_metadata_timeout_gate_for_test(
        mut self,
        gate: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.magnet_metadata_timeout_gate = Some(gate);
        self.magnet_metadata_timeout_gate_used = Arc::new(AtomicBool::new(false));
        self
    }

    pub fn tcp_listen_port(&self) -> Option<u16> {
        self.inner.tcp_listen_port()
    }

    /// Canonical payload root owned by this session. Higher layers may use it
    /// to derive and revalidate the queue's durable content path, but raw
    /// engine handles and library persistence remain private to the adapter.
    pub fn output_root(&self) -> &Path {
        &self.output_root
    }

    pub fn set_download_limit_bps(&self, bytes_per_second: Option<NonZeroU32>) {
        self.inner.ratelimits.set_download_bps(bytes_per_second);
    }

    pub fn set_upload_limit_bps(&self, bytes_per_second: Option<NonZeroU32>) {
        self.inner.ratelimits.set_upload_bps(bytes_per_second);
    }

    /// Start the crate-private M0 seeder with a multi-port range so rqbit can
    /// bind the first available port itself. This removes the test-only
    /// bind/drop/rebind race without weakening the production requirement for
    /// one explicit non-zero listen port.
    #[cfg(test)]
    async fn start_with_listen_range_for_m0(
        output_root: PathBuf,
        listen_port_range: Range<u16>,
    ) -> Result<Self, TorrentError> {
        if listen_port_range.start == 0 || listen_port_range.start >= listen_port_range.end {
            return Err(TorrentError::InvalidListenPortRange);
        }
        install_process_crypto_provider()?;
        std::fs::create_dir_all(&output_root).map_err(engine_error)?;
        let output_root = std::fs::canonicalize(output_root).map_err(engine_error)?;
        let options = session_options(
            false,
            true,
            Some(listen_port_range),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let inner = Session::new_with_opts(output_root.clone(), options)
            .await
            .map_err(engine_error)?;
        Ok(Self {
            inner,
            dht_enabled: false,
            output_root,
            proxy_enabled: false,
            magnet_metadata_timeout: MAGNET_METADATA_TIMEOUT,
            #[cfg(any(test, feature = "test-support"))]
            magnet_metadata_timeout_gate: None,
            #[cfg(any(test, feature = "test-support"))]
            magnet_metadata_timeout_gate_used: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn add_metainfo(
        &self,
        bytes: Vec<u8>,
        mut config: TorrentAddConfig,
    ) -> Result<TorrentHandle, TorrentError> {
        normalize_initial_peers(&mut config.initial_peers)?;
        validate_metainfo_admission(&bytes, self.proxy_enabled, self.dht_enabled)?;
        let output_root = config.output_root.as_deref().unwrap_or(&self.output_root);
        validate_existing_filesystem_paths(&bytes, output_root)?;
        self.add_validated_metainfo(bytes.into(), config).await
    }

    /// Admit validated metainfo through a caller-supplied storage factory for
    /// the crate-private M0 fault-injection unit probe. This method is absent
    /// from every non-test build.
    #[cfg(test)]
    async fn add_metainfo_with_storage_for_m0(
        &self,
        bytes: Vec<u8>,
        mut config: TorrentAddConfig,
        storage_factory: librqbit::storage::BoxStorageFactory,
    ) -> Result<TorrentHandle, TorrentError> {
        normalize_initial_peers(&mut config.initial_peers)?;
        validate_metainfo_admission(&bytes, self.proxy_enabled, self.dht_enabled)?;
        validate_existing_filesystem_paths(&bytes, &self.output_root)?;
        let mut options = managed_add_options(config);
        options.storage_factory = Some(storage_factory);
        self.add_validated_metainfo_with_options(bytes.into(), options)
            .await
    }

    pub async fn add_magnet(
        &self,
        magnet: String,
        mut config: TorrentAddConfig,
    ) -> Result<TorrentHandle, TorrentError> {
        normalize_initial_peers(&mut config.initial_peers)?;
        let resolved = self
            .resolve_validated_magnet(magnet, config.initial_peers.clone())
            .await?;
        let output_root = config.output_root.as_deref().unwrap_or(&self.output_root);
        validate_existing_filesystem_paths(resolved.torrent_bytes.as_ref(), output_root)?;
        extend_with_resolved_peers(&mut config.initial_peers, resolved.seen_peers);
        self.add_validated_metainfo(resolved.torrent_bytes, config)
            .await
    }

    async fn resolve_validated_magnet(
        &self,
        magnet: String,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<ResolvedMagnet, TorrentError> {
        self.resolve_validated_magnet_with_timeout(
            magnet,
            initial_peers,
            self.magnet_metadata_timeout,
        )
        .await
    }

    async fn resolve_validated_magnet_with_timeout(
        &self,
        magnet: String,
        initial_peers: Vec<SocketAddr>,
        timeout: Duration,
    ) -> Result<ResolvedMagnet, TorrentError> {
        let magnet = validate_magnet_contract(&magnet, self.proxy_enabled)?;
        validate_magnet_discovery_available(&magnet, self.dht_enabled, !initial_peers.is_empty())?;
        let resolve = self.inner.add_torrent(
            AddTorrent::from_url(magnet),
            Some(magnet_resolution_options(initial_peers)),
        );
        #[cfg(any(test, feature = "test-support"))]
        let result = if let Some(gate) = self.magnet_metadata_timeout_gate.as_ref().filter(|_| {
            !self
                .magnet_metadata_timeout_gate_used
                .swap(true, Ordering::SeqCst)
        }) {
            tokio::select! {
                result = resolve => result,
                _ = async {
                    gate.notified().await;
                    tokio::time::sleep(timeout).await;
                } => return Err(TorrentError::MagnetMetadataTimeout),
            }
        } else {
            tokio::time::timeout(timeout, resolve)
                .await
                .map_err(|_| TorrentError::MagnetMetadataTimeout)?
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let result = tokio::time::timeout(timeout, resolve)
            .await
            .map_err(|_| TorrentError::MagnetMetadataTimeout)?;
        let resolved = result.map_err(|error| {
            if error
                .downcast_ref::<librqbit::InvalidResolvedMagnetMetadataError>()
                .is_some()
            {
                TorrentError::InvalidResolvedMagnetMetadata
            } else {
                engine_error(error)
            }
        })?;
        let AddTorrentResponse::ListOnly(resolved) = resolved else {
            return Err(TorrentError::MissingResolvedMagnet);
        };

        // The list-only result has a verified v1 info hash but is not yet a
        // managed torrent. Apply every nzbd admission rule, including the
        // private+DHT policy, before storage or peer handoff can exist.
        validate_metainfo_admission(
            resolved.torrent_bytes.as_ref(),
            self.proxy_enabled,
            self.dht_enabled,
        )?;
        Ok(ResolvedMagnet {
            torrent_bytes: resolved.torrent_bytes,
            seen_peers: resolved.seen_peers,
        })
    }

    async fn add_validated_metainfo(
        &self,
        bytes: bytes::Bytes,
        config: TorrentAddConfig,
    ) -> Result<TorrentHandle, TorrentError> {
        self.add_validated_metainfo_with_options(bytes, managed_add_options(config))
            .await
    }

    async fn add_validated_metainfo_with_options(
        &self,
        bytes: bytes::Bytes,
        options: AddTorrentOptions,
    ) -> Result<TorrentHandle, TorrentError> {
        let handle = self
            .inner
            // Keep rqbit's URL variant outside managed admission. Its stable
            // HTTP client buffers the response before nzbd can enforce the
            // metainfo limit and does not expose the proposal's fetch policy.
            .add_torrent(AddTorrent::from_bytes(bytes), Some(options))
            .await
            .map_err(engine_error)?
            .into_handle()
            .ok_or(TorrentError::MissingHandle)?;
        Ok(TorrentHandle { inner: handle })
    }

    pub async fn pause(&self, torrent: &TorrentHandle) -> Result<(), TorrentError> {
        self.inner.pause(&torrent.inner).await.map_err(engine_error)
    }

    pub async fn resume(&self, torrent: &TorrentHandle) -> Result<(), TorrentError> {
        self.inner
            .unpause(&torrent.inner)
            .await
            .map_err(engine_error)
    }

    /// Forget a torrent idempotently, optionally deleting the files the
    /// engine's parsed metainfo owns. Higher layers still prove the persisted
    /// canonical root before they may request `delete_files`.
    pub async fn delete(
        &self,
        torrent: &TorrentHandle,
        delete_files: bool,
    ) -> Result<(), TorrentError> {
        let id = TorrentIdOrHash::Id(torrent.id());
        if self.inner.get(id).is_none() {
            return Ok(());
        }
        self.inner
            .delete(id, delete_files)
            .await
            .map_err(engine_error)
    }

    pub async fn stop(&self) {
        self.inner.stop().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetainfoDescriptor {
    pub info_hash_v1: String,
    pub name: String,
    pub files: Vec<(PathBuf, u64)>,
    pub total_bytes: u64,
}

/// Validate and project durable descriptor facts without creating storage.
pub fn inspect_metainfo(
    bytes: &[u8],
    proxy_enabled: bool,
    dht_enabled: bool,
) -> Result<MetainfoDescriptor, TorrentError> {
    validate_metainfo_admission(bytes, proxy_enabled, dht_enabled)?;
    let meta =
        librqbit::torrent_from_bytes::<librqbit::ByteBuf<'_>>(bytes).map_err(engine_error)?;
    let name = meta
        .info
        .name
        .as_ref()
        .and_then(|n| std::str::from_utf8(n.as_ref()).ok())
        .unwrap_or("torrent")
        .to_string();
    let mut files = Vec::new();
    let mut total_bytes = 0u64;
    for file in meta.info.iter_file_details().map_err(engine_error)? {
        if file.attrs().padding {
            continue;
        }
        let mut path = PathBuf::new();
        for component in file.filename.iter_components() {
            path.push(
                std::str::from_utf8(component.map_err(engine_error)?.as_ref())
                    .map_err(engine_error)?,
            );
        }
        total_bytes =
            total_bytes
                .checked_add(file.len)
                .ok_or(TorrentError::InvalidMetainfoGeometry(
                    "aggregate payload length overflows u64",
                ))?;
        files.push((path, file.len));
    }
    Ok(MetainfoDescriptor {
        info_hash_v1: meta.info_hash.as_string(),
        name,
        files,
        total_bytes,
    })
}

/// Syntax-only magnet admission check for callers that must reject malformed
/// requests before reserving durable queue state.
pub fn validate_magnet_source(magnet: &str, proxy_enabled: bool) -> Result<(), TorrentError> {
    validate_magnet_contract(magnet, proxy_enabled).map(|_| ())
}

/// Engine-owned identity. This deliberately contains no nzbd queue identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EngineIdentity {
    pub id: usize,
    pub info_hash_v1: String,
}

/// One queue-authorized engine restore descriptor. Metainfo is supplied by
/// nzbd's durable state boundary; rqbit's persistence file is never enumerated
/// as an admission source.
#[derive(Debug, Clone)]
pub struct RestoreDescriptor {
    pub metainfo: Vec<u8>,
    pub expected_info_hash_v1: String,
    pub preferred_id: Option<usize>,
    pub selected_files: Option<Vec<usize>>,
    pub output_root: Option<PathBuf>,
}

/// Owns all raw handles for one maintained session, indexed only by engine
/// identity and info hash. Raw handles never need to cross this boundary.
pub struct TorrentRegistry {
    session: TorrentSession,
    by_hash: HashMap<String, TorrentHandle>,
    id_to_hash: HashMap<usize, String>,
}

impl TorrentRegistry {
    pub fn new(session: TorrentSession) -> Self {
        Self {
            session,
            by_hash: HashMap::new(),
            id_to_hash: HashMap::new(),
        }
    }

    pub fn set_download_limit_bps(&self, bytes_per_second: Option<NonZeroU32>) {
        self.session.set_download_limit_bps(bytes_per_second);
    }

    pub fn set_upload_limit_bps(&self, bytes_per_second: Option<NonZeroU32>) {
        self.session.set_upload_limit_bps(bytes_per_second);
    }

    /// Managed admission after the caller's durable queue commit.
    pub async fn add_committed(
        &mut self,
        metainfo: Vec<u8>,
        config: TorrentAddConfig,
    ) -> Result<EngineIdentity, TorrentError> {
        let handle = self.session.add_metainfo(metainfo, config).await?;
        let identity = EngineIdentity {
            id: handle.id(),
            info_hash_v1: handle.info_hash(),
        };
        if self.by_hash.contains_key(&identity.info_hash_v1)
            || self.id_to_hash.contains_key(&identity.id)
        {
            self.session.delete(&handle, false).await?;
            return Err(TorrentError::DuplicateEngineIdentity);
        }
        self.id_to_hash
            .insert(identity.id, identity.info_hash_v1.clone());
        self.by_hash.insert(identity.info_hash_v1.clone(), handle);
        Ok(identity)
    }

    /// Restore only caller-authorized descriptors, always paused. Duplicate
    /// durable identities fail before a second wrapper can be admitted.
    pub async fn restore_selected(
        &mut self,
        descriptors: impl IntoIterator<Item = RestoreDescriptor>,
    ) -> Result<Vec<EngineIdentity>, TorrentError> {
        let mut restored = Vec::new();
        for descriptor in descriptors {
            if self.by_hash.contains_key(&descriptor.expected_info_hash_v1)
                || descriptor
                    .preferred_id
                    .is_some_and(|id| self.id_to_hash.contains_key(&id))
            {
                return Err(TorrentError::DuplicateEngineIdentity);
            }
            let handle = self
                .session
                .add_metainfo(
                    descriptor.metainfo,
                    TorrentAddConfig {
                        paused: true,
                        overwrite: true,
                        only_files: descriptor.selected_files,
                        preferred_id: descriptor.preferred_id,
                        output_root: descriptor.output_root,
                        ..Default::default()
                    },
                )
                .await?;
            let identity = EngineIdentity {
                id: handle.id(),
                info_hash_v1: handle.info_hash(),
            };
            if identity.info_hash_v1 != descriptor.expected_info_hash_v1 {
                self.session.delete(&handle, false).await?;
                return Err(TorrentError::RestoreHashMismatch);
            }
            if self.id_to_hash.contains_key(&identity.id)
                || self.by_hash.contains_key(&identity.info_hash_v1)
            {
                self.session.delete(&handle, false).await?;
                return Err(TorrentError::DuplicateEngineIdentity);
            }
            self.id_to_hash
                .insert(identity.id, identity.info_hash_v1.clone());
            self.by_hash.insert(identity.info_hash_v1.clone(), handle);
            restored.push(identity);
        }
        Ok(restored)
    }

    pub fn identities(&self) -> Vec<EngineIdentity> {
        let mut identities: Vec<_> = self
            .id_to_hash
            .iter()
            .map(|(&id, hash)| EngineIdentity {
                id,
                info_hash_v1: hash.clone(),
            })
            .collect();
        identities.sort_by_key(|identity| identity.id);
        identities
    }

    pub fn identity_by_hash(&self, hash: &str) -> Option<EngineIdentity> {
        self.by_hash.get(hash).map(|handle| EngineIdentity {
            id: handle.id(),
            info_hash_v1: handle.info_hash(),
        })
    }

    pub async fn resume(&self, identity: &EngineIdentity) -> Result<(), TorrentError> {
        let handle = self
            .by_hash
            .get(&identity.info_hash_v1)
            .filter(|handle| handle.id() == identity.id)
            .ok_or(TorrentError::MissingHandle)?;
        if !handle.is_paused() {
            return Ok(());
        }
        self.session.resume(handle).await
    }

    pub async fn pause(&self, identity: &EngineIdentity) -> Result<(), TorrentError> {
        let handle = self
            .by_hash
            .get(&identity.info_hash_v1)
            .filter(|handle| handle.id() == identity.id)
            .ok_or(TorrentError::MissingHandle)?;
        if handle.is_paused() {
            return Ok(());
        }
        self.session.pause(handle).await
    }

    pub async fn delete(
        &mut self,
        identity: &EngineIdentity,
        delete_files: bool,
    ) -> Result<(), TorrentError> {
        let handle = self
            .by_hash
            .get(&identity.info_hash_v1)
            .filter(|handle| handle.id() == identity.id)
            .cloned()
            .ok_or(TorrentError::MissingHandle)?;
        self.session.delete(&handle, delete_files).await?;
        self.by_hash.remove(&identity.info_hash_v1);
        self.id_to_hash.remove(&identity.id);
        Ok(())
    }

    pub fn is_paused(&self, identity: &EngineIdentity) -> Option<bool> {
        self.by_hash
            .get(&identity.info_hash_v1)
            .filter(|handle| handle.id() == identity.id)
            .map(TorrentHandle::is_paused)
    }

    pub fn stats(&self, identity: &EngineIdentity) -> Option<TorrentStats> {
        self.by_hash
            .get(&identity.info_hash_v1)
            .filter(|handle| handle.id() == identity.id)
            .map(TorrentHandle::stats)
    }

    pub async fn wait_until_initialized(
        &self,
        identity: &EngineIdentity,
    ) -> Result<(), TorrentError> {
        let handle = self
            .by_hash
            .get(&identity.info_hash_v1)
            .filter(|handle| handle.id() == identity.id)
            .ok_or(TorrentError::MissingHandle)?;
        handle.wait_until_initialized().await
    }

    pub async fn stop(self) {
        self.session.stop().await;
    }
}

fn magnet_resolution_options(initial_peers: Vec<SocketAddr>) -> AddTorrentOptions {
    exact_add_options(false, false, true, initial_peers)
}

fn managed_add_options(config: TorrentAddConfig) -> AddTorrentOptions {
    let mut options =
        exact_add_options(config.paused, config.overwrite, false, config.initial_peers);
    options.only_files = config.only_files;
    options.preferred_id = config.preferred_id;
    options.output_folder = config
        .output_root
        .map(|path| path.to_string_lossy().into_owned());
    options
}

fn exact_add_options(
    paused: bool,
    overwrite: bool,
    list_only: bool,
    initial_peers: Vec<SocketAddr>,
) -> AddTorrentOptions {
    // Keep every stable 8.1.1 field explicit. In particular, the adapter does
    // not allow selection, alternate output roots, injected trackers, custom
    // storage, or deferred writes to appear through an upstream default.
    AddTorrentOptions {
        paused,
        only_files_regex: None,
        only_files: None,
        overwrite,
        list_only,
        output_folder: None,
        sub_folder: None,
        peer_opts: None,
        force_tracker_interval: None,
        disable_trackers: false,
        ratelimits: Default::default(),
        initial_peers: Some(initial_peers),
        peer_limit: None,
        known_peer_limit: None,
        preferred_id: None,
        storage_factory: None,
        defer_writes: None,
        trackers: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn session_options(
    dht: bool,
    pex: bool,
    listen_port_range: Option<Range<u16>>,
    socks_proxy_url: Option<String>,
    persistence_dir: Option<PathBuf>,
    max_peers_per_torrent: Option<usize>,
    max_peers_total: Option<usize>,
    max_known_peers_per_torrent: Option<usize>,
    max_known_peers_total: Option<usize>,
    metainfo_max_bytes: Option<u32>,
) -> SessionOptions {
    SessionOptions {
        disable_dht: !dht,
        disable_pex: !pex,
        // librqbit's default persistent DHT state lives in rqbit's
        // process-global cache directory. nzbd must not share a listen port or
        // routing table with another session on the same host.
        disable_dht_persistence: true,
        listen_port_range,
        // librqbit 8.1.1 unconditionally compiles an advisory-affected
        // quick-xml through its UPnP helper. The M0 adapter deliberately has
        // no input that can turn this runtime path on.
        enable_upnp_port_forwarding: false,
        socks_proxy_url,
        // Spell out every stable 8.1.1 option instead of inheriting upstream
        // defaults. A newly added engine capability must become a compile-time
        // review event before it can affect nzbd's network or storage boundary.
        dht_config: None,
        fastresume: persistence_dir.is_some(),
        persistence: persistence_dir.map(|folder| librqbit::SessionPersistenceConfig::Json {
            folder: Some(folder),
        }),
        // Even when M2 supplies an nzbd-owned persistence directory, session
        // construction must admit nothing until the queue owner explicitly
        // restores its authoritative jobs.
        disable_auto_restore: true,
        // One Runner identity per engine session: a BEP 20 peer ID, a
        // matching HTTP User-Agent, and (in the peer options) the BEP 10 `v`.
        peer_id: Some(identity::generate_peer_id()),
        peer_opts: Some(explicit_peer_connection_options(metainfo_max_bytes)),
        http_user_agent: Some(identity::CLIENT_USER_AGENT.to_owned()),
        known_peer_limit: Some(max_known_peers_per_torrent.unwrap_or(MAX_KNOWN_PEERS_PER_TORRENT)),
        known_peer_limit_total: Some(max_known_peers_total.unwrap_or(MAX_KNOWN_PEERS_TOTAL)),
        defer_writes_up_to: None,
        default_storage_factory: None,
        cancellation_token: None,
        // An initialization can scan and hash the payload before a torrent
        // becomes live. Keep that disk-heavy work serial even though stable
        // rqbit's unset default permits three concurrent initializations.
        concurrent_init_limit: Some(MAX_CONCURRENT_TORRENT_INITIALIZATIONS),
        root_span: None,
        ratelimits: Default::default(),
        peer_limit: Some(max_peers_per_torrent.unwrap_or(MAX_LIVE_PEERS_PER_TORRENT)),
        peer_limit_total: Some(max_peers_total.unwrap_or(MAX_LIVE_PEERS_TOTAL)),
        blocklist_url: None,
        trackers: HashSet::new(),
    }
}

fn explicit_peer_connection_options(metainfo_max_bytes: Option<u32>) -> PeerConnectionOptions {
    // Pin stable 8.1.1's effective fallbacks so a future engine default cannot
    // silently lengthen connection or I/O lifetime at nzbd's network boundary.
    PeerConnectionOptions {
        connect_timeout: Some(PEER_CONNECT_TIMEOUT),
        read_write_timeout: Some(PEER_READ_WRITE_TIMEOUT),
        keep_alive_interval: Some(PEER_KEEP_ALIVE_INTERVAL),
        max_metadata_size: Some(metainfo_max_bytes.unwrap_or(DEFAULT_MAX_METAINFO_BYTES as u32)),
        client_version: Some(identity::CLIENT_HANDSHAKE_VERSION),
    }
}

fn validate_listen_port_range(range: Option<&Range<u16>>) -> Result<(), TorrentError> {
    let Some(range) = range else {
        return Ok(());
    };
    if range.start == 0 || range.start.checked_add(1) != Some(range.end) {
        return Err(TorrentError::InvalidListenPortRange);
    }
    Ok(())
}

fn normalize_initial_peers(peers: &mut Vec<SocketAddr>) -> Result<(), TorrentError> {
    let mut unique = HashSet::with_capacity(peers.len().min(MAX_INITIAL_PEERS));
    let mut normalized = Vec::with_capacity(peers.len().min(MAX_INITIAL_PEERS));
    for peer in peers.iter().copied() {
        if !valid_initial_peer(peer) {
            return Err(TorrentError::InvalidInitialPeer(
                "peers must be unicast IPv4 endpoints with non-zero ports",
            ));
        }
        if unique.insert(peer) {
            if normalized.len() == MAX_INITIAL_PEERS {
                return Err(TorrentError::TooManyInitialPeers {
                    count: normalized.len() + 1,
                    limit: MAX_INITIAL_PEERS,
                });
            }
            normalized.push(peer);
        }
    }
    *peers = normalized;
    Ok(())
}

fn extend_with_resolved_peers(
    peers: &mut Vec<SocketAddr>,
    resolved: impl IntoIterator<Item = SocketAddr>,
) {
    let mut unique = peers.iter().copied().collect::<HashSet<_>>();
    for peer in resolved {
        if peers.len() == MAX_INITIAL_PEERS {
            break;
        }
        if valid_initial_peer(peer) && unique.insert(peer) {
            peers.push(peer);
        }
    }
}

fn valid_initial_peer(peer: SocketAddr) -> bool {
    let SocketAddr::V4(peer) = peer else {
        return false;
    };
    let address = peer.ip();
    peer.port() != 0
        && !address.is_unspecified()
        && !address.is_multicast()
        && *address != std::net::Ipv4Addr::BROADCAST
}

#[cfg(any(test, feature = "fuzzing"))]
fn validate_metainfo_contract(bytes: &[u8], proxy_enabled: bool) -> Result<bool, TorrentError> {
    validate_metainfo_contract_with_limit(bytes, proxy_enabled, false, DEFAULT_MAX_METAINFO_BYTES)
}

fn validate_metainfo_contract_with_limit(
    bytes: &[u8],
    proxy_enabled: bool,
    dht_enabled: bool,
    max_metainfo_bytes: usize,
) -> Result<bool, TorrentError> {
    validate_metainfo_size_with_limit(bytes.len(), max_metainfo_bytes)?;
    validate_metainfo_version(bytes)?;
    let metainfo =
        librqbit::torrent_from_bytes::<librqbit::ByteBuf<'_>>(bytes).map_err(engine_error)?;
    validate_metainfo_geometry(&metainfo.info)?;
    validate_metainfo_paths(&metainfo.info)?;
    if metainfo.info.private && dht_enabled {
        return Err(TorrentError::PrivateMetainfoWithDht);
    }
    let mut trackers = HashSet::new();
    for tracker in metainfo.iter_announce() {
        let tracker = AsRef::<[u8]>::as_ref(tracker);
        if validate_tracker_url(tracker)? && proxy_enabled {
            return Err(TorrentError::ProxyWithUdpTracker);
        }
        if !tracker.is_empty()
            && trackers.insert(tracker.to_vec())
            && trackers.len() > MAX_TRACKERS_PER_TORRENT
        {
            return Err(TorrentError::TooManyTrackers {
                count: trackers.len(),
                limit: MAX_TRACKERS_PER_TORRENT,
            });
        }
    }
    if !metainfo.info.private {
        return Ok(false);
    }
    if trackers.len() != 1 {
        return Err(TorrentError::PrivateTrackerCount(trackers.len()));
    }
    Ok(true)
}

/// Exercise the metadata contract stage shared by file and resolved-magnet admission.
///
/// This surface exists only for the out-of-workspace fuzz harness. It starts
/// no session and performs no network or filesystem I/O. File admission adds
/// the private-torrent/DHT check and filesystem checks after this stage.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_metainfo_preflight(bytes: &[u8], proxy_enabled: bool) -> Result<bool, TorrentError> {
    validate_metainfo_contract(bytes, proxy_enabled)
}

/// Exercise the exact magnet preflight used before engine parsing.
///
/// This surface exists only for the out-of-workspace fuzz harness. It starts
/// no session and performs no network or filesystem I/O.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_magnet_preflight(magnet: &str, proxy_enabled: bool) -> Result<String, TorrentError> {
    validate_magnet_contract(magnet, proxy_enabled)
}

/// Exercise the complete metadata-only file-admission preflight.
///
/// This includes the private-torrent/DHT relationship but deliberately stops
/// before filesystem inspection, session creation, or network I/O.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_metainfo_admission(
    bytes: &[u8],
    proxy_enabled: bool,
    dht_enabled: bool,
) -> Result<(), TorrentError> {
    validate_metainfo_admission(bytes, proxy_enabled, dht_enabled)
}

fn validate_metainfo_admission(
    bytes: &[u8],
    proxy_enabled: bool,
    dht_enabled: bool,
) -> Result<(), TorrentError> {
    let private = validate_metainfo_contract_with_limit(
        bytes,
        proxy_enabled,
        dht_enabled,
        DEFAULT_MAX_METAINFO_BYTES,
    )?;
    validate_private_discovery(private, dht_enabled)
}

fn validate_private_discovery(private: bool, dht_enabled: bool) -> Result<(), TorrentError> {
    if private && dht_enabled {
        return Err(TorrentError::PrivateMetainfoWithDht);
    }
    Ok(())
}

fn validate_metainfo_geometry<BufType: AsRef<[u8]>>(
    info: &librqbit::TorrentMetaV1Info<BufType>,
) -> Result<(), TorrentError> {
    if info.piece_length == 0 {
        return Err(TorrentError::InvalidMetainfoGeometry(
            "piece length must be greater than zero",
        ));
    }

    let mut total_length = 0_u64;
    for length in info.iter_file_lengths().map_err(engine_error)? {
        total_length =
            total_length
                .checked_add(length)
                .ok_or(TorrentError::InvalidMetainfoGeometry(
                    "aggregate payload length overflows u64",
                ))?;
    }
    if total_length == 0 {
        return Err(TorrentError::InvalidMetainfoGeometry(
            "aggregate payload length must be greater than zero",
        ));
    }

    let piece_hash_bytes = info.pieces.as_ref().len();
    if piece_hash_bytes % 20 != 0 {
        return Err(TorrentError::InvalidMetainfoGeometry(
            "v1 piece hashes must be a whole number of 20-byte SHA-1 values",
        ));
    }
    let actual_piece_count = piece_hash_bytes / 20;
    let expected_piece_count = total_length.div_ceil(u64::from(info.piece_length));
    if u64::try_from(actual_piece_count).ok() != Some(expected_piece_count) {
        return Err(TorrentError::InvalidMetainfoGeometry(
            "piece hash count does not match payload length and piece length",
        ));
    }

    let piece_length = u64::from(info.piece_length);
    let normal_chunks = piece_length.div_ceil(RQBIT_CHUNK_BYTES);
    let last_piece_length = match total_length % piece_length {
        0 => piece_length,
        remainder => remainder,
    };
    let total_chunks = expected_piece_count
        .checked_sub(1)
        .and_then(|normal_pieces| normal_pieces.checked_mul(normal_chunks))
        .and_then(|chunks| chunks.checked_add(last_piece_length.div_ceil(RQBIT_CHUNK_BYTES)))
        .ok_or(TorrentError::InvalidMetainfoGeometry(
            "aggregate chunk count overflows the adapter representation",
        ))?;
    if total_chunks > u64::from(u32::MAX) {
        return Err(TorrentError::InvalidMetainfoGeometry(
            "aggregate chunk count exceeds rqbit's u32 representation",
        ));
    }

    Ok(())
}

fn validate_existing_filesystem_paths(
    bytes: &[u8],
    output_root: &Path,
) -> Result<(), TorrentError> {
    let metainfo =
        librqbit::torrent_from_bytes::<librqbit::ByteBuf<'_>>(bytes).map_err(engine_error)?;
    let multi_file_root = metainfo
        .info
        .files
        .as_ref()
        .and(metainfo.info.name.as_ref())
        .map(|name| {
            std::str::from_utf8(name.as_ref()).map_err(|_| {
                TorrentError::UnsafeMetainfoPath("path components must contain valid UTF-8")
            })
        })
        .transpose()?;

    for file in metainfo.info.iter_file_details().map_err(engine_error)? {
        let mut candidate = output_root.to_path_buf();
        if let Some(root) = multi_file_root {
            candidate.push(root);
            if validate_existing_path(&candidate, ExistingPathKind::Directory)? {
                continue;
            }
        }
        let mut components = file.filename.iter_components().peekable();
        while let Some(component) = components.next() {
            let component = component.map_err(|_| {
                TorrentError::UnsafeMetainfoPath(
                    "components must be portable UTF-8 names without traversal or separators",
                )
            })?;
            candidate.push(component);
            let expected = if components.peek().is_some() {
                ExistingPathKind::Directory
            } else {
                ExistingPathKind::File
            };
            if validate_existing_path(&candidate, expected)? {
                break;
            }
        }
    }
    Ok(())
}

/// Returns true once a missing prefix proves that no deeper existing symlink
/// can currently exist. Production writes still require a descriptor-relative
/// containment design to close the check/write race.
#[derive(Clone, Copy)]
enum ExistingPathKind {
    Directory,
    File,
}

fn validate_existing_path(path: &Path, expected: ExistingPathKind) -> Result<bool, TorrentError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(TorrentError::ExistingPathSymlink),
        Ok(metadata) if matches!(expected, ExistingPathKind::Directory) && !metadata.is_dir() => {
            Err(TorrentError::ExistingPathType(
                "a payload prefix is not a directory",
            ))
        }
        Ok(metadata) if matches!(expected, ExistingPathKind::File) && !metadata.is_file() => Err(
            TorrentError::ExistingPathType("a payload leaf is not a regular file"),
        ),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(engine_error(error)),
    }
}

fn validate_metainfo_paths<BufType: AsRef<[u8]>>(
    info: &librqbit::TorrentMetaV1Info<BufType>,
) -> Result<(), TorrentError> {
    validate_metainfo_paths_with_limits(info, MetainfoPathLimits::PROPOSAL)
}

#[derive(Clone, Copy)]
struct MetainfoPathLimits {
    files: usize,
    relative_path_bytes: usize,
    all_path_bytes: usize,
}

impl MetainfoPathLimits {
    const PROPOSAL: Self = Self {
        files: MAX_TORRENT_FILES,
        relative_path_bytes: MAX_TORRENT_RELATIVE_PATH_BYTES,
        all_path_bytes: MAX_TORRENT_PATH_BYTES,
    };
}

fn validate_metainfo_paths_with_limits<BufType: AsRef<[u8]>>(
    info: &librqbit::TorrentMetaV1Info<BufType>,
    limits: MetainfoPathLimits,
) -> Result<(), TorrentError> {
    let declared_name = info.name.as_ref().ok_or(TorrentError::UnsafeMetainfoPath(
        "torrent metainfo must declare an explicit payload name",
    ))?;
    let (root_path_bytes, root_collision_key) = if info.files.is_some() {
        validate_path_component(declared_name.as_ref())?;
        let root = std::str::from_utf8(declared_name.as_ref()).map_err(|_| {
            TorrentError::UnsafeMetainfoPath("path components must contain valid UTF-8")
        })?;
        (root.len(), portable_collision_key(root))
    } else {
        (0, String::new())
    };

    let files = info.iter_file_details().map_err(engine_error)?;
    let mut file_count = 0usize;
    let mut all_path_bytes = 0usize;
    let mut file_path_collision_keys = HashSet::new();
    let mut directory_path_collision_keys = HashSet::new();
    for file in files {
        file_count = file_count.saturating_add(1);
        if file_count > limits.files {
            return Err(TorrentError::TooManyFiles {
                count: file_count,
                limit: limits.files,
            });
        }
        if file.attrs().symlink || file.symlink_path.is_some() {
            return Err(TorrentError::UnsafeMetainfoPath(
                "metainfo-declared symlinks are not supported",
            ));
        }

        let mut component_count = 0usize;
        let mut relative_path_bytes = root_path_bytes;
        let mut collision_key = root_collision_key.clone();
        let mut components = file.filename.iter_components().peekable();
        while let Some(component) = components.next() {
            component_count += 1;
            let component = component.map_err(|_| {
                TorrentError::UnsafeMetainfoPath(
                    "components must be portable UTF-8 names without traversal or separators",
                )
            })?;
            validate_path_component(component.as_bytes())?;
            if !collision_key.is_empty() {
                collision_key.push('/');
            }
            collision_key.push_str(&portable_collision_key(component));
            if relative_path_bytes != 0 {
                relative_path_bytes = relative_path_bytes.saturating_add(1);
            }
            relative_path_bytes = relative_path_bytes.saturating_add(component.len());
            if relative_path_bytes > limits.relative_path_bytes {
                return Err(TorrentError::PathTooLong {
                    size: relative_path_bytes,
                    limit: limits.relative_path_bytes,
                });
            }
            if components.peek().is_some() {
                if file_path_collision_keys.contains(&collision_key) {
                    return Err(TorrentError::PathCollision);
                }
                directory_path_collision_keys.insert(collision_key.clone());
            }
        }
        if component_count == 0 {
            return Err(TorrentError::UnsafeMetainfoPath(
                "file paths must contain at least one component",
            ));
        }
        if directory_path_collision_keys.contains(&collision_key)
            || !file_path_collision_keys.insert(collision_key)
        {
            return Err(TorrentError::PathCollision);
        }
        all_path_bytes = all_path_bytes.saturating_add(relative_path_bytes);
        if all_path_bytes > limits.all_path_bytes {
            return Err(TorrentError::PathMetadataTooLarge {
                size: all_path_bytes,
                limit: limits.all_path_bytes,
            });
        }
    }
    Ok(())
}

fn portable_collision_key(component: &str) -> String {
    let case_mapper = icu_casemap::CaseMapper::new();
    // Full folding covers aliases observed on default case-insensitive macOS
    // storage, including sharp-s/SS and compatibility ligature expansions.
    // It deliberately leaves compatibility width variants distinct. Windows'
    // invariant path comparison additionally aliases dotless i with ASCII I,
    // so keep that conservative portability mapping explicit.
    let fully_folded = case_mapper.fold_string(component);
    let windows_folded = fully_folded
        .chars()
        .map(|character| match character {
            '\u{0131}' => 'i',
            _ => character,
        })
        .collect::<String>();
    icu_normalizer::ComposingNormalizer::new_nfc()
        .normalize(&windows_folded)
        .into_owned()
}

fn validate_metainfo_size_with_limit(size: usize, limit: usize) -> Result<(), TorrentError> {
    if size > limit {
        return Err(TorrentError::MetainfoTooLarge { size, limit });
    }
    Ok(())
}

fn validate_magnet_size(size: usize) -> Result<(), TorrentError> {
    if size > MAX_MAGNET_URI_BYTES {
        return Err(TorrentError::MagnetTooLong {
            size,
            limit: MAX_MAGNET_URI_BYTES,
        });
    }
    Ok(())
}

fn validate_path_component(component: &[u8]) -> Result<(), TorrentError> {
    if component.is_empty() {
        return Err(TorrentError::UnsafeMetainfoPath(
            "path components cannot be empty",
        ));
    }
    if component.len() > MAX_TORRENT_PATH_COMPONENT_BYTES {
        return Err(TorrentError::PathComponentTooLong {
            size: component.len(),
            limit: MAX_TORRENT_PATH_COMPONENT_BYTES,
        });
    }
    if component.contains(&0) {
        return Err(TorrentError::UnsafeMetainfoPath(
            "path components cannot contain NUL",
        ));
    }
    if component.contains(&b'/') || component.contains(&b'\\') {
        return Err(TorrentError::UnsafeMetainfoPath(
            "path components cannot contain platform separators",
        ));
    }
    if matches!(component, b"." | b"..") {
        return Err(TorrentError::UnsafeMetainfoPath(
            "dot path components are not allowed",
        ));
    }
    if component.len() >= 2 && component[0].is_ascii_alphabetic() && component[1] == b':' {
        return Err(TorrentError::UnsafeMetainfoPath(
            "Windows drive prefixes are not allowed",
        ));
    }
    if component
        .iter()
        .any(|byte| *byte < b' ' || b"<>:\"|?*".contains(byte))
    {
        return Err(TorrentError::UnsafeMetainfoPath(
            "Windows-reserved characters are not allowed",
        ));
    }
    if matches!(component.last(), Some(b'.' | b' ')) {
        return Err(TorrentError::UnsafeMetainfoPath(
            "path components cannot end with a dot or space",
        ));
    }
    let component = std::str::from_utf8(component).map_err(|_| {
        TorrentError::UnsafeMetainfoPath("path components must contain valid UTF-8")
    })?;
    if is_windows_reserved_component(component) {
        return Err(TorrentError::UnsafeMetainfoPath(
            "Windows device names are not allowed",
        ));
    }
    Ok(())
}

fn is_windows_reserved_component(component: &str) -> bool {
    let basename = component
        .split('.')
        .next()
        .unwrap_or(component)
        .trim_end_matches(' ')
        .to_ascii_uppercase();
    matches!(
        basename.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "CLOCK$"
            | "CONIN$"
            | "CONOUT$"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "COM¹"
            | "COM²"
            | "COM³"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
            | "LPT¹"
            | "LPT²"
            | "LPT³"
    )
}

fn validate_metainfo_version(bytes: &[u8]) -> Result<(), TorrentError> {
    let (has_v1, has_v2) = MetainfoVersionScanner::new(bytes).scan()?;
    match (has_v1, has_v2) {
        (true, true) => Err(TorrentError::UnsupportedHybridMetainfo),
        (false, true) => Err(TorrentError::UnsupportedV2Metainfo),
        _ => Ok(()),
    }
}

/// Read only the direct keys of the metainfo `info` dictionary. librqbit's
/// v1 struct deliberately ignores unknown BEP 52 fields, so using that struct
/// alone would silently accept hybrid input. This scanner skips length-framed
/// bencode values without inspecting payload bytes, which avoids false hits
/// inside piece hashes or filenames.
struct MetainfoVersionScanner<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> MetainfoVersionScanner<'a> {
    const MAX_DEPTH: usize = 128;

    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn scan(mut self) -> Result<(bool, bool), TorrentError> {
        self.expect(b'd', "metainfo root must be a dictionary")?;
        let mut version = None;
        while self.peek()? != b'e' {
            let key = self.byte_string()?;
            if key == b"info" {
                if version.is_some() {
                    return Err(Self::malformed("metainfo has duplicate info dictionaries"));
                }
                version = Some(self.scan_info_dictionary()?);
            } else {
                self.skip_value(0)?;
            }
        }
        self.position += 1;
        if self.position != self.bytes.len() {
            return Err(Self::malformed("metainfo has trailing bytes"));
        }
        version.ok_or_else(|| Self::malformed("metainfo has no info dictionary"))
    }

    fn scan_info_dictionary(&mut self) -> Result<(bool, bool), TorrentError> {
        self.expect(b'd', "metainfo info value must be a dictionary")?;
        let mut has_v1 = false;
        let mut has_v2 = false;
        while self.peek()? != b'e' {
            let key = self.byte_string()?;
            has_v1 |= key == b"pieces";
            has_v2 |= key == b"meta version";
            self.skip_value(0)?;
        }
        self.position += 1;
        Ok((has_v1, has_v2))
    }

    fn skip_value(&mut self, depth: usize) -> Result<(), TorrentError> {
        if depth > Self::MAX_DEPTH {
            return Err(Self::malformed("metainfo nesting is too deep"));
        }
        match self.peek()? {
            b'i' => {
                self.position += 1;
                let integer_start = self.position;
                while self.peek()? != b'e' {
                    self.position += 1;
                }
                if self.position == integer_start {
                    return Err(Self::malformed("metainfo contains an empty integer"));
                }
                self.position += 1;
            }
            b'l' => {
                self.position += 1;
                while self.peek()? != b'e' {
                    self.skip_value(depth + 1)?;
                }
                self.position += 1;
            }
            b'd' => {
                self.position += 1;
                while self.peek()? != b'e' {
                    self.byte_string()?;
                    self.skip_value(depth + 1)?;
                }
                self.position += 1;
            }
            b'0'..=b'9' => {
                self.byte_string()?;
            }
            _ => return Err(Self::malformed("metainfo contains invalid bencode")),
        }
        Ok(())
    }

    fn byte_string(&mut self) -> Result<&'a [u8], TorrentError> {
        let mut len = 0usize;
        let mut saw_digit = false;
        loop {
            let byte = self.peek()?;
            if byte == b':' {
                if !saw_digit {
                    return Err(Self::malformed("metainfo byte string has no length"));
                }
                self.position += 1;
                break;
            }
            if !byte.is_ascii_digit() {
                return Err(Self::malformed(
                    "metainfo byte string has an invalid length",
                ));
            }
            saw_digit = true;
            len = len
                .checked_mul(10)
                .and_then(|value| value.checked_add((byte - b'0') as usize))
                .ok_or_else(|| Self::malformed("metainfo byte string length overflows"))?;
            self.position += 1;
        }
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Self::malformed("metainfo byte string is truncated"))?;
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn expect(&mut self, expected: u8, message: &'static str) -> Result<(), TorrentError> {
        if self.peek()? != expected {
            return Err(Self::malformed(message));
        }
        self.position += 1;
        Ok(())
    }

    fn peek(&self) -> Result<u8, TorrentError> {
        self.bytes
            .get(self.position)
            .copied()
            .ok_or_else(|| Self::malformed("metainfo is truncated"))
    }

    fn malformed(message: &'static str) -> TorrentError {
        TorrentError::Engine(message.into())
    }
}

fn validate_magnet_contract(magnet: &str, proxy_enabled: bool) -> Result<String, TorrentError> {
    validate_magnet_size(magnet.len())?;
    let url = url::Url::parse(magnet)
        .map_err(|_| TorrentError::InvalidMagnet("URI syntax is not valid"))?;
    if !url.scheme().eq_ignore_ascii_case("magnet") || url.has_host() || !url.path().is_empty() {
        return Err(TorrentError::InvalidMagnet(
            "expected a magnet URI without an authority or path",
        ));
    }

    let mut v1_topics = 0usize;
    let mut has_v2 = false;
    let mut normalize_base32 = false;
    for (key, topic) in url.query_pairs() {
        match key.as_ref() {
            "xt" => {
                if let Some(hash) = topic.strip_prefix("urn:btih:") {
                    if !valid_btih(hash.as_bytes()) {
                        return Err(TorrentError::InvalidMagnet(
                            "btih must be 40 hexadecimal or 32 base32 characters",
                        ));
                    }
                    normalize_base32 |=
                        hash.len() == 32 && hash.as_bytes().iter().any(u8::is_ascii_lowercase);
                    v1_topics = v1_topics.saturating_add(1);
                } else if let Some(hash) = topic.strip_prefix("urn:btmh:") {
                    if !valid_btmh(hash.as_bytes()) {
                        return Err(TorrentError::InvalidMagnet(
                            "btmh must contain the 1220 multihash prefix and a 32-byte hexadecimal digest",
                        ));
                    }
                    has_v2 = true;
                } else {
                    return Err(TorrentError::InvalidMagnet(
                        "only v1 btih and v2 btmh exact topics are supported",
                    ));
                }
            }
            // rqbit 8.1.1 eagerly expands every selected range into a Vec
            // while parsing the URI. The dormant adapter does not expose
            // selective-file admission, so reject the parameter before an
            // attacker-controlled range can allocate without a useful bound.
            "so" => {
                return Err(TorrentError::InvalidMagnet(
                    "select-only parameters are not supported",
                ));
            }
            _ => {}
        }
    }

    if v1_topics > 0 && has_v2 {
        return Err(TorrentError::UnsupportedHybridMagnet);
    }
    if v1_topics == 0 && has_v2 {
        return Err(TorrentError::UnsupportedV2Magnet);
    }
    if v1_topics == 0 {
        return Err(TorrentError::InvalidMagnet(
            "a v1 xt=urn:btih topic is required",
        ));
    }
    if v1_topics > 1 {
        return Err(TorrentError::InvalidMagnet(
            "multiple v1 exact-topic values are ambiguous",
        ));
    }

    let mut trackers = HashSet::new();
    for (key, tracker) in url.query_pairs() {
        if key == "tr" {
            if validate_tracker_url(tracker.as_bytes())? && proxy_enabled {
                return Err(TorrentError::ProxyWithUdpTracker);
            }
            if !tracker.is_empty()
                && trackers.insert(tracker.into_owned())
                && trackers.len() > MAX_TRACKERS_PER_TORRENT
            {
                return Err(TorrentError::TooManyTrackers {
                    count: trackers.len(),
                    limit: MAX_TRACKERS_PER_TORRENT,
                });
            }
        }
    }
    if !normalize_base32 {
        return Ok(magnet.to_owned());
    }

    let normalized_pairs = url
        .query_pairs()
        .map(|(key, value)| {
            let value = if key == "xt" {
                value
                    .strip_prefix("urn:btih:")
                    .filter(|hash| hash.len() == 32)
                    .map(|hash| format!("urn:btih:{}", hash.to_ascii_uppercase()))
                    .unwrap_or_else(|| value.into_owned())
            } else {
                value.into_owned()
            };
            (key.into_owned(), value)
        })
        .collect::<Vec<_>>();
    let mut normalized = url;
    normalized
        .query_pairs_mut()
        .clear()
        .extend_pairs(normalized_pairs);
    Ok(normalized.into())
}

fn validate_magnet_discovery_available(
    magnet: &str,
    dht_enabled: bool,
    has_initial_peers: bool,
) -> Result<(), TorrentError> {
    if dht_enabled || has_initial_peers || magnet_has_supported_tracker(magnet) {
        return Ok(());
    }
    Err(TorrentError::MagnetDiscoveryUnavailable)
}

fn magnet_has_supported_tracker(magnet: &str) -> bool {
    // This runs only after `validate_magnet_contract`, which has already
    // rejected malformed and unsupported tracker URLs. Count exactly the
    // non-empty `tr` values that rqbit's parser hands to tracker discovery;
    // ignored parameters such as `x.pe` are deliberately not peer sources.
    url::Url::parse(magnet).is_ok_and(|url| {
        url.query_pairs()
            .any(|(key, tracker)| key == "tr" && !tracker.is_empty())
    })
}

fn valid_btih(hash: &[u8]) -> bool {
    (hash.len() == 40 && hash.iter().all(u8::is_ascii_hexdigit))
        || (hash.len() == 32
            && hash
                .iter()
                .all(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'2'..=b'7')))
}

fn valid_btmh(hash: &[u8]) -> bool {
    hash.len() == 68 && hash.starts_with(b"1220") && hash[4..].iter().all(u8::is_ascii_hexdigit)
}

fn validate_tracker_url(tracker: &[u8]) -> Result<bool, TorrentError> {
    if tracker.is_empty() {
        return Ok(false);
    }
    if tracker.len() > MAX_TRACKER_URL_BYTES {
        return Err(TorrentError::TrackerUrlTooLong {
            size: tracker.len(),
            limit: MAX_TRACKER_URL_BYTES,
        });
    }
    let tracker = std::str::from_utf8(tracker)
        .map_err(|_| TorrentError::InvalidTracker("tracker URLs must contain valid UTF-8"))?;
    let tracker = url::Url::parse(tracker)
        .map_err(|_| TorrentError::InvalidTracker("tracker URL syntax is not valid"))?;
    if tracker.host_str().is_none() {
        return Err(TorrentError::InvalidTracker(
            "tracker URLs must include a host",
        ));
    }
    if tracker.port() == Some(0) {
        return Err(TorrentError::InvalidTracker(
            "tracker URLs must not use port zero",
        ));
    }
    match tracker.scheme() {
        "http" | "https" => Ok(false),
        "udp" if tracker.port().is_some() => Ok(true),
        "udp" => Err(TorrentError::InvalidTracker(
            "UDP tracker URLs must include an explicit port",
        )),
        _ => Err(TorrentError::InvalidTracker(
            "only HTTP, HTTPS, and UDP trackers are supported",
        )),
    }
}

#[derive(Clone)]
pub struct TorrentHandle {
    inner: Arc<ManagedTorrent>,
}

impl TorrentHandle {
    pub fn id(&self) -> usize {
        self.inner.id()
    }

    pub fn name(&self) -> Option<String> {
        self.inner.name()
    }

    pub fn info_hash(&self) -> String {
        self.inner.info_hash().as_string()
    }

    pub fn is_paused(&self) -> bool {
        self.inner.is_paused()
    }

    pub fn stats(&self) -> TorrentStats {
        let stats = self.inner.stats();
        let content_files = self
            .inner
            .with_metadata(|metadata| {
                project_content_files(&metadata.file_infos, &stats.file_progress)
            })
            .unwrap_or_default();
        project_stats_snapshot(stats, content_files)
    }

    pub async fn wait_until_initialized(&self) -> Result<(), TorrentError> {
        self.inner
            .wait_until_initialized()
            .await
            .map_err(engine_error)
    }

    pub async fn wait_until_completed(&self) -> Result<(), TorrentError> {
        self.inner
            .wait_until_completed()
            .await
            .map_err(engine_error)
    }
}

fn project_stats_snapshot(
    stats: EngineTorrentStats,
    content_files: Vec<TorrentContentFile>,
) -> TorrentStats {
    let phase = match stats.state {
        TorrentStatsState::Initializing => TorrentPhase::Initializing,
        TorrentStatsState::Live => TorrentPhase::Live,
        TorrentStatsState::Paused => TorrentPhase::Paused,
        TorrentStatsState::Error => TorrentPhase::Error,
    };
    let (download_bps, upload_bps, peers) = stats
        .live
        .as_ref()
        .map(|live| {
            let peers = &live.snapshot.peer_stats;
            (
                mib_per_second_to_bps(live.download_speed.mbps),
                mib_per_second_to_bps(live.upload_speed.mbps),
                TorrentPeerStats {
                    queued: peers.queued,
                    connecting: peers.connecting,
                    live: peers.live,
                    seen: peers.seen,
                    dead: peers.dead,
                },
            )
        })
        .unwrap_or_default();
    let eta_seconds = (download_bps > 0 && stats.progress_bytes < stats.total_bytes)
        .then(|| (stats.total_bytes - stats.progress_bytes).div_ceil(download_bps));
    TorrentStats {
        phase,
        progress_bytes: stats.progress_bytes,
        uploaded_bytes: stats.uploaded_bytes,
        total_bytes: stats.total_bytes,
        file_progress_bytes: stats.file_progress,
        content_files,
        download_bps,
        upload_bps,
        eta_seconds,
        peers,
        discovery: TorrentDiscoveryState::Unknown,
        finished: stats.finished,
        error: stats.error.map(|error| display_safe_error(&error)),
    }
}

fn project_content_files(
    file_infos: &[librqbit::file_info::FileInfo],
    file_progress: &[u64],
) -> Vec<TorrentContentFile> {
    file_infos
        .iter()
        .enumerate()
        .filter(|(_, file)| !file.attrs.padding)
        .map(|(index, file)| TorrentContentFile {
            relative_path: file.relative_filename.clone(),
            size_bytes: file.len,
            progress_bytes: file_progress.get(index).copied().unwrap_or(0),
        })
        .collect()
}

fn mib_per_second_to_bps(mib_per_second: f64) -> u64 {
    if !mib_per_second.is_finite() || mib_per_second <= 0.0 {
        return 0;
    }
    (mib_per_second * 1024.0 * 1024.0)
        .round()
        .clamp(0.0, u64::MAX as f64) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentPhase {
    Initializing,
    Live,
    Paused,
    Error,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentPeerStats {
    pub queued: usize,
    pub connecting: usize,
    pub live: usize,
    pub seen: usize,
    pub dead: usize,
}

/// Volatile discovery detail exposed by the selected engine boundary.
///
/// The maintained v8.1.1 patch set does not ship the optional tracker/DHT
/// telemetry patch, so the adapter reports absence explicitly. Peer counts
/// must never be reinterpreted as discovery health.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TorrentDiscoveryState {
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentStats {
    pub phase: TorrentPhase,
    pub progress_bytes: u64,
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
    /// Engine-indexed progress retained for low-level diagnostics.
    pub file_progress_bytes: Vec<u64>,
    /// Importer-safe file inventory. BEP 47 padding entries are omitted.
    pub content_files: Vec<TorrentContentFile>,
    pub download_bps: u64,
    pub upload_bps: u64,
    pub eta_seconds: Option<u64>,
    pub peers: TorrentPeerStats,
    pub discovery: TorrentDiscoveryState,
    pub finished: bool,
    /// Redacted, control-character-free, and bounded to 2 KiB.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentContentFile {
    pub relative_path: PathBuf,
    pub size_bytes: u64,
    pub progress_bytes: u64,
}

#[cfg(test)]
mod preflight_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_adapter_boundary_cannot_import_or_own_job_ids() {
        let manifest = include_str!("../Cargo.toml");
        let source = include_str!("lib.rs");
        assert!(!manifest.contains("nzbd-types"));
        assert!(!manifest.contains(&["nzbd", "_types"].concat()));
        assert!(!source.contains(&["Job", "Id"].concat()));
        assert!(!source.contains(&["nzbd", "_types"].concat()));
    }

    fn file_info(path: &str, len: u64, padding: bool) -> librqbit::file_info::FileInfo {
        let mut file = librqbit::file_info::FileInfo {
            relative_filename: PathBuf::from(path),
            offset_in_torrent: 0,
            piece_range: 0..1,
            attrs: Default::default(),
            len,
        };
        file.attrs.padding = padding;
        file
    }

    #[test]
    fn content_file_projection_hides_padding_and_keeps_engine_progress_alignment() {
        let files = vec![
            file_info("release/video.mkv", 100, false),
            file_info("release/.pad/64", 64, true),
            file_info("release/readme.txt", 10, false),
        ];

        assert_eq!(
            project_content_files(&files, &[75, 64, 3]),
            vec![
                TorrentContentFile {
                    relative_path: PathBuf::from("release/video.mkv"),
                    size_bytes: 100,
                    progress_bytes: 75,
                },
                TorrentContentFile {
                    relative_path: PathBuf::from("release/readme.txt"),
                    size_bytes: 10,
                    progress_bytes: 3,
                },
            ]
        );
    }

    #[test]
    fn stats_discovery_stays_unknown_when_live_peers_are_present() {
        let mut live = librqbit::api::LiveStats::default();
        live.snapshot.peer_stats.live = 7;
        live.snapshot.peer_stats.seen = 11;
        let stats = project_stats_snapshot(
            EngineTorrentStats {
                state: TorrentStatsState::Live,
                file_progress: Vec::new(),
                error: None,
                progress_bytes: 1,
                uploaded_bytes: 0,
                total_bytes: 2,
                finished: false,
                live: Some(live),
            },
            Vec::new(),
        );

        assert_eq!(stats.peers.live, 7);
        assert_eq!(stats.peers.seen, 11);
        assert_eq!(stats.discovery, TorrentDiscoveryState::Unknown);
    }

    #[test]
    fn session_options_are_an_explicit_dormant_boundary() {
        // This pins the helper used by TorrentSession::start. If start stops
        // delegating here, move the assertion to the replacement call path.
        let options = session_options(false, true, None, None, None, None, None, None, None, None);
        assert!(options.disable_dht);
        assert!(options.disable_dht_persistence);
        assert!(options.dht_config.is_none());
        assert!(!options.fastresume);
        assert!(options.persistence.is_none());
        assert!(options.disable_auto_restore);
        let peer_id = options.peer_id.expect("peer ID must identify Runner");
        assert_eq!(peer_id.0[..8], identity::peer_id_prefix());
        assert_eq!(
            options.http_user_agent.as_deref(),
            Some(identity::CLIENT_USER_AGENT)
        );
        let peer_options = options.peer_opts.expect("peer policy must be explicit");
        assert_eq!(peer_options.connect_timeout, Some(PEER_CONNECT_TIMEOUT));
        assert_eq!(
            peer_options.read_write_timeout,
            Some(PEER_READ_WRITE_TIMEOUT)
        );
        assert_eq!(
            peer_options.keep_alive_interval,
            Some(PEER_KEEP_ALIVE_INTERVAL)
        );
        assert_eq!(
            peer_options.max_metadata_size,
            Some(DEFAULT_MAX_METAINFO_BYTES as u32)
        );
        assert_eq!(
            peer_options.client_version,
            Some(identity::CLIENT_HANDSHAKE_VERSION)
        );
        assert!(options.listen_port_range.is_none());
        assert!(!options.enable_upnp_port_forwarding);
        assert!(options.defer_writes_up_to.is_none());
        assert!(options.default_storage_factory.is_none());
        assert!(options.socks_proxy_url.is_none());
        assert!(options.cancellation_token.is_none());
        assert_eq!(
            options.concurrent_init_limit,
            Some(MAX_CONCURRENT_TORRENT_INITIALIZATIONS)
        );
        assert!(options.root_span.is_none());
        assert_eq!(options.ratelimits, Default::default());
        assert_eq!(options.peer_limit, Some(MAX_LIVE_PEERS_PER_TORRENT));
        assert_eq!(options.peer_limit_total, Some(MAX_LIVE_PEERS_TOTAL));
        assert_eq!(options.known_peer_limit, Some(MAX_KNOWN_PEERS_PER_TORRENT));
        assert_eq!(options.known_peer_limit_total, Some(MAX_KNOWN_PEERS_TOTAL));
        assert!(options.blocklist_url.is_none());
        assert!(options.trackers.is_empty());
    }

    #[test]
    fn per_torrent_options_are_an_explicit_admission_boundary() {
        let peer = "127.0.0.1:6881".parse().unwrap();
        let resolving = magnet_resolution_options(vec![peer]);
        assert!(!resolving.paused);
        assert!(!resolving.overwrite);
        assert!(resolving.list_only);
        assert_eq!(resolving.initial_peers, Some(vec![peer]));
        assert_add_options_cannot_redirect_or_expand_scope(&resolving);

        let managed = managed_add_options(TorrentAddConfig {
            paused: true,
            overwrite: true,
            initial_peers: vec![peer],
            ..Default::default()
        });
        assert!(managed.paused);
        assert!(managed.overwrite);
        assert!(!managed.list_only);
        assert_eq!(managed.initial_peers, Some(vec![peer]));
        assert_add_options_cannot_redirect_or_expand_scope(&managed);
    }

    fn assert_add_options_cannot_redirect_or_expand_scope(options: &AddTorrentOptions) {
        assert!(options.only_files_regex.is_none());
        assert!(options.only_files.is_none());
        assert!(options.output_folder.is_none());
        assert!(options.sub_folder.is_none());
        assert!(options.peer_opts.is_none());
        assert!(options.force_tracker_interval.is_none());
        assert!(!options.disable_trackers);
        assert_eq!(options.ratelimits, Default::default());
        assert!(options.peer_limit.is_none());
        assert!(options.known_peer_limit.is_none());
        assert!(options.preferred_id.is_none());
        assert!(options.storage_factory.is_none());
        assert!(options.defer_writes.is_none());
        assert!(options.trackers.is_none());
    }

    #[test]
    fn listen_range_is_exactly_one_explicit_port() {
        assert!(validate_listen_port_range(None).is_ok());
        assert!(validate_listen_port_range(Some(&(6881..6882))).is_ok());

        for range in [0..1, 6881..6881, 6881..6883, u16::MAX..u16::MAX] {
            assert!(matches!(
                validate_listen_port_range(Some(&range)),
                Err(TorrentError::InvalidListenPortRange)
            ));
        }
    }

    #[test]
    fn initial_peers_are_validated_deduplicated_and_bounded() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 6881));
        let mut duplicates = vec![peer; MAX_INITIAL_PEERS + 1];
        normalize_initial_peers(&mut duplicates).unwrap();
        assert_eq!(duplicates, vec![peer]);

        for peer in [
            SocketAddr::from(([127, 0, 0, 1], 0)),
            SocketAddr::from(([0, 0, 0, 0], 6881)),
            SocketAddr::from(([224, 0, 0, 1], 6881)),
            SocketAddr::from(([255, 255, 255, 255], 6881)),
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 6881)),
        ] {
            assert!(matches!(
                normalize_initial_peers(&mut vec![peer]),
                Err(TorrentError::InvalidInitialPeer(_))
            ));
        }

        let mut too_many = (1..=MAX_INITIAL_PEERS + 1)
            .map(|last| SocketAddr::from(([192, 0, 2, last as u8], 6881)))
            .collect::<Vec<_>>();
        assert!(matches!(
            normalize_initial_peers(&mut too_many),
            Err(TorrentError::TooManyInitialPeers {
                count,
                limit: MAX_INITIAL_PEERS
            }) if count == MAX_INITIAL_PEERS + 1
        ));
    }

    #[test]
    fn resolved_peers_keep_explicit_order_and_stop_at_the_limit() {
        let first = SocketAddr::from(([192, 0, 2, 1], 6881));
        let second = SocketAddr::from(([192, 0, 2, 2], 6881));
        let mut peers = vec![first];
        extend_with_resolved_peers(
            &mut peers,
            [first, SocketAddr::from(([192, 0, 2, 3], 0)), second],
        );
        assert_eq!(peers, vec![first, second]);

        let mut full = (1..=MAX_INITIAL_PEERS)
            .map(|last| SocketAddr::from(([198, 51, 100, last as u8], 6881)))
            .collect::<Vec<_>>();
        let overflow = SocketAddr::from(([203, 0, 113, 1], 6881));
        extend_with_resolved_peers(&mut full, [overflow]);
        assert_eq!(full.len(), MAX_INITIAL_PEERS);
        assert!(!full.contains(&overflow));
    }

    #[test]
    fn proxy_credentials_are_separate_and_redacted() {
        let proxy = TorrentProxyConfig {
            url: "socks5://127.0.0.1:1080".into(),
            username: Some("alice".into()),
            password: Some("do-not-log-me".into()),
        };
        assert_eq!(
            proxy.engine_url().unwrap(),
            "socks5://alice:do-not-log-me@127.0.0.1:1080"
        );
        let shown = format!("{proxy:?}");
        assert!(shown.contains("***"));
        assert!(!shown.contains("do-not-log-me"));
    }

    #[test]
    fn proxy_refuses_embedded_or_ambiguous_credentials() {
        let embedded = TorrentProxyConfig {
            url: "socks5://alice:secret@127.0.0.1:1080".into(),
            ..Default::default()
        };
        let shown = format!("{embedded:?}");
        assert!(!shown.contains("alice"));
        assert!(!shown.contains("secret"));
        assert!(matches!(
            embedded.engine_url(),
            Err(TorrentError::InvalidProxy(_))
        ));

        let partial = TorrentProxyConfig {
            url: "socks5://127.0.0.1:1080".into(),
            username: Some("alice".into()),
            password: None,
        };
        assert!(matches!(
            partial.engine_url(),
            Err(TorrentError::InvalidProxy(_))
        ));
    }

    #[test]
    fn proxy_accepts_only_an_origin_url() {
        let trailing_slash = TorrentProxyConfig {
            url: "socks5://127.0.0.1:1080/".into(),
            ..Default::default()
        };
        assert_eq!(
            trailing_slash.engine_url().unwrap(),
            "socks5://127.0.0.1:1080"
        );

        for url in [
            "socks5://127.0.0.1:1080/private",
            "socks5://127.0.0.1:1080/?token=secret",
            "socks5://127.0.0.1:1080/#fragment",
        ] {
            let proxy = TorrentProxyConfig {
                url: url.into(),
                ..Default::default()
            };
            assert!(matches!(
                proxy.engine_url(),
                Err(TorrentError::InvalidProxy(
                    "URL must not contain a path, query, or fragment"
                ))
            ));
        }
    }

    #[test]
    fn engine_errors_are_redacted_and_display_safe() {
        let message = concat!(
            "fetch magnet:?xt=urn:btih:0123456789012345678901234567890123456789&dn=secret-name ",
            "tracker=https://alice:secret@tracker.example/passkey?auth=secret ",
            "proxy=socks5://bob:hunter2@127.0.0.1:1080 ",
            "socks_proxy_password=plain-secret ",
            "password: spaced-secret api_token = equals-secret ",
            "\"authorization\":\"json-secret\" cookie : cookie-secret ",
            "authorization: Bearer bearer-secret visible-context ",
            "proxy_authorization=Basic basic-secret visible-context ",
            "password => arrow-secret visible-context ",
            "secret := walrus-secret token -> pointer-secret visible-context ",
            "authorization: Digest username=digest-user, response=digest-secret visible-context ",
            "cookie: first-cookie=one; second-cookie=two visible-context ",
            "query=tracker.example/private-passkey?auth=query-secret ",
            "peers=[203.0.113.7:6881, /Users/alice/private/file]\nforbidden"
        );
        let shown = engine_error(message).to_string();

        for secret in [
            "secret-name",
            "alice",
            "hunter2",
            "plain-secret",
            "spaced-secret",
            "equals-secret",
            "json-secret",
            "cookie-secret",
            "bearer-secret",
            "basic-secret",
            "arrow-secret",
            "walrus-secret",
            "pointer-secret",
            "digest-user",
            "digest-secret",
            "first-cookie",
            "second-cookie",
            "private-passkey",
            "query-secret",
            "203.0.113.7",
            "/Users/alice",
        ] {
            assert!(!shown.contains(secret), "leaked {secret:?}: {shown}");
        }
        assert!(shown.contains("tracker.example"));
        assert!(shown.contains("visible-context"));
        assert!(!shown.contains('\n'));
    }

    #[test]
    fn engine_errors_redact_embedded_and_tracker_specific_credentials() {
        let message = concat!(
            "user=public&password=hunter2 ",
            "{\"user\":\"public\",\"password\":\"p@ss\"} ",
            "password=before-url,https://tracker.example/announce ",
            "torrent_pass=gazelle authkey=tracker-auth apikey=api-one ",
            "api_key=api-two sig=signed signature=signature-value ",
            "sessionid=session-value"
        );
        let shown = engine_error(message).to_string();

        for secret in [
            "hunter2",
            "p@ss",
            "before-url",
            "gazelle",
            "tracker-auth",
            "api-one",
            "api-two",
            "signed",
            "signature-value",
            "session-value",
        ] {
            assert!(!shown.contains(secret), "leaked {secret:?}: {shown}");
        }
        assert!(shown.contains("user=public"));
        assert!(shown.contains("tracker.example"));
    }

    #[test]
    fn display_safe_errors_are_utf8_safe_bounded_and_marked() {
        let exact = display_safe_error(&"x".repeat(DISPLAY_SAFE_ERROR_MAX_BYTES));
        assert_eq!(exact.len(), DISPLAY_SAFE_ERROR_MAX_BYTES);
        assert!(!exact.ends_with(ERROR_TRUNCATION_MARKER));

        let first_excess = display_safe_error(&format!(
            "{}é",
            "x".repeat(DISPLAY_SAFE_ERROR_MAX_BYTES - 1)
        ));
        assert_eq!(first_excess.len(), DISPLAY_SAFE_ERROR_MAX_BYTES);
        assert!(first_excess.ends_with(ERROR_TRUNCATION_MARKER));

        let unicode = display_safe_error(&"é".repeat(DISPLAY_SAFE_ERROR_MAX_BYTES));
        assert!(unicode.len() <= DISPLAY_SAFE_ERROR_MAX_BYTES);
        assert!(unicode.ends_with(ERROR_TRUNCATION_MARKER));
        assert!(std::str::from_utf8(unicode.as_bytes()).is_ok());

        let huge_url = display_safe_error(&format!(
            "https://tracker.example/{}?passkey=secret",
            "x".repeat(DISPLAY_SAFE_ERROR_MAX_BYTES * 4)
        ));
        assert_eq!(huge_url, "<redacted-url>");
    }
}
