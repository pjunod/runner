//! Process-lifetime control plane. No queue, history or inventory handle is
//! required to serve it. Blocking bootstrap work is accounted for separately.
use crate::{anyhow_lite, tls};
use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const PROBE_BUDGET: Duration = Duration::from_secs(2);
const LOSS_BUDGET: Duration = Duration::from_secs(30);

pub fn monitor_bind(cli: Option<&str>) -> anyhow_lite::Result<SocketAddr> {
    let value = match cli {
        Some(value) => value.to_owned(),
        None => match std::env::var("NZBD_MONITOR_BIND") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "127.0.0.1:6790".into(),
            Err(error) => {
                return Err(anyhow_lite::Error::msg(format!(
                    "invalid NZBD_MONITOR_BIND: {error}"
                )))
            }
        },
    };
    let addr: SocketAddr = value.parse().map_err(|_| {
        anyhow_lite::Error::msg("monitor bind must be a numeric loopback IP address and port")
    })?;
    if !addr.ip().is_loopback() {
        return Err(anyhow_lite::Error::msg("monitor bind must be loopback"));
    }
    Ok(addr)
}

/// Validate text before URL normalization can turn shorthand numeric hosts,
/// userinfo, escaped paths, or a hostname into a different destination.
fn probe_url(raw: &str, ready: bool) -> anyhow_lite::Result<String> {
    let (scheme, rest) = raw
        .split_once("://")
        .ok_or_else(|| anyhow_lite::Error::msg("health URL requires http:// or https://"))?;
    if !matches!(scheme, "http" | "https") {
        return Err(anyhow_lite::Error::msg("health URL must use HTTP or HTTPS"));
    }
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.contains(['@', '?', '#']) || !matches!(path, "" | "healthz") || ready {
        return Err(anyhow_lite::Error::msg("health --url accepts only a literal-loopback authority and exact /healthz route; --ready requires the monitor"));
    }
    let ip = if authority.starts_with('[') {
        let end = authority
            .find(']')
            .ok_or_else(|| anyhow_lite::Error::msg("invalid loopback IPv6 URL"))?;
        let tail = &authority[end + 1..];
        if !tail.is_empty() && (!tail.starts_with(':') || tail[1..].parse::<u16>().is_err()) {
            return Err(anyhow_lite::Error::msg("invalid health URL port"));
        }
        authority[1..end].parse::<std::net::IpAddr>()
    } else {
        let host = if let Some((host, port)) = authority.split_once(':') {
            port.parse::<u16>()
                .map_err(|_| anyhow_lite::Error::msg("invalid health URL port"))?;
            host
        } else {
            authority
        };
        host.parse::<std::net::IpAddr>()
    }
    .map_err(|_| {
        anyhow_lite::Error::msg("health URL requires a numeric loopback IP; DNS is disabled")
    })?;
    if !ip.is_loopback() {
        return Err(anyhow_lite::Error::msg("health URL must be loopback"));
    }
    Ok(format!("{scheme}://{authority}/healthz"))
}

fn probe_client() -> anyhow_lite::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        // Only used for the nonsecret probe, after literal IP validation.
        .danger_accept_invalid_certs(true)
        .timeout(PROBE_BUDGET)
        .build()?)
}

pub fn health(ready: bool, bind: Option<&str>, url: Option<&str>) -> anyhow_lite::Result<()> {
    let target = match url {
        Some(url) => probe_url(url, ready)?,
        None => format!(
            "http://{}/{}",
            monitor_bind(bind)?,
            if ready { "readyz" } else { "healthz" }
        ),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(PROBE_BUDGET, async {
            let response = probe_client()?.get(target).send().await?;
            if response.status() != StatusCode::OK {
                return Err(anyhow_lite::Error::msg(format!(
                    "health probe returned {}",
                    response.status()
                )));
            }
            // Bound response receipt, not merely connection/header arrival.
            response.bytes().await?;
            Ok(())
        })
        .await
        .map_err(|_| anyhow_lite::Error::msg("health probe exceeded its 2-second deadline"))?
    })
}

struct OwnerObservation {
    engine: nzbd_engine::EngineHandle,
    last_progress_unix: u64,
    last_changed: Instant,
    state: &'static str,
}

impl OwnerObservation {
    fn observe(&mut self, progress_unix: u64, now: Instant, stall_after: Duration) {
        if progress_unix != self.last_progress_unix {
            self.last_progress_unix = progress_unix;
            self.last_changed = now;
        }
        self.state = if now.saturating_duration_since(self.last_changed) >= stall_after {
            "stalled"
        } else if progress_unix == 0 {
            "starting"
        } else {
            "responsive"
        };
    }
}

struct Status {
    configuration: &'static str,
    trusted_configuration: Option<nzbd_config::Config>,
    backend: &'static str,
    identity: &'static str,
    owner: Option<OwnerObservation>,
    reason: Option<String>,
    backend_router: Option<Router>,
    loading_router: Router,
    public_url: Option<String>,
    public_probe: Option<reqwest::Client>,
    public_ready: bool,
    ever_ready: bool,
    last_success: Instant,
    signature: Option<String>,
    listener_cancel: CancellationToken,
}
struct Shared {
    status: Mutex<Status>,
    shutdown: CancellationToken,
    live: AtomicBool,
    log: Arc<nzbd_api::LogBuffer>,
    owner_stall_after: Duration,
}

pub struct Controller {
    shared: Arc<Shared>,
    runtime: Option<tokio::runtime::Runtime>,
    monitor_runtime: Option<tokio::runtime::Runtime>,
}

impl Controller {
    /// Binding occurs synchronously and precedes every config/storage read.
    pub fn start(bind: SocketAddr, log: Arc<nzbd_api::LogBuffer>) -> anyhow_lite::Result<Self> {
        let listener = std::net::TcpListener::bind(bind).map_err(|e| {
            anyhow_lite::Error::msg(format!("cannot bind bootstrap monitor {bind}: {e}"))
        })?;
        listener.set_nonblocking(true)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        // Public handlers may depend on backend runtimes. Keep bootstrap
        // liveness, deadlines and listener watchdog on an independent executor.
        let monitor_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let shared = Arc::new(Shared {
            status: Mutex::new(Status {
                configuration: "loading",
                trusted_configuration: None,
                backend: "loading",
                identity: "unknown",
                owner: None,
                reason: None,
                backend_router: None,
                loading_router: unavailable_router(),
                public_url: None,
                public_probe: None,
                public_ready: false,
                ever_ready: false,
                last_success: Instant::now(),
                signature: None,
                listener_cancel: CancellationToken::new(),
            }),
            shutdown: CancellationToken::new(),
            live: AtomicBool::new(true),
            log,
            owner_stall_after: match std::env::var("NZBD_OWNER_STALL_SECS") {
                Err(std::env::VarError::NotPresent) => Duration::from_secs(30),
                value => {
                    let seconds = value
                        .map_err(|error| {
                            anyhow_lite::Error::msg(format!(
                                "invalid NZBD_OWNER_STALL_SECS: {error}"
                            ))
                        })?
                        .parse::<u64>()
                        .map_err(|_| {
                            anyhow_lite::Error::msg(
                                "NZBD_OWNER_STALL_SECS must be positive integer seconds",
                            )
                        })?;
                    if seconds == 0 {
                        return Err(anyhow_lite::Error::msg(
                            "NZBD_OWNER_STALL_SECS must be positive",
                        ));
                    }
                    Duration::from_secs(seconds)
                }
            },
        });
        let task = shared.clone();
        let _guard = monitor_runtime.enter();
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let app = Router::new()
            .route("/healthz", get(monitor_health))
            .route("/readyz", get(monitor_ready))
            .with_state(shared.clone());
        monitor_runtime.spawn(async move {
            if let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(task.shutdown.clone().cancelled_owned())
                .await
            {
                tracing::error!(%error, "bootstrap monitor failed");
                std::process::exit(1);
            }
        });
        let task = shared.clone();
        monitor_runtime.spawn(async move {
            crate::shutdown_signal().await;
            task.shutdown.cancel();
        });
        monitor_runtime.spawn(watch_public(shared.clone()));
        monitor_runtime.spawn(observe_owner(shared.clone()));
        let task = shared.clone();
        monitor_runtime.spawn(async move {
            task.shutdown.cancelled().await;
            tokio::time::sleep(Duration::from_secs(20)).await;
            tracing::error!("application drain exceeded 20 seconds; exiting with durable recovery evidence retained");
            std::process::exit(1);
        });
        drop(_guard);
        tracing::info!(%bind, "bootstrap monitor listening");
        Ok(Self {
            shared,
            runtime: Some(runtime),
            monitor_runtime: Some(monitor_runtime),
        })
    }

    pub fn reason(&self, component: &'static str, reason: impl Into<String>) {
        let mut state = self.shared.status.lock().unwrap();
        if component == "configuration" {
            state.configuration = "loading";
        }
        state.reason = Some(reason.into());
    }
    pub fn trusted_configuration(&self) -> Option<nzbd_config::Config> {
        self.shared
            .status
            .lock()
            .unwrap()
            .trusted_configuration
            .clone()
    }
    pub fn security_known(&self) -> bool {
        {
            let status = self.shared.status.lock().unwrap();
            status.signature.is_some() || status.ever_ready
        }
    }
    pub fn is_stopping(&self) -> bool {
        self.shared.shutdown.is_cancelled()
    }
    pub async fn stopped(&self) {
        self.shared.shutdown.cancelled().await;
    }
    pub fn identity(&self, enrolled: bool, reason: Option<String>) {
        let mut state = self.shared.status.lock().unwrap();
        state.identity = if enrolled { "enrolled" } else { "unenrolled" };
        if reason.is_some() {
            state.reason = reason;
        }
    }
    pub fn loading(&self) {
        let mut state = self.shared.status.lock().unwrap();
        state.backend = "loading";
        state.backend_router = None;
        state.owner = None;
    }
    pub fn install(&self, router: Router, engine: nzbd_engine::EngineHandle) {
        let mut state = self.shared.status.lock().unwrap();
        state.backend_router = Some(router);
        state.backend = "ready";
        let mut observation = OwnerObservation {
            engine,
            last_progress_unix: 0,
            last_changed: Instant::now(),
            state: "starting",
        };
        observation.observe(
            observation.engine.owner_last_progress_unix(),
            Instant::now(),
            self.shared.owner_stall_after,
        );
        state.owner = Some(observation);
        if state.identity != "unenrolled" {
            state.reason = None;
        }
    }

    pub fn public(
        &self,
        cfg: &nzbd_config::Config,
        bind: &str,
        tls: Option<tls::TlsSetup>,
    ) -> anyhow_lite::Result<()> {
        let auth = nzbd_api::AuthConfig {
            username: cfg.api.username.clone(),
            password: cfg.api.password.clone(),
            token: cfg.api.token.clone(),
        };
        // Compare only in memory; never put secrets in logs or diagnostics.
        let signature = format!(
            "{bind}|{:?}|{:?}|{}",
            auth,
            cfg.api,
            tls.as_ref().map(|t| t.note.as_str()).unwrap_or("http")
        );
        {
            let mut state = self.shared.status.lock().unwrap();
            state.configuration = "ready";
            state.trusted_configuration = Some(cfg.clone());
            if state.signature.as_ref() == Some(&signature) {
                return Ok(());
            }
            state.public_ready = false;
            state.listener_cancel.cancel();
            state.signature = None;
        }
        // The old accept loop drops the socket before acknowledging cancellation.
        // Same-bind security changes use a short, bounded rebind retry.
        let start = Instant::now();
        let listener = loop {
            match std::net::TcpListener::bind(bind) {
                Ok(listener) => break listener,
                Err(error)
                    if error.kind() == std::io::ErrorKind::AddrInUse
                        && start.elapsed() < Duration::from_secs(1) =>
                {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => {
                    self.reason(
                        "public_listener",
                        format!("public listener unavailable: {error}"),
                    );
                    return Err(error.into());
                }
            }
        };
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let probe_address = SocketAddr::new(
            if address.ip().is_unspecified() {
                if address.is_ipv4() {
                    std::net::Ipv4Addr::LOCALHOST.into()
                } else {
                    std::net::Ipv6Addr::LOCALHOST.into()
                }
            } else {
                address.ip()
            },
            address.port(),
        );
        let cancel = CancellationToken::new();
        let public_probe = pinned_probe_client(tls.as_ref())?;
        let app = public_router(self.shared.clone(), auth.clone());
        let tls_config = tls.as_ref().map(|tls| tls.config.clone());
        {
            let mut state = self.shared.status.lock().unwrap();
            state.listener_cancel = cancel.clone();
            state.public_url = Some(format!(
                "{}://{probe_address}/healthz",
                if tls.is_some() { "https" } else { "http" }
            ));
            state.loading_router = nzbd_api::require_auth(unavailable_router(), auth);
            state.public_probe = Some(public_probe);
            state.signature = Some(signature);
        }
        let runtime = self.runtime.as_ref().unwrap();
        let _guard = runtime.enter();
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let shared = self.shared.clone();
        let advertiser = crate::discovery::Advertiser::start(
            &cfg.api,
            address,
            cfg.cluster
                .enabled
                .then_some(cfg.cluster.node_name.as_str()),
            tls.is_some(),
        );
        runtime.spawn(async move {
            let _advertiser = advertiser;
            let result = serve_public(
                listener,
                tls_config,
                app,
                cancel.clone(),
                shared.shutdown.clone(),
            )
            .await;
            if !cancel.is_cancelled() && !shared.shutdown.is_cancelled() {
                let mut state = shared.status.lock().unwrap();
                state.public_ready = false;
                state.reason = Some(format!("public listener stopped: {result:?}"));
            }
        });
        tracing::info!(%bind, tls = tls.is_some(), "nzbd control listener started; backend loading");
        Ok(())
    }
}
impl Drop for Controller {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(1));
        }
        if let Some(runtime) = self.monitor_runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

async fn monitor_health(State(shared): State<Arc<Shared>>) -> Response {
    (
        if shared.live.load(Ordering::Relaxed) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        "ok",
    )
        .into_response()
}
async fn monitor_ready(State(shared): State<Arc<Shared>>) -> Response {
    let state = shared.status.lock().unwrap();
    (if state.public_ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE },
     Json(json!({"state": if state.public_ready {"ready"} else {"loading"}, "components": {
         "configuration": state.configuration, "public_listener": if state.public_ready {"ready"} else {"unavailable"}, "backend": state.backend }}))).into_response()
}
async fn service(State(shared): State<Arc<Shared>>) -> Response {
    let state = shared.status.lock().unwrap();
    let owner_state = state
        .owner
        .as_ref()
        .map(|owner| owner.state)
        .unwrap_or("starting");
    let owner = state.owner.as_ref().map(|owner| json!({
        "state": owner.state,
        "last_progress_unix": owner.last_progress_unix,
        "seconds_since_progress": owner.last_changed.elapsed().as_secs(),
        "stall_after_seconds": shared.owner_stall_after.as_secs(),
        "reason": if owner.state == "stalled" { Some("owner progress has not changed within the observation deadline") } else { None },
    })).unwrap_or_else(|| json!({"state": "starting"}));
    ([(header::CACHE_CONTROL, "no-store")], Json(json!({
        "state": if state.backend == "ready" {"ready"} else {"loading"},
        "components": {"configuration": state.configuration, "public_listener": if state.public_ready {"ready"} else {"starting"},
            "backend": state.backend, "identity": state.identity, "owner": owner_state},
        "owner": owner, "reason": state.reason,
    }))).into_response()
}

async fn dispatch(State(shared): State<Arc<Shared>>, request: Request) -> Response {
    let router = {
        let state = shared.status.lock().unwrap();
        state
            .backend_router
            .as_ref()
            .unwrap_or(&state.loading_router)
            .clone()
    };
    router.oneshot(request).await.unwrap()
}
fn unavailable_router() -> Router {
    Router::new().fallback(|| async {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                (header::RETRY_AFTER, "1"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            Json(json!({"error":"backend_loading", "component":"backend", "retryable":true})),
        )
    })
}

fn public_router(shared: Arc<Shared>, auth: nzbd_api::AuthConfig) -> Router {
    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/v1/service", get(service))
        .route(
            "/api/v1/service/shutdown",
            post(|State(shared): State<Arc<Shared>>| async move {
                shared.shutdown.cancel();
                StatusCode::ACCEPTED
            }),
        )
        .route(
            "/api/v1/logs",
            get(
                |State(shared): State<Arc<Shared>>, request: Request| async move {
                    if shared.status.lock().unwrap().backend_router.is_some() {
                        return dispatch(State(shared), request).await;
                    }
                    (
                        [(header::CACHE_CONTROL, "no-store")],
                        Json(json!({"entries": shared.log.tail(200)})),
                    )
                        .into_response()
                },
            ),
        )
        .route(
            "/",
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                        (header::CACHE_CONTROL, "no-cache"),
                    ],
                    include_str!("../../nzbd-api/ui/index.html"),
                )
            }),
        )
        .route(
            "/manifest.webmanifest",
            get(|| async {
                embedded(
                    "application/manifest+json",
                    include_bytes!("../../nzbd-api/ui/manifest.webmanifest"),
                )
            }),
        )
        .route(
            "/sw.js",
            get(|| async {
                embedded("text/javascript", include_bytes!("../../nzbd-api/ui/sw.js"))
            }),
        )
        .route(
            "/icons/icon-192.png",
            get(|| async {
                embedded(
                    "image/png",
                    include_bytes!("../../nzbd-api/ui/icons/icon-192.png"),
                )
            }),
        )
        .route(
            "/icons/icon-512.png",
            get(|| async {
                embedded(
                    "image/png",
                    include_bytes!("../../nzbd-api/ui/icons/icon-512.png"),
                )
            }),
        )
        .route(
            "/icons/icon-maskable-512.png",
            get(|| async {
                embedded(
                    "image/png",
                    include_bytes!("../../nzbd-api/ui/icons/icon-maskable-512.png"),
                )
            }),
        )
        .route(
            "/apple-touch-icon.png",
            get(|| async {
                embedded(
                    "image/png",
                    include_bytes!("../../nzbd-api/ui/icons/apple-touch-icon.png"),
                )
            }),
        )
        .with_state(shared.clone());
    Router::new()
        .merge(nzbd_api::require_auth(router, auth))
        .fallback(move |request: Request| dispatch(State(shared.clone()), request))
}

async fn serve_public(
    listener: tokio::net::TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
    app: Router,
    cancel: CancellationToken,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = shutdown.cancelled() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let app = app.clone().layer(axum::Extension(axum::extract::ConnectInfo(peer)));
                let acceptor = acceptor.clone();
                connections.spawn(async move {
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    let builder = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
                    if let Some(acceptor) = acceptor {
                        if let Ok(Ok(stream)) = tokio::time::timeout(PROBE_BUDGET, acceptor.accept(stream)).await {
                            let _ = builder.serve_connection_with_upgrades(hyper_util::rt::TokioIo::new(stream), service).await;
                        }
                    } else { let _ = builder.serve_connection_with_upgrades(hyper_util::rt::TokioIo::new(stream), service).await; }
                });
            }
        }
    }
    // Security reconfiguration must fence keep-alive requests on old transport.
    connections.abort_all();
    Ok(())
}
async fn watch_public(shared: Arc<Shared>) {
    loop {
        tokio::select! { _ = shared.shutdown.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
        let (url, client, generation) = {
            let state = shared.status.lock().unwrap();
            (
                state.public_url.clone(),
                state.public_probe.clone(),
                state.signature.clone(),
            )
        };
        let ok = if let (Some(url), Some(client), Some(_)) = (url, client, generation.as_ref()) {
            tokio::time::timeout(PROBE_BUDGET, async {
                let response = client.get(url).send().await.ok()?;
                if response.status() != StatusCode::OK {
                    return None;
                }
                let bytes = response.bytes().await.ok()?;
                (bytes.as_ref() == b"ok").then_some(())
            })
            .await
            .ok()
            .flatten()
            .is_some()
        } else {
            false
        };
        let mut state = shared.status.lock().unwrap();
        if state.signature != generation {
            continue;
        }
        state.public_ready = ok;
        if ok {
            state.ever_ready = true;
            state.last_success = Instant::now();
        }
        if state.ever_ready && state.last_success.elapsed() >= LOSS_BUDGET {
            shared.live.store(false, Ordering::Relaxed);
            tracing::error!(reason = ?state.reason, "public control service lost for 30 seconds; exiting");
            // Make the failed liveness transition observable while retaining
            // a bounded nonzero exit; no backend can be restarted in this state.
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(250)).await;
                std::process::exit(1);
            });
            return;
        }
    }
}

fn embedded(content_type: &'static str, bytes: &'static [u8]) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        bytes,
    )
        .into_response()
}

// The controller knows the configured server certificate. Pin that identity
// rather than disabling verification for a nonloopback public bind address.
#[derive(Debug)]
struct PinnedCertificate {
    certificate: Vec<u8>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}
impl rustls::client::danger::ServerCertVerifier for PinnedCertificate {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() != self.certificate.as_slice() {
            return Err(rustls::Error::General(
                "public control certificate differs from configured identity".into(),
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
fn pinned_probe_client(tls: Option<&tls::TlsSetup>) -> anyhow_lite::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(PROBE_BUDGET);
    if let Some(tls) = tls {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedCertificate {
                certificate: tls.certificate.clone(),
                provider,
            }))
            .with_no_client_auth();
        builder = builder.use_preconfigured_tls(config);
    }
    Ok(builder.build()?)
}

/// Observe progress with a monotonic clock; wall-clock timestamps are opaque
/// change counters here. A stalled owner never withdraws working control UI
/// readiness and never starts a second process-restart policy.
async fn observe_owner(shared: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(250)) => {},
        }
        let mut state = shared.status.lock().unwrap();
        if let Some(owner) = state.owner.as_mut() {
            let before = owner.state;
            owner.observe(
                owner.engine.owner_last_progress_unix(),
                Instant::now(),
                shared.owner_stall_after,
            );
            if owner.state != before {
                if owner.state == "stalled" {
                    tracing::warn!(
                        "queue owner progress stalled; secure control service remains available"
                    );
                } else {
                    tracing::info!(state = owner.state, "queue owner progress state changed");
                }
            }
        }
    }
}
