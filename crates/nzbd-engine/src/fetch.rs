//! Minimal HTTPS/HTTP fetcher for URL jobs (`AddUrl`): hyper HTTP/1.1 over
//! the same rustls stack the NNTP transport uses. Follows up to 5
//! redirects, caps bodies at 64 MiB (an NZB, not a payload), 60 s timeout
//! per hop. Rate limits, service-unavailable responses and transient network
//! failures retry the same durable URL job until success or a permanent
//! response, backing off for 1, 5, 10, then 30 minutes indefinitely. An
//! operator "retry now" (see [`http_get_observed`]'s `kick`) cuts any wait
//! short.
//!
//! A provider's `Retry-After` of up to 30 minutes is a floor on the step. A
//! longer one is an indexer quota answer (an exhausted daily allowance comes
//! back as 429 with a 12–22 hour Retry-After): no retry inside the ladder can
//! succeed, so the fetch ends at once as `fetch{indexer_unavailable}`
//! (contract §2.2), naming the wait. Curator then re-queues the want after the
//! outage instead of the job sitting in "Fetching NZB" for a day; obeying the
//! wait parked jobs for a day, and ignoring it kept retrying a locked account
//! every 30 minutes forever (job 4198, 2026-11-25).

use http_body_util::BodyExt;
use hyper::Request;
use hyper_util::rt::TokioIo;
use nzbd_types::CertLevel;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio::sync::Notify;

const MAX_REDIRECTS: usize = 5;
const MAX_BODY: usize = 64 * 1024 * 1024;
const HOP_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const RETRY_BACKOFF_SECONDS: [u64; 4] = [60, 5 * 60, 10 * 60, 30 * 60];
pub(crate) const RETRY_ATTEMPT_PARAM: &str = "*URL:retry-attempt";
pub(crate) const NEXT_RETRY_PARAM: &str = "*URL:next-retry-at";
pub(crate) const BLOCKED_SINCE_PARAM: &str = "*URL:blocked-since";
pub(crate) const LAST_ERROR_PARAM: &str = "*URL:last-error";

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("bad url: {0}")]
    BadUrl(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(String),
    #[error("transient network error: {0}")]
    TransientNetwork(String),
    #[error("http: status {status}")]
    HttpStatus {
        status: hyper::StatusCode,
        retry_after: Option<Duration>,
    },
    /// A 429/503 whose Retry-After lies beyond the ladder: an exhausted
    /// indexer quota. Terminal, never retried inside the job.
    #[error("http: status {status}; the indexer asked to wait {}", human_wait_of(.retry_after))]
    IndexerRefused {
        status: hyper::StatusCode,
        retry_after: Duration,
    },
    #[error("redirect loop / too many redirects")]
    TooManyRedirects,
    #[error("timed out")]
    Timeout,
    #[error("URL fetch stopped because its queue job is no longer fetching")]
    Cancelled,
}

/// The operator's "retry now" for one fetch. A kick names the deadline it
/// saw published; it lands only while the worker is waiting on exactly that
/// deadline, so a double click, or a kick racing a wait that already ended,
/// can never pre-arm the next wait to be skipped.
#[derive(Debug, Default)]
pub struct FetchKick {
    /// The deadline (unix seconds) the worker is waiting on; 0 when not.
    waiting_on: std::sync::Mutex<i64>,
    notify: Notify,
}

impl FetchKick {
    fn lock(&self) -> std::sync::MutexGuard<'_, i64> {
        self.waiting_on
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Set before the deadline is published, so a kick that sees it matches.
    fn arm(&self, deadline: Option<i64>) {
        *self.lock() = deadline.unwrap_or(0);
    }

    /// Cleared, with any unconsumed permit, the moment a wait ends.
    fn disarm(&self) {
        let mut waiting_on = self.lock();
        *waiting_on = 0;
        let stale = self.notify.notified();
        tokio::pin!(stale);
        stale.as_mut().enable();
    }

    /// Ends the wait on `deadline` if the worker is still in it. The permit
    /// covers a kick that lands after publication but before the sleep
    /// starts.
    pub fn kick(&self, deadline: i64) -> bool {
        let waiting_on = self.lock();
        let landed = *waiting_on != 0 && *waiting_on == deadline;
        if landed {
            self.notify.notify_one();
        }
        landed
    }
}

#[derive(Debug, Clone, Default)]
pub struct FetchRetryState {
    pub attempts: u32,
    pub next_retry_at: Option<i64>,
    pub blocked_since: Option<i64>,
    pub last_error: Option<String>,
}

struct Url {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

fn parse_url(url: &str) -> Result<Url, FetchError> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(FetchError::BadUrl(url.into()));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(FetchError::BadUrl(url.into()));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !h.is_empty() => (
            h.to_string(),
            p.parse().map_err(|_| FetchError::BadUrl(url.into()))?,
        ),
        _ => (authority.to_string(), if https { 443 } else { 80 }),
    };
    Ok(Url {
        https,
        host,
        port,
        path: path.to_string(),
    })
}

/// GET a URL and return the body bytes.
pub async fn http_get(url: &str) -> Result<Vec<u8>, FetchError> {
    http_get_observed(
        url,
        FetchRetryState::default(),
        |_| async { true },
        Arc::default(),
    )
    .await
}

/// GET a URL once: redirects are followed, but nothing is retried and each
/// hop has the usual timeout. For callers that would rather fall back than
/// wait out the job retry schedule (the release probe).
pub async fn http_get_once(url: &str) -> Result<Vec<u8>, FetchError> {
    let mut current = url.to_string();
    for _ in 0..=MAX_REDIRECTS {
        match tokio::time::timeout(HOP_TIMEOUT, get_once(&current)).await {
            Err(_) => return Err(FetchError::Timeout),
            Ok(Ok(Hop::Body(bytes))) => return Ok(bytes),
            Ok(Ok(Hop::Redirect(next))) => current = resolve_redirect(&current, next)?,
            Ok(Err(e)) => return Err(e),
        }
    }
    Err(FetchError::TooManyRedirects)
}

/// Resolve a redirect target against the URL that answered with it.
fn resolve_redirect(current: &str, next: String) -> Result<String, FetchError> {
    if next.starts_with("http://") || next.starts_with("https://") {
        return Ok(next);
    }
    // Relative redirect: resolve against the current origin.
    let u = parse_url(current)?;
    let scheme = if u.https { "https" } else { "http" };
    Ok(if next.starts_with('/') {
        format!("{scheme}://{}:{}{next}", u.host, u.port)
    } else {
        format!("{scheme}://{}:{}/{next}", u.host, u.port)
    })
}

/// Retry transient acquisition failures while persisting the absolute retry
/// deadline and elapsed-wait origin through `on_retry`. A restart can pass
/// the saved state back in, preserving the same token-scoped attempt and its
/// outstanding deadline (clamped to the schedule, so a deadline written under
/// an older, longer policy cannot outlive it). [`FetchKick::kick`] ends the
/// current wait at once: the operator's "retry now".
pub async fn http_get_observed<F, Fut>(
    url: &str,
    retry_state: FetchRetryState,
    mut on_retry: F,
    kick: Arc<FetchKick>,
) -> Result<Vec<u8>, FetchError>
where
    F: FnMut(FetchRetryState) -> Fut,
    Fut: Future<Output = bool>,
{
    // A resumed fetch waits on the deadline it was published with.
    kick.arm(retry_state.next_retry_at);
    let arm = kick.clone();
    let wait = kick.clone();
    http_get_observed_with_sleep(
        url,
        retry_state,
        move |state| {
            if state.next_retry_at.is_some() {
                arm.arm(state.next_retry_at);
            }
            on_retry(state)
        },
        move |delay| {
            let kick = wait.clone();
            async move {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = kick.notify.notified() => {
                        tracing::info!("NZB fetch retry requested by operator");
                    }
                }
                kick.disarm();
            }
        },
    )
    .await
}

async fn http_get_observed_with_sleep<F, Fut, S, SFut>(
    url: &str,
    mut retry_state: FetchRetryState,
    mut on_retry: F,
    mut sleep: S,
) -> Result<Vec<u8>, FetchError>
where
    F: FnMut(FetchRetryState) -> Fut,
    Fut: Future<Output = bool>,
    S: FnMut(Duration) -> SFut,
    SFut: Future<Output = ()>,
{
    let mut current = url.to_string();
    let mut redirects = 0;
    if let Some(deadline) = retry_state.next_retry_at {
        let wait = resume_wait(deadline, retry_state.attempts, unix_now());
        if !wait.is_zero() {
            sleep(wait).await;
        }
    }
    retry_state.next_retry_at = None;
    if !on_retry(retry_state.clone()).await {
        return Err(FetchError::Cancelled);
    }
    loop {
        match tokio::time::timeout(HOP_TIMEOUT, get_once(&current)).await {
            Err(_) => {
                let error = FetchError::Timeout;
                if !wait_before_retry(&mut retry_state, &mut on_retry, &mut sleep, &error, None)
                    .await
                {
                    return Err(FetchError::Cancelled);
                }
            }
            Ok(Ok(Hop::Body(bytes))) => return Ok(bytes),
            Ok(Ok(Hop::Redirect(next))) => {
                if redirects == MAX_REDIRECTS {
                    return Err(FetchError::TooManyRedirects);
                }
                redirects += 1;
                current = resolve_redirect(&current, next)?;
            }
            Ok(Err(e)) => {
                if let Some(refused) = quota_refusal(&e) {
                    tracing::warn!(error = %refused, attempts = retry_state.attempts,
                        "NZB fetch ended: the indexer refused for longer than the retry ladder");
                    return Err(refused);
                }
                let retry_after = match &e {
                    FetchError::HttpStatus { retry_after, .. } => *retry_after,
                    _ => None,
                };
                if retry_delay(&e, retry_state.attempts).is_none() {
                    return Err(e);
                }
                if !wait_before_retry(&mut retry_state, &mut on_retry, &mut sleep, &e, retry_after)
                    .await
                {
                    return Err(FetchError::Cancelled);
                }
            }
        }
    }
}

async fn wait_before_retry<F, Fut, S, SFut>(
    state: &mut FetchRetryState,
    on_retry: &mut F,
    sleep: &mut S,
    error: &FetchError,
    retry_after: Option<Duration>,
) -> bool
where
    F: FnMut(FetchRetryState) -> Fut,
    Fut: Future<Output = bool>,
    S: FnMut(Duration) -> SFut,
    SFut: Future<Output = ()>,
{
    let delay = retry_schedule(state.attempts, retry_after);
    let unix_deadline =
        unix_now().saturating_add(i64::try_from(delay.as_secs()).unwrap_or(i64::MAX));
    state.attempts = state.attempts.saturating_add(1);
    state.blocked_since.get_or_insert_with(unix_now);
    state.next_retry_at = Some(unix_deadline);
    // Do not persist URLs or error bodies; errors from providers may contain
    // credentials. The display string carries the useful stable diagnosis.
    // A Retry-After past the ladder never reaches here (quota_refusal), and
    // one inside it is the step's floor, so the step is the whole story.
    state.last_error = Some(error.to_string());
    tracing::warn!(error = %error, retry = state.attempts, delay_secs = delay.as_secs(),
        retry_after_secs = retry_after.map(|asked| asked.as_secs()), "NZB fetch will retry");
    if !on_retry(state.clone()).await {
        return false;
    }
    sleep(delay).await;
    state.next_retry_at = None;
    on_retry(state.clone()).await
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Contract §2.2: why an exhausted fetch failed. The retried branch of
/// [`retry_delay`] (429/503, I/O, timeouts, transport) ran out on an indexer
/// that was unavailable; everything it does not retry (404/410/403 and the
/// other non-retried statuses, a bad URL, a redirect loop) is the indexer
/// answering for this item: `gone`.
pub fn cause_of(error: &FetchError) -> nzbd_types::FetchCause {
    if matches!(error, FetchError::IndexerRefused { .. }) || retry_delay(error, 0).is_some() {
        nzbd_types::FetchCause::IndexerUnavailable
    } else {
        nzbd_types::FetchCause::Gone
    }
}

fn retry_delay(error: &FetchError, retries: u32) -> Option<Duration> {
    match error {
        FetchError::HttpStatus {
            status,
            retry_after,
        } if matches!(status.as_u16(), 429 | 503) => Some(retry_schedule(retries, *retry_after)),
        FetchError::Io(_) | FetchError::Timeout | FetchError::TransientNetwork(_) => {
            Some(retry_backoff(retries))
        }
        _ => None,
    }
}

fn retry_backoff(retries: u32) -> Duration {
    Duration::from_secs(
        RETRY_BACKOFF_SECONDS[usize::try_from(retries)
            .unwrap_or(usize::MAX)
            .min(RETRY_BACKOFF_SECONDS.len() - 1)],
    )
}

/// The longest wait the ladder schedules (its last step).
fn ladder_ceiling() -> Duration {
    Duration::from_secs(RETRY_BACKOFF_SECONDS[RETRY_BACKOFF_SECONDS.len() - 1])
}

/// A 429/503 asking for longer than the ladder's ceiling is an indexer quota
/// reset, not a rate-limit window: the job ends as `indexer_unavailable`
/// rather than retrying an account the indexer has locked.
fn quota_refusal(error: &FetchError) -> Option<FetchError> {
    match error {
        FetchError::HttpStatus {
            status,
            retry_after: Some(asked),
        } if matches!(status.as_u16(), 429 | 503) && *asked > ladder_ceiling() => {
            Some(FetchError::IndexerRefused {
                status: *status,
                retry_after: *asked,
            })
        }
        _ => None,
    }
}

/// A provider's Retry-After inside the ladder's own range (≤ 30 min) is a
/// genuine rate-limit window: wait at least that long rather than earn a
/// certain second refusal. Longer asks end the fetch ([`quota_refusal`]).
fn retry_schedule(retries: u32, retry_after: Option<Duration>) -> Duration {
    let step = retry_backoff(retries);
    let ceiling = ladder_ceiling();
    match retry_after.filter(|asked| *asked <= ceiling) {
        Some(asked) => step.max(asked),
        None => step,
    }
}

/// How long a restarted fetch still waits on a persisted deadline. This
/// policy never schedules past the 30-minute ceiling; a deadline beyond it
/// came from an older policy (one that obeyed a 22-hour Retry-After) and is
/// cut back to the step of the attempt that wrote it.
fn resume_wait(deadline: i64, attempts: u32, now: i64) -> Duration {
    let remaining = Duration::from_secs(deadline.saturating_sub(now).max(0) as u64);
    let ceiling = Duration::from_secs(RETRY_BACKOFF_SECONDS[RETRY_BACKOFF_SECONDS.len() - 1]);
    // Within the ceiling the deadline is one this policy could have written
    // (a short Retry-After raises a step); beyond it, it cannot.
    if remaining <= ceiling {
        remaining
    } else {
        retry_backoff(attempts.saturating_sub(1))
    }
}

fn human_wait_of(wait: &Duration) -> String {
    human_wait(*wait)
}

fn human_wait(wait: Duration) -> String {
    let minutes = wait.as_secs().div_ceil(60);
    match (minutes / 60, minutes % 60) {
        (0, m) => format!("{m}m"),
        (h, 0) => format!("{h}h"),
        (h, m) => format!("{h}h {m}m"),
    }
}

fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return value.parse().ok().map(Duration::from_secs);
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|date| date.duration_since(now).unwrap_or_default())
}

enum Hop {
    Body(Vec<u8>),
    Redirect(String),
}

async fn get_once(url: &str) -> Result<Hop, FetchError> {
    let u = parse_url(url)?;
    let tcp = TcpStream::connect((u.host.as_str(), u.port)).await?;
    if u.https {
        // Same rustls stack (and platform verifier) as the NNTP transport.
        let config = nzbd_nntp::transport::tls_client_config(CertLevel::Strict)
            .map_err(|e| FetchError::Http(e.to_string()))?;
        let connector = tokio_rustls::TlsConnector::from(config);
        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(u.host.clone())
            .map_err(|_| FetchError::BadUrl(url.into()))?;
        let tls = connector.connect(name, tcp).await?;
        request(TokioIo::new(tls), &u).await
    } else {
        request(TokioIo::new(tcp), &u).await
    }
}

async fn request<S>(io: S, u: &Url) -> Result<Hop, FetchError>
where
    S: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| FetchError::TransientNetwork(e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::get(&u.path)
        .header(hyper::header::HOST, u.host.as_str())
        .header(hyper::header::USER_AGENT, "nzbd")
        .header(hyper::header::ACCEPT, "*/*")
        .body(String::new())
        .map_err(|e| FetchError::Http(e.to_string()))?;
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| FetchError::TransientNetwork(e.to_string()))?;
    let status = resp.status();
    if status.is_redirection() {
        let loc = resp
            .headers()
            .get(hyper::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| FetchError::Http("redirect without Location".into()))?;
        return Ok(Hop::Redirect(loc.to_string()));
    }
    if !status.is_success() {
        let retry_after = resp
            .headers()
            .get(hyper::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_retry_after(v, SystemTime::now()));
        return Err(FetchError::HttpStatus {
            status,
            retry_after,
        });
    }
    let mut body = Vec::new();
    let mut incoming = resp.into_body();
    while let Some(frame) = incoming.frame().await {
        let frame = frame.map_err(|e| FetchError::TransientNetwork(e.to_string()))?;
        if let Some(chunk) = frame.data_ref() {
            if body.len() + chunk.len() > MAX_BODY {
                return Err(FetchError::Http("body too large for an NZB".into()));
            }
            body.extend_from_slice(chunk);
        }
    }
    Ok(Hop::Body(body))
}

#[cfg(test)]
mod tests {

    /// Contract §2.2 / plan M1 (l): the retried classes are an unavailable
    /// indexer; what the fetcher does not retry is the indexer answering for
    /// this item.
    #[test]
    fn fetch_404_is_gone_fetch_503_exhausted_is_indexer_unavailable() {
        use nzbd_types::FetchCause;
        let status = |code: u16| FetchError::HttpStatus {
            status: hyper::StatusCode::from_u16(code).unwrap(),
            retry_after: None,
        };
        for code in [404, 410, 403, 400, 500] {
            assert_eq!(cause_of(&status(code)), FetchCause::Gone, "{code}");
        }
        for code in [429, 503] {
            assert_eq!(
                cause_of(&status(code)),
                FetchCause::IndexerUnavailable,
                "{code}"
            );
        }
        assert_eq!(
            cause_of(&FetchError::Timeout),
            FetchCause::IndexerUnavailable
        );
        assert_eq!(
            cause_of(&FetchError::TransientNetwork("reset".into())),
            FetchCause::IndexerUnavailable
        );
        assert_eq!(cause_of(&FetchError::TooManyRedirects), FetchCause::Gone);
        assert_eq!(cause_of(&FetchError::BadUrl("x".into())), FetchCause::Gone);
        assert!(nzbd_types::FailureReason::Fetch.blames_release(Some(FetchCause::Gone)));
        assert!(
            !nzbd_types::FailureReason::Fetch.blames_release(Some(FetchCause::IndexerUnavailable))
        );
    }
    use super::*;
    use std::io::{Read as _, Write as _};

    #[test]
    fn retry_after_and_backoff_policy() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(
            parse_retry_after(" 120 ", now),
            Some(Duration::from_secs(120))
        );
        assert_eq!(parse_retry_after("0", now), Some(Duration::ZERO));
        assert_eq!(
            parse_retry_after(&httpdate::fmt_http_date(now + Duration::from_secs(90)), now),
            Some(Duration::from_secs(90))
        );
        assert_eq!(
            parse_retry_after(&httpdate::fmt_http_date(now - Duration::from_secs(90)), now),
            Some(Duration::ZERO)
        );
        for value in ["", "garbage", "-1", "+1", "18446744073709551616"] {
            assert_eq!(parse_retry_after(value, now), None);
        }
        let error = FetchError::HttpStatus {
            status: hyper::StatusCode::TOO_MANY_REQUESTS,
            retry_after: None,
        };
        for (retry, seconds) in [60, 300, 600, 1800, 1800].into_iter().enumerate() {
            assert_eq!(
                retry_delay(&error, retry as u32),
                Some(Duration::from_secs(seconds))
            );
        }
        assert_eq!(
            retry_delay(&error, u32::MAX),
            Some(Duration::from_secs(1800))
        );
        // Retry-After never shortens a step, raises one only within the
        // ladder's 30-minute ceiling, and a 12-hour quota reset still retries
        // on the next step.
        for (retry, server_seconds, expected) in [
            (0, 5, 60),
            (0, 90, 90),
            (0, 1800, 1800),
            (0, 1801, 60),
            (1, 90, 300),
            (2, 12 * 3600, 600),
            (3, 22 * 3600, 1800),
        ] {
            let status = FetchError::HttpStatus {
                status: hyper::StatusCode::SERVICE_UNAVAILABLE,
                retry_after: Some(Duration::from_secs(server_seconds)),
            };
            assert_eq!(
                retry_delay(&status, retry),
                Some(Duration::from_secs(expected))
            );
        }
        let permanent = FetchError::HttpStatus {
            status: hyper::StatusCode::FORBIDDEN,
            retry_after: Some(Duration::ZERO),
        };
        assert_eq!(retry_delay(&permanent, 0), None);
    }

    async fn serve_responses(
        responses: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/start", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                }
                requests.push(String::from_utf8(request).unwrap());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (url, server)
    }

    #[tokio::test]
    async fn a_single_attempt_follows_redirects_and_never_retries() {
        let (url, server) = serve_responses(vec![
            "HTTP/1.1 302 Found\r\nLocation: /nzb\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n<nzb />".into(),
        ])
        .await;
        assert_eq!(http_get_once(&url).await.unwrap(), b"<nzb />");
        assert_eq!(server.await.unwrap().len(), 2);

        // A rate limit is an answer, not a wait: the probe falls back at once.
        let (url, server) = serve_responses(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        ])
        .await;
        let started = std::time::Instant::now();
        assert!(matches!(
            http_get_once(&url).await,
            Err(FetchError::HttpStatus { .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rate_limit_after_redirect_waits_then_recovers() {
        let (url, server) = serve_responses(vec![
            "HTTP/1.1 302 Found\r\nLocation: /real\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n<nzb />".into(),
        ]).await;
        let delays = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let capture = delays.clone();
        assert_eq!(
            http_get_observed_with_sleep(
                &url,
                FetchRetryState::default(),
                |_| async { true },
                move |delay| {
                    capture.lock().unwrap().push(delay);
                    async {}
                },
            )
            .await
            .unwrap(),
            b"<nzb />"
        );
        let requests = server.await.unwrap();
        assert!(requests[1].starts_with("GET /real "));
        assert!(requests[2].starts_with("GET /real "));
        assert_eq!(*delays.lock().unwrap(), [Duration::from_secs(60)]);
    }

    #[tokio::test]
    async fn retryable_statuses_keep_the_same_request_until_success() {
        for (status, retry_after, expected) in [
            ("429 Too Many Requests", "0", [60, 300, 600]),
            ("503 Service Unavailable", "1", [60, 300, 600]),
            ("503 Service Unavailable", "90", [90, 300, 600]),
            ("429 Too Many Requests", "1800", [1800, 1800, 1800]),
        ] {
            let response = format!(
                "HTTP/1.1 {status}\r\nRetry-After: {retry_after}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let (url, server) = serve_responses(vec![
                response.clone(),
                response.clone(),
                response,
                "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n<nzb />".into(),
            ])
            .await;
            let delays = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let delay_capture = delays.clone();
            let states = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let state_capture = states.clone();
            assert_eq!(
                http_get_observed_with_sleep(
                    &url,
                    FetchRetryState::default(),
                    move |state| {
                        state_capture.lock().unwrap().push(state);
                        async { true }
                    },
                    move |delay| {
                        delay_capture.lock().unwrap().push(delay);
                        async {}
                    },
                )
                .await
                .unwrap(),
                b"<nzb />"
            );
            let requests = server.await.unwrap();
            assert_eq!(requests.len(), 4);
            assert_eq!(
                *delays.lock().unwrap(),
                expected.map(Duration::from_secs),
                "Retry-After {retry_after} inside the ladder is only the step's floor"
            );
            let scheduled = states
                .lock()
                .unwrap()
                .iter()
                .filter(|state| state.next_retry_at.is_some())
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(scheduled.len(), 3);
            for (state, delay) in scheduled.iter().zip(expected) {
                let error = state.last_error.as_deref().unwrap();
                assert!(
                    !error.contains("asked to wait"),
                    "a Retry-After inside the ladder is obeyed, not reported: {error}"
                );
                assert!(state.blocked_since.is_some());
                assert!(state.next_retry_at.is_some());
                assert!(state.attempts > 0);
                let remaining = state.next_retry_at.unwrap() - unix_now();
                let delay_seconds = delay as i64;
                assert!(
                    (delay_seconds - 1..=delay_seconds).contains(&remaining),
                    "durable retry deadline must represent the selected {delay}s delay"
                );
            }
        }
    }

    /// Job 4198 (2026-11-25): nzb.life answered an exhausted daily allowance
    /// with 429 and a ~24 h Retry-After, and the fetch retried the locked
    /// account every 30 minutes, the job stuck at "Fetching NZB". A wait
    /// beyond the ladder now ends the fetch at once, as an unavailable
    /// indexer (never blaming the release), naming the wait.
    #[tokio::test]
    async fn a_quota_length_retry_after_ends_the_fetch_as_indexer_unavailable() {
        for (status, code) in [("429 Too Many Requests", 429), ("503 Service Unavailable", 503)] {
            let (url, server) = serve_responses(vec![format!(
                "HTTP/1.1 {status}\r\nRetry-After: 85296\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )])
            .await;
            let delays = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let delay_capture = delays.clone();
            let error = http_get_observed_with_sleep(
                &url,
                FetchRetryState::default(),
                |_| async { true },
                move |delay| {
                    delay_capture.lock().unwrap().push(delay);
                    async {}
                },
            )
            .await
            .unwrap_err();
            match &error {
                FetchError::IndexerRefused { status, retry_after } => {
                    assert_eq!(status.as_u16(), code);
                    assert_eq!(*retry_after, Duration::from_secs(85_296));
                }
                other => panic!("{code}: expected IndexerRefused, got {other:?}"),
            }
            assert!(error.to_string().contains("asked to wait 23h 42m"), "{error}");
            assert_eq!(cause_of(&error), nzbd_types::FetchCause::IndexerUnavailable);
            assert!(!nzbd_types::FailureReason::Fetch.blames_release(Some(cause_of(&error))));
            assert!(delays.lock().unwrap().is_empty(), "a locked account was waited on");
            assert_eq!(server.await.unwrap().len(), 1, "a locked account was asked twice");
        }
    }

    #[test]
    fn transient_network_errors_retry_indefinitely_with_capped_backoff() {
        let error = FetchError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert_eq!(retry_delay(&error, 0), Some(Duration::from_secs(60)));
        assert_eq!(
            retry_delay(&FetchError::Timeout, 1),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            retry_delay(&FetchError::Timeout, 2),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            retry_delay(&FetchError::Timeout, 3),
            Some(Duration::from_secs(1800))
        );
        assert_eq!(
            retry_delay(&FetchError::Timeout, 100),
            Some(Duration::from_secs(1800))
        );
        assert_eq!(
            retry_delay(&FetchError::BadUrl("not-a-url".into()), 0),
            None,
            "invalid URL submissions stop immediately"
        );
    }

    #[test]
    fn restart_clamps_a_deadline_from_an_older_longer_policy() {
        let now = 1_000_000;
        // A deadline written under the old policy, obeying a 22 h Retry-After.
        assert_eq!(
            resume_wait(now + 22 * 3600, 1, now),
            Duration::from_secs(60)
        );
        assert_eq!(
            resume_wait(now + 22 * 3600, 9, now),
            Duration::from_secs(1800)
        );
        // A deadline this policy could write is honored; a past one is due.
        assert_eq!(resume_wait(now + 200, 2, now), Duration::from_secs(200));
        assert_eq!(resume_wait(now - 5, 2, now), Duration::ZERO);
        assert_eq!(resume_wait(now + 10, 0, now), Duration::from_secs(10));
        assert_eq!(resume_wait(now + 1800, 1, now), Duration::from_secs(1800));
        assert_eq!(resume_wait(now + 1801, 1, now), Duration::from_secs(60));
        assert_eq!(human_wait(Duration::from_secs(43_200)), "12h");
        assert_eq!(human_wait(Duration::from_secs(5_430)), "1h 31m");
        assert_eq!(human_wait(Duration::from_secs(61)), "2m");
    }

    /// Serves 429 (with no Retry-After: the ladder's own wait) `refusals`
    /// times, then the NZB.
    async fn refuse_then_serve(refusals: usize) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let mut responses = vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string();
            refusals
        ];
        responses.push(
            "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n<nzb />".into(),
        );
        serve_responses(responses).await
    }

    /// Starts a real-sleep fetch; each published wait is sent on the channel.
    fn spawn_fetch(
        url: String,
        kick: Arc<FetchKick>,
    ) -> (
        tokio::task::JoinHandle<Result<Vec<u8>, FetchError>>,
        tokio::sync::mpsc::UnboundedReceiver<i64>,
    ) {
        let (waits_tx, waits) = tokio::sync::mpsc::unbounded_channel();
        let fetch = tokio::spawn(async move {
            http_get_observed(
                &url,
                FetchRetryState::default(),
                move |state| {
                    if let Some(deadline) = state.next_retry_at {
                        let _ = waits_tx.send(deadline);
                    }
                    async { true }
                },
                kick,
            )
            .await
        });
        (fetch, waits)
    }

    #[tokio::test]
    async fn operator_kick_ends_the_wait_and_retries_at_once() {
        let (url, server) = refuse_then_serve(1).await;
        let kick = Arc::new(FetchKick::default());
        let (fetch, mut waits) = spawn_fetch(url, kick.clone());
        // Kick as soon as the wait is published, possibly before the sleep
        // has started: the permit must still end the one-minute wait.
        let deadline = waits.recv().await.unwrap();
        assert!(
            !kick.kick(deadline + 1),
            "a kick for another deadline misses"
        );
        assert!(kick.kick(deadline));
        let body = tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("kick did not end the wait")
            .unwrap()
            .unwrap();
        assert_eq!(body, b"<nzb />");
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_double_kick_cannot_skip_the_next_wait() {
        let (url, server) = refuse_then_serve(2).await;
        let kick = Arc::new(FetchKick::default());
        let (fetch, mut waits) = spawn_fetch(url, kick.clone());
        let first = waits.recv().await.unwrap();
        assert!(kick.kick(first));
        // The second click of a double click: the wait it names is over.
        let second = loop {
            kick.kick(first);
            if let Ok(deadline) = waits.try_recv() {
                break deadline;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        // The second refusal must actually wait (five minutes), not run on a
        // permit left behind by the double kick.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!fetch.is_finished(), "a stale kick skipped the next wait");
        assert!(kick.kick(second));
        tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("kick did not end the second wait")
            .unwrap()
            .unwrap();
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[test]
    fn url_parsing() {
        let u = parse_url("https://indexer.example/api?t=get&id=1").unwrap();
        assert!(u.https);
        assert_eq!(u.host, "indexer.example");
        assert_eq!(u.port, 443);
        assert_eq!(u.path, "/api?t=get&id=1");

        let u = parse_url("http://10.0.0.5:8080/x.nzb").unwrap();
        assert!(!u.https);
        assert_eq!(u.port, 8080);

        assert!(parse_url("ftp://nope").is_err());
        assert!(parse_url("https://").is_err());
    }

    /// Plain-HTTP round trip against an in-process listener, including a
    /// redirect hop and a chunked response body.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_get_with_redirect_and_chunked() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            // Hop 1: redirect.
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{port}/real\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
            drop(s);
            // Hop 2: chunked body.
            let (mut s, _) = listener.accept().unwrap();
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                  5\r\n<nzb \r\n2\r\n/>\r\n0\r\n\r\n",
            );
        });
        let body = http_get(&format!("http://127.0.0.1:{port}/start"))
            .await
            .unwrap();
        assert_eq!(body, b"<nzb />");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relative_redirect_loops_and_http_failures_are_explicit() {
        assert!(parse_url("http://example.test:99999/file.nzb").is_err());

        // Alternate the two relative redirect spellings until the bounded
        // redirect budget is exhausted. This is deterministic and proves the
        // fetcher cannot spin forever on a hostile indexer.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let redirects = std::thread::spawn(move || {
            for hop in 0..=MAX_REDIRECTS {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request);
                let location = if hop % 2 == 0 { "next" } else { "/start" };
                let _ = socket.write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                );
            }
        });
        let error = http_get(&format!("http://127.0.0.1:{port}/start"))
            .await
            .unwrap_err();
        assert!(matches!(error, FetchError::TooManyRedirects));
        redirects.join().unwrap();

        for (response, expected) in [
            (
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "status 404",
            ),
            (
                "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "redirect without Location",
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let response = response.to_string();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request);
                let _ = socket.write_all(response.as_bytes());
            });
            let error = http_get(&format!("http://127.0.0.1:{port}/file.nzb"))
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "unexpected error: {error}");
            server.join().unwrap();
        }
    }
}
