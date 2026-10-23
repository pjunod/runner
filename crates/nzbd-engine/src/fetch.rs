//! Minimal HTTPS/HTTP fetcher for URL jobs (`AddUrl`): hyper HTTP/1.1 over
//! the same rustls stack the NNTP transport uses. Follows up to 5
//! redirects, caps bodies at 64 MiB (an NZB, not a payload), 60 s timeout
//! per hop. Rate limits, service-unavailable responses and transient network
//! failures retry the same durable URL job until success or a permanent
//! response, honoring Retry-After or backing off for 30, 60, 120, then 300
//! seconds indefinitely.

use http_body_util::BodyExt;
use hyper::Request;
use hyper_util::rt::TokioIo;
use nzbd_types::CertLevel;
use std::future::Future;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;

const MAX_REDIRECTS: usize = 5;
const MAX_BODY: usize = 64 * 1024 * 1024;
const HOP_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_BACKOFF_SECONDS: [u64; 4] = [30, 60, 120, 300];
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
    #[error("redirect loop / too many redirects")]
    TooManyRedirects,
    #[error("timed out")]
    Timeout,
    #[error("URL fetch stopped because its queue job is no longer fetching")]
    Cancelled,
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
    http_get_observed(url, FetchRetryState::default(), |_| async { true }).await
}

/// Retry transient acquisition failures while persisting the absolute retry
/// deadline and elapsed-wait origin through `on_retry`. A restart can pass
/// the saved state back in, preserving the same token-scoped attempt and
/// honoring a provider's outstanding Retry-After delay.
pub async fn http_get_observed<F, Fut>(
    url: &str,
    mut retry_state: FetchRetryState,
    mut on_retry: F,
) -> Result<Vec<u8>, FetchError>
where
    F: FnMut(FetchRetryState) -> Fut,
    Fut: Future<Output = bool>,
{
    let mut current = url.to_string();
    let mut redirects = 0;
    if let Some(deadline) = retry_state.next_retry_at {
        let wait = deadline.saturating_sub(unix_now()).max(0) as u64;
        if wait > 0 {
            tokio::time::sleep(Duration::from_secs(wait)).await;
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
                if !wait_before_retry(&mut retry_state, &mut on_retry, &error, None).await {
                    return Err(FetchError::Cancelled);
                }
            }
            Ok(Ok(Hop::Body(bytes))) => return Ok(bytes),
            Ok(Ok(Hop::Redirect(next))) => {
                if redirects == MAX_REDIRECTS {
                    return Err(FetchError::TooManyRedirects);
                }
                redirects += 1;
                current = if next.starts_with("http://") || next.starts_with("https://") {
                    next
                } else {
                    // Relative redirect: resolve against the current origin.
                    let u = parse_url(&current)?;
                    let scheme = if u.https { "https" } else { "http" };
                    if next.starts_with('/') {
                        format!("{scheme}://{}:{}{next}", u.host, u.port)
                    } else {
                        format!("{scheme}://{}:{}/{next}", u.host, u.port)
                    }
                };
            }
            Ok(Err(e)) => {
                let retry_after = match &e {
                    FetchError::HttpStatus { retry_after, .. } => *retry_after,
                    _ => None,
                };
                if retry_delay(&e, retry_state.attempts).is_none() {
                    return Err(e);
                }
                if !wait_before_retry(&mut retry_state, &mut on_retry, &e, retry_after).await {
                    return Err(FetchError::Cancelled);
                }
            }
        }
    }
}

async fn wait_before_retry<F, Fut>(
    state: &mut FetchRetryState,
    on_retry: &mut F,
    error: &FetchError,
    retry_after: Option<Duration>,
) -> bool
where
    F: FnMut(FetchRetryState) -> Fut,
    Fut: Future<Output = bool>,
{
    let delay = retry_after.unwrap_or_else(|| retry_backoff(state.attempts));
    let unix_deadline =
        unix_now().saturating_add(i64::try_from(delay.as_secs()).unwrap_or(i64::MAX));
    state.attempts = state.attempts.saturating_add(1);
    state.blocked_since.get_or_insert_with(unix_now);
    state.next_retry_at = Some(unix_deadline);
    // Do not persist URLs or error bodies; errors from providers may contain
    // credentials. The display string carries the useful stable diagnosis.
    state.last_error = Some(error.to_string());
    tracing::warn!(error = %error, retry = state.attempts, delay_secs = delay.as_secs(), "NZB fetch will retry");
    if !on_retry(state.clone()).await {
        return false;
    }
    let mut remaining = delay;
    let chunk = Duration::from_secs(24 * 60 * 60);
    while !remaining.is_zero() {
        let wait = remaining.min(chunk);
        tokio::time::sleep(wait).await;
        remaining = remaining.saturating_sub(wait);
    }
    state.next_retry_at = None;
    on_retry(state.clone()).await
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn retry_delay(error: &FetchError, retries: u32) -> Option<Duration> {
    match error {
        FetchError::HttpStatus {
            status,
            retry_after,
        } if matches!(status.as_u16(), 429 | 503) => {
            Some(retry_after.unwrap_or_else(|| retry_backoff(retries)))
        }
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
        for (retry, seconds) in [30, 60, 120, 300, 300].into_iter().enumerate() {
            assert_eq!(
                retry_delay(&error, retry as u32),
                Some(Duration::from_secs(seconds))
            );
        }
        assert_eq!(
            retry_delay(&error, u32::MAX),
            Some(Duration::from_secs(300))
        );
        let permanent = FetchError::HttpStatus {
            status: hyper::StatusCode::FORBIDDEN,
            retry_after: Some(Duration::ZERO),
        };
        assert_eq!(retry_delay(&permanent, 0), None);
    }

    async fn serve_responses(
        responses: Vec<String>,
    ) -> (
        String,
        tokio::task::JoinHandle<Vec<(String, std::time::Instant)>>,
    ) {
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
                requests.push((
                    String::from_utf8(request).unwrap(),
                    std::time::Instant::now(),
                ));
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (url, server)
    }

    #[tokio::test]
    async fn rate_limit_after_redirect_waits_then_recovers() {
        let (url, server) = serve_responses(vec![
            "HTTP/1.1 302 Found\r\nLocation: /real\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n<nzb />".into(),
        ]).await;
        assert_eq!(http_get(&url).await.unwrap(), b"<nzb />");
        let requests = server.await.unwrap();
        assert!(requests[1].0.starts_with("GET /real "));
        assert!(requests[2].0.starts_with("GET /real "));
        assert!(requests[2].1.duration_since(requests[1].1) >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn retryable_statuses_keep_the_same_request_until_success() {
        for status in ["429 Too Many Requests", "503 Service Unavailable"] {
            let response = format!("HTTP/1.1 {status}\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let (url, server) = serve_responses(vec![
                response.clone(),
                response.clone(),
                response,
                "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n<nzb />".into(),
            ])
            .await;
            assert_eq!(http_get(&url).await.unwrap(), b"<nzb />");
            assert_eq!(server.await.unwrap().len(), 4);
        }
    }

    #[test]
    fn transient_network_errors_retry_indefinitely_with_capped_backoff() {
        let error = FetchError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert_eq!(retry_delay(&error, 0), Some(Duration::from_secs(30)));
        assert_eq!(
            retry_delay(&FetchError::Timeout, 1),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            retry_delay(&FetchError::Timeout, 2),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            retry_delay(&FetchError::Timeout, 3),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            retry_delay(&FetchError::Timeout, 100),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            retry_delay(&FetchError::BadUrl("not-a-url".into()), 0),
            None,
            "invalid URL submissions stop immediately"
        );
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
