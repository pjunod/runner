//! Minimal intra-cluster HTTP client (worker → leader JSON calls and the
//! any-node → leader reverse proxy) on hyper-util's pooled legacy client.

use crate::proto::{MAX_RPC_BYTES, SECRET_HEADER};
use axum::body::Body;
use http_body_util::BodyExt;
use hyper::body::Body as _;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

const WORK_RPC_TOTAL_TIMEOUT: Duration = Duration::from_secs(10);
const WORK_RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const WORK_RPC_MAX_BODY: usize = MAX_RPC_BYTES;

#[derive(Clone)]
pub struct ClusterClient {
    inner: HyperClient<HttpConnector, Body>,
    secret: String,
}

impl ClusterClient {
    pub fn new(secret: String) -> ClusterClient {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(WORK_RPC_CONNECT_TIMEOUT));
        ClusterClient {
            inner: HyperClient::builder(TokioExecutor::new()).build(connector),
            secret,
        }
    }

    pub async fn post_json<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        req: &Req,
    ) -> Result<Resp, String> {
        let uri: hyper::Uri = format!("{}{}", base_url.trim_end_matches('/'), path)
            .parse()
            .map_err(|e| format!("bad url: {e}"))?;
        let body = serde_json::to_vec(req).map_err(|e| e.to_string())?;
        if body.len() > WORK_RPC_MAX_BODY {
            return Err(format!("request exceeds {WORK_RPC_MAX_BODY} byte cluster RPC limit; reduce the descriptor before retrying"));
        }
        let request = hyper::Request::post(uri)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(SECRET_HEADER, &self.secret)
            .body(Body::from(body))
            .map_err(|e| e.to_string())?;
        tokio::time::timeout(WORK_RPC_TOTAL_TIMEOUT, async {
            let mut resp = self
                .inner
                .request(request)
                .await
                .map_err(|e| format!("request: {e}"))?;
            let status = resp.status();
            if resp
                .body()
                .size_hint()
                .upper()
                .is_some_and(|size| size > WORK_RPC_MAX_BODY as u64)
            {
                return Err(format!(
                    "response exceeds {WORK_RPC_MAX_BODY} byte cluster RPC limit"
                ));
            }
            let mut bytes = Vec::new();
            while let Some(frame) = resp.body_mut().frame().await {
                let frame = frame.map_err(|e| format!("body: {e}"))?;
                if let Some(data) = frame.data_ref() {
                    if bytes.len().saturating_add(data.len()) > WORK_RPC_MAX_BODY {
                        return Err(format!(
                            "response exceeds {WORK_RPC_MAX_BODY} byte cluster RPC limit"
                        ));
                    }
                    bytes.extend_from_slice(data);
                }
            }
            if !status.is_success() {
                return Err(format!(
                    "{status}: {}",
                    String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
                ));
            }
            serde_json::from_slice(&bytes).map_err(|e| format!("decode: {e}"))
        })
        .await
        .map_err(|_| "work RPC total deadline exceeded".to_owned())?
    }

    /// Forward an arbitrary request to the leader (streaming both ways).
    pub async fn forward(
        &self,
        base_url: &str,
        mut req: hyper::Request<Body>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, String> {
        let path_q = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".into());
        let uri: hyper::Uri = format!("{}{}", base_url.trim_end_matches('/'), path_q)
            .parse()
            .map_err(|e| format!("bad url: {e}"))?;
        *req.uri_mut() = uri;
        self.inner
            .request(req)
            .await
            .map_err(|e| format!("proxy: {e}"))
    }
}

/// Constant-time-ish secret comparison (length + fold, no early exit).
pub fn secret_matches(provided: Option<&str>, expected: &str) -> bool {
    let Some(p) = provided else { return false };
    if p.len() != expected.len() {
        return false;
    }
    p.bytes()
        .zip(expected.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_compare() {
        assert!(secret_matches(Some("abc"), "abc"));
        assert!(!secret_matches(Some("abd"), "abc"));
        assert!(!secret_matches(Some("ab"), "abc"));
        assert!(!secret_matches(None, "abc"));
    }
    #[tokio::test]
    async fn work_rpc_round_trips_descriptors_above_one_mib_and_bounds_both_directions() {
        use axum::{extract::DefaultBodyLimit, routing::post, Json, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/rpc",
                post(|Json(value): Json<serde_json::Value>| async move { Json(value) }),
            )
            .route(
                "/oversized-response",
                post(|| async { Json("x".repeat(MAX_RPC_BYTES + 1)) }),
            )
            .layer(DefaultBodyLimit::max(MAX_RPC_BYTES));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = ClusterClient::new("secret".into());
        let descriptor = serde_json::json!({"job": {"files": "x".repeat(2 * 1024 * 1024)}});
        let base = format!("http://{address}");
        let response: serde_json::Value =
            client.post_json(&base, "/rpc", &descriptor).await.unwrap();
        assert_eq!(response, descriptor);
        let too_large = "x".repeat(MAX_RPC_BYTES + 1);
        let error = client
            .post_json::<_, serde_json::Value>(&base, "/rpc", &too_large)
            .await
            .unwrap_err();
        assert!(error.contains("request exceeds"));
        let error = client
            .post_json::<_, serde_json::Value>(&base, "/oversized-response", &())
            .await
            .unwrap_err();
        assert!(error.contains("response exceeds"));
        server.abort();
    }
}
