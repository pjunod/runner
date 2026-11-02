use percent_encoding::percent_decode;
use reqwest::header::LOCATION;
use reqwest::{Client, StatusCode};
use rustls::client::danger::ServerCertVerifier;
use rustls::{ClientConfig, ConfigBuilder};
use rustls_platform_verifier::Verifier;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::{
    install_process_crypto_provider, validate_metainfo_contract_with_limit, TorrentError,
    DEFAULT_MAX_METAINFO_BYTES, DEFAULT_TORRENT_SOURCE_CONNECT_TIMEOUT,
    DEFAULT_TORRENT_SOURCE_TOTAL_TIMEOUT, MAX_CONFIGURED_MAX_METAINFO_BYTES,
    MAX_TORRENT_SOURCE_REDIRECTS, MIN_CONFIGURED_MAX_METAINFO_BYTES,
};

/// Resource limits for one authenticated HTTP(S) `.torrent` fetch.
///
/// The helper is deliberately not wired to daemon configuration or API input.
/// These values make the dormant boundary reviewable before that integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentSourceFetchLimits {
    pub max_metainfo_bytes: usize,
    pub max_redirects: usize,
    pub connect_timeout: Duration,
    pub total_timeout: Duration,
}

impl Default for TorrentSourceFetchLimits {
    fn default() -> Self {
        Self {
            max_metainfo_bytes: DEFAULT_MAX_METAINFO_BYTES,
            max_redirects: MAX_TORRENT_SOURCE_REDIRECTS,
            connect_timeout: DEFAULT_TORRENT_SOURCE_CONNECT_TIMEOUT,
            total_timeout: DEFAULT_TORRENT_SOURCE_TOTAL_TIMEOUT,
        }
    }
}

impl TorrentSourceFetchLimits {
    fn validate(self) -> Result<Self, TorrentError> {
        if !(MIN_CONFIGURED_MAX_METAINFO_BYTES..=MAX_CONFIGURED_MAX_METAINFO_BYTES)
            .contains(&self.max_metainfo_bytes)
        {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "metainfo limit must be between 1 MiB and 100 MiB",
            ));
        }
        if self.max_redirects > MAX_TORRENT_SOURCE_REDIRECTS {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "redirect limit cannot exceed 5",
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "connect timeout must be non-zero",
            ));
        }
        if self.total_timeout.is_zero() {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "total timeout must be non-zero",
            ));
        }
        if self.connect_timeout > self.total_timeout {
            return Err(TorrentError::InvalidTorrentSourceLimits(
                "connect timeout cannot exceed total timeout",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone)]
struct SourceCredentials {
    username: String,
    password: Option<String>,
}

/// Fetch and preflight one authenticated HTTP(S) `.torrent` source.
///
/// Redirects are followed manually so every target is revalidated. Basic URL
/// credentials survive only same-origin redirects, no response cookies are
/// retained, the body is bounded while it streams, and the total timeout spans
/// the complete redirect chain and body. Returned bytes have passed the same
/// metainfo, path, geometry, privacy, and tracker checks used before engine
/// admission. This helper starts no torrent session and performs no payload I/O.
pub async fn fetch_torrent_source(
    source: &str,
    limits: TorrentSourceFetchLimits,
    engine_proxy_enabled: bool,
) -> Result<Vec<u8>, TorrentError> {
    let limits = limits.validate()?;
    let mut source_url = Url::parse(source)
        .map_err(|_| TorrentError::InvalidTorrentSource("URL syntax is not valid"))?;
    validate_source_url(&source_url)?;
    let credentials = take_source_credentials(&mut source_url)?;
    let timeout_origin = safe_origin(&source_url);

    install_process_crypto_provider()?;
    let tls_config =
        platform_tls_config().map_err(|_| TorrentError::TorrentSourceRequestFailed {
            origin: timeout_origin.clone(),
        })?;
    let client = build_source_client(limits, tls_config).map_err(|_| {
        TorrentError::TorrentSourceRequestFailed {
            origin: timeout_origin.clone(),
        }
    })?;

    match tokio::time::timeout(
        limits.total_timeout,
        fetch_redirect_chain(
            &client,
            source_url,
            credentials,
            limits,
            engine_proxy_enabled,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(TorrentError::TorrentSourceTimeout {
            origin: timeout_origin,
        }),
    }
}

fn platform_tls_config() -> Result<ClientConfig, rustls::Error> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .ok_or_else(|| rustls::Error::General("process crypto provider is not installed".into()))?;
    let verifier = Verifier::new(provider.clone())?;
    tls_config_with_verifier(provider, Arc::new(verifier))
}

fn tls_config_with_verifier(
    provider: Arc<rustls::crypto::CryptoProvider>,
    verifier: Arc<dyn ServerCertVerifier>,
) -> Result<ClientConfig, rustls::Error> {
    let builder: ConfigBuilder<ClientConfig, rustls::WantsVerifier> =
        ClientConfig::builder_with_provider(provider).with_safe_default_protocol_versions()?;
    Ok(builder
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth())
}

fn build_source_client(
    limits: TorrentSourceFetchLimits,
    tls_config: ClientConfig,
) -> Result<Client, reqwest::Error> {
    source_client_builder(limits, tls_config).build()
}

fn source_client_builder(
    limits: TorrentSourceFetchLimits,
    tls_config: ClientConfig,
) -> reqwest::ClientBuilder {
    Client::builder()
        .user_agent(crate::identity::CLIENT_USER_AGENT)
        .no_proxy()
        .use_preconfigured_tls(tls_config)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(limits.connect_timeout)
}

async fn fetch_redirect_chain(
    client: &Client,
    mut source_url: Url,
    mut credentials: Option<SourceCredentials>,
    limits: TorrentSourceFetchLimits,
    engine_proxy_enabled: bool,
) -> Result<Vec<u8>, TorrentError> {
    let mut redirects = 0usize;
    loop {
        let origin = safe_origin(&source_url);
        let mut request = client.get(source_url.clone());
        if let Some(credentials) = &credentials {
            request = request.basic_auth(&credentials.username, credentials.password.as_deref());
        }
        let mut response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                TorrentError::TorrentSourceTimeout {
                    origin: origin.clone(),
                }
            } else {
                TorrentError::TorrentSourceRequestFailed {
                    origin: origin.clone(),
                }
            }
        })?;

        if is_followed_redirect(response.status()) {
            if redirects == limits.max_redirects {
                return Err(TorrentError::TooManyTorrentSourceRedirects {
                    limit: limits.max_redirects,
                });
            }
            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "Location header is missing",
                })?
                .to_str()
                .map_err(|_| TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "Location header is not valid text",
                })?;
            let mut next_url = source_url.join(location).map_err(|_| {
                TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "target URL syntax is not valid",
                }
            })?;
            validate_source_url(&next_url).map_err(|_| {
                TorrentError::InvalidTorrentSourceRedirect {
                    origin: origin.clone(),
                    reason: "target must be an HTTP(S) URL with a host",
                }
            })?;

            let same_origin = source_url.origin() == next_url.origin();
            clear_source_credentials(&mut next_url);
            if !same_origin {
                credentials = None;
            }
            source_url = next_url;
            redirects += 1;
            continue;
        }

        if !response.status().is_success() {
            if matches!(response.status().as_u16(), 429 | 503) {
                let now = std::time::SystemTime::now();
                let not_before_unix = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| {
                        value
                            .parse::<u64>()
                            .ok()
                            .and_then(|seconds| {
                                now.checked_add(std::time::Duration::from_secs(seconds))
                            })
                            .or_else(|| httpdate::parse_http_date(value).ok())
                    })
                    .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
                    .and_then(|at| i64::try_from(at.as_secs()).ok());
                return Err(TorrentError::TorrentSourceRetry {
                    status: response.status().as_u16(),
                    origin,
                    not_before_unix,
                });
            }
            return Err(TorrentError::TorrentSourceHttpStatus {
                status: response.status().as_u16(),
                origin,
            });
        }

        if let Some(size) = response.content_length() {
            if size > limits.max_metainfo_bytes as u64 {
                return Err(TorrentError::MetainfoTooLarge {
                    size: usize::try_from(size).unwrap_or(usize::MAX),
                    limit: limits.max_metainfo_bytes,
                });
            }
        }

        let capacity = response
            .content_length()
            .and_then(|size| usize::try_from(size).ok())
            .unwrap_or(0);
        let mut bytes = Vec::with_capacity(capacity);
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                TorrentError::TorrentSourceTimeout {
                    origin: origin.clone(),
                }
            } else {
                TorrentError::TorrentSourceRequestFailed {
                    origin: origin.clone(),
                }
            }
        })? {
            let next_size = bytes.len().saturating_add(chunk.len());
            if next_size > limits.max_metainfo_bytes {
                return Err(TorrentError::MetainfoTooLarge {
                    size: next_size,
                    limit: limits.max_metainfo_bytes,
                });
            }
            bytes.extend_from_slice(&chunk);
        }

        validate_metainfo_contract_with_limit(
            &bytes,
            engine_proxy_enabled,
            false,
            limits.max_metainfo_bytes,
        )?;
        return Ok(bytes);
    }
}

fn validate_source_url(url: &Url) -> Result<(), TorrentError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(TorrentError::InvalidTorrentSource(
            "scheme must be http or https",
        ));
    }
    if url.host_str().is_none() {
        return Err(TorrentError::InvalidTorrentSource("host is required"));
    }
    Ok(())
}

fn take_source_credentials(url: &mut Url) -> Result<Option<SourceCredentials>, TorrentError> {
    let credentials = if url.username().is_empty() && url.password().is_none() {
        None
    } else {
        let username = percent_decode(url.username().as_bytes())
            .decode_utf8()
            .map_err(|_| TorrentError::InvalidTorrentSource("username must contain valid UTF-8"))?
            .into_owned();
        let password = url
            .password()
            .map(|password| {
                percent_decode(password.as_bytes())
                    .decode_utf8()
                    .map(|password| password.into_owned())
                    .map_err(|_| {
                        TorrentError::InvalidTorrentSource("password must contain valid UTF-8")
                    })
            })
            .transpose()?;
        Some(SourceCredentials { username, password })
    };
    clear_source_credentials(url);
    Ok(credentials)
}

fn clear_source_credentials(url: &mut Url) {
    url.set_username("")
        .expect("HTTP(S) URLs support clearing usernames");
    url.set_password(None)
        .expect("HTTP(S) URLs support clearing passwords");
}

fn safe_origin(url: &Url) -> String {
    url.origin().ascii_serialization()
}

fn is_followed_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_os = "android"))]
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair, KeyUsagePurpose,
    };
    #[cfg(not(target_os = "android"))]
    use rustls::pki_types::PrivatePkcs8KeyDer;
    #[cfg(not(target_os = "android"))]
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[cfg(not(target_os = "android"))]
    use tokio::net::TcpListener;
    #[cfg(not(target_os = "android"))]
    use tokio_rustls::TlsAcceptor;

    const VALID_METAINFO: &[u8] =
        b"d4:infod6:lengthi1e4:name1:a12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";

    #[cfg(not(target_os = "android"))]
    #[tokio::test]
    async fn preconfigured_platform_verifier_accepts_a_private_ca_loopback() {
        install_process_crypto_provider().expect("install aws-lc provider");

        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA params");
        let mut ca_name = DistinguishedName::new();
        ca_name.push(DnType::CommonName, "nzbd test root CA");
        ca_params.distinguished_name = ca_name;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate().expect("CA key");
        let ca = ca_params.self_signed(&ca_key).expect("self-signed CA");

        let mut intermediate_params =
            CertificateParams::new(Vec::new()).expect("intermediate params");
        let mut intermediate_name = DistinguishedName::new();
        intermediate_name.push(DnType::CommonName, "nzbd test intermediate CA");
        intermediate_params.distinguished_name = intermediate_name;
        intermediate_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        intermediate_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let intermediate_key = KeyPair::generate().expect("intermediate key");
        let intermediate = intermediate_params
            .signed_by(&intermediate_key, &ca, &ca_key)
            .expect("root-signed intermediate");

        let mut leaf_params =
            CertificateParams::new(vec!["localhost".into()]).expect("leaf params");
        let mut leaf_name = DistinguishedName::new();
        leaf_name.push(DnType::CommonName, "localhost");
        leaf_params.distinguished_name = leaf_name;
        leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let leaf_key = KeyPair::generate().expect("leaf key");
        let leaf = leaf_params
            .signed_by(&leaf_key, &intermediate, &intermediate_key)
            .expect("intermediate-signed leaf");

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone(), intermediate.der().clone()],
                PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
            )
            .expect("TLS server config");
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind TLS loopback");
        let address = listener.local_addr().expect("TLS loopback address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept TLS client");
            let mut stream = TlsAcceptor::from(Arc::new(server_config))
                .accept(stream)
                .await
                .expect("complete TLS handshake");
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.expect("read HTTPS request");
                assert_ne!(read, 0, "client closed before sending request headers");
                request.extend_from_slice(&buffer[..read]);
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                VALID_METAINFO.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write HTTPS response headers");
            stream
                .write_all(VALID_METAINFO)
                .await
                .expect("write HTTPS metainfo");
        });

        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .expect("installed crypto provider");
        let verifier = Verifier::new_with_extra_roots([ca.der().clone()], provider.clone())
            .expect("platform verifier with private CA");
        let tls_config = tls_config_with_verifier(provider, Arc::new(verifier))
            .expect("platform-verifier TLS config");
        let limits = TorrentSourceFetchLimits::default();
        let client = source_client_builder(limits, tls_config)
            // Keep the certificate name realistic while making the loopback
            // address family deterministic on every native runner.
            .resolve("localhost", address)
            .build()
            .expect("source client");
        let source_url = Url::parse(&format!("https://localhost:{}/source", address.port()))
            .expect("HTTPS source URL");

        let bytes = fetch_redirect_chain(&client, source_url, None, limits, false)
            .await
            .expect("private-CA HTTPS fetch");
        assert_eq!(bytes, VALID_METAINFO);
        server.await.expect("TLS server task");
    }

    #[test]
    fn configured_metainfo_limits_accept_the_exact_range() {
        for max_metainfo_bytes in [
            MIN_CONFIGURED_MAX_METAINFO_BYTES,
            MAX_CONFIGURED_MAX_METAINFO_BYTES,
        ] {
            assert!(TorrentSourceFetchLimits {
                max_metainfo_bytes,
                ..TorrentSourceFetchLimits::default()
            }
            .validate()
            .is_ok());
        }

        for max_metainfo_bytes in [
            MIN_CONFIGURED_MAX_METAINFO_BYTES - 1,
            MAX_CONFIGURED_MAX_METAINFO_BYTES + 1,
        ] {
            assert!(matches!(
                TorrentSourceFetchLimits {
                    max_metainfo_bytes,
                    ..TorrentSourceFetchLimits::default()
                }
                .validate(),
                Err(TorrentError::InvalidTorrentSourceLimits(_))
            ));
        }
    }

    #[test]
    fn redirect_and_timeout_limits_fail_closed() {
        assert!(TorrentSourceFetchLimits {
            max_redirects: MAX_TORRENT_SOURCE_REDIRECTS,
            ..TorrentSourceFetchLimits::default()
        }
        .validate()
        .is_ok());
        assert!(matches!(
            TorrentSourceFetchLimits {
                max_redirects: MAX_TORRENT_SOURCE_REDIRECTS + 1,
                ..TorrentSourceFetchLimits::default()
            }
            .validate(),
            Err(TorrentError::InvalidTorrentSourceLimits(_))
        ));

        for limits in [
            TorrentSourceFetchLimits {
                connect_timeout: Duration::ZERO,
                ..TorrentSourceFetchLimits::default()
            },
            TorrentSourceFetchLimits {
                total_timeout: Duration::ZERO,
                ..TorrentSourceFetchLimits::default()
            },
            TorrentSourceFetchLimits {
                connect_timeout: Duration::from_secs(2),
                total_timeout: Duration::from_secs(1),
                ..TorrentSourceFetchLimits::default()
            },
        ] {
            assert!(matches!(
                limits.validate(),
                Err(TorrentError::InvalidTorrentSourceLimits(_))
            ));
        }
    }
}
