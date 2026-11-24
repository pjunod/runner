//! Fetch and decode individual articles outside the job pipeline.
//!
//! The engine's pool exists to keep many connections busy on queued jobs; a
//! probe reads a handful of articles once, so it opens its own connection
//! per server on demand, through the same transport and the same yEnc
//! decoder downloads use, and closes them when done.

use std::time::Duration;

use nzbd_nntp::transport::{tls_client_config, NntpConnection, TransportError};
use nzbd_nntp::{codes, Command};
use nzbd_types::{ServerDef, TlsMode};
use nzbd_yenc::{Status, YencDecoder};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// One decoded article.
#[derive(Debug, Clone)]
pub struct Decoded {
    /// Where these bytes sit in the file (yEnc `=ypart begin=`, 0-based).
    pub offset: u64,
    pub data: Vec<u8>,
    /// The file name the poster wrote in `=ybegin name=`; the real name of
    /// an obfuscated file.
    pub name: String,
    /// The whole file's size from `=ybegin size=`.
    pub file_size: u64,
}

/// What fetching one article came to.
#[derive(Debug)]
pub enum Article {
    Decoded(Decoded),
    /// Every server said it no longer has the article (430).
    Missing,
    /// The article arrived and did not decode (bad CRC, broken yEnc).
    Corrupt,
}

/// Fetches articles from the configured servers in tier order, keeping one
/// lazily-opened connection per server.
pub struct Fetcher {
    servers: Vec<ServerDef>,
    conns: Vec<Option<NntpConnection>>,
}

impl Fetcher {
    pub fn new(mut servers: Vec<ServerDef>) -> Self {
        servers.retain(|s| s.active);
        servers.sort_by_key(|s| s.tier);
        let conns = servers.iter().map(|_| None).collect();
        Fetcher { servers, conns }
    }

    pub fn has_servers(&self) -> bool {
        !self.servers.is_empty()
    }

    /// Fetch one article: each server in turn until one serves it.
    pub async fn article(&mut self, message_id: &str) -> Article {
        let mut corrupt = false;
        for i in 0..self.servers.len() {
            match self.try_server(i, message_id).await {
                Ok(Some(d)) => return Article::Decoded(d),
                Ok(None) => {} // missing here; the next tier may have it
                Err(Failure::Corrupt) => corrupt = true,
                Err(Failure::Connection(e)) => {
                    tracing::debug!(server = %self.servers[i].name, error = %e, "probe: connection failed");
                    self.conns[i] = None;
                }
            }
        }
        if corrupt {
            Article::Corrupt
        } else {
            Article::Missing
        }
    }

    async fn connection(&mut self, i: usize) -> Result<&mut NntpConnection, TransportError> {
        if self.conns[i].is_none() {
            let server = &self.servers[i];
            let tls = match server.tls {
                TlsMode::Tls => Some(tls_client_config(server.cert_verification)?),
                TlsMode::None => None,
            };
            let (mut conn, _) =
                NntpConnection::connect(server, tls, CONNECT_TIMEOUT, READ_TIMEOUT).await?;
            if let Some(user) = server.username.as_deref().filter(|u| !u.is_empty()) {
                conn.authenticate(user, server.password.as_deref().unwrap_or(""))
                    .await?;
            }
            self.conns[i] = Some(conn);
        }
        Ok(self.conns[i].as_mut().expect("connection just opened"))
    }

    async fn try_server(&mut self, i: usize, message_id: &str) -> Result<Option<Decoded>, Failure> {
        let conn = self.connection(i).await.map_err(Failure::Connection)?;
        conn.send(&Command::Body(message_id))
            .await
            .map_err(Failure::Connection)?;
        let resp = conn.read_response().await.map_err(Failure::Connection)?;
        if resp.code != codes::BODY_FOLLOWS {
            if resp.is_article_missing() {
                return Ok(None);
            }
            return Err(Failure::Connection(TransportError::Unexpected(
                resp.code, resp.text,
            )));
        }
        let mut decoder = YencDecoder::new();
        let mut out = Vec::with_capacity(768 * 1024);
        loop {
            let chunk = conn.body_chunk().await.map_err(Failure::Connection)?;
            if chunk.is_empty() {
                return Err(Failure::Connection(TransportError::Closed));
            }
            match decoder.push(chunk, &mut out) {
                Ok((Status::NeedMore, consumed)) => conn.consume(consumed),
                Ok((Status::Finished, consumed)) => {
                    conn.consume(consumed);
                    break;
                }
                Err(_) => {
                    // Stay in protocol sync before giving up on the article.
                    let len = chunk.len();
                    conn.consume(len);
                    conn.drain_body().await.map_err(Failure::Connection)?;
                    return Err(Failure::Corrupt);
                }
            }
        }
        let Some(result) = decoder.take_result() else {
            return Err(Failure::Corrupt);
        };
        if result.crc_ok == Some(false) || !result.len_ok {
            return Err(Failure::Corrupt);
        }
        Ok(Some(Decoded {
            offset: result.offset,
            data: out,
            name: result.header.name.clone(),
            file_size: result.header.size,
        }))
    }

    /// Say goodbye to every server we opened.
    pub async fn close(mut self) {
        for conn in self.conns.iter_mut().filter_map(Option::take) {
            let _ = conn.quit().await;
        }
    }
}

enum Failure {
    Connection(TransportError),
    Corrupt,
}
