//! Read the decoded head of a release's main media file without downloading
//! the release (Curator ADR 0031 §6).
//!
//! Bytes, not opinions: this crate picks the file, fetches only the NNTP
//! articles covering its first bytes, decodes the yEnc, and unwraps a
//! store-mode RAR, and that is all. It never parses the media — Curator does
//! that with the same prober it runs on imported files. When the head cannot
//! be read it says why (encrypted, compressed, missing articles), so the
//! caller can fall back to what the release name claims.

pub mod fetch;
pub mod pick;
pub mod rar;

use nzbd_nzb::ParsedFile;
use nzbd_types::ServerDef;
use serde::Serialize;

use fetch::{Article, Fetcher};
use rar::Volume;

/// The most a caller may ask for. A UHD keyframe and its RPU are well inside
/// this; anything larger is a download, not a peek.
pub const MAX_BYTES: usize = 16 << 20;

/// How many candidate files a probe may open looking for the media. An
/// obfuscated RAR set has to be searched for its first volume, one article
/// per volume, so this covers a typical 50-volume remux set whole.
const MAX_CANDIDATES: usize = 64;

/// Total article budget for one probe, across all candidates: the search
/// above plus the head itself (a 16 MiB head is about 22 articles).
const MAX_ARTICLES: usize = 112;

/// How far to read when RAR headers do not fit in the first article.
const RAR_SLACK: usize = 64 << 10;

/// Why a head could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// Password-protected archive.
    Encrypted,
    /// The archive is compressed, so its bytes are not the file's.
    Compressed,
    /// The articles holding the head are gone from every server.
    MissingArticles,
    /// Nothing in the release looks like media.
    NoMedia,
    /// Anything else: a malformed NZB, an archive we could not read.
    Unreadable,
}

/// A decoded head.
#[derive(Debug, Clone, Serialize)]
pub struct Head {
    /// The media file's name (inside the archive when there is one).
    pub file: String,
    /// The media file's full size.
    pub file_size: u64,
    /// How the bytes were reached: "yenc", then "rar-store".
    pub path: Vec<&'static str>,
    /// Articles read, and how many of those were missing or broken.
    pub articles: usize,
    pub articles_missing: usize,
    #[serde(skip)]
    pub bytes: Vec<u8>,
}

/// What a probe came to.
#[derive(Debug, Clone)]
pub enum Outcome {
    Head(Head),
    Unavailable { reason: Reason, detail: String },
}

fn unavailable(reason: Reason, detail: impl Into<String>) -> Outcome {
    Outcome::Unavailable {
        reason,
        detail: detail.into(),
    }
}

/// Probe a release: the NZB document and the servers to read it from.
pub async fn probe(nzb: &[u8], servers: Vec<ServerDef>, max_bytes: usize) -> Outcome {
    let max_bytes = max_bytes.clamp(1, MAX_BYTES);
    let parsed = match nzbd_nzb::parse(nzb) {
        Ok(p) => p,
        Err(e) => return unavailable(Reason::Unreadable, format!("not an NZB: {e}")),
    };
    let order = pick::candidates(&parsed.files);
    if order.is_empty() {
        return unavailable(
            Reason::NoMedia,
            "the release holds only par2 and companion files",
        );
    }
    let mut fetcher = Fetcher::new(servers);
    if !fetcher.has_servers() {
        return unavailable(Reason::Unreadable, "no news servers are configured");
    }
    let outcome = probe_files(&mut fetcher, &parsed.files, &order, max_bytes).await;
    fetcher.close().await;
    outcome
}

async fn probe_files(
    fetcher: &mut Fetcher,
    files: &[ParsedFile],
    order: &[usize],
    max_bytes: usize,
) -> Outcome {
    let mut budget = MAX_ARTICLES;
    let mut skipped_later_volume = false;
    for &idx in order.iter().take(MAX_CANDIDATES) {
        let mut r = Reader::new(&files[idx]);
        // One article first: enough to tell media from archive from junk.
        r.fill(fetcher, 1, &mut budget).await;
        if r.buf.is_empty() {
            if r.unreachable {
                return unavailable(
                    Reason::Unreadable,
                    "no news server answered (connection, login or timeout)",
                );
            }
            if r.missing > 0 && !skipped_later_volume {
                return unavailable(
                    Reason::MissingArticles,
                    format!("the first article of {} is gone", r.label()),
                );
            }
            continue;
        }
        let mut volume = rar::inspect(&r.buf);
        if matches!(volume, Volume::Unreadable(_)) {
            // Archive headers longer than one article: read a little more.
            r.fill(fetcher, RAR_SLACK * 4, &mut budget).await;
            volume = rar::inspect(&r.buf);
        }
        match volume {
            Volume::NotRar => {
                if !looks_like_media(&r.buf) {
                    continue; // an obfuscated par2 or sample index: keep looking
                }
                r.fill(fetcher, max_bytes, &mut budget).await;
                r.buf.truncate(max_bytes);
                let file_size = if r.size > 0 {
                    r.size
                } else {
                    files[idx].encoded_size()
                };
                return Outcome::Head(Head {
                    file: r.label(),
                    file_size,
                    path: vec!["yenc"],
                    articles: r.articles,
                    articles_missing: r.missing,
                    bytes: r.buf,
                });
            }
            Volume::Stored { data_offset, .. } => {
                r.fill(fetcher, data_offset as usize + max_bytes, &mut budget)
                    .await;
                // Re-read the headers from the longer buffer: same answer,
                // now with the data behind it.
                let Volume::Stored {
                    name,
                    unpacked,
                    data_offset,
                    packed,
                } = rar::inspect(&r.buf)
                else {
                    return unavailable(
                        Reason::Unreadable,
                        "the archive headers read differently a second time",
                    );
                };
                let start = data_offset as usize;
                let end = start
                    .saturating_add((packed as usize).min(max_bytes))
                    .min(r.buf.len());
                let bytes = r
                    .buf
                    .get(start..end)
                    .map(<[u8]>::to_vec)
                    .unwrap_or_default();
                if bytes.is_empty() {
                    if r.unreachable {
                        return unavailable(
                            Reason::Unreadable,
                            "no news server answered (connection, login or timeout)",
                        );
                    }
                    return unavailable(
                        Reason::MissingArticles,
                        "the archive's data starts past the articles that remain",
                    );
                }
                if !looks_like_media(&bytes) {
                    return unavailable(
                        Reason::NoMedia,
                        format!("the archive's first file, {name}, does not start like media"),
                    );
                }
                return Outcome::Head(Head {
                    file: name,
                    file_size: unpacked,
                    path: vec!["yenc", "rar-store"],
                    articles: r.articles,
                    articles_missing: r.missing,
                    bytes,
                });
            }
            Volume::NotFirst => {
                // A later volume ahead of the first happens only when names
                // are obfuscated: keep searching the set for its first volume.
                skipped_later_volume = true;
            }
            Volume::Encrypted => {
                return unavailable(
                    Reason::Encrypted,
                    format!("{} is password-protected", r.label()),
                )
            }
            Volume::Compressed => {
                return unavailable(
                    Reason::Compressed,
                    format!("{} is a compressed archive", r.label()),
                )
            }
            Volume::Unreadable(why) => {
                return unavailable(Reason::Unreadable, format!("{}: {why}", r.label()))
            }
        }
    }
    if skipped_later_volume {
        return unavailable(
            Reason::Unreadable,
            "the first volume of the archive set was not found",
        );
    }
    unavailable(
        Reason::NoMedia,
        "no file in the release starts like media or an archive",
    )
}

/// Container signatures the probe will hand back as media.
fn looks_like_media(b: &[u8]) -> bool {
    b.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) // Matroska / WebM (EBML)
        || b.get(4..8) == Some(b"ftyp") // MP4 / MOV
        || (b.first() == Some(&0x47) && b.get(188) == Some(&0x47)) // MPEG-TS
        || b.get(4) == Some(&0x47) && b.get(196) == Some(&0x47) // M2TS (4-byte timecode)
        || b.starts_with(b"RIFF") // AVI
        || b.starts_with(&[0x30, 0x26, 0xb2, 0x75]) // ASF / WMV
}

/// Reads one file's segments in order into a contiguous head.
struct Reader<'a> {
    file: &'a ParsedFile,
    next: usize,
    buf: Vec<u8>,
    articles: usize,
    missing: usize,
    stopped: bool,
    /// Every server failed during this read; the head stops, and a hole
    /// here says nothing about the release.
    unreachable: bool,
    name: String,
    size: u64,
}

impl<'a> Reader<'a> {
    fn new(file: &'a ParsedFile) -> Self {
        Reader {
            file,
            next: 0,
            buf: Vec::new(),
            articles: 0,
            missing: 0,
            stopped: false,
            unreachable: false,
            name: String::new(),
            size: 0,
        }
    }

    /// The best name known: the poster's yEnc name, else the subject's hint.
    fn label(&self) -> String {
        if self.name.is_empty() {
            self.file.filename_hint()
        } else {
            self.name.clone()
        }
    }

    /// Read segments until the head holds `need` bytes, the file ends, a
    /// segment is missing (the head cannot continue past a hole), or the
    /// probe's article budget runs out.
    async fn fill(&mut self, fetcher: &mut Fetcher, need: usize, budget: &mut usize) {
        while self.buf.len() < need
            && !self.stopped
            && self.next < self.file.segments.len()
            && *budget > 0
        {
            let seg = &self.file.segments[self.next];
            self.next += 1;
            *budget -= 1;
            self.articles += 1;
            match fetcher.article(&seg.message_id).await {
                Article::Decoded(d) => {
                    if self.name.is_empty() {
                        self.name = d.name.clone();
                        self.size = d.file_size;
                    }
                    let at = d.offset as usize;
                    if at > self.buf.len() {
                        // A gap: the head ends where the bytes stop being contiguous.
                        self.stopped = true;
                        continue;
                    }
                    let skip = self.buf.len() - at;
                    if skip < d.data.len() {
                        self.buf.extend_from_slice(&d.data[skip..]);
                    }
                }
                Article::Missing | Article::Corrupt => {
                    self.missing += 1;
                    self.stopped = true;
                }
                Article::Unreachable => {
                    self.articles -= 1;
                    self.unreachable = true;
                    self.stopped = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
