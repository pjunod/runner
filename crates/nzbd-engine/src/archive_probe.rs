//! Early encrypted-archive verdicts (the download-time half of unpack).
//!
//! An encrypted release that no known password opens ends `failed/unpack`
//! once post-processing reaches it. Every byte downloaded after its first
//! archive volume is waste: the verdict is already decided by that volume.
//! SABnzbd aborts such a job during download (`pause_on_pwrar`); this is
//! the same decision, reached with the same evidence unpack would use.
//!
//! The engine knows when a file is complete and intact; it does not know
//! archive formats, extractor binaries or the password file. Those belong
//! to post-processing, which implements [`ArchiveProbe`] and is handed to
//! the engine as [`crate::EngineConfig::archive_probe`]. The owner asks it
//! about every intact finished file as part of the finished-file
//! inspection (the same scoped, fenced read that learns a job's name from
//! its par2 index), and a [`ArchiveVerdict::Locked`] answer ends the
//! download through the owner's ordinary early-failure path: writers
//! retired, connections released, terminal `failed/unpack` recorded.
use std::path::Path;

use tokio_util::sync::CancellationToken;

/// What the probe knows about the job that owns the file.
#[derive(Debug, Clone, Default)]
pub struct ArchiveProbeJob {
    /// The job's category, for per-category unpack rules.
    pub category: Option<String>,
    /// The release's own passwords (`*Unpack:Password`), tried first.
    pub passwords: Vec<String>,
}

/// The probe's answer for one finished file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveVerdict {
    /// Not an encrypted archive this check can judge: keep asking about
    /// later files.
    Plain,
    /// An encrypted archive that a known password opens, or that this
    /// volume alone cannot judge. Unpack decides; stop asking for this job.
    Opens,
    /// An encrypted archive that every known password was refused by.
    /// Unpack would end the job `failed/unpack` with this message.
    Locked(String),
}

/// Judges a finished file. Called on the blocking pool while the job's
/// terminal-use guard is held, so the file cannot be disposed underneath
/// it; it must return (any failure to judge is [`ArchiveVerdict::Plain`]
/// or [`ArchiveVerdict::Opens`], never a hang) and should stop promptly
/// once `cancel` fires.
pub trait ArchiveProbe: Send + Sync {
    fn probe(
        &self,
        file: &Path,
        job: &ArchiveProbeJob,
        cancel: &CancellationToken,
    ) -> ArchiveVerdict;
}
