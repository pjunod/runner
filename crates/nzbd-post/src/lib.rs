//! Post-processing (ARCHITECTURE.md §9): the NZBGet-equivalent stage graph
//! par-verify (native quick path) → repair (with delayed-par fetch) →
//! unpack (⇆ repair retry) → cleanup → extension scripts.
//!
//! Layout:
//! - [`par2`] — packet parsing + **native quick verification**: we hold
//!   whole-file CRC32s from download, so an intact set is proven with zero
//!   data re-reads. GF(2^16) repair math stays in the `par2` subprocess.
//! - [`tools`] — hardened subprocess runner + `par2`/`unrar`/`7z` wrappers
//!   (argv-only, scrubbed env, timeouts, bounded output, kill-on-drop).
//! - [`script`] — the NZBGet extension-script protocol byte-for-byte
//!   (`NZBPP_*` env, `[NZB] KEY=value` stdout commands, exit codes 92–95).
//! - [`manager`] — the orchestrator: watches engine events, drives each
//!   finished job through the stages, records history, stamps jobs so
//!   restarts never re-process.
//! - [`rename`] — evidence-based obfuscation recovery (par2 16k-hashes,
//!   archive signatures); [`deobfuscate`] — the final heuristic pass
//!   (job-name rename, season-pack numbering) for what evidence missed.

use std::path::PathBuf;

mod attempt;
pub mod deobfuscate;
mod fingerprint;
pub use fingerprint::FileStamp;
pub mod manager;
pub mod namespace;
pub mod par2;
pub mod rename;
pub mod repair_workspace;
pub mod script;
pub mod tools;

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    /// The stage already persisted its authoritative control fact.
    #[error("post-processing held")]
    Held,
    #[error("subprocess quiescence unconfirmed: {0}")]
    Unquiesced(String),
    #[error("tool not found: {0}")]
    ToolMissing(String),
    #[error("subprocess failed: {0}")]
    Subprocess(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyResult {
    /// All files present and matching — repair not needed.
    Intact,
    /// Repair is possible; `blocks_needed` recovery blocks required.
    Repairable {
        blocks_available: u32,
        blocks_needed: u32,
    },
    /// More recovery blocks required than available.
    NeedMoreBlocks {
        blocks_needed: u32,
    },
    Unrepairable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairResult {
    Repaired,
    Failed,
}

/// Known per-file evidence gathered during download, used for quick verify.
#[derive(Debug, Clone)]
pub struct DownloadEvidence {
    pub path: PathBuf,
    /// Combined CRC32 of the fully-downloaded file, if all segments succeeded.
    pub crc32: Option<u32>,
    /// (offset, len, crc) of each successfully downloaded segment, for
    /// partial files.
    pub segment_crcs: Vec<(u64, u32, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    Rar,
    SevenZip,
    Zip,
    Split, // .001/.002 joinable fragments
}

#[derive(Debug, Clone)]
pub struct ExtractOutcome {
    /// Actual output directory; retries use a fresh sibling in the owned workspace.
    pub output_dir: PathBuf,
    pub attempts: Vec<ExtractAttempt>,
    pub quota_error: bool,
    pub success: bool,
    /// Password was wrong / required (drives the password-file retry loop).
    pub password_error: bool,
    /// Out of disk space (unrar exit code 5).
    pub disk_space_error: bool,
}

#[derive(Debug, Clone)]
pub struct ExtractAttempt {
    pub executable: String,
    pub exit_code: i32,
    pub diagnostic: String,
}

/// NZBGet extension-script exit codes (post-processing scripts).
pub mod script_exit {
    pub const PAR_CHECK: i32 = 92;
    pub const SUCCESS: i32 = 93;
    pub const ERROR: i32 = 94;
    pub const NONE: i32 = 95;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptKind {
    PostProcessing, // NZBPP_*
    Scan,           // NZBNP_*
    Queue,          // NZBNA_*
    Scheduler,      // NZBSP_*
    Feed,           // NZBFP_*
    Command,        // NZBCP_*
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptOutcome {
    pub exit_code: i32,
    /// `[NZB] KEY=value` commands emitted by the script.
    pub commands: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_match_nzbget_protocol() {
        assert_eq!(script_exit::PAR_CHECK, 92);
        assert_eq!(script_exit::SUCCESS, 93);
        assert_eq!(script_exit::ERROR, 94);
        assert_eq!(script_exit::NONE, 95);
    }
}
