//! A set-scoped repair session keeps fingerprints and private tool inputs stable.
use crate::{
    fingerprint::{self, FileStamp, Fingerprint},
    par2::Par2Set,
    tools::Par2Tool,
    PostError, RepairResult, VerifyResult,
};
use nzbd_state::artifacts::{Inventory, Workspace};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

pub(crate) struct RepairSession {
    inventory: Arc<Inventory>,
    workspace: Workspace,
    pub set: Par2Set,
    partials: Vec<(PathBuf, String)>,
    fingerprints: HashMap<PathBuf, Fingerprint>,
    metadata_bytes: usize,
    directory: Option<PathBuf>,
    prefix: String,
    prepared: HashMap<PathBuf, (FileStamp, PathBuf, FileStamp, [u8; 16])>,
    main_stamp: Option<FileStamp>,
    ordinals: HashMap<PathBuf, usize>,
    mappings: Vec<PathBuf>,
    recovered: Vec<PathBuf>,
    pub matching_bytes: u64,
}
fn state_error(error: impl std::fmt::Display) -> PostError {
    PostError::Subprocess(error.to_string())
}
impl RepairSession {
    pub fn new(
        inventory: Arc<Inventory>,
        job: u32,
        set: Par2Set,
        partials: Vec<(PathBuf, String)>,
    ) -> Result<Self, PostError> {
        let token = set
            .set_id
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let workspace = inventory
            .workspace(job, "par_repair", &token)
            .map_err(state_error)?;
        // Reserve an entire first path component, including nested catalog names.
        if set.slice_size == 0 {
            return Err(PostError::Subprocess("PAR slice size is zero".into()));
        }
        let names = set
            .files
            .iter()
            .map(|file| crate::namespace::relative(&file.name))
            .collect::<Result<Vec<_>, _>>()?;
        let mut prefix = String::new();
        for ordinal in 0..100_001 {
            let candidate = format!("nzbd-par-{token}-{ordinal}");
            if !names.iter().any(|p| {
                p.components()
                    .next()
                    .is_some_and(|c| c.as_os_str().to_string_lossy().starts_with(&candidate))
            }) {
                prefix = candidate;
                break;
            }
        }
        if prefix.is_empty() {
            return Err(PostError::Subprocess("PAR tool namespace exhausted".into()));
        }
        Ok(Self {
            inventory,
            workspace,
            set,
            partials,
            fingerprints: HashMap::new(),
            metadata_bytes: 0,
            directory: None,
            prefix,
            main_stamp: None,
            prepared: HashMap::new(),
            ordinals: HashMap::new(),
            mappings: Vec::new(),
            recovered: Vec::new(),
            matching_bytes: 0,
        })
    }
    /// Unchanged candidates require only descriptor metadata, never another payload scan.
    pub fn sources_changed(&self) -> Result<bool, PostError> {
        self.inventory
            .validate_workspace_source(&self.workspace)
            .map_err(state_error)?;
        for (path, fingerprint) in &self.fingerprints {
            if fingerprint::stamp(path)? != fingerprint.stamp {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn candidates(&self) -> Result<Vec<PathBuf>, PostError> {
        let mut candidates = crate::namespace::files(&self.set.root)?;
        candidates.extend(
            self.partials
                .iter()
                .filter(|(p, _)| p.parent() == Some(self.set.root.as_path()) && p.exists())
                .map(|(p, _)| p.clone()),
        );
        candidates.sort();
        candidates.dedup();
        candidates.retain(|candidate| {
            candidate
                .strip_prefix(&self.workspace.source.path)
                .is_ok_and(|relative| {
                    self.workspace
                        .source
                        .files
                        .iter()
                        .any(|entry| entry.path == relative.to_string_lossy())
                })
                && !self.set.par_paths.contains(candidate)
                && !candidate
                    .extension()
                    .is_some_and(|e| e.eq_ignore_ascii_case("par2"))
                && (!candidate.extension().is_some_and(|e| e == "part")
                    || self.partials.iter().any(|(p, _)| p == candidate))
        });
        Ok(candidates)
    }
    pub fn match_candidates(&mut self) -> Result<(), PostError> {
        crate::attempt::checkpoint()?;
        self.inventory
            .validate_workspace_source(&self.workspace)
            .map_err(state_error)?;
        let candidates = self.candidates()?;
        let lengths: HashSet<_> = self.set.files.iter().map(|file| file.length).collect();
        if let Some(control) = crate::attempt::current() {
            control.matching_total(
                candidates
                    .iter()
                    .filter(|p| fingerprint::stamp(p).is_ok_and(|s| lengths.contains(&s.length)))
                    .count(),
            );
        }
        for candidate in &candidates {
            let stamp = fingerprint::stamp(candidate)?;
            if !lengths.contains(&stamp.length) {
                continue;
            }
            if self
                .fingerprints
                .get(candidate)
                .is_some_and(|f| f.stamp == stamp)
            {
                continue;
            }
            let cost = stamp
                .length
                .div_ceil(self.set.slice_size)
                .saturating_mul(4)
                .saturating_add(candidate.as_os_str().len() as u64 + 128);
            let old = self
                .fingerprints
                .get(candidate)
                .map_or(0, |f| f.crcs.len() * 4 + candidate.as_os_str().len() + 128);
            let next = (self.metadata_bytes.saturating_sub(old) as u64).saturating_add(cost);
            if next > 64 * 1024 * 1024 {
                return Err(PostError::Subprocess(
                    "PAR session fingerprint metadata exceeds 64 MiB; split the recovery set"
                        .into(),
                ));
            }
            let fingerprint = fingerprint::scan(candidate, self.set.slice_size)?;
            self.matching_bytes = self.matching_bytes.saturating_add(fingerprint.stamp.length);
            self.metadata_bytes = next as usize;
            self.fingerprints.insert(candidate.clone(), fingerprint);
            if let Some(control) = crate::attempt::current() {
                control.file_done();
            }
        }
        Ok(())
    }
    pub fn prepare(&mut self, authorized_recovery: &HashSet<PathBuf>) -> Result<(), PostError> {
        self.match_candidates()?;
        let candidates = self.candidates()?;
        // A changed prepared input is never reused. Its prior generation remains
        // journal-owned for whole-job retirement; allocate a fresh child instead.
        let stale = self
            .prepared
            .iter()
            .any(|(source, (stamp, target, target_stamp, _))| {
                !fingerprint::stamp(source).is_ok_and(|current| &current == stamp)
                    || !fingerprint::stamp(target).is_ok_and(|current| &current == target_stamp)
            })
            || self.main_stamp.as_ref().is_some_and(|stamp| {
                !fingerprint::stamp(&self.main()).is_ok_and(|current| &current == stamp)
            });
        if self.directory.is_none() || stale {
            self.directory = Some(
                self.inventory
                    .begin_workspace_attempt(&mut self.workspace)
                    .map_err(state_error)?,
            );
            self.prepared.clear();
            self.main_stamp = None;
        }
        let root = self.directory.as_ref().unwrap().clone();
        let _capacity =
            nzbd_state::capacity::reserve(&root, self.set.files.iter().map(|f| f.length).sum())?;
        self.mappings.clear();
        self.recovered.clear();
        let mut by_length: HashMap<u64, Vec<PathBuf>> = HashMap::new();
        let mut by_digest: HashMap<(u64, [u8; 16]), Vec<PathBuf>> = HashMap::new();
        for candidate in &candidates {
            if let Some(fingerprint) = self.fingerprints.get(candidate) {
                by_length
                    .entry(fingerprint.stamp.length)
                    .or_default()
                    .push(candidate.clone());
                by_digest
                    .entry((fingerprint.stamp.length, fingerprint.md5))
                    .or_default()
                    .push(candidate.clone());
            }
        }
        for file in &self.set.files {
            crate::attempt::checkpoint()?;
            let relative = crate::namespace::relative(&file.name)?;
            let target = root.join(&relative);
            let pool = by_digest
                .get(&(file.length, file.md5_full))
                .or_else(|| by_length.get(&file.length));
            let mut ranked: Vec<_> = pool
                .into_iter()
                .flatten()
                .filter_map(|candidate| {
                    let fingerprint = self.fingerprints.get(candidate)?;
                    if fingerprint.stamp.length != file.length {
                        return None;
                    }
                    let score = if fingerprint.md5 == file.md5_full {
                        usize::MAX
                    } else {
                        fingerprint
                            .crcs
                            .iter()
                            .zip(&file.slice_crcs)
                            .filter(|(a, b)| a == b)
                            .count()
                    };
                    (score > 0).then_some((score, candidate.clone()))
                })
                .collect();
            ranked.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
            let Some((score, source)) = ranked.first() else {
                self.recovered.extend(
                    self.partials
                        .iter()
                        .filter(|(_, name)| name == &file.name)
                        .map(|(p, _)| p.clone()),
                );
                continue;
            };
            if ranked.get(1).is_some_and(|(next, _)| next == score) {
                return Err(PostError::Subprocess(
                    "ambiguous PAR candidate mapping; review required".into(),
                ));
            }
            let fingerprint = &self.fingerprints[source];
            if self.partials.iter().any(|(p, _)| p == source) {
                self.recovered.push(source.clone());
            } else {
                self.mappings.push(source.clone());
            }
            if !self.prepared.contains_key(source) || !target.exists() {
                if let Some(parent) = relative.parent() {
                    nzbd_state::fileops::parents(&root, parent).map_err(state_error)?;
                }
                copy_checked(source, &target)?;
                if fingerprint::stamp(source)? != fingerprint.stamp
                    || crate::rename::full_md5(&target) != Some(fingerprint.md5)
                {
                    return Err(PostError::Subprocess(
                        "prepared PAR input changed during copying".into(),
                    ));
                }
                self.prepared.insert(
                    source.clone(),
                    (
                        fingerprint.stamp.clone(),
                        target.clone(),
                        fingerprint::stamp(&target)?,
                        fingerprint.md5,
                    ),
                );
            }
            if let Some(control) = crate::attempt::current() {
                control.file_done();
            }
        }
        for path in &self.set.par_paths {
            let captured = path
                .strip_prefix(&self.workspace.source.path)
                .is_ok_and(|relative| {
                    self.workspace
                        .source
                        .files
                        .iter()
                        .any(|entry| entry.path == relative.to_string_lossy())
                });
            if !captured && !authorized_recovery.contains(path) {
                return Err(PostError::Subprocess(
                    "PAR recovery input is outside engine custody".into(),
                ));
            }
            if self.prepared.contains_key(path) {
                if let Some(control) = crate::attempt::current() {
                    control.file_done();
                }
                continue;
            }
            let next = self.ordinals.len();
            let ordinal = *self.ordinals.entry(path.clone()).or_insert(next);
            let target = if self.set.main_path.as_ref() == Some(path) {
                root.join(format!("{}.par2", self.prefix))
            } else {
                root.join(format!("{}.vol{ordinal:05}+00001.par2", self.prefix))
            };
            let before = fingerprint::stamp(path)?;
            if !self
                .set
                .input_stamps
                .get(path)
                .is_some_and(|stamp| stamp == &before)
            {
                return Err(PostError::Subprocess(
                    "PAR recovery input changed after packet validation".into(),
                ));
            }
            copy_checked(path, &target)?;
            let digest = crate::rename::full_md5(&target)
                .ok_or_else(|| PostError::Subprocess("PAR companion digest unavailable".into()))?;
            if before != fingerprint::stamp(path)? || crate::rename::full_md5(path) != Some(digest)
            {
                return Err(PostError::Subprocess(
                    "PAR recovery identity changed during copying".into(),
                ));
            }
            if let Some(control) = crate::attempt::current() {
                control.file_done();
            }
            self.prepared.insert(
                path.clone(),
                (before, target.clone(), fingerprint::stamp(&target)?, digest),
            );
        }
        let main = root.join(format!("{}.par2", self.prefix));
        if !main.exists() {
            return Err(PostError::Subprocess(
                "PAR main index missing from validated companions".into(),
            ));
        }
        self.main_stamp = Some(fingerprint::stamp(&main)?);
        if let Some(control) = crate::attempt::current() {
            control.file_done();
        }
        Ok(())
    }
    pub fn main(&self) -> PathBuf {
        self.directory
            .as_ref()
            .unwrap()
            .join(format!("{}.par2", self.prefix))
    }
    pub fn validate_publish(&mut self) -> Result<Vec<PathBuf>, PostError> {
        self.inventory
            .validate_workspace_source(&self.workspace)
            .map_err(state_error)?;
        let root = self.directory.as_ref().unwrap();
        // Validate every result before publishing any of them.
        let mut outputs = HashMap::new();
        for file in &self.set.files {
            crate::attempt::checkpoint()?;
            let output = root.join(crate::namespace::relative(&file.name)?);
            let before = fingerprint::stamp(&output)?;
            if before.length != file.length
                || crate::rename::full_md5(&output) != Some(file.md5_full)
                || fingerprint::stamp(&output)? != before
            {
                return Err(PostError::Subprocess(
                    "repaired PAR output identity mismatch".into(),
                ));
            }
            outputs.insert(output, before);
            if let Some(control) = crate::attempt::current() {
                control.file_done();
            }
        }
        for source in self.mappings.clone() {
            crate::attempt::checkpoint()?;
            if self.fingerprints.get(&source).is_none_or(|f| {
                !fingerprint::stamp(&source).is_ok_and(|current| current == f.stamp)
            }) {
                return Err(PostError::Subprocess(
                    "original PAR candidate identity changed before publication".into(),
                ));
            }
            let relative = source
                .strip_prefix(&self.workspace.source.path)
                .map_err(std::io::Error::other)?;
            // Keep an intact canonical original in place; preserve damaged and
            // obfuscated originals through the existing transform journal.
            let canonical_intact = self.set.files.iter().any(|f| {
                relative == std::path::Path::new(&f.name)
                    && self.fingerprints[&source].md5 == f.md5_full
            });
            if !canonical_intact {
                self.inventory
                    .retain_transform_original(&mut self.workspace, &relative.to_string_lossy())
                    .map_err(state_error)?;
            }
        }
        for file in &self.set.files {
            crate::attempt::checkpoint()?;
            self.inventory
                .validate_workspace_source(&self.workspace)
                .map_err(state_error)?;
            let relative = crate::namespace::relative(&file.name)?;
            let target = self.set.root.join(&relative);
            if target.exists() && crate::rename::full_md5(&target) == Some(file.md5_full) {
                continue;
            }
            if target.exists() {
                self.inventory
                    .retain_transform_original(&mut self.workspace, &relative.to_string_lossy())
                    .map_err(state_error)?;
            }
            if let Some(parent) = relative.parent() {
                nzbd_state::fileops::parents(&self.set.root, parent).map_err(state_error)?;
            }
            let output = root.join(relative);
            let expected = &outputs[&output];
            nzbd_state::fileops::copy_publish_checked(&output, &target, &|| {
                crate::attempt::checkpoint()?;
                if !fingerprint::stamp(&output).is_ok_and(|stamp| &stamp == expected) {
                    return Err(std::io::Error::other(
                        "validated PAR output changed before publication",
                    ));
                }
                Ok(())
            })
            .map_err(state_error)?;
        }
        self.inventory
            .finish_workspace(&self.workspace)
            .map_err(state_error)?;
        Ok(self.recovered.clone())
    }
    pub fn abandon(&self) -> Result<(), PostError> {
        self.inventory
            .abandon_repair_workspace(&self.workspace)
            .map_err(state_error)
    }
}
fn copy_checked(source: &std::path::Path, target: &std::path::Path) -> Result<(), PostError> {
    nzbd_state::fileops::copy_publish_checked(source, target, &crate::attempt::checkpoint)
        .map_err(state_error)
}

/// Standalone entry point retained for callers outside the manager.
pub async fn repair(
    inventory: &Arc<Inventory>,
    job: u32,
    set: &Par2Set,
    tool: &Par2Tool,
    partials: &[(PathBuf, String)],
    restored: &mut HashSet<PathBuf>,
) -> Result<VerifyResult, PostError> {
    let inventory = inventory.clone();
    let set = set.clone();
    let partials = partials.to_vec();
    let mut session = tokio::task::spawn_blocking(move || {
        let mut session = RepairSession::new(inventory, job, set, partials)?;
        session.prepare(&HashSet::new())?;
        Ok::<_, PostError>(session)
    })
    .await
    .map_err(|e| PostError::Subprocess(e.to_string()))??;
    let verified = tool.verify_full(&session.main()).await?;
    match verified {
        VerifyResult::Intact => {}
        VerifyResult::Repairable { .. }
            if tool.repair(&session.main()).await? == RepairResult::Repaired => {}
        VerifyResult::Repairable { .. } => return Ok(VerifyResult::Unrepairable),
        other => return Ok(other),
    }
    let recovered = tokio::task::spawn_blocking(move || session.validate_publish())
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))??;
    restored.extend(recovered);
    Ok(VerifyResult::Intact)
}

#[cfg(test)]
mod tests {
    use super::*;
    use md5::{Digest, Md5};
    fn fixture(names: &[&str]) -> (tempfile::TempDir, Arc<Inventory>, Par2Set, Vec<u8>) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
        let dir = root.join("job");
        inventory.allocate(4, &root, &dir).unwrap();
        let bytes: Vec<_> = (0..50000).map(|n| ((n * 7) % 251) as u8).collect();
        for name in names {
            std::fs::write(dir.join(name), &bytes).unwrap();
        }
        let crcs: Vec<_> = bytes
            .chunks(8192)
            .map(|chunk| {
                let mut padded = vec![0; 8192];
                padded[..chunk.len()].copy_from_slice(chunk);
                crc32fast::hash(&padded)
            })
            .collect();
        let set = Par2Set {
            root: dir,
            slice_size: 8192,
            files: vec![crate::par2::Par2File {
                name: "-file with spaces.bin".into(),
                length: bytes.len() as u64,
                md5_full: Md5::digest(&bytes).into(),
                id: [1; 16],
                md5_16k: Md5::digest(&bytes[..16384]).into(),
                slice_crcs: crcs,
            }],
            ..Default::default()
        };
        (temp, inventory, set, bytes)
    }
    #[test]
    fn unchanged_round_reuses_fingerprints_and_ambiguous_evidence_stays_held() {
        let (_temp, inventory, set, bytes) = fixture(&["candidate-a", "unrelated"]);
        std::fs::write(set.root.join("unrelated"), vec![0; bytes.len()]).unwrap();
        let mut session = RepairSession::new(inventory, 4, set, vec![]).unwrap();
        session.match_candidates().unwrap();
        assert_eq!(session.matching_bytes, (bytes.len() * 2) as u64);
        session.match_candidates().unwrap();
        assert_eq!(session.matching_bytes, (bytes.len() * 2) as u64);
        assert!(!session.sources_changed().unwrap());
        std::fs::write(session.set.root.join("unrelated"), &bytes).unwrap();
        assert!(session.sources_changed().unwrap());
        assert!(session
            .prepare(&HashSet::new())
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        assert!(session.set.root.join("candidate-a").exists());
    }
    #[tokio::test]
    async fn extensionless_recovery_and_damaged_first_slice_repair_with_safe_tool_names() {
        let available = std::process::Command::new("par2")
            .arg("-V")
            .output()
            .is_ok();
        if !available {
            assert!(std::env::var_os("NZBD_REQUIRE_TOOLS").is_none());
            return;
        }
        let (_temp, inventory, mut set, bytes) = fixture(&["-file with spaces.bin"]);
        let status = std::process::Command::new("par2")
            .current_dir(&set.root)
            .args([
                "create",
                "-q",
                "-s8192",
                "-c8",
                "safe.par2",
                "./-file with spaces.bin",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let volumes: Vec<_> = std::fs::read_dir(&set.root)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "par2"))
            .collect();
        for (ordinal, path) in volumes.iter().enumerate() {
            std::fs::rename(
                path,
                set.root.join(format!("-obfuscated recovery {ordinal}")),
            )
            .unwrap();
        }
        let payload = set.root.join("-file with spaces.bin");
        let obfuscated = set.root.join("opaque-payload");
        std::fs::rename(&payload, &obfuscated).unwrap();
        let mut damaged = bytes.clone();
        damaged[..8192].fill(0);
        std::fs::write(&obfuscated, &damaged).unwrap();
        set = crate::par2::load_dir(&set.root).unwrap().unwrap();
        let tool = Par2Tool {
            cmd: "par2".into(),
            timeout: std::time::Duration::from_secs(30),
        };
        let mut restored = HashSet::new();
        assert_eq!(
            repair(&inventory, 4, &set, &tool, &[], &mut restored)
                .await
                .unwrap(),
            VerifyResult::Intact
        );
        assert_eq!(std::fs::read(&payload).unwrap(), bytes);
        assert!(
            set.par_paths.iter().all(|p| p.exists()),
            "sources keep their original PAR names"
        );
        assert!(std::fs::read_dir(&set.root).unwrap().flatten().any(|e| e
            .file_name()
            .to_string_lossy()
            .starts_with(".runner-original-")));
    }
    #[test]
    fn unrecorded_partial_and_unsafe_catalog_never_enter_workspace() {
        let (_temp, inventory, mut set, bytes) = fixture(&["original"]);
        let unknown = set.root.join(".runner-file-999.part");
        std::fs::write(&unknown, bytes).unwrap();
        let session = RepairSession::new(inventory.clone(), 4, set.clone(), vec![]).unwrap();
        assert!(!session.candidates().unwrap().contains(&unknown));
        set.files[0].name = "../escape".into();
        assert!(RepairSession::new(inventory, 4, set, vec![]).is_err());
    }
}
