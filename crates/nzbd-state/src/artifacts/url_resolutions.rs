//! Immutable, checksummed URL inputs and resolution outbox in shared state.
use super::{Error, Result};
use nzbd_types::Job;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const MAX_SOURCE: usize = 64 * 1024 * 1024;
const MAX_RECORD: usize = 16 * 1024 * 1024;
const MAX_OUTBOX_PAGE: usize = 32 * 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UrlResolutionRecord {
    pub resolution_id: String,
    pub source_identity: String,
    pub source_sha256: Option<String>,
    pub origin: Job,
    pub resolved: Job,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UrlResolutionFault {
    pub resolution_id: String,
    pub error: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UrlStorageRetry {
    pub attempts: u32,
    pub next_retry_at: i64,
    pub blocked_since: i64,
    pub error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UrlFileReservation {
    pub first_file_id: u32,
    pub count: u32,
    pub maximum_file_id: u32,
}

#[derive(Serialize, Deserialize)]
struct CacheTemporary {
    id: String,
    suffix: String,
    name: String,
    identity: super::Identity,
}
#[cfg(test)]
thread_local! { static CRASH_BEFORE_RENAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub struct UrlResolutionStore {
    root: PathBuf,
    maintenance_cursor: std::sync::Mutex<String>,
    cleanup_cursors: std::sync::Mutex<std::collections::HashMap<&'static str, String>>,
    pending_cursor: std::sync::Mutex<String>,
    fault_cursor: std::sync::Mutex<String>,
}

pub fn url_source_identity(job: &Job) -> Result<String> {
    let param = |name: &str| {
        job.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    };
    let url = param("*URL").ok_or_else(|| Error::Conflict("URL admission has no source".into()))?;
    let identity = serde_json::to_vec(&(
        job.id.0,
        job.queued_at_unix,
        url,
        param("monarr-transfer"),
        param("*URL:AdmissionGeneration"),
    ))?;
    Ok(format!("{:x}", Sha256::digest(identity)))
}

pub fn url_storage_retry_deadline(
    id: &str,
    attempts: u32,
    now: i64,
    previous: i64,
    not_before: Option<i64>,
) -> i64 {
    let base = [30, 60, 120, 300][attempts.min(3) as usize];
    let seed = u64::from_str_radix(id.get(..16).unwrap_or("0"), 16).unwrap_or(0)
        ^ now as u64
        ^ attempts as u64;
    let jitter = (seed % (base as u64 / 10 + 1)) as i64;
    now.saturating_add(base + jitter)
        .max(previous)
        .max(not_before.unwrap_or(0))
}

impl UrlResolutionStore {
    pub fn open(state_dir: &Path) -> Result<Self> {
        let root = state_dir.join("url-resolutions");
        crate::fsx::create_dir_all_durable(&root)
            .map_err(|error| Error::Conflict(error.to_string()))?;
        super::fs::open_dir(&root)?;
        for directory in [
            "sources",
            "records",
            "acknowledged",
            "cancelled",
            "reservations",
            "highwater",
            "faults",
            "retries",
            "quarantine",
            "retirements",
            "temporaries",
        ] {
            crate::fsx::create_dir_all_durable(&root.join(directory))
                .map_err(|error| Error::Conflict(error.to_string()))?;
        }
        Ok(Self {
            root,
            maintenance_cursor: std::sync::Mutex::new(String::new()),
            cleanup_cursors: std::sync::Mutex::new(std::collections::HashMap::new()),
            pending_cursor: std::sync::Mutex::new(String::new()),
            fault_cursor: std::sync::Mutex::new(String::new()),
        })
    }
    fn path(&self, id: &str, suffix: &str) -> Result<PathBuf> {
        if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::Conflict("invalid URL resolution identity".into()));
        }
        let directory = match suffix {
            "source" => "sources",
            "record" => "records",
            "ack" => "acknowledged",
            "cancelled" => "cancelled",
            "reservation" => "reservations",
            "fault" => "faults",
            "retry" => "retries",
            "quarantine" => "quarantine",
            "retirement" => "retirements",
            other if other.starts_with("highwater-") => "highwater",
            _ => return Err(Error::Conflict("invalid URL store record kind".into())),
        };
        Ok(self.root.join(directory).join(if directory == "highwater" {
            format!("{id}.{suffix}")
        } else {
            id.to_owned()
        }))
    }
    fn read(&self, path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
        let file = match crate::fileops::open(path) {
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None)
            }
            other => other?,
        };
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(Error::Conflict(
                "URL resolution exceeds supported durable size; evidence retained".into(),
            ));
        }
        Ok(Some(bytes))
    }
    fn publish(&self, id: &str, suffix: &str, bytes: &[u8]) -> Result<()> {
        self.write_tracked(id, suffix, bytes, false)
    }
    fn write_tracked(&self, id: &str, suffix: &str, bytes: &[u8], replace: bool) -> Result<()> {
        let target = self.path(id, suffix)?;
        let name = format!(
            ".tmp-{}-{}-{}",
            std::process::id(),
            super::now_millis(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let temporary = target.parent().unwrap().join(&name);
        let receipt = self.root.join("temporaries").join(format!("{name}.json"));
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        output.try_lock().map_err(|error| {
            Error::Conflict(format!("URL cache writer lock unavailable: {error}"))
        })?;
        let metadata = CacheTemporary {
            id: id.into(),
            suffix: suffix.into(),
            name,
            identity: super::fs::identity_for_file(&output)?,
        };
        // Register exact inode ownership before any potentially large write.
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&receipt)?;
        marker.write_all(&serde_json::to_vec(&metadata)?)?;
        marker.sync_all()?;
        super::fs::sync_directory(&super::fs::open_dir(receipt.parent().unwrap())?)?;
        let result = (|| {
            output.write_all(bytes)?;
            output.sync_all()?;
            #[cfg(test)]
            if CRASH_BEFORE_RENAME.with(|crash| crash.replace(false)) {
                return Err(Error::Conflict(
                    "simulated interrupted URL cache writer".into(),
                ));
            }
            if replace {
                std::fs::rename(&temporary, &target)?;
            } else {
                match crate::fileops::rename_exclusive(&temporary, &target) {
                    Ok(()) => {}
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            super::fs::sync_directory(&super::fs::open_dir(target.parent().unwrap())?)?;
            Ok(())
        })();
        let interrupted = result
            .as_ref()
            .err()
            .is_some_and(|error| error.to_string() == "simulated interrupted URL cache writer");
        drop(output);
        if !interrupted {
            let _ = std::fs::remove_file(&temporary);
            let _ = std::fs::remove_file(&receipt);
            let _ = super::fs::sync_directory(&super::fs::open_dir(receipt.parent().unwrap())?);
        }
        result
    }
    fn retirement_id(job: &Job) -> Result<String> {
        let param = |key: &str| {
            job.params
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value)
        };
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                job.id,
                job.kind,
                job.queued_at_unix,
                param("monarr-transfer"),
                param("*URL:AdmissionGeneration")
            ))?)
        ))
    }
    pub fn retire_job(&self, job: &Job) -> Result<()> {
        let identity = Self::retirement_id(job)?;
        let url_id = job
            .params
            .iter()
            .any(|(key, _)| key == "*URL")
            .then(|| url_source_identity(job))
            .transpose()?;
        self.publish(
            &identity,
            "retirement",
            &serde_json::to_vec(&(job.id, job.queued_at_unix, super::now(), &url_id))?,
        )?;
        if let Some(id) = url_id {
            if let Err(error) = self.publish(&id, "cancelled", b"authoritative admission retired\n")
            {
                tracing::warn!(resolution=%id, %error, "durable retirement will reconcile URL residue");
            }
        }
        Ok(())
    }
    pub fn is_retired(&self, job: &Job) -> Result<bool> {
        self.path(&Self::retirement_id(job)?, "retirement")?
            .try_exists()
            .map_err(Into::into)
    }
    pub fn source(&self, origin: &Job) -> Result<Option<Vec<u8>>> {
        let id = url_source_identity(origin)?;
        if self.is_retired(origin)?
            || self.path(&id, "cancelled")?.try_exists()?
            || self.path(&id, "ack")?.try_exists()?
        {
            return Err(Error::NotFound);
        }
        let Some(raw) = self.read(&self.path(&id, "source")?, MAX_SOURCE + 77)? else {
            return Ok(None);
        };
        if raw.len() < 77 || &raw[..12] != b"RUNNER-NZB1\n" || raw[76] != b'\n' {
            return Err(Error::Conflict(
                "URL source checksum header is invalid; source retained".into(),
            ));
        }
        let expected = std::str::from_utf8(&raw[12..76])
            .map_err(|_| Error::Conflict("invalid URL source checksum".into()))?;
        if format!("{:x}", Sha256::digest(&raw[77..])) != expected {
            return Err(Error::Conflict(
                "durable URL source checksum changed; source retained".into(),
            ));
        }
        Ok(Some(raw[77..].to_vec()))
    }
    pub fn cache_source(&self, origin: &Job, bytes: &[u8]) -> Result<Vec<u8>> {
        if bytes.len() > MAX_SOURCE {
            return Err(Error::Conflict(
                "fetched NZB exceeds supported durable size".into(),
            ));
        }
        let id = url_source_identity(origin)?;
        if self.is_retired(origin)?
            || self.path(&id, "cancelled")?.try_exists()?
            || self.path(&id, "ack")?.try_exists()?
        {
            return Err(Error::NotFound);
        }
        if let Some(cached) = self.source(origin)? {
            return Ok(cached);
        }
        let mut framed = format!("RUNNER-NZB1\n{:x}\n", Sha256::digest(bytes)).into_bytes();
        framed.extend_from_slice(bytes);
        self.publish(&id, "source", &framed)?;
        if self.is_retired(origin)?
            || self.path(&id, "cancelled")?.try_exists()?
            || self.path(&id, "ack")?.try_exists()?
        {
            self.compact_settled(&id)?;
            return Err(Error::NotFound);
        }
        self.source(origin)?.ok_or(Error::NotFound)
    }
    pub fn stage(&self, origin: &Job, resolved: &Job) -> Result<UrlResolutionRecord> {
        let id = url_source_identity(origin)?;
        if origin.id != resolved.id
            || origin.status != nzbd_types::JobStatus::Fetching
            || resolved.status == nzbd_types::JobStatus::Fetching
            || self.is_retired(origin)?
            || self.path(&id, "cancelled")?.try_exists()?
        {
            return Err(Error::Conflict(
                "URL resolution no longer owns its admission".into(),
            ));
        }
        if self.path(&id, "ack")?.try_exists()? {
            return Err(Error::NotFound);
        }
        let source_sha256 = self
            .source(origin)?
            .map(|bytes| format!("{:x}", Sha256::digest(bytes)));
        let record = UrlResolutionRecord {
            resolution_id: id.clone(),
            source_identity: id.clone(),
            source_sha256,
            origin: origin.clone(),
            resolved: resolved.clone(),
            created_at: super::now(),
        };
        let bytes = serde_json::to_vec(&record)?;
        if bytes.len() > MAX_RECORD {
            return Err(Error::Conflict(
                "resolved NZB descriptor exceeds supported durable size; source retained".into(),
            ));
        }
        let mut framed = format!("RUNNER-URL1\n{:x}\n", Sha256::digest(&bytes)).into_bytes();
        framed.extend_from_slice(&bytes);
        self.publish(&id, "record", &framed)?;
        if self.is_retired(origin)?
            || self.path(&id, "cancelled")?.try_exists()?
            || self.path(&id, "ack")?.try_exists()?
        {
            self.compact_settled(&id)?;
            return Err(Error::NotFound);
        }
        self.record(&id)?.ok_or(Error::NotFound)
    }
    pub fn record(&self, id: &str) -> Result<Option<UrlResolutionRecord>> {
        let Some(bytes) = self.read(&self.path(id, "record")?, MAX_RECORD + 77)? else {
            return Ok(None);
        };
        if bytes.len() < 77 || &bytes[..12] != b"RUNNER-URL1\n" || bytes[76] != b'\n' {
            return Err(Error::Conflict(
                "URL descriptor checksum header is invalid".into(),
            ));
        }
        let expected = std::str::from_utf8(&bytes[12..76])
            .map_err(|_| Error::Conflict("invalid URL descriptor checksum".into()))?;
        if format!("{:x}", Sha256::digest(&bytes[77..])) != expected {
            return Err(Error::Conflict(
                "URL descriptor checksum changed; original evidence retained".into(),
            ));
        }
        let record: UrlResolutionRecord = serde_json::from_slice(&bytes[77..])?;
        if record.resolution_id != id
            || record.source_identity != id
            || url_source_identity(&record.origin)? != id
            || record.origin.id != record.resolved.id
        {
            return Err(Error::Conflict(
                "URL resolution record identity changed".into(),
            ));
        }
        Ok(Some(record))
    }

    pub fn reservation(&self, origin: &Job) -> Result<Option<UrlFileReservation>> {
        self.read(
            &self.path(&url_source_identity(origin)?, "reservation")?,
            4096,
        )?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
        .transpose()
    }
    pub fn reserve_files(
        &self,
        origin: &Job,
        first_file_id: u32,
        count: u32,
    ) -> Result<UrlFileReservation> {
        let maximum_file_id = first_file_id
            .checked_add(count.saturating_sub(1))
            .ok_or_else(|| Error::Conflict("URL descriptor file IDs exhausted".into()))?;
        let reservation = UrlFileReservation {
            first_file_id,
            count,
            maximum_file_id,
        };
        let id = url_source_identity(origin)?;
        self.publish(&id, "reservation", &serde_json::to_vec(&reservation)?)?;
        let saved = self.reservation(origin)?.ok_or(Error::NotFound)?;
        if saved.count != count {
            return Err(Error::Conflict("URL descriptor file count changed".into()));
        }
        self.publish(
            &id,
            &format!("highwater-{}", saved.maximum_file_id),
            b"durable file ID reservation\n",
        )?;
        Ok(saved)
    }
    pub fn pending(&self, limit: u32) -> Result<Vec<UrlResolutionRecord>> {
        let mut ids = std::fs::read_dir(self.root.join("records"))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
            .filter(|name| !name.starts_with('.'))
            .collect::<Vec<_>>();
        ids.sort();
        if !ids.is_empty() {
            let after = self.pending_cursor.lock().unwrap().clone();
            let start = ids.partition_point(|id| id <= &after) % ids.len();
            ids.rotate_left(start);
        }
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for id in ids
            .into_iter()
            .take(limit.clamp(1, 1000).saturating_mul(4).max(25) as usize)
        {
            let size = self
                .path(&id, "record")
                .ok()
                .and_then(|path| path.metadata().ok())
                .map(|metadata| metadata.len() as usize)
                .unwrap_or(0);
            if !records.is_empty() && bytes.saturating_add(size) > MAX_OUTBOX_PAGE {
                break;
            }
            *self.pending_cursor.lock().unwrap() = id.clone();
            if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                let diagnostic_id = format!("{:x}", Sha256::digest(id.as_bytes()));
                let fault = UrlResolutionFault {
                    resolution_id: diagnostic_id.clone(),
                    error: "unexpected entry in URL outbox; original entry retained for review"
                        .into(),
                };
                let _ = self.publish(&diagnostic_id, "fault", &serde_json::to_vec(&fault)?);
                continue;
            }
            if self.path(&id, "ack")?.try_exists()? || self.path(&id, "cancelled")?.try_exists()? {
                continue;
            }
            match self.record(&id) {
                Ok(Some(record)) => {
                    bytes += size;
                    records.push(record);
                }
                Ok(None) => {}
                Err(error) => {
                    let fault = UrlResolutionFault {
                        resolution_id: id.clone(),
                        error: format!("URL resolution requires review: {error}"),
                    };
                    // A corrupt descriptor cannot block valid later rows. Keep
                    // the exact bad bytes and expose a durable fault record.
                    if let Err(fault_error) =
                        self.publish(&id, "fault", &serde_json::to_vec(&fault)?)
                    {
                        tracing::error!(resolution=%id, %fault_error, "could not persist URL outbox review fault");
                    }
                    let source = self.path(&id, "record")?;
                    let target = self.path(&id, "quarantine")?;
                    if let Err(move_error) = crate::fileops::rename_exclusive(&source, &target) {
                        tracing::error!(resolution=%id, %move_error, "URL outbox fault remains retained in place");
                    } else {
                        super::fs::sync_directory(&super::fs::open_dir(source.parent().unwrap())?)?;
                        super::fs::sync_directory(&super::fs::open_dir(target.parent().unwrap())?)?;
                    }
                }
            }
            if records.len() >= limit.clamp(1, 1000) as usize {
                break;
            }
        }
        Ok(records)
    }
    pub fn faults(&self, limit: u32) -> Result<Vec<UrlResolutionFault>> {
        let mut entries = std::fs::read_dir(self.root.join("faults"))?
            .filter_map(|entry| entry.ok())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        if !entries.is_empty() {
            let after = self.fault_cursor.lock().unwrap().clone();
            let start = entries.partition_point(|entry| {
                entry.file_name().to_string_lossy().as_ref() <= after.as_str()
            }) % entries.len();
            entries.rotate_left(start);
        }
        let mut faults = Vec::new();
        for entry in entries
            .into_iter()
            .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
            .take(limit.clamp(1, 1000) as usize)
        {
            let identity = format!("{:x}", Sha256::digest(entry.file_name().as_encoded_bytes()));
            let fault = self.read(&entry.path(), 8192).and_then(|bytes| {
                bytes
                    .map(|bytes| {
                        serde_json::from_slice::<UrlResolutionFault>(&bytes).map_err(Into::into)
                    })
                    .transpose()
            });
            faults.push(match fault {
                Ok(Some(fault)) => fault,
                _ => UrlResolutionFault {
                    resolution_id: identity,
                    error: "URL review diagnostic is malformed; original evidence retained".into(),
                },
            });
        }
        Ok(faults)
    }
    pub fn retry(&self, id: &str) -> Result<Option<UrlStorageRetry>> {
        self.read(&self.path(id, "retry")?, 8192)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }
    pub fn schedule_storage_retry(&self, id: &str, message: &str) -> Result<UrlStorageRetry> {
        self.schedule_storage_retry_not_before(id, message, None)
    }
    pub fn schedule_storage_retry_not_before(
        &self,
        id: &str,
        message: &str,
        not_before: Option<i64>,
    ) -> Result<UrlStorageRetry> {
        let prior = self.retry(id)?.unwrap_or_default();
        let now = super::now();
        let retry = UrlStorageRetry {
            attempts: prior.attempts.saturating_add(1),
            next_retry_at: url_storage_retry_deadline(
                id,
                prior.attempts,
                now,
                prior.next_retry_at,
                not_before,
            ),
            blocked_since: if prior.blocked_since == 0 {
                now
            } else {
                prior.blocked_since
            },
            error: message.chars().take(4096).collect(),
        };
        self.write_tracked(id, "retry", &serde_json::to_vec(&retry)?, true)?;
        Ok(retry)
    }
    pub fn acknowledge(&self, id: &str) -> Result<bool> {
        if self.path(id, "ack")?.try_exists()? {
            self.compact_settled(id)?;
            return Ok(true);
        }
        let Some(record) = self.record(id)? else {
            return Ok(false);
        };
        let maximum = record
            .resolved
            .files
            .iter()
            .map(|file| file.id.0)
            .max()
            .unwrap_or(0)
            .max(
                self.reservation(&record.origin)?
                    .map(|reservation| reservation.maximum_file_id)
                    .unwrap_or(0),
            );
        self.publish(
            id,
            &format!("highwater-{maximum}"),
            b"reserved file ID highwater\n",
        )?;
        self.publish(id, "ack", &serde_json::to_vec(&(maximum, super::now()))?)?;
        // Only a caller's confirmed authoritative queue/control commit permits
        // compaction. The small tombstone/highwater precede every removal.
        self.compact_settled(id)?;
        for entry in std::fs::read_dir(self.root.join("highwater"))? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some((owner, value)) = name
                .to_str()
                .and_then(|name| name.rsplit_once(".highwater-"))
            {
                if owner.len() == 64
                    && owner.bytes().all(|byte| byte.is_ascii_hexdigit())
                    && value
                        .parse::<u32>()
                        .ok()
                        .is_some_and(|value| value < maximum)
                    && (self.path(owner, "ack")?.try_exists()?
                        || self.path(owner, "cancelled")?.try_exists()?)
                {
                    std::fs::remove_file(entry.path())?;
                }
            }
        }
        super::fs::sync_directory(&super::fs::open_dir(&self.root.join("highwater"))?)?;
        Ok(true)
    }
    fn compact_settled(&self, id: &str) -> Result<()> {
        if !self.path(id, "ack")?.try_exists()? && !self.path(id, "cancelled")?.try_exists()? {
            return Ok(());
        }
        if let Some(bytes) = self.read(&self.path(id, "reservation")?, 4096)? {
            let maximum = serde_json::from_slice::<UrlFileReservation>(&bytes)?.maximum_file_id;
            self.publish(
                id,
                &format!("highwater-{maximum}"),
                b"settled file ID reservation\n",
            )?;
        }
        for suffix in ["source", "record", "reservation", "retry"] {
            let path = self.path(id, suffix)?;
            match std::fs::remove_file(&path) {
                Ok(()) => super::fs::sync_directory(&super::fs::open_dir(path.parent().unwrap())?)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    pub fn is_cancelled(&self, origin: &Job) -> Result<bool> {
        Ok(self.is_retired(origin)?
            || self
                .path(&url_source_identity(origin)?, "cancelled")?
                .try_exists()?)
    }
    pub fn cancel(&self, origin: &Job) -> Result<()> {
        self.publish(
            &url_source_identity(origin)?,
            "cancelled",
            b"admission cancelled\n",
        )
    }
    /// Bounded cleanup of exact owned temporaries and late writes whose
    /// authoritative acceptance/cancellation tombstone already committed.
    pub fn maintenance(&self, limit: usize) -> Result<usize> {
        let mut entries = std::fs::read_dir(self.root.join("temporaries"))?
            .filter_map(|entry| entry.ok())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        let after = self.maintenance_cursor.lock().unwrap().clone();
        let mut completed = 0;
        let mut visited = 0;
        for entry in entries
            .into_iter()
            .filter(|entry| entry.file_name().to_string_lossy().as_ref() > after.as_str())
            .take(limit.max(1))
        {
            visited += 1;
            *self.maintenance_cursor.lock().unwrap() =
                entry.file_name().to_string_lossy().into_owned();
            let metadata = self.read(&entry.path(), 8192).and_then(|bytes| {
                bytes
                    .map(|bytes| {
                        serde_json::from_slice::<CacheTemporary>(&bytes).map_err(Into::into)
                    })
                    .transpose()
            });
            let Ok(Some(metadata)) = metadata else {
                continue;
            };
            if Path::new(&metadata.name).components().count() != 1
                || !metadata.name.starts_with(".tmp-")
            {
                continue;
            }
            let Ok(target) = self.path(&metadata.id, &metadata.suffix) else {
                continue;
            };
            let temporary = target.parent().unwrap().join(&metadata.name);
            let file = match crate::fileops::open(&temporary) {
                Ok(file) => file,
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::remove_file(entry.path())?;
                    completed += 1;
                    continue;
                }
                _ => continue,
            };
            if file.try_lock().is_err()
                || !metadata
                    .identity
                    .same_object(&super::fs::identity_for_file(&file)?)
            {
                continue;
            }
            std::fs::remove_file(&temporary)?;
            std::fs::remove_file(entry.path())?;
            super::fs::sync_directory(&super::fs::open_dir(temporary.parent().unwrap())?)?;
            super::fs::sync_directory(&super::fs::open_dir(&self.root.join("temporaries"))?)?;
            completed += 1;
        }
        if visited == 0 {
            self.maintenance_cursor.lock().unwrap().clear();
        }
        for directory in ["retirements", "sources", "records", "reservations"] {
            let mut entries = std::fs::read_dir(self.root.join(directory))?
                .filter_map(|entry| entry.ok())
                .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
                .collect::<Vec<_>>();
            entries.sort_by_key(|entry| entry.file_name());
            let after = self
                .cleanup_cursors
                .lock()
                .unwrap()
                .get(directory)
                .cloned()
                .unwrap_or_default();
            let mut visited = 0;
            for entry in entries
                .into_iter()
                .filter(|entry| entry.file_name().to_string_lossy().as_ref() > after.as_str())
                .take(limit.max(1))
            {
                visited += 1;
                let name = entry.file_name();
                let Some(id) = name.to_str() else {
                    continue;
                };
                self.cleanup_cursors
                    .lock()
                    .unwrap()
                    .insert(directory, id.to_owned());
                if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    continue;
                }
                if directory == "retirements" {
                    if let Ok(Some(bytes)) = self.read(&entry.path(), 8192) {
                        if let Ok((_, _, _, Some(url_id))) =
                            serde_json::from_slice::<(nzbd_types::JobId, i64, i64, Option<String>)>(
                                &bytes,
                            )
                        {
                            if self
                                .path(&url_id, "cancelled")
                                .is_ok_and(|path| !path.exists())
                            {
                                self.publish(
                                    &url_id,
                                    "cancelled",
                                    b"authoritative admission retired\n",
                                )?;
                            }
                        }
                    }
                } else if self.path(id, "ack")?.try_exists()?
                    || self.path(id, "cancelled")?.try_exists()?
                {
                    match self.compact_settled(id) {
                        Ok(()) => completed += 1,
                        Err(error) => {
                            let fault = UrlResolutionFault {
                                resolution_id: id.into(),
                                error: format!("URL residue requires review: {error}"),
                            };
                            let _ = self.publish(id, "fault", &serde_json::to_vec(&fault)?);
                        }
                    }
                }
            }
            if visited == 0 {
                self.cleanup_cursors.lock().unwrap().remove(directory);
            }
        }
        Ok(completed)
    }

    pub fn reserved_file_id(&self) -> Result<u32> {
        let mut maximum = 0;
        for entry in std::fs::read_dir(self.root.join("highwater"))? {
            let entry = entry?;
            if let Some(value) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.rsplit_once("highwater-"))
                .and_then(|(_, value)| value.parse::<u32>().ok())
            {
                maximum = maximum.max(value);
            }
        }
        for entry in std::fs::read_dir(self.root.join("reservations"))? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with('.'))
            {
                continue;
            }
            if let Some(bytes) = self.read(&entry.path(), 4096)? {
                match serde_json::from_slice::<UrlFileReservation>(&bytes) {
                    Ok(reservation) => maximum = maximum.max(reservation.maximum_file_id),
                    Err(error) => {
                        let name = entry.file_name();
                        if let Some(id) = name.to_str() {
                            let fault = UrlResolutionFault { resolution_id: id.into(), error: format!("URL file reservation requires review; durable highwater retained: {error}") };
                            let _ = self.publish(id, "fault", &serde_json::to_vec(&fault)?);
                        }
                    }
                }
            }
        }
        Ok(maximum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn origin(id: u32) -> Job {
        Job {
            id: nzbd_types::JobId(id),
            kind: nzbd_types::JobKind::Url,
            name: format!("job-{id}"),
            dir_name: format!("job-{id}"),
            name_provisional: false,
            delivery_lifecycle: true,
            queued_at_unix: 1,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![
                ("*URL".into(), format!("http://example.test/{id}.nzb")),
                ("monarr-transfer".into(), format!("request-{id}")),
            ],
            files: Vec::new(),
            totals: Default::default(),
            status: nzbd_types::JobStatus::Fetching,
            torrent: None,
            stages: Vec::new(),
        }
    }
    #[test]
    fn url_acknowledgement_compacts_bulk_and_preserves_reserved_id_tombstone() {
        let temporary = tempfile::tempdir().unwrap();
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        let origin = origin(1);
        let identity = url_source_identity(&origin).unwrap();
        store.cache_source(&origin, b"durable NZB bytes").unwrap();
        store.reserve_files(&origin, 11, 2).unwrap();
        let mut resolved = origin.clone();
        resolved.status = nzbd_types::JobStatus::Failed;
        store.stage(&origin, &resolved).unwrap();
        assert_eq!(store.pending(100).unwrap().len(), 1);
        store.acknowledge(&identity).unwrap();
        assert!(store.pending(100).unwrap().is_empty());
        assert!(!store.path(&identity, "record").unwrap().exists());
        assert!(!store.path(&identity, "source").unwrap().exists());
        assert_eq!(store.reserved_file_id().unwrap(), 12);
        assert!(matches!(
            store.cache_source(&origin, b"late bytes"),
            Err(Error::NotFound)
        ));
        assert!(store.acknowledge(&identity).unwrap());
    }
    #[test]
    fn corrupt_and_unexpected_outbox_entries_do_not_block_later_rows_or_fault_visibility() {
        let temporary = tempfile::tempdir().unwrap();
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        let bad = origin(2);
        let good = origin(3);
        for source in [&bad, &good] {
            let mut resolved = source.clone();
            resolved.status = nzbd_types::JobStatus::Failed;
            store.stage(source, &resolved).unwrap();
        }
        let bad_id = url_source_identity(&bad).unwrap();
        std::fs::write(store.path(&bad_id, "record").unwrap(), b"{").unwrap();
        std::fs::write(
            store.root.join("records/unexpected-name"),
            b"unknown evidence",
        )
        .unwrap();
        let records = store.pending(100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].origin.id, good.id);
        assert!(store.path(&bad_id, "quarantine").unwrap().exists());
        assert_eq!(store.faults(100).unwrap().len(), 2);
        std::fs::write(store.root.join("faults/malformed-diagnostic"), b"{").unwrap();
        assert_eq!(store.faults(100).unwrap().len(), 3);
        assert!(store.root.join("records/unexpected-name").exists());
    }
    #[test]
    fn cancelled_url_late_residue_and_interrupted_owned_temp_are_reclaimed_safely() {
        let temporary = tempfile::tempdir().unwrap();
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        let origin = origin(4);
        let identity = url_source_identity(&origin).unwrap();
        store.cache_source(&origin, b"received bytes").unwrap();
        store.reserve_files(&origin, 31, 1).unwrap();
        store.retire_job(&origin).unwrap();
        // A writer that checked before retirement can crash after its publish.
        std::fs::write(store.path(&identity, "source").unwrap(), b"late residue").unwrap();
        store.maintenance(100).unwrap();
        assert!(!store.path(&identity, "source").unwrap().exists());
        assert_eq!(store.reserved_file_id().unwrap(), 31);
        assert!(matches!(
            store.cache_source(&origin, b"late bytes"),
            Err(Error::NotFound)
        ));
        let next = super::tests::origin(5);
        CRASH_BEFORE_RENAME.with(|crash| crash.set(true));
        assert!(store.cache_source(&next, &vec![7; 1024 * 1024]).is_err());
        let metadata = std::fs::read_dir(store.root.join("temporaries"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let receipt: CacheTemporary =
            serde_json::from_slice(&std::fs::read(metadata.path()).unwrap()).unwrap();
        let target = store.path(&receipt.id, &receipt.suffix).unwrap();
        let orphan = target.parent().unwrap().join(receipt.name);
        assert_eq!(std::fs::metadata(&orphan).unwrap().len(), 1024 * 1024 + 77);
        drop(store);
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        store.maintenance(100).unwrap();
        assert!(!orphan.exists());
        std::fs::write(
            store.root.join("sources/.tmp-unregistered"),
            b"unknown local evidence",
        )
        .unwrap();
        store.maintenance(100).unwrap();
        assert!(store.root.join("sources/.tmp-unregistered").exists());
    }
    #[test]
    fn owned_temporary_cleanup_preserves_live_writer_lock() {
        let temporary = tempfile::tempdir().unwrap();
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        let origin = origin(7);
        let id = url_source_identity(&origin).unwrap();
        let name = ".tmp-live-writer".to_owned();
        let path = store.root.join("sources").join(&name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.try_lock().unwrap();
        file.write_all(b"active cache writer").unwrap();
        let receipt = CacheTemporary {
            id,
            suffix: "source".into(),
            name,
            identity: super::super::fs::identity_for_file(&file).unwrap(),
        };
        std::fs::write(
            store.root.join("temporaries/live.json"),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        store.maintenance(100).unwrap();
        assert!(path.exists(), "live open writer lock prevents cleanup");
        drop(file);
        store.maintenance(100).unwrap();
        store.maintenance(100).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn url_storage_backoff_and_checksum_are_durable() {
        let temporary = tempfile::tempdir().unwrap();
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        let origin = origin(6);
        let identity = url_source_identity(&origin).unwrap();
        store.cache_source(&origin, b"received bytes").unwrap();
        let now = super::super::now();
        let provider_floor = now + 600;
        assert_eq!(
            url_storage_retry_deadline(&identity, 0, now, 0, Some(provider_floor)),
            provider_floor,
            "in-memory fallback obeys Retry-After even if storage cannot persist its schedule"
        );
        let jittered = url_storage_retry_deadline(&identity, 0, now, 0, None);
        assert!((now + 30..=now + 33).contains(&jittered));
        // Persistence may cross a wall-clock second. Measure the retry floor
        // before scheduling, rather than moving it forward after the write.
        let first_started = super::super::now();
        let first = store
            .schedule_storage_retry(&identity, "destination full")
            .unwrap();
        assert!(first.next_retry_at >= first_started + 30);
        drop(store);
        let store = UrlResolutionStore::open(temporary.path()).unwrap();
        assert_eq!(store.retry(&identity).unwrap().unwrap().attempts, 1);
        let second_started = super::super::now();
        let second = store
            .schedule_storage_retry(&identity, "still full")
            .unwrap();
        assert_eq!(second.blocked_since, first.blocked_since);
        assert!(second.next_retry_at >= second_started + 60);
        let source = store.path(&identity, "source").unwrap();
        let mut bytes = std::fs::read(&source).unwrap();
        bytes[77] ^= 1;
        std::fs::write(source, bytes).unwrap();
        assert!(
            matches!(store.source(&origin), Err(Error::Conflict(message)) if message.contains("checksum changed"))
        );
    }
}
