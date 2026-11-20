use crate::snapshot_metrics::{SnapshotOperation, SnapshotTimer};
use crate::store::StorageResult;
use crate::store::state_machine::sqlite::TypeConfigSqlite;
use crate::store::state_machine::sqlite::state_machine::StateMachineSqlite;
use crate::store::state_machine::sqlite::writer::{SnapshotRequest, WriterRequest};
use crate::{Node, NodeId};
use openraft::{
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{Mutex, oneshot};
use tokio::{fs, task};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub(crate) const CURRENT_SNAPSHOT_POINTER: &str = "current";
const CURRENT_SNAPSHOT_POINTER_TEMP: &str = "current.temp";
pub(crate) const PENDING_SNAPSHOT_POINTER: &str = "pending";
const PENDING_SNAPSHOT_POINTER_TEMP: &str = "pending.temp";
const NO_CURRENT_SNAPSHOT: &str = "none";

#[cfg(test)]
static FAIL_CURRENT_PUBLICATION_AFTER_RENAME: std::sync::Mutex<Option<String>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
pub(crate) static CURRENT_PUBLICATION_RENAMED: Notify = Notify::const_new();
#[cfg(test)]
pub(crate) static RELEASE_CURRENT_PUBLICATION: Notify = Notify::const_new();

#[cfg(test)]
pub(crate) fn inject_current_publication_failure(path_snapshots: &str) {
    *FAIL_CURRENT_PUBLICATION_AFTER_RENAME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(path_snapshots.to_owned());
}

#[derive(Debug, Clone)]
pub struct SQLiteSnapshotBuilder {
    // pub last_applied_log_id: Option<LogId<NodeId>>,
    // pub last_membership: StoredMembership<NodeId, Node>,
    #[cfg(feature = "backup")]
    pub path_backups: String,
    pub path_snapshots: String,
    pub write_tx: flume::Sender<WriterRequest>,
    pub(crate) snapshot_files: Arc<Mutex<SnapshotFileState>>,
    pub(crate) snapshot_recovery_pending: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
pub(crate) struct SnapshotFileState {
    pub(crate) current_id: Option<String>,
    pub(crate) pending_id: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SnapshotPointer {
    Missing,
    Empty,
    Snapshot(String),
}

impl RaftSnapshotBuilder<TypeConfigSqlite> for SQLiteSnapshotBuilder {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfigSqlite>, StorageError<NodeId>> {
        let timer = SnapshotTimer::start(SnapshotOperation::Build);
        // Snapshot construction and installation both replace the current
        // state-machine image. Serialize their file publication so cleanup
        // always reads the operation that actually completed last.
        let snapshot_files = self.snapshot_files.clone();
        let mut snapshot_files_guard = snapshot_files.lock().await;
        if self.snapshot_recovery_pending.load(Ordering::Acquire) {
            let error = std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "snapshot build refused while install recovery is pending",
            );
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        // - build new snapshot id
        // - make sure target path exists
        // - send snapshot request to db writer
        // - await vaccuum response
        // - open db snapshot file
        // - return snapshot handle

        let snapshot_id = Uuid::now_v7();

        let path = format!("{}/{}", self.path_snapshots, snapshot_id);
        let path_temp = format!("{path}.temp");
        let (ack, rx) = oneshot::channel();
        let req = WriterRequest::Snapshot(SnapshotRequest {
            snapshot_id,
            // last_membership: self.last_membership.clone(),
            path: path_temp.clone(),
            ack,
        });
        self.write_tx
            .send_async(req)
            .await
            .expect("Sender to always be listening");

        let resp = rx.await.expect("to always receive a snapshot response")?;
        sync_file(&path_temp).await?;
        fs::rename(&path_temp, &path)
            .await
            .map_err(|err| StorageError::IO {
                source: StorageIOError::write_state_machine(&err),
            })?;
        sync_directory(&self.path_snapshots).await?;
        let snapshot = fs::File::open(&path)
            .await
            .map_err(|err| StorageError::IO {
                source: StorageIOError::read_state_machine(&err),
            })?;

        let snapshot_id = snapshot_id.to_string();
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: resp.meta.last_applied_log_id,
                last_membership: resp.meta.last_membership,
                snapshot_id: snapshot_id.clone(),
            },
            snapshot: Box::new(snapshot),
        };

        publish_current_snapshot(&self.path_snapshots, Some(&snapshot_id)).await?;
        snapshot_files_guard.current_id = Some(snapshot_id);
        drop(snapshot_files_guard);
        // Cleanup can happen in the background, but it resolves the current
        // id under the same lock as later builds and installs instead of
        // retaining this task's now-stale captured id.
        task::spawn(snapshots_cleanup(
            self.path_snapshots.clone(),
            #[cfg(feature = "backup")]
            self.path_backups.clone(),
            snapshot_files,
        ));
        timer.success();
        Ok(snapshot)
    }
}

pub(crate) async fn snapshots_cleanup(
    path_snapshots: String,
    #[cfg(feature = "backup")] path_backups: String,
    snapshot_files: Arc<Mutex<SnapshotFileState>>,
) -> Result<(), StorageError<NodeId>> {
    let mut snapshot_files = snapshot_files.lock().await;
    // The pointer is the durable publication boundary. Process-local state can
    // lag it when cancellation or an fsync error arrives after the atomic
    // rename, so cleanup must resolve ownership from disk and fail closed when
    // that decision cannot be read.
    let keep_id = match load_current_snapshot(&path_snapshots).await? {
        SnapshotPointer::Snapshot(snapshot_id) => Some(snapshot_id),
        SnapshotPointer::Empty => None,
        SnapshotPointer::Missing => {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot cleanup refused without a current pointer",
            );
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
    };
    let pending_id = match load_pending_snapshot(&path_snapshots).await? {
        SnapshotPointer::Snapshot(snapshot_id) => Some(snapshot_id),
        SnapshotPointer::Missing => None,
        SnapshotPointer::Empty => {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "pending snapshot pointer cannot contain an empty generation",
            );
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
    };
    snapshot_files.current_id.clone_from(&keep_id);
    snapshot_files.pending_id.clone_from(&pending_id);
    let mut list = tokio::fs::read_dir(&path_snapshots)
        .await
        .map_err(|err| StorageError::IO {
            source: StorageIOError::read(&err),
        })?;

    let mut deletes = Vec::new();
    loop {
        let Some(entry) = list.next_entry().await.map_err(|error| StorageError::IO {
            source: StorageIOError::read(&error),
        })?
        else {
            break;
        };
        let file_name = entry.file_name();
        let name = file_name.to_str().unwrap_or("UNKNOWN");

        let meta = entry.metadata().await.map_err(|err| StorageError::IO {
            source: StorageIOError::read(&err),
        })?;

        // we only expect sub-dirs in the snapshot dir
        if meta.is_dir() {
            warn!("Invalid folder in snapshots dir: {name}");
            continue;
        }

        // `begin_receiving_snapshot` owns `temp`; the durable pointer owns the
        // current local build or peer install. A cleanup task may run long
        // after the build that spawned it, so process-local state is not a safe
        // retention decision.
        if Some(name) != keep_id.as_deref()
            && Some(name) != pending_id.as_deref()
            && name != "temp"
            && name != CURRENT_SNAPSHOT_POINTER
            && name != CURRENT_SNAPSHOT_POINTER_TEMP
            && name != PENDING_SNAPSHOT_POINTER
            && name != PENDING_SNAPSHOT_POINTER_TEMP
        {
            deletes.push(name.to_string());
        }
    }

    #[cfg(feature = "backup")]
    {
        debug!("Cleaning up possibly existing old backup restore files");
        let restore_path = format!("{}/{}", path_backups, crate::backup::BACKUP_DB_NAME);
        let _ = fs::remove_file(&restore_path).await;
    }

    for file_name in deletes {
        let path = format!("{path_snapshots}/{file_name}");
        if let Err(err) = fs::remove_file(path).await {
            error!("Error removing old snapshot {file_name}: {err}");
        }
    }

    Ok(())
}

pub(crate) async fn load_current_snapshot(path_snapshots: &str) -> StorageResult<SnapshotPointer> {
    load_snapshot_pointer(path_snapshots, CURRENT_SNAPSHOT_POINTER, true).await
}

pub(crate) async fn load_pending_snapshot(path_snapshots: &str) -> StorageResult<SnapshotPointer> {
    load_snapshot_pointer(path_snapshots, PENDING_SNAPSHOT_POINTER, false).await
}

async fn load_snapshot_pointer(
    path_snapshots: &str,
    pointer_name: &str,
    allow_empty: bool,
) -> StorageResult<SnapshotPointer> {
    let path = format!("{path_snapshots}/{pointer_name}");
    let value = match fs::read_to_string(&path).await {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SnapshotPointer::Missing);
        }
        Err(error) => {
            return Err(StorageError::IO {
                source: StorageIOError::read_state_machine(&error),
            });
        }
    };
    let value = value.trim();
    if value == NO_CURRENT_SNAPSHOT {
        if allow_empty {
            return Ok(SnapshotPointer::Empty);
        }
        let error = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{pointer_name} snapshot pointer cannot be empty"),
        );
        return Err(StorageError::IO {
            source: StorageIOError::read_state_machine(&error),
        });
    }
    let id = Uuid::parse_str(value).map_err(|error| StorageError::IO {
        source: StorageIOError::read_state_machine(&error),
    })?;
    Ok(SnapshotPointer::Snapshot(id.to_string()))
}

pub(crate) async fn publish_current_snapshot(
    path_snapshots: &str,
    snapshot_id: Option<&str>,
) -> StorageResult<()> {
    publish_snapshot_pointer(
        path_snapshots,
        CURRENT_SNAPSHOT_POINTER,
        CURRENT_SNAPSHOT_POINTER_TEMP,
        snapshot_id,
    )
    .await
}

pub(crate) async fn publish_pending_snapshot(
    path_snapshots: &str,
    snapshot_id: &str,
) -> StorageResult<()> {
    publish_snapshot_pointer(
        path_snapshots,
        PENDING_SNAPSHOT_POINTER,
        PENDING_SNAPSHOT_POINTER_TEMP,
        Some(snapshot_id),
    )
    .await
}

async fn publish_snapshot_pointer(
    path_snapshots: &str,
    pointer_name: &str,
    pointer_temp_name: &str,
    snapshot_id: Option<&str>,
) -> StorageResult<()> {
    let value = match snapshot_id {
        Some(snapshot_id) => Uuid::parse_str(snapshot_id)
            .map_err(|error| StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            })?
            .to_string(),
        None => NO_CURRENT_SNAPSHOT.to_owned(),
    };
    let path_temp = format!("{path_snapshots}/{pointer_temp_name}");
    let path = format!("{path_snapshots}/{pointer_name}");
    let mut file = fs::File::create(&path_temp)
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })?;
    file.write_all(value.as_bytes())
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })?;
    file.write_all(b"\n")
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })?;
    file.sync_all().await.map_err(|error| StorageError::IO {
        source: StorageIOError::write_state_machine(&error),
    })?;
    drop(file);
    fs::rename(&path_temp, &path)
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })?;
    #[cfg(test)]
    let fail_current_publication = pointer_name == CURRENT_SNAPSHOT_POINTER && {
        let mut target = FAIL_CURRENT_PUBLICATION_AFTER_RENAME
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if target.as_deref() == Some(path_snapshots) {
            target.take();
            true
        } else {
            false
        }
    };
    #[cfg(test)]
    if fail_current_publication {
        CURRENT_PUBLICATION_RENAMED.notify_one();
        RELEASE_CURRENT_PUBLICATION.notified().await;
        let error = std::io::Error::other("injected current-pointer durability failure");
        return Err(StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        });
    }
    sync_directory(path_snapshots).await
}

pub(crate) async fn clear_pending_snapshot(path_snapshots: &str) -> StorageResult<()> {
    for name in [PENDING_SNAPSHOT_POINTER, PENDING_SNAPSHOT_POINTER_TEMP] {
        let path = format!("{path_snapshots}/{name}");
        match fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::IO {
                    source: StorageIOError::write_state_machine(&error),
                });
            }
        }
    }
    sync_directory(path_snapshots).await
}

pub(crate) async fn sync_file(path: &str) -> StorageResult<()> {
    // Windows flushes through FlushFileBuffers, which requires a handle with
    // write access: syncing a read-only handle fails with "Access is denied"
    // (os error 5). Unix fsync accepts either, so open for writing everywhere.
    // This never creates or truncates; the file must already exist.
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })?;
    file.sync_all().await.map_err(|error| StorageError::IO {
        source: StorageIOError::write_state_machine(&error),
    })
}

#[cfg(unix)]
pub(crate) async fn sync_directory(path: &str) -> StorageResult<()> {
    let path = path.to_owned();
    task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })?
        .map_err(|error| StorageError::IO {
            source: StorageIOError::write_state_machine(&error),
        })
}

#[cfg(not(unix))]
pub(crate) async fn sync_directory(_path: &str) -> StorageResult<()> {
    Ok(())
}

#[cfg(test)]
mod snapshot_metrics_cleanup_contract {
    use super::*;

    #[tokio::test]
    async fn snapshot_pointer_survives_a_newer_interrupted_publication() {
        let root =
            std::env::temp_dir().join(format!("hiqlite-snapshot-cleanup-{}", Uuid::now_v7()));
        fs::create_dir_all(&root)
            .await
            .expect("create snapshot cleanup root");
        let installed_id = Uuid::parse_str("018f0000-0000-7000-8000-000000000001")
            .expect("valid installed snapshot id");
        let newer_orphan_id = Uuid::parse_str("019f0000-0000-7000-8000-000000000002")
            .expect("valid orphan snapshot id");
        let installed_path = root.join(installed_id.to_string());
        let newer_orphan_path = root.join(newer_orphan_id.to_string());
        let interrupted_stage_path = root.join(format!("{newer_orphan_id}.installing"));
        let receive_path = root.join("temp");
        fs::write(&installed_path, b"installed peer snapshot")
            .await
            .expect("write installed snapshot");
        publish_current_snapshot(
            root.to_str().expect("UTF-8 snapshot cleanup root"),
            Some(&installed_id.to_string()),
        )
        .await
        .expect("publish installed snapshot pointer");
        fs::write(&newer_orphan_path, b"interrupted final snapshot")
            .await
            .expect("write interrupted final snapshot");
        fs::write(&interrupted_stage_path, b"interrupted staged snapshot")
            .await
            .expect("write interrupted staged snapshot");
        fs::write(&receive_path, b"receiving")
            .await
            .expect("write inflight install");

        let expected = SnapshotPointer::Snapshot(installed_id.to_string());
        assert_eq!(
            load_current_snapshot(root.to_str().expect("UTF-8 snapshot cleanup root"))
                .await
                .expect("load current snapshot before restart"),
            expected
        );
        assert_eq!(
            load_current_snapshot(root.to_str().expect("UTF-8 snapshot cleanup root"))
                .await
                .expect("load current snapshot after simulated restart"),
            SnapshotPointer::Snapshot(installed_id.to_string())
        );

        #[cfg(feature = "backup")]
        let backups = root.join("backups");
        #[cfg(feature = "backup")]
        fs::create_dir_all(&backups)
            .await
            .expect("create backup cleanup root");

        let snapshot_files = Arc::new(Mutex::new(SnapshotFileState {
            // Emulate cancellation or directory-fsync failure after the
            // durable pointer rename but before the process-local assignment.
            current_id: Some(newer_orphan_id.to_string()),
            pending_id: None,
        }));
        snapshots_cleanup(
            root.to_str()
                .expect("UTF-8 snapshot cleanup root")
                .to_owned(),
            #[cfg(feature = "backup")]
            backups
                .to_str()
                .expect("UTF-8 backup cleanup root")
                .to_owned(),
            snapshot_files,
        )
        .await
        .expect("clean old snapshots");

        assert!(installed_path.exists());
        assert!(receive_path.exists());
        assert!(!newer_orphan_path.exists());
        assert!(!interrupted_stage_path.exists());
        assert!(root.join(CURRENT_SNAPSHOT_POINTER).exists());
        fs::remove_dir_all(root)
            .await
            .expect("remove snapshot cleanup root");
    }
}
