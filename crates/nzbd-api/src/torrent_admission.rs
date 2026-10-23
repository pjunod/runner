//! Native BitTorrent admission and runtime reconciliation.
use axum::extract::State;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use nzbd_engine::backend::{
    BackendAdapterPort, BackendCommand, BackendFact, RemovalOutcome, SafeError, StopReason,
};
use nzbd_engine::torrent_runtime::{validate_removal_payload, RemovalRefusal};
use nzbd_engine::{AddOpts, EngineHandle};
use nzbd_state::torrent_sources::PendingSourceStore;
use nzbd_torrent::{
    inspect_metainfo, TorrentAddConfig, TorrentRegistry, TorrentSession, TorrentSourceFetchLimits,
};
use nzbd_types::{
    JobId, SeedPolicy, TorrentFileRecord, TorrentPhase, TorrentRecord, TorrentSource,
};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct TorrentAdmissionService {
    engine: EngineHandle,
    session: TorrentSession,
    registry: Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
    state_dir: PathBuf,
    proxy_enabled: bool,
    dht_enabled: bool,
    default_seed_policy: SeedPolicy,
    category_seed_policies: HashMap<String, SeedPolicy>,
    category_payload_roots: Arc<std::sync::RwLock<HashMap<String, PathBuf>>>,
    upload_limit_bps: Option<u64>,
    source_fetch_limits: TorrentSourceFetchLimits,
    #[cfg(test)]
    before_managed_add: Option<Arc<dyn Fn() -> Result<(), AdmissionError> + Send + Sync>>,
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("{0}")]
    Torrent(#[from] nzbd_torrent::TorrentError),
    #[error("{0}")]
    Engine(#[from] nzbd_engine::EngineError),
    #[error("{0}")]
    State(#[from] nzbd_state::StateError),
    #[error("torrent descriptor I/O failed")]
    Io(#[from] std::io::Error),
    #[error("invalid torrent source encoding")]
    Encoding,
    #[error("pending torrent admission disappeared")]
    MissingPending,
    #[error("torrent backend adapter has already been taken")]
    MissingBackendAdapter,
    #[error("torrent metainfo exceeds the configured limit")]
    MetainfoTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionResult {
    pub id: JobId,
    pub created: bool,
    pub info_hash: String,
}

#[derive(Clone)]
struct Association {
    identity: nzbd_torrent::EngineIdentity,
    /// `None` means that the engine's initial paused state has not yet been
    /// reconciled with a queue-owner control. Managed admission deliberately
    /// starts paused, while the durable queue starts runnable.
    applied_pause: Option<bool>,
    content_path: PathBuf,
    files: Vec<PathBuf>,
    uploaded_base: u64,
    engine_uploaded_origin: Option<u64>,
    last_engine_progress: u64,
    last_engine_uploaded: u64,
    ready_emitted: bool,
    error_emitted: bool,
}

impl TorrentAdmissionService {
    pub fn new(
        engine: EngineHandle,
        session: TorrentSession,
        state_dir: PathBuf,
        proxy_enabled: bool,
        dht_enabled: bool,
    ) -> Self {
        Self {
            engine,
            registry: Arc::new(tokio::sync::Mutex::new(TorrentRegistry::new(
                session.clone(),
            ))),
            associations: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            session,
            state_dir,
            proxy_enabled,
            dht_enabled,
            default_seed_policy: SeedPolicy::default(),
            category_seed_policies: HashMap::new(),
            category_payload_roots: Arc::new(std::sync::RwLock::new(HashMap::new())),
            upload_limit_bps: None,
            source_fetch_limits: TorrentSourceFetchLimits::default(),
            #[cfg(test)]
            before_managed_add: None,
        }
    }

    pub fn with_source_fetch_limits(mut self, limits: TorrentSourceFetchLimits) -> Self {
        self.source_fetch_limits = limits;
        self
    }

    pub fn with_category_payload_roots(mut self, roots: HashMap<String, PathBuf>) -> Self {
        self.category_payload_roots = Arc::new(std::sync::RwLock::new(roots));
        self
    }

    pub fn register_category_payload_root(&self, category: String, root: PathBuf) {
        self.category_payload_roots
            .write()
            .unwrap()
            .insert(category, root);
    }

    pub fn max_request_body_bytes(&self) -> usize {
        self.source_fetch_limits.max_metainfo_bytes
    }

    pub fn output_root(&self) -> &Path {
        self.session.output_root()
    }

    pub fn seed_defaults(&self, category: Option<&str>) -> SeedPolicy {
        let category = category.and_then(|name| {
            self.category_seed_policies
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, policy)| policy)
        });
        let mut policy = nzbd_engine::torrent_runtime::normalized_seed_policy(
            category
                .and_then(|p| p.ratio_limit)
                .or(self.default_seed_policy.ratio_limit),
            category
                .and_then(|p| p.time_limit_secs)
                .or(self.default_seed_policy.time_limit_secs),
        );
        policy.stop_on_complete = category
            .unwrap_or(&self.default_seed_policy)
            .stop_on_complete;
        policy
    }

    pub fn with_transfer_policy(
        mut self,
        default_seed_policy: SeedPolicy,
        category_seed_policies: HashMap<String, SeedPolicy>,
        upload_limit_bps: Option<u64>,
    ) -> Self {
        self.default_seed_policy = default_seed_policy;
        self.category_seed_policies = category_seed_policies;
        self.upload_limit_bps = upload_limit_bps;
        self
    }

    pub async fn admit_raw(
        &self,
        bytes: Vec<u8>,
        opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        self.finish(None, bytes, TorrentSource::Metainfo, opts)
            .await
    }

    pub async fn admit_source(
        &self,
        source: TorrentSource,
        secret: String,
        opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        match source {
            TorrentSource::Magnet => {
                nzbd_torrent::validate_magnet_source(&secret, self.proxy_enabled)?;
                self.session.validate_magnet_discovery(&secret)?;
            }
            TorrentSource::Url => {
                let url = url::Url::parse(&secret).map_err(|_| AdmissionError::Encoding)?;
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(AdmissionError::Encoding);
                }
            }
            TorrentSource::Metainfo => return Err(AdmissionError::Encoding),
        }
        let job = self
            .engine
            .reserve_torrent_admission(source, secret.as_bytes().to_vec(), opts.clone())
            .await?;
        let attempt = async {
            let bytes = match source {
                TorrentSource::Magnet => self.session.resolve_magnet_metadata(secret).await?,
                TorrentSource::Url => {
                    nzbd_torrent::fetch_torrent_source(
                        &secret,
                        self.source_fetch_limits,
                        self.proxy_enabled,
                    )
                    .await?
                }
                TorrentSource::Metainfo => return Err(AdmissionError::Encoding),
            };
            self.finish(Some(job), bytes, source, opts).await
        }
        .await;
        if attempt.is_err() {
            // Cancellation is conditional on the reservation still being
            // pending. If `finish` committed before a later managed-engine
            // failure, the owner returns false and the durable job remains.
            self.engine.cancel_torrent_admission(job).await?;
        }
        attempt
    }

    /// Reconcile queue-authorized descriptors and pending secret sidecars.
    /// Orphans are removed; linked sources remain until `finish` has made the
    /// descriptor and structural replacement durable.
    pub async fn recover(&self) -> Result<Vec<AdmissionResult>, AdmissionError> {
        self.recover_active().await?;
        self.recover_pending().await
    }

    /// Restore durable torrents without waiting for network-sourced admissions.
    pub async fn recover_active(&self) -> Result<(), AdmissionError> {
        let snapshot = nzbd_state::SnapshotStore::open(&self.state_dir)?
            .load()?
            .unwrap_or_default();
        let source_store = PendingSourceStore::open(&self.state_dir)?;
        let linked = snapshot
            .pending_admissions
            .iter()
            .map(|pending| pending.job_id)
            .collect::<std::collections::HashSet<_>>();
        for orphan in source_store
            .inventory()?
            .into_iter()
            .filter(|job| !linked.contains(job))
        {
            source_store.remove(orphan)?;
        }

        let mut payload_roots = vec![self.session.output_root().to_path_buf()];
        payload_roots.extend(
            self.category_payload_roots
                .read()
                .unwrap()
                .values()
                .cloned(),
        );
        let restore_plan = nzbd_engine::torrent_runtime::plan_restore_with_roots(
            &snapshot.jobs,
            &HashMap::new(),
            &payload_roots,
            &std::collections::HashSet::new(),
        );
        if !restore_plan.diagnostics.is_empty() {
            tracing::warn!(
                rejected = restore_plan.diagnostics.len(),
                "torrent recovery left unsafe or terminal records fenced"
            );
        }

        for request in restore_plan.requests {
            let job = snapshot.jobs.iter().find(|job| job.id == request.job);
            let Some(job) = job else {
                continue;
            };
            let Some(torrent) = &job.torrent else {
                continue;
            };
            let payload_root = if torrent.payload_root.as_os_str().is_empty() {
                self.session.output_root()
            } else {
                &torrent.payload_root
            };
            if payload_root != self.session.output_root()
                && !self
                    .category_payload_roots
                    .read()
                    .unwrap()
                    .values()
                    .any(|root| root == payload_root)
            {
                tracing::warn!(
                    job = job.id.0,
                    "torrent recovery refused an unauthorized payload root"
                );
                continue;
            }
            let descriptor_path = self.state_dir.join(&request.metadata_file);
            let bytes =
                std::fs::read(&descriptor_path).map_err(|error| nzbd_state::StateError::Io {
                    op: "read torrent descriptor",
                    path: descriptor_path,
                    source: error,
                })?;
            let descriptor = inspect_metainfo(&bytes, self.proxy_enabled, self.dht_enabled)?;
            let content_path = payload_root.join(&descriptor.name);
            let files = descriptor
                .files
                .iter()
                .map(|(path, _)| path.clone())
                .collect();
            let identities = self
                .registry
                .lock()
                .await
                .restore_selected([nzbd_torrent::RestoreDescriptor {
                    metainfo: bytes,
                    expected_info_hash_v1: request.info_hash_v1,
                    preferred_id: request.preferred_engine_id,
                    selected_files: Some(request.selected_files),
                    output_root: (!torrent.payload_root.as_os_str().is_empty())
                        .then(|| torrent.payload_root.clone()),
                }])
                .await?;
            if let Some(identity) = identities.into_iter().next() {
                self.associations.lock().await.insert(
                    job.id,
                    Association {
                        identity,
                        applied_pause: None,
                        content_path,
                        files,
                        uploaded_base: torrent.uploaded_bytes,
                        engine_uploaded_origin: None,
                        last_engine_progress: torrent.downloaded_bytes,
                        last_engine_uploaded: 0,
                        ready_emitted: torrent.ready_at_unix.is_some(),
                        error_emitted: false,
                    },
                );
            }
        }
        Ok(())
    }

    /// Retry pending sources after the API listener is ready. Magnet lookup
    /// can take 120 seconds per job and must not hold the health check offline.
    pub async fn recover_pending(&self) -> Result<Vec<AdmissionResult>, AdmissionError> {
        let snapshot = nzbd_state::SnapshotStore::open(&self.state_dir)?
            .load()?
            .unwrap_or_default();
        let source_store = PendingSourceStore::open(&self.state_dir)?;
        let mut restored = Vec::new();
        for pending in snapshot.pending_admissions {
            let opts = AddOpts {
                category: pending.category.clone(),
                priority: pending.priority,
                paused: pending.paused,
                seed_ratio_limit: pending.seed_ratio_limit,
                seed_time_limit_secs: pending.seed_time_limit_secs,
                stop_seeding_on_complete: pending.stop_seeding_on_complete,
                params: pending.params.clone(),
                client: pending.client.clone(),
                torrent_tags: pending.torrent_tags.clone(),
                ..Default::default()
            };
            let attempt: Result<AdmissionResult, AdmissionError> = async {
                let source_bytes = source_store.read(pending.job_id)?;
                let bytes = match pending.source {
                    TorrentSource::Magnet => {
                        self.session
                            .resolve_magnet_metadata(
                                String::from_utf8(source_bytes)
                                    .map_err(|_| AdmissionError::Encoding)?,
                            )
                            .await?
                    }
                    TorrentSource::Url => {
                        nzbd_torrent::fetch_torrent_source(
                            &String::from_utf8(source_bytes)
                                .map_err(|_| AdmissionError::Encoding)?,
                            self.source_fetch_limits,
                            self.proxy_enabled,
                        )
                        .await?
                    }
                    TorrentSource::Metainfo => source_bytes,
                };
                self.finish(Some(pending.job_id), bytes, pending.source, opts)
                    .await
            }
            .await;
            match attempt {
                Ok(result) => restored.push(result),
                Err(AdmissionError::MissingPending) => tracing::info!(
                    job = pending.job_id.0,
                    "pending torrent admission was removed during recovery"
                ),
                Err(error)
                    if pending.source == TorrentSource::Magnet
                        && deterministic_magnet_recovery_failure(&error) =>
                {
                    match self.engine.cancel_torrent_admission(pending.job_id).await {
                        Ok(true) => tracing::warn!(
                            job = pending.job_id.0,
                            error = %error,
                            "deterministically rejected pending magnet was removed during recovery"
                        ),
                        Ok(false) => tracing::info!(
                            job = pending.job_id.0,
                            "pending torrent admission was removed during recovery"
                        ),
                        Err(cancel_error) => tracing::error!(
                            job = pending.job_id.0,
                            error = %cancel_error,
                            "could not remove rejected pending magnet; continuing recovery"
                        ),
                    }
                }
                Err(error) => tracing::warn!(
                    job = pending.job_id.0,
                    source = ?pending.source,
                    error = %error,
                    "pending torrent admission could not be recovered; continuing with later jobs"
                ),
            }
        }
        Ok(restored)
    }

    /// Start the single consumer of queue-owner backend controls. The daemon
    /// still decides when this feature-gated service is mounted; once it is,
    /// controls remain ordered on the existing backend FIFO.
    pub fn spawn_backend_executor(&self) -> Result<tokio::task::JoinHandle<()>, AdmissionError> {
        let adapter = self
            .engine
            .take_backend_adapter()
            .ok_or(AdmissionError::MissingBackendAdapter)?;
        let registry = self.registry.clone();
        let associations = self.associations.clone();
        let upload_limit_bps = self.upload_limit_bps;
        let engine = self.engine.clone();
        Ok(tokio::spawn(async move {
            run_backend_executor(adapter, registry, associations, upload_limit_bps, engine).await;
        }))
    }

    pub async fn shutdown(&self) {
        self.session.stop().await;
    }

    /// Return the retained descriptor for an authorized live torrent. The
    /// persisted path is always relative and is revalidated at read time so a
    /// corrupted snapshot cannot turn this endpoint into an arbitrary file
    /// read.
    pub async fn export_metainfo(&self, job: JobId) -> Result<Option<Vec<u8>>, AdmissionError> {
        let Some(job) = self.engine.export_job(job).await? else {
            return Ok(None);
        };
        let Some(torrent) = job.torrent else {
            return Ok(None);
        };
        if torrent.metadata_file.is_absolute()
            || torrent
                .metadata_file
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(AdmissionError::Encoding);
        }
        let bytes = std::fs::read(self.state_dir.join(torrent.metadata_file))?;
        Ok(Some(bytes))
    }

    async fn finish(
        &self,
        pending: Option<JobId>,
        bytes: Vec<u8>,
        source: TorrentSource,
        mut opts: AddOpts,
    ) -> Result<AdmissionResult, AdmissionError> {
        if bytes.len() > self.source_fetch_limits.max_metainfo_bytes {
            return Err(AdmissionError::MetainfoTooLarge);
        }
        // Parser diagnostics come from the embedded engine and are not an API
        // contract (and may echo hostile bytes). At this boundary they are a
        // generic client-input failure; named policy errors stay named.
        let descriptor =
            inspect_metainfo(&bytes, self.proxy_enabled, self.dht_enabled).map_err(|error| {
                match error {
                    nzbd_torrent::TorrentError::Engine(_) => AdmissionError::Encoding,
                    other => AdmissionError::Torrent(other),
                }
            })?;
        // Match qBittorrent/*arr category names case-insensitively while
        // retaining the configured spelling in durable queue state.
        if let Some(requested) = opts.category.as_deref() {
            let canonical = self
                .category_seed_policies
                .keys()
                .find(|name| name.eq_ignore_ascii_case(requested))
                .cloned()
                .or_else(|| {
                    self.category_payload_roots
                        .read()
                        .unwrap()
                        .keys()
                        .find(|name| name.eq_ignore_ascii_case(requested))
                        .cloned()
                });
            if let Some(canonical) = canonical {
                opts.category = Some(canonical);
            }
        }
        let category_policy = opts.category.as_ref().and_then(|category| {
            self.category_seed_policies
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(category))
                .map(|(_, policy)| policy)
        });
        let mut seed_policy = nzbd_engine::torrent_runtime::normalized_seed_policy(
            opts.seed_ratio_limit
                .or_else(|| category_policy.and_then(|policy| policy.ratio_limit))
                .or(self.default_seed_policy.ratio_limit),
            opts.seed_time_limit_secs
                .or_else(|| category_policy.and_then(|policy| policy.time_limit_secs))
                .or(self.default_seed_policy.time_limit_secs),
        );
        seed_policy.stop_on_complete = opts.stop_seeding_on_complete.unwrap_or_else(|| {
            category_policy
                .unwrap_or(&self.default_seed_policy)
                .stop_on_complete
        });
        let payload_root = opts
            .category
            .as_ref()
            .and_then(|category| {
                self.category_payload_roots
                    .read()
                    .unwrap()
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(category))
                    .map(|(_, root)| root.clone())
            })
            .unwrap_or_else(|| self.session.output_root().to_path_buf());
        TorrentSession::validate_metainfo_filesystem(&bytes, &payload_root)?;
        let job = match pending {
            Some(job) => job,
            None => {
                self.engine
                    .reserve_torrent_admission(TorrentSource::Metainfo, bytes.clone(), opts.clone())
                    .await?
            }
        };
        let relative = PathBuf::from(format!(
            "torrents/sources/{}.torrent",
            descriptor.info_hash_v1
        ));
        persist_descriptor(&self.state_dir.join(&relative), &bytes)?;
        let record = TorrentRecord {
            info_hash_v1: descriptor.info_hash_v1.clone(),
            source,
            metadata_file: relative,
            payload_root: payload_root.clone(),
            phase: TorrentPhase::Queued,
            control_intent: nzbd_types::TorrentControlIntent::Running,
            removal_intent: None,
            removal_outcome: None,
            removal_confirmed_at_unix: None,
            stop_reason: None,
            files: descriptor
                .files
                .iter()
                .map(|(path, length)| TorrentFileRecord {
                    path: path.clone(),
                    length: *length,
                    selected: true,
                    downloaded_bytes: 0,
                })
                .collect(),
            total_bytes: descriptor.total_bytes,
            selected_bytes: descriptor.total_bytes,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy,
            last_activity_unix: None,
            last_error: None,
        };
        let content_path = payload_root.join(&descriptor.name);
        let content_files = descriptor
            .files
            .iter()
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        let committed = self
            .engine
            .commit_torrent_admission(job, descriptor.name.clone(), opts, record)
            .await?
            .ok_or(AdmissionError::MissingPending)?;
        match committed {
            Err(existing) => Ok(AdmissionResult {
                id: existing,
                created: false,
                info_hash: descriptor.info_hash_v1,
            }),
            Ok(id) => {
                #[cfg(test)]
                if let Some(hook) = &self.before_managed_add {
                    hook()?;
                }
                let identity = self
                    .registry
                    .lock()
                    .await
                    .add_committed(
                        bytes,
                        TorrentAddConfig {
                            paused: true,
                            output_root: Some(payload_root),
                            ..Default::default()
                        },
                    )
                    .await?;
                self.associations.lock().await.insert(
                    id,
                    Association {
                        identity: identity.clone(),
                        applied_pause: None,
                        content_path,
                        files: content_files,
                        uploaded_base: 0,
                        engine_uploaded_origin: None,
                        last_engine_progress: 0,
                        last_engine_uploaded: 0,
                        ready_emitted: false,
                        error_emitted: false,
                    },
                );
                Ok(AdmissionResult {
                    id,
                    created: true,
                    info_hash: identity.info_hash_v1,
                })
            }
        }
    }

    pub async fn scan_watch_once(
        &self,
        dir: &Path,
    ) -> Result<Vec<AdmissionResult>, AdmissionError> {
        let mut entries = std::fs::read_dir(dir)
            .map_err(|e| nzbd_state::StateError::Io {
                op: "read torrent watch directory",
                path: dir.to_path_buf(),
                source: e,
            })?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        let mut results = Vec::new();
        for entry in entries {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("torrent") {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|e| nzbd_state::StateError::Io {
                op: "read torrent watch source",
                path: path.clone(),
                source: e,
            })?;
            let result = match self.admit_raw(bytes, AddOpts::default()).await {
                Ok(result) => result,
                Err(error) if error.is_input_error() => {
                    std::fs::rename(&path, path.with_extension("torrent.rejected")).map_err(
                        |e| nzbd_state::StateError::Io {
                            op: "rename rejected torrent watch source",
                            path: path.clone(),
                            source: e,
                        },
                    )?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let suffix = if result.created {
                "processed"
            } else {
                "duplicate"
            };
            std::fs::rename(&path, path.with_extension(format!("torrent.{suffix}"))).map_err(
                |e| nzbd_state::StateError::Io {
                    op: "rename torrent watch source",
                    path: path.clone(),
                    source: e,
                },
            )?;
            results.push(result);
        }
        Ok(results)
    }
}

async fn run_backend_executor(
    mut adapter: BackendAdapterPort,
    registry: Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
    upload_limit_bps: Option<u64>,
    engine: EngineHandle,
) {
    enum Wake {
        Command(Option<BackendCommand>),
        Poll,
    }

    let mut poll = tokio::time::interval(std::time::Duration::from_secs(1));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    registry
        .lock()
        .await
        .set_upload_limit_bps(rate_limit(upload_limit_bps));
    loop {
        let wake = tokio::select! {
            command = adapter.next_command() => Wake::Command(command),
            _ = poll.tick() => Wake::Poll,
        };
        let command = match wake {
            Wake::Command(Some(command)) => command,
            Wake::Command(None) => break,
            Wake::Poll => {
                publish_backend_state(&adapter, &registry, &associations, &engine).await;
                continue;
            }
        };
        let fact = match command {
            BackendCommand::Start { job } => {
                apply_pause_resume(job, false, StopReason::Paused, &registry, &associations).await
            }
            BackendCommand::Pause { job } => {
                apply_pause_resume(job, true, StopReason::Paused, &registry, &associations).await
            }
            BackendCommand::PauseForSeedPolicy { job } => {
                apply_pause_resume(
                    job,
                    true,
                    StopReason::SeedPolicyReached,
                    &registry,
                    &associations,
                )
                .await
            }
            BackendCommand::PauseForStorage { job } => {
                apply_pause_resume(job, true, StopReason::StorageFull, &registry, &associations)
                    .await
            }
            BackendCommand::PauseForScheduler { job } => {
                apply_pause_resume(
                    job,
                    true,
                    StopReason::SchedulerYield,
                    &registry,
                    &associations,
                )
                .await
            }
            BackendCommand::Resume { job } => {
                apply_pause_resume(job, false, StopReason::Paused, &registry, &associations).await
            }
            BackendCommand::Remove {
                job,
                delete_data,
                content_path,
                files,
                allowed_roots,
            } => {
                apply_remove(
                    job,
                    delete_data,
                    content_path,
                    files,
                    allowed_roots,
                    &registry,
                    &associations,
                )
                .await
            }
            // Transfer limits are session-global; pause, resume, and removal
            // retain per-job ownership through the association table.
            BackendCommand::SetDownloadLimit { bytes_per_sec } => {
                registry
                    .lock()
                    .await
                    .set_download_limit_bps(rate_limit(bytes_per_sec));
                None
            }
            BackendCommand::SetUploadLimit { bytes_per_sec } => {
                registry
                    .lock()
                    .await
                    .set_upload_limit_bps(rate_limit(bytes_per_sec));
                None
            }
            BackendCommand::SetPriority { .. } => None,
        };
        if let Some(fact) = fact {
            if adapter.structural(fact).await.is_err() {
                break;
            }
        }
    }
}

fn rate_limit(bytes_per_sec: Option<u64>) -> Option<std::num::NonZeroU32> {
    bytes_per_sec
        .and_then(|rate| u32::try_from(rate).ok())
        .and_then(std::num::NonZeroU32::new)
}

async fn publish_backend_state(
    adapter: &BackendAdapterPort,
    registry: &Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: &Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
    engine: &EngineHandle,
) {
    let accepted_ready = engine
        .snapshot()
        .jobs
        .iter()
        .filter(|job| job.ready_at_unix.is_some())
        .map(|job| job.id)
        .collect::<std::collections::HashSet<_>>();
    {
        let mut associations = associations.lock().await;
        for (job, association) in associations.iter_mut() {
            // Explicit missing-file recovery revokes historical readiness.
            // Rearm completion delivery until the owner accepts a fresh Ready.
            association.ready_emitted = accepted_ready.contains(job);
        }
    }
    let association_snapshot = associations.lock().await.clone();
    let samples = {
        let registry = registry.lock().await;
        association_snapshot
            .iter()
            .filter_map(|(job, association)| {
                registry
                    .stats(&association.identity)
                    .map(|stats| (*job, stats))
            })
            .collect::<Vec<_>>()
    };

    for (job, stats) in samples {
        let (progress, ready, error) = {
            let mut associations = associations.lock().await;
            let Some(association) = associations.get_mut(&job) else {
                continue;
            };
            let origin = *association
                .engine_uploaded_origin
                .get_or_insert(stats.uploaded_bytes);
            let uploaded_bytes = association
                .uploaded_base
                .saturating_add(stats.uploaded_bytes.saturating_sub(origin));
            let progressed = stats.progress_bytes > association.last_engine_progress
                || stats.uploaded_bytes > association.last_engine_uploaded;
            association.last_engine_progress =
                association.last_engine_progress.max(stats.progress_bytes);
            association.last_engine_uploaded =
                association.last_engine_uploaded.max(stats.uploaded_bytes);
            if stats.error.is_none() {
                association.error_emitted = false;
            }
            let last_activity_unix = (progressed || stats.peers.live > 0).then(unix_now);
            let useful_peers = u32::try_from(stats.peers.live).unwrap_or(u32::MAX);
            let progress = nzbd_engine::backend::TransferProgress {
                downloaded_bytes: stats.progress_bytes,
                verified_bytes: stats.progress_bytes,
                file_progress_bytes: association
                    .files
                    .iter()
                    .map(|path| {
                        stats
                            .content_files
                            .iter()
                            .find(|file| &file.relative_path == path)
                            .map_or(0, |file| file.progress_bytes.min(file.size_bytes))
                    })
                    .collect(),
                uploaded_bytes,
                download_bps: stats.download_bps,
                upload_bps: stats.upload_bps,
                useful_peers,
                last_activity_unix,
            };
            let ready = (stats.finished && !association.ready_emitted)
                .then(|| (association.content_path.clone(), association.files.clone()));
            let error = stats
                .error
                .filter(|_| !association.error_emitted)
                .inspect(|_| {
                    association.error_emitted = true;
                });
            (progress, ready, error)
        };
        adapter.progress(job, progress);

        if let Some((content_path, files)) = ready {
            let durable =
                tokio::task::spawn_blocking(move || durable_content_path(&content_path, &files))
                    .await
                    .ok()
                    .and_then(Result::ok);
            let fact = match durable {
                Some(content_path) => BackendFact::Ready { job, content_path },
                None => BackendFact::Stopped {
                    job,
                    reason: StopReason::MissingContent,
                },
            };
            if adapter.structural(fact).await.is_err() {
                return;
            }
            // The next poll observes the queue owner's durable ready stamp
            // before latching this association. Until then, retrying Ready is
            // intentional: channel delivery is not owner acceptance.
        }

        if let Some(error) = error {
            let reason = if nzbd_engine::is_out_of_space(&error) {
                StopReason::StorageFull
            } else {
                StopReason::Transient
            };
            if adapter
                .structural(BackendFact::Stopped { job, reason })
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn durable_content_path(content_path: &Path, files: &[PathBuf]) -> std::io::Result<PathBuf> {
    let canonical = std::fs::canonicalize(content_path)?;
    if canonical.is_file() {
        std::fs::File::open(&canonical)?.sync_all()?;
    } else {
        for relative in files {
            let file = std::fs::canonicalize(canonical.join(relative))?;
            if !file.starts_with(&canonical) || !file.is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "torrent payload escaped its canonical content root",
                ));
            }
            std::fs::File::open(file)?.sync_all()?;
        }
    }
    #[cfg(unix)]
    if let Some(parent) = canonical.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(canonical)
}

async fn apply_remove(
    job: JobId,
    delete_data: bool,
    content_path: Option<PathBuf>,
    files: Vec<TorrentFileRecord>,
    allowed_roots: Vec<PathBuf>,
    registry: &Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: &Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
) -> Option<BackendFact> {
    let Some(association) = associations.lock().await.get(&job).cloned() else {
        // A missing handle during removal is ambiguous: the prior attempt may
        // have stopped it before crashing. Keep the durable removal intent
        // fenced and retryable; never turn ambiguity into terminal history.
        return Some(BackendFact::Stopped {
            job,
            reason: StopReason::Transient,
        });
    };
    let outcome = if delete_data {
        let Some(content_path) = content_path else {
            return Some(BackendFact::Removed {
                job,
                outcome: RemovalOutcome::RefusedUnsafeRoot,
            });
        };
        match validate_removal_payload(&content_path, &files, &allowed_roots) {
            Ok(()) => {
                let command_path = std::fs::canonicalize(&content_path).ok();
                let owned_path = std::fs::canonicalize(&association.content_path).ok();
                let command_files = files
                    .iter()
                    .map(|file| file.path.clone())
                    .collect::<Vec<_>>();
                if command_path != owned_path || command_files != association.files {
                    return Some(BackendFact::Removed {
                        job,
                        outcome: RemovalOutcome::RefusedInventoryMismatch,
                    });
                }
                RemovalOutcome::DataDeleted
            }
            Err(RemovalRefusal::UnsafeRoot) => RemovalOutcome::RefusedUnsafeRoot,
            Err(RemovalRefusal::InventoryMismatch) => RemovalOutcome::RefusedInventoryMismatch,
        }
    } else {
        RemovalOutcome::DataKept
    };
    if matches!(
        outcome,
        RemovalOutcome::RefusedUnsafeRoot | RemovalOutcome::RefusedInventoryMismatch
    ) {
        return Some(BackendFact::Removed { job, outcome });
    }
    let result = registry
        .lock()
        .await
        .delete(&association.identity, delete_data)
        .await;
    match result {
        Ok(()) | Err(nzbd_torrent::TorrentError::MissingHandle) => {
            associations.lock().await.remove(&job);
            Some(BackendFact::Removed { job, outcome })
        }
        Err(error) => Some(control_error_fact(job, error)),
    }
}

async fn apply_pause_resume(
    job: JobId,
    pause: bool,
    pause_reason: StopReason,
    registry: &Arc<tokio::sync::Mutex<TorrentRegistry>>,
    associations: &Arc<tokio::sync::Mutex<HashMap<JobId, Association>>>,
) -> Option<BackendFact> {
    let Some(association) = associations.lock().await.get(&job).cloned() else {
        // The durable queue commit precedes managed-engine association. A
        // scheduler Start can cross that narrow boundary, so keep it
        // retryable rather than terminally failing a valid admission.
        return Some(BackendFact::Stopped {
            job,
            reason: StopReason::Transient,
        });
    };
    if association.applied_pause == Some(pause) {
        return None;
    }
    let result = {
        let registry = registry.lock().await;
        if pause {
            registry.pause(&association.identity).await
        } else {
            registry.resume(&association.identity).await
        }
    };
    match result {
        Ok(()) => {
            if let Some(association) = associations.lock().await.get_mut(&job) {
                association.applied_pause = Some(pause);
            }
            if pause {
                Some(BackendFact::Stopped {
                    job,
                    reason: pause_reason,
                })
            } else {
                Some(BackendFact::Resumed { job })
            }
        }
        // Never send an engine diagnostic across the owner boundary: it may
        // contain a passkey, query, peer address, or untrusted path. A handle
        // that disappeared is terminal, but rqbit can reject a control while
        // it is still initializing; that is a live, retryable condition.
        Err(error) => Some(control_error_fact(job, error)),
    }
}

fn control_failure(job: JobId) -> BackendFact {
    BackendFact::Failed {
        job,
        error: SafeError::from_redacted("torrent control target is unavailable"),
    }
}

fn control_error_fact(job: JobId, error: nzbd_torrent::TorrentError) -> BackendFact {
    if matches!(error, nzbd_torrent::TorrentError::MissingHandle) {
        control_failure(job)
    } else {
        BackendFact::Stopped {
            job,
            reason: StopReason::Transient,
        }
    }
}

#[derive(Deserialize)]
struct TypedRequest {
    source: TypedSource,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    seed_ratio_limit: Option<f64>,
    #[serde(default)]
    seed_time_limit_secs: Option<u64>,
    #[serde(default)]
    stop_seeding_on_complete: Option<bool>,
    #[serde(default)]
    params: std::collections::BTreeMap<String, String>,
}
#[derive(Deserialize)]
struct TypedSource {
    #[serde(rename = "type")]
    kind: String,
    uri: String,
}

pub fn router(service: TorrentAdmissionService) -> Router {
    let body_limit = service.max_request_body_bytes().saturating_add(64 * 1024);
    Router::new()
        .route(
            "/api/v1/jobs",
            axum::routing::post(post_job).layer(axum::extract::DefaultBodyLimit::max(body_limit)),
        )
        .with_state(service)
}

async fn post_job(
    State(service): State<TorrentAdmissionService>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    service.handle_http_post(headers, body).await
}

impl TorrentAdmissionService {
    pub async fn handle_http_post(&self, headers: HeaderMap, body: axum::body::Bytes) -> Response {
        self.handle_http_post_with_options(headers, body, AddOpts::default())
            .await
    }

    pub async fn handle_http_post_with_options(
        &self,
        headers: HeaderMap,
        body: axum::body::Bytes,
        mut raw_options: AddOpts,
    ) -> Response {
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .unwrap_or("");
        let client = headers
            .get("x-nzbd-client")
            .or_else(|| headers.get(axum::http::header::USER_AGENT))
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        raw_options.client = client.clone();
        let result = match content_type {
            "application/x-bittorrent" => self.admit_raw(body.to_vec(), raw_options).await,
            "application/json" => match serde_json::from_slice::<TypedRequest>(&body) {
                Ok(request) => {
                    if request.params.keys().any(|key| key.starts_with('*')) {
                        return (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            Json(json!({"error":"parameter keys starting with '*' are reserved"})),
                        )
                            .into_response();
                    }
                    let source = match request.source.kind.as_str() {
                        "magnet" => TorrentSource::Magnet,
                        "torrent_url" => TorrentSource::Url,
                        _ => {
                            return (
                                StatusCode::UNPROCESSABLE_ENTITY,
                                Json(json!({"error":"unsupported torrent source type"})),
                            )
                                .into_response()
                        }
                    };
                    self.admit_source(
                        source,
                        request.source.uri,
                        AddOpts {
                            category: request.category,
                            priority: request.priority,
                            paused: request.paused,
                            seed_ratio_limit: request.seed_ratio_limit,
                            seed_time_limit_secs: request.seed_time_limit_secs,
                            stop_seeding_on_complete: request.stop_seeding_on_complete,
                            params: request.params.into_iter().collect(),
                            client,
                            ..Default::default()
                        },
                    )
                    .await
                }
                Err(_) => {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(json!({"error":"invalid typed torrent request"})),
                    )
                        .into_response()
                }
            },
            _ => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"error":"unsupported torrent content type"})),
                )
                    .into_response()
            }
        };
        match result {
            Ok(result) if result.created => (
                StatusCode::CREATED,
                Json(json!({"id":result.id,"info_hash":result.info_hash})),
            )
                .into_response(),
            Ok(result) => (
                StatusCode::OK,
                Json(json!({"id":result.id,"info_hash":result.info_hash,"created":false})),
            )
                .into_response(),
            Err(AdmissionError::Torrent(
                nzbd_torrent::TorrentError::MagnetMetadataTimeout,
            )) => (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({"error":"Magnet metadata could not be resolved within 120 seconds. Check peer availability and DHT or tracker connectivity, then retry."})),
            )
                .into_response(),
            Err(error) if error.is_input_error() => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error":error.to_string()})),
            )
                .into_response(),
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":"torrent admission failed"})),
            )
                .into_response(),
        }
    }
}

impl AdmissionError {
    fn is_input_error(&self) -> bool {
        matches!(self, Self::Encoding | Self::MetainfoTooLarge)
            || matches!(self, Self::Torrent(error) if !matches!(error, nzbd_torrent::TorrentError::Engine(_)))
    }
}

fn deterministic_magnet_recovery_failure(error: &AdmissionError) -> bool {
    match error {
        AdmissionError::Torrent(
            nzbd_torrent::TorrentError::Engine(_)
            | nzbd_torrent::TorrentError::MagnetMetadataTimeout
            | nzbd_torrent::TorrentError::MissingResolvedMagnet,
        ) => false,
        AdmissionError::Torrent(_)
        | AdmissionError::Encoding
        | AdmissionError::MetainfoTooLarge => true,
        AdmissionError::Engine(_)
        | AdmissionError::State(_)
        | AdmissionError::Io(_)
        | AdmissionError::MissingPending
        | AdmissionError::MissingBackendAdapter => false,
    }
}

fn persist_descriptor(path: &Path, bytes: &[u8]) -> Result<(), nzbd_state::StateError> {
    let parent = path.parent().unwrap();
    std::fs::create_dir_all(parent).map_err(|e| nzbd_state::StateError::Io {
        op: "create torrent descriptor directory",
        path: parent.to_path_buf(),
        source: e,
    })?;
    let tmp = path.with_extension("torrent.tmp");
    let mut file = std::fs::File::create(&tmp).map_err(|e| nzbd_state::StateError::Io {
        op: "create torrent descriptor",
        path: tmp.clone(),
        source: e,
    })?;
    use std::io::Write;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| nzbd_state::StateError::Io {
            op: "persist torrent descriptor",
            path: tmp.clone(),
            source: e,
        })?;
    std::fs::rename(&tmp, path).map_err(|e| nzbd_state::StateError::Io {
        op: "rename torrent descriptor",
        path: path.to_path_buf(),
        source: e,
    })?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| nzbd_state::StateError::Io {
            op: "fsync torrent descriptor directory",
            path: parent.to_path_buf(),
            source: e,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use nzbd_engine::backend::backend_channel;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_torrent::{TorrentAddConfig, TorrentSessionConfig};
    use sha1::{Digest, Sha1};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tower::ServiceExt;

    const DHT_SWARM_FILE: &str = "dht-production.bin";

    fn metainfo(name: &[u8]) -> Vec<u8> {
        fn bytes(out: &mut Vec<u8>, value: &[u8]) {
            out.extend_from_slice(value.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(value);
        }
        let mut torrent = b"d4:infod6:lengthi1e4:name".to_vec();
        bytes(&mut torrent, name);
        torrent.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
        torrent.extend_from_slice(&[0; 20]);
        torrent.extend_from_slice(b"ee");
        torrent
    }

    fn swarm_info(payload: &[u8], private: bool) -> (Vec<u8>, [u8; 20]) {
        fn bytes(out: &mut Vec<u8>, value: &[u8]) {
            out.extend_from_slice(value.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(value);
        }
        let mut info = vec![b'd'];
        bytes(&mut info, b"length");
        info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
        bytes(&mut info, b"name");
        bytes(&mut info, DHT_SWARM_FILE.as_bytes());
        bytes(&mut info, b"piece length");
        info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
        bytes(&mut info, b"pieces");
        bytes(&mut info, &Sha1::digest(payload));
        if private {
            bytes(&mut info, b"private");
            info.extend_from_slice(b"i1e");
        }
        info.push(b'e');
        let info_hash = Sha1::digest(&info).into();
        (info, info_hash)
    }

    fn swarm_metainfo(payload: &[u8], private: bool) -> (Vec<u8>, [u8; 20]) {
        fn bytes(out: &mut Vec<u8>, value: &[u8]) {
            out.extend_from_slice(value.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(value);
        }
        let (info, info_hash) = swarm_info(payload, private);
        let mut torrent = vec![b'd'];
        if private {
            bytes(&mut torrent, b"announce");
            bytes(&mut torrent, b"http://127.0.0.1:9/announce");
        }
        bytes(&mut torrent, b"info");
        torrent.extend_from_slice(&info);
        torrent.push(b'e');
        (torrent, info_hash)
    }

    fn peer_handshake(info_hash: [u8; 20]) -> Vec<u8> {
        let mut handshake = Vec::with_capacity(68);
        handshake.push(19);
        handshake.extend_from_slice(b"BitTorrent protocol");
        let mut reserved = [0_u8; 8];
        reserved[5] = 0x10;
        handshake.extend_from_slice(&reserved);
        handshake.extend_from_slice(&info_hash);
        handshake.extend_from_slice(b"-NZ0001-METAPREFLT12");
        assert_eq!(handshake.len(), 68);
        handshake
    }

    fn extended_message(extension_id: u8, payload: &[u8]) -> Vec<u8> {
        let length = u32::try_from(payload.len() + 2).unwrap();
        let mut message = Vec::with_capacity(payload.len() + 6);
        message.extend_from_slice(&length.to_be_bytes());
        message.push(20);
        message.push(extension_id);
        message.extend_from_slice(payload);
        message
    }

    async fn read_peer_message(stream: &mut TcpStream) -> Vec<u8> {
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut message = vec![0_u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut message).await.unwrap();
        message
    }

    fn advertised_metadata_id(payload: &[u8]) -> Option<u8> {
        let marker = b"11:ut_metadatai";
        let start = payload
            .windows(marker.len())
            .position(|window| window == marker)?
            + marker.len();
        let end = payload[start..].iter().position(|byte| *byte == b'e')? + start;
        std::str::from_utf8(&payload[start..end]).ok()?.parse().ok()
    }

    async fn metadata_peer(listener: TcpListener, info: Vec<u8>, info_hash: [u8; 20]) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut incoming_handshake = [0_u8; 68];
        stream.read_exact(&mut incoming_handshake).await.unwrap();
        assert_eq!(&incoming_handshake[28..48], &info_hash);
        stream.write_all(&peer_handshake(info_hash)).await.unwrap();
        let handshake = format!("d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", info.len());
        stream
            .write_all(&extended_message(0, handshake.as_bytes()))
            .await
            .unwrap();
        let response_id = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut response_id = None;
            loop {
                let request = read_peer_message(&mut stream).await;
                if request.starts_with(&[20, 0]) {
                    response_id = advertised_metadata_id(&request[2..]);
                }
                if request.starts_with(&[20, 1]) {
                    break response_id.unwrap();
                }
            }
        })
        .await
        .unwrap();
        let mut response =
            format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
        response.extend_from_slice(&info);
        stream
            .write_all(&extended_message(response_id, &response))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    fn dht_transaction_id(packet: &[u8]) -> [u8; 2] {
        let marker = b"1:t2:";
        let start = packet
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("DHT request did not contain a two-byte transaction id")
            + marker.len();
        [packet[start], packet[start + 1]]
    }

    fn compact_ipv4(address: SocketAddr) -> [u8; 6] {
        let SocketAddr::V4(address) = address else {
            panic!("loopback DHT fixture requires IPv4");
        };
        let mut compact = [0_u8; 6];
        compact[..4].copy_from_slice(&address.ip().octets());
        compact[4..].copy_from_slice(&address.port().to_be_bytes());
        compact
    }

    fn dht_response(
        transaction: [u8; 2],
        node_id: [u8; 20],
        node: SocketAddr,
        peer: Option<SocketAddr>,
    ) -> Vec<u8> {
        let mut response = b"d1:rd2:id20:".to_vec();
        response.extend_from_slice(&node_id);
        if let Some(peer) = peer {
            response.extend_from_slice(b"5:token4:test6:valuesl6:");
            response.extend_from_slice(&compact_ipv4(peer));
            response.extend_from_slice(b"ee");
        } else {
            response.extend_from_slice(b"5:nodes26:");
            response.extend_from_slice(&node_id);
            response.extend_from_slice(&compact_ipv4(node));
            response.push(b'e');
        }
        response.extend_from_slice(b"1:t2:");
        response.extend_from_slice(&transaction);
        response.extend_from_slice(b"1:y1:re");
        response
    }

    async fn dht_swarm_node(
        socket: tokio::net::UdpSocket,
        info_hash: [u8; 20],
        peer: SocketAddr,
        get_peers_count: Arc<AtomicUsize>,
    ) {
        let node_id = [0x24; 20];
        let node = socket.local_addr().unwrap();
        let mut packet = [0_u8; 2048];
        loop {
            let (length, source) = socket.recv_from(&mut packet).await.unwrap();
            let request = &packet[..length];
            let transaction = dht_transaction_id(request);
            let discovered_peer = if request
                .windows(b"9:get_peers".len())
                .any(|window| window == b"9:get_peers")
            {
                let marker = b"9:info_hash20:";
                let start = request
                    .windows(marker.len())
                    .position(|window| window == marker)
                    .expect("get_peers request omitted info_hash")
                    + marker.len();
                assert_eq!(&request[start..start + 20], &info_hash);
                get_peers_count.fetch_add(1, Ordering::SeqCst);
                Some(peer)
            } else {
                None
            };
            socket
                .send_to(
                    &dht_response(transaction, node_id, node, discovered_peer),
                    source,
                )
                .await
                .unwrap();
        }
    }

    async fn empty_dht_node(
        socket: tokio::net::UdpSocket,
        get_peers_count: Arc<AtomicUsize>,
        first_get_peers: Option<tokio::sync::oneshot::Sender<()>>,
    ) {
        let node_id = [0x25; 20];
        let node = socket.local_addr().unwrap();
        let mut packet = [0_u8; 2048];
        let mut first_get_peers = first_get_peers;
        loop {
            let (length, source) = socket.recv_from(&mut packet).await.unwrap();
            let request = &packet[..length];
            if request
                .windows(b"9:get_peers".len())
                .any(|window| window == b"9:get_peers")
            {
                get_peers_count.fetch_add(1, Ordering::SeqCst);
                if let Some(ready) = first_get_peers.take() {
                    let _ = ready.send(());
                }
            }
            let transaction = dht_transaction_id(request);
            socket
                .send_to(&dht_response(transaction, node_id, node, None), source)
                .await
                .unwrap();
        }
    }

    async fn service(tmp: &tempfile::TempDir) -> (TorrentAdmissionService, EngineHandle) {
        let state = tmp.path().join("state");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state.clone(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let session =
            TorrentSession::start(tmp.path().join("payload"), TorrentSessionConfig::default())
                .await
                .unwrap();
        (
            TorrentAdmissionService::new(engine.clone(), session, state, false, false),
            engine,
        )
    }

    async fn next_fact(owner: &mut nzbd_engine::backend::BackendOwnerPort) -> BackendFact {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Ok(fact) = owner.try_structural() {
                    return fact;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("backend executor did not emit a structural fact")
    }

    #[test]
    fn transient_control_errors_do_not_terminally_fail_the_job() {
        let job = JobId(7);
        assert_eq!(
            control_error_fact(
                job,
                nzbd_torrent::TorrentError::Engine("initializing".into())
            ),
            BackendFact::Stopped {
                job,
                reason: StopReason::Transient,
            }
        );
        assert_eq!(
            control_error_fact(job, nzbd_torrent::TorrentError::MissingHandle),
            BackendFact::Failed {
                job,
                error: SafeError::from_redacted("torrent control target is unavailable"),
            }
        );
    }

    #[test]
    fn recovery_keeps_transient_magnet_failures_and_reaps_policy_failures() {
        assert!(!deterministic_magnet_recovery_failure(
            &AdmissionError::Torrent(nzbd_torrent::TorrentError::MagnetMetadataTimeout)
        ));
        assert!(!deterministic_magnet_recovery_failure(
            &AdmissionError::Torrent(nzbd_torrent::TorrentError::Engine(
                "peer stream exhausted".into()
            ))
        ));
        assert!(deterministic_magnet_recovery_failure(
            &AdmissionError::Torrent(nzbd_torrent::TorrentError::PrivateMetainfoWithDht)
        ));
        assert!(deterministic_magnet_recovery_failure(
            &AdmissionError::Torrent(nzbd_torrent::TorrentError::MagnetDiscoveryUnavailable)
        ));
        assert!(deterministic_magnet_recovery_failure(
            &AdmissionError::Torrent(nzbd_torrent::TorrentError::InvalidResolvedMagnetMetadata)
        ));
    }

    #[tokio::test]
    async fn native_seed_policy_edit_and_defaults_preserve_explicit_intent() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let service = service.with_transfer_policy(
            SeedPolicy {
                stop_on_complete: true,
                ratio_limit: Some(2.0),
                time_limit_secs: Some(3600),
            },
            HashMap::from([(
                "Linux".into(),
                SeedPolicy {
                    stop_on_complete: false,
                    ratio_limit: Some(0.0),
                    time_limit_secs: None,
                },
            )]),
            None,
        );
        assert!(service.seed_defaults(None).stop_on_complete);
        let category = service.seed_defaults(Some("linux"));
        assert!(!category.stop_on_complete);
        assert_eq!(
            category.ratio_limit, None,
            "category zero explicitly overrides the global ratio"
        );
        assert_eq!(category.time_limit_secs, Some(3600));
        let added = service
            .admit_raw(
                metainfo(b"policy"),
                AddOpts {
                    stop_seeding_on_complete: Some(false),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let original = engine
            .export_job(added.id)
            .await
            .unwrap()
            .unwrap()
            .torrent
            .unwrap();
        assert!(
            !original.seed_policy.stop_on_complete,
            "per-add false overrides the global stop policy"
        );
        let app = crate::router(engine.clone());
        for (body, expected) in [
            (json!({"ratio_limit": -1}), StatusCode::UNPROCESSABLE_ENTITY),
            (
                json!({"time_limit_secs": 0}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (json!({"stop_on_complete": true}), StatusCode::OK),
        ] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method("PUT")
                        .uri(format!("/api/v1/jobs/{}/torrent/seed-policy", added.id.0))
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
        let job = engine.export_job(added.id).await.unwrap().unwrap();
        assert!(job.torrent.unwrap().seed_policy.stop_on_complete);
        assert!(
            engine
                .snapshot()
                .jobs
                .iter()
                .find(|j| j.id == added.id)
                .unwrap()
                .seed_policy
                .unwrap()
                .stop_on_complete
        );
        // A previously completed association must report completion again
        // when explicit recovery has revoked the owner's ready stamp.
        service
            .associations
            .lock()
            .await
            .get_mut(&added.id)
            .unwrap()
            .ready_emitted = true;
        let (_owner, adapter) = nzbd_engine::backend::backend_channel(1, 1);
        publish_backend_state(&adapter, &service.registry, &service.associations, &engine).await;
        assert!(!service.associations.lock().await[&added.id].ready_emitted);
        engine.shutdown().await;
        service.session.stop().await;
    }

    #[tokio::test]
    async fn backend_executor_pauses_and_resumes_once_per_state_change() {
        let tmp = tempfile::tempdir().unwrap();
        let session =
            TorrentSession::start(tmp.path().join("payload"), TorrentSessionConfig::default())
                .await
                .unwrap();
        let mut registry = TorrentRegistry::new(session.clone());
        let identity = registry
            .add_committed(
                metainfo(b"executor.bin"),
                TorrentAddConfig {
                    paused: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let registry = Arc::new(tokio::sync::Mutex::new(registry));
        let associations = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
            JobId(7),
            Association {
                identity: identity.clone(),
                applied_pause: None,
                content_path: tmp.path().join("payload/executor.bin"),
                files: vec![PathBuf::from("executor.bin")],
                uploaded_base: 0,
                engine_uploaded_origin: None,
                last_engine_progress: 0,
                last_engine_uploaded: 0,
                ready_emitted: false,
                error_emitted: false,
            },
        )])));
        let (mut owner, adapter) = backend_channel(8, 8);
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            tmp.path().join("executor-state"),
            tmp.path().join("executor-dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let executor = tokio::spawn(run_backend_executor(
            adapter,
            registry.clone(),
            associations,
            None,
            engine.clone(),
        ));

        owner
            .try_command(BackendCommand::Pause { job: JobId(7) })
            .unwrap();
        assert_eq!(
            next_fact(&mut owner).await,
            BackendFact::Stopped {
                job: JobId(7),
                reason: StopReason::Paused,
            }
        );
        assert_eq!(registry.lock().await.is_paused(&identity), Some(true));

        owner
            .try_command(BackendCommand::Pause { job: JobId(7) })
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), next_fact(&mut owner))
                .await
                .is_err()
        );

        owner
            .try_command(BackendCommand::Resume { job: JobId(7) })
            .unwrap();
        assert_eq!(
            next_fact(&mut owner).await,
            BackendFact::Resumed { job: JobId(7) }
        );
        assert_eq!(registry.lock().await.is_paused(&identity), Some(false));

        owner
            .try_command(BackendCommand::Resume { job: JobId(7) })
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), next_fact(&mut owner))
                .await
                .is_err()
        );

        owner
            .try_command(BackendCommand::Pause { job: JobId(8) })
            .unwrap();
        assert_eq!(
            next_fact(&mut owner).await,
            BackendFact::Stopped {
                job: JobId(8),
                reason: StopReason::Transient,
            }
        );

        drop(owner);
        executor.await.unwrap();
        engine.shutdown().await;
        session.stop().await;
    }

    #[tokio::test]
    async fn raw_route_commits_descriptor_before_managed_add_and_deduplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let body = metainfo(b"payload.bin");

        let response = router(service.clone())
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/x-bittorrent")
                    .body(axum::body::Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let first: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        let torrent = persisted.jobs[0].torrent.as_ref().unwrap();
        assert!(tmp
            .path()
            .join("state")
            .join(&torrent.metadata_file)
            .exists());
        assert!(persisted.pending_admissions.is_empty());

        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/x-bittorrent")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let duplicate: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(duplicate["id"], first["id"]);
        assert_eq!(duplicate["created"], false);
        assert_eq!(engine.snapshot().jobs.len(), 1);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn managed_add_failure_observes_descriptor_and_queue_already_durable() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut service, engine) = service(&tmp).await;
        let state = tmp.path().join("state");
        let observed_state = state.clone();
        service.before_managed_add = Some(Arc::new(move || {
            let persisted = nzbd_state::SnapshotStore::open(&observed_state)?
                .load()?
                .expect("queue commit must precede managed add");
            let torrent = persisted.jobs[0]
                .torrent
                .as_ref()
                .expect("torrent row must precede managed add");
            assert!(observed_state.join(&torrent.metadata_file).exists());
            Err(AdmissionError::MissingPending)
        }));

        let error = service
            .admit_raw(metainfo(b"ordered.bin"), AddOpts::default())
            .await
            .unwrap_err();
        assert!(matches!(error, AdmissionError::MissingPending));
        assert_eq!(engine.snapshot().jobs.len(), 1);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_input_is_422_without_a_live_or_pending_job() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/x-bittorrent")
                    .body(axum::body::Body::from("not bencode"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(engine.snapshot().jobs.is_empty());
        assert!(nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap_or_default()
            .pending_admissions
            .is_empty());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn trackerless_magnet_without_dht_is_rejected_before_reservation() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let request = serde_json::json!({
            "source": {
                "type": "magnet",
                "uri": "magnet:?xt=urn:btih:0000000000000000000000000000000000000000&dn=offline"
            }
        });
        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("no usable peer source while DHT is disabled"));
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap_or_default();
        assert!(persisted.pending_admissions.is_empty());
        assert!(PendingSourceStore::open(&tmp.path().join("state"))
            .unwrap()
            .inventory()
            .unwrap()
            .is_empty());
        engine.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pending_public_magnet_recovers_through_dht_and_transfers() {
        let tmp = tempfile::tempdir().unwrap();
        let payload = (0..64 * 1024)
            .map(|index| ((index * 29 + 11) % 251) as u8)
            .collect::<Vec<_>>();
        let (torrent, info_hash) = swarm_metainfo(&payload, false);

        let seed_root = tmp.path().join("seed");
        std::fs::create_dir_all(&seed_root).unwrap();
        std::fs::write(seed_root.join(DHT_SWARM_FILE), &payload).unwrap();
        let seeder = TorrentSession::start_with_dht_bootstrap_for_test(
            seed_root,
            TorrentSessionConfig {
                listen_port_range: Some(30_000..60_000),
                ..Default::default()
            },
            Vec::new(),
        )
        .await
        .unwrap();
        let seed_port = seeder.tcp_listen_port().unwrap();
        let seed = seeder
            .add_metainfo(
                torrent,
                TorrentAddConfig {
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            seed.wait_until_completed(),
        )
        .await
        .expect("loopback seeder hash check timed out")
        .unwrap();

        let dht_socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let dht_address = dht_socket.local_addr().unwrap();
        let get_peers_count = Arc::new(AtomicUsize::new(0));
        let dht_task = tokio::spawn(dht_swarm_node(
            dht_socket,
            info_hash,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, seed_port)),
            get_peers_count.clone(),
        ));

        let state = tmp.path().join("state");
        let payload_root = tmp.path().join("payload");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state.clone(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let session = TorrentSession::start_with_dht_bootstrap_for_test(
            payload_root.clone(),
            TorrentSessionConfig {
                dht: true,
                ..Default::default()
            },
            vec![dht_address],
        )
        .await
        .unwrap();
        let service =
            TorrentAdmissionService::new(engine.clone(), session, state.clone(), false, true);
        let executor = service.spawn_backend_executor().unwrap();
        let magnet = format!(
            "magnet:?xt=urn:btih:{}&dn=production",
            info_hash
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let pending = engine
            .reserve_torrent_admission(
                TorrentSource::Magnet,
                magnet.into_bytes(),
                AddOpts::default(),
            )
            .await
            .unwrap();
        let recovered = service.recover().await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, pending);
        assert!(recovered[0].created);

        let downloaded = payload_root.join(DHT_SWARM_FILE);
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                if std::fs::read(&downloaded).is_ok_and(|bytes| bytes == payload) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("production DHT magnet transfer timed out");
        assert!(
            get_peers_count.load(Ordering::SeqCst) >= 2,
            "production admission must rediscover a peer after metadata-only resolution"
        );
        let persisted = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(persisted.pending_admissions.is_empty());
        assert_eq!(persisted.jobs.len(), 1);

        service.shutdown().await;
        seeder.stop().await;
        dht_task.abort();
        engine.shutdown().await;
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), executor).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dht_resolved_private_magnet_is_rejected_and_cleans_pending_state() {
        let tmp = tempfile::tempdir().unwrap();
        let payload = b"private-over-public-discovery".to_vec();
        let (info, info_hash) = swarm_info(&payload, true);
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let peer_address = peer_listener.local_addr().unwrap();
        let peer_task = tokio::spawn(metadata_peer(peer_listener, info, info_hash));

        let dht_socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let dht_address = dht_socket.local_addr().unwrap();
        let get_peers_count = Arc::new(AtomicUsize::new(0));
        let dht_task = tokio::spawn(dht_swarm_node(
            dht_socket,
            info_hash,
            peer_address,
            get_peers_count.clone(),
        ));

        let state = tmp.path().join("private-state");
        let payload_root = tmp.path().join("private-payload");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state.clone(),
            tmp.path().join("private-dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let session = TorrentSession::start_with_dht_bootstrap_for_test(
            payload_root.clone(),
            TorrentSessionConfig {
                dht: true,
                ..Default::default()
            },
            vec![dht_address],
        )
        .await
        .unwrap();
        let service =
            TorrentAdmissionService::new(engine.clone(), session, state.clone(), false, true);
        let magnet = format!(
            "magnet:?xt=urn:btih:{}",
            info_hash
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            router(service.clone()).oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({"source":{"type":"magnet","uri":magnet}}).to_string(),
                    ))
                    .unwrap(),
            ),
        )
        .await
        .expect("private DHT magnet admission timed out")
        .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("private and cannot be added while DHT is enabled"));
        assert!(get_peers_count.load(Ordering::SeqCst) >= 1);
        let persisted = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(persisted.pending_admissions.is_empty());
        assert!(persisted.jobs.is_empty());
        assert!(PendingSourceStore::open(&state)
            .unwrap()
            .inventory()
            .unwrap()
            .is_empty());
        assert_eq!(std::fs::read_dir(&payload_root).unwrap().count(), 0);
        tokio::time::timeout(std::time::Duration::from_secs(5), peer_task)
            .await
            .expect("private metadata peer did not finish")
            .unwrap();

        service.shutdown().await;
        dht_task.abort();
        engine.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dht_timeout_stops_lookup_cleans_explicit_admission_and_keeps_recovery_pending() {
        let tmp = tempfile::tempdir().unwrap();
        let dht_socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let dht_address = dht_socket.local_addr().unwrap();
        let get_peers_count = Arc::new(AtomicUsize::new(0));
        let (first_query_sent, first_query_received) = tokio::sync::oneshot::channel();
        let timeout_gate = Arc::new(tokio::sync::Notify::new());
        let dht_task = tokio::spawn(empty_dht_node(
            dht_socket,
            get_peers_count.clone(),
            Some(first_query_sent),
        ));

        let state = tmp.path().join("timeout-state");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state.clone(),
            tmp.path().join("timeout-dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let session = TorrentSession::start_with_dht_bootstrap_for_test(
            tmp.path().join("timeout-payload"),
            TorrentSessionConfig {
                dht: true,
                ..Default::default()
            },
            vec![dht_address],
        )
        .await
        .unwrap()
        .with_magnet_metadata_timeout_for_test(std::time::Duration::from_millis(300))
        .with_magnet_metadata_timeout_gate_for_test(timeout_gate.clone());
        let service =
            TorrentAdmissionService::new(engine.clone(), session, state.clone(), false, true);
        let magnet = "magnet:?xt=urn:btih:1111111111111111111111111111111111111111";
        let request = axum::http::Request::post("/api/v1/jobs")
            .header(CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(
                json!({"source":{"type":"magnet","uri":magnet}}).to_string(),
            ))
            .unwrap();
        let request_task = tokio::spawn(router(service.clone()).oneshot(request));
        tokio::time::timeout(std::time::Duration::from_secs(5), first_query_received)
            .await
            .expect("timed out before DHT issued its first get_peers request")
            .expect("DHT node stopped before reporting its first get_peers request");
        timeout_gate.notify_one();
        let response = tokio::time::timeout(std::time::Duration::from_secs(5), request_task)
            .await
            .expect("metadata resolution did not time out after the DHT request")
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let snapshot = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(snapshot.pending_admissions.is_empty());
        assert!(PendingSourceStore::open(&state)
            .unwrap()
            .inventory()
            .unwrap()
            .is_empty());
        let queries_after_timeout = get_peers_count.load(Ordering::SeqCst);
        assert!(queries_after_timeout >= 1);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(
            get_peers_count.load(Ordering::SeqCst),
            queries_after_timeout,
            "dropping the timed-out resolution must stop its DHT request stream"
        );

        let pending = engine
            .reserve_torrent_admission(
                TorrentSource::Magnet,
                magnet.as_bytes().to_vec(),
                AddOpts::default(),
            )
            .await
            .unwrap();
        assert!(service.recover().await.unwrap().is_empty());
        let snapshot = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.pending_admissions.len(), 1);
        assert_eq!(snapshot.pending_admissions[0].job_id, pending);
        assert_eq!(
            PendingSourceStore::open(&state)
                .unwrap()
                .inventory()
                .unwrap(),
            vec![pending]
        );

        service.shutdown().await;
        dht_task.abort();
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn typed_http_source_uses_the_same_durable_admission_path() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let tmp = tempfile::tempdir().unwrap();
        let body = metainfo(b"fetched.bin");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&body).await.unwrap();
        });
        let (service, engine) = service(&tmp).await;
        let request = serde_json::json!({
            "source": {"type": "torrent_url", "uri": format!("http://{address}/source?passkey=secret")},
            "category": "test"
        });
        let response = router(service)
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .header(CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        server.await.unwrap();
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(persisted.jobs.len(), 1);
        assert_eq!(persisted.jobs[0].category.as_deref(), Some("test"));
        assert!(persisted.pending_admissions.is_empty());
        let serialized = serde_json::to_string(&persisted).unwrap();
        assert!(!serialized.contains("passkey"));
        assert!(
            nzbd_state::torrent_sources::PendingSourceStore::open(&tmp.path().join("state"))
                .unwrap()
                .inventory()
                .unwrap()
                .is_empty()
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn category_root_is_validated_instead_of_the_unused_default_root() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let category_root = tmp.path().join("category-payload");
        std::fs::create_dir_all(&category_root).unwrap();
        std::fs::create_dir_all(tmp.path().join("payload/category.bin")).unwrap();
        let service = service.with_category_payload_roots(HashMap::from([(
            "Movies".to_string(),
            category_root.clone(),
        )]));

        let result = service
            .admit_raw(
                metainfo(b"category.bin"),
                AddOpts {
                    category: Some("movies".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("unused default-root collision must not reject category admission");
        assert!(result.created);
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(persisted.jobs.len(), 1);
        assert_eq!(persisted.jobs[0].category.as_deref(), Some("Movies"));
        assert_eq!(
            persisted.jobs[0].torrent.as_ref().unwrap().payload_root,
            category_root
        );

        service.shutdown().await;
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn category_root_collision_is_rejected_before_durable_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let category_root = tmp.path().join("category-payload");
        std::fs::create_dir_all(category_root.join("collision.bin")).unwrap();
        let service = service
            .with_category_payload_roots(HashMap::from([("Movies".to_string(), category_root)]));

        let error = service
            .admit_raw(
                metainfo(b"collision.bin"),
                AddOpts {
                    category: Some("movies".into()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("category-root type collision must be rejected");
        assert!(matches!(
            error,
            AdmissionError::Torrent(nzbd_torrent::TorrentError::ExistingPathType(_))
        ));
        let persisted = nzbd_state::SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap_or_default();
        assert!(persisted.jobs.is_empty());
        assert!(persisted.pending_admissions.is_empty());

        service.shutdown().await;
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn recover_resumes_a_durable_http_intent_and_reaps_orphans() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let tmp = tempfile::tempdir().unwrap();
        let body = metainfo(b"recovered.bin");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&body).await.unwrap();
        });
        let (service, engine) = service(&tmp).await;
        let state = tmp.path().join("state");
        let secret = format!("http://{address}/source?passkey=restart-secret");
        let job = engine
            .reserve_torrent_admission(TorrentSource::Url, secret.into_bytes(), AddOpts::default())
            .await
            .unwrap();
        let store = PendingSourceStore::open(&state).unwrap();
        store.write(JobId(999), b"orphan-secret").unwrap();

        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(state.join("queue.json")).unwrap()).unwrap();
        assert_eq!(raw["schema_version"], nzbd_state::QUEUE_SCHEMA_VERSION);
        assert_eq!(raw["pending_admissions"][0]["job_id"], job.0);
        assert_eq!(raw["pending_admissions"][0]["source"], "url");
        assert_eq!(
            raw["pending_admissions"][0]["secret_ref"],
            format!("torrents/pending/{}.source", job.0)
        );
        assert!(!raw.to_string().contains("restart-secret"));

        // Boot restores local state first; remote source fetches can wait
        // until after the API has started accepting health probes.
        tokio::time::timeout(std::time::Duration::from_secs(1), service.recover_active())
            .await
            .expect("local recovery stalled on a network source")
            .unwrap();
        assert_eq!(store.inventory().unwrap(), vec![job]);
        assert_eq!(
            nzbd_state::SnapshotStore::open(&state)
                .unwrap()
                .load()
                .unwrap()
                .unwrap()
                .pending_admissions
                .len(),
            1
        );
        let recovered = service.recover_pending().await.unwrap();
        server.await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, job);
        assert!(store.inventory().unwrap().is_empty());
        let persisted = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(persisted.pending_admissions.is_empty());
        assert_eq!(persisted.jobs[0].id, job);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn recover_removes_a_deterministically_unusable_pending_magnet() {
        let tmp = tempfile::tempdir().unwrap();
        let (service, engine) = service(&tmp).await;
        let state = tmp.path().join("state");
        let job = engine
            .reserve_torrent_admission(
                TorrentSource::Magnet,
                b"magnet:?xt=urn:btih:0000000000000000000000000000000000000000".to_vec(),
                AddOpts::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            PendingSourceStore::open(&state)
                .unwrap()
                .inventory()
                .unwrap(),
            vec![job]
        );

        assert!(service.recover().await.unwrap().is_empty());
        let persisted = nzbd_state::SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(persisted.pending_admissions.is_empty());
        assert!(PendingSourceStore::open(&state)
            .unwrap()
            .inventory()
            .unwrap()
            .is_empty());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn watch_rejects_a_bad_entry_and_continues_to_the_next() {
        let tmp = tempfile::tempdir().unwrap();
        let watch = tmp.path().join("watch");
        std::fs::create_dir(&watch).unwrap();
        std::fs::write(watch.join("a.torrent"), b"bad").unwrap();
        std::fs::write(watch.join("b.torrent"), metainfo(b"watch.bin")).unwrap();
        let (service, engine) = service(&tmp).await;

        let results = service.scan_watch_once(&watch).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(watch.join("a.torrent.rejected").exists());
        assert!(watch.join("b.torrent.processed").exists());
        assert!(service.scan_watch_once(&watch).await.unwrap().is_empty());
        assert_eq!(engine.snapshot().jobs.len(), 1);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn watch_leaves_valid_input_in_place_after_an_internal_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let watch = tmp.path().join("watch");
        std::fs::create_dir(&watch).unwrap();
        let source = watch.join("retry.torrent");
        std::fs::write(&source, metainfo(b"retry.bin")).unwrap();
        let (service, engine) = service(&tmp).await;
        engine.shutdown().await;

        let error = service.scan_watch_once(&watch).await.unwrap_err();
        assert!(matches!(error, AdmissionError::Engine(_)));
        assert!(source.exists());
        assert!(!watch.join("retry.torrent.rejected").exists());
    }

    #[test]
    fn internal_errors_are_opaque_server_failures() {
        assert!(!AdmissionError::MissingPending.is_input_error());
        assert!(!AdmissionError::Torrent(nzbd_torrent::TorrentError::Engine(
            "magnet:?xt=secret".into()
        ))
        .is_input_error());
        assert!(AdmissionError::Encoding.is_input_error());
    }
}
