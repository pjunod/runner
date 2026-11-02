//! Projection of the authoritative consumer admission catalog.
use crate::{owner::QueueCommand, EngineError, EngineHandle};
use nzbd_state::UrlAdmissionReceipt;
use nzbd_types::Job;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

pub(crate) fn identity(receipt: &UrlAdmissionReceipt) -> String {
    format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                receipt.protocol_version,
                &receipt.request_token,
                &receipt.submission_sha256,
                receipt.job_id,
                &receipt.generation,
                &receipt.runner_instance_id,
            ))
            .expect("admission identity serializes")
        )
    )
}
pub(crate) fn valid_page(rows: &[UrlAdmissionReceipt]) -> bool {
    if rows.len() > 100 || serde_json::to_vec(rows).map_or(true, |bytes| bytes.len() > 1024 * 1024)
    {
        return false;
    }
    let mut tokens = std::collections::HashSet::new();
    rows.iter().all(|row| {
        row.protocol_version == 1
            && !row.request_token.is_empty()
            && !row.submission_sha256.is_empty()
            && !row.generation.is_empty()
            && !row.runner_instance_id.is_empty()
            && row.job_id > 0
            && tokens.insert(&row.request_token)
    })
}
pub(crate) fn page(
    rows: &[UrlAdmissionReceipt],
    after: &str,
    limit: usize,
) -> Result<Vec<UrlAdmissionReceipt>, String> {
    if !(1..=100).contains(&limit) || after.len() > 1024 * 1024 {
        return Err("admission catalog page requires limit 1..100 and bounded cursor".into());
    }
    let mut candidates = rows
        .iter()
        .filter(|row| row.request_token.as_str() > after)
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|a, b| a.request_token.cmp(&b.request_token));
    let mut result = Vec::new();
    let mut bytes = 2usize; // JSON array brackets, plus commas below.
    for row in candidates.into_iter().take(limit) {
        let encoded = serde_json::to_vec(row).map_err(|error| error.to_string())?;
        let next = bytes
            .saturating_add(encoded.len())
            .saturating_add(usize::from(!result.is_empty()));
        if next > 1024 * 1024 {
            if result.is_empty() {
                return Err("admission receipt exceeds supported 1MiB catalog page; admission requires review".into());
            }
            break;
        }
        bytes = next;
        result.push(row.clone());
    }
    Ok(result)
}

pub(crate) fn merge_current(
    local: &UrlAdmissionReceipt,
    incoming: &UrlAdmissionReceipt,
) -> UrlAdmissionReceipt {
    if identity(local) != identity(incoming) {
        return incoming.clone();
    }
    let mut merged = incoming.clone();
    // A committed handoff and terminal outcome never move backwards. Source
    // cleanup and Keep remain mutable and are refreshed from the local source.
    if local.custody_state == "curator" {
        merged.custody_state = local.custody_state.clone();
        merged.publication_path = local.publication_path.clone();
        merged.terminal_outcome = local.terminal_outcome.clone();
        merged.outcome = local.outcome.clone();
        merged.source_artifact_id = local.source_artifact_id.clone();
        merged.source_generation = local.source_generation.clone();
        merged.keep_source_on_failure = local.keep_source_on_failure;
        if merged.source_cleanup_status.is_none() {
            merged.source_cleanup_status = local.source_cleanup_status.clone();
        }
    } else if local.terminal_outcome.is_some() && merged.terminal_outcome.is_none() {
        merged.terminal_outcome = local.terminal_outcome.clone();
        merged.outcome = local.outcome.clone();
        merged.failure_stage = local.failure_stage.clone();
        merged.failure_cause = local.failure_cause.clone();
    }
    if local.source_cleanup_status.as_deref() == Some("removed") {
        merged.source_cleanup_status = local.source_cleanup_status.clone();
    }
    merged
}

impl EngineHandle {
    /// Newly local receipts awaiting an authoritative catalog decision.
    /// This reads the bounded pending index, never the settled history.
    pub async fn url_unaccepted_admissions(
        &self,
        limit: usize,
    ) -> Result<Vec<UrlAdmissionReceipt>, EngineError> {
        if !(1..=100).contains(&limit) {
            return Err(EngineError::RequestConflict(
                "unaccepted admission limit must be 1..100".into(),
            ));
        }
        let (reply, result) = oneshot::channel();
        self.send(QueueCommand::UnacceptedUrlAdmissions { limit, reply })
            .await?;
        result
            .await
            .map_err(|_| EngineError::Closed)?
            .map_err(EngineError::RequestConflict)
    }

    /// Read a token-ordered catalog page, including retired admissions.
    /// Advance with the last returned token; an empty page ends the scan.
    pub async fn url_admissions_page(
        &self,
        after_token: &str,
        limit: usize,
    ) -> Result<Vec<UrlAdmissionReceipt>, EngineError> {
        if !(1..=100).contains(&limit) || after_token.len() > 1024 * 1024 {
            return Err(EngineError::RequestConflict(
                "invalid admission page limit/cursor".into(),
            ));
        }
        let (reply, result) = oneshot::channel();
        self.send(QueueCommand::UrlAdmissionsPage {
            after: after_token.into(),
            limit,
            reply,
        })
        .await?;
        result
            .await
            .map_err(|_| EngineError::Closed)?
            .map_err(EngineError::RequestConflict)
    }

    /// Call while the cluster API is fenced, before paging its durable catalog.
    pub async fn begin_url_admission_adoption(&self) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::BeginUrlAdmissionAdoption { reply })
            .await
    }
    pub async fn adopt_url_admissions(
        &self,
        rows: Vec<UrlAdmissionReceipt>,
    ) -> Result<bool, EngineError> {
        if !valid_page(&rows) {
            return Err(EngineError::RequestConflict(
                "admission catalog page exceeds 100 rows/1MiB or has invalid identities".into(),
            ));
        }
        self.roundtrip_bool(|reply| QueueCommand::AdoptUrlAdmissions { rows, reply })
            .await
    }
    /// Prune local admissions absent from the completed authoritative catalog.
    pub async fn finish_url_admission_adoption(&self) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::FinishUrlAdmissionAdoption { reply })
            .await
    }
    /// The caller must have proved this exact admission absent from durable
    /// authority while holding the cluster mutation serial. Never recreates a job.
    pub async fn discard_url_admission(
        &self,
        expected: UrlAdmissionReceipt,
        origin: Option<Job>,
    ) -> Result<bool, EngineError> {
        if !valid_page(std::slice::from_ref(&expected)) {
            return Err(EngineError::RequestConflict(
                "invalid discard admission identity".into(),
            ));
        }
        if origin.as_ref().is_some_and(|origin| {
            serde_json::to_vec(origin).map_or(true, |bytes| bytes.len() > 1024 * 1024)
        }) {
            return Err(EngineError::RequestConflict(
                "discard origin exceeds 1MiB".into(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::CheckDiscardUrlAdmission {
            expected: Box::new(expected.clone()),
            reply: tx,
        })
        .await?;
        if !rx.await.map_err(|_| EngineError::Closed)? {
            return Ok(false);
        }
        if let Some(origin) = origin {
            if origin.id.0 != expected.job_id
                || !origin.params.iter().any(|(key, value)| {
                    key == "monarr-transfer" && value == &expected.request_token
                })
            {
                return Err(EngineError::RequestConflict(
                    "discard origin differs from the unaccepted admission".into(),
                ));
            }
            let store = self.url_resolutions.clone();
            tokio::task::spawn_blocking(move || store.cancel(&origin))
                .await
                .map_err(|_| EngineError::Closed)?
                .map_err(|error| EngineError::Lifecycle(error.to_string()))?;
        }
        self.roundtrip_bool(|reply| QueueCommand::DiscardUrlAdmission {
            expected: Box::new(expected),
            reply,
        })
        .await
    }
    pub async fn adopt_replicated_highwater(
        &self,
        next_job_id: u32,
        next_file_id: u32,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::AdoptReplicatedHighwater {
            next_job_id,
            next_file_id,
            reply,
        })
        .await
    }
    pub async fn set_consumer_instance_id(&self, instance: String) -> Result<bool, EngineError> {
        if instance.is_empty() || instance.len() > 128 || instance.chars().any(char::is_control) {
            return Err(EngineError::RequestConflict(
                "consumer instance must contain 1..128 bytes without control characters".into(),
            ));
        }
        self.roundtrip_bool(|reply| QueueCommand::SetConsumerInstanceId { instance, reply })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn receipt(token: &str, id: u32) -> UrlAdmissionReceipt {
        serde_json::from_value(serde_json::json!({"protocolVersion":1,"requestToken":token,"submissionSha256":"accepted-fingerprint","jobId":id,"generation":format!("generation-{id}"),"runnerInstanceId":"cluster-consumer"})).unwrap()
    }
    #[test]
    fn admission_page_bounds_and_monotonic_handoff_are_explicit() {
        let mut local = receipt("token", 5);
        local.custody_state = "curator".into();
        local.outcome = "completed".into();
        local.terminal_outcome = Some("SUCCESS".into());
        local.publication_path = Some("/publication/exact".into());
        local.keep_source_on_failure = true;
        local.source_cleanup_status = Some("removed".into());
        let merged = merge_current(&local, &receipt("token", 5));
        assert_eq!(merged.custody_state, "curator");
        assert_eq!(merged.publication_path, local.publication_path);
        assert_eq!(merged.terminal_outcome, local.terminal_outcome);
        assert!(merged.keep_source_on_failure);
        assert_eq!(merged.source_cleanup_status.as_deref(), Some("removed"));
        assert!(!valid_page(&vec![receipt("duplicate", 1); 101]));
        let mut oversized = receipt("large", 2);
        oversized.publication_path = Some("x".repeat(1024 * 1024));
        assert!(!valid_page(&[oversized]));
        let rows = vec![
            receipt("z-retired", 3),
            receipt("a-retired", 1),
            receipt("m-retired", 2),
        ];
        let first = page(&rows, "", 2).unwrap();
        assert_eq!(
            first
                .iter()
                .map(|row| row.request_token.as_str())
                .collect::<Vec<_>>(),
            vec!["a-retired", "m-retired"]
        );
        assert_eq!(
            page(&rows, &first.last().unwrap().request_token, 2).unwrap()[0].request_token,
            "z-retired"
        );
        assert!(page(&rows, "z-retired", 2).unwrap().is_empty());
        assert!(page(&rows, "", 101).is_err());
        let mut large = receipt("a", 1);
        large.publication_path = Some("x".repeat(700 * 1024));
        let mut second = large.clone();
        second.request_token = "b".into();
        assert_eq!(page(&[large, second], "", 100).unwrap().len(), 1);
        assert!(valid_page(
            &(1..=100)
                .map(|id| receipt(&format!("token-{id}"), id))
                .collect::<Vec<_>>()
        ));
    }
    #[tokio::test]
    async fn standalone_url_history_never_accumulates_a_cluster_pending_index() {
        let temporary = tempfile::tempdir().unwrap();
        let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
            Vec::new(),
            temporary.path().join("state"),
            temporary.path().join("working"),
            crate::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        for id in 1..=2 {
            let token = format!("standalone-{id}");
            engine
                .add_url(
                    &format!("standalone-{id}"),
                    &format!("http://127.0.0.1:9/{id}.nzb"),
                    crate::AddOpts {
                        params: vec![
                            ("monarr-transfer".into(), token.clone()),
                            ("monarr-delivery".into(), "1".into()),
                        ],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(engine.url_admission(&token).await.unwrap().is_some());
        }
        assert_eq!(engine.url_admissions_page("", 100).await.unwrap().len(), 2);
        assert!(engine
            .url_unaccepted_admissions(100)
            .await
            .unwrap()
            .is_empty());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn public_consumer_identity_and_tombstone_highwater_survive_projection_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
            Vec::new(),
            temporary.path().join("state"),
            temporary.path().join("working"),
            crate::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let storage = engine.artifacts().installation.clone();
        assert!(engine
            .set_consumer_instance_id("stable-cluster-consumer".into())
            .await
            .unwrap());
        assert!(engine.adopt_replicated_highwater(900, 700).await.unwrap());
        engine.adopt_replicated_authority(Vec::new()).await.unwrap();
        assert!(engine.begin_url_admission_adoption().await.unwrap());
        assert!(engine.finish_url_admission_adoption().await.unwrap());
        let id = engine
            .add_url(
                "original",
                "http://127.0.0.1:9/unavailable.nzb",
                crate::AddOpts {
                    params: vec![
                        ("monarr-transfer".into(), "public-identity-token".into()),
                        ("monarr-delivery".into(), "1".into()),
                    ],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(id.0 > 900);
        let admission = engine
            .url_admission("public-identity-token")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            engine.url_unaccepted_admissions(100).await.unwrap().len(),
            1
        );
        assert_eq!(admission.runner_instance_id, "stable-cluster-consumer");
        assert!(admission
            .generation
            .starts_with("stable-cluster-consumer-url-"));
        assert_eq!(engine.artifacts().installation, storage);
        let origin = engine.export_job(id).await.unwrap().unwrap();
        let private_generation = origin
            .params
            .iter()
            .find(|(key, _)| key == "*URL:AdmissionGeneration")
            .map(|(_, value)| value.clone());
        assert!(private_generation.is_some());
        assert!(engine.begin_url_admission_adoption().await.unwrap());
        assert!(engine
            .adopt_url_admissions(vec![receipt("retired-token", 1001)])
            .await
            .unwrap());
        assert!(engine.finish_url_admission_adoption().await.unwrap());
        assert!(engine
            .url_unaccepted_admissions(100)
            .await
            .unwrap()
            .is_empty());
        let next = engine
            .reserve_torrent_admission(
                nzbd_types::TorrentSource::Metainfo,
                b"private-test-descriptor".to_vec(),
                crate::AddOpts::default(),
            )
            .await
            .unwrap();
        assert!(next.0 > 1001);
        assert!(engine.cancel_torrent_admission(next).await.unwrap());
        assert_eq!(
            engine
                .export_job(id)
                .await
                .unwrap()
                .unwrap()
                .params
                .iter()
                .find(|(key, _)| key == "*URL:AdmissionGeneration")
                .map(|(_, value)| value.clone()),
            private_generation
        );
        engine.shutdown().await;
    }
}
