//! Durable custody facts, never permission to delete another node's source.
use crate::delivery::{AuthoritySourceCopy, AUTHORITY_COPY_PARAM};
use crate::leader::LeaderShared;
use nzbd_types::{Job, JobId};
use std::collections::HashSet;

struct CandidateId(i64);
impl From<&mut hiqlite::Row<'_>> for CandidateId {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self(row.get("job_id"))
    }
}
impl crate::control::ControlStore {
    /// Routing only: private metadata can exclude already-bound/stamped rows,
    /// but cannot authorize reading or publishing any payload.
    async fn source_fact_candidates(&self) -> Result<Vec<JobId>, String> {
        self.client
            .query_consistent_map::<CandidateId, _>(
                "SELECT job_id FROM control_jobs j WHERE deleted=0 AND NOT EXISTS (
               SELECT 1 FROM json_each(j.intent_json,'$.params') p
               WHERE json_extract(p.value,'$[0]') IN (?1,?2)) ORDER BY job_id",
                hiqlite::params!(AUTHORITY_COPY_PARAM, nzbd_types::PP_DONE_PARAM),
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|row| {
                u32::try_from(row.0)
                    .map(JobId)
                    .map_err(|error| error.to_string())
            })
            .collect()
    }
}

pub(crate) fn bind_owned(shared: &LeaderShared, job: &mut Job) -> Result<bool, String> {
    if job
        .params
        .iter()
        .any(|(key, _)| key == AUTHORITY_COPY_PARAM)
        || job
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
    {
        return Ok(false);
    }
    let inventory = shared.engine.artifacts();
    let Some(artifact) = inventory.for_job(job.id.0).map_err(|e| e.to_string())? else {
        return Ok(false);
    };
    let dirname = nzbd_engine::queue::job_dir_name(job);
    if std::path::Path::new(&dirname).components().count() != 1
        || artifact.path != shared.dest_dir.join(&dirname)
        || !artifact.owned
        || artifact.terminal()
    {
        return Ok(false);
    }
    let Some(copy) = inventory
        .cluster_download_authority_source(job.id.0)
        .map_err(|e| e.to_string())?
    else {
        return Ok(false);
    };
    let value = serde_json::to_string(&AuthoritySourceCopy {
        owner_node: shared.cfg.node_name.clone(),
        copy,
    })
    .map_err(|e| e.to_string())?;
    if value.len() > 8192 {
        return Err("owned source fact exceeds supported8KiB bound".into());
    }
    job.params.push((AUTHORITY_COPY_PARAM.into(), value));
    Ok(true)
}

/// Before scheduler admission, make already-owned writer custody durable.
/// Caller holds mutation_serial and refreshes only the bound job projections.
/// Returned IDs remain ineligible for this pass: recovery may finish after a
/// skipped export, but new custody still needs its own checked binding pass.
pub(crate) async fn reconcile(shared: &LeaderShared) -> Result<HashSet<JobId>, String> {
    let Some(control) = &shared.control else {
        return Ok(HashSet::new());
    };
    let issuer = shared.authority_identity();
    let mut changes = Vec::new();
    let mut fences = std::collections::HashMap::new();
    let candidates = control.source_fact_candidates().await?;
    let mut pending: HashSet<_> = candidates.iter().copied().collect();
    // Snapshot metadata can only postpone an export. Avoid the public export's
    // absence/busy fallback (and whole-row copies) for known recovering rows.
    let available: HashSet<_> = shared
        .engine
        .snapshot()
        .jobs
        .iter()
        .filter(|job| job.verification.is_none())
        .map(|job| job.id)
        .collect();
    for id in candidates {
        if !available.contains(&id) {
            continue;
        }
        let local = match shared.engine.export_job(id).await {
            Ok(Some(local)) => local,
            Ok(None) => continue,
            Err(nzbd_engine::EngineError::Lifecycle(reason))
                if reason == "job verification or writer retirement is still in progress; retry later" => continue,
            Err(error) => return Err(format!("export source binding candidate {}: {error}", id.0)),
        };
        if local
            .params
            .iter()
            .any(|(key, _)| key == AUTHORITY_COPY_PARAM)
        {
            continue;
        }
        let Some((mut durable, revision, _)) = control.current_job_intent(local.id.0).await? else {
            continue;
        };
        if !crate::control_recovery::source_intent_matches(&local, &durable)?
            || local.control() != durable.control()
            || local.status != durable.status
        {
            continue;
        }
        // Do not invalidate a pre-existing grant or an unacknowledged old
        // completion merely to add metadata. Newly admitted/resolved work is
        // bound before its first grant; legacy proof remains unchanged.
        if control.job_has_work_grant(local.id.0).await? {
            pending.remove(&id);
            continue;
        }
        if bind_owned(shared, &mut durable)? {
            fences.insert(u64::from(local.id.0), revision);
            changes.push((u64::from(local.id.0), Some(durable)));
        }
        pending.remove(&id);
    }
    if !changes.is_empty() {
        control
            .commit_job_delta_guarded(&changes, &fences, &[], Some(issuer))
            .await?;
        let bound: Vec<_> = changes
            .iter()
            .map(|(id, _)| JobId(u32::try_from(*id).expect("source fact IDs originate as u32")))
            .collect();
        shared.restore_control_jobs_projection(&bound).await?;
    }
    Ok(pending)
}

#[derive(Clone)]
pub(crate) struct OwnedSourceFact {
    pub dirname: String,
    pub value: String,
}
pub(crate) fn for_resolved(
    shared: &LeaderShared,
    job: &Job,
) -> Result<Option<OwnedSourceFact>, String> {
    let mut next = job.clone();
    if !bind_owned(shared, &mut next)? {
        return Ok(None);
    }
    Ok(next
        .params
        .iter()
        .find(|(key, _)| key == AUTHORITY_COPY_PARAM)
        .map(|(_, value)| OwnedSourceFact {
            dirname: nzbd_engine::queue::job_dir_name(&next),
            value: value.clone(),
        }))
}
