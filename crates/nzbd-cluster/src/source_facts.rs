//! Durable custody facts, never permission to delete another node's source.
use crate::delivery::{AuthoritySourceCopy, AUTHORITY_COPY_PARAM};
use crate::leader::LeaderShared;
use nzbd_types::Job;

pub(crate) fn bind_owned(shared: &LeaderShared, job: &mut Job) -> Result<bool, String> {
    if job
        .params
        .iter()
        .any(|(key, _)| key == AUTHORITY_COPY_PARAM)
        || job.params.iter().any(|(key, _)| key == "*PP_DONE")
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
/// Caller holds mutation_serial and then restores the committed projection.
pub(crate) async fn reconcile(shared: &LeaderShared) -> Result<(), String> {
    let Some(control) = &shared.control else {
        return Ok(());
    };
    let issuer = shared.authority_identity();
    let mut changes = Vec::new();
    let mut fences = std::collections::HashMap::new();
    for local in shared.engine_projection().await?.into_values() {
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
        if crate::control_recovery::business_intent(&local)?
            != crate::control_recovery::business_intent(&durable)?
            || local.control() != durable.control()
            || local.status != durable.status
        {
            continue;
        }
        // Do not invalidate a pre-existing grant or an unacknowledged old
        // completion merely to add metadata. Newly admitted/resolved work is
        // bound before its first grant; legacy proof remains unchanged.
        if control.job_has_work_grant(local.id.0).await? {
            continue;
        }
        if bind_owned(shared, &mut durable)? {
            fences.insert(u64::from(local.id.0), revision);
            changes.push((u64::from(local.id.0), Some(durable)));
        }
    }
    if !changes.is_empty() {
        control
            .commit_job_delta_guarded(&changes, &fences, &[], Some(issuer))
            .await?;
        shared.restore_control_projection().await?;
    }
    Ok(())
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
