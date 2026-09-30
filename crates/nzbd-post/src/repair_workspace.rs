//! Damaged PAR candidates repair in a journal-owned workspace; originals stay.
use crate::{par2::Par2Set, tools::Par2Tool, PostError, RepairResult, VerifyResult};
use std::path::Path;

pub async fn repair(
    inventory: &nzbd_state::artifacts::Inventory,
    job: u32,
    set: &Par2Set,
    tool: &Par2Tool,
) -> Result<VerifyResult, PostError> {
    let token = set
        .set_id
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let mut workspace = inventory
        .workspace(job, "par_repair", &token)
        .map_err(|e| PostError::Subprocess(e.to_string()))?;
    let root = workspace.scratch.path.clone();
    let bytes = set.files.iter().map(|f| f.length).sum();
    let _capacity = nzbd_state::capacity::reserve(&root, bytes)?;
    let candidates = crate::namespace::files(&set.root)?;
    let mut mappings = Vec::new();
    for file in &set.files {
        let relative = crate::namespace::relative(&file.name)?;
        let target = root.join(&relative);
        let mut ranked = Vec::new();
        for candidate in &candidates {
            // A resumed workspace must re-establish original custody from the
            // captured source manifest, not adopt newly published outputs.
            if !candidate
                .strip_prefix(&workspace.source.path)
                .is_ok_and(|p| {
                    workspace
                        .source
                        .files
                        .iter()
                        .any(|entry| entry.path == p.to_string_lossy())
                })
            {
                continue;
            }
            if candidate
                .extension()
                .is_some_and(|e| e == "part" || e == "par2")
            {
                continue;
            }
            if !std::fs::metadata(candidate).is_ok_and(|m| m.len() == file.length) {
                continue;
            }
            let full = crate::rename::full_md5(candidate);
            let score = if full == Some(file.md5_full) {
                usize::MAX
            } else {
                matching_blocks(candidate, file, set.slice_size)?
            };
            if score > 0 {
                ranked.push((score, candidate));
            }
        }
        ranked.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
        let Some((score, source)) = ranked.first() else {
            continue;
        };
        if ranked.get(1).is_some_and(|(next, _)| next == score) {
            return Err(PostError::Subprocess(
                "ambiguous PAR candidate mapping; review required".into(),
            ));
        }
        if let Some(parent) = relative.parent() {
            nzbd_state::fileops::parents(&root, parent)
                .map_err(|e| PostError::Subprocess(e.to_string()))?;
        }
        mappings.push(source.to_path_buf());
        if !target.exists() {
            nzbd_state::fileops::copy_publish(source, &target)
                .map_err(|e| PostError::Subprocess(e.to_string()))?;
        }
    }
    for path in &set.par_paths {
        let target = root.join(
            path.file_name()
                .ok_or_else(|| PostError::Subprocess("PAR index name missing".into()))?,
        );
        nzbd_state::fileops::copy_publish(path, &target)
            .map_err(|e| PostError::Subprocess(e.to_string()))?;
    }
    let main = root.join(
        set.main_path
            .as_ref()
            .ok_or_else(|| PostError::Subprocess("PAR index missing".into()))?
            .file_name()
            .unwrap(),
    );
    let verified = tool.verify_full(&main).await?;
    let repaired = match verified {
        VerifyResult::Intact => true,
        VerifyResult::Repairable { .. } => tool.repair(&main).await? == RepairResult::Repaired,
        other => return Ok(other),
    };
    if !repaired {
        return Ok(VerifyResult::Unrepairable);
    }
    for original in mappings {
        let relative = original
            .strip_prefix(&workspace.source.path)
            .map_err(std::io::Error::other)?;
        inventory
            .retain_transform_original(&mut workspace, &relative.to_string_lossy())
            .map_err(|e| PostError::Subprocess(e.to_string()))?;
    }

    for file in &set.files {
        let relative = crate::namespace::relative(&file.name)?;
        let source = root.join(&relative);
        if std::fs::metadata(&source)?.len() != file.length
            || crate::rename::full_md5(&source) != Some(file.md5_full)
        {
            return Err(PostError::Subprocess(
                "repaired PAR output identity mismatch".into(),
            ));
        }
        let target = set.root.join(&relative);
        if target.exists() && crate::rename::full_md5(&target) == Some(file.md5_full) {
            continue;
        }
        if target.exists() {
            inventory
                .retain_transform_original(&mut workspace, &relative.to_string_lossy())
                .map_err(|e| PostError::Subprocess(e.to_string()))?;
        }
        if let Some(parent) = relative.parent() {
            nzbd_state::fileops::parents(&set.root, parent)
                .map_err(|e| PostError::Subprocess(e.to_string()))?;
        }
        nzbd_state::fileops::copy_publish(&source, &target)
            .map_err(|e| PostError::Subprocess(e.to_string()))?;
    }
    inventory
        .finish_workspace(&workspace)
        .map_err(|e| PostError::Subprocess(e.to_string()))?;
    Ok(VerifyResult::Intact)
}

fn matching_blocks(
    path: &Path,
    file: &crate::par2::Par2File,
    slice: u64,
) -> Result<usize, PostError> {
    use std::io::Read;
    if slice == 0 || slice > 16 * 1024 * 1024 {
        return Err(PostError::Subprocess("PAR block size limit".into()));
    }
    let mut input =
        nzbd_state::fileops::open(path).map_err(|e| PostError::Subprocess(e.to_string()))?;
    let mut buffer = vec![0; slice as usize];
    let mut matching = 0;
    for expected in &file.slice_crcs {
        buffer.fill(0);
        let mut done = 0;
        while done < buffer.len() {
            let n = input.read(&mut buffer[done..])?;
            if n == 0 {
                break;
            }
            done += n;
        }
        if done > 0 && crc32fast::hash(&buffer) == *expected {
            matching += 1;
        }
    }
    Ok(matching)
}
