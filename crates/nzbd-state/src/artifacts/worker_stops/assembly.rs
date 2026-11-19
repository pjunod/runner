//! Node-local ownership of the one private output written by an assembly grant.
use super::*;
use std::path::Component;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerAssemblyRetirement {
    pub stop_id: String,
    pub removal_revision: u64,
    pub storage_instance_id: String,
    pub had_output: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct AssemblyWorkspace {
    root: PathBuf,
    root_identity: Identity,
    relative: PathBuf,
    allocation: Option<AssemblyAllocation>,
    retirement: Option<WorkerAssemblyRetirement>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AssemblyAllocation {
    parent: Identity,
    directory: Identity,
    filename: String,
    output: Identity,
}

fn conflict(reason: &str) -> Error {
    Error::Conflict(reason.into())
}

fn assembly_relative(job: u32, lease: &str) -> Result<PathBuf> {
    let prefix = format!("work/job/{job}/file/");
    let (file, fence) = lease
        .strip_prefix(&prefix)
        .and_then(|suffix| suffix.split_once("/assemble@"))
        .ok_or_else(|| conflict("assembly workspace requires the exact file/grant identity"))?;
    let file: u32 = file
        .parse()
        .map_err(|_| conflict("invalid assembly file identity"))?;
    let fence: u64 = fence
        .parse()
        .map_err(|_| conflict("invalid assembly grant fence"))?;
    if file == 0 || fence == 0 || lease != format!("{prefix}{file}/assemble@{fence}") {
        return Err(conflict("noncanonical assembly file/grant identity"));
    }
    Ok(PathBuf::from(format!(
        ".nzbd-cluster/assembly/job-{job}/file-{file}/fence-{fence}"
    )))
}

fn current_root(workspace: &AssemblyWorkspace, configured: &Path) -> Result<File> {
    if workspace.root != configured {
        return Err(conflict(
            "assembly destination changed; reconnect the original configured root",
        ));
    }
    let root = fs::open_dir(configured)?;
    if !workspace
        .root_identity
        .same_object(&fs::identity_for_file(&root)?)
    {
        return Err(conflict(
            "original assembly volume is unavailable or changed",
        ));
    }
    Ok(root)
}

fn same_mount(root: &File, child: &File) -> Result<()> {
    let root = fs::identity_for_file(root)?;
    let child = fs::identity_for_file(child)?;
    if root.device != child.device || root.mount_id != child.mount_id {
        return Err(conflict("assembly workspace crossed its recorded volume"));
    }
    Ok(())
}

fn open_parent(root: &File, relative: &Path, create: bool) -> Result<Option<File>> {
    let mut parent = root.try_clone()?;
    for part in relative.components() {
        let Component::Normal(name) = part else {
            return Err(conflict("invalid assembly workspace parent"));
        };
        if fs::entry_missing_at(&parent, name)? {
            if !create {
                return Ok(None);
            }
            match fs::create_dir_exclusive_at(&parent, Path::new(name)) {
                Ok(()) => fs::sync_directory(&parent)?,
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        let child = fs::open_at(&parent, Path::new(name), true)?;
        same_mount(root, &child)?;
        parent = child;
    }
    Ok(Some(parent))
}

impl Inventory {
    /// Enrollment shares the same commit as the grant's start record. An old
    /// start without this binding cannot adopt a path seen after a restart.
    pub fn begin_worker_assembly_grant(
        &self,
        request: &WorkerStopRequest,
        root: &Path,
    ) -> Result<()> {
        let relative = assembly_relative(request.job, &request.context.lease_id)?;
        let directory = fs::open_dir(root)?;
        let workspace = AssemblyWorkspace {
            root: root.to_owned(),
            root_identity: fs::identity_for_file(&directory)?,
            relative,
            allocation: None,
            retirement: None,
        };
        self.begin_worker_grant_with_assembly(request, Some(workspace))
    }

    /// Create the private output exclusively, and commit its object identity
    /// before returning either descriptor. The reader is the same open object.
    pub fn allocate_worker_assembly(
        &self,
        job: u32,
        context: &WorkerControlContext,
        root: &Path,
        filename: &str,
    ) -> Result<(File, File, PathBuf)> {
        let name = Path::new(filename);
        if !matches!(name.components().next(), Some(Component::Normal(part)) if part == name.as_os_str())
            || name.components().count() != 1
            || filename.contains('\\')
            || filename.len() > 255
        {
            return Err(conflict("assembly output must be one regular filename"));
        }
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let id = key(job, context);
        let mut journal = load(&db, &id)?.ok_or(Error::NotFound)?;
        if journal.phase != "started" || self.worker_grant_is_stopped_unlocked(&db, job, context)? {
            return Err(conflict("assembly grant was stopped before allocation"));
        }
        let workspace = journal
            .assembly
            .as_mut()
            .ok_or_else(|| conflict("assembly enrollment unavailable"))?;
        if workspace.allocation.is_some()
            || workspace.retirement.is_some()
            || workspace.relative != assembly_relative(job, &context.lease_id)?
        {
            return Err(conflict(
                "assembly allocation identity already exists or changed",
            ));
        }
        let volume = current_root(workspace, root)?;
        let parent_relative = workspace
            .relative
            .parent()
            .ok_or_else(|| conflict("assembly workspace has no parent"))?;
        let parent = open_parent(&volume, parent_relative, true)?
            .ok_or_else(|| conflict("assembly workspace parent unavailable"))?;
        let slot = Path::new(
            workspace
                .relative
                .file_name()
                .ok_or_else(|| conflict("assembly workspace has no name"))?,
        );
        fs::create_dir_exclusive_at(&parent, slot)?;
        let directory = fs::open_at(&parent, slot, true)?;
        same_mount(&volume, &directory)?;
        let writer = fs::create_file_exclusive_at(&directory, name)?;
        let reader = fs::open_at(&directory, name, false)?;
        let output = fs::identity_for_file(&writer)?;
        if !reader.metadata()?.is_file() || !output.same_object(&fs::identity_for_file(&reader)?) {
            return Err(conflict("assembly output changed before it could be bound"));
        }
        same_mount(&volume, &writer)?;
        writer.sync_all()?;
        fs::sync_directory(&directory)?;
        fs::sync_directory(&parent)?;
        workspace.allocation = Some(AssemblyAllocation {
            parent: fs::identity_for_file(&parent)?,
            directory: fs::identity_for_file(&directory)?,
            filename: filename.into(),
            output,
        });
        let relative = workspace.relative.clone();
        save(&db, &id, &journal)?;
        Ok((writer, reader, relative))
    }

    /// The caller already joined the assembly and any seal reading its
    /// workspace. Require the matching durable receiver fence before unlink.
    pub fn retire_worker_assembly(
        &self,
        stopped: &WorkerStoppedReceipt,
        removal_revision: u64,
        root: &Path,
    ) -> Result<Option<WorkerAssemblyRetirement>> {
        if stopped.storage_instance_id != self.installation
            || stopped.id != key(stopped.job, &stopped.context)
            || removal_revision <= stopped.context.job_revision
        {
            return Err(conflict(
                "assembly retirement identity differs from its stopped grant",
            ));
        }
        let relative = assembly_relative(stopped.job, &stopped.context.lease_id)?;
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let mut journal = load(&db, &stopped.id)?.ok_or(Error::NotFound)?;
        let exact_fence: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM worker_removal_fences WHERE job=?1 AND incarnation=?2 AND revision=?3)",
            params![stopped.job, &stopped.context.job_incarnation, i64::try_from(removal_revision).map_err(|_| conflict("invalid assembly removal revision"))?],
            |row| row.get(0),
        )?;
        if !exact_fence
            || journal.phase != "stopped"
            || journal
                .receipt
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()?
                != Some(serde_json::to_vec(stopped)?)
        {
            return Err(conflict(
                "assembly workspace is awaiting a durable joined removal fence",
            ));
        }
        let receipt = WorkerAssemblyRetirement {
            stop_id: stopped.id.clone(),
            removal_revision,
            storage_instance_id: self.installation.clone(),
            had_output: journal
                .assembly
                .as_ref()
                .is_some_and(|workspace| workspace.allocation.is_some()),
        };
        let Some(workspace) = journal.assembly.as_mut() else {
            // A start-ordered grant with no local start is positive evidence;
            // a historic start with no allocation journal is indistinguishable.
            return Ok(stopped.never_started.then_some(receipt));
        };
        if let Some(previous) = &workspace.retirement {
            if previous != &receipt {
                return Err(conflict("assembly retirement receipt changed"));
            }
            return Ok(Some(previous.clone()));
        }
        if workspace.relative != relative {
            return Err(conflict("assembly workspace generation changed"));
        }
        let volume = current_root(workspace, root)?;
        retire_workspace(&volume, workspace)?;
        workspace.retirement = Some(receipt.clone());
        save(&db, &stopped.id, &journal)?;
        Ok(Some(receipt))
    }
}

fn retire_workspace(root: &File, workspace: &AssemblyWorkspace) -> Result<()> {
    let relative = &workspace.relative;
    let slot = relative
        .file_name()
        .ok_or_else(|| conflict("assembly workspace has no name"))?;
    let Some(parent) = open_parent(
        root,
        relative
            .parent()
            .ok_or_else(|| conflict("assembly workspace has no parent"))?,
        false,
    )?
    else {
        return if workspace.allocation.is_none() {
            Ok(())
        } else {
            Err(conflict(
                "recorded assembly workspace parent is unavailable",
            ))
        };
    };
    let Some(allocation) = &workspace.allocation else {
        if fs::entry_missing_at(&parent, slot)? {
            return fs::sync_directory(&parent);
        }
        return Err(conflict(
            "an unrecorded assembly workspace occupies this exact grant slot; review its custody",
        ));
    };
    if !allocation
        .parent
        .same_object(&fs::identity_for_file(&parent)?)
    {
        return Err(conflict("recorded assembly workspace parent changed"));
    }
    if fs::entry_missing_at(&parent, slot)? {
        return fs::sync_directory(&parent);
    }
    let directory = fs::open_at(&parent, Path::new(slot), true)?;
    if !allocation
        .directory
        .same_object(&fs::identity_for_file(&directory)?)
    {
        return Err(conflict("recorded assembly workspace directory changed"));
    }
    let names = fs::names(&directory)?;
    let filename = std::ffi::OsStr::new(&allocation.filename);
    if names.iter().any(|name| name != filename) {
        return Err(conflict(
            "assembly workspace contains an unrecorded entry; review its custody",
        ));
    }
    if !names.is_empty() {
        let current = fs::open_at(&directory, Path::new(filename), false)?;
        let identity = fs::identity_for_file(&current)?;
        if !current.metadata()?.is_file() || !allocation.output.same_object(&identity) {
            return Err(conflict(
                "assembly output object changed; review its custody",
            ));
        }
        fs::remove_entry(
            &directory,
            &FileEntry {
                path: allocation.filename.clone(),
                identity,
                digest: None,
            },
        )?;
    }
    fs::sync_directory(&directory)?;
    fs::remove_entry(
        &parent,
        &FileEntry {
            path: slot.to_string_lossy().into_owned(),
            identity: allocation.directory.clone(),
            digest: None,
        },
    )?;
    fs::sync_directory(&parent)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;

    fn grant(inventory: &Inventory) -> WorkerStopRequest {
        WorkerStopRequest {
            job: 11,
            context: WorkerControlContext {
                lease_id: "work/job/11/file/2/assemble@3".into(),
                job_incarnation: "assembly-job".into(),
                job_revision: 5,
                control_revision: 5,
                epoch: 7,
            },
            owner_incarnation: "original-worker".into(),
            storage_instance_id: inventory.installation.clone(),
            removal_revision: None,
            authority_source_json: None,
            start_ordered: true,
        }
    }

    fn stop(inventory: &Inventory, mut grant: WorkerStopRequest) -> WorkerStoppedReceipt {
        grant.removal_revision = Some(6);
        let ticket = inventory.prepare_worker_stop(&grant).unwrap();
        inventory.commit_worker_stopped(&ticket, &[]).unwrap()
    }

    #[test]
    fn worker_assembly_retires_owned_output_after_reopen_and_preserves_unknown_entries() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state");
        let dest = temp.path().join("dest");
        std::fs::create_dir(&dest).unwrap();
        let inventory = Inventory::open(&state).unwrap();
        let grant = grant(&inventory);
        inventory
            .begin_worker_assembly_grant(&grant, &dest)
            .unwrap();
        let (mut writer, reader, relative) = inventory
            .allocate_worker_assembly(grant.job, &grant.context, &dest, "raw.bin")
            .unwrap();
        writer.write_all(b"incomplete").unwrap();
        writer.sync_all().unwrap();
        drop((writer, reader, inventory));

        let reopened = Inventory::open(&state).unwrap();
        let slot = dest.join(relative);
        let payload = slot.join("raw.bin");
        let outside = temp.path().join("kept-link");
        std::fs::hard_link(&payload, &outside).unwrap();
        let foreign = slot.join("foreign");
        std::fs::write(&foreign, b"unrelated").unwrap();
        let stopped = stop(&reopened, grant);
        assert!(reopened.retire_worker_assembly(&stopped, 6, &dest).is_err());
        assert!(payload.exists());
        assert_eq!(std::fs::read(&foreign).unwrap(), b"unrelated");
        std::fs::remove_file(&foreign).unwrap();

        // A replacement of the same name is not an owned output either.
        let moved = temp.path().join("moved-output");
        std::fs::rename(&payload, &moved).unwrap();
        std::fs::write(&payload, b"replacement").unwrap();
        assert!(reopened.retire_worker_assembly(&stopped, 6, &dest).is_err());
        assert_eq!(std::fs::read(&payload).unwrap(), b"replacement");
        std::fs::remove_file(&payload).unwrap();
        std::fs::rename(&moved, &payload).unwrap();

        let retired = reopened
            .retire_worker_assembly(&stopped, 6, &dest)
            .unwrap()
            .unwrap();
        assert!(retired.had_output);
        assert!(!slot.exists());
        assert_eq!(std::fs::read(outside).unwrap(), b"incomplete");
        assert_eq!(
            reopened.retire_worker_assembly(&stopped, 6, &dest).unwrap(),
            Some(retired)
        );
    }

    #[test]
    fn worker_assembly_uses_positive_absence_and_preserves_unknown_legacy() {
        let temp = tempfile::tempdir().unwrap();
        let dest = temp.path().join("dest");
        std::fs::create_dir(&dest).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let planned = grant(&inventory);
        inventory
            .begin_worker_assembly_grant(&planned, &dest)
            .unwrap();
        let mut legacy = planned.clone();
        legacy.context.lease_id = "work/job/11/file/2/assemble@4".into();
        inventory.begin_worker_grant(&legacy).unwrap();
        let planned = stop(&inventory, planned);
        let legacy = stop(&inventory, legacy);
        let receipt = inventory
            .retire_worker_assembly(&planned, 6, &dest)
            .unwrap()
            .unwrap();
        assert!(!receipt.had_output);
        assert!(inventory
            .retire_worker_assembly(&legacy, 6, &dest)
            .unwrap()
            .is_none());
    }
}
