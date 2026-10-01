use nzbd_post::{rename::par_rename, tools::Extractors, ArchiveKind};
use std::{fs, os::unix::fs::PermissionsExt, process::Command, time::Duration};

#[test]
fn par_catalog_subdirectories_should_be_restored_before_verification() {
    if Command::new("par2").output().is_err() {
        assert!(
            std::env::var_os("NZBD_REQUIRE_TOOLS").is_none(),
            "par2 required in strict lane"
        );
        return;
    }
    let t = tempfile::tempdir().unwrap();
    fs::create_dir(t.path().join("Episode01")).unwrap();
    let nested = t.path().join("Episode01/episode.rar");
    let data: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    fs::write(&nested, data).unwrap();
    assert!(Command::new("par2")
        .args([
            "create",
            "-q",
            "-q",
            "-s8192",
            "-c4",
            "set.par2",
            "Episode01/episode.rar"
        ])
        .current_dir(t.path())
        .status()
        .unwrap()
        .success());
    fs::rename(&nested, t.path().join("episode.rar")).unwrap();
    fs::remove_dir(t.path().join("Episode01")).unwrap();
    let set = nzbd_post::par2::load_dir(t.path()).unwrap().unwrap();
    assert_eq!(set.files[0].name, "Episode01/episode.rar");
    par_rename(t.path()).unwrap();
    assert!(
        nested.exists(),
        "matching flat file was not restored to its catalog path"
    );
}

fn tool(path: &std::path::Path, script: &str) {
    fs::write(path, script).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn linux_7zip_disk_full_should_be_identified() {
    let t = tempfile::tempdir().unwrap();
    let seven = t.path().join("7z");
    tool(
        &seven,
        "#!/bin/sh\necho 'ERROR: No space left on device' >&2\nexit 2\n",
    );
    let archive = t.path().join("payload.rar");
    fs::write(&archive, b"fixture").unwrap();
    let e = Extractors {
        unrar_cmd: t.path().join("missing-unrar").display().to_string(),
        sevenzip_cmd: seven.display().to_string(),
        timeout: Duration::from_secs(5),
    };
    let outcome = e
        .extract(&archive, ArchiveKind::Rar, &t.path().join("out"), None)
        .await
        .unwrap();
    assert!(!outcome.success);
    assert!(
        outcome.disk_space_error,
        "Linux ENOSPC was reduced to a generic unpack failure"
    );
}

#[tokio::test]
async fn fallback_disk_failure_should_not_be_masked_by_first_tool() {
    let t = tempfile::tempdir().unwrap();
    let first = t.path().join("unrar");
    let second = t.path().join("7z");
    tool(
        &first,
        "#!/bin/sh\necho 'unsupported archive' >&2\nexit 2\n",
    );
    tool(
        &second,
        "#!/bin/sh\necho 'There is not enough space' >&2\nexit 2\n",
    );
    let archive = t.path().join("payload.rar");
    fs::write(&archive, b"fixture").unwrap();
    let e = Extractors {
        unrar_cmd: first.display().to_string(),
        sevenzip_cmd: second.display().to_string(),
        timeout: Duration::from_secs(5),
    };
    let outcome = e
        .extract(&archive, ArchiveKind::Rar, &t.path().join("out"), None)
        .await
        .unwrap();
    assert!(
        outcome.disk_space_error,
        "first failure erased the fallback's disk-space error"
    );
}

#[tokio::test]
async fn resource_failure_never_runs_fallback() {
    let t = tempfile::tempdir().unwrap();
    let first = t.path().join("unrar");
    let second = t.path().join("7z");
    let marker = t.path().join("ran-fallback");
    tool(
        &first,
        "#!/bin/sh\necho 'Disk quota exceeded' >&2\nexit 5\n",
    );
    tool(
        &second,
        &format!("#!/bin/sh\ntouch '{}'\nexit 2\n", marker.display()),
    );
    let archive = t.path().join("payload.rar");
    fs::write(&archive, b"fixture").unwrap();
    let extractors = Extractors {
        unrar_cmd: first.display().to_string(),
        sevenzip_cmd: second.display().to_string(),
        timeout: Duration::from_secs(5),
    };
    let out = extractors
        .extract(&archive, ArchiveKind::Rar, &t.path().join("out"), None)
        .await
        .unwrap();
    assert!(out.disk_space_error && out.quota_error);
    assert!(!marker.exists());
    assert_eq!(out.attempts.len(), 1);
}

#[tokio::test]
async fn review_diskfull_fallback_can_be_retried_in_same_workspace() {
    let t = tempfile::tempdir().unwrap();
    let seven = t.path().join("7z");
    tool(
        &seven,
        "#!/bin/sh\nfor arg do case \"$arg\" in -o*) dest=${arg#-o};; esac; done\nprintf partial > \"$dest/partial.bin\"\necho 'ERROR: No space left on device' >&2\nexit 2\n",
    );
    let archive = t.path().join("payload.rar");
    fs::write(&archive, b"fixture").unwrap();
    let ex = Extractors {
        unrar_cmd: t.path().join("missing-unrar").display().to_string(),
        sevenzip_cmd: seven.display().to_string(),
        timeout: Duration::from_secs(5),
    };
    let out = t.path().join("output");
    let first = ex
        .extract(&archive, ArchiveKind::Rar, &out, None)
        .await
        .unwrap();
    assert!(first.disk_space_error);
    let retry = ex.extract(&archive, ArchiveKind::Rar, &out, None).await;
    assert!(
        retry.is_ok(),
        "same-workspace retry stopped before extractor: {retry:?}"
    );

    assert_eq!(
        fs::read(first.output_dir.join("partial.bin")).unwrap(),
        b"partial"
    );
    tool(&seven, "#!/bin/sh\nfor arg do case \"$arg\" in -o*) dest=${arg#-o};; esac; done\nprintf recovered > \"$dest/media.bin\"\necho 'Everything is Ok'\n");
    let restored = ex
        .extract(&archive, ArchiveKind::Rar, &out, None)
        .await
        .unwrap();
    assert!(restored.success, "restored capacity failed: {restored:?}");
    assert_eq!(
        fs::read(restored.output_dir.join("media.bin")).unwrap(),
        b"recovered"
    );
    assert_eq!(
        fs::read(out.with_extension("retry-1").join("partial.bin")).unwrap(),
        b"partial"
    );
}

#[test]
fn review_nested_par_set_keeps_its_catalog_root() {
    let t = tempfile::tempdir().unwrap();
    let episode = t.path().join("Episode01");
    fs::create_dir(&episode).unwrap();
    let payload = episode.join("payload.bin");
    fs::write(&payload, vec![5u8; 50_000]).unwrap();
    assert!(Command::new("par2")
        .args([
            "create",
            "-q",
            "-q",
            "-s8192",
            "-c4",
            "set.par2",
            "payload.bin"
        ])
        .current_dir(&episode)
        .status()
        .unwrap()
        .success());
    par_rename(t.path()).unwrap();
    assert!(
        payload.exists(),
        "valid episode-local catalog file was incorrectly moved into the job root"
    );
    assert!(!t.path().join("payload.bin").exists());
}
