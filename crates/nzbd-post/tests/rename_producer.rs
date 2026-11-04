//! Real download/writer/owner admission followed by 134 custody-bound renames.
use md5::{Digest, Md5};
use nzbd_engine::{Engine, EngineConfig, Event, Tuning};
use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use nzbd_post::rename::{naming_candidates, par_rename_owned_with_candidates};
use nzbd_types::{CertLevel, JobStatus, ServerDef, ServerId, TlsMode};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn packet(kind: &[u8; 16], body: &[u8]) -> Vec<u8> {
    let mut out = b"PAR2\0PKT".to_vec();
    out.extend_from_slice(&[0; 24]);
    out.extend_from_slice(&[37; 16]);
    out.extend_from_slice(kind);
    out.extend_from_slice(body);
    while out.len() % 4 != 0 {
        out.push(0);
    }
    let len = out.len() as u64;
    out[8..16].copy_from_slice(&len.to_le_bytes());
    let digest = Md5::digest(&out[32..]);
    out[16..32].copy_from_slice(&digest);
    out
}

fn catalog(files: &[(String, Vec<u8>)]) -> Vec<u8> {
    let ids: Vec<[u8; 16]> = (0..files.len())
        .map(|i| Md5::digest(format!("file-{i}").as_bytes()).into())
        .collect();
    let mut main = 16384u64.to_le_bytes().to_vec();
    main.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for id in &ids {
        main.extend_from_slice(id);
    }
    let mut out = packet(b"PAR 2.0\0Main\0\0\0\0", &main);
    for ((name, bytes), id) in files.iter().zip(ids) {
        let mut desc = id.to_vec();
        desc.extend_from_slice(&Md5::digest(bytes));
        desc.extend_from_slice(&Md5::digest(&bytes[..bytes.len().min(16384)]));
        desc.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        desc.extend_from_slice(name.as_bytes());
        out.extend(packet(b"PAR 2.0\0FileDesc", &desc));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_writers_feed_134_renames_with_one_destination_pass() {
    let temporary = tempfile::tempdir().unwrap();
    let mut files: Vec<_> = (0..134)
        .map(|i| (format!("restored-{i:03}.bin"), prng_bytes(13000 + i, 32768)))
        .collect();
    // Equal prefixes with distinct full digests must still choose distinct names.
    files[0].1[..16384].fill(9);
    files[1].1[..16384].fill(9);
    let mut inputs = vec![("index.par2".to_owned(), catalog(&files))];
    inputs.extend(
        files
            .iter()
            .enumerate()
            .map(|(i, (_, bytes))| (format!("obfuscated-{i:03}"), bytes.clone())),
    );
    let refs: Vec<_> = inputs
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let post = build_post("rename-evidence", &refs, 65536);
    let provider = NservBuilder::new().with_post(&post).start().await.unwrap();
    let engine = Engine::spawn(EngineConfig::single_node(
        vec![ServerDef {
            id: ServerId(1),
            name: "rename-provider".into(),
            host: "127.0.0.1".into(),
            port: provider.port(),
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 8,
            pipeline_depth: 2,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        }],
        temporary.path().join("state"),
        temporary.path().join("data"),
        Tuning {
            min_free_disk_bytes: 0,
            ..Tuning::default()
        },
        None,
    ))
    .await
    .unwrap();
    let mut events = engine.subscribe();
    let job = engine
        .add_nzb(
            "rename-producer-fixture",
            post.nzb.as_bytes(),
            Some("fixture".into()),
            0,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            match events.recv().await {
                Ok(Event::JobFinished {
                    job: id, status, ..
                }) if id == job => {
                    assert_eq!(status, JobStatus::Completed);
                    break;
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(error) => panic!("writer event stream: {error}"),
            }
        }
    })
    .await
    .expect("all real writers must finalize");
    let admission = tokio::time::timeout(
        Duration::from_secs(10),
        engine.wait_post_admission(job, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let scoped = engine.for_post_admission(&admission);
    let record = scoped.export_job(job).await.unwrap().unwrap();
    assert!(record.files.iter().any(|f| f.is_par2));
    let inventory = engine.artifacts();
    let source = inventory.for_job(job.0).unwrap().unwrap();
    let dir = source.path.clone();
    let candidates = naming_candidates(&record, &dir);
    assert_eq!(candidates.len(), 134);
    let use_guard = inventory
        .acquire_attempt_use_checked(job.0, &|| Ok(()))
        .unwrap()
        .unwrap();
    let expected_bytes: u64 = files.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let check_files = files.clone();
    let output = tokio::task::spawn_blocking(move || {
        let _use_guard = use_guard;
        let outcome =
            par_rename_owned_with_candidates(&dir, Some((&inventory, job.0)), &candidates).unwrap();
        assert_eq!(outcome.metrics.cache_hits, 134);
        assert_eq!(outcome.metrics.cache_misses, 0);
        assert_eq!(outcome.metrics.source_read_bytes, 0);
        assert_eq!(outcome.metrics.readback_bytes, expected_bytes);
        assert_eq!(outcome.metrics.renamed_files, 134);
        assert_eq!(outcome.renames.len(), 134);
        for (name, bytes) in &check_files {
            assert_eq!(std::fs::read(dir.join(name)).unwrap(), *bytes);
        }
        // Sibling revision changes must not have evicted the remaining tokens.
        // After consumption, ordinary same-prefix ambiguity retains full matching.
        let second =
            par_rename_owned_with_candidates(&dir, Some((&inventory, job.0)), &[]).unwrap();
        assert!(second.renames.is_empty());
        assert_eq!(second.metrics.source_read_bytes, 2 * 32768);
        assert_eq!(second.metrics.readback_bytes, 0);
        assert_eq!(
            inventory.for_job(job.0).unwrap().unwrap().generation,
            source.generation
        );
        outcome.metrics
    })
    .await
    .unwrap();
    assert_eq!(output.readback_bytes, expected_bytes);
    assert_eq!(
        provider.total_hits(),
        135,
        "no refetch while consuming producer evidence"
    );
    engine.end_post_admission(admission).await.unwrap();
    assert!(engine.shutdown().await);
}
