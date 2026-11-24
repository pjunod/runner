//! End-to-end probes against the in-process news server.

use nzbd_nserv::{build_post, prng_bytes, Behavior, NservBuilder};
use nzbd_types::{CertLevel, ServerDef, ServerId, TlsMode};

use super::*;
use crate::rar::tests::{rar4_volume, rar5_volume};

fn server(port: u16) -> ServerDef {
    ServerDef {
        id: ServerId(1),
        name: "nserv".into(),
        host: "127.0.0.1".into(),
        port,
        tls: TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: 1,
        pipeline_depth: 1,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    }
}

/// A fake Matroska file: the EBML magic, then noise.
fn mkv(len: usize) -> Vec<u8> {
    let mut v = vec![0x1a, 0x45, 0xdf, 0xa3];
    v.extend(prng_bytes(7, len - 4));
    v
}

fn head(outcome: Outcome) -> Head {
    match outcome {
        Outcome::Head(h) => h,
        Outcome::Unavailable { reason, detail } => panic!("unavailable: {reason:?} {detail}"),
    }
}

fn reason(outcome: Outcome) -> Reason {
    match outcome {
        Outcome::Unavailable { reason, .. } => reason,
        Outcome::Head(h) => panic!("expected a refusal, got a head of {}", h.file),
    }
}

#[tokio::test]
async fn loose_media_reads_only_the_articles_it_needs() {
    let media = mkv(200_000);
    let post = build_post(
        "Title.2021.2160p",
        &[
            ("Title.2021.2160p.mkv", media.clone()),
            ("Title.2021.2160p.par2", prng_bytes(3, 4_000)),
            ("Title.2021.2160p.nfo", b"nfo".to_vec()),
        ],
        16_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let h = head(probe(post.nzb.as_bytes(), vec![server(ns.port())], 40_000).await);
    assert_eq!(h.file, "Title.2021.2160p.mkv");
    assert_eq!(h.file_size, media.len() as u64);
    assert_eq!(h.path, vec!["yenc"]);
    assert_eq!(h.bytes, media[..40_000]);
    // 40 000 bytes at 16 000 per article is three articles, not thirteen.
    assert_eq!(h.articles, 3);
    assert_eq!(ns.total_hits(), 3);
}

#[tokio::test]
async fn store_mode_rar_is_unwrapped_from_the_first_volume() {
    let media = mkv(120_000);
    for (label, volume1) in [
        (
            "rar4",
            rar4_volume("Title.2021.mkv", &media[..100_000], 0x0002, 0x30),
        ),
        (
            "rar5",
            rar5_volume("Title.2021.mkv", &media[..100_000], 0x0010, 0, false),
        ),
    ] {
        let post = build_post(
            "Title.2021",
            &[
                ("Title.2021.part2.rar", prng_bytes(11, 30_000)),
                ("Title.2021.part1.rar", volume1),
            ],
            20_000,
        );
        let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
        let h = head(probe(post.nzb.as_bytes(), vec![server(ns.port())], 50_000).await);
        assert_eq!(h.file, "Title.2021.mkv", "{label}");
        assert_eq!(h.path, vec!["yenc", "rar-store"], "{label}");
        assert_eq!(h.bytes, media[..50_000], "{label}");
        // The test helpers record unpacked = 3 × the volume's data.
        assert_eq!(h.file_size, 300_000, "{label}");
    }
}

#[tokio::test]
async fn refusals_name_their_reason() {
    let media = mkv(40_000);
    let cases = [
        (
            "encrypted",
            rar5_volume("m.mkv", &media, 0, 0, true),
            Reason::Encrypted,
        ),
        (
            "compressed",
            rar4_volume("m.mkv", &media, 0, 0x33),
            Reason::Compressed,
        ),
    ];
    for (label, volume, want) in cases {
        let post = build_post("Title", &[("Title.part1.rar", volume)], 20_000);
        let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
        assert_eq!(
            reason(probe(post.nzb.as_bytes(), vec![server(ns.port())], 10_000).await),
            want,
            "{label}"
        );
    }

    let post = build_post("Title", &[("Title.mkv", media.clone())], 20_000);
    let first = post.message_id("Title.mkv", 1);
    let ns = NservBuilder::new()
        .with_post(&post)
        .behavior(&first, Behavior::NotFound)
        .start()
        .await
        .unwrap();
    assert_eq!(
        reason(probe(post.nzb.as_bytes(), vec![server(ns.port())], 10_000).await),
        Reason::MissingArticles
    );

    let post = build_post("Title", &[("Title.par2", prng_bytes(1, 500))], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    assert_eq!(
        reason(probe(post.nzb.as_bytes(), vec![server(ns.port())], 10_000).await),
        Reason::NoMedia
    );

    assert_eq!(
        reason(probe(b"<not an nzb", vec![server(1)], 10).await),
        Reason::Unreadable
    );
}

#[tokio::test]
async fn a_hole_ends_the_head_and_is_counted() {
    let media = mkv(100_000);
    let post = build_post("Title", &[("Title.mkv", media.clone())], 20_000);
    let third = post.message_id("Title.mkv", 3);
    let ns = NservBuilder::new()
        .with_post(&post)
        .behavior(&third, Behavior::NotFound)
        .start()
        .await
        .unwrap();
    let h = head(probe(post.nzb.as_bytes(), vec![server(ns.port())], 90_000).await);
    assert_eq!(h.bytes, media[..40_000]);
    assert_eq!(h.articles_missing, 1);
}

#[tokio::test]
async fn a_backup_tier_fills_what_the_main_server_lost() {
    let media = mkv(60_000);
    let post = build_post("Title", &[("Title.mkv", media.clone())], 20_000);
    let first = post.message_id("Title.mkv", 1);
    let main = NservBuilder::new()
        .with_post(&post)
        .behavior(&first, Behavior::NotFound)
        .start()
        .await
        .unwrap();
    let backup = NservBuilder::new().with_post(&post).start().await.unwrap();
    let mut b = server(backup.port());
    b.tier = 1;
    let h = head(probe(post.nzb.as_bytes(), vec![b, server(main.port())], 30_000).await);
    assert_eq!(h.bytes, media[..30_000]);
    assert_eq!(h.articles_missing, 0);
    assert_eq!(backup.hits(&first), 1);
}

#[tokio::test]
async fn an_obfuscated_set_is_searched_for_its_first_volume() {
    let media = mkv(60_000);
    let volume1 = rar5_volume("Title.mkv", &media, 0x0010, 0, false);
    let volume2 = rar5_volume("Title.mkv", &prng_bytes(5, 60_000), 0x0008, 0, false);
    // Same size, no names: the later volume happens to come first.
    let post = build_post(
        "a8f3c1e9d04b77e2",
        &[("e2c519f0a6b77d04", volume2), ("9d04b77e2c519f0a", volume1)],
        20_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let h = head(probe(post.nzb.as_bytes(), vec![server(ns.port())], 30_000).await);
    assert_eq!(h.file, "Title.mkv");
    assert_eq!(h.bytes, media[..30_000]);
}

/// A port that accepts connections, counts them and hangs up before the
/// greeting: a server that is up but refuses this client.
async fn refusing_server() -> (u16, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = accepted.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            drop(socket);
        }
    });
    (port, accepted)
}

#[tokio::test]
async fn a_server_that_will_not_talk_is_not_called_missing_articles() {
    let post = build_post("Title", &[("Title.mkv", mkv(60_000))], 20_000);
    let (port, accepted) = refusing_server().await;
    match probe(post.nzb.as_bytes(), vec![server(port)], 50_000).await {
        Outcome::Unavailable { reason, detail } => {
            assert_eq!(reason, Reason::Unreadable);
            assert!(detail.contains("no news server answered"), "{detail}");
        }
        Outcome::Head(h) => panic!("head of {}", h.file),
    }
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_dead_main_server_is_tried_once_and_the_backup_serves_the_head() {
    let media = mkv(60_000);
    let post = build_post("Title", &[("Title.mkv", media.clone())], 20_000);
    let (port, accepted) = refusing_server().await;
    let backup = NservBuilder::new().with_post(&post).start().await.unwrap();
    let mut b = server(backup.port());
    b.tier = 1;
    let h = head(probe(post.nzb.as_bytes(), vec![server(port), b], 50_000).await);
    assert_eq!(h.bytes, media[..50_000]);
    // Three articles, one connection attempt on the dead server.
    assert_eq!(h.articles, 3);
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_stored_member_that_is_not_media_is_not_returned() {
    let volume = rar5_volume("Title.2021.mkv", &prng_bytes(9, 40_000), 0, 0, false);
    let post = build_post("Title", &[("Title.part1.rar", volume)], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    assert_eq!(
        reason(probe(post.nzb.as_bytes(), vec![server(ns.port())], 10_000).await),
        Reason::NoMedia
    );
}
