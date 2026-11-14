use hiqlite::{Client, Node, NodeConfig, Param};
use serde_json::{json, Value};
use std::{
    borrow::Cow,
    io::{BufRead, Write},
    path::Path,
    time::Duration,
};

fn emit(value: Value) {
    println!("COMPAT:{value}");
    std::io::stdout().flush().unwrap();
}

async fn dispatch(client: &Client, request: Value) -> Value {
    match request["op"].as_str().unwrap() {
        "transport" => {
            serde_json::to_value(client.local_snapshot_transport_status().unwrap().snapshot())
                .unwrap()
        }
        "metrics" => serde_json::to_value(client.metrics_db().await.unwrap()).unwrap(),
        "snapshot" => {
            let target = client.trigger_db_snapshot().await.unwrap();
            let last = std::sync::Mutex::new(None);
            let done = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let m = client.metrics_db().await.unwrap();
                    if m.snapshot.is_some_and(|s| s.index >= target)
                        && m.purged
                            .is_some_and(|s| s.index >= target.saturating_sub(1))
                    {
                        break;
                    }
                    *last.lock().unwrap() = Some(format!("{m:?}"));
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await;
            // Report what stalled (snapshot, purge, or a fatal storage error in
            // the Raft running state) instead of an opaque `Elapsed`.
            if done.is_err() {
                panic!(
                    "snapshot {target} did not complete within 20s; last metrics: {}",
                    last.lock().unwrap().as_deref().unwrap_or("none observed")
                );
            }
            json!({"snapshot_index":target})
        }
        "stage-unapplied" => {
            let id = request["id"].as_i64().unwrap();
            let before = client.metrics_db().await.unwrap().last_applied;
            hiqlite::validation_pause_apply();
            let writer = client.clone();
            tokio::spawn(async move {
                writer
                    .execute(
                        "INSERT INTO compat VALUES (?, ?, ?, ?, ?, ?)",
                        vec![
                            Param::Integer(id),
                            Param::Text(format!("row-{id}-é-💾")),
                            Param::Integer(i64::MIN + id),
                            Param::Real(-123.5),
                            Param::Blob((0..=255).collect()),
                            Param::Null,
                        ],
                    )
                    .await
                    .unwrap();
            });
            tokio::time::timeout(Duration::from_secs(20),async {
                loop {
                    let metrics=client.metrics_db().await.unwrap();
                    let wal=client.local_db_wal_status().unwrap().snapshot();
                    if hiqlite::validation_apply_pause_observed() && metrics.last_log_index.is_some_and(|index| index>before.map_or(0,|v|v.index) && wal.last_durable_index.is_some_and(|durable|durable>=index)) {
                        assert_eq!(metrics.last_applied,before,"paused entry reached SQLite before the crash");
                        break json!({"staged":id,"index":metrics.last_log_index,"last_applied":before,"wal":wal});
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.unwrap()
        }
        "seed" => {
            client.execute("CREATE TABLE IF NOT EXISTS compat (id INTEGER PRIMARY KEY, text TEXT, num INTEGER, real REAL, blob BLOB, optional TEXT)", vec![]).await.unwrap();
            let id = request["id"].as_i64().unwrap();
            let params = vec![
                Param::Integer(id),
                Param::Text(format!("row-{id}-é-💾")),
                Param::Integer(i64::MIN + id),
                Param::Real(-123.5),
                Param::Blob((0..=255).collect()),
                Param::Null,
            ];
            client
                .txn(vec![
                    ("INSERT INTO compat VALUES (?, ?, ?, ?, ?, ?)", params),
                    (
                        "UPDATE compat SET num = num WHERE id = ?",
                        vec![Param::Integer(id)],
                    ),
                ])
                .await
                .unwrap()
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            client.batch("UPDATE compat SET num = num WHERE id < 0; UPDATE compat SET text = text WHERE id < 0;").await.unwrap().into_iter().collect::<Result<Vec<_>,_>>().unwrap();
            // A real remote error crosses the response/error serialization path too.
            assert!(client
                .execute("INSERT INTO missing_compat_table VALUES (1)", vec![])
                .await
                .is_err());
            json!({"written":id})
        }
        "read" => {
            let mut rows = client
                .query_consistent(
                    "SELECT id,text,num,real,blob,optional FROM compat ORDER BY id",
                    vec![],
                )
                .await
                .unwrap();
            let result: Vec<Value> = rows.iter_mut().map(|r| json!({"id":r.get::<i64>("id"),"text":r.get::<String>("text"),"num":r.get::<i64>("num"),"real":r.get::<f64>("real"),"blob":r.get::<Vec<u8>>("blob"),"optional":r.get::<Option<String>>("optional")})).collect();
            json!(result)
        }
        "shutdown" => {
            client.shutdown().await.unwrap();
            json!("stopped")
        }
        other => panic!("unknown operation {other}"),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "fixtures" => {
            let out = Path::new(&args[2]);
            std::fs::create_dir_all(out).unwrap();
            std::fs::write(
                out.join("hiqlite.json"),
                serde_json::to_vec_pretty(&hiqlite::codec_compatibility::fixtures()).unwrap(),
            )
            .unwrap();
            std::fs::write(
                out.join("peer.json"),
                serde_json::to_vec_pretty(&peer_fixtures()).unwrap(),
            )
            .unwrap();
            std::fs::write(
                out.join("wal.json"),
                serde_json::to_vec_pretty(&hiqlite_wal::codec_compatibility::fixtures()).unwrap(),
            )
            .unwrap();
            let keys = cryptr::EncKeys {
                enc_key_active: "compat-active".into(),
                enc_keys: vec![
                    ("compat-active".into(), vec![7; 32]),
                    ("compat-previous".into(), (0..32).collect()),
                ],
            };
            let bytes = bincode::encode_to_vec(&keys, bincode::config::legacy()).unwrap();
            assert_eq!(cryptr::EncKeys::try_from(bytes.as_slice()).unwrap(), keys);
            let mut errors: Vec<String> = (0..bytes.len())
                .map(|end| {
                    format!(
                        "{:?}",
                        cryptr::EncKeys::try_from(&bytes[..end]).err().unwrap()
                    )
                })
                .collect();
            let invalid_utf8 = 1_u64
                .to_le_bytes()
                .into_iter()
                .chain([255])
                .chain(0_u64.to_le_bytes())
                .collect::<Vec<_>>();
            errors.push(format!(
                "{:?}",
                cryptr::EncKeys::try_from(invalid_utf8.as_slice()).unwrap_err()
            ));
            std::fs::write(
                out.join("key-errors.json"),
                serde_json::to_vec_pretty(&errors).unwrap(),
            )
            .unwrap();
            let mut trailing = bytes.clone();
            trailing.extend([0xAA, 0x55]);
            assert_eq!(
                cryptr::EncKeys::try_from(trailing.as_slice()).unwrap(),
                keys
            );
            std::fs::write(out.join("keys.bin"), bytes).unwrap();
            let sealed =
                cryptr::keys::EncKeysSealed::seal(keys, "compatibility-only-test-password")
                    .unwrap();
            sealed
                .save_to_file(out.join("keys.sealed").to_str().unwrap())
                .await
                .unwrap();
        }
        "key-bounds" => {
            // Native legacy headers with impossible collection/string lengths.
            // The maintained decoder must not reserve from unverified input.
            let cases = [
                vec![0_u8; 8]
                    .into_iter()
                    .chain(u64::MAX.to_le_bytes())
                    .collect::<Vec<_>>(),
                u64::MAX.to_le_bytes().to_vec(),
                vec![0_u8; 8]
                    .into_iter()
                    .chain(1_u64.to_le_bytes())
                    .chain(u64::MAX.to_le_bytes())
                    .collect::<Vec<_>>(),
            ];
            for bytes in cases {
                assert!(cryptr::EncKeys::try_from(bytes.as_slice()).is_err());
            }
        }
        "keys-read" => {
            let root = Path::new(&args[2]);
            let bytes = std::fs::read(root.join("keys.bin")).unwrap();
            let keys = cryptr::EncKeys::try_from(bytes.as_slice()).unwrap();
            assert_eq!(keys.enc_key_active, "compat-active");
            assert_eq!(keys.enc_keys.len(), 2);
            let sealed = cryptr::keys::EncKeysSealed::read_from_file(
                root.join("keys.sealed").to_str().unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(
                sealed.unseal("compatibility-only-test-password").unwrap(),
                keys
            );
        }
        "inspect" => {
            emit(
                match hiqlite_wal::inspection::inspect_logs_dir(Path::new(&args[2])) {
                    Ok(v) => json!({"ok":v}),
                    Err(e) => json!({"error":format!("{e:?}")}),
                },
            );
        }
        "node" => {
            let id: u64 = args[2].parse().unwrap();
            let nodes: Vec<Node> = serde_json::from_str(&args[4]).unwrap();
            let own = nodes.iter().find(|n| n.id == id).unwrap();
            let mut cfg = NodeConfig {
                #[cfg(feature = "cache-backup")]
                enc_keys: cryptr::EncKeys {
                    enc_key_active: "compat-active".into(),
                    enc_keys: vec![("compat-active".into(), vec![7; 32])],
                },
                node_id: id,
                nodes: nodes.clone(),
                listen_addr_api: Cow::Borrowed("127.0.0.1"),
                listen_addr_raft: Cow::Borrowed("127.0.0.1"),
                data_dir: Cow::Owned(args[3].clone()),
                filename_db: Cow::Borrowed("compat.db"),
                secret_raft: "compat-raft-only-test-secret-32-bytes".into(),
                secret_api: "compat-api-only-test-secret-32-bytes".into(),
                health_check_delay_secs: 0,
                raft_config: NodeConfig::default_raft_config(10000),
                wal_size: 2 * 1024 * 1024,
                ..NodeConfig::default()
            };
            // start_node takes its ports from the configured node addresses.
            assert_eq!(
                cfg.nodes.iter().find(|n| n.id == id).unwrap().addr_api,
                own.addr_api
            );
            cfg.raft_config.heartbeat_interval = 500;
            let client = tokio::time::timeout(Duration::from_secs(25), hiqlite::start_node(cfg))
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(25), client.wait_until_healthy_db())
                .await
                .unwrap();
            emit(json!({"ready":id}));
            let (tx, mut rx) = tokio::sync::mpsc::channel(8);
            std::thread::spawn(move || {
                for line in std::io::stdin().lock().lines() {
                    if tx.blocking_send(line.unwrap()).is_err() {
                        break;
                    }
                }
            });
            while let Some(line) = rx.recv().await {
                let request: Value = serde_json::from_str(&line).unwrap();
                let stop = request["op"] == "shutdown";
                let result =
                    tokio::time::timeout(Duration::from_secs(30), dispatch(&client, request))
                        .await
                        .unwrap();
                emit(result);
                if stop {
                    break;
                }
            }
        }
        mode => panic!("unknown mode {mode}"),
    }
}

fn peer_fixtures() -> std::collections::BTreeMap<String, (Vec<u8>, Vec<String>)> {
    use buffers::ByteBuf;
    use peer::extended::PeerExtendedMessageIds;
    use peer::{Handshake, Message, Piece, Request};
    let mut out = std::collections::BTreeMap::new();
    let handshake = Handshake {
        pstr: ByteBuf(b"BitTorrent protocol"),
        reserved: [0xFF; 8],
        info_hash: std::array::from_fn(|i| i as u8),
        peer_id: std::array::from_fn(|i| 255 - i as u8),
    };
    let mut bytes = Vec::new();
    handshake.serialize(&mut bytes);
    let (decoded, consumed) = Handshake::deserialize(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    let mut encoded = Vec::new();
    decoded.serialize(&mut encoded);
    assert_eq!(encoded, bytes);
    let errors = (0..bytes.len())
        .map(|end| format!("{:?}", Handshake::deserialize(&bytes[..end]).err().unwrap()))
        .collect();
    out.insert("handshake".into(), (bytes, errors));
    let messages: Vec<Message<ByteBuf<'_>>> = vec![
        Message::Request(Request::new(u32::MAX, 65537, 16384)),
        Message::Cancel(Request::new(257, u32::MAX, 1)),
        Message::Have(u32::MAX),
        Message::Choke,
        Message::Unchoke,
        Message::Interested,
        Message::NotInterested,
        Message::KeepAlive,
        Message::Bitfield(ByteBuf(&[0x80, 0xFF, 0, 1])),
        Message::Piece(Piece {
            index: 65537,
            begin: 257,
            block: ByteBuf(&[0, 255, 1, 254]),
        }),
    ];
    for (i, msg) in messages.iter().enumerate() {
        let mut bytes = Vec::new();
        let size = msg
            .serialize(&mut bytes, &PeerExtendedMessageIds::default)
            .unwrap();
        bytes.truncate(size);
        let (decoded, consumed) = Message::<ByteBuf<'_>>::deserialize(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        let mut encoded = Vec::new();
        let size = decoded
            .serialize(&mut encoded, &PeerExtendedMessageIds::default)
            .unwrap();
        encoded.truncate(size);
        assert_eq!(encoded, bytes);
        let errors = (0..bytes.len())
            .map(|end| {
                format!(
                    "{:?}",
                    Message::<ByteBuf<'_>>::deserialize(&bytes[..end])
                        .err()
                        .unwrap()
                )
            })
            .collect();
        out.insert(format!("message-{i}"), (bytes, errors));
    }
    out
}
