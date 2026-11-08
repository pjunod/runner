use super::*;
use rusqlite::types::Value;

const INDEXES: [&str; 3] = [
    "operations_transform_scratch",
    "operations_scan_recent",
    "operations_kind",
];

fn operation(key: &str, kind: &str, state: &str, request: serde_json::Value) -> Operation {
    Operation {
        id: key.into(),
        artifact: "source".into(),
        kind: kind.into(),
        state: state.into(),
        request: request.to_string(),
        created_at: 1,
        not_before: 0,
        attempts: 0,
        next_retry: 0,
        error: None,
    }
}

fn results(db: &Connection, sql: &str, args: &[Value]) -> Vec<Vec<Value>> {
    let mut statement = db.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map(rusqlite::params_from_iter(args), |row| {
            (0..columns).map(|column| row.get(column)).collect()
        })
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap()
}

fn cases() -> Vec<(&'static str, Vec<Value>, &'static str)> {
    use operation_queries::*;
    let text = |s: &str| Value::Text(s.into());
    vec![
        (TRANSFORM_SCRATCH, vec![text("scratch-extract")], INDEXES[0]),
        (TRANSFORM_SCRATCH, vec![text("source")], INDEXES[0]),
        (
            TRANSFORM_SCRATCH_GENERATION,
            vec![text("scratch-extract"), text("g")],
            INDEXES[0],
        ),
        (
            TRANSFORM_SCRATCH_GENERATION,
            vec![text("scratch-extract"), text("wrong")],
            INDEXES[0],
        ),
        (
            TORRENT_TRANSFORM_SCRATCH,
            vec![text("source"), text("scratch-extract")],
            INDEXES[0],
        ),
        (DISCOVERY_STATUS, vec![], INDEXES[1]),
        (
            COMPLETED_RELOCATIONS,
            vec![text("source"), text("g")],
            INDEXES[2],
        ),
        (WAKE_RELOCATION_CLEANUP, vec![text("g")], INDEXES[2]),
        (
            RELOCATION_CLEANUP_CANDIDATES,
            vec![text("/processing")],
            INDEXES[2],
        ),
        (
            TORRENT_STAGE_CLEANUP,
            vec![Value::Integer(0), Value::Integer(i64::MAX)],
            INDEXES[2],
        ),
        (TORRENT_COPY_COMPACTION, vec![Value::Integer(0)], INDEXES[2]),
    ]
}

#[test]
fn operation_indexes_upgrade_existing_inventory_and_cover_production_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("state");
    let inventory = Inventory::open(&path).unwrap();
    let cases = cases();
    let before;
    {
        let mut db = inventory.db.lock().unwrap();
        for index in INDEXES {
            db.execute_batch(&format!("DROP INDEX {index}")).unwrap();
        }
        let tx = db.transaction().unwrap();
        // Large unrelated journal entries must not be examined by scratch misses.
        // A non-transform request need not be JSON; the partial index shields it.
        for n in 0..512 {
            let mut op = operation(
                &format!("noise-{n}"),
                "par_restore",
                "succeeded",
                serde_json::Value::Null,
            );
            op.request = "legacy non-JSON request ".repeat(128);
            save_operation(&tx, &op).unwrap();
        }
        for kind in ["extract", "par_repair"] {
            save_operation(
                &tx,
                &operation(
                    kind,
                    kind,
                    "succeeded",
                    serde_json::json!({
                        "scratch": {"id": format!("scratch-{kind}"), "generation": "g"}
                    }),
                ),
            )
            .unwrap();
        }
        for (key, created) in [("old-scan", 1), ("new-scan", 9)] {
            let mut op = operation(key, "scan", "succeeded", serde_json::Value::Null);
            op.created_at = created;
            save_operation(&tx, &op).unwrap();
        }
        save_operation(&tx, &operation("move", "relocate", "succeeded", serde_json::json!({
            "source": {"id": "source", "generation": "g", "root": "/processing", "automatic_cleanup_eligible": true},
            "cleanup_done": false
        }))).unwrap();
        // Populate both torrent cursor queries, including the query without a
        // state restriction. A kind/state index would force a sort for that one.
        tx.execute("INSERT INTO artifacts(id,job,path,state,updated_at,data) VALUES('source',NULL,'/processing/source','deleted',1,'{}')", []).unwrap();
        for n in 0..96 {
            let state = if n % 2 == 0 { "succeeded" } else { "failed" };
            save_operation(
                &tx,
                &operation(
                    &format!("torrent-{n}"),
                    "torrent-copy",
                    state,
                    serde_json::json!({"job": n + 1}),
                ),
            )
            .unwrap();
        }
        tx.commit().unwrap();
        assert_eq!(
            results(
                &db,
                operation_queries::TRANSFORM_SCRATCH,
                &[Value::Text("source".into())]
            ),
            Vec::<Vec<Value>>::new()
        );
        before = cases
            .iter()
            .map(|(sql, args, _)| results(&db, sql, args))
            .collect::<Vec<_>>();
        assert!(!before[0].is_empty());
        assert!(before[3].is_empty());
        let latest: Operation = serde_json::from_str(match &before[5][0][0] {
            Value::Text(raw) => raw,
            _ => panic!("expected operation JSON"),
        })
        .unwrap();
        assert_eq!(latest.id, "new-scan");
        assert!(!before[9].is_empty());
        assert!(!before[10].is_empty());
    }
    inventory.close().unwrap();
    drop(inventory);
    // Reopening a pre-index installation creates the new indexes, without
    // ANALYZE or any data rewrite. A second open proves migration idempotence.
    for _ in 0..2 {
        let inventory = Inventory::open(&path).unwrap();
        {
            let db = inventory.db.lock().unwrap();
            let stats: i64 = db
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name='sqlite_stat1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stats, 0);
            for ((sql, args, expected_index), original) in cases.iter().zip(&before) {
                assert_eq!(
                    &results(&db, sql, args),
                    original,
                    "query results changed: {sql}"
                );
                let plan = results(&db, &format!("EXPLAIN QUERY PLAN {sql}"), args);
                let details = plan
                    .iter()
                    .map(|row| match &row[3] {
                        Value::Text(s) => s.as_str(),
                        _ => panic!("expected plan detail"),
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(
                    details.contains(&format!("USING INDEX {expected_index}")),
                    "{sql}\n{details}"
                );
                assert!(
                    !details.contains("TEMP B-TREE"),
                    "unexpected sort: {sql}\n{details}"
                );
                if *sql == operation_queries::TORRENT_STAGE_CLEANUP
                    || *sql == operation_queries::TORRENT_COPY_COMPACTION
                {
                    assert!(
                        details.contains("rowid>?"),
                        "cursor must constrain the index: {details}"
                    );
                }
            }
        }
        inventory.close().unwrap();
    }
}

#[test]
fn operation_indexes_track_kind_request_state_and_discovery_order_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let inventory = Inventory::open(tmp.path()).unwrap();
    let db = inventory.db.lock().unwrap();
    let mut op = operation(
        "changing",
        "extract",
        "succeeded",
        serde_json::json!({"scratch":{"id":"old","generation":"g"}}),
    );
    save_operation(&db, &op).unwrap();
    let scratch = |key: &str| {
        results(
            &db,
            operation_queries::TRANSFORM_SCRATCH,
            &[Value::Text(key.into())],
        )
    };
    assert_eq!(scratch("old").len(), 1);
    op.kind = "par_restore".into();
    op.request = "non-JSON legacy request".into();
    save_operation(&db, &op).unwrap();
    assert!(scratch("old").is_empty());
    op.kind = "par_repair".into();
    op.request = serde_json::json!({"scratch":{"id":"new","generation":"g2"}}).to_string();
    op.state = "failed".into();
    save_operation(&db, &op).unwrap();
    assert_eq!(scratch("new").len(), 1);
    assert!(results(
        &db,
        operation_queries::TRANSFORM_SCRATCH_GENERATION,
        &[Value::Text("new".into()), Value::Text("g".into())]
    )
    .is_empty());
    assert_eq!(results(&db, "SELECT id FROM operations WHERE json_extract(data,'$.kind')='par_repair' AND state='failed'", &[]).len(), 1);
    assert!(results(
        &db,
        "SELECT id FROM operations WHERE json_extract(data,'$.kind')='extract'",
        &[]
    )
    .is_empty());
    let mut scan = operation("scan", "scan", "succeeded", serde_json::Value::Null);
    scan.created_at = 10;
    save_operation(&db, &scan).unwrap();
    op.kind = "scan".into();
    op.created_at = 11;
    save_operation(&db, &op).unwrap();
    let latest = results(&db, operation_queries::DISCOVERY_STATUS, &[]);
    let raw = match &latest[0][0] {
        Value::Text(raw) => raw,
        _ => panic!("expected JSON"),
    };
    assert_eq!(
        serde_json::from_str::<Operation>(raw).unwrap().id,
        "changing"
    );
    op.kind = "par_restore".into();
    save_operation(&db, &op).unwrap();
    let latest = results(&db, operation_queries::DISCOVERY_STATUS, &[]);
    let raw = match &latest[0][0] {
        Value::Text(raw) => raw,
        _ => panic!("expected JSON"),
    };
    assert_eq!(serde_json::from_str::<Operation>(raw).unwrap().id, "scan");
}

#[test]
fn workspace_admission_keeps_one_operation_per_derived_scratch_id() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("processing");
    std::fs::create_dir(&root).unwrap();
    let inventory = Inventory::open(&tmp.path().join("state")).unwrap();
    let source = inventory.allocate(1, &root, &root.join("job")).unwrap();
    let first = inventory.workspace(1, "extract", "ab").unwrap();
    let repeated = inventory.workspace(1, "extract", "ab").unwrap();
    let distinct = inventory.workspace(1, "extract", "cd").unwrap();
    assert_eq!(first.scratch.id, repeated.scratch.id);
    assert_ne!(first.scratch.id, distinct.scratch.id);
    assert_ne!(source.id, first.scratch.id);
    let db = inventory.db.lock().unwrap();
    for scratch in [&first.scratch.id, &distinct.scratch.id] {
        let rows = results(
            &db,
            operation_queries::TRANSFORM_SCRATCH,
            &[Value::Text(scratch.clone())],
        );
        assert_eq!(rows.len(), 1);
        let raw = match &rows[0][0] {
            Value::Text(raw) => raw,
            _ => panic!("expected JSON"),
        };
        let operation: Operation = serde_json::from_str(raw).unwrap();
        assert_eq!(*scratch, format!("scratch-{}", operation.id));
    }
}

#[test]
fn operation_index_creation_failure_refuses_inventory_open() {
    let tmp = tempfile::tempdir().unwrap();
    let inventory = Inventory::open(tmp.path()).unwrap();
    {
        let db = inventory.db.lock().unwrap();
        for index in INDEXES {
            db.execute_batch(&format!("DROP INDEX {index}")).unwrap();
        }
        db.execute("INSERT INTO operations(id,artifact,state,data) VALUES('corrupt','source','succeeded','{')", []).unwrap();
    }
    inventory.close().unwrap();
    drop(inventory);
    assert!(
        Inventory::open(tmp.path()).is_err(),
        "schema failures must not be silently ignored"
    );
}
