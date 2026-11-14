//! Validation-only fixtures for the actual SQLite/Raft serialization boundary.
//! Compiled against both the preserved prior tree and the candidate tree.
use crate::helpers::{deserialize, serialize};
use crate::network::raft_server::{
    RaftStreamRequest, RaftStreamResponse, RaftStreamResponsePayload,
};
use crate::store::state_machine::sqlite::state_machine::{
    Query, QueryWrite, Response, ResponseBatch, ResponseExecute, StateMachineData, StoredSnapshot,
};
use crate::{Node, Param};
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, Membership, SnapshotMeta, StoredMembership, Vote,
};
use serde::{de::DeserializeOwned, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

/// Bytes and malformed-input outcomes. The independent processes compare the
/// complete result, including all truncation errors and trailing-byte policy.
pub fn fixtures() -> BTreeMap<String, (Vec<u8>, Vec<String>)> {
    let mut out = BTreeMap::new();
    fn record<T: Serialize + DeserializeOwned>(
        out: &mut BTreeMap<String, (Vec<u8>, Vec<String>)>,
        name: &str,
        value: &T,
    ) {
        let bytes = serialize(value).unwrap();
        let decoded: T = deserialize(&bytes).unwrap();
        assert_eq!(serialize(&decoded).unwrap(), bytes);
        let mut outcomes = Vec::new();
        for end in 0..bytes.len() {
            outcomes.push(match deserialize::<T>(&bytes[..end]) {
                Ok(v) => format!("ok:{:?}", serialize(&v).unwrap()),
                Err(e) => format!("err:{e:?}"),
            });
        }
        let mut trailing = bytes.clone();
        trailing.extend([0xAA, 0x55]);
        let decoded: T = deserialize(&trailing).unwrap();
        assert_eq!(serialize(&decoded).unwrap(), bytes);
        out.insert(name.into(), (bytes, outcomes));
    }
    let log = LogId::new(CommittedLeaderId::new(0x1020304050607080, 257), 0x102030405);
    let nodes = BTreeMap::from([
        (
            1,
            Node {
                id: 1,
                addr_raft: "127.0.0.1:15001".into(),
                addr_api: "127.0.0.1:16001".into(),
            },
        ),
        (
            257,
            Node {
                id: 257,
                addr_raft: "[::1]:15002".into(),
                addr_api: "[::1]:16002".into(),
            },
        ),
    ]);
    let membership = Membership::new(vec![BTreeSet::from([1, 257]), BTreeSet::from([257])], nodes);
    let stored = StoredMembership::new(Some(log), membership.clone());
    let meta = SnapshotMeta {
        last_log_id: Some(log),
        last_membership: stored.clone(),
        snapshot_id: "snapshot-257-\u{00e9}".into(),
    };
    let params = vec![
        Param::Null,
        Param::Integer(i64::MIN),
        Param::Integer(i64::MAX),
        Param::Real(-123.5),
        Param::Text("\u{00e9}\0\u{1f4be}".into()),
        Param::Blob((0..=255).collect()),
        Param::StmtOutputIndexed(257, 65537),
        Param::StmtOutputNamed(1, Cow::Borrowed("returning-id")),
    ];
    record(&mut out, "params", &params);
    record(&mut out, "state-none", &StateMachineData::default());
    record(
        &mut out,
        "state-some",
        &StateMachineData {
            last_applied_log_id: Some(log),
            last_membership: stored,
            last_snapshot_id: Some("snapshot-257-\u{00e9}".into()),
        },
    );
    record(&mut out, "snapshot-meta", &meta);
    record(
        &mut out,
        "stored-snapshot",
        &StoredSnapshot {
            meta: meta.clone(),
            path: "snapshots/257.bin".into(),
        },
    );
    record(&mut out, "vote-uncommitted", &Vote::new(257, 1));
    record(&mut out, "vote-committed", &Vote::new_committed(65537, 257));
    let query = Query {
        sql: Cow::Borrowed("INSERT INTO jobs VALUES (?, ?, ?)"),
        params,
    };
    let queries = [
        QueryWrite::Execute(query.clone()),
        QueryWrite::ExecuteReturning(query.clone()),
        QueryWrite::Transaction(vec![query]),
        QueryWrite::Batch(Cow::Borrowed("SELECT 1; SELECT 257")),
        QueryWrite::Migration(vec![crate::migration::Migration {
            id: 257,
            name: "codec-compatibility".into(),
            hash: "0123456789abcdef".repeat(4),
            content: b"CREATE TABLE compat(id INTEGER);".to_vec(),
        }]),
        QueryWrite::RTT,
    ];
    for (i, query) in queries.into_iter().enumerate() {
        let entry = Entry::<crate::store::state_machine::sqlite::TypeConfigSqlite> {
            log_id: log,
            payload: EntryPayload::Normal(query),
        };
        record(&mut out, &format!("entry-normal-{i}"), &entry);
    }
    record(
        &mut out,
        "entry-blank",
        &Entry::<crate::store::state_machine::sqlite::TypeConfigSqlite> {
            log_id: log,
            payload: EntryPayload::Blank,
        },
    );
    record(
        &mut out,
        "entry-membership",
        &Entry::<crate::store::state_machine::sqlite::TypeConfigSqlite> {
            log_id: log,
            payload: EntryPayload::Membership(membership),
        },
    );
    for (i, response) in [
        Response::Empty,
        Response::Execute(ResponseExecute { result: Ok(257) }),
        Response::Execute(ResponseExecute {
            result: Err(crate::Error::BadRequest("invalid \u{00e9}".into())),
        }),
        Response::Transaction(Ok(vec![Ok(0), Err(crate::Error::Error("rejected".into()))])),
        Response::Batch(ResponseBatch {
            result: Ok(vec![Ok(65537)]),
        }),
        Response::Migrate(Ok(())),
        Response::Backup(Err(crate::Error::Error("unsupported".into()))),
        Response::RTT,
    ]
    .iter()
    .enumerate()
    {
        record(&mut out, &format!("response-{i}"), response);
    }
    let vote = Vote::new_committed(257, 1);
    record(
        &mut out,
        "wire-vote",
        &RaftStreamRequest::VoteDB((
            257,
            openraft::raft::VoteRequest {
                vote,
                last_log_id: Some(log),
            },
        )),
    );
    record(
        &mut out,
        "wire-snapshot",
        &RaftStreamRequest::SnapshotDB((
            65537,
            openraft::raft::InstallSnapshotRequest {
                vote,
                meta,
                offset: 257,
                data: vec![0, 255, 1, 254],
                done: false,
            },
        )),
    );
    record(
        &mut out,
        "wire-append",
        &RaftStreamRequest::AppendDB((
            1,
            openraft::raft::AppendEntriesRequest {
                vote,
                prev_log_id: Some(log),
                entries: Vec::new(),
                leader_commit: None,
            },
        )),
    );
    record(
        &mut out,
        "wire-vote-response",
        &RaftStreamResponse {
            request_id: 257,
            payload: RaftStreamResponsePayload::VoteDB(Ok(openraft::raft::VoteResponse {
                vote,
                vote_granted: true,
                last_log_id: Some(log),
            })),
        },
    );
    #[cfg(feature = "backup")]
    record(
        &mut out,
        "query-backup",
        &QueryWrite::Backup((257, i64::MIN)),
    );
    #[cfg(feature = "cache")]
    {
        use crate::store::state_machine::memory::state_machine::{CacheRequest, CacheResponse};
        for (i, request) in [
            CacheRequest::Get {
                cache_idx: 257,
                key: "cache-é".into(),
            },
            CacheRequest::Put {
                cache_idx: 1,
                key: Cow::Borrowed("cache-é"),
                value: vec![0, 255, 1],
                expires: Some(i64::MAX),
            },
            CacheRequest::Put {
                cache_idx: 1,
                key: Cow::Borrowed("cache-é"),
                value: vec![],
                expires: None,
            },
            CacheRequest::Delete {
                cache_idx: 1,
                key: Cow::Borrowed("cache-é"),
            },
            CacheRequest::Clear { cache_idx: 1 },
        ]
        .iter()
        .enumerate()
        {
            record(&mut out, &format!("cache-request-{i}"), request);
        }
        for (i, response) in [
            CacheResponse::Empty,
            CacheResponse::Ok,
            CacheResponse::Value(None),
            CacheResponse::Value(Some(vec![0, 255, 1])),
        ]
        .iter()
        .enumerate()
        {
            record(&mut out, &format!("cache-response-{i}"), response);
        }
    }
    out.insert(
        "invalid-state-option-tag".into(),
        (
            vec![2],
            vec![format!(
                "{:?}",
                deserialize::<StateMachineData>(&[2]).unwrap_err()
            )],
        ),
    );
    out.insert(
        "invalid-parameter-discriminant".into(),
        (
            vec![255; 4],
            vec![format!(
                "{:?}",
                deserialize::<Param>(&[255; 4]).unwrap_err()
            )],
        ),
    );
    out.insert(
        "invalid-wire-discriminant".into(),
        (
            vec![255; 4],
            vec![format!(
                "{:?}",
                deserialize::<RaftStreamRequest>(&[255; 4]).unwrap_err()
            )],
        ),
    );
    out
}
