//! Validation-only standard metadata and legacy Raft record fixtures.
use openraft::{CommittedLeaderId, LogId, Vote};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;

pub fn fixtures() -> BTreeMap<String, (Vec<u8>, Vec<String>)> {
    fn standard<T: Serialize + DeserializeOwned>(value: &T) -> (Vec<u8>, Vec<String>) {
        let bytes = crate::utils::serialize(value).unwrap();
        let mut errors = Vec::new();
        for end in 0..bytes.len() {
            errors.push(match crate::utils::deserialize::<T>(&bytes[..end]) {
                Ok(v) => format!("ok:{:?}", crate::utils::serialize(&v).unwrap()),
                Err(e) => format!("err:{e:?}"),
            });
        }
        let mut trailing = bytes.clone();
        trailing.push(0xAA);
        let decoded: T = crate::utils::deserialize(&trailing).unwrap();
        assert_eq!(crate::utils::serialize(&decoded).unwrap(), bytes);
        (bytes, errors)
    }
    let log = LogId::new(CommittedLeaderId::new(65537, 257), 0x102030405);
    let purged = crate::log_store_impl::serialize(&log).unwrap();
    let vote = crate::log_store_impl::serialize(&Vote::new_committed(65537, 257)).unwrap();
    let mut out = BTreeMap::new();
    for (name, metadata) in [
        (
            "metadata-none",
            crate::metadata::Metadata {
                last_purged_log_id: None,
                vote: None,
            },
        ),
        (
            "metadata-some",
            crate::metadata::Metadata {
                last_purged_log_id: Some(purged),
                vote: Some(vote),
            },
        ),
    ] {
        out.insert(name.into(), standard(&metadata));
    }
    out.insert(
        "standard-varint-boundaries".into(),
        standard(&vec![
            0_u64,
            1,
            250,
            251,
            255,
            256,
            65535,
            65536,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX,
        ]),
    );
    out.insert(
        "standard-signed-boundaries".into(),
        standard(&vec![
            i64::MIN,
            -65537,
            -251,
            -1,
            0,
            1,
            251,
            65537,
            i64::MAX,
        ]),
    );
    for (i, bytes) in [
        vec![251, 1, 0],
        vec![252, 1, 0, 0, 0],
        vec![253, 1, 0, 0, 0, 0, 0, 0, 0],
        vec![254],
        vec![255],
    ]
    .into_iter()
    .enumerate()
    {
        let result = match crate::utils::deserialize::<u64>(&bytes) {
            Ok(value) => format!("ok:{value}"),
            Err(error) => format!("err:{error:?}"),
        };
        out.insert(
            format!("standard-noncanonical-or-invalid-{i}"),
            (bytes, vec![result]),
        );
    }
    out
}
