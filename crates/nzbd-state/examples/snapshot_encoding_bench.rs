//! Read-only CPU/memory benchmark. No inventory, engine, journal or output file.
use nzbd_state::{SnapshotEncoder, SnapshotStore};
use serde_json::json;
use std::io::Write;
use std::time::Instant;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = std::env::args_os()
        .nth(1)
        .ok_or("usage: snapshot_encoding_bench SNAPSHOT")?;
    let source = std::fs::read(input)?;
    let doc = SnapshotStore::decode(&source)?;
    let mut next = doc.clone();
    let largest = next
        .jobs
        .iter()
        .enumerate()
        .max_by_key(|(_, job)| job.files.iter().map(|f| f.segments.len()).sum::<usize>())
        .map(|(i, _)| i);
    if let Some(index) = largest {
        next.jobs[index]
            .params
            .push(("snapshot-benchmark-only".into(), "changed".into()));
    }
    let t = Instant::now();
    let raw = serde_json::to_vec(&doc)?;
    let raw_crc = crc32fast::hash(&raw);
    let mut z = zstd::stream::write::Encoder::new(Vec::new(), 1)?;
    z.include_checksum(true)?;
    z.set_pledged_src_size(Some(raw.len() as u64))?;
    z.write_all(&raw)?;
    let monolithic = z.finish()?;
    let baseline_seconds = t.elapsed().as_secs_f64();
    let raw_len = raw.len();
    let expected_next = serde_json::to_vec(&next)?;
    let mut encoder = SnapshotEncoder::default();
    let cold_doc = doc.clone();
    let t = Instant::now();
    let cold = encoder.encode(cold_doc)?;
    let cold_seconds = t.elapsed().as_secs_f64();
    assert_eq!(SnapshotStore::decode_json_bytes(&cold)?.as_ref(), raw);
    let t = Instant::now();
    let warm = encoder.encode(doc)?;
    let warm_seconds = t.elapsed().as_secs_f64();
    let warm_stats = encoder.stats();
    assert_eq!(SnapshotStore::decode_json_bytes(&warm)?.as_ref(), raw);
    let t = Instant::now();
    let changed = encoder.encode(next)?;
    let changed_seconds = t.elapsed().as_secs_f64();
    assert_eq!(
        SnapshotStore::decode_json_bytes(&changed)?.as_ref(),
        expected_next
    );
    let peak_rss_kib = if cfg!(target_os = "linux") {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|line| line.starts_with("VmHWM:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|n| n.parse::<u64>().ok())
            })
    } else {
        None
    };
    println!(
        "{}",
        json!({"input_bytes":source.len(),"raw_bytes":raw_len,"monolithic_bytes":monolithic.len()+20,"raw_crc32":raw_crc,"monolithic_encode_seconds":baseline_seconds,"cold_cached_seconds":cold_seconds,"warm_cached_seconds":warm_seconds,"largest_job_changed_seconds":changed_seconds,"encoded_bytes":changed.len(),"warm_stats":warm_stats,"changed_stats":encoder.stats(),"exact_document_roundtrips":true,"benchmark_peak_rss_kib":peak_rss_kib,"peak_rss_includes_multiple_comparison_documents_and_buffers":true})
    );
    Ok(())
}
