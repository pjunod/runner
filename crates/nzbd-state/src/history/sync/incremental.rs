//! Ephemeral per-file contributions and committed complete-line offsets.
//!
//! SQLite remains the durable index. Cache publication follows all associated
//! SQLite commits under the mutation fence. A failed pass retains its old
//! offsets, so retry is idempotent; restart reconstructs everything from logs.
use super::*;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};

const ANCHOR_BYTES: u64 = 128;
const VERIFY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct Cache {
    files: BTreeMap<PathBuf, FileState>,
    initialized: bool,
}

pub(super) struct ScanStats {
    pub kind: &'static str,
    pub entries: u64,
    pub rebuilt: u64,
    pub incomplete: u64,
    pub malformed: u64,
}
impl Default for ScanStats {
    fn default() -> Self {
        Self {
            kind: "none",
            entries: 0,
            rebuilt: 0,
            incomplete: 0,
            malformed: 0,
        }
    }
}

struct FileState {
    fingerprint: Fingerprint,
    offset: u64,
    anchor: Vec<u8>,
    entries: HashMap<HistoryKey, Contribution>,
}

#[derive(Clone)]
struct Contribution {
    // First immutable payload and last effective mutable values within a file.
    entry: HistoryEntry,
    first_offset: u64,
}
impl Contribution {
    fn absorb(&mut self, newer: &HistoryEntry) {
        self.entry.hidden = newer.hidden;
        if newer.removed_at_unix.is_some() {
            self.entry.removed_at_unix = newer.removed_at_unix;
        }
        if newer.picked_up_by.is_some() {
            self.entry.picked_up_by.clone_from(&newer.picked_up_by);
        }
        if newer.record.is_some() {
            self.entry.record.clone_from(&newer.record);
        }
    }
}

struct Delta {
    fingerprint: Fingerprint,
    offset: u64,
    anchor: Vec<u8>,
    reset: bool,
    entries: HashMap<HistoryKey, Contribution>,
}

impl HistoryDb {
    pub(in crate::history) fn replay_logs(&self, quiet: bool) -> Result<(), StateError> {
        let Some(dir) = self.jsonl.as_ref().and_then(|p| p.parent()) else {
            return Ok(());
        };
        // Only reconciliation takes this mutex; readers and local writers do not.
        let mut cache = self.sync.incremental.lock().unwrap();
        let generation = self.sync.generation.load(Ordering::SeqCst);
        let paths = log_paths(dir)?;
        if !cache.files.is_empty() && paths.is_empty() && !fsx::exists(dir)? {
            return Err(StateError::Corrupt(
                "history log directory unavailable".into(),
            ));
        }
        let mut snapshots = Vec::new();
        for path in paths {
            self.replay_checkpoint(quiet, generation)?;
            let file = fsx::open(&path)?;
            let meta = fsx::ctx(file.metadata(), "inspect history log", &path)?;
            snapshots.push((Fingerprint::new(path, &meta), file));
        }
        let fingerprints: Vec<_> = snapshots.iter().map(|(fp, _)| fp.clone()).collect();
        let full = {
            let mut p = self.sync.progress.lock().unwrap();
            let full = !quiet
                || !cache.initialized
                || self.sync.dirty.load(Ordering::SeqCst)
                || !p.last_full.is_some_and(|t| t.elapsed() < VERIFY_INTERVAL);
            if !full && p.fingerprints == fingerprints {
                p.skipped_unchanged += 1;
                *self.sync.last_scan.lock().unwrap() = ScanStats {
                    kind: "unchanged",
                    ..Default::default()
                };
                return Ok(());
            }
            p.passes += 1;
            full
        };
        let mut stats = ScanStats {
            kind: if full { "full" } else { "incremental" },
            ..Default::default()
        };
        let floor = self.ingest_floor()?;
        let mut deltas = BTreeMap::new();
        let mut affected = HashSet::new();
        let mut tombstones = HashSet::new();
        let present: HashSet<_> = fingerprints.iter().map(|fp| fp.path.clone()).collect();
        // Losing a file does not delete history or tombstones. Re-evaluate its
        // keys from remaining contributions, just as a sorted full replay does.
        for (path, old) in &cache.files {
            if !present.contains(path) {
                affected.extend(old.entries.keys().copied());
            }
        }
        for (fp, mut file) in snapshots {
            self.replay_checkpoint(quiet, generation)?;
            let old = cache.files.get(&fp.path);
            if !full && old.is_some_and(|old| old.fingerprint == fp) {
                continue;
            }
            let mut reset = full
                || old.is_none_or(|old| {
                    !same_file(&old.fingerprint, &fp) || fp.len < old.fingerprint.len
                });
            if !reset {
                let old = old.unwrap();
                // A same-size rewrite is not an append. For growth, verify the
                // committed boundary to catch truncate/regrow and replacement.
                reset = fp.len == old.fingerprint.len
                    || self.read_anchor(&mut file, &fp.path, old.offset)? != old.anchor;
            }
            let offset = if reset { 0 } else { old.unwrap().offset };
            if reset {
                stats.rebuilt += 1;
                if let Some(old) = old {
                    affected.extend(old.entries.keys().copied());
                }
            }
            fsx::ctx(
                file.seek(SeekFrom::Start(offset)),
                "seek history suffix",
                &fp.path,
            )?;
            let mut reader = BufReader::new((&mut file).take(fp.len - offset));
            let mut committed = offset;
            let mut consumed = offset;
            let mut entries: HashMap<HistoryKey, Contribution> = HashMap::new();
            let mut line = Vec::new();
            loop {
                self.replay_checkpoint(quiet, generation)?;
                line.clear();
                let n = fsx::ctx(
                    reader.read_until(b'\n', &mut line),
                    "read history suffix",
                    &fp.path,
                )?;
                if n == 0 {
                    break;
                }
                self.sync.bytes.fetch_add(n as u64, Ordering::Relaxed);
                consumed += n as u64;
                if line.last() != Some(&b'\n') {
                    stats.incomplete += 1;
                    break; // Never publish or advance through an unfinished line.
                }
                let at = committed;
                committed = consumed;
                if serde_json::from_slice::<HistoryMutationProbe<'_>>(&line)
                    .ok()
                    .and_then(|p| p.op)
                    == Some("tombstone")
                {
                    if let Ok(HistoryMutation::Tombstone {
                        job,
                        completed_at_unix,
                    }) = serde_json::from_slice(&line)
                    {
                        tombstones.insert((job.0, completed_at_unix));
                        continue;
                    }
                } else if let Ok(entry) = serde_json::from_slice::<HistoryEntry>(&line) {
                    if entry.completed_at_unix >= floor {
                        let key = (entry.job.0, entry.completed_at_unix);
                        affected.insert(key);
                        let contribution = entries.entry(key).or_insert_with(|| {
                            if !reset {
                                if let Some(previous) = old.and_then(|old| old.entries.get(&key)) {
                                    return previous.clone();
                                }
                            }
                            Contribution {
                                entry: entry.clone(),
                                first_offset: at,
                            }
                        });
                        contribution.absorb(&entry);
                    }
                    continue;
                }
                // One aggregate diagnostic per pass, never one warning per line.
                stats.malformed += 1;
            }
            drop(reader);
            if consumed != fp.len {
                return Err(StateError::Corrupt(format!(
                    "history log {} changed while reading",
                    fp.path.display()
                )));
            }
            let after = fsx::ctx(file.metadata(), "inspect history snapshot", &fp.path)?;
            if after.len() < fp.len
                || (after.len() == fp.len && after.modified().ok() != fp.modified)
            {
                return Err(StateError::Corrupt(
                    "history log truncated during ingestion".into(),
                ));
            }
            let anchor = self.read_anchor(&mut file, &fp.path, committed)?;
            deltas.insert(
                fp.path.clone(),
                Delta {
                    fingerprint: fp,
                    offset: committed,
                    anchor,
                    reset,
                    entries,
                },
            );
        }
        // Portable tombstones always precede entry publication, including when
        // an entry and its deletion arrive in different files in the same pass.
        self.replay_tombstones(&tombstones, quiet, generation)?;
        let mut merged = Vec::new();
        for key in affected {
            self.replay_checkpoint(quiet, generation)?;
            if key.1 < floor || tombstones.contains(&key) {
                continue;
            }
            let mut value: Option<(PathBuf, Contribution)> = None;
            for fp in &fingerprints {
                let delta = deltas.get(&fp.path);
                let contribution = delta.and_then(|d| d.entries.get(&key)).or_else(|| {
                    if delta.is_some_and(|d| d.reset) {
                        None
                    } else {
                        cache.files.get(&fp.path).and_then(|f| f.entries.get(&key))
                    }
                });
                if let Some(contribution) = contribution {
                    if let Some((_, merged)) = &mut value {
                        merged.absorb(&contribution.entry);
                    } else {
                        value = Some((fp.path.clone(), contribution.clone()));
                    }
                }
            }
            if let Some(value) = value {
                merged.push(value);
            }
        }
        // New cursor IDs follow first encounter in sorted-file/line order.
        // Existing IDs and immutable payloads are left untouched by replay_entry.
        merged.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.first_offset.cmp(&b.1.first_offset)));
        stats.entries = merged.len() as u64;
        let mut batch = Vec::new();
        let mut bytes = 0;
        for (_, contribution) in merged {
            bytes += serde_json::to_vec(&contribution.entry)?.len();
            batch.push(contribution.entry);
            if batch.len() >= 16 || bytes >= 128 * 1024 {
                self.replay_batch(&batch, quiet, generation)?;
                batch.clear();
                bytes = 0;
            }
        }
        self.replay_batch(&batch, quiet, generation)?;
        let _mutation = self.sync.mutation.lock().unwrap();
        self.replay_checkpoint(quiet, generation)?;
        // Only now may offsets advance. Any failure above keeps the old cache,
        // even if some SQLite batches committed; replay_entry is idempotent.
        cache.files.retain(|path, _| present.contains(path));
        for (path, delta) in deltas {
            if delta.reset {
                cache.files.insert(
                    path,
                    FileState {
                        fingerprint: delta.fingerprint,
                        offset: delta.offset,
                        anchor: delta.anchor,
                        entries: delta.entries,
                    },
                );
            } else {
                let old = cache.files.get_mut(&path).unwrap();
                old.fingerprint = delta.fingerprint;
                old.offset = delta.offset;
                old.anchor = delta.anchor;
                old.entries.extend(delta.entries);
            }
        }
        cache.initialized = true;
        let mut p = self.sync.progress.lock().unwrap();
        p.fingerprints = fingerprints;
        if full {
            p.last_full = Some(Instant::now());
        }
        self.sync.dirty.store(false, Ordering::SeqCst);
        if stats.malformed > 0 {
            tracing::warn!(
                lines = stats.malformed,
                "skipped unrecognized complete history lines"
            );
        }
        *self.sync.last_scan.lock().unwrap() = stats;
        Ok(())
    }

    fn read_anchor(
        &self,
        file: &mut File,
        path: &Path,
        offset: u64,
    ) -> Result<Vec<u8>, StateError> {
        let start = offset.saturating_sub(ANCHOR_BYTES);
        fsx::ctx(
            file.seek(SeekFrom::Start(start)),
            "seek history boundary",
            path,
        )?;
        let mut bytes = vec![0; (offset - start) as usize];
        fsx::ctx(file.read_exact(&mut bytes), "read history boundary", path)?;
        self.sync
            .bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

fn same_file(a: &Fingerprint, b: &Fingerprint) -> bool {
    #[cfg(unix)]
    {
        a.dev == b.dev && a.ino == b.ino
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    } // Conservative until a stable identity is available.
}

#[cfg(test)]
mod tests {
    use super::super::tests::entry;
    use super::*;
    use std::io::Write;

    struct Pair {
        fast: HistoryDb,
        reference: HistoryDb,
        logs: PathBuf,
        _temp: tempfile::TempDir,
    }
    impl Pair {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let logs = temp.path().join("logs");
            let fast = HistoryDb::open(&temp.path().join("fast.sqlite"), Some(&logs)).unwrap();
            let reference =
                HistoryDb::open(&temp.path().join("reference.sqlite"), Some(&logs)).unwrap();
            Self {
                fast,
                reference,
                logs,
                _temp: temp,
            }
        }
        fn append(&self, name: &str, rows: &[HistoryEntry]) {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.logs.join(name))
                .unwrap();
            for row in rows {
                serde_json::to_writer(&mut f, row).unwrap();
                f.write_all(b"\n").unwrap();
            }
        }
        fn replace(&self, name: &str, rows: &[HistoryEntry]) {
            let tmp = self.logs.join("replacement.tmp");
            let mut f = std::fs::File::create(&tmp).unwrap();
            for row in rows {
                serde_json::to_writer(&mut f, row).unwrap();
                f.write_all(b"\n").unwrap();
            }
            drop(f);
            std::fs::rename(tmp, self.logs.join(name)).unwrap();
        }
        fn step(&self) {
            self.fast.sync.bytes.store(0, Ordering::Relaxed);
            self.fast.replay_logs(true).unwrap();
            self.reference.full_replay_logs(false).unwrap();
            self.equal();
        }
        fn equal(&self) {
            assert_eq!(
                serde_json::to_value(self.fast.list_filtered(10_000, true).unwrap()).unwrap(),
                serde_json::to_value(self.reference.list_filtered(10_000, true).unwrap()).unwrap()
            );
        }
    }

    #[test]
    fn append_merges_all_file_contributions_in_reference_order() {
        let p = Pair::new();
        let mut first = entry(1, 100);
        first.name = "first immutable payload".into();
        first.removed_at_unix = Some(10);
        first.record = Some(crate::JobRecord {
            original_name: Some("first record".into()),
            ..Default::default()
        });
        let mut later_in_a = first.clone();
        later_in_a.name = "must never replace the first name".into();
        later_in_a.removed_at_unix = None;
        later_in_a.record = None;
        let mut b = later_in_a.clone();
        b.hidden = true;
        b.picked_up_by = Some("B wins".into());
        p.append(
            "history.a.jsonl",
            &[first, later_in_a.clone(), entry(4, 400)],
        );
        p.append("history.b.jsonl", &[b.clone(), entry(2, 200)]);
        p.step();
        for db in [&p.fast, &p.reference] {
            db.conn.lock().unwrap().execute("UPDATE history SET first_seen=11,last_seen=22,seen_count=7,portable_synced=1 WHERE job_id=1", []).unwrap();
        }
        later_in_a.hidden = false;
        later_in_a.picked_up_by = Some("new append in A still loses to B".into());
        p.append("history.a.jsonl", &[later_in_a]);
        p.step();
        let rows = p.fast.list_filtered(10, true).unwrap();
        let one = rows.iter().find(|e| e.job.0 == 1).unwrap();
        assert!(one.hidden);
        assert_eq!(one.name, "first immutable payload");
        assert_eq!(one.removed_at_unix, Some(10));
        assert_eq!(one.picked_up_by.as_deref(), Some("B wins"));
        assert_eq!(
            one.record.as_ref().unwrap().original_name.as_deref(),
            Some("first record")
        );
        assert_eq!(one.seen_count, 7);
        assert_eq!(p.fast.sync_status().last_entries_reconciled, 1);
        #[cfg(unix)]
        assert_eq!(p.fast.sync_status().last_files_rebuilt, 0);
        assert_eq!(p.fast.sync_status().last_scan, "incremental");
        // A newly discovered file belongs at its sorted position, not at the end.
        let mut middle = b;
        middle.hidden = false;
        middle.picked_up_by = Some("AA must also lose".into());
        p.append("history.aa.jsonl", &[middle, entry(3, 300)]);
        p.step();
    }

    #[test]
    fn replacement_disappearance_and_truncation_match_full_replay() {
        let p = Pair::new();
        let a = entry(1, 100);
        let mut b = a.clone();
        b.hidden = true;
        b.removed_at_unix = Some(20);
        p.append("history.a.jsonl", std::slice::from_ref(&a));
        p.append("history.b.jsonl", &[b]);
        p.step();
        p.replace("history.b.jsonl", &[entry(2, 200)]);
        p.step();
        let one = p
            .fast
            .list_filtered(10, true)
            .unwrap()
            .into_iter()
            .find(|e| e.job.0 == 1)
            .unwrap();
        assert!(!one.hidden);
        assert_eq!(
            one.removed_at_unix,
            Some(20),
            "missing COALESCE contribution cannot clear retained metadata"
        );
        std::fs::remove_file(p.logs.join("history.a.jsonl")).unwrap();
        p.step();
        assert_eq!(
            p.fast.count_filtered(true).unwrap(),
            2,
            "disappearance is not deletion"
        );
        std::fs::write(p.logs.join("history.b.jsonl"), b"").unwrap();
        p.step();
        p.append("history.a.jsonl", &[a]);
        p.step();
    }

    #[test]
    fn incomplete_line_is_neither_published_nor_consumed_until_newline() {
        let p = Pair::new();
        let path = p.logs.join("history.a.jsonl");
        let row = serde_json::to_vec(&entry(1, 100)).unwrap();
        std::fs::write(&path, &row[..row.len() / 2]).unwrap();
        p.fast.replay_logs(true).unwrap();
        assert_eq!(p.fast.count_filtered(true).unwrap(), 0);
        assert_eq!(
            p.fast.sync.incremental.lock().unwrap().files[&path].offset,
            0
        );
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&row[row.len() / 2..]).unwrap();
        p.fast.replay_logs(true).unwrap();
        assert_eq!(
            p.fast.count_filtered(true).unwrap(),
            0,
            "even valid JSON needs a complete line"
        );
        assert_eq!(p.fast.sync_status().last_incomplete_tails, 1);
        f.write_all(b"\nunknown future format\n").unwrap();
        p.step();
        assert_eq!(p.fast.sync_status().last_malformed_lines, 1);
        assert_eq!(
            p.fast.sync.incremental.lock().unwrap().files[&path].offset,
            row.len() as u64 + 23
        );
    }

    #[test]
    fn failed_batch_does_not_advance_offsets_and_retry_preserves_cursors() {
        let p = Pair::new();
        let path = p.logs.join("history.a.jsonl");
        p.append(
            "history.a.jsonl",
            &(1..=40).map(|id| entry(id, id as i64)).collect::<Vec<_>>(),
        );
        p.fast.conn.lock().unwrap().execute_batch("CREATE TRIGGER injected BEFORE INSERT ON history WHEN new.job_id=17 BEGIN SELECT RAISE(FAIL,'injected'); END;").unwrap();
        assert!(p.fast.replay_logs(true).is_err());
        assert_eq!(p.fast.count_filtered(true).unwrap(), 16);
        assert!(!p
            .fast
            .sync
            .incremental
            .lock()
            .unwrap()
            .files
            .contains_key(&path));
        p.fast
            .conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER injected")
            .unwrap();
        p.step();
        // Losing every derived byte offset (as on process restart) is safe.
        *p.fast.sync.incremental.lock().unwrap() = Cache::default();
        p.step();
        assert_eq!(p.fast.sync_status().last_scan, "full");
        assert_eq!(p.fast.list_since(16, 100).unwrap()[0].seq, 17);
    }

    #[test]
    fn tombstones_stay_monotone_across_suffixes_replacement_and_restart() {
        let p = Pair::new();
        p.append("history.a.jsonl", &[entry(1, 100)]);
        p.step();
        let tombstone = HistoryMutation::Tombstone {
            job: crate::JobId(1),
            completed_at_unix: 100,
        };
        std::fs::write(
            p.logs.join("history.z.jsonl"),
            format!("{}\n", serde_json::to_string(&tombstone).unwrap()),
        )
        .unwrap();
        p.append("history.a.jsonl", &[entry(1, 100)]);
        p.step();
        assert_eq!(p.fast.count_filtered(true).unwrap(), 0);
        p.replace("history.z.jsonl", &[]);
        p.append("history.a.jsonl", &[entry(1, 100), entry(2, 200)]);
        p.step();
        *p.fast.sync.incremental.lock().unwrap() = Cache::default();
        p.step();
        assert_eq!(p.fast.count_filtered(true).unwrap(), 1);
    }

    #[test]
    fn startup_reconstructs_cache_without_changing_index_metadata() {
        let p = Pair::new();
        p.append("history.a.jsonl", &[entry(2, 200), entry(1, 100)]);
        p.step();
        let Pair {
            fast,
            reference: _reference,
            logs,
            _temp,
        } = p;
        let path = _temp.path().join("fast.sqlite");
        let before = serde_json::to_value(fast.list_filtered(10, true).unwrap()).unwrap();
        // W5b: the index is EXCLUSIVE to one connection; the restart closes it.
        drop(fast);
        let reopened = HistoryDb::open(&path, Some(&logs)).unwrap();
        assert_eq!(
            serde_json::to_value(reopened.list_filtered(10, true).unwrap()).unwrap(),
            before
        );
        assert_eq!(reopened.sync_status().last_scan, "full");
        assert_eq!(reopened.sync.incremental.lock().unwrap().files.len(), 1);
    }

    #[test]
    fn local_mutation_already_in_progress_fences_later_cache_publication() {
        let temp = tempfile::tempdir().unwrap();
        let logs = temp.path().join("logs");
        let db = Arc::new(HistoryDb::open(&temp.path().join("db.sqlite"), Some(&logs)).unwrap());
        db.record(&entry(1, 100)).unwrap();
        db.replay_logs(false).unwrap();
        let offset = db.sync.incremental.lock().unwrap().files[&logs.join("history.jsonl")].offset;
        // Grow the file before scanning, then keep a local mutation open while
        // the scanner captures its starting generation and reads the old rows.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(logs.join("history.jsonl"))
            .unwrap();
        writeln!(f, "{}", serde_json::to_string(&entry(2, 200)).unwrap()).unwrap();
        let fence = MutationGuard::new(&db);
        db.sync.bytes.store(0, Ordering::Relaxed);
        let expected = std::fs::metadata(logs.join("history.jsonl")).unwrap().len();
        let other = db.clone();
        let worker = std::thread::spawn(move || other.replay_logs(true));
        let start = Instant::now();
        while db.sync.bytes.load(Ordering::Relaxed) < expected {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "scanner did not reach mutation fence"
            );
            std::thread::yield_now();
        }
        db.conn
            .lock()
            .unwrap()
            .execute("UPDATE history SET hidden=1 WHERE job_id=1", [])
            .unwrap();
        let mut hidden = entry(1, 100);
        hidden.hidden = true;
        db.append_jsonl(&hidden).unwrap();
        fence.commit();
        assert!(worker.join().unwrap().is_err());
        assert!(
            db.list_filtered(10, true)
                .unwrap()
                .iter()
                .find(|e| e.job.0 == 1)
                .unwrap()
                .hidden
        );
        assert_eq!(
            db.sync.incremental.lock().unwrap().files[&logs.join("history.jsonl")].offset,
            offset
        );
        db.replay_logs(true).unwrap();
        assert_eq!(db.count_filtered(true).unwrap(), 2);
    }

    #[test]
    #[cfg(unix)]
    fn small_append_reads_suffix_not_accumulated_history() {
        let p = Pair::new();
        let rows: Vec<_> = (1..=1000)
            .map(|id| {
                let mut row = entry(id, id as i64);
                row.record = Some(crate::JobRecord {
                    original_name: Some("x".repeat(1024)),
                    ..Default::default()
                });
                row
            })
            .collect();
        p.append("history.a.jsonl", &rows);
        p.step();
        let old_len = std::fs::metadata(p.logs.join("history.a.jsonl"))
            .unwrap()
            .len();
        p.append("history.a.jsonl", &[entry(1001, 1001)]);
        p.step();
        let new_len = std::fs::metadata(p.logs.join("history.a.jsonl"))
            .unwrap()
            .len();
        let bytes = p.fast.sync.bytes.load(Ordering::Relaxed);
        assert_eq!(bytes, new_len - old_len + 2 * ANCHOR_BYTES);
        assert!(bytes < old_len / 100);
        assert_eq!(p.fast.sync_status().last_entries_reconciled, 1);
        #[cfg(unix)]
        assert_eq!(p.fast.sync_status().last_files_rebuilt, 0);
    }

    #[test]
    fn periodic_verification_detects_same_identity_rewrites_and_honors_retention() {
        let p = Pair::new();
        p.append("history.a.jsonl", &[entry(1, 100), entry(2, 200)]);
        p.step();
        // Full verification is the conservative fallback for edits that do not
        // respect the append-only protocol, including edits away from anchors.
        std::fs::write(
            p.logs.join("history.a.jsonl"),
            format!("{}\n", serde_json::to_string(&entry(3, 300)).unwrap()),
        )
        .unwrap();
        p.fast.sync.progress.lock().unwrap().last_full = None;
        p.step();
        for db in [&p.fast, &p.reference] {
            db.set_retention(Retention {
                keep_max: 1,
                keep_days: 0,
            })
            .unwrap();
            db.prune(400).unwrap();
        }
        p.append("history.a.jsonl", &[entry(1, 100)]);
        p.step();
        assert_eq!(p.fast.count_filtered(true).unwrap(), 1);
    }

    #[test]
    fn deletion_suffix_uses_indexed_writer_work_without_the_page_reader() {
        let t = tempfile::tempdir().unwrap();
        let logs = t.path().join("logs");
        let db = Arc::new(HistoryDb::open(&t.path().join("db.sqlite"), Some(&logs)).unwrap());
        db.record(&entry(1, 100)).unwrap();
        db.record(&entry(2, 200)).unwrap();
        db.replay_logs(false).unwrap();
        {
            let mut conn = db.conn.lock().unwrap();
            let tx = conn.transaction().unwrap();
            for id in 100..5100 {
                tx.execute(
                    "INSERT INTO history_tombstones(job_id,completed_at) VALUES(?1,100)",
                    [id],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        let mut f = std::fs::File::create(logs.join("history.peer.jsonl")).unwrap();
        for id in [1, 100] {
            writeln!(
                f,
                "{}",
                serde_json::to_string(&HistoryMutation::Tombstone {
                    job: crate::JobId(id),
                    completed_at_unix: 100,
                })
                .unwrap()
            )
            .unwrap();
        }
        db.replay_logs(true).unwrap();
        assert_eq!(db.list_filtered(10, true).unwrap()[0].job.0, 2);
        assert_eq!(db.count_filtered(true).unwrap(), 1);
        assert_eq!(
            db.conn
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM history_tombstones", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            5001
        );
    }

    #[test]
    fn generated_cross_file_updates_match_reference_after_every_pass() {
        let p = Pair::new();
        let mut seed = 91_u64;
        for round in 0..80 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = ((seed >> 32) % 9 + 1) as u32;
            let file = format!(
                "history.{}.jsonl",
                (b'a' + ((seed >> 40) % 3) as u8) as char
            );
            let mut row = entry(id, id as i64);
            row.hidden = seed & 1 != 0;
            row.removed_at_unix = (seed & 2 != 0).then_some(round);
            row.picked_up_by = (seed & 4 != 0).then(|| format!("client-{round}"));
            row.record = (seed & 8 != 0).then(|| crate::JobRecord {
                original_name: Some(format!("version-{round}")),
                ..Default::default()
            });
            if round % 13 == 0 {
                p.replace(&file, &[row]);
            } else {
                p.append(&file, &[row]);
            }
            p.step();
        }
    }
}
