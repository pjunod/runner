# nzbd — Project Status

## Pipeline contract v2 M6a — one clock per job, errno not text, docs from the contract — 2026-11-08

**Status:** draft PR (#PR) on `claude/pipeline-v2-m6a-runner` (runner 0.5.0),
base `main` (M3 #283 merged in). Not reviewed, not merged, not deployed.
Safe to merge right after M3: no wire field is removed and nothing persisted
changes shape. Plan: `IMPL_RUNNER.md` §8 / `PIPELINE_REWRITE_PLAN.md` M6.
M6b (wire removals, persisted `Held`) is stacked on this branch and merges
only after deploy confirmation.

- **Backoff clocks collapsed** to the hold ladder and the disposal ladder.
  Holds: the owner's `HOLD_RETRY_DELAYS_SECONDS` only — the PP manager's
  second scheduler (`schedule_recoverable_post_retry` /
  `prepare_recoverable_post_retry`, `POST_RETRY_DELAYS_SECONDS`, its jitter)
  and `QueueCommand::SchedulePostRetry` / `EngineHandle::schedule_post_retry`
  are deleted; the owner already schedules every timed hold it writes and
  re-arms an unscheduled one on load. Disposal: `Operation.next_retry`. The
  `*PP:failed-cleanup-next-retry/blocked-since/last-error` params (no writer
  since M1a) are dropped when the queue loads (`drop_retired_params`);
  `failed_cleanup` on the job summary keeps the attempt count. The
  verification backoff (`owner_recovery.rs`) is unchanged.
- **Errno, not text.** `is_out_of_space` / `is_storage_unavailable` /
  `io_hold_cause` are replaced by `errno_hold_cause`, `io_error_hold_cause`
  (`raw_os_error()`, else the std kind, else the wrapped error),
  `error_hold_cause` (walks `source()` to the io error or a SQLite
  `DiskFull`/`SystemIoFailure` code) and `stderr_hold_cause` — the one text
  rule, for subprocess output. Writer errors, payload resolution, allocation,
  terminal write faults, `pp_verdict`, salvage (now `Result<_, PostError>`),
  disposal and torrent io errors are typed. Still text, each named at its
  call site: par2/unrar/7z output and `PostError::Subprocess`,
  `EngineError::Lifecycle` messages, a torrent backend's error string, a
  persisted operation error, and the pre-v2 `io` hold message (goes in M6b).
- **cfg(test) twins deleted** where a test-only wrapper carried the doc
  comment of the production path: `Owner::reschedule_hold`,
  `Inventory::reconcile_deleted_artifacts`, `reconcile_transforms(_root)`,
  `ControlStore::commit_url_outbox`, `tls::server_config`. `spawn_writer`
  stays as test support and says so.
- **Docs.** `docs/INTEGRATION.md` rewritten from the contract with every
  literal read from `event_json` and the handlers (deprecated fields marked
  "removed in M6b"); 29 superseded plans and finished status/review records
  moved to `docs/archive/` with one-line pointers; `docs/README.md` indexes
  every file; ADR-20 (`docs/adr/0020-pipeline-contract-v2.md`, row in
  `ARCHITECTURE.md` §15).
- **Accept tests.** `nzbd-api` `integration_doc_names_every_sse_event` (doc
  event tables == `event_json` names and the SSE handler's frame literals;
  a new `Event` variant does not compile until listed);
  `crates/nzbd/tests/docs_index.rs` (index lists every doc; every relative
  link in `docs/` and `README.md` resolves).
- **Rolling-upgrade read for M6b:** `JobStatus::Paused` deserializes
  `"held"` (`#[serde(alias = "held")]`), so a node on this release reads the
  persisted `held` status an M6b node writes as today's representation
  (`Paused` + `control.lifecycle == "held"`). M6b therefore needs every node
  on **0.5.0 (M6a)**, not merely M1a: an M1a/M3 node cannot decode `held`.
- **Found doing it:** the web job detail read `post_timing`, `url_fetch` and
  `failed_cleanup` under camelCase keys the API never sends, so none of the
  three lines ever rendered; fixed, asserted in the UI DOM harness. An
  extractor's full-disk report during salvage settled the obligation empty;
  it is now a `StorageFull` io error and the obligation waits for space.

**Deleted (non-test, approx.):** PP retry scheduler ~95 lines,
`SchedulePostRetry` command/handle ~70, failed-cleanup progress fields and
param list ~15, text classifiers ~30, cfg(test) wrappers ~35. Whole-branch
non-test diff under `crates/`: +901 / −614 (includes the new classifier,
its tests in `lib.rs`, and the 198-line doc test).

**Behaviour change to review:** an `io::Error` whose *message* names a full
or dropped volume but whose errno does not (e.g. `io::Error::other("… (os
error 5)")`) is now a terminal `failed/io` (parked, no blame) instead of a
hold. Real errno-bearing errors classify exactly as before.

**Fast lane run:** `cargo clippy -p nzbd-post -p nzbd-engine -p nzbd-api
-p nzbd-cluster -p nzbd-state -p nzbd --all-targets --features
nzbd-state/test-support -- -D warnings` clean; `cargo test -p nzbd-engine
--lib` 336/337 (the failure is `volumes::…inaccessible_path…`, a mode-000
directory check that cannot fail as root in this sandbox; file untouched);
`cargo test -p nzbd-post --lib` 120/120; `nzbd-post` `failure_regressions`
6/6, `terminal_first` 15/15, `pp_pipeline` capacity hold 1/1; `nzbd-engine`
e2e enospc/stranded/post-hold 3/3; `nzbd-state --lib` transforms /
writer_naming / relocation_failure / artifacts tests 123 passed; `nzbd-api
--lib` torrent_admission 25/25 and the doc test; `nzbd` `docs_index` 2/2
and `tls` 6/6; `nzbd-cluster --lib delivery` 3/3. Full suite not run (the
orchestrator's merge gate).

**Not verified:** the UI DOM harness assertion (`crates/nzbd/tests/ui_dom.rs`
needs the full `nzbd` test build; left to the gate); behaviour on macOS
(`EREMOTEIO` is Linux-only and gated); anchors inside doc links (the link
test checks paths only).

**Decisions for Paul:** versions — M6a 0.5.0, M6b 0.6.0.
`FILE_LIFECYCLE_OPERATIONS.md` stays live (it is the operations reference
M3 updated), while `FILE_LIFECYCLE_PLAN/REVIEW/STATUS` are archived. The
BitTorrent reports stay in place because the release-review script reads
them. Out-of-repo links in the archived file-lifecycle docs (to a sibling
`monarr` checkout) became code spans.

**Post-deploy checks:** none new — no wire or schema change. A held job's
`control.next_retry_at_unix` still advances 30 s → 1 m → 2 m → 5 m
(`curl -s $NZBD/api/v1/jobs | jq '.jobs[] | select(.status_projected=="held") | .control | {cause, retry_attempt, next_retry_at_unix}'`).

## Pipeline contract v2 M3 — journaled per-file publication, one hash, no ack — 2026-11-07

**Status:** draft PR [#283](https://github.com/pjunod/runner/pull/283) on
`claude/pipeline-v2-m3-runner` (runner 0.4.0), `main` (with M1a #281) merged
in (latest `1f73dea`, PR #284); review round 1 addressed (below). Not merged, not deployed. Plan:
`IMPL_RUNNER.md` §7 / `PIPELINE_REWRITE_PLAN.md` M3.

**M3 must not reach nuc3 before curator #95 (M2) is deployed there.** Curator
v1 retries `accept`'s 410 forever (a visible `Error`, warn logs) and, with the
`accepted` filter gone, re-pages the whole `/deliveries` feed every sweep.

**Review r1 (#283, Fable) — resolutions:**
- *1 (blocking) manager at boot:* `crash_after_*_finishes_with_manager_at_boot`
  for every crash point (10 s delivery deadline, inside the 30 s rescan).
  Root causes: `job_status_changed` was keyed on the owner's status, which
  never leaves `Post` during boot verification (only the snapshot shows
  `queued`) — now emitted from the published projection, after it is stored;
  the manager now rescans on the owner's next queue change after a deferred
  export/admission, on a `completed` terminal's emission and when an attempt
  ends with one; `drop_unneeded_verifications` lifted its fence without an
  epoch bump; boot verification and payload recovery failed against a
  restart's reconciliation lease and backed off 10 s+ (they now wait on its
  release); `resume_committed_publication` wrote the delivery row **before**
  the terminal (now terminal, then row, then source retirement);
  `finish_stamped_delivery` reads the owed delivery from the terminal row.
  The publication suite went from ~230 s to ~30 s.
- *2 (blocking) data loss:* a cross-volume unwind removes a copy only while
  the source holds the journaled original; a linked repair input drops its
  publication name only while the source name is the same inode; otherwise
  conflict, copy retained. Tests for both.
- *3:* `Finishing` re-entry re-locates unjournaled renames; new crash point
  `after_rename_before_journal` in the inventory and post (c) suites.
- *4:* *Abandon move and retain original* (Files/Attention, the existing
  `abandon-relocation` route) covers per-file moves: unwind, or close without
  deleting anything and register the target for review; the job then fails
  `io` and parks. A stuck move's operation error is mirrored onto the
  artifact.
- *5:* the cross-volume capacity reservation is held for the whole move (and
  a restart's finish reserves what is left to copy).
- *6:* (e) asserts its one outcome: boot verification holds the job for
  repair, no terminal.
- *7:* re-covered — a recovery copy whose streamed bytes disagree with the
  journaled digest never lands; a lease expiring mid-copy stops at the next
  chunk and the same attempt finishes.
- *8:* INTEGRATION §2a documents what identity without a digest proves
  (inode/device/size/mtime/ctime).
- *9:* a second selected publication of a generation is `Conflict`.
- *10:* intents whose artifact/operation is gone close as unwound with
  evidence; a pre-M3 queued deletion unwinds the move.
- *11:* cluster `accept` answers 410 before any lookup.
- *12:* one `RETRY_LADDER_SECONDS` / `retry_delay()` replaces eight copies.
- *13:* the cross-volume tests find a second device (tmpfs, temp dir, test
  binary's volume) and fail under `NZBD_REQUIRE_TOOLS` instead of skipping.
- Contract copy: §2.5's "rename" admits a hard link for PAR repair inputs.

**M6 must delete** (rollback shims kept for one release): the
`deliveries.accepted_at`/`consumer_receipt_id` columns, `delivery_row_json`'s
`status`, the cluster `encode_record` status shim and
`cluster_deliveries.accepted_at`, both `include_accepted` params (local and
cluster), the v1 delivery row write and `pp_status`/`final_dir`,
`custodyState`/`sourceCleanupStatus`, and the `review: interrupted move` hold
rewrite in `commit_intent` (`intents.rs`).

- **The move is journaled before it starts.** A selected publication inserts
  its intent into the new `relocation_intents` table (generation, target,
  every selected file's identity, journaled digest and destination) in the
  same transaction as the relocation `Operation`, before the first rename.
  `crates/nzbd-state/src/artifacts/intents.rs`.
- **Selected files only, per file.** Same volume: `rename_exclusive`
  (renameat2 `RENAME_NOREPLACE`, PR #247 link + unlink fallback kept); a file
  without a journaled digest gets one read pass. Cross volume: one streaming
  copy per file into `<dst>.part` that hashes as it writes. The processing
  directory keeps only unselected siblings and is disposed of after the
  delivery row. The "always stage privately" branch for selections is gone.
  Exception (decision for Paul): a selected file that is still a PAR repair
  input of the source (`repair_dependency`) is linked, not renamed, so the
  set stays repairable after curator deletes the publication — the existing
  test `failed_health_par_tool_failure_still_publishes_good_sibling_and_retains_repair_set`
  requires it.
- **Manifest written last.** `.runner-manifest.json` = `{generation, job,
  transfer, created_at, cross_volume, files[{path,bytes,sha256}], digest}`,
  `.part` + fsync + exclusive rename, the deployed `runner-manifest-v1`
  digest. Publications are named `<name>.job-<id>.runner-<gen>`.
- **Restart reconciliation** (startup root workers, the 30 s tick, and every
  explicit retry / PP restart via `reconcile_job_intents`): manifest present →
  commit; every file found unchanged in the target or the source → finish;
  anything missing or changed → unwind (moved files back by identity, target
  removed, the generation's next publication fails `io`). One of the two,
  logged with per-file evidence. While an intent is open: `disposal_allowed`
  is `None`, deletion requests are refused, the retire sweep waits, and the
  source shows under Files → Attention.
- **One hash pass.** The delivery record reuses the journaled digests; with
  the scratch tree gone, so are the extra reads.
- **Delivery after history** (already M1a's #280-2 ordering, kept). The v1
  delivery record is still written for one release and now carries the
  manifest body (`manifest`). Salvage resumes a committed move from its
  journal instead of selecting again from the publication.
- **No acceptance.** `POST /deliveries/{id}/accept` answers `410 Gone` (local
  and cluster). Deleted: `accept_delivery`, `Delivery.accepted_at`,
  `consumer_receipt_id`, `Delivery.status` and its derivation, the
  `include_accepted` filters (local and cluster discovery — the query param
  is accepted and ignored for one release), the test-only
  `accept_cluster_delivery`. `/deliveries` is documented as the
  reconciliation feed; `custodyState`/`sourceCleanupStatus` are `deprecated`
  in `/api/v1/openapi.json`.
- **Park root failures are journaled** (orchestrator addition, contract
  §2.6): a move whose destination root cannot be created — a failed-source
  park onto an unavailable volume — now records a `relocate` operation in
  `retry` with the error and the 30 s/1 m/2 m/5 m ladder, notes it on the
  artifact (Files → Attention) and so drives `disposal_retry_at`; the same
  request supersedes it (keeping the attempt count) once the root exists.
  Test `park_root_creation_failure_lands_on_the_operation_ladder`.
- **Found while testing the journal:** a job's own attempt reconciles its
  open intent under its processing lease (a reconciliation lease is refused
  by the attempt's own use), and the retire sweep gates only the retirement
  on an open intent, not the salvage that finishes it.
- **Engine fix found by the restart tests:** a boot verification that was
  waiting for its payload root when the terminal replay stamped the job was
  never run or cleared, so the job could be neither exported nor admitted
  again (crash after the history write → restart hung). Such verifications
  are now dropped (`drop_unneeded_verifications`).
- **Tests** (fast lane; `TMPDIR=/dev/shm` because the shared workspace disk
  had < 1 GiB free, below the capacity guard's headroom), after review r1:
  - `crates/nzbd-post/tests/publication.rs` 15/15 — (a)
    `same_volume_publication_renames_selected_only`, (c)
    `crash_after_{rename_before_journal,first_rename,last_rename,manifest,history,delivery}`
    both `_finishes_on_restart` and `_finishes_with_manager_at_boot`, (e)
    `crash_with_corrupted_source_unwinds`, `delivery_row_written_after_history`.
    The one intermittent failure seen in review r1 work (no delivery in
    10 s, ~1 in 10 full-suite runs) was reproduced under CPU load and fixed
    at the root: the owner emits `job_pp_finished` **before** writing the
    terminal's `emitted` flag, so a manager scan woken by the event could
    read `emitted = false`, classify it as merely pending, and wait for the
    30 s rescan. The owner now announces a queue change when the terminal
    chain releases (`release_terminal`), and an unemitted terminal is a
    deferred follow-up that waits for it. Free disk at the failure was
    1.6 GB on `/` and 7.9 GB on the tmpfs the tests use — not disk
    exhaustion. After the fix: 20/20 full-suite runs under two `yes` loads.
    `terminal_first::queue_changes_after_the_emitted_flag_lands` fails
    without the fix and passes with it.
  - `nzbd-state` lib 340 passed, 2 ignored (pre-existing performance
    probes): `artifacts::intents::tests` 19, incl. (b)
    `manifest_digest_matches_curator_fixture`, (d) `nfs_fixture_fallback_chain`,
    the cross-volume set on a real second device (/dev/shm vs the disk).
  - `nzbd-post` lib 120, `pp_pipeline` 34, `terminal_first` 15,
    `failure_regressions` 6, `rename_producer` 2.
  - `nzbd-api` lib 88, `nzbd-cluster` lib 57, `nzbd-engine` lib 315/316 — the
    failure is environmental (`inaccessible_path_is_unknown…` sets mode 000,
    which root ignores; untouched by this branch).
  - Clippy `-D warnings` on the five touched crates, all targets.
- **CI gate 1 on #283** (`f492aa9`): the sealed cluster PP generation
  copied the worker publication's `.runner-manifest.json` as payload, so its
  file list never matched the journaled one ("selected PP publication changed
  after its durable journal committed", retried every 150 ms) and no cluster
  delivery appeared — the three `cluster_e2e` READY/remote-PP timeouts. The
  manifest is now excluded from the sealed payload (`c74608a`). Two
  `cluster_e2e` tests and one `nzbd` bin test asserted v1 behaviour M3
  removes (contract §2.5: accept → 410 for every id, publications named
  `<name>.job-<id>.runner-<gen>`) and were updated. Local: `cluster_e2e`
  22/22 in 237 s; `nzbd` bin 27/27.
- **Cluster publications carry the manifest** (orchestrator, contract
  §2.5): the leader, after landing the sealed payload in its publication
  directory and before the cluster delivery row, writes `.runner-manifest.json`
  through the single-node writer (`write_runner_manifest`: `.part`, fsync,
  exclusive rename, directory fsync); `generation` is the result id,
  `cross_volume` is true (the payload was copied), and its digest is the
  row's `manifestDigest`. `cluster_e2e`
  `delivery_receipts_survive_authority_restart_and_consumer_removed_publication`
  asserts it. Local: `cluster_e2e` 22/22, 250.6 s (plus a 64 s build);
  `nzbd-cluster` lib 57; `nzbd-state` lib 340.
- **Not verified:** `cargo test --workspace` and clippy `--workspace` (the
  gate's); `NZBD_REQUIRE_TOOLS=1`; a real NFS mount (no CI NFS fixture exists — (d) drives the
  fallback through the EINVAL rename seam); deploy.
- **Engine fix 2:** a boot verification that read an allocation the
  startup intent reconciliation had just replaced (the job's artifact moved
  to its publication) was blocked as `review` forever ("allocation authority
  changed before verification"). The owner now reloads the allocation and
  verifies again. Before this fix one run settled only after a 10 s + 20 s
  backoff; with it every restart test settles in about a second.
- **Post fix:** `finish_stamped_delivery` treated a transient export refusal
  (verification or journal loading) as "row gone" and retired the job
  without its delivery row; it now waits.
- **(e) end to end:** after the unwind, the engine's own boot verification
  sees the corrupted file's CRC and holds the job for a generation-safe
  repair before post-processing runs again; the `failed/io` terminal IMPL §7.2
  names happens when publication is attempted (asserted at the inventory
  level). The post-level test asserts the hold (review r1 #6). Nothing is
  published or disposed.
- **Decisions for Paul:**
  - Review r1 #4: an abandoned publication move fails the job `io` on its
    next attempt and parks the source (the generation keeps its one intent
    row), rather than allowing a second publication of that generation.
  - Review r1 #1: boot verification and payload recovery now block their
    worker thread on a reconciliation lease's release (condvar) instead of
    failing and backing off; the lease is held only for one pass.
  - `accept` answers `410` (IMPL §7.4; the contract's one-release `200`
    was M1a's release).
  - The `accepted_at`/`consumer_receipt_id` columns stay (written NULL, never
    read) and persisted rows keep `status:"ready"` for one release, so an M1a
    rollback or mixed-version cluster can still decode them; M6 drops both.
  - The publication generation is the source artifact's generation (IMPL
    said a fresh id), so delivery ids, `Delivery:Generation` and source
    retirement keep their keys.
  - `relocation_intents` has four columns beyond IMPL §3.3 (transfer,
    operation, source_dir, evidence) — reconciliation needs them.
  - Published PAR repair inputs are linked, not renamed (above).
  - The fs-ops counter is keyed by inventory, not thread-local (publication
    runs on a blocking worker); `rename` counts payload files, the
    manifest's own commit rename is `manifest_write`.
  - Cross-volume copies keep the source's mode (the scratch copy was 0600);
    the manifest is 0644.

**Post-deploy checks:** history `stages` show `move` p90 < 60 s over the first
day; `GET /api/v1/artifacts?filter=attention` lists no `transitioning`
sources after the first restart (no open intents).

## Pipeline contract v2 M1b — surfaces — 2026-11-07

**Status:** built on `claude/pipeline-v2-m1b-runner` (stacked on M1a, mobile
1.1.5, iOS build 17, Android 16), not merged, not deployed. Plan:
`IMPL_RUNNER.md` §6.

- **Web queue row:** a held job names its cause, then Runner's message, the
  retry timing and the deadline countdown (`deadline_unix`, else
  `deadline_in_s`), with the existing pause/retry button.
- **Web Files tab:** a parked source shows why (failure reason, assessment
  text, `salvage pending`) on the row and in the details panel. To carry the
  reason, `Artifact` gains `failure_reason`, written in the same transaction
  as the terminal row (`artifacts/terminals.rs`).
- **Mobile:** held cards show cause + message, `retries paused`, and the
  deadline countdown; `holdAction` no longer models a review policy. The app
  has no Files tab, so there is no parked-source surface there.
- **Docs:** USAGE (held row, parked sources), MOBILE (held card), INTEGRATION
  (`failure_reason`/`salvage` on artifact rows, `deadline_in_s` semantics).
- **Deleted:** the mobile review-hold special case (`AUTOMATIC` list and the
  "a person decides" branch).
- **Not verified:** no screenshots of a held row or a parked Files row (no
  browser or device in the build workspace); full `ui_dom` Rust gate not run
  (the node harness was run directly).
- **Decision for Paul:** `controlPending: review_required` ("Source needs
  review") is a control-acceptance state, not a hold, and was left alone.

## Pipeline contract v2 M1a — review rounds (#280, #281) — 2026-11-08

**Status:** all #280 findings fixed on `claude/pipeline-v2-m1a-runner`
(PR #281, draft). Highlights since the first entry below:

- Completed terminal before the delivery row and source retirement, with
  crash repair from the stamped `Delivery:*` markers.
- `PostError::Identity`; native torrent failures, partial torrents and
  worker post-processing results are typed terminals through the shared
  chain. In a cluster exactly one node records each terminal: the leader
  when it holds the history store, else the worker that writes the history
  row.
- Unrecorded terminals are kept and retried on the owner; a stored row is
  never forgotten.
- Failed-source assessment restored: `recoverable` / `unknown` (unproven
  payload) / `unusable`. Only `unusable` is deleted on the default policy.
- Cluster cleanup ledger: publish/io/identity/hold_expired raw copies are
  retained (park-always); a worker disposes its own attempt copy after the
  leader accepts the result.
- Flakes fixed at the root:
  - Transforms ran outside attempt custody, racing the artifact tick.
  - Operator Keep and recovery staging were refused while a maintenance
    lease was held; they now wait for the lease to be released.

**Behaviour change for Paul:** failed-source disposal now retries on the
disposal operation's own durable `next_retry` ladder until it succeeds,
visible on `/api/v1/artifacts`. The old in-memory, three-attempt budget —
which retired the queue row with a `delete failed: …` note after about 60 s —
is gone. A failed row stays queued until its disposal is recorded.

**Deleted-scenario coverage:**
- v2 tests: `a_failed_attempt_that_left_no_files_retains_nothing`,
  `absent_integrity_proof_is_unknown_not_cleanup_authority` and
  `failed_park_losing_admission_after_move_is_fenced_and_retried`.
- The fenced-park test uses a test-support admission-loss injection. In
  code, disposal no longer records an outcome when the admission was lost
  after the move.


**Open:** `terminal_junk_with_explicit_keep_retains_owned_sources_and_reports_custody`
failed once in a full `cluster_e2e` run (1 of 22). I could not reproduce it
in 40 further runs:
- 2 more full-suite runs.
- 12 runs as 4 parallel copies.
- 12 runs as 6 parallel copies with 4 CPU burners.
- 4 standalone runs.

No failure message was captured. The free disk on this shared VM dropped to
about 0.7–1.8 GB several times that day (other agents' builds), and several
unrelated tests then failed with ENOSPC. That is the leading suspect, not
confirmed.

**Retry-now on the disposal ladder** (`claude/pipeline-v2-m1a-tests`):
- Test: `retry_now_runs_a_failed_source_follow_up_past_its_disposal_ladder`
  (v2 of the deleted `retry_now_releases_a_due_failed_source…`). A park
  relocate hits EIO before commit and goes onto its own ladder. The
  manager's rescan waits for it; Retry now parks and records it at once.
- Fixed: a park that moved its bytes but did not commit was recorded as
  `no payload`. Disposal now treats an unresolved delete/relocate operation
  as holding the payload (`Inventory::disposal_in_flight`).
- Fixed: Retry now could not complete such a park before `next_retry`.
  Terminal custody verifies the pre-move path, which is gone. A forced
  follow-up now resumes the pending relocation first
  (`Inventory::retry_disposal_now`, the post-retry resume path).
- Open, not fixed: a park whose destination root cannot be created fails
  before its operation is written. It has no durable ladder and is not
  visible on `/api/v1/artifacts`; only the 30 s rescan retries it. The
  relocate request records the destination root's identity, so fixing this
  means reworking that request. That is a relocation change, outside M1a.

**Tests changed to v2 behaviour (contract clause):**
- `terminal_unusable_cluster_completion_…` → `…_parks_raw_copies_and_drains_sealed_generations`:
  §2.2, publish always parks.
- `completed_text_document_is_retained_with_visible_review_action` →
  `…_is_one_publish_failure_with_bytes_retained`: §2.2/§2.3, a
  post-processing failure is terminal and there is no review hold.

## Pipeline contract v2 M1a — terminal first, typed at the source — 2026-11-07

**Status:** built on `claude/pipeline-v2-m1a-runner` (runner 0.3.0), not merged,
not deployed. Plan: `IMPL_RUNNER.md` §5 / `PIPELINE_REWRITE_PLAN.md` M1a.
Curator M2 depends on this landing first.

- **One terminal per job, durable before it is announced.** A new
  `job_terminals` row in the artifacts DB, together with the job's salvage
  obligation in one transaction. Then the history append, the queue-row
  markers, and the single `job_pp_finished` built from that row. Each step is
  flagged on the row and replayed at boot and every 30 s.
  `inject_terminal_crash` (test-support) proves the replay produces one
  history row and one event.
- **Typed at the producer.** The closed `FailureReason` set is health · fetch
  · par · unpack · script · publish · io · identity · hold_expired.
  - `fetch` carries `FetchCause` gone/indexer_unavailable (`fetch::cause_of`).
  - `blames_release` and `salvage_possible` are decided by the reason.
  - Producers:
    - A write failure is `io`, not `health`.
    - yEnc and overlap conflicts are `identity`.
    - A zero-article health failure never reaches post-processing.
    - URL fetch failures are `fetch`.
    - PP failures go through `pp_verdict`.
    - Operator delete is `cancelled` and carries the transfer.
- **Holds are environmental, automatic, and end.**
  - `HoldCause` lost io, identity_conflict and post_failure.
    `HoldPolicy::Review` and the first-cause lock are gone.
  - Every hold gets `deadline_unix`/`deadline_in_s` from `[post]
    hold_deadline_secs` (a day) or `password_hold_deadline_secs` (a week).
    Past the deadline the job fails `hold_expired`, even when the queue is
    paused.
  - At boot, persisted legacy holds convert to the matching terminal. This
    waits until startup verification has run.
- **Salvage and disposal follow the terminal** (`failure_followup`). Salvage
  publishes the verified part as a `kind: salvage` delivery and emits
  `job_publication_ready`. Disposal precedence is keep_on_failure, then
  park-always (publish/io/identity/hold_expired), then `failure_action`. A
  failed queue row retires only once both have settled. keep_on_failure only
  affects retention.
- **R3:** a verified file with an unknown extension is sniffed (magic bytes,
  then ffprobe), so obfuscated Matroska publishes. The payload list gains
  m2ts/mts/vob/mpg/mpeg/iso/mka.
- **Wire:**
  - `job_pp_finished` has the full contract field set. The legacy names stay
    until M6.
  - New events: `job_status_changed` and `job_publication_ready`.
  - `status_projected` ("held") and `transfer` in `/api/v1/jobs`.
  - `/history` returns `incarnation`, `head_seq` and `retained_min_seq`.
  - `/deliveries` rows return `kind`.
  - `accept` is a 200 no-op, and the cluster no longer depends on it.
  - The admission receipt has typed `failure_reason`, `fetch_cause`,
    `blames_release` and `salvage`.
- **Deleted:**
  - `HoldPolicy::Review` and `automatic()`.
  - The io/identity_conflict/post_failure causes.
  - `Owner::retry_hold` (cfg(test)) and the first-cause lock.
  - `post_hold_cause`, `handle_failed_job(_admitted)`, `failed_source_assessment` and
    `FailedSourceAssessment`, `replay_failed_disposition`, and
    `record_failed_cleanup_retry`.
  - The lifecycle `FailureAction::Delete` override.
  - The `DeliveryAcceptance` and cluster `Acceptance` bodies.
  - Non-test Rust totals vs main: +2961 / −1391.
- **Acceptance tests:**
  - `crates/nzbd-post/tests/terminal_first.rs`: 6/6.
  - Engine: `write_failure_is_io_not_health`,
    `zero_article_health_failure_skips_pp_admission`,
    `fetch_404_is_gone_fetch_503_exhausted_is_indexer_unavailable` (unit),
    and `legacy_identity_conflict_hold_converts_at_boot`.
  - API: `accept_answers_200_noop`,
    `operator_delete_emits_cancelled_terminal_with_transfer` and
    `old_curator_fixture_parses_new_event`.
  - State: `rebuild_changes_incarnation`.
- **Not built:**
  - `cluster_terminal_releases_lease_once_disposes` (a multi-node e2e).
  - `partial_torrent_is_io_with_salvage_pending` (needs a torrent fixture).
  - The intents half of `retire_sweep_waits_for_salvage_and_open_intents`.
  - `manifest_digest_golden_vector_unchanged` against
    `.runner-manifest.json`, which is M3. The existing `a0acee1d…` vector
    test still passes.
  - The fetch test is a classifier unit test rather than two URL jobs.
- **Not verified:**
  - `cargo test --workspace` (the fast-lane rule forbids it).
  - Runs with `NZBD_REQUIRE_TOOLS=1`.
  - The cluster and torrent paths beyond `cargo check`/clippy.
  - Deploy and live checks.
  - Mobile still models `review` holds. That is harmless: an unknown policy
    already offers Retry.
- **Decisions for Paul:** park-always for publish/io/identity, the contract
  default, which both reviewers recommend. It also applies to
  `hold_expired`, since an expired hold is environmental.

## Pipeline contract v2 — M0 (contract in the repo) — 2026-11-07

**Status:** in progress on `claude/pipeline-v2-m0-runner` · **Owner:** Claude
(Opus builder session) · **PR:** [#279](https://github.com/pjunod/runner/pull/279) · **Plan:**
`PIPELINE_REWRITE_PLAN.md` M0 (project doc).

- Added [docs/PIPELINE_CONTRACT.md](docs/PIPELINE_CONTRACT.md), the r4
  contract that the M1a/M1b/M3/M6 runner work builds to. When code and
  contract disagree during the rewrite, the contract describes the target.
- "Being replaced by" banner on `docs/INTEGRATION.md`; `CURATOR_DELIVERY.md`
  and `HELD_JOBS_PLAN.md` marked superseded.
- Docs only: no code, no version bump.

## Empty failed sources retained "for recovery" — 2026-11-02

**Status:** fixed on `codex/empty-failed-source`, paired with Curator
`codex/runner-failure-handoff`. Field report (Paul, 2026-11-02, eleven Dexter:
Resurrection jobs #2192–#2202): "curator is just sitting there like it doesn't
know. I would expect it to instantly know the status of the downloads is
failed, and to continue queueing up more attempts" — "interconnection is a
first class citizen between the two apps. time to act like it".

- Every job failed `FAILURE/HEALTH` before a file finished; each
  `/processing/<job>` was empty. `failed_source_assessment` classified an
  empty outcome set as `unknown`, so the source was "retained for recovery:
  no regular source files were available…" as `runner_owned`. Curator holds
  replacement while Runner owns failed bytes, so the season froze behind
  eleven empty directories.
- The retained generation was never pinned into the request receipt (the
  history row got `Artifact:Id` without `Artifact:Generation`), so a later
  delete or Keep in Files could not reach Curator either: the hold was
  permanent even with a person trying to clear it.
- Fix: an empty outcome set is `unusable` (the PP path already said so) and is
  disposed at failure time. The file-lifecycle tick disposes retained failed
  sources whose complete manifest is empty, unless Keep is set, through the
  same generation-bound `failed-delete-<artifact>` authority Keep-off uses.
  Retained failures pin id + generation. Receipts written before the pin adopt
  their job's allocation on the next lookup, so the eleven live rows settle on
  deploy with no click.
- Review (Fable): root cause confirmed. Two follow-ups taken: an owned
  `source_gone` source now settles the receipt like a delete (121 live
  artifacts are `source_gone`), and the sweep reads one directory instead of
  walking a manifest, with a cursor past 200 rows. Reverted: purging every
  `unknown` source — that bucket holds complete no-CRC episodes, which are
  salvageable. Live check: all eleven Dexter artifacts are `retained`, owned,
  cleanup-eligible, no Keep, no hold, zero files.
- CI found `acknowledged_worker_api_import_is_noop_for_late_preterminal_descriptor`
  failing under load (main 4/4 green, branch 1 in 2–3 red). Cause: the sweep ran
  ahead of the ordered tick steps, shifting their timing into a window where
  that test publishes while the engine's first maintenance tick runs. The sweep
  now runs last in the tick (its deletes run on the next tick): 6/6 green.
  The underlying exposure — that test racing the engine's immediate first
  tick — predates this change and is left for a separate fix.

## Held jobs with no way out — 2026-11-01

**Status:** fixed on `fix/held-jobs-always-have-an-exit`; plan and root cause
in [docs/archive/HELD_JOBS_PLAN.md](docs/archive/HELD_JOBS_PLAN.md). Field report: #2181 and
#2183 on nuc3 sat `HELD` with only *delete* after a sub-six-minute
`/processing` outage (ENOTCONN, EIO) — "this keeps happening".

- Root cause, post side: the scheduled retry for I/O holds was written by
  re-importing the row, and `import_job` deliberately keeps a held job's
  local control — so since it shipped (`f53cdcb`, 2026-10-22) it wrote
  `review` back every time. Download side: write errors never got a retry
  at all. Every surface then treated `review` as "no exit".
- `HoldCause` (nzbd-types) is the closed set; `policy()` is the one table,
  and a new cause does not compile without one. Causes encode whether
  retrying helps: a storage errno (`(os error 5|107|116|…)`) is
  `storage_unavailable` and retries on 30/60/120/300 s backoff; a digest or
  size mismatch stays `io` for a person.
- The retry time is stamped by `hold_job` in the same save as the hold. One
  recovery task (off the owner loop, authority only) releases due holds,
  including capacity/quota once space returns — which had no automatic exit
  either. The custody check and capacity probe run in `spawn_blocking`.
- Every hold answers `resume` (409 with the reason when refused). Web row
  and mobile card: *pause* while Runner is retrying, *retry* otherwise.
- Boot and cluster adoption re-arm stored holds, reclassifying `io` holds
  whose own message carries a storage errno — #2181 and #2183 recover on
  deploy with no click.

## Deobfuscated extractions held as unpublishable — 2026-11-01

**Status:** fixed; state and end-to-end PP regressions added. Field report
(2026-10-27, 17 held jobs, 15 of them Good Trouble episodes): "I frequently
have downloads that end up 'held' for the same reason with some message about a
subprocess failed … The message makes no sense to me and it's annoying that
they keep getting hung up." Every one was an obfuscated release
(`<hash>.partNN.rar` holding `<hash>.mkv`) that extracted cleanly; the final
deobfuscation pass then renamed the media and volumes to the release name. The
rename changed the media's ctime, the extraction journal's complete-identity
proof stopped matching, nothing was eligible for publication, and the job was
held with `subprocess failed: no verified media, native book, or paired sidecar
is eligible for publication`. Retries reached the same verdict. The rename
journal already carried the proof for exactly this transition (pre-rename
identity + digest); publication never consulted it for transform outputs.

- `Inventory::verified_custody_evidence` judges the rename, extraction and
  PAR-repair journals in one hashing pass. An output is followed through a
  rename only when the journaled pre-rename identity equals the identity the
  transform recorded and the current bytes match the rename digest; consumed
  archive inputs follow their renames the same way. All four PP/salvage
  callers use it.
- A retry restarts PP from the beginning, after the volumes were renamed (and
  a PAR2 catalog may rename them back). The extraction workspace is keyed by
  archive path, so that either re-extracted the set (two copies published) or
  failed `published extraction output changed`. Unpack now skips an archive
  whose every volume the custody journals prove a successful extraction
  already consumed.
- "Consumed" authorizes deletion, so it now requires the exact identity the
  extraction bound, at the bound path or carried by the rename journal.
- The held message is now `nothing publishable:` (its own `PostError` variant;
  nothing failed to run, always cause `post_failure` so Retry stays available)
  and names each payload file and what it lacks.

## Stuck "Fetching NZB" jobs — 2026-11-01

**Status:** root cause fixed in Curator (monarr v0.38.3); Runner app shows the
fetch retry state (mobile 1.1.3).

Field report: two URL jobs sat in **Fetching NZB** for most of a day showing
`0 B / 0 B · ETA —`. Runner was correct: the indexer answered 429 with a
22-hour `Retry-After` and the job was parked until then (`url_fetch` on the
job carried it; the web UI showed it, the app did not). The quota was spent by
Curator, which had grabbed the same two obfuscated releases thirteen times
each since 2026-10-27. Runner delivered all 26 and Curator accepted all 26,
but every import failed with `cannot determine episodes from
"321d96….mkv"`, and each failure released the episode for another grab.
Curator now imports a sole anonymous file under its grabbed release, never
re-grabs a release whose payload is already retained awaiting import, and
applies its regrab cap to RSS.

Runner changes: the app renders `url_fetch` on fetching jobs (refusal, next
retry, time waited, attempt) instead of empty byte counters, and
`formatDuration` no longer prints "9h 60m".

## Download incident recovery — 2026-10-01

Implementation in progress in an isolated agent clone. See [live progress and merge status](docs/archive/DOWNLOAD_INCIDENT_STATUS.md). One combined PR; adversarial review at merge readiness, followed by required validation.

## Files tab rework — 2026-09-28

**Status:** implemented; state, API and UI harness regressions added; verified
against a local daemon in Chromium (scan → adopt → keep → delete → Undo →
delete). Field report: after **Scan** every folder read `unknown · 0 entries ·
0 B`, **Inspect** appeared to do nothing, and the list was one unpaged wall of
cards. Causes and fixes:

- Discovery recorded a folder without walking it and the UI rendered
  "unmeasured" as zero → a scan now inspects every folder it discovers (a walk
  failure lands on the row as its error, not as a failed scan); `Artifact`
  carries `inspected_at`, the list reports `measured`, and an unmeasured row
  shows `—`.
- Scan/inspect ran only on the 30 s maintenance tick, five at a time → the API
  kicks the task runner on admission, the runner drains the queue (one runner at
  a time), and a delete/prune is kicked again when its Undo window closes.
- No total, no filters, sort by last-updated only, panel rendered below the
  list → `GET /api/v1/artifacts` takes `filter/sort/q/limit/offset` and returns
  `total` + per-filter `counts` (summary `files`/`bytes` columns, backfilled on
  open, so size sort never deserializes a manifest); the Files tab is one
  server-paged table with filter chips, search, sort, pagers above and below,
  and the folder panel as a row under the one you clicked, rendered in place so
  the 5 s refresh never eats a click or a ticked checkbox; every background
  operation is polled to completion and toasted.
- A missing scan root (the failed dir before any failure) no longer fails the
  scan.

Adversarial review (10 findings) addressed: Runner-written payloads listed as
unmeasured because the stripped row could not see its manifest (P1 — list rows
now carry `measured` from SQL, and `finish`/`reconcile_startup` stamp
`inspected_at`); a non-OK list threw on render; a kicked runner drained past
`close()`; a folder whose walk failed was re-walked by every periodic scan;
deletes queued behind scans; the column migration was not atomic; a slow older
list response could overwrite a newer one; `compact()` zeroed the size of
cleared payloads; a transport blip mid-poll reported a task failed.

Tests: nzbd-state artifacts +10 · nzbd-api +1 · UI DOM harness 811 assertions.

Follow-up (same day, field report #2): the folder panel offered *Preview
recovery copy* on a folder already held by an open recovery handoff, which the
server refuses ("stale selection, unowned or held source"). The panel now
mirrors `preview_recovery`'s preconditions (owned · unheld · parked_failed /
retained / completed): it says which one is missing, lists the folder's own
handoffs with their state, and offers **cancel handoff** in place. UI DOM
harness 824 assertions; adopt → stage → held → cancel verified in Chromium.

Same day, field report #3: handoff `70a9015f…` for a 46.8 GiB folder ran 24
minutes and failed with a bare `filesystem: Invalid argument (os error 22)`.
The staging copy went through `std::io::copy` → `copy_file_range` across two
different network mounts (`/working` → `/processing`), which can fail with
EINVAL after partial progress with no fallback; and no step carried an
operation or a path. Staging now copies with a plain read/write loop that
hashes the source in the same pass (three passes over the file became two),
directory fsyncs tolerate filesystems that refuse them (EINVAL/ENOTSUP), every
filesystem step names its operation and path, and each handoff records the
folder it came from (`source`) so Curator can name it. The panel lists the
holding handoff even when it is off the listed page, says "cancel" rather than
"import" for a failed one, and cannot double-cancel. UI DOM harness 829.

## Incremental history ingestion — 2026-09-25

**Status:** M6 implemented and adversarial review addressed; 73 state tests
passed. [PR #238](https://github.com/pjunod/runner/pull/238) tracks final CI and
merge. [Delivery status](docs/HISTORY_INCREMENTAL.md)
tracks the remaining history work through merge. Ordinary shared-log appends
read only complete suffix lines, preserving sorted-file merge semantics.


## History loading performance — 2026-09-25

**Status:** adversarial review findings addressed; local affected checks passed.
Live CI and merge status are tracked in PR [#237](https://github.com/pjunod/runner/pull/237).
[Live delivery status](docs/archive/HISTORY_LOADING_STATUS.md) tracks review, final
verification, and merge. Indexed page reads no longer await replay; an owned
worker handles shared reconciliation and local repair. Local index relocation,
consistent pagination, and Dev enable controls preserve the recovery contracts
in the [reviewed proposal](docs/HISTORY_LOADING_PLAN.md).


## Pending torrent startup readiness — 2026-09-25

**Status:** `nuc3` service restored after three saved magnets delayed API
readiness for about six minutes; PR [#236](https://github.com/pjunod/runner/pull/236)
contains the code fix and [review record](docs/archive/STARTUP_RECOVERY_REVIEW.md).
Not merged or deployed. The code restores local torrents, opens the API, then
retries pending network sources in a cancellable background task. Review
findings added a real-daemon regression, accurate concurrent-removal logs,
continued retries after infrastructure errors, and bounded shutdown. The
daemon test passes and fails when the old startup call is restored. Required
PR checks passed on 2026-09-25; the `nuc3` rollout ordering check remains.

## Torrent client identity and tracker compliance — 2026-09-22

**Status:** implemented and adversarially reviewed; findings fixed. Local
gates on the final tree: full rqbit series proof, `make fast-check`, and one
full `cargo test --workspace --no-fail-fast` (permission-based tests that
root bypasses re-run green as an unprivileged user; one pre-existing
`local_swarm` pause race fixed). Branch `claude/runner-tracker-identity`;
the PR records CI and merge.

Trackers used to see stable rqbit 8.1.1 (`-rQ8110-`, no HTTP User-Agent,
`v = rqbit 8.1.1`). A review of what those announces contained found protocol
defects that cost private-tracker users ratio or credibility:

- a passkey carried in the announce URL's query was replaced by the announce
  parameters, so query-passkey trackers never saw it;
- HTTP trackers never received `completed` or `stopped`; UDP trackers got
  `started` on every announce while downloading and `completed` on every
  announce while seeding;
- `downloaded` was verified progress, so data already on disk was charged
  as downloaded again on every start;
- no `key`, and the response's `tracker id` was never parsed (wrong
  dictionary key) or echoed.

Fix: maintained patch `0021` (twelfth in the series) adds embedder-set
User-Agent and BEP 10 `v` and a shared HTTP/UDP announce lifecycle; nzbd
reports Runner everywhere (`nzbd_torrent::identity`, BEP 20 prefix `-RN`).
Adversarial review found four defects in the first cut, all fixed: a
rejected `completed` was retried every 5 s; magnet lookups produced spurious
seeder-looking `started`/`stopped` pairs; UDP `stopped` died with the session
on shutdown; and per-tracker 5 s polls scanned every torrent. Now: backoff
60 s → 30 min, lookups send no events, `Session::stop` waits (≤3 s) for
`stopped`, a restart's `started` waits for the previous `stopped`, one cheap
poll per torrent, and logged tracker URLs are reduced to scheme/host/port.
Proofs: 11 new tracker-comms tests (lifecycle, backoff, passkey query, key,
tracker id, early `completed`, `stopped` on drop, restart ordering, UDP
parity, no-event lookups, log redaction), 4 new librqbit tests (User-Agent,
`v`, session-relative `downloaded`), 4 identity unit tests, and one
end-to-end private-swarm test asserting everything a tracker and a remote
peer observe, including the shutdown `stopped`.
Docs: CONFIGURATION `[torrent]` client identity; ADR-19 amendment item 12.

## Torrent queue lifecycle — 2026-09-20

**Status:** adversarial review approved after fixes. UI boot, 599 DOM
assertions, formatting, and diff checks pass. Required CI and merge state are
tracked in [PR #232](https://github.com/pjunod/nzbd/pull/232).
See [the live work status](docs/archive/TORRENT_QUEUE_STATUS.md) for scope, decisions,
review findings, validation, and merge progress. Existing queue layout retained;
seeding and completion gain distinct sections, metrics, and durable controls.

## Settings form schema repair — 2026-09-20

**Status:** adversarial review approved; final local fast lane passed.
**PR:** [#226](https://github.com/pjunod/nzbd/pull/226) records required CI and merge.

Deploying PR #224 to nuc3 removed the missing BitTorrent defaults error, but
live browser verification exposed another blocker: the form submitted the old
`post.health_action` alias alongside `post.failure_action`. The server rejected
the entire save as a duplicate field.

- The form now uses `failure_action`, displays the saved failure policy, and
  describes its current behavior. Existing configuration aliases remain valid.
- The DOM regression now collects all rendered settings from an actual Config
  fixture. Rust deserializes that submitted payload and verifies that enabling
  BitTorrent preserves every other setting, including the supported `Pause` failure-policy spelling.
- Follow-up work uses an independent clone on `codex/fix-settings-field-contract`.
- Adversarial review caught case-insensitive and legacy failure-policy values
  being reset by the select. Selects now preserve unlisted configured values
  unless explicitly changed; the round-trip fixture covers this edge case.
- Final fast lane passed: formatting, UI boot, UI DOM (529 assertions), and
  the Rust full-form round-trip regression. Required PR checks run in GitHub.
- Deployment completion is verified through an actual browser save followed
  by restart; operational progress is recorded in the task status page.

## Settings save and CI repair — 2026-09-20

**Status:** implementation complete; adversarial review approved and local
checks green. The PR below records the required CI result and merge state.
**PR:** [#224](https://github.com/pjunod/nzbd/pull/224).

- Fixed the API response so default BitTorrent values are present when the
  settings form first enables the feature; the compact TOML format is retained.
- Save confirmations remain visible after reload; form collection failures
  produce an error instead of an unhandled rejection.
- Regression coverage checks persistence, restart advice, save feedback, and
  retryable validation failures.
- Work is isolated in an agent-owned clone on `codex/settings-save-repair`.
- Final fast lane passed: `cargo fmt --all --check`, UI boot, UI DOM
  (525 assertions), and `settings_can_enable_default_torrent_config`
  (one targeted API test). No further full-suite runs were started locally.
- Adversarial review found a lost-success-message edge case when the reload
  request fails after saving. Both editors now retain confirmation and restart
  advice; network and HTTP failures have regression coverage.
- No new release gate or automatic restart is introduced. The Dev-tab
  preference is recorded for feature work; this PR repairs existing behavior.
- Main requires the full `unit + e2e` check. Branch protection stays intact
  without a bypass. The initial PR run failed the same three daemon tests as
  the preceding main run:
  `delete_parks_the_job_and_requeue_brings_it_back`,
  `permission_denied_state_dir_suggests_the_fix`, and
  `restart_completes_while_post_processing_runs`.
  Evidence: [PR job](https://github.com/pjunod/nzbd/actions/runs/35479476603/job/105994545460)
  and [main run](https://github.com/pjunod/nzbd/actions/runs/35476537458).
  Repairs now included in this PR: raw NZB test uploads use
  `application/x-nzb`; history-directory errors retain the path and permission
  repair hint. The failing assertions remain in place.
- Replaced manual ETA division with `checked_div` to satisfy the current
  Clippy lint while retaining the zero-rate fallback.
- The repair batch received adversarial approval with no findings. Final
  local checks passed: 20 daemon integration tests, 22 binary unit tests, the
  settings regression, UI harnesses (525 DOM assertions), formatting, and
  workspace Clippy. GitHub runs the required full workspace suite before merge.
- The pre-existing RustSec check separately reports `RUSTSEC-2026-0285` in
  rustls 0.23.42. Dependency remediation is outside this test-repair batch; no
  advisory exception or suppression was added.

The explicit ledger of what this project intends to do and whether it is
done. **Update this file in every feature commit.** Derived from the
roadmaps in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) §16 and
[`docs/CLUSTERING.md`](docs/CLUSTERING.md) §13.

Legend: ✅ done (implemented, tested, committed) · 🔶 partial · ⬜ not
started · 👤 operator action (Paul)

**Snapshot (2026-07-29):** 333 tests · clippy clean · MSRV 1.85 · **phases
0–4 complete incl. RSS feeds, cluster C1+C2 complete, phase 5 partial,
integration phase 1 (N1–N7) complete** — every NZBGet user-facing surface
exists; what remains is beyond-parity performance work and operator
actions

| Phase | State | Evidence |
|---|---|---|
| 0 — Scaffold | ✅ complete | `10eed82` |
| 1 — Core engine | ✅ complete | `2f45cd5` |
| C1 — Cluster foundation | ✅ complete | `e4178b2` (design), `0969a79` (impl) |
| CI & quality gates | ✅ complete (2 decisions open) | `b0b5530`, `0de429b` |
| 2 — Post-processing | ✅ complete | `1fdad15` + this commit |
| C2 — PP leases + anti-affinity | ✅ complete | `9f402d8` |
| 3a — *arr compat core | ✅ complete | `3793ad8` |
| 3b — importer · auth · SSE · metrics | ✅ complete | `e00990c`, `b4c422d` |
| 3c — compat C2 + XML-RPC + golden tests | ✅ complete | `fe6d2be` |
| 4 — Web UI + ecosystem | ✅ complete | `77b7660` |
| 5 — Beyond parity (+ C3) | 🔶 adaptive pipelining done; rest scoped | this commit |
| BT — BitTorrent backend | 🔶 ADR-19 selects the reproducibly derived nine-patch rqbit v8.1.1 engine; all eleven M0 gates and independent review are complete; M2a dormant config and M2b selective runtime ownership merged in PRs #161/#181, remaining M2 slices are tracked by #154–#160, and #163 retains sole activation ownership; production daemon session/listener/admission wiring remains absent and M5/M6 require their own gates | [proposal](docs/BITTORRENT_PROPOSAL.md), [M0 report](docs/BITTORRENT_M0_REPORT.md), [release review](docs/BITTORRENT_RELEASE_REVIEW.md), [rqbit maintained engine](contrib/rqbit/README.md) |

---

## Phase 0 — Scaffold ✅

- ✅ Cargo workspace (13 crates), edition 2021, MSRV 1.85
- ✅ Domain model + NZBGet's exact health formulas (`nzbd-types`)
- ✅ Streaming NZB parser: entities, DOCTYPE, unordered/dup segments (`nzbd-nzb`)
- ✅ Incremental chunk-boundary-safe yEnc decoder + CRC32 + `crc32_combine` (`nzbd-yenc`)
- ✅ NNTP codec: responses, command injection guards, multiline reader (`nzbd-nntp`)
- ✅ Server failover ladder as a pure, scenario-tested function (`nzbd-engine`)

## Phase 1 — Core engine ✅

- ✅ Single-owner queue task: mpsc commands, arc-swap snapshots, broadcast events
- ✅ Scheduler wired to the ladder: tiers, groups, fill servers, per-server retention pre-fail, per-server retry reset, force priority, PropagationDelay
- ✅ Per-server connection pools, connect-on-demand, 5 s idle retirement
- ✅ NNTP pipelining (per-server depth), terminator-aware bounded yEnc consumption
- ✅ rustls transport: TLS with Strict/Minimal/None cert levels, AUTHINFO
- ✅ DirectWrite writers: sparse preallocate, positional writes, gap zero-fill, atomic rename, combined whole-file CRCs
- ✅ Delayed-par pausing (`*.volNNN+MM.par2` queued paused)
- ✅ Health-gated completion (Completed vs Failed below critical health)
- ✅ Token-bucket rate limiter (debt model) + 30×1 s speed meter
- ✅ Crash safety: append-only journal + atomic snapshots + unclean marker; kill -9 resume proven in e2e (no re-fetch of journaled segments)
- ✅ Diagnosable state I/O (2026-07-24): every filesystem call in `nzbd-state` goes through `fsx` wrappers that attach the **operation and path** to the error (`StateError::Io { op, path, source }`), so a startup failure reads `state: write /data/queue/unclean.local: Permission denied (os error 13)` instead of a bare errno. Startup logs the resolved `state_dir`/`dest_dir` before touching them, a permission failure appends an actionable hint (walks the source chain, so it survives the cluster wrapper), and `anyhow_lite::Error` prints via `Display` so multi-line hints aren't Debug-escaped. Guard test fails the build if a raw `std::fs` call is added back to the crate
- ✅ `nzbd-nserv` mock NNTP server: generated posts, 430/CRC/disconnect/latency injection, hit + concurrency gauges
- ✅ Native API subset: status, jobs add/list/detail, job + queue actions, speed limit
- ✅ Compat shim: `version`, `status`, `listgroups` in NZBGet's JSON-RPC 1.1 dialect with Lo/Hi/MB triplets
- ✅ CLI `run` / `add` / `status`; whole-daemon test (real binary, real CLI, SIGINT)
- ✅ URL jobs: `AddUrl` via API/append — registered instantly (`Fetching`), NZB fetched over HTTPS (hyper on the NNTP rustls stack, redirects, 64 MiB cap), then queued; fetch failure → `FAILURE/FETCH` in history
- ✅ Multi-root min-free-disk-space guard: one configured inventory covers local state, shared cluster state when enabled, downloads, working, failed, intermediate, temporary, watch, and category roots. Each admitted filesystem call gets an independent two-second response deadline; a degraded process-wide permit pool can serialize a cycle without blocking startup or hiding responsive volumes permanently behind a wedged mount. Paths are grouped by containing filesystem where the platform exposes stable identity; Windows conservatively keeps distinct path rows. The lowest reading stops local leasing (even force jobs). Held cluster workers publish that guard state, request zero free download/PP slots, and are excluded by leaders from new placement. Native status names the local limiting roles/path/free bytes and distinguishes an active write-failure latch; cluster diagnostics publish the same facts per node. An incomplete cycle retains last-known data and cannot clear an existing hold
- ✅ Quotas + per-server volume counters: daily/monthly windows (`QuotaStartDay` civil-date periods), per-node `volumes.<node>.json` summed cluster-wide, force-priority bypasses quota, `QuotaReached` live in compat status, counters in snapshots
- ✅ Filename deobfuscation: par-rename (16k-MD5 match incl. content-detected par2s) + rar-rename (RAR4/RAR5/7z/zip signatures, RAR5 volume numbers) — see Phase 2
- ⬜ COMPRESS DEFLATE (RFC 8054) — deferred: single-digit % savings on yEnc bodies; scoped for a later pass
- ✖ RAM article cache — intentionally not applicable: `ArticleCache` exists in NZBGet to reduce fragmentation when DirectWrite is off; nzbd's DirectWrite positional writer is always on, so there is nothing for a cache to fix
- 👤 Real-provider smoke test (point `nzbd run` at an actual news server) — never yet done

## Cluster — C1 foundation ✅ (design ADR-13…16 accepted)

**2026-09-19 reassessment:** C1/C2 are implemented, but their original
acceptance scope is narrower than plurx's current transactional authority and
recovery contracts. The remaining work includes ownership/publication
hardening as well as C3. See the
[source comparison and finite plan](docs/CLUSTERING_COMPLETION_PLAN.md) and
[live progress page](docs/CLUSTERING_STATUS.md). This is documentation and
planning only; it does not claim the proposed control store, deadlines, or
segment execution are implemented. Physical Gluster qualification remains
unrun in this pass.

- ✅ Leader election on the shared volume: monotonic staleness observation, write–wait–verify, priority stagger (observing), epoch fencing via verify-before-commit snapshot guard
- ✅ Node registry (presence, capabilities, load; seq-progression liveness)
- ✅ Per-job fenced journals with union replay (`jobs/<id>/journal.<node>`) — overlap-safe reclaim without locks
- ✅ Work-lease protocol: poll/heartbeat/complete, TTL reclaim, **adoption** of running leases across leader failover
- ✅ Whole-job download distribution; engine worker mode (import/export, delegation, mirror overlay, crash-only demotion)
- ✅ Cluster-wide provider connection-budget partitioning (non-download nodes pinned to zero)
- ✅ Any-node API: full API + shim everywhere, transparent proxy to the leader
- ✅ `[cluster]` config + validation; single-node mode untouched
- ✅ 5 multi-node e2e tests: single-leader invariant, distributed download via proxied add with budget held, worker-death reclaim (zero re-fetch), leader-death failover with lease adoption, restart persistence
- ✅ C2: PP work leases — `LeaseKind::Post` in the poll/heartbeat/complete protocol; leader **anti-affinity scheduler** (idle PP nodes first, downloading nodes last, capacity-aware incl. in-flight backlog); `.pp.<lease>/` extraction staging with a local lease check before publication; staging cleanup; lease adoption across leader failover for PP too; dead-node delegation reconcile; provider budgets include PP recovery fetches; per-node `history.<node>.jsonl` on the shared volume (cross-client O_APPEND is not trusted), union rebuild into each local SQLite index. Whole-pipeline isolation, transactional publication, and expired-worker cancellation remain completion-plan work
- ✅ C2 e2e: leader downloads a real par2-set job, the idle non-download node quick-verifies it, stamps it, appends shared history, hands it back — bit-identical payload, zero staging residue
- ⬜ Cluster completion P0–P5: fast-lane delivery, transactional control authority, worker/PP lifetime, weighted scheduling and acknowledged budget transfer, segment-split downloads, advisory settings and passive diagnostics — [implementation order](docs/CLUSTERING_COMPLETION_PLAN.md#4-finite-delivery--six-milestones-one-implementation-pr)
- 👤 Real-Gluster soak checklist (CLUSTERING.md §11): quorum on, node reboots, volume heal mid-download

## CI & quality gates ✅

- ✅ Workflows: **Tests** (full suite + MSRV 1.85), **Lint** (fmt + clippy -D warnings), **Coverage** (cargo-llvm-cov → self-hosted badges → `badges` branch + lcov/HTML artifact)
- ✅ Git hooks (`.githooks/`): pre-commit fmt, pre-push clippy + tests — `git config core.hooksPath .githooks`
- ✅ rustfmt enforced workspace-wide; clippy zero warnings; MSRV verified
- ✅ External-tool tests (par2/7z fixtures) self-skip with a notice on machines without the binaries, so the pre-push hook passes on a stock Mac; CI installs the tools and sets `NZBD_REQUIRE_TOOLS=1` so a skip there is a hard failure — `brew install par2 p7zip` for full local coverage
- ✅ First Coverage run on GitHub succeeded (it published the `badges` branch)
- ✅ 87 tests / 87.3% line coverage (local measurement matching CI methodology)
- 👤 Branch protection on `main` requiring Tests/Lint/Coverage (repo Settings)
- ✅ Badge rendering decision (2026-07-18): **repo goes public** — the badges-branch + raw-URL setup then works as-is, no code changes. Flip: repo Settings → General → Danger Zone → Change visibility

## Docs ✅

- 🔶 BitTorrent M2 decomposition (2026-08-22): M2a issue #152 is complete via merged PR #161; its dormant `[torrent]` configuration is present, but validation rejects `enabled = true` with the named backend-not-activated guard. M2b issue #153 is complete via merged PR #181; it establishes dormant selective-session and queue-adjacent runtime ownership without wiring the production daemon. M2c issue #154 now supplies the unmounted native admission router, idempotent watch scanner, protected pending-source recovery, descriptor-before-managed-add boundary, runtime JobId association, and unconditional queue schema 4. Remaining M2 slices are #156–#160, and final activation #163 retains sole activation ownership. Dormant config/runtime children cannot expose a silent half-wired backend; #163 alone may mount the route, spawn the watch task, or remove the guard after complete daemon, recovery, rollback, mixed-protocol, storage-fault, and independent-review evidence. The exact schema-3 binary rejects schema 4 rather than silently discarding pending intents. No production daemon `TorrentSession`, listener, or torrent admission path is wired.

- 🔶 BitTorrent maintained-engine M0 amendment (accepted 2026-08-14; status reconciled 2026-08-22): ADR-19 now selects rqbit v8.1.1 from its immutable checked archive plus exactly nine ordered stable patches (2,819 patch lines). The workspace resolves the dormant adapter through the byte-for-byte derived vendor. CI checks checksum, series membership/order, clean application, vendor drift, named revert-sensitive proofs, and the affected upstream suites. The adapter pins 80/400 live peers, 1,024/4,096 retained peers, the 10 MiB pre-allocation metadata ceiling, and the rest of the accepted tracker/handshake/response/discovery/storage boundaries. A real process-kill proof starts an empty session and selectively restores only nzbd's chosen record without promoting valid bytes written after its 16 KiB checkpoint. Required stats remain explicit; unavailable tracker/DHT detail is `unknown`, never inferred from peers or rendered as a health percentage. This entry supersedes older status text that required upstream stable acceptance for gates 7–9. All eleven gates pass locally and in the [five-platform maintained-engine run](https://github.com/pjunod/nzbd/actions/runs/31837867629), and [independent review](https://github.com/pjunod/nzbd/pull/101#issuecomment-5298914349) is complete. M2a and M2b have since merged; remaining M2 slices are tracked by #154–#160 with activation owned solely by #163, and production daemon wiring stays disabled
- 🔶 BitTorrent sampled memory guards (2026-08-14 UTC): optimized dormant-adapter probes repeat the DHT-disabled, trackerless, peerless 100-torrent mix and construct and validate the accepted 100,000-file metainfo inventory on every native M0 runner. The [maintained-engine five-platform run](https://github.com/pjunod/nzbd/actions/runs/31837867629) passed after deriving the exact nine-patch engine and proving exact-ceiling acceptance plus first-byte-excess rejection through the measurement guard. Sampled session growth was at most 5,144,576 bytes under the preliminary 32 MiB ceiling; sampled preflight growth was at most 27,295,744 bytes under the preliminary 64 MiB ceiling. This is retained-memory regression evidence, not true allocation peaks, active-swarm behavior, concurrent hostile-submission, or exhaustion-resistance proof; production wiring remains disabled
- 🔶 BitTorrent storage-full probes: the write-time local swarm injects `StorageFull` from `pwrite_all` after one 16 KiB write call returns successfully. A separate 64 MiB filesystem-backed torrent must already be live with nonzero incomplete progress, remain live at the fault boundary, complete in the same rqbit session, and leave the fault state and exact write accounting unchanged; an 8 MiB/s seed cap makes that boundary deterministic, rqbit binds directly from a test-only port range, and fault stats use an independently enforced one-second response deadline. The [2026-08-10 hardened five-platform run](https://github.com/pjunod/nzbd/actions/runs/31345445318) passed that behavioral contract. A second ignored proof injects `StorageFull` from initialization-time `ensure_file_length`; the maintained engine must expose a bounded file/length/cause error during admission or initialization, any returned handle must enter `Error`, and the 256 KiB file remains zero bytes after one sizing request and zero writes. Reverting maintained patch `0018` restores the historical `Paused`-then-`Live` fail-open result and makes the proof fail. The [2026-08-13 replacement run](https://github.com/pjunod/nzbd/actions/runs/31654021866) records that superseded behavior; the [2026-08-14 maintained-engine run](https://github.com/pjunod/nzbd/actions/runs/31837867629) proves the fail-closed result on all five native targets. The daemon now has a protocol-neutral multi-root enforcing guard and limiting-root API fact, but the dormant torrent adapter remains unwired to report its `StorageFull` signal, pause new piece requests, preserve existing uploads, or prove recovery; the sizing result therefore remains a release blocker
- 🔶 rqbit file-sizing error contribution: exact stable-8.1.1 and current-main patch candidates stop initialization at the first selected-file `ensure_file_length` failure, preserve padding/selection behavior, and retain file name, requested length, and storage cause in the error chain. Stable patch `0018` is now part of nzbd's selected maintained v8.1.1 series. The main candidate also gives checksum cancellation a typed marker, so a concurrent pause suppresses that marker only and cannot hide an overlapping storage error. A mutation inside the helper makes the exact test fail; fail-closed verifier assertions reject a discarded helper result or bypassed pause-error classifier at the actual state boundaries. The verifier rejects missing or zero-test proofs and compiles the Rust-TLS library on exact stable and documented-main blocking legs, with moving-main drift advisory on pull requests. Nothing has been submitted upstream, and this does not authorize production wiring
- ✅ BitTorrent M5 pressure and release-review groundwork (2026-08-09): one DHT-disabled session retains 100 real engine torrents in an exact 10-active/90-paused mix with admission and shutdown deadlines; `make gate` deterministically combines formatting, strict lint, whole-workspace tests, Rust 1.85, daemon isolation, dependency review, and fuzz seed contracts; and the checked operations review makes public traffic, ports, paths, seeding, deletion, evidence, and stop conditions independently reviewable. At that milestone, the drift guard pinned gates 7 and 8 as failed and gate 9 as Partial; the maintained-engine amendment above supersedes those gate results while production wiring remains disabled
- ✅ BitTorrent error secret redaction (2026-08-07): the adapter's 2 KiB display-safe error boundary now redacts secret values written as `key=value`, `key:value`, `key: value`, `key = value`, or JSON-like assignments. Regression coverage includes proxy passwords, auth tokens, cookies, URLs, peers, paths, and control characters; production wiring remains disabled
- ✅ BitTorrent multi-token secret redaction (2026-08-07): the adapter now keeps redaction active across authorization schemes, cookie/digest continuations, and `=>`, `:=`, or `->` assignment separators, preventing header credentials or arrow-delimited secrets from reappearing in the remaining error text. Non-secret context remains visible and the 2 KiB bound is unchanged; production wiring remains disabled
- ✅ BitTorrent review remediation (2026-08-07): the consolidated adapter branch redacts secrets after any earlier key in a token, before URL rewriting, and recognizes private-tracker/API/signature/session credential names; proxy `Debug` never prints its raw URL. Known-private metainfo now fails before admission when session DHT is live. Portable collision keys use Unicode simple case folding plus NFC and the Windows dotless-i alias, lowercase base32 magnets are normalized for rqbit, the mutation corpus pins exact dispositions, and encoded-byte path limits have multibyte boundaries. Windows-invalid names remain an intentional all-platform rejection because silent renaming would break seeding and cross-platform restore; production wiring remains disabled
- ✅ BitTorrent explicit session options (2026-08-07): the dormant adapter now assigns every stable rqbit 8.1.1 session option by name, keeping library fast-resume/session persistence, global trackers, remote blocklists, deferred writes, and UPnP absent while making any future upstream option a compile-time review event. Production wiring remains disabled
- ✅ BitTorrent explicit per-add options (2026-08-07): metadata resolution and managed admission now assign every stable rqbit 8.1.1 add option by name, preventing file selection, alternate output roots, injected trackers, custom storage, or deferred writes from appearing through a changed upstream default. Production wiring remains disabled
- 🔶 BitTorrent runtime peer-cap audit (2026-08-07): the proposal and gate-9 brief now distinguish the 80-entry explicit bootstrap guard from stable rqbit 8.1.1's hard-coded 128 live peers per torrent. Stable has no session-total cap; current rqbit main exposes a per-torrent limit but still no shared 400-peer budget. Production integration requires an accepted stable per-torrent API plus an nzbd-owned shared-session cap, or an explicit re-review of different measured limits
- ✅ BitTorrent serial initialization budget (2026-08-07): the dormant session now permits one disk-heavy torrent initialization/integrity scan at a time instead of inheriting stable rqbit 8.1.1's default of three. This bounds initialization I/O only; it does not claim to implement the future cross-protocol active-download scheduler or missing runtime peer caps, and production wiring remains disabled
- ✅ BitTorrent explicit peer timeouts (2026-08-07): the dormant session now pins stable rqbit 8.1.1's effective 10-second connect and read/write timeouts plus 120-second keepalive interval instead of passing an unset peer policy. Per-add options inherit only that reviewed session policy, future engine default changes cannot silently lengthen connection lifetime, and production wiring remains disabled
- ✅ BitTorrent bytes-only managed admission (2026-08-07): the private managed-add helper now accepts only preflighted metainfo bytes, making it impossible for a future in-crate caller to pass rqbit's unbounded HTTP URL variant around nzbd's size, redirect, timeout, and redaction contract. Authenticated source fetching remains future nzbd-owned API work and production wiring remains disabled
- ✅ BitTorrent tracker fan-out limits (2026-08-07): `.torrent` files and magnets now admit at most 64 unique non-empty trackers and 2 KiB per decoded tracker URL before rqbit can create tracker work. Exact-limit fixtures pass, the first excess entry or byte fails with a named error, private torrents remain stricter at exactly one tracker, and production wiring remains disabled
- ✅ rqbit tracker request-budget contribution kit (2026-08-07 historical; superseded 2026-08-14): exact-v8.1.1 and current-main patches now stream HTTP tracker bodies behind a 1 MiB cap and 30-second complete-request deadline, clamp hostile unforced HTTP/UDP announce intervals to at least 60 seconds, and prove declared/chunked overflow plus timeout and interval boundaries. A drift-aware verifier and stable/main Actions matrix package the candidate for human upstream review; nothing was submitted; at that milestone gate 9 remained Partial, and production wiring remains disabled
- ✅ rqbit session peer-budget contribution kit (2026-08-07 historical; superseded 2026-08-14): exact-v8.1.1 and current-main patches now expose caller-selected per-torrent and shared-session live-peer ceilings. Incoming and outgoing peer managers hold both applicable permits through release; exact per-torrent, two-torrent aggregate, and awaited release/resume tests pass on Rust 1.85 and current main. A drift-aware verifier, stable/main Actions matrix, and human-review draft package the independent candidate; nothing was submitted; at that milestone gate 9 remained Partial, and production wiring remains disabled
- ✅ rqbit pending-handshake budget evidence (2026-08-07 historical; superseded 2026-08-14): exact v8.1.1 now has a tested stable-only backport that pauses TCP accepts at 256 incomplete pre-routing handshake checks, closing the unbounded socket/task set that live-peer permits cannot cover. The verifier checks the exact 255/256 boundary on Rust 1.85 and current main's native configurable equivalent without applying a redundant patch. The value still needs human acceptance and an accepted stable release; nothing was submitted; at that milestone gate 9 remained Partial, and no production listener exists
- ✅ rqbit retained-peer budget contribution kit (2026-08-07 historical; superseded 2026-08-14): exact-v8.1.1 and current-main patches now expose caller-selected per-torrent and shared-session limits for queued, backoff, dead, and not-needed peer records that sit outside live-manager permits. RAII holds both record permits through removal, exact local/shared/refusal/release tests pass, incoming-only records no longer persist after disconnect, and alternate-address reconnects carry the record handle and cannot be queued repeatedly. The preliminary 1,024/4,096 policy still needs full memory review and an accepted stable release; nothing was submitted; at that milestone gate 9 remained Partial, and production wiring remains disabled
- ✅ rqbit peer-response budget contribution kit (2026-08-07 historical; superseded 2026-08-14): exact-v8.1.1 and current-main patches give each established peer one 128-permit window for remote-triggered payload and BEP 9 metadata responses. Piece permits span the upload scheduler and writer; metadata permits span the writer, and both remain held through the socket write. The exact proof shows a scheduler-to-writer move cannot reopen the window, a deliberate guard bypass makes it fail, and the stable/main Actions matrix keeps both source lines visible. The value and backpressure policy need human review and an accepted stable release; nothing was submitted; at that milestone gate 9 remained Partial, and production wiring remains disabled
- ✅ rqbit discovery-pressure contribution kit (2026-08-07 historical; superseded 2026-08-14): exact-v8.1.1 and current-main patches bound outgoing DHT datagrams, recursive nodes, delivered peers, and maintenance queues at 256 records; cap recursive and maintenance work at 32 active requests per worker; and limit concurrent bootstraps to eight. The metadata resolver's semaphore-plus-unbounded-futures shape becomes 128 active attempts and 4,096 retained candidates. The current-main candidate also bounds LSD results at 256, cancels periodic announces with their stream, and prevents stale teardown from deleting a replacement registration; both source lines now reuse one response instead of issuing every recursive DHT request twice. Exact affected suites and full upstream workspace builds pass, deliberate DHT/LSD guard regressions fail, and a drift-aware stable/main verifier packages the evidence. Every value and overload policy still needs human review and an accepted stable release; nothing was submitted; at that milestone gate 9 remained Partial, and production discovery remains disabled
- ✅ rqbit metadata allocation contribution kit (2026-08-07 historical; superseded 2026-08-14): exact-v8.1.1 and current-main patches expose a caller-selected BEP 9 metadata ceiling checked before peer-buffer allocation or metadata requests. The default remains 32 MiB; nzbd can request its reviewed 10 MiB ceiling only after upstream acceptance and a stable release. The verifier now proves its exact test exists before running it; nothing was submitted; at that milestone gate 9 remained Partial, and production magnet wiring remains disabled
- ✅ BitTorrent proxy-origin preflight (2026-08-07): the dormant adapter now accepts one SOCKS endpoint syntax, normalizes an optional root slash, and rejects paths, queries, and fragments before stable rqbit's peer and HTTP-tracker clients can interpret the same security-sensitive setting differently. Credentials remain separate and redacted; production wiring remains disabled
- ✅ BitTorrent listen-port preflight (2026-08-07): the dormant adapter now admits exactly one explicit non-zero TCP peer port and rejects empty or multi-port ranges before rqbit can silently request an ephemeral port or probe multiple host ports. Production wiring remains disabled
- ✅ BitTorrent endpoint preflight (2026-08-07): the dormant adapter now preserves caller order while deduplicating and bounding explicit bootstrap peers to 80, rejects port-zero/non-unicast/IPv6 peers before the v1 IPv4 engine path, and rejects tracker port zero by name. Production wiring remains disabled
- ✅ BitTorrent magnet selection preflight (2026-08-07): the dormant adapter now mirrors stable rqbit's exact `xt` spelling, validates complete v1/v2 topic forms, and rejects unknown topics or engine-incompatible casing with nzbd-owned errors. It rejects every magnet `so=` parameter before rqbit can eagerly expand an attacker-selected numeric range into an unbounded vector; future selective-file admission must use an explicit bounded nzbd input. Production wiring remains disabled
- 🔶 BitTorrent ADR-19 (2026-08-05 historical; superseded 2026-08-14): both reviews are incorporated in `docs/BITTORRENT_PROPOSAL.md`; stable `librqbit` 8.1.1 remains isolated in the M0 adapter. Deterministic local `.torrent`/magnet, seeding, mid-download control, fail-closed SOCKS, private-tracker, provider, path, delete, and metainfo/magnet format-boundary tests pass. A socket-free mutation corpus now exercises every truncation plus bounded byte replacement, deletion, and insertion around v1, v2-only, and hybrid metainfo. Checked v1 geometry prevents zero-divide, aggregate-length wrap, malformed hash width, mismatched hash count, and rqbit absolute chunk-index wrap before admission. Magnet preflight reads decoded exact-topic parameters, accepts one valid hex/base32 v1 hash, and ignores version-looking text outside `xt`; resolved metadata now returns through rqbit list-only mode and is revalidated before storage construction, with a fake BEP 9 peer proving an unsafe path leaves the destination empty. Privacy-unknown magnets fail closed when session DHT is enabled because stable rqbit cannot suppress DHT per unresolved add; a loopback test proves rejection precedes explicit-peer contact. Tracker URLs fail closed for malformed/non-UTF-8 input, unsupported schemes, missing hosts, and portless UDP instead of being silently dropped by rqbit. The adapter itself requires an explicit payload name instead of accepting rqbit's shared `torrent-content` fallback, and rejects non-portable single/multi-file components, Windows device/character/trailing-alias names, metainfo-declared symlinks, exact duplicate paths, file-versus-directory-prefix overlaps, and portable Unicode-NFC/lowercase collisions before rqbit admission; a real-admission corpus still proves traversal and both platform separators fail before filesystem escape. It canonicalizes the output root, rejects existing payload symlinks, and requires existing prefixes/leaves to be directories/regular files before storage; descriptor-relative containment is still required to close the check/write race. Its importer-safe content inventory omits BEP 47 padding entries without shifting later file progress. The proposal's metainfo, magnet, file-count, component, per-path, aggregate-path, and 2 KiB display-safe error constants are centralized with boundary tests; rqbit operation/stat errors are redacted before leaving the adapter. Empirical filesystem-specific case/normalization behavior remains M5 work; the independent rqbit contribution kit now proves a caller-selected magnet metadata limit can be enforced before allocation. Proxy+DHT and proxy+UDP trackers fail by name, DHT persistence cannot escape into rqbit's global cache, and a recording relay proves peer TCP did not also connect directly. Reproducible patches against v8.1.1 and the pinned rqbit-main snapshot now prove authoritative restore and credential-safe per-torrent tracker/DHT health; at that time, gates 7 and 8 remained failed pending stable APIs. The maintained-engine amendment above supersedes that result. Disk F1–F3 and durable history deletion were complete, so M2 was ready to decompose under the accepted maintained-engine result. No production switch or listener existed at that historical milestone
- ✅ BitTorrent metadata fuzz layer (2026-08-09): a separate feature-gated libFuzzer target exercises the complete metadata-only file-admission wrapper across proxy and DHT modes. Nine contract-checked version, tracker, privacy, path, and multifile seeds stay committed under `fuzz/seeds/`; the evolving generated corpus remains ignored. This bounded layer starts no session and performs no network or filesystem I/O, so it does not satisfy M5 or enable production wiring
- ✅ rqbit contribution kit (2026-08-07): the issue/PR drafts, patch map, human-review boundary, submission order, and reproduction commands live under `contrib/rqbit/`; Fable reviews verified the pinned bases and patch application, caught a public `TrackerHealth::Debug` credential leak, and made the authoritative-restore progress proof discriminate persisted state from a full disk recheck. Stable patch checks are visibly prefixed `blocking:`; current-main checks are prefixed `drift:` and run as non-blocking compatibility evidence instead of reporting success without executing. Nothing has been posted upstream, and the full M0 path—not a test rerun alone—still controls M2
- 🔶 BitTorrent gate 9 review boundary (2026-08-07 historical; accepted 2026-08-14; dependency exception refreshed 2026-09-19): the repository-wide `scripts/check-reviewed-dependency-exceptions.sh` blocks changes to the two remaining exact RustSec ignores, the quick-xml/UPnP package set, and the sole MPL-2.0 package path while normalizing nzbd's own release version. The compiler-floor upgrade selected fixed `time 0.3.51`, so its obsolete exception and feature allowance were removed. The adapter does not expose an UPnP input and tests that rqbit options keep it false. `docs/BITTORRENT_GATE9_REVIEW.md` isolates the one-sample resource/dependency/license/advisory decision for Fable; gate 9 was then Partial pending explicit review. The maintained-engine amendment and independent approval above supersede that state; all eleven gates now pass
- ✅ Queue/backend M1b (2026-08-05): schema 3 adds the defaulted torrent control record while preserving schema-2 Usenet meaning and a loud downgrade boundary; native summaries gain additive kind/readiness fields; one mixed active-download set enforces shared pause/priority/cap and a 60 s stalled-torrent yield; commands, structural facts, and coalesced progress are separate. Production recovery refuses torrent rows because no daemon backend exists, and no config, admission route, listener, DHT, tracker task, or production payload I/O was added
- ✅ Mobile BitTorrent prerequisites (2026-08-06): Android release artifacts now fail closed unless a private upload key is fully configured outside version control, Basic auth encodes non-ASCII credentials as UTF-8, and Node 22 CI pins the mobile typecheck, Jest suite, and secretless release refusal. The future M3 mobile surface no longer inherits those P0 defects; M3 torrent UI work remains downstream of completing the tracked M2 slices #154–#160 and final activation #163
- ✅ Operator documentation (2026-07-18): reworked `README` (accurate status, quickstart, doc index) + `docs/INSTALL.md` (binaries/Docker/Homebrew/source/musl), `docs/CONFIGURATION.md` (full annotated `nzbd.toml` reference), `docs/USAGE.md` (CLI, UI, *arr hookup, feed filter language, scripts, deobfuscation), `docs/DEPLOY.md` (copy-paste recipes: Docker by hand incl. volume map + lifecycle, Compose, Kubernetes, systemd, multi-node cluster)
- ✅ Docker config mount fixed (2026-07-25): the shipped Compose recipe delivered the config through a Compose `configs:` entry — always mounted **read-only** — so every settings-editor save failed with `Read-only file system (os error 30)`; `docs/INSTALL.md`, `docs/DEPLOY.md` and the README used `:ro` config binds for the same net effect. All four now bind the config **directory** read-write (`./config:/etc/nzbd`), which also sidesteps the Docker-invents-a-directory trap for a missing config file and lets the first-run wizard write one. `dev/docker-compose.yml`'s ownership hint corrected to `chown -R 1000:1000` (the container runs as UID 1000; `$USER` only works if you happen to be 1000). Guard test `shipped_compose_files_mount_the_config_writable` fails the build if a shipped compose file goes read-only again; Kubernetes stays deliberately read-only and DEPLOY.md now says the editor is view-and-copy there
- ✅ Dev discovery image build fix (2026-09-19): the single adversarial review found no actionable P0–P3 issues and the scoped fast lane is green, as recorded in [`docs/archive/DEV_COMPOSE_BUILD_STATUS.md`](docs/archive/DEV_COMPOSE_BUILD_STATUS.md). The dev Compose file now assigns its local build definition to both services so a clean host never tries to pull the checkout-only `nzbd:dev` tag while `docker compose up --build` is building it.
- ✅ Deployable examples under `examples/`: `docker-compose/` (compose + `nzbd.toml.example`), `kubernetes/` (namespace/secret/PVC/deployment/service/kustomization + README incl. RWX cluster shape), `systemd/` (hardened unit)
- ✅ `dev/` local-build compose (image from the repo Dockerfile, `compose watch` rebuilds, throwaway `dev/data/`, gitignored dev config) + root `.dockerignore` (target/ was going into every build context); example configs are parse-tested against the real validator (`nzbd-config/tests/examples.rs`)

## Phase 2 — Post-processing ✅ complete

- ✅ par2 packet parser + **native quick-verify** from download CRCs — zero data re-read for intact sets (`nzbd-post/src/par2.rs`, proven against real `par2 create` output)
- ✅ par2 verify/repair subprocess wrapper (par2cmdline-compatible output parsing: Intact / Repairable / NeedMoreBlocks / Unrepairable)
- ✅ Delayed-par unpause: `UnpauseParBlocks` engine command, smallest covering set from `.volXX+NN` names; repair loop waits for the fetched blocks
- ✅ **Delayed-par unpause prices hash-named volumes (F1 of the re-grab loop, 2026-07-31)** — an obfuscated post's recovery volumes carry no `.volXX+NN` marker, and the old filter *dropped* what it could not price, so `unpause_par_blocks` returned 0 with 5 GB of recovery data paused beside the job and every damaged download died PAR_FAILURE. Now: the vol marker stays the fast path (and understands the `.volAAA-BBB` range form), falling back to `bytes / block_size` with the slice size read from the job's own par2 index (`nzbd_par2::scan`), and last-resort unpausing the smallest paused volume as a probe. Every path that can still return 0 **warns with the reason** — the silent "nothing left to fetch" branch is gone
- ✅ **A terminal failure disposes of its own files (F2 of the re-grab loop, 2026-07-31)** — `[post] failure_action = none | park | delete` (old name `health_action` still parses) now governs **every** terminal outcome, not just the health gate: PAR_FAILURE, unpack failure, script failure, health abort. **Default is `delete`** — the bytes are known-bad and the NZB is parked with the history row, so `requeue` gets them back; `park` moves the tree to `[post] failed_dir` (default `<main_dir>/failed`), off the category tree an importer watches. Every failed row now carries a `Failure:Files` param (deleted / parked at … / kept) and a `final_dir` that points at files that exist. **Behaviour change:** the default also arms critical-health abort, previously off unless an operator set `health_action`
- ✅ **An observed ENOSPC latches the disk guard (F3 of the re-grab loop, 2026-07-31)** — the disk-low guard trusted the cached statvfs prober alone, and on a quota-backed mount that answer was wrong for hours: writers, finalize and PP all took `No space left on device` while `/api/v1/status` said `disk_low: false` and intake ran at wire speed (725 GB in one day, onto a full volume). Now any ENOSPC/EDQUOT reaching a write path (`nzbd_engine::is_out_of_space`, reported by the writer, finalize, the category move and PP) **latches the guard immediately** — same intake hold as the threshold — and clears only when statvfs shows twice the floor free, or on operator resume. `/api/v1/status` grows `enospc_observed` + `enospc_where`, and the UI banner names the failed write as the source
- ✅ Unpack: unrar/7z subprocess, hardened (argv-only, scrubbed env, timeouts, 256 KiB output caps, kill-on-drop); NZBGet exit-code maps (unrar 11=password, 5=disk; 7z requires "Everything is Ok"); `.zip`/`.7z`/`.rar` multi-volume first-only/`.001`
- ✅ PP orchestrator: par verify → repair → unpack (⇆ forced-repair retry once) → cleanup → scripts; PostStrategy slots (sequential/balanced/aggressive/rocket); `*PP:done` stamp makes restarts idempotent; 30 s rescan covers leader takeover + lagged events
- ✅ NZBGet extension-script protocol: `NZBPP_*`/`NZBPR_*` env, `[LEVEL]` stdout log lines, `[NZB] KEY=value` commands (FINALDIR honored), exit codes 92–95, legacy header + v2 `manifest.json` discovery
- ✅ History: local SQLite index + authoritative append-only JSONL (shared volume in cluster mode per ADR-16; SQLite never on network FS; index rebuilt from JSONL on divergence)
- ✅ `[post]` config section; daemon wiring single-node **and** cluster (PP runs on the leader, gated live on election state)
- ✅ 6-test e2e suite against real binaries: intact fast path + script env/FINALDIR, corrupt→repaired bit-identical, unrepairable→PAR_FAILURE, unpack+cleanup, script-error→SCRIPT_FAILURE, event-driven manager + restart-skip
- ✅ par-rename / rar-rename: obfuscated posts recover real names before verify/unpack — par2 16k-MD5 catalog (obfuscated `.par2`s found by magic), RAR4/RAR5/7z/zip signatures, RAR5 internal volume numbers, evidence paths remapped so quick-verify still runs; e2e proves obfuscated → renamed → Intact
- ✅ Final-name deobfuscation (`post.deobfuscate_final`, default on): after unpack, whatever still carries a meaningless name gets the job name — SABnzbd's dominant-file rule (biggest ≥ 3× next) with its heuristics ported, plus **season packs** (which SABnzbd skips): several similar-sized videos, all hex/uuid-grade obfuscated → stable `<job> - NN` numbering, logged as heuristic. par2-set names are evidence-protected (never overridden); companions (`.srt`, `-sample`) follow their media file; per-daemon e2e through the real binary. Discrete status: queue shows the `post_unpack_rename` stage (compat `RENAMING`) while it runs; every applied rename is logged and recorded as `Deobfuscate:Count`/`Deobfuscate:Files` job params that persist into history and the compat `Parameters` array
- ✅ Per-job unpack passwords (`*Unpack:Password` job parameter, NZBGet convention) — e2e with a passworded archive
- ✅ Dupe handling (key/score/mode): append carries DupeKey/Score/Mode onto the job; Score/All block against queue + history successes, Force overrides; rejects recorded as `DELETED/DUPE`; real dupe fields in listgroups/history
- ✅ Health-check actions (`HealthCheck`: none/park/delete) — delete removes the failed download's files; recorded `FAILURE/HEALTH`
- ⬜ Direct unpack (`unrar -vp` volume feed during download) — deferred (deep coupling with the download pipeline; unpack-after-download covers the outcome)
- ⬜ Fixture suite extras: par2 damage matrices, multi-volume/passworded rar
- ✅ C2: PP work-lease type + anti-affinity scheduling (a job downloaded on node B post-processes on node C) — see the cluster section
- ⬜ C2 fixture extras: kill-mid-PP reclaim e2e (reclaim machinery itself is exercised by the download-lease tests)

## Phase 3 — Native API + compat ✅ complete

- ✅ Compat C1 — the Sonarr/Radarr certification surface: `append` (v13+ 9-arg form AND legacy 5-arg positional form; base64 or raw XML; AddPaused honored; returns NZBID or 0), `history` (full NZBGet field shape: `TOTAL/DETAIL` statuses, Lo/Hi/MB triplets, Parameters, FinalDir/DestDir, Par/Unpack/Script statuses, deprecated aliases), `editqueue` (3-arg v16+ AND 4-arg v13 forms: Group Pause/Resume/Delete/FinalDelete/SetPriority/SetCategory/SetParameter, HistoryDelete; GroupDelete records a `DELETED/MANUAL` history entry), `config`/`loadconfig` (option projection incl. `CategoryN.*`), `rate`, `pausedownload`/`resumedownload`
- ✅ Queue→history lifecycle (NZBGet parity): post-processed jobs retire out of the queue — immediately after local PP, via the leader sweep in cluster mode; health-failed jobs stamped + retired the same way
- ✅ Native `GET /api/v1/history` (limit param; cluster-aware via throttled JSONL union refresh)
- ✅ Post-stage queue status vocabulary in `listgroups` (VERIFYING_SOURCES / REPAIRING / UNPACKING / EXECUTING_SCRIPT / …)
- ✅ e2e: `sonarr_style_flow_over_jsonrpc` against the real daemon binary — version gate → config category check → base64 append → listgroups poll to empty → history shows SUCCESS/ALL + FinalDir → file imported bit-identical
- ✅ HTTP auth: Basic (NZBGet `ControlUsername`/`ControlPassword` parity, constant-time compare, `WWW-Authenticate` challenge) + Bearer token; enforced across native API and compat shim when configured; `/healthz` open; cluster peer endpoints keep their own shared-secret auth; importer maps `ControlUsername`/`ControlPassword` (with a warning on NZBGet's well-known default)
- ✅ `GET /api/v1/events` — engine events as SSE (job added/finished/deleted, file finished, segment exhausted, server blocked; lagged signal)
- ✅ `GET /metrics` — Prometheus text exposition (rate, remaining, session bytes, paused, speed limit, jobs by status)
- ✅ Compat C2: `listfiles` (full file detail), per-file editqueue actions (FilePause/FileResume/FileDelete via new engine file commands), `log`/`writelog` on the daemon log ring, `scan` + NzbDir watch-dir (30 s + on-demand, `.queued`/`.error` renames, authority-only in cluster mode)
- ✅ XML-RPC (`/xmlrpc`): full value codec (string/int/i4/i8/boolean/double/base64/nil/array/struct, entity refs), `system.multicall`, fault responses — same method table as JSON-RPC
- ✅ JSON-P + GET forms: `GET /jsonrpc?method=…&params=…[&callback=…]`, `/jsonprpc`
- ✅ Golden structural tests: exact wire field sets locked for status/listgroups/history/listfiles/log/envelope — a renamed field fails the suite
- ✅ Native: `GET /api/v1/logs` + `/api/v1/openapi.json` surface summary; log ring fed by a tracing layer
- ⬜ Nightly live *arr containers (CI workflow using real Sonarr/Radarr images) — operator infrastructure; the golden suite + sonarr-flow e2e cover the wire contract in-repo
- ⬜ Auth roles (restricted/add-only users) — full-control auth shipped in 3b
- ✅ `nzbget.conf` importer: KEY=value + `ServerN.*`/`CategoryN.*` blocks, recursive `${Var}` expansion, NZBGet→nzbd vocabulary (Level→tier, Optional→fill, Encryption→tls), mapped/skipped/unknown/warnings report, hostless-server drop, zero-connection raise; `nzbd import-config <nzbget.conf> -o nzbd.toml` writes the converted file + prints the report; round-trips through the TOML parser
- ⬜ `rapidyenc-sys` FFI feature (vendored) + differential fuzzing — deferred to phase 5 (the scalar decoder saturates typical line rates)

## Phase 4 — Web UI + ecosystem ✅ complete

- ✅ Embedded web UI at `/`: one self-contained page compiled into the binary (`include_str!` — zero build toolchain, an explicit simplification from the Svelte plan; ADR-17). Queue with live progress/actions, history, log tail, pause/resume/speed-limit controls, quota/paused badges, SSE-driven refresh with poll fallback, dark/light. **Rebuilt in phase 5 (UI v2) — see that entry for the as-built behavior**
- ✅ Restart hang fixed + queue-load state: an open SSE stream (`/api/v1/events`, always held by the browser) blocked axum's graceful shutdown, so a UI-triggered restart hung the daemon mid-reload and it never re-served ("clicking restart does nothing"). SSE streams now end when shutdown is signaled (watch channel → forwarding task), and the drain has a 3s hard deadline as a backstop; e2e holds an SSE open across a restart and asserts the stream closes + daemon re-serves (verified it hangs without the fix). Also fixed the "queue is empty" flash: the engine now seeds its recovered snapshot synchronously before `spawn` returns, so the first API read is authoritative (regression-tested); UI shows "Loading the queue…" vs "No downloads in the queue."
- ✅ UI boot smoke test: node harness executes the embedded page's inline JS against a DOM shim on every `cargo test` — fails on load-time errors, unhandled rejections, `$()` lookups of missing ids, or boot never starting the SSE plumbing (the class of bug where the script parses but dies at load; proven against the real connectSse regression). Self-skips without node; NZBD_REQUIRE_TOOLS makes CI strict
- ✅ Health-abort transparency (field feedback: "hit 57% health and it still hasn't been cut off"): critical-health abort is armed only when `[post] health_action` is park/delete (matching NZBGet's default-off HealthCheck); the queue now shows each job's par2 **repair limit** under its health %, and a downloading job below that limit is badged **unrepairable · aborting** (armed) or **unrepairable · will fail at end** with a how-to-enable tooltip (disarmed) — `health_abort` exposed via QueueSnapshot → StatusDto
- ✅ Handoff visibility (field feedback: "the *arr handoff is a black box"): the pull is now observed — history entries record first/last-seen (client polls), seen-count and imported-by; NZBGet `HistoryDelete` = hide + stamp (matching real *arr cleanup), `FinalDelete` = remove; `history(true)` returns hidden rows; native history includes hidden + handoff fields; `GET /api/v1/clients` + UI strip show every compat consumer (UA, polling/quiet, last call); History tab handoff chips (awaiting pickup / seen ×N / ✓ imported by) with per-entry restore (re-expose → *arr re-imports), hide, forget, delete-files; hidden state survives index rebuilds (JSONL re-append + upsert); compat test drives the full lifecycle
- ✅ Settings tab v2 (field feedback: "make it a normal settings page"): form-based UI over structured config JSON — grouped cards (paths / servers with add-remove / speed & queue / web UI & API / post-processing / categories), masked passwords kept unless retyped, dirty-tracked sticky save bar; save live-applies what the daemon can absorb (speed limit) and section-diffs the rest into a persistent **restart required** banner with a Restart button (`POST /api/v1/restart`); raw-TOML editor demoted to a collapsed Advanced panel (feeds/cluster); e2e drives live-apply → restart-flag → restart → secrets-preserved
- ✅ Settings API v1: `GET/PUT /api/v1/config` — the live TOML with secrets masked (`***unchanged***` round-trips without retyping passwords), strict validation on save, write + hot-reload via the generalized setup/reload handle (always present now); UI editor with save/discard/download, read-only fallbacks for unwritable paths and `--config`-less runs; fixed a latent bug where a configured speed limit was ignored whenever a queue snapshot existed (config now wins when set)
- ✅ Test depth: SIGTERM graceful shutdown (what docker stop/tini/systemd send) + e2e teardown uses it, so spawned-daemon coverage flushes; full-suite line coverage 85.9% (was 83.2 measured, 26.8 when only `--lib` is run — the suite is e2e-heavy by design); tls.rs 6→90%, nzbd-api 58→80%, main.rs 49→64%; new unit tests for TLS cert lifecycle, compat XML-RPC edges, mask/merge, main.rs option projections
- ✅ Queue UX round 2 (field feedback): job-name cleaning (glued indexer query params stripped, NZB meta-title / par2 set-name fallback for obfuscated hashes); per-job rate EMA + downloaded/ETA in API and UI; global time-left tile; health as green→red gradient against the par-repair limit with tooltip; critical-health early abort (`health_action != none` fails pending segments the moment repair becomes impossible — e2e proves it finishes in 0.25s instead of downloading a doomed job); queue reordering (move top/up/down/bottom) through engine + native API + compat GroupMove*; scheduler tiebreaker = queue position (stable within priority); stall transparency (disk-low / quota / blocked-server holds surfaced as a UI banner + status DTO, block logs at warn); reconnect stagger after resume; dark-default theme following the system with manual auto/dark/light toggle; mobile layout fixes; UI screenshot-verified via headless Chromium (dark/light/hold/mobile)
- ✅ PWA + built-in HTTPS: web manifest, generated icon set (192/512/maskable/apple-touch), app-shell service worker (never caches live data), standalone display + iOS meta, responsive phone layout; PWA assets auth-exempt (browsers fetch them credential-less). `[api] tls = true` serves HTTPS natively — self-signed cert generated once under the state dir (fingerprint logged, `tls_sans` for extra names) or bring-your-own `tls_cert`/`tls_key`; importer maps `SecureControl`/`SecureCert`/`SecureKey`; e2e proves HTTPS handshake → healthz + manifest + icons over TLS
- ✅ First-run setup wizard: a missing `--config` boots setup mode instead of erroring; container-proof saving — boot-time writability probe surfaced in the UI, `preview` mode renders the TOML without writing, failed writes return the TOML with copy/download fallback (read-only mounts/ConfigMaps), directory-at-config-path yields an actionable boot error — the UI serves a form (paths, one server, optional UI password), `POST /api/v1/setup` writes the TOML (round-tripped through the strict parser first) and the daemon hot-reloads with it (`RunOutcome` loop, no restart); Docker image's `/etc/nzbd` is nzbd-writable so a mounted empty config dir + the wizard is the zero-config container path; e2e proves boot → wizard → reload → auth-on
- ✅ Compat C3: `servervolumes` (live per-server total/day/month counters), `sysinfo` (OS/arch + tool paths), `testserver` (real NNTP connect + greeting + AUTHINFO through the production transport — proven against nserv in tests)
- ✅ Packaging: multi-stage `Dockerfile` (tini + par2/unrar/7z, unprivileged user), tag-triggered release workflow (musl static x86_64 + aarch64, macOS aarch64, sha256 sums, ghcr.io Docker push), Homebrew formula with service block
- ✅ Live *arr smoke workflow (`arr-live.yml`, weekly + manual): boots real Sonarr against nzbd and asserts the NZBGet download-client validation passes
- ✅ RSS feeds + filter language (`nzbd-feed`): per-feed pollers over the URL-job fetcher; RSS 2.0 / Atom / newznab parsing (enclosures, `newznab:attr` size, entity refs, CDATA); NZBGet-style filter language (Accept/Reject/Require + `A:`/`R:`/`Q:`, wildcard title/category/url terms, `size:` windows, `age:`, negation, Accept options category/priority/pause/dupekey/dupescore); guid seen-ledger (90-day retention, shared-volume in cluster mode so failover never re-downloads a backlog); leader-gated polling; `fetchfeeds`/`viewfeed` compat RPCs; `FeedN.*` mapped by the nzbget.conf importer (`%` → newline in filters); e2e: feed poll → filter → URL job queued once, deduped on re-poll
- ✅ Live dashboard round 3 (field report 2026-07-25: "screen frozen until browser refresh · unpause showed nothing · header speed 4× off from the row's · titles full of query junk"): **(1) cache discipline** — every live JSON endpoint now sends `Cache-Control: no-store` via a router-wide default layer (some browsers heuristically cache header-less `fetch()` GETs, so the 5 s poll re-served its first response forever; header rate and row rate were two *differently-stale* cache entries — hence 6.4 vs 24.8 MiB/s); the UI's fetches all pass `cache: "no-store"` as the client-side belt, the shell revalidates (`no-cache`), icons keep `max-age`; **(2) SSE `tick`** — the event stream now pushes the full `{status, jobs}` read model at 1 Hz from ONE snapshot (progress/rate/ETA move live with zero fetch round-trips; header and rows can't disagree by construction; idle ticks dedupe to keep-alives; poll drops to a quiet-stream fallback); **(3) complete event wiring** — `queue_pause_changed`/`speed_limit_changed`/`job_assigned` cross under their own names now (previously an opaque `event` the UI ignored) and the UI subscribes to every one; **(4) pause/resume feedback** — the current-state button latches (disabled + amber), the time-left tile flips to "paused"/back optimistically on click, the server confirms via tick + event within a second; **(5) fetching rows** say "fetching the NZB from the indexer…" instead of the broken-looking "0 B of 0 B · 0%" and drop the pause button (the engine rejects pausing a fetch anyway). Regression tests: cache-header matrix, SSE first-frame-is-tick with status+jobs payload, event-wire names, UI boot harness
- ✅ Restart never waits on post-processing (field report 2026-07-25, #2: clicked Restart in Settings, the page never came back — the container was up, the engine kept serving fetches, but teardown sat in `pp_tracker.wait()` behind one running PP job): PP job tasks are cancel-aware at both await points now — a queued job won't *start* mid-shutdown, and a running pipeline aborts on cancel (safe by the pipeline's own crash model: no `*PP:done` stamp yet, kill-on-drop subprocesses, next pass's rescan re-runs it); the daemon also bounds the feed/PP drain at 10 s with a warning as a belt, so no future subsystem can hold the listener offline. Regression e2e: full download → PP reaches a 600 s extension script → `POST /api/v1/restart` → daemon re-serves in seconds with the job still queued for re-PP
- ✅ News-server "test connection" (field request 2026-07-25): `POST /api/v1/servers/test` — live connect + greeting + AUTHINFO through the production NNTP transport (same `probe_server` now backs the compat `testserver` RPC); buttons in **Settings → each server card** (tests the form as it stands, unsaved edits included — a stored password the browser only knows as `***unchanged***` is resolved server-side via `server_index`, no retyping) and in the **first-run wizard** (verify before saving anything); inline ✓/✗ result with the greeting or the failure. Tests: live probe against nserv (good + wrong password), dead port, mask resolution, mask-without-config refusal
- ✅ Trustworthy dashboard round 4 (field reports 2026-07-25 #3–5: queue pause flapping every second · row rate 55.9 vs header 93.1 MiB/s and "the download looks super stale" · per-file lines drowning the Logs tab): **(1) pause attribution** — the engine has exactly two writers of the pause flag, both client commands; every pause/resume now carries WHO (UI sends `X-Nzbd-Client: web-ui`, compat calls use the caller's UA), logged at info (`queue pause requested source=…`), carried on the `queue_pause_changed` event, and shown on the paused badge ("paused · by monarr/1.0") — a flapping queue is a one-glance diagnosis, plus every state-changing compat RPC logs method+client; **(2) wire-true job rates** — the header rate is wire bytes but per-job rates were completed-article deltas: two different quantities whose gap (retries/discards) read as a stale lying row; wire bytes are now attributed per job at the meter and the row rate is the SAME measurement as the header, with a per-job `retried_articles` counter surfaced in the row ("N retries") so wasted bandwidth is visible instead of mysterious; **(3) scoped logs** — every ring entry is tagged `system`/`job`/`file` (+ job id) at the tracing layer; `/api/v1/logs?scope=&job=`; the Logs tab gets scope toggles with per-file OFF by default; **(4)** the "history index rebuilt imported=57" line no longer fires on every 5 s poll (the JSONL union is an upsert and conflict-updates were counted as imports; true-insert counting via row-count delta, poll path logs at debug). e2e: pause holds across owner ticks until a client resumes; scope/job filter unit tests; event-wire carries source
- ✅ Job detail + uploads + activity legibility (field request 2026-07-25): click any queue row → inline detail panel with per-file table (`GET /api/v1/jobs/{id}/files`: sizes, segment done/failed counts, paused/par2/assembled), a live per-job activity tail (`/api/v1/logs?job=N`), article counts + health + dupe key, and **download nzb** (`GET /api/v1/jobs/{id}/nzb` — regenerated from queue state, round-trips through the real parser); **+ add nzb** button uploads .nzb files straight into the queue (multi-file, inline result); post-processing stages render as plain words in the queue — checking integrity / repairing / extracting / cleaning up / moving / running scripts — so what a job is doing is never a mystery **(1)** placeholder names run through the junk-stripper at add time — glued query strings (and the API key riding in them) never reach the UI, history, or disk paths; **(2)** adding a URL that is already `Fetching` returns the existing job instead of queueing another copy (client retries were how one release became four 51 GiB downloads); **(3)** recovery re-spawns the NZB fetch for jobs restarted mid-fetch — the fetch task died with the old process and nothing ever picked the job back up, which is exactly how they got stuck at "0 B of 0 B" — one fetch per unique URL, same-URL pile-ups failed as `duplicate of job #N`; **(4)** in-flight fetches abort on shutdown (previously they could hold graceful shutdown for the 60 s hop timeout). e2e drives the full arc: junky-URL add → clean name asserted → same-URL dedupe → stalled fetch → shutdown under 10 s → restart → auto re-fetch → bit-identical download; plus refetch-plan unit tests
- ✅ History retention + paging, and a log ring that rolls per scope (field report 2026-07-29: "paginate the history tab · set a maximum length · logs are so large it takes forever to load them"). **Measured the live daemon before designing**, and the numbers moved the work: `GET /api/v1/history` took **3116 ms cold / ~250 ms warm for 179 rows** while `/status` took 8–22 ms and `/logs` took 11 ms. 179 rows at 3.1 s is a per-REQUEST cost, not a per-row one, so paging alone would have fixed nothing. Two causes, both syscalls on the network state mount: the cold hit re-reads every `history*.jsonl` line (`refresh()` → `rebuild_from_jsonl`, throttled to 5 s — which is why the repeats drop), and the warm 250 ms is `has_spool()` doing **one `stat` per row** to derive `can_requeue`. **(1) Retention** — new `[history] keep_max` / `keep_days` (1000 / 90 days; `0` disables a bound, both apply, first to bite wins). A trim deletes the index rows, **compacts this node's own JSONL** to the survivors, and raises a monotone `retention_floor` that ingest honours — without the watermark the next refresh re-imports what the trim dropped, the same resurrect-with-a-new-cursor shape later closed by the durable-forget work in `DEFECT_HISTORY_DELETE.md`. That doc rejects "delete the JSONL line"; compaction is not that (a node rewrites only the file it alone appends to, tmp+rename, serialized against its own appends). Explicit forget now has separate everywhere semantics backed by portable tombstones, while retention remains a local bounded-view policy. Every surviving line is kept, not the newest per key — the reader merges `removed_at`/`picked_up_by` with COALESCE. Runs at boot and on a 60 s throttle as jobs finish; the parked NZB dies with its entry; `KeepHistory` now IMPORTS onto `keep_days` instead of being skipped. **(2) Server-side paging** — `?limit=`/`?offset=` with `total` in the response; the `?since_seq=` cursor shape untouched and ignoring `offset` (a cursor already carries a position); History tab gets the queue's pager, size persisted, clamping back a page when a trim empties the one you were on. **(3) The stat-per-row is gone** — one cached directory listing per request decorates the whole page; single-row `has_spool` stays exact for the requeue path itself, because a display snapshot and an answer someone acts on are not the same claim. **(4) The log ring rolls per scope** — the 1000-line ring was one FIFO shared by every scope, so the noisiest one decided how far back all of them reached: on an IDLE nuc3 it held **662 per-file lines, 289 job, 49 system**, and under load the per-file stream evicts the boot banner and every job transition within minutes. Per-file records now roll against their own budget and can never evict a system or job line; reads merge the two by id, so every existing caller sees the single-FIFO order it expects. The Logs tab stopped fetching all three scopes to throw two thirds away (it paid for 300 and showed ~100), the client ring got the same per-class budget, renders coalesce to one per frame, and a record's formatted timestamp is computed once instead of per row per frame. Tests: 9 in `HistoryDb` (count bound, age bound, both, compaction shrinks the log, three refresh cycles + a peer's uncompacted log cannot undo a trim, COALESCE fields survive a rebuild, unlimited touches nothing, pruned entry's NZB reaped, batched spool agrees with `has_spool` row for row, pages tile without overlap or gap), 2 in `logbuf` (a flood cannot evict what matters; reads merge in order), 1 API paging test on the wire contract, 1 importer test on `KeepHistory`, and DOM-harness coverage of the pager, the per-class client ring and the scope-driven backfill — plus the harness now honours `checked` from the markup, which is what made the last one testable at all
- ✅ A job names itself (field report 2026-07-29, screenshot of job #182: a 4.8 GiB download titled `cc310b9901757996b0bdfd880c666e3812e6531d` — "you gotta at least set the name to what it is or what requested it or something other than this random string of characters. That is not useful at all"). Live check: monarr sends the hash as the name for some indexers and a real release title for others (#178/#182 hashes, #177/#179/#180/#181 proper titles), and #182's payload filenames and par2 filenames were ALL obfuscated too — so the existing evidence chain (meta title → par2 set base → common payload stem, `798b1a6`) correctly found nothing and correctly kept the junk. Paul's direction: *"utilize the tight integration between apps… derived from the requestor… and then as soon as possible, pull the name info out of the download metadata itself, whether it's the nzb, the par2 files, etc."* Both halves built. **(1) Provisional name from the requestor** — `*Client` (a new reserved param, set from the request's client header on the native, compat and feed add paths) + the indexer's registrable domain + a short id → `monarr · drunkenslug · cc310b99`. IP literals yield nothing rather than a label (`127.0.0.1` → `0 · af51ab64` was the first attempt, worse than the hash it replaced), and with no context at all it returns `None` — no context, no invention. **(2) The real name, mid-download** — par2 FileDesc packets carry the true filenames, and the recovery index lands early (#182's was 20.5 KiB, finished one minute in). Every completed file is content-sniffed for `PAR2\0PKT` — **never the extension**, since an obfuscated post hides its par2 files too and `is_par2` is a guess off the NZB subject, so it is `false` for exactly the files this needs — read on a blocking thread, and the job renames itself. New leaf crate **`nzbd-par2`** holds the one packet walker: `nzbd-post` depends on `nzbd-engine`, so the engine cannot reach the PP copy, and a second copy would drift; `nzbd-post::par2::load_dir` now delegates to it. Two invariants make the rename safe: **`Job::dir_name` is split from `Job::name`** (the display name may improve, the storage directory never moves — `writer_for` recomputes its target from the job on every writer spawn, so a moving name splits a job's files across two directories; PP reads its input from `dir_name` and moves the result to a destination named from the display name, so the good name still reaches disk), and **`name_provisional` is a flag rather than an inference** — a good placeholder reads as a real title, so "does this look like junk?" would have refused the par2 name when it arrived, which is exactly what the first cut did. e2e: an obfuscated post with an obfuscated par2 index downloads, renames itself to the real release name mid-flight, and lands every file in ONE directory
- ✅ History keeps the job, not a summary of it (field report 2026-07-29: "the whole lot of info for each download job during the download is not being carried over to history like I asked. I want all of that info kept with the job as it moves to history, so it can be looked back upon if needed, and that nzb for that job can be downloaded again if necessary"). **(1) `HistoryEntry::record`** — an optional `JobRecord` captured off the `Job` as it leaves the queue: the per-file table (name, size, segments done/failed, par2 flag), article totals, par size, source URL, requesting client, queue-entry time, storage directory, and the `original_name` the job was admitted under when it later renamed itself, so an *arr that added `cc310b99…` can still find the row. Persisted as a JSON column and in the portable JSONL, `None` on pre-upgrade entries, and merged with `COALESCE` so a later `hide` re-append — which carries no record — cannot erase one. **(2) The NZB is kept.** `spool_nzb` had exactly ONE caller, the delete path, so a job that *succeeded* kept no requeue source and its row read `can_requeue: false` forever — "that nzb can be downloaded again if necessary" was simply untrue of every job that worked. Post-processing now parks every finished job's regenerated NZB, **gzipped** (~10:1 on XML, so ~200 KB each — a thousand entries is a couple of hundred MB, not a couple of GB), reaped with its entry so history retention bounds it for free; `read_spool` sniffs the gzip magic rather than trusting the name, so older plain spools still requeue with no migration. `job_to_nzb` moved from nzbd-api down into nzbd-engine for the same reason `nzbd-par2` exists — nzbd-post needs it and cannot reach nzbd-api. **(3) A History detail panel** mirroring the queue's, opened per row, needing no fetch: everything it shows was captured into the entry, which is the point — a record you have to ask the queue for is one that stops existing when the queue forgets the job. Tests: the record round-trips through SQLite *and* through a rebuild from the portable log; a record is never erased by a line that lacks one; a parked NZB round-trips gzipped and a pre-compression spool still reads; the detail model surfaces requestor, indexer, original name, article counts and duration, marks files that lost segments, and says so plainly on pre-upgrade entries instead of rendering an empty panel that looks like a bug
- ✅ History shows the queue's own detail panel, not a lookalike (field report 2026-07-31: *"I still don't love the history tab. Why can't the job in history just be exactly like it was in the queue?"*). The previous round built history a panel *mirroring* the queue's, and a mirror drifts: it was a bare meta string over a table with no stylesheet behind it (`.jd-meta` and `.jd-files` were never written), so the same job read as two different objects depending on which tab you found it in — and the history one read as the lesser. There is now **one** panel (`KINDS.detail`) and two model builders: `detailModel` reads the live snapshot, `histDetailModel` reads the `JobRecord` captured as the job left the queue, and the renderer cannot tell them apart. History therefore gets the head line (`#id` · status · articles · health · dupe), the post-processing pipeline — its `stages` were already stored, so it is the same widget on the same list, with "now" pinned to `completed_at_unix` so a stage that never closed (the daemon died inside it) reads as time spent rather than time still ticking — and the four-column file table with `DONE` / `SHORT` / `PAR2` states; the queue picks up the provenance line history already had, carrying its non-`*` params. Two things stay different, and both are facts about the *data*, not about the widget: no `download nzb` in history (the parked NZB is reachable only through `requeue`, and a link that 404s is worse than no link) and no activity tail (that is the daemon's live log ring, not part of the record — an empty box would read as "nothing happened"). **The file list folds**: a `<details>`, open in the queue (you opened a live job to watch its files fill in) and closed in history (the job is over; "which part 47" is no longer the question). Because the model writes `open` only when the DEFAULT changes, and it never does inside one open panel, folding or unfolding it yourself survives every tick — law #5. Opening a history row still fetches nothing. DOM harness: the fold default per tab, that a tick does not re-open a list you folded, and that the history model reports the queue's `kind`, its own `scope`, and the article/health/file-state fields it now shares
- ✅ "At once" relabelled to "Downloads at once" — the number was unlabelled in any useful sense
- ✖ Windows packaging — cut (per Paul, 2026-07-17)
- ⬜ Extension manager UI — scripts are discovered + run; a management surface remains

## Phase 5 — Beyond parity 🔶

- ✅ Display catalogue shared with plurx/monarr across web and native mobile: each previously shipped UI is preserved as the default **Classic** layout; opt-in Plex and Theater structures work on desktop, phone, and tablet; Classic, Terminal, noirr, Amber, Giallo, Silver, Void, VHS, Paper, and Tide palettes are independent from Auto/Light/Dark appearance; invalid stored values fall back to Classic and Void/VHS stay midnight-only. Web preferences apply before first paint and switching them does not rebuild live queue rows; native preferences migrate the existing theme and live in Keychain/Keystore. DOM and mobile theme tests pin the default catalogue and dark-only behavior.

- ✅ Per-provider adaptive pipelining: AIMD depth controller per connection — climbs one step after sustained clean batches, halves on connection failure; configured `pipeline_depth` is the ceiling, 1 the floor. Weak providers settle low, healthy ones ride the ceiling (exercised by the full e2e suite)
- ⬜ Native Rust par2 repair swap-in — the GF(2^16) Reed-Solomon engine is a project of its own; the subprocess boundary (`Par2Tool`) was designed for exactly this swap
- ⬜ COMPRESS DEFLATE (RFC 8054) — carried from phase 1; single-digit % on yEnc bodies
- ⬜ io_uring file I/O — blocked on tokio-uring maturity; DirectWrite already avoids the copy-heavy paths
- ⬜ Article-streaming / mount-mode groundwork — design work first (ARCHITECTURE.md §15)
- ⬜ Cluster completion P0–P5: stronger transactional authority and worker lifetime, then segment-split downloads, weighted placement, acknowledged budget transfer, and advisory diagnostics. The existing `kind` field is only a starting point: range scope, exact tokens, and durable results need protocol changes. See [the completion plan](docs/CLUSTERING_COMPLETION_PLAN.md).
- ✅ RSS feeds + filter language — shipped (see phase 4)
- ⬜ `rapidyenc-sys` FFI + differential fuzzing — scalar decoder saturates typical line rates today
- 🔶 UI v2 — live-by-construction dashboard (field report 2026-07-25 #6: innerHTML-per-tick rendering eats clicks; confirm() on delete; silent action failures). **All code milestones shipped 2026-07-25; M0 is the remaining operator step.** Decisions locked with Paul 2026-07-25; plan + wire contracts in [docs/UI_V2_PLAN.md](docs/UI_V2_PLAN.md), one commit per milestone:
  - ⬜ M0 👤 redeploy nuc3 on a post-`9df6b7d` image (and add the missing `./config:/etc/nzbd` bind first) — kills the stale-cache half of the report before any v2 code lands
  - ✅ M1 keyed in-place renderer — store + `rowModel`/`reconcileRows`/`applyRow`, one delegated click listener, detail panel as a stable subtree; `crates/nzbd/tests/ui_dom.rs` pins the five rendering laws
  - ✅ M2 optimistic actions with revert + toasts — every mutating click applies to the view immediately and is reverted (with the daemon's own error text) if the POST fails or nothing confirms it inside 5 s; honest `live`/`reconnecting`/`unreachable` states, the last with a page-wide banner
  - ✅ M3 delete parks the regenerated NZB to history as `DELETED` + `requeue` action — spool beside the history index (`nzbs/<job>.nzb`, reaped with its entry and swept for orphans on open), `delete` answers `{ok, parked}`, entries carry `can_requeue`; a job still `Fetching` parks its `*URL` instead. Engine untouched
  - ✅ M4 instant undoable delete — one click, the row is gone before your finger lifts, an 8 s `Undo` toast wired to `requeue`; history `forget` is instant, `delete files` (the one irreversible action) arms in place for 3 s; zero `confirm()`/`alert()` calls left, pinned by the DOM test
  - ✅ M5 SSE `hb` + `log` events, per-server wire rates — `hb` fires when an idle tick is deduped >5 s (closing the "idle queue vs dead stream" hole the quiet-stream property creates), `log` batches ride the same 1 Hz loop and report what the 200-line cap skipped, per-server EMAs come off the SAME counters as the header rate and reach the status DTO as named rows
  - ✅ M6 sparkline, title ticker, server chips, live log tail — 180-sample rate ring drawn DPR-aware in the rate tile (skipped while the tab is hidden), `▼ rate · ETA — nzbd` title that resets when idle, per-provider chips that go red when a server is blocked, and a 500-line client log ring fed by the `log` stream with scope toggles filtering client-side and a marker wherever lines were skipped
  - ✅ post-review hardening: a mutating POST that never settles (daemon accepts the connection, then answers nothing) left its pending op in flight forever — an in-flight delete means a row you cannot see until reload. `post()` now aborts after 20 s and the sweeper gives in-flight ops a 30 s ceiling rather than an exemption. Also, the Logs tab was re-polling 300 entries every 5 s over a healthy stream, discarding the live tail and its skipped-line markers; it is backfill-only now
  - ✅ queue paging (Paul, 2026-07-26): 20 rows per page by default · 50 · 100 · all, remembered in `localStorage`, controls hidden while the queue fits on one page. Client-side out of the tick's read model — paging server-side would put page params on `tick` and cost the one-snapshot property. Move arrows use the GLOBAL queue index (page 2's first row can still move up) and the page index clamps as the queue shrinks; both pinned by tests
  - ✅ M7 docs sweep — ARCHITECTURE §12 rewritten as-built (rendering laws, store + reconciler, pending ops, the no-build decision and its limits), §10.1 documents `hb`/`log` and the `parked`/`requeue` contract, USAGE gains "how to read the header" and the delete/Undo model, README's UI bullet matches what ships, ADR-17/18 added. `git grep -i svelte docs/` returns only decision context
- ✅ Trustworthy dashboard round 5 (field report 2026-07-26: "speeds always way off · dash goes long times without any update, then it updates · deleted items came back after refresh"): **(1) one rate, one window, time-normalized** — the header tile was a 30×1 s ring average while rows/chips were 5 s EMAs (two windows over one counter routinely disagree: 24.8 vs 9.9 MiB/s in the report's screenshot), and every consumer read each owner drain as "one second of bytes", so a tick delayed by fsync work inflated every rate by the stall factor; meter drains now carry their measured wall time, rates are bytes ÷ measured seconds (`SpeedMeter::drain` + `fold_wire_ema`, cadence-immunity pinned by paused-clock tests), and the header `download_rate_bps` **is Σ per-server EMAs** — tile, chips and rows agree to integer rounding by construction; **(2) `hb` is liveness, not freshness** — a wedged engine (slow state volume) stops publishing, the stream dedups down to heartbeats, and the old client fed `hb` into the tick clock: frozen page under a green "live" dot with the poll fallback disarmed, which is exactly the "long times without any update" report; the clocks are split, and ticks-stale + hb-fresh + wire-was-moving now reads `◌ updates delayed` (new `stalled` state) with the poll fallback engaged for everything but a healthy idle stream; **(3) the page owns stream retry** — EventSource's built-in reconnect covers only clean network drops, any non-200 answer (a 503 mid-restart) closes it *permanently*, which is how "polling — reconnecting…" could be up forever with a stream that was never coming back; a fatally-closed stream rebuilds with capped backoff (1 s → 15 s) plus belts in the 5 s pass and on tab-visibility return; **(4) the engine tick is instrumented and publish-first** — the snapshot publish now precedes journal fsync + debounced save inside `on_tick`, and a tick >1.5 s logs `engine tick ran long` with `journal_sync_ms`/`snapshot_save_ms` (the stall that makes deletes time out client-side — rows hidden, then "back after refresh" — now names itself and its culprit in `docker logs`); a delete whose journal removal fails logs at error instead of `let _` (that failure + a failed snapshot save = the delete resurrecting on restart); **(5)** the sparkline feeds from ticks AND poll results through a ~1 Hz sampler, so a dead stream no longer freezes the shape while the number keeps polling. Tests: drain normalization, EMA cadence-immunity, attribution partition (rate.rs), header == Σ chips == Σ rows (api), DOM harness 199→215: stalled-state machine, hb-never-clears-a-stall, healthy-idle stays live without burning polls, fatal-close rebuild scheduling + `ensureStream` belt, sampler collapse

- ✅ Snapshot save was 65 s of syscalls (field data 2026-07-26, from round 5's own `engine tick ran long` instrumentation: `journal_sync_ms≈20`, `snapshot_save_ms≈65,000`, warnings back-to-back — the engine spent ~95% of wall time blocked in saves, the dashboard honestly said "updates delayed" the whole while): `SnapshotStore::save_guarded` fed `serde_json::to_writer` an unbuffered `File`, one tiny write syscall per JSON token — a ~110 GiB queue is ~30 MiB of JSON and millions of writes, ruinous through a FUSE/network state volume (the volume itself was healthy: 20 ms fsyncs). Fixed: **encode to memory, ONE `write_all`** (matches every other writer in the crate), save returns its byte count, the owner logs `bytes/ms/throughput` per save (warn >1 s), and the debounce is **adaptive** — next save waits 10× the last save's duration (floor 2 s, cap 5 min), bounding a slow volume at ~10% owner duty cycle with zero durability cost (the fsync'd journal covers the gap; recovery = snapshot + replay). Deletes/imports still save immediately, now in milliseconds — which also closes the delete-resurrect window for real. Deliberately NOT done (escalation path if field data ever demands): moving saves onto a persister task off the owner loop — with one buffered write + adaptive spacing the residual inline cost is a single fsync per spaced save. Tests: production-sized 200k-segment round-trip + byte-count-matches-disk (state), `save_spacing` scaling (engine). ARCHITECTURE §8.6 updated

- ✅ statvfs starved the scheduler at wire speed (field data 2026-07-26, from the tick-breakdown warns after the snapshot fix landed: `total_ms≈1600–2000` with `journal_sync_ms≈20` + `snapshot_save_ms≤269` — ~1.5 s unaccounted, warns spaced ~21 s = every 10th tick, correlating with >100 MiB/s wire and a ~30% throughput sawtooth): `update_guards` ran **statvfs against the destination volume inline in the owner loop** every 10th tick, and a write-saturated FUSE/gluster mount answers statvfs in seconds — the owner blocked, lease handout starved, speed dove and slow-recovered. Fixed: the disk-low guard reads a **cached** multi-root value maintained by a dedicated prober task. Each root runs on a detached OS thread with a two-second response deadline; an in-flight syscall is not duplicated and cannot hold Tokio runtime shutdown. The first cycle starts immediately without delaying startup, the owner checks the cache each tick, and **every section of the owner tick is timed** — the `engine tick ran long` warn carries `guards/volumes/publish/journal_sync/finalize/snapshot_save` so no stall can hide in an unmeasured section again. Known-inline remainders, visible under the new fields if they ever matter: `volumes.save_if_dirty` (30 s, small file) and quota `cluster_totals` (peer-file reads, quota users only). e2e covers both a high-floor hold and zero-floor storage observability. ARCHITECTURE §8.5 updated

- ✅ Version identifies the build, from the footer (field request 2026-07-26: a version pinned at the top that never changes identifies nothing — "which build is nuc3 running?" burned three debugging rounds in one evening): the header chip moved into the footer (`nzbd v0.1.0+g729114f — …`), and the string now carries **build identity**: `nzbd-api/build.rs` emits `NZBD_VERSION_FULL` (Cargo version `+g<9-char hash>`, `-dirty` when the tree was, bare when the build saw no git — the Docker context strips `.git` deliberately) and `NZBD_BUILT` (UTC compile stamp, dependency-free civil-from-days; layer-cached rebuilds keep their stamp — same binary, same build). Status DTO gains `built`; the footer tooltip shows it. Tests: build-env shape (api), footer placement + version/tooltip render (DOM harness, 218 assertions)

- ✅ **Integration phase 1 — nzbd's side of the monarr pipeline** (N1–N7 of [docs/archive/INTEGRATION_PLAN.md](docs/archive/INTEGRATION_PLAN.md); master plan lives in the monarr repo). The seam between nzbd and its consumer was a 30 s timer plus an inference: an *arr polled history and guessed from rows. Everything here is additive and the compat surface is untouched (golden tests pass unmodified) — current consumers keep working against every intermediate state.
  - ✅ **N1 post-processing events** — `job_finished` fires at *download* completion, so verify/repair/unpack/move/scripts were log lines only and no event ever carried a path. `job_pp_stage` marks each transition; `job_pp_finished` carries `final_dir`, `pp_status`, the job's params and a `history_seq` cursor, published through the new `EngineHandle::emit` onto the engine's own broadcast (a second bus would mean a second ordering). **The ordering is normative**: the event is emitted only after `HistoryDb::record` returns, so a consumer can react and immediately read `?since_seq=<history_seq-1>` with no race and no retry loop. The terminal paths that never reach PP — health gate, failed fetch — emit it too with their existing `FAILURE/*` strings, because an event that *usually* fires teaches consumers to add a timeout instead of trusting it
  - ✅ **N2 SSE resume** — engine frames carry `id: <seq>` (also `"seq"` in the body), stamped by one pump task so every consumer sees the same numbers in the same order; the last 1024 are replayed on `Last-Event-ID`, and a gap the ring cannot cover — or an id from a previous process, since `seq` resets on restart — answers `event: reset` rather than a stream that looks contiguous and is not. `tick`/`hb`/`log` stay unnumbered: stream-local views, not engine facts
  - ✅ **N3 history cursor** — `seq` on history entries (the local rowid) and `?since_seq=N` returning newer rows ascending, hidden included (hidden means *another* client imported it, not that it never happened). Kept out of the portable JSONL, which is re-imported into indices that assign their own rowids. `?limit=` is byte-identical to before
  - ✅ **N4 add-time params** — `POST /api/v1/jobs?params=<json>` applies params in the same owner-loop turn as the add, so a tracking id is on the job from its first appearance and reaches history and compat `Parameters` through existing plumbing. `*` keys rejected 422 by name (that namespace holds `*PP:done`; a client writing there could forge PP state). Found and fixed two silences on the way: `JobSummary` carried no params, and compat `listgroups` hardcoded `"Parameters": []`
  - ✅ **N5 attribution** — the client registry now hears native `/api/v1` callers (`X-Nzbd-Client`, UA fallback) and counts open event subscriptions, so the clients strip says "subscribed" instead of leaving a healthy push consumer looking like nothing connected. Subscribers are exempt from the idle TTL — a push consumer legitimately goes quiet for hours. A native history hide records `picked_up_by` like the compat path always has
  - ✅ **N6 category destination honesty** — `[[category]] dest_dir`/`unpack`/`extensions` were parsed and advertised as `CategoryN.*` while post-processing ignored all three: advertised paths were not actual paths, which is a silent import failure with nothing in any log to explain it. PP gained a Move stage (the long-unused `PostStage::Move`) that relocates the finished folder before scripts run and before any path is reported, cross-device moves fall back to copy+remove, and a failed move reports where the files ARE. **Behavior change** for configs that set these keys expecting nothing to happen — noted in CONFIGURATION.md
  - ✅ **N7 metrics + docs** — `nzbd_events_emitted_total{event}`, `nzbd_sse_clients`, `nzbd_pp_stage_seconds{stage}`; ARCHITECTURE §10.1 documents the pp events, the ordering guarantee, the resume protocol and add-time params; USAGE gains "Subscribing to the handoff" and no longer opens the handoff section with *"There is no push."*
  - Tests: `crates/nzbd/tests/integration_events.rs` drives a real daemon over real sockets — event order, id/no-id per frame class, the transfer id surviving add→completion→history, the history row readable the instant the event fires (no retry loop, because a retry loop would hide the race the guarantee rules out), replay of exactly the missed tail, `reset` on an uncoverable gap, `*`-key rejection, subscriber visibility, native-hide attribution. Plus ring eviction (api), cursor properties and JSONL portability (state), and the category rules including the case that must stay boring — no matching rule, nothing changes (post)
  - ✅ Post-build adversarial review, five defects found and fixed: **(1)** a crash between the category move and the `*PP:done` stamp wedged the job permanently — the re-run drove the pipeline against a directory that no longer existed, errored on par2 load, and so was never stamped, never in history, never announced, never retired; PP now adopts the already-moved directory, which is what keeps "the stages are idempotent" true across the one stage that relocates its own input. **(2)** a bare `seq` cannot tell "you missed events 51-60" from "we restarted and these are *different* events 51-60", so the restart case the plan requires `reset` for was being served as a clean resume; the wire id is now `<boot>-<seq>` (opaque to the client, plain `seq` still in the body). **(3)** the cross-device move copied straight into the library, so an interrupted copy left a half-populated folder a consumer would happily import from and every later rename would fail ENOTEMPTY against; it now copies to a sibling scratch dir, fsyncs, and renames into place — an interrupted move is always "not moved" or "moved", never "half moved". **(4)** the event pump falling behind the engine dropped events *before* they were numbered, leaving no seq gap and no way for any consumer to find out; the pump now publishes its own `lagged` as a numbered event, so the one invisible loss mode became the loudest one. **(5)** the URL-add dedup path returned before applying params, losing the tracking id on exactly the adds most likely to be retried
- ✅ The first-run wizard stopped eating configured installs (field report 2026-07-26, screenshot: "First-run setup — No configuration found" **rendered on top of a live dashboard**, on nuc3, again — "this has been broken since forever"). Two independent defects, both fixed here. **(1) The config was never durable.** `/etc/nzbd` in a container is writable whether or not anything is mounted at it: with no bind mount the wizard's write, and every later Settings save, succeeded into the image's writable layer — so saves "worked", survived `docker restart`, and were destroyed by the next `compose up --build`/image pull. A configured, downloading install reverted to unconfigured on **every deploy**, and the page blamed the config path instead of the missing mount. This was a known trap (DEPLOY.md documented it; the operator checklist carried "add the missing `./config:/etc/nzbd` bind" as a 👤 TODO) — which is exactly the problem: correctness sat behind an operator remembering a mount. Now the config is a **replica, not a singleton** — every successful write (wizard + settings editor) also lands `<state_dir>/nzbd.toml.saved` on the **data** volume, the mount the queue itself proves persists, and a boot that finds no config file **recovers from it, restores the file, and comes up configured** before it will even consider serving the wizard. `NZBD_MAIN_DIR` (set by the image to its declared `VOLUME`, pinned by a test that fails if the two ever disagree) is how recovery finds the data volume with no config to read `paths.main_dir` from; `/proc/self/mountinfo` tells `durability()` "on a volume" from "on the container's own layer", so the daemon warns **before** the first loss — boot log and a UI banner naming the path, the volume line that fixes it, and where the durable copy lives. A recovered boot says so in the same banner. **(2) The wizard rendered on top of the app.** `checkSetup` hides every `.wrap` child it doesn't need via `el.hidden = true` — but the UA stylesheet's `[hidden] { display: none }` loses on specificity to any class rule setting `display`, so `.stats` (grid), `.controls` and `nav.tabs` (flex) — and *exactly* those three, precisely what the screenshot shows — stayed on screen: a dashboard of dashes and dead controls under a setup form. One global `[hidden] { display: none !important }` fixes it for every element the page will ever add (the `.pager[hidden]` one-off it replaces was this same bug, patched locally once already). Tests: 7 unit (mirror round-trip + write-then-rename, search order, unparseable mirror skipped not fatal, mountinfo longest-prefix + octal escapes), a daemon e2e that boots with **no config file** and a mirror on the data volume and demands auth-on/wizard-gone/file-restored/queue-live, the wizard test now asserting the mirror is byte-identical to the config, the Dockerfile ENV↔VOLUME guard, and DOM harness 246→272 (setup takeover keeps header/banner/wizard/footer and hides the rest; the stylesheet must carry the global `[hidden]` override; the banner stays silent when mounts are healthy and names path + fix + copy when they are not). **Confirmed live on nuc3 the same evening**, through the browser: `/api/v1/setup` answered `setup_mode:true, writable:true, container:true, config_path:/etc/nzbd/nzbd.toml` — writable and empty, the exact ephemeral-layer signature — and computed styles showed `.stats` (grid), `.controls` and `nav.tabs` (flex) with `hidden=true` but `display` NOT none while every other hidden sibling was `display:none`. That check also caught a flaw in the first cut: in setup mode `current` is still `Config::default()`, so the endpoint was naming a mirror path derived from a `main_dir` the operator had not chosen yet — `mirror_path` is now null until a real config exists and the banner promises the behaviour instead of a guessed path. ARCHITECTURE §11 + DEPLOY.md updated

- ✅ The settings editor could delete a password and report success (field report 2026-07-26, minutes after the durability fix landed: "suddenly having issues connecting to my newsgroup server even though my newsgroup account is still fine" → "it imported the config file but lost my password"). Secrets are plaintext in `nzbd.toml`; the editor masks them to `***unchanged***` for display and `merge_masked_secrets()` swaps the real ones back on save. Two silent holes. **(1)** An unresolvable mask — renamed server, newly added server, a running config that never held that secret — was written through as `None`: the password was *deleted*, the save returned **200**, and nothing said a word. Masks that cannot be resolved are now left in place, the merge returns the list of them (`#[must_use]`), and `PUT /api/v1/config` answers 422 naming the exact field to retype; the strict validator is a second net behind it. **(2)** Nothing on the boot path rejected a config that still contained the mask, so a file restored from a copy taken out of the editor left the daemon authenticating with the literal string `***unchanged***` against a perfectly healthy account — the symptom pointing at the provider, the cause three days earlier in a text editor. `Config::validate()` now refuses any masked secret and names both the field and where such a file comes from; `Config::parse_toml_unvalidated()` is the single seam that skips validation, for the editor's merge path only. And the trap that produced the bad file is gone at the source: the editor's **Download** used to hand back the masked text named `nzbd.toml` — the exact filename you drop into `/etc/nzbd` — and now names it `nzbd-masked.toml` behind a "NOT A USABLE CONFIG" header, with an inline warning in the editor itself. This mattered more than it looked: the mirror shipped hours earlier would have replicated a password-less config onto the data volume and restored it on every boot. Tests: masked-config-refused-at-boot (per field, with a near-miss string that must still parse), unresolvable-mask-reported-not-blanked (index fallback still works; the second server is reported, left masked, and fails validation), the masked round trip re-pinned through the unvalidated seam, a daemon e2e that PUTs an unresolvable mask and demands 422 + the real secret still on disk (verified to return `{"ok":true}` against the old merge), and DOM harness 272→278 on the download naming/header. ARCHITECTURE §11 + CONFIGURATION.md updated

  - ✅ **History forget is durable everywhere (2026-08-05)** — [the confirmed delete-resurrection defect](docs/archive/DEFECT_HISTORY_DELETE.md) is closed with portable JSONL tombstones. `HistoryDb::delete` appends one mutation per `(job, completed_at)` before removing the derived SQLite row; every upgraded node projects those mutations into `history_tombstones`, and inserts refuse a covered key. File order cannot act as a cluster clock, so the tombstone is monotone and compaction keeps it even after the obsolete entry line disappears. The UI no longer brings a forgotten row back on refresh, `?since_seq=` never publishes it under a fresh cursor, and delete → Undo/requeue leaves no lying `DELETED` record. Tests cover three replay cycles, hidden re-appends, lexically later peer files, fresh-index convergence, compaction, the native API requeue flow, and the unchanged NZBGet golden surface
  - ⬜ Next: monarr phase 2 (native client + push subscription) consumes all of this; nothing here waits on it

- ✅ A client chip is a glance, not a User-Agent dump (field report 2026-07-27, screenshot: one chip in the API-clients strip ran the full width of the page). The strip renders whatever `User-Agent` each client sent, and an open browser tab sends ~130 characters of Mozilla-compatibility archaeology — so the browser's own chip dwarfed and pushed down the two chips that answer the question the strip exists for (*is the \*arr connected?*). `clientLabel()` now collapses a browser agent to browser + major version + platform (`Chrome 150 (macOS)`), matching most-specific-first so Edge and Opera are not read as Chrome and Chrome's own `Safari/` token is not read as Safari; a product token (`Monarr/0.7.0`, `Sonarr/4.0.0.748`) is already the short name and is passed through untouched, capped only against an absurd one. The raw agent is not lost — it moved to the first line of the tooltip, where length costs nothing — and the chip's reconciler **key stays the raw agent**, because two browsers can collapse to the same label and a duplicate key would merge two clients into one row. Chip CSS tightened (11 px, 2×9 padding, 6 px gap) with a `max-width` + ellipsis belt so no future agent can stretch the strip past the table above it; the empty-state chip is a chip again, its sentence of explanation moved to its own tooltip. Tests: DOM harness 272→303 — Chrome/Firefox/Edge/Safari-on-iOS labels, the unrecognised-Mozilla fallback, product tokens and the absurd-token cap, missing agent, the chip text/tooltip split, quiet vs live, and two same-label browsers still reconciling to two chips

- ✅ A job is named by the NZB, not by whatever the client called the file (field report 2026-07-27, screenshot: job #118 titled **`yEnc`** while its own file list read `Bates.Motel.S01E07.720p.WEB-DL.DD5.1.H.264-KiNGS.part01.rar` — "some of these are still getting the wrong name"). Live check on nuc3: **9 of 120 history rows** were junk-titled, all of them monarr adds — eight `yEnc` and one `UcsRDCyhGHPCP2TqBJWrnUg`. `clean_job_name` treated the caller's filename as authoritative and had exactly one way to distrust it, `looks_obfuscated`, which only fires on a **hex** hash of 16+ characters: `yEnc` is four characters and not hex, so it short-circuited before the `<meta>` title and the par2 set name were ever consulted, and the base64-shaped id was not hex so it sailed through too. The name a client hands us is a **hint**; the NZB is **evidence** — a hint that carries no information now loses to what the document says about itself. Two new tests join the hex one: `looks_like_token` (a 16+ character run with no separator at all is an id, not a title — every scene name carries dots, spaces or dashes) and `is_posting_artifact` (drop the `yEnc` marker and bare part counters; if under three characters of anything remain, the name was furniture). Evidence order is `<meta>` title → par2 recovery-set base → **common payload stem**, the last one new: `X.part01.rar` + `X.part02.rar` → `X`, with the counter, the volume word and the separator trimmed back in that order so a release ending in a year (`Blade.Runner.2049.001`) keeps it. With no evidence anywhere the junk is still shown rather than replaced by an invention — the operator can see what the client actually sent. Tests: 4 new (`clean_name_rejects_posting_furniture` pins the exact screenshot, `clean_name_rejects_opaque_token` pins job 120 and proves a terse-but-separated title still wins, `clean_name_falls_back_to_common_file_stem` covers the no-par2 NZB and the trailing year, `clean_name_keeps_junk_when_there_is_no_evidence`), suite 261→265. Note the fix is at admission only — the nine rows already in history keep their names

- ✅ The version identifies the build (field report 2026-07-27: "nzbd still isn't being versioned properly" — "the version at the bottom of the main page has been the same for a hundred commits"). `355d811` added a `+g<hash>` suffix and a compile stamp and was believed to have fixed this; it had not, on the only machine that matters. **Two causes, and the first hid the second.** **(1) The image cannot see its own commit.** `.dockerignore` excludes `.git` to keep the build context small, so `build.rs`'s `git rev-parse` found nothing inside the Docker build and fell back to the bare Cargo version — silently, which is the whole problem. Confirmed live: nuc3's `/api/v1/status` answered `version: "0.1.0"` flat, no hash, with only `built` distinguishing one deploy from the next. The commit now travels in as `--build-arg NZBD_GIT_DESCRIBE`, declared and forwarded by the Dockerfile, filled in by `make docker` / `make docker-build` / `make version` and passed through by both shipped compose files — and a build that still has no identity renders **`+unknown`** rather than impersonating a release, so an anonymous image announces itself. **(2) Nothing was ever tagged and nothing ever bumped.** The workspace sat at `0.1.0` from the scaffold commit onward, so even a build that knew its hash led with a number that had described a hundred different trees. Versions now come from `git describe --tags --always --dirty --match='v[0-9]*'`: on a tag `0.2.0`, past it `0.2.0-7-g798b1a691`, unclean `-dirty`, untagged `0.2.0+g798b1a691` — it moves on **every commit**. Workspace bumped to **0.2.0** and tagged `v0.2.0`; DEPLOY.md gains "Which build is this?" and "Cutting a release". Structurally, `build.rs` now only gathers raw facts and the composition moved into `nzbd-api/src/version.rs`, because a pure function can be tested and a build script cannot. Tests: 7 new — 6 on the composition (tag exactly, commits past the tag, dirty, untagged repo, a tag *behind* the crate version kept as `0.2.0+v0.1.0-…` rather than claiming the wrong release, and every form still parseable from the front) plus `the_image_is_told_which_commit_it_is`, which pins the whole chain (Dockerfile ARG + ENV forward, the Makefile `--build-arg`, and any compose file that actually builds) so it cannot rot back; `build_identity_is_populated` now fails outright if a build **from a checkout** comes out `+unknown`, and only notes it for a tarball build that genuinely cannot know. Suite 265→272

- ✅ The requestor placeholder stopped eating its own tail, and one directory means one job (field report 2026-07-29, screenshot: **sixteen queued jobs, every one of them titled `monarr/0.11.0 · drunkenslug · monarr/0`** — "it's not getting the names correctly"). Went and looked, and the daemon's own log had both renames three seconds apart. **(1) `requestor_name` took its discriminator from `job.name` — the field the rename it feeds then overwrites.** It runs twice for a URL job, once at admission and once when the fetch completes, and the second call read the first's output: the first eight characters of `monarr/0.11.0 · drunkenslug · e44dbc35` are `monarr/0`. That is a fixed point, and the *same* fixed point for every job from one client and one indexer. The ident now comes from `original_name` — what the client called it, which nothing overwrites — and falls back to `#<job id>` when that is missing or merely repeats a part already in the name. **(2) A provisional name masked the NZB.** `complete_url_fetch` re-ran the evidence pass with the *current* name as the hint, and a provisional name reads as perfectly informative — that is its whole design — so `name_from_evidence` handed it straight back and the par2 set sitting right there in the NZB was never consulted. Sixteen jobs whose recovery sets named them exactly (`72.Hours.2026.2160p.NF.WEB-DL…`, `The.Death.of.Robin.Hood.2026…`) were titled after their requestor instead. A provisional name now offers **no** hint at all: ask the NZB alone, and if it has nothing, keep the placeholder. **(3) The consequence, which was the real damage: `rename_job(storage_too)` writes `dir_name` from the name, so sixteen jobs shared one download directory** — `/working/monarr/completed/monarr_0.11.0 · drunkenslug · monarr_0/` — two releases interleaved in a folder, post-processing verifying and moving them as though they were one. A name can never be relied on to be unique (a placeholder is built from client + indexer, identical across every add; and a duplicate add is legitimate anyway), so uniqueness is no longer left to luck: `QueueState::unique_dir_name` appends the job id on collision, admission and every rename go through it, and the owner loop renames via `set_job_name` rather than the free function. **(4) The queue already in this state repairs itself on boot** rather than needing fifteen deletes by hand: a job with an open name is told what it is by its own stored file list (`name_from_files` — the same evidence pass, asked of `FileEntry` names instead of a parsed NZB), and a shared directory is split — but only for a job that has written nothing, because moving the folder of one with bytes in it orphans them. Tests: 7 new — the placeholder is stable under repetition (three passes), two jobs from one requestor stay distinct, a provisional name no longer masks the NZB, a fetch with no evidence still keeps its placeholder, two jobs never share a directory (duplicate add *and* colliding rename), boot repairs names and splits directories while leaving a part-written job's folder alone, and `name_from_files` agrees with the NZB pass. Suite 326→333

- 🔶 **The re-grab loop (field report 2026-07-31: ~1 TB of duplicates — "5 copies of true lies?")** — diagnosed live on nuc3, four defects feeding one loop. **(1)** repair asks for delayed par blocks it can never receive: `unpause_par_blocks` prices vols via `vol_par_blocks`'s `.volXX+NN.` filename parse, which obfuscated posts don't carry, so `freed==0` → silent "nothing left to fetch" → every job with ≥1 failed article (of ~116k) dies PAR_FAILURE with 4–5 GB of recovery data paused beside it — repair has NEVER succeeded on nuc3. **(2)** terminal failures leave their full dirs in the category tree: ~523 GB of corpses in the last 100 history rows alone. **(3)** dozens of observed ENOSPC (writer/finalize/PP) never flip the disk-low guard — statvfs said fine, intake ran at wire speed into a full volume, 725 GB burned in one day. **(4)** monarr half-imports successful season packs on the same full disk, surfaces nothing, cleans up nothing, re-grabs (five True Lies in 10 h; the same NZB twice in 6 min). Handoff plan with evidence, guardrails, milestones (nzbd F1–F3 · monarr M1–M4 · nuc3 runbook R1–R7): **[docs/archive/REGRAB_LOOP_PLAN.md](docs/archive/REGRAB_LOOP_PLAN.md)**. No code changed in this commit — the plan is the handoff. Also verified healthy while in there: the `2453665` dir-uniquifier live, monarr 0.13.0 params + pickup on 100/100 rows (the rename-breaks-matching hypothesis is dead).

## Operator checklist 👤

- ✅ Push `main` (done — CI ran; `badges` branch is CI-owned, never push it: `git branch -D badges && git fetch --prune`)
- ✅ Enable hooks on your clone (done — your pre-push ran the suite)
- ⬜ **Redeploy nuc3** on a current image. The missing `./config:/etc/nzbd` bind is no longer load-bearing — this build keeps a durable copy of the config on `/data` and restores it on boot, so the wizard cannot eat the install again — but add the mount anyway and the warning banner goes away: `mkdir config && docker cp nzbd:/etc/nzbd/nzbd.toml ./config/ && chown 1000 config/nzbd.toml`, add the volume line, `docker compose up -d --build`. Deployed 2026-07-29 18:49 UTC as `0.2.0+unknown`: the running code is current through `2453665`, but the deploy drops the build arg and the deploy checkout is stale (`make docker-build` "missing" there = pre-`30e811b` Makefile) — pull + rebuild per [REGRAB_LOOP_PLAN.md](docs/archive/REGRAB_LOOP_PLAN.md) §8 R5 until the footer stops saying `+unknown`
- ⬜ **Drain the re-grab loop** (before any redeploy): pause the queue, reclaim ~530 GB of confirmed-failure corpses (exact `rm` list in [REGRAB_LOOP_PLAN.md](docs/archive/REGRAB_LOOP_PLAN.md) §8 R2 — do NOT touch the EPSiLON S04 / Bates S01 dirs), answer the `df`/gluster-quota question (§9 Q1), then have monarr re-import the two half-landed packs
- ⬜ Optional stopgap until F2 ships: `[post] health_action = "delete"` so health-gated failures stop leaving partials
- ⬜ Optional: `brew install par2 p7zip` for full local test coverage (without them the tool-backed tests self-skip; CI always runs them)
- ⬜ Branch protection for `main`
- ⬜ Flip repo to public (decided 2026-07-18; Settings → Danger Zone → Change visibility) — Coverage/Test-count badges render once flipped
- ⬜ Real-provider download smoke test
- ⬜ Real-Gluster cluster soak (CLUSTERING.md §11)
