# Integration recovery — custody, admission, and publication

**Status:** implemented; review, qualification and rollout evidence tracked in
[PR #272](https://github.com/pjunod/runner/pull/272) ·
**Written:** 2026-11-03 (America/New_York) · **Branch:**
`codex/baseline-integration-recovery`

Companion to [publication performance](PUBLICATION_PERFORMANCE_PLAN.md) and
[deployment](DEPLOY.md). This record owns the remaining integration failures
and the checks required before deploying the publication improvements.

## Failures — distinguish fixture ordering from application behavior

At main `8ce989bd`, the post-processing pipeline target reports five passes
and 34 failures. The initial whole-workspace run also fails local-server
creation with `Operation not permitted` inside the execution sandbox. Those
network failures require an unrestricted rerun; they are not product fixes.

| Cause | Correction and retained evidence |
|---|---|
| Import verification is asynchronous | Write fixture bytes before import, supply expected publication sizes from declared completed ranges, and retry only the explicit verification-busy read result. CRCs and deliberately corrupt payloads remain unchanged. |
| Imported payload jobs do not emit a completion event | Import without emitting, await the verified exported row, then emit the fixture's single requested event. Missing jobs remain failures. |
| Direct pipeline calls omit processing admission | Use the owner-issued admission, process through its scoped handle, and await release on success or failure. |
| Script FINALDIR tries to replace an in-use source association | Retain the admitted source and its exact generation/job association; preserve existing Keep. Observe the external output as a separate unowned, unassociated artifact. |
| Queue removal precedes independent scratch cleanup | Await checked inventory reconciliation and require the exact retirement operation, deleted scratch state, absent path, and matching source/scratch/outcome/history receipt. |
| A final-stamp fault depends on gate-call counts | Inject only after durable history; acknowledge failed-handler completion before restoring write permissions and retrying. Preserve the unstamped-state and exactly-one-event/history assertions. |

The fixture correction alone reaches 36 passes out of 39, leaving the script
association conflict and two readiness/cleanup acknowledgement failures.
The candidate adds a Keep-preservation variant and negative external-output
ownership/association/path-replacement coverage.

The first unrestricted whole-workspace run completed with seven failing
targets: daemon, integration events, cluster fairness, compatibility units,
engine units, engine end-to-end, and the post-processing pipeline. This is
the full failure roster, including failures outside the original pipeline.

| Additional cause | Correction and retained evidence |
|---|---|
| API authentication becomes active before backend readiness | Poll the explicit backend-ready state before backend endpoint assertions. Event daemons use independent ephemeral monitor ports. |
| Same-root recovery starts one verifier per one-second tick | Start the next eligible verifier after the completed worker joins and its evidence is durably acknowledged. A 20-job regression drives real acknowledgements without periodic ticks and preserves pending-byte and concurrency assertions. |
| Accepted journal ranges lose declared publication size across adoption/restart | Persist nonzero declared size only for an accepted matching file/segment; retain existing expected-size evidence. Verify real CRC ranges and publication after fold and restart. |
| Newly learned size is mistaken for changed replicated intent | Permit only a unique, positive, canonical local-only size for an existing file ID when comparing recovery agreement. Existing durable size conflicts and guarded-write hashes remain exact. |
| Standalone URL admissions accumulate an external-authority pending index | Index unaccepted admissions only when an external persistence guard is configured; retain stable private source-generation identity separately from public consumer metadata. |
| Authority adoption omits the speed limit | Include the recovered limit in the projection transaction, preserve newer operator revisions, and restore the prior limit before persisting a failed adoption's rollback. Assert both memory and disk outcomes. |
| Quiescing a source with no writer handles leaves an empty retirement fence | Release that exact empty fence only after its durable quiesce acknowledgement; retain real writer joins and the paused state. |
| A failed terminal write and failed rollback can leave a permanent admission fence | Keep the fence until a matching restored-record snapshot is durably acknowledged; qualify automatic retry after storage recovers. |
| A released rollback fence does not notify waiting admissions | Notify the owner epoch only after durable release; test the public admission waiter across failed writes and storage recovery. |
| Terminal receipt activation also waits for independent scratch cleanup | Require exact stamp/history/generation agreement and durable receipt activation, then let inventory maintenance perform its separate checked retirement. |
| Restart treats a partially deleted failed source as a new processing input | Admit only replay of its exact existing generation-bound deletion journal. Keep ordinary payload export, repair, salvage, and publication fenced; allow only failure metadata/history/stamp updates. Persist that classification through a terminal stamp and restart until queue retirement. |
| Delivery acceptance commits, then fails an unrelated queue transaction | Keep the replicated catalog's authentication, quorum, authority and generation CAS; bypass queue projection only for its two exact acceptance/retry routes. |
| Completion fixtures read between snapshot publication and event emission | Await the matching bounded completion event before asserting the terminal snapshot and partial private bytes. |
| Snapshot-repair polling treats documented election gaps as queue observations | Retry only the two exact read-only election responses within the original deadline; require a real successful Completed response. |
| Failed-cleanup restart fixture assumes startup observation is a distinct completed retry | Prove the legacy cohort, retry metadata and exact deletion journal survive restart, then exercise explicit operator retry occasions after the manager is serving. Retry deadlines and payload fences remain unchanged. |
| Operator retry routes legacy cleanup through fenced payload export | Route failed cleanup summaries to owner admission even for the legacy cohort or an idle verifier. Exact proof-bound cleanup custody remains the authority; ordinary payload export stays fenced. |
| Slow retirement or connection-budget drain blocks healthy lease renewal | Revoke authority immediately, retain capacity while tracked workers join, and retire only the original grant context. Serialize actual budget drains separately; heartbeat submission does not wait for them, while new grant admission still does. |
| Completion preflight rejects the saved token after a healthy renewal | Accept only predecessor tokens of the same owner/incarnation/resource/fence at preflight, matching the existing renewable publication transaction. Preserve immutable outbox/result identity; the transaction still checks current expiry, terminal state and exact job intent. |
| Ready Post completion waits behind fast download results | Alternate Post and download attempts independently for fresh results and durable replay; rotate retries within each lane. Retain one copy/verification operation at a time and replay without requiring a live lease. Bound outbox lookahead and decode only the selected full descriptor. |
| Every heartbeat writes unchanged provider-budget state | Advance revision only on semantic handoff changes; serialize and deduplicate confirmed durable writes. Changed commands require exact saved state, current node command/generation, and live issuer fencing before issuance. |
| A hold fixture cancels its own unfinished import recovery | Await the verified imported input before installing the hold; retain the later custody-probe expiry injection and all refusal/source assertions. |
| Direct archive fixture lacks source custody during terminal commit | Hold a real processing attempt across extraction and commit, join its workers, then release custody before salvage assertions. |
| A mailbox barrier can run before the manager's startup scan | Signal the real skipped scan through the fixture gate, then drain it through the mailbox. Retry only explicit NotReady within the original budget while proving no attempt, stamp, history or journal mutation occurred. |
| Source reconciliation blocks all scheduling on unrelated queue verification | Query only durable candidate IDs without an existing source binding/stamp. Each new binding still needs checked export, current intent and guarded CAS. Busy candidates remain ineligible; assignments require checked readiness and consume capacity only after actual actor acceptance. |

Engine fixtures must await only the explicit export-busy condition, use real
inventory paths and worker contexts, and bound event receives. A payload
import is not a download-completion event. Reserved-parameter tests compare
the actual before/after metadata because new admissions already carry their
own lifecycle marker.

The failed-cleanup capability does not certify surviving bytes. It preserves
the existing operation's request and retry deadline; the existing executor
still checks Keep, holds, current ownership, root identity, active workers,
and the surviving manifest before unlinking. A successful deletion receipt
can replay metadata after the path is gone. Replaced files remain untouched.
The integration fixture keeps its permissions fault active across restarts
and inspects cleanup metadata separately from verified payload export.
It requires three distinct retry occasions, one terminal history row, no fourth
attempt, and preservation of the original leftover bytes. Restart alone does
not bypass the scheduled automatic retry deadline.

The fairness regression uses a two-second lease with a conservative local
deadline of 1.9 seconds. Retirement and budget owner replies can take ten
seconds, so awaiting either inside heartbeat can expire unrelated healthy
work. The correction separates execution lanes while retaining actual join
acknowledgements, durable generation barriers, and truthful drain receipts.
An expired grant retains its resume journal; only accepted completion permits
its compaction. A delayed retirement cannot cancel a successor's custody.
Healthy renewals also exposed a contradictory completion preflight: it required
full token equality even though saved immutable completions predate later
renewals. The existing final publication transaction already selects the
latest live token of the exact same fenced operation. Align the preflight
with that contract; future tokens, another owner/fence, changed job revision,
expired current authority, and conflicting immutable results remain rejected.
The detailed trace then isolated completion ordering: a ready Post result
waited about 16.6 seconds behind seven download results, keeping its scarce
slot occupied. Independent fresh/replay turns bound progress for both classes;
one blocked receipt cannot force every fresh turn back to downloads. A
bounded rotating header scan replaces full outbox sorting and repeated full-job
decoding. Only the selected immutable request is fully decoded for submission.

Provider-budget persistence is deduplicated at the application boundary, not
by issuing repeated Raft writes whose SQL happens to make no change. An old
snapshot cannot acknowledge a newer in-memory revision. Exact duplicate
readback handles a lost acknowledgement; a conflicting/newer durable row or a
stale issuer refuses the command. Expired holders are removed from pending
targets before promotion so a later handoff cannot wait forever for their ACK.
Source reconciliation no longer exports every bound row just to discover that
its source fact already exists. Candidate IDs are routing metadata; they grant
no payload authority. Known recovering candidates remain pending for that
scheduling pass, and every new binding and actual grant retains checked
export. Terminal retirement requires a checked row matching durable source,
control, status and stamp, its exact inspected revision, and the existing
selected-Delivery guard. Whole-queue re-verification during authority projection
replacement remains a separate optimization; this correction reuses no proof.
See the [independent I/O audit](IO_EFFICIENCY_AUDIT.md) for remaining cost
investigations; those follow-ups are outside this qualification fix.

## Custody — FINALDIR reports output without transferring authority

The original source remains `retained` and associated with its job. Its
`Artifact:Id` and `Artifact:Generation` remain the terminal receipt's source.
An existing operator Keep choice is preserved; automatic Keep would also
prevent independent private-scratch retirement.

The external observer verifies the discovered directory and rejects owned or
job-associated records. It records no job association and grants no deletion
or Delivery authority. External IDs use separate output parameters. History's
delete-files operation continues to reject a final-directory/source mismatch;
inspect the two artifacts through Files to manage them explicitly.

Filesystem finalization runs on the attempt's tracked blocking worker after
repair/extraction/script workers have joined. The original attempt custody
stays live. Generic legacy-enrollment custody checks remain unchanged.

## Qualification — require actual execution before deployment

| Gate | Required evidence |
|---|---|
| All-target compilation and Clippy | Run against the frozen reviewed candidate |
| Astra review of the complete candidate | Complete before final unit execution; independently review Astra-authored changes |
| Full workspace tests with required PAR2/7z tools and local-server access | Zero failed targets; report existing ignored fixtures explicitly |
| Full post-processing target in the normal startup CI lane | Execute complete pipeline and failure-regression targets |
| Required CI and merge | Qualify the current PR head and protected-base combination |
| Qualified rollout and observed runtime | Record exact build identity, readiness, restart count, and preserved rollback material |

Run the full workspace suite using the existing isolated build slot, outside
the sandbox that refuses loopback listeners. A passing command has zero failed
targets; existing ignored environment/stress tests remain reported explicitly.
The startup fast lane now runs the complete pipeline and failure-regression
targets, so the previously hidden failures cannot return behind one passing
filtered test.

Full qualification runs `cargo test --locked --workspace --exclude nzbd-cluster`
and then `cargo test --locked -p nzbd-cluster -- --test-threads=1`. Together
these commands include every workspace unit, integration and documentation
target. The cluster invocation matches `fast-check-rust`'s existing isolation
policy: independent fixtures take turns, while each fixture's actual workers,
Raft nodes, failover behavior, lease deadlines and integrity assertions retain
their original concurrency and checks.

The parallel full run at `c2d9358` showed two independent cluster authorities
demoting within 230 ms, an article refetch and a READY timeout; the focused
range scenario passed with all 96 single-fetch assertions. This supports
isolating time-compressed fixtures from shared-host scheduling and I/O
pressure, but does not identify the exact delayed renewal phase or establish
a production lease fix. Preserve those failed receipts as stress evidence.
Any recurrence in the isolated qualification remains a deployment blocker.

## Deployment — qualify the exact revision on nuc3

The actual service checkout is `/opt/noirr/runner`; the media inventory's
`/opt/noirr/nzbd` path does not exist. The tracked checkout was clean at
`8ce989bd`; untracked `config/` and `docker-compose.yml` must be preserved.
The running `nzbd` container is healthy. Its plain CLI version does not prove
which commit produced the image.

Use the existing media Ansible workflow scoped to Runner on nuc3, with an app
path override. Preserve the old image, source revision, configuration, and
active state before restart. Disable untracked cleanup and image pruning.
Inspect the live Compose build controls and forward `NZBD_GIT_DESCRIBE`
explicitly: generic `BUILD_REF` alone is not Runner's build argument. Verify
the running build identity, control readiness, queue recovery, and cleanup
behavior after rollout. Record actual rollout and rollback receipts outside
the compiler cache; do not report deployment before those checks succeed.
