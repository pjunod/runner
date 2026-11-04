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
