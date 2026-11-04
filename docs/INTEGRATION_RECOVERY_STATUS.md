# Integration qualification — verified fixtures and safe script results

**Status:** candidate implemented; Astra review and full qualification pending ·
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

| Gate | State |
|---|---|
| Focused all-target compilation and Clippy | Passed before final review |
| Astra review of the complete candidate | Pending |
| Full workspace tests with required PAR2/7z tools and local-server access | Pending |
| Full post-processing target in the normal startup CI lane | Added; execution pending |
| Required CI and merge | Pending |
| Qualified rollout and observed runtime | Pending |

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
