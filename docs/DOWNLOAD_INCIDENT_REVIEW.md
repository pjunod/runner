# Download incident — root causes, proposed fixes, and review status

Companion to [CONFIGURATION.md](CONFIGURATION.md): this document explains the
October 1 Runner incident and the changes needed to correct it. It incorporates
[Opus review v1](DOWNLOAD_INCIDENT_OPUS_REVIEW.md) and
[Opus review v2](DOWNLOAD_INCIDENT_OPUS_REVIEW_V2.md). Earlier sections 1–13
are preserved only in the [superseded investigation](DOWNLOAD_INCIDENT_INVESTIGATION_ARCHIVE.md).
This document replaces that account; readers need not resolve contradictory
proposals across old sections.

**Status, October 1, 2026:** local candidate under review, not ready to deploy.
Several fixes are implemented and locally tested; directory publication,
known-extension RAR stem recovery, PP-hold recovery, and relocation abandonment
remain incomplete. No production deployment, configuration change, payload
rename, or retry of a live failed job has been performed in this follow-up.

## 1. Evidence and corrections

The user's screenshots and reports establish stalled downloads, archive-only
folders under the completed path, extensionless media rejected by Curator,
unreleasable review holds, and a cancelled handoff with no clear dismissal.
The two independent reviews add production logs, filesystem probes, archive
headers, and PAR2 metadata. Production observations below are attributed to
those reviews; local source inspection and tests validate candidate behavior,
not the current fleet.

| Item | Evidence / interpretation |
|---|---|
| Deployed source | Review v1 identifies `9d3288b` as the September 30 deployment; relevant local baseline is `b4ed40e`. Earlier failed RAR jobs predate that deployment. |
| Working filesystem | Container `/processing/` maps to host `/mnt/processing`, GlusterFS FUSE. It is used for state and other configured roles. The claim that Runner does not use it was wrong. |
| Completed filesystem | Container `/working` maps to host `/mnt/qnap/working`, NFS 4.1. Completed setting is `/working/monarr/completed`. |
| Filesystem primitives | Reviews report `RENAME_NOREPLACE` returns `EINVAL` on both mounts; hard links and exclusive `mkdir` work and refuse existing targets. |
| Path configuration | `main_dir=/processing/`, `dest_dir=/working/monarr/completed`, `inter_dir` empty. Empty `InterDir` means download to `DestDir` under the NZBGet contract; do not invent a `MainDir` fallback. |
| Job 1703 | Six directly posted Matroska files plus an extensionless PAR2 index; no unpack stage. The index has exact episode filenames. Calling these extracted media was incorrect. |
| Job 1704 | Downloaded at 21:39–21:40 UTC, then failed at 21:40:52 UTC during extraction fallback. Distinct random stems retain `.rar`/`.rNN` extensions. |

The unexpected event that woke scheduling around 21:39 UTC remains unknown.
The review's suggested operator action is a hypothesis, not an established fact.

## 2. Root causes and architectural owners

### 2.1 Filesystem capability failure plus swallowed naming errors (D1/D3)

The shared exclusive rename primitive assumes filesystem support for flagged
rename. Both payload mounts reject it. This breaks file renaming, parking,
recovery publication, and directory transforms.

Post-processing previously swallowed required rename failures. PAR2 discovery
then looked only for `.par2`; archive discovery depended on recognizable
extensions. Consequently, unsuccessful deobfuscation could bypass both repair
and extraction and still report SUCCESS. Fix the filesystem primitive and
error propagation; a final extension-only archive check cannot repair this
causal chain.

Job 1703 demonstrates it exactly: renaming its magic-detected PAR2 index fails,
the index is missed, exact episode names are not recovered, and SUCCESS is
reported. Review v2 matched the six media files to PAR2 names using prefix
hash plus size. Runner's existing exact-name recovery additionally checks the
full MD5; production full-digest matching has not been repeated here.

### 2.2 Allocation failure remains eligible and stops leasing (D4/D5)

Pausing 1703 freed its slot. Scheduler selection reached 1704, whose pathname
collided with retained older data. `grant_work` broke on allocation failure
without removing that job from eligibility. This stopped other work and left
sleeping workers waiting for an epoch change. Deleting the artifact did not
create that queue transition.

The owner must durably hold only the refused job, continue selecting others,
and offer a revision-checked explicit retry. URL admission must use the same
fresh allocation rules as direct NZB admission. No periodic rescan or inventory
watchdog is required.

### 2.3 Archive order and archive membership are separate (D2)

For jobs 1691/1692/1695/1696, lexical sorting of obfuscated RAR4 volumes
assigned the wrong positions. Review v1 found a head renamed to `part56` and
a final volume renamed to `part01`. Use checked archive-header ordering;
unknown order must not be invented.

Job 1704 is a different case: known `.rar`/`.rNN` extensions have unrelated
stems. The extractor looks for a shared stem and under-delivers. Its retry
then attempts an unsupported directory rename. Fixing either problem alone
does not repair the whole job. Number continuity alone does not prove that
volumes from different archives belong together; normalization also needs
archive/file-header evidence and ambiguity rejection.

### 2.4 Deletion did not revoke relocation authority

Job 1688's deletion succeeded, and another allocation reused the pathname.
An unfinished relocation still held the old source snapshot. Startup failure
handling wrote `retained / review` back over the deleted record, reviving
metadata whose inode identity no longer matched the current directory.
This was resurrection of an old database record, not deleted bytes returning.

Deletion must revoke pending relocation authority for the same generation.
Every relocation completion/failure must validate that its source generation
still owns the transition. Artifact IDs and pathnames alone are insufficient,
especially for deterministic scratch IDs.

### 2.5 Holds and terminal handoffs lack complete operator transitions

A PP exception becomes `unknown / review / paused`. Generic resume refuses it,
and the existing post-restart endpoint also rejects paused jobs. Job 1704
therefore has no effective PP retry. A held live relocation likewise needs
explicit abandonment; releasing its artifact hold alone leaves an operation
that can recreate the hold at startup.

The cancelled recovery handoff is terminal history, not an active transfer.
The UI always requested terminal entries and offered no way to hide them.
Its display issue is distinct from the filesystem failure that cancelled it.

## 3. Candidate implementation and finding coverage

“Implemented” means local uncommitted code; it does not imply deployment or
that the remaining incident is solved.

| Review finding | Current disposition | Remaining work |
|---|---|---|
| D1 regular-file rename | Implemented: unsupported flagged rename falls back to descriptor-relative hard-link creation, fsync, identity checks, source unlink. | Verify on actual Linux NFS/Gluster with isolated fixtures; review caller durability. |
| v2 same-inode replay | Implemented: an existing target with the same regular-file identity permits finishing source unlink. Equal bytes on another inode fail closed. Same source/destination entry is a no-op. | Broader crash-boundary injection and mount-level diagnostics. |
| D1 directory publication | Proposed design below; unsupported directory rename still fails closed. | Journaled incremental publication and consumer-visibility decision. |
| D3 failed extraction retry | Implemented: keep failed output in place, exclusively create a fresh retry directory, return actual output path in `ExtractOutcome`, commit that path. | Real production-like multipart fixture validation. |
| Required PP rename errors | Implemented: PAR/archive/final rename failures propagate instead of producing silent SUCCESS. | Extend fault coverage across every rename caller. |
| v2 PAR2 discovery | Implemented: detect index packet magic independently of extension; load/parse errors propagate. | Full incident-shaped six-file recovery regression. |
| D4 held allocation | Implemented: `allocation` cause, normal selection continues, public resume dispatches existing revision-checked release; next grant revalidates allocation. | Persistence-failure and restart coverage of the new cause. |
| D5 URL allocation parity | Implemented shared fresh-allocation helper for URL and direct admission. | Keep path/settings behavior unchanged. |
| D2 hidden RAR4 order | Implemented CRC-checked header walk and ENDARC volume index; contiguous order validation before volume renames. | Real multipart extraction fixture; known-extension normalization below. |
| v2 known-extension mismatched RAR stems | Accepted; not implemented. | Prove set membership, normalize stems, reject ambiguity, test real old-style volumes. |
| v2 lexical season numbering | Removed. Unit and pipeline tests require obfuscated packs to keep their names absent evidence. | No replacement guessing heuristic. |
| New deletion generation binding | Implemented for request, execution, relocation cancellation, and reconciliation. | Review explicit cancellation/abandon flow. |
| v2 legacy deletion proof | Implemented stricter form: matching uncommitted relocation must predate the delete request, not merely success. | Production read-only proof validation; ambiguous legacy entries stay for review. |
| PP exception retry | Accepted; not implemented. Existing `unknown` holds remain non-resumable. | Revision-checked PP retry with custody validation and cancellation fencing. |
| Relocation abandonment | Accepted; not implemented. | Operation/generation-scoped transition with source and scratch disposition. |
| Select all | Implemented across the inspected file manifest, with revision consistency and selected count. | Browser verification with a multi-page manifest. |
| Cancelled handoff display | Implemented default active-only query and opt-in completed/cancelled history. | Browser verification. |
| D6 explicit `inter_dir` | Accepted; deferred until publication and script-path contract are corrected. | Wire configured root without changing empty-value semantics. |
| Content extension detection / ffprobe | Separate follow-up proposal, not implemented and not needed to establish 1703's cause. | See section 6. |

The previous owner-only retry helper was removed: it was unreachable through
the public `resume_job` hold filter. Both layers now use the existing release
protocol. Capacity/quota resume retains its health probe; allocation resume
cannot require a payload that allocation has not yet created. Custody/identity
holds remain excluded. Writer retirement remains a transient refusal rather
than an operator allocation hold.

## 4. Proposed directory publication contract

Keep `rename_exclusive` atomic for supported directory renames. Do not quietly
replace that contract with a recursive copy/link routine. Add an explicit
journaled publication operation for filesystems lacking atomic no-replace
directory rename, using the existing inventory lifecycle rather than a watchdog.

1. Persist source generation, verified manifest, scratch identity, target path,
   and intended publication mode before exposing the destination.
2. Prepare and verify scratch on the destination filesystem. Hard links cannot
   cross mounts; cross-filesystem work must first copy into that scratch.
3. Exclusively create target directories and record their operation identities.
   An existing unrelated directory is a conflict even when empty.
4. Link each verified regular file into its target using no-replace semantics.
   On recovery, a matching inode means that link completed; a missing target
   permits retry; a different object requires review.
5. Fsync bottom-up and verify the complete destination manifest before marking
   publication committed. Retain source/scratch until durable commit and cleanup
   authorization, so partial publication cannot discard the recovery source.
6. Reconcile cleanup through the same operation. Do not infer ownership merely
   from an operation marker found in an otherwise unverified path.

The creation-to-journal crash boundary also needs a rule: a directory created
but not durably bound to the operation must be left for review, never adopted
on pathname alone. That conservative outcome is safe even if not automatically
recoverable. Tests must cover every boundary and foreign replacement of empty
and populated directories, not just completed happy-path copies.

**Decision still required:** incremental target visibility is acceptable only
if all import consumers obey the completion/commit boundary. Review v2 proposes
this trade-off; this follow-up has not verified Curator's scans or other watchers.
A filename-independent watcher could see a partial directory before SUCCESS.
Preserving no-overwrite does not by itself preserve atomic visibility. Do not
ship this publication mode until consumer behavior or the new visibility
contract is explicitly settled. Reservation plus ordinary rename is rejected
as an equivalent strict no-replace primitive.

## 5. Recovery transitions still to implement

### PP exceptions

Use the existing post-restart action and queue owner, not a background retry.
A request must bind the observed hold revision and generation, verify retained
payload custody, and fence/cancel any active attempt before requeueing from a
safe restart point. A fresh workspace must be allocated for a new extraction
attempt. A failed admission returns a specific error and preserves the hold.
A successful transition persists before waking PP. Identity, destructive
cleanup, and uncertain script execution holds must not become generic resume.

For the existing 1704 `unknown` hold, recovery must inspect its saved stage and
retained operation before admitting a retry. Simply making every `unknown`
hold resumable would weaken unrelated safeguards. Test the public REST action,
not just an internal owner method.

### Relocation abandonment and legacy deletion

Abandonment must name the operation, source generation, and observed revision;
validate whether source, scratch, or target publication is still owned; then
cancel only that operation durably. Keep residual data inventoried with an
explicit disposition. A conflicting identity remains a conflict. Abandonment
must not accidentally commit a partial move, delete a replacement, or allow
another startup to reapply the cancelled operation's hold.

For legacy successful deletes lacking a generation, the candidate accepts only
an unfinished relocation of the same artifact and current generation whose
creation timestamp is strictly earlier than the delete request's creation.
Generation changes on committed relocation/new allocation; the old snapshot
supplies evidence independent of the current pathname. Scratch IDs without
such a relocation do not qualify. Equal timestamps, missing proof, completed
moves, or different generations are left alone. Legacy pending deletes without
a generation are not executed automatically; a newly authorized request must
bind current identity. This rule repairs metadata only; it never accesses or
deletes whatever now occupies the deleted source's pathname.

## 6. Naming responsibility and optional media probing

Runner owns exact download filename recovery and usable output types. Curator
owns library matching and import. For 1703, use the existing pre-extraction
PAR2 mapping; there is no demonstrated need for a post-extraction pass or
ffprobe. Six raw Matroska files have exact episode names in the index. Sorting
the obfuscated filenames would assign all six episodes incorrectly, according
to review v2's mapping.

For other posts without exact mappings, signature-based extension recovery is
a useful separate improvement. A post-extraction PAR2 pass can recover metadata
bundled inside archives, but this incident does not establish that requirement.

The user's `ffprobe` proposal remains useful for optional container/stream
inspection and embedded title/tag evidence. [FFmpeg documents these outputs](https://ffmpeg.org/ffprobe.html).
Use structured output, existing subprocess limits, and validated local-file
access. Embedded titles can be missing or wrong; they must not override an exact
content-to-name mapping or manufacture episode order. A successful probe does
not prove full-file integrity. This proposal is deferred rather than silently
removed, and is not part of the demonstrated repair for 1703.

## 7. Validation and release boundary

Checks on this candidate:

| Check | Result |
|---|---|
| `cargo test -p nzbd-state -p nzbd-engine -p nzbd-post` | 418 passed, 2 existing performance tests ignored; doc tests passed. |
| `cargo clippy -p nzbd-state -p nzbd-engine -p nzbd-post --all-targets -- -D warnings` | Passed. |
| `make ui-test` | Boot and DOM harnesses passed; terminal-history query coverage added. |
| `git diff --check` | Passed. |

Tests cover unsupported regular-file rename, same-inode crash replay,
different-inode conflicts, conservative directory refusal, allocation release
and re-hold, public resume versus identity holds, PAR2 discovery after index
names are obfuscated, preserved season-pack names, fresh extraction retry
outputs, generation reuse, and the narrowly proven legacy resurrection.
The initial sandboxed engine run could not bind three local socket fixtures;
the complete rerun outside the sandbox passed. These are local results.

Local tests do not establish production filesystem behavior: the current
regular-file fallback tests inject unsupported-syscall errors on macOS.
Archive header fixtures are synthetic; a real multipart extraction fixture
remains a release requirement for the archive ordering changes.

Do not deploy this working tree as a completed incident fix. Before release:

- Finish directory publication with an explicit visibility contract.
- Finish known-extension RAR recovery, PP-hold retry, and relocation abandonment.
- Exercise interruption, conflicting identity, duplicate-content/different-inode,
  absent header evidence, and real multipart extraction cases.
- Verify existing path settings and script directory behavior.
- Review the reconciled candidate and decide separately whether to retry or
  repair live jobs 1699, 1700, 1702, 1703, and 1704. No live repair was performed.

No mount-specific paths, periodic retry watchdogs, or silent force-adoption
exceptions belong in these fixes.
