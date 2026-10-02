# Download incident — root causes, proposed fixes, and review status

Companion to [CONFIGURATION.md](CONFIGURATION.md): this document explains the
October 1 Runner incident and the changes needed to correct it. It incorporates
[Opus review v1](DOWNLOAD_INCIDENT_OPUS_REVIEW.md) and
[Opus review v2](DOWNLOAD_INCIDENT_OPUS_REVIEW_V2.md). Earlier sections 1–13
are preserved only in the [superseded investigation](DOWNLOAD_INCIDENT_INVESTIGATION_ARCHIVE.md).
This document replaces that account; readers need not resolve contradictory
proposals across old sections.

**Status, October 1, 2026:** implementation complete in draft
[PR #247](https://github.com/pjunod/runner/pull/247), awaiting the final adversarial
review and test pass. See [the status page](DOWNLOAD_INCIDENT_STATUS.md) for the
validation ledger and merge result. No production deployment, configuration
change, payload rename, or live retry was performed in this follow-up.

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

## 3. Implementation and review finding coverage

Both Opus reviews are incorporated. Implementation is in the combined PR;
compilation is verified, while final regression execution follows adversarial
review. Historical test counts from the earlier candidate are not evidence
for this version.

| Finding | Implemented resolution |
|---|---|
| D1 file rename / same-inode replay | Anchored no-replace hard-link fallback on unsupported flagged rename; identity checks, fsync, and source unlink. Same-inode replay completes an interrupted rename; equal bytes on a different inode remain a conflict. |
| D1 directory publication | Separate journaled publication operation, with exclusive mkdir and links when atomic rename is unsupported. Cross-filesystem relocation first makes a verified destination-local copy. |
| D3 extraction retries | Each attempt gets a fresh directory; failed output is retained. The extractor returns its actual successful output directory to the commit path. |
| Swallowed naming failures | Required renames propagate errors. Filename changes use the existing custody journal, including PAR index, archive and final media names. |
| D2 hidden RAR4 / v2 unrelated known stems | CRC-checked headers establish order, split-file continuity establishes membership, and archive flags select old/new naming. Missing, mixed or ambiguous sets fail without guessed ordering. Existing correctly named sets stay intact. |
| v2 hidden PAR2 / raw media | Index discovery reads magic independently of extension. Prefix digest narrows candidates; length and full MD5 establish exact names. A six-file regression reverses lexical order. |
| v2 lexical episode numbering | Removed. Multi-file packs need per-file evidence. Supported container headers can restore a missing extension without assigning an episode. |
| D4 scheduler / D5 URL admission | Allocation conflicts durably hold the refused job while other jobs remain eligible. Explicit revision-checked retry wakes scheduling. URL and direct admission share fresh-path allocation. |
| Deleted artifact resurrection | Delete requests bind generation; deletion revokes relocation authority. Move and transform replay cannot restore authority over a terminal or replaced generation. Legacy repair requires independent older matching-move evidence. |
| PP exception retry | Existing PP restart action admits a quiescent, revision-checked known PP failure after custody validation. Identity, deletion, unresolved relocation and uncertain script execution remain explicit conflicts. |
| Relocation abandonment | UI names the pending operation. Queue owner checks quiescence; inventory checks revision, generation and source manifest before cancelling it. Original and residual copies retain explicit custody. Verified publication must reconcile, not be abandoned. |
| Select all / cancelled recovery | Manifest-wide select-all checks revision across pages; selected count and clear action. Terminal handoffs are opt-in history. |
| D6 configured intermediate root | Explicit nonempty `inter_dir` drives download allocation; `dest_dir` or category destination receives successful publication before scripts. Empty intermediate retains the established destination behavior. Recorded custody locates existing payloads across settings changes. |

No mount-specific paths, new feature flags, or periodic retry watchdogs were
added. Existing operation reconciliation owns interrupted transitions.

## 4. Directory publication contract

Keep `rename_exclusive` atomic for supported directory renames. Do not quietly
replace that contract with a recursive copy/link routine. The implementation adds an explicit
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

**Visibility decision:** completion is the import boundary. Fallback publication
may expose entries incrementally; Runner reports success only after verifying
and syncing the full destination and committing custody. A failed final move
now stops PP instead of merely logging an error and continuing to SUCCESS.

Read-only verification of Curator main at `08af1064f9c617fa35387cadab4bdd3d2f91446d`
confirmed that its [NZBGet adapter](https://github.com/pjunod/curator/blob/08af1064f9c617fa35387cadab4bdd3d2f91446d/internal/adapters/nzbget/nzbget.go)
and [Runner adapter](https://github.com/pjunod/curator/blob/08af1064f9c617fa35387cadab4bdd3d2f91446d/internal/adapters/nzbd/nzbd.go)
map successful history to completed status. Its
[event adapter](https://github.com/pjunod/curator/blob/08af1064f9c617fa35387cadab4bdd3d2f91446d/internal/adapters/nzbd/events.go)
reserves completion for PP completion, and the
[acquisition transition](https://github.com/pjunod/curator/blob/08af1064f9c617fa35387cadab4bdd3d2f91446d/internal/app/acquisition/acquisition.go)
requires a completed status with a nonempty payload path before automatic import.
This verifies source behavior, not the deployed Curator revision. Manual imports
or third-party watchers of directory existence do not provide that contract.
The empty-intermediate configuration already exposes downloads within DestDir;
operators needing a distinct download area use the existing InterDir setting.

## 5. Recovery transitions

### PP exceptions

Use the existing post-restart action and queue owner, not a background retry.
A request must bind the observed hold revision and generation, verify retained
payload custody, and fence/cancel any active attempt before requeueing from a
safe restart point. A fresh workspace must be allocated for a new extraction
attempt. A failed admission returns a specific error and preserves the hold.
A successful transition persists before waking PP. Identity, destructive
cleanup, and uncertain script execution holds must not become generic resume.

The existing restart route now inspects 1704-style `unknown` holds by saved
stage. It admits only a beginning-of-PP retry, with no active attempt. A manager
integration regression covers the public restart handle; the owner regression
covers stale revision and uncertain script refusal. It does not automatically
retry live jobs.

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

For posts without exact mappings, bounded container-header parsing restores
missing Matroska, WebM, MP4/M4A and AVI extensions. It preserves basenames and
never overrides exact PAR evidence. This establishes file type, not integrity
or episode identity. Unknown headers remain unchanged.

The user's `ffprobe` proposal remains useful for optional stream validation and
embedded title/tag evidence. [FFmpeg documents those outputs](https://ffmpeg.org/ffprobe.html).
Decision: do not add an external probe dependency to this repair. The observed
six-file incident has exact PAR evidence; supported extension recovery uses
container headers. Embedded titles can be absent or incorrect and must never
override exact mappings. Full stream verification remains a distinct enhancement.

RAR parsing follows the vendor's
[RAR4 format note](https://sources.debian.org/src/rar/2:3.9.3-1/technote.txt/),
[UnRAR header definitions](https://github.com/pmachapman/unrar/blob/master/headers.hpp)
and [RAR5 format note](https://www.rarlab.com/technote.htm).
Obfuscated multipart RAR5 without exact names currently fails explicitly because
volume numbers alone cannot prove membership. Correctly named RAR5 sets retain
the normal extractor path.

## 7. Validation and release boundary

The whole workspace and all test targets compile. Final unit/regression tests
are deliberately deferred until the combined adversarial review has completed,
per the requested CI/CD workflow. Results belong in the
[validation ledger](DOWNLOAD_INCIDENT_STATUS.md); earlier candidate counts must
not be presented as a pass for this branch.

Regression coverage includes unsupported file and directory rename, replay,
foreign directory/file collisions, generation reuse, legacy deletion proof,
operation abandonment, recovery copy inode independence, PP retry and uncertain
script refusal, configured publication success/failure, manifest-wide selection,
hidden PAR mapping, multipart membership refusal, and a complete stored RAR4
split-file extraction through the independent 7-Zip tool.

Tests injecting unsupported rename on a local filesystem do not verify NFS or
Gluster durability. The reviewers' mount probes establish primitive capability;
production rollout and existing-job recovery remain separate from merging this
PR. No live data repair, filesystem force-adoption, or configuration change is
included. Existing held jobs may need the explicit retry or abandonment action;
a changed identity remains for investigation rather than being force-released.
