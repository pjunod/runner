# File lifecycle operations

**Status:** implementation under final preparation · **Updated:** 2026-09-25

Runner records payload ownership separately from History. Removing a History
row does not abandon its files. Unknown folders are retained for review, and
failed or pending deletion preserves the retry record. See the
[delivery status](FILE_LIFECYCLE_STATUS.md) for review and test results.

## 1. Enable and inspect

Settings → Dev → **Enable file lifecycle automation** controls automatic failed
payload expiry. Enablement always accepts the operator's choice; readiness
information is advisory. Manual inspection, Keep and recovery remain available.
A specific destructive action still requires verified ownership and no active
writer, recovery claim or Keep hold.

New owned failures use seven days by default. Expiry requires both its wall
clock deadline and seven days of observed eligible daemon uptime. Downtime and
holds extend retention. Zero days means indefinitely. Removing Keep or applying
a changed policy starts a fresh period. Existing unknown folders never acquire
retroactive expiry merely because automation is enabled.

In **Files**, request a scan. Scans run as durable operations and start the
moment they are admitted (the 30-second maintenance tick is only the safety
net); a scan measures every folder it discovers, so a discovered row carries
its file count and size straight away. A row showing `—` has not been
measured — a walk failed and the reason sits on the row, or the folder was
recorded by an older build — and **inspect** walks it. A root that does not
exist yet (the failed directory before the first parked failure) is skipped,
not a scan failure. Open a folder's **details**; adopt unknown files only
after reviewing the file list. Adoption enables Keep. Newly added or replaced
content revokes ownership when inspected. A reviewed hold can be released
explicitly; active recovery claims cannot be released that way. The list is
filtered (**Needs review** = live rows that are unowned, held or in error),
searched, sorted and paged on the server; the counts on the filter chips
describe the whole inventory, not the current search.

**Preview retention for existing failures** shows up to 1,000 exact IDs and
revisions. Apply checks the whole selection in one transaction. A changed row
requires another preview. Kept, held, unknown and unchanged rows are excluded.
Repeat the preview to process another batch if necessary.

Manual Files deletion has an eight-second Undo window, and the deletion runs
as soon as the window closes. Native and compatibility
callers that request terminal deletion do not add that delay. An existing UI
Undo window is preserved. A pending operation is never reported as HTTP success;
retry the same request. Transient deletion failures retry after 1 minute,
5 minutes, 30 minutes and then 6 hours. Identity conflicts require review.

## 2. Recover media into Curator

1. Inspect and, if needed, adopt the source. Select regular media files.
2. Preview staging capacity, then stage the selection. Keep may remain enabled.
3. Mount only `<recovery-root>/published` in Curator as `/recovery:ro`.
   Never expose `.staging`, the entire processing directory or a library root
   through this mount. The default recovery root is `<main-dir>/recovery`.
4. Configure Curator's Dev settings with `/recovery`, Runner's exact published
   prefix and the same separate recovery credential configured in Runner.
   Normal Runner API authentication remains necessary too.
5. Curator Settings → Recovery selects the handoff, title, library copy, files
   and TV episode mappings. Inspect the destination preview and queue it.
6. Follow persisted activity, copied bytes and receipt delivery. Cancellation
   waits until the worker stops. Completed library files remain recorded.

Staging uses independent copied files, SHA-256 verification and an external
manifest. Digests prove equal bytes, not healthy media. Curator uses its bounded
native container parser; no external probe is launched for recovery. Recognized
but unverified media requires an explicit choice. Book recovery is outside this
initial media-recovery contract.

Peak storage may include the original, staging copy, library copy and an old
replacement. Runner requires full staging bytes plus 1 GiB free reserve. Curator
checks its library volume independently with the same reserve. Replacement
rollback links share the old inode on that volume until metadata commits.

Only a complete per-file receipt starts staging's 24-hour eligible-uptime
retention. Source retention restarts after receipt, respecting Keep and zero-day
policy. Unselected source files remain held. **Delete receipted source files**
removes only successfully imported/already-present files whose identities and
hashes still match; it has its own eight-second Undo window. Partial or silent
consumers never cause expiry. A missing heartbeat is diagnostic only.

## 3. Interrupted work

- Allocation intent precedes writer creation. Ambiguous allocation after a
  crash resumes without granting destructive ownership.
- Queue retirement waits for writer-stop acknowledgements. Startup reconciles
  interrupted retirement before resuming writers.
- Category moves and failed-file parking journal their source and destination.
  Publication refuses an existing destination. Cross-volume moves verify copies
  before exact-entry source cleanup. Incomplete scratch is retained for review.
- A fully journaled recovery publication can finish after a crash by checking
  directory identity, manifest, sizes and hashes. Incomplete staging is retained
  and exposed for explicit review; it is not erased by matching a scratch name.
- Curator journals placement before filesystem publication. Library metadata,
  quality/provenance, episode links and each file receipt commit together.
  Outbox delivery retries independently of the browser or original HTTP request.

Do not manually erase SQLite journals, sidecars, rollback files or a directory
with an active claim. A review state is a request to inspect the recorded paths
and actual filesystem, not evidence that the files are disposable.

## 4. Backup and restore

Stop Runner before taking this offline inventory snapshot:

```bash
nzbd artifacts-backup --state-dir /state --output /backups/lifecycle-20260925
```

The output directory must not exist. `artifacts.sqlite` is copied using SQLite
`VACUUM INTO`, including committed WAL data, and external allocation identities
are copied with it. `backup.json` describes a completed snapshot. This command
does not back up media volumes, queue snapshots or History; include those in the
same operational backup window separately. Keep incomplete backup directories
for inspection until their failure has been understood.

Restore the matching inventory and `artifact-identities` while Runner is stopped,
then quarantine before starting the daemon:

```bash
nzbd artifacts-restore --state-dir /state
```

Quarantine enables Keep and review holds on surviving records, resets retention
clocks and invalidates pending operations. Never copy an old database into a
running daemon. Never restore old metadata while allowing pre-restore consumers
to keep modifying media. Raw rollback without quarantine is unsupported.
Curator's database and library must likewise be restored as a coordinated set;
keep Runner sources held while reviewing any recovered import/outbox state.

## 5. Platform and cluster boundaries

Linux and macOS use descriptor-relative, no-follow access and exclusive
publication. Windows keeps ordinary queue operation and inspection; destructive
ownership is refused where equivalent deletion primitives are unavailable.
This is an operation-level inability to prove safe deletion, not an enable gate.

Each cluster node keeps its inventory in its local control directory. Imported
pre-existing worker payloads remain unowned. A remote writer lease must retire
before local queue deletion can quiesce writers. The initial operational rollout
is standalone: cluster enablement remains advisory, but it does not manufacture
cross-node deletion authority. Do not adopt a folder that another node can still
write. Shared-volume permission and device-identity behavior must be verified on
the deployment filesystem before using managed destructive operations there.

## 6. Diagnostics and limits

Runner `/metrics` includes `nzbd_artifacts`, `nzbd_artifact_operations` and
`nzbd_recoveries`, labeled only by state. Inventory events explain specific
operations. No metric label contains a path, job or recovery ID.

Lists are paged; file pages contain 200 entries. A manifest is bounded at 100,000
entries and depth 64; recovery selection is at most 1,000 files. Scans stop with
an explicit incomplete-result error after 2,000 directories. Terminal manifests
and detailed events compact after 90 days; minimal identity and idempotence
records remain. Files reported removed outside Runner are quiet tombstones.

During a filesystem outage, retry or review states remain visible. An unavailable
root is not proof that a payload is gone. Resolve mounts/permissions, then retry
or inspect the named operation. Keep is the immediate retention override.
