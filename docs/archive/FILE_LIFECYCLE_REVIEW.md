# File lifecycle review — adversarial findings and required corrections

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [FILE_LIFECYCLE_OPERATIONS.md](../FILE_LIFECYCLE_OPERATIONS.md).

**Status:** agent review plus Opus follow-up; full scope retained;
implementation unstarted ·
**Reviewed:** 2026-09-25 · **Method:** three independent agent reviews,
primary-agent source verification, then targeted agent re-review.

Companion to [FILE_LIFECYCLE_PLAN.md](FILE_LIFECYCLE_PLAN.md). This report
records the defects found in the original proposal, the changes made to the
plan, and the tests that must prove the implementation. Read this before
building to understand why the less obvious safeguards are required.
Sections 1–4 record the first agent-review round. Section 5 records the
subsequent Opus review, primary-agent code verification, and the user's
decision to retain the full robustness scope. Where contracts changed again,
§5 and the current plan supersede the earlier proposed remedy.

## 1. First-review verdict — the original proposal needed changes

The review found **two P1 and four P2 findings**. The original plan should
not have been implemented unchanged. The revised plan addresses the six
failure scenarios at the design level; it is ready for implementation
review, not production approval. No runtime code was changed, no retention
policy was enabled, and no live files were moved or deleted in this review.

The main correction is that filesystem ownership alone is insufficient.
Deletion also depends on current policy authority, valid time, and consumer
state. Import receipts require a durable library placement protocol before
they can authorize source cleanup.

## 2. Review scope and evidence

The independent agents reviewed these bounded areas:

| Reviewer | Area | Findings |
|---|---|---|
| `review_ownership` | Retention, deletion, crashes, backup restoration, clock behavior | R1, R2 |
| `review_handoff` | Runner–Curator claims, imports, receipts, partial completion, library races | R3, R4 |
| `review_scope` | Existing clients, configuration migration, downgrade, implementation boundaries | R5, R6 |

Agents read the plan and relevant local code without modifying files or
accessing live servers. The primary agent verified the cited code and
edited the plan. Reviewers then checked their corrections; this was a
targeted re-review, not a second exhaustive audit of every paragraph.

Original document SHA-256:

```text
71f243d9a70e0af00b162ba446af1ff6e27871ca6112e9f788d3b7d80dc5daca
```

Original line references below apply to that 1,014-line draft, not the
revised document. Source revisions were Runner
`f959cf922b47c581713df9401003c7c6f96cf5f7` and Curator
`0da1c755da44fdb0cd7bf615997016c8c096dc5c`. Source links name the functions
to re-verify if either repository moves.

## 3. Findings — failure scenario, correction, and proof

### 3.1 R1 · P1 — old backups can reverse Keep and recovery holds

**Original location:** plan §5.1, lines 194–199.

**Failure scenario:** Take an inventory backup while a payload has an
expiry deadline. Later, choose Keep or start recovery. Restore the old
backup after its original deadline. The directory, marker, contents, and
generation can all be unchanged, so filesystem reconciliation succeeds,
but the restored policy permits deleting a protected payload. Scanning
cannot reconstruct a policy decision lost with the newer database.

**Correction:** The revised §5.1 requires an explicit offline restore
procedure, fresh epoch, invalidation of restored deletion authority, and
review holds on all nonterminal artifacts/recoveries. Consumer claims must
be reconciled before releasing holds or assigning new consumers. An
independent authority checkpoint can detect some stale restores, but does
not make unannounced full-volume rollback safe. Disk reconciliation alone
never resumes a restored deadline.

**Required proof:** Back up before Keep and before a recovery claim, restore
after the old deadline, leave all disk identities unchanged, and verify no
deletion resumes. F1 and the storage test matrix now require these cases.

**Disposition:** Addressed in the plan; ownership reviewer confirmed the fix.

### 3.2 R2 · P2 — restarting with a wrong clock can shorten retention

**Original location:** plan §12, lines 927–932.

**Failure scenario:** Restart with a clock advanced several weeks. A fresh
monotonic baseline cannot detect that the persisted UTC deadline is now
falsely overdue. A completed filesystem scan would have allowed expiry.

The first correction gated only deadlines already overdue at startup. The
reviewer found a second case: advancing six days against a seven-day
deadline leaves it future-dated at boot, then deletes after one real day.

**Correction:** Revised §12 requires clock validation before any automatic
expiry after startup, including deadlines becoming due later. When time
cannot be established, automatic expiry stays held and the UI explains why.
An explicit trusted baseline/review can resolve the gate; filesystem scans
and persisted timestamps cannot substitute for clock validation.

**Required proof:** Restart with the clock advanced past the deadline and
just before it, without a synchronization signal. Both must remain held.
Also test normal time validation, runtime jumps, and backward changes.

**Disposition:** Corrected after the targeted second-pass counterexample;
ownership reviewer verified the final gate covers both startup cases.
Implementation must still exercise both.

### 3.3 R3 · P1 — a stale recovery preview can overwrite a new import

**Original location:** plan §8.3, lines 543–556.

**Failure scenario:** The user approves recovering an episode. While the
recovery waits or copies, ordinary acquisition upgrades that same episode.
The recovery then applies its old replacement decision and overwrites or
removes the newly imported library copy.

**Source evidence:** Curator's
`runImportTracked` (curator `internal/app/acquisition/importers.go`)
locks by download ID, not library target. Episode placement in
`import.go` (curator `internal/app/acquisition/import.go`) can bypass
ordinary non-upgrade rejection for manual imports, replace an existing
destination, and remove superseded files. A reservation on the recovery ID
does not serialize a different download for the same target.

**Correction:** Revised §8.3 binds the preview to item, copy, episode set,
library revision, file identities, quality policy, and destination paths.
Ordinary and recovery imports share a target coordinator. They reserve and
revalidate targets before publication/replacement; changes require a new
preview. Restart and cancellation preserve worker ownership until quiescent.

**Required proof:** Change the target through an ordinary import after
recovery preview, then attempt publication. The recovery must conflict
without overwriting or removing the new library content. F5 requires it.

**Disposition:** Addressed in the plan; handoff reviewer confirmed the fix.

### 3.4 R4 · P2 — an outbox does not cover publication before database commit

**Original location:** plan §8.3, lines 558–562; §10, line 766.

**Failure scenario:** Curator publishes the first selected file, then dies
before its library metadata and receipt-outbox transaction commits. There
is no completed per-file result to replay. Retrying can recopy/overwrite
the destination or classify it as an unresolved quality skip, violating the
plan's promise to resume only unfinished work.

**Source evidence:** Curator's
`placeContext` (curator `internal/app/acquisition/import.go`) returns
after rename without the required file/directory synchronization protocol.
Episode import separately removes old files, upserts the new file, and
updates episode links. An outbox added only at the end leaves those earlier
crash boundaries uncovered.

**Correction:** Revised §8.3 specifies a durable per-file placement intent,
checked copy/digest/sync, publication under target reservation, preserved
replacement backup, and one database transaction for required library
metadata, per-file completion, and outbox state. Restart reconciles an
already-published file against its pending intent before recopying or
emitting a receipt. The §10 failure table distinguishes publication from
database/outbox commit.

**Required proof:** Terminate after publication, before/between metadata
operations, and before outbox commit. Resume without duplicate placement,
lost old-library content, or a premature receipt. F5 requires these cases.

**Disposition:** Addressed in the plan; handoff reviewer confirmed the fix.

### 3.5 R5 · P2 — existing Curator treats asynchronous deletion as completion

**Original location:** plan §6.3, lines 384–389.

**Failure scenario:** Runner returns `202` for a deletion operation. Curator
marks the payload removed, then Runner's deletion fails or remains blocked.
Curator stops selecting the payload for cleanup retries, recreating the
unnoticed retained-file problem.

**Source evidence:** Curator's native
`Client.Remove` (curator `internal/adapters/nzbd/nzbd.go`) treats any
successful HTTP response as completion. Its
`removePayload` (curator `internal/app/acquisition/cleanup.go`) then
calls `MarkPayloadRemoved` and logs reclaimed bytes. Updating only Runner's
web/mobile callers would not fix this consumer.

**Correction:** Revised §6.3 reserves asynchronous admission for the new
artifact-operation API. Existing queue and History `delete-files` routes
return `2xx` only for verified completion. Pending work returns a retryable
non-success response and reuses the same durable operation across retries
and queue retirement. Terminal receipts provide idempotent success. Queue
writers and PP must be quiescent before file deletion.

**Required proof:** An unchanged Curator receives `503` for pending deletion
and retains its cleanup flag. After actual removal, retry succeeds and only
then changes the flag. Exercise both routes and transition between them.
This is an F3 obligation, not deferred to F5 recovery integration.

**Disposition:** Addressed in the plan; scope reviewer confirmed the fix.

### 3.6 R6 · P2 — disabling the feature does not make its config downgrade-safe

**Original location:** plan §10, lines 786–791; §13, lines 968–974.

**Failure scenario:** Disable artifact management and start an older Runner.
It rejects the new `[artifacts]` section or consumer-auth fields, so the
documented rollback cannot start the service. A saved-config mirror can
reintroduce the incompatible TOML even after editing the primary file.

**Source evidence:** Runner's
[`Config`](../../crates/nzbd-config/src/lib.rs) uses
`#[serde(deny_unknown_fields)]`. Unknown sections are rejected regardless of
their `mode` value.

**Correction:** Revised §10 requires a compatible configuration export or
restoration for the exact prior binary, including the mirror and new auth
fields, while preserving inventory and holds. The rollback process backs
up the new config for re-upgrade and quiesces both services first.

**Required proof:** Actually start the prior binary with prepared primary
configuration and mirror in a disposable fixture. New-version parsing or
setting `mode = "off"` is insufficient. F6 now requires this evidence.

**Disposition:** Addressed in the plan; scope reviewer confirmed the fix.

## 4. Remaining decisions and validation limits

No agent found a concrete reason to reject the explicitly stated
standalone-first boundary. Cluster mutation support remains deferred and
must not be advertised as delivered. The inventory, operation records, and
receipts add work, but the reviewers did not find a safe generic shortcut
that preserves the four approved capabilities and crash guarantees.

The plan still needs implementation choices reviewed as listed in §14,
including supported filesystems/platforms, clock validation availability,
consumer credentials, and the staged-copy storage cost. Addressing a prose
finding does not demonstrate that its eventual code is correct.

Only documentation checks were run: local link resolution, balanced code
fences, heading structure, whitespace, and diff review. No new runtime tests
exist yet. Before release, implement and run the failure fixtures attached
to each finding and the plan's broader compatibility/crash test matrix.

Further review should prioritize R1/R3 because they can permit deleting
protected source or replacing newly imported library content. R2/R4/R5/R6
must also be closed with executable evidence before the relevant milestone
is marked complete.

## 5. Opus follow-up — correct the mechanics, retain the improvements

The user supplied an Opus review on 2026-09-25 and then explicitly reaffirmed
that this is a fix **plus robustness improvements**, with all proposed
capabilities retained. Opus did not have repository access and marked its
code claims unverified. The primary agent checked the relevant paths locally
before accepting, qualifying, or declining its recommendations.

### 5.1 Disposition of the six blockers

| Opus item | Disposition | Change or rationale |
|---|---|---|
| O1 — RCA omitted; fix the History record-loss bug first | Accept the fix priority; qualify attribution | Add F0 before F1–F6. Current code proves the defect, but not that it caused the July folders. An internal `.tmp` file is not the helper's sibling `.pp-move.<tag>` directory |
| O2 — payload markers can prevent consumer cleanup | Accept | Put identity sidecars in Runner state, outside all payload/category trees; test successful import leaves no control file behind |
| O3 — Docker startup clock gate prevents unattended expiry | Accept with explicit timing semantics | Replace the clock-service/operator gate with wall deadline AND persisted observed eligible uptime; offline/crash time never advances retention, so expiry may be later |
| O4 — Undo consumes the legacy API wait; verify retry | Accept; code check found an additional bypass | UI explicitly requests eight seconds; legacy/API uses zero grace. Curator must suppress native Runner direct-disk fallback on pending/refused deletion, not merely return an HTTP error |
| O5 — inventory failure blocks downloading; off is misleading | Retain the deliberate robustness dependency; clarify it | Document new-allocation admission pause/error and affected finalization behavior; expose only observe/manage plus a separate scan switch. No mode falsely promises to disable core ownership |
| O6 — remove Curator transactional placement/shared target work | Do not reduce scope | User reaffirmed robustness. Current code still has publication-before-metadata and conflicting-target gaps; a completion receipt alone does not close them. Keep the work as explicit Curator milestones |

F0 is a small independently reviewable bug fix, not a substitute for the
full inventory and recovery work. F1–F6 remain required to complete the
approved feature set. None was implemented by this documentation revision.

### 5.2 Retention now works without a container clock service

The first agent review correctly caught premature expiry after a clock
change, but its remedy introduced an availability problem. The new plan
§6.1 accumulates a persisted lower bound on eligible monotonic runtime and
requires both that bound and the UTC deadline before automatic deletion.
Uncheckpointed crash time is lost rather than double-counted; restart does
not infer elapsed time from a potentially wrong wall clock. Keep/recovery
holds suspend the policy, and new policy periods reset the budget.

This deliberately means at least seven observed running days, not a promise
of deletion exactly seven calendar days later. The UI shows an earliest
estimate and explains downtime/holds. The same mechanism governs automatic
staging cleanup. There is no recurring operator clock approval or required
NTP/systemd signal inside Docker. The plan includes unattended restart and
clock-shift fixtures; these tests have not yet been implemented.

### 5.3 Additional verified gap — Curator bypasses deletion refusal on disk

Our earlier R5 correction was incomplete. Checking only
`Client.do` / `Remove` (curator `internal/adapters/nzbd/nzbd.go`) showed
that `503` becomes an error, but did not establish end-to-end retry safety.
Reading the full
`cleanup.go` (curator `internal/app/acquisition/cleanup.go`) now shows:

```text
Runner returns pending/refused/error
        │
        v
removePayload returns false
        │
        v
CleanupPayloads / cleanupAfterImport calls removeImportedDir
        │
        v
Curator may remove the source directly, outside Runner's holds
```

`removeImportedDir` guards library roots and checks `RemoveCompleted`, but
does not know Runner artifact holds. The configured completed-download
mount can be writable, so this is an actual ownership-protocol bypass.
The hourly sweep only retries if the fallback has not already removed and
marked the payload. The native adapter itself has no HTTP `503` retry loop;
it also tries History after arbitrary queue errors, not just a missing job.

Revised plan §6.3 and F3 require typed pending/refused outcomes, no local
fallback for native Runner-managed payloads, queue-to-History fallback only
on `404`, and a later sweep retry against the same durable operation.
Recovery paths must never invoke generic source cleanup. This Curator fix
precedes enabling Runner management where Curator can bypass holds.

Required test: inject `503`, `409`, auth failure, and unreachable Runner;
assert the payload flag is unchanged and the local deletion function is
never called. Then complete the Runner operation and verify the next sweep
records removal only after terminal success. The original “unchanged
Curator retries safely” acceptance statement has been removed.

### 5.4 Disposition of the significant recommendations

| Recommendation | Disposition |
|---|---|
| Reduce the potential three-copy space cost with a read-only failed mount | Document the real per-volume peak and admission checks; retain staged snapshots for all recovery sources. Direct-source transport is a future optimization requiring equivalent immutability/version guarantees |
| Move media probing to Curator | Accept. Source shows a native MKV/MP4 probe with bounded reads; the distroless image does not normally ship ffprobe. Reuse the native probe and optional existing fallback; Runner gains no executable dependency |
| Put recovery outside the completed tree | Accept. Default is `<main_dir>/recovery`; only `published/` is mounted read-only in Curator. Ordinary completed scanners cannot discover either published recoveries or private staging |
| Simplify restoration | Keep restore review holds and invalidated authority; remove the external checkpoint mechanism. A supported restore sets a persistent quarantine flag and resets elapsed retention budgets; raw rollback cannot claim automatic safety |
| Account for successful-file tombstone growth | Accept. Quietly classify normal completed-source disappearance; compact bulky file/audit/sidecar data after 90 days while keeping minimal idempotency identity. Add 365k/1M-artifact size measurements and published estimates |
| Split the initiative to drop/defer broad improvements | Decline as a scope change after the user's clarification. Keep small PR/milestone boundaries, including F0, while preserving every approved capability and its robustness work |

The read-only recovery mount reduces exposure to the fixed published
snapshot, rather than granting Curator access to Runner state or all sources.
It does not eliminate the storage cost: staging and library placement may
each require a full additional copy, and replacement backup can add more.
Both sides must report/check capacity on their own destination volumes.

### 5.5 What remains to prove

The full plan is retained, with explicit admission-availability and
space-versus-snapshot trade-offs. No platform signal or missing ffprobe may
silently make the core retention feature unusable. No scope objection is
being used to remove the user's requested robustness improvements.

The follow-up edits were checked against source and for document consistency
by the primary agent. The earlier agent re-review does not constitute fresh
independent approval of these new contracts. Runtime implementation and all
acceptance tests remain outstanding, especially dual-clock accounting,
Curator fallback suppression, sidecar identity/moves, and transactional
library placement. This is a revised implementation specification, not a
claim that the production problem has already been fixed in code.
