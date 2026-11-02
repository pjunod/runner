# Startup runtime recovery — implementation and merge status

**Owner:** Codex (`codex/startup-runtime-recovery`) · **Updated:** 2026-11-01
**State:** implementation and Astra corrections complete · **Live validation/merge status and owner:** [PR #262](https://github.com/pjunod/runner/pull/262)
**Base:** `368b500` (fresh remote clone; includes held-job recovery updates)

Companion to [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md)
(reviewed design) and [STARTUP_VERIFICATION_REVIEW.md](STARTUP_VERIFICATION_REVIEW.md)
(review history). This is the implementation ledger. The linked PR body is the live status page: Codex updates its check results, remaining work and merge state without creating source-only commits that restart passing CI lanes.

| Workstream | Owner | Implementation | Validation/merge state |
|---|---|---|---|
| Owner, provenance, bounded verification and persistence | Codex / Sol / Luna | Complete | Live PR checks |
| Writer safety and PP/torrent admission | Sol | Complete | Live PR checks |
| Persistent secure control and shutdown | Sol | Complete | Live PR checks |
| Deployment templates, operator docs and UI 503 retention | Luna | Complete | Live PR checks |
| Enrollment and startup fast-lane wiring | Luna / Codex | Complete | Live PR checks |
| Combined adversarial implementation review | Astra | Complete; R1–R10 corrected | Original report linked below |
| Fast lane, merge and owned-clone cleanup | Codex | Authorized after corrections | Live PR body and checks |

## Validation policy

Astra reviewed candidate `5c7b80b` against base `368b500` and required changes.
The integrated corrections pass `cargo check --workspace --all-targets --locked`.
No tests ran before the review corrections were complete. The affected fast lane
runs on the corrected merge candidate; passing checks are not repeated unless
later changes invalidate them. Failed checks are corrected and rerun. The PR's
Checks tab and owner-maintained body record actual results and merge status.

## Astra findings — correction and validation disposition

| Finding | Correction | Disposition |
|---|---|---|
| R1 · Secure startup fallback | Retain established secure identity across missing config/mirror cases; disable TLS/auth only for proven first install. | Implemented; covered by affected lane |
| R2 · Deferred PP admission | Scope admitted updates to their lifetime and cancel external replacements before awaiting custody. | Implemented; covered by affected lane |
| R3 · Durable PP replacement | Replace a job with one acknowledged document, avoiding a durable remove/insert gap. | Implemented; covered by affected lane |
| R4 · Delegated journal ownership | Fence delegation and compact only the exact journal incarnation/cutoff covered by its snapshot. | Implemented; covered by affected lane |
| R5 · Recovery startup latency | Load metadata first, then reconcile lifecycle state in bounded root-scoped workers. | Implemented; covered by affected lane |
| R6 · Inventory lock scope | Separate bounded payload I/O from metadata commits and reserve exact artifact generations during inspection. | Implemented; root isolation regressions in affected lane |
| R7 · Writer fairness | Skip pending allocations and keep selecting other eligible writers. | Implemented; covered by affected lane |
| R8 · Replicated authority adoption | Reclassify recovered metadata, rebuild allocation caches, and return durable adoption failures. | Implemented; covered by affected lane |
| R9 · Resume persistence fence | Fence dispatch by the exact pending control revision until durable save acknowledgement. | Implemented; covered by affected lane |
| R10 · Bounded verification | Preserve identity-bound batch cursors, publish intermediate progress, and reschedule newly eligible higher-priority work. | Implemented; regression in affected lane |

## Decisions and operating limits

- One combined PR with substantive commits, owned by Codex.
- Source and receipts live in an independently registered clone under
  `~/.codex/agent-workspaces/`; no task artifacts in `/tmp` or personal repos.
- PR #262 is based on `368b500`, newer than reviewed baseline `a253562`;
  preserve held-job fixes and adapt the reviewed contracts to those interfaces.
- Feature behavior is enabled normally; no feature flags are introduced.
- Correctness admission based on evidence is intrinsic runtime behavior,
  not a feature-enable gate.
- A bounded ordered persistence lane acknowledges actual durable writes and
  compacts only the exact captured journal incarnation and cutoff. Periodic
  saves coalesce while a prior save is pending. Cluster workers checkpoint
  generations privately without replacing the shared queue or compacting
  foreign journals.
- Payload syscalls use a separate bounded lane from ordered metadata commits.
  Startup recovery and runtime inventory/transform/relocation operations keep
  payload probes outside the inventory-wide mutation coordinator. Exact
  generation reservations survive until the blocking worker actually exits.
- Post-processing updates carry their admission scope through deferred owner
  work. Durable imports replace a job in one acknowledged snapshot. Resume and
  Start remain fenced until the exact pending control revision is saved.
- Accepted cluster publications create a journaled fresh inventory generation;
  prior directories retain their custody at backup. Current authority, actual
  worker drain, durable history and canonical queue replacement fence the
  transition. Startup replay cannot blindly rebind a substituted inode.
- Explicit operator Retry safely stages corrupt legacy download-owned final
  publications back to partial files after durable generation retirement;
  current-proof integrity contradictions remain review holds.
- Command revisions are separate from writer/job lifetime generations.
  Background I/O cannot erase newer operator intent or accepted range facts.
- Protocol 1 metadata-only recovery requires recognized local filesystem
  evidence. Unqualified network/FUSE mounts, including GlusterFS, continue
  range verification in the background; public control readiness is independent.
- No production change has been made. Production deployment and live Gluster
  qualification are not performed by this implementation task. Unsupported
  identity/provenance falls back safely to verification; do not claim
  production performance without it.
- The supplied Forgejo credential is not written into source, receipts, or logs.

## Live progress

[PR #262](https://github.com/pjunod/runner/pull/262) is the live status page for
check results, outstanding failures, merge and cleanup. Codex owns integration,
fixes, test receipts, PR updates and merge. The original independent report is
[STARTUP_IMPLEMENTATION_REVIEW.md](STARTUP_IMPLEMENTATION_REVIEW.md).
