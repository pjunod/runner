# Startup runtime recovery — implementation and merge status

**Owner:** Codex (`codex/startup-runtime-recovery`) · **Updated:** 2026-11-01
**State:** implementation complete; independent review next · draft PR · **PR:** [#262](https://github.com/pjunod/runner/pull/262)
**Base:** `368b500` (fresh remote clone; includes held-job recovery updates)

Companion to [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md)
(reviewed design) and [STARTUP_VERIFICATION_REVIEW.md](STARTUP_VERIFICATION_REVIEW.md)
(review history). This is the canonical status page for the combined PR.

| Workstream | Owner | State | Remaining |
|---|---|---|---|
| Owner, provenance, verification and async persistence integration | Codex / Sol / Luna | Written; compile checked | Independent review, then affected tests |
| Writer/recovery helper and PP/torrent admission call sites | Sol | Written; compile checked | Independent review, then affected tests |
| Persistent control availability and shutdown | Sol | Written, unvalidated | Independent review and final affected checks |
| Deployment templates/operator docs; web/mobile 503 retention | Luna | Written, unvalidated | Integrate with final runtime interfaces |
| Enrollment regressions and startup fast-lane wiring | Luna | Written, unvalidated | Run in the final affected lane after review |
| Combined adversarial implementation review | Independent reviewer | Not started | One review after integration is complete |
| Fast lane | Codex | Wired, not run | Run once after review fixes; rerun failed checks only |
| Merge and cleanup | Codex | Pending | Merge after required checks pass; finish owned clone |

## Validation policy

No tests or adversarial implementation review have run. The integrated workspace
passes `cargo check --workspace --all-targets --locked`; compile-only integration
errors were corrected without executing tests. Formatting is applied. Conduct
one combined adversarial review next. Run each affected fast-lane check once on the merge candidate; rerun
failed checks only unless a later code change invalidates a passing result.
Do not mark modules validated or the draft PR ready before that point.

## Decisions and operating limits

- One combined PR with substantive commits, owned by Codex.
- Source and receipts live in an independently registered clone under
  `~/.codex/agent-workspaces/`; no task artifacts in `/tmp` or personal repos.
- Draft PR #262 is based on `368b500`, newer than reviewed baseline `a253562`;
  preserve held-job fixes and adapt the reviewed contracts to those interfaces.
- Feature behavior is enabled normally; no feature flags are introduced.
- Correctness admission based on evidence is intrinsic runtime behavior,
  not a feature-enable gate.
- A bounded ordered persistence lane now acknowledges actual durable writes.
  Periodic saves coalesce while a prior save is pending. Cluster workers
  checkpoint generations privately without replacing the shared queue or
  compacting foreign journals.
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

## Next steps

Complete integration → one combined adversarial review → fast lane → rerun
failed checks only → merge. Root owns integration, status, and merge; Sol's
runtime and Luna's UI/deployment work remain unvalidated until the combined
review and fast lane are complete.
