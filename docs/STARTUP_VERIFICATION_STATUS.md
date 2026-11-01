# Startup runtime recovery — implementation and merge status

**Owner:** Codex (`codex/startup-runtime-recovery`) · **Updated:** 2026-11-01
**State:** implementation in progress · **PR:** being prepared
**Base:** `368b500` (fresh remote clone; includes held-job recovery updates)

Companion to [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md)
(reviewed design) and [STARTUP_VERIFICATION_REVIEW.md](STARTUP_VERIFICATION_REVIEW.md)
(review history). This is the canonical status page for the combined PR.

| Workstream | Owner | State | Remaining |
|---|---|---|---|
| Typed I/O outcomes and evidence preservation | Codex | In progress | Implement and add regressions |
| Durable provenance, overlap protection and retirement | Codex / delegated writer implementation | Planned | Implement schema, writer and owner integration |
| Publication recovery and finite drain | Codex | Planned | Implement and add crash-boundary regressions |
| Bootstrap controller, availability and reload | Codex / delegated runtime implementation | Planned | Implement independent service/component states |
| Deployment recipes and operator docs | GPT-6 Luna | Planned | Implement probe/rollout contracts |
| Combined adversarial review | Independent agent | Not started | Only after implementation is merge-ready |
| Fast lane | Codex | Not run | Only after adversarial fixes; retain passing results |
| Merge and cleanup | Codex | Pending | Green required checks, merge PR, finish owned clone |

## Validation policy

Tests have not run. Per user instruction, finish the implementation and
combined adversarial review first. Run each affected fast-lane check once
on the merge candidate; rerun failed checks only unless a later code change
invalidates a previously passing result. Do not run tests from pre-push
hooks during implementation or mark the draft PR ready before that point.

## Decisions and operating limits

- One combined PR with substantive commits, owned by Codex.
- Source and receipts live in an independently registered clone under
  `~/.codex/agent-workspaces/`; no task artifacts in `/tmp` or personal repos.
- Fresh base is newer than reviewed `a253562`; preserve held-job fixes and
  adapt the reviewed contracts to those interfaces.
- Feature behavior is enabled normally; no feature flags are introduced.
- Correctness admission based on evidence is intrinsic runtime behavior,
  not a feature-enable gate.
- Production deployment and live Gluster qualification are not performed
  by this implementation task. Unsupported identity/provenance falls back
  safely to verification; do not claim production performance without it.
- The supplied Forgejo credential is not written into source, receipts, or logs.

## Next update

Record the draft PR URL, concrete owner interfaces, and implemented commits.
Every open PR update must state owner, current stage, validation state, and
what remains before merge. If interrupted, update the PR last.
