# Dev Compose build status — local image ownership

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [DEPLOY.md](../DEPLOY.md).

**Status:** reviewed, qualified, and merge approved · **Branch:**
`codex/fix-dev-compose-build` · **Updated:** 2026-09-19

Companion to [STATUS.md](../../STATUS.md), which owns the project-wide ledger.
This page records the isolated fix for the dev Compose build failure reported on
`nuc3`; it is not a general Docker operations guide.

## Current position — implementation complete, merge approved

| Workstream | State | Next observable result |
|---|---|---|
| Reproduction and diagnosis | complete | The image-only discovery service is identified as the registry pull owner |
| Compose image ownership | implemented | Both services resolve the same local build definition |
| Regression contract | implemented | A source-level test rejects an image-only dev discovery service |
| Adversarial review | complete | No actionable P0–P3 findings |
| Fast-lane qualification | complete | Formatting, six config tests, Compose validation, and build-ownership assertion pass |
| Required CI | qualified | The required unit/e2e check and 14 other jobs pass; coverage remains below its repository-wide minimum and is deferred under the operator's batching policy |
| Merge | approved | The final branch is conflict-free on current `main`; the status-only head may use the documented administrator override without another redundant full-suite run |

## Working rules — one review and one qualification pass

- Work happens in an isolated clone at
  `/private/tmp/nzbd-codex-compose-build`; user checkouts remain untouched.
- The implementation receives one adversarial review only when the complete
  diff is ready to merge.
- Review findings are addressed before the fast-lane tests run. Full-suite
  failures remain for the separate batching process requested by the operator.
- The change adds no runtime feature gate. It repairs build ownership in the
  developer Compose model.

## Decisions applied during implementation

1. **Share the build mapping with a YAML anchor.** Both services need an
   explicit `build` entry, while one definition prevents their Dockerfile,
   context, and version argument from drifting.
2. **Keep `depends_on` for runtime readiness only.** Compose resolves images
   before container startup ordering, so health-based dependency ordering
   cannot make an image-only service consume a concurrently built tag.
3. **Pin the regression at the shipped-recipe boundary.** The failure came
   from the Compose contract, so the test inspects that contract instead of
   exercising unrelated daemon behavior.

## Activity log

| Date | Change | Evidence |
|---|---|---|
| 2026-09-19 | Reproduced the resolved Compose model from the reported checkout | Only `nzbd` owned a build; `nzbd-discovery` referenced `nzbd:dev` as an image |
| 2026-09-19 | Moved work into an isolated GitHub clone | Clean branch `codex/fix-dev-compose-build` from `origin/main` |
| 2026-09-19 | Shared the build definition and added the regression contract | Source inspection complete; tests intentionally deferred until after review |
| 2026-09-19 | Completed the single adversarial merge-readiness review | No actionable P0–P3 correctness, compatibility, test, documentation, or side-effect findings |
| 2026-09-19 | Ran the single fast-lane qualification pass | Workspace formatting, six shipped-config tests, Compose schema resolution, and both-service build ownership are green |
| 2026-09-19 | Remediated a required CI failure disclosed after review | RUSTSEC-2026-0285 affects base-branch `rustls 0.23.42`; the product and isolated fuzz lockfiles now select patched `0.23.45` and matching crypto dependencies |
| 2026-09-19 | Batched a required lint remediation exposed by the security rerun | Replaced a manual nonzero division guard in qBittorrent ETA projection with equivalent `checked_div`; this is unrelated to the Compose fix and preserves the zero-rate sentinel |
| 2026-09-19 | Qualified the final code head for merge | Lint, MSRV, RustSec, dependency policy, licensing, mobile, fuzz preflight, and all platform build jobs pass |
| 2026-09-19 | Deferred the existing full-suite failures under the operator's batching policy | `unit + e2e` and coverage reproduce three unrelated daemon-test failures; the protected check requires an administrator merge override |
| 2026-09-19 | Rebased after `main` advanced through PR #224 | The only conflict was the same Clippy ETA cleanup already on `main`; the redundant product-code hunk was dropped, while the patched fuzz lockfile remains |
| 2026-09-19 | Requalified after PR #224 repaired the daemon tests | Required unit/e2e, lint, MSRV, supply-chain, fuzz, mobile, and platform checks pass; a flaky macOS swarm initialization race passed on its targeted rerun |
| 2026-09-19 | Deferred repository-wide coverage debt | The instrumented suite completes, but total line coverage is 87.28%, below the configured minimum; no Compose product regression is implicated |
| 2026-09-19 | Synced conflict-free onto `main` after PR #226 | The final change is this status record only; merging without another automatic full-suite cycle avoids duplicating already-green qualification |
