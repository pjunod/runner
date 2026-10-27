# Upstream rqbit submissions (ikatson/rqbit)

Prepared 2026-10-27 from the Runner patches in `contrib/rqbit/`. **Nothing here has been pushed, forked, opened or sent.**
Every branch is one clean commit by Paul Junod on upstream `main` at **f4ca7d919638a7b8b031ab062a61861a9eb586e4** (`f4ca7d91`), conventional subject, no AI trailer
in the commit. Read `AI_POLICY.md` upstream before submitting: it requires disclosure (each description ends with an
**AI disclosure** section) and a human submitter who can explain every line.

Files, for each `NN-slug`: `NN-slug.patch` (`git format-patch` against the base above), `NN-slug.md` (the PR description,
used as the PR body), and `submit.sh` (one command, see below).

| # | slug | upstream base | size (own commit) | tests | status | overlap / notes |
|---|---|---|---|---|---|---|
| 01 | `metadata-size-limit` | `f4ca7d91` | 3 files, +39/-6 | 1 new; pass; `librqbit --lib`: 46p/4f (e2e: no IPv6); with #707: librqbit lib 50p/0f | ready / needs Paul's review | Merged #704 is on the serving side (`send_metadata_piece`): no overlap. |
| 02 | `file-sizing-errors` | `f4ca7d91` | 3 files, +151/-31 | 2 new; pass; `librqbit --lib`: 47p/4f (e2e: no IPv6); with #707: librqbit lib 51p/0f | ready / needs Paul's review | Interacts with merged 5a52e43f (pause during initial check): upstream suppresses *every* init error while a pause is requested, which would hide a sizing failure again. The PR types the pause error so only it is suppressed. No open PR. |
| 03 | `tracker-request-bounds` | `f4ca7d91` | 1 file, +155/-8 | 2 new; pass; `librqbit-tracker-comms --lib`: 6p/0f; with #707: librqbit lib 49p/0f | ready / needs Paul's review | No open PR. Behaviour change (60 s floor, 1 MiB, 30 s): asks maintainers. |
| 04 | `disable-auto-restore` | `f4ca7d91` | 4 files, +160/-0 | 1 new; new test needs IPv6: fails here (os error 97), passes with #707; `librqbit --lib`: 45p/5f (e2e: no IPv6); with #707: librqbit lib 50p/0f | ready / needs Paul's review | Trivial textual adjacency with #682 (`rqbit/src/main.rs`) and #708 (`tests/mod.rs`). Test needs IPv6 or #707. |
| 05 | `session-peer-limit` | `f4ca7d91` | 3 files, +117/-7 | 3 new; pass; `librqbit --lib`: 48p/4f (e2e: no IPv6); with #707: librqbit lib 52p/0f | ready / needs Paul's review | No open PR. Test module moved to end of file (clippy `items_after_test_module`). |
| 06 | `peer-response-budget` | `f4ca7d91` | 3 files, +213/-31 | 2 new; pass; `librqbit --lib`: 47p/4f (e2e: no IPv6); with #707: librqbit lib 51p/0f | ready / needs Paul's review | Budget derived from `max_request_window` (merged #687) instead of the draft's fixed 128: see semantic note below. |
| 07 | `known-peer-limit` | `f4ca7d91` | 6 files, +220/-31 | 5 new; pass; `librqbit --lib`: 53p/4f (e2e: no IPv6); with #707: librqbit lib 56p/1f (one flaky run, see below) | ready / needs Paul's review | Stacked on session-peer-limit. Overlaps open **#667** (same maps, different policy); see PR text. |
| 08 | `dht-queue-bounds` | `f4ca7d91` | 1 file, +279/-104 | 1 new; pass; `librqbit-dht --lib`: 29p/0f; with #707: librqbit lib 49p/0f | ready / needs Paul's review | Overlaps open **#706** (same loop). LSD hunks dropped (e0e9348 merged, #713 open). |
| 09 | `metadata-resolver-bounds` | `f4ca7d91` | 1 file, +205/-20 | 2 new; pass; `librqbit --lib`: 47p/4f (e2e: no IPv6); with #707: librqbit lib 51p/0f | ready / needs Paul's review | Overlaps open **#708** (same function in `dht_utils.rs`). |

Sizes are the PR's own commit (`+added/-deleted`). Tests: "pass" means the focused tests ran and passed on the committed tree;
`p`/`f` are passed/failed counts of the whole `--lib` suite of the crate. The numbers come from the run on 2026-10-27 in the
container (Rust 1.97.1, Linux, **no IPv6**); re-run before submitting.

## Submitting

```sh
contrib/rqbit/upstream/submit.sh --dry-run all      # applies every patch to current upstream main, touches nothing on GitHub
contrib/rqbit/upstream/submit.sh metadata-size-limit # fork (if needed), push up/<slug> to pjunod/rqbit, open the PR
contrib/rqbit/upstream/submit.sh all                 # in order; skips slugs whose PR already exists
```

It needs `gh` authenticated as `pjunod`. `known-peer-limit` is refused until the `session-peer-limit` PR is merged upstream. If a
patch no longer applies to the then-current `main` the script stops with a message instead of pushing anything; regenerate
that patch (apply the `.patch` to current upstream `main`, resolve, `git format-patch -1`; heads below are for reference) and run again.
`--draft` opens the PRs as drafts.

The submitted patches do not conflict with each other (checked pairwise with `git merge-tree`, and by stacking all nine on
`main`), so they can be sent in any order, except `known-peer-limit`, which needs `session-peer-limit` first.

Local branch heads: `metadata-size-limit` 34465af1, `file-sizing-errors` 4b7f5ebf, `tracker-request-bounds` 04842f26, `disable-auto-restore` 7275cc03, `session-peer-limit` 0b5cae55, `peer-response-budget` d877b2fc, `known-peer-limit` 635237c6, `dht-queue-bounds` 93cc7996, `metadata-resolver-bounds` 24865b62.

## How it was checked

**Full upstream test suite, on a host with IPv6 (2026-10-27).** On a 16-core Ubuntu lab host, with Rust 1.97.1 and `OPENSSL_DIR=/usr`, each branch was checked out and ran exactly what upstream's Ubuntu CI runs, `cargo test` (default workspace):

| branch | `cargo test` |
|---|---|
| upstream main `f4ca7d91` (baseline) | ok, 168 passed / 0 failed |
| metadata-size-limit | ok, 169 / 0 |
| file-sizing-errors | ok, 170 / 0 |
| tracker-request-bounds | ok, 170 / 0 |
| disable-auto-restore | ok, 169 / 0 |
| session-peer-limit | ok, 171 / 0 |
| peer-response-budget | ok, 170 / 0 |
| known-peer-limit | ok, 176 / 0; and `cargo test -p librqbit --lib` 10 runs in a row, 10/10 ok |
| dht-queue-bounds | ok, 169 / 0 |
| metadata-resolver-bounds | ok, 170 / 0 |

So the IPv6-dependent e2e tests and `disable-auto-restore`'s persistence test pass on a pristine branch, and the one flaky `test_e2e_download_tcp`
seen earlier in an IPv6-less container (with #707's diff patched in) did not recur in 10 runs.

**Also, per branch:** `cargo fmt --all -- --check`, `cargo check`, `cargo check --workspace --exclude rqbit-desktop` and `cargo clippy --all-targets`
on Rust 1.97 and 1.94 (CI's compat matrix), all clean; origin/main has 0 clippy warnings and no branch adds one.

**Not covered:** the Windows and macOS legs of upstream CI; the desktop crate (needs GTK; the branches only add fields it fills with
`..Default::default()`); and `dht-queue-bounds` has no automated test that drives a real DHT lookup through the changed loops (the network DHT
tests are `#[ignore]`d) — it is the PR to read hardest.

## What changed relative to the Runner drafts (semantic changes)

- **peer-response-budget**: the budget is `max_request_window` (merged #687, default 128) instead of a hard-coded 128, and that value is what is advertised
  as `reqq`. Consequence (called out in the PR text as an open question): a very small `max_request_window` also shrinks how many requests a peer may have
  outstanding with us before being disconnected.
- **known-peer-limit / session-peer-limit / disable-auto-restore**: the Runner patches did not compile for the `rqbit` CLI, whose `SessionOptions` literal has
  no `..Default::default()`; each now adds the new field (`None`/`false`) there. No CLI flags were added.
- **disable-auto-restore**: the new test's clippy cast warnings are fixed.
- **session-peer-limit**: test module moved to the end of `live/mod.rs` (clippy `items_after_test_module`). **peer-response-budget**'s tests live in an out-of-line
  file `live/peer_response_budget_tests.rs` so they do not conflict with that module at the end of the same file.
- **dht-queue-bounds / metadata-resolver-bounds**: the Runner "discovery pressure" patch (526 lines) is split in two. Dropped: all LSD hunks (merged e0e9348 cancels
  the announce task; open #713 covers registrations), the "send each recursive request once" hunk (already fixed upstream by af9b843e, the hunk was a no-op),
  a cosmetic bootstrap loop edit, and the reordering of peers vs nodes in `request_one`.
- **metadata-resolver-bounds**: the draft enqueued candidates inside `debug_assert!(queues.enqueue(..))`, which is not evaluated in release builds, so no peer would
  ever have been tried there. Fixed, plus a test (`initial_and_streamed_candidates_are_connected_to`) that fails if the enqueue is removed, and a needless-`let`
  clippy warning.
- **file-sizing-errors, tracker-request-bounds, metadata-size-limit**: as in the drafts. Their PR texts note the behaviour change (tracker floor of 60 s, the
  sizing failure now ends in `Error`, error message wording).
- The Runner drafts' commentary about nzbd, stable v8.1.1 and Runner's verifier scripts is gone; descriptions are written as Paul.

## Not submitted

- R4 `0009` pending-handshake budget: fixed upstream (`ListenerOptions::max_pending_incoming_handshake_checks`).
- R10 `0003`/`0004` discovery-health snapshot: design issue first, large API.
- R11 `0019-disable-peer-exchange`, R12 `0020-expose-test-only-dht-bootstrap`, R13 `0021-...announce-lifecycle`: later (no `-main` variants / project-specific / too large).
- The LSD part of `0015` (see above).

## Things for Paul before sending

- Upstream defers to Ghostty's AI policy (checked 2026-10-27, `ghostty-org/ghostty` `AI_POLICY.md`): disclose the tool and how much it
  did (each description ends with that section), you must be able to explain every change without AI, and AI-written issue/PR text must
  be **reviewed and edited by you** before it is posted — so edit each `NN-slug.md` into your own words first. Ghostty also keeps a public
  denouncement list for low-quality AI contributions that other projects can reuse; rqbit adopted the AI policy only, not Ghostty's vouch
  system, so no prior approval is required.
- Pace them: send `metadata-size-limit` and `file-sizing-errors` first and wait for the maintainer's reaction before the rest, rather than
  nine PRs on one day. For the larger ones (`known-peer-limit`, `dht-queue-bounds`, `peer-response-budget`, `tracker-request-bounds`) a short
  issue first is the polite route.
- Read every line of the PRs you send; the descriptions state facts from the runs above in the passive/neutral voice but are not a substitute for that.
- Open PRs #667, #706, #708 overlap three of these: look at them before sending `known-peer-limit`, `dht-queue-bounds`, `metadata-resolver-bounds`.
