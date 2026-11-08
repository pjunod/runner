# Lifecycle remediation — delivery status

> **Archived 2026-11-08 (pipeline contract v2 M6).** Historical record; current: [PIPELINE_CONTRACT.md](../PIPELINE_CONTRACT.md) and [FILE_LIFECYCLE_OPERATIONS.md](../FILE_LIFECYCLE_OPERATIONS.md).

**Updated:** 2026-11-02. **Scope:** Runner and Curator lifecycle fixes; production
backlog cleanup remains separate and unapproved.

## 1. Delivery and preserved changes

Curator fixes merged in [PR #82](https://github.com/pjunod/curator/pull/82),
followed by the correction for its actual lint failure in
[PR #84](https://github.com/pjunod/curator/pull/84). Independently merged shutdown,
sample filtering, retry pacing, title locking, verification budgeting and mobile
changes remain intact.

Runner preserves independently merged PRs #261, #262 and #263, including the
startup recovery and asynchronous persistence architecture. Mobile build
numbers remain iOS 15 and Android 14. Apple workflows select and verify Xcode
27.2. This continuation does not deploy services or operate on production media.

## 2. Implemented behavior

- Publication resumes its durable operation after storage errors. Recoverable
  inputs remain available; disposable source cleanup follows durable handoff.
- Manual acquisitions retain independent request identities and library copies.
  Replay does not create another download. Native torrent children publish
  independent payloads while retaining their separate seed source.
- Seed retirement waits for backend stop/forget and dependent child use. A later
  grab of the same hash can create a fresh donor, including after restart.
- Cluster admission, publication and source-copy obligations survive coordinator
  changes. Acknowledgements bind exact owner, storage and grant identities.
- Cancellation of a sealed completion stops actual writers before deletion.
  Keep is applied by the machine holding the copy; an offline owner remains
  visibly pending.
- Existing desktop/mobile layouts show stage timing, handoff, cleanup, retries
  and pending owner/error information.
- Pre-upgrade in-flight downloads may finish importing. Existing backlog review
  and batch approval gates remain separate.

## 3. Validation and limits

Existing passing unit results were retained. The owner instructed us to stop
repeated review/test rounds and merge; no final Astra review or new full suite
is claimed. New code received its first relevant checks, and observed failures
were corrected. The PR description records final candidate checks.

Passing evidence includes focused publication/custody/retry and cluster tests,
Curator acquisition/SQLite and cancellation-concurrency checks, desktop/mobile
checks, and the actual Usenet Runner–Curator fixture. The latter verified library
placement, physical processing/publication cleanup, and cleanup retry after a
permissions failure and restart.

Native checks on Runner `7b12a88` prove paused acquisition/restart, real local-peer
download, publication, physical seed retirement and a fresh same-hash donor
after forget/restart. The paired fixture with Curator `6fd9d1e` proves two
same-item/same-hash manual copies, replay without duplication, independent
library bytes/inodes, physical child source/publication cleanup, and unchanged
donor bytes and seed policy. These are recorded checkpoints, not a claim that
every later combined change repeated those fixtures.

**Deferred limitation:** automatic deletion of redundant raw copies from a
partial cluster failure requires proof that all recovery dependencies remain
safely owned elsewhere. Current behavior retains uncertain copies with an
explicit reason. A new cross-node dependency-claim protocol was not added in
this merge. Explicit scoped deletion and proven unusable-source cleanup retain
their existing semantics. This limits automatic space recovery; uncertain
recoverable bytes are not discarded.

A canceled raw download that never emitted a sealed completion is not assigned
a new private cancellation ledger by this change. Its original source facts
and tombstone are retained, and the receipt explains the retained source and
required owner review; the sealed-result cancellation proof does not claim
automatic cleanup of that additional case.

## 4. Durable evidence

Source, original findings, check logs, binary hashes, integration fixture receipts
and merge receipts remain outside disposable build caches in the task-owned
`lifecycle-implementation-20261101/evidence` directory. Workspaces remain active
until integration and publication actually finish.
