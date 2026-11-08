# Startup recovery — pacing, admission pressure, and durable progress

**Status:** implemented; validation and delivery recorded in the PR.
**Written:** 2026-11-08 (America/New_York). **Incident checkout:** `1f73dea`.

Read this alongside [startup verification](STARTUP_VERIFICATION_PLAN.md),
[URL acquisition recovery](URL_FETCH_RECOVERY.md), and the measured
[download performance investigation](DOWNLOAD_SPEED_ROOT_CAUSE_PLAN.md).
The fixes use the existing owner, bounded lanes, verification tickets, and
ordered persistence. They do not disable integrity checks or redesign v2 receipts.

## 1. What production showed

After the user's update, one snapshot contained 192 recovery-verification
entries; 171 reported zero downloaded bytes. A held job reported
`corrupt state: payload lane unavailable; operation remains unadmitted`.
The queue later showed 79, then 74 verification entries, with no active
payload download despite many unfenced queued rows. Disk, quota, pause, and
provider-block guards were clear in those samples. These are snapshots during
ongoing recovery, not fixed counts or a claim that verification alone explains
every idle connection.

Live logs also reported `bounded persistence lane unavailable`, including
failures to schedule snapshots and volume-counter saves. The running build
predated the pending-writer owner-loop correction in PR #285; this branch
integrates that correction rather than duplicating its scheduling changes.

The displayed corruption and journal messages have different meanings:

- Payload admission used `StateError::Corrupt` when its 64-entry queue was full.
  That message was a scheduling refusal, not evidence of a failed CRC.
- A refused journal append took the same callback path as an actual failed
  write/sync, producing `segment journal is not durable; evidence retained`.
  Bytes may exist on disk while their restart record is not confirmed.
- Real I/O errors and identity/CRC conflicts remain genuine failures. The
  absence of those errors in the inspected excerpt does not certify all data.

## 2. Why verification was slow

A verifier intentionally yields after at most 32 MiB, 64 ranges, or its
250-millisecond cooperative work budget. The owner releases the completed
reader's root reservation and keeps its partial CRC/progress for the next batch.
However, the incomplete-result branch did not schedule another reader. It
waited for the next one-second owner tick. A storage root permits one verifier,
so the gap limited useful work to roughly 32 MiB per tick even when the storage
could read much faster. Owner stalls and smaller time-budgeted batches reduced
that further. A fairness boundary had accidentally become a throughput throttle.

There was also unnecessary work: `needs_payload` included every parsed queued
job with files. Even after journals and allocation metadata established that a
job had no completed byte claims, it entered checksum verification and then
requested a full durable queue snapshot to finish a zero-byte transition.
Hundreds of such jobs amplified metadata and persistence traffic.

The guarantee is removal of the artificial waits and unnecessary checksum
transitions. Recovery still shares real storage with downloads and processing;
this change does not promise an arbitrary percentage of network download time.

## 3. Completion-driven verification

An incomplete verification result now immediately calls the existing
`start_verifications` after publishing its partial progress. The real worker
has already joined and released its descriptors/custody before that message
arrives. The next job is selected through the existing priority, pause, turn,
root-reservation, and global-worker-limit rules.

No worker count, batch budget, CRC rule, or identity check is relaxed. Partial
CRC state is reused. A newly eligible higher-priority job can take the freed
root before the old job's next batch. Periodic ticks remain a fallback for
retries, not the cadence for healthy readers.

## 4. Admission pressure stays pending

`StateError::Busy` represents work that was not admitted. Queue saturation
cannot claim that stored state is corrupt.

Recovery reserves future payload capacity while its inventory lookup is in
flight, using the existing `allocating_jobs` set and lane occupancy. This
prevents a burst of state-lane metadata completions from overrunning the
64-entry payload queue. Deferred rows retain their existing loading ticket;
payload and state-lane completions retry them as capacity becomes available.
No unbounded overflow queue or age-based watchdog is introduced.

Normal download allocation also checks capacity before scheduling work. A
refusal leaves the row queued; its callback does not create an allocation
hold for `Busy`. The scheduler also recognizes this typed deferral at its
caller boundary and yields the current batch without inventing an in-flight
allocation or creating a hold. The real `grant_work` regression covers that
boundary as well as the allocator itself. Actual allocation, namespace, mount, or identity failures
continue through the existing failure policy.

## 5. Zero claims still receive custody checks

A zero displayed byte count is insufficient to skip verification. Journal
replay may restore ranges absent from a snapshot, and counters may be stale.

The metadata-only completion path requires all of the following:

- Journal loading for the current generation is complete.
- No Done segment, finalized file, stored CRC, proof, publication, repair
  intent, or processing stage requires payload verification.
- No transform custody is present.
- The record remains an ordinary queued or paused download.
- Fresh allocation identity, root/path, generation, and anchored custody
  checks succeed on the existing worker lane.

The owner rechecks current record claims and epoch before applying the result.
It then removes the empty verification ticket and recovery markers. There is
no changed segment/proof evidence requiring a dedicated per-job snapshot;
ordinary dirty-state persistence records the metadata cleanup. Real ranges
continue through the original verifier, including when display totals say zero.

## 6. Saturated journal admission retains responsibility

The bounded persistence queue previously discarded an unadmitted append
request and invoked its failure continuation. That created a storage hold even
though no write had been attempted. More hold/recovery transitions added more
snapshot traffic to the already saturated lane.

The owner already retains accepted segment states and file proofs. On append
admission saturation, it now marks a volatile checkpoint generation and stops
issuing new leases or promoting newly completed jobs. Further already-leased
writer completions update that generation and remain in the same owner state.
No second queue of journal records is allocated.

When the ordered lane has capacity, the owner submits one full snapshot using
its existing guarded persistence path. The snapshot follows older accepted
appends, so its normal journal synchronization and compaction remain ordered.
The completion clears backpressure only if it committed and still covers the
latest checkpoint generation. A later writer fact requires another snapshot;
an older acknowledgement cannot release newer progress.

Actual append write/sync errors retain their existing failure handling. A
failed checkpoint keeps the fence and evidence, then waits the existing
30-second storage-recovery cadence before retrying; it cannot spin on failed saves. Ordinary background snapshots
do not compete with a pending journal checkpoint. State-lane completions and
the owner tick drive retries. Shutdown's existing final snapshot covers all
accepted owner state before disarming the unclean marker.

This is producer backpressure at the durability boundary. Increasing queue
sizes, hiding errors, retrying old rows by age, or clearing holds globally
would not provide this ownership and acknowledgement guarantee.

## 7. Validation

The focused tests exercise actual owner channels, joined readers, inventory,
and persistence rather than only checking display strings:

- Multiple partial batches complete without any owner tick.
- A resumed higher-priority job takes the next root slot fairly.
- A 160-job pristine backlog stays bounded and drains without CRC workers.
- Payload capacity refusal is `Busy`, never a corruption/allocation hold.
- A real invalid Done range is verified and rejected despite zero totals.
- More than 256 accepted writer facts survive a blocked persistence lane.
- A completion arriving after checkpoint capture prevents premature release;
  the final durable snapshot contains all 301 accepted completion facts.
- A genuine failed checkpoint retains the fence, backs off, and releases only
  after storage recovers and a new checkpoint actually commits.

The existing metadata-responsiveness and pending-delete fixtures were updated
to reflect metadata-only completion and an actual retained byte claim.
Broader engine, workspace, startup, and cluster checks are recorded in the PR.
The owner tests are already selected by `make fast-check-startup`.

No production restart, payload deletion, forced status edit, or deployment
was performed during diagnosis or local implementation. Existing real-data
holds continue through their normal identity-checked recovery transitions.
