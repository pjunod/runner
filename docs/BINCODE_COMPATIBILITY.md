# Bincode compatibility — maintained codecs with unchanged bytes

**Status:** compatibility contract and reproducible validation · **Date:** 2026-11-13 ·
**Scope:** peer messages, Hiqlite/Raft/WAL, snapshot metadata and Cryptr key bundles ·
**Decision:** [ADR-19 serialization renewal](BITTORRENT_PROPOSAL.md#435-serialization-maintenance-renewal--2026-11-13)

Runner uses maintained codec implementations without changing its storage or
wire formats. Existing files are read in place. This change adds no migration,
format marker, data rewrite, deployment or release operation.

## 1. Implementations and provenance

| Boundary | Prior implementation | Maintained implementation |
| --- | --- | --- |
| rqbit peer serialization | bincode 1.3.3 | [fugue-bincode 1.3.4](https://github.com/fugue-re/fugue-bincode/tree/31e009b581653c8d657989da12771f39bf703d1c) |
| Hiqlite and WAL Serde | bincode 2.0.1 | [grovedb-bincode 2.1.0](https://github.com/dashpay/grovedb/tree/52ae761f86c7c26215755f282dc52b19cc20e30c/grovedb-bincode) |
| Cryptr native key encoding | bincode 2.0.1 | the same GroveDB runtime and matching derive package |

These are independent maintained forks, not new names for the retired crate.
Fugue retains the bincode 1 API and contains a fallible reader-allocation fix.
GroveDB retains the original ordinary encoder/decoder behavior and adds explicit
untrusted decoding APIs. Its published provenance identifies the bincode 2.0.1
archive and original MIT license. The Cargo dependency aliases preserve existing
source imports while the actual package identities and checksums identify the
forks honestly. No advisory is ignored or dismissed.

Fugue's registry metadata retains an older upstream repository URL; the release
commit linked above and the checksum in the lockfile identify the reviewed
source. Patch `0025-maintained-peer-codec.patch` records the peer dependency and
upstream lock change. The immutable rqbit archive, ordered eighteen-patch series
and byte-for-byte vendor derivation remain mandatory.

[Vendored Cryptr](../vendor/cryptr/) comes from the checksum-verified 0.10.0
release, revision `abca462b82c5d4b3b4c9eb6c1a1cafc444150593`. Its original
source, README, Apache-2.0 license and original manifest are retained. The local change
selects the GroveDB package, keeps ordinary native `Encode`/`Decode` contracts,
and also derives `DecodeUntrusted`. The public key-file reader selects that
untrusted decoder. It grows collections after verified input instead of
reserving memory from an unverified length header. Valid bytes, trailing-byte
acceptance and the public `CryptrError::Deserialization` failure remain stable.
No new maximum valid key-bundle size is imposed. The owned standalone lock
uses fixed Rand/Tokio versions. Its optional S3 dependency selects maintained
0.9.1 with fixed XML parsing; the cache/backup profile is audited too. These
validation runs use loopback nodes and no S3 service or production keys.

The retired bincode 3.0.0 package is an error marker. Other community forks add
broader format, configuration, derive or Serde behavior. The selected forks keep
the relevant compatibility surfaces narrow; independent validation below, rather
than their package names or compatibility claims, establishes Runner's contract.

## 2. Encoding contract

| Consumer | API and configuration | Preserved details |
| --- | --- | --- |
| Peer Request/Cancel | Serde, fixed integers, big endian | Three u32 fields; framing and message identifiers unchanged |
| Peer handshake | Serde default options | Existing compact length prefix and fixed byte arrays; borrowed decoding retained |
| Hiqlite network, state and snapshots | Serde `config::legacy()` | Little endian, fixed integers, field order, enum tags, options and existing trailing-byte acceptance |
| WAL Raft entries, votes and purged log IDs | Serde `config::legacy()` | Same entry payloads and OpenRaft identities |
| WAL outer metadata | Serde `config::standard()` | Little endian and variable integers, including nested legacy-encoded vote/log bytes |
| Cryptr key bundles | Native derives, `config::legacy()` | Active key ID and ordered `(ID, bytes)` pairs; sealed envelope unchanged |

WAL magic/version, record IDs, CRCs, length fields, commit ordering and atomic
metadata publication are unchanged. SQLite images and their existing snapshot
publication/recovery protocol are unchanged. Queue snapshots use Runner's
separate [queue encoding contract](QUEUE_SNAPSHOT_ENCODING.md).

Hiqlite's optional features can change enum discriminants. Compatibility is
proved within each supported feature profile; this does not make historically
different feature configurations interchangeable in a mixed cluster. The
production SQLite profile and the cache/backup profile have separate frozen
references and locked validation graphs.

## 3. Independent old/new evidence

[`build.py`](../scripts/bincode-compatibility/build.py) builds the same probe
against two independent source trees. The prior tree is extracted from immutable
Runner commit `5e44e1acfb590c8657a10cf7983f8c37cb04c654`; its real bincode 1.3.3
and 2.0.1 implementations come from checksum-locked registry packages. Only
validation-only modules are grafted onto that tree. Its storage, network,
encoder, decoder and key-reader implementations remain the prior sources.
The candidate uses the current tree and the maintained forks. Each probe has a
separate committed lockfile, and the build checks actual package identities.

The [frozen reference](../scripts/bincode-compatibility/legacy-fixtures.json)
contains bytes produced by the prior process, counts and hashes for every
truncated-prefix outcome, and the public key-error contract. CI rebuilds the
prior process and compares it with that reference before comparing the candidate.
The candidate cannot generate both sides of the comparison.

Actual production structures include peer handshake/Request/Cancel messages;
all SQL parameter forms; normal, blank and membership Raft entries; query,
transaction, batch and migration variants; responses and errors; votes;
SnapshotMeta, StoredSnapshot and StateMachineData; and the actual WAL Metadata
structure. Fixtures exercise nonzero discriminants, Some/None, signed extremes,
variable-integer boundaries, big-endian u32 values, Unicode, blobs, nested records,
all truncated prefixes and accepted trailing bytes. Cryptr tests exercise its
native derives and real password-sealed envelope in both directions. Candidate
regressions also reject impossible untrusted collection/string lengths without
header-driven allocation.

[`run.py`](../scripts/bincode-compatibility/run.py) then runs real old and new
Hiqlite processes over private loopback sockets and owned test directories:

1. Old writes, new restarts and reads, new writes, and old restarts and reads.
   Before each crash, the existing validation-only apply pause holds a real SQL
   entry after its WAL durable index advances. An independent read-only SQLite
   query proves that row is absent. After switching binaries, the exact row must
   appear once, proving replay from the prior WAL rather than a cached database
   image. Durable and applied indexes are retained as evidence.
2. Published SQLite/Raft snapshots and state metadata recover in both directions.
3. Unpublished metadata staging files and partially written, uncommitted WAL
   tails survive restart without replacing committed state or adding a record.
4. Truncated metadata, invalid CRCs and unknown versions produce matching bounded
   inspection failures. File hashes enforce the read-only inspection contract.
5. Three-node clusters roll between old and new binaries. A stopped follower's
   test directory is removed only after a leader snapshot is published and its
   covered logs are purged. The other implementation must receive that snapshot.
   Assertions require inbound `complete`, a positive received-byte count and the
   expected applied/snapshot index, as well as exact persisted query results.
   This runs in both directions with real writes, transactions, responses and
   error responses crossing mixed-version sockets.

All process operations have deadlines; failures retain stderr, dependency graphs,
fixture outputs and snapshot transport evidence. Existing workload and fairness
assertions remain separate protected gates. The reference processes receive
bounded compatibility fixtures; arbitrary hostile key inputs exercise the
candidate's untrusted decoder.

## 4. Reproduce and renew

Use an owned external-drive workspace and the existing external compiler slot.
Set `CARGO_HOME`, `RUSTUP_HOME` and `CARGO_TARGET_DIR` to that workflow's configured
paths. The work directory below must be new and outside the source checkout:

```sh
python3 scripts/bincode-compatibility/build.py --work-root "$COMPAT_WORK" --side legacy
python3 scripts/bincode-compatibility/build.py --work-root "$COMPAT_WORK" --side candidate
python3 scripts/bincode-compatibility/run.py --work-root "$COMPAT_WORK"
```

Use `--profile cache-backup` on all three commands for the optional feature
profile. `--update-locks` explicitly refreshes a probe lock during maintenance;
CI always uses reviewed locks. `--freeze` can create an absent reference only,
after independent old/new comparison. It refuses silent replacement.

The [hosted compatibility workflow](../.github/workflows/bincode-compatibility.yml)
runs real old/new validation on fresh Linux, macOS and Windows hosts. The
[bounded fuzz workflow](../.github/workflows/bittorrent-fuzz.yml) retains metainfo
and magnet campaigns and adds peer-frame and native-key campaigns. Their corpus
starts from the frozen prior bytes, truncated prefixes and impossible length
headers. Scheduled campaigns retain the longer run budget.

Changing a codec, derive version, feature profile, configuration, fixture schema,
archive, patch membership or storage/network type requires fresh independent
bytes, malformed-input outcomes, cross-version recovery, hosted native and fuzz
proofs. Protected merge requires all publisher-bound checks on the exact head.
Legacy oracle lockfiles deliberately retain the retired implementations as
validation inputs; they are not product or fuzz runtime dependencies. Optional
stock upstream contribution/provenance graphs remain separately inventoried.
