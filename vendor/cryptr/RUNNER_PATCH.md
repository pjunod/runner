# Runner Cryptr source contract

**Upstream:** Cryptr 0.10.0, revision `abca462b82c5d4b3b4c9eb6c1a1cafc444150593` ·
**License:** original Apache-2.0 [`LICENSE`](LICENSE) · **Scope:** native key codec maintenance

The source is reproduced from the published archive using
[`upstream-0.10.0.sha256`](../../contrib/cryptr/upstream-0.10.0.sha256) and
[`0001-maintained-native-codec.patch`](../../contrib/cryptr/0001-maintained-native-codec.patch).
The original manifest remains provenance; the normalized manifest is the executed
one. The patch updates the native key reader and its manifest/lock. The optional S3
dependency is pinned to 0.9.1, whose XML parser includes the upstream security
fixes; standalone Rand/Tokio locks are synchronized with fixed product versions.

The dependency selects GroveDB bincode 2.1.0. Ordinary Encode/Decode derives and
legacy little-endian fixed-width encoding remain available. The public key reader
uses the additional DecodeUntrusted implementation to avoid allocation from
unverified input lengths. Existing raw and sealed bundles, valid trailing-byte
behavior and public deserialization failures are verified against the actual
prior implementation. No file migration or key rotation is performed.

[`check-cryptr-source.py`](../../scripts/bincode-compatibility/check-cryptr-source.py)
compares every source/archive file after applying the patch; only this ownership
note is an additional file. The [compatibility contract](../../docs/BINCODE_COMPATIBILITY.md)
records frozen prior bytes, independent processes, feature profiles, hosted
platforms, fuzzing and bidirectional key/WAL/snapshot recovery. Update the
canonical patch and repeat those checks for future source or codec changes.
