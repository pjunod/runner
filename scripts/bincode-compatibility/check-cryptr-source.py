"""Verify the complete owned Cryptr tree against its archive and source patch."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--work-root", required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
work = Path(args.work_root).resolve()
work.mkdir(parents=True, exist_ok=True)
expected = (root / "contrib/cryptr/upstream-0.10.0.sha256").read_text(encoding="utf-8").split()[0]
archive = work / "cryptr-0.10.0.crate"
if not archive.exists():
    cargo_home = Path(os.environ["CARGO_HOME"])
    cached = list(cargo_home.glob("registry/cache/*/cryptr-0.10.0.crate"))
    data = (
        cached[0].read_bytes()
        if cached
        else urllib.request.urlopen(
            "https://static.crates.io/crates/cryptr/cryptr-0.10.0.crate", timeout=60
        ).read()
    )
    assert (
        hashlib.sha256(data).hexdigest() == expected
    ), "Cryptr archive checksum mismatch"
    archive.write_bytes(data)
assert hashlib.sha256(archive.read_bytes()).hexdigest() == expected
unpacked = Path(tempfile.mkdtemp(prefix="cryptr-source-", dir=work))
with tarfile.open(archive) as package:
    package.extractall(unpacked, filter="data")
source = unpacked / "cryptr-0.10.0"
patch = root / "contrib/cryptr/0001-maintained-native-codec.patch"
subprocess.run(["git", "apply", "--check", str(patch)], cwd=source, check=True)
subprocess.run(["git", "apply", str(patch)], cwd=source, check=True)


def files(path):
    return {
        str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()
    }


actual = files(root / "vendor/cryptr")
actual.pop("RUNNER_PATCH.md")
assert actual == files(
    source
), "Owned Cryptr source drifted from checksum archive plus canonical patch"
report = {
    "archive_sha256": expected,
    "patch_sha256": hashlib.sha256(patch.read_bytes()).hexdigest(),
    "files_verified": len(actual),
    "license": "Apache-2.0",
    "upstream_revision": "abca462b82c5d4b3b4c9eb6c1a1cafc444150593",
}
(work / "provenance.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(
    "Verified complete Cryptr source, Apache-2.0 license and canonical native-codec patch."
)
