"""Seed the codec fuzzers from independently frozen prior bytes."""

import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--corpus-root", required=True)
args = parser.parse_args()
reference = json.loads((Path(__file__).parent / "legacy-fixtures.json").read_text())
root = Path(args.corpus_root)
for target in ["peer_protocol", "key_bundle"]:
    (root / target).mkdir(parents=True, exist_ok=True)
for name, fixture in reference["peer"].items():
    data = bytes.fromhex(fixture["bytes_hex"])
    folder = root / "peer_protocol"
    (folder / name).write_bytes(data)
    for end in range(len(data)):
        (folder / f"{name}-prefix-{end:03}").write_bytes(data[:end])
data = bytes.fromhex(reference["keys_hex"])
folder = root / "key_bundle"
(folder / "legacy-native-keys").write_bytes(data)
for end in range(len(data)):
    (folder / f"legacy-prefix-{end:03}").write_bytes(data[:end])
(folder / "impossible-active-name").write_bytes((2**64 - 1).to_bytes(8, "little"))
(folder / "impossible-key-count").write_bytes(
    bytes(8) + (2**64 - 1).to_bytes(8, "little")
)
