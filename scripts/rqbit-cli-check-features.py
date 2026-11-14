#!/usr/bin/env python3
"""Preserve upstream CLI defaults except its unrelated stock frontend build."""
import sys
import tomllib
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: rqbit-cli-check-features.py /path/to/crates/rqbit/Cargo.toml")
    manifest = tomllib.loads(Path(sys.argv[1]).read_text())
    defaults = manifest.get("features", {}).get("default")
    if not isinstance(defaults, list) or not all(isinstance(f, str) for f in defaults):
        raise SystemExit("upstream CLI default features changed; review the compile proof")
    if "webui" not in defaults:
        raise SystemExit("upstream CLI webui feature changed; review before running build hooks")
    selected = [f for f in defaults if f != "webui"]
    if not selected:
        raise SystemExit("upstream CLI has no remaining default integration features")
    print(",".join(selected))


if __name__ == "__main__":
    main()
