#!/usr/bin/env python3
"""Run the supported native Windows security contract, refusing empty filters."""
import subprocess
import sys

if sys.platform != "win32":
    raise SystemExit("This contract requires a real native Windows runner")

cargo = ["cargo", "test", "--locked", "-p", "nzbd-state", "--lib",
         "--target", "x86_64-pc-windows-msvc"]
expected = [
    "windows_secret::tests::windows_sidecar_acl_is_owner_only",
    "windows_secret::tests::windows_sidecar_hardens_a_stale_temporary_file",
    "artifacts::fs::windows_boundaries::unsupported_absence_and_deletion_refuse_without_touching_payload",
    "artifacts::intents::manifest_tests::manifest_digest_golden_vector_unchanged",
    "artifacts::intents::manifest_tests::manifest_digest_matches_curator_fixture",
]
listing = subprocess.run(cargo + ["--", "--list"], check=True,
                         capture_output=True, text=True)
found = {line.strip().removesuffix(": test")
         for line in listing.stdout.splitlines() if line.strip().endswith(": test")}
missing = set(expected) - found
if missing:
    raise SystemExit("Missing native Windows regressions: " + ", ".join(sorted(missing)))
for name in expected:
    subprocess.run(cargo + [name, "--", "--exact"], check=True)
