#!/usr/bin/env bash
set -euo pipefail

# The fuzz crate is intentionally outside the product workspace so
# libfuzzer-sys never enters a release graph. Keep every shared package on the
# reviewed product lock and admit only the two pinned, test-only additions.
#
# fuzz/Cargo.lock is derived from Cargo.lock. Any change to the product lock
# (including Dependabot updates, which only manage the root manifest) must be
# carried into the fuzz lock in the same pull request:
#
#   scripts/check-bittorrent-fuzz-deps.sh --sync   (make fuzz-lock)
#
# rebuilds the fuzz lock from the product lock, pins the test-only tooling to
# the identities below, and then runs the same policy check.

registry='registry+https://github.com/rust-lang/crates.io-index'
expected_fuzz_only="$(
  printf '%s\t%s\t%s\n' \
    arbitrary 1.4.2 "$registry" \
    libfuzzer-sys 0.4.13 "$registry" \
    nzbd-torrent-fuzz 0.0.0 '(path)'
)"

usage() {
  echo "usage: $0 [--sync]" >&2
  exit 2
}

sync=false
case "$#:${1-}" in
  0:) ;;
  1:--sync) sync=true ;;
  *) usage ;;
esac

cd "$(dirname "$0")/.."

lock_identities() {
  awk '
    function emit() {
      if (name != "") {
        print name "\t" version "\t" source
      }
    }

    $1 == "[[package]]" {
      emit()
      name = ""
      version = ""
      source = "(path)"
      next
    }

    $1 == "name" {
      name = $3
      gsub(/"/, "", name)
      next
    }

    $1 == "version" {
      version = $3
      gsub(/"/, "", version)
      next
    }

    $1 == "source" {
      source = $3
      gsub(/"/, "", source)
    }

    END { emit() }
  ' "$1" | LC_ALL=C sort -u
}

if ! grep -Fq 'libfuzzer-sys = "=0.4.13"' fuzz/Cargo.toml; then
  echo 'fuzz/Cargo.toml must pin libfuzzer-sys exactly to 0.4.13' >&2
  exit 1
fi

if ! grep -Fq 'features = ["fuzzing"]' fuzz/Cargo.toml; then
  echo 'the fuzz crate must use nzbd-torrent through its fuzzing feature' >&2
  exit 1
fi

if ! grep -Fq 'path = "../contrib/rqbit/vendor/crates/librqbit"' fuzz/Cargo.toml; then
  echo 'the fuzz crate must use the same maintained rqbit vendor as the product lock' >&2
  exit 1
fi

if [[ "$sync" == true ]]; then
  # Start from the product lock so every shared package keeps its reviewed
  # identity; cargo then drops product-only packages and adds the fuzz-only
  # ones. Registry tooling is pinned to the reviewed versions above.
  cp Cargo.lock fuzz/Cargo.lock
  cargo metadata \
    --manifest-path fuzz/Cargo.toml \
    --format-version 1 >/dev/null
  while IFS=$'\t' read -r name version source; do
    if [[ "$source" == "$registry" ]]; then
      cargo update \
        --manifest-path fuzz/Cargo.toml \
        --package "$name" \
        --precise "$version"
    fi
  done <<<"$expected_fuzz_only"
fi

cargo metadata \
  --locked \
  --manifest-path fuzz/Cargo.toml \
  --format-version 1 >/dev/null

actual_fuzz_only="$(
  comm -13 \
    <(lock_identities Cargo.lock) \
    <(lock_identities fuzz/Cargo.lock)
)"

if [[ "$actual_fuzz_only" != "$expected_fuzz_only" ]]; then
  echo 'unexpected package identities entered the isolated fuzz lock:' >&2
  printf '%s\n' "$actual_fuzz_only" >&2
  echo 'fuzz/Cargo.lock is derived from Cargo.lock; regenerate it with: make fuzz-lock' >&2
  exit 1
fi

echo 'BitTorrent fuzz dependency policy: product lock plus pinned test-only tooling'
