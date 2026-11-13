#!/usr/bin/env bash
set -euo pipefail

# Repository policy permits no RustSec exceptions and one exact
# MPL-2.0 package only while their reviewed dependency and feature boundaries
# remain intact. Fail closed on output drift, but do not make a workspace
# version bump look like a third-party dependency change.

workspace_package_names="$(
  cargo tree --locked --workspace --depth 0 --prefix none |
    sed -E 's/ v[^ ]+.*$//' |
    grep -E '^[[:alnum:]_.+-]+$' |
    LC_ALL=C sort -u
)"

package_set() {
  local line
  local name

  sed -E 's/ \([^)]*\)( \(\*\))?$//; s/ \(\*\)$//' |
    while IFS= read -r line; do
      if [[ ! "$line" =~ ^[[:alnum:]_.+-]+[[:space:]]v[^[:space:]]+$ ]]; then
        continue
      fi

      name="${line%% v*}"
      if grep -Fqx -- "$name" <<<"$workspace_package_names"; then
        printf '%s (workspace)\n' "$name"
      else
        printf '%s\n' "$line"
      fi
    done |
    LC_ALL=C sort -u
}

require_exact_set() {
  local label="$1"
  local subject="$2"
  local actual="$3"
  local expected="$4"

  if [[ "$actual" != "$expected" ]]; then
    echo "$label $subject changed" >&2
    echo 'expected:' >&2
    echo "$expected" >&2
    echo 'actual:' >&2
    echo "$actual" >&2
    exit 1
  fi
}

inverse_tree() {
  local label="$1"
  local package_spec="$2"
  local edge_kinds="$3"
  local remedy="$4"
  local output

  if ! output="$(
    cargo tree --locked --workspace --target all -e "$edge_kinds" \
      -i "$package_spec" --prefix none 2>&1
  )"; then
    if grep -Fq 'did not match any packages' <<<"$output"; then
      echo "$label: $package_spec is absent from the locked graph." >&2
      echo "remedy: $remedy; do not update the expected set blindly." >&2
    else
      echo "$label: cargo could not inspect $package_spec." >&2
      echo "$output" >&2
    fi
    exit 1
  fi

  printf '%s\n' "$output"
}

if cargo tree --locked --workspace --target all -e normal --prefix none | grep -E '^quick-xml v' | grep -Ev '^quick-xml v0\.41\.'; then
  echo 'advisory-affected quick-xml re-entered the normal graph' >&2
  exit 1
fi

option_ext_tree="$(
  inverse_tree \
    'MPL-2.0 option-ext scope' \
    'option-ext@0.2.0' \
    'normal' \
    'remove or replace the exact option-ext license exception in deny.toml and update the gate 9 evidence'
)"
option_ext_packages="$(package_set <<<"$option_ext_tree")"
expected_option_ext_packages="$(LC_ALL=C sort <<'EOF'
directories v6.0.0
dirs-sys v0.5.0
librqbit v8.1.1
librqbit-core v5.0.0
librqbit-dht v5.3.1
librqbit-peer-protocol v4.3.0
librqbit-tracker-comms v3.0.0
nzbd (workspace)
nzbd-api (workspace)
nzbd-cluster (workspace)
nzbd-compat (workspace)
nzbd-qbit-compat (workspace)
nzbd-torrent (workspace)
option-ext v0.2.0
EOF
)"
require_exact_set \
  'MPL-2.0 option-ext exception' \
  'dependency package set' \
  "$option_ext_packages" \
  "$expected_option_ext_packages"

ignored_advisories="$(
  sed -n '/^ignore = \[/,/^\]/p' deny.toml |
    grep -Eo 'RUSTSEC-[0-9]{4}-[0-9]{4}' |
    LC_ALL=C sort -u || true
)"
expected_ignored_advisories=''
require_exact_set \
  'repository RustSec exceptions' \
  'reviewed identifier set' \
  "$ignored_advisories" \
  "$expected_ignored_advisories"

echo 'Reviewed dependency exceptions: exact package, feature, license, and advisory sets'
