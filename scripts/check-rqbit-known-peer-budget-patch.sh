#!/usr/bin/env bash

set -euo pipefail

readonly stable_base="00b97485160ff5b5aa2b379ea0815d568ec665f0"
readonly main_base="4e5f94cbcf1d57ec500885c77cf1e24d70232d89"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /path/to/clean/rqbit-v8.1.1-or-documented-main" >&2
  exit 2
fi

readonly source_dir="$1"
readonly repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_head="$(git -C "$source_dir" rev-parse HEAD)"
readonly source_head

case "$source_head" in
  "$stable_base")
    readonly patch_file="contrib/rqbit/0010-bound-known-peer-records.patch"
    readonly source_variant="stable"
    ;;
  *)
    if ! git -C "$source_dir" cat-file -e "$main_base^{commit}" 2>/dev/null; then
      echo "rqbit main base $main_base is missing; use a full main checkout" >&2
      exit 1
    fi
    if ! git -C "$source_dir" merge-base --is-ancestor "$main_base" "$source_head"; then
      echo "expected rqbit v8.1.1 at $stable_base or a descendant of main base $main_base; found $source_head" >&2
      exit 1
    fi
    readonly patch_file="contrib/rqbit/0011-bound-known-peer-records-main.patch"
    readonly source_variant="main"
    ;;
esac

echo "verifying rqbit $source_variant known-peer-budget patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-known-peer-budget.XXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT

git clone --quiet --no-hardlinks "$source_dir" "$work_dir/rqbit"
if [[ "$source_variant" == "stable" ]]; then
  readonly -a stable_prerequisites=(
    'contrib/rqbit/0001-allow-persistence-without-auto-restore.patch'
    'contrib/rqbit/0005-bound-tracker-requests.patch'
    'contrib/rqbit/0007-bound-session-peers.patch'
    'contrib/rqbit/0009-bound-pending-incoming-handshakes.patch'
  )
  for prerequisite in "${stable_prerequisites[@]}"; do
    git -C "$work_dir/rqbit" apply --check "$repository_root/$prerequisite"
    git -C "$work_dir/rqbit" apply "$repository_root/$prerequisite"
  done
  git -C "$work_dir/rqbit" apply --check "$repository_root/$patch_file"
  git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"
elif git -C "$work_dir/rqbit" apply --check "$repository_root/$patch_file"; then
  git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"
else
  echo "direct apply drifted; attempting a three-way apply against $source_head" >&2
  git -C "$work_dir/rqbit" apply --3way "$repository_root/$patch_file"
fi

readonly session_source="$work_dir/rqbit/crates/librqbit/src/session.rs"
readonly live_source="$work_dir/rqbit/crates/librqbit/src/torrent_state/live/mod.rs"
readonly peer_source="$work_dir/rqbit/crates/librqbit/src/torrent_state/live/peer/mod.rs"
readonly peers_source="$work_dir/rqbit/crates/librqbit/src/torrent_state/live/peers/mod.rs"

for invariant in \
  'known_peer_limit: Option<usize>' \
  'known_peer_limit_total: Option<usize>' \
  'KnownPeerSemaphores' \
  'type PeerQueueEntry = (PeerHandle, SocketAddr)' \
  'known_peer_semaphores.try_acquire()' \
  '_known_peer_permits: KnownPeerPermits' \
  'self.incoming && pe.value().outgoing_address.is_none()' \
  'self.set_state(PeerState::Queued, counters)' \
  'per_torrent_limit_is_exact_and_released' \
  'session_limit_is_shared_between_torrents' \
  'peer_records_hold_slots_until_removed' \
  'failed_shared_acquisition_releases_the_local_slot' \
  'alternate_outgoing_address_is_queued_once_with_record_handle'
do
  if ! grep -Fq "$invariant" "$session_source" "$live_source" "$peer_source" "$peers_source"; then
    echo "known-peer-budget patch is missing invariant: $invariant" >&2
    exit 1
  fi
done

if [[ "$source_variant" == "main" ]] && ! grep -Fq 'KnownPeerLimitReached' "$live_source"; then
  echo "rqbit main patch is missing its explicit known-peer limit result" >&2
  exit 1
fi

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  readonly exact_tests=(
    'torrent_state::live::peers::known_peer_budget_tests::per_torrent_limit_is_exact_and_released'
    'torrent_state::live::peers::known_peer_budget_tests::session_limit_is_shared_between_torrents'
    'torrent_state::live::peers::known_peer_budget_tests::peer_records_hold_slots_until_removed'
    'torrent_state::live::peers::known_peer_budget_tests::failed_shared_acquisition_releases_the_local_slot'
    'torrent_state::live::peers::known_peer_budget_tests::alternate_outgoing_address_is_queued_once_with_record_handle'
  )
  test_list="$(cargo test -p librqbit --lib -- --list)"
  readonly test_list
  for exact_test in "${exact_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$test_list"; then
      echo "known-peer-budget proof test was not discovered: $exact_test" >&2
      exit 1
    fi
    cargo test -p librqbit --lib "$exact_test" -- --exact --nocapture
  done
  # Compile the contributed CLI integration without installing the unrelated
  # stock web UI's provenance snapshot and npm build-time dependency graph.
  cli_features="$(python3 "$repository_root/scripts/rqbit-cli-check-features.py" crates/rqbit/Cargo.toml)"
  cargo check -p rqbit --all-targets --no-default-features --features "$cli_features"
)
