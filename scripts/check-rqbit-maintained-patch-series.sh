#!/usr/bin/env bash
set -euo pipefail

readonly repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly series_file="$repository_root/contrib/rqbit/maintained-series.txt"
readonly checksum_file="$repository_root/contrib/rqbit/upstream-v8.1.1.sha256"
readonly vendor_dir="$repository_root/contrib/rqbit/vendor"
readonly upstream_url="https://github.com/ikatson/rqbit/archive/refs/tags/v8.1.1.tar.gz"
readonly expected_series=$'0001-allow-persistence-without-auto-restore.patch\n0005-bound-tracker-requests.patch\n0007-bound-session-peers.patch\n0009-bound-pending-incoming-handshakes.patch\n0010-bound-known-peer-records.patch\n0012-bound-peer-response-backlog.patch\n0014-bound-discovery-pressure.patch\n0016-limit-peer-metadata-before-allocation.patch\n0018-propagate-file-sizing-errors.patch\n0019-disable-peer-exchange.patch\n0020-expose-test-only-dht-bootstrap.patch\n0021-configurable-client-identity-and-announce-lifecycle.patch\n0022-read-only-piece-verifier.patch\n0023-upgrade-upnp-xml-parser.patch\n0024-replace-unmaintained-retry-and-sha1-backends.patch\n0025-maintained-peer-codec.patch'
readonly expected_vendor_entries=$'LICENSE\nREADME.md\ncrates'

if [[ "$(<"$series_file")" != "$expected_series" ]]; then
  echo "maintained rqbit patch order or membership changed unexpectedly" >&2
  diff -u <(printf '%s\n' "$expected_series") "$series_file" || true
  exit 1
fi

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-series.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

expected_checksum="$(awk 'NR == 1 { print $1 }' "$checksum_file")"
readonly expected_checksum

archive_checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{ print $1 }'
  else
    shasum -a 256 "$1" | awk '{ print $1 }'
  fi
}

verify_archive() {
  [[ -f "$1" ]] && [[ "$(archive_checksum "$1")" == "$expected_checksum" ]]
}

if [[ -n "${RQBIT_UPSTREAM_ARCHIVE:-}" ]]; then
  archive="$RQBIT_UPSTREAM_ARCHIVE"
  if ! verify_archive "$archive"; then
    echo "rqbit upstream archive is missing or does not match $checksum_file: $archive" >&2
    exit 1
  fi
else
  cache_root="${SWARM_BUILD_CACHE_DIR:-${CARGO_TARGET_DIR:-$repository_root/target}}"
  archive="${RQBIT_UPSTREAM_CACHE:-$cache_root/rqbit-sources/rqbit-v8.1.1.tar.gz}"
  if [[ -e "$archive" ]]; then
    if ! verify_archive "$archive"; then
      echo "cached rqbit archive does not match $checksum_file: $archive" >&2
      echo "remove that cache entry or set RQBIT_UPSTREAM_ARCHIVE to a verified copy" >&2
      exit 1
    fi
  else
    download="$work_dir/rqbit-v8.1.1.tar.gz.download"
    if ! curl --fail --location --silent --show-error "$upstream_url" --output "$download"; then
      echo "could not fetch rqbit v8.1.1; set RQBIT_UPSTREAM_ARCHIVE to a verified local copy" >&2
      exit 1
    fi
    if ! verify_archive "$download"; then
      echo "downloaded rqbit archive does not match $checksum_file" >&2
      echo "GitHub archive bytes may have changed; supply the reviewed archive with RQBIT_UPSTREAM_ARCHIVE" >&2
      exit 1
    fi
    mkdir -p "$(dirname "$archive")"
    mv "$download" "$archive"
  fi
fi

tar -xzf "$archive" -C "$work_dir"
readonly source_dir="$work_dir/rqbit-8.1.1"
git -C "$source_dir" init --quiet
git -C "$source_dir" add --all

while IFS= read -r patch_name; do
  patch_file="$repository_root/contrib/rqbit/$patch_name"
  git -C "$source_dir" apply --check "$patch_file"
  git -C "$source_dir" apply "$patch_file"
  git -C "$source_dir" diff --check
  git -C "$source_dir" add --all
done <"$series_file"

vendor_entries="$(
  cd "$vendor_dir"
  find . -mindepth 1 -maxdepth 1 -print | sed 's#^\./##' | LC_ALL=C sort
)"
readonly vendor_entries
if [[ "$vendor_entries" != "$expected_vendor_entries" ]]; then
  echo "checked-in rqbit vendor root contains unexpected or missing entries" >&2
  diff -u <(printf '%s\n' "$expected_vendor_entries") <(printf '%s\n' "$vendor_entries") || true
  exit 1
fi

if ! diff -u "$source_dir/LICENSE" "$vendor_dir/LICENSE" \
  || ! diff -u "$source_dir/README.md" "$vendor_dir/README.md" \
  || ! diff -ru "$source_dir/crates" "$vendor_dir/crates"; then
  echo "checked-in rqbit vendor tree drifted from v8.1.1 plus maintained-series.txt" >&2
  exit 1
fi

if ! grep -Fq 'pub fn new_with_opts_for_test(' \
  "$source_dir/crates/librqbit/src/session.rs"; then
  echo "maintained rqbit test-only DHT bootstrap hook was not discovered" >&2
  exit 1
fi
if ! grep -Fq 'pub struct InvalidResolvedMagnetMetadataError;' \
  "$source_dir/crates/librqbit/src/session.rs"; then
  echo "maintained rqbit invalid resolved-metadata marker was not discovered" >&2
  exit 1
fi

if ! grep -Fq 'pub http_user_agent: Option<String>,' \
  "$source_dir/crates/librqbit/src/session.rs"; then
  echo "maintained rqbit tracker User-Agent option was not discovered" >&2
  exit 1
fi

if [[ "${RQBIT_SERIES_DERIVE_ONLY:-0}" == "1" ]]; then
  echo "rqbit maintained patch series: checksum, ordered apply, and vendor drift checks passed"
  exit 0
fi

check_dht_dispatch() (
  cd "$source_dir"
  # Compile the production dispatch path with release assertion semantics.
  export CARGO_PROFILE_TEST_DEBUG_ASSERTIONS=false
  readonly dispatch_test='dht::queue_budget_tests::recursive_lookup_dispatches_requests_without_debug_assertions'
  dispatch_tests="$(cargo test --locked -p librqbit-dht --lib --no-default-features --features sha1-ring -- --list)"
  if ! grep -Fxq "$dispatch_test: test" <<<"$dispatch_tests"; then
    echo "maintained rqbit DHT dispatch proof was not discovered" >&2
    exit 1
  fi
  cargo test --locked -p librqbit-dht --lib --no-default-features \
    --features sha1-ring "$dispatch_test" -- --exact
)

check_retry_and_sha1() (
  cd "$source_dir"
  readonly retry_test='retry::tests::'
  retry_tests="$(cargo test --locked -p librqbit-core --lib --no-default-features --features sha1-ring -- --list)"
  for exact_test in \
    retry::tests::peer_schedule_preserves_growth_jitter_and_base_cap \
    retry::tests::bootstrap_fractional_growth_matches_original_nanosecond_vectors \
    retry::tests::elapsed_budget_includes_work_and_sleep_and_reset_starts_over; do
    grep -Fxq "$exact_test: test" <<<"$retry_tests" || { echo "missing retry policy proof: $exact_test" >&2; exit 1; }
  done
  cargo test --locked -p librqbit-core --lib --no-default-features --features sha1-ring "$retry_test"
  readonly sha_test='tests::sha1_protocol_vectors_match_for_whole_and_segmented_input'
  sha_tests="$(cargo test --locked -p librqbit-sha1-wrapper --lib -- --list)"
  grep -Fxq "$sha_test: test" <<<"$sha_tests" || { echo "missing SHA-1 protocol proof" >&2; exit 1; }
  cargo test --locked -p librqbit-sha1-wrapper --lib "$sha_test" -- --exact
  cargo test --locked -p librqbit-sha1-wrapper --lib --no-default-features --features sha1-ring "$sha_test" -- --exact
  protocol_tests="$(cargo test --locked -p librqbit-peer-protocol --lib --no-default-features --features sha1-ring -- --list)"
  grep -Fxq 'tests::test_handshake_serialize: test' <<<"$protocol_tests" || { echo "missing bincode peer wire fixture" >&2; exit 1; }
  cargo test --locked -p librqbit-peer-protocol --lib --no-default-features --features sha1-ring
)

if [[ "${RQBIT_SERIES_RETRY_SHA1_ONLY:-0}" == "1" ]]; then
  check_retry_and_sha1
  exit 0
fi

if [[ "${RQBIT_SERIES_DISPATCH_ONLY:-0}" == "1" ]]; then
  check_retry_and_sha1
  check_dht_dispatch
  echo "rqbit fast lane: maintained derivation and release DHT dispatch passed"
  exit 0
fi

check_retry_and_sha1

(
  cd "$source_dir"
  cargo fmt --all -- --check

  librqbit_tests="$(cargo test --locked -p librqbit --lib --no-default-features --features rust-tls -- --list)"
  tracker_tests="$(cargo test --locked -p librqbit-tracker-comms --lib --no-default-features --features sha1-ring -- --list)"
  dht_tests="$(cargo test --locked -p librqbit-dht --lib --no-default-features --features sha1-ring -- --list)"
  readonly librqbit_tests tracker_tests dht_tests

  readonly -a expected_librqbit_tests=(
    'tests::persistence::persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset'
    'torrent_state::live::peer_semaphore_tests::per_torrent_limit_is_exact'
    'torrent_state::live::peer_semaphore_tests::session_limit_is_shared_between_torrents'
    'torrent_state::live::peer_semaphore_tests::waiting_peer_resumes_after_session_permit_is_released'
    'session::tests::pending_handshake_budget_blocks_and_resumes_listener_accepts'
    'torrent_state::live::peers::known_peer_budget_tests::per_torrent_limit_is_exact_and_released'
    'torrent_state::live::peers::known_peer_budget_tests::session_limit_is_shared_between_torrents'
    'torrent_state::live::peers::known_peer_budget_tests::peer_records_hold_slots_until_removed'
    'torrent_state::live::peers::known_peer_budget_tests::failed_shared_acquisition_releases_the_local_slot'
    'torrent_state::live::peers::known_peer_budget_tests::alternate_outgoing_address_is_queued_once_with_record_handle'
    'peer_connection::peer_response_writer_tests::production_writer_holds_permit_until_socket_write_finishes'
    'torrent_state::live::peer_response_budget_tests::production_admission_spans_scheduler_and_writer_queues'
    'dht_utils::tests::production_metadata_queues_close_at_exact_boundaries'
    'peer_info_reader::tests::configured_metadata_limit_is_checked_before_allocation'
    'torrent_state::initializing::tests::file_sizing_error_stops_initialization'
    'torrent_state::live::pex_policy_tests::session_pex_toggle_and_private_flag_are_both_authoritative'
    'peer_connection::client_version_tests::configured_client_version_replaces_the_engine_default'
    'peer_connection::client_version_tests::client_version_is_not_part_of_serialized_options'
    'session::client_identity_tests::tracker_downloaded_counts_fetched_payload_not_existing_data'
    'session::client_identity_tests::session_http_client_sends_the_configured_user_agent'
  )
  for exact_test in "${expected_librqbit_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$librqbit_tests"; then
      echo "maintained rqbit proof test was not discovered: $exact_test" >&2
      exit 1
    fi
  done
  for exact_test in \
    'tracker_comms::tests::http_tracker_response_is_bounded_and_timed' \
    'tracker_comms::tests::hostile_tracker_intervals_are_clamped' \
    'tracker_comms::tests::announce_url_keeps_the_trackers_own_query' \
    'tracker_comms::tests::lifecycle_sends_started_until_accepted_then_completed_once' \
    'tracker_comms::tests::lifecycle_never_reports_completed_for_a_session_that_started_complete' \
    'tracker_comms::tests::observation_needs_a_live_incomplete_session_before_completion' \
    'tracker_comms::tests::failed_announces_back_off' \
    'tracker_comms::tests::logged_tracker_endpoints_omit_passkeys' \
    'tracker_comms::tests::udp_events_use_bep15_codes' \
    'tracker_comms::tests::http_response_tracker_id_is_parsed' \
    'tracker_comms::tests::http_announces_follow_the_tracker_lifecycle' \
    'tracker_comms::tests::a_rejected_completed_waits_for_the_retry_interval' \
    'tracker_comms::tests::peer_lookups_send_no_lifecycle_events' \
    'tracker_comms::tests::a_restarted_session_announces_started_after_the_previous_stopped' \
    'tracker_comms::tests::udp_announces_follow_the_same_lifecycle'
  do
    if ! grep -Fxq "$exact_test: test" <<<"$tracker_tests"; then
      echo "maintained rqbit tracker proof was not discovered: $exact_test" >&2
      exit 1
    fi
  done
  if ! grep -Fxq 'dht::queue_budget_tests::queue_budgets_close_at_exact_boundaries: test' <<<"$dht_tests"; then
    echo "maintained rqbit DHT pressure proof was not discovered" >&2
    exit 1
  fi

  cargo test --locked -p librqbit --lib --no-default-features --features rust-tls
  cargo test --locked -p librqbit-tracker-comms --lib --no-default-features --features sha1-ring
  cargo test --locked -p librqbit-dht --lib --no-default-features --features sha1-ring
  check_dht_dispatch
  # Validate maintained library APIs without executing the unshipped stock
  # CLI web UI build script (npm install) or the desktop packaging stack.
  cargo check --locked --workspace --exclude rqbit-desktop --exclude rqbit
)

echo "rqbit maintained patch series: contracts and locked upstream tests passed"
