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
    readonly patch_file="contrib/rqbit/0014-bound-discovery-pressure.patch"
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
    readonly patch_file="contrib/rqbit/0015-bound-discovery-pressure-main.patch"
    readonly source_variant="main"
    ;;
esac

echo "verifying rqbit $source_variant discovery-pressure patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-discovery-pressure.XXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT

git clone --quiet --no-hardlinks "$source_dir" "$work_dir/rqbit"
if git -C "$work_dir/rqbit" apply --check "$repository_root/$patch_file"; then
  git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"
elif [[ "$source_variant" == "main" ]]; then
  echo "direct apply drifted; attempting a three-way apply against $source_head" >&2
  git -C "$work_dir/rqbit" apply --3way "$repository_root/$patch_file"
else
  echo "stable v8.1.1 patch no longer applies to its pinned base" >&2
  exit 1
fi

readonly dht_source="$work_dir/rqbit/crates/dht/src/dht.rs"
readonly metadata_source="$work_dir/rqbit/crates/librqbit/src/dht_utils.rs"

extract_rust_item() {
  local signature="$1"
  local source="$2"
  awk -v signature="$signature" '
    index($0, signature) { printing = 1 }
    printing {
      print
      opens = gsub(/{/, "{")
      closes = gsub(/}/, "}")
      depth += opens - closes
      if (opens > 0) { saw_body = 1 }
      if (saw_body && depth == 0) { exit }
    }
  ' "$source"
}

for invariant in \
  'MAX_PENDING_DHT_SENDS: usize = 256' \
  'MAX_PENDING_DISCOVERED_PEERS: usize = 256' \
  'MAX_PENDING_RECURSIVE_NODES: usize = 256' \
  'MAX_CONCURRENT_RECURSIVE_REQUESTS: usize = 32' \
  'MAX_PENDING_MAINTENANCE_REQUESTS: usize = 256' \
  'MAX_CONCURRENT_MAINTENANCE_REQUESTS: usize = 32' \
  'struct BoundedFuturesUnordered' \
  'RootQueueStatus::Saturated' \
  'fn try_send_worker(&self, request: WorkerSendRequest)' \
  '.reserve()' \
  'queue_budgets_close_at_exact_boundaries'
do
  if ! grep -Fq "$invariant" "$dht_source"; then
    echo "discovery-pressure patch is missing DHT invariant: $invariant" >&2
    exit 1
  fi
done

for removed_invariant in 'MAX_CONCURRENT_BOOTSTRAPS' 'buffer_unordered('; do
  if grep -Fq "$removed_invariant" "$dht_source"; then
    echo "discovery-pressure patch retains the starvation-prone bootstrap cap: $removed_invariant" >&2
    exit 1
  fi
done

if grep -Fq 'unbounded_channel' "$dht_source"; then
  echo "DHT source still contains an unbounded channel" >&2
  exit 1
fi

request_one_source="$(extract_rust_item 'async fn request_one(' "$dht_source")"
readonly request_one_source
if [[ "$(grep -Fc '.request(' <<<"$request_one_source")" -ne 1 ]]; then
  echo "recursive request_one must issue exactly one DHT request" >&2
  exit 1
fi
for invariant in '.node_tx.try_send(' '.peer_tx.try_send('; do
  if ! grep -Fq "$invariant" <<<"$request_one_source"; then
    echo "recursive request_one is missing non-blocking production traversal: $invariant" >&2
    exit 1
  fi
done

readonly bounded_window_source="$(extract_rust_item 'struct BoundedFuturesUnordered' "$dht_source")"
if [[ -z "$bounded_window_source" ]]; then
  echo "bounded future window type could not be extracted" >&2
  exit 1
fi
if ! grep -Fq 'Send once: callbacks and traversal must consume the same response.' \
  <<<"$request_one_source"; then
  echo "recursive request_one is missing its single-send ownership invariant" >&2
  exit 1
fi

for invariant in \
  'MAX_CONCURRENT_METADATA_PEERS: usize = 128' \
  'MAX_PENDING_METADATA_PEERS: usize = 256' \
  'MAX_METADATA_PEER_CANDIDATES: usize = 4096' \
  'struct MetadataPeerQueues' \
  'CandidateAdmission::Untracked' \
  'production_metadata_queues_close_at_exact_boundaries'
do
  if ! grep -Fq "$invariant" "$metadata_source"; then
    echo "discovery-pressure patch is missing metadata invariant: $invariant" >&2
    exit 1
  fi
done

if [[ "$source_variant" == "main" ]]; then
  readonly lsd_source="$work_dir/rqbit/crates/librqbit_lsd/src/lib.rs"
  for invariant in \
    'MAX_PENDING_LSD_PEERS: usize = 256' \
    'struct AnnounceLifecycle' \
    'spawn_announce_task(' \
    'self.cancel_token.cancel()' \
    'production_queue_and_announce_lifecycle_are_bounded'
  do
    if ! grep -Fq "$invariant" "$lsd_source"; then
      echo "discovery-pressure patch is missing LSD invariant: $invariant" >&2
      exit 1
    fi
  done
fi

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check

  readonly dht_test='dht::queue_budget_tests::queue_budgets_close_at_exact_boundaries'
  readonly metadata_test='dht_utils::tests::production_metadata_queues_close_at_exact_boundaries'

  dht_test_list="$(cargo test -p librqbit-dht --lib -- --list)"
  readonly dht_test_list
  if ! grep -Fxq "$dht_test: test" <<<"$dht_test_list"; then
    echo "DHT queue-budget proof test was not discovered: $dht_test" >&2
    exit 1
  fi

  metadata_test_list="$(cargo test -p librqbit --lib -- --list)"
  readonly metadata_test_list
  if ! grep -Fxq "$metadata_test: test" <<<"$metadata_test_list"; then
    echo "metadata candidate-budget proof test was not discovered: $metadata_test" >&2
    exit 1
  fi

  cargo test -p librqbit-dht --lib "$dht_test" -- --exact --nocapture
  cargo test -p librqbit --lib "$metadata_test" -- --exact --nocapture

  if [[ "$source_variant" == "main" ]]; then
    readonly lsd_test='queue_budget_tests::production_queue_and_announce_lifecycle_are_bounded'
    lsd_test_list="$(cargo test -p librqbit-lsd --lib -- --list)"
    readonly lsd_test_list
    if ! grep -Fxq "$lsd_test: test" <<<"$lsd_test_list"; then
      echo "LSD queue/lifecycle proof test was not discovered: $lsd_test" >&2
      exit 1
    fi
    cargo test -p librqbit-lsd --lib "$lsd_test" -- --exact --nocapture
  fi
  # The desktop member requires host WebKit/GTK development packages. This
  # verifier is intentionally headless, but still compiles every affected
  # library, CLI, example, and test workspace member.
  # Preserve library and CLI compile coverage; stock web UI is not part of
  # these contribution proofs and must not run its npm installation hook.
  cargo check --workspace --exclude rqbit-desktop --exclude rqbit
  cli_features="$(python3 "$repository_root/scripts/rqbit-cli-check-features.py" crates/rqbit/Cargo.toml)"
  cargo check -p rqbit --all-targets --no-default-features --features "$cli_features"
)
