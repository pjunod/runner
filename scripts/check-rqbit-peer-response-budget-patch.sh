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
    readonly patch_file="contrib/rqbit/0012-bound-peer-response-backlog.patch"
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
    readonly patch_file="contrib/rqbit/0013-bound-peer-response-backlog-main.patch"
    readonly source_variant="main"
    ;;
esac

echo "verifying rqbit $source_variant peer-response-budget patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-peer-response-budget.XXXXXX")"
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

readonly peer_connection_source="$work_dir/rqbit/crates/librqbit/src/peer_connection.rs"
readonly live_source="$work_dir/rqbit/crates/librqbit/src/torrent_state/live/mod.rs"

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
  'MAX_PENDING_PEER_RESPONSES_PER_PEER: usize = 128' \
  'struct PeerResponseBudget' \
  'fn try_acquire(&self)' \
  'handshake.reqq = Some(MAX_PENDING_PEER_RESPONSES_PER_PEER as u32)' \
  'production_admission_spans_scheduler_and_writer_queues'
do
  if ! grep -Fq "$invariant" "$live_source"; then
    echo "peer-response-budget live path is missing invariant: $invariant" >&2
    exit 1
  fi
done

readonly download_request_source="$(extract_rust_item 'fn on_download_request(' "$live_source")"
readonly metadata_response_source="$(extract_rust_item 'fn send_metadata_piece(' "$live_source")"
readonly upload_scheduler_source="$(extract_rust_item 'async fn task_upload_scheduler(' "$live_source")"
readonly socket_writer_source="$(extract_rust_item 'async fn write_with_peer_response_permit(' "$peer_connection_source")"

if ! grep -Fq 'enqueue_piece_response(' <<<"$download_request_source"; then
  echo "production on_download_request does not use bounded peer-response admission" >&2
  exit 1
fi
if grep -Fq '.await' <<<"$download_request_source"; then
  echo "production on_download_request may park the socket reader" >&2
  exit 1
fi
if ! grep -Fq 'enqueue_metadata_response(' <<<"$metadata_response_source"; then
  echo "production metadata response does not share bounded peer-response admission" >&2
  exit 1
fi
if ! grep -Fq 'forward_piece_response(' <<<"$upload_scheduler_source"; then
  echo "production upload scheduler bypasses permit-carrying writer forwarding" >&2
  exit 1
fi
for invariant in 'let _peer_response_permit = peer_response_permit' 'write.write_all(bytes)'; do
  if ! grep -Fq "$invariant" <<<"$socket_writer_source"; then
    echo "socket writer does not retain the peer-response permit through write: $invariant" >&2
    exit 1
  fi
done
if ! grep -Fq 'production_writer_holds_permit_until_socket_write_finishes' "$peer_connection_source"; then
  echo "peer-response writer lifetime proof is missing" >&2
  exit 1
fi

case "$source_variant" in
  stable)
    if ! grep -Fq 'MessageWithPermit(MessageOwned, OwnedSemaphorePermit)' "$peer_connection_source"; then
      echo "stable writer request does not carry a metadata response permit" >&2
      exit 1
    fi
    if ! grep -Fq 'send(WriterRequest::MessageWithPermit(message, permit))' "$live_source"; then
      echo "stable metadata enqueue does not attach its response permit" >&2
      exit 1
    fi
    ;;
  main)
    if ! grep -Fq 'UtMetadata(UtMetadata<ByteBufOwned>, OwnedSemaphorePermit)' "$peer_connection_source"; then
      echo "main writer request does not carry a metadata response permit" >&2
      exit 1
    fi
    if ! grep -Fq 'send(WriterRequest::UtMetadata(message, permit))' "$live_source"; then
      echo "main metadata enqueue does not attach its response permit" >&2
      exit 1
    fi
    ;;
esac

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  readonly admission_test='torrent_state::live::peer_response_budget_tests::production_admission_spans_scheduler_and_writer_queues'
  readonly writer_test='peer_connection::peer_response_writer_tests::production_writer_holds_permit_until_socket_write_finishes'
  test_list="$(cargo test -p librqbit --lib -- --list)"
  readonly test_list
  for exact_test in "$admission_test" "$writer_test"; do
    if ! grep -Fxq "$exact_test: test" <<<"$test_list"; then
      echo "peer-response-budget proof test was not discovered: $exact_test" >&2
      exit 1
    fi
    cargo test -p librqbit --lib "$exact_test" -- --exact --nocapture
  done
  # The desktop member requires host WebKit/GTK development packages. This
  # verifier is intentionally headless, but still compiles every affected
  # library, CLI, example, and test workspace member.
  # Preserve library and CLI compile coverage; stock web UI is not part of
  # these contribution proofs and must not run its npm installation hook.
  cargo check --workspace --exclude rqbit-desktop --exclude rqbit
  cli_features="$(python3 "$repository_root/scripts/rqbit-cli-check-features.py" crates/rqbit/Cargo.toml)"
  cargo check -p rqbit --all-targets --no-default-features --features "$cli_features"
)
