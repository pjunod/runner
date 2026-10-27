#!/usr/bin/env bash
# Submit one (or all) of the prepared rqbit pull requests.
#
#   contrib/rqbit/upstream/submit.sh [--dry-run] [--draft] <slug|all>
#
# For each slug this script:
#   1. fetches the current upstream main (ikatson/rqbit) into a private cache clone;
#   2. skips the slug if a PR from pjunod:up/<slug> already exists (any state);
#   3. forks ikatson/rqbit to pjunod/rqbit if the fork is missing;
#   4. creates branch up/<slug> on that fresh upstream main and applies NN-<slug>.patch
#      with `git am --3way` (it stops with a clear message if the patch no longer applies);
#   5. pushes the branch to the fork;
#   6. opens the PR with NN-<slug>.md as the body, the commit subject as the title.
#
# known-peer-limit is stacked on session-peer-limit and is refused until the
# session-peer-limit PR has been merged upstream.
#
# --dry-run does everything up to and including step 4 and nothing that talks to
# GitHub except the read-only fetch of upstream main: no gh calls, no fork, no
# push, no PR. For a stacked slug it applies the prerequisite patch first so the
# apply path can still be exercised.
# --draft opens the PR as a draft.
#
# Requirements: git, and (not in --dry-run) gh authenticated as pjunod.
# Environment overrides (for testing): RQBIT_UPSTREAM_URL, RQBIT_UPSTREAM_BRANCH,
# RQBIT_SUBMIT_WORKDIR.
set -euo pipefail

UPSTREAM_REPO="ikatson/rqbit"
FORK_OWNER="pjunod"
FORK_REPO="rqbit"
UPSTREAM_URL="${RQBIT_UPSTREAM_URL:-https://github.com/${UPSTREAM_REPO}.git}"
UPSTREAM_BRANCH="${RQBIT_UPSTREAM_BRANCH:-main}"
WORKDIR="${RQBIT_SUBMIT_WORKDIR:-${XDG_CACHE_HOME:-$HOME/.cache}/rqbit-upstream-submit}"
CLONE="$WORKDIR/rqbit"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Submission order. known-peer-limit depends on session-peer-limit.
SLUGS=(
  metadata-size-limit
  file-sizing-errors
  tracker-request-bounds
  disable-auto-restore
  session-peer-limit
  peer-response-budget
  known-peer-limit
  dht-queue-bounds
  metadata-resolver-bounds
)

prereq_of() {
  case "$1" in
    known-peer-limit) echo session-peer-limit ;;
    *) echo "" ;;
  esac
}

DRY_RUN=0
DRAFT=0
TARGET=""

die() { echo "error: $*" >&2; exit 1; }
info() { echo "==> $*"; }

usage() { sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --draft) DRAFT=1 ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) [ -z "$TARGET" ] || die "only one slug (or 'all') may be given"; TARGET="$1" ;;
  esac
  shift
done
[ -n "$TARGET" ] || { usage; exit 2; }

command -v git >/dev/null || die "git is required"
if [ "$DRY_RUN" -eq 0 ]; then
  command -v gh >/dev/null || die "gh is required (or use --dry-run)"
  gh_user="$(gh api user --jq .login 2>/dev/null)" || die "gh is not authenticated (run: gh auth login)"
  [ "$gh_user" = "$FORK_OWNER" ] || die "gh is authenticated as '$gh_user', expected '$FORK_OWNER'"
fi

slug_known() { local s; for s in "${SLUGS[@]}"; do [ "$s" = "$1" ] && return 0; done; return 1; }
patch_of() { local f; f="$(ls "$HERE"/[0-9][0-9]-"$1".patch 2>/dev/null | head -n1)"; [ -n "$f" ] || die "no patch file for '$1' in $HERE"; echo "$f"; }
md_of() { local f; f="$(ls "$HERE"/[0-9][0-9]-"$1".md 2>/dev/null | head -n1)"; [ -n "$f" ] || die "no PR description for '$1' in $HERE"; echo "$f"; }

export GIT_COMMITTER_NAME="Paul Junod"
export GIT_COMMITTER_EMAIL="donujp@gmail.com"

ensure_clone() {
  mkdir -p "$WORKDIR"
  if [ ! -d "$CLONE/.git" ]; then
    info "cloning $UPSTREAM_URL into $CLONE"
    # A full clone, not a partial one: pushing from a partial clone to the fork needs objects it does not have.
    git clone --quiet --no-checkout "$UPSTREAM_URL" "$CLONE"
  fi
  git -C "$CLONE" remote set-url origin "$UPSTREAM_URL"
  git -C "$CLONE" fetch --quiet origin "+refs/heads/${UPSTREAM_BRANCH}:refs/remotes/origin/${UPSTREAM_BRANCH}"
  BASE_SHA="$(git -C "$CLONE" rev-parse "origin/${UPSTREAM_BRANCH}")"
  info "upstream ${UPSTREAM_BRANCH} is at ${BASE_SHA}"
}

fresh_branch() { # $1 = branch name
  git -C "$CLONE" am --abort >/dev/null 2>&1 || true
  git -C "$CLONE" reset --quiet --hard 2>/dev/null || true
  git -C "$CLONE" clean --quiet -fdx 2>/dev/null || true
  git -C "$CLONE" checkout --quiet -B "$1" "origin/${UPSTREAM_BRANCH}"
}

apply_patch() { # $1 = patch file, $2 = slug (for the message)
  if ! git -C "$CLONE" am --quiet --3way "$1" >/dev/null 2>&1; then
    git -C "$CLONE" am --abort >/dev/null 2>&1 || true
    die "$2: the patch no longer applies to upstream ${UPSTREAM_BRANCH} (${BASE_SHA}). Rebase it by hand (it was cut against f4ca7d919638a7b8b031ab062a61861a9eb586e4) and regenerate the .patch before submitting."
  fi
}

# gh's --head filter takes a bare branch name (no owner:), so filter on the fork owner afterwards.
pr_url_for_head() { # prints the URL of any PR (open/merged/closed) from the fork branch, or nothing
  gh pr list --repo "$UPSTREAM_REPO" --head "$1" --state all --json url,headRepositoryOwner \
    --jq "[.[] | select(.headRepositoryOwner.login == \"${FORK_OWNER}\")][0].url // empty"
}

pr_merged() { # $1 = fork branch
  [ "$(gh pr list --repo "$UPSTREAM_REPO" --head "$1" --state merged --json number,headRepositoryOwner \
        --jq "[.[] | select(.headRepositoryOwner.login == \"${FORK_OWNER}\")] | length")" -gt 0 ]
}

ensure_fork() {
  if gh repo view "${FORK_OWNER}/${FORK_REPO}" >/dev/null 2>&1; then
    local parent
    parent="$(gh repo view "${FORK_OWNER}/${FORK_REPO}" --json parent \
      --jq 'if .parent then (.parent.owner.login + "/" + .parent.name) else "" end')"
    [ "$parent" = "$UPSTREAM_REPO" ] || die "${FORK_OWNER}/${FORK_REPO} exists but is not a fork of ${UPSTREAM_REPO}"
  else
    info "forking ${UPSTREAM_REPO} to ${FORK_OWNER}/${FORK_REPO}"
    gh repo fork "$UPSTREAM_REPO" --clone=false
    local i
    for i in 1 2 3 4 5 6; do
      gh repo view "${FORK_OWNER}/${FORK_REPO}" >/dev/null 2>&1 && return 0
      sleep 5
    done
    die "fork ${FORK_OWNER}/${FORK_REPO} did not appear"
  fi
}

push_branch() { # $1 = branch
  local i
  for i in 1 2 3; do
    if git -C "$CLONE" -c credential.helper= -c "credential.helper=!gh auth git-credential" \
         push --quiet --force "https://github.com/${FORK_OWNER}/${FORK_REPO}.git" "HEAD:refs/heads/$1"; then
      return 0
    fi
    info "push failed (attempt $i), retrying"
    sleep 5
  done
  die "could not push $1 to ${FORK_OWNER}/${FORK_REPO}"
}

# Returns 0 = done/skipped, 3 = deferred (prerequisite not merged).
submit_one() {
  local slug="$1" branch patch md prereq title
  branch="up/${slug}"
  patch="$(patch_of "$slug")"
  md="$(md_of "$slug")"
  prereq="$(prereq_of "$slug")"

  echo
  info "[$slug] patch $(basename "$patch"), description $(basename "$md")"

  [ -s "$md" ] || die "$slug: description $md is empty"
  grep -q 'AI disclosure' "$md" || die "$slug: description has no AI disclosure section"
  if grep -qE 'TODO|FIXME|\{\{' "$md"; then die "$slug: description still has placeholders"; fi

  if [ "$DRY_RUN" -eq 0 ]; then
    local existing
    existing="$(pr_url_for_head "$branch")"
    if [ -n "$existing" ]; then
      info "[$slug] a PR from ${FORK_OWNER}:${branch} already exists: $existing (skipping)"
      return 0
    fi
    if [ -n "$prereq" ] && ! pr_merged "up/${prereq}"; then
      echo "[$slug] REFUSING: it is stacked on '${prereq}', whose PR (from ${FORK_OWNER}:up/${prereq}) is not merged upstream yet." >&2
      echo "[$slug] Submit '${prereq}' first, wait for it to merge, then run this again." >&2
      return 3
    fi
  fi

  ensure_clone
  fresh_branch "$branch"

  if [ -n "$prereq" ] && [ "$DRY_RUN" -eq 1 ]; then
    info "[$slug] dry-run: not asking GitHub whether '${prereq}' is merged; applying its patch first to exercise the stacked apply path"
    if git -C "$CLONE" am --quiet --3way "$(patch_of "$prereq")" >/dev/null 2>&1; then
      info "[$slug] dry-run: prerequisite '${prereq}' applied on ${BASE_SHA}"
    else
      git -C "$CLONE" am --abort >/dev/null 2>&1 || true
      info "[$slug] dry-run: prerequisite does not apply (probably already in upstream); applying '${slug}' on upstream directly"
    fi
  fi

  apply_patch "$patch" "$slug"
  title="$(git -C "$CLONE" log -1 --format=%s)"
  info "[$slug] applied cleanly on ${BASE_SHA}: $title"
  git -C "$CLONE" log -1 --format='    author: %an <%ae>%n    committer: %cn <%ce>'
  git -C "$CLONE" show --shortstat --format= HEAD | sed 's/^/    /'

  if [ "$DRY_RUN" -eq 1 ]; then
    info "[$slug] dry-run: would fork (if missing), push ${branch} to ${FORK_OWNER}/${FORK_REPO}, and open a PR titled: $title"
    return 0
  fi

  ensure_fork
  push_branch "$branch"
  local draft_flag=""
  if [ "$DRAFT" -eq 1 ]; then draft_flag="--draft"; fi
  local url
  # shellcheck disable=SC2086  # draft_flag is empty or a single flag
  url="$(gh pr create --repo "$UPSTREAM_REPO" --base "$UPSTREAM_BRANCH" --head "${FORK_OWNER}:${branch}" \
          --title "$title" --body-file "$md" $draft_flag)"
  info "[$slug] opened $url"
}

main() {
  local rc=0 slug status
  if [ "$TARGET" = "all" ]; then
    for slug in "${SLUGS[@]}"; do
      status=0
      submit_one "$slug" || status=$?
      case "$status" in
        0) ;;
        3) echo "[$slug] deferred" ;;
        *) rc=1 ;;
      esac
    done
  else
    slug_known "$TARGET" || die "unknown slug '$TARGET' (known: ${SLUGS[*]})"
    status=0
    submit_one "$TARGET" || status=$?
    [ "$status" -eq 0 ] || rc="$status"
  fi
  exit "$rc"
}
main
