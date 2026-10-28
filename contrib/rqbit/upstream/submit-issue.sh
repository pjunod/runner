#!/usr/bin/env bash
# Usage: submit-issue.sh <10|11|13> [--dry-run]
# Review and edit the issue yourself, as rqbit's AI policy requires, before sending.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY_RUN=0
UNIT=
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    10|11|13) [ -z "$UNIT" ] || { echo 'give one issue number' >&2; exit 2; }; UNIT=$arg ;;
    *) echo 'usage: submit-issue.sh <10|11|13> [--dry-run]' >&2; exit 2 ;;
  esac
done
[ -n "$UNIT" ] || { echo 'give an issue number: 10, 11 or 13' >&2; exit 2; }
case "$UNIT" in 10) SLUG=discovery-health ;; 11) SLUG=disable-pex ;; 13) SLUG=tracker-lifecycle ;; esac
FILE="$HERE/issues/$UNIT-$SLUG.md"
TITLE="$(sed -n '1s/^# //p' "$FILE")"
[ -n "$TITLE" ] || { echo "missing issue title in $FILE" >&2; exit 1; }
if [ "$DRY_RUN" -eq 1 ]; then
  printf 'Would open on ikatson/rqbit: %s\nBody: %s\n' "$TITLE" "$FILE"
  exit 0
fi
[ "$(gh api user --jq .login)" = pjunod ] || { echo 'gh must be authenticated as pjunod' >&2; exit 1; }
# Search by author, then compare titles exactly. Closed issues count as existing:
# a rerun must not silently open a duplicate after maintainers close a discussion.
EXISTING="$(gh issue list --repo ikatson/rqbit --author pjunod --state all --limit 1000 --json title,url | \
  python3 -c 'import json,sys; print(next((x["url"] for x in json.load(sys.stdin) if x["title"] == sys.argv[1]), ""))' "$TITLE")"
if [ -n "$EXISTING" ]; then printf '%s\n' "$EXISTING"; exit 0; fi
WORK="${RQBIT_SUBMIT_WORKDIR:-${XDG_CACHE_HOME:-$HOME/.cache}/rqbit-upstream-submit}"
mkdir -p "$WORK"
BODY="$(mktemp "$WORK/issue-body.XXXXXX")"
trap 'rm -f "$BODY"' EXIT
sed '1{/^# /d;}' "$FILE" > "$BODY"
gh issue create --repo ikatson/rqbit --title "$TITLE" --body-file "$BODY"
