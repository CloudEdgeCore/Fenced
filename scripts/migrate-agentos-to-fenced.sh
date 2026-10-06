#!/usr/bin/env bash
# migrate-agentos-to-fenced.sh
#
# Rewrites AgentOS-era identifiers in a configuration, deployment, or source
# tree to their Fenced equivalents. Use it to migrate manifests, environment
# files, Helm values, and CI definitions after upgrading to a Fenced build.
#
#   AGENTOS_*            -> FENCED_*
#   AgentOS              -> Fenced
#   Agentos              -> Fenced
#   agentos              -> fenced
#
# Usage:
#   ./scripts/migrate-agentos-to-fenced.sh [--dry-run] [--quiet] [PATH]
#
#   --dry-run   Report the files that would change without writing anything.
#   --quiet     Only print the final summary.
#   PATH        Directory or file to migrate. Defaults to the current directory.
#
# Exit codes: 0 = success (including "nothing to do"), 1 = usage error.
#
# Notes:
#   - Binary files are skipped (grep -I).
#   - .git, node_modules, and __pycache__ are excluded.
#   - The script rewrites file *contents*. It does not rename directories or
#     binaries; see docs/COMPATIBILITY.md for what must change in a deployment.
#   - Always run --dry-run first on a tree you care about.

set -euo pipefail

DRY_RUN=0
QUIET=0
TARGET=""

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --quiet)   QUIET=1; shift ;;
    -h|--help)
      sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    -*) echo "unknown option: $1" >&2; exit 1 ;;
    *)  TARGET="$1"; shift ;;
  esac
done

[ -n "$TARGET" ] || TARGET="."
if [ ! -e "$TARGET" ]; then
  echo "migrate-agentos-to-fenced: path not found: $TARGET" >&2
  exit 1
fi

EXCLUDES=(--exclude-dir=.git --exclude-dir=node_modules --exclude-dir=__pycache__ --exclude-dir=vendor)

files=()
while IFS= read -r line; do
  files+=("$line")
done < <(
  grep -rlI "${EXCLUDES[@]}" \
    -e 'agentos' -e 'AgentOS' -e 'AGENTOS' -e 'Agentos' \
    "$TARGET" 2>/dev/null || true
)

total=${#files[@]}
if [ "$total" -eq 0 ]; then
  echo "migrate-agentos-to-fenced: nothing to migrate under $TARGET"
  exit 0
fi

if [ "$DRY_RUN" -eq 1 ]; then
  echo "migrate-agentos-to-fenced: DRY RUN, $total file(s) would change under $TARGET"
else
  echo "migrate-agentos-to-fenced: rewriting $total file(s) under $TARGET"
fi
echo

hits_total=0
for f in "${files[@]}"; do
  hits=$(grep -oI -e 'agentos' -e 'AgentOS' -e 'AGENTOS' -e 'Agentos' "$f" 2>/dev/null | wc -l | tr -d ' ')
  hits_total=$((hits_total + hits))
  if [ "$QUIET" -eq 0 ]; then
    printf '  %5s  %s\n' "$hits" "$f"
  fi
  if [ "$DRY_RUN" -eq 0 ]; then
    sed -i \
      -e 's/AgentOS/Fenced/g' \
      -e 's/AGENTOS/FENCED/g' \
      -e 's/Agentos/Fenced/g' \
      -e 's/agentos/fenced/g' \
      "$f"
  fi
done

echo
if [ "$DRY_RUN" -eq 1 ]; then
  echo "summary: $total file(s), $hits_total replacement(s) pending (dry run, nothing written)"
else
  echo "summary: $total file(s), $hits_total replacement(s) applied"
  echo
  echo "next steps:"
  echo "  1. review the diff (git diff)"
  echo "  2. rename AGENTOS_* variables in your secret store and CI settings"
  echo "  3. redeploy: gRPC clients and servers must both be rebuilt, because the"
  echo "     protobuf package changed and the wire protocol is not backward compatible"
fi
