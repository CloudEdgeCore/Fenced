#!/usr/bin/env bash
# resign-manifests.sh
#
# Re-signs agent manifests after the AgentOS → Fenced rename.
#
# Why this exists: the rename normalized legacy `agentos.*` identifiers to
# `fenced.*` inside DecodeManifest. A manifest published under the former name
# therefore has a different canonical form — and a different digest — than the
# bytes that were signed. Any signature or package-trust record covering the old
# digest no longer matches. Re-run this pipeline to produce packages signed over
# the canonical fenced.* form.
#
# Pipeline per manifest (the same order the control plane verifies):
#   1. validate-manifest  -> canonical AgentManifest JSON + digest
#   2. package-manifest   -> unsigned package manifest with provenance
#   3. sign               -> signed package JSON
#   4. verify             -> optional check against a trust key
#
# Usage:
#   ./scripts/resign-manifests.sh --manifest agent-manifest.json \
#       --key-id ci-builder-1 --private-key <base64> [--trust-key ci-builder-1=<base64pub>]
#
#   ./scripts/resign-manifests.sh --dir ./manifests --key-id ci-builder-1 \
#       --private-key <base64> --out-dir ./signed
#
# Options:
#   --manifest FILE       one AgentManifest JSON file
#   --dir DIR             every *.json directly inside DIR
#   --key-id ID           signing key identity (required)
#   --private-key B64     base64 raw std ed25519 private key (required)
#   --trust-key ID=B64PUB optional; when set, each signed package is verified
#   --builder STR         provenance builder      (default: resign-manifests)
#   --workflow STR        provenance workflow     (default: resign-manifests)
#   --git-commit STR      provenance git commit   (default: current HEAD)
#   --built-at RFC3339    provenance timestamp    (default: now, UTC)
#   --out-dir DIR         output directory        (default: alongside input)
#   --pkg PATH            prebuilt fenced-pkg binary (default: build from source)
#   --dry-run             print the plan, run nothing
#
# Exit code 0 means every manifest was re-signed (and verified when requested).

set -euo pipefail

MANIFEST=""
DIR=""
KEY_ID=""
PRIVATE_KEY=""
TRUST_KEY=""
BUILDER="resign-manifests"
WORKFLOW="resign-manifests"
GIT_COMMIT=""
BUILT_AT=""
OUT_DIR=""
PKG=""
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --manifest)    MANIFEST="${2:-}"; shift 2 ;;
    --dir)         DIR="${2:-}"; shift 2 ;;
    --key-id)      KEY_ID="${2:-}"; shift 2 ;;
    --private-key) PRIVATE_KEY="${2:-}"; shift 2 ;;
    --trust-key)   TRUST_KEY="${2:-}"; shift 2 ;;
    --builder)     BUILDER="${2:-}"; shift 2 ;;
    --workflow)    WORKFLOW="${2:-}"; shift 2 ;;
    --git-commit)  GIT_COMMIT="${2:-}"; shift 2 ;;
    --built-at)    BUILT_AT="${2:-}"; shift 2 ;;
    --out-dir)     OUT_DIR="${2:-}"; shift 2 ;;
    --pkg)         PKG="${2:-}"; shift 2 ;;
    --dry-run)     DRY_RUN=1; shift ;;
    -h|--help)     sed -n '2,43p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "resign-manifests: unknown option: $1" >&2; exit 1 ;;
  esac
done

cd "$(dirname "$0")/.."

die() { echo "resign-manifests: $*" >&2; exit 1; }

[ -n "$KEY_ID" ] || die "--key-id is required"
[ -n "$PRIVATE_KEY" ] || die "--private-key is required"
[ -n "$MANIFEST" ] || [ -n "$DIR" ] || die "one of --manifest or --dir is required"
[ -z "$MANIFEST" ] || [ -z "$DIR" ] || die "--manifest and --dir are mutually exclusive"

manifests=()
if [ -n "$MANIFEST" ]; then
  [ -f "$MANIFEST" ] || die "manifest not found: $MANIFEST"
  manifests+=("$MANIFEST")
else
  [ -d "$DIR" ] || die "directory not found: $DIR"
  while IFS= read -r f; do
    manifests+=("$f")
  done < <(find "$DIR" -maxdepth 1 -type f -name '*.json' | sort)
fi
[ "${#manifests[@]}" -gt 0 ] || die "no manifest JSON files found"

[ -n "$BUILT_AT" ] || BUILT_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
[ -n "$GIT_COMMIT" ] || GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"

exe_suffix() {
  case "$(uname -s 2>/dev/null || echo unknown)" in
    MINGW*|MSYS*|CYGWIN*) echo ".exe" ;;
    *) echo "" ;;
  esac
}

# Work files live next to the repository root rather than in $TMPDIR: on
# Windows/Git Bash, TMPDIR is a drive-prefixed path (C:\...) that mixes badly
# with POSIX tooling. The trap removes the directory on every exit path.
WORK="$(pwd)/.resign-work.$$"
mkdir -p "$WORK"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT INT TERM

if [ "$DRY_RUN" -eq 0 ] && [ -z "$PKG" ]; then
  command -v go >/dev/null 2>&1 || die "go is required to build fenced-pkg"
  echo "building fenced-pkg..."
  PKG="$WORK/fenced-pkg$(exe_suffix)"
  go build -o "$PKG" ./cmd/fenced-pkg
fi

echo "resign-manifests: ${#manifests[@]} manifest(s), key-id=$KEY_ID, built-at=$BUILT_AT"

status=0
for manifest in "${manifests[@]}"; do
  base="$(basename "$manifest" .json)"
  target_dir="${OUT_DIR:-$(dirname "$manifest")}"
  signed="$target_dir/$base.package.json"

  echo
  echo "== $manifest"
  echo "   -> $signed"

  if [ "$DRY_RUN" -eq 1 ]; then
    echo "   [dry-run] would validate, package, sign${TRUST_KEY:+, verify}"
    continue
  fi

  mkdir -p "$target_dir"
  "$PKG" validate-manifest -manifest "$manifest" -out "$WORK/$base.canonical.json"
  "$PKG" package-manifest \
    -agent-manifest "$manifest" \
    -builder "$BUILDER" -workflow "$WORKFLOW" \
    -git-commit "$GIT_COMMIT" -built-at "$BUILT_AT" \
    -out "$WORK/$base.unsigned.json"
  "$PKG" sign -manifest "$WORK/$base.unsigned.json" \
    -key-id "$KEY_ID" -private-key "$PRIVATE_KEY" -out "$signed"
  if [ -n "$TRUST_KEY" ]; then
    "$PKG" verify -package "$signed" -trust-key "$TRUST_KEY"
  fi
  echo "   signed: $signed"
done

echo
if [ "$DRY_RUN" -eq 1 ]; then
  echo "summary: dry run, nothing written"
else
  echo "summary: re-signed ${#manifests[@]} manifest(s); signatures now cover the canonical fenced.* form"
  echo "next: publish the packages and update your trust registry if the digest changed"
fi
exit "$status"
