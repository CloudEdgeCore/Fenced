#!/usr/bin/env bash
# rebuild-and-verify.sh
#
# Rebuilds both ends of the Fenced wire contract from one regenerated set of
# protobuf stubs, then proves they agree.
#
# Why this exists: renaming the protobuf package changed every gRPC method name
# and type URL. A client and a server built from different generations of the
# stubs cannot talk to each other, and the mismatch is invisible until run time.
# Run this after pulling the rename, in CI, and before a release.
#
# Steps:
#   1. buf generate        regenerate gen/ from proto/
#   2. go build ./...      rebuild every binary (client and server side)
#   3. wire guard test     assert no descriptor still carries a legacy name
#   4. compat tests        assert legacy inputs are still accepted
#   5. conformance suite   black-box check that a client and an adapter agree
#
# Usage:
#   ./scripts/rebuild-and-verify.sh
#   ./scripts/rebuild-and-verify.sh --skip-conformance
#   ./scripts/rebuild-and-verify.sh --conformance-cmd "python3 path/to/server.py --port 0"
#
# Exit code 0 means every end was rebuilt from the same stubs and they agree.

set -euo pipefail

SKIP_CONFORMANCE=0
CONFORMANCE_CMD=""

while [ $# -gt 0 ]; do
  case "$1" in
    --skip-conformance) SKIP_CONFORMANCE=1; shift ;;
    --conformance-cmd)  CONFORMANCE_CMD="${2:-}"; shift 2 ;;
    -h|--help) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "rebuild-and-verify: unknown option: $1" >&2; exit 1 ;;
  esac
done

cd "$(dirname "$0")/.."

step() { printf '\n==> %s\n' "$1"; }

command -v go >/dev/null 2>&1 || { echo "rebuild-and-verify: go is required" >&2; exit 1; }
command -v buf >/dev/null 2>&1 || { echo "rebuild-and-verify: buf is required (https://buf.build)" >&2; exit 1; }

step "1/5 regenerate protobuf stubs (buf generate)"
buf generate

step "2/5 rebuild every binary (go build ./...)"
go build ./...

step "3/5 wire guard: no descriptor may carry a legacy name"
go test ./api/compatibility/... -run WireNames -count=1

step "4/5 compatibility guard: legacy inputs must still be accepted"
go test ./internal/platform/compat/... ./internal/kernel/agentversion/... -count=1

if [ "$SKIP_CONFORMANCE" -eq 1 ]; then
  step "5/5 conformance suite (skipped)"
else
  step "5/5 conformance suite: client and adapter must agree on the wire"
  if [ -z "$CONFORMANCE_CMD" ]; then
    if command -v python3 >/dev/null 2>&1; then
      PYTHON=python3
    else
      PYTHON=python
    fi
    CONFORMANCE_CMD="$PYTHON examples/agents/python_remote/server.py --port 0"
  fi
  # Prepend the SDK path: an existing PYTHONPATH must be preserved, and Windows
  # Python uses ';' as the separator while POSIX uses ':'.
  SEP=":"
  case "$(uname -s 2>/dev/null || echo unknown)" in
    MINGW*|MSYS*|CYGWIN*) SEP=";" ;;
  esac
  if [ -n "${PYTHONPATH:-}" ]; then
    PYTHONPATH="./sdk/python${SEP}${PYTHONPATH}"
  else
    PYTHONPATH="./sdk/python"
  fi
  export PYTHONPATH
  go run ./cmd/fenced-conformance -cmd "$CONFORMANCE_CMD" -timeout 40s
fi

printf '\nOK: both ends rebuilt from the same stubs and verified consistent.\n'
