#!/usr/bin/env bash
# Submit one research goal through the application-layer research API and
# follow it to completion (design doc §17 showcase). Requires bootstrap.sh to
# have started the research API and the control plane.
#
# Usage:
#   scripts/run-research.sh "What does the agent runtime market look like in 3 years?"
#
# Env:
#   FENCED_RESEARCH_API  research API endpoint (default http://127.0.0.1:9095)
set -euo pipefail

GOAL="${1:?usage: run-research.sh \"<research goal>\"}"
EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/..")"
REPO_ROOT="$(cd "$EXAMPLE_DIR/../.." && pwd)"
ENDPOINT="${FENCED_RESEARCH_API:-http://127.0.0.1:9095}"

echo "[run-research] submitting goal through the research API: $GOAL"
go run "$REPO_ROOT/cmd/fenced" research \
  -endpoint "$ENDPOINT" \
  -goal "$GOAL"
