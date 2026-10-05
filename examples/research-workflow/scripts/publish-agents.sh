#!/usr/bin/env bash
# Publish the eight research agent manifests (planner, search, reader,
# collector, analyst, critic, writer, citation-validator) as immutable
# AgentVersions. Model placeholders in the manifests are substituted with the
# deployment's model reference first.
#
# Usage: scripts/publish-agents.sh [control-api-endpoint]
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." )"
AGENTS_DIR="$EXAMPLE_DIR/agents"
ENDPOINT="${1:-${FENCED_CONTROL_API:-http://127.0.0.1:8080}}"
MODEL_REF="${MODEL_REF:-openai/gpt-4o-mini}"

REPO_ROOT="$(cd "$EXAMPLE_DIR/../.." && pwd)"
FENCED="$REPO_ROOT/cmd/fenced"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

for manifest in "$AGENTS_DIR"/*.json; do
  name="$(basename "$manifest" .json)"          # planner, citation-validator...
  published="research-$name"                    # research-planner, ...
  target="$STAGE/$published.json"
  sed -e "s#__MODEL_FAST__#$MODEL_REF#g" \
      -e "s#__MODEL_READER__#$MODEL_REF#g" \
      -e "s#__MODEL_REASONING__#$MODEL_REF#g" \
      "$manifest" > "$target"
  echo "[publish-agents] publishing $published@1.0.0"
  go run "$FENCED" publish -endpoint "$ENDPOINT" -manifest "$target" \
    -idempotency-key "research-$published-1.0.0"
done

echo "[publish-agents] done: eight agent versions published to $ENDPOINT"
