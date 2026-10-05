#!/usr/bin/env bash
# Stop the research deployment's background processes (gateway, control,
# orchestrator, controller, outbox, runtime adapters). Data in PostgreSQL is
# preserved; drop the schema manually if you also want a data wipe.
set -euo pipefail

echo "[cleanup] stopping Fenced research processes"
for pattern in \
  fenced-gateway fenced-control$ fenced-orchestrator \
  fenced-controller fenced-outbox fenced-runtime-adapter; do
  pkill -f "$pattern" 2>/dev/null && echo "  stopped $pattern" || true
done
echo "[cleanup] done"
