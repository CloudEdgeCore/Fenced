#!/usr/bin/env bash
# Inject a failure into the running research deployment (design doc §15).
#
# Every injection targets the local reference stack that bootstrap.sh starts:
#
#   inject-failure.sh kill-worker [research-worker-00]
#       SIGKILL a runtime adapter mid-flight; lease-expiry recovery requeues
#       its attempts and a surviving/restarted instance finishes them.
#
#   inject-failure.sh kill-reader [research-worker-00]
#       Same mechanism, documented intent: kill the worker hosting readers.
#       (Readers share the adapter fleet, so the process target is a worker.)
#
#   inject-failure.sh runtime-disconnect [research-worker-01]
#       SIGSTOP a worker: the gRPC connection drops without releasing leases,
#       so the lease expires and recovery reclaims the attempts (a crash whose
#       process stays alive).
#
#   inject-failure.sh model-429 [seconds]
#       SIGSTOP the model provider process for N seconds (default 5): in-flight
#       and new model calls stall, the provider times out, and bounded retries
#       absorb the failure. Matches the provider pattern
#       (vllm|sglang|ollama|lmdeploy|model-provider) so local OpenAI-compatible
#       servers are hit; requires the provider to run on the same host.
#
#   inject-failure.sh tool-500 [seconds]
#       SIGSTOP the webtools webhook process for N seconds (default 5): tool
#       invocations time out at the gateway and the agent's retry policy
#       absorbs the failure. Also accepts "kill" to SIGKILL the webhook
#       process (connection refused -> TOOL_ENDPOINT_HTTP_5xx).
#
#   inject-failure.sh sse-reset [seconds]
#       SIGSTOP the Control API process for N seconds (default 5), then
#       SIGCONT: event streams stall and the operator terminal reconnects;
#       the SSE contract says clients reconcile with GET after reconnect.
#
#   inject-failure.sh cordon <pool-id>     drain a pool via the durable
#       registry so placement stops there without killing leases.
#   inject-failure.sh uncordon <pool-id>   re-admit the pool.
#
# Observe the §15 recovery chain after every injection:
#   failure injected -> attempt failed -> recovery -> new attempt ->
#   workflow continued
set -euo pipefail

KIND="${1:?usage: inject-failure.sh crash|kill-worker|kill-reader|runtime-disconnect|model-429|tool-500|sse-reset|cordon|uncordon [target]}"
TARGET="${2:-}"

observe() {
  echo "[observe] failure injected -> attempt failed -> recovery -> new attempt -> workflow continued"
}

case "$KIND" in
  crash|kill-worker|kill-reader)
    INSTANCE="${TARGET:-research-worker-00}"
    echo "[inject] SIGKILL runtime adapter $INSTANCE"
    pkill -9 -f "runtime-instance-id $INSTANCE" || {
      echo "[inject] no process matched '$INSTANCE'"; exit 1; }
    observe
    ;;
  runtime-disconnect)
    INSTANCE="${TARGET:-research-worker-01}"
    echo "[inject] SIGSTOP runtime adapter $INSTANCE (connection drops, leases held until expiry)"
    pkill -STOP -f "runtime-instance-id $INSTANCE" || {
      echo "[inject] no process matched '$INSTANCE'"; exit 1; }
    observe
    ;;
  model-429)
    SECONDS="${TARGET:-5}"
    PATTERN='vllm|sglang|ollama|lmdeploy|model-provider'
    echo "[inject] SIGSTOP model provider for ${SECONDS}s (quota-like stall)"
    pkill -STOP -f "$PATTERN" || {
      echo "[inject] no local model provider matched '$PATTERN'; this host must run the provider" >&2; exit 1; }
    sleep "$SECONDS"
    pkill -CONT -f "$PATTERN" || true
    observe
    ;;
  tool-500)
    ARG="${TARGET:-5}"
    PATTERN='tools/cmd/webtools|webtools'
    if [ "$ARG" = "kill" ]; then
      echo "[inject] SIGKILL webtools webhook (connection refused -> 5xx)"
      pkill -9 -f "$PATTERN" || { echo "[inject] no webtools process matched '$PATTERN'"; exit 1; }
    else
      echo "[inject] SIGSTOP webtools webhook for ${ARG}s (tool timeout)"
      pkill -STOP -f "$PATTERN" || { echo "[inject] no webtools process matched '$PATTERN'"; exit 1; }
      sleep "$ARG"
      pkill -CONT -f "$PATTERN" || true
    fi
    observe
    ;;
  sse-reset)
    SECONDS="${TARGET:-5}"
    echo "[inject] SIGSTOP Control API for ${SECONDS}s (SSE stalls), then SIGCONT"
    pkill -STOP -f 'fenced-control' || {
      echo "[inject] no fenced-control process matched"; exit 1; }
    sleep "$SECONDS"
    pkill -CONT -f 'fenced-control' || true
    echo "[inject] reconnect the event stream; clients must reconcile with GET /v1/tasks/{id}"
    observe
    ;;
  cordon|uncordon)
    POOL="${TARGET:?pool id required}"
    STATUS="$([ "$KIND" = cordon ] && echo CORDONED || echo ACTIVE)"
    : "${DATABASE_URL:?set DATABASE_URL for direct registry updates}"
    echo "[inject] setting pool $POOL status=$STATUS"
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
      -c "UPDATE runtime_pools SET status='$STATUS', updated_at=now() WHERE id='$POOL'"
    ;;
  *)
    echo "unknown failure kind: $KIND" >&2; exit 2;;
esac
