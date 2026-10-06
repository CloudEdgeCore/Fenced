#!/usr/bin/env bash
# v0.7: run the OCI/gVisor real-machine leg from a clean Linux host (root).
# Used by the runtime-linux-leg CI job's local equivalent and by self-hosted
# KVM runners. Assumes a checkout at the current directory, Go on PATH, and
# FENCED_TEST_DATABASE_URL pointing at a reachable PostgreSQL.
set -euo pipefail

: "${FENCED_TEST_DATABASE_URL:?FENCED_TEST_DATABASE_URL is required}"
: "${FENCED_OCI_CONTAINERD_NAMESPACE:=fenced-ci}"
# Pinned runtime toolchain; CI overrides these with the approved matrix.
: "${CONTAINERD_VERSION:=2.2.7}"
: "${CONTAINERD_SHA256:=91cd216ea26a1b8b512219d3f205375c967e7b7de4dae571bc3dd16bfacd34b5}"
: "${RUNSC_TAG:=20260810.0}"
export CONTAINERD_VERSION CONTAINERD_SHA256 RUNSC_TAG
# Overlayfs on production hosts; nested containers (Docker Desktop/WSL2)
# cannot mount overlay-on-overlay and must use the native snapshotter.
: "${SNAPSHOTTER:=overlayfs}"
export SNAPSHOTTER

# Install and start the pinned containerd + runsc when the host does not
# already have a ready daemon (first bring-up path).
if ! ctr version >/dev/null 2>&1; then
  bash deploy/ci/setup-containerd-runsc.sh
fi

# Environment fingerprint: every runtime result must be reproducible against
# the same fingerprint.
bash deploy/ci/env-fingerprint.sh | tee /tmp/runtime-fingerprint.txt

# Build the provider and put it on PATH (the conformance worker is spawned
# by the test through exec.LookPath).
go build -o /usr/local/bin/fenced-runtime-oci ./cmd/fenced-runtime-oci

# The conformance worker runs with --skip-image-pull, so the workload image
# must already live in the namespace. The default is immutable so a local run
# validates the same workload bytes as CI.
if [ -z "${FENCED_OCI_IMAGE:-}" ]; then
  ctr -n "$FENCED_OCI_CONTAINERD_NAMESPACE" images pull \
    --snapshotter "$SNAPSHOTTER" docker.io/library/alpine@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
  export FENCED_OCI_IMAGE="docker.io/library/alpine@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b"
fi
export FENCED_OCI_SNAPSHOTTER="$SNAPSHOTTER"
export FENCED_OCI_RUNTIME_CONFIG="${RUNSC_CONFIG_PATH:-/etc/containerd/runsc.toml}"

bash deploy/ci/runtime-isolation.sh

# Run the OCI/gVisor conformance leg on the real containerd + runsc host.
go test -tags=integration -count=1 \
  -run 'TestSameAgentVersionRunsOnBothProviders/oci' \
  ./internal/runtime/control/...
