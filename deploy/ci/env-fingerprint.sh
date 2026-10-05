#!/usr/bin/env bash
# v0.7: Agent OS runtime environment fingerprint.
# Prints a stable key=value document (one pair per line) capturing the host
# and pinned runtime toolchain. CI logs it and saves it as an artifact so
# every runtime test run is reproducible against an equal Linux host.
#
# Keys are stable and emitted in a fixed order; unknown values are reported
# as "unknown" or "not-installed" instead of failing, so the fingerprint is
# always comparable across runs.
set -u

echo "fingerprint.schema=1"
echo "host.uname=$(uname -srmo 2>/dev/null || echo unknown)"
echo "host.kernel=$(uname -r 2>/dev/null || echo unknown)"
echo "host.arch=$(uname -m 2>/dev/null || echo unknown)"

if command -v containerd >/dev/null 2>&1; then
  echo "containerd.version=$(containerd --version 2>/dev/null | sed 's/^containerd //' || echo unknown)"
else
  echo "containerd.version=not-installed"
fi

if command -v ctr >/dev/null 2>&1; then
  echo "ctr.version=$(ctr --version 2>/dev/null | sed 's/^ctr //' || echo unknown)"
else
  echo "ctr.version=not-installed"
fi

if command -v runsc >/dev/null 2>&1; then
  echo "runsc.version=$(runsc --version 2>/dev/null | head -n 1 || echo unknown)"
else
  echo "runsc.version=not-installed"
fi

if [ -e /dev/kvm ]; then
  echo "kvm.device=present"
else
  echo "kvm.device=absent"
fi

if command -v firecracker >/dev/null 2>&1; then
  echo "firecracker.version=$(firecracker --version 2>/dev/null | head -n 1 || echo unknown)"
else
  echo "firecracker.version=not-installed"
fi

echo "oci.namespace=${FENCED_OCI_CONTAINERD_NAMESPACE:-unset}"
echo "oci.image=${FENCED_OCI_IMAGE:-unset}"
echo "oci.containerd_address=${CONTAINERD_ADDRESS:-unset}"
echo "oci.runtime_config=${FENCED_OCI_RUNTIME_CONFIG:-unset}"
echo "oci.snapshotter=${FENCED_OCI_SNAPSHOTTER:-unset}"
