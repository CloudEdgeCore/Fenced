#!/usr/bin/env bash
# v0.7: install and start a pinned containerd + runsc on a Linux host for the
# OCI/gVisor real-machine leg. Must run as root (GitHub Actions: sudo).
#
# Pinned sources:
#   - containerd release tarball from GitHub Releases;
#   - the complete gVisor release tarball from GCS, including runsc, the
#     containerd shim, and gvisor-bin sidecars. gVisor releases after 2026-07
#     are not valid binary-only installations.
set -euo pipefail

# Every network or daemon-facing command below is bounded: a wedged registry
# connection or a stuck sandbox must fail loudly (with the timeout's stderr in
# the step log) instead of hanging until the CI job timeout kills the step.
command -v timeout >/dev/null 2>&1 || { echo "coreutils timeout is required" >&2; exit 1; }

CONTAINERD_VERSION="${CONTAINERD_VERSION:?set a pinned containerd release (e.g. 2.2.7)}"
CONTAINERD_SHA256="${CONTAINERD_SHA256:?set the approved containerd archive SHA-256}"
RUNSC_TAG="${RUNSC_TAG:?set a pinned gVisor release tag (e.g. 20260810.0)}"
GVISOR_ARCH="$(uname -m)"
RUNSC_BASE="https://storage.googleapis.com/gvisor/releases/release/${RUNSC_TAG}/${GVISOR_ARCH}"

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) ARCH="amd64" ;;
  aarch64) ARCH="arm64" ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# Host facts recorded for later diagnosis: runsc sandbox creation needs clone()
# with namespace flags and (for the ptrace/systrap platforms) ptrace access.
# A host that blocks either (seccomp filter, userns sysctl) will hang the
# sandbox silently — these lines document that constraint in the job log.
echo ">> host kernel: $(uname -r)"
echo ">> unprivileged_userns_clone=$(cat /proc/sys/kernel/unprivileged_userns_clone 2>/dev/null || echo n/a) max_user_namespaces=$(cat /proc/sys/user/max_user_namespaces 2>/dev/null || echo n/a)"
echo ">> seccomp=$(awk '/^Seccomp:/{print $2}' /proc/self/status 2>/dev/null || echo n/a) (0=disabled,2=filter)"
echo ">> apparmor=$(cat /sys/module/apparmor/parameters/enabled 2>/dev/null || echo n/a)"

# --- containerd ------------------------------------------------------------
# Runner images may ship containerd (e.g. 2.3.3 on ubuntu-24.04). The pinned
# version must win, so install when missing or when the installed binary is
# not the pinned one. `containerd --version` prints
# "containerd github.com/containerd/containerd/v2 v2.2.7 ..." — the third
# field is the version.
INSTALLED_CONTAINERD="$(containerd --version 2>/dev/null | awk '{print $3}' || true)"
if [ "$INSTALLED_CONTAINERD" != "v${CONTAINERD_VERSION}" ]; then
  echo ">> installing containerd ${CONTAINERD_VERSION} (${ARCH}); installed: ${INSTALLED_CONTAINERD:-none}"
  timeout 300 curl -fsSL -o "$TMPDIR/containerd.tar.gz" \
    "https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}-linux-${ARCH}.tar.gz"
  echo "${CONTAINERD_SHA256}  $TMPDIR/containerd.tar.gz" | sha256sum -c -
  tar -C /usr/local -xzf "$TMPDIR/containerd.tar.gz"
else
  echo ">> containerd ${CONTAINERD_VERSION} already installed"
fi
containerd --version

# --- complete gVisor release -------------------------------------------------
echo ">> installing complete gVisor ${RUNSC_TAG} release"
timeout 300 curl -fsSL -o "$TMPDIR/gvisor.tar.bz2" "${RUNSC_BASE}/gvisor.tar.bz2"
timeout 120 curl -fsSL -o "$TMPDIR/gvisor.tar.bz2.sha512" "${RUNSC_BASE}/gvisor.tar.bz2.sha512"
(cd "$TMPDIR" && sha512sum -c gvisor.tar.bz2.sha512)
tar -xjf "$TMPDIR/gvisor.tar.bz2" -C /usr/local/bin
for required in /usr/local/bin/runsc /usr/local/bin/containerd-shim-runsc-v1; do
  if [ ! -x "$required" ]; then
    echo "gVisor release is missing executable: $required" >&2
    exit 1
  fi
done
if [ ! -d /usr/local/bin/gvisor-bin ] || ! find /usr/local/bin/gvisor-bin -maxdepth 1 -type f -perm /111 -print -quit | grep -q .; then
  echo "gVisor release is missing executable gvisor-bin sidecars" >&2
  exit 1
fi
RUNSC_VERSION_OUTPUT="$(runsc --version 2>&1 || true)"
echo "$RUNSC_VERSION_OUTPUT"
if ! grep -Fq "release-${RUNSC_TAG}" <<<"$RUNSC_VERSION_OUTPUT"; then
  echo "installed runsc does not match pinned release ${RUNSC_TAG}" >&2
  exit 1
fi

# --- gVisor shim + containerd configuration ---------------------------------
mkdir -p /etc/containerd
RUNSC_CONFIG_PATH="${RUNSC_CONFIG_PATH:-/etc/containerd/runsc.toml}"
RUNSC_PLATFORM="${RUNSC_PLATFORM:-systrap}"
RUNSC_NETWORK="${RUNSC_NETWORK:-none}"
RUNSC_SYSTEMD_CGROUP="${RUNSC_SYSTEMD_CGROUP:-false}"
RUNSC_IGNORE_CGROUPS="${RUNSC_IGNORE_CGROUPS:-false}"
RUNSC_OCI_SECCOMP="${RUNSC_OCI_SECCOMP:-true}"
CONTAINERD_SOCKET_GID="${CONTAINERD_SOCKET_GID:-${SUDO_GID:-0}}"
RUNSC_LOG_DIR="/var/log/fenced-runsc"
mkdir -p "$RUNSC_LOG_DIR"

# The gVisor shim captures `runsc create` output with an os/exec pipe. The
# long-lived gofer and sandbox inherit that pipe, so Cmd.Wait cannot observe
# EOF after the short-lived create process exits. This adapter redirects only
# create output; every other command remains an exact runsc exec.
cat > /usr/local/bin/fenced-runsc <<'EOF'
#!/bin/sh

printf 'pid=%s argv=' "$$" >>/var/log/fenced-runsc/launcher.log
printf ' <%s>' "$@" >>/var/log/fenced-runsc/launcher.log
printf '\n' >>/var/log/fenced-runsc/launcher.log

case " $* " in
  *" create "*) exec /usr/local/bin/runsc "$@" >>/var/log/fenced-runsc/runsc.create.log 2>&1 ;;
  *) exec /usr/local/bin/runsc "$@" ;;
esac
EOF
chmod 0755 /usr/local/bin/fenced-runsc

cat > "$RUNSC_CONFIG_PATH" <<EOF
binary_name = "/usr/local/bin/fenced-runsc"
log_path = "${RUNSC_LOG_DIR}/shim.log"
log_level = "debug"

[runsc_config]
  platform = "${RUNSC_PLATFORM}"
  network = "${RUNSC_NETWORK}"
  systemd-cgroup = "${RUNSC_SYSTEMD_CGROUP}"
  ignore-cgroups = "${RUNSC_IGNORE_CGROUPS}"
  oci-seccomp = "${RUNSC_OCI_SECCOMP}"
  debug = "true"
  # runsc expands %COMMAND%, giving create/gofer/boot dedicated logs without
  # relying on shim-specific path interpolation.
  debug-log = "${RUNSC_LOG_DIR}/gvisor.%COMMAND%.log"
EOF

# Validate the pinned runtime itself before involving containerd. This keeps a
# host incompatibility distinct from a shim/OCI integration failure.
echo ">> running direct runsc probe (bounded 30s)"
mkdir -p /run/fenced-runsc-direct
if ! timeout --signal=KILL 30 runsc \
  --root=/run/fenced-runsc-direct \
  --platform="$RUNSC_PLATFORM" \
  --network=none \
  --ignore-cgroups=true \
  --debug=true \
  --debug-log="${RUNSC_LOG_DIR}/direct.%COMMAND%.log" \
  do /bin/true </dev/null; then
  echo ">> direct runsc probe: FAIL" >&2
  find "$RUNSC_LOG_DIR" -maxdepth 4 -type f -print -exec tail -n 120 {} \; >&2 || true
  exit 1
fi
echo ">> direct runsc probe: PASS"

cat > /etc/containerd/config.toml <<EOF
version = 3

[debug]
  level = "info"

[grpc]
  gid = ${CONTAINERD_SOCKET_GID}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc.options]
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "${RUNSC_CONFIG_PATH}"
EOF

# --- start containerd and wait for readiness -------------------------------
# Run an isolated daemon so the host's Docker/system containerd state cannot
# win a socket race or leak stale shims into this acceptance leg.
CONTAINERD_ADDRESS="${CONTAINERD_ADDRESS:-/run/fenced-containerd/containerd.sock}"
CONTAINERD_ROOT="${CONTAINERD_ROOT:-/var/lib/fenced-containerd}"
CONTAINERD_STATE="${CONTAINERD_STATE:-/run/fenced-containerd}"
export CONTAINERD_ADDRESS
mkdir -p "$CONTAINERD_ROOT" "$CONTAINERD_STATE"
echo ">> starting isolated containerd ${CONTAINERD_VERSION} at ${CONTAINERD_ADDRESS}"
nohup containerd \
  --address "$CONTAINERD_ADDRESS" \
  --root "$CONTAINERD_ROOT" \
  --state "$CONTAINERD_STATE" \
  --config /etc/containerd/config.toml \
  >/tmp/fenced-containerd.log 2>&1 &
CONTAINERD_PID=$!
for attempt in $(seq 1 30); do
  if timeout 5 ctr version >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$CONTAINERD_PID" 2>/dev/null; then
    echo "isolated containerd exited during startup; log:" >&2
    cat /tmp/fenced-containerd.log >&2 || true
    exit 1
  fi
  sleep 1
  if [ "$attempt" -eq 30 ]; then
    echo "containerd did not become ready; log:" >&2
    tail -n 50 /tmp/fenced-containerd.log >&2 || true
    exit 1
  fi
done
echo ">> containerd ready"
VERSION_OUTPUT="$(ctr version 2>&1 || true)"
if ! echo "$VERSION_OUTPUT" | grep -q "v${CONTAINERD_VERSION}"; then
  echo "containerd on the socket is not the pinned ${CONTAINERD_VERSION}; full version output:" >&2
  echo "$VERSION_OUTPUT" >&2
  exit 1
fi
echo ">> containerd on socket is pinned ${CONTAINERD_VERSION}"

# --- verify the runsc runtime end to end ------------------------------------
SNAPSHOTTER="${SNAPSHOTTER:-overlayfs}"
PROBE_IMAGE="${PROBE_IMAGE:-docker.io/library/busybox@sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0}"
echo ">> pulling digest-pinned probe image (bounded 300s)"
timeout 300 ctr -n "${FENCED_OCI_CONTAINERD_NAMESPACE:-fenced-ci}" images pull \
  --snapshotter "$SNAPSHOTTER" "$PROBE_IMAGE" >/dev/null
echo ">> running fail-closed runsc probe (bounded 120s)"
# Keep the sandbox alive past shim watcher attachment. An immediate /bin/true
# can exit successfully before runsc wait connects, losing the zero exit status
# and producing a false status=128 result on the pinned gVisor shim.
set +e
timeout --signal=KILL 120 ctr -n "${FENCED_OCI_CONTAINERD_NAMESPACE:-fenced-ci}" run \
  --rm \
  --runtime io.containerd.runsc.v1 \
  --runtime-config-path "$RUNSC_CONFIG_PATH" \
  --snapshotter "$SNAPSHOTTER" \
  "$PROBE_IMAGE" fenced-runtime-probe \
  /bin/sleep 1 \
  </dev/null \
  >/tmp/probe-out.log 2>&1
PROBE_STATUS=$?
set -e
if [ "$PROBE_STATUS" -ne 0 ]; then
  echo ">> runsc runtime probe: FAIL status=${PROBE_STATUS} (the pinned matrix must be re-validated)" >&2
  echo ">> probe output:" >&2
  cat /tmp/probe-out.log 2>/dev/null >&2 || true
  echo ">> requesting shim goroutine dump:" >&2
  while read -r shim_pid; do
    [ -n "$shim_pid" ] || continue
    kill -12 "$shim_pid" 2>/dev/null || true
  done < <(pgrep -f 'containerd-shim-runsc-v1.*fenced-runtime-probe' || true)
  sleep 2
  echo ">> runsc/shim processes:" >&2
  # Keep ps here because state and wait-channel are part of the diagnostics.
  # shellcheck disable=SC2009
  ps -eo pid,ppid,user,stat,wchan,cmd | grep -E 'runsc|shim|ctr' | grep -v grep >&2 || true
  echo ">> runsc/shim file descriptors:" >&2
  while read -r runtime_pid; do
    [ -n "$runtime_pid" ] || continue
    echo "process ${runtime_pid}:" >&2
    ls -l "/proc/${runtime_pid}/fd" 2>/dev/null >&2 || true
    for fd_path in "/proc/${runtime_pid}/fd/3" "/proc/${runtime_pid}/fd/4"; do
      [ -L "$fd_path" ] || continue
      echo "tail ${fd_path} -> $(readlink "$fd_path" 2>/dev/null || true):" >&2
      timeout 2 tail -n 120 "$fd_path" 2>/dev/null >&2 || true
    done
  done < <(pgrep -f 'containerd-shim-runsc-v1|runsc-(gofer|sandbox)' || true)
  echo ">> runsc/shim logs:" >&2
  ls -la "$RUNSC_LOG_DIR" >&2 || true
  for runtime_log in \
    launcher.log shim.log runsc.create.log \
    gvisor.create.log gvisor.boot.log gvisor.start.log \
    gvisor.wait.log gvisor.state.log gvisor.kill.log gvisor.delete.log; do
    [ -f "$RUNSC_LOG_DIR/$runtime_log" ] || continue
    echo "--- $runtime_log" >&2
    tail -n 80 "$RUNSC_LOG_DIR/$runtime_log" >&2 || true
  done
  echo ">> containerd log tail:" >&2
  tail -n 100 /tmp/fenced-containerd.log >&2 || true
  echo ">> dmesg tail:" >&2
  dmesg 2>/dev/null | tail -n 30 >&2 || true
  exit 1
fi
echo ">> runsc runtime probe: PASS (snapshotter=${SNAPSHOTTER}, platform=${RUNSC_PLATFORM})"
