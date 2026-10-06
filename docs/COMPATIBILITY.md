# Compatibility: running a pre-rename deployment against Fenced

> **中文摘要**：本文说明 AgentOS → Fenced 改名后，**哪些旧输入仍被自动接受**（环境变量、旧 manifest、旧执行头、`agentos` 命令），以及**哪些必须手动处理**（gRPC wire 不兼容、数据库里存的旧标识、已签名 manifest 的身份变化）。配套工具：`scripts/migrate-agentos-to-fenced.sh` / `.ps1`，兼容代码在 `internal/platform/compat`。完整改名映射见 [NAMING.md](NAMING.md)。

The rename changed environment variable prefixes and protocol identifiers. This
document states exactly what a pre-rename deployment can keep doing without
changes, and what it must do before upgrading.

Implementation: [`internal/platform/compat`](../internal/platform/compat/compat.go).

## 1. Accepted automatically (no action required)

| Legacy input | How it keeps working |
| --- | --- |
| `AGENTOS_*` environment variables | Every shipped binary and every entry point under `examples/` calls `compat.AliasLegacyEnv()` at start-up, which copies `AGENTOS_*` to the matching `FENCED_*` name when that name is not already set. A deprecation notice is printed to stderr. |
| Manifests using `agentos.*` identifiers | `agentversion.DecodeManifest` calls `NormalizeLegacyIdentifiers()` before validation, so `apiVersion: agentos.dev/v1`, `interface: agentos.runtime.interface/v1`, and `runtimeABI: agentos.oci/v1` are rewritten to their `fenced.*` form and the manifest loads. |
| `X-Agentos-Execution` request header | The MCP server reads the current header first and falls back to the legacy one. |
| The `agentos` command | `cmd/agentos` is a shim that forwards to the corresponding `fenced` binary (`agentos control …` → `fenced-control …`, anything else → `fenced …`). It prints a deprecation notice. |

Precedence rules for environment variables:

- `FENCED_X` wins over `AGENTOS_X`.
- An explicit empty `FENCED_X=""` also wins; the legacy value is not copied.
- Aliasing is idempotent and per-process.

## 2. NOT compatible (you must act)

### 2.1 gRPC and protobuf wire protocol

The protobuf package was renamed from `agentos.*` to `fenced.*`. This changes
every gRPC full method name (`/agentos.runtime.v1.Runtime/…` →
`/fenced.runtime.v1.Runtime/…`) and every protobuf type URL. A client built
before the rename **cannot** talk to a server built after it, and vice versa.

There is no alias for this. **Rebuild and redeploy both sides together.**
Regenerate any client stubs from `proto/fenced/`.

### 2.2 Persisted protocol identifiers

Anything already stored that embeds an `agentos.*` identifier is not recognized
by the new binary. Audit and migrate:

- checkpoint payloads that pin a runtime ABI or interface string;
- effect receipts and mailbox records that carry protocol identifiers;
- API-group values such as `agentos.dev/v1` in stored objects;
- `agent_version` rows whose `runtime_abi` / `interface` columns hold legacy values.

Re-register or re-publish affected agent versions so their stored identifiers
are canonical.

### 2.3 Signed manifests change identity

`DecodeManifest` computes the canonical JSON and SHA-256 **after** normalization.
A manifest published under the former name therefore has a different digest once
it is normalized. If you verified that digest against a signature or a package
trust key, the old signature no longer matches — **re-sign the manifest** in its
canonical `fenced.*` form:

```bash
# one manifest
./scripts/resign-manifests.sh --manifest agent-manifest.json \
    --key-id ci-builder-1 --private-key <base64> \
    --trust-key ci-builder-1=<base64pub>

# a directory of manifests, writing signed packages to ./signed
./scripts/resign-manifests.sh --dir ./manifests \
    --key-id ci-builder-1 --private-key <base64> --out-dir ./signed
```

The script runs the same pipeline the control plane verifies —
`validate-manifest` → `package-manifest` → `sign` → `verify` — so a legacy
manifest is normalized to `fenced.*` and the resulting signature covers the
canonical form. Add `--dry-run` to preview without writing.

### 2.4 The v1.2 contract freeze is superseded

The frozen identifiers were `agentos.*`. The rename constitutes a new contract
generation; see [contracts/v1.2-contract-freeze.md](contracts/v1.2-contract-freeze.md).

## 3. Migration procedure

### 3.1 Rebuild both ends together

The wire protocol is not aliased (section 2.1), so clients and servers must be
rebuilt from the same regenerated stubs and deployed together. One command runs
the whole cycle and proves the two ends agree:

```bash
./scripts/rebuild-and-verify.sh
```

It regenerates `gen/` with `buf generate`, rebuilds every binary, runs the wire
guard test and the compatibility tests, then finishes with the black-box
conformance suite (`Fenced Compatible = PASS`). Use `--skip-conformance` to stop
after the tests, or `--conformance-cmd "..."` to point at your own adapter.

### 3.2 Migrate configuration and deployment files

```bash
# 1. Preview the changes in your deployment tree (nothing is written).
./scripts/migrate-agentos-to-fenced.sh --dry-run ./deploy

# 2. Apply them.
./scripts/migrate-agentos-to-fenced.sh ./deploy

# 3. Review.
git diff
```

PowerShell equivalent:

```powershell
./scripts/migrate-agentos-to-fenced.ps1 -DryRun ./deploy
./scripts/migrate-agentos-to-fenced.ps1 ./deploy
```

The scripts rewrite file **contents** only.

A worked example of the whole cutover — systemd and Docker Compose, before and
after, including the safe rollout order — lives in
[`examples/migration/`](../examples/migration/README.md).

### 3.3 Re-sign manifests

```bash
./scripts/resign-manifests.sh --dir ./manifests \
    --key-id ci-builder-1 --private-key <base64> \
    --trust-key ci-builder-1=<base64pub> --out-dir ./signed
```

See section 2.3 for details.

### 3.4 Remaining manual steps

1. Rename `AGENTOS_*` variables in your secret store, CI settings, and systemd
   units. The alias layer keeps them working meanwhile, but it is deprecated.
2. Migrate persisted identifiers (section 2.2).
3. Rename any container entrypoints or wrapper scripts that invoke `agentos`.

## 4. Removing the compatibility layer

The shims are transitional. When every deployment has migrated:

1. drop `compat.AliasLegacyEnv()` calls from `cmd/*/main.go` and from the entry
   points under `examples/`;
2. remove `NormalizeLegacyIdentifiers()` from `agentversion.DecodeManifest`;
3. delete `cmd/agentos` and `internal/platform/compat`;
4. remove this document.

Do that in a major release, and announce the removal in `CHANGELOG.md`.
