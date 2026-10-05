# Runtime 版本矩阵与环境指纹(v0.7)

本文件是 v0.7 Linux Runtime 真机闭环的版本钉定契约与验收映射。原则:

- 所有组件钉定版本(沿用仓库既有 CI 的 digest 钉风格);首次 runner
  落地时由 `setup-containerd-runsc.sh` 输出下载产物的 sha256,回填下表
  "校验和"列后该行才算闭合。
- 每次 runtime 测试运行都必须附带环境指纹(见 §2),以便结果可复现到
  同等 Linux 主机。
- 每个隔离/能力项必须能映射到代码面与测试(正向或负向);无测试覆盖
  的项在真机腿落地前保持"待补"状态,不宣称已验证。

## 1. 版本矩阵

| 组件 | 钉定版本 | 安装来源 | 校验和(回填) | 状态 |
|---|---|---|---|---|
| containerd | `2.0.2` | GitHub Releases tarball(`containerd-2.0.2-linux-amd64.tar.gz`,URL 已验证) | `sha256:9bd5b6a1bdf505d520d9a329c520258ed0a17faa9fe3db12712ee858ad59aae3`(2026-08-16 实拉) | 已回填 |
| runsc(gVisor) | `20260810.0` | GCS `releases/release/20260810.0/x86_64/runsc`(URL 已验证,含 `.sha512`) | `.sha512` 校验通过(2026-08-16 实拉) | 已回填 |
| containerd-shim-runsc-v1 | `20260810.0` | GCS 同版本 `x86_64/containerd-shim-runsc-v1`(URL 已验证) | `.sha512` 校验通过(2026-08-16 实拉) | 已回填 |
| Firecracker | `v1.9.0` | GitHub Releases `firecracker-v1.9.0-x86_64.tgz`(URL 已验证) | 待首次运行回填 | 待验证(KVM 门控) |
| kernel | runner 默认(ubuntu-24.04 ≈ 6.8) | — | 指纹输出即记录 | 记录,不钉死 |
| Go | 1.26.6 | 既有 CI digest 钉 | 既有 | 已闭合 |
| Rust | 1.97.1 | 既有 CI 钉 | 既有 | 已闭合 |
| PostgreSQL | pgvector pg18(digest 钉) | 既有 CI | 既有 | 已闭合 |
| NATS | 2.14.1-alpine(digest 钉) | 既有 CI | 既有 | 已闭合 |

> 回填状态:containerd sha256 与 runsc/shim sha512 已于 2026-08-16 在
> Linux 容器实拉并校验回填。剩余回填项见 §5。

## 2. 环境指纹契约

`deploy/ci/env-fingerprint.sh` 输出稳定的 `key=value` 文档(固定键、固定
顺序;未知值报告 `unknown` / `not-installed`,永不失败)。CI 真机腿把它
写入日志并保存为构建产物(`runtime-fingerprint.txt`)。

键清单:`fingerprint.schema`、`host.uname`、`host.kernel`、`host.arch`、
`containerd.version`、`ctr.version`、`runsc.version`、`kvm.device`、
`firecracker.version`、`oci.namespace`、`oci.image`。

任何 runtime 测试失败都必须附带该指纹;同等指纹的两次运行结果应当一致
(版本矩阵闭合后)。

## 3. 隔离/能力项验收映射

| # | 项 | 代码面 | 显式参数? | 现有测试 | v0.7 真机动作 |
|---|---|---|---|---|---|
| 1 | CPU 限额 | `runsc_linux.go` Prepare `--cpu-quota`(微秒) | 是 | worker→executor 限额单测(纯逻辑) | ctr 命令面复核;conformance 腿验证限额生效 |
| 2 | 内存限额 | `--memory`(字节) | 是 | 同上 | 同上 |
| 3 | 工作区 tmpfs | `--mount type=tmpfs,size=` | 是 | 单测 | 复核尺寸生效 |
| 4 | 只读 rootfs | runsc 默认 profile + workload.json 只读 bind | 部分(只读 bind) | 无 | 新增负向测试(容器内写 rootfs 失败) |
| 5 | capability drop | runsc 默认 profile | 否 | 无 | 新增负向测试(提权调用失败,如 `capsh --print`) |
| 6 | seccomp | runsc 默认 profile | 否 | 无 | 新增负向测试(禁用的 syscall 失败) |
| 7 | user namespaces | runsc 默认 | 否 | 无 | 复核容器内 uid 映射 |
| 8 | cgroups | runsc 默认 | 否 | 无 | 复核 |
| 9 | 网络边界 | runsc netstack 默认 | 否 | 无 | 复核(无宿主网络泄漏) |
| 10 | 镜像 digest/签名/SBOM 准入 | worker + admission(ADR-010) | 是 | 单测/CLI 烟测 | conformance 腿用 digest 钉镜像跑通 |
| 11 | stdout spool | `spoolOutput` + `SpoolCap` | 是 | 单测(上限/截断/恰好满) | 真机复核 |
| 12 | 孤儿容器回收 | `reapOrphans` + active 注册表 | — | 单测(过滤逻辑) | 真机复核(崩溃 worker 留容器→下次 Prepare 清理) |
| 13 | 恢复链路(心跳/checkpoint/失联/接管/fencing) | runtime/control + recovery + leasekeeper | — | 集成套件(逻辑面) | 真机跑 security/recovery 套件(已在既有 CI job 于 Linux 执行) |

> 第 4–9 项当前依赖 runsc 默认 profile(未显式传参);真机复核后决定是否
> 需要把 seccomp/capability 显式编入 ctr 命令面(`ctr run` 原生支持
> `--cap-add/--cap-drop/--seccomp/--read-only`,记入 v0.7 决策点)。

## 3b. Bring-up 发现(2026-08-16,真实 Linux 内核容器实证)

以下发现来自在 privileged Linux 容器中执行 `setup-containerd-runsc.sh`
与 runsc 探测的实证记录,已编入脚本与 executor 行为:

1. **版本钉定已验证**:containerd 2.0.2 tarball sha256 与 runsc/shim
   `20260810.0` 的 sha512 实拉校验通过(§1 已回填)。
2. **snapshotter 语义(关键)**:containerd 2.0.2 实测——
   - `ctr images pull`(unpack)在嵌套环境用 overlayfs **成功**;
   - `ctr run` 任务 rootfs 用 overlayfs **失败**(`mount source overlay
     ... invalid argument`,嵌套 overlay-on-overlay);
   - native snapshotter **无法 unpack**(`no unpack platforms defined`)。
   → 拉取保持 containerd 默认,运行显式传 snapshotter(executor
   `--snapshotter` / `WithSnapshotter` 已实现)。
3. **runsc flag 注解被门控**:`dev.gvisor.flag.*` 注解由 shim 应用,但
   受 `--allow-flag-override` 门控,而该 flag 自身也受同一门控(鸡生蛋,
   实测报错)。→ 需要额外 runsc flag 的环境(嵌套容器 cgroup
   `subtree_control` EBUSY)通过 `RUNSC_EXTRA_FLAGS` 包装器直接加在
   runsc 命令行(实测包装器机制有效)。
4. **嵌套环境限制**:Docker Desktop/WSL2 内 runsc 沙箱创建最终被
   SIGKILL(exit 137,systrap 平台受限)。→ 完整 conformance 腿必须在
   真实 Linux runner(GitHub Actions ubuntu-latest:systemd cgroups + 真
   内核)或 KVM 主机执行;指纹契约记录环境差异,嵌套环境结果仅作
   bring-up 证据。
5. **ctr 命令面(隔离项显式化路径)**:`ctr run` 原生支持
   `--cap-add/--cap-drop/--seccomp/--read-only/--annotation`——§3
   第 4–9 项"是否显式传参"决策点有现成路径,待真机 runner 逐项验证。
6. **systrap 平台在无 KVM VM 上挂起(2026-08-17,真机 runner 实测)**:
   runsc 20260810.0 默认 systrap 平台在 GitHub 托管 runner(VM,无
   /dev/kvm)上沙箱启动挂起(300s 超时);`--platform ptrace` 是该环境
   的稳定平台,已通过 `RUNSC_EXTRA_FLAGS` 编入 CI 腿。KVM 自托管
   runner 可去掉该 flag 回到 systrap/kvm。CI 腿上的 sandbox 启动必须
   保持有界(超时包裹)以便快速失败并定位。

## 4. CI 真机腿(job:`runtime-linux-leg`)

见 `.github/workflows/ci.yml`。流程:安装钉定 containerd+runsc → 指纹
(存产物)→ 构建 `fenced-runtime-oci` → digest 钉定并预拉 conformance
镜像 → 跑 OCI conformance 腿(`TestSameAgentVersionRunsOnBothProviders/oci`,
`FENCED_OCI_CONTAINERD_NAMESPACE=fenced-ci`)→ Firecracker 探测
(`--allow-no-kvm`:GitHub 托管 runner 明确报告 KVM 缺失并跳过)。

自托管带 KVM 的 runner(`label: fenced-kvm`)额外执行:Firecracker 完整
探测(无 `--allow-no-kvm`)与后续 boot smoke。

> 嵌套容器注记:在 Docker Desktop/WSL2 等嵌套环境跑真机腿时,
> overlayfs 无法 overlay-on-overlay,必须 `SNAPSHOTTER=native`(containerd
> 2.x 服务端默认 snapshotter);物理机/VM runner 保持默认 overlayfs。

## 5. 待回填清单(首次 runner 落地时)

- [x] containerd 2.0.2 sha256 已回填 §1(2026-08-16 实拉)
- [x] runsc/shim `20260810.0` sha512 已回填 §1(2026-08-16 实拉校验)
- [ ] Firecracker v1.9.0 的 sha256 回填 §1(仅 KVM runner)
- [ ] 第 4–9 项负向测试在真机上编写并接入 CI(正向+负向)
- [ ] 决策点:seccomp/capability 是否显式编入 ctr 命令面
- [ ] conformance 工作负载镜像从 `:latest` 改为 digest 钉(CI 步骤已
      实现"先解析 digest 再按 digest 拉取",回填时固化该 digest)
- [ ] runsc 容器内真机执行(probe + conformance 腿)在目标 runner 全绿
