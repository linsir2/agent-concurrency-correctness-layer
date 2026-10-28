# S12：egress 后端收口 + 审计正确性 + 多 run 隔离

| 项目 | 内容 |
|---|---|
| 版本 | **v1.2（实现完成）** |
| 日期 | 2026-10-27 |
| 上游 | [S11 v1.2](s11-egress-enforcement.md) §11.5（K-49..K-56） |
| 覆盖 | **K-45**（收据 fail-closed）、**K-43/K-33/K-40**（多 run 隔离）、**K-55**（生产 gate）、**`unshare -Urn` backend**、**Landlock errno 探测** |
| **推迟** | **orchestration 大改（HITL/replan/spawn_child/scheduler）→ S13**（评审结论：爆炸半径过大） |
| 硬约束 | **调用外部库/外部工具，不自研** |
| 修订 | v1.0 → v1.1：修 5 P0 + 6 P1（§8） |

---

## 0. 调研结论（决定 9 个设计选择）

### 0.1 Landlock 在本沙箱 **ENOSYS**（ABI=0）

| 成因 | 表现 | 判定 |
|---|---|---|
| `CONFIG_SECURITY_LANDLOCK=n` | ENOSYS(38) | **✓**（kallsyms 中 `__x64_sys_landlock_*` 为 weak） |
| 编译但未启用 | EOPNOTSUPP(95) | ✗ |
| seccomp 过滤 | EPERM(1) | ✗（Docker ≥20.10.18 默认放行） |
| 用户态内核（gVisor） | ENOSYS(38) | 可能 |

### 0.2 Landlock ABI 版本（**修正**）

| ABI | 内核 | 能力 |
|---|---|---|
| v4 | 6.7 | **TCP** bind/connect |
| v6 | 6.12 | abstract unix socket / signal scope |
| v7 | 6.15 | 审计日志 |
| v8 | 7.0 | 多线程强制 |
| v9 | 7.1 | resolve unix |
| **v10** | **7.2** | **UDP** ← DNS 需要（v1.0 误写 6.15） |

**gVisor 不支持 Landlock**（返回 ENOSYS，官方测试 `SKIP_IF(IsRunningOnGvisor())`）。

### 0.3 运行时选型

| | gVisor | Firecracker | Kata |
|---|---|---|---|
| 硬件强制 | ❌ 用户态内核 | ✅ KVM | ✅ KVM |
| Python 兼容 | 最好 | 好 | 好 |
| 生产采用 | **Anthropic/OpenAI/Google** | AWS Lambda/Cloudflare | 蚂蚁 |

### 0.4 本沙箱最有价值的落地点：`unshare -Urn`

实测可用（`ENETUNREACH`），`ip netns` 需 root（EPERM）。→ 做成生产 backend。

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | **K-45 收据回扫失败 fail-closed** | S12-1（含静默实现分叉的负对照） |
| G2 | **预算事件带 run_id**（K-40 前提） | S12-2 |
| G3 | **per-run epoch**（两层：全局 writer lease + per-run run epoch） | S12-3 |
| G4 | **per-run retention/cursor** | S12-4 |
| G5 | **`unshare -Urn` 生产 backend** | S12-5 |
| G6 | **Landlock errno 探测**（区分 38/95） | S12-6 |
| G7 | **生产入口 fail-closed**（显式 `profile="prod"`） | S12-7 |
| G8 | gVisor 适配（外部部署） | S12-8 |

**推迟到 S13**：HITL 超时/DurableTimer、replan、spawn_child、Scheduler worker 池（评审：爆炸半径过大，独立里程碑）。

---

## 2. 设计

### 2.1 K-45：fail-closed（**修 P0：水位重置会 taint**）

**v1.0 的两个选项都错**：
- 复用 `EgressUnavailable` → 语义污染（收据回扫与 egress 无关）
- **「重置水位到 0」→ 确定性正确性破坏**：水位是全局单值，而 `_receipt_heads` 是 per-scope 且**不重置** → 重扫历史收据时 `prev != head` → **所有 scope 永久 taint**

**v1.1 定死**：

```python
class ReceiptScanFailed(RuntimeError): ...     # 专用异常（不复用 EgressUnavailable）

# _refresh_receipts_locked
try:
    records = self._receipt_records(scope)
except Exception as e:
    self._receipt_scan_failed = True           # 粘滞闩锁
    raise ReceiptScanFailed(str(e))             # 拒绝服务（不 append、不改 head）

# commit_batch
except ReceiptScanFailed as e:
    return Result(Status.UPSTREAM_FAILED, {"reason": "receipt_scan_failed"})
# 不得加入 _tainted_scopes（无 append，链未污染）
```

**粘滞性**：失败后每次提交都被拒，直到一次**成功的全量重建**（`_rebuild_receipts()`，它把水位推到 tail）。

### 2.2 K-40 前提：预算事件带 run_id

```python
# budget._emit 增 run_id（从 node.owner 或当前 run 上下文透传）
self.event_log.append(kind, node_id, payload, run_id=self._run_id, notify=False)
# budget_events(run_id) 按 run_id 过滤 —— 否则恒为空（v1.0 空断言）
```

### 2.3 K-43：两层 epoch（**修 P0：`_run_epochs` 是装饰性的**）

| 层 | 作用 | 存储 |
|---|---|---|
| **writer lease epoch** | 接管旧写者（fencing） | `wal.log.epoch`（全局，保留） |
| **run epoch** | 隔离同 run 内的租约 | `EpochStore.read_epoch(run_id)` |

```python
class EpochStore:
    def read_epoch(self, run_id: str) -> int
    def bump_epoch(self, run_id: str) -> int
```

`IntentRegistry`/`BudgetManager`/`Wal.append_frame` 按 run_id 参数化；`BUDGET_EPOCH` 事件带 run_id 并分 run 重放。

### 2.4 K-33：per-run retention

**过滤 ≠ 保留**：全局 tail 已截断的日志上做 per-run 过滤**找不回**被丢事件。需 **per-run 高水位游标**独立推进。

```python
self._run_cursors: dict[str, int] = {}     # 每 run 独立
def floor_for(self, run_id: str) -> int:
    tail = self._run_tail(run_id)          # per-run tail
    return max(0, tail - self.retention + 1)
```

### 2.5 `unshare -Urn` backend（**修 P1：参数名错**）

```python
# 正确参数：--map-root-user（不是 --map-root）
[unshare, "--user", "--map-root-user", "--net", "--fork", "--pid", "--mount-proc", *argv]
```

**启动前探针**（不依赖「已强制」的声明）：
```python
def _probe_userns() -> bool:
    r = subprocess.run([unshare, "-Urn", sys.executable, "-c",
                        "import socket; s=socket.socket(); s.connect(('192.0.2.1',443))"],
                       capture_output=True, timeout=5)
    return r.returncode != 0 and "ENETUNREACH" in r.stderr.decode()
```

### 2.6 Landlock errno 探测（**修 P1：errno 被丢弃**）

```python
def landlock_probe(syscall_fn=None) -> tuple[int, int | None]:
    libc = ctypes.CDLL(None, use_errno=True)
    ctypes.set_errno(0)
    r = libc.syscall(444, None, 0, 1)
    if r >= 1: return (r, None)
    return (0, ctypes.get_errno())      # 38=ENOSYS, 95=EOPNOTSUPP, 1=EPERM
```

**444 在 x86_64/arm64 通用**（v1.0 误判 arm64 无 444）；非通用架构（i686/riscv）返回 `(0, None)` + 登记。

### 2.7 生产入口 fail-closed（**修 P1：env 依赖是假绿**）

```python
# 显式生产入口（不依赖运维记得设 env）
def create_production_harness(...) -> Harness:
    h = Harness(..., egress_required=True)
    h.egress.require_available()        # 无强制点 → 拒绝启动
    return h
```

**口径**：`Harness()` 默认 `None`（本地可跑）；**生产必须走 `create_production_harness`**。S12-7 的验收写「生产入口 fail-closed」，**不写**「Harness 默认 fail-closed」。

### 2.8 gVisor 适配（外部部署）

**优先直接调 `runsc` + OCI bundle**（绕开 Docker daemon）：
```python
def spawn_in_gvisor(argv: list[str], bundle: str) -> subprocess.Popen:
    runsc = shutil.which("runsc")
    if runsc is None: raise SandboxUnavailable
    return subprocess.Popen([runsc, "run", "--bundle", bundle, ...])
```

**注意**：gVisor **不支持 Landlock** → 在 gVisor 内用 netns/防火墙兜底。

---

## 3. 测试

| # | 用例 | 关键判据 |
|---|---|---|
| S12-1 | K-45 | ①注入回扫失败（**三条路径**：`_receipt_records`/`Wal.receipts_from`/`_rescan_wal_from_disk`）→ 提交被拒、tail 不变、head 不变、既有链仍 True、**粘滞**；②**负对照**：静默实现 → 分叉（h1/h2 共享 WAL，h2 提交后 h1 静默回退 → 断链） |
| S12-2 | run_id | 预算事件 `e.run_id == run_id`；`budget_events(run_id)` 非空 |
| S12-3 | per-run epoch | 两 run 各自 bump → 互不 fence；**负对照**（`epoch_scope="global"` 参数化）→ 互相 fence |
| S12-4 | per-run retention | run B 活动**不推进** run A 的保留窗口；视图不串 run（同名 subject） |
| S12-5 | `unshare -Urn` | 子进程 connect → `ENETUNREACH`（**非** ECONNREFUSED）；**负对照**：无 `--net` → 结果不同；启动前探针 |
| S12-6 | Landlock errno | fake syscall 返回 -1 + errno 38/95 → `(0, 38)`/`(0, 95)`；`≥1` → `(abi, None)` |
| S12-7 | 生产 gate | `create_production_harness()` 无强制点 → 拒绝启动；**「env 未设」的生产入口**（xfail 暴露缺口） |
| S12-8 | gVisor | 外部部署（本沙箱 xfail + 登记） |

---

## 4. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/harness.py` | K-45 专用异常 + 粘滞闩锁；`EpochStore`；per-run cursor |
| `harness/budget.py` | `_emit` 带 run_id；`BUDGET_EPOCH` 带 run_id |
| `harness/egress/netns.py` | `unshare -Urn` backend + 探针 |
| `harness/egress/lsm.py` | `landlock_probe` 返回 `(abi, errno)` |
| `harness/egress/container.py`（新，**不叫 runtime.py** 避免与 `harness/runtime.py` 混淆） | gVisor/Firecracker 适配 |
| `harness/harness.py` | `create_production_harness` |
| `tests/` | `test_s12_audit_and_isolation.py` |

---

## 5. 风险与对策

| 风险 | 对策 |
|---|---|
| 水位重置 → taint | **不做**；用粘滞闩锁 + 全量重建 |
| per-run epoch 改动面大 | 两层分离；`epoch_scope` 参数化负对照 |
| 预算事件无 run_id → 空断言 | 先补 run_id（S12-2 是 S12-3/4 前提） |
| `unshare` 在 CI 不稳定 | 探针 + `xfail` 登记（**不 skip**） |
| 生产 gate 依赖 env | 显式 `create_production_harness` |
| gVisor 无 Landlock | netns/防火墙兜底；文档明确 |

---

## 6. 评审修订记录（v1.0 → v1.1）

### 6.1 P0（5 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | **「重置水位到 0」会永久 taint 所有 scope**（水位全局、head per-scope 不重置 → 重扫历史收据 `prev != head`） | 专用 `ReceiptScanFailed` + **粘滞闩锁**；全量重建走 `_rebuild_receipts()` |
| **P0-2** | **per-run epoch 不可实现**：`_run_epochs` dict 是装饰性的（epoch 横跨 `IntentRegistry`/`BudgetManager`/`Wal` 三层，且 `wal.log.epoch` 承担 writer takeover fencing） | 两层分离：全局 writer lease epoch + per-run `EpochStore` |
| **P0-3** | **预算事件无 run_id** → `budget_events(run_id)` 恒为空（空断言） | `_emit` 带 run_id（S12-2 是 S12-3/4 前提） |
| **P0-4** | **HITL 默认批准 → 审批绕过**（跨进程恢复后 `WAITING_APPROVAL` 被覆写 + `resolve` 默认 approved） | **推迟 S13**（独立里程碑） |
| **P0-5** | **Scheduler worker 池 → 双重执行**（池与 `_drain_scheduler` 都 pop）+ `CrashInjected` 被 Future 吞 | **推迟 S13**（默认 `scheduler_mode="sync"`） |

### 6.2 P1（6 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| 1 | **spawn_child 取消级联失效**：`cancel_run` 只按 run_id；`parent/children` 未持久化 | **推迟 S13**（需先持久化 parent/children） |
| 2 | **replan 与 `escalate_quarantined` 冲突** + `_id_aliases` 碰撞 + **缺再准入** | **推迟 S13** |
| 3 | **`unshare --map-root` 参数名错**（应为 `--map-root-user`）；`--pid` 进程管理不可靠；无探针 | 修正参数 + 启动前探针 + 专用异常 |
| 4 | **Landlock errno 被丢弃**（38/95 全塌缩成 0）→ 四成因判别是空的 | `landlock_probe` 返回 `(abi, errno)` |
| 5 | **生产 gate 依赖 env → 假绿**（忘记设 env 仍能启动） | 显式 `create_production_harness` |
| 6 | **gVisor 用 Docker daemon 过重** + `--network=none` 与「代理」描述矛盾 | 直接调 `runsc` + OCI bundle |

### 6.3 P2（已修）

| 问题 | 修法 |
|---|---|
| `harness/egress/runtime.py` 与 `harness/runtime.py` 同名 | 改名 `container.py` |
| 444 在 arm64 不存在（误判） | 444 在 x86_64/arm64 **通用**；非通用架构返回 `(0,None)` + 登记 |
| S12-1 只测「拒绝服务」测不出 taint | 负对照构造**静默实现真分叉** |
| S12-2 负对照靠临时改源码 | `epoch_scope` 参数化 |

### 6.4 新增已知边界（v1.1）

| # | 边界 |
|---|---|
| K-57 | orchestration 大改（HITL/replan/spawn_child/scheduler）**推迟 S13** |
| K-58 | `unshare -Urn` 的 `--net` 会连 loopback 一起断（可用性代价） |
| K-59 | `create_production_harness` 是生产唯一入口；`Harness()` 默认非 fail-closed |
| K-60 | gVisor 内 Landlock 不可用 → netns/防火墙兜底 |

---

## 7. 决策（v1.1 定案）

| # | 问题 | 结论 |
|---|---|---|
| Q1 | S12 范围 | **收窄**：egress 后端 + K-45 + 多 run 隔离；orchestration → **S13** |
| Q2 | K-45 修法 | **粘滞闩锁 + 拒绝服务**（不清水位） |
| Q3 | per-run epoch | **两层**（全局 writer lease + per-run run epoch） |
| Q4 | 生产 gate | **显式 `create_production_harness`**（不依赖 env） |

---

## 8. 实现结果（v1.1 → v1.2）

按 v1.1 实现，**未扩大范围**（HITL/replan/spawn_child/scheduler 全部推迟 S13）。

### 8.1 交付

| 项 | 实现 |
|---|---|
| **K-45** | 专用 `ReceiptScanFailed`（非 `EgressUnavailable`）+ **粘滞闩锁** `_receipt_scan_failed`；失败不 append/不改 head/不 taint/**不清水位**；`commit_batch` → `UPSTREAM_FAILED(receipt_scan_failed)`；恢复仅靠 `_rebuild_receipts()` |
| **K-40** | `BudgetNode.run_id` + `_node_payload.run_id` + `_emit(run_id=)`；`budget_events(run_id=None)` / `_declared_budget_nodes` / `_tree_integrity` 同步；无参向后兼容（`run_id=""`） |
| **K-43** | 两层：全局 `wal.log.epoch`（writer takeover，**不动**）+ `EpochStore`（per-run `wal.log.epoch.<run_id>`）；`run_scope`/`epoch_scope` 默认 `"global"` → **`bump_epoch()` 无参仍全局**（350+ 用例兼容） |
| **K-33** | `floor_for` 按 scope：`run` 用 per-run tail，`global` 保持现状；`EventLog.replay(run_id=)` 过滤（同名 subject 不串味） |
| **K-49** | `spawn_userns` 严格 `--user --map-root-user --net --fork --pid --mount-proc`（**修正 v1.0 的 `--map-root`**）；启动前探针，失败 `SandboxUnavailable`（**不降级**为残缺 argv）；探针 session 级缓存 |
| **K-54** | `landlock_probe() -> (abi, errno)`（`set_errno(0)` 前置 / `get_errno()` 后置）；38=ENOSYS / 95=EOPNOTSUPP / 1=EPERM **可区分**；非通用架构 `(0, None)` + 登记 |
| **K-55** | `create_production_harness()`：`egress_required=True` + `require_available()`；`Harness()` 默认仍 `None` |
| gVisor | `harness/egress/container.py`（**不叫 runtime.py**）：`spawn_in_gvisor` 直调 `runsc run --bundle`（绕开 Docker daemon）；`enforce()` 增 `userns`/`gvisor` 后端 |

### 8.2 验证

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **384 passed + 4 xfailed**（连跑 5 轮全绿；基线 350 + 新增 34） |
| `-m fault` | 5 passed |
| 覆盖率 | **85.49%**（≥80），`check_coverage.py` exit 0 |
| bench | `--n 30` exit 0；`report_check` 一致（C3/B3 rel 3.7% 容差内） |

### 8.3 环境能力缺失（`xfail` + `register()` 显式登记，**不 skip**）

| 环境 | 原因 |
|---|---|
| `unshare --mount-proc` | 本沙箱 **EPERM** → 完整 argv 真子进程用例 xfail；`-Urn` 短探针与负对照**仍真实**验证 `ENETUNREACH` |
| `runsc` | 未安装 → gVisor 环境用例 xfail；fake runsc 验证 argv 正确 |
| Landlock | 无 `sandlock` + ABI=0（既有） |
| `ip netns` | 无 CAP_NET_ADMIN（既有） |

### 8.4 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| K-61 | `unshare --mount-proc` 在本沙箱 EPERM → `spawn_userns` 完整 argv 路径**未真跑通**（短探针已验证隔离语义） |
| K-62 | per-run epoch/retention 默认 `"global"`（保持兼容）；生产需显式 `epoch_scope="run"` |
