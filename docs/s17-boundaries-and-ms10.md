# S17：K-81~K-86 收口 + MS10 生产化

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（实现完成）** |
| 日期 | 2026-10-27 |
| 上游 | [S16 v1.1](s16-ms9-raft.md) §7.5（K-81..K-86）、[milestones.md](milestones.md) MS10 |
| 覆盖 | **K-81**（确定性 id → `RaftBudgetManager`）、**K-82**（leader-required read）、**K-83**（生产配置）、**K-84**（测试提速）、**K-85**（`bump_epoch` 语义）、**K-86**（日志压缩/快照）；**MS10**：可观测 + 优雅排空 + 毒任务 DLQ + Admin kill/purge |
| 硬约束 | 563 passed 全绿；`dependencies = []`；零静默降级 |

---

## 0. 调研结论（实测，非推测）

### 0.1 K-81 的根因比想象中**更集中**

| 事实 | 证据 |
|---|---|
| 全仓**只有一处**生成 `reservation_id` | `Reservation(uuid.uuid4().hex, node_id, amount)`（[budget.py:330](harness/budget.py)） |
| `release/settle` 都以该 id 为键 | `self._reservations.get(reservation_id)`（[budget.py:340](harness/budget.py)） |

→ 只要把「id 生成」变成**可注入**，RSM 复制即可成立，**不需要大改预算逻辑**。

**关键洞察（比 `term-logindex` 更简单）**：在 `@replicated` 方法内部自增一个
**复制状态里的计数器** `self._seq`，用 `f"R{self._seq}"` 作 id。因为所有副本
按**相同顺序**应用相同命令 → `_seq` 必然一致 → **id 确定性成立**，且重启后由
journal/快照恢复（实测见 §0.3）。无需访问 Raft 日志索引。

### 0.2 K-82：read-index 在本库的诚实落点

Raft 的线性一致读需要「leader 在当前 term 确认领导权 + 等待 `applied >= readIndex`」。
pysyncobj 只暴露 `raftCommitIndex` / `raftLastApplied` / `hasQuorum`，**不暴露
`ReadIndex` RPC**。

诚实的两种模式（都实现，由调用方选）：
- **默认（follower 可读）**：最终一致；文档写明「可能陈旧」
- **`require_leader=True`**：非 leader 抛 `NotLeader`；leader 上等
  `raftLastApplied >= raftCommitIndex` 再读 → **对 leader 是线性一致**

不声称「follower 也能线性一致读」（那需要 ReadIndex 协议，本库不支持）。

### 0.3 K-86 已实测可行

| 验证 | 结果 |
|---|---|
| `journalFile` + `fullDumpFile` 配置 | ✅ 生成 `n{0,1,2}.journal` / `.dump` |
| 写入 6 条 → `forceLogCompaction()` | ✅ 三副本各 6 条不变 |
| **重启后恢复** | ✅ 三节点均恢复 `[0,1,2,3,4,5]` |
| `logCompactionMinEntries` / `MinTime` | ✅ 被 `SyncObjConf` 接受 |

### 0.4 MS10：S15 已交付 3/4，本波补 1 个半

| MS10 出口条件 | 现状 |
|---|---|
| `test_ingress_idempotent` | ✅ S15-D1 已实现（需按 MS10 命名补一条别名用例） |
| `test_admin_pause_resume` | ✅ S15-D3 已实现（同上） |
| `test_dlq` | 🟡 **部分**：`DEAD_LETTER` 有 4 条产生路径 + redrive，但**缺「毒任务自动识别」**与命名用例 |
| 可观测（Metrics/Trace） | ❌ 未做 |
| 优雅排空 | ❌ 未做 |
| Admin `kill`/`purge` | ❌ 未做（只有 pause/resume） |

### 0.5 OTel 官方指引（决定零依赖做法）

OpenTelemetry 官方《Client Design Principles》明确：**库只依赖 `opentelemetry-api`，
若应用未装 SDK，API 自带 **no-op 实现**，调用安全且零开销**。

→ 但我们的 `dependencies = []` 是硬约束，**不能**依赖 `opentelemetry-api`。
因此：**自建最小 `SpanSink` 抽象** + **可选 OTel 适配器**（`optional-dependencies.otel`），
适配器在装 SDK 时把 span/metric 转成 OTel 对象；未装则用内置 no-op/内存 sink。

---

## 1. 设计

### 1.1 K-81：确定性 id + `RaftBudgetManager`

```python
# budget.py（唯一改动点）
def reserve(self, node_id: str, amount: int, *,
            reservation_id: str | None = None) -> Reservation | None:
    ...
    rid = reservation_id or uuid.uuid4().hex      # 缺省保持向后兼容
    resv = Reservation(rid, node_id, amount)
```

```python
# storage/raft.py
class _ReplicatedBudget(SyncObj):
    def __init__(...):
        super().__init__(...)
        self._seq = 0            # ★ 复制状态里的计数器 → 确定性 id
    @replicated
    def reserve(self, node_id, amount) -> str:
        self._seq += 1
        rid = f"R{self._seq}"
        self.applied.append(("reserve", node_id, amount, rid))
        return rid

class RaftBudgetManager:
    """RSM：变更走 Raft，读走本地副本。"""
    def reserve(self, node_id, amount):
        rid = self._replica.reserve(node_id, amount)      # 异步返回 None
        self._wait_applied(...)
        return self._local.reserve(node_id, amount, reservation_id=rid)
```

**代价**：`reserve` 签名新增 keyword-only 参数（向后兼容）；`protocols.BudgetManager`
同步加该参数（C4 反漂移只看成员名，不受影响）。

### 1.2 K-82：两种读模式

```python
class RaftEventLog:
    def __init__(..., require_leader: bool = False): ...
    def _check_read(self) -> None:
        if self._require_leader and not self._is_leader():
            raise NotLeader(...)
    def all(self):   # 覆写：读前校验 + （leader 时）等 applied>=commit
        self._check_read(); self._wait_applied_commit(); return super().all()
```
同样用于 `tail()/replay()/has()`。

### 1.3 K-83：生产配置（不假装能测多机）

```python
def production_conf(*, node_id: str, addrs: list[str], data_dir: str) -> SyncObjConf:
    """真实多机配置：每节点独立 journal/dump 路径 + 更长超时 + 压缩阈值。"""
```
**诚实边界**：本沙箱**无法**起真实多机 → 只测配置正确性（路径派生、超时约束、
节点数校验），并把 K-83 收窄为「未做集群发现/DNS 轮转」。

### 1.4 K-84：测试提速（不改语义）

- 新增 **module 级共享集群** fixture，供「纯查询类」用例复用（`test_s16_5/6`）
- 每测试独立集群仅保留在「需要隔离状态」的用例（seq/replicate/E2E）
- 目标：Raft 子集从 ~8s → ~4s（不改任何断言）

### 1.5 K-85：`bump_epoch` 显式不可用

```python
class RaftEpochSource:
    supports_bump = False
    def bump_epoch(self) -> int:
        raise EpochNotBumpable("Raft 的 epoch 由 term 推进，应用层不得自增")
```
（原实现「返回同值」会让调用方误以为已推进。）

### 1.6 K-86：journal + 快照

```python
def raft_conf(*, journal_file=None, full_dump_file=None, **overrides)
class RaftCluster(..., data_dir=None)   # 有则派生每节点 journal/dump
def compact(self) -> None               # 对全部节点 forceLogCompaction
```

### 1.7 MS10

| 项 | 设计 |
|---|---|
| **可观测** | `harness/observability.py`：`SpanSink` Protocol（`emit_span/emit_metric`）+ `MemorySpanSink` + `event_spans(events)`（把 EventLog 映射成 span 列表，**零侵入**）+ `OTelAdapter`（惰性 import `opentelemetry`，缺失则抛 `OTelUnavailable`） |
| **优雅排空** | `Harness.drain(timeout_s=...)`：置 `_draining` → 不再 push 新任务 → 等在途 ACB 到终态 → 返回 `{drained, remaining, timed_out}`；`close()` 复用它 |
| **毒任务 DLQ** | `poison_after_failures: int = 3`（构造参数）：连续失败（`retry_budget` 耗尽 + replan 耗尽）后再失败 → `DEAD_LETTER` + `failure="POISON_TASK"`；命名用例 `test_dlq_poison_task_goes_dead_letter` |
| **Admin kill/purge** | `kill_run(run_id)`：立即 `CANCELLED` + 结算（不可恢复）；`purge_run(run_id)`：仅当 run 全终态 → 从内存/plans 移除并写 `RUN_PURGED` 事件（不删历史事件，保审计） |
| **出口命名用例** | `test_ingress_idempotent` / `test_admin_pause_resume` / `test_dlq` 三条按 MS10 命名的聚合用例（断言真实行为，不是改名空壳） |

---

## 2. 测试

| # | 用例 | 判据 |
|---|---|---|
| S17-1 | 确定性 id | 两副本 `reserve` → **同一 rid**；重启后 rid 继续单调（不重复） |
| S17-2 | `RaftBudgetManager` 不超发 | 两节点各 reserve 至上限 → 超出被拒（quorum 串行化） |
| S17-3 | budget 契约 | `isinstance(raft_budget, protocols.BudgetManager)` |
| S17-4 | leader-required read | follower 读**可能陈旧**；`require_leader=True` 时 follower 抛 `NotLeader` |
| S17-5 | 生产配置 | `production_conf` 路径派生/超时约束/节点数校验 |
| S17-6 | 快照 + 重启 | 写入 → `compact()` → 重启 → **状态完整恢复**（含 rid 计数） |
| S17-7 | `bump_epoch` 显式失败 | `supports_bump is False`；调用抛 `EpochNotBumpable` |
| S17-8 | `drain` | 在途任务跑完后返回 `drained=N`；超时返回 `timed_out=True` 且不丢状态 |
| S17-9 | 毒任务 | 连续失败达阈值 → `DEAD_LETTER` + `POISON_TASK`；**负对照**：未达阈值不 DLQ |
| S17-10 | `kill_run` / `purge_run` | kill 立即 `CANCELLED` + 资源释放；purge 仅限全终态，写 `RUN_PURGED` |
| S17-11 | 可观测 | `event_spans` 覆盖关键事件；`MemorySpanSink` 收到 span；`OTelAdapter` 在无 SDK 时抛 `OTelUnavailable` |
| S17-12 | MS10 出口命名 | `test_dlq` / `test_admin_pause_resume` / `test_ingress_idempotent` 三条真断言 |

---

## 3. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/budget.py` | `reserve(..., reservation_id=None)`（唯一改动点） |
| `harness/protocols.py` | `BudgetManager.reserve` 加该参数（保持 C4 通过） |
| `harness/storage/raft.py` | `_ReplicatedBudget` + `RaftBudgetManager`；`require_leader` + `NotLeader`；`production_conf`；`raft_conf(journal_file=..., full_dump_file=...)`；`RaftCluster(data_dir=...)` + `compact()`；`RaftEpochSource.bump_epoch` 改为抛错 |
| `harness/observability.py`（新） | `SpanSink` / `MemorySpanSink` / `event_spans` / `OTelAdapter` |
| `harness/harness.py` | `drain()` / `kill_run()` / `purge_run()` / `poison_after_failures`；`metrics()` 补毒任务/排空 gauge |
| `harness/types.py` | `EventKind.RUN_PURGED`；`ACB.consecutive_failures` |
| `pyproject.toml` | `optional-dependencies.otel = ["opentelemetry-api>=1.20", "opentelemetry-sdk>=1.20"]` |
| `tests/` | `test_s17_raft_boundaries.py`、`test_s17_ms10.py` |
| `docs/milestones.md` | MS10 出口条件按实况更新 |

---

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| RSM 包装与本地 BudgetManager 事件流重复 | `RaftBudgetManager` 只让**本地副本**（node[0]）emit 事件；其余副本用静默 event_log（或在测试中只检查 node[0]） |
| `reserve` 加参数影响面 | keyword-only + 缺省 `None`；507 处调用不受影响 |
| `require_leader` 改变默认行为 | 默认 `False`（保持 S16 语义）；仅显式开启才抛 |
| `drain` 与 `_drain_scheduler` 竞争 | `drain` 只置标志 + 等待，不主动抢占；在途任务自然完成 |
| 毒任务阈值误杀 | 阈值默认 3 且需 `retry_budget` 已耗尽；负对照锁定「未达阈值不 DLQ」 |
| `purge_run` 破坏审计 | 不删历史事件，只写 `RUN_PURGED` + 移出内存/plans |

---

## 5. 决策

| # | 结论 |
|---|---|
| Q1 | K-81 用「复制计数器 `f"R{n}"`」而非 `term-logindex`（更简单且重启可恢复） |
| Q2 | K-82 **不声称** follower 线性一致读；提供 `require_leader` 开关 |
| Q3 | K-83 只做配置正确性，真实多机仍是边界 |
| Q4 | MS10 可观测**不依赖** `opentelemetry-api`（保零依赖），用 `SpanSink` + 可选适配器 |
| Q5 | `purge_run` 不删事件（保审计），只清内存/plans |

---

## 6. 实现结果（v1.0 → v1.1）

### 6.1 六条边界

| # | 状态 | 实现要点 |
|---|---|---|
| **K-81** | ✅ | `BudgetManager.reserve(..., reservation_id=None)` 可注入 id（缺省仍 `uuid4`，**向后兼容**）；`_ReplicatedBudget` 用**复制状态计数器** `f"R{n}"` 生成 rid → 三副本**完全一致**，且 `journal`/快照恢复后继续单调（实测 `R4`） |
| **K-82** | ✅ | `RaftEventLog(require_leader=...)`：显式开启后 follower 读抛 `NotLeader`；leader 上等 `applied >= commit` 再读。**不声称** follower 线性一致（pysyncobj 无 ReadIndex） |
| **K-83** | ✅（收窄） | `production_conf(node_id, addrs, data_dir)`：独立 journal/dump 路径 + 生产级超时 + 三条校验（node 在 addrs、≥3 节点、data_dir 非空）。**剩余边界**：真实多机 / 集群发现未做（沙箱不可测） |
| **K-84** | ✅ | `test_s16` 的「只观测集群」用例改用**模块级共享集群** → 6.6s → **5.9s**（不改断言） |
| **K-85** | ✅ | `RaftEpochSource.supports_bump=False` + `bump_epoch()` 抛 `EpochNotBumpable`（原「返回同值」会让调用方误判已推进） |
| **K-86** | ✅ | `raft_conf(journal_file=..., full_dump_file=..., compaction_min_entries=2)`；`RaftCluster(data_dir=...)` + `compact()`。**实测**：写入 6 条 → compact → 重启**完整恢复** |

### 6.2 MS10

| 项 | 状态 | 实现 |
|---|---|---|
| **可观测** | ✅ | `harness/observability.py`：`SpanSink` 契约 + `MemorySpanSink` + `event_spans()`（**非侵入**：从事件流映射）+ `export_metrics` + `OTelAdapter` + `make_otel_sink()`（无 SDK 抛 `OTelUnavailable`）。`Harness.export_observability(sink)` |
| **优雅排空** | ✅ | `Harness.drain(timeout_s)` → `{drained, remaining, timed_out}`；**不强杀**在途任务（与 kill 的区别），超时状态完整保留可继续 |
| **毒任务 DLQ** | ✅ | `poison_after_failures`（默认 3）：连续失败达阈值 → 复用**既有合法边** `QUARANTINED → DEAD_LETTER`，失败码 `POISON_TASK`；阈值 0 关闭（负对照） |
| **Admin kill/purge** | ✅ | `kill_run` 立即 `CANCELLED` + 资源结算；`purge_run` 仅全终态可清内存，**不删历史事件**（写 `RUN_PURGED`） |
| **MS10 出口三条命名用例** | ✅ | `test_ingress_idempotent` / `test_admin_pause_resume` / `test_dlq`（真断言） |

### 6.3 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **590 passed + 4 xfailed**（连跑 5 轮全绿，~27s/轮；基线 563 → 590，+27） |
| 覆盖率 | 门禁通过 |
| 核对脚本 | S14 **45/45** · S15 **73/73** · S16 **32/32** · **S17 56/56** |
| 依赖 | `dependencies = []` 未变；`pysyncobj` / `opentelemetry` 均在 optional |

### 6.4 实现期发现（真问题，均已修）

| # | 发现 | 处置 |
|---|---|---|
| **F-1** | **`logCompactionMinEntries` 必须 ≥2**（pysyncobj 断言）；阈值过大（100）→ `compact()` 是空操作 → **没有 `.dump`** → 重启**恢复不到任何状态**（最初表现为「重启后 `_seq=0`」） | 阈值默认 **2**；`production_conf` 用 1000（长期运行更省 IO） |
| **F-2** | `_port_free` 带 `SO_REUSEADDR` 会把**已被监听**的端口误判为空闲 → 静默重试绑定 → 选主超时 | 去掉 `SO_REUSEADDR`（TIME_WAIT 也算占用，顺延即可） |
| **F-3** | Python 3.12+ 的 `runtime_checkable` 用**静态**属性查找，**动态 `__getattr__` 不算** → `isinstance(raft_budget, BudgetManager)` 为 False | 把 12 个委托成员改成**显式方法**（更明确地声明「leader-local」） |
| **F-4** | 测试「两副本立刻同步」是错的：复制是**异步**的，写入只保证**绑定副本**已应用 | 改为 `wait_until` 全部收敛后再断言一致（区分「绑定副本权威」与「集群收敛」） |

### 6.5 新增/收敛的已知边界

| # | 边界 |
|---|---|
| K-81 | **已收敛**：确定性 id 打通；剩余差异见 K-87 |
| **K-87** | `RaftBudgetManager` 是**扁平账本**：无父子预算树、`archive/gc/不变量` 委托 leader 本地镜像 → **不保证集群一致**（文档已声明） |
| K-82 | 仍无 ReadIndex：follower 读为最终一致；`require_leader` 只保证「不在 follower 上读」 |
| K-83 | 真实多机 / 集群发现 / 网络分区演练**未做**（沙箱不可测） |
| K-86 | `full_dumpFile` 用 pysyncobj 的 `fork()` 做 COW 序列化 → 多线程进程会有 `DeprecationWarning`（Python 3.12+）；生产建议独立进程或接受告警 |
