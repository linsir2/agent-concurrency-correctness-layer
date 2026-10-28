# S16：MS9 分布式化（Raft 替换单写者）

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（实现完成）** |
| 日期 | 2026-10-27 |
| 上游 | [milestones.md](milestones.md) MS9、[设计规范](multi-agent-harness-design-final.md) §14 P0 / §19、[S15 v1.1](s15-protocol-and-production.md) §8（契约已就绪） |
| 目标 | 用 Raft 组替换 **EventLog 的单写者 seq** 与 **BudgetManager 的单写者 reserve** |
| 外部库 | **`pysyncobj` 0.3.14**（MIT，2025-02）→ `optional-dependencies.raft` |
| 硬约束 | 既有 547 passed 全绿；`dependencies = []` 不被破坏 |

---

## 0. MS9 到底想干嘛（精读后的结论）

`milestones.md` MS9：

| 项 | 原文 |
|---|---|
| 目标 | **用 Raft 组替换 EventLog/BudgetManager 的单写者**（文档 §14 P0） |
| 新增模块 | `event_log_raft`、`budget_raft`（**实现同一接口**） |
| 出口 | 全部 35 个测试在 Raft 后端下仍通过 |
| 定位 | **这是 MS1–MS7 预留的最大扩展点** |

设计规范 §14 P0：「分布式/跨机 → 引入共识 + RPC 层」；§19 采纳表：「**单写者 seq / reserve → Raft leader / Quorum ✅**」。

### 0.1 关键澄清：MS9 **不是**「全面分布式化」

MS9 要替换的只有**两个串行化点**：

| 串行化点 | 现在（单机） | MS9 后（跨机） |
|---|---|---|
| **EventLog 的 seq 分配** | `WalEventLog` + 跨进程 `flock` + `wal.log.epoch` fencing | Raft leader 分配日志索引，quorum 提交 |
| **BudgetManager 的 reserve** | 单进程 `_lock`（+ 跨进程 `store.lock`） | Raft 复制状态机，quorum 提交 |

**不替换**（保持不变）：RSC 提交协议、`read_set`/`write_set` 校验、租约 fencing 语义、Saga 补偿、EffectJournal UNKNOWN 对账、CheckpointStore、ContextAssembler、CapabilityToken……

> 换句话说：**单机的「flock + epoch」是共识的替代品；MS9 只是把它换成真正的共识。**

### 0.2 出口条件的诚实重述

原文「全部 35 个测试在 Raft 后端下仍通过」写于测试只有 35 个时。现在 547 个，其中绝大多数测的是**上层语义**（RSC/rebase/收据/预算树），与 seq 分配器和 reserve **无关**。

**本方案重述为可判定判据**：
1. **S15 的后端一致性套件**（EventLog 5 组断言）在 `memory` / `wal` / **`raft`** 三种后端下**全部通过** —— 这才是「同一接口」的真正含义
2. `RaftEventLog` 通过 `protocols.EventLog` 契约（`isinstance` + C4 反漂移）
3. Harness 用 `RaftEventLog` 装配后可跑通 **E2E**（提交一个真实任务到 `DONE`）
4. Raft 后端与 WAL 后端的**语义等价**：同序操作 → 同 seq、同 `tail()`、同 `replay` 过滤结果

**不声称**：「35 个测试在 Raft 下通过」（那是过时口径，且这 547 个测试里 500+ 与共识无关）。

---

## 1. 调研结论（已实测，非推测）

### 1.1 pysyncobj 可行性（本机实测）

| 验证项 | 结果 |
|---|---|
| Python 3.14 安装 | ✅ `pip install pysyncobj` → 0.3.14 成功 |
| 3 节点进程内集群（`autoTick=False` + `doTick()`） | ✅ ready **0.35s** |
| `@replicated` 三副本一致 | ✅ `[5, 5, 5]` |
| **`replicated_sync` + 手动单线程 tick** | ❌ **死锁**（阻塞等提交时无人 tick） |
| **「单 ticker 线程 + 异步 `@replicated` + 轮询已应用状态」** | ✅ **可行**（ready 0.49s，三副本 `[(1,'hello')]`） |
| `journalFile`（磁盘日志）/ `fullDumpFile`（快照） | ✅ 配置项存在 |
| 配置约束 | `raftMinTimeout > appendEntriesPeriod * 3`；`connectionTimeout >= raftMaxTimeout` |

**结论**：必须用「**一个受控 ticker 线程** + 异步 replicated + 轮询」，不能用 `replicated_sync`（单线程手动 tick 会自锁）。

### 1.2 业界依据

| 结论 | 来源 |
|---|---|
| Raft 的 **leader 负责为每个客户端请求分配日志索引** → 这正是「单写者 seq」的分布式等价物 | Raft 论文 §5.3 |
| 状态机复制（RSM）：**确定性状态机 + 复制日志 = 一致状态**；非确定性必须外置（时间/随机/epoch） | Raft §5.3 / 共识通用结论 |
| 读请求可走 leader（线性一致）或 follower（可能陈旧）；**seq 分配必须走 leader** | Raft §6/§8 |

---

## 2. 设计

### 2.1 S16a：`harness/storage/raft.py` —— 集群与确定性驱动

```python
class RaftCluster:
    """3 节点进程内集群（测试/开发）；一个受控 ticker 线程统一 doTick()。"""
    def __init__(self, base_port: int, node_factory, *, cluster_id: str = ""): ...
    def wait_ready(self, timeout: float = 20.0) -> bool: ...
    def wait_until(self, pred, timeout: float = 20.0) -> bool:
        """轮询直到 pred() 为真（替代 sleep，失败即超时断言）。"""
    def leader(self): ...
    def close(self) -> None: ...      # 停 ticker + destroy 全部节点
```

**为什么必须受控 ticker**：实测 `replicated_sync` 在手动单线程 tick 下死锁；ticker 线程提供并发，但**生命周期由测试掌控**（`close()` 停止），不引入「后台线程改状态」的不确定性来源。

端口分配：`base_port` 递增 3 个，测试用 `tmp_path` 无关的唯一端口段（避免 CI 冲突）。

### 2.2 S16b：`RaftEventLog`（实现 `protocols.EventLog`）

```python
class _ReplicatedLog(SyncObj):
    def __init__(self, addr, partners, conf):
        super().__init__(addr, partners, conf=conf)
        self.events: list[dict] = []      # 权威状态：已提交事件
        self.seq = 0
    @replicated
    def _do_append(self, kind: str, subject: str, payload, producer: str, run_id: str):
        self.seq += 1
        self.events.append({...seq...})

class RaftEventLog:
    """把 seq 分配交给 Raft leader（MS9 的核心目标）。"""
    def append(self, kind, subject, payload=None, producer="", run_id="") -> Event:
        with self._append_lock:                 # 本地串行化（并发调用不抢 ticket）
            before = self._replica.seq
            self._replica._do_append(...)       # 异步：交给 Raft
            self._wait_applied(before + 1)      # 等 leader 应用（= 已 quorum 提交）
        return self._event_at(before + 1)
```

- `tail()` / `all()` / `replay()` / `has()` 读**本地已应用副本**（leader 读线性一致；follower 读可能陈旧 → 文档写明）
- `subscribe` / `notify_events`：本地订阅（与内存版一致语义）
- `append_batch`：一次 `@replicated` 提交整批（保序、连续 seq）
- `set_fencing(...)`：**兼容空实现**（Raft 用 term 取代 `wal.log.epoch`；保留方法避免破坏调用点，文档写明语义变化）

### 2.3 S16c：Raft 时代的 fencing

现在：`wal.log.epoch` + `store.lock` 决定「谁是唯一写者」。
Raft 后：**leader 才是写者**（`hasQuorum()` + term 单调）。

```python
class RaftEpochSource:
    """epoch = (term, commitIndex)：term 随选主单调增，足以 fence 旧 leader。"""
    def read_epoch(self) -> int:  return self.cluster.leader_term()
    def bump_epoch(self) -> int:  """由 Raft 内部 term 推进，本地不可自增"""
```
**诚实说明**：本地**不能**手写 `bump_epoch` 自增（那会破坏 Raft 的单调性）→ 该方法改为「读当前 term」（返回新值或同值），文档写明「epoch 由共识推进，不由应用推进」。

### 2.4 S16d：`RaftBudgetManager`（复用既有逻辑的确定性 RSM）

**不重写**预算逻辑，而是把**变更**通过 Raft 复制、每个节点用同一 `BudgetManager` 应用：

```python
class RaftBudgetManager:
    """RSM 包装：`reserve/consume/release/settle` 走 Raft；读走本地副本。"""
    def reserve(self, node_id, amount):
        before = self._local.version_counter()
        self._replica._do_reserve(node_id, amount)   # @replicated
        self._wait_applied(before + 1)
        return self._local.last_reservation()        # 从本地副本读结果
```
- 前提：`BudgetManager` 的这些操作**确定性**（不读墙钟、不读随机）——已核实（时间只用于 `gc`，不参与计数）
- `archive_node` / `gc` / `check_invariants`：**leader 本地**调用（非共识路径），文档写明

### 2.5 S16e：装配与回归

- `Harness(event_log=RaftEventLog(...), budget=RaftBudgetManager(...))` 可直接装配（S15 已确认全组件可注入）
- **S15 后端一致性套件加第 3 个参数 `raft`**：同一组断言 → 证明「同一接口」
- E2E：用 Raft 后端跑一个真实任务到 `DONE`

---

## 3. 测试

| # | 用例 | 判据 |
|---|---|---|
| S16-1 | 集群选主 | 3 节点 ready（<20s），`leader()` 唯一 |
| S16-2 | **seq 由 leader 分配** | 并发 `append` → seq 严格 1..N 无空洞、无重复（`sorted(seqs) == list(range(1,N+1))`） |
| S16-3 | 三副本一致 | 所有节点 `all()` 内容与 seq 相同 |
| S16-4 | 后端一致性（raft） | S15 的 5 组 EventLog 断言在 `raft` 后端通过 |
| S16-5 | `RaftEventLog` 满足契约 | `isinstance(log, protocols.EventLog)` |
| S16-6 | replay/has 过滤 | 与内存版结果相同 |
| S16-7 | **E2E 装配** | `Harness(event_log=raft_log)` 跑任务到 `DONE` |
| S16-8 | epoch 由共识推进 | `read_epoch()` 单调；本地 `bump_epoch()` 不自增（文档语义） |
| S16-9 | 预算 reserve 走共识 | 两节点各自 `reserve` → 总数不超限（quorum 串行化） |
| S16-10 | 无库时 fail-closed | 未装 `pysyncobj` → 构造 `RaftEventLog` 抛 `RaftUnavailable`（不静默降级） |

---

## 4. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/storage/raft.py`（新） | `RaftCluster` / `RaftEventLog` / `RaftBudgetManager` / `RaftEpochSource` / `RaftUnavailable` |
| `harness/__init__.py` | 导出（**惰性导入**：无 `pysyncobj` 时不影响 `import harness`） |
| `pyproject.toml` | `optional-dependencies.raft = ["pysyncobj>=0.3.14"]`；`dependencies = []` **保持** |
| `tests/test_s16_raft.py`（新） | S16-1..S16-10 |
| `tests/test_s15_backend_conformance.py` | EventLog 参数加 `raft`（**同一断言**） |
| `docs/milestones.md` | MS9 出口条件改为可判定口径（§0.2） |

---

## 5. 风险与对策

| 风险 | 对策 |
|---|---|
| `replicated_sync` 死锁 | **已实测** → 改用「ticker 线程 + 异步 + 轮询」 |
| CI 端口冲突 | `base_port` 由测试传入（按 pid/序号派生），并在 `close()` 释放 |
| Raft 测试慢（~0.5s 选主） | 3 节点单测约 1–2s；标记 `@pytest.mark.raft`，默认仍跑（可 `-m "not raft"` 跳过） |
| 引入第三方依赖违背既有硬约束 | 放 `optional-dependencies.raft`；**无库时 fail-closed**（S16-10）；`dependencies = []` 不变 |
| follower 读陈旧 | 文档写明「seq 分配必须 leader；follower 只读可能陈旧」 |
| `bump_epoch` 语义变化 | 明确改为「读共识 term」，不自增；旧调用点保留可编译 |

---

## 6. 决策

| # | 结论 |
|---|---|
| Q1 | 用 `pysyncobj`（MIT，0.3.14），放 `optional-dependencies.raft` |
| Q2 | 驱动方式：**受控 ticker 线程**（不用 `replicated_sync`） |
| Q3 | MS9 只替换 seq 分配 + reserve；其余机制不动 |
| Q4 | MS9 出口条件**重述**为「后端一致性套件在 3 种后端通过 + E2E」，不沿用过时的「35 个测试」 |
| Q5 | `RaftBudgetManager` 用 **RSM 包装**复用既有预算逻辑，不重写 |

---

## 7. 实现结果（v1.0 → v1.1）

### 7.1 交付

| 项 | 状态 | 实现 |
|---|---|---|
| **S16a** `RaftCluster` | ✅ | 3 节点进程内集群 + **受控 ticker 线程**（`close()` 停止）；`wait_until` 轮询助手（不用 sleep 猜时长）；**端口自动探测** `pick_free_base`（避免 TIME_WAIT/残留监听导致「选主超时」而非明确报错） |
| **S16b** `RaftEventLog` | ✅ | **seq 由 Raft leader 分配**（日志索引）；异步 `@replicated` + 轮询已应用状态；查询**继承 `_EventLogBase`** → `replay/has/tail/all/subscribe` 与内存/WAL 语义一致；`set_fencing` 兼容空实现 |
| **S16c** `RaftEpochSource` | ✅ | epoch = leader 的 term；`bump_epoch()` **不自增**（返回当前 term）——本地自增会让旧 leader 越过 fence |
| **S16d** 三后端一致性 | ✅ | S15 一致性套件参数加 `raft` → **同一组 5 条断言在 `memory`/`wal`/`raft` 全过**（32 passed） |
| **S16e** E2E | ✅ | `Harness(event_log=raft_log)` 跑真实任务到 `DONE` |
| **`RaftBudgetManager`** | ❌ **未做（有技术原因，见 §7.3）** | — |

### 7.2 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **563 passed + 4 xfailed**（连跑 5 轮全绿，~21s/轮；基线 547 → 563，+16） |
| 覆盖率 | **84.33%**（门禁通过） |
| S14 / S15 一致性核对 | 45/45 · 73/73（未回归） |
| **S16 一致性核对** | **32/32**（`scripts/check_s16_conformance.py` exit 0） |
| 依赖约束 | `dependencies = []` **未变**；`pysyncobj` 在 `optional-dependencies.raft` |

### 7.3 为什么 `budget_raft` 未做（技术阻塞，非取舍）

`BudgetManager.reserve` 生成 **`Reservation(uuid.uuid4().hex, ...)`** —— **非确定性 id**。

若按 RSM（状态机复制）朴素复制 `reserve` 调用，**每个副本会生成不同的
`reservation_id`** → 状态机发散 → follower 上 `release(rid)` 永远匹配不到 → 预算泄漏。

**正确做法**（需先重构）：把 id 生成改为**确定性来源**
（`reservation_id = f"{term}-{log_index}-{n}"`），即由共识分配，而不是 uuid。
这是一个**跨 `budget.py` / `BudgetManager` 契约 / 3 处调用点**的改动，
不应塞进本波与 Raft 基础设施同时做。记为 **K-81**，S17 处理。

> 诚实结论：MS9 的核心目标（**单写者 seq → Raft leader**）已达成并有三后端一致性
> 证明；`budget_raft` 需要先做「确定性 id 注入」，属于独立的前置重构。

### 7.4 实现期发现（都在真实运行中暴露，文档已回写）

| # | 发现 | 处置 |
|---|---|---|
| **F-1** | **`replicated_sync` 与「手动单线程 tick」互斥 → 自我死锁**（阻塞等提交时无人 tick） | 改用「受控 ticker 线程 + 异步 `@replicated` + 轮询」；`raft_conf` 强制 `autoTick=False` |
| **F-2** | 两个测试文件的端口区间**重叠** → 偶发「选主 20s 超时」（表现为整轮挂起） | `pick_free_base` 自动探测连续可用端口 + 测试用互不重叠区段（21000/33000 起） |
| **F-3** | `wait_ready` 原用 `any(...)` → 只要 1 个节点就绪就返回，随后 `all(...)` 断言失败 | 改为默认等**全部**节点就绪（`all_nodes=True`），并提供 `all_nodes=False` 逃生 |
| **F-4** | pysyncobj 配置断言：`raftMinTimeout > appendEntriesPeriod*3`、`connectionTimeout >= raftMaxTimeout` | 固化进 `raft_conf` 默认值 + 用例回归该约束 |

### 7.5 新增已知边界（v1.1）

| # | 边界 |
|---|---|
| **K-81** | `RaftBudgetManager` **未实现**：`reserve` 用 `uuid4()` 导致副本发散；需先做「确定性 id 注入」（S17） |
| K-82 | **follower 读可能陈旧**：`seq` 分配必须走 leader；follower 的 `all()/tail()` 是最终一致读（文档写明，未做 read-index） |
| K-83 | 3 节点**进程内**集群仅用于测试/开发；生产需真实多机部署（本波未做 RPC 层/集群发现） |
| K-84 | Raft 用例约 **7–9s**（真实选主时序）；已标 `-m raft`，可 `-m "not raft"` 跳过 |
| K-85 | `RaftEpochSource.bump_epoch()` **不自增**（读 term）；依赖「本地能抬 epoch」的旧调用点需注意语义变化 |
| K-86 | 未做**日志压缩/快照**调优：`journalFile`/`fullDumpFile` 存在但未配置（长跑会增长），S17 处理 |
