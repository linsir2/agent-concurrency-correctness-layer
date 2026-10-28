# S18：K-87 预算树纳入共识 + K-82 线性一致读 + K-83 外部集群

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（实现完成 + 终审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [S17 v1.1](s17-boundaries-and-ms10.md) §6.5（K-87 / K-82 / K-83）、[milestones.md](milestones.md) MS9/MS10 |
| 覆盖 | **K-87**（`RaftBudgetManager` 从「扁平账本」升级为**真 RSM**：树/归档/gc/不变量全走共识）、**K-82**（读穿日志 → 线性一致读）、**K-83**（外部地址集群） |
| 不覆盖 | 多租户/计量（S19）、真实多机网络（沙箱不可测，K-83 只收窄） |
| 硬约束 | 590 passed 全绿；`dependencies = []`；零静默降级 |

---

## 0. 调研结论（代码实况 + 外部依据）

### 0.1 K-87：`RaftBudgetManager` 现在是**半成品**

| 事实 | 证据 |
|---|---|
| 现状是**扁平账本**：只有 `_limits` / `_reserved` / `_resv` 三个 dict | [raft.py](harness/storage/raft.py) `_ReplicatedBudget` |
| **无父子预算树** → `subtree_reserved` 语义丢失（`_subtree_nodes` 返回 `[node]`） | 同上 |
| `archive_node` / `gc` / `check_invariants` **委托 leader 本地镜像** → 各副本不一致 | `RaftBudgetManager` 的显式委托方法 |
| 但 `Harness` 的 I5a/I5b/I5c 依赖**树**与**归档折叠** | [budget.py:525-570](harness/budget.py) |

→ **K-87 的真实含义**：把 `BudgetManager` 的**完整语义**（树/归档/永久账折叠/gc/不变量）纳入 RSM，而不是维护一个简化账本。

### 0.2 K-87 的技术阻塞点（已定位到行）

`BudgetManager` 里**只有两处**非确定性：

| 位置 | 代码 | 影响 |
|---|---|---|
| [budget.py:560](harness/budget.py) | `archive_node` 内 `now = int(time.time())` 写 tombstone 过期时间 | 各副本 `expires_at` 不同 → 状态发散 |
| [budget.py:740](harness/budget.py) | 归档**事件重放**路径同样用 `time.time()` | 同上（仅影响 gc 时机） |

其余全部确定性（`create_node` 无 uuid/时间；`reserve` 的 id 已在 S17 可注入；`gc(now)` 本就**接受 now 入参**）。

**修法（最小）**：`archive_node(node_id, *, now: float | None = None)`；`None` → `time.time()`（向后兼容），RSM 路径由 **leader 决定 `now`** 并作为命令参数复制 → 所有副本用同一值。

### 0.3 K-82：ReadIndex 在 pysyncobj 上**不可直接实现**

Raft 的正式 ReadIndex 三步（Ongaro 博士论文 §6.4 / etcd 实现）：
1. leader 记录当前 `commitIndex` → `readIndex`
2. **为本次读**发起一轮心跳，收 quorum 确认（**不能复用旧确认**）
3. 等 `applied >= readIndex` 后服务读

pysyncobj 只暴露 `hasQuorum`（布尔）、`raftCommitIndex`、`raftLastApplied`，**没有「针对本次读的 quorum 确认」API** → 无法严格实现第 2 步。

**可实现的等价方案：读穿日志（read-through-log）**
```python
@replicated
def barrier(self) -> None:
    self.barrier_count += 1        # 一条 no-op 日志项
```
写一条 no-op 日志 → 等它提交（= quorum 已确认当前 leader）→ 本地读。
代价：**每次线性一致读多一条日志写入**（Ongaro §6.4 明确列出该方案及其代价）。
这是唯一能在不改 pysyncobj 的前提下**正确**做到的路径。

### 0.4 K-83：能测什么、不能测什么

| 项 | 可测性 |
|---|---|
| 真实 TCP + 独立 socket（非共享内存） | ✅ 现有实现**已用真实 TCP**（`127.0.0.1:port`） |
| 多进程（独立 PID） | ✅ 可测（但需 IPC 才能写入，价值低） |
| **跨机**（不同 host / 网络分区 / 时钟漂移） | ❌ 沙箱不可测 |
| 外部地址集群（部署时传 `host:port` 列表） | ✅ 可加 `from_addrs` 并校验 |

→ K-83 本波只补 **`RaftCluster.from_addrs()`**（让部署能传真实地址）+ 校验，**仍保留**「跨机/分区未测」为边界。

---

## 1. 设计

### 1.1 K-87：`_ReplicatedBudget` 改为「真 BudgetManager 的 RSM」

```python
class _ReplicatedBudget(SyncObj):
    def __init__(...):
        super().__init__(...)
        self.bm = BudgetManager(event_log=None)   # ★ 静默（不 emit 事件）
        self.version = 0
        self._seq = 0

    @replicated
    def create_node(self, node_id, limits, parent_id, run_id):
        self.bm.create_node(node_id, limits, parent_id, run_id=run_id)
        self.version += 1

    @replicated
    def reserve(self, node_id, amount):
        self._seq += 1
        rid = f"R{self._seq}"
        resv = self.bm.reserve(node_id, amount, reservation_id=rid)
        self.last_rid = resv.reservation_id if resv else ""
        self.version += 1

    @replicated
    def archive_node(self, node_id, now):        # ★ now 由 leader 决定并复制
        self.bm.archive_node(node_id, now=now)
        self.version += 1

    @replicated
    def gc(self, now): ...
```

`RaftBudgetManager`：
- 写：`_bump_and_wait` 后返回
- 读：**直接委托** `self._replica.bm`（树、`root`、`_nodes`、`_reservations`、
  `_subtree_nodes`、`check_invariants`）→ **不再是「leader-local」**
- `check_invariants()` 在**任意副本**上跑 → 集群一致性**可验证**

**收益（可判定）**：
1. `subtree_reserved` 语义恢复（父子树真实存在）
2. `archive_node` 折叠永久账（I5c）在集群一致
3. `check_invariants` 可在 follower 上跑并一致 → 这就是「集群一致性可验证」

### 1.2 K-82：`barrier()` + `linearizable()`

```python
class RaftEventLog:
    def barrier(self) -> None:
        """写一条 no-op 日志并等其提交 → 之后本地读是线性一致的。"""
    def linearizable(self):
        """上下文：with log.linearizable(): ... 期间的读走 barrier 后的本地副本。"""
```

`require_leader=True` 语义**保留**（更便宜的近似），并在文档写明二者差异：
- `require_leader`：只保证「不在 follower 上读」，**不是**严格线性一致
- `barrier()`：**严格线性一致**，代价一条日志写入

### 1.3 K-83：`RaftCluster.from_addrs()`

```python
@classmethod
def from_addrs(cls, node_factory, *, addrs: list[str], data_dir: str | None = None)
```
校验：≥3 地址、无重复、`host:port` 格式、`data_dir` 可写。
文档保留边界：跨机 / 分区 / 时钟漂移未测。

---

## 2. 测试

| # | 用例 | 判据 |
|---|---|---|
| S18-1 | **K-87 父子树** | 建 `run → child` 树；`child.reserve` → **父的 `subtree_reserved` 随之增加**（扁平账本做不到） |
| S18-2 | K-87 归档折叠 | 子树归档 → 父 `folded_consumed` 增加、`subtree_reserved` 回落；**三副本一致** |
| S18-3 | K-87 不变量可验证 | 在**非绑定副本**上跑 `check_invariants()` 通过（原实现会失败/无树） |
| S18-4 | K-87 上限拒绝 | 父限额 10、子要 20 → 拒（链式检查生效，扁平账本也过但树语义不同） |
| S18-5 | K-87 重启恢复 | 建树+预留+归档 → compact → 重启 → 树结构与账目完整 |
| S18-6 | **K-82 线性一致读** | `barrier()` 后读到的 `tail()` ≥ 写入前；**负对照**：不 barrier 时 follower 可能落后 |
| S18-7 | K-82 与 `require_leader` 区别 | `require_leader` 在 follower 抛错；`barrier` 不抛但开销一条日志 |
| S18-8 | **K-83 from_addrs** | 合法地址列表可建簇；<3 / 重复 / 非法格式 → `ValueError` |

---

## 3. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/budget.py` | `archive_node(..., now=None)`（唯一改动点，向后兼容） |
| `harness/protocols.py` | `BudgetManager.archive_node` 同步签名（C4 只看成员名，不受影响） |
| `harness/storage/raft.py` | `_ReplicatedBudget` 改用真 `BudgetManager`；`RaftBudgetManager` 读全部委托副本；`barrier()` / `linearizable()`；`RaftCluster.from_addrs()` |
| `tests/test_s18_raft_rsm.py`（新） | S18-1..S18-8 |
| `docs/milestones.md` | MS9 出口补「K-87 已收敛」 |
| `scripts/check_s18_conformance.py`（新） | 防漂移核对 |

---

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| RSM 内 `archive_node` 的 `now` 若各副本不同 → 发散 | **由 leader 决定并复制**（`now` 作为命令参数） |
| `BudgetManager` 在副本内 emit 事件 | `event_log=None` → `_emit` 直接 return（已核实 [budget.py:229](harness/budget.py)） |
| `barrier()` 每次读一条日志 → 写放大 | 文档写明代价；默认**不**自动使用，仅 `linearizable()` 显式开启 |
| 树语义改动破坏现有 S17 测试 | S17 的扁平假设（`_subtree_nodes` 返回自身）会被**真实树**取代 → 需改 S17 对应断言（是**加强**） |
| K-83 仍不可测跨机 | 明确保留为边界，不假装 |

---

## 5. 决策

| # | 结论 |
|---|---|
| Q1 | K-87 用「真 `BudgetManager` 做 RSM」而非继续扩扁平账本 —— 否则树/归档语义永远是假的 |
| Q2 | `archive_node` 加 `now` 参数（唯一非确定性入口），由 leader 复制 |
| Q3 | K-82 用**读穿日志**（唯一正确路径），并保留 `require_leader` 作为廉价近似 |
| Q4 | K-83 只加 `from_addrs` + 校验；跨机仍是边界 |
| Q5 | 多租户/计量推迟 S19（避免本波过载） |

---

## 6. 实现结果（v1.0 → v1.1）

### 6.1 交付

| 项 | 状态 | 实现 |
|---|---|---|
| **K-87** | ✅ | `BudgetManager` 加 `__getstate__`/`__setstate__`（去掉 `RLock`/`event_log`/epoch 源 → 可 pickle）；`_ReplicatedBudget` **直接持有真 `BudgetManager`**，`create_node/reserve/consume/release/settle/archive_node/remove_node/gc` 全走共识；`RaftBudgetManager` 读全部委托 `_bm`（树/账本/不变量真实） |
| **K-87 装配面** | ✅ | 补 `_lock` / `_last_budget_seq` / `_reset_baseline`（否则「装进 Harness 即 `AttributeError`」） |
| **K-82** | ✅ | `_ReplicatedLog.barrier()`（no-op 日志）+ `RaftEventLog.barrier()/linearizable()`；**barrier 后刷新本地视图** |
| **K-83** | ✅（收窄） | `RaftCluster.from_addrs(addrs, data_dir)`：≥3 / 去重 / `host:port` 校验。跨机/分区仍未测 |

### 6.2 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **604 passed + 4 xfailed**（连跑 3 轮，~37s/轮；基线 590 → 604，+14） |
| 覆盖率 | 门禁通过 |
| 核对脚本 | S14 45/45 · S15 73/73 · S16 32/32 · S17 56/56 · **S18 37/37** |

### 6.3 终审（对抗式）发现的 4 个 P0 —— 全部已修

| # | 问题 | 反例 | 修法 |
|---|---|---|---|
| **P0-1** | **`barrier()` 不刷新本地视图**：只等 `barrier_count`，`_events` 仍是旧缓存 | 在**另一个节点**写一条 → `barrier()` 后 `all()` 仍只有 1 条 | barrier 内 `_sync_applied()`；`has()` 在 `require_leader` 下也等 `applied>=commit` |
| **P0-2** | **`consume` 丢 `acb` → claim fencing 静默失效**（副本无 `claim_guard`） | 带 `claim_epoch` 的 consume 照常成功 | **fail-closed**：带 claim 直接 `RaftUnavailable`（K-89），不假装成功 |
| **P0-3** | **`archive_node`/`remove_node` 恒返回 True** | `archive_node("ghost") → True`；Harness 据此清空 `budget_node` → 节点/预留泄漏 | 副本记录 `last_ok` 并回传真实结果 |
| **P1-4** | **`create_node` 静默返回旧节点** | 先建 limits=100、再建 5 → 仍返回 100 | 回传 `last_ok`；冲突返回 `None` |

### 6.4 终审发现的 P2（记录为边界，未修）

| # | 边界 |
|---|---|
| **K-88** | 快照**无上限**：`_reservations` 只清 `ARCHIVED`+gc；树 400 层约 37KB、2000 条已 release 仍留 117KB。长跑需节点/预留数上限 + 修剪 `RELEASED/SETTLED` |
| **K-89** | **claim fencing 未复制**：`RaftBudgetManager.consume(acb=...)` 带 claim 时**拒绝**（fail-closed）；`pool` 模式下应改用单机 `BudgetManager` |
| **K-90** | **重放仍取 wall clock**：`_apply_archived` 用 `time.time()`，事件 payload 无 `expires_at` → 重放与 RSM 的 tombstone 过期可能不一致 |
| **K-91** | `check()` 语义是「该节点**任一** ACTIVE 预留余额够付」，与「剩余额度」不同；`check_reservations` 亦为 ANY 语义（协议注释已说明） |
| **K-92** | `from_addrs` 只验格式，**不验连通性**；不可达地址会在 `wait_ready` 等满 20s。外部集群的密码硬编码 `harness-raft`（应可配） |
| **K-93** | `RaftBudgetManager.gc()` 返回副本真实删除数，但 `archive_node` 的 `now` 由**客户端**取（仍确定，随命令复制）；`remove_node` 的 `owner` 校验未复制 |

### 6.5 因「语义升级」而改写的既有断言（加强，非放宽）

| 用例 | 原断言 | 现断言 | 理由 |
|---|---|---|---|
| S17 `check()` | `check("n1",5) is False`（基于我自造的「剩余额度」语义） | `check("n1",6) is True` / `check("n1",7) is False` + `remaining == 4` | 真实语义是「任一 ACTIVE 预留余额够付」（K-91） |
| S17 `_resv` | 读扁平账本 `_replica._resv` | 读真账本 `_replica.bm._reservations`（并按 `(rid,node,amount,state)` 归一比较） | 扁平账本已不存在 |
| S18-6 | `tail() >= 2`（绑定副本上恒真） | 从**另一节点**写入后 barrier，要求 `tail() == 2` | 原断言是恒真（终审 P0-1） |
| S18-3 | 直接在其它副本跑不变量 | **先等收敛**再跑，并断言语义值 | 任意日志前缀都满足不变量 → 原断言无判别力 |
