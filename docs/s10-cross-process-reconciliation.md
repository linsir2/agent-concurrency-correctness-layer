# S10：跨进程预算对账 + 跨进程锁与 fencing

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（已按两路对抗评审修订）** |
| 日期 | 2026-10-27 |
| 上游 | [S5 v1.2](s5-recovery-and-journals.md)（非目标：跨进程预算对账）、[S9 v1.2](s9-persistence-and-receipts.md)（K-33/K-34） |
| 覆盖 | **MS9** 跨进程预算对账、**K-34** 跨进程 scope 锁 |
| **推迟** | **egress 档 (a) 实现整体推迟阶段二**（评审结论：本质不可绕过，做绕过检测是「君子协定」）；只保留 `FinalitySink` 接口草案 |
| 分波 | **S10a** 预算事件溯源 + 全量重放 → **S10b** 跨进程 flock + fencing + WAL seq → **S10c** 接口草案 |
| 修订 | v1.0 → v1.1：修 2 P0 + 8 P1（§8） |

---

## 0. 调研结论（决定 7 个选择）

### 0.1 跨进程预算对账：真实缺口 + 一个**假绿**

| # | 缺口 | 后果 |
|---|---|---|
| G1 | checkpoint 只存**单 ACB 祖先路径 + 本节点逐笔**，无兄弟/全局摘要 | 少计 → **超发** |
| G2 | `root.limits` **从不持久化** | 配置漂移静默改边界 |
| G3 | 预算变更**几乎不进 EventLog** | 无法重放重建整棵树 |
| G4 | **`check_invariants` 只遍历 `self._nodes`** → 兄弟缺失不触发 | **假绿** |
| G5 | 准入全在进程内 | 跨进程无全局仲裁 |

**超支路径**：P1 reserve 300 → P2 只恢复兄弟 ACB2 → P2 的 `root.subtree_reserved = 0` → 再 reserve 到 1000 → **1300 > 1000**。
> **只要兄弟预留不可见，任何基于 `root.subtree_reserved` 的本地判断都会超发**（与崩溃无关）。

**单进程窗口是安全的**（reserve 丢→少发；consume 丢→受 `amount` 硬上限约束；release 丢→误拒）。**只有跨进程兄弟不可见才超支。**

### 0.2 关键修正：**摘要方案（b）有结构性缺陷，改以事件溯源（d）为主**

评审证明 b 方案三个 P0：
1. 「epoch ≤ 本地最大值才采纳」→ **静默丢弃更新的兄弟数据** → 原样复现超发
2. 「重建节点数 == 摘要节点数」→ **同义反复**（摘要是唯一来源）→ 假绿保留
3. 摘要覆盖**不幂等、顺序相关**；聚合值与逐笔叠加**口径不清** → 双重计数

**v1.1 定死**：**预算变更事件溯源（d）为唯一真相源**，摘要降级为**仅交叉校验**（非权威）。

### 0.3 跨进程锁：**flock 最匹配「durable 单写者」**

| 维度 | flock | Redis/PG | 租约+fencing | per-scope WAL |
|---|---|---|---|---|
| 崩溃安全 | ✅ **OS 自动释放，无 stale** | 中/高 | 需超时判断 | ✅ |
| 依赖 | 无（POSIX） | Redis/PG | ZK/DB | 无 |
| NFS | ❌ **不安全** | ✅ | ✅ | ✅ |

### 0.4 egress：分清**两个轴**（调研最重要洞察）

| 轴 | 问题 | HMAC | egress |
|---|---|---|---|
| **A 权限伪造（自证）** | 能否伪造 authority | 不能 | **不能** |
| **B 强制点绕过** | 能否不遵守就产生后果 | 无关 | **能** |

**egress 不解决 K-15/K-10，但改变威胁模型**（agent 是否同进程不再影响强制结果）。
**四档**：a 进程内（**不解决**）/ b sidecar（部分）/ c 内核 eBPF·Landlock（**解决**）/ d 沙箱（根本）。
**本阶段只能做 a**，且评审结论：**整体推迟**（做绕过检测是「君子协定」，投入产出比低，且放大 `_gateway_check` 拒绝面会波及 214 用例）。

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | 预算事件溯源 + 全量重放建**完整树** | S10-1 不超发 |
| G2 | `root.limits` 持久化 + 漂移检测 | S10-2 |
| G3 | 兄弟可见（重放而非摘要） | S10-3 |
| G4 | 树完整性**锚定事件声明的节点集**（非同义反复） | S10-4 |
| G5 | 全局对账等式（3 条） | S10-5 |
| G6 | 跨进程 flock + WAL seq + fencing | S10-6/7/8 |
| G7 | `FinalitySink` 接口草案 | 文档 |

---

## 2. S10a 设计（v1.1 重写）

### 2.1 预算变更事件溯源 = **唯一真相源**

```python
# 新增 EventKind（payload 统一带 budget_epoch 与 node 全量身份）
BUDGET_NODE_CREATED  # {node_id, parent_id, limits, epoch}
BUDGET_RESERVED      # {node_id, rid, amount, epoch}
BUDGET_CONSUMED      # {node_id, rid, amount, epoch}
BUDGET_RELEASED      # {node_id, rid, amount, epoch}
BUDGET_SETTLED       # {node_id, rid, amount, epoch}
BUDGET_ARCHIVED      # {node_id, folded_consumed, epoch}
BUDGET_EPOCH         # {epoch, reason}              # epoch 单调递增，与事件同流
```

**写入顺序（不可换）**：
```
① 同一临界区内：改内存账 + append_batch(BUDGET_*) + fsync
② 返回
③ checkpoint 只存 budget_cursor（+ 聚合值**仅作 checksum**）
```

**关键**：**内存账与事件在同一临界区**，checkpoint 快照必须在事件 append **完成之后**取（否则聚合含新 reserve 而 cursor 不含 → 重放双份，见 §8 P1-3）。

### 2.2 恢复 = 纯重放（摘要降级为交叉校验）

```python
def _restore_budget(data):
    # ① run_limits 漂移检测（修 G2）
    if data["run_limits"] != configured: → QUARANTINED
    # ② 从 EventLog 重放 BUDGET_* 事件（从 0 或 budget_cursor），**重放前重置基线为 0**
    #    （不叠加到 checkpoint 聚合上 —— 修 P1-3 双份）
    # ③ 按 rid 幂等（set 语义）
    # ④ 摘要仅用于交叉校验：不等 → needs_readmit
```

> **重放语义**：`_replay_events` 从 `event_cursor` 开始，`seq >= cursor` 的事件都会重放 → **不是漏，是重复应用**。所以必须「**重放前重置基线**」，且预算投影按 `rid` **幂等**。

### 2.3 树完整性校验（修 P0-2：非同义反复）

**真相源 = EventLog 声明的节点集**，不是摘要：

```python
declared = {ev.payload["node_id"] for ev in budget_events}
rebuilt  = set(self.budget._nodes)
if rebuilt != declared:
    # 缺席是否合法？运行中兄弟未落盘 → 用 budget_epoch / slots_active 判定
    if 缺席不合法: → needs_readmit
    else: 记录为「运行中缺席」（不算失败）
```

**必须容忍「运行中兄弟未 checkpoint」** —— 否则可用性归零（评审反例 B）。

### 2.4 全局对账等式（不依赖树遍历）

```python
root.subtree_reserved == Σ_{所有节点} self_reserved
root.consumed          == Σ_{所有节点} (consumed + folded_consumed)
Σ_{ACTIVE} amount      == Σ self_reserved − Σ_非ACTIVE.consumed − Σ folded
```

### 2.5 跨进程准入：单写者 + **lease 接管**（修 P1-4）

| 组件 | 处理 |
|---|---|
| 写者身份 | 持 `store.lock` 下的**租约**（带 `expires_at`） |
| 接管 | 确认旧租约过期 → `bump_epoch` → 落盘 → 才开始服务 |
| `bump_epoch` | **同一 flock 内 read-modify-write**（禁止「读旧值+1 写回」→ 两进程都写 5 → epoch 相同 → 不 fence） |
| `SlotTracker` | **必须下沉共享存储**或并入同一配额服务（否则槽位与预算各超各的） |

### 2.6 预算事件必须 `notify=False`（修 P1：gc 用例）

`_on_event` 用 `_event_count` 每事件 +1 触发 gc → 新增预算事件会**提前触发** gc → `test_s8_coverage` 的「第 2 个事件触发」精确断言全破。
**预算事件 append 时必须 `notify=False`**。

---

## 3. S10b 设计

### 3.1 `_FileLock` 下沉（修 P1-5：循环导入）

```
checkpoint.py → event_log.py → storage/wal.py
若 wal.py `from ..checkpoint import _FileLock` → 成环 → ImportError
```

**修法**：新建 `harness/storage/_lock.py` 放 `_FileLock`；`checkpoint.py` 与 `wal.py` 都从那里导入。

### 3.2 三把跨进程锁的**全序**（修 P1-5：死锁）

```
resource → receipts.lock → wal.lock → store.lock → eventlog._lock
```

**写死并加断言**禁止反向获取（路径甲持 `receipts.lock` 取 `store.lock` 会成环）。

### 3.3 WAL seq：**缓存尾偏移 + fstat**（修 P1-6）

| 方案 | 问题 |
|---|---|
| 每次 append 全量扫描 | **O(N²)** + 内存 O(N) |
| 只信内存 `_seq` | **跨进程仍冲突**（正是要修的） |

**修法**：
```python
with wal_flock:                       # flock 覆盖「读-分配-写-fdatasync」全程
    if os.fstat(fd).st_size != self._cached_end:   # O(1) 比对
        self._rescan_from_disk()                   # 极少发生
    base = self._seq
    append_frame + fdatasync
    self._cached_end = os.fstat(fd).st_size
```

### 3.4 fencing epoch：**同一 flock + append 前复验**（修 P1-7）

```python
# 写者：append 前、同一 flock 内读 store.json 的 epoch
with store_flock:
    if self._epoch < read_epoch(store.json): → 立即拒绝（不写入、不 ack）
    append_frame(records with self._epoch) + fdatasync
# bump_epoch 也用同一把 store_flock
```

**修两个反例**：
- **ack 后丢写**：写者 epoch=4 的帧已 ack，监督者 bump 到 5 → 恢复时拒绝该帧 → **持久性违背**。→ 写者 append 前复验，小于即拒。
- **作用域矛盾**：v1.0 说 run 级、store 级混用 → **统一为 run 级**（store 级会一次 bump 作废其他 run 的租约）。

---

## 4. S10c：`FinalitySink` 接口草案（仅此一项）

```python
class FinalitySink(Protocol):
    def authorize(self, candidate: CandidateAct) -> Authority: ...   # 非 bearer/单次/可撤销
    def enforce(self, actual_call: Call, authority: Authority) -> Verdict: ...
    # Verdict ∈ {ALLOW, DENY, TRANSFORM, DRY_RUN, APPROVE_REQUIRED}
```

对齐 IETF `draft-das-agentic-execution-finality`（验证在**生效边界**、针对**这一次实际调用**）与 `alternate-path closure`。

**本阶段不做**：`_gateway_check` 提升、`net.py`、绕过检测 —— **整体推迟阶段二**（评审结论：档 (a) 本质不可绕过）。

---

## 5. 测试

| # | 用例 | 关键判据（含负对照） |
|---|---|---|
| S10-1 | **跨进程不超发** | ①**前提断言**：每个 payload 的 `budget_siblings` 含另一兄弟（否则测错对象）；②P1 reserve 300 落盘 → **关闭 WAL** → P2 从磁盘取 ref → 断言 `root.subtree_reserved == 600` → 再 reserve 700 **被拒**；③**负对照**：禁用事件重放 → 必超发 |
| S10-2 | run_limits 漂移 | 配置与账本不一致 → `QUARANTINED`；一致 → 正常 |
| S10-3 | **重放锚定事件身份** | ①事件层：`budget_cursor` 后新增事件的 `event_id` 全在 `_applied_event_ids`，且每个 `rid` 能在重建树找到、`payload.consumed == resv.consumed`；②节点层：节点数 + **每个节点** self/subtree/consumed/folded **逐一相等**；③全局等式 |
| S10-4 | **树完整性（真实路径）** | 用 **epoch 过期**构造缺失兄弟（走 `_restore_budget` 的 epoch 过滤代码，**不删文件**——删文件是作弊）；断言 `needs_readmit`；**负对照**：完整树必须**不抛**且 `declared == rebuilt` |
| S10-5 | 全局对账等式 | 3 条等式在重放后成立 |
| S10-6 | 跨进程 scope 锁 | **文件信号 barrier**（复用 S9-7 marker 模式，**不用 `threading.Barrier`**——跨不了进程）；正对照：flock 生效 → barrier 不超时 + `verify_receipts is True`；**负对照**：`nullcontext` 替锁 → barrier 到达 → 链分叉 → `verify is False` |
| S10-7 | WAL seq 不冲突 | 两进程**同时**进入 `append_frame`（barrier）；断言 seq 集合**恰为 `1..2N`**（无重复、无空洞）+ `_load` 不抛 `WalCorrupt` + **总记录数 == 两进程写入数之和**（防「丢一方但 seq 仍连续」的假绿） |
| S10-8 | fencing | 旧 epoch 写者被拒；**ack 后丢写**反例：写者 append 前复验 → 拒绝 |
| S10-9 | 预算事件不触发 gc | `notify=False` → `_event_count` 不受影响 → 现有 gc 用例仍绿 |

---

## 6. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/storage/_lock.py`（新） | `_FileLock` 下沉（解循环导入） |
| `harness/types.py` | 新增 7 个 `BUDGET_*` EventKind |
| `harness/budget.py` | 预算操作在临界区内 append 事件（`notify=False`）；`check_invariants` 增全局等式 + **仅跨进程启用**树完整性 |
| `harness/checkpoint.py` | payload 增 `run_limits`/`budget_cursor`；`_FileLock` 改从新模块导入；`budget_siblings` **仅交叉校验** |
| `harness/harness.py` | `_restore_budget` 重写（**重放前重置基线** + 幂等 + 漂移检测）；`_replay_events` 增 budget 投影；租约接管；`bump_epoch` 原子 RMW |
| `harness/orchestration.py` | `try_admit` 单写者仲裁；`SlotTracker` 下沉 |
| `harness/storage/wal.py` | `append_frame` 在 flock 内「fstat 比对 → 分配 → 写」；记录 `fencing_epoch` |
| `harness/intent_registry.py` | `epoch` 读持久化值（run 级） |
| `harness/finality.py`（新） | `FinalitySink` 接口草案 |
| `tests/` | `test_s10_budget_reconcile.py`、`test_s10_cross_process_locks.py` |

---

## 7. 风险与对策

| 风险 | 对策 |
|---|---|
| 预算事件放大 WAL（K-33） | 事件体小；评估 retention 影响；必要时拆独立账本（方案 c） |
| 重放双份 | **重放前重置基线** + 按 `rid` 幂等 |
| 摘要与重放不一致 | 摘要**仅交叉校验**，不等 → `needs_readmit` |
| 单写者不可用 | lease + 接管 |
| flock 在 NFS 不安全 | 文档明确「仅本地 FS」 |
| 破坏 214 用例 | 见 §8 回归防护 6 条 |

---

## 8. 评审修订记录（v1.0 → v1.1）

### 8.1 P0（2 项，均「修复后仍超发/假绿」）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | **epoch 过滤主动复现 bug**：「只采纳 epoch ≤ 本地最大值」→ P2 读到 P1 的 epoch=5 > 本地 3 → **不采纳** → `subtree=0` → 超发 1300。且「混合 epoch → needs_readmit」→ `requeue_readmit` 只看本地树 → **再次超发** | **放弃摘要过滤**：预算事件溯源为唯一真相源；摘要**仅交叉校验** |
| **P0-2** | **树完整性校验是同义反复**：b 方案无事件，「账本节点数」唯一来源是摘要本身 → 「重建数 == 摘要数」**恒真** → G4 假绿原样保留 | 完整性**锚定 EventLog 声明的节点集**；并**容忍运行中兄弟未落盘**（用 epoch/slots 判定缺席是否合法） |

### 8.2 P1（8 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| 1 | **摘要覆盖不幂等、顺序相关**：同进程先 recover(ACB1) 再 recover(ACB2)，兄弟 X 被后恢复者覆盖 → 取值随恢复顺序变 | 摘要仅校验；权威值来自重放 |
| 2 | **聚合与逐笔口径不清** → 双重计数（父 = 400 应为 300） | 明确「摘要仅校验；权威 = 自底向上重算」 |
| 3 | **预算事件重放双份**：checkpoint 快照在「①改内存账之后、②append 之前」取 → 聚合含新 reserve 而 cursor 不含 → 恢复重放该事件 → **叠加两份**。且 `applied_event_ids` 存 checkpoint 里，窗口内未更新 → **去重失效** | 内存账 + 事件 + 游标**同一临界区**；**重放前重置基线** |
| 4 | **单写者无 lease** → SIGKILL 后准入永久不可用；`bump_epoch` 非原子 → 两进程都写 5 → **不 fence** | lease + 接管；`bump_epoch` 同一 flock 内 RMW |
| 5 | **`_FileLock` 复用成循环导入**（checkpoint→event_log→wal→checkpoint）+ **三锁偏序未定义** → 死锁 | 下沉 `storage/_lock.py`；写死全序 `resource→receipts→wal→store→eventlog` |
| 6 | **WAL seq**：全量扫描 O(N²) 或内存 `_seq` 跨进程冲突 | flock 内 **fstat 比对缓存尾偏移**（O(1)），极少回扫 |
| 7 | **fencing 非原子**：ack 后 bump → 恢复拒绝已 ack 帧 → **持久性违背**；run/store 作用域矛盾 | append **前**同一 flock 内复验 epoch；统一 **run 级** |
| 8 | **egress 档 (a)**：默认拒绝会崩 214 用例；绕过检测不稳定；文档自相矛盾 | **整体推迟阶段二**，只留接口草案 |

### 8.3 回归防护（6 条，防破坏 214 用例）

| # | 防护 |
|---|---|
| 1 | 预算事件 **`notify=False`**（否则 gc 用例的「第 N 个事件」断言全破） |
| 2 | `_restore_budget` **兼容手构 dict**（`test_ms2`/`test_s8` 直接调它，无新字段 → 保持旧分支） |
| 3 | `check_invariants` 树完整性**仅跨进程恢复启用**（单进程保持原语义） |
| 4 | `Action` 新字段 **`Optional = None`**，`_gateway_check` 保持「无字段 → 放行」 |
| 5 | `test_ms2_restore_budget_children_never_overwritten`（断言 `subtree==50`）**不得被摘要重建覆盖** |
| 6 | `test_s9_sigkill` 的 `child_seq == old_seq + 1` 精确断言**不得因 flock 改变时序** |

### 8.4 测试强度修正

| 问题 | 修正 |
|---|---|
| S10-1 判据与过滤规则冲突 | 改为「共享 WAL + 单写者」串行化 + **前提断言**兄弟摘要存在 + **负对照**（禁用重放必超发） |
| S10-3 只断言「树一致」是弱断言 | 三层：事件身份（`event_id`/`rid`）+ 逐节点精确值 + 全局等式 |
| S10-4 删文件是**作弊** | 用 **epoch 过期**（真实路径）+ 负对照（完整树不抛） |
| S10-6 `threading.Barrier` 跨不了进程 | **文件信号 barrier** + 负对照 |
| S10-7 串行执行会假绿 | barrier 强制同时进入 + 断言「总数 == 两进程之和」 |
| S10-9 无正对照 → patch 未生效会误判 | 子进程内 patch + **正对照证明 patch 生效** + 断言特定告警 EventKind |

### 8.5 新增已知边界（v1.1）

| # | 边界 |
|---|---|
| K-40 | 预算事件**放大 WAL**，加剧 K-33（全局 tail 跨 run 淘汰）；必要时拆独立账本 |
| K-41 | 「运行中兄弟未落盘」的缺席**不算失败**（否则可用性归零）→ 完整性校验有**有界盲区** |
| K-42 | egress 档 (a) **推迟阶段二**；本阶段仅 `FinalitySink` 接口草案 |
| K-43 | `fencing_epoch` 为 **run 级**；跨 run 需各自 epoch |

---

## 9. 实现后对抗评审（v1.1 → v1.2）

实现分两波（S10a 预算事件溯源 → S10b 跨进程锁），随后两轮对抗评审。**1 P0 + 4 P1 + 10 P2/P3 已修**。

### 9.1 P0（1 项：durable 部署必现挂死）

| # | 问题（实测） | 修法 |
|---|---|---|
| **P0-1** | **`budget._lock ↔ store.lock` AB-BA 死锁**：路径 A `budget.reserve → _emit → append → wal fencing read_epoch → store.lock`；路径 B `checkpoints.save → store.lock → _build_payload → budget._lock`。两线程并发结算 **8s 内挂死**。`LOCK_ORDER` 全序表**不含 `budget._lock`**（裸 RLock）→ 断言框架失明，245 个用例全绿也拦不住 | `read_epoch` 改**无锁原子读**（`store.json` 由 `atomic_write` 写 → 不撕裂）→ 断开 A 路径的 `store.lock`。**压测验证**：8 线程 save × 8 线程 reserve，修复前超时挂死，修复后全部完成 |

### 9.2 P1（4 项）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P1-1** | **`BUDGET_ARCHIVED` 重放只摘单个节点**（`_apply_archived` 只 `pop(node_id)`），而 `archive_node` 摘除**整个子树** → 重放后孤儿子节点残留 → E1 破 → **合法归档后恢复反而失败**；孤立预留永不进 gc → 无界增长 | `_apply_archived` 复用 `_subtree_nodes` 整体摘除 |
| **P1-2** | **单写者租约用绝对时间戳** + 跨进程时钟偏移 → 时钟快的进程提前判定过期 → 接管 → **双写者**；时钟慢的进程 → **永久拒绝服务** | 过期判定改用内核单调证据（`st_mtime`）/本地时钟自洽差 |
| **P1-3** | **`commit_batch` 返回 `UPSTREAM_FAILED`** → runtime 通用分支直接判任务 `FAILED`（无重试、无隔离）→ 60s 租约争用被放大成整条 plan 失败传播 | 改 `QUARANTINED + needs_readmit`，交 `requeue_readmit` |
| **P1-4** | **WAL fencing 读的是本 Harness 自己的 store**，而 `event_log` 与 `checkpoint_root` 是**两个独立构造参数** → 共享 WAL 的两实例互不 fence → K-43 失效 | 绑定 WAL 与 store 归属；epoch 写每帧 |

### 9.3 P2/P3（10 项，择要）

| 问题 | 修法 |
|---|---|
| 收据 head **锁内刷新**：跨进程下 `__init__` 时构建的 head 缓存过期 → 链分叉（实测 h1/h2 场景） | 锁内从 EventLog 刷新 head/ids |
| `_tree_integrity` **只查 `declared - rebuilt`**（单向）→ `rebuilt ⊃ declared` 时静默通过 | 双向校验 + 数值校验（`root.subtree == Σ self`） |
| epoch 豁免可被绕过成「永远合法」 | 要求显式 `BUDGET_EPOCH` 事件锚定 |
| `requeue_readmit` 抹掉 `RUN_LIMITS_DRIFT` 信号 | 漂移禁止 re-admit |
| `consume` 在 `_lock` 外 emit + 重放 CONSUMED 无 state 守卫 | emit 移回锁内 + 加 `state != ACTIVE` 守卫 |
| 重放失败回滚用 `remove_node`（拒绝非空节点）→ 不回滚 | 强制摘除 + `_reset_baseline` |
| `_reset_baseline` 不持锁、不重置 `_epoch` | 移入锁 + `_epoch=0` |
| `declared` 无 `run_id` 过滤（budget 事件根本没带 run_id） | `_emit` 增 `run_id`；`budget_events`/`_declared_budget_nodes` 按 run 过滤 |
| SlotTracker 共享读-改-写非原子 → 跨进程槽位超发 | 同一 flock 内 CAS（`update_slots`） |
| 租约未接线（`_try/_renew` 生产路径从未调用） | `__init__`/`recover` acquire；`commit_batch` 前 renew |

### 9.4 基准诚实修正

**B3（并发）的 `conflict_rate`/`wasted_work` 本质非确定** —— 它们是**线程交错的函数**（实测 30 次里偶发 1 次 0.6 vs 0.5555）。这不是实现 bug，是并发负载的固有性质。

**修正**：`assert_deterministic` 只对**串行条件**（B1/B2）强制逐值相等；B3 改用**有界带**（±25%，期望 0 的指标严格 0）+ 记录分布。**B1/B2 仍逐值相等** —— 若它们也非确定，那**才是** bug。

### 9.5 报告同源性

新增 `bench/regen_report.py`：从 `result.json` 重新生成 `report.md` 的**数值表**（`<!-- BEGIN:AUTO -->` 块）。时序指标每次重跑后刷新，不再与 `result.json` 脱节；叙述章节（§1–§3、§5–§9）由人维护。

### 9.6 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| K-44 | 收据 head 锁内刷新**每次 commit 扫 EventLog**（O(N) 热路径）→ 需增量索引（未做） |
| K-45 | 收据刷新失败时回退**不安全**（静默 → 分叉 + 永久 tainted）→ 应拒绝服务或重置水位 |
| K-46 | `fencing_epoch` 绑定 store 而非 WAL → 跨实例共享 WAL 的 fencing 语义未完全收口 |
| K-47 | 「运行中兄弟未落盘」的缺席不算失败 → 完整性校验有**有界盲区**（K-41） |
| K-48 | B3 并发指标本质非确定 → 报告用有界带；**B1/B2 必须逐值稳定** |
