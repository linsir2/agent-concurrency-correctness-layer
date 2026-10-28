# S21：K-97 租户准入 · K-99 过期租约回收 · K-100 墓碑窗口 · 优先级份额

| 项目 | 内容 |
|---|---|
| 版本 | v1.1（评审修复后） |
| 日期 | 2026-10-28 |
| 上游 | [S20 v1.1](s20-tenant-metering-and-k94.md) §6.4（K-97..K-101）、[milestones.md](milestones.md) MS10 |
| 覆盖 | **K-97**（`plan["tenant"]` 自动接准入）、**K-99**（过期租约回收器；跨机状态复制见 K-103）、**K-100**（墓碑窗口量化 + 时点安全）、**优先级份额**（tier reservation） |
| 硬约束 | 631 passed 基线全绿（终态 647 passed / 4 xfailed）；不变量 I5c/E1/E2 不受影响；「无 tenant 且无 tier 时行为完全不变」 |

---

## 0. 调研结论（代码实况 + 外部依据）

### 0.1 K-97：准入管线形态（决定「低风险接入」可行）

| 事实 | 位置 |
|---|---|
| 父节点由 `task["footprint"]["parent_budget"]` 决定，**默认 `"run"`** | [orchestration.py:148](harness/orchestration.py)、[:589](harness/orchestration.py) |
| 它是 **task/footprint 层**，不是 `spec` 层 | `Planner.plan` 产出 `footprint.parent_budget` |
| 每 task 独立 → 同 run 不同 task 可挂不同父节点（已被 `spawn_child` 使用） | 同上 |

**结论**：在 `_try_admit_commit` 的**唯一一处**做「无 `parent_budget` 时用 `tenant:<t>`」的解析即可，
**改动 1 处 + 1 个解析函数**，风险远低于动 `spec` 管线。

### 0.2 K-99：租约的真实生命周期（追根溯源）

| 阶段 | 事实 | 证据 |
|---|---|---|
| 创建 | `reserve` 设 `expires_at = now + ttl_ms` | [intent_registry.py:70](harness/intent_registry.py) |
| 判定 | `is_valid` 按 `expires_at` 惰性失效（**不删除**） | [:86-99](harness/intent_registry.py) |
| 续期 | `renew_for` **只续期**，跳过已过期的 | [:130-145](harness/intent_registry.py) |
| 回收 | **没有任何主动回收**（`_leases` 只增不减） | 全仓无 reaper |
| 跨进程 | 已**跨进程判定**：两个 worker 同进程内共享 `IntentRegistry` → 单写者；跨进程靠 Raft epoch + `flock` | S10/S16 |

**两个层次要说清**：
- 「分布式租约」（**跨主机**）——已有：单写者 + Raft epoch。**缺口只在「孤儿回收」**。
- 「租约状态复制」——**不需要**：同进程共享字典即可，复制反而引入写放大与心跳风暴。

→ **K-99 收窄为「过期租约回收器（reaper）」**，并把「状态复制」记为 K-103。

### 0.3 K-100：墓碑窗口必须用「**租约时长**」而非「真实时间」

`lease_ms` 默认 **60s**，但**可配置**（`Harness(lease_ms=...)`）；claim 租约
`claim_lease_ms` 还可更大（构造期已校验 `>= lease_ms`）。

**量化公式**（写死进实现与文档）：
```
强制阈值（逐墓碑）= max_lease_ttl_s = ceil(max(lease_ms, claim_lease_ms) / 1000)
诊断量（满员覆盖窗口）= claim_tombstone_max × max_lease_ttl_s
```

**强制规则**：墓碑只有「入墓已满 `max_lease_ttl_s`」才允许被忘记。理由：此刻
旧 worker 的租约（含 claim 租约）**必然已过期**，即使 `claims.get` 回落到 0，
其旧 epoch 也会被租约/claim 校验拒绝 ⇒ fencing 不被削弱。

**为什么不能向下取整**：`lease_ms=1500` 的真实租约是 1.5s，若取整成 1s，墓碑
会在旧持有者租约仍有效时被忘记。故用 `ceil`，且取 `max(lease_ms, claim_lease_ms)`。

**容量超限怎么办**：若墓碑数超过 `claim_tombstone_max` 但没有足够老的墓碑 →
**不淘汰**（宁可暂时超界，也不削弱 fencing），并计数 `claim_window_exceeded`。
这是「稳态有界」：超界量 ≤ 一个 `max_lease_ttl_s` 内新增的墓碑数，之后自动收敛。

### 0.4 优先级份额（阶段三）

现状：`limits = min_budget + child_alloc + retry_budget + 1`（[orchestration.py:580-582](harness/orchestration.py)）→ 公平共享、**无预留**。

| 依据 | 结论 |
|---|---|
| Microsoft Learn「按 tier 分配配额」：premium 保留 60%、standard 30%、developer 10% | 保留 + 动态借用（低层可超额用） |
| AWS「Fairness in multi-tenant systems」：token bucket 是**本地准入控制** | 我们已有树 + 链式限额，**不需要新限流器** |

**方案**：`ensure_tenant(t, quota, min_share=0)` → 建 `tenant:<t>:reserved` 子节点（`min_share`）；
任务按 `tier` 决定 `parent_budget` 指向 **reserved 或 shared**（超额时借用 shared）。

---

## 1. 设计

### 1.1 K-97：`parent_budget` 的 tenant 解析

```python
def _tenant_of(self, acb, task) -> str:
    """spec → footprint → task → **plan 顶层**（四处来源，兼容手工构造的 plan）"""
def _resolve_parent_budget(self, acb, task) -> str:
    """① 显式**非默认** `parent_budget`（≠ "run"）→ 用它
       ② 解析出 tenant 且 `tenant:<t>` 存在 → 用它
       ③ 否则 → "run"（**默认不变**）"""
    explicit = (task.get("footprint") or {}).get("parent_budget")
    if explicit and str(explicit) != "run":
        return str(explicit)
    tenant = self._tenant_of(acb, task)
    if tenant and self.h.budget._nodes.get(f"tenant:{tenant}") is not None:
        return f"tenant:{tenant}"
    return "run"
```
**零风险点**：无 tenant 且无 tier → 返回 `"run"`，footprint 与旧版**逐字节相同**。
`Planner.plan` 仅在 spec 带 tenant/tier 时才写入 footprint（缺省不写键）。

**计量一致性**：`node.tenant` 由**实际父节点 id** 反推（`_tenant_of_node_id`），
而非声明的 tenant —— 否则「声明了 tenant 但节点不存在 → 实际挂 run」的用量会被
错误归集到该租户。`spawn_child` 的子节点同样按父节点反推。

### 1.2 K-99：租约回收器（`reap_expired`）

```python
def reap_expired(self, now_ms: int | None = None) -> int:
    """回收**已失效**的租约。删除条件（任一）：
       ① `not lease.valid`（revoke / IntentRegistry.bump_epoch 的终态）
       ② `lease.epoch != 当前 epoch`（全局 Harness.bump_epoch 只推进 epoch，
          不逐个置 valid=False；只看 ① 会让这些租约永不回收 → 无界增长）
    **不要求「已过期」**：expires_at==0 的失效租约同样要回收。
    `valid=True` **且 epoch 匹配** 的一律不删（哪怕过期）——诊断证据。"""
```
**不复制状态**（记为 K-103）：同进程共享字典已足够；Raft 复制会写放大。
回收由 `Harness.tick()` 每轮触发（与 aging/HITL 超时共用驱动面），并传入
**同一个 `now`**（避免 tick 注入时间与 clock 不同步）。

### 1.3 K-100：`max_lease_ttl_s` + 逐墓碑年龄淘汰（时点安全）

```python
# Harness：向上取整，且覆盖 claim 租约（claim_lease_ms >= lease_ms）
_max_ttl_ms = max(int(lease_ms), int(claim_lease_ms))
self.max_lease_ttl_s = max(1, -(-_max_ttl_ms // 1000))   # ceil
def claim_window_s(self) -> int:
    """诊断量：满员墓碑集合的覆盖窗口 = tombstone_max × max_lease_ttl_s"""
```
**强制点在 Raft RSM**（`_ReplicatedBudget.prune`）：墓碑记录 `claim_tombstone_at`，
只有 `now - 入墓时刻 >= max_lease_ttl_s` 才可淘汰；容量超限且无足够老墓碑 →
**拒绝淘汰**并计数 `claim_window_exceeded`。

**RSM 确定性（关键）**：`now` / `retention_s` / `live_acb_ids` / `max_lease_ttl_s` /
**`tombstone_max`** 全部作为**命令参数**下发，副本内**不读**本地配置
（`RaftBudgetManager` 从 `_replica`（即 `nodes[0]`）读取一次上限并随命令下发，
所有副本用**同一个值**；不要求它是 leader）。

### 1.4 优先级份额

```python
def ensure_tenant(self, tenant, quota, *, parent="run", min_share=0):
    """① 先校验 quota>0 且 0<=min_share<=quota（失败零残留，不建任何节点）
       ② 建 tenant:<t>(quota)；配置冲突（同名不同 quota/parent）显式抛错
       ③ min_share>0 时建 tenant:<t>:reserved(min_share)"""
def _tier_parent(self, acb, task) -> str:
    """tier 规范化（strip+lower）后 ∈ {premium,standard,developer} 且父节点
    确为 tenant:<t> → tenant:<t>:reserved（存在时）；未知/超额 → tenant:<t>"""
```
上限（显式拒绝，不静默截断）：`Σ min_share ≤ quota`；未知档位**不享受**预留
（回落共享池，即「超额借用」）。

**计量一致性**：`node.tenant` 由**实际父节点 id** 反推（`_tenant_of_node_id`），
而非声明的 tenant —— 否则「声明了 tenant 但节点不存在 → 实际挂 run」的用量会被
错误归集到该租户。

---

## 2. 测试

| # | 用例 | 判据 |
|---|---|---|
| S21-1 | **K-97 tenant 接入** | `tenant`（plan 或 task spec）→ 预算节点挂在 `tenant:<t>` 下；租户节点 `subtree_reserved` 反映用量 |
| S21-2 | K-97 负对照 | 无 tenant → 挂在 `run` 下（与现状逐字节相同）；显式 `parent_budget` 优先 |
| S21-3 | K-97 配额生效 | tenant 超额 → 拒；**另一个 tenant 不受影响** |
| S21-4 | **K-99 回收** | 注入失效（`revoke`）→ `reap_expired` 计数 >0；`valid=True` 的**不被删** |
| S21-5 | K-99 负对照 | 未失效 → 不回收；`is_valid` 语义不变；`tick()` 触发回收 |
| S21-6 | **K-100 窗口量化** | `claim_window_s() == tombstone_max × max_lease_ttl_s`；`lease_ms` 越大窗口越大 |
| S21-7 | K-100 时点安全 | 墓碑「未满 `max_lease_ttl_s`」→ **不淘汰** 且 `claim_window_exceeded` 递增；满窗后淘汰到上限 |
| S21-8 | **优先级份额** | premium `min_share=6`（quota 10）→ 挂 `tenant:t1:reserved` |
| S21-9 | 份额超额借用 | 未知 tier → 挂 `tenant:<t>`（共享池），不误吃预留 |
| S21-10 | 份额上限/冲突校验 | `min_share > quota` → 显式拒绝且**零残留**；同名不同 quota → 显式拒绝 |
| S21-11 | **K-100 RSM 确定性** | 三副本设不同 `claim_tombstone_max`，同一条 `prune` 后墓碑数**一致**（上限随命令复制） |
| S21-12 | 计量一致性 | tenant 声明但节点不存在 → 父为 `run` 且 `node.tenant==""`；`usage_report` 不误归集 |
| S21-13 | **K-99 epoch 失效回收** | 全局 `bump_epoch()`（只推进 epoch）后，`valid=True` 的旧租约仍被回收 |
| S21-14 | K-97 生产入口 | `dispatch_and_run` 的 plan 级 tenant/tier 生效（先登记 plan 再准入） |
| S21-15 | 恢复后计量 | 预算事件重放后 `node.tenant` 保留，`usage_report` 不归零 |
| S21-16 | 非法入参 | 小数份额 / 含 `:` 租户名 / 缺失父节点 → 显式拒绝且零残留 |
| S21-17 | Raft tenant 复制 | `RaftBudgetManager.create_node(tenant=)` 三副本标签一致 |

---

## 3. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/orchestration.py` | `_tenant_of`/`_tier_of`/`_resolve_parent_budget`/`_tier_parent`；`_tenant_of_node_id`；`Planner.plan` 透传 tenant/tier；`_RESERVED_TIERS` |
| `harness/harness.py` | `max_lease_ttl_s`（ceil，覆盖 claim 租约）；`lease_ms>0` 校验；`claim_window_s`；`ensure_tenant(..., min_share=)` 先校验后建（含小数/冒号/父缺失拒绝）；`tick()` 传 `now` 触发 `reap_expired`；`prune_budget` 下发 `max_lease_ttl_s` 并回读计数；`dispatch_and_run` 先登记 plan；重放恢复 tenant；`spawn_child` 子节点带 tenant |
| `harness/intent_registry.py` | `reap_expired(now_ms=None) -> int`（失效或 epoch 失效即回收，清 `_active`） |
| `harness/protocols.py` | 声明 `reap_expired`；`prune` 增补 `live_acb_ids`/`max_lease_ttl_s` |
| `harness/budget.py` | 单机 `prune` 接收并忽略 Raft 专用参数（统一签名）；`BUDGET_NODE_CREATED` 事件带 `tenant`；命中旧节点时补齐标签 |
| `harness/storage/raft.py` | 墓碑入墓时刻 + 按年龄淘汰；`tombstone_max` 随命令复制；`claim_window_exceeded` 计数与诊断属性；`create_node` 复制 `tenant` |
| `tests/test_s21_tenant_and_leases.py`（新） | S21-1..S21-17（21 个用例函数） |
| `scripts/check_s21_conformance.py`（新） | 77 项防漂移核对 |

---

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| tenant 解析改变现有准入 | 无 tenant **且无 tier** → 返回 `"run"`；显式非默认 `parent_budget` 优先；S21-2 负对照 |
| 回收误删活跃租约 | **只删 `valid=False`**（失效是终态）；`valid=True` 一律不删；S21-4/5 双向断言 |
| 失效租约无界增长 | 失效（含 **epoch 失效**）即回收，并清 `_active`；`lease_ms<=0` 构造期拒绝 |
| 墓碑窗口用错尺度 | 用 `ceil(max(lease_ms, claim_lease_ms))`；逐墓碑年龄 ≥ 该阈值才可忘记（S21-7） |
| RSM 因本地配置发散 | `tombstone_max`/`retention_s`/`max_lease_ttl_s`/`now` 全部随命令复制；S21-11 三副本一致性断言 |
| Raft 后端缺 tenant 支持 | `create_node` 复制 `tenant`（S21-17）；否则装配期直接 TypeError |
| 恢复后计量归零 | tenant 落事件并在重放时透传（S21-15） |
| 暂时超界被误当 bug | 文档写明「稳态有界」：宁可超界也不削弱 fencing |
| 份额被误解为**速率**限制 | 文档明确：它是**总量预留**（静态份额），不是 token bucket |
| 多租户被误解为**安全**隔离 | 文档明确：配额隔离；安全隔离靠 egress/沙箱（K-98） |
| 计量错位 | `node.tenant` 由实际父节点反推；S21-12 |

---

## 5. 决策

| # | 结论 |
|---|---|
| Q1 | K-97 在 `_try_admit_commit` 单点解析（显式非默认 `parent_budget` 优先，其次 tenant 节点，默认 `"run"`）；`node.tenant` 由**实际父节点**反推 |
| Q2 | K-99 只做**失效租约回收器**（缺口是孤儿回收，不是跨机复制）；失效即回收，不等过期 |
| Q3 | K-100 用**逐墓碑年龄 ≥ `max_lease_ttl_s`** 做强制；`tombstone_max × max_lease_ttl_s` 仅作诊断；上限随命令复制 |
| Q4 | 份额用**预算树子节点**（不是令牌桶），上限/冲突显式拒绝且零残留 |
| Q5 | 租约**状态复制**记为 **K-103**（单写者已够，复制是过度设计） |
| Q6 | 墓碑淘汰按**年龄**而非纯 LRU（评审 P0：纯 LRU 会让旧 worker 复活）；容量不足时**拒绝淘汰** |
| Q7 | `lease_ms<=0` 构造期拒绝（否则 `expires_at=0` 永不过期 → fencing 失效 + 无界增长） |

---

## 6. 实现结果（终态）

### 6.1 交付

| 项 | 状态 |
|---|---|
| K-97 tenant 自动接准入 | ✅ 已实现（plan/task spec 两处来源；无 tenant 时行为不变） |
| K-99 过期租约回收器 | ✅ 已实现（`reap_expired` + `tick` 驱动；状态复制显式**不做** → K-103） |
| K-100 墓碑窗口量化 + 时点安全 | ✅ 已实现（逐墓碑年龄 + 命令参数化上限 + 拒绝淘汰计数） |
| 多租户优先级份额 | ✅ 已实现（reserved 池 + 未知档位回落共享池 + 上限/冲突显式拒绝） |

### 6.2 验证

| 门禁 | 结果 |
|---|---|
| 全量测试 | **652 passed / 4 xfailed**（S21 新增 21 个用例函数） |
| S21 一致性核对 | `scripts/check_s21_conformance.py` **77 / 0** |
| 历史一致性核对 | S14–S20 全部 **0 失败**（45/73/32/56/37/40/34） |
| 覆盖率门禁 | 整体 **84.6%**（多轮 84.58–84.60 波动）≥ 80；三档硬门禁全过 |
| ruff | 新增/改动文件除既有 `S110`/`S112`（刻意的 `try/except/pass|continue`）外无告警 |

### 6.3 评审驱动的修复（两路对抗式评审）

| 级别 | 发现 | 修复 |
|---|---|---|
| P0 | `_ReplicatedBudget.prune` 读副本本地 `claim_tombstone_max` → 三副本淘汰数量不同（实测 `[2,3,3]`）→ RSM 发散 | 上限改为**命令参数** `tombstone_max` 随命令复制；新增 S21-11 三副本一致性断言 |
| P0 | `max_lease_ttl_s` 用 `floor(lease_ms/1000)` 且忽略 `claim_lease_ms`；`lease_ms<=0` 时窗口=1s | 改为 `ceil(max(lease_ms, claim_lease_ms)/1000)`；构造期拒绝 `lease_ms<=0` |
| P1 | 升级遗留墓碑缺 `claim_tombstone_at` → 永远「太年轻」、永不淘汰 | `setdefault(aid, now)` 首次见到即补记 |
| P1 | `expires_at==0` 的失效租约永不回收 → `_leases`/`_active` 无界增长 | 失效即回收（不等过期），并清 `_active` |
| P1 | `ensure_tenant` 先建节点后校验 → 失败留半成品 `tenant:<t>` | 校验前置；配置冲突显式抛错 |
| P1 | 不同 quota 重复 `ensure_tenant` 静默返回 None | 显式 `ValueError`（不静默沿用旧配置） |
| P1 | `node.tenant` 用声明的 tenant → 未隔离用量被错误归集 | 由实际父节点反推；新增 S21-12 |
| P2 | `_tier_parent` 守卫过宽（`base != "run"`）、tier 大小写敏感、死分支 `:<tier>:reserved` | 收紧为 `base.startswith("tenant:")`；`strip().lower()`；删除死分支 |
| P2 | `tick(now_ms=...)` 未把 `now` 传给回收器 | 传同一 `now` |
| P2 | 文档把 `1024×ttl` 当强制阈值、标题写「复制」 | §0.3/§1.3/§2 全部改写；标题与覆盖表收窄 |

**终审追加修复（第二轮）**：

| 级别 | 发现 | 修复 |
|---|---|---|
| P1 | 全局 `Harness.bump_epoch` 只推进 epoch、不置 `valid=False` → epoch 失效租约**永不回收** | `reap_expired` 同时回收「`valid=False`」与「epoch 不匹配」两类；新增 S21-13 |
| P1 | Raft 后端 `create_node` 不接受 `tenant=` → Harness+Raft 装配准入直接 `TypeError` | `_ReplicatedBudget.create_node`/`RaftBudgetManager.create_node` 接收并复制 `tenant`；新增 S21-17 |
| P1 | `node.tenant` 未落 `BUDGET_NODE_CREATED` 事件 → 重放/恢复后计量归零 | 事件 payload 带 `tenant`；重放透传；`create_node` 命中旧节点时补齐标签；新增 S21-15 |
| P1 | `dispatch_and_run` 先准入后登记 plan → plan 级 tenant/tier 静默失效 | 先登记 `h.plans[run_id]` 再 `try_admit`；新增 S21-14 |
| P2 | `ensure_tenant` 父缺失报错误导、小数份额静默截断、租户名含 `:` 破坏解析 | 分别显式拒绝（零残留）；新增 S21-16 |
| P2 | `spawn_child` 子节点不带 tenant → 子预留不计入租户计量 | 用实际父节点反推 tenant |

### 6.4 显式不做（有理由，非遗漏）

| 项 | 理由 |
|---|---|
| 租约状态跨机复制 | 同进程共享字典 + 单写者 + Raft epoch 已足够；复制引入写放大与心跳风暴 → **K-103** |
| 按档位细分预留池（`tenant:<t>:<tier>:reserved`） | 无生产路径产出；单一 `tenant:<t>:reserved` 已满足「预留 vs 借用」语义 |
| 多租户**安全**隔离 | 配额隔离不等于安全隔离；安全隔离由 egress/沙箱（K-98）负责 |
| 运行时动态改 `claim_tombstone_max` | 需要 replicated setter；当前由 `RaftBudgetManager` 读取一次并随命令下发，已保证一致性 |
| 恢复自定义后端的旧 `prune` 签名 | `prune_budget` 删除了 `except TypeError` 回退；协议已更新，第三方后端需实现新签名（`live_acb_ids`/`max_lease_ttl_s`） |

**已知边界（如实记录）**：`BudgetManager.prune` / `RaftBudgetManager.prune` 的
`max_lease_ttl_s` 默认 `0.0` 表示「不启用年龄保护」（仅测试/单机直调）；
生产路径 `Harness.prune_budget` **恒传** `>=1`，不会退化为无保护淘汰。
