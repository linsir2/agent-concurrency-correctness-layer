# S20：K-94~K-96 + 阶段三 MVP（多租户 / 计量 / 统一 Admin 面）

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（实现完成）** |
| 日期 | 2026-10-27 |
| 上游 | [S19 v1.1](s19-budget-gc-and-hardening.md) §6.5（K-94..K-96）、[positioning-and-roadmap](positioning-and-roadmap.md) §6.5 阶段三 MVP |
| 覆盖 | **K-94** `claims` 表清理、**K-95** `prune` 的 `live_rids` 契约（fail-safe）、**K-96** 就绪后复核连通性；**阶段三**：多租户配额（复用预算树）、计量（事件派生，幂等）、统一 Admin 命令面 |
| 不覆盖 | **分布式租约**（见 §0.4 的追根溯源与推迟理由）、跨机部署实测 |
| 硬约束 | 620 passed 全绿；`dependencies = []`；不变量不受影响 |

---

## 0. 追根溯源（三项边界 + 阶段三）

### 0.1 K-94：`claims` 为什么会无界

| 事实 | 证据 |
|---|---|
| `claims: dict[acb_id, epoch]` 只在 `consume` 时**写入或抬高** | [raft.py](harness/storage/raft.py) `_ReplicatedBudget.consume` |
| **没有任何删除路径** | 全仓 `claims` 仅 3 处：初始化 / 读 / 写 |
| 量级 = **ACB 总数**（每个派生子任务一条） | 与 `acb.id` 一一对应 |

**根因**：`claims` 是「单调 fencing 墓碑」，语义上**不能提前删**（删了旧 worker 就能复活）。
→ 正确剪法是**与 ACB 生命周期对齐**：ACB 进入终态并被 `purge` 后，其 id 永不复用（id 带 attempt 计数），才可以删。
→ **不能只看时间**（否则旧 worker 在窗口后复活）。

### 0.2 K-95：`live_rids` 的默认值是**危险默认**

现状签名 `prune(now, live_rids=(), ...)`：**不传 = 空集 = 全部可剪**。
调用方若忘记传，会剪掉活引用 → 后续 `settle`/`release` 静默变 no-op（账目永久偏差）。

→ **fail-safe**：`live_rids=None`（默认）= **拒绝剪枝**并返回 0 + 记录原因；
显式传集合（哪怕是空集）才执行。这是「默认安全」而非「默认放行」。

### 0.3 K-96：`precheck` 只是 SYN 可达

`precheck` 探测的是 TCP 能否建连，**不等于** pysyncobj 能通信（密码/版本/集群 ID 不一致仍会失败）。
→ 补**就绪后复核**：`wait_ready` 成功后检查 `isNodeConnected(peer)`，不一致立即报错并**列出断连 peer**。

### 0.4 阶段三：逐项追根溯源

| 阶段三交付物 | 现状 | 结论 |
|---|---|---|
| **跨主机 fencing** | ✅ `RaftEpochSource`（epoch = term） | **已具备** |
| **分布式租约** | ⚠️ `IntentRegistry` 租约是**进程内 dict** + epoch 校验；Raft leader 是唯一写者 → 正确性已够 | **推迟（K-99）**：只有当出现「多 leader / worker 直连交接」时才需要复制租约表；现在做是**过度设计**（且会引入心跳风暴） |
| **多租户** | ❌ 无 tenant 概念 | **本波做**：复用**已测试的**预算树链式限额（`run → tenant:<t> → node`），而不是新写令牌桶 |
| **Admin 控制面** | 🟡 pause/resume/kill/purge 已有，但 `submit_command` 只路由 3 个 | **本波做**：补齐 `KILL_RUN`/`PURGE_RUN`/`DLQ_REDRIVE`/`PRUNE` 路由 |
| **计量** | ❌ 无 | **本波做**：**从 EventLog 派生**（单一真相源、天然幂等），不做原子计数器 |

**为什么计量不从原子计数器做**：AWS Builders' Library 明确指出**原子计数器不幂等**
（重试会重复计数）。我们的 EventLog 是唯一提交点（I1），从它重算**天然幂等**。

**为什么多租户复用预算树而不是令牌桶**：AWS「Fairness in multi-tenant systems」的令牌桶
解决的是**速率**限制；我们要的是**总量配额 + 优先级保留**。预算树的
`subtree_reserved ≤ limits` 已经是被 I5a/b/c 断言过的机制，新增 tenant 层即可
（Microsoft 的「按 tier 预留容量」正是树的静态份额）。

---

## 1. 设计

### 1.1 K-94：`claims` **不能裸删** —— 需要「有界墓碑」

**自评审发现（必须记录）**：直接 `claims.pop(acb_id)` 会**复活旧 worker** ——
`consume` 的判据是 `claim_epoch < claims.get(acb_id, 0)`，删掉后 `get` 回 0，
于是任何 `epoch >= 0` 的旧 worker 都能通过 → **fencing 被抹掉**。

**正确设计（有界 + 不丢 fencing）**：
```python
claims: dict[acb_id, epoch]          # 只保留**活 ACB**
claim_tombstones: dict[acb_id, epoch]  # 已剪 ACB 的墓碑，**有界 LRU**
claim_tombstone_max: int = 1024      # 上限（可配）
```
- `consume` 判据：`epoch < max(claims.get(aid,0), tombstones.get(aid,0))` → 拒绝
- `prune`：活集合外的 acb → **先写墓碑**（同时把最旧的挤出，LRU）再删 claims
- **有界性**：`|claims| ≤ 活 ACB 数`，`|tombstones| ≤ claim_tombstone_max`
- **残余风险**（如实记录为 K-100）：被挤出 LRU 的 acb 若还有**极旧** worker 存活，
  其低 epoch 会被接受。缓解：ACB id 含 attempt 计数且不复用；实际窗口远大于
  任何租约时长（`lease_ms` 默认 60s）。

`Harness.prune_budget()` 同时传 `live_acb_ids`（非终态 ACB 的 id 集合）。

### 1.2 K-95：fail-safe 默认

```python
def prune(self, now, live_rids=None, *, max_age_s=None, retention_s=None) -> int:
    if live_rids is None:
        # 不知道活引用 → 一律不剪（默认安全）
        self._prune_refused += 1
        return 0
```

### 1.3 K-96：就绪后复核连通性

```python
def verify_connected(self, timeout=5.0) -> None:
    """`wait_ready` 之后再确认每个 peer 真的连上（SYN 可达 ≠ 可通信）。"""
```
**为什么不在 `wait_ready` 里自动调**：`wait_ready` 只要「全部节点本地就绪」即返回，
而 peer 间连通可能仍在**收敛中**；自动校验会把「瞬时不连通」变成**启动失败**（新 flaky）。
因此 `verify_connected()` 作为**显式入口**（部署/CI 用），失败时列出断连 peer。

### 1.4 多租户（复用预算树）

- `create_node(..., tenant="")`：`BudgetNode.tenant` 字段（已可加）
- `Harness.ensure_tenant(tenant, quota)`：在 `run` 下建 `tenant:<t>` 节点（幂等）
- **链式限额自动生效**：任何挂在 `tenant:<t>` 下的节点，其 `subtree_reserved`
  都受 `quota` 约束（已被 I5a/b/c 断言的机制，无需新写限流器）
- **本波范围**：账本层 + `Harness.ensure_tenant()` 便捷入口；
  **自动把 `plan["tenant"]` 接到 `parent_budget` 的准入接线** 记为 **K-97**
  （需动 `Dispatcher.try_admit` 的 `parent_budget` 管线，且必须保持
  「无 tenant 时行为完全不变」，单独一波更安全）
- 隔离语义（诚实）：**配额隔离**，不是安全隔离（同进程共享内存；安全隔离靠 egress/沙箱）

### 1.5 计量（事件派生，幂等）

```python
def usage_report(self, *, tenant: str | None = None) -> dict:
    """从 EventLog 重算：{tenant: {"actions": n, "consumed": n, "runs": k}}。

    幂等：同一日志重算多次结果恒等（不累加内存计数器）。
    """
```

### 1.6 统一 Admin 命令面

`submit_command` 补齐：`KILL_RUN` / `PURGE_RUN` / `DLQ_REDRIVE` / `PRUNE`（幂等语义：目标态已达成 → `OK`）。

---

## 2. 测试

| # | 用例 | 判据 |
|---|---|---|
| S20-1 | **K-95 fail-safe** | `prune(now)` 不传 `live_rids` → 返回 0 **且不删**；传空集合才删 |
| S20-2 | **K-94 claims 清理** | ACB 终态后 `prune_budget()` → 该 acb 的 claim 被删；**仍在活的 acb 的 claim 保留** |
| S20-3 | K-94 负对照 | **删 claims 后旧 worker 仍被墓碑拒绝**（否则 fencing 被抹）；墓碑有界（≤ `claim_tombstone_max`） |
| S20-4 | **K-96 连通性复核** | 3 节点就绪后 `verify_connected()` 通过；模拟 peer 不可达 → 抛错并列出 peer |
| S20-5 | **多租户配额** | 两 tenant 各配额 10；A 用满 10 后 B 仍可预留（互不侵占）；`ensure_tenant` 幂等 |
| S20-6 | 多租户隔离负对照 | A 超配额 → 拒绝；B 的 `subtree_reserved` 不受影响 |
| S20-7 | **计量幂等** | 同日志调 `usage_report()` 两次结果相同；与 `Σ事件` 一致 |
| S20-8 | 计量按 tenant 过滤 | `usage_report(tenant="t1")` 只统计 t1 |
| S20-9 | **统一 Admin 面** | 4 条命令都真路由且幂等（重复调用 `OK`） |

---

## 3. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/budget.py` | `prune(live_rids=None)` fail-safe；`BudgetNode.tenant`；`remaining` 已有 |
| `harness/storage/raft.py` | 副本 `prune(..., live_acb_ids)` + 清理 claims；`verify_connected()` |
| `harness/harness.py` | `prune_budget` 传 `live_acb_ids`；`ensure_tenant`；`usage_report`；`submit_command` 补路由 |
| `harness/orchestration.py` | `_try_admit_commit` 支持 `plan["tenant"]` → 父节点指向 `tenant:<t>` |
| `tests/test_s20_tenant_metering.py`（新） | S20-1..S20-9 |
| `scripts/check_s20_conformance.py`（新） | 防漂移核对 |

---

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| 删 claim 导致旧 worker 复活 | **只删「不在活 ACB 集合」的**（不是按时间）；S20-3 负对照锁定 |
| `prune` fail-safe 让调用方「静默不生效」 | 返回 0 且 `_prune_refused` 计数 + 文档写明；`Harness` 永远传精确集合 |
| 多租户被误解为**安全**隔离 | 文档明确：只做**配额**隔离；安全隔离靠 egress/沙箱（S11） |
| `usage_report` O(N) 扫全量事件 | 低频调用；与 `metrics()` 同源；文档标注 |
| tenant 节点层级改变现有 `parent_budget` 默认 | 无 `plan["tenant"]` 时**完全不变**（向后兼容） |

---

## 5. 决策

| # | 结论 |
|---|---|
| Q1 | K-94 按 **ACB 生命周期**删 claims（不是按时间） |
| Q2 | K-95 改 **fail-safe 默认**（`live_rids=None` → 不剪） |
| Q3 | K-96 用 **`wait_ready` 后 `verify_connected`** 复核 |
| Q4 | 多租户**复用预算树**（不新写令牌桶），且明确「配额隔离 ≠ 安全隔离」 |
| Q5 | 计量**从事件派生**（幂等），不用原子计数器 |
| Q6 | **分布式租约推迟**（K-99）：单写者 + Raft term 已够，现在做是过度设计 |

---

## 6. 实现结果（v1.0 → v1.1）

### 6.1 交付

| 项 | 状态 | 实现 |
|---|---|---|
| **K-94** | ✅ | 自评审发现「裸删 claims 会**复活旧 worker**」→ 改为**有界墓碑**：`claims`（活 ACB）+ `claim_tombstones`（LRU，≤`claim_tombstone_max`）；`consume` 判据取 `max(claims, tombstones)`；`prune` 对活集合外的 acb **先写墓碑再删** |
| **K-95** | ✅ | `prune(live_rids=None)` **fail-safe**：缺 `live_rids` → 返回 0 **且不删** + `_prune_refused` 计数（原默认 `()` = 全部可剪 = 危险默认） |
| **K-96** | ✅ | `verify_connected()`：`wait_ready` 后用 `isNodeConnected(otherNodes)` 逐 peer 复核；失败列出断连 peer。**不自动接入** `wait_ready`（peer 收敛可能瞬时未完成，自动会引入新 flaky） |
| **多租户** | ✅（账本层） | `BudgetNode.tenant` + `create_node(tenant=)` + `Harness.ensure_tenant(tenant, quota)`（幂等）；配额靠**链式限额**自动生效（I5a/b/c 已断言）。**准入接线（`plan["tenant"]` → `parent_budget`）记 K-97** |
| **计量** | ✅ | `Harness.usage_report(tenant=None)`：从 EventLog 重算 → **幂等**；`by_tenant` 按节点 `tenant` 标签归集预留事件 |
| **统一 Admin** | ✅ | `submit_command` 补齐 `KILL_RUN` / `PURGE_RUN` / `DLQ_REDRIVE` / `PRUNE`（幂等语义：目标态已达成 → `OK`） |

### 6.2 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **631 passed + 4 xfailed**（连跑 3 轮，~43s/轮；基线 620 → 631，+11） |
| 覆盖率 | 门禁通过 |
| 核对脚本 | S14 45 · S15 73 · S16 32 · S17 56 · S18 37 · S19 40 · **S20 35**（全绿） |

### 6.3 实现期发现（真问题）

| # | 发现 | 处置 |
|---|---|---|
| **F-1** | 自评审发现「裸删 claims 会**复活旧 worker**」（`claims.get` 回 0 → 任何 epoch 通过） | 改为**有界墓碑**（先写墓碑再删）；S20-3 锁定「剪后旧 worker 仍被拒」 |
| **F-2** | `verify_connected` 初版把**地址字符串**传给 `isNodeConnected` → 抛错 | pysyncobj 收的是 **Node 对象**（peer 列表在 `otherNodes`）；修正 + 注释 |
| **F-3** | `wait_ready` 只表示「本地就绪」，peer 收敛可能未完成 → 自动校验会**把瞬时态变启动失败** | `verify_connected` 作为**显式入口**，不自动接入 |

### 6.4 新增/收敛边界

| # | 边界 |
|---|---|
| K-94 | **已收敛**：`claims ≤ 活 ACB 数`，`tombstones ≤ 1024`（S20-3 断言） |
| K-95 | **已收敛**：fail-safe 默认（S20-1） |
| K-96 | **已收敛**：显式复核（S20-4） |
| **K-97** | `plan["tenant"]` → 自动接 `parent_budget` 的**准入接线**未做（需动 Dispatcher 管线，单独一波） |
| **K-98** | 多租户只有**配额**隔离；安全隔离靠 egress/沙箱（S11），文档已明确 |
| **K-99** | **分布式租约未做**：`IntentRegistry` 租约是进程内 dict；单写者 + Raft term 已够；多 leader 场景才需要（过度设计） |
| **K-100** | 被挤出 LRU 墓碑的 acb 若仍有**极旧** worker 存活，其低 epoch 会被接受（ACB id 含 attempt 不复用；窗口远大于租约时长） |
| **K-101** | `prune` 需调用方提供精确 `live_rids`；`Harness.prune_budget` 已提供；裸用 `BudgetManager` 传错会剪活引用（K-95 的 fail-safe 兜底） |
