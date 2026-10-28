# S19：K-88~K-93 收口（预算账本修剪 + claim fencing 复制 + 重放时间戳 + 配置加固）

| 项目 | 内容 |
|---|---|
| 版本 | **v1.1（实现完成）** |
| 日期 | 2026-10-27 |
| 上游 | [S18 v1.1](s18-budget-rsm-and-linearizable-read.md) §6.4（K-88..K-93） |
| 覆盖 | **K-88** 账本修剪（**精密三栅栏算法**）、**K-89** claim fencing 复制、**K-90** 重放时间戳、**K-91** `check` 语义显式化、**K-92** 连通性预检 + 可配密钥、**K-93** `remove_node` owner 复制 |
| 硬约束 | 604 passed 全绿；不变量 I5c/E1/E2 **剪后仍成立**；`dependencies = []` |

---

## 0. 调研结论（代码实况 + 外部依据）

### 0.1 K-88 根因：**`release`/`settle` 从不设 `expires_at`**

| 事实 | 证据 |
|---|---|
| `archive_node` 会设 `resv.expires_at = now + ttl` | [budget.py:560-570](harness/budget.py) |
| **`release`/`settle` 只改 `state`，不设 `expires_at`** | [budget.py](harness/budget.py) `release`/`settle` 内只有 `resv.state = "RELEASED"/"SETTLED"` |
| `gc(now)` 只删 `state == "ARCHIVED" and expires_at <= now` | [budget.py:573-585](harness/budget.py) |

→ **RELEASED/SETTLED 条目永远留在 `_reservations` 里**，`expires_at = 0` 使它们既不满足 gc 条件、也永不过期。这就是「无上限」的确切成因。

### 0.2 剪枝必须满足的不变量（决定「能不能剪」）

`check_invariants` 的判定式（[budget.py:930-940](harness/budget.py)）：

```
self_reserved == ACTIVE.amount 之和 + 非ACTIVE.consumed 之和 + folded_consumed
consumed      == Σ(全部现存条目.consumed)      + folded_consumed
E2: Σ consumed == Σ(现存条目.consumed) + Σ folded_consumed
```

**推论（关键）**：删掉一条 `RELEASED`/`SETTLED` 条目会让 `Σ条目.consumed` 下降，
但 `node.consumed` 不变 → **E2 立刻被破坏**。
**唯一安全剪法是「先折账再删」**：`node.folded_consumed += resv.consumed`，再删条目。
（这正是 `archive_node` 已经在用的机制 —— `folded_consumed` 就是为「无对应条目的永久账」设计的。）

### 0.3 外部依据：三栅栏剪枝（low-watermark + tombstone retention）

| 依据 | 结论 |
|---|---|
| **Low watermark**（分布式系统通用）：安全删除点 = 所有观察者位置的**最小值**；只有确认「没人再需要」才推进 | 我们精确到 **rid 粒度**（比序号水位更强，因为我们能枚举活引用） |
| **Kafka tombstone**：墓碑必须保留 `delete.retention.ms`（默认 24h）作为**安全栅栏**，否则「从头读的消费者」会漏语义 | 我们用 `_tombstone_ttl_s` 作时间栅栏 |
| **LSM/Raft 快照**：压缩点不能越过「最小活跃读」+ 必须先把被删记录的账折入新基线 | 与 §0.2 的折账一致 |

### 0.4 K-89 的复制设计（单条日志项完成校验 + 变更）

现状：`consume(acb=...)` 在 S18 里被改成 **fail-closed 拒绝**（因为副本没有 `claim_guard`）。
正解不是「复制一个 guard 回调」（不可序列化），而是**把 claim 作为命令参数**：

```
@replicated consume(rid, amount, acb_id, claim_epoch):
    cur = self.claims.get(acb_id, 0)
    if claim_epoch < cur:  last_ok = False; return      # 旧 worker 被 fence
    self.claims[acb_id] = max(cur, claim_epoch)
    last_ok = bm.consume(rid, amount) 成功
```
**单调性**保证「旧 worker 的低 epoch 永久被拒」，且**一次日志写入**完成校验+消费。

---

## 1. 设计

### 1.1 K-88：`prune(now, live_rids)` —— **三栅栏 + 折账**

```python
def prune(self, now: float, live_rids: Iterable[str], *,
          max_age_s: float | None = None) -> int:
    """剪掉「已终结 + 无人引用 + 超过保留期」的预留条目。

    三个栅栏**全部满足**才剪（缺一不可，避免误删活引用或破坏审计）：
      ① 状态栅栏：只剪 RELEASED / SETTLED（ACTIVE 永不剪；ARCHIVED 由 gc 按 TTL 处理）
      ② 引用栅栏：`rid ∈ live_rids` 不剪（活 ACB 仍可能 settle/release）
      ③ 时间栅栏：`expires_at` 未到不剪（Kafka tombstone retention 同构）
    剪前**必须折账**：`node.folded_consumed += resv.consumed`，否则 E2/I5c 被破坏。
    """
```

配套改动：
- `release`/`settle` 加 `now=None` 参数并设 `resv.expires_at = now + ttl`
  → 让所有终态条目都有**明确的保留期**（而不是 0=永不过期）
- `Harness.prune_budget(max_age_s=None)`：从**活 ACB** 精确计算 `live_rids`
  （`a.state not in TASK_TERMINAL` 的 `a.reservation_ids` 并集），再调 `budget.prune`
- Raft 路径：`prune` 也是 `@replicated`（`now` + `live_rids` 作为命令参数 → 确定性）

**为什么不用「按 seq 水位」**：单机模式的 rid 是 `uuid4`（无单调 seq）。
用 **`live_rids` 精确集合**替代水位近似，安全且更强。

### 1.2 K-89：claim fencing 复制（单调 claims 表）

`_ReplicatedBudget` 增加 `claims: dict[str,int]` + 上文的 `consume(rid, amount, acb_id, claim_epoch)`。
`RaftBudgetManager.consume(..., acb=...)` 从 `acb` 取 `id`/`claim_epoch` 传入（不再拒绝）。

### 1.3 K-90：重放不再取 wall clock

`BUDGET_ARCHIVED` 事件 payload 增加 `expires_at`；`_apply_archived` 优先用 payload 值，
缺失才回退 `time.time()`（兼容旧事件）。

### 1.4 K-91：`check` 语义显式化（不改行为，改名 + 补 API）

- 新增 `remaining(node_id) -> int`：`limits - subtree_reserved`（**这才是「剩余额度」**）
- `check`/`check_reservations` 的 docstring 与 Protocol 明确写「**ANY**（任一 ACTIVE 预留余额够付）」

### 1.5 K-92：连通性预检 + 可配密钥

```python
RaftCluster.from_addrs(..., password="...", precheck=True, precheck_timeout=1.0)
```
`precheck`：对每个地址做 `TCP connect`（1s 超时）；任一失败 → `RaftUnavailable` 并**列出失败地址**
（而不是干等 20s 的 `wait_ready`）。密码进入 `raft_conf(password=...)`（去掉硬编码）。

### 1.6 K-93：`remove_node` 的 owner 复制

`@replicated remove_node(node_id, owner)` → 副本内 `bm.remove_node(node_id, owner=owner)`；
`gc` 返回副本真实删除数（S18 已改，本波补测试）。

---

## 2. 测试

| # | 用例 | 判据 |
|---|---|---|
| S19-1 | **剪枝基本** | RELEASED 条目超过保留期 + 无引用 → 被剪；`_reservations` 数量下降 |
| S19-2 | **剪后不变量** | 剪前后 `check_invariants()` 均通过；`node.consumed` 不变、`folded_consumed` 增加 |
| S19-3 | **三栅栏各自生效** | ①ACTIVE 不剪 ②`live_rids` 含它不剪 ③未到 TTL 不剪（三个负对照） |
| S19-4 | **有界性** | 循环 100 次 reserve/release/prune → `_reservations` 规模**不随次数线性增长** |
| S19-5 | K-89 claim 复制 | 高 epoch 先写入后，低 epoch `consume` 被拒；同 epoch 通过；副本一致 |
| S19-6 | K-90 重放时间戳 | `BUDGET_ARCHIVED` payload 带 `expires_at`；重放后与 RSM 的 `expires_at` **一致** |
| S19-7 | K-91 API | `remaining()` = `limits - subtree_reserved`；`check` 仍是 ANY 语义（负对照：remaining=4 时 `check(6)` 仍 True） |
| S19-8 | K-92 预检 | 不可达地址 → `RaftUnavailable` 且**快速失败**（<3s，不是 20s）；可配密码生效 |
| S19-9 | K-93 owner | 错误 owner 的 `remove_node` 返回 False（且三副本一致） |

---

## 3. 迁移清单

| 文件 | 变更 |
|---|---|
| `harness/budget.py` | `release/settle(now=None)` 设 `expires_at`；新增 `prune(now, live_rids, max_age_s=None)`；新增 `remaining()`；`_apply_archived` 用 payload `expires_at` |
| `harness/protocols.py` | `BudgetManager` 补 `prune`/`remaining`；`check`/`check_reservations` 注释改 ANY |
| `harness/harness.py` | `prune_budget(max_age_s=None)`（从活 ACB 算 `live_rids`） |
| `harness/storage/raft.py` | `_ReplicatedBudget` 加 `claims` + `consume(..., acb_id, claim_epoch)` + `prune`；`RaftBudgetManager.consume` 传 claim、新增 `prune`；`from_addrs(password=, precheck=)`；`remove_node(owner)` |
| `tests/test_s19_budget_gc.py`（新） | S19-1..S19-9 |
| `scripts/check_s19_conformance.py`（新） | 防漂移核对 |

---

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| 剪枝误删活引用 → `settle` 静默失效 | 引用栅栏用**精确集合**`live_rids`；负对照用例锁定 |
| 剪枝破坏 I5c/E2 | **先折账再删**；S19-2 断言剪前后不变量都过 |
| `expires_at` 默认值改变 `gc` 行为 | 只有「设了 `expires_at`」的条目才可被 `prune` 剪；`gc` 逻辑不动（只处理 ARCHIVED） |
| Raft 下 `prune` 非确定性（`live_rids` 随时间变） | `live_rids` 与 `now` **都作为命令参数**复制 → 副本执行同一集合 |
| `precheck` 误伤（防火墙丢 SYN） | 默认 1s 超时且**只做提示性校验**；可用 `precheck=False` 关闭 |
| claim 单调表无界增长 | `claims` 随 ACB 数增长；ACB 终态后由 prune 同批清理（记入 K-94 若未做） |

---

## 5. 决策

| # | 结论 |
|---|---|
| Q1 | K-88 用**三栅栏 + 折账**（状态/引用/时间），引用栅栏用精确 `live_rids` 而非序号水位 |
| Q2 | `release`/`settle` 必须设 `expires_at`（否则「永不过期」就是无上限的根因） |
| Q3 | K-89 把 claim 作为**命令参数**（一次日志完成校验+消费），`claims` 单调 |
| Q4 | K-91 **不改** `check` 行为（有既有调用），新增 `remaining()` 并在契约里写明 ANY 语义 |
| Q5 | K-92 预检默认开、1s 超时、可关；密钥可配（去掉硬编码） |

---

## 6. 实现结果（v1.0 → v1.1）

### 6.1 交付

| 项 | 状态 | 实现 |
|---|---|---|
| **K-88** | ✅ | `Reservation.settled_at`（终态时刻）；`release`/`settle`/`archive` 均记录 `settled_at` + `expires_at`；`BudgetManager.prune(now, live_rids, *, max_age_s, retention_s)`：**三栅栏 + 剪前折账**；`Harness.prune_budget()` 从**活 ACB** 精确算 `live_rids` |
| **K-89** | ✅ | 副本 `claims` 表 + `consume(rid, amount, acb_id, claim_epoch)`：**单调**拒绝旧 epoch（一次日志内完成校验+变更） |
| **K-90** | ✅ | `BUDGET_ARCHIVED` payload 带 `expires_at`；`_apply_archived` 优先用 payload 值，缺失才回退 wall clock |
| **K-91** | ✅ | 新增 `remaining(node_id)`；契约里 `check()` 明确标注 **ANY 语义** |
| **K-92** | ✅ | `from_addrs(..., password=..., precheck="auto", precheck_timeout=1.0)`：**全不可达=首次启动放行，部分可达=配置错误快速失败**；密码可配 |
| **K-93** | ✅ | `remove_node(node_id, owner)` 的 owner 校验走共识；`gc` 回传真实删除数 |

### 6.2 门禁

| 门禁 | 结果 |
|---|---|
| pytest 全量 | **620 passed + 4 xfailed**（连跑 3 轮，~39s/轮；基线 604 → 620，+16） |
| 覆盖率 | 门禁通过 |
| 核对脚本 | S14 45 · S15 73 · S16 32 · S17 56 · S18 37 · **S19 40**（全绿） |

### 6.3 修剪算法（最终形态）

```
prune(now, live_rids, *, max_age_s=None, retention_s=None):
    retention = retention_s if retention_s is not None
                else (ttl if max_age_s is None else min(ttl, max_age_s))
    for rid, (node, resv) in _reservations:
        if resv.state not in (RELEASED, SETTLED):   continue   # ① 状态栅栏
        if rid in live_rids:                        continue   # ② 引用栅栏（精确集合）
        if settled_at and now - settled_at < retention: continue  # ③ 时间栅栏
        node.folded_consumed += resv.consumed       # ★ 剪前折账（否则 E2/I5c 破坏）
        node.reservations.pop(rid); _reservations.pop(rid)
```

**为什么这是「精密」而不是「拍脑袋」**：
- ① 只剪**已终结**的（ACTIVE 还在占额度，ARCHIVED 由 `gc` 按 TTL 处理）
- ② 用**精确引用集合**而非序号水位（单机 rid 是 uuid，无单调 seq；精确集合更强）
- ③ 用 `settled_at` 而非 `expires_at`（前者是「进入终态的时刻」，后者含 TTL 偏移，
  用它算 age 会把保留期算成两倍）；`max_age_s` **只能收紧**不能放宽
- ★ 折账是**不变量的硬要求**：`consumed == Σ条目.consumed + folded_consumed`

### 6.4 实现期发现（真问题）

| # | 发现 | 处置 |
|---|---|---|
| **F-1** | `prune` 最初直接读副本本地的 `_tombstone_ttl_s` → **配置漂移会让同一命令在各副本得出不同结果**（状态机发散） | 生效保留期 `retention_s` 作为**命令参数**由调用方算好复制（同 K-87 的 `now`） |
| **F-2** | `max_age_s` 初版用 `cutoff = now - max_age` 与 `expires_at` 比较 → 语义混乱（保留期被算成两倍） | 改用 `settled_at`，边界明确为 `now - settled_at >= retention` |
| **F-3** | `precheck` 初版「全须可达」→ **首次启动必失败**（此时无人监听） | 改 `"auto"`：全不可达=首次启动放行；**部分可达才报错**（那才是真配置错误） |

### 6.5 新增/收敛边界

| # | 边界 |
|---|---|
| K-88 | **已收敛**：账本有界（S19-4 断言 100 轮后残留 ≤1 条） |
| K-89 | **已收敛**：claim fencing 复制（单调）；`claims` 表随 ACB 增长，终态后未单独清理（并入 K-94） |
| **K-94** | `claims` 表**未随 prune 清理**（ACB 终态后条目仍在）；量级为「ACB 总数」，需要时按 `prune` 同批清理 |
| K-95 | `prune` 需**调用方**提供 `live_rids`（`Harness.prune_budget` 已提供）；直接用手写 `BudgetManager` 的调用方必须自己传，否则可能剪掉活引用 |
| K-96 | `precheck` 只是**提示性**可达性检查（SYN 可达 ≠ pysyncobj 可通信：密码/版本不匹配仍会在 `wait_ready` 暴露） |
