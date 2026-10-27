# S5 + 边界收口详细方案：effect/compensation 落盘 + probe + recover + K-6..K-14

| 项目 | 内容 |
|---|---|
| 版本 | **v1.2（实现完成 + 实现后审查修订）** |
| 日期 | 2026-10-26 |
| 上游 | [第一阶段方案 v1.1](phase1-implementation-plan.md) §4.8/§5/§7、[设计规范 v1.12](multi-agent-harness-design-final.md) §5.4/§5.8、[S4 方案 v1.2](s4-budget-and-admission.md) §13.4 |
| 覆盖测试 | T11、T12、T13、T25、T26、T27、T28、T29 |
| 交付判据 | **91 passed**（连跑 10 轮稳定）；`pytest -m ms3` → 8 |
| 修订 | v1.1 → v1.2：按实现后对抗评审修 4 个 P0 + 5 个高（§12） |

---

## 0. 现状

| 能力 | 文档要求 | 代码现状 |
|---|---|---|
| `EffectJournal` | 落盘 + probe | **内存 `dict`**；`begin` 不发事件；无 probe |
| `CompensationJournal` | 落盘 + `comp_id` + `COMPENSATING` | **内存 `list`**，仅 `{resource, state}` |
| `recover` | `resume_policy` | 未实现；不重建 journal |
| `checkpoint` | 含 `workspace_commit`/`event_cursor` | 均未存 |

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | `EffectJournal` 落盘 + probe 三态 + **INTENT 也 probe** | T11 |
| G2 | `CompensationJournal` 落盘 + `comp_id` + 批量原子 + 续做 | T12、T26 |
| G3 | `recover` 带 `resume_policy` + journal 重建 | T12、T13 |
| G4 | K-6/K-7：逐笔账本 + 路径重建 | T25 |
| G5 | K-13：终态摘除 + tombstone TTL | 回归 |
| G6 | K-14：生产不变量（O(1)）+ 恢复后全树必查 | T27 |
| G7 | K-8/K-11：`{tool:path}` + network/data_class 强制点 | T28 |
| G8 | K-10：**明确降级为「防外部篡改/重放」**（见 §5.9） | T28 |

**非目标**：跨进程预算对账、egress gateway 下沉、**同进程内的自证防护**（见 §5.9）。

---

## 2. 调研结论

| 结论 | 影响 |
|---|---|
| outbox 核心 = **本地原子写**（一次 `fdatasync`），投影可重放 | journal 事件进**同一个** EventLog（不新建 WAL） |
| TCC 空回滚：Cancel 必先查/写 fence log | `COMPENSATING` 先落盘、后释放租约 |
| 补偿是**正向命令**（Saga 补偿） | `comp_id` 幂等 + 按 journal 状态分支 |
| **超时 ≠ 未发生**；probe 必须三态 | `probe → True/False/None`，`None` → 升级人工 |
| tombstone 必须保留到迟到窗口结束 | 回收用 TTL（时间+数量双阈值） |

---

## 3. 数据结构（v1.1：修正数据源冲突）

### 3.1 单一数据源（修 P0-2）

> **v1.0 的矛盾**：§3.3 定义了「独立 WAL 后端 `JournalStore`」，而 §4.3 却用 `event_log` 重建 —— 若 journal 写独立 WAL，`event_log` 里没有事件 → 重建为空 → 全量重复副作用。

**v1.1 定死**：**journal 事件直接写 `EventLog`**（复用现有 WAL，提交点已保证持久）。`JournalStore` 改为**薄封装**，只提供 `append/read_all`，内部调 `event_log`。

### 3.2 事件（subject 统一，修 P0-4）

> **v1.0 的漏洞**：现有 effect 事件 `subject = idem_key`，而 rebuild 按 `acb.id` 过滤 → 一条都匹配不到。

```python
# 统一：subject = acb.id；idem_key / comp_id 放 payload
EFFECT_INTENT      # {idem_key, tool, args, effect_id}
EFFECT_CONFIRMED   # {idem_key, effect_id, tool, result}
EFFECT_UNKNOWN     # {idem_key, tool, reason}
EFFECT_PROBED      # {idem_key, probe}          # probe ∈ True|False|None
COMPENSATING_BATCH # {acb_id, comp_ids: [...]}  # ← 批次标记，与全部条目同一帧
COMPENSATION_ENTRY # {comp_id, resource, version_before, applied_hash, target_hash}
COMPENSATION_STATE # {comp_id, state}           # COMPENSATING|COMPENSATED|FAILED|PIVOT
```

**迁移**：现有 `EFFECT_CONFIRMED`/`EFFECT_UNKNOWN` 的 `subject` 从 `idem_key` 改为 `acb_id`，`idem_key` 移入 payload。

### 3.3 `EffectEntry`（三态必处理）

```python
@dataclass
class EffectEntry:
    idem_key: str
    effect_id: str
    tool: str
    args: dict
    state: str = "INTENT"        # INTENT | CONFIRMED | UNKNOWN
    probe: str = ""              # "" | "True" | "False" | "None"
    updated_at: float = 0.0
```

### 3.4 `CompEntry`

```python
@dataclass
class CompEntry:
    comp_id: str     # sha256(run_id|acb_id|logical_subgoal_id|resource|version_before|applied_hash|target_hash)[:16]
    acb_id: str
    resource: str
    version_before: int
    applied_hash: str
    target_hash: str
    state: str = "PENDING"       # PENDING|COMPENSATING|COMPENSATED|FAILED|PIVOT
    deleted: bool = False        # ← DELETE 哨兵补偿必须用版本号，不能用空 hash
```

> `comp_id` 增 `logical_subgoal_id` 与 `target_hash`（修 P2：不含目标的幂等键不充分）。

### 3.5 `EffectJournal` / `CompensationJournal`

```python
class EffectJournal:
    def begin(self, acb, action) -> str
    def confirm(self, idem_key, result) -> bool
    def mark_unknown(self, idem_key, reason) -> bool
    def pending(self, acb_id) -> list[EffectEntry]      # INTENT + UNKNOWN（不是只 UNKNOWN）
    def probe_one(self, idem_key) -> str                # 异常/超时 → "None"，只写一次
    def rebuild_from(self, events: list[Event]) -> int   # 传快照，不自己读

class CompensationJournal:
    def plan(self, acb, applied) -> list[CompEntry]     # 一批一帧
    def mark(self, comp_id, state, **kw)
    def resume(self, acb) -> ResumePlan                  # 先信 journal 状态
    def rebuild_from(self, events: list[Event]) -> int
```

---

## 4. 算法

### 4.1 tool 执行（v1.1：INTENT 与 UNKNOWN 同路径）

```python
if action.kind == "tool":
    if not self._capability_ok(...): return DENIED_CAPABILITY
    if not self._gateway_check(acb, action): return DENIED_CAPABILITY
    idem = self._idem_key(acb, action, i)
    st = self.effects.state(idem)
    if st == "CONFIRMED":
        return Result(Status.OK, {"effect_id": ...})          # 只有 CONFIRMED 可短路
    if st in ("INTENT", "UNKNOWN"):                            # ← v1.1：INTENT 也 probe
        probe = self.effects.probe_one(idem)
        if probe == "True":
            self.effects.confirm(idem, {"recovered": True}); return OK
        if probe is None:
            return Result(Status.NEEDS_RECONCILIATION, {"idem_key": idem})
        # probe == "False" → 落到下面重执行（同一 idem）
    if not self._consume_budget(acb, action.cost): return ...
    self.effects.begin(acb, action)                            # 写 INTENT（幂等）
    result = self.toolgw.invoke(action.tool, action.args)
    self.effects.confirm(idem, result)
    return Result(Status.OK, {"effect_id": ...})
```

### 4.2 补偿（v1.1：批量原子 + 续做按状态）

```python
def compensate(self, acb, applied: list[Write]) -> None:
    entries = self.comp_journal.plan(acb, applied)      # ← 一次 append_batch：条目 + 批次标记
    self.intents.revoke_all(acb.id)                      # ← 后释放租约
    for e in reversed(entries):
        self._run_one_compensation(acb, e)

def _run_one_compensation(self, acb, e: CompEntry) -> None:
    if e.state in ("COMPENSATED",):
        return                                            # 已完成的幂等跳过
    exists = self.ws.exists(e.resource)
    if e.deleted:
        # 删除类：必须用版本号判定（空 hash 不可区分「不存在」与「删除后」）
        if not exists and self.ws.version(e.resource) == e.version_before + 1:
            self.comp_journal.mark(e.comp_id, "COMPENSATED"); return
        if exists and self.ws.content_hash(e.resource) != e.applied_hash:
            self.comp_journal.mark(e.comp_id, "PIVOT"); return
    else:
        if self.ws.content_hash(e.resource) == e.target_hash:
            self.comp_journal.mark(e.comp_id, "COMPENSATED"); return
        if self.ws.content_hash(e.resource) != e.applied_hash:
            self.comp_journal.mark(e.comp_id, "PIVOT"); return
    ...  # 执行反写
```

> **两条修 P0 的关键**：① `plan()` 用**一次 `append_batch`**（同帧、一次 fdatasync）写全部条目 + 批次标记 → 「写了一半」不可能发生；② `resume` **先按 `e.state` 分支**（`COMPENSATED→done`、`FAILED→升级`、`PIVOT→人工`），仅 `COMPENSATING` 才看 hash/版本 → 不会被后续合法写误判成 pivot，也不会把已补偿的删除误判成 retry。

### 4.3 `recover`（v1.1：终态策略 + 单一数据源）

```python
def recover(self, ref, *, resume_policy: str = "RESUME") -> ACB:
    data = self.checkpoints.load(ref)
    events = self.event_log.snapshot()                     # ← 锁内快照（修 P1 撕裂视图）
    self.effects.rebuild_from([e for e in events if e.producer == data["acb_id"]])
    self.comp_journal.rebuild_from([e for e in events if e.producer == data["acb_id"]])
    acb = self._restore_acb(data, resume_policy)           # 失败改 needs_readmit，不抛
    # ① 先重放事件（把外部状态对齐），再 probe
    self._replay_events(from_seq=data["event_cursor"], dedup=True)
    for entry in self.effects.pending(acb.id):             # 含 INTENT
        probe = self.effects.probe_one(entry.idem_key)
        if probe == "True":  self.effects.confirm(entry.idem_key, {"recovered": True})
        elif probe is None:  acb.state = TaskState.QUARANTINED
    # ② COMPENSATE 是**终态策略**：补偿前丢弃 staged，补偿后不恢复执行
    if resume_policy == "COMPENSATE":
        acb.staged = {}
        plan = self.comp_journal.resume(acb)
        for e in plan["retry"]: self._run_one_compensation(acb, e)
        if plan["pivot"]: acb.state = TaskState.QUARANTINED
    # ③ 恢复后全树必查（K-14）
    try:
        self.budget.check_invariants()
    except InvariantError:
        acb.state = TaskState.QUARANTINED
        self.slots.release(acb.id)
    return acb
```

**状态机**：`NEEDS_RECONCILIATION` → `run()` 显式分支置 `QUARANTINED` + `failure="NEEDS_RECONCILIATION"`（不走通用 FAILED 分支）。

---

## 5. 边界修复

### 5.1 K-6：会计**只以逐笔账本为准**（修 P1 双真相源）

```python
# checkpoint.save 增
"reservations": [{"rid": r, "node_id": n, "amount": a, "consumed": c, "state": s} for ...]
# _restore_budget
for item in data["reservations"]:
    rid, amt, con, st = item[...]
    resv = Reservation(rid, item["node_id"], amt, con, st)
    node.reservations[rid] = resv
    self._reservations[rid] = (node, resv)
node.self_reserved = sum(r.amount for r in node.reservations.values())
node.consumed      = sum(r.consumed for r in node.reservations.values())
node.subtree_reserved = node.self_reserved + sum(c.subtree_reserved for c in node.children)
# 聚合快照只作交叉校验，不一致 → needs_readmit（不覆盖）
if node.self_reserved != data.get("budget_self"): acb.needs_readmit = True
```

### 5.2 K-7：路径重建（修 P1 环 + 部分残留）

```python
# checkpoint.save 增 "budget_path": [root ... parent]
def _restore_path(self, path: list[str]) -> list[BudgetNode]:
    if len(path) != len(set(path)): raise ValueError("path has cycle")
    built = []
    for nid in path:                      # 自顶向下
        node = self.budget.create_node(nid, limits=..., parent_id=...)
        if node is None:
            for b in built: self.budget.remove_node(b.node_id)
            raise RuntimeError("path restore failed")
        built.append(node)
    return built
```

### 5.3 K-13：归档（修 P1 引用悬空）

```python
def archive_node(self, node_id) -> None:
    node = self._nodes.get(node_id)
    if node is None: return
    for r in node.reservations.values():
        if r.state == "ACTIVE": return                     # 有活跃预留不归档
    # ① consumed 折入父级（永久账不丢）
    if node.parent:
        node.parent.consumed += node.consumed
        node.parent.children.remove(node)
    # ② 子节点与全局索引只留 tombstone（带 expires_at），不立即删
    for rid in list(node.reservations):
        node.reservations[rid].state = "ARCHIVED"
    del self._nodes[node_id]

def gc(self, now: float) -> int:
    """清除已过期的 tombstone；retention = max(TTL, 迟到窗口)。"""
```

`acb.budget_node` 置 `None`；`check_invariants` 对 `ARCHIVED` 节点只校验父级聚合。

### 5.4 K-14：分层不变量（修 P1/P2）

```python
class InvariantError(RuntimeError): ...

def _check_local(self, node) -> None:        # O(1)，热路径默认开（锁外调用）
    if not (0 <= node.consumed <= node.self_reserved <= node.subtree_reserved <= node.limits):
        raise InvariantError(f"local invariant broken at {node.node_id}")

def check_invariants(self) -> None:          # O(N)，只在 recover 后与 debug 调用
    ...
    raise InvariantError(...)                # 不用 assert（-O 会剥离）
```

> **修 P1**：现有 `check_invariants` 全用 `assert` → 在 `python -O` 下静默通过。改为 `raise InvariantError`。

### 5.5 K-8：入口规范化

```python
def _normalize_required(t) -> dict[str, str]:
    if t is None: return {}
    if isinstance(t, (set, frozenset, list)): return {x: "" for x in t}
    return dict(t)                                # {tool: path}
```

### 5.6 K-11：gateway 强制点

```python
def _gateway_check(self, acb, action) -> bool:
    token = self._effective_token(acb)
    if token is None: return False                # deny-by-default
    if getattr(action, "network", None) and not token.allows_network(action.network): return False
    if getattr(action, "data_class", None) and not token.allows_data_class(action.data_class): return False
    return True
```

### 5.7 K-9：显式重入

`Harness.requeue_readmit(acb)`；`needs_readmit` 的 ACB 在 `_drain_scheduler` 跳过。

### 5.8 K-12：**不做**

at-least-once 固有边界，由 §4.1 的 `INTENT`+probe 对账覆盖。

### 5.9 K-10：**明确降级**（修 P1 安全目标落空）

**诚实结论**：HMAC 只能防「**没有 secret 的第三方**」篡改/重放。威胁模型是 agent 与 harness **同进程** —— agent 可读 `os.environ` 或直接调 `Signer(secret).mint(...)`，验签必然通过。**同进程内无法靠算法解决自证**。

**v1.1 定死**：
1. 目标降级为「**防外部篡改与重放**」—— 文档删除「解决自证」表述。
2. `check_capability(principal, token, tool, path, *, network=None, data_class=None)` —— **token 显式传入**，删除按 `agent_id` 的隐式查找（修 P1：不传 token 则验签无从谈起）。
3. `verify` 维护**已见 `jti` 集合**（带 TTL 至 `exp`），拒绝重放（修 P2）。
4. secret 从环境变量注入，缺失抛 `MissingSecret`（fail-closed）。
5. **真隔离归阶段二**（独立信任域/enclave），本阶段不承诺。

---

## 6. 迁移清单（v1.1 完整）

| 文件 | 变更 |
|---|---|
| `harness/checkpoint.py` | `EffectJournal`：`begin` 发事件、`pending()` 含 INTENT、`probe_one`（异常→None、只写一次）、`rebuild_from(events)`；新增 `CompensationJournal`、`JournalStore`（薄封装 EventLog）；`save` 增 `reservations`/`budget_path`/`event_cursor`/`failure`/`spec`/`deadline`/`no_progress_steps` |
| `harness/compensation.py`（新增） | `CompEntry`、`CompensationJournal` |
| `harness/auth.py`（新增） | `Signer`/`mint`/`verify`（`jti` 去重）、`MissingSecret` |
| `harness/runtime.py` | tool 分支 §4.1；`_gateway_check`；`NEEDS_RECONCILIATION → QUARANTINED` |
| `harness/harness.py` | `compensate` §4.2；`recover` §4.3；`archive_node`/`gc`/`requeue_readmit`；`_restore_budget` §5.1/5.2 |
| `harness/budget.py` | `archive_node`、`gc`、`_check_local`、`InvariantError`、`check_invariants` 改 raise |
| `harness/policy.py` | `mint`（保留 `issue` 兼容）；`check_capability` 五参 + 四维 |
| `harness/orchestration.py` | `_normalize_required`；`_capability_ok` 传 token + 四维 |
| `harness/types.py` | 新增 6 个 `EventKind`；新增 `Status.NEEDS_RECONCILIATION` |

---

## 7. 测试（8 组，v1.1 修正可运行性）

### T11 `test_ms3_effect_unknown_reconcile`

```python
calls = CountingToolGateway()
h = make_harness(toolgw=calls, probe=lambda key: True)
... 崩溃注入 → recover
assert calls.count == 1                      # 已发生 → 不重复

h2 = make_harness(toolgw=calls2, probe=lambda key: False)
assert calls2.count == 2                    # 未发生 → 重执行一次（同一 idem）

h3 = make_harness(probe=lambda key: None)
assert acb.state is TaskState.QUARANTINED   # ← 需 runtime 显式分支，否则变 FAILED
assert acb.failure == "NEEDS_RECONCILIATION"
```

> `conftest.make_harness` 增 `probe` / `toolgw` 注入参数。

### T12 `test_ms3_compensation_crash_resume`

```python
# ① 用真实崩溃注入（compensate 支持 crash_at），不是手写状态
h = make_harness(compensate_crash_at=1)     # 第 1 条后崩溃
... 触发补偿
# ② 断言**磁盘上**的 journal 状态（重放 EventLog）
entries = h.comp_journal.rebuild_from(h.event_log.snapshot())
by_id = {e.comp_id: e for e in entries}
assert by_id[c1].state == "COMPENSATED"
assert by_id[c2].state == "COMPENSATING"
assert len(by_id) == 2                      # ← 批次原子：不会只有一半
# ③ resume 分类
plan = h.comp_journal.resume(acb)
assert plan["done"] == [c1] and plan["retry"] == [c2]
```

### T13 `test_ms3_idem_key_stability`

```python
k_a = idem_key("R1", "LG1", tool, args)
k_b = idem_key("R1", "LG1", tool, args)
assert k_a == k_b
# task_id / generation 变 → 不变（v1.1：显式构造，v1.0 只注释没测）
k_c = idem_key_with(task_id="T-other", generation=9, run_id="R1", lg="LG1", tool, args)
assert k_c == k_a
# logical_subgoal_id 变 → 变
k_d = idem_key("R1", "LG-OTHER", tool, args)
assert k_d != k_a
```

### T25 `test_ms3_per_reservation_ledger_restore`

```python
h = make_harness()
acb = h.dispatch_and_run(plan, "T1", [...], required_tools=...)
h.runtime._ensure_retry_budget(acb)          # ← v1.1：必须真实追加（断言 reserve_ids 变 2）
assert len(acb.reservation_ids) == 2, "追加预留未发生，测试前提不成立"
ref = h.checkpoints.save(acb)
r = make_harness().recover(ref)
assert len(r.reservation_ids) == 2
assert h2.budget.balance_of(r.reservation_ids[0]) == 30
assert h2.budget.balance_of(r.reservation_ids[1]) == 20
h2.budget.check_invariants()
```

### T26 `test_ms3_compensation_idempotent_key`

```python
# comp_id 稳定；重复补偿 → COMPENSATED；删除类用版本号不用空 hash
```

### T27 `test_ms3_production_invariants`

```python
h = make_harness(run_limits=100)
acb = h.dispatch_and_run(plan, "T1", [...])
ref = h.checkpoints.save(acb)                # ← v1.1：先定义 ref
h.budget.check_invariants()                  # 正常路径不抛

# 破坏 → 用**可写** property（v1.1：v1.0 的只读 property 会抛 AttributeError）
monkeypatch.setattr(BudgetNode, "self_reserved",
                    property(lambda s: -1, lambda s, v: None))
with pytest.raises(InvariantError):
    h.budget.reserve("run", 1)
# 反证：去掉检查就不抛（证明判据有效）
```

### T28 `test_ms3_network_data_class_and_signature`

```python
# ① policy 层
t = h.auth.mint("A1", tools={"http"}, paths={"*"}, network={"api.example.com"})
assert h.policy.check_capability("A1", t, "http", "https://evil.com", network="evil.com") is False
# ② 签名
assert h.auth.verify(h.auth.tamper(t)) is False
assert h.auth.verify(t) is not None
assert h.auth.verify(replay(t)) is None        # jti 去重
# ③ fail-closed
with pytest.raises(MissingSecret): Signer(None)
# ④ **runtime 强制点**（v1.1：v1.0 只测 policy，证明不了不可绕过）
acb = h.dispatch_and_run(plan_with_evil_network, "T1", [...])
assert acb.state is TaskState.FAILED and acb.failure == "DENIED_CAPABILITY"
```

### T29 `test_ms3_requeue_readmit`

```python
# needs_readmit 的 ACB 不进调度；requeue_readmit 后重新准入
```

---

## 8. 风险与对策

| 风险 | 对策 |
|---|---|
| journal 事件量大 | 只写状态变更；`JournalStore` 可切后端 |
| probe 引入外部依赖 | `probe=None` → 一律 `NEEDS_RECONCILIATION`（fail-closed） |
| HMAC 不能防同进程自证 | **文档明确降级**；真隔离归阶段二（§5.9） |
| `archive_node` 破坏不变量 | 归档只动 `children`/`_nodes`；父级聚合不动；归档后校验 |
| 生产不变量拖慢热路径 | 只做 O(1) 单节点（锁外）；全树只在 recover 后 |

---

## 9. 已知边界（v1.1 诚实清单）

| # | 边界 |
|---|---|
| K-15 | HMAC **不解决同进程自证**（§5.9）——需独立信任域 |
| K-16 | `probe` 的一致性级别由外部系统保证（read-your-writes），本层只要求三态返回 |
| K-17 | `COMPENSATE` 策略下任务**不恢复执行**（终态策略），需人工或 replan |
| K-18 | journal 事件与业务事件同流，未做容量隔离 |
| K-19 | `gc` 的 retention 需 ≥ 迟到释放窗口，配置不当会丢幂等性 |

---

## 10. 决策（已定案）

| # | 问题 | 结论 |
|---|---|---|
| **Q1** | journal 落盘方式 | **同一个 EventLog**（提交点已保证持久；独立 WAL 会让重建为空） |
| **Q2** | probe 未实现时 | 一律 `NEEDS_RECONCILIATION`（fail-closed，不静默重执行） |
| **Q3** | K-10 算法 | HMAC + `jti` 去重；**目标降级为防篡改/重放**，同进程自证留阶段二 |

---

## 11. 评审修订记录（v1.0 → v1.1）

### 11.1 P0（6 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| **P0-1** | `begin` 发 INTENT 后，恢复**只 probe UNKNOWN** → INTENT 被静默重执行（同一副作用两次） | `pending()` 同时返回 INTENT+UNKNOWN；§4.1 增加 INTENT 分支（§4.1） |
| **P0-2** | §3.3「独立 WAL 后端」与 §4.3「从 EventLog 重建」**自相矛盾** → 重建为空 → 全量重复副作用 | 定死单一数据源：journal 写同一个 EventLog；`JournalStore` 降为薄封装（§3.1） |
| **P0-3** | `resume` **完全不读 `e.state`**，只看 hash → ① 后续合法写被误判 pivot；② DELETE 的空 hash 让已补偿删除被误判 retry | `resume` 先按 journal 状态分支；删除类用**版本号**判定（§3.4/§4.2） |
| **P0-4** | effect 事件 `subject=idem_key`，rebuild 按 `acb.id` 过滤 → **一条都匹配不到** | 统一 `subject=acb.id`，`idem_key` 入 payload；迁移清单显式改（§3.2） |
| **P0-5** | `NEEDS_RECONCILIATION` 无状态机落点 → runtime 通用分支置 FAILED，T11 必失败，人工入口被绕过 | `run()` 显式分支 → `QUARANTINED` + `failure`（§4.3） |
| **P0-6** | T12「崩溃中途」靠手写状态 → 只验证分类器，**验证不了批量原子性** | 加真实崩溃注入；断言**磁盘 journal** 状态与批次完整（§7 T12） |

### 11.2 P1（11 项）

| # | 问题 | 修法 |
|---|---|---|
| 1 | `rebuild_from` 读侧无快照；`Wal._load` 只跑一次 → 撕裂视图 / 读不到新帧 | `event_log.snapshot()` 锁内快照后传入（§4.3） |
| 2 | `probe_one` 抛异常会冒泡终止 run → 停在 INTENT 且不 probe | 内部 `except BaseException → "None"` + 有界超时（§3.5） |
| 3 | `plan()` 批量写非原子 → 部分意图丢失 | 一次 `append_batch`（同帧、一次 fdatasync）（§4.2） |
| 4 | `COMPENSATE` 下补偿与正常执行交错 → 补偿被覆写；且 `compensate(CompEntry)` 类型不匹配 | COMPENSATE 定义为**终态策略**：清 `staged`、补偿后不恢复执行（§4.3） |
| 5 | `event_cursor`/`_replay_events` 不存在；`cursor` 是上下文游标语义不同 | 新增独立 `event_cursor` 与 `_replay_events(from_seq, dedup)`（§4.3） |
| 6 | K-6 逐笔与聚合**双真相源**漂移 → 已消耗预算被遗忘 | 会计只以逐笔为准；聚合仅交叉校验，不一致 → `needs_readmit`（§5.1） |
| 7 | K-7 路径含重复 → `_chain` 无限循环；部分失败 → 孤立残留 | 环检测 + 失败回滚已建节点（§5.2） |
| 8 | K-13 归档后 `budget_node` 悬空、`_reservations` 改到脱离树的节点、子 `consumed` 丢失 | `consumed` 折入父级；子节点置 `ARCHIVED` tombstone；`budget_node=None`（§5.3） |
| 9 | K-10 HMAC 在同进程**不解决自证** | 目标降级为防篡改/重放；真隔离归阶段二（§5.9） |
| 10 | `check_capability` 无 token 入参 → 「不信任内存 `_tokens`」不可实现 | 五参签名，token 显式传入（§5.9） |
| 11 | `check_invariants` 全用 `assert` → `-O` 下静默通过（正是要避免的失效模式） | 改 `raise InvariantError`（§5.4） |

### 11.3 P2（已顺带修）

| 问题 | 修法 |
|---|---|
| probe 结果被反复覆盖 | 只写一次（已有 `EFFECT_PROBED` 则复用） |
| probe 打到最终一致从库 | 契约要求 read-your-writes；结论带外部版本（K-16） |
| `comp_id` 不含目标 | 纳入 `logical_subgoal_id` + `target_hash`（§3.4） |
| 「轻量不变量 O(1)」与复用 `check_invariants`（O(N)）矛盾 | 新增 `_check_local` O(1)，锁外调用（§5.4） |
| 恢复后必查若不 catch → 抛异常且**槽位泄漏** | `recover` 内 `try/except InvariantError → QUARANTINED + slots.release`（§4.3） |
| T25 前提不成立（`_ensure_retry_budget` 判据使追加不发生） | 显式设 `min_budget`/`retry_budget`；先断言追加真的发生（§7） |
| T27 用只读 property → `AttributeError` 而非 `InvariantError`；`ref` 未定义 | 改可写 property；先 `save`（§7） |
| T28 符号不存在 + 未测 runtime 强制点 | 落地 `mint`/`auth`/`Signer`/`MissingSecret`；加 runtime 级用例（§7） |
| T13 注释声称测 task_id/generation 但没构造 | 显式构造不同 `task_id`/`generation` 的 ACB（§7） |
| `jti` 无重放缓存 | `verify` 维护已见 `jti` 至 `exp`（§5.9） |

---

## 12. 实现后对抗评审（v1.1 → v1.2）

实现分三波（journal → runtime/harness/auth → 测试迁移+新用例），随后对抗评审。**4 个 P0 + 5 个高已修**，并修掉一个 flaky。

### 12.1 P0（已修）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P0-A** | `_restore_budget` 末尾**无条件** `needs_readmit=False` → K-6 交叉校验与已持久化的标记**一起被抹掉** → 带陈旧预留的 ACB 被正常调度 | 仅当逐笔重建成功**且**与聚合快照一致才置 `False`，否则保持 `True`（OR 叠加） |
| **P0-B** | `archive_node` 对**非叶子节点**只折 `self/consumed` 不折 `subtree` → 破坏 I5b → 恢复后全树必查**直接抛** | 折入时同步 `parent.subtree_reserved += node.subtree_reserved`、`parent.self_reserved += node.self_reserved`，并维护 `folded_consumed` |
| **P0-C** | 恢复含 `SETTLED` 预留的 checkpoint → `check_invariants` 误判 `INVARIANT_BROKEN` → 任务被隔离 | I5c 改为逐笔重建：`self_reserved == Σ(ACTIVE amount) + Σ(非 ACTIVE consumed) + folded_consumed` |
| **P0-D** | `recover(COMPENSATE)` 是终态策略却**不结算预留/不归档/不改终态** → 预算永久泄漏 | 补偿后 `settle` 全部预留 → `archive_node` → 置 `DONE_PARTIAL`/`DEAD_LETTER` → `checkpoints.save` |

### 12.2 高（已修）

| # | 问题 | 修法 |
|---|---|---|
| H-1 | 嵌套预算路径 `depth≥3` 永远恢复失败 | `_restore_path` 逐级 `create_node`，parent 恒为路径前一个；新增 `budget_path_limits` |
| H-2 | `EffectJournal` **无任何锁** → 并发 `probe_one` 击穿「只写一次」（实测外部 probe ×2、事件 ×2） | 加 `RLock`；`probe_one` double-checked locking |
| H-3 | `resume` 的 `escalate` 分支**零消费者** → 补偿失败被静默吞掉 | `recover` 对 `plan["escalate"]` 置 `QUARANTINED` + `COMPENSATE_FAILED` |
| H-4 | `_replay_events` 是**空壳**（只计数），去重集合不持久化 | 新增 `workspace.apply_event` 真正投影；`applied_event_ids` 写入 checkpoint |
| H-5 | `requeue_readmit` 泄漏旧槽位与旧预留（实测 slots 1→2、subtree 10→20） | 成功分支释放旧槽位、逐笔 settle 旧 rid、`remove_node`/`archive_node` |

### 12.3 中（已修）

| 问题 | 修法 |
|---|---|
| `escalate_quarantined` 不释放槽位 | 补 `slots.release` |
| `check_capability` 不校验 `token.agent_id == principal` → **A 的 token 可授权 B** | 增加身份绑定校验（`"*"` 保留为测试通配） |
| `Signer.verify`：`exp` 缺失永不过期；`float(exp)` 非法值抛 `ValueError` | 缺失/非法/过期一律 `False`（fail-closed）；周期 `gc_seen` |
| `_seen` 无界增长 | Harness 按 `auth_gc_every` 条事件触发 GC |

### 12.4 测试强度修正（去假绿）

| # | 问题 | 修法 |
|---|---|---|
| T-1 | **T12 是假绿**：崩溃注入在 `_run_one_compensation`（`plan` 早已返回）→ `len==2` 在「两次独立写」下同样成立，**证不了批次原子** | 崩溃点移到 `plan` 的 `append_batch` **内部**；断言重放后条目数 `∈ {0, N}` |
| T-2 | **T29 是假绿**：只断言 id 不同，无泄漏断言 | 补 `slots.active == 1`、`root.subtree_reserved` 回到旧值、旧 `budget_node is None`、`check_invariants()` |
| T-3 | **T27 反证偏弱**：patch 的是判据本身 | 保留（能证明「是检查在抛」）；补 `check_invariants` 版反证更佳 |
| T-4 | **T28 断言钉不住位置**：capability 与 gateway 共用 `DENIED_CAPABILITY` | 因 `policy.check_capability` 签名不含 `network`，network 拒绝**只能**来自 `_gateway_check` → 结论有效；补 detail reason 更佳 |

### 12.5 flaky 修复（重要）

`test_ms3_network_data_class_and_signature` 偶发失败（连跑 3 轮出现 1 次 `1 failed`）。

**根因**：`Signer.tamper` 只翻转 base64 签名的**最后一个字符** —— base64 末位含未使用的填充位，某些翻转会解码出**完全相同的签名字节**，HMAC 仍通过 → `verify` 返回 payload 而非 `False`。

**修法**：改为解码签名后 XOR 第一个字节再编码，保证签名字节必然变化。

**验证**：`pytest` 连跑 **10 轮**全部 `91 passed`；T28 + 3 个并发用例定向复跑 20 轮全部通过。

### 12.6 新增已知边界（v1.2）

| # | 边界 |
|---|---|
| K-20 | runtime 仍不调用 `toolgw.invoke`（外部副作用未抽象为可注入网关）→ 「同帧原子性」未被真实执行路径检验 |
| K-21 | `mark_unknown` 仅崩溃注入一处调用；生产「工具已调用但副作用未确认」目前停在 `INTENT`（由 `pending()` 含 INTENT + probe 兜住） |
| K-22 | `_replay_events` 已实现投影，但去重水位依赖 checkpoint 的 `applied_event_ids`；旧 checkpoint 无该字段时按 0 处理 |
| K-23 | `Signer` 的 HMAC secret 仍需外部注入；轮换靠 `jti` + `exp` |
