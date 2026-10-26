# S4 详细方案：预算树（Reservation + 幂等释放 + 子树守恒）+ 准入回滚 + 能力收口

| 项目 | 内容 |
|---|---|
| 版本 | **v1.2（实现完成 + 实现后审查修订）** |
| 日期 | 2026-10-26 |
| 上游 | [第一阶段方案 v1.1](phase1-implementation-plan.md) §4.7/§4.9/§4.11、[设计规范 v1.12](multi-agent-harness-design-final.md) §9.1/§4.4 |
| 覆盖测试 | T9、T10、T14、T23、T24 + 7 个最小回归 |
| 交付判据 | **83 passed**（连跑 3 轮稳定）；`pytest -m ms2` → 9 |
| 修订 | v1.1 → v1.2：按实现后对抗评审修 5 个 P0 + 5 个高（§13） |

---

## 0. 现状：4 个 P0（已核实）

| # | 缺陷 | 证据 | 后果 |
|---|---|---|---|
| **P0-1** | `release()` 把 `consumed` 清零（[budget.py:L47-L52](file:///workspace/harness/budget.py#L47-L52)） | 预留 100 → consume 100 → release → `remaining` 回到 100 | `limits` 退化成并发峰值，累计成本可无限超支 |
| **P0-2** | `children_reserved` 只加**直接子节点**（[budget.py:L24-L25](file:///workspace/harness/budget.py#L24-L25)） | root=100、A 下 B 预留 90 → root.remaining 仍 100 → C 再预留 100 成功（实占 190） | 多级嵌套击穿根节点 |
| **P0-3** | `reserve` 不查 `children_reserved`（[budget.py:L34](file:///workspace/harness/budget.py#L34)） | P.limit=100、子 C 预留 80 后，P 自己再预留 100 成功 | 单节点自身超限 |
| **P0-4** | 无 `Reservation` 句柄，`release(node_id)` 释放整节点 | 同 id 重复准入覆盖 `_nodes` → 旧预留永久泄漏 | 无法按预留回滚 |

**另外 3 个已核实**：`create_node` 总返回节点（`if not` 是死分支）；`try_admit` 预算失败时节点已建未撤销；同 `task_id` 重入覆盖句柄。

---

## 1. 目标

| # | 目标 | 判据 |
|---|---|---|
| G1 | `limits` = **累计预算**：`consumed` 永不清零，`release` 只还「未消耗部分」 | T23 ① |
| G2 | **子树守恒**：祖先看到全部后代占用（显式 `subtree_reserved`） | T23 ③④ |
| G3 | `Reservation` 句柄 + `release(reservation_id)` **幂等** | T23 ② |
| G4 | 准入四级（slots/能力/预算/逻辑）失败**零残留**（含节点身份） | T9 |
| G5 | 并发测试**可证伪**（阴性对照 + 真实实现的注入点检验） | T24 ①② |
| G6 | 能力收口：生产路径不能自证身份 | T14 |

**非目标**：多维预算、跨进程配额、父 DONE 过户、**多级预算树的生产路径接线**（见 §10 K-3）。

---

## 2. 调研结论（决定 6 个设计选择）

| 结论 | 影响 |
|---|---|
| 配额系统（Kueue/YARN/Seata TCC）区分 nominal 与 hard limit，`consumed` 是**永久账** | G1：`release` 只放 `amount − consumed` |
| **TCC 空回滚**：Cancel 必须先查/写 fence log，否则迟到 Try 会重占 | G3：幂等 release + **全局** tombstone（不挂在节点上） |
| `check + deduct` 非原子；单写者锁是阶段一唯一正确做法 | 保留 `_lock`，检查与扣减同临界区 |
| **Envoy local vs global rate limit**：本地副本各计数会超发 N 倍 | 阶段一单进程可接受；跨进程归 MS9 |
| **GIL 掩盖短竞态**（实测：无锁实现 5 线程×500 轮 **0/500** 检出；加 0.1ms 延迟 **300/300**） | G5：**必须**配阴性对照，否则绿灯不可信 |
| `len(_leases)==0` 不能判「无租约残留」（`revoke` 留 tombstone） | T9 用「无 valid 租约 + active 为空」 |

---

## 3. 数据结构

### 3.1 `Reservation`（**唯一定义在 `budget.py`**）

> ⚠️ `types.py` 已有同名 `Reservation`（3 字段 + `__bool__`）。**删除 `types.Reservation`**，`protocols.py` 改从 `budget` 导入（避免两份定义导致 `isinstance` 歧义）。

```python
@dataclass
class Reservation:
    reservation_id: str      # uuid4().hex（不可枚举，防他组件误释放）
    node_id: str
    amount: int
    consumed: int = 0
    state: str = "ACTIVE"    # ACTIVE | SETTLED | RELEASED | EXPIRED

    @property
    def balance(self) -> int:      # 剩余可用
        return self.amount - self.consumed

    def __bool__(self) -> bool:    # 兼容旧代码 `if reserve(...)`
        return True
```

**状态机**：`ACTIVE → SETTLED`｜`ACTIVE → RELEASED`｜`ACTIVE → EXPIRED`。终态不可逆。

### 3.2 `BudgetNode`（v2）

```python
class BudgetNode:
    node_id: str
    limits: int
    self_reserved: int = 0        # 本节点持有的预留总额（**含已消耗**）
    subtree_reserved: int = 0     # 本节点 + 全部后代（显式维护，沿祖先链传播）
    consumed: int = 0             # 已确认消耗（永久账）
    children: list[BudgetNode]
    reservations: dict[str, Reservation]     # 本节点发起的预留

    @property
    def remaining(self) -> int:
        return self.limits - self.subtree_reserved
```

**为什么 `subtree_reserved` 显式维护**：递归 `sum` 是 O(子树大小)，显式维护是 O(depth)。

### 3.3 三条不变量（v1.1 修正：v1.0 的 I5a 自相矛盾）

```python
def check_invariants(self) -> None:
    for n in self._nodes.values():
        # I5a：消耗 ≤ 自身预留 ≤ 子树占用 ≤ 上限
        assert 0 <= n.consumed <= n.self_reserved <= n.subtree_reserved <= n.limits
        # I5b：子树 = 自身 + 各子子树（结构一致）
        assert n.subtree_reserved == n.self_reserved + sum(c.subtree_reserved for c in n.children)
        # I5c：自身预留 = 已消耗 + 全部 ACTIVE 预留的余额
        active_balance = sum(r.balance for r in n.reservations.values() if r.state == "ACTIVE")
        assert n.self_reserved == n.consumed + active_balance
```

> **v1.0 的错误**：写成 `consumed <= self_reserved + subtree_reserved <= limits`——`subtree` 已含 `self`，等于算两遍。用 T23 ④ 就能打穿：`P.self=20, P.subtree=100, limits=100 → 120 > 100` 必失败。
> **v1.1 修正后自检**：P（self 20 / subtree 100 / consumed 0 / active 余额 20）→ `0≤0≤20≤100≤100` ✓；C（self 80 / subtree 80 / active 80）→ `0≤0≤80≤80≤100` ✓。

---

## 4. 算法（**含全部 v1.1 加固**）

### 4.1 `reserve(node_id, amount) -> Reservation | None`

```python
def reserve(self, node_id: str, amount: int) -> Reservation | None:
    if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
        return None                                   # ① 拒绝 0/负/非整数（v1.0 缺陷 1.1）
    with self._lock:
        node = self._nodes.get(node_id)
        if node is None:
            return None
        chain = self._chain(node)                     # [node, parent, ..., root]
        for a in chain:                               # ② 祖先链逐级检查
            if a.subtree_reserved + amount > a.limits:
                return None                           # 任一不够 → 整体拒绝（无部分预留）
        for a in chain:                               # ③ 扣减沿链传播
            a.subtree_reserved += amount
        node.self_reserved += amount
        resv = Reservation(uuid.uuid4().hex, node_id, amount)
        node.reservations[resv.reservation_id] = resv
        self._reservations[resv.reservation_id] = (node, resv)   # ④ 全局索引（节点删除后仍可查）
        return resv
```

### 4.2 `release(reservation_id) -> int`（幂等，只还未消耗）

```python
def release(self, reservation_id: str) -> int:
    with self._lock:
        found = self._reservations.get(reservation_id)
        if found is None:
            return 0
        node, resv = found
        if resv.state != "ACTIVE":
            return 0                                   # 幂等：终态 → 0
        freed = resv.balance                           # 只还未消耗（P0-1 修复）
        if freed > 0:
            for a in self._chain(node):
                a.subtree_reserved -= freed
            node.self_reserved -= freed
        resv.state = "RELEASED"
        return freed                                   # 保留记录（tombstone 在全局索引里）
```

### 4.3 `consume(reservation_id, amount) -> Result`

```python
def consume(self, reservation_id: str, amount: int = 1) -> Result:
    if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
        return Result(Status.INVALID_INPUT, {"reason": "bad_amount", "amount": amount})
    with self._lock:
        found = self._reservations.get(reservation_id)
        if found is None:
            return Result(Status.INVALID_INPUT, {"reason": "unknown_reservation"})  # ← 与「预算耗尽」区分
        node, resv = found
        if resv.state != "ACTIVE":
            return Result(Status.INVALID_INPUT, {"reason": "reservation_settled",
                                                 "state": resv.state})
        if resv.consumed + amount > resv.amount:
            return Result(Status.BUDGET_EXHAUSTED, {"reservation": reservation_id,
                                                    "balance": resv.balance})
        resv.consumed += amount
        node.consumed += amount                        # 永久账
        return Result(Status.OK, {"consumed": resv.consumed, "balance": resv.balance})
```

> **关键**：`unknown_reservation`/`reservation_settled` 与 `BUDGET_EXHAUSTED` **分开**——否则「句柄没接上」会被误判成「预算耗尽」（评审 P1 6.4 的 A13 假绿）。

### 4.4 `settle(reservation_id) -> int`

```python
def settle(self, reservation_id: str) -> int:
    """任务终态：未用部分归还；已消耗部分**永久留在 subtree_reserved**（累计账）。"""
    with self._lock:
        found = self._reservations.get(reservation_id)
        if found is None:
            return 0
        node, resv = found
        if resv.state != "ACTIVE":
            return 0
        freed = resv.balance
        if freed > 0:
            for a in self._chain(node):
                a.subtree_reserved -= freed
            node.self_reserved -= freed
        resv.state = "SETTLED"
        return freed
```

### 4.5 `check(node_id, amount) -> bool`（只读 guard）

```python
def check(self, node_id: str, amount: int = 0) -> bool:
    """该节点是否有**任一**活跃预留的余额足以支付 amount。"""
    with self._lock:
        node = self._nodes.get(node_id)
        if node is None:
            return False
        return any(r.state == "ACTIVE" and r.balance >= amount
                   for r in node.reservations.values())
```

### 4.6 `create_node` / `remove_node`（v1.1 加固归属校验）

```python
def create_node(self, node_id, limits, parent_id="run") -> BudgetNode | None:
    if limits <= 0:
        return None
    with self._lock:
        cur = self._nodes.get(node_id)
        if cur is not None:
            return cur if cur.limits == limits and cur.parent is self._nodes.get(parent_id) else None
            # ↑ 同 id 但 limits/parent 不同 → 拒绝（不静默复用，不静默覆盖）
        parent = self._nodes.get(parent_id)
        if parent is None:
            return None                                # 父不存在 → 拒绝（不静默挂 root）
        node = BudgetNode(node_id, limits, parent)
        parent.children.append(node)
        self._nodes[node_id] = node
        return node

def remove_node(self, node_id: str, *, owner: str = "") -> bool:
    """仅当节点「无预留、无消耗、无子、且归属匹配」时才删（准入失败回滚用）。"""
    with self._lock:
        node = self._nodes.get(node_id)
        if node is None:
            return False
        if node.self_reserved or node.subtree_reserved or node.consumed or node.children:
            return False
        if owner and node.owner != owner:
            return False                               # 归属校验（防误删他人节点）
        if node.parent:
            node.parent.children.remove(node)
        del self._nodes[node_id]
        return True
```

**节点身份（v1.1，修 P0 1.2）**：`node_id = f"B-{run_id}-{task_id}-{attempt}"`，由 `Dispatcher` 生成，**不再用 `B-{task_id}`**（否则同 task 重试必撞旧节点）。`owner` = `acb.id`。

---

## 5. `try_admit` 改造（v1.1：committed 哨兵 + 四级回滚）

```python
def try_admit(self, task, run_id, required_tools=None) -> ACB:
    acb = ACB(id=f"A-{run_id}-{task['task_id']}-{self._next_attempt()}",   # 唯一 id
              run_id=run_id, task_id=task["task_id"],
              logical_subgoal_id=task["logical_subgoal_id"])

    node = None
    resv = None
    declared: list[str] = []
    committed = False                                  # ← 哨兵：所有副作用完成后才置 True
    try:
        # ① slots（v1.1：真计数，见 §5.2）
        if not self.h.slots.try_acquire(acb.id):
            return self._deny(acb, "NO_SLOT")
        # ② 能力（与运行时同一判定入口）
        if required_tools and not self._capability_ok(acb, required_tools):
            self.h.slots.release(acb.id); return self._deny(acb, "DENIED_CAPABILITY")
        # ③ 预算
        node = self.h.budget.create_node(f"B-{acb.id}", task["footprint"]["min_budget"],
                                         parent_id=task["footprint"].get("parent_budget", "run"))
        if node is None:
            self.h.slots.release(acb.id); return self._deny(acb, "BUDGET_UNAVAILABLE")
        resv = self.h.budget.reserve(node.node_id, task["footprint"]["min_budget"])
        if resv is None:
            self.h.slots.release(acb.id); return self._deny(acb, "BUDGET_EXHAUSTED")
        acb.reservation_ids = [resv.reservation_id]
        acb.budget_node = node
        # ④ 逻辑声明
        for dep in task["footprint"].get("logical_deps", []):
            self.h.intents.declare_logical(dep["id"], acb.id, dep["kind"])
            declared.append(dep["id"])
            if not self.h.intents.compatible(dep["id"], acb.id, dep["kind"]):
                return self._deny(acb, "CONTRACT_CONFLICT")     # finally 回滚
        acb.state = TaskState.ADMITTED
        events = [self.h.event_log.append(EventKind.BUDGET_RESERVED, node.node_id,
                                          {"amount": resv.amount})]   # ← 副作用
        committed = True                                        # ← 最后才置 True
        return acb
    finally:
        if not committed:
            self._rollback(acb, node, resv, declared)

def _rollback(self, acb, node, resv, declared):
    """逆序撤销；自身用 try 包裹，并把原异常继续上抛（不吞）。"""
    for dep_id in declared:
        try: self.h.intents.undeclare_logical(dep_id, acb.id)
        except Exception: pass
    if resv is not None:
        try: self.h.budget.release(resv.reservation_id)
        except Exception: pass
    if node is not None:
        try: self.h.budget.remove_node(node.node_id, owner=acb.id)
        except Exception: pass
    acb.reservation_ids = []
    acb.budget_node = None
    self.h.slots.release(acb.id)
```

**四条要点**：
1. **`committed` 哨兵**（修 P0 2.1）：`event_log.append` 等副作用完成后才置 `True`；异常路径必回滚。
2. **回滚顺序逆序**：逻辑 → 预留 → 节点 → 槽位。
3. **`_rollback` 内部吞异常**（避免掩盖原始错误），但**不回滚两次**（只有 `finally` 一处）。
4. **`reservation_ids` 是 list**（修 P0 1.3）：`_ensure_retry_budget` 可追加。

### 5.2 Slots（v1.1 新增，修 P1 2.4）

```python
class SlotTracker:
    """活跃槽位计数（可回滚的资源）。"""
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._active: set[str] = set()
        self._lock = threading.Lock()

    def try_acquire(self, key: str) -> bool:
        with self._lock:
            if key in self._active:
                return True                     # 幂等
            if len(self._active) >= self._limit:
                return False
            self._active.add(key)
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._active.discard(key)

    @property
    def active(self) -> int:
        with self._lock:
            return len(self._active)
```

`_finalize` 里 `slots.release(acb.id)` → 修复「终态不移除导致永久 NO_SLOT」。

### 5.3 ACB 字段

```python
reservation_ids: list[str] = field(default_factory=list)   # 主 + 追加预留
```

**写入点**：`reserve` 成功后立即写；回滚时清空；随 checkpoint 持久化（修 P0 1.4）。

---

## 6. `CapabilityToken` 收口（v1.1：生产禁止自证）

```python
class CapabilityToken:
    def __init__(self, agent_id: str = "", *, tools=None, paths=None,
                 network=None, data_class="*") -> None:
        ...   # 保留旧位置参数 agent_id，兼容 test_ms7

class PolicyEngine:
    def issue(self, arg, *, tools=None, paths=None, network=None,
              data_class="*", allow_legacy: bool = False) -> CapabilityToken:
        """两种用法：
        - issue(agent_id, tools=...)          ← 新签名（生产推荐）
        - issue(token, allow_legacy=True)     ← 旧签名，仅测试/兼容
        """
        if isinstance(arg, CapabilityToken):
            if not allow_legacy:
                raise ValueError("issue(token) 仅允许 allow_legacy=True（防自证身份）")
            token = arg
        else:
            token = CapabilityToken(arg, tools=tools, paths=paths,
                                    network=network, data_class=data_class)
        self._tokens[token.agent_id] = token
        return token
```

- **`isinstance` 显式分支**（修 P1 5.2：v1.0 的「检测有 agent_id 属性」对 str 不可实现）。
- **`allow_legacy=False` 默认**（修 P1 5.1：生产路径不能自证）。
- `test_ms7_capability.py` 需改成 `issue(token, allow_legacy=True)`——列入 §7 迁移。
- `conftest.make_harness` 的 `default_token` 也改为 `issue("*", tools={"*"}, ..., allow_legacy=False)`。

---

## 7. 迁移清单（v1.1 完整，含全部受影响测试）

### 7.1 实现文件

| 文件 | 变更 |
|---|---|
| `harness/budget.py` | **重写**：`Reservation`（唯一）、`uuid` id、`subtree_reserved`、`reserve→Reservation\|None`、`release/settle` 幂等、`consume→Result`、`check`、`create_node→Optional`（同 id 冲突拒绝）、`remove_node(owner=)`、`_chain`、`_reservations` 全局索引、`check_invariants`、**只读兼容属性 `own_reserved`/`children_reserved`** |
| `harness/types.py` | `ACB` 增 `reservation_ids: list[str]`；**删除 `Reservation`**（改 re-export 自 budget） |
| `harness/orchestration.py` | `try_admit` 重写（§5）；`node_id` 含 run/attempt；新增 `SlotTracker`；`_next_attempt()` |
| `harness/runtime.py` | `budget.consume` 改 `reservation_id`；`_ensure_retry_budget` 追加并登记 `reservation_ids`；`check(node_id, 1)` 保留 |
| `harness/harness.py` | `_finalize` 遍历 `settle(reservation_id)` + `slots.release`；`escalate_quarantined` 改 settle；`recover` 恢复 `reservation_ids` + 节点字段 |
| `harness/checkpoint.py` | `save` 增 `reservation_ids` + `budget_node_id` + 节点三个计数字段 |
| `harness/policy.py` | `issue(arg, *, ..., allow_legacy=False)` |
| `harness/protocols.py` | `BudgetManager` 协议更新；`PolicyEngine.issue` 签名；`Reservation` 改从 budget 导入 |

### 7.2 测试文件（**v1.0 漏了 6 个，全部会红**）

| 文件 | 为何会红 | 迁移 |
|---|---|---|
| `tests/test_adversarial.py` A2 | `root.children_reserved` | 靠兼容属性保住（返回 `Σ children.subtree_reserved`） |
| A4 | `reserve` 返回 bool → `is not None`；`children_reserved` | 改断言 `own+children==60` → 用兼容属性仍成立 |
| A12 | `reserve("run",6)` 返回 bool；`sum==1` | 改 `resv is not None`；断言 `root.subtree_reserved == 6` |
| **A13** | `consume` 签名变 → 假绿风险 | 加断言 `node.consumed == 2`（区分真耗尽与句柄未接） |
| **A8** | `len(_leases)==0` 空断言 + `recover` 无预留 | 改「无 valid 租约 + active 为空」；recover 恢复预留 |
| `tests/test_ms8_conformance.py` I5 | `own_reserved` + 旧公式 | 改 `check_invariants()` + 预留/消费边界断言 |
| **`tests/test_ms1_rebase_loop.py` T21** | `_ensure_retry_budget` 读写 `own_reserved`、`consume(node_id)`、`escalate_quarantined` 用 `release(node_id)`、`_acb` 同 id 冲突、断言 `own_reserved == 0` | 全链路改：追加预留登记、settle、`_acb` 用唯一 id、断言改 `self_reserved == consumed` |
| **`tests/test_real_cases.py` U4** | `recover` 后无预留 → `consume` 必失败 | `recover` 恢复原预留 |
| `tests/test_ms7_capability.py` | `issue(token)` 现在要 `allow_legacy=True` | 加参数 |
| `tests/conftest.py` | `_acb` 建节点无预留；`default_token` 直接赋值 | `_acb` 改为 create+reserve 并返回 `(acb, resv)`；`default_token` 走 `issue` |

---

## 8. 测试（5 组）

### T23 `test_ms1_budget_conservation_and_idempotent_release`

```python
# ① consumed 永久账
h = make_harness(run_limits=100)
node = h.budget.create_node("B1", 100)
r1 = h.budget.reserve("B1", 60)
assert r1 is not None and r1.amount == 60 and r1.balance == 60
assert h.budget.root.remaining == 40
assert h.budget.consume(r1.reservation_id, 40) == Status.OK
assert h.budget.release(r1.reservation_id) == 20        # 只还未消耗
assert h.budget.root.remaining == 60                    # 已花 40 不退
assert node.consumed == 40
h.budget.check_invariants()

# ①′ 边界：释放后还能再预留 40，41 必须拒绝（v1.1 新增，判据更硬）
assert h.budget.reserve("B1", 40) is not None
assert h.budget.reserve("B1", 41) is None
h.budget.check_invariants()

# ② release 幂等
assert h.budget.release(r1.reservation_id) == 0
assert h.budget.release("nonexistent") == 0

# ③ 子树守恒（证伪 P0-2）
h2 = make_harness(run_limits=100)
h2.budget.create_node("P", 100)
h2.budget.create_node("C", 100, parent_id="P")
assert h2.budget.reserve("C", 90) is not None
assert h2.budget.root.remaining == 10                   # v1 是 100
assert h2.budget.reserve("run", 50) is None
h2.budget.check_invariants()

# ④ 自身+子（证伪 P0-3）
h3 = make_harness(run_limits=200)
h3.budget.create_node("P", 100)
h3.budget.create_node("C", 100, parent_id="P")
assert h3.budget.reserve("C", 80) is not None
assert h3.budget.reserve("P", 50) is None               # 50+80 > 100
assert h3.budget.reserve("P", 20) is not None           # 20+80 = 100
h3.budget.check_invariants()

# ⑤ 负/零/非整数
assert h.budget.reserve("P", -1) is None
assert h.budget.reserve("P", 0) is None
assert h.budget.reserve("P", True) is None

# ⑥ 负消耗不得反向记账（**独立节点**，不复用 h3 的满额 P 子树）
h4 = make_harness(run_limits=100)
h4.budget.create_node("N", 100)
r = h4.budget.reserve("N", 10)
assert h4.budget.consume(r.reservation_id, -5) == Status.INVALID_INPUT    # bad_amount
assert h4.budget.consume(r.reservation_id, 0) == Status.INVALID_INPUT
assert h4.budget.consume(r.reservation_id, True) == Status.INVALID_INPUT
assert h4.budget.consume(r.reservation_id, 100) == Status.BUDGET_EXHAUSTED
assert h4.budget.consume("unknown-rid", 1) == Status.INVALID_INPUT        # 与耗尽区分
h4.budget.check_invariants()
```

### T24 `test_ms1_concurrent_reserve_and_negative_control`（**v1.1 重写**）

```python
L, A, N = 10, 6, 5

def _race(reserve_fn) -> list:
    results = [None] * N
    errors: list = []
    barrier = threading.Barrier(N)
    def worker(i):
        try:
            barrier.wait(timeout=5)
            results[i] = reserve_fn(i)
        except BaseException as e:
            errors.append(e)
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
    for t in ts: t.start()
    for t in ts: t.join(timeout=10)
    assert all(not t.is_alive() for t in ts), "线程未结束 → 疑似死锁"
    assert not errors, f"线程异常: {errors}"
    return results

def _assert_conserved(h, results):
    """正例与阴性对照**共用**的判据（v1.1：修 `assert False` 恒真）。"""
    ok = [r for r in results if r is not None]
    assert all(r.amount == A and r.balance == A for r in ok)         # 返回对象自洽
    assert len({r.reservation_id for r in ok}) == len(ok)            # id 唯一
    assert len(ok) == L // A, f"成功数应为 {L//A}，实际 {len(ok)}"
    assert h.budget.root.subtree_reserved == A * len(ok)
    assert h.budget.root.remaining == L - A * len(ok)
    assert h.budget.root.remaining < A
    h.budget.check_invariants()

# ① 正例
h = make_harness(run_limits=L)
_assert_conserved(h, _race(lambda i: h.budget.reserve("run", A)))

# ② 阴性对照：坏实现必须被同一判据抓住
def test_negative_control(monkeypatch):
    def broken(self, node_id, amount):                 # 非原子 check-then-act
        node = self._nodes.get(node_id)
        if node is None or node.subtree_reserved + amount > node.limits:
            return None
        time.sleep(1e-4)                               # 放大竞态窗口
        node.subtree_reserved += amount
        node.self_reserved += amount
        return Reservation(uuid.uuid4().hex, node_id, amount)

    monkeypatch.setattr(BudgetManager, "reserve", broken)
    assert BudgetManager.reserve is broken                     # ③ 证明补丁生效
    h = make_harness(run_limits=L)
    res = _race(lambda i: h.budget.reserve("run", A))
    ok = [r for r in res if r is not None]
    assert len(ok) > L // A, "坏实现应超支（否则注入无效）"    # ④ 先证明坏实现真的坏了
    with pytest.raises(AssertionError):
        _assert_conserved(h, res)                              # ⑤ 再证明判据能抓住

# ③ 注入点检验：真实实现 + 检查后切线程，仍须守恒
def test_real_impl_with_yield_point(monkeypatch):
    orig = BudgetNode.__dict__.get("_chain") or BudgetManager._chain
    calls = {"n": 0}
    def yielding_chain(self, node):
        calls["n"] += 1
        time.sleep(0)                                   # 让出 GIL（检查与扣减之间）
        return orig(self, node)
    monkeypatch.setattr(BudgetManager, "_chain", yielding_chain)
    h = make_harness(run_limits=L)
    _assert_conserved(h, _race(lambda i: h.budget.reserve("run", A)))
    assert calls["n"] >= N                              # 证明注入点被走到
```

> **为什么必须有 ④**：`pytest.raises(AssertionError)` 只有在「坏实现真的超支、且判据真的能抓」时才成立。如果只写 `with pytest.raises: assert False`，那是**恒真**（v1.0 的缺陷）。

### T9 `test_ms2_admit_rollback_no_residue`

```python
def _snap(h):
    return {
        "nodes": {nid: (n.self_reserved, n.subtree_reserved, n.consumed, id(n))
                  for nid, n in h.budget._nodes.items()},
        "reservations": {rid: (r.state, r.consumed) for rid, (_, r) in h.budget._reservations.items()},
        "counter": h.budget._counter,
        "root_children": [(c.node_id, id(c)) for c in h.budget.root.children],
        "root_subtree": h.budget.root.subtree_reserved,
        "valid_leases": {lid for lid, l in h.intents._leases.items() if l.valid},
        "active_intents": {r: len(v) for r, v in h.intents._active.items()},
        "logical": {dep: dict(holders) for dep, holders in h.intents._logical.items()},
        "acbs": set(h.acbs),
        "slots": h.slots.active,
        "events": len(h.event_log.all()),
    }

# 预算失败
h = make_harness(run_limits=100)
plan = h.planner.plan("R", [{"task_id": "T1", "min_budget": 60},
                            {"task_id": "T2", "min_budget": 60}])
acb1 = h.dispatcher.try_admit(plan["tasks"]["T1"], "R")
assert acb1.state is TaskState.ADMITTED
mid = _snap(h)
acb2 = h.dispatcher.try_admit(plan["tasks"]["T2"], "R")
assert acb2.state is TaskState.INADMISSIBLE and acb2.failure == "BUDGET_EXHAUSTED"
assert _snap(h) == mid, "失败准入留下残留"        # ① 全字段快照（含 counter/reservations/slots/events）
assert acb2.reservation_ids == [] and acb2.budget_node is None

# 逻辑冲突（先声明不冲突的 c2，再声明冲突的 c1 → 两个都回滚）
...
assert _snap(h) == before2

# 能力失败 / NO_SLOT 同样比对快照（NO_SLOT 需先占满 slots）
```

### T10 `test_ms2_run_task_goes_through_dispatcher`（v1.1 修断言）

```python
calls = []
orig = h.dispatcher.try_admit
h.dispatcher.try_admit = lambda *a, **k: (calls.append(1), orig(*a, **k))[1]
acb = h.run_task_spec(plan, "T1", [decision(1, finish())])   # 新入口：task spec → dispatcher → run
assert len(calls) == 1                                            # ① 走 dispatcher
assert acb.reservation_ids != []                                  # ② 拿到预留
assert acb.state is TaskState.DONE
# ③ 同步结束后预算已结算：未消耗归还、consumed 留账
assert h.budget.root.remaining == h.budget.root.limits            # finish 不消耗 → 全额归还
assert h.slots.active == 0                                        # ④ 槽位已释放
```

> v1.0 的 `assert subtree_reserved > 0` 在同步路径必假（`_finalize` 已 settle）。改为断言「结算后归还 + 槽位释放」。

### T14 `test_i4_capability_production_no_self_issue`

```python
# ① 新签名可用
h = Harness()
t = h.policy.issue("A1", tools={"read"}, paths={"/src"})
assert h.policy.check_capability("A1", "read", "/src/a.ts") is True
assert h.policy.check_capability("A1", "write", "/src/a.ts") is False

# ② 生产路径不能自证身份
with pytest.raises(ValueError):
    h.policy.issue(CapabilityToken("A1", tools={"*"}), )        # 默认 allow_legacy=False

# ③ deny-by-default
assert h.policy.check_capability("A-unknown", "read", "/x") is False

# ④ 准入与运行时同一判定：required_tools ⊄ token → DENIED_CAPABILITY
...
```

---

## 9. 风险与对策

| 风险 | 对策 |
|---|---|
| 重写 budget 打挂 9 个测试 | 按 §7.2 清单逐个改，每改一个跑一次 pytest；保留 `own_reserved`/`children_reserved` 兼容属性 |
| `reservation_ids` 跨 checkpoint 恢复 | checkpoint 存 id + 节点三计数；`recover` 重建 `_reservations` 条目 |
| `uuid` 让测试不可复现 | 断言只比 `amount/balance/state`，不比 id 字面值 |
| 阴性对照拖慢测试 | 只在对照里 `sleep(1e-4)`，总开销 < 10ms |
| slots 改为 `SlotTracker` 影响 A13 | A13 走 `run_task`，`_finalize` 会 release，不受影响 |

---

## 10. 已知边界（v1.1 明确）

| # | 边界 | 影响 |
|---|---|---|
| K-1 | 生产路径不建多级预算树（`Dispatcher` 默认 `parent_budget="run"`） | P0-2/P0-3 是**库能力**，T23 ③④ 手工建树验证；父子 agent 场景归 MS4 |
| K-2 | 预算不跨进程（单写者 + 进程内 `_lock`） | 跨进程超发风险归 MS9（epoch + 对账） |
| K-3 | 崩溃恢复：只恢复 `reservation_ids` 与节点计数，不重建 `Reservation` 对象链 | 若 WAL 也丢失则退化为重新 `try_admit`；跨进程对账归 S5 |
| K-4 | 无 TTL / 心跳回收孤儿预留 | 阶段一依赖 `_finalize` 必达；异常崩溃归 S5 |
| K-5 | 多维预算（tokens/cost/wall_ms）未实现 | 标量 only |

---

## 11. 评审修订记录（v1.0 → v1.1）

### 11.1 P0（9 项）

| # | v1.0 问题 | 修法 |
|---|---|---|
| P0-1 | `consume` 无 `amount > 0` 校验 → `tool(cost=-1)` 让 `consumed` 变负，`release` 多扣，`remaining` 超 `limits`（印钞） | `reserve/release/settle/consume` 全做「正整数」校验；`consume` 返回 `INVALID_INPUT`（§4.1/4.3） |
| P0-2 | `node_id = B-{task_id}` + 幂等 `create_node` + `limits=min_budget` → 同 task 重试拿到旧节点，预算不够**永远重试不了** | `node_id = B-{acb.id}`（含 run/attempt）；`create_node` 同 id 但 limits/parent 不同 → 拒绝（§4.6） |
| P0-3 | `_ensure_retry_budget` 丢弃 `Reservation`；ACB 单 `reservation_id` 无法结算追加预留 | `ACB.reservation_ids: list`；`_ensure_retry_budget` 追加并登记；`_finalize` 遍历 settle（§5.3） |
| P0-4 | 崩溃路径预留永久孤儿；`recover` 造无预留节点（U4/A8 会红或泄漏） | checkpoint 存 `reservation_ids` + 节点三计数；`recover` 恢复（§7 / K-3） |
| P0-5 | 回滚用 `state == ADMITTED` 作哨兵 → `event_log.append` 抛异常时不回滚 | `committed = False` 哨兵，最后一行才置 True（§5） |
| P0-6 | T24 阴性对照 `with pytest.raises: assert False` **恒真** | 抽出 `_assert_conserved` 共用；先断言「坏实现真超支」，再断言判据能抓（§8 T24） |
| P0-7 | I5a `self_reserved + subtree_reserved ≤ limits` 与 I5b 矛盾（subtree 已含 self）→ `check_invariants` 必失败 | 改为 `consumed ≤ self ≤ subtree ≤ limits`（§3.3） |
| P0-8 | T21 全链路遗漏（`_ensure_retry_budget`/`consume`/`escalate`/`_acb` 同 id/`own_reserved==0` 断言） | §7.2 明列并给出迁移方式 |
| P0-9 | A2/A4/A12/I5 依赖 `own_reserved`/`children_reserved`，删除后 AttributeError | 保留**只读兼容属性**（`own_reserved → self_reserved`、`children_reserved → Σ subtree`） |

### 11.2 P1（11 项）

| # | 问题 | 修法 |
|---|---|---|
| 1 | `remove_node` 不校验归属 → 可删他人节点 | 加 `owner` 参数校验 |
| 2 | tombstone 挂在节点上，`remove_node` 会删掉 fence log | 预留索引 `_reservations` 移到**全局**，生命周期长于节点 |
| 3 | `settle` 与在途 `consume` 竞态 | `consume` 对终态预留返回 `INVALID_INPUT`（不伪装成耗尽） |
| 4 | slots 只是读判断 + `h.acbs` 只增不减 → 永久 `NO_SLOT`；T9 slots 断言是空的 | 新增 `SlotTracker`（可回滚资源），`_finalize` release |
| 5 | T24 正例检测力最弱（单轮无注入） | 加 `test_real_impl_with_yield_point`（检查后让出 GIL） |
| 6 | `_run_race` 未收 errors、未断言线程结束、未断言槽位全写 | worker 捕获 `BaseException`；`join(timeout)` + `is_alive()`；断言 `len(results)==N` |
| 7 | `len(ok)` 可能被「伪造成功对象」欺骗 | 断言 `amount/balance` 与 `reservation_id` 唯一 |
| 8 | `CapabilityToken` 旧路径仍可自证 | `issue(token)` 需 `allow_legacy=True`，默认 False |
| 9 | 「检测 token 有 agent_id 属性」对 str 不可实现 | 显式 `isinstance(arg, CapabilityToken)` 分支 |
| 10 | A13 可能因「句柄没接上」而假绿 | 断言 `node.consumed == 2` |
| 11 | `consume` 返回 bool 时无法区分「未知预留」与「预算耗尽」 | 返回 `Result`，三种状态分开 |

### 11.3 P2（已顺带修）

| 问题 | 修法 |
|---|---|
| `types.py` 与 `budget.py` 两份 `Reservation` | 删除 `types.Reservation`，改 re-export |
| `protocols.PolicyEngine.issue` 未更新 | 列入 §7.1 |
| `escalate_quarantined` 仍 `release(node_id)` | 改 `settle(reservation_id)` |
| `release/settle` 返回值被忽略 | 回滚处断言 `freed == amount` |
| `conftest.default_token` 绕过 `issue` | 改走 `issue("*", ...)` |
| T9 快照缺 `_counter`/`reservations`/slots/events | 补进 `_snap` |
| K-1 生产路径不建树 | 记为已知边界 |

---

## 12. 决策（已定案）

| # | 问题 | 结论 |
|---|---|---|
| **Q1** | `limits` 是累计预算还是并发峰值？ | **累计**（`consumed` 永不清零） |
| **Q2** | 准入顺序 | `slots → 能力 → 预算 → 逻辑` |
| **Q3** | `issue(token)` 旧路径 | 默认禁止，需 `allow_legacy=True` |

---

## 13. 实现后对抗评审（v1.1 → v1.2）

实现分四波（`budget.py` 重写 → SlotTracker+try_admit+policy → runtime/harness/checkpoint 适配 → 测试迁移+新用例），随后对抗评审。**5 个 P0 + 5 个高已修**，补 7 个最小回归。

### 13.1 P0（已修）

| # | 问题（反例） | 修法 |
|---|---|---|
| **P0-A** | **tool 预算在副作用确认之后才扣**：余额 1、cost 2 → `effects.confirm` + `TOOL_COMPLETED` 已写入，`consume` 才失败 → 外部工具真跑了但预算欠账，且重试因 `prev==CONFIRMED` 直接返回 OK **免费再跑** | tool 分支改为「先 `_consume_budget` 成功，再做 `effects.begin/confirm` + 写事件」；失败不执行工具、不写事件 |
| **P0-B** | `run` 把 `QUARANTINED` 覆写成 `FAILED`，`failure` 也被改 → `escalate_quarantined` 返回 `INVALID_INPUT`，`DEAD_LETTER` 流程走不通（T21 绿是因为它绕过 `run`） | `run` 见 `QUARANTINED` 直接返回，不改状态、不覆盖 `failure` |
| **P0-C** | slots 与 ACB 生命周期不一致：crash → `_finalize` 释放槽 → recover 不重占 → 再 admit 一个 → **两个 ACB 同时跑，`max_concurrent` 失效** | `checkpoint.save` 记 `slots_active`；`recover` 重新 `try_acquire`，失败标 `QUARANTINED/NO_SLOT` |
| **P0-D** | recover 无 `budget_node_id` 时回退用 `B-{task_id}`——**正是 §4.6 明确禁止的命名**：跨 run 同 task 复用同一节点 → `reserve` 满额失败 → `reservation_ids=[]` 但 `budget_node` 指向别人的节点 | 删除该回退：无 budget 信息则不建节点、不改 budget，置 `acb.needs_readmit=True` 交上层重准入 |
| **P0-E** | `_restore_budget` **按快照覆盖 `subtree_reserved`**：P 有子 C(sub=20)，恢复 P(sub=10) → P.subtree 被置 10 → **I5b 直接断言失败** | 有 children 时 `subtree = self + Σ children.subtree`（绝不用快照覆盖）；只有「新建且无子」才恢复自身计数 |

### 13.2 高（已修）

| # | 问题 | 修法 |
|---|---|---|
| H-1 | `_capability_ok` 直读 `policy.token()`（不在 Protocol 里）+ 不检查 path → 准入与 runtime 判定不一致 | 改为逐个 `policy.check_capability(id, tool, path)`（支持 `{tool: path}`），`default_token` 由它兜底 |
| H-2 | `guard` 用 `check(node_id, 1)` 与 `_consume_budget` 的「按 rid、单笔」脱钩 → 恢复后 `rids=[]` 时 guard 仍 True | `guard` 改为「ACB 名下存在 ACTIVE 且 `balance >= 1`」 |
| H-3 | `_ensure_retry_budget` 追加分支**生产不可达**：`node.limits == min_budget == 初始预留额` → `self_reserved == limits` → 追加必失败 → 一次 merge 都没试就 quarantine | 准入时 `limits = min_budget + retry_budget + 1`（留重试余量），仍只 `reserve(min_budget)` |
| H-4 | checkpoint 不存 `limits`/`parent_id` → 恢复用 `max(...)` 当上限（几何上碰巧对，语义错） | `save` 增 `budget_limits`/`budget_parent_id`，按其重建 |
| H-5 | T10 在测试内复现入口，掩盖 `Harness` 没有生产入口 | 新增 `Harness.dispatch_and_run(plan, task_id, decisions)` 作为唯一入口 |

### 13.3 测试有效性

| 问题 | 处置 |
|---|---|
| T9 的 `_counter` 恒 `None` | `BudgetManager` 用 uuid，无 `_counter`；快照以 `getattr(..., None)` 保留（同场景前后一致，仍能检测残留） |
| T9 未覆盖异常路径（`event_log.append` 抛异常） | 补最小回归：异常时 `committed` 哨兵必回滚 |
| T10 用测试内 helper | 改调 `Harness.dispatch_and_run` |
| A8 把「终态恢复仍占 30」写成期望 | 保留（K-4 的直接投影：非终态结算前 checkpoint，恢复后预留仍 ACTIVE） |

### 13.4 记为已知边界（未修）

| # | 边界 | 影响 |
|---|---|---|
| K-6 | 多 `reservation_ids` 无法逐笔还原余额（首笔承载全部余额，其余 SETTLED） | 逐笔预算账本需后续改造 |
| K-7 | `budget_parent_id` 指向的祖先不存在时不递归重建，直接抛错让上层 `try_admit` | 跨进程完整预算树重建未做 |
| K-8 | `required_tools` 为 `set` 时按 `path=""` 判定 → 带 `paths` 限制的 token 会被保守拒绝 | 生产应传 `{tool: path}` |
| K-9 | `needs_readmit` 仅打标，恢复入口不自动重准入 | 由上层决定 |
| K-10 | `issue(agent_id, ...)` **本身就是自证入口**：任何持有 harness 的代码可给自己签发通配 token | 「生产路径不能自证」需独立受信 ID 服务（阶段二）；`allow_legacy` 只挡住 token 对象形态 |
| K-11 | `network`/`data_class` 无执行点（装饰性权限） | 接入 gateway 或从签名移除（MS7） |
| K-12 | 预算先扣后崩（扣费成功但 effect 未 confirm） | at-least-once 固有边界，依赖 UNKNOWN + 外部反查 |
| K-13 | 终态节点与 tombstone 不回收（`remove_node` 因 `consumed>0` 永远拒绝） | 长跑内存增长，需 retention/TTL |
| K-14 | `check_invariants` 只在测试里调用，生产路径无断言 | 恢复后未校验祖先上限一致性 |
