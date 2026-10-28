"""BudgetManager —— 文档 `s4-budget-and-admission.md` v1.1 §3/§4。

核心语义：
- `limits` 是**累计预算**：`consumed` 是永久账，`release`/`settle` 只归还「未消耗部分」；
- `subtree_reserved` 显式维护「自身 + 全部后代」占用，沿祖先链传播（O(depth)）；
- `reserve` 返回 `Reservation` 句柄，`release(reservation_id)` **幂等**；
- 单写者锁，检查与扣减同临界区，避免「check + deduct」非原子超支。

v1.11（S10a §2.1）：**预算变更事件溯源 = 唯一真相源**。
- `reserve` / `release` / `settle` / `consume` / `archive_node` / `create_node`
  在**同一临界区**内 append 对应 `BUDGET_*` 事件（`notify=False`，避免触发 gc）；
- `check_invariants` 增 **3 条全局对账等式**；树完整性校验
  （`declared` = EventLog 声明的节点集 vs `rebuilt` = `_nodes`）**仅跨进程恢复启用**
  （`cross_process=True`），单进程保持原语义（§8.3 防护 3）。
"""

from __future__ import annotations

import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass

from .types import Event, EventKind, Result, Status


class InvariantError(RuntimeError, AssertionError):
    """不变量破坏（K-14 / §5.4）。

    同时继承 `AssertionError`：旧用例 `pytest.raises(AssertionError)` 仍可命中；
    同时满足 §5.4「不用 `assert`」（`python -O` 会剥离 assert）的要求。
    """


@dataclass
class Reservation:
    """预算预留句柄（`release`/`settle` 幂等）。"""

    reservation_id: str      # uuid4().hex（不可枚举，防他组件误释放）
    node_id: str
    amount: int
    consumed: int = 0
    state: str = "ACTIVE"    # ACTIVE | SETTLED | RELEASED | EXPIRED | ARCHIVED
    expires_at: int = 0      # tombstone 过期时间（epoch s）；0 = 不过期

    @property
    def balance(self) -> int:
        return self.amount - self.consumed

    def __bool__(self) -> bool:
        return True


class BudgetNode:
    def __init__(
        self,
        node_id: str,
        limits: int,
        parent: BudgetNode | None = None,
        owner: str = "",
        run_id: str = "",
    ) -> None:
        self.node_id = node_id
        self.limits = limits
        self.self_reserved = 0        # 本节点持有的预留总额（含已消耗）
        self.subtree_reserved = 0     # 本节点 + 全部后代（显式维护）
        self.consumed = 0             # 已确认消耗（永久账）
        self.folded_consumed = 0      # 归档子树折入的永久账（无对应预留条目）
        self.children: list[BudgetNode] = []
        self.reservations: dict[str, Reservation] = {}
        self.owner = owner
        self.run_id = run_id          # K-40：预算事件按 run 归属（per-run 过滤）
        self.parent = parent
        if parent is not None:
            parent.children.append(self)

    @property
    def remaining(self) -> int:
        return self.limits - self.subtree_reserved

    # ---- 只读兼容属性（旧测试/旧调用方使用） ----
    @property
    def own_reserved(self) -> int:
        return self.self_reserved

    @property
    def children_reserved(self) -> int:
        return sum(c.subtree_reserved for c in self.children)


_BUDGET_KINDS = frozenset({
    EventKind.BUDGET_NODE_CREATED, EventKind.BUDGET_RESERVED,
    EventKind.BUDGET_CONSUMED, EventKind.BUDGET_RELEASED,
    EventKind.BUDGET_SETTLED, EventKind.BUDGET_ARCHIVED,
    EventKind.BUDGET_EPOCH,
})
_BUDGET_VALUES = frozenset(k.value for k in _BUDGET_KINDS)


def _is_budget_kind(kind) -> bool:
    if isinstance(kind, EventKind):
        return kind in _BUDGET_KINDS
    value = getattr(kind, "value", kind)
    return str(value) in _BUDGET_VALUES


def _node_payload(node: BudgetNode, rid: str, amount: int, consumed: int,
                  state: str, epoch: int) -> dict:
    """统一的 BUDGET_* payload（node_id/rid/amount/consumed/state/limits/parent_id/epoch/run_id）。"""
    return {
        "node_id": node.node_id,
        "rid": rid,
        "amount": amount,
        "consumed": consumed,
        "state": state,
        "limits": node.limits,
        "parent_id": node.parent.node_id if node.parent is not None else None,
        "budget_epoch": epoch,
        "run_id": getattr(node, "run_id", "") or "",
    }


class BudgetManager:
    def __init__(self, run_limits: int = 1000, tombstone_ttl_s: float = 86400.0,
                 event_log=None, epoch_store=None,
                 run_scope: str = "global") -> None:
        self.root = BudgetNode("run", run_limits)
        self._lock = threading.RLock()
        self._nodes: dict[str, BudgetNode] = {"run": self.root}
        # 全局索引：rid -> (node, resv)；生命周期长于节点（tombstone）
        self._reservations: dict[str, tuple[BudgetNode, Reservation]] = {}
        self._tombstone_ttl_s = tombstone_ttl_s
        # ---- S10a：事件溯源（唯一真相源）----
        self.event_log = event_log          # 由 Harness 注入（可 None）
        self._epoch = 0                      # 预算 epoch（全局 writer lease 层）
        # ---- S12 K-40 / K-43 ----
        self._run_id = ""                    # 当前 run 上下文（事件 run_id 兜底）
        self._epoch_store = epoch_store      # run 层 epoch（Harness 注入）
        # `run_scope="run"` 才启用 per-run fencing；默认 "global" 保持旧语义
        self._run_scope = run_scope if run_scope in ("run", "global") else "global"
        self._suppress_depth = 0             # 事件抑制深度（准入事务用）
        self._pending: list[dict] = []       # 被抑制的事件草稿（set 语义）
        self._last_budget_seq = 0            # 最近一条已 append 预算事件的 seq
        # fencing：durable epoch 读取入口（由 Harness 注入 `checkpoints.read_epoch`）。
        # 仅用于**写操作**的 fence 校验；重放/恢复路径不经过该守卫。
        self._epoch_source = None

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    def set_run_id(self, run_id: str) -> str:
        """设置当前 run 上下文（K-40：预算事件 run_id 的兜底来源）。"""
        self._run_id = run_id or ""
        return self._run_id

    @property
    def run_id(self) -> str:
        return self._run_id

    def _node_run_id(self, node_id: str) -> str:
        """事件的 run 归属：节点自带 `run_id` 优先，否则当前 run 上下文。"""
        node = self._nodes.get(node_id)
        if node is not None and getattr(node, "run_id", ""):
            return node.run_id
        return self._run_id

    def _run_scoped(self, run_id: str | None) -> bool:
        return (self._run_scope == "run" and bool(run_id)
                and self._epoch_store is not None)

    def _epoch_for(self, run_id: str | None) -> int:
        """**本地** epoch（fencing 比较的本地侧）：全局 writer lease epoch。

        K-43：per-run epoch 是**durable 侧**的隔离层（见 `_durable_epoch`），
        不作为本地比较基准 —— 否则 per-run bump 会同时抬高本地值 → 永远不 fence
        （那正是 v1.0 `_run_epochs` 装饰性的根因）。
        """
        return self._epoch

    def _durable_epoch(self, run_id: str | None = None) -> int:
        """读取持久化 epoch（无注入 → 退回本地值，不 fence）。

        K-43：`run_scope="run"` → `max(全局 writer lease epoch, run 层 store)`；
        否则只读 writer lease epoch（`wal.log.epoch` / `store.json`，旧语义）。
        """
        durable = 0
        if self._run_scoped(run_id):
            durable = int(self._epoch_store.read_epoch(run_id))
        source = self._epoch_source
        if source is None:
            return max(durable, self._epoch)
        try:
            return max(durable, int(source()))
        except Exception:  # noqa: BLE001  # 读失败不得误杀写路径
            return max(durable, self._epoch)

    def _epoch_fenced(self, durable: int, run_id: str | None = None) -> bool:
        """本地 epoch 落后于持久化 epoch → 本写者已被 fence（慢写者被接管）。"""
        return self._epoch_for(run_id) < durable

    def _chain(self, node: BudgetNode) -> list[BudgetNode]:
        """[node, parent, ..., root]"""
        chain: list[BudgetNode] = []
        cur: BudgetNode | None = node
        while cur is not None:
            chain.append(cur)
            cur = cur.parent
        return chain

    @staticmethod
    def _valid_amount(amount: object) -> bool:
        return isinstance(amount, int) and not isinstance(amount, bool) and amount > 0

    # ------------------------------------------------------------------ #
    # 事件溯源（S10a §2.1 / §2.6）
    # ------------------------------------------------------------------ #
    def _emit(self, kind: EventKind, node_id: str, payload: dict,
              run_id: str | None = None) -> None:
        """临界区内 append 预算事件（`notify=False`）。

        K-40：事件带 `run_id`（`budget_events(run_id)` 才能按 run 过滤，
        否则恒为空）。归属优先级：显式 `run_id` → 节点 `run_id` → 当前 run 上下文。

        抑制期间只记录草稿（`_pending`），由 `flush_pending` 一次性 append_batch；
        被丢弃时由 `discard_pending` 清空 —— 保证「失败准入零残留」（含事件）。
        """
        if run_id is None:
            run_id = self._node_run_id(node_id)
        if self.event_log is None:
            return
        if self._suppress_depth > 0:
            self._pending.append({"kind": kind, "subject": node_id,
                                  "payload": payload, "run_id": run_id or ""})
            return
        ev = self.event_log.append(kind, node_id, payload, run_id=run_id or "",
                                   notify=False)
        self._last_budget_seq = max(self._last_budget_seq, ev.seq)

    @contextmanager
    def suppressed(self):
        """抑制预算事件 append（进入准入/恢复等事务）。"""
        self._suppress_depth += 1
        try:
            yield self
        finally:
            self._suppress_depth -= 1

    def flush_pending(self) -> None:
        """把被抑制的事件草稿一次性 append_batch（同帧，原子）。

        §4.4：交换 `_pending` 必须在 `_lock` 内完成 —— 锁外 `drafts = self._pending;
        self._pending = []` 是非原子交换，并发调用可各取同一批 → 重复 append。
        """
        with self._lock:
            if not self._pending:
                return
            drafts = self._pending
            self._pending = []
            if self.event_log is not None:
                events = self.event_log.append_batch(drafts, notify=False)
                if events:
                    self._last_budget_seq = max(self._last_budget_seq, events[-1].seq)

    def discard_pending(self) -> None:
        with self._lock:
            self._pending = []

    def set_epoch(self, epoch: int, emit: bool = True,
                  run_id: str | None = None) -> int:
        """设置 epoch（单调；`emit` 控制是否写入事件流）。

        K-43：`run_id` 给定 → 写 **run 层**（`EpochStore`，durable 侧隔离层，
        使该 run 的预算写被 fence）；否则写全局 writer lease 层。
        """
        with self._lock:
            epoch = int(epoch or 0)
            if self._run_scoped(run_id):
                cur = int(self._epoch_store.read_epoch(run_id))
                if epoch <= cur:
                    return cur
                self._epoch_store.set_epoch(run_id, epoch)
                if emit:
                    self._emit(EventKind.BUDGET_EPOCH, "run",
                               {"epoch": epoch, "reason": "fence",
                                "run_id": run_id},
                               run_id=run_id)
                return int(self._epoch_store.read_epoch(run_id))
            if epoch <= self._epoch:
                return self._epoch
            self._epoch = epoch
            if emit:
                self._emit(EventKind.BUDGET_EPOCH, "run",
                           {"epoch": epoch, "reason": "fence", "run_id": ""})
            return self._epoch

    def bump_epoch(self, reason: str = "", run_id: str | None = None) -> int:
        """epoch 单调递增（fencing 依据），并写入事件流。

        `run_id` 给定 → run 层（K-43）；否则全局（向后兼容）。
        """
        with self._lock:
            return self.set_epoch(self._durable_epoch(run_id) + 1, emit=True,
                                  run_id=run_id)

    # ------------------------------------------------------------------ #
    # 预留 / 释放 / 结算
    # ------------------------------------------------------------------ #
    def reserve(self, node_id: str, amount: int, *,
                reservation_id: str | None = None) -> Reservation | None:
        """预留 `amount`。

        `reservation_id`（S17 K-81）：**可注入**确定性 id —— Raft 后端在复制
        状态机内用递增计数器生成（`f"R{n}"`），使各副本得到**相同** id，
        从而使预算状态机可复制。缺省 `None` → `uuid4()`（保持既有语义）。
        """
        if not self._valid_amount(amount):
            return None
        node0 = self._nodes.get(node_id)
        run_id = getattr(node0, "run_id", "") or self._run_id
        # fencing：先读 durable epoch（不得在 `_lock` 内读 → 避免 budget→checkpoint 反向锁序）
        durable = self._durable_epoch(run_id)
        with self._lock:
            if self._epoch_fenced(durable, run_id):
                return None
            node = self._nodes.get(node_id)
            if node is None:
                return None
            chain = self._chain(node)
            for a in chain:
                if a.subtree_reserved + amount > a.limits:
                    return None
            for a in chain:
                a.subtree_reserved += amount
            node.self_reserved += amount
            resv = Reservation(reservation_id or uuid.uuid4().hex, node_id, amount)
            node.reservations[resv.reservation_id] = resv
            self._reservations[resv.reservation_id] = (node, resv)
            self._emit(EventKind.BUDGET_RESERVED, node_id,
                       _node_payload(node, resv.reservation_id, amount, 0, "ACTIVE",
                                     self._epoch_for(run_id)))
            self._check_local(node)
            return resv

    def release(self, reservation_id: str) -> int:
        found0 = self._reservations.get(reservation_id)
        run_id = (getattr(found0[0], "run_id", "") or self._run_id) if found0 else self._run_id
        durable = self._durable_epoch(run_id)
        with self._lock:
            if self._epoch_fenced(durable, run_id):
                return 0
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
            resv.state = "RELEASED"
            self._emit(EventKind.BUDGET_RELEASED, node.node_id,
                       _node_payload(node, reservation_id, resv.amount, resv.consumed,
                                     "RELEASED", self._epoch_for(run_id)))
            self._check_local(node)
            return freed

    def settle(self, reservation_id: str) -> int:
        found0 = self._reservations.get(reservation_id)
        run_id = (getattr(found0[0], "run_id", "") or self._run_id) if found0 else self._run_id
        durable = self._durable_epoch(run_id)
        with self._lock:
            if self._epoch_fenced(durable, run_id):
                return 0
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
            self._emit(EventKind.BUDGET_SETTLED, node.node_id,
                       _node_payload(node, reservation_id, resv.amount, resv.consumed,
                                     "SETTLED", self._epoch_for(run_id)))
            self._check_local(node)
            return freed

    def consume(self, reservation_id: str, amount: int = 1, *,
                acb=None) -> Result:
        """按预留消费预算。

        S13d §2.5 P0-10：传入 `acb` 时先过 claim fencing（旧 worker 的
        `claim_epoch` 落后 → `REJECT_STALE`，不消费、不写事件）。
        """
        if not self._valid_amount(amount):
            return Result(Status.INVALID_INPUT, {"reason": "bad_amount", "amount": amount})
        guard = getattr(self, "claim_guard", None)
        if acb is not None and callable(guard) and guard(acb):
            return Result(Status.REJECT_STALE, {
                "reason": "claim_fenced", "acb_id": getattr(acb, "id", ""),
                "reservation": reservation_id})
        found0 = self._reservations.get(reservation_id)
        run_id = (getattr(found0[0], "run_id", "") or self._run_id) if found0 else self._run_id
        durable = self._durable_epoch(run_id)
        with self._lock:
            if self._epoch_fenced(durable, run_id):
                return Result(Status.REJECT_STALE, {
                    "reason": "budget_epoch_fenced",
                    "budget_epoch": self._epoch_for(run_id), "durable_epoch": durable,
                })
            found = self._reservations.get(reservation_id)
            if found is None:
                return Result(Status.INVALID_INPUT, {"reason": "unknown_reservation"})
            node, resv = found
            if resv.state != "ACTIVE":
                return Result(
                    Status.INVALID_INPUT,
                    {"reason": "reservation_settled", "state": resv.state},
                )
            if resv.consumed + amount > resv.amount:
                return Result(
                    Status.BUDGET_EXHAUSTED,
                    {"reservation": reservation_id, "balance": resv.balance},
                )
            resv.consumed += amount
            node.consumed += amount
            payload = _node_payload(node, reservation_id, resv.amount, resv.consumed,
                                    resv.state, self._epoch_for(run_id))
            self._check_local(node)
            # 事件 append 必须在 `_lock` 内：否则与 release/settle 交错时事件序变成
            # RESERVED→RELEASED→CONSUMED，重放态与实时态不符（E3 误破）。
            self._emit(EventKind.BUDGET_CONSUMED, node.node_id, payload)
            return Result(
                Status.OK, {"consumed": resv.consumed, "balance": resv.balance}
            )

    def check(self, node_id: str, amount: int = 0) -> bool:
        """该节点是否有**任一**活跃预留的余额足以支付 amount。"""
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return False
            return any(
                r.state == "ACTIVE" and r.balance >= amount
                for r in node.reservations.values()
            )

    def check_reservations(self, reservation_ids, amount: int = 1) -> bool:
        """给定预留中是否存在**一笔** ACTIVE 且余额足够支付 amount 的。"""
        with self._lock:
            for rid in reservation_ids:
                found = self._reservations.get(rid)
                if found is None:
                    continue
                _, resv = found
                if resv.state == "ACTIVE" and resv.balance >= amount:
                    return True
            return False

    # ------------------------------------------------------------------ #
    # 节点生命周期
    # ------------------------------------------------------------------ #
    def create_node(
        self, node_id: str, limits: int, parent_id: str = "run",
        run_id: str = ""
    ) -> BudgetNode | None:
        if limits <= 0:
            return None
        with self._lock:
            cur = self._nodes.get(node_id)
            if cur is not None:
                if cur.limits == limits and cur.parent is self._nodes.get(parent_id):
                    return cur
                return None
            parent = self._nodes.get(parent_id)
            if parent is None:
                return None
            node = BudgetNode(node_id, limits, parent, run_id=run_id or self._run_id)
            self._nodes[node_id] = node
            self._emit(EventKind.BUDGET_NODE_CREATED, node_id,
                       {"node_id": node_id, "parent_id": parent_id, "limits": limits,
                        "budget_epoch": self._epoch_for(node.run_id or None),
                        "run_id": node.run_id})
            return node

    def remove_node(self, node_id: str, *, owner: str = "") -> bool:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return False
            if (
                node.self_reserved
                or node.subtree_reserved
                or node.consumed
                or node.children
            ):
                return False
            if owner and node.owner != owner:
                return False
            if node.parent is not None:
                node.parent.children.remove(node)
            del self._nodes[node_id]
            return True

    # ------------------------------------------------------------------ #
    # 归档 / 回收（K-13）
    # ------------------------------------------------------------------ #
    def _subtree_nodes(self, node: BudgetNode) -> list[BudgetNode]:
        """返回 `node` 及其全部后代（归档需要整体折叠）。"""
        out: list[BudgetNode] = []
        stack = [node]
        while stack:
            cur = stack.pop()
            out.append(cur)
            stack.extend(cur.children)
        return out

    def archive_node(self, node_id: str) -> bool:
        """终态摘除（§5.3）：子树内无 ACTIVE 预留才可归档。

        ① 整个子树的 `consumed` 折入父级（永久账不丢），并同步父级
           `self_reserved` / `subtree_reserved`，保证 I5a/I5b/I5c 同时成立；
        ② 全局索引只留 tombstone（带 `expires_at`），不立即删；
        ③ 从 `_nodes` 摘除整个子树。返回是否归档。
        """
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return False
            subtree = self._subtree_nodes(node)
            for n in subtree:
                for r in n.reservations.values():
                    if r.state == "ACTIVE":
                        return False
            parent = node.parent
            if parent is not None:
                total_subtree = node.subtree_reserved
                total_consumed = sum(n.consumed for n in subtree)
                # node 原先以 total_subtree 计入 parent.subtree；摘除后改以
                # total_consumed 计入 parent 自身，故 subtree 的净变化为
                # total_consumed - total_subtree（结算正确时通常为 0）。
                parent.self_reserved += total_consumed
                parent.subtree_reserved += total_consumed - total_subtree
                parent.consumed += total_consumed
                # 折入的永久账没有对应的预留条目，单独记账，供 I5c 校验
                parent.folded_consumed += total_consumed
                if node in parent.children:
                    parent.children.remove(node)
                self._check_local(parent)
                folded_consumed = total_consumed
            else:
                folded_consumed = 0
            now = int(time.time())
            for n in subtree:
                for rid in list(n.reservations):
                    resv = n.reservations[rid]
                    resv.state = "ARCHIVED"
                    resv.expires_at = now + int(self._tombstone_ttl_s)
                self._nodes.pop(n.node_id, None)
            self._emit(EventKind.BUDGET_ARCHIVED, node_id,
                       {"node_id": node_id, "folded_consumed": folded_consumed,
                        "budget_epoch": self._epoch_for(node.run_id or None),
                        "run_id": node.run_id})
            return True

    def gc(self, now: float) -> int:
        """清除已过期的 ARCHIVED tombstone（§5.3）。"""
        removed = 0
        with self._lock:
            for rid in list(self._reservations):
                node, resv = self._reservations[rid]
                if resv.state == "ARCHIVED" and resv.expires_at and resv.expires_at <= now:
                    del self._reservations[rid]
                    if node is not None:
                        node.reservations.pop(rid, None)
                    removed += 1
        return removed

    # ------------------------------------------------------------------ #
    # 事件重放（S10a §2.2）—— 唯一真相源
    # ------------------------------------------------------------------ #
    def budget_events(self, run_id: str | None = None) -> list[Event]:
        """EventLog 中的 `BUDGET_*` 事件（按 seq 升序）。

        K-40：`run_id` 给定 → 只返回该 run 的事件（按 `e.run_id` 过滤）；
        `None` → 全部（向后兼容）。
        """
        if self.event_log is None:
            return []
        events = [e for e in self.event_log.all() if _is_budget_kind(e.kind)]
        if run_id is None:
            return events
        return [e for e in events if e.run_id == run_id]

    def _reset_baseline(self) -> None:
        """重放前重置基线为 0（只留 `run`，清空全部账）—— 修 P1-3 双份。

        §4.4：必须在 `_lock` 内完成（并发 recover 不得读到中间态），并同时把
        `_epoch` 归零 —— 重放会按 `BUDGET_EPOCH` 事件重建 epoch；若不归零，
        残留的高 epoch 会放大树完整性豁免面（旧节点被误判为已 fence）。
        """
        with self._lock:
            root = self.root
            for nid in list(self._nodes):
                if nid != "run":
                    del self._nodes[nid]
            root.children = []
            root.self_reserved = 0
            root.subtree_reserved = 0
            root.consumed = 0
            root.folded_consumed = 0
            root.reservations = {}
            self._reservations = {}
            self._epoch = 0

    def apply_budget_event(self, ev: Event) -> bool:
        """按 `rid` / `node_id` **幂等**应用一条 `BUDGET_*` 事件（set 语义）。

        返回是否被接受（`True`）或被视为重复（`True`）；无法应用返回 `False`。
        """
        p = ev.payload or {}
        with self._lock:
            kind = ev.kind
            if kind == EventKind.BUDGET_EPOCH:
                self._epoch = max(self._epoch, int(p.get("epoch", 0) or 0))
                return True
            if kind == EventKind.BUDGET_NODE_CREATED:
                nid = p.get("node_id")
                if not nid:
                    return False
                parent_id = p.get("parent_id") or "run"
                limits = int(p.get("limits", 0) or 0)
                run_id = p.get("run_id") or ""
                cur = self._nodes.get(nid)
                if cur is not None:
                    return (cur.limits == limits
                            and cur.parent is self._nodes.get(parent_id))
                if limits <= 0:
                    return False
                parent = self._nodes.get(parent_id)
                if parent is None:
                    return False
                self._nodes[nid] = BudgetNode(nid, limits, parent, run_id=run_id)
                return True
            if kind in (EventKind.BUDGET_RESERVED, EventKind.BUDGET_CONSUMED,
                        EventKind.BUDGET_RELEASED, EventKind.BUDGET_SETTLED):
                return self._apply_reservation_event(kind, p)
            if kind == EventKind.BUDGET_ARCHIVED:
                return self._apply_archived(p)
            return False

    def _apply_reservation_event(self, kind: EventKind, p: dict) -> bool:
        rid = p.get("rid")
        if not rid:
            return False
        found = self._reservations.get(rid)
        if kind == EventKind.BUDGET_RESERVED:
            if found is not None:
                # set 语义：已存在 → 幂等跳过；但必须比对身份/额度，否则
                # 「同 rid 不同 amount/node_id/limits」会被静默吞掉（重放态失真）。
                existing_node, existing = found
                amount = int(p.get("amount", existing.amount) or 0)
                node_id = p.get("node_id") or existing.node_id
                limits = p.get("limits")
                mismatch = (existing.amount != amount
                            or existing.node_id != node_id
                            or (limits is not None
                                and existing_node.limits != int(limits)))
                return not mismatch
            node = self._nodes.get(p.get("node_id") or "")
            if node is None:
                return False
            amount = int(p.get("amount", 0) or 0)
            consumed = int(p.get("consumed", 0) or 0)
            state = p.get("state") or "ACTIVE"
            if amount <= 0:
                return False
            resv = Reservation(rid, node.node_id, amount, consumed, state)
            node.reservations[rid] = resv
            self._reservations[rid] = (node, resv)
            if state == "ACTIVE":
                node.self_reserved += amount
                for a in self._chain(node):
                    a.subtree_reserved += amount
            else:
                node.self_reserved += consumed
                node.consumed += consumed
            return True
        if found is None:
            return False
        node, resv = found
        if kind == EventKind.BUDGET_CONSUMED:
            # 对齐运行时守卫：非 ACTIVE 条目不得再累加 consumed。否则
            # RESERVED→RELEASED→CONSUMED 的交错重放会污染永久账（E3 误破）。
            if resv.state != "ACTIVE":
                return True
            target = int(p.get("consumed", resv.consumed) or 0)
            if target == resv.consumed:
                return True
            if target < resv.consumed:
                return True                     # 幂等：不回退
            delta = target - resv.consumed
            resv.consumed = target
            node.consumed += delta
            return True
        # RELEASED / SETTLED：只归还「未消耗部分」
        if resv.state == "ACTIVE":
            freed = resv.balance
            if freed > 0:
                for a in self._chain(node):
                    a.subtree_reserved -= freed
                node.self_reserved -= freed
        resv.state = (p.get("state")
                      or ("RELEASED" if kind == EventKind.BUDGET_RELEASED else "SETTLED"))
        return True

    def _apply_archived(self, p: dict) -> bool:
        nid = p.get("node_id")
        if not nid:
            return False
        node = self._nodes.get(nid)
        if node is None:
            return True                          # 幂等：已摘除
        folded = int(p.get("folded_consumed", 0) or 0)
        parent = node.parent
        if parent is not None:
            parent.self_reserved += folded
            parent.subtree_reserved += folded - node.subtree_reserved
            parent.consumed += folded
            parent.folded_consumed += folded
            if node in parent.children:
                parent.children.remove(node)
        now = int(time.time())
        for r in node.reservations.values():
            r.state = "ARCHIVED"
            r.expires_at = now + int(self._tombstone_ttl_s)
        self._nodes.pop(nid, None)
        return True

    def node_epoch(self, node_id: str) -> int:
        """该节点相关预算事件的**最大** `budget_epoch`（-1 = 无事件）。"""
        best = -1
        for e in self.budget_events():
            p = e.payload or {}
            if p.get("node_id") == node_id:
                best = max(best, int(p.get("budget_epoch", 0) or 0))
        return best

    def snapshot(self) -> dict:
        """预算聚合快照（供 checkpoint 交叉校验，**非权威**）。"""
        with self._lock:
            return {
                "run_limits": self.root.limits,
                "budget_cursor": self._last_budget_seq,
                "budget_epoch": self._epoch,
                "nodes": {
                    nid: {
                        "self_reserved": n.self_reserved,
                        "subtree_reserved": n.subtree_reserved,
                        "consumed": n.consumed,
                        "folded_consumed": n.folded_consumed,
                        "limits": n.limits,
                    }
                    for nid, n in self._nodes.items()
                },
            }

    # ------------------------------------------------------------------ #
    # 不变量（K-14 / §5.4 / S10a §2.4）
    # ------------------------------------------------------------------ #
    def _check_local(self, node: BudgetNode) -> None:
        """O(1) 单节点断言（热路径默认开）。"""
        if not (0 <= node.consumed <= node.self_reserved
                <= node.subtree_reserved <= node.limits):
            raise InvariantError(f"local invariant broken at {node.node_id}")

    def _global_equations(self) -> None:
        """S10a §2.4：3 条全局对账等式（不依赖单节点遍历的重复）。

        - E1 `root.subtree_reserved == Σ_n self_reserved`（子树守恒的全局形式）
        - E2 `Σ_n consumed == Σ_{现有节点预留} r.consumed + Σ_n folded_consumed`
          （I5c 的 consumed 子句之和；等价于 run 级 consumed 恒等式）
        - E3 `Σ_{ACTIVE} amount == Σ_n self_reserved − Σ_{非ACTIVE} consumed − Σ_n folded`
        """
        total_self = sum(n.self_reserved for n in self._nodes.values())
        if self.root.subtree_reserved != total_self:
            raise InvariantError(
                f"global E1 broken: root.subtree_reserved="
                f"{self.root.subtree_reserved} != Σ self_reserved={total_self}")
        existing: set[str] = set()
        for n in self._nodes.values():
            existing.update(n.reservations)
        ledger_consumed = sum(self._reservations[rid][1].consumed for rid in existing)
        total_folded = sum(n.folded_consumed for n in self._nodes.values())
        total_consumed = sum(n.consumed for n in self._nodes.values())
        if total_consumed != ledger_consumed + total_folded:
            raise InvariantError(
                f"global E2 broken: Σ consumed={total_consumed} != "
                f"Σ ledger.consumed={ledger_consumed} + Σ folded={total_folded}")
        active_amount = sum(self._reservations[rid][1].amount for rid in existing
                            if self._reservations[rid][1].state == "ACTIVE")
        nonactive_consumed = sum(self._reservations[rid][1].consumed for rid in existing
                                 if self._reservations[rid][1].state != "ACTIVE")
        if active_amount != total_self - nonactive_consumed - total_folded:
            raise InvariantError(
                f"global E3 broken: Σ ACTIVE amount={active_amount} != "
                f"Σ self_reserved - Σ non-ACTIVE.consumed - Σ folded = "
                f"{total_self - nonactive_consumed - total_folded}")

    def _tree_integrity(self, run_id: str | None = None) -> None:
        """S10a §2.3：树完整性**锚定 EventLog 声明的节点集**（非同义反复）。

        S12 K-40：`run_id` 给定 → 只按该 run 的预算事件校验（分 run 重放用）；
        否则全局（旧语义）。

        `declared` = 事件声明的 node_id 集（ARCHIVED 视为已摘除）；
        `rebuilt` = `self._nodes`。**双向**校验：
        - `declared - rebuilt`：缺席节点必须是「确有显式 fence 记录」的旧 epoch 节点；
        - `rebuilt - declared`：重放不得凭空造出事件未声明的节点（单向检查的假阴性）。

        fence 判定不再只比较数值：必须存在一条 `BUDGET_EPOCH` 事件，其 epoch
        **大于该节点最大事件 epoch**（显式 fence 记录），且不低于当前 cutoff。
        否则「checkpoint meta 里的高 epoch（emit=False）」会把所有旧节点都豁免。
        """
        total_self = sum(n.self_reserved for n in self._nodes.values())
        if self.root.subtree_reserved != total_self:
            raise InvariantError(
                f"tree integrity broken: root.subtree_reserved="
                f"{self.root.subtree_reserved} != Σ self_reserved={total_self}")

        declared: set[str] = set()
        node_max: dict[str, int] = {}
        fence_epochs: list[int] = []
        for ev in self.budget_events(run_id):
            p = ev.payload or {}
            if ev.kind == EventKind.BUDGET_EPOCH:
                fence_epochs.append(int(p.get("epoch", 0) or 0))
                continue
            nid = p.get("node_id")
            if not nid or nid == "run":
                continue
            if ev.kind == EventKind.BUDGET_ARCHIVED:
                declared.discard(nid)
            else:
                declared.add(nid)
            node_max[nid] = max(node_max.get(nid, -1),
                                int(p.get("budget_epoch", 0) or 0))
        rebuilt = set(self._nodes) - {"run"}

        extra = rebuilt - declared
        if extra:
            raise InvariantError(
                f"tree integrity broken: node {min(extra)!r} rebuilt but "
                f"not declared by EventLog")

        missing = declared - rebuilt
        if not missing:
            return
        cutoff = self._epoch
        fence = max(fence_epochs) if fence_epochs else -1
        for nid in sorted(missing):
            node_epoch = node_max.get(nid, -1)
            if node_epoch >= 0 and fence > node_epoch and fence >= cutoff:
                continue                        # 显式 fence 记录 → 合法缺席
            raise InvariantError(
                f"tree integrity broken: node {nid!r} declared by EventLog "
                f"but not rebuilt")

    def check_invariants(self, *, cross_process: bool = False) -> None:
        """O(N) 全树校验（只在 recover 后 / debug 调用）。不用 `assert`。

        I5c 必须与 `settle` / `release` / `archive` 的语义自洽：
        - ACTIVE 条目按 `amount` 计入 `self_reserved`（含未消耗余额）；
        - SETTLED / RELEASED / ARCHIVED 条目只保留 `consumed`（余额已归还）；
        - `consumed` 是永久账，等于全部条目的 `consumed` 之和；
        - 归档子树折入的永久账记在 `folded_consumed`（无对应预留条目）。
        等价于 `self_reserved == consumed + Σ(ACTIVE balance)`。

        §4.4：加 `_lock`，避免周期 `gc` 与 recover 后的 `check_invariants` 并发
        读到中间态 → 偶发 `InvariantError`（新 flaky）。

        S10a §2.4：追加 3 条全局对账等式（E1/E2/E3）。
        S10a §8.3 防护 3：树完整性（`cross_process=True`）**仅跨进程恢复启用**，
        单进程保持原语义。
        """
        with self._lock:
            for n in self._nodes.values():
                self._check_local(n)
                if n.subtree_reserved != n.self_reserved + sum(
                        c.subtree_reserved for c in n.children):
                    raise InvariantError(f"subtree mismatch at {n.node_id}")
                active_amount = 0
                nonactive_consumed = 0
                consumed_sum = 0
                for r in n.reservations.values():
                    consumed_sum += r.consumed
                    if r.state == "ACTIVE":
                        active_amount += r.amount
                    else:
                        nonactive_consumed += r.consumed
                if n.self_reserved != (active_amount + nonactive_consumed
                                       + n.folded_consumed):
                    raise InvariantError(f"self_reserved mismatch at {n.node_id}")
                if n.consumed != consumed_sum + n.folded_consumed:
                    raise InvariantError(f"consumed mismatch at {n.node_id}")
            self._global_equations()
            if cross_process:
                self._tree_integrity()
