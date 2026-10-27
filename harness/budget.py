"""BudgetManager —— 文档 `s4-budget-and-admission.md` v1.1 §3/§4。

核心语义：
- `limits` 是**累计预算**：`consumed` 是永久账，`release`/`settle` 只归还「未消耗部分」；
- `subtree_reserved` 显式维护「自身 + 全部后代」占用，沿祖先链传播（O(depth)）；
- `reserve` 返回 `Reservation` 句柄，`release(reservation_id)` **幂等**；
- 单写者锁，检查与扣减同临界区，避免「check + deduct」非原子超支。
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass

from .types import Result, Status


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
        parent: "BudgetNode | None" = None,
        owner: str = "",
    ) -> None:
        self.node_id = node_id
        self.limits = limits
        self.self_reserved = 0        # 本节点持有的预留总额（含已消耗）
        self.subtree_reserved = 0     # 本节点 + 全部后代（显式维护）
        self.consumed = 0             # 已确认消耗（永久账）
        self.folded_consumed = 0      # 归档子树折入的永久账（无对应预留条目）
        self.children: list["BudgetNode"] = []
        self.reservations: dict[str, Reservation] = {}
        self.owner = owner
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


class BudgetManager:
    def __init__(self, run_limits: int = 1000, tombstone_ttl_s: float = 86400.0) -> None:
        self.root = BudgetNode("run", run_limits)
        self._lock = threading.RLock()
        self._nodes: dict[str, BudgetNode] = {"run": self.root}
        # 全局索引：rid -> (node, resv)；生命周期长于节点（tombstone）
        self._reservations: dict[str, tuple[BudgetNode, Reservation]] = {}
        self._tombstone_ttl_s = tombstone_ttl_s

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
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
    # 预留 / 释放 / 结算
    # ------------------------------------------------------------------ #
    def reserve(self, node_id: str, amount: int) -> Reservation | None:
        if not self._valid_amount(amount):
            return None
        with self._lock:
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
            resv = Reservation(uuid.uuid4().hex, node_id, amount)
            node.reservations[resv.reservation_id] = resv
            self._reservations[resv.reservation_id] = (node, resv)
            self._check_local(node)
            return resv

    def release(self, reservation_id: str) -> int:
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
            resv.state = "RELEASED"
            self._check_local(node)
            return freed

    def settle(self, reservation_id: str) -> int:
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
            self._check_local(node)
            return freed

    def consume(self, reservation_id: str, amount: int = 1) -> Result:
        if not self._valid_amount(amount):
            return Result(Status.INVALID_INPUT, {"reason": "bad_amount", "amount": amount})
        with self._lock:
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
            self._check_local(node)
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
        self, node_id: str, limits: int, parent_id: str = "run"
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
            node = BudgetNode(node_id, limits, parent)
            self._nodes[node_id] = node
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
            now = int(time.time())
            for n in subtree:
                for rid in list(n.reservations):
                    resv = n.reservations[rid]
                    resv.state = "ARCHIVED"
                    resv.expires_at = now + int(self._tombstone_ttl_s)
                self._nodes.pop(n.node_id, None)
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
    # 不变量（K-14 / §5.4）
    # ------------------------------------------------------------------ #
    def _check_local(self, node: BudgetNode) -> None:
        """O(1) 单节点断言（热路径默认开）。"""
        if not (0 <= node.consumed <= node.self_reserved
                <= node.subtree_reserved <= node.limits):
            raise InvariantError(f"local invariant broken at {node.node_id}")

    def check_invariants(self) -> None:
        """O(N) 全树校验（只在 recover 后 / debug 调用）。不用 `assert`。

        I5c 必须与 `settle` / `release` / `archive` 的语义自洽：
        - ACTIVE 条目按 `amount` 计入 `self_reserved`（含未消耗余额）；
        - SETTLED / RELEASED / ARCHIVED 条目只保留 `consumed`（余额已归还）；
        - `consumed` 是永久账，等于全部条目的 `consumed` 之和；
        - 归档子树折入的永久账记在 `folded_consumed`（无对应预留条目）。
        等价于 `self_reserved == consumed + Σ(ACTIVE balance)`。
        """
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
