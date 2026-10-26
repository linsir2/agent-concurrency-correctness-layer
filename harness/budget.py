"""BudgetManager —— 文档 `s4-budget-and-admission.md` v1.1 §3/§4。

核心语义：
- `limits` 是**累计预算**：`consumed` 是永久账，`release`/`settle` 只归还「未消耗部分」；
- `subtree_reserved` 显式维护「自身 + 全部后代」占用，沿祖先链传播（O(depth)）；
- `reserve` 返回 `Reservation` 句柄，`release(reservation_id)` **幂等**；
- 单写者锁，检查与扣减同临界区，避免「check + deduct」非原子超支。
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass

from .types import Result, Status


@dataclass
class Reservation:
    """预算预留句柄（`release`/`settle` 幂等）。"""

    reservation_id: str      # uuid4().hex（不可枚举，防他组件误释放）
    node_id: str
    amount: int
    consumed: int = 0
    state: str = "ACTIVE"    # ACTIVE | SETTLED | RELEASED | EXPIRED

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
    def __init__(self, run_limits: int = 1000) -> None:
        self.root = BudgetNode("run", run_limits)
        self._lock = threading.RLock()
        self._nodes: dict[str, BudgetNode] = {"run": self.root}
        # 全局索引：rid -> (node, resv)；生命周期长于节点（tombstone）
        self._reservations: dict[str, tuple[BudgetNode, Reservation]] = {}

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
    # 不变量
    # ------------------------------------------------------------------ #
    def check_invariants(self) -> None:
        for n in self._nodes.values():
            # I5a：消耗 ≤ 自身预留 ≤ 子树占用 ≤ 上限
            assert 0 <= n.consumed <= n.self_reserved <= n.subtree_reserved <= n.limits
            # I5b：子树 = 自身 + 各子子树
            assert n.subtree_reserved == n.self_reserved + sum(
                c.subtree_reserved for c in n.children
            )
            # I5c：自身预留 = 已消耗 + 全部 ACTIVE 预留余额
            active_balance = sum(
                r.balance for r in n.reservations.values() if r.state == "ACTIVE"
            )
            assert n.self_reserved == n.consumed + active_balance
