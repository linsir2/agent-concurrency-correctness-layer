"""BudgetManager —— 文档 §9.1。

`parent.remaining = limits − own_reserved − sum(children.reserved)`（v1.7 P0-4）
`reserve` 单写者（原子），避免「check + deduct」非原子超支。
"""

from __future__ import annotations

import threading


class BudgetNode:
    def __init__(self, node_id: str, limits: int, parent: "BudgetNode | None" = None) -> None:
        self.node_id = node_id
        self.limits = limits
        self.own_reserved = 0
        self.consumed = 0
        self.parent = parent
        self.children: list["BudgetNode"] = []
        if parent:
            parent.children.append(self)

    @property
    def children_reserved(self) -> int:
        return sum(c.own_reserved for c in self.children)

    @property
    def remaining(self) -> int:
        return self.limits - self.own_reserved - self.children_reserved

    def reserve(self, amount: int) -> bool:
        if amount < 0:
            return False
        if self.own_reserved + amount > self.limits:
            return False
        if self.parent and self.parent.remaining < amount:
            return False
        self.own_reserved += amount
        return True

    def consume(self, amount: int) -> bool:
        if self.consumed + amount > self.own_reserved:
            return False
        self.consumed += amount
        return True

    def release(self) -> int:
        """只归还未被孩子占用的部分（文档 §9.1）。"""
        freed = self.own_reserved - self.children_reserved
        self.own_reserved = 0
        self.consumed = 0
        return max(0, freed)


class BudgetManager:
    def __init__(self, run_limits: int = 1000) -> None:
        self.root = BudgetNode("run", run_limits)
        self._nodes: dict[str, BudgetNode] = {"run": self.root}
        self._lock = threading.RLock()

    def create_node(self, node_id: str, limits: int, parent_id: str = "run") -> BudgetNode:
        with self._lock:
            parent = self._nodes.get(parent_id, self.root)
            node = BudgetNode(node_id, limits, parent)
            self._nodes[node_id] = node
            return node

    def reserve(self, node_id: str, amount: int) -> bool:
        with self._lock:
            node = self._nodes.get(node_id)
            return node.reserve(amount) if node else False

    def consume(self, node_id: str, amount: int) -> bool:
        with self._lock:
            node = self._nodes.get(node_id)
            return node.consume(amount) if node else False

    def release(self, node_id: str) -> int:
        with self._lock:
            node = self._nodes.get(node_id)
            return node.release() if node else 0

    def check(self, node_id: str, amount: int = 0) -> bool:
        """执行期 guard 用的只读查询（v1.10 修复）。

        **有意义的判据**：`consumed + amount <= reserved`（否则恒真，guard 空操作）。
        """
        with self._lock:
            node = self._nodes.get(node_id)
            return bool(node and node.consumed + amount <= node.own_reserved)
