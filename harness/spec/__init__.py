"""L2 规范四件套（阶段二交付物）—— 对应设计文档 §18。

- `resources`：资源模型（FILE / SYMBOL / CONTRACT / INVARIANT / TYPE_GRAPH / BUDGET_NODE / EFFECT）
- `protocol`：协调协议（GRANT / WAIT / DIE）
- `receipt`：收据格式（hash-chain + signature）
- `invariants`：conformance suite 的不变量语料（I1–I14）

**规范与实现分离**：本包只定义契约，不依赖 `harness` 实现（可被任何实现引用）。
"""

from .invariants import INVARIANT_IDS, INVARIANTS
from .protocol import Die, Grant, Wait, decide
from .receipt import (
                      GENESIS_HASH,
                      Receipt,
                      build_receipt,
                      receipt_hash,
                      verify_chain,
                      verify_chain_detailed,
)
from .resources import RESOURCE_KINDS, ResourceKind, ResourceSpec, resource_kind

__all__ = [
                      "GENESIS_HASH",
                      "INVARIANTS",
                      "INVARIANT_IDS",
                      "RESOURCE_KINDS",
                      "Die",
                      "Grant",
                      "Receipt",
                      "ResourceKind",
                      "ResourceSpec",
                      "Wait",
                      "build_receipt",
                      "decide",
                      "receipt_hash",
                      "resource_kind",
                      "verify_chain",
                      "verify_chain_detailed",
]
