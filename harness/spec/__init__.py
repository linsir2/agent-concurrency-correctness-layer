"""L2 规范四件套（阶段二交付物）—— 对应设计文档 §18。

- `resources`：资源模型（FILE / SYMBOL / CONTRACT / INVARIANT / TYPE_GRAPH / BUDGET_NODE / EFFECT）
- `protocol`：协调协议（GRANT / WAIT / DIE）
- `receipt`：收据格式（hash-chain + signature）
- `invariants`：conformance suite 的不变量语料（I1–I14）

**规范与实现分离**：本包只定义契约，不依赖 `harness` 实现（可被任何实现引用）。
"""

from .resources import RESOURCE_KINDS, ResourceKind, ResourceSpec, resource_kind
from .protocol import Grant, Wait, Die, decide
from .receipt import (GENESIS_HASH, Receipt, build_receipt, receipt_hash,
                      verify_chain, verify_chain_detailed)
from .invariants import INVARIANTS, INVARIANT_IDS

__all__ = [
    "RESOURCE_KINDS", "ResourceKind", "ResourceSpec", "resource_kind",
    "Grant", "Wait", "Die", "decide",
    "GENESIS_HASH", "Receipt", "build_receipt", "receipt_hash", "verify_chain",
    "verify_chain_detailed",
    "INVARIANTS", "INVARIANT_IDS",
]
