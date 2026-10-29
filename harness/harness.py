"""兼容入口：组合根已迁至 `harness/kernel/kernel.py`（S5-d）。

保留本模块是为了不破坏既有导入面：
    from harness.harness import Harness, EpochStore, ReceiptScanFailed
    from harness.harness import create_production_harness, _MAX_INLINE_DEPTH
以及 `tests/*` 里对 `harness.harness` 的引用。
"""

from __future__ import annotations

from .kernel.dispatch import _MAX_INLINE_DEPTH
from .kernel.kernel import EpochStore, Harness, create_production_harness
from .kernel.receipts import ReceiptScanFailed

__all__ = [
    "EpochStore",
    "Harness",
    "ReceiptScanFailed",
    "_MAX_INLINE_DEPTH",
    "create_production_harness",
]
