"""跨进程 `flock` 原语 + 锁全序（S10b §3.1 / §3.2 / S11b §3.2）。

`_FileLock` 从 `checkpoint.py` **下沉**到此，解开循环导入：

    checkpoint → event_log → storage/wal → storage/_lock（**无环**）

若 `wal.py` 反向 `from ..checkpoint import _FileLock`，则
`checkpoint → event_log → wal → checkpoint` 成环 → `ImportError`。

跨进程锁的**全序**（写死，禁止反向获取，否则成环死锁，S10b §3.2 / S11b §3.2）：

    resource(0) → budget(1) → receipts(2) → wal(3) → epoch(4) → store(5) → eventlog(6)

`resource` / `eventlog._lock` 为**进程内**锁（无跨进程语义）；其余为跨进程
`flock`，按上表**偏序**获取。反向获取（如「持 `store.lock` 取 `epoch` 锁」）
会让两条路径互相等待成环 → 死锁；`_FileLock(order=...)` 在运行期把这种反向
获取变成显式 `LockOrderViolation`。

S11b K-46：`epoch` 为**独立**锁（`wal.log.epoch`），`bump_epoch` 只取该锁、
**不与其它锁嵌套**；WAL append 的复验路径按 wal(3) → epoch(4) 正序获取。
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path

# 全序常量（下标越小越先获取）。测试断言此顺序不可更改。
LOCK_ORDER = ("resource", "budget", "receipts", "wal", "epoch", "store", "eventlog")
BUDGET_ORDER = 1
RECEIPTS_ORDER = 2
WAL_ORDER = 3
EPOCH_ORDER = 4
STORE_ORDER = 5
EVENTLOG_ORDER = 6


class LockOrderViolation(RuntimeError):
    """反向获取跨进程锁（持高序锁又取低序锁 → 会成环死锁）。"""


_tls = threading.local()


def _stack() -> list[int]:
    stack = getattr(_tls, "order_stack", None)
    if stack is None:
        stack = []
        _tls.order_stack = stack
    return stack


def _push(order: int) -> None:
    stack = _stack()
    if stack and order < stack[-1]:
        raise LockOrderViolation(
            f"锁序违规：持 {LOCK_ORDER[stack[-1]]} 又取 {LOCK_ORDER[order]}"
            f"（全序 {' → '.join(LOCK_ORDER)}，禁止反向获取）")
    stack.append(order)


def _pop(order: int) -> None:
    stack = _stack()
    if stack and stack[-1] == order:
        stack.pop()


class _FileLock:
    """跨进程 `flock`；同线程可重入（`flock` 以 open file description 计）。

    `order` 非 `None` 时启用**锁序断言**：同一线程内嵌套获取必须按 `LOCK_ORDER`
    非递减，反向获取立即抛 `LockOrderViolation`（把潜在死锁变成显式失败）。
    """

    def __init__(self, path: str | Path, order: int | None = None) -> None:
        self._path = os.fspath(path)
        self._order = order
        self._local = threading.local()

    @contextmanager
    def __call__(self):
        depth = getattr(self._local, "depth", 0)
        if depth:
            self._local.depth = depth + 1
            try:
                yield
            finally:
                self._local.depth -= 1
            return

        if self._order is not None:
            _push(self._order)
        try:
            parent = os.path.dirname(os.path.abspath(self._path))
            os.makedirs(parent, exist_ok=True)
            try:
                import fcntl
            except ImportError:  # pragma: no cover - 非 POSIX 平台降级为仅线程互斥
                fcntl = None

            fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_EX)
            self._local.depth = 1
            try:
                yield
            finally:
                self._local.depth = 0
                try:
                    if fcntl is not None:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
        finally:
            if self._order is not None:
                _pop(self._order)


def wal_lock_path(path: str | Path) -> str:
    """WAL 锁文件路径（与 WAL 同目录，文件名加 `.lock` 后缀）。"""
    return os.fspath(path) + ".lock"


def wal_epoch_path(path: str | Path) -> str:
    """WAL epoch 文件路径（S11b K-46：与 WAL 同目录，`wal.log.epoch`）。"""
    return os.fspath(path) + ".epoch"


def wal_epoch_lock_path(path: str | Path) -> str:
    """独立 epoch 锁文件路径（不与 wal.lock / store.lock 共用）。"""
    return os.fspath(path) + ".epoch.lock"


def wal_generation_path(path: str | Path) -> str:
    """WAL generation 标记路径（终审 7-3：WAL 重建后旧写者必须被拒）。"""
    return os.fspath(path) + ".generation"


def receipts_lock_path(root: str | Path) -> str:
    """receipts（scope）锁文件路径（checkpoint root 下）。"""
    return os.path.join(os.fspath(root), "receipts.lock")


class FencingRejected(Exception):
    """写者 epoch 落后于存储 epoch → 拒绝写入（不写入、不 ack，S10b §3.4）。"""
