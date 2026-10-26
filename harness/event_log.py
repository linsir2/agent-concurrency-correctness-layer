"""EventLog —— 文档 §5.6 / §4.5。

B3：只有 EventLog 能分配全局 `seq`（单写者）。
本实现用内存列表；生产实现可换 WAL/Postgres（见 `docs/milestones.md` §1.3）。
"""

from __future__ import annotations

import threading
from typing import Callable, Iterable, Optional

from .types import Event, EventKind


class EventLog:
    def __init__(self) -> None:
        self._events: list[Event] = []
        self._seq = 0
        self._lock = threading.RLock()
        self._subscribers: list[Callable[[Event], None]] = []
        self._by_subject: dict[str, list[Event]] = {}

    def append(self, kind: EventKind, subject: str, payload=None,
               producer: str = "", caused_by: str = "",
               idempotency_key: str = "") -> Event:
        """分配全局单调 seq（B3）。"""
        with self._lock:
            self._seq += 1
            ev = Event(seq=self._seq, kind=kind, subject=subject, payload=payload,
                       producer=producer, caused_by=caused_by,
                       idempotency_key=idempotency_key)
            self._events.append(ev)
            self._by_subject.setdefault(subject, []).append(ev)
        self._notify(ev)
        return ev

    def append_in_txn(self, kind: EventKind, subject: str, payload=None,
                      producer: str = "", caused_by: str = "",
                      idempotency_key: str = "") -> Event:
        """与资源替换同事务（I8 前半句）。

        本实现中 `WorkspaceManager.apply` 在持锁期间调用本方法，
        因此「资源替换」与「事件追加」在同一临界区内完成。
        """
        return self.append(kind, subject, payload, producer, caused_by, idempotency_key)

    def subscribe(self, fn: Callable[[Event], None]) -> None:
        self._subscribers.append(fn)

    def _notify(self, ev: Event) -> None:
        for fn in list(self._subscribers):
            fn(ev)

    def replay(self, subject: Optional[str] = None, from_seq: int = 0) -> Iterable[Event]:
        """按 `(resource, version)` 单调投递（文档 §5.6）。"""
        source = self._by_subject.get(subject, []) if subject else self._events
        return [e for e in source if e.seq >= from_seq]

    def has(self, subject: str, kind: EventKind) -> bool:
        """供 ReconcileLoop 对账（文档 §5.2.1）。"""
        return any(e.kind == kind for e in self._by_subject.get(subject, []))

    def tail(self) -> int:
        return self._seq

    def all(self) -> list[Event]:
        return list(self._events)
