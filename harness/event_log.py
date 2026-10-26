"""EventLog —— 文档 §5.6 / §4.5。

B3：只有 EventLog 能分配全局 `seq`（单写者）。
本实现用内存列表；`S1` 会加 `WalEventLog`（同一 Protocol，落盘 + CRC + 恢复截断）。
"""

from __future__ import annotations

import threading
import uuid
from typing import Callable, Iterable, Optional

from .clock import Clock, SystemClock
from .types import Event, EventKind


class EventLog:
    def __init__(self, clock: Optional[Clock] = None) -> None:
        self._events: list[Event] = []
        self._seq = 0
        self._lock = threading.RLock()
        self._subscribers: list[Callable[[Event], None]] = []
        self._by_subject: dict[str, list[Event]] = {}
        self._clock = clock or SystemClock()

    def _build(self, kind, subject, payload, producer, caused_by,
               idempotency_key, run_id) -> Event:
        self._seq += 1
        return Event(
            seq=self._seq,
            kind=kind,
            subject=subject,
            payload=payload,
            producer=producer,
            caused_by=caused_by,
            idempotency_key=idempotency_key,
            event_id=uuid.uuid4().hex,
            run_id=run_id,
            ts=self._clock.now_ms(),
            schema_version=1,
        )

    def append(self, kind, subject: str, payload=None,
               producer: str = "", caused_by: str = "",
               idempotency_key: str = "", *, run_id: str = "") -> Event:
        """分配全局单调 seq（B3）。"""
        with self._lock:
            ev = self._build(kind, subject, payload, producer, caused_by,
                             idempotency_key, run_id)
            self._events.append(ev)
            self._by_subject.setdefault(subject, []).append(ev)
        self._notify(ev)
        return ev

    def append_batch(self, drafts: list[dict]) -> list[Event]:
        """一次临界区内分配连续 seq（多资源原子提交的提交点，方案 §5.1）。"""
        events: list[Event] = []
        with self._lock:
            for d in drafts:
                ev = self._build(
                    d["kind"], d["subject"], d.get("payload"),
                    d.get("producer", ""), d.get("caused_by", ""),
                    d.get("idempotency_key", ""), d.get("run_id", ""),
                )
                self._events.append(ev)
                self._by_subject.setdefault(ev.subject, []).append(ev)
                events.append(ev)
        for ev in events:
            self._notify(ev)
        return events

    def append_in_txn(self, kind, subject: str, payload=None,
                      producer: str = "", caused_by: str = "",
                      idempotency_key: str = "", *, run_id: str = "") -> Event:
        """与资源替换同临界区（I8 前半句；真正原子性由 S1 的 WAL 提交点保证）。"""
        return self.append(kind, subject, payload, producer, caused_by,
                           idempotency_key, run_id=run_id)

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
