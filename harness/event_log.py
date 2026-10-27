"""EventLog —— 文档 §5.6 / §4.5。

B3：只有 EventLog 能分配全局 `seq`（单写者）。

两种实现共用同一契约：
- `EventLog`：内存版（测试默认，零 I/O）
- `WalEventLog`：append-only WAL 版（`S1` 交付）。**提交点 = 帧写入并 fdatasync 成功**，
  多资源写在同一帧内 → 一帧要么全生效要么全不生效（方案 §5.1）。
"""

from __future__ import annotations

import threading
import uuid
from typing import Callable, Iterable, Optional

from .clock import Clock, SystemClock
from .storage._lock import WAL_ORDER, _FileLock, wal_lock_path
from .storage.wal import Wal
from .types import Event, EventKind


def _draft(kind, subject, payload, producer, caused_by, idempotency_key,
           run_id, clock: Clock) -> dict:
    return {
        "kind": kind.value if isinstance(kind, EventKind) else str(kind),
        "subject": subject,
        "payload": payload,
        "producer": producer,
        "caused_by": caused_by,
        "idempotency_key": idempotency_key,
        "run_id": run_id,
        "event_id": uuid.uuid4().hex,
        "ts": clock.now_ms(),
        "schema_version": 1,
    }


def _event(rec: dict) -> Event:
    raw_kind = rec["kind"]
    try:
        kind = EventKind(raw_kind)
    except ValueError:
        kind = raw_kind
    return Event(
        seq=rec["seq"],
        kind=kind,
        subject=rec["subject"],
        payload=rec.get("payload"),
        producer=rec.get("producer", ""),
        caused_by=rec.get("caused_by", ""),
        idempotency_key=rec.get("idempotency_key", ""),
        event_id=rec.get("event_id", ""),
        run_id=rec.get("run_id", ""),
        ts=rec.get("ts", 0),
        schema_version=rec.get("schema_version", 1),
    )


class _EventLogBase:
    def __init__(self, clock: Optional[Clock] = None) -> None:
        self._clock = clock or SystemClock()
        self._events: list[Event] = []
        self._by_subject: dict[str, list[Event]] = {}
        self._subscribers: list[Callable[[Event], None]] = []
        self._close_hooks: list[Callable[[], None]] = []

    def on_close(self, fn: Callable[[], None]) -> None:
        """注册关闭钩子（写者会话结束：释放单写者租约等）。"""
        self._close_hooks.append(fn)

    def _run_close_hooks(self) -> None:
        for fn in list(self._close_hooks):
            try:
                fn()
            except Exception:  # noqa: BLE001  # 关闭钩子不得阻断关闭
                pass

    def _index(self, ev: Event) -> Event:
        self._events.append(ev)
        self._by_subject.setdefault(ev.subject, []).append(ev)
        return ev

    def _notify(self, events: list[Event]) -> None:
        for ev in events:
            for fn in list(self._subscribers):
                fn(ev)

    def subscribe(self, fn: Callable[[Event], None]) -> None:
        self._subscribers.append(fn)

    def notify_events(self, events: list[Event]) -> None:
        self._notify(events)

    def replay(self, subject: Optional[str] = None, from_seq: int = 0) -> Iterable[Event]:
        """按 `(resource, version)` 单调投递（文档 §5.6）。"""
        source = self._by_subject.get(subject, []) if subject else self._events
        return [e for e in source if e.seq >= from_seq]

    def has(self, subject: str, kind: EventKind) -> bool:
        """供 ReconcileLoop 对账（文档 §5.2.1）。"""
        return any(e.kind == kind for e in self._by_subject.get(subject, []))

    def tail(self) -> int:
        return self._events[-1].seq if self._events else 0

    def all(self) -> list[Event]:
        return list(self._events)

    # 子类必须实现 append / append_batch
    def append(self, kind, subject: str, payload=None, producer: str = "",
               caused_by: str = "", idempotency_key: str = "", *,
               run_id: str = "", notify: bool = True) -> Event:
        raise NotImplementedError

    def append_batch(self, drafts: list[dict], notify: bool = True) -> list[Event]:
        raise NotImplementedError

    def append_in_txn(self, kind, subject: str, payload=None, producer: str = "",
                      caused_by: str = "", idempotency_key: str = "", *,
                      run_id: str = "") -> Event:
        return self.append(kind, subject, payload, producer, caused_by,
                           idempotency_key, run_id=run_id)


class EventLog(_EventLogBase):
    """内存实现（默认）。"""

    def __init__(self, clock: Optional[Clock] = None) -> None:
        super().__init__(clock)
        self._lock = threading.RLock()
        self._seq = 0

    def close(self) -> None:
        self._run_close_hooks()

    def append(self, kind, subject: str, payload=None, producer: str = "",
               caused_by: str = "", idempotency_key: str = "", *,
               run_id: str = "", notify: bool = True) -> Event:
        with self._lock:
            self._seq += 1
            rec = _draft(kind, subject, payload, producer, caused_by,
                         idempotency_key, run_id, self._clock)
            rec["seq"] = self._seq
            ev = self._index(_event(rec))
        if notify:
            self._notify([ev])
        return ev

    def append_batch(self, drafts: list[dict], notify: bool = True) -> list[Event]:
        events: list[Event] = []
        with self._lock:
            for d in drafts:
                self._seq += 1
                rec = _draft(d["kind"], d["subject"], d.get("payload"),
                             d.get("producer", ""), d.get("caused_by", ""),
                             d.get("idempotency_key", ""), d.get("run_id", ""),
                             self._clock)
                rec["seq"] = self._seq
                events.append(self._index(_event(rec)))
        if notify:
            self._notify(events)
        return events


class WalEventLog(_EventLogBase):
    """WAL 实现：`append_frame` 成功返回即已落盘。

    S10b §3.2/§3.3/§3.4：
    - `wal.lock`（跨进程 flock）覆盖 append 的「读-分配-写-fdatasync」全程；
    - `eventlog._lock` 是全序末位（进程内 RLock，仅护内存索引）；
    - fencing：`set_fencing(...)` 注入 store_flock + epoch 读口 + 本写者 epoch，
      append 前**同一 flock 内**复验（`Wal.append_frame`）。
    """

    def __init__(self, path, clock: Optional[Clock] = None,
                 fsync: bool = True) -> None:
        super().__init__(clock)
        # eventlog._lock：全序末位（进程内，无跨进程语义）
        self._lock = threading.RLock()
        # wal.lock：全序第 2 位（resource → receipts → wal → store → eventlog）
        self._wal_lock = _FileLock(wal_lock_path(path), order=WAL_ORDER)
        self._wal = Wal(path, fsync=fsync, lock=self._wal_lock)
        # S10b §3.4：fencing 注入点（由 Harness 在组装后设置；默认关闭）
        self._fencing_lock = None
        self._fencing_reader = None
        self._epoch_provider = None
        for rec in self._wal.records():
            self._index(_event(rec))

    def set_fencing(self, *, lock, epoch_reader, epoch_provider) -> None:
        """注入 fencing 三件套（S10b §3.4）。

        - `lock`：store_flock（与 `bump_epoch` 共用同一把 `store.lock`）；
        - `epoch_reader`：读 store.json 的 run 级 epoch；
        - `epoch_provider`：返回**本写者**当前 run 级 epoch（用于复验比较）。
        """
        self._fencing_lock = lock
        self._fencing_reader = epoch_reader
        self._epoch_provider = epoch_provider

    @property
    def _fencing_enabled(self) -> bool:
        return self._fencing_lock is not None and self._fencing_reader is not None

    @property
    def truncated_at(self) -> Optional[int]:
        return self._wal.truncated_at

    def append(self, kind, subject: str, payload=None, producer: str = "",
               caused_by: str = "", idempotency_key: str = "", *,
               run_id: str = "", notify: bool = True) -> Event:
        return self.append_batch([{
            "kind": kind, "subject": subject, "payload": payload,
            "producer": producer, "caused_by": caused_by,
            "idempotency_key": idempotency_key, "run_id": run_id,
        }], notify=notify)[0]

    def append_batch(self, drafts: list[dict], notify: bool = True) -> list[Event]:
        prepared = [
            _draft(d["kind"], d["subject"], d.get("payload"),
                   d.get("producer", ""), d.get("caused_by", ""),
                   d.get("idempotency_key", ""), d.get("run_id", ""),
                   self._clock)
            for d in drafts
        ]
        # ① wal.lock（+ 可选 store_flock）覆盖「读-分配-写-fdatasync」全程
        if self._fencing_enabled:
            fencing_epoch = int(self._epoch_provider() or 0)
            records = self._wal.append_frame(
                prepared, fencing_epoch=fencing_epoch,
                fencing_lock=self._fencing_lock,
                fencing_reader=self._fencing_reader)
        else:
            records = self._wal.append_frame(prepared)      # ← 提交点
        # ② eventlog._lock（全序末位）：仅护内存索引
        with self._lock:
            events = [self._index(_event(rec)) for rec in records]
        if notify:
            self._notify(events)
        return events

    def close(self) -> None:
        self._wal.close()
        self._run_close_hooks()
