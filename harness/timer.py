"""S24-11：DurableTimer —— 期限写入事件日志，重启后可重建并触发。

**要解决什么**：现有 `tick()` 只扫描**内存里**的 deadline（HITL 超时、aging），
进程崩溃后这些期限只剩 checkpoint 里的零散字段，既没有统一索引也没有
「已触发」标记。本模块把定时器做成**事件溯源**：

- `TIMER_SET` / `TIMER_FIRED` / `TIMER_CANCELLED` 与业务事件写同一个 EventLog
  （WAL 模式下同一帧 = 原子）；
- 恢复时 `rebuild()` 按 seq 重放 → 状态回到崩溃前；
- `fire_due(now)` **只负责标记**并返回到期记录，handler 由宿主幂等执行
  （与 EffectJournal 的约定一致，避免「标记前执行」的重复副作用）。

**不引后台线程**：仍由 `Harness.tick()` 驱动，时钟可注入（S24-10 确定性兼容）。

**边界**：v1 不做分布式分片 / cron / 重复定时；内存 EventLog 不是持久后端
（`Harness(durable_timers=True)` 会拒绝）。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .types import EventKind

PENDING = "PENDING"
FIRING = "FIRING"
FIRED = "FIRED"
CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class TimerRecord:
    """一个定时器的不可变快照。"""

    timer_id: str
    fire_at_ms: int
    owner: str = ""
    kind: str = "generic"
    payload: dict | None = None
    run_id: str = ""
    state: str = PENDING
    fired_at_ms: int | None = None

    def as_dict(self) -> dict:
        return {"timer_id": self.timer_id, "fire_at_ms": self.fire_at_ms,
                "owner": self.owner, "kind": self.kind,
                "payload": self.payload, "run_id": self.run_id,
                "state": self.state, "fired_at_ms": self.fired_at_ms}


class DurableTimer:
    """基于 EventLog 的持久定时器索引（单写者、无后台线程）。"""

    def __init__(self, event_log, clock, *, run_id: str = "") -> None:
        self._log = event_log
        self._clock = clock
        self._run_id = str(run_id)
        self._lock = threading.RLock()
        self._items: dict[str, TimerRecord] = {}
        self._watermark_ms = 0
        self._subscribe()
        self.rebuild()

    # ---- 事件索引 -------------------------------------------------
    def _subscribe(self) -> None:
        fn = getattr(self._log, "subscribe", None)
        if callable(fn):
            fn(self._on_event)

    def _on_event(self, ev) -> None:
        if ev.kind in (EventKind.TIMER_SET, EventKind.TIMER_FIRED,
                       EventKind.TIMER_CANCELLED):
            self._apply(ev.kind, ev.payload or {})

    def rebuild(self, events=None) -> int:
        """从事件日志重建状态（幂等；返回已索引的定时器数）。"""
        with self._lock:
            self._items = {}
            self._watermark_ms = 0
            source = events if events is not None else self._log.all()
            for ev in source:
                if ev.kind in (EventKind.TIMER_SET, EventKind.TIMER_FIRED,
                               EventKind.TIMER_CANCELLED):
                    self._apply(ev.kind, ev.payload or {})
            return len(self._items)

    def _apply(self, kind: EventKind, payload: dict) -> None:
        tid = str(payload.get("timer_id") or "")
        if not tid:
            return
        cur = self._items.get(tid)
        if kind is EventKind.TIMER_SET:
            if cur is not None:
                # 已有记录（FIRED/CANCELLED 或重复 SET）：以先到的 SET 为准
                return
            self._items[tid] = TimerRecord(
                timer_id=tid, fire_at_ms=int(payload.get("fire_at_ms", 0) or 0),
                owner=str(payload.get("owner", "")),
                kind=str(payload.get("kind", "generic")),
                payload=payload.get("payload"),
                run_id=str(payload.get("run_id", "")), state=PENDING)
            return
        if cur is None:
            return
        if kind is EventKind.TIMER_FIRED and cur.state in (PENDING, FIRING):
            fired_at = int(payload.get("fired_at_ms", 0) or 0)
            self._watermark_ms = max(self._watermark_ms, fired_at)
            self._items[tid] = TimerRecord(
                timer_id=tid, fire_at_ms=cur.fire_at_ms, owner=cur.owner,
                kind=cur.kind, payload=cur.payload, run_id=cur.run_id,
                state=FIRED, fired_at_ms=fired_at)
        elif kind is EventKind.TIMER_CANCELLED and cur.state in (PENDING, FIRING):
            self._items[tid] = TimerRecord(
                timer_id=tid, fire_at_ms=cur.fire_at_ms, owner=cur.owner,
                kind=cur.kind, payload=cur.payload, run_id=cur.run_id,
                state=CANCELLED, fired_at_ms=cur.fired_at_ms)

    # ---- 写 -------------------------------------------------------
    def schedule(self, timer_id: str, fire_at_ms: int, *, owner: str = "",
                 kind: str = "generic", payload: dict | None = None,
                 run_id: str = "") -> TimerRecord:
        """登记/重复登记一个定时器。

        同 id 且同 `fire_at_ms` → **幂等**返回既有记录；同 id 不同期限或已终结
        → `ValueError`（必须换新 id，绝不静默改期）。
        """
        tid = str(timer_id or "")
        if not tid:
            raise ValueError("timer_id 不能为空")
        fire_at = int(fire_at_ms)
        if fire_at < 0:
            raise ValueError("fire_at_ms 不能为负")
        with self._lock:
            cur = self._items.get(tid)
            if cur is not None:
                if cur.state == PENDING and cur.fire_at_ms == fire_at:
                    return cur
                raise ValueError(
                    f"timer {tid!r} 已存在（state={cur.state}, "
                    f"fire_at={cur.fire_at_ms}），不得改期/复活")
            draft = {"timer_id": tid, "fire_at_ms": fire_at,
                     "owner": str(owner), "kind": str(kind),
                     "payload": payload, "run_id": str(run_id or self._run_id)}
            self._log.append(EventKind.TIMER_SET, tid, draft,
                             run_id=draft["run_id"])
            if tid not in self._items:       # 无订阅者时兜底
                self._apply(EventKind.TIMER_SET, draft)
            return self._items[tid]

    def cancel(self, timer_id: str, *, reason: str = "") -> bool:
        """取消一个**尚未触发**的定时器；已触发/已取消 → False。"""
        tid = str(timer_id or "")
        with self._lock:
            cur = self._items.get(tid)
            if cur is None or cur.state != PENDING:
                return False
            draft = {"timer_id": tid, "reason": str(reason)}
            self._log.append(EventKind.TIMER_CANCELLED, tid, draft,
                             run_id=cur.run_id)
            if self._items.get(tid, cur).state == PENDING:
                self._apply(EventKind.TIMER_CANCELLED, draft)
            return True

    def fire_due(self, now_ms: int | None = None) -> list[TimerRecord]:
        """把 `fire_at <= now` 的 PENDING 定时器标记为 FIRED，返回其记录。

        **只标记、不执行 handler**：宿主拿到返回值后自行幂等处理（崩溃后
        重放不会重复触发；漏执行由宿主的下一次 `tick`/对账兜底）。
        """
        now = int(self._clock.now_ms() if now_ms is None else now_ms)
        # 时钟回拨保护：已触发过的时间不会「复活」
        now = max(now, self._watermark_ms)
        with self._lock:
            due = sorted(
                (rec for rec in self._items.values()
                 if rec.state == PENDING and rec.fire_at_ms <= now),
                key=lambda r: (r.fire_at_ms, r.timer_id))
            for rec in due:
                self._items[rec.timer_id] = TimerRecord(
                    timer_id=rec.timer_id, fire_at_ms=rec.fire_at_ms,
                    owner=rec.owner, kind=rec.kind, payload=rec.payload,
                    run_id=rec.run_id, state=FIRING)
        fired: list[TimerRecord] = []
        for rec in due:
            draft = {"timer_id": rec.timer_id, "fired_at_ms": now}
            try:
                self._log.append(EventKind.TIMER_FIRED, rec.timer_id, draft,
                                 run_id=rec.run_id)
            except Exception:
                with self._lock:              # append 失败 → 回到 PENDING
                    self._items[rec.timer_id] = rec
                raise
            with self._lock:
                self._items[rec.timer_id] = TimerRecord(
                    timer_id=rec.timer_id, fire_at_ms=rec.fire_at_ms,
                    owner=rec.owner, kind=rec.kind, payload=rec.payload,
                    run_id=rec.run_id, state=FIRED, fired_at_ms=now)
            fired.append(self._items[rec.timer_id])
        return fired

    # ---- 查询 -----------------------------------------------------
    def get(self, timer_id: str) -> TimerRecord | None:
        with self._lock:
            return self._items.get(str(timer_id))

    def pending(self) -> list[TimerRecord]:
        with self._lock:
            return sorted((r for r in self._items.values() if r.state == PENDING),
                          key=lambda r: (r.fire_at_ms, r.timer_id))

    def due(self, now_ms: int | None = None) -> list[TimerRecord]:
        now = int(self._clock.now_ms() if now_ms is None else now_ms)
        return [r for r in self.pending() if r.fire_at_ms <= now]

    def next_deadline_ms(self) -> int | None:
        pending = self.pending()
        return pending[0].fire_at_ms if pending else None
