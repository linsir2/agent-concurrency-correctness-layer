"""S24-11：DurableTimer（事件溯源、重启重建、只触发一次）。

RIPR：
- Reachability：SET/FIRED/CANCELLED 事件真的进 EventLog 并被索引；
- Infection：到期才触发、取消后不触发、重复调度拒绝、已触发的重建后不再触发；
- Propagation：`Harness.tick` 驱动 fire，`next_deadline_ms`/`pump` 收敛；
- Revealability：重启（新实例/新 Harness）后状态与崩溃前一致。
"""

from __future__ import annotations

import pathlib

import pytest
from conftest import make_harness

from harness.clock import ManualClock
from harness.event_log import EventLog, WalEventLog
from harness.harness import Harness
from harness.timer import CANCELLED, FIRED, PENDING, DurableTimer
from harness.types import EventKind


def _timer(clock: ManualClock | None = None) -> tuple[DurableTimer, EventLog, ManualClock]:
    clock = clock or ManualClock(start_ms=1_000)
    log = EventLog(clock=clock)
    return DurableTimer(log, clock), log, clock


# ---------------------------------------------------------------- 基本语义
def test_fires_only_when_due():
    timer, log, _clock = _timer()
    timer.schedule("t1", 1_500, owner="a")
    assert timer.get("t1").state == PENDING
    assert timer.fire_due(1_499) == []
    fired = timer.fire_due(1_500)
    assert len(fired) == 1 and fired[0].timer_id == "t1"
    assert timer.get("t1").state == FIRED
    assert sum(1 for e in log.all() if e.kind is EventKind.TIMER_FIRED) == 1
    assert timer.fire_due(9_999) == []          # 不重复触发


def test_cancel_prevents_fire():
    timer, log, _clock = _timer()
    timer.schedule("t1", 1_500)
    assert timer.cancel("t1", reason="not_needed") is True
    assert timer.get("t1").state == CANCELLED
    assert timer.fire_due(2_000) == []
    assert timer.cancel("t1") is False          # 已取消 → 幂等 False
    kinds = [e.kind for e in log.all()]
    assert kinds.count(EventKind.TIMER_CANCELLED) == 1
    assert kinds.count(EventKind.TIMER_FIRED) == 0


def test_schedule_is_idempotent_but_no_reschedule():
    timer, _log, _clock = _timer()
    first = timer.schedule("t1", 1_500)
    again = timer.schedule("t1", 1_500)
    assert first.timer_id == again.timer_id and again.state == PENDING
    with pytest.raises(ValueError):
        timer.schedule("t1", 1_600)             # 改期 → 拒绝
    timer.fire_due(1_500)
    with pytest.raises(ValueError):
        timer.schedule("t1", 9_999)             # 已终结 → 拒绝复活


def test_clock_rollback_does_not_refire():
    timer, _log, clock = _timer()
    timer.schedule("t1", 1_500)
    assert len(timer.fire_due(2_000)) == 1
    clock.advance(0)
    assert timer.fire_due(1_000) == []          # 回拨不复活
    assert timer.get("t1").state == FIRED


# ---------------------------------------------------------------- 持久化
def test_rebuild_from_wal_survives_restart(tmp_path):
    path = tmp_path / "wal.log"
    clock = ManualClock(start_ms=1_000)
    log = WalEventLog(str(path), fsync=False, clock=clock)
    timer = DurableTimer(log, clock)
    timer.schedule("t1", 1_800, owner="agent")
    log.close()

    clock2 = ManualClock(start_ms=1_000)
    log2 = WalEventLog(str(path), fsync=False, clock=clock2)
    timer2 = DurableTimer(log2, clock2)
    assert timer2.get("t1").state == PENDING
    assert len(timer2.fire_due(1_800)) == 1
    log2.close()

    clock3 = ManualClock(start_ms=1_000)
    log3 = WalEventLog(str(path), fsync=False, clock=clock3)
    timer3 = DurableTimer(log3, clock3)
    assert timer3.get("t1").state == FIRED
    assert timer3.fire_due(99_999) == []        # 恢复不双触发
    log3.close()


def test_durable_timers_requires_durable_log():
    with pytest.raises(ValueError):
        Harness(durable_timers=True)            # 默认内存 EventLog


def test_harness_tick_drives_and_restarts(tmp_path):
    root = pathlib.Path(tmp_path) / "state"
    root.mkdir()
    clock = ManualClock(start_ms=1_000)
    log = WalEventLog(str(root / "wal.log"), fsync=False, clock=clock)
    h = Harness(event_log=log, checkpoint_root=str(root),
                timers=None, durable_timers=True)
    assert h.timers is not None
    h.set_timer("t1", 1_500, owner="supervisor")
    assert h.next_deadline_ms() == 1_500
    assert h.pump(1_400) > 0                    # 到期前仍待结算
    assert h.timers.get("t1").state == PENDING
    assert h.pump(1_500) == 0                   # tick 触发后收敛
    assert h.timers.get("t1").state == FIRED
    log.close()

    clock2 = ManualClock(start_ms=1_000)
    log2 = WalEventLog(str(root / "wal.log"), fsync=False, clock=clock2)
    h2 = Harness(event_log=log2, checkpoint_root=str(root), durable_timers=True)
    assert h2.timers.get("t1").state == FIRED
    assert h2.next_deadline_ms() is None


def test_default_off_has_no_timer_state():
    h = make_harness()
    assert h.timers is None
    h.tick()
    assert not any(e.kind in (EventKind.TIMER_SET, EventKind.TIMER_FIRED,
                              EventKind.TIMER_CANCELLED)
                   for e in h.event_log.all())
    with pytest.raises(RuntimeError):
        h.set_timer("x", 1)


def test_pump_counts_pending_timer():
    clock = ManualClock(start_ms=1_000)
    log = EventLog(clock=clock)
    timer = DurableTimer(log, clock)
    h = make_harness(clock=clock, timers=timer)
    h.set_timer("t1", 5_000)
    assert h.pump(1_000) == 1
    assert h.next_deadline_ms() == 5_000
