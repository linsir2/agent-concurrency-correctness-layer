"""S24-P0：读取面纯化 —— `next_deadline_ms()`/`pump()` 不得有写副作用。

RIPR：
- Reachability：持久快照被真正水合（deadline 与 acb 一致）；
- Infection：旧实现调用 `next_deadline_ms()` 会把 `wal.log.epoch` +1 并占槽位
  （这会 fence 活跃写者）；修复后字节/槽位均不变；
- Propagation：未 `recover` 的 aging 到期项能通过 `pump()` 结算（旧实现恒 >0）；
- Revealability：持久状态落盘为 `INADMISSIBLE`，槽位释放。
"""

from __future__ import annotations

import pathlib

from harness import RscSession  # noqa: F401  （保持测试导入面与其它 S24 用例一致）
from harness.clock import ManualClock
from harness.event_log import WalEventLog
from harness.harness import Harness
from harness.types import TaskState


def _harness(root: pathlib.Path, clock: ManualClock, *, aging_ms: int = 10):
    wal = WalEventLog(str(root / "wal.log"), fsync=False, clock=clock)
    h = Harness(event_log=wal, checkpoint_root=str(root), clock=clock,
                run_limits=10, aging_ms=aging_ms)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return wal, h


def _admit(h: Harness):
    plan = h.planner.plan("run-1", [{
        "task_id": "t", "logical_subgoal_id": "g", "min_budget": 1}])
    return h.tasks.admit_and_register(plan, "t")


def _epoch(root: pathlib.Path) -> int:
    path = root / "wal.log.epoch"
    return int(path.read_text(encoding="utf-8").strip() or 0) if path.exists() else 0


def _durable_aging(tmp_path: pathlib.Path):
    """留下一个 ADMITTED 且已 arm aging 的持久快照，然后关闭写者。"""
    root = pathlib.Path(tmp_path) / "state"
    root.mkdir()
    clock = ManualClock(start_ms=1_000_000)
    wal, h = _harness(root, clock)
    acb = _admit(h)
    assert acb.state is TaskState.ADMITTED
    assert acb.aging_deadline == 1_000_010
    # 真实系统在 run_task 入口落盘（runtime.py:76）；这里等价地显式保存
    h.checkpoints.save(acb)
    wal.close()
    return root, clock, acb


# ---------------------------------------------------------------- 只读
def test_next_deadline_ms_is_read_only(tmp_path):
    root, clock, acb = _durable_aging(tmp_path)
    wal2, h2 = _harness(root, clock)
    try:
        epoch_before = _epoch(root)
        wal_before = (root / "wal.log").read_bytes()
        slots_before = set(h2.slots.active_keys())

        deadline = h2.next_deadline_ms()

        assert deadline == acb.aging_deadline
        assert _epoch(root) == epoch_before          # 不得 bump epoch
        assert (root / "wal.log").read_bytes() == wal_before
        assert set(h2.slots.active_keys()) == slots_before
    finally:
        wal2.close()


def test_pending_deadlines_is_read_only(tmp_path):
    root, clock, _acb = _durable_aging(tmp_path)
    wal2, h2 = _harness(root, clock)
    try:
        epoch_before = _epoch(root)
        h2._pending_deadlines()
        assert _epoch(root) == epoch_before
    finally:
        wal2.close()


# ---------------------------------------------------------------- 结算
def test_pump_settles_persisted_aging_without_full_recover(tmp_path):
    root, clock, acb = _durable_aging(tmp_path)
    clock.advance(20)                                 # 越过 aging_deadline
    wal2, h2 = _harness(root, clock)
    try:
        assert h2.pump(clock.now_ms()) == 0           # 收敛，不再空转
        assert acb.id not in h2.slots.active_keys()
        ref = h2.checkpoints.latest_ref_for_acb(acb.id)
        assert ref is not None
        data = h2.checkpoints.load_strict(ref)
        assert data["state"] == "INADMISSIBLE"
    finally:
        wal2.close()


def test_aging_disabled_still_reads_none(tmp_path):
    root = pathlib.Path(tmp_path) / "state"
    root.mkdir()
    clock = ManualClock(start_ms=1_000_000)
    wal, h = _harness(root, clock, aging_ms=0)
    try:
        acb = _admit(h)
        assert acb.aging_deadline is None
        assert h.next_deadline_ms() is None
    finally:
        wal.close()
