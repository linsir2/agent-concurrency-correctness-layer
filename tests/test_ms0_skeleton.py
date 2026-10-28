"""MS0 / S0 出口：骨架、schema 定稿、状态机表驱动、可注入时钟。

对应方案 §8 S0 判据：
- `Event` 全字段（D6）
- `append_batch` 连续 seq（多资源原子提交的提交点）
- `Result` 带载 + 兼容 `== Status`
- `TRANSITIONS` 表驱动 + 非法转移拒绝（D10）
- Task 级 / Run 级终态分离
- `ManualClock` 可控时间
"""

from __future__ import annotations

import pytest

from harness import (
    ACB,
    RUN_TERMINAL,
    TASK_TERMINAL,
    EventKind,
    EventLog,
    ManualClock,
    Result,
    RunState,
    Status,
    TaskState,
    advance,
)

pytestmark = pytest.mark.ms0


def _acb(state: TaskState = TaskState.PLANNED) -> ACB:
    return ACB(id="A", run_id="R", task_id="T", state=state)


def test_event_has_full_schema():
    log = EventLog(clock=ManualClock(12345))
    ev = log.append(EventKind.RESOURCE_COMMITTED, "r", {"version": 1}, run_id="R1")

    assert ev.event_id and len(ev.event_id) == 32
    assert ev.run_id == "R1"
    assert ev.ts == 12345
    assert ev.schema_version == 1
    assert ev.seq == 1


def test_append_batch_assigns_contiguous_seq():
    log = EventLog()
    events = log.append_batch([
        {"kind": EventKind.RESOURCE_COMMITTED, "subject": "a", "run_id": "R"},
        {"kind": EventKind.RESOURCE_COMMITTED, "subject": "b", "run_id": "R"},
        {"kind": EventKind.RESOURCE_COMMITTED, "subject": "c", "run_id": "R"},
    ])
    assert [e.seq for e in events] == [1, 2, 3]
    assert len({e.event_id for e in events}) == 3
    assert log.tail() == 3


def test_result_carries_detail_and_compares_to_status():
    r = Result(Status.CONFLICT, {"resource": "a", "version": 2, "diff": {"k": 1}})
    assert r == Status.CONFLICT
    assert r != Status.OK
    assert r.value == "CONFLICT"
    assert r.detail["version"] == 2


def test_advance_follows_table():
    acb = _acb()
    assert advance(acb, "try_admit") == Status.OK
    assert acb.state is TaskState.ADMITTED
    assert advance(acb, "instantiate") == Status.OK
    assert acb.state is TaskState.INSTANTIATING
    assert advance(acb, "spawn") == Status.OK
    assert acb.state is TaskState.READY


def test_advance_rejects_illegal_transition():
    acb = _acb()
    r = advance(acb, "schedule")          # PLANNED 不能直接 schedule
    assert r == Status.INVALID_INPUT
    assert r.detail["from"] == "PLANNED"
    assert acb.state is TaskState.PLANNED


def test_advance_cancel_from_running():
    acb = _acb(TaskState.RUNNING)
    assert advance(acb, "cancel") == Status.OK
    assert acb.state is TaskState.CANCELLED


def test_advance_cancel_after_terminal_is_rejected():
    acb = _acb(TaskState.DONE)
    assert advance(acb, "cancel") == Status.INVALID_INPUT
    assert acb.state is TaskState.DONE


def test_task_and_run_terminal_sets_are_separate():
    # S13c C3：父汇总的 DONE_PARTIAL 是**任务级终态**（否则预算永不归档、
    # Run 永不 finalize）；表 B 的 RunState 仍独立。
    assert TaskState.DONE_PARTIAL in TASK_TERMINAL
    assert RunState.DONE_PARTIAL in RUN_TERMINAL
    assert TaskState.DEAD_LETTER in TASK_TERMINAL


def test_manual_clock_advances_only_explicitly():
    c = ManualClock(1000)
    assert c.now_ms() == 1000
    c.advance(500)
    assert c.now_ms() == 1500
    with pytest.raises(ValueError):
        c.advance(-1)
