"""S14B · MS8 I12 补强：**不饿死**（starvation freedom）。

MS8 文档自评 I12 为「弱：只断言存在非终态任务，未验证不饿死」。
原用例 `test_i12_nonterminal_task_not_finalized` 只验证「非终态 → Run 不 finalize」。

本文件补上真正的方向：**一个迟迟得不到调度的任务，必须有界地离开「等待」**
（aging → INADMISSIBLE → readmit 或 DEAD_LETTER），不能永久停在 ADMITTED。
对应 `docs/s14-boundaries-and-ms8-closure.md` §3.1 / S14-10。
"""

from __future__ import annotations

from conftest import admit, make_harness

from harness import ManualClock, TaskState


def _aging_harness(**kw):
    """构造带 aging 配置的 Harness（`make_harness` 未透传 aging 参数）。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    for key, value in kw.items():
        setattr(h, key, value)
    return h, clock


def test_s14_10_starved_task_leaves_admitted_within_bounded_pumps():
    """**I12 不饿死**：不被调度 → aging 有界推进，最终到终态（不是永远 ADMITTED）。"""
    h, _clock = _aging_harness(aging_ms=100, aging_max_attempts=2)
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    deadline = acb.aging_deadline
    assert deadline is not None, "准入后必须起算 aging（否则本用例测不到）"

    # 未到期：仍是 ADMITTED（不能提前踢掉）
    h.pump(now_ms=deadline - 1)
    assert acb.state is TaskState.ADMITTED

    # 第 1 次到期 → INADMISSIBLE（非终态，但**离开**了 ADMITTED）
    h.pump(now_ms=deadline)
    assert acb.state is TaskState.INADMISSIBLE, (
        f"饿死：仍停在 {acb.state}（aging 未生效）")
    assert acb.failure == "AGING_TIMEOUT"

    # readmit → 重新可运行；再次饿死 → 第 2 次达上限 → DEAD_LETTER（真终态）
    fresh = h.readmit_after_aging(acb)
    assert fresh.state in (TaskState.PLANNED, TaskState.ADMITTED,
                           TaskState.INADMISSIBLE, TaskState.DEAD_LETTER)

    if fresh.state is not TaskState.DEAD_LETTER:
        d2 = fresh.aging_deadline
        assert d2 is not None
        h.pump(now_ms=d2)
    assert fresh.state is TaskState.DEAD_LETTER, (
        f"有界 N 次后未到终态：{fresh.state}（I12 不饿死被破坏）")
    assert fresh.failure == "AGING_EXHAUSTED"
    h.budget.check_invariants()


def test_s14_10_negative_control_without_aging_stays_admitted():
    """负对照：`aging_ms=0`（禁用）→ 该任务**永远**停在 ADMITTED（证明上面靠 aging）。"""
    h, clock = _aging_harness(aging_ms=0)
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")
    assert acb.aging_deadline is None, "禁用时不应起算 aging"

    for _ in range(5):
        clock.advance(1000)
        h.pump()
    assert acb.state is TaskState.ADMITTED, (
        "禁用 aging 后仍被踢出 —— 说明不是 aging 起的作用")


def test_s14_10_aging_disabled_is_opt_in_not_default():
    """默认配置（不传 `aging_ms`）**不得**让 ADMITTED 任务被踢出（向后兼容）。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")
    assert h.aging_ms == 0
    assert acb.aging_deadline is None
    assert h.pump() == 0
    assert acb.state is TaskState.ADMITTED
