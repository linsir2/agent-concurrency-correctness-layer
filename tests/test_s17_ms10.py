"""S17 / MS10：生产化（`docs/s17-boundaries-and-ms10.md` §1.7）。

含 MS10 出口条件的三条**命名**用例（真断言，不是改名空壳）：
`test_ingress_idempotent` / `test_admin_pause_resume` / `test_dlq`。
"""

from __future__ import annotations

import pytest
from conftest import admit, decision, finish, make_harness, seed

from harness import EventKind, Harness, Status, TaskState
from harness.observability import (
    MemorySpanSink,
    OTelUnavailable,
    event_spans,
    export_metrics,
    export_spans,
    make_otel_sink,
)


def _plan(h: Harness, run_id: str = "R1", task_id: str = "T1") -> dict:
    return h.planner.plan(run_id, [{"task_id": task_id, "min_budget": 10}])


def _admitted(h: Harness, run_id: str = "R1", task_id: str = "T1"):
    plan = _plan(h, run_id, task_id)
    h.plans[run_id] = plan
    return admit(h, run_id, plan, task_id)


# ==================================================== MS10 出口：Ingress
def test_ingress_idempotent():
    """**MS10 出口条件**：重复用户请求不重复建 Run（IETF Idempotency-Key）。"""
    h = make_harness()
    plan = _plan(h, "R1")
    first = h.ingress.submit(idempotency_key="k1", plan=plan)
    second = h.ingress.submit(idempotency_key="k1", plan=plan)
    assert first.status is Status.OK and first.replayed is False
    assert second.status is Status.OK and second.replayed is True
    assert second.run_id == first.run_id
    # 只写了一条受理事件（= 只建了一个 Run）
    accepted = [e for e in h.event_log.all()
                if e.kind is EventKind.INGRESS_ACCEPTED]
    assert len(accepted) == 1


# ==================================================== MS10 出口：Admin
def test_admin_pause_resume():
    """**MS10 出口条件**：pause 阻止调度但保留资源；resume 后可执行。"""
    h = make_harness(run_limits=100)
    acb = _admitted(h)
    slot_before = set(h.slots.active_keys())

    assert h.pause_run("R1") is True
    h.scheduler.push(acb.id)
    h.dispatch.drain_scheduler()
    assert acb.state is TaskState.ADMITTED                     # 未执行
    assert (acb.id, "run_paused") in h.dispatch.skipped
    assert set(h.slots.active_keys()) == slot_before           # 资源保留

    assert h.resume_run("R1") is True
    st = h.run_task(acb, [decision(1, finish())])
    assert st is Status.OK and acb.state is TaskState.DONE


# ==================================================== MS10 出口：DLQ
def test_dlq():
    """**MS10 出口条件**：**毒任务**（连续失败达阈值）自动进 `DEAD_LETTER`。

    负对照：未达阈值不得进 DLQ（避免误杀）。
    """
    h = make_harness(run_limits=100, poison_after_failures=2)
    acb = _admitted(h)

    # 第 1 次隔离 → 计数 1，未达阈值 → 仍是 QUARANTINED
    acb.state = TaskState.QUARANTINED
    acb.consecutive_failures = 0
    h.tasks.finalize(acb)
    assert acb.consecutive_failures == 1
    assert acb.state is TaskState.QUARANTINED, "未达阈值不应进 DLQ（负对照）"

    # 第 2 次 → 达阈值 → DEAD_LETTER + POISON_TASK
    h.tasks.finalize(acb)
    assert acb.consecutive_failures >= 2
    assert acb.state is TaskState.DEAD_LETTER
    assert acb.failure == "POISON_TASK"
    # 资源已结算（终态）
    assert acb.id not in h.slots.active_keys()
    h.budget.check_invariants()


def test_dlq_negative_control_zero_threshold_disables():
    """`poison_after_failures=0` → 关闭毒任务识别（回退纯 replan 预算语义）。"""
    h = make_harness(run_limits=100, poison_after_failures=0)
    acb = _admitted(h)
    acb.state = TaskState.QUARANTINED
    acb.consecutive_failures = 10
    h.tasks.finalize(acb)
    assert acb.state is TaskState.QUARANTINED, "阈值 0 应禁用毒任务识别"


def test_dlq_redrive_after_poison():
    """毒任务进 DLQ 后仍可**显式** redrive（有界），且旧 ACB 保持终态。"""
    h = make_harness(run_limits=100, poison_after_failures=1)
    acb = _admitted(h)
    acb.state = TaskState.QUARANTINED
    h.tasks.finalize(acb)
    assert acb.state is TaskState.DEAD_LETTER

    fresh = h.redrive_dead_letter(acb)
    assert fresh is not None and fresh.redrive_count == 1
    assert acb.state is TaskState.DEAD_LETTER        # 历史结论不被改写


# ==================================================== 优雅排空
def test_drain_completes_when_all_terminal():
    """全部终态 → `drain` 立即返回 `timed_out=False`。"""
    h = make_harness(run_limits=100)
    acb = _admitted(h)
    h.run_task(acb, [decision(1, finish())])

    out = h.drain(timeout_s=1.0)
    assert out["timed_out"] is False
    assert out["remaining"] == 0
    assert out["drained"] >= 1


def test_drain_times_out_without_killing():
    """有未调度任务 → 超时返回 `timed_out=True`，且**不销毁**状态（可继续）。"""
    h = make_harness(run_limits=100)
    acb = _admitted(h)                       # ADMITTED 但从不调度

    out = h.drain(timeout_s=0.05)
    assert out["timed_out"] is True
    assert out["remaining"] == 1
    assert acb.state is TaskState.ADMITTED, "drain 不得强杀（与 kill 的区别）"

    # 之后仍可正常执行（状态未丢）
    st = h.run_task(acb, [decision(1, finish())])
    assert st is Status.OK and acb.state is TaskState.DONE


def test_drain_hook_is_idempotent():
    """重复 drain 安全（不抛、结果一致）。"""
    h = make_harness(run_limits=100)
    assert h.drain(timeout_s=0.01)["remaining"] == 0
    assert h.drain(timeout_s=0.01)["remaining"] == 0


# ==================================================== Admin kill / purge
def test_kill_run_cancels_and_settles():
    """`kill_run`：立即 CANCELLED + 资源结算（与 pause 的「保留」相反）。"""
    h = make_harness(run_limits=100)
    acb = _admitted(h)

    killed = h.kill_run("R1")
    assert killed == 1
    assert acb.state is TaskState.CANCELLED
    assert acb.failure == "KILLED_BY_ADMIN"
    assert acb.id not in h.slots.active_keys()
    assert h.budget.root.subtree_reserved == 0
    h.budget.check_invariants()


def test_purge_run_requires_terminal_and_keeps_events():
    """`purge_run`：仅全终态可清内存；**不删历史事件**（保审计）。"""
    h = make_harness(run_limits=100)
    acb = _admitted(h)

    assert h.purge_run("R1") is False, "存在未终态任务时不得 purge"

    h.run_task(acb, [decision(1, finish())])
    n_before = len(h.event_log.all())
    assert h.purge_run("R1") is True

    assert acb.id not in h.acbs
    assert "R1" not in h.plans
    # 事件只增不减（新增 RUN_PURGED）
    assert len(h.event_log.all()) == n_before + 1
    assert any(e.kind is EventKind.RUN_PURGED for e in h.event_log.all())


# ==================================================== 可观测
def test_event_spans_maps_key_events():
    """`event_spans` 只映射关键事件，且字段完整。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")                  # 产生 WORKSPACE_COMMITTED
    acb = _admitted(h)
    h.run_task(acb, [decision(1, finish())])

    spans = event_spans(h.event_log.all())
    names = {s["name"] for s in spans}
    assert "harness.commit" in names or "harness.receipt" in names, f"spans={names}"
    for s in spans:
        assert {"name", "trace_id", "span_id", "seq", "subject",
                "attributes"} <= set(s)


def test_export_observability_to_memory_sink():
    """`export_observability` 把 span + metric 推给 sink（零依赖）。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    acb = _admitted(h)
    h.run_task(acb, [decision(1, finish())])

    sink = MemorySpanSink()
    n = h.export_observability(sink)
    assert n > 0
    assert len(sink.spans) == n
    assert sink.metrics, "metrics 也应导出"
    assert all(m[2].get("type") in ("counter", "gauge") for m in sink.metrics)


def test_export_metrics_counts_match():
    """导出的 counter 值与 `metrics()` 一致（防硬编码）。"""
    sink = MemorySpanSink()
    export_metrics(sink, {"counters": {"X": 3}, "gauges": {"Y": 2}})
    assert ("X", 3.0, {"type": "counter"}) in sink.metrics
    assert ("Y", 2.0, {"type": "gauge"}) in sink.metrics


def test_otel_adapter_fails_closed_without_sdk():
    """未装 `opentelemetry` → `make_otel_sink()` 抛 `OTelUnavailable`（不静默丢指标）。"""
    try:
        import opentelemetry  # noqa: F401
        pytest.xfail("本环境已安装 opentelemetry，无法验证 fail-closed 分支")
    except ImportError:
        pass
    with pytest.raises(OTelUnavailable):
        make_otel_sink()


def test_export_spans_counts():
    """`export_spans` 返回推送条数 = 关键事件数。"""
    h = make_harness(run_limits=100)
    acb = _admitted(h)
    h.run_task(acb, [decision(1, finish())])
    sink = MemorySpanSink()
    n = export_spans(sink, h.event_log.all())
    assert n == len(sink.spans)
