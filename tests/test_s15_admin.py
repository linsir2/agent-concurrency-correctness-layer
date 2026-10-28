"""S15 D2/D3/D4：DLQ redrive、Admin pause/resume、可观测快照。

对应 `docs/s15-protocol-and-production.md` §3.2 / §3.3 / §3.4。
"""

from __future__ import annotations

from conftest import admit, decision, finish, make_harness

from harness import EventKind, Harness, ManualClock, Status, TaskState


def _plan(h: Harness, run_id: str = "R1") -> dict:
    return h.planner.plan(run_id, [{"task_id": "T1", "min_budget": 10}])


def _aging_harness(*, max_redrives: int = 3, **kw):
    """带 aging 的 Harness（`make_harness` 不透传 aging 参数）。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    h.aging_ms = kw.pop("aging_ms", 100)
    h.aging_max_attempts = kw.pop("aging_max_attempts", 1)
    h.max_redrives = max_redrives
    for k, v in kw.items():
        setattr(h, k, v)
    return h, clock


def _to_dead_letter(h: Harness, run_id: str = "R1"):
    """用**真实路径**（aging 耗尽）产出 DEAD_LETTER，而不是手工改状态。"""
    plan = _plan(h, run_id)
    h.plans[run_id] = plan
    acb = admit(h, run_id, plan, "T1")
    assert acb.aging_deadline is not None
    h.pump(now_ms=acb.aging_deadline)
    assert acb.state is TaskState.DEAD_LETTER, (
        f"前提未满足：aging 耗尽应到 DEAD_LETTER，实际 {acb.state}")
    return acb, plan


# ===================================================================== D2
def test_d2_redrive_dead_letter_creates_runnable_acb():
    """`DEAD_LETTER` → redrive 造**新** ACB（`ADMITTED`），旧 ACB 仍为终态。"""
    h, _ = _aging_harness()
    acb = _to_dead_letter(h)[0]
    old_state = acb.state

    fresh = h.redrive_dead_letter(acb)

    assert fresh is not None
    assert fresh.state is TaskState.ADMITTED
    assert fresh.id != acb.id
    assert fresh.redrive_count == 1
    # 旧 ACB 的结论**不被改写**（仍是终态，不是 CANCELLED）
    assert acb.state is old_state is TaskState.DEAD_LETTER
    # 审计事件
    redriven = [e for e in h.event_log.all() if e.kind is EventKind.DLQ_REDRIVEN]
    assert len(redriven) == 1
    assert redriven[0].payload["to_acb_id"] == fresh.id
    h.budget.check_invariants()


def test_d2_redrive_acb_can_actually_run():
    """**自评审 R2**：redrive 出的 ACB 必须能真执行（token 身份/预算/槽位都对）。

    只断言 `state is ADMITTED` 是不够的：S13b 曾出现「replan 复用旧 token →
    `agent_id` 不匹配 → 100% `DENIED_CAPABILITY`」，当时被 `"*"` 夹具掩盖。
    """
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    h.aging_ms = 100
    h.aging_max_attempts = 1
    h.max_redrives = 3
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.pump(now_ms=acb.aging_deadline)
    assert acb.state is TaskState.DEAD_LETTER

    fresh = h.redrive_dead_letter(acb)
    assert fresh is not None

    st = h.run_task(fresh, [decision(1, finish())])
    assert st is Status.OK, f"redrive 出的 ACB 无法执行：{st}"
    assert fresh.state is TaskState.DONE
    assert fresh.id not in h.slots.active_keys()
    h.budget.check_invariants()


def test_d2_redrive_is_bounded():
    """`redrive_count >= max_redrives` → 拒绝（有界，对齐 SQS redrive）。"""
    h, _ = _aging_harness(max_redrives=1)
    acb = _to_dead_letter(h)[0]

    first = h.redrive_dead_letter(acb)
    assert first is not None and first.redrive_count == 1

    # 第一个 redrive 出来的 ACB 再次耗尽 → DEAD_LETTER
    h.pump(now_ms=first.aging_deadline)
    if first.state is not TaskState.DEAD_LETTER:
        h.aging_max_attempts = 1
        h.pump(now_ms=(first.aging_deadline or h.clock.now_ms()) + 10_000)
    assert first.state is TaskState.DEAD_LETTER

    # 已达上限 → 拒绝
    assert h.redrive_dead_letter(first) is None
    assert (first.id, "redrive_limit_exceeded") in h._drain_skipped


def test_d2_redrive_rejects_non_dead_letter():
    """非 `DEAD_LETTER` 一律拒绝（不误用：不制造状态机暗门）。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    assert h.redrive_dead_letter(acb) is None

    acb.state = TaskState.DONE
    assert h.redrive_dead_letter(acb) is None


# ===================================================================== D3
def test_d3_pause_blocks_scheduling_but_keeps_resources():
    """pause 后 `_drain_scheduler` 不跑该 run，但**槽位/预算/节点保留**。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")

    slot_before = set(h.slots.active_keys())
    reserved_before = h.budget.root.subtree_reserved
    node_before = acb.budget_node

    assert h.pause_run("R1") is True
    h.scheduler.push(acb.id)
    h._drain_scheduler()

    # 未执行
    assert acb.state is TaskState.ADMITTED
    assert (acb.id, "run_paused") in h._drain_skipped
    # 资源保留（这是 pause 与 cancel 的核心区别）
    assert set(h.slots.active_keys()) == slot_before
    assert h.budget.root.subtree_reserved == reserved_before
    assert acb.budget_node is node_before


def test_d3_resume_allows_execution():
    """resume 后正常调度执行。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")

    h.pause_run("R1")
    assert h.resume_run("R1") is True
    assert h.is_paused("R1") is False

    st = h.run_task(acb, [decision(1, finish())])
    assert st is Status.OK and acb.state is TaskState.DONE


def test_d3_pause_is_idempotent_and_reports_transition():
    """重复 pause/resume 返回 `False`（表示未发生状态变化，不重复写事件）。"""
    h = make_harness()
    assert h.pause_run("R1") is True
    assert h.pause_run("R1") is False
    assert h.resume_run("R1") is True
    assert h.resume_run("R1") is False

    paused = [e for e in h.event_log.all() if e.kind is EventKind.RUN_PAUSED]
    resumed = [e for e in h.event_log.all() if e.kind is EventKind.RUN_RESUMED]
    assert len(paused) == 1 and len(resumed) == 1


def test_d3_pause_survives_restart():
    """**持久化**：新 Harness 共享事件流 → recover 后仍暂停。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.pause_run("R1", reason="维护")
    ref = h.checkpoints.save(acb)          # 内存 store 不会自动落盘
    acb.result_ref = ref

    h2 = Harness(event_log=h.event_log, checkpoints=h.checkpoints,
                 clock=h.clock)
    h2.policy.default_token = h2.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    h2.recover(ref)

    assert h2.is_paused("R1") is True
    assert h2.paused_runs() == ["R1"]


def test_d3_resume_after_restart():
    """恢复后 resume 也生效（事件顺序覆盖：PAUSED → RESUMED）。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.pause_run("R1")
    h.resume_run("R1")
    acb.result_ref = h.checkpoints.save(acb)

    h2 = Harness(event_log=h.event_log, checkpoints=h.checkpoints,
                 clock=h.clock)
    h2.recover(acb.result_ref)
    assert h2.is_paused("R1") is False


def test_d3_submit_command_routes_pause():
    """`submit_command` 的 `PAUSE_RUN` / `RESUME_RUN` 真路由，且**幂等**。"""
    h = make_harness()
    assert h.run_coordinator.submit_command(
        {"kind": "PAUSE_RUN", "run_id": "R9"}) is Status.OK
    assert h.is_paused("R9")
    # 自评审 R1：Admin 命令幂等 —— 重复 pause 仍是 OK（目标态已达成），不是错误
    assert h.run_coordinator.submit_command(
        {"kind": "PAUSE_RUN", "run_id": "R9"}) is Status.OK
    assert h.run_coordinator.submit_command(
        {"kind": "RESUME_RUN", "run_id": "R9"}) is Status.OK
    assert not h.is_paused("R9")
    assert h.run_coordinator.submit_command(
        {"kind": "RESUME_RUN", "run_id": "R9"}) is Status.OK
    # 缺参才是错误
    assert h.run_coordinator.submit_command(
        {"kind": "PAUSE_RUN", "run_id": ""}) is Status.INVALID_INPUT


# ===================================================================== D4
def test_d4_metrics_counts_match_events():
    """`metrics()["counters"]` 与真实事件计数一致。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.run_task(acb, [decision(1, finish())])

    m = h.metrics()
    actual: dict[str, int] = {}
    for ev in h.event_log.all():
        name = ev.kind.value if hasattr(ev.kind, "value") else str(ev.kind)
        actual[name] = actual.get(name, 0) + 1
    assert m["counters"] == actual
    assert m["gauges"]["event_log_tail"] == h.event_log.tail()


def test_d4_metrics_negative_control_new_event_increments():
    """负对照：多写一条事件 → 对应计数 +1（证明计数不是硬编码）。"""
    h = make_harness()
    before = h.metrics()["counters"].get(EventKind.RUN_PAUSED.value, 0)
    h.pause_run("R1")
    after = h.metrics()["counters"].get(EventKind.RUN_PAUSED.value, 0)
    assert after == before + 1


def test_d4_metrics_gauges_reflect_state():
    """gauges 反映当前状态（非终态/暂停/死信/等待审批）。"""
    h, _ = _aging_harness()
    _to_dead_letter(h)

    m = h.metrics()["gauges"]
    assert m["dead_letter"] >= 1
    assert m["acbs_nonterminal"] == sum(
        1 for a in h.acbs.values() if a.state not in
        (TaskState.DONE, TaskState.FAILED, TaskState.CANCELLED,
         TaskState.DEAD_LETTER, TaskState.DONE_PARTIAL))

    h.pause_run("R1")
    assert h.metrics()["gauges"]["paused_runs"] == 1


def test_d4_metrics_is_read_only():
    """`metrics()` 不改变任何状态（可随时调用）。"""
    h = make_harness(run_limits=100)
    plan = _plan(h)
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")

    tail0 = h.event_log.tail()
    state0 = acb.state
    for _ in range(3):
        h.metrics()
    assert h.event_log.tail() == tail0
    assert acb.state is state0
