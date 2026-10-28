"""S13d：Scheduler（默认 sync / pool 单所有者 / claim+fencing / aging 接线）。

对应 `docs/s13-orchestration.md` v1.1 §2.5 与 §3 的 S13-11 / S13-12，以及
P0-9（pool 嵌套死锁）、P0-10（claim + fencing）、P1-1（INADMISSIBLE 非终态
→ Run 永不 finalize）、B-3（`CrashInjected` 被 Future 吞）、B-4（aging 死字段）。

夹具：`_pool_harness` 必须用裸 `Harness(scheduler_mode="pool")` + 通配 token
（`make_harness` 不传调度模式）；同步回归见 `tests/test_ms4_runtime.py` 等。
"""

from __future__ import annotations

import threading

import pytest
from conftest import admit, commit, decision, finish, stage, tool

import harness.runtime as runtime_module
from harness import (
    ACB,
    Action,
    EventKind,
    Harness,
    ManualClock,
    Result,
    Status,
    TaskState,
    Write,
    advance,
)
from harness.runtime import CrashInjected
from harness.storage._lock import FencingRejected
from harness.types import TASK_TERMINAL


# ---------------------------------------------------------------- helpers
def _harness(**kwargs) -> Harness:
    h = Harness(**kwargs)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def _pool_harness(**kwargs) -> Harness:
    kwargs.setdefault("scheduler_mode", "pool")
    return _harness(**kwargs)


def _tool_events(h: Harness) -> list:
    return [e for e in h.event_log.all() if e.kind.value == "TOOL_COMPLETED"]


def _admitted(h: Harness, run_id: str, plan: dict, task_id: str) -> ACB:
    acb = admit(h, run_id, plan, task_id)
    assert acb.state is TaskState.ADMITTED
    return acb


# ============================================================ S13-11 单所有者
def test_s13_11_pool_single_owner_despite_duplicate_ready_keys():
    """pool 模式：重复 ready 键（绕过 push 去重）只执行一次，claim 只 1 owner。"""
    h = _pool_harness(run_limits=100)
    plan = h.planner.plan("R11", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R11"] = plan
    acb = _admitted(h, "R11", plan, "T1")

    calls: list[str] = []
    orig_run_one = h._run_one

    def spy(acb_, decisions=None, crash_at=None):
        calls.append(acb_.id)
        return orig_run_one(acb_, decisions, crash_at=crash_at)

    h._run_one = spy
    # 绕过 `push` 去重：同一 acb.id 在队列中出现两次
    h.scheduler._ready.extend([acb.id, acb.id])
    assert h.scheduler._ready.count(acb.id) == 2

    st = h.run_task(acb, [decision(1, stage("r", "v1"),
                                   tool("t", {"p": "/x"}), commit("r"))])

    assert st == Status.OK
    assert acb.state is TaskState.DONE
    # ① `_run_one` 调用次数 == 1（单一 pop 所有者）
    assert len(calls) == 1
    # ② 该 idem_key 的 TOOL_COMPLETED == 1
    keys = [e.idempotency_key for e in _tool_events(h)]
    assert len(keys) == 1 and keys[0] == h.idem_key(acb, tool("t", {"p": "/x"}), 1)
    # ③ 收据数量 == 1
    assert len(h.receipts_of("R11", "S1")) == 1
    # ④ claim 只 1 owner（同一 worker 的重复 claim 不算第二个 owner）
    owners = {c.worker_id for c in h.scheduler.claim_history}
    assert len(owners) == 1, owners
    assert all(c.acb_id == acb.id for c in h.scheduler.claim_history)
    # 队列已被 worker（单一 pop 所有者）消费完
    assert h.scheduler.is_empty()


def test_s13_11_crash_injected_is_reraised_and_drain_is_not_rerun():
    """B-3：`CrashInjected` 必须被 `pytest.raises` 捕获，且崩溃后不再 drain（防双跑）。"""
    h = _pool_harness(run_limits=100)
    plan = h.planner.plan("R11C", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R11C"] = plan
    acb = _admitted(h, "R11C", plan, "T1")

    run_calls: list[str] = []
    drain_calls: list[str] = []
    orig_run_one = h._run_one
    orig_drain = h._drain_scheduler

    def spy_run_one(acb_, decisions=None, crash_at=None):
        run_calls.append(acb_.id)
        return orig_run_one(acb_, decisions, crash_at=crash_at)

    def spy_drain():
        drain_calls.append("drain")
        return orig_drain()

    h._run_one = spy_run_one
    h._drain_scheduler = spy_drain
    h.scheduler._ready.extend([acb.id, acb.id])

    with pytest.raises(CrashInjected):
        h.run_task(acb, [decision(1, tool("t", {"p": "/x"}), finish())],
                   crash_at=(1, 0))

    assert len(run_calls) == 1
    # 崩溃路径不的 drain → 重复 ready 键不得被二次消费（否则 TOOL_COMPLETED 会翻倍）
    assert drain_calls == []
    assert _tool_events(h) == []


def test_s13_11_negative_control_unclaimed_workers_race_two_tool_events():
    """负对照：**不 claim** 的两个 worker 并发跑同一 ACB → TOOL_COMPLETED == 2。

    两个线程在同一时刻读到「effect 尚未 CONFIRMED」（屏障保证），各自
    `begin/confirm/append` —— 证明主用例的事件计数确实对双跑敏感（idem 短路
    只能挡住**共享 journal** 的顺序重入，挡不住共享-less 的并发双跑）。
    """
    h = _harness(run_limits=100)              # 无 claim（sync 语义）
    plan = h.planner.plan("R11N", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R11N"] = plan
    acb = _admitted(h, "R11N", plan, "T1")

    entry = threading.Barrier(2)              # 两个 worker 都先越过入口判态
    barrier = threading.Barrier(2)
    orig_begin = h.effects.begin

    def racing_begin(acb_, action_, *legacy):
        # 屏障卡在 `begin`：两边都已消费预算、都还没写 EFFECT_INTENT/TOOL_COMPLETED
        try:
            barrier.wait(timeout=10)
        except threading.BrokenBarrierError:
            pass
        return orig_begin(acb_, action_, *legacy)

    h.effects.begin = racing_begin
    decisions = [decision(1, tool("t", {"p": "/x"}), finish())]
    errors: list[BaseException] = []

    def worker():
        try:
            entry.wait(timeout=10)            # 保证两边都不在入口看到终态
            h._run_one(acb, decisions)
        except BaseException as exc:          # noqa: BLE001 - 收集后断言
            errors.append(exc)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert all(not t.is_alive() for t in threads)
    assert errors == []
    assert len(_tool_events(h)) == 2


# ============================================================ S13-12 aging
def test_s13_12_aging_deadline_tick_marks_inadmissible_and_run_can_finalize():
    """墙钟 aging：deadline-1 未超时、deadline 超时 → INADMISSIBLE + 结算；
    单次超时可 readmit，累计 N 次 → DEAD_LETTER（P1-1：Run 才能 finalize）。
    """
    clock = ManualClock()
    h = _harness(run_limits=100, clock=clock, aging_ms=1000,
                 aging_max_attempts=2)
    plan = h.planner.plan("R12", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "logical_subgoal_id": "S2", "min_budget": 10}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R12"] = plan
    a1 = _admitted(h, "R12", plan, "T1")

    deadline = a1.aging_deadline
    assert deadline is not None
    assert deadline == clock.now_ms() + 1000

    h.tick(now_ms=deadline - 1)
    assert a1.state is TaskState.ADMITTED          # deadline-1 不超时
    assert a1.aging_deadline == deadline
    assert a1.reservation_ids                     # 未结算：资源仍由本 ACB 持有

    h.tick(now_ms=deadline)
    assert a1.state is TaskState.INADMISSIBLE      # 墙钟到期 → 非终态
    assert a1.failure == "AGING_TIMEOUT"
    assert a1.aging_count == 1
    # 结算：租约 / 预留 / 预算节点 / 槽位
    assert not [l for l in h.intents._leases.values() if l.valid]
    assert a1.reservation_ids == [] and a1.budget_node is None
    assert a1.id not in h.slots.active_keys()
    assert h.budget.root.subtree_reserved == 0
    assert a1.id not in h._admission_index
    h.budget.check_invariants()

    # P1-1 ①：单次超时可 readmit（INADMISSIBLE → PLANNED → 重新准入）
    fresh = h.readmit_after_aging(a1)
    assert fresh is not a1
    assert fresh.state is TaskState.ADMITTED
    assert fresh.aging_count == 1                  # 计数随新 ACB 继承
    assert fresh.aging_deadline == clock.now_ms() + 1000
    h.budget.check_invariants()

    # P1-1 ②：累计 N 次 → DEAD_LETTER（真终态）→ 后继取消 → Run 可 finalize
    h.tick(now_ms=fresh.aging_deadline)
    assert fresh.state is TaskState.DEAD_LETTER
    assert fresh.failure == "AGING_EXHAUSTED"
    assert fresh.aging_count == 2
    assert fresh.reservation_ids == [] and fresh.budget_node is None
    states = {tid: task["state"] for tid, task in h.plans["R12"]["tasks"].items()}
    assert states["T1"] is TaskState.DEAD_LETTER
    assert states["T2"] is TaskState.CANCELLED     # 后继被传播取消
    assert all(s in TASK_TERMINAL for s in states.values())
    assert all(a.state in TASK_TERMINAL for a in h.acbs.values())
    h.budget.check_invariants()


def test_s13_12_aging_disabled_still_runs_and_scan_is_wired():
    """负对照：`aging_ms=0`（禁用）→ 仍正常执行；且 `tick → scan_deadlines`
    在 `run_task` 期间至少被调用一次（触发器有效性，不是空承诺）。
    """
    clock = ManualClock()
    h = _harness(run_limits=100, clock=clock, aging_ms=0)
    plan = h.planner.plan("R12D", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R12D"] = plan
    acb = _admitted(h, "R12D", plan, "T1")
    assert acb.aging_deadline is None

    scans: list[int] = []
    orig_scan = h.reconcile.scan_deadlines
    h.reconcile.scan_deadlines = (
        lambda now, **kw: (scans.append(int(now)), orig_scan(now, **kw))[1])

    clock.advance(10_000)                           # 远超任何 aging 阈值
    st = h.run_task(acb, [decision(1, finish())])
    assert st == Status.OK
    assert acb.state is TaskState.DONE
    assert len(scans) >= 1


# ============================================================ S13-13 嵌套
def test_s13_13_worker_nested_run_task_inlines_no_pool_deadlock(monkeypatch):
    """P0-9：worker 内 `run_task(child)` 必须**内联**（池只有 1 个 worker，
    若入队则父等子 → 永久死锁）。用 `join(timeout)` + `is_alive()` 断言。
    """
    h = _pool_harness(run_limits=100, pool_workers=1)
    plan = h.planner.plan("R13", [
        {"task_id": "P", "logical_subgoal_id": "SP", "min_budget": 10},
        {"task_id": "C", "logical_subgoal_id": "SC", "min_budget": 10}])
    h.plans["R13"] = plan
    parent = _admitted(h, "R13", plan, "P")
    child = _admitted(h, "R13", plan, "C")

    nested_calls: list[bool] = []

    def nested(rt, acb_, action, i):
        nested_calls.append(bool(getattr(rt.h._tls, "in_worker", False)))
        return rt.h.run_task(child, [decision(1, finish())])

    monkeypatch.setitem(runtime_module._HANDLERS, "nested_child", nested)

    results: list = []

    def outer():
        results.append(h.run_task(
            parent, [decision(1, Action(kind="nested_child"), finish())]))

    thread = threading.Thread(target=outer, daemon=True)
    thread.start()
    thread.join(timeout=10)                       # 不是 sleep：明确的超时上界
    assert not thread.is_alive(), "worker 内嵌套 run_task 必须内联（池满不得死锁）"
    assert nested_calls == [True]
    assert results and results[0] == Status.OK
    assert child.state is TaskState.DONE
    assert parent.state is TaskState.DONE


def test_s13_x4_inline_child_is_claimed_by_parent_worker(monkeypatch):
    """X4：内联子必须 claim（以父 worker 身份）——否则队列可重 claim → 双跑。"""
    h = _pool_harness(run_limits=100, pool_workers=1)
    plan = h.planner.plan("RX4", [
        {"task_id": "P", "logical_subgoal_id": "SP", "min_budget": 10},
        {"task_id": "C", "logical_subgoal_id": "SC", "min_budget": 10}])
    h.plans["RX4"] = plan
    parent = _admitted(h, "RX4", plan, "P")
    child = _admitted(h, "RX4", plan, "C")

    def nested(rt, acb_, action, i):
        return rt.h.run_task(child, [decision(1, finish())])

    monkeypatch.setitem(runtime_module._HANDLERS, "nested_child_x4", nested)

    results: list = []

    def outer():
        results.append(h.run_task(
            parent, [decision(1, Action(kind="nested_child_x4"), finish())]))

    thread = threading.Thread(target=outer, daemon=True)
    thread.start()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert results and results[0] == Status.OK
    assert child.state is TaskState.DONE

    child_claims = [c for c in h.scheduler.claim_history if c.acb_id == child.id]
    assert child_claims, "内联子必须持有 claim（否则可被队列重 claim → 双跑）"
    assert child_claims[-1].worker_id.startswith("W")
    assert child.spec.get("inline_parent") == parent.id
    assert h.scheduler.claim_of(child.id) is None     # 内联结束即释放


# ============================================================ S13-14 重 claim
def test_s13_14_expired_claim_reclaim_fences_old_writer_and_keeps_one_effect():
    """P0-10：claim 过期 → 第二 worker 可重 claim；旧 worker 的写被 fencing 拒；
    副作用仍只 1 次（同一 idem_key 短路）。
    """
    clock = ManualClock()
    h = _pool_harness(run_limits=100, clock=clock, lease_ms=400,
                      claim_lease_ms=500)
    plan = h.planner.plan("R14", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R14"] = plan
    acb = _admitted(h, "R14", plan, "T1")
    action = tool("t", {"p": "/x"})

    c1 = h.scheduler.claim(acb.id, "w1")
    assert c1 is not None and c1.worker_id == "w1" and c1.attempt == 1
    # 租约有效 → 第二个 worker 不得抢（防双持）
    assert h.scheduler.claim(acb.id, "w2") is None

    # 旧 worker 的副作用已完成（模拟「工具已确认」后崩溃）。
    # X1：fence epoch 绑在**执行上下文**（此处用 `claim_scope` 模拟 worker
    # 线程），不再写回共享 ACB（旧用例的 `copy.copy(acb)` 已删除 —— 那会掩盖
    # 「旧 worker 与主线程持同一对象」的共享缺陷）。
    with h.claim_scope(acb, c1):
        st = h.runtime._do_action(acb, action, 0)
    assert st == Status.OK
    assert len(_tool_events(h)) == 1

    clock.advance(501)                            # 越过 claim 租约
    c2 = h.scheduler.claim(acb.id, "w2")          # 重 claim：原子 supervisor_note_crash
    assert c2 is not None and c2.worker_id == "w2"
    assert c2.attempt == 2
    assert c2.fence_epoch > c1.fence_epoch

    # 旧 worker（旧 epoch 上下文）的写一律 REJECT_STALE
    # （commit / budget.consume / effects.begin / checkpoints.save）
    with h.claim_scope(acb, c1.fence_epoch):
        acb.staged = {"r": "old"}
        assert h.commit_batch(acb, [Write(resource="r")]) == Status.REJECT_STALE
        assert h.budget.consume(acb.reservation_ids[0], 1,
                                acb=acb) == Status.REJECT_STALE
        assert h.effects.begin(acb, action) == ""
        with pytest.raises(FencingRejected):
            h.checkpoints.save(acb)
    acb.staged = {}

    # 新 worker 接手执行 → idem 短路：TOOL_COMPLETED 仍只 1 次
    h.scheduler.release_claim(acb.id, "w2")
    st = h.run_task(acb, [decision(1, tool("t", {"p": "/x"}), finish())])
    assert st == Status.OK
    assert acb.state is TaskState.DONE
    assert len(_tool_events(h)) == 1
    assert h.budget._reservations[acb.reservation_ids[0]][1].consumed == 1


# ============================================================ X5 / X7 / X8
def test_s13_x5_replan_write_ahead_fenced_rolls_back_memory_count():
    """X5：replan 的 write-ahead `save` 被 fence → 内存 `replan_count` 必须回滚，
    且不产生 `PLAN_PATCHED`；`replan_attempts` 仍永不回滚（熔断风暴）。"""
    clock = ManualClock()
    h = _pool_harness(run_limits=100, clock=clock, lease_ms=400,
                      claim_lease_ms=500)
    plan = h.planner.plan("RX5", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["RX5"] = plan
    acb = _admitted(h, "RX5", plan, "T1")

    c1 = h.scheduler.claim(acb.id, "w1")
    acb.state = TaskState.QUARANTINED       # replan 允许的入口态
    clock.advance(501)
    c2 = h.scheduler.claim(acb.id, "w2")
    assert c2 is not None and c2.fence_epoch > c1.fence_epoch

    patched_before = [e for e in h.event_log.all()
                      if e.kind is EventKind.PLAN_PATCHED]
    with h.claim_scope(acb, c1.fence_epoch), pytest.raises(FencingRejected):
        h.replan(acb, plan, diagnosis={"failing_subgoals": ["S1"]})

    assert acb.replan_count == 0                       # 内存回滚
    assert acb.replan_attempts == 1                    # 尝试数永不回滚
    assert [e for e in h.event_log.all()
            if e.kind is EventKind.PLAN_PATCHED] == patched_before
    assert (acb.id, "save_fenced") in h._drain_skipped


def test_s13_x7_claim_fields_persisted_and_expired_lease_released_on_restore():
    """X7：claim 三字段随快照落盘；租约过期 → 恢复即释放、允许重 claim。"""
    clock = ManualClock()
    h = _pool_harness(run_limits=100, clock=clock, lease_ms=400,
                      claim_lease_ms=500)
    plan = h.planner.plan("RX7", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["RX7"] = plan
    acb = _admitted(h, "RX7", plan, "T1")

    c1 = h.scheduler.claim(acb.id, "w1")
    with h.claim_scope(acb, c1):
        ref = h.checkpoints.save(acb)
    data = h.checkpoints.load(ref)
    assert data["claim_epoch"] == c1.fence_epoch
    assert data["claim_worker"] == "w1"
    assert data["claim_lease_until_ms"] == c1.lease_until_ms

    clock.advance(501)                                 # 越过 claim 租约
    restored = h._restore_acb(h.checkpoints.load(ref))
    assert restored.claim_epoch == 0
    assert restored.claim_worker == ""
    assert restored.claim_lease_until_ms == 0
    assert (restored.id, "claim_lease_expired_released") in h._drain_skipped


# ============================================================ X1 跨波次（终审）
def test_s13_x1_tick_timeout_on_inflight_worker_stays_failed_not_done(monkeypatch):
    """X1：pool + 在途 worker + 主线程 `tick` 判审批超时。

    终态必须是 `FAILED(APPROVAL_TIMEOUT)` —— 不得被解锁后的 worker 改回 DONE、
    不得 append `TASK_COMPLETED`；旧 worker 解锁后的写一律 `REJECT_STALE`
    （commit / effects.begin）。
    """
    clock = ManualClock()
    h = _pool_harness(run_limits=100, clock=clock, hitl_timeout_ms=100)
    plan = h.planner.plan("R15", [{
        "task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R15"] = plan
    acb = _admitted(h, "R15", plan, "T1")

    entered = threading.Event()
    release = threading.Event()
    observed: dict = {}

    def blocked_hitl(rt, acb_, action, i):
        # 模拟生产 HITL：进入等待审批后**阻塞**，期间主线程 tick 判超时。
        assert advance(acb_, "request_approval") == Status.OK
        acb_.hitl_pending = {"approval_id": "ap-x1",
                             "deadline_ms": clock.now_ms() + 100,
                             "plan_version": 1}
        rt.h.checkpoints.save(acb_)
        entered.set()
        assert release.wait(10), "tick 未在超时内放行"
        # 解锁后：旧 worker 的写必须被 fence（绝不写副作用/事件）。
        acb_.staged = {"r": "old"}
        observed["commit"] = rt.h.commit_batch(acb_, [Write(resource="r")])
        observed["begin"] = rt.h.effects.begin(acb_, tool("t", {"p": "/x"}))
        observed["state_after_resume"] = acb_.state
        return Result(Status.OK)

    monkeypatch.setitem(runtime_module._HANDLERS, "blocked_hitl_x1", blocked_hitl)

    results: list = []

    def outer():
        results.append(h.run_task(acb, [decision(1, Action(kind="blocked_hitl_x1"))]))

    thread = threading.Thread(target=outer, daemon=True)
    thread.start()
    assert entered.wait(10)
    clock.advance(200)                            # 越过 hitl_timeout_ms
    h.tick()                                      # 主线程判超时（与 worker 并发）
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"
    assert (acb.hitl_outcome or {}).get("decision") == "timeout"

    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert results == [Status.APPROVAL_TIMEOUT]   # worker 返回终态语义，不是 OK/DONE
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"
    assert observed["state_after_resume"] is TaskState.FAILED
    assert observed["commit"] == Status.REJECT_STALE
    assert observed["begin"] == ""
    assert [e for e in h.event_log.all()
            if e.kind is EventKind.TASK_COMPLETED] == []
