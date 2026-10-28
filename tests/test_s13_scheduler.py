"""S13d：Scheduler（默认 sync / pool 单所有者 / claim+fencing / aging 接线）。

对应 `docs/s13-orchestration.md` v1.1 §2.5 与 §3 的 S13-11 / S13-12，以及
P0-9（pool 嵌套死锁）、P0-10（claim + fencing）、P1-1（INADMISSIBLE 非终态
→ Run 永不 finalize）、B-3（`CrashInjected` 被 Future 吞）、B-4（aging 死字段）。

夹具：`_pool_harness` 必须用裸 `Harness(scheduler_mode="pool")` + 通配 token
（`make_harness` 不传调度模式）；同步回归见 `tests/test_ms4_runtime.py` 等。
"""

from __future__ import annotations

import copy
import threading

import pytest

import harness.runtime as runtime_module
from conftest import admit, commit, decision, finish, stage, tool

from harness import ACB, Action, Harness, ManualClock, Status, TaskState, Write
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
    h.reconcile.scan_deadlines = lambda now: (scans.append(int(now)), orig_scan(now))[1]

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
    stale = copy.copy(acb)                        # 旧 worker 的本地视图（旧 fence）
    assert stale.claim_epoch == c1.fence_epoch

    # 旧 worker 的副作用已完成（模拟「工具已确认」后崩溃）
    st = h.runtime._do_action(acb, action, 0)
    assert st == Status.OK
    assert len(_tool_events(h)) == 1

    clock.advance(501)                            # 越过 claim 租约
    c2 = h.scheduler.claim(acb.id, "w2")          # 重 claim：原子 supervisor_note_crash
    assert c2 is not None and c2.worker_id == "w2"
    assert c2.attempt == 2
    assert c2.fence_epoch > c1.fence_epoch
    assert stale.claim_epoch < c2.fence_epoch

    # 旧 worker 的写一律 REJECT_STALE（commit / budget.consume / effects.begin）
    stale.staged = {"r": "old"}
    assert h.commit_batch(stale, [Write(resource="r")]) == Status.REJECT_STALE
    assert h.budget.consume(acb.reservation_ids[0], 1,
                            acb=stale) == Status.REJECT_STALE
    assert h.effects.begin(stale, action) == ""
    with pytest.raises(FencingRejected):
        h.checkpoints.save(stale)

    # 新 worker 接手执行 → idem 短路：TOOL_COMPLETED 仍只 1 次
    h.scheduler.release_claim(acb.id, "w2")
    st = h.run_task(acb, [decision(1, tool("t", {"p": "/x"}), finish())])
    assert st == Status.OK
    assert acb.state is TaskState.DONE
    assert len(_tool_events(h)) == 1
    assert h.budget._reservations[acb.reservation_ids[0]][1].consumed == 1
