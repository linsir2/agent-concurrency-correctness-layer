"""不变量 E2E（S6 §4）：全部串行确定性构造，断言落在事件日志 / 持久化字段 / 账本。

覆盖 I1/I2/I3/I5/I6/I8/I9/I10/I12/I13 与 I4/I7/I11 补强。
I13（收据链）在 S9b 起由 `commit_batch` 生产路径生成（K-24），纳入本套件。
"""

from __future__ import annotations

import pytest
from conftest import admit, commit, decision, finish, make_harness, seed, stage, tool

from harness import TASK_TERMINAL, EventKind, Harness, Mode, Status, TaskState, Write
from harness.spec.receipt import receipt_hash

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------- I1
def test_i1_single_writer_stage_before_commit():
    """I1：两个 agent 都基于 v0 stage → 提交点 CAS 只放行一个 OK。"""
    h = make_harness()
    v0 = seed(h, "r", {"a": 0, "b": 0})
    plan = h.planner.plan("I1", [{"task_id": "T1", "contingent": ["r"]},
                                 {"task_id": "T2", "contingent": ["r"]}])
    h.plans["I1"] = plan
    a = admit(h, "I1", plan, "T1")
    b = admit(h, "I1", plan, "T2")

    h.workspace.stage(b, "r", {"a": 0, "b": 2})
    assert b.base_version["r"] == v0

    st_a = h.run_task(a, [decision(1, stage("r", {"a": 1, "b": 0}), commit("r"))])
    st_b = h.commit_batch(b, [Write("r")])

    outcomes = [st_a, st_b]
    assert outcomes.count(Status.OK) == 1
    assert st_b == Status.CONFLICT
    assert h.workspace.version("r") == v0 + 1
    assert h.workspace.get("r") == {"a": 1, "b": 0}


# ---------------------------------------------------------------- I2
def test_i2_no_lost_update_through_rebase():
    """I2：经 run_task 的 rebase 回路，合并值同时含两侧改动。"""
    h = make_harness()
    v0 = seed(h, "r", {"a": 0, "b": 0})
    plan = h.planner.plan("I2", [{"task_id": "T1", "contingent": ["r"]},
                                 {"task_id": "T2", "contingent": ["r"]}])
    h.plans["I2"] = plan
    a = admit(h, "I2", plan, "T1")
    b = admit(h, "I2", plan, "T2")

    h.workspace.stage(b, "r", {"a": 0, "b": 2})
    h.run_task(a, [decision(1, stage("r", {"a": 1, "b": 0}), commit("r"))])
    st = h.run_task(b, [decision(1, commit("r"))])

    assert st == Status.OK
    assert b.rebase_attempts == 1
    assert h.workspace.version("r") == v0 + 2
    assert h.workspace.get("r") == {"a": 1, "b": 2}


# ---------------------------------------------------------------- I3
def test_i3_fencing_stale_lease_rejected():
    """I3：reserve → supervisor_note_crash（revoke + bump epoch）→ 旧租约提交 REJECT_STALE。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    plan = h.planner.plan("I3", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I3"] = plan
    acb = admit(h, "I3", plan, "T1")

    lease = h.intents.reserve("r", acb.id, 0, 60_000)
    assert h.intents.is_valid(lease) is True

    h.supervisor_note_crash(acb)
    assert h.intents.is_valid(lease) is False

    acb.staged["r"] = {"v": 1}
    st = h.workspace.prepare(acb, Write("r", content={"v": 1}), lease, Mode.COMMIT)
    assert st == Status.REJECT_STALE
    assert h.workspace.get("r") == {"v": 0}


# ---------------------------------------------------------------- I5
def test_i5_budget_conservation_on_serial_admit():
    """I5：串行准入不超支；失败准入后全树不变量自洽。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("I5", [{"task_id": "T1", "min_budget": 60},
                                 {"task_id": "T2", "min_budget": 60}])
    h.plans["I5"] = plan

    a1 = admit(h, "I5", plan, "T1")
    a2 = admit(h, "I5", plan, "T2")

    assert a1.state is TaskState.ADMITTED
    assert a2.state is TaskState.INADMISSIBLE
    assert a2.failure == "BUDGET_EXHAUSTED"
    assert h.budget.root.remaining == 40
    assert h.budget.root.own_reserved + h.budget.root.children_reserved == 60
    h.budget.check_invariants()


# ---------------------------------------------------------------- I6
def test_i6_context_view_causal_order():
    """I6：context_view 按 seq 有序重放，游标推进到 tail。"""
    h = make_harness()
    plan = h.planner.plan("I6", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I6"] = plan
    acb = admit(h, "I6", plan, "T1")
    acb.subscriptions.add("r")
    start = acb.cursor

    for i in range(1, 6):
        h.event_log.append(EventKind.RESOURCE_COMMITTED, "r",
                           {"content": {"i": i}, "version": i})

    events = h.event_log.replay(from_seq=start)
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)
    assert len(seqs) >= 5

    view = h.context.context_view(acb)
    assert view["r"] == {"i": 5}
    assert acb.cursor == h.event_log.tail()


# ---------------------------------------------------------------- I8
def test_i8_event_atomicity_on_failed_commit():
    """I8：陈旧 base 的失败 commit 不写任何事件（批内全有或全无）。"""
    h = make_harness()
    seed(h, "r1", {"v": 0})
    seed(h, "r2", {"v": 0})
    plan = h.planner.plan("I8", [{"task_id": "T1", "committed": ["r1", "r2"]}])
    h.plans["I8"] = plan
    acb = admit(h, "I8", plan, "T1")

    h.workspace.stage(acb, "r1", {"v": 1})
    h.workspace.stage(acb, "r2", {"v": 1})
    h.workspace.put("r2", {"v": 9})

    tail = h.event_log.tail()
    st = h.commit_batch(acb, [Write("r2"), Write("r1")])

    assert st == Status.CONFLICT
    assert h.event_log.tail() == tail
    assert h.event_log.has("r1", EventKind.RESOURCE_COMMITTED) is False
    assert h.event_log.has("r2", EventKind.RESOURCE_COMMITTED) is False
    assert h.workspace.get("r1") == {"v": 0}
    assert h.workspace.get("r2") == {"v": 9}


# ---------------------------------------------------------------- I10
def test_i10_isolation_between_acbs():
    """I10：一个 agent 预算失败不影响另一个 ACB 完成。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("I10", [{"task_id": "T1", "min_budget": 2},
                                  {"task_id": "T2", "min_budget": 10}])
    bad = h.dispatch_and_run(plan, "T1", [decision(1, tool("deploy", {"x": 1}, cost=5))])
    good = h.dispatch_and_run(plan, "T2", [decision(1, finish())])

    assert bad.state is TaskState.FAILED
    assert bad.failure == "BUDGET_EXHAUSTED"
    assert good.state is TaskState.DONE
    assert good.failure is None
    assert good.budget_node is None
    assert h.budget.root.remaining == h.budget.root.limits
    assert h.slots.active == 0
    h.budget.check_invariants()


# ---------------------------------------------------------------- I9
def test_i9_plan_validity_rejects_cycle():
    """I9：含环的逻辑边在准入前被拒绝（add_logical_edge 抛 ValueError）。"""
    h = make_harness()
    plan = h.planner.plan("I9", [{"task_id": "T1"}, {"task_id": "T2"},
                                 {"task_id": "T3"}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.planner.add_logical_edge(plan, "T2", "T3")
    assert len(plan["edges"]) == 2

    with pytest.raises(ValueError):
        h.planner.add_logical_edge(plan, "T3", "T1")
    assert len(plan["edges"]) == 2


# ---------------------------------------------------------------- I12
def test_i12_nonterminal_task_not_finalized():
    """I12：存在非终态任务时 Run 不被 finalize（无 TASK_COMPLETED、账本与槽位仍占用）。"""
    h = make_harness()
    plan = h.planner.plan("I12", [{"task_id": "T1", "min_budget": 10},
                                  {"task_id": "T2", "min_budget": 10}])
    h.plans["I12"] = plan
    t1 = admit(h, "I12", plan, "T1")
    t2 = admit(h, "I12", plan, "T2")

    h.run_task(t1, [decision(1, finish())])
    assert t1.state is TaskState.DONE
    assert plan["tasks"]["T1"]["state"] is TaskState.DONE

    assert plan["tasks"]["T2"]["state"] not in TASK_TERMINAL
    assert h.event_log.has("T2", EventKind.TASK_COMPLETED) is False
    assert t2.budget_node is not None
    assert t2.budget_node.node_id in h.budget._nodes
    assert t2.id in h.slots.active_keys()


# ---------------------------------------------------------------- I13
def test_i13_receipts_from_production_path():
    """I13：`commit_batch` 生产路径签发收据链，可离线验证，且 head 锚点匹配。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r", {"v": 0})
    plan = h.planner.plan("I13", [{"task_id": "T1", "contingent": ["r"]}])
    h.plans["I13"] = plan
    acb = h.dispatch_and_run(
        plan, "T1", [decision(1, stage("r", {"v": 1}), commit("r"))])
    assert acb.state is TaskState.DONE

    subgoal = acb.logical_subgoal_id
    receipts = h.receipts_of("I13", subgoal)
    assert receipts, "生产路径必须产生收据"
    assert all(r.signed for r in receipts)
    assert h.verify_receipts("I13", subgoal) is True
    assert h.verify_receipts("I13") is True

    completed = next(e for e in h.event_log.all()
                     if e.kind == EventKind.TASK_COMPLETED and e.subject == "T1")
    head = completed.payload["receipt_head"]
    assert completed.payload["run_id"] == "I13"
    assert completed.payload["receipt_count"] == len(receipts)
    assert head == receipt_hash(receipts[-1])
    # plan 镜像（非权威）与 EventLog 链尾一致
    assert plan["receipt_heads"][subgoal] == head
    assert h.state.receipt_heads[("I13", subgoal)] == head


# ---------------------------------------------------------------- I4/I7/I11
def _check_i4() -> None:
    """I4：required_tools 在准入 → 运行时 → 事件三层同一判定。"""
    h = make_harness()
    plan = h.planner.plan("I4", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I4"] = plan
    acb = h.dispatch_and_run(
        plan, "T1",
        [decision(1, tool("read", {"p": "/src/a"}), finish())],
        required_tools={"read": "/src/a"})

    assert acb.state is TaskState.DONE
    tool_events = [e for e in h.event_log.all()
                   if e.kind == EventKind.TOOL_COMPLETED]
    assert len(tool_events) == 1

    denied_h = Harness(run_limits=100)
    denied_h.policy.issue("A-I4D-T1-1", tools={"read"}, paths={"/src"})
    denied_plan = denied_h.planner.plan("I4D", [{"task_id": "T1", "min_budget": 10}])
    denied_h.plans["I4D"] = denied_plan
    denied = denied_h.dispatcher.try_admit(
        denied_plan["tasks"]["T1"], "I4D",
        required_tools={"deploy": "/src/a"})
    assert denied.state is TaskState.INADMISSIBLE
    assert denied.failure == "DENIED_CAPABILITY"
    assert denied.reservation_ids == [] and denied.budget_node is None


def _probe_case(probe_result):
    calls: list[str] = []

    def probe(idem_key: str):
        calls.append(idem_key)
        return probe_result

    h = make_harness(probe=probe)
    plan = h.planner.plan("I7", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I7"] = plan
    acb = admit(h, "I7", plan, "T1")
    act = tool("deploy", {"x": 1})
    key = h.idem_key(acb, act, 0)
    h.effects.begin(acb, act)
    h.run_task(acb, [decision(1, act)])
    return h, acb, key, calls


def _check_i7() -> None:
    """I7：probe 三态 → CONFIRMED 短路 / 重执行 / 隔离，外部探针恰好一次。"""
    h_t, a_t, key_t, calls_t = _probe_case(True)
    assert a_t.state is TaskState.DONE
    assert h_t.effects.state(key_t) == "CONFIRMED"
    assert calls_t == [key_t]
    assert not [e for e in h_t.event_log.all()
                if e.kind == EventKind.TOOL_COMPLETED]

    h_f, a_f, key_f, calls_f = _probe_case(False)
    assert a_f.state is TaskState.DONE
    assert h_f.effects.state(key_f) == "CONFIRMED"
    assert calls_f == [key_f]
    assert len([e for e in h_f.event_log.all()
                if e.kind == EventKind.TOOL_COMPLETED]) == 1

    h_n, a_n, key_n, calls_n = _probe_case(None)
    assert a_n.state is TaskState.QUARANTINED
    assert a_n.failure == "NEEDS_RECONCILIATION"
    assert h_n.effects.state(key_n) == "INTENT"
    assert calls_n == [key_n]


def _check_i11() -> None:
    """I11：retention 已接入 ContextAssembler；窗口外的重放仍从 cursor 起、事件不丢。

    当前 harness 未实现 cursor floor（`retention` 只配置不读取），
    故以「配置贯通 + 有界重放不丢事件」作为替代断言（见交付报告）。
    """
    h = Harness(retention=3)
    assert h.retention == 3
    assert h.context.retention == 3

    plan = h.planner.plan("I11", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I11"] = plan
    acb = admit(h, "I11", plan, "T1")
    acb.subscriptions.add("r")
    acb.cursor = h.event_log.tail()

    for i in range(10):
        h.event_log.append(EventKind.RESOURCE_COMMITTED, "r",
                           {"content": {"i": i}, "version": i})

    start = acb.cursor
    replayed = h.event_log.replay(from_seq=start)
    r_events = [e for e in replayed
                if e.subject == "r" and e.kind == EventKind.RESOURCE_COMMITTED]
    assert len(r_events) == 10
    assert [e.seq for e in r_events] == sorted(e.seq for e in r_events)

    view = h.context.context_view(acb)
    assert view["r"] == {"i": 9}
    assert acb.cursor == h.event_log.tail()


def test_i4_i7_i11_reinforcements():
    """I4/I7/I11 补强：required_tools 全链路 / probe 三态 / retention 窗口。"""
    _check_i4()
    _check_i7()
    _check_i11()
