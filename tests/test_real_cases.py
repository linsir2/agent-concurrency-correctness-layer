"""4 个真实用例的可执行测试（对应 use-cases-and-adversarial-tests.md U1–U4）。

v1.1（S6 §3）：U1×2/U2/U4 走真实路径，U3 拆正/负对照。
"""

from __future__ import annotations

import pytest
from conftest import (
    admit,
    commit,
    decision,
    finish,
    make_harness,
    seed,
    stage,
    state_of,
    tool,
)

from harness import EventKind, Status, TaskState, Write
from harness.runtime import CrashInjected


# ---------------------------------------------------------------- U1-A
def test_u1_dependency_gate_blocks_until_predecessor_done():
    """U1-A：T1→T2 逻辑边存在时，T1 未 DONE → T2 INADMISSIBLE；DONE 后 ADMITTED。"""
    h = make_harness()
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10},
                                 {"task_id": "T2", "min_budget": 10}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R1"] = plan

    t2 = admit(h, "R1", plan, "T2")
    assert t2.state is TaskState.INADMISSIBLE
    assert t2.failure == "PREDECESSOR_NOT_DONE"
    assert t2.reservation_ids == [] and t2.budget_node is None
    assert h.slots.active == 0

    t1 = admit(h, "R1", plan, "T1")
    h.run_task(t1, [decision(1, finish())])
    assert t1.state is TaskState.DONE
    assert state_of(h, "R1", "T1") is TaskState.DONE

    t2_again = admit(h, "R1", plan, "T2")
    assert t2_again.state is TaskState.ADMITTED


def test_u1_dependency_gate_negative_control_without_edge():
    """U1-A 负对照：不建边 → T2 立即可准入。"""
    h = make_harness()
    plan = h.planner.plan("R1N", [{"task_id": "T1", "min_budget": 10},
                                  {"task_id": "T2", "min_budget": 10}])
    h.plans["R1N"] = plan

    t2 = admit(h, "R1N", plan, "T2")
    assert t2.state is TaskState.ADMITTED


# ---------------------------------------------------------------- U1-B
def test_u1_stage_before_commit_conflict_and_rebase():
    """U1-B：B 先 stage（base=v0）→ A 经 run_task 提交 v1 → B 提交必然 CONFLICT 并 rebase。"""
    h = make_harness()
    v0 = seed(h, "src/auth/types.ts", {"a": 0, "b": 0})
    plan = h.planner.plan("R1B", [
        {"task_id": "A", "contingent": ["src/auth/types.ts"]},
        {"task_id": "B", "contingent": ["src/auth/types.ts"]},
    ])
    h.plans["R1B"] = plan
    a = admit(h, "R1B", plan, "A")
    b = admit(h, "R1B", plan, "B")

    h.workspace.stage(b, "src/auth/types.ts", {"a": 0, "b": 2})
    assert b.base_version["src/auth/types.ts"] == v0

    st_a = h.run_task(a, [decision(1, stage("src/auth/types.ts", {"a": 1, "b": 0}),
                                   commit("src/auth/types.ts"))])
    assert st_a == Status.OK
    assert h.workspace.version("src/auth/types.ts") == v0 + 1

    st_b = h.run_task(b, [decision(1, commit("src/auth/types.ts"))])
    assert st_b == Status.OK
    assert b.state is TaskState.DONE
    assert b.rebase_attempts == 1
    assert h.workspace.version("src/auth/types.ts") == v0 + 2
    assert h.workspace.get("src/auth/types.ts") == {"a": 1, "b": 2}


# ---------------------------------------------------------------- U2
def test_u2_parallel_review_conflict_and_rebase():
    """U2：reviewer 并发写同一文件 → stage-before-commit → CONFLICT → rebase 无丢失更新。"""
    h = make_harness()
    v0 = seed(h, "src/a.ts", {"x": 0, "y": 0})
    plan = h.planner.plan("R2", [{"task_id": "R1", "contingent": ["src/a.ts"]},
                                 {"task_id": "R2", "contingent": ["src/a.ts"]}])
    h.plans["R2"] = plan
    r1 = admit(h, "R2", plan, "R1")
    r2 = admit(h, "R2", plan, "R2")

    h.workspace.stage(r2, "src/a.ts", {"x": 0, "y": 2})
    assert r2.base_version["src/a.ts"] == v0

    st1 = h.run_task(r1, [decision(1, stage("src/a.ts", {"x": 1, "y": 0}),
                                   commit("src/a.ts"))])
    assert st1 == Status.OK
    assert h.workspace.version("src/a.ts") == v0 + 1

    st2 = h.run_task(r2, [decision(1, commit("src/a.ts"))])
    assert st2 == Status.OK
    assert r2.rebase_attempts == 1
    assert h.workspace.version("src/a.ts") == v0 + 2
    assert h.workspace.get("src/a.ts") == {"x": 1, "y": 2}


def test_u2_stale_read_dependency_refreshed():
    """U2 分支：stage 后他人推进 read_set 资源 → REJECT_STALE_READ → 刷新后重试。"""
    h = make_harness()
    seed(h, "src/a.ts", {"x": 0})
    vs = seed(h, "src/contract.json", {"v": 0})
    plan = h.planner.plan("R2S", [{"task_id": "R1", "contingent": ["src/a.ts"]}])
    h.plans["R2S"] = plan
    acb = admit(h, "R2S", plan, "R1")

    h.workspace.stage(acb, "src/a.ts", {"x": 1})
    h.workspace.stage(acb, "src/contract.json", {"v": 0})
    acb.staged.pop("src/contract.json")
    assert acb.read_set["src/contract.json"] == vs

    h.workspace.put("src/contract.json", {"v": 9})

    st = h.commit_batch(acb, [Write("src/a.ts")])
    assert st == Status.REJECT_STALE_READ
    assert acb.read_set["src/contract.json"] == vs

    st2 = h.run_task(acb, [decision(1, commit("src/a.ts"))])
    assert st2 == Status.OK
    assert acb.rebase_attempts == 1
    assert acb.read_set["src/contract.json"] == h.workspace.version("src/contract.json")
    assert h.workspace.get("src/a.ts") == {"x": 1}


# ---------------------------------------------------------------- U3
def _residue(h) -> dict:
    """准入失败前后的账本/声明/槽位/事件快照。"""
    return {
        "slots": h.slots.active_keys(),
        "logical": {dep: dict(holders) for dep, holders in h.intents._logical.items()},
        "reservations": {rid: (r.state, r.consumed)
                         for rid, (_, r) in h.budget._reservations.items()},
        "nodes": sorted(h.budget._nodes),
        "root_subtree": h.budget.root.subtree_reserved,
        "events": len(h.event_log.all()),
    }


def test_u3_same_dep_id_contract_conflict_zero_residue():
    """U3 正例：同 logical_deps dep_id → 第二个 INADMISSIBLE + CONTRACT_CONFLICT + 零残留。"""
    h = make_harness()
    plan = h.planner.plan("R3", [
        {"task_id": "M1", "min_budget": 10,
         "logical_deps": [{"kind": "CONTRACT", "id": "c1"}]},
        {"task_id": "M2", "min_budget": 10,
         "logical_deps": [{"kind": "CONTRACT", "id": "c1"}]},
    ])
    h.plans["R3"] = plan
    m1 = admit(h, "R3", plan, "M1")
    assert m1.state is TaskState.ADMITTED

    before = _residue(h)
    m2 = admit(h, "R3", plan, "M2")
    assert m2.state is TaskState.INADMISSIBLE
    assert m2.failure == "CONTRACT_CONFLICT"
    assert _residue(h) == before
    assert m2.reservation_ids == [] and m2.budget_node is None
    assert m2.id not in h.intents._logical.get("c1", {})
    assert h.intents._logical["c1"] == {m1.id: "CONTRACT"}


def test_u3_different_dep_id_both_admitted():
    """U3 负对照：dep_id 不同 → 两者都 ADMITTED。"""
    h = make_harness()
    plan = h.planner.plan("R3N", [
        {"task_id": "M1", "min_budget": 10,
         "logical_deps": [{"kind": "CONTRACT", "id": "c1"}]},
        {"task_id": "M2", "min_budget": 10,
         "logical_deps": [{"kind": "CONTRACT", "id": "c2"}]},
    ])
    h.plans["R3N"] = plan
    m1 = admit(h, "R3N", plan, "M1")
    m2 = admit(h, "R3N", plan, "M2")

    assert m1.state is TaskState.ADMITTED
    assert m2.state is TaskState.ADMITTED
    assert m1.reservation_ids and m2.reservation_ids
    assert h.slots.active == 2
    assert set(h.intents._logical) == {"c1", "c2"}


def test_u3_merge_five_agents_all_ok():
    """U3（独立用例）：5 个 agent 并发加 strict → JSON-aware merge 消解假冲突。"""
    h = make_harness()
    v0 = seed(h, "tsconfig.json", {"compilerOptions": {}})

    plan = h.planner.plan("R3M", [
        {"task_id": f"M{i}", "contingent": ["tsconfig.json"],
         "logical_deps": [{"kind": "TYPE_GRAPH", "id": "f2"}]}
        for i in range(1, 6)
    ])
    h.plans["R3M"] = plan

    versions = []
    for i in range(1, 6):
        acb = admit(h, "R3M", plan, f"M{i}")
        acb.base_version["tsconfig.json"] = h.workspace.version("tsconfig.json")
        acb.read_set["tsconfig.json"] = acb.base_version["tsconfig.json"]
        base = h.workspace.get("tsconfig.json") or {"compilerOptions": {}}
        ours = {"compilerOptions": {"strict": True}}
        theirs = {"compilerOptions": {"strict": True}}
        merged = h.workspace.rebase_json(base, ours, theirs)["merged"]
        acb.staged["tsconfig.json"] = merged
        st = h.commit_batch(acb, [Write("tsconfig.json", content=merged)])
        versions.append(st)

    assert all(v == Status.OK for v in versions)
    assert h.workspace.get("tsconfig.json")["compilerOptions"]["strict"] is True
    assert h.workspace.version("tsconfig.json") == v0 + 5


# ---------------------------------------------------------------- U4
def _u4_setup(probe_result):
    calls: list[str] = []

    def probe(idem_key: str):
        calls.append(idem_key)
        return probe_result

    h = make_harness(probe=probe)
    plan = h.planner.plan("R4", [{"task_id": "T1", "logical_subgoal_id": "deploy-service",
                                  "min_budget": 100}])
    h.plans["R4"] = plan
    acb = admit(h, "R4", plan, "T1")
    decisions = [decision(step, tool("deploy", {"step": step})) for step in range(1, 21)]

    with pytest.raises(CrashInjected) as exc:
        h.run_task(acb, decisions, crash_at=(12, 0))
    assert exc.value.turn_seq == 12
    return h, acb, decisions, calls


@pytest.mark.parametrize("probe_result", [True, False])
def test_u4_crash_recovery_probe_three_states(probe_result):
    """U4：真 probe（True/False）→ EFFECT_PROBED 先于 EFFECT_CONFIRMED，probe 恰好一次。"""
    h, acb, decisions, calls = _u4_setup(probe_result)
    key12 = h.idem_key(acb, decisions[11].actions[0], 0)
    assert h.effects.state(key12) == "UNKNOWN"

    recovered = h.recover(acb.result_ref)
    assert recovered.generation == 2
    assert calls == [key12]

    probed = [e for e in h.event_log.all()
              if e.kind == EventKind.EFFECT_PROBED and e.idempotency_key == key12]
    assert len(probed) == 1

    h.run_task(recovered, decisions[11:])
    assert recovered.state is TaskState.DONE
    assert h.effects.state(key12) == "CONFIRMED"
    assert calls == [key12]

    confirmed = [e for e in h.event_log.all()
                 if e.kind == EventKind.EFFECT_CONFIRMED and e.idempotency_key == key12]
    assert len(confirmed) == 1
    assert probed[0].seq < confirmed[0].seq

    tool_events = [e for e in h.event_log.all() if e.kind == EventKind.TOOL_COMPLETED]
    assert len(tool_events) == (19 if probe_result else 20)


def test_u4_crash_recovery_probe_none_quarantines():
    """U4：probe=None → QUARANTINED（不重执行、不产生 CONFIRMED）。"""
    h, acb, decisions, calls = _u4_setup(None)
    key12 = h.idem_key(acb, decisions[11].actions[0], 0)
    assert h.effects.state(key12) == "UNKNOWN"

    recovered = h.recover(acb.result_ref)
    assert recovered.state is TaskState.QUARANTINED
    assert recovered.failure == "NEEDS_RECONCILIATION"
    assert calls == [key12]
    assert h.effects.state(key12) == "UNKNOWN"

    probed = [e for e in h.event_log.all()
              if e.kind == EventKind.EFFECT_PROBED and e.idempotency_key == key12]
    assert len(probed) == 1
    assert not [e for e in h.event_log.all()
                if e.kind == EventKind.EFFECT_CONFIRMED and e.idempotency_key == key12]
