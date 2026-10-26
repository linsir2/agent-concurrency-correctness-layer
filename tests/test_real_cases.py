"""4 个真实用例的可执行测试（对应 use-cases-and-adversarial-tests.md U1–U4）。"""

from __future__ import annotations

import pytest

from conftest import (admit, commit, decision, finish, hitl, make_harness,
                      seed, stage, state_of, tool)
from harness import Status, TaskState
from harness.runtime import CrashInjected


# ---------------------------------------------------------------- U1
def test_u1_parallel_refactor_serial_branch():
    """U1 分支 A：committed 写集重叠 → DAG 串行 → 无冲突，顺序提交。"""
    h = make_harness()
    seed(h, "src/auth/types.ts", {"fields": []})

    plan = h.planner.plan("R1", [
        {"task_id": "T1", "committed": ["src/auth/login.ts", "src/auth/types.ts"]},
        {"task_id": "T2", "committed": ["src/auth/session.ts", "src/auth/types.ts"]},
        {"task_id": "T3", "committed": ["src/auth/oauth.ts", "src/auth/types.ts"]},
    ])
    h.plans["R1"] = plan

    # T1 提交 types.ts
    a1 = admit(h, "R1", plan, "T1")
    h.run_task(a1, [decision(1, stage("src/auth/types.ts", {"fields": ["JwtPayload"]}),
                             stage("src/auth/login.ts", {"fn": "login"}),
                             commit("src/auth/types.ts", "src/auth/login.ts"))])
    assert h.workspace.version("src/auth/types.ts") == 1
    assert a1.state == TaskState.DONE

    # T2 基于 v1 提交
    a2 = admit(h, "R1", plan, "T2")
    h.run_task(a2, [decision(1, stage("src/auth/types.ts", {"fields": ["JwtPayload", "Session"]}),
                             stage("src/auth/session.ts", {"fn": "session"}),
                             commit("src/auth/types.ts", "src/auth/session.ts"))])
    assert h.workspace.version("src/auth/types.ts") == 2

    # T3 基于 v2 提交
    a3 = admit(h, "R1", plan, "T3")
    h.run_task(a3, [decision(1, stage("src/auth/types.ts",
                                      {"fields": ["JwtPayload", "Session", "OAuth"]}),
                             stage("src/auth/oauth.ts", {"fn": "oauth"}),
                             commit("src/auth/types.ts", "src/auth/oauth.ts"))])
    assert h.workspace.version("src/auth/types.ts") == 3
    assert a3.state == TaskState.DONE
    assert state_of(h, "R1", "T1") == TaskState.DONE


def test_u1_parallel_refactor_concurrent_conflict_and_merge():
    """U1 分支 B：concurrent 写同一资源 → 一方 CONFLICT → 3-way merge。"""
    h = make_harness()
    seed(h, "src/auth/types.ts", {"fields": []})

    plan = h.planner.plan("R1", [{"task_id": "T1", "contingent": ["src/auth/types.ts"]},
                                 {"task_id": "T2", "contingent": ["src/auth/types.ts"]}])
    h.plans["R1"] = plan

    a1 = admit(h, "R1", plan, "T1")
    h.run_task(a1, [decision(1, stage("src/auth/types.ts", {"fields": ["A"]}),
                             commit("src/auth/types.ts"))])
    assert h.workspace.version("src/auth/types.ts") == 1

    a2 = admit(h, "R1", plan, "T2")
    a2.base_version["src/auth/types.ts"] = 0          # 陈旧 base
    a2.read_set["src/auth/types.ts"] = 0
    st = h.commit_batch(a2, [__import__("harness").Write("src/auth/types.ts",
                                                          content={"fields": ["B"]})])
    assert st == Status.CONFLICT

    # 3-way merge：base={"a":1}, ours={"a":1,"b":2}, theirs={"a":1,"c":3}
    result = h.workspace.rebase_json({"a": 1}, {"a": 1, "b": 2}, {"a": 1, "c": 3})
    assert result["conflicts"] == []
    merged = result["merged"]
    assert merged == {"a": 1, "b": 2, "c": 3}

    a2.base_version["src/auth/types.ts"] = 1
    a2.read_set["src/auth/types.ts"] = 1
    a2.staged["src/auth/types.ts"] = merged
    st2 = h.commit_batch(a2, [__import__("harness").Write("src/auth/types.ts", content=merged)])
    assert st2 == Status.OK
    assert h.workspace.version("src/auth/types.ts") == 2
    assert h.workspace.get("src/auth/types.ts") == {"a": 1, "b": 2, "c": 3}


# ---------------------------------------------------------------- U2
def test_u2_parallel_review_conflict_and_rebase():
    """U2：reviewer 并发写同一文件 → CONFLICT → rebase → 最终无丢失更新。"""
    h = make_harness()
    seed(h, "src/a.ts", {"issues": []})

    plan = h.planner.plan("R2", [{"task_id": "R1"}, {"task_id": "R2"}])
    h.plans["R2"] = plan

    r1 = admit(h, "R2", plan, "R1")
    h.run_task(r1, [decision(1, stage("src/a.ts", {"issues": ["sql-injection"]}),
                             commit("src/a.ts"))])
    assert h.workspace.version("src/a.ts") == 1

    r2 = admit(h, "R2", plan, "R2")
    r2.base_version["src/a.ts"] = 0
    r2.read_set["src/a.ts"] = 0
    from harness import Write
    st = h.commit_batch(r2, [Write("src/a.ts", content={"issues": ["no-index"]})])
    assert st == Status.CONFLICT

    merged = h.workspace.rebase_json({"issues": []},
                                     {"issues": ["sql-injection"]},
                                     {"issues": ["no-index"]})["merged"]
    r2.base_version["src/a.ts"] = 1
    r2.read_set["src/a.ts"] = 1
    r2.staged["src/a.ts"] = merged
    assert h.commit_batch(r2, [Write("src/a.ts", content=merged)]) == Status.OK
    assert h.workspace.version("src/a.ts") == 2


# ---------------------------------------------------------------- U3
def test_u3_batch_migration_json_aware_merge():
    """U3：5 个 agent 并发加 strict → JSON-aware merge 消解假冲突。"""
    h = make_harness()
    seed(h, "tsconfig.json", {"compilerOptions": {}})

    plan = h.planner.plan("R3", [
        {"task_id": f"M{i}", "contingent": ["tsconfig.json"],
         "logical_deps": [{"kind": "TYPE_GRAPH", "id": "f2"}]}
        for i in range(1, 6)
    ])
    h.plans["R3"] = plan

    versions = []
    for i in range(1, 6):
        acb = admit(h, "R3", plan, f"M{i}")
        acb.base_version["tsconfig.json"] = h.workspace.version("tsconfig.json")
        acb.read_set["tsconfig.json"] = acb.base_version["tsconfig.json"]
        base = h.workspace.get("tsconfig.json") or {"compilerOptions": {}}
        ours = {"compilerOptions": {"strict": True}}
        theirs = {"compilerOptions": {"strict": True}}
        merged = h.workspace.rebase_json(base, ours, theirs)["merged"]
        acb.staged["tsconfig.json"] = merged
        from harness import Write
        st = h.commit_batch(acb, [Write("tsconfig.json", content=merged)])
        versions.append(st)

    assert all(v == Status.OK for v in versions)
    assert h.workspace.get("tsconfig.json")["compilerOptions"]["strict"] is True
    assert h.workspace.version("tsconfig.json") == 5


# ---------------------------------------------------------------- U4
def test_u4_long_task_crash_recovery_idempotent():
    """U4：20 步任务第 12 步崩溃 → 恢复 → 副作用有效一次（I7）。"""
    h = make_harness()
    plan = h.planner.plan("R4", [{"task_id": "T1", "logical_subgoal_id": "deploy-service",
                                  "min_budget": 100}])
    h.plans["R4"] = plan
    acb = admit(h, "R4", plan, "T1")

    decisions = []
    for step in range(1, 21):
        decisions.append(decision(step, tool("deploy", {"step": step})))

    with pytest.raises(CrashInjected) as exc:
        h.run_task(acb, decisions, crash_at=(12, 0))
    assert exc.value.turn_seq == 12

    ref = acb.result_ref
    recovered = h.recover(ref)
    assert recovered.generation == 2

    # 崩溃时 effect 停在 UNKNOWN（文档 §5.8⑤）
    key12 = h.idem_key(recovered, decisions[11].actions[0], 0)
    assert h.effects.state(key12) == "UNKNOWN"

    # 恢复流程反查外部系统：已发生 → 标记 CONFIRMED（不再重执行）
    h.effects.confirm(key12, recovered.id)

    recovered.state = TaskState.RUNNING
    h.run_task(recovered, decisions[11:])
    assert recovered.state == TaskState.DONE

    # 副作用有效一次：第 12 步经「UNKNOWN → 反查 → CONFIRMED」恰好一次；
    # 其余 19 步各产生一条 TOOL_COMPLETED。
    tool_events = [e for e in h.event_log.all() if e.kind.value == "TOOL_COMPLETED"]
    assert len(tool_events) == 19
    assert h.effects.state(key12) == "CONFIRMED"
    confirmed = [e for e in h.event_log.all()
                 if e.kind.value == "EFFECT_CONFIRMED" and e.idempotency_key == key12]
    assert len(confirmed) == 1
