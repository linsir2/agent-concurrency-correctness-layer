"""MS3 出口条件：崩溃恢复与幂等（S5 / T11、T12、T13、T25、T26、T27、T28、T29）。

对应 `docs/s5-recovery-and-journals.md` v1.1 §7：
- T11：effect UNKNOWN 反查三分支（probe=True/False/None）
- T12：补偿中途崩溃续做（真实崩溃注入 + 磁盘 journal 批次原子）
- T13：idem_key 稳定性
- T25：逐笔账本恢复
- T26：补偿幂等键
- T27：生产不变量（含反证）
- T28：network/data_class + 签名 + runtime 强制点
- T29：requeue_readmit
"""

from __future__ import annotations

import pytest
from conftest import _acb, admit, decision, finish, make_harness, seed, tool

from harness import (
    ACB,
    DELETE,
    Action,
    EventKind,
    Harness,
    InvariantError,
    MissingSecret,
    Signer,
    Status,
    TaskState,
    Write,
)
from harness.budget import BudgetManager, BudgetNode
from harness.checkpoint import effect_idem_key
from harness.compensation import CompensationJournal, comp_id_of
from harness.runtime import CrashInjected


def _tool_completed_count(h) -> int:
    """TOOL_COMPLETED 事件数（工具执行次数的 harness 侧记录）。"""
    return len([e for e in h.event_log.all() if e.kind == EventKind.TOOL_COMPLETED])


# ================================================================ T11
@pytest.mark.ms3
def test_ms3_effect_unknown_reconcile_three_branches():
    """T11：effect UNKNOWN 反查三分支（probe=True/False/None）。"""
    act = tool("deploy")

    # ① probe=True：已发生 → 不重复（recover 后 CONFIRMED，重跑短路）
    h = make_harness(probe=lambda key: True)
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10}])
    acb = admit(h, "R1", plan, "T1")
    with pytest.raises(CrashInjected):
        h.run_task(acb, [decision(1, act)], crash_at=(1, 0))
    idem = h.idem_key(acb, act, 0)
    assert h.effects.state(idem) == "UNKNOWN"
    rec = h.recover(acb.result_ref)
    assert rec.state is TaskState.RUNNING
    assert h.effects.state(idem) == "CONFIRMED"
    before = _tool_completed_count(h)
    h.run_task(rec, [decision(1, act)])
    assert _tool_completed_count(h) == before          # 已确认 → 不重复执行

    # ② probe=False：未发生 → 重执行一次（同一 idem）
    h2 = make_harness(probe=lambda key: False)
    plan2 = h2.planner.plan("R2", [{"task_id": "T1", "min_budget": 10}])
    acb2 = admit(h2, "R2", plan2, "T1")
    with pytest.raises(CrashInjected):
        h2.run_task(acb2, [decision(1, act)], crash_at=(1, 0))
    idem2 = h2.idem_key(acb2, act, 0)
    rec2 = h2.recover(acb2.result_ref)
    assert h2.effects.state(idem2) == "UNKNOWN"          # 未确认
    before2 = _tool_completed_count(h2)
    h2.run_task(rec2, [decision(1, act)])
    assert _tool_completed_count(h2) == before2 + 1     # 重执行一次

    # ③ probe=None：升级人工 → QUARANTINED + NEEDS_RECONCILIATION（不是 FAILED）
    h3 = make_harness(probe=lambda key: None)
    plan3 = h3.planner.plan("R3", [{"task_id": "T1", "min_budget": 10}])
    acb3 = admit(h3, "R3", plan3, "T1")
    with pytest.raises(CrashInjected):
        h3.run_task(acb3, [decision(1, act)], crash_at=(1, 0))
    rec3 = h3.recover(acb3.result_ref)
    assert rec3.state is TaskState.QUARANTINED
    assert rec3.failure == "NEEDS_RECONCILIATION"
    assert rec3.failure != "FAILED"


# ================================================================ T12
@pytest.mark.ms3
def test_ms3_compensation_crash_resume():
    """T12：补偿中途崩溃续做（崩溃注入在 journal 批次 append 内）。

    断言重放后的条目数只能是 0 或 N（同帧原子），绝不允许「写了一半」。
    """
    h = make_harness()
    acb = _acb(h, "r1.ts")
    for res, val in (("r1.ts", {"v": 1}), ("r2.ts", {"v": 2})):
        acb.base_version[res] = 0
        acb.staged[res] = val
        assert h.commit_batch(acb, [Write(res, content=val)]) == Status.OK
    applied = [Write(res, content=val, version_before=0,
                     applied_hash=h.workspace.content_hash(res))
               for res, val in (("r1.ts", {"v": 1}), ("r2.ts", {"v": 2}))]

    # ① 崩溃注入在 `CompensationJournal.plan` 的 append_batch 内
    original_append_batch = h.event_log.append_batch

    def crashy_append_batch(drafts, notify=True):
        raise CrashInjected(0, 0)

    h.event_log.append_batch = crashy_append_batch
    try:
        with pytest.raises(CrashInjected):
            h.compensate(acb, applied)
    finally:
        h.event_log.append_batch = original_append_batch

    # ② 重放 EventLog：批次原子 → 条目数 ∈ {0, N}，不可能只有一半
    rebuilt = CompensationJournal(h.event_log)
    rebuilt.rebuild_from(h.event_log.all())
    n_entries = len([e for e in rebuilt._entries.values() if e.acb_id == acb.id])
    assert n_entries in (0, len(applied))
    assert n_entries != 1, "批次原子被破坏：出现只写一半的 journal"
    assert h.workspace.get("r1.ts") == {"v": 1}          # 崩溃发生在补偿执行前
    assert h.workspace.get("r2.ts") == {"v": 2}

    # ③ 正常落盘后：第一条补偿完成、第二条前崩溃 → resume 只续做未完成
    h.comp_journal = rebuilt
    entries = h.comp_journal.plan(acb, applied)
    assert len(entries) == 2
    h._run_one_compensation(acb, entries[0], applied[0])
    plan = h.comp_journal.resume(acb, h.workspace)
    assert len(plan["done"]) == 1
    assert len(plan["retry"]) == 1
    assert plan["done"][0].state == "COMPENSATED"
    assert plan["retry"][0].state == "COMPENSATING"

    # ④ deleted 分支：resume 必须传 workspace 才能按版本号判定删除已完成
    hd = make_harness()
    seed(hd, "d.ts", {"v": 1})
    acbd = _acb(hd, "d.ts")
    acbd.base_version["d.ts"] = 1
    acbd.staged["d.ts"] = DELETE
    assert hd.commit_batch(acbd, [Write("d.ts", content=DELETE)]) == Status.OK
    dw = Write("d.ts", content=DELETE, version_before=1, applied_hash="")
    hd.comp_journal.plan(acbd, [dw])
    dplan = hd.comp_journal.resume(acbd, hd.workspace)
    assert len(dplan["done"]) == 1
    assert dplan["done"][0].deleted is True


# ================================================================ T13
@pytest.mark.ms3
def test_ms3_idem_key_stability():
    """T13：idem_key 稳定性（task_id/generation 无关；logical_subgoal_id 相关）。"""
    act = Action(kind="tool", tool="deploy", args={"x": 1})
    a = ACB(id="A1", run_id="R1", task_id="T1", logical_subgoal_id="LG1")
    k_a = effect_idem_key(a, act)
    assert k_a == effect_idem_key(a, Action(kind="tool", tool="deploy",
                                            args={"x": 1}))

    # task_id / generation 变 → 不变
    c = ACB(id="A2", run_id="R1", task_id="T-other",
            logical_subgoal_id="LG1", generation=9)
    assert effect_idem_key(c, act) == k_a

    # logical_subgoal_id 变 → 变
    d = ACB(id="A3", run_id="R1", task_id="T1", logical_subgoal_id="LG-OTHER")
    assert effect_idem_key(d, act) != k_a

    # 与 Harness.idem_key 一致
    h = make_harness()
    assert h.idem_key(a, act, 0) == k_a


# ================================================================ T25
@pytest.mark.ms3
def test_ms3_per_reservation_ledger_restore():
    """T25：逐笔账本恢复（_ensure_retry_budget 真实追加 → 2 笔 → recover 逐笔 balance）。"""
    h = make_harness(run_limits=1000)
    acb = ACB(id="A-T25", run_id="R", task_id="T1", logical_subgoal_id="LG1")
    acb.retry_budget = 40                       # needed = 41 > available 30 → 追加 11
    node = h.budget.create_node("B-T25", 100)
    node.owner = acb.id
    acb.budget_node = node
    resv = h.budget.reserve("B-T25", 30)        # min_budget = 30
    acb.reservation_ids = [resv.reservation_id]

    assert h.runtime._ensure_retry_budget(acb) is True
    assert len(acb.reservation_ids) == 2, "追加预留未发生，测试前提不成立"
    assert [h.budget._reservations[r][1].balance
            for r in acb.reservation_ids] == [30, 11]

    ref = h.checkpoints.save(acb)
    # 新进程：共享 checkpoint 存储，独立 budget / event_log
    h2 = Harness(run_limits=1000, checkpoints=h.checkpoints)
    r = h2.recover(ref)
    assert len(r.reservation_ids) == 2
    assert [h2.budget._reservations[x][1].balance
            for x in r.reservation_ids] == [30, 11]
    h2.budget.check_invariants()


# ================================================================ T26
@pytest.mark.ms3
def test_ms3_compensation_idempotent_key():
    """T26：补偿幂等键（comp_id 稳定；重复补偿 → COMPENSATED；删除类用版本号）。"""
    c1 = comp_id_of("R1", "A1", "LG1", "res", 3, "hashA", "hashB")
    assert c1 == comp_id_of("R1", "A1", "LG1", "res", 3, "hashA", "hashB")
    assert comp_id_of("R1", "A1", "LG2", "res", 3, "hashA", "hashB") != c1
    assert comp_id_of("R1", "A1", "LG1", "res", 3, "hashA", "hashC") != c1
    # 删除类用版本号（version_before 入键），不用空 hash
    assert comp_id_of("R1", "A1", "LG1", "res", 0, "hashA", "") != \
        comp_id_of("R1", "A1", "LG1", "res", 1, "hashA", "")

    # 重复补偿 → COMPENSATED（幂等，不二次删、版本不推进）
    h = make_harness()
    acb = _acb(h, "new.ts")
    acb.base_version["new.ts"] = 0
    acb.staged["new.ts"] = {"code": 1}
    assert h.commit_batch(acb, [Write("new.ts", content={"code": 1})]) == Status.OK
    ph = h.workspace.content_hash("new.ts")
    h.compensate(acb, [Write("new.ts", content={"code": 1},
                             version_before=0, applied_hash=ph)])
    assert h.workspace.get("new.ts") is None
    ver = h.workspace.version("new.ts")
    h.compensate(acb, [Write("new.ts", content={"code": 1},
                             version_before=0, applied_hash=ph)])
    assert h.workspace.get("new.ts") is None
    assert h.workspace.version("new.ts") == ver
    assert h.compensation_journal[-1] == {"resource": "new.ts", "state": "COMPENSATED"}

    # 删除类补偿：CompEntry.deleted=True，版本号入键（不用空 hash）
    hd = make_harness()
    seed(hd, "d.ts", {"v": 1})
    acbd = _acb(hd, "d.ts")
    dw = Write("d.ts", content=DELETE, version_before=1,
               applied_hash=hd.workspace.content_hash("d.ts"))
    entries = hd.comp_journal.plan(acbd, [dw])
    assert len(entries) == 1 and entries[0].deleted is True
    assert entries[0].version_before == 1


# ================================================================ T27
@pytest.mark.ms3
def test_ms3_production_invariants(monkeypatch):
    """T27：生产不变量（check_invariants 用 raise；破坏用可写 property；含反证）。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [{"task_id": "T1", "min_budget": 10}])
    acb = h.dispatch_and_run(plan, "T1", [decision(1, finish())])
    ref = h.checkpoints.save(acb)               # 先定义 ref
    assert h.checkpoints.load(ref)["acb_id"] == acb.id
    h.budget.check_invariants()                 # 正常路径不抛

    # 破坏 → 用**可写** property（只读 property 会抛 AttributeError 而非 InvariantError）
    monkeypatch.setattr(BudgetNode, "self_reserved",
                        property(lambda s: -1, lambda s, v: None), raising=False)
    with pytest.raises(InvariantError):
        h.budget.reserve("run", 1)

    # 反证：去掉检查就不抛（证明判据有效）
    monkeypatch.setattr(BudgetManager, "_check_local", lambda self, node: None)
    resv = h.budget.reserve("run", 1)                # 不抛，且确实执行了预留
    assert resv is not None


# ================================================================ T28
@pytest.mark.ms3
def test_ms3_network_data_class_and_signature():
    """T28：network/data_class 四维拒绝 + 签名（防篡改/重放）+ runtime 强制点。"""
    h = make_harness(auth_secret="test-secret")
    base = {"tools": {"http"}, "paths": {"/src"},
            "network": {"api.example.com"}, "data_class": "public"}

    def mint(**over):
        return h.auth.mint("A1", **{**base, **over})

    # ① policy 层四维拒绝（每次新签 token：verify 的 jti 去重只允许一次验签）
    assert h.policy.check_capability("A1", mint(), "http", "/src/a",
                                     network="api.example.com",
                                     data_class="public") is True
    assert h.policy.check_capability("A1", mint(), "http", "https://evil.com",
                                     network="evil.com") is False       # network
    assert h.policy.check_capability("A1", mint(), "http",
                                     "/etc/passwd") is False           # path
    assert h.policy.check_capability("A1", mint(), "ssh",
                                     "/src/a") is False                # tool
    assert h.policy.check_capability("A1", mint(), "http", "/src/a",
                                     data_class="secret") is False     # data_class

    # ② 签名：篡改 → False；重放 → None
    t = h.auth.mint("A1", **base)
    assert h.auth.verify(h.auth.tamper(t)) is False
    assert h.auth.verify(t) is not None
    assert h.auth.verify(t) is None             # jti 去重

    # ③ fail-closed
    with pytest.raises(MissingSecret):
        Signer(None)

    # ④ runtime 强制点：evil network 的 tool action 必须 DENIED_CAPABILITY
    h4 = make_harness()
    # token 必须绑定到 dispatcher 生成的 ACB id（A-{run}-{task}-{attempt}）
    tok = h4.policy.issue("A-R-T1-1", tools={"http"}, paths={"*"},
                          network={"api.example.com"}, data_class="*")
    plan4 = h4.planner.plan("R", [{"task_id": "T1", "min_budget": 10}])
    act4 = tool("http")
    act4.network = "evil.com"
    acb4 = h4.dispatch_and_run(plan4, "T1", [decision(1, act4)], token=tok)
    assert acb4.state is TaskState.FAILED
    assert acb4.failure == "DENIED_CAPABILITY"


# ================================================================ T29
@pytest.mark.ms3
def test_ms3_requeue_readmit():
    """T29：needs_readmit 的 ACB 不进调度；requeue_readmit 后重新准入。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")
    acb.needs_readmit = True
    old_subtree = h.budget.root.subtree_reserved

    # ① needs_readmit 的 ACB 不进调度（drain 跳过：状态/预留不变）
    h.scheduler.push(acb.task_id)
    h._drain_scheduler()
    assert acb.state is TaskState.ADMITTED
    assert acb.needs_readmit is True
    assert acb.reservation_ids != []
    assert h.slots.active == 1

    # ② requeue_readmit 后重新准入，且旧槽位 / 旧预留 / 旧节点均已清理
    fresh = h.requeue_readmit(acb)
    assert fresh.state is TaskState.ADMITTED
    assert fresh.needs_readmit is False
    assert fresh.id != acb.id
    assert acb.id not in h.acbs and fresh.id in h.acbs
    assert h.slots.active == 1                       # 旧槽位释放、新槽位占用
    assert h.budget.root.subtree_reserved == old_subtree   # 无旧预留泄漏
    assert acb.budget_node is None
    h.budget.check_invariants()
