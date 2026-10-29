"""S22 内核收口：C1–C5 的 RIPR 用例（`docs/s22-implementation-plan.md` §3）。

RIPR = Reachability / Infection / Propagation / Revealability（缺一不可）：
每条用例都注明了「若实现有 bug，哪一处状态会错、从哪里可观测、断言是什么」。
"""

from __future__ import annotations

from conftest import make_harness

from harness import EventKind, ManualClock, Status, TaskState, Write


# ---------------------------------------------------------------- helpers
def _begin(h, agent_id: str = "a1", intent_key: str = "i1",
           min_budget: int = 10, token: bool = True, ttl_ms: int = 10_000):
    """构造一个门面 token（C7 未落地前，用内核 API 直接模拟 begin）。"""
    plan = h.planner.plan(f"rsc-{agent_id}", [{
        "task_id": agent_id,
        "logical_subgoal_id": intent_key,
        "min_budget": min_budget,
    }])
    acb = h.tasks.admit_and_register(plan, agent_id)
    if token:
        acb.spec["rsc_token"] = True
        acb.spec["token_deadline_ms"] = int(h.clock.now_ms()) + int(ttl_ms)
    return acb


def _commit_events(h, resource: str = "r"):
    return [e for e in h.event_log.all()
            if e.kind is EventKind.RESOURCE_COMMITTED
            and (e.payload or {}).get("resource") == resource]


# ================================================================ C1
def test_c1_abort_releases_everything():
    """R: begin→abort；I: 只 release 不摘节点/槽位；P: 预算/槽位/逻辑公开可读。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    node_id = acb.budget_node.node_id
    rid = acb.reservation_ids[0]
    h.intents.declare_logical("D1", acb.id, "CONTRACT")
    assert h.budget.remaining("run") == 90

    h.abort(acb)

    assert node_id not in h.budget._nodes, "节点必须摘除"
    assert rid not in h.budget._reservations, "预留必须消失（退款）"
    assert acb.budget_node is None and acb.reservation_ids == []
    assert acb.id not in h.slots.active_keys(), "槽位必须释放"
    assert h.budget.remaining("run") == 100, "余额必须回满"
    assert "D1" not in h.intents._logical, "逻辑声明必须撤销"


def test_c1_abort_after_partial_apply_keeps_consumed():
    """R: 已提交并消费后再 abort；I: 用 remove_node 摘不掉 → 节点泄漏；
    P: 节点归档 + 已消费折入父级。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    rid = acb.reservation_ids[0]
    assert h.commit_batch(acb, [Write("r", {"v": 1})]) == Status.OK
    assert h.budget.consume(rid, 1) == Status.OK
    node_id = acb.budget_node.node_id

    h.abort(acb)

    assert node_id not in h.budget._nodes, "已消费节点必须归档，不得残留"
    run = h.budget._nodes["run"]
    assert run.folded_consumed == 1, "已消费必须折入父级"


# ================================================================ C2
def test_c2_finish_settles_and_releases():
    """R: begin→commit→consume→finish；I: 只改状态不结算；P: 预留状态/节点/槽位。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    rid = acb.reservation_ids[0]
    assert h.commit_batch(acb, [Write("r", {"v": 1})]) == Status.OK
    assert h.budget.consume(rid, 1) == Status.OK
    node_id = acb.budget_node.node_id

    assert h.finish(acb) == Status.OK

    assert acb.state is TaskState.DONE
    assert acb.budget_node is None, "终态必须归档节点"
    assert node_id not in h.budget._nodes
    assert acb.id not in h.slots.active_keys(), "槽位必须释放"
    resv = h.budget._reservations[rid][1]
    # settle 后归档会把条目置 ARCHIVED（终态摘除）；关键是**已消费部分留账**。
    assert resv.state in ("SETTLED", "ARCHIVED")
    assert resv.consumed == 1, "已消费部分必须留账"


def test_c2_finish_without_apply_refunds():
    """R: begin→finish（无写）；I: 用 consume 代替 settle；P: remaining 不回升。"""
    h = make_harness(run_limits=100)
    acb = _begin(h, min_budget=10)
    assert h.budget.remaining("run") == 90
    assert h.finish(acb) == Status.OK
    assert h.budget.remaining("run") == 100


def test_c2_double_finish_is_noop():
    """R: 连续两次 finish；I: 重复结算；P: 预算事件数不再增长。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    assert h.finish(acb) == Status.OK
    n = len(h.budget.budget_events())
    r = h.finish(acb)
    assert r == Status.OK
    assert len(h.budget.budget_events()) == n


# ================================================================ C3
def test_c3_same_key_same_content_returns_same_receipt():
    """R: 同键同内容提交两次；I: 不查外部键 → 第二次也真写；P: 事件数/收据 id。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    r1 = h.commit_batch(acb, [Write("r", {"v": 1})], idempotency_key="K")
    assert r1 == Status.OK and not r1.detail.get("dedup")
    r2 = h.commit_batch(acb, [Write("r", {"v": 1})], idempotency_key="K")

    assert r2 == Status.OK and r2.detail.get("dedup") is True
    assert r2.detail["receipt_ids"] == r1.detail["receipt_ids"]
    assert len(_commit_events(h)) == 1, "同键同内容不得产生第二条提交事件"
    assert h.workspace.version("r") == 1


def test_c3_same_key_different_content_conflicts():
    """R: 同键异内容；I: 静默覆盖；P: 返回 CONFLICT 且 version 不变。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    assert h.commit_batch(acb, [Write("r", {"v": 1})], idempotency_key="K") == Status.OK
    r = h.commit_batch(acb, [Write("r", {"v": 2})], idempotency_key="K")

    assert r == Status.CONFLICT
    assert r.detail["reason"] == "idempotency_key_reused_with_different_content"
    assert h.workspace.version("r") == 1
    assert h.workspace.get("r") == {"v": 1}


def test_c3_replay_after_other_token():
    """R: 新 token（模拟重启后）用同键重放；I: 依赖旧 token 才能去重；P: 仍返回同收据。"""
    h = make_harness(run_limits=100)
    acb1 = _begin(h, agent_id="a1", intent_key="i1")
    r1 = h.commit_batch(acb1, [Write("r", {"v": 1})], idempotency_key="K")
    assert r1 == Status.OK

    acb2 = _begin(h, agent_id="a2", intent_key="i2")     # 不同 run/subgoal
    r2 = h.commit_batch(acb2, [Write("r", {"v": 1})], idempotency_key="K")

    assert r2 == Status.OK and r2.detail.get("dedup") is True
    assert r2.detail["receipt_ids"] == r1.detail["receipt_ids"], (
        "收据必须从事件读，不能用新 ACB 重算（否则 id 不同）")
    assert h.workspace.version("r") == 1


def test_c3_replay_refreshes_base_version():
    """评审 P1-4：外部键重放命中后必须刷新 base_version（否则 gate 误判 stale）。"""
    h = make_harness(run_limits=100)
    acb1 = _begin(h, agent_id="a1", intent_key="i1")
    assert h.commit_batch(acb1, [Write("r", {"v": 1})], idempotency_key="K") == Status.OK

    acb2 = _begin(h, agent_id="a2", intent_key="i2")
    assert acb2.base_version.get("r") is None
    r2 = h.commit_batch(acb2, [Write("r", {"v": 1})], idempotency_key="K")

    assert r2 == Status.OK and r2.detail.get("dedup") is True
    assert acb2.base_version.get("r") == h.workspace.version("r"), (
        "重放命中也要把 ACB 的 base_version 刷新到当前版本")


# ================================================================ C4
def test_c4_expired_token_reclaimed():
    """R: token 超时 → tick；I: 无回收；P: 状态/预算/内存三项。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    acb = _begin(h, min_budget=10, ttl_ms=1000)
    assert h.budget.remaining("run") == 90

    clock.advance(1001)
    h.tick()

    assert acb.state is TaskState.CANCELLED
    assert h.budget.remaining("run") == 100, "预算必须退还"
    assert acb.id not in h.acbs, "回收后必须移出内存"
    assert any(k == acb.id and r == "token_expired" for k, r in h.dispatch.skipped)


def test_c4_finished_token_not_reaped():
    """R: finish 后超时；I: 二次结算；P: 预算事件数不变。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    acb = _begin(h, ttl_ms=1000)
    h.finish(acb)
    n = len(h.budget.budget_events())
    clock.advance(5000)
    h.tick()
    assert len(h.budget.budget_events()) == n


def test_c4_inadmissible_token_not_reaped():
    """评审 P1-5：aging 判 INADMISSIBLE（可重试）后，token 过期不得转终态/清内存。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    h.aging_ms = 500
    acb = _begin(h, ttl_ms=1000)
    clock.advance(600)
    h.tick()
    assert acb.state is TaskState.INADMISSIBLE

    clock.advance(1000)
    h.tick()
    assert acb.state is TaskState.INADMISSIBLE, "可重试态不得被 token reaper 终结"
    assert acb.id in h.acbs


def test_c4_token_with_active_claim_not_reaped():
    """评审 P1-6：有活跃 claim（提交临界区）时不得退款。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    acb = _begin(h, ttl_ms=1000)
    claim = h.scheduler.claim(acb.id, "w1")
    assert claim is not None

    clock.advance(2000)
    h.tick()
    assert acb.state is TaskState.ADMITTED, "在途 claim 期间不得回收"

    h.scheduler.release_claim(acb.id, "w1")
    h.tick()
    assert acb.state is TaskState.CANCELLED


def test_c4_non_token_acb_untouched():
    """R: 普通 ACB（无 rsc_token）超时；I: reaper 误伤；P: 状态不变。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    acb = _begin(h, token=False)
    acb.spec["token_deadline_ms"] = 0          # 即使有 deadline，也不该被 token reaper 处理
    clock.advance(5000)
    h.tick()
    assert acb.state is TaskState.ADMITTED
    assert acb.id in h.acbs


# ================================================================ C5
def test_c5_restore_acb_without_global_epoch_bump():
    """R: 从快照恢复 token；I: 恢复时推进全局 epoch；P: epoch 值 + 预算重绑。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    h.checkpoints.save(acb)
    ref = h.checkpoints.latest_ref_for_acb(acb.id)
    assert ref

    epoch0 = h.intents.get_epoch()
    data = h.checkpoints.load_strict(ref)
    restored = h.recovery.restore_acb(data, bump_epoch=False)

    assert restored.id == acb.id
    assert restored.state is TaskState.ADMITTED
    assert restored.budget_node is not None, "预算节点必须重绑"
    assert restored.reservation_ids, "预留必须重绑"
    assert h.intents.get_epoch() == epoch0, "懒恢复不得推进全局 epoch"


def test_c5_lazy_restore_does_not_reset_budget_tree():
    """R: 同进程懒恢复一个 token；I: 触发 `_replay_budget_events` 重置基线；
    P: 另一个在途 token 的节点仍在。"""
    h = make_harness(run_limits=100)
    acb = _begin(h, agent_id="a1", intent_key="i1")
    other = _begin(h, agent_id="a2", intent_key="i2")
    other_node = other.budget_node.node_id
    h.checkpoints.save(acb)
    ref = h.checkpoints.latest_ref_for_acb(acb.id)
    data = h.checkpoints.load_strict(ref)

    restored = h.recovery.restore_acb(data, bump_epoch=False)

    assert restored.budget_node is not None
    assert other_node in h.budget._nodes, "懒恢复不得重置整棵预算树"
    assert other.budget_node is not None and other.reservation_ids


def test_c5_bump_epoch_true_keeps_legacy_semantics():
    """负对照：默认 `bump_epoch=True` 仍推进 epoch（崩溃恢复的既有语义）。"""
    h = make_harness(run_limits=100)
    acb = _begin(h)
    h.checkpoints.save(acb)
    ref = h.checkpoints.latest_ref_for_acb(acb.id)
    data = h.checkpoints.load_strict(ref)
    epoch0 = h.intents.get_epoch()

    h.recovery.restore_acb(data)                       # 默认 True

    assert h.intents.get_epoch() == epoch0 + 1
