"""S15 D1：Ingress 幂等（`docs/s15-protocol-and-production.md` §3.1）。

覆盖 IETF `Idempotency-Key` 的三条硬语义：
- 空 key → 拒绝
- 同 key + 同指纹 → **重放**（不新建）
- 同 key + 异指纹 → **CONFLICT**
外加「跨进程仍有效」（指纹落在 EventLog，不靠内存 dict）。
"""

from __future__ import annotations

from conftest import make_harness

from harness import EventKind, Harness, Status
from harness.ingress import fingerprint


def _plan(h: Harness, run_id: str = "R1") -> dict:
    return h.planner.plan(run_id, [{"task_id": "T1", "min_budget": 10}])


def test_d1_empty_key_is_rejected():
    """IETF：空/缺失 key 必须拒绝（否则等于没有幂等保护）。"""
    h = make_harness()
    r = h.ingress.submit(idempotency_key="", plan=_plan(h))
    assert r.status is Status.INVALID_INPUT
    assert r.reason == "idempotency_key_required"
    assert r.run_id == ""
    # 不得写入任何受理事件
    assert not [e for e in h.event_log.all()
                if e.kind is EventKind.INGRESS_ACCEPTED]


def test_d1_same_key_same_payload_replays_without_new_run():
    """同 key + 同指纹 → `replayed=True`，且**不新建** Run（事件数不变）。"""
    h = make_harness()
    plan = _plan(h)
    first = h.ingress.submit(idempotency_key="k1", plan=plan)
    assert first.status is Status.OK and first.replayed is False
    assert first.run_id == "R1"

    n_before = len(h.event_log.all())
    second = h.ingress.submit(idempotency_key="k1", plan=plan)
    assert second.status is Status.OK
    assert second.replayed is True
    assert second.run_id == "R1"
    assert len(h.event_log.all()) == n_before, "重放不得写新事件（等于重复受理）"


def test_d1_same_key_different_payload_conflicts():
    """同 key + **不同**指纹 → `CONFLICT`（对齐 IETF 409），也不新建。"""
    h = make_harness()
    p1 = _plan(h, "R1")
    p2 = h.planner.plan("R2", [{"task_id": "T1", "min_budget": 999}])
    h.ingress.submit(idempotency_key="k1", plan=p1)

    n_before = len(h.event_log.all())
    r = h.ingress.submit(idempotency_key="k1", plan=p2)
    assert r.status is Status.CONFLICT
    assert r.reason == "idempotency_key_reused_with_different_payload"
    assert len(h.event_log.all()) == n_before


def test_d1_fingerprint_is_order_insensitive():
    """指纹对**字段顺序**不敏感（否则同内容会被误判为冲突）。"""
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})
    assert fingerprint({"a": 1}) != fingerprint({"a": 2})


def test_d1_idempotency_survives_new_harness():
    """**跨进程**：新 Harness 共享同一 EventLog → 仍能识别重放（不依赖内存）。"""
    h = make_harness()
    plan = _plan(h)
    h.ingress.submit(idempotency_key="k1", plan=plan)

    h2 = Harness(event_log=h.event_log, checkpoints=h.checkpoints)
    h2.policy.default_token = h2.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    r = h2.ingress.submit(idempotency_key="k1", plan=plan)
    assert r.replayed is True and r.run_id == "R1"


def test_d1_negative_control_memory_dict_would_duplicate():
    """负对照：若把去重放在**内存 dict** 上，新实例会重复受理（证明持久化必要）。"""
    h = make_harness()
    plan = _plan(h)
    h.ingress.submit(idempotency_key="k1", plan=plan)

    # 模拟「内存态去重」：清掉事件后，新实例看不到任何记录 → 会再受理一次
    empty = make_harness()
    r = empty.ingress.submit(idempotency_key="k1", plan=plan)
    assert r.replayed is False, (
        "无持久记录时应重新受理（这正是内存态去重的缺陷：重启后会重复建 Run）")
    accepted = [e for e in empty.event_log.all()
                if e.kind is EventKind.INGRESS_ACCEPTED]
    assert len(accepted) == 1


def test_d1_submit_command_routes_to_ingress():
    """`submit_command` 不再是无条件 `OK` 的空壳：`INGRESS_SUBMIT` 真的路由。"""
    h = make_harness()
    plan = _plan(h)
    st = h.run_coordinator.submit_command({
        "kind": "INGRESS_SUBMIT", "idempotency_key": "k2", "plan": plan})
    assert st is Status.OK
    st2 = h.run_coordinator.submit_command({
        "kind": "INGRESS_SUBMIT", "idempotency_key": "", "plan": plan})
    assert st2 is Status.INVALID_INPUT, "空 key 必须被拒绝，不能静默 OK"
