"""S24-10：确定性原语（seeded uuid/now）与单线程逐字节重放。

RIPR：
- Reachability：事件 id / 时间戳 / run_id / 预算 id / WAL generation 都来自熵源；
- Infection：同 seed 的两次执行产生**逐字节相同**的 WAL；换 seed 必须不同；
- Propagation：默认（无 entropy）仍走 uuid4/墙钟，两次运行不同；
- Revealability：确定性模式拒绝 auth_secret / pool 调度 / merge queue（fail-closed）。
"""

from __future__ import annotations

import itertools
import pathlib

import pytest
from conftest import decision, hitl

from harness import RscSession
from harness.entropy import Deterministic
from harness.event_log import WalEventLog
from harness.harness import Harness
from harness.types import TaskState


def _run(root: pathlib.Path, seed: str | None):
    """固定场景：两个写 + finish；返回 WAL 原始字节与关键 id。"""
    root.mkdir(parents=True)
    ent = Deterministic(seed) if seed is not None else None
    wal = WalEventLog(str(root / "wal.log"), fsync=False,
                      clock=ent, entropy=ent)
    h = Harness(event_log=wal, checkpoint_root=str(root),
                entropy=ent, run_limits=10)
    session = RscSession(h, tenant="t", min_budget=1,
                         state_dir=str(root / "idx"))
    tok = session.begin("agent-a", "goal", writes=["a.md"])
    assert hasattr(tok, "token")
    first = session.apply(tok.token, [("a.md", "v1")],
                          idempotency_key="k1", agent_id="agent-a")
    second = session.apply(tok.token, [("a.md", "v2")],
                           idempotency_key="k2", agent_id="agent-a")
    session.finish(tok.token)
    wal.close()
    events = h.event_log.all()
    return {
        "bytes": (root / "wal.log").read_bytes(),
        "event_ids": [e.event_id for e in events],
        "ts": [e.ts for e in events],
        "run_ids": sorted({e.run_id for e in events if e.run_id}),
        "first_receipt": getattr(first, "receipt_id", ""),
        "second_receipt": getattr(second, "receipt_id", ""),
    }


# ---------------------------------------------------------------- entropy
def test_domains_are_isolated_and_counters_monotonic():
    ent = Deterministic("seed-1")
    a = [ent.uuid("event_id") for _ in range(100)]
    b = [ent.uuid("run_id") for _ in range(100)]
    assert len(set(a)) == 100 and len(set(b)) == 100
    assert not (set(a) & set(b))
    # 同域同序号可重建
    ent2 = Deterministic("seed-1")
    assert [ent2.uuid("event_id") for _ in range(3)] == a[:3]


def test_clock_is_strictly_monotonic():
    ent = Deterministic("s", start_ms=5_000, tick_ms=7)
    values = [ent.now_ms() for _ in range(1_000)]
    assert all(b > a for a, b in itertools.pairwise(values))
    assert values[0] == 5_007


def test_tokens_are_not_reused_across_domains():
    ent = Deterministic("s")
    assert ent.token("writer_instance") != ent.token("wal_generation")


# ---------------------------------------------------------------- 重放
def test_same_seed_replays_identical_wal(tmp_path):
    a = _run(tmp_path / "a", "replay-seed")
    b = _run(tmp_path / "b", "replay-seed")
    assert a["bytes"] == b["bytes"]                 # 逐字节
    assert a["event_ids"] == b["event_ids"]
    assert a["ts"] == b["ts"]
    assert a["run_ids"] == b["run_ids"]
    assert a["first_receipt"] == b["first_receipt"]
    assert a["second_receipt"] == b["second_receipt"]


def test_different_seed_changes_bytes(tmp_path):
    a = _run(tmp_path / "a", "seed-1")
    b = _run(tmp_path / "b", "seed-2")
    assert a["bytes"] != b["bytes"]
    assert a["event_ids"] != b["event_ids"]


def test_default_mode_is_still_random(tmp_path):
    a = _run(tmp_path / "a", None)
    b = _run(tmp_path / "b", None)
    assert a["bytes"] != b["bytes"]
    assert a["event_ids"] != b["event_ids"]


def test_event_ids_are_deterministic_hex(tmp_path):
    got = _run(tmp_path / "a", "s")
    assert got["event_ids"] and all(len(x) == 32 for x in got["event_ids"])


# ---------------------------------------------------------------- HITL
def _hitl_run(root: pathlib.Path, seed: str) -> bytes:
    root.mkdir(parents=True)
    ent = Deterministic(seed)
    wal = WalEventLog(str(root / "wal.log"), fsync=False, clock=ent, entropy=ent)
    h = Harness(event_log=wal, checkpoint_root=str(root), entropy=ent,
                run_limits=10)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    plan = h.planner.plan("run-1", [{
        "task_id": "t", "logical_subgoal_id": "g", "min_budget": 1}])
    acb = h._admit_and_register(plan, "t")
    h.run_task(acb, [decision(1, hitl())])
    assert acb.state is TaskState.WAITING_APPROVAL
    wal.close()
    return (root / "wal.log").read_bytes()


def test_hitl_same_seed_replays_identical_wal(tmp_path):
    """`approval_id` 曾是 uuid4 → HITL 场景不可重放（S24-P0 回归）。"""
    a = _hitl_run(tmp_path / "a", "hitl-seed")
    b = _hitl_run(tmp_path / "b", "hitl-seed")
    assert a == b


def test_hitl_different_seed_differs(tmp_path):
    assert _hitl_run(tmp_path / "a", "seed-a") != _hitl_run(tmp_path / "b", "seed-b")


# ---------------------------------------------------------------- 边界
def test_entropy_rejects_real_clock():
    with pytest.raises(ValueError):
        Harness(clock=object(), entropy=Deterministic("s"))


def test_entropy_rejects_auth_secret(monkeypatch):
    monkeypatch.delenv("HARNESS_AUTH_SECRET", raising=False)
    with pytest.raises(ValueError):
        Harness(entropy=Deterministic("s"), auth_secret="k")


def test_entropy_rejects_pool_scheduler():
    with pytest.raises(ValueError):
        Harness(entropy=Deterministic("s"), scheduler_mode="pool")


def test_entropy_rejects_merge_queue():
    h = Harness(entropy=Deterministic("s"))
    with pytest.raises(ValueError):
        RscSession(h, merge_queue={"capacity": 4})
