"""S24-8：可复制写者租约（`MemoryLeaseStore` + Harness 接线）。

RIPR：
- Reachability：租约走 `lease_store` 分支（不读 `writer_lease.json`）；
- Infection：第二个写者 / 过期接管 / epoch 推进确实改变 grant 与内部 epoch；
- Propagation：旧写者的 `commit_batch` 返回 `UPSTREAM_FAILED` 且事件 tail 不变；
- Revealability：`reason=writer_lease_held|writer_lease_unavailable`、`lease_epoch` 可见。

本文件**不依赖 pysyncobj**（真实 Raft 见 `test_s24_raft_lease.py`）。
"""

from __future__ import annotations

import pathlib

from conftest import make_harness

from harness import RscSession
from harness.clock import ManualClock
from harness.facade import Wait
from harness.storage.lease import (
    ACQUIRED,
    FENCED,
    HELD,
    RENEWED,
    LeaseState,
    MemoryLeaseStore,
)


# ---------------------------------------------------------------- 纯状态机
def test_first_acquire_and_second_held():
    st = LeaseState()
    g = st.acquire(owner="a", instance="i1", now_ms=1000, ttl_ms=100)
    assert (g.outcome, g.epoch) == (ACQUIRED, 1)
    g2 = st.acquire(owner="b", instance="i2", now_ms=1010, ttl_ms=100)
    assert g2.outcome == HELD and not g2.ok
    assert st.epoch == 1


def test_same_instance_renews_same_epoch():
    st = LeaseState()
    st.acquire(owner="a", instance="i1", now_ms=1000, ttl_ms=100)
    g = st.acquire(owner="a", instance="i1", now_ms=1050, ttl_ms=100)
    assert g.outcome == RENEWED and g.epoch == 1


def test_expiry_takeover_bumps_epoch_and_fences_old():
    st = LeaseState()
    st.acquire(owner="a", instance="i1", now_ms=1000, ttl_ms=100)
    g = st.acquire(owner="b", instance="i2", now_ms=1101, ttl_ms=100,
                   expected_epoch=1)
    assert (g.outcome, g.epoch) == (ACQUIRED, 2)
    old = st.renew(owner="a", instance="i1", epoch=1, now_ms=1102, ttl_ms=100)
    assert old.outcome == FENCED and not old.ok
    assert not st.authorizes(owner="a", instance="i1", epoch=1, now_ms=1102)


def test_wild_expected_epoch_is_fenced():
    st = LeaseState()
    st.acquire(owner="a", instance="i1", now_ms=1000, ttl_ms=100)
    g = st.acquire(owner="b", instance="i2", now_ms=1010, ttl_ms=100,
                   expected_epoch=7)
    assert g.outcome == FENCED


def test_release_keeps_epoch_and_next_acquire_bumps():
    st = LeaseState()
    st.acquire(owner="a", instance="i1", now_ms=1000, ttl_ms=100)
    assert st.release(owner="a", instance="i1", epoch=1).outcome == "released"
    g = st.acquire(owner="b", instance="i2", now_ms=1001, ttl_ms=100,
                   expected_epoch=1)
    assert (g.outcome, g.epoch) == (ACQUIRED, 2)
    # 旧写者的续租仍然被拒（release 不清零 epoch）
    assert st.renew(owner="a", instance="i1", epoch=1, now_ms=1002,
                    ttl_ms=100).outcome == FENCED


def test_advance_keeps_supervisor_active_but_fences_others():
    st = LeaseState()
    st.acquire(owner="a", instance="i1", now_ms=1000, ttl_ms=100)
    g = st.advance(owner="a", instance="i1", epoch=1, now_ms=1050, ttl_ms=100)
    assert (g.outcome, g.epoch) == ("advanced", 2)
    assert st.authorizes(owner="a", instance="i1", epoch=2, now_ms=1060)
    assert st.renew(owner="a", instance="i1", epoch=1, now_ms=1060,
                    ttl_ms=100).outcome == FENCED


# ---------------------------------------------------------------- Harness
def _session(h) -> RscSession:
    return RscSession(h, tenant="t", min_budget=1)


def _commit(session, tok, value: str, key: str):
    return session.apply(tok.token, [("a.md", value)], idempotency_key=key,
                         agent_id="a")


def test_second_harness_is_held_and_cannot_write():
    store = MemoryLeaseStore()
    clock = ManualClock()
    h1 = make_harness(lease_store=store, clock=clock)
    h2 = make_harness(lease_store=store, clock=clock)
    assert h1.state.writer_lease_ok is True
    assert h2.state.writer_lease_ok is False

    s1 = _session(h1)
    tok = s1.begin("a", "g", writes=["a.md"])
    assert _commit(s1, tok, "v1", "k1")

    s2 = _session(h2)
    tok2 = s2.begin("b", "g2", writes=["a.md"])
    before = h2.event_log.tail()
    denied = _commit(s2, tok2, "v2", "k2")
    assert isinstance(denied, Wait)
    assert denied.reason == "writer_lease_held"
    assert h2.event_log.tail() == before          # 未 append


def test_takeover_fences_old_writer_commit():
    store = MemoryLeaseStore()
    clock = ManualClock()
    h1 = make_harness(lease_store=store, clock=clock, lease_ms=1000)
    s1 = _session(h1)
    tok = s1.begin("a", "g", writes=["a.md"])
    assert _commit(s1, tok, "v1", "k1")

    clock.advance(1001)
    h2 = make_harness(lease_store=store, clock=clock, lease_ms=1000)
    assert h2.state.writer_lease_ok is True
    assert store.read_epoch() == 2

    before = h1.event_log.tail()
    denied = _commit(s1, tok, "v2", "k2")
    assert isinstance(denied, Wait)
    assert denied.reason == "writer_lease_held"
    assert h1.event_log.tail() == before
    # 旧 epoch 的续租被拒（不是静默成功）
    old = store.renew(owner=h1.state.writer_id, instance=h1.state.lease_instance,
                      epoch=1, now_ms=clock.now_ms(), ttl_ms=1000)
    assert old.outcome == FENCED


def test_store_failure_is_fail_closed_and_no_file_fallback(tmp_path):
    root = pathlib.Path(tmp_path) / "ckpt"
    store = MemoryLeaseStore()
    h = make_harness(lease_store=store, checkpoint_root=str(root))
    s = _session(h)
    tok = s.begin("a", "g", writes=["a.md"])
    assert _commit(s, tok, "v1", "k1")

    store.set_fail(True)
    before = h.event_log.tail()
    denied = _commit(s, tok, "v2", "k2")
    assert isinstance(denied, Wait)
    assert denied.reason == "writer_lease_unavailable"
    assert h.event_log.tail() == before
    # 绝不回退到本地文件租约
    assert not (root / "writer_lease.json").exists()


def test_bump_epoch_advances_store_and_self_stays_valid():
    store = MemoryLeaseStore()
    clock = ManualClock()
    h = make_harness(lease_store=store, clock=clock)
    s = _session(h)
    tok = s.begin("a", "g", writes=["a.md"])
    assert h.state.writer_epoch == 1
    assert h.bump_epoch() == 2
    assert store.read_epoch() == 2
    assert h.state.writer_epoch == 2
    # 监督者自己仍然可写
    assert _commit(s, tok, "v1", "k1")
    # 旧 epoch 的视图被拒
    assert store.renew(owner=h.state.writer_id, instance=h.state.lease_instance,
                       epoch=1, now_ms=clock.now_ms(), ttl_ms=60_000).outcome \
        == FENCED


def test_release_makes_lease_available_to_next_host():
    store = MemoryLeaseStore()
    clock = ManualClock()
    h1 = make_harness(lease_store=store, clock=clock)
    h1.leases.release_writer_lease()             # 显式会话结束
    h2 = make_harness(lease_store=store, clock=clock)
    assert h2.state.writer_lease_ok is True
    assert h2.state.writer_epoch == 2
