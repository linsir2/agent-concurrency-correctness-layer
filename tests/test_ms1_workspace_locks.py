"""MS1 出口条件：workspace manifest + 每资源锁 + 租约时钟。

对应 docs/s2-workspace-and-locks.md §8 的 4 组判据：
- T4：提交点 = WAL（种子 + 提交都被重放，前像随 WAL 重建）
- T5：读集合陈旧 → REJECT_STALE_READ，且 detail.resource 指向被读资源
- T5b：租约过期由注入时钟判定（半开区间），renew 检查过期并续期
- T7：每资源锁按 LOCK_KEY 全序获取 → 反向顺序不死锁
"""

from __future__ import annotations

import threading

import pytest
from conftest import _acb, admit, make_harness, seed

from harness import (
    Harness,
    IntentRegistry,
    Mode,
    Status,
    WalEventLog,
    WorkspaceManager,
    Write,
)
from harness.clock import ManualClock
from harness.storage.blob import FileBlobStore


# ---------------------------------------------------------------- T4
@pytest.mark.ms1
def test_ms1_commit_point_is_wal(tmp_path):
    """T4：提交点 = WAL；丢弃实例用同一 WAL + 同一 blob 目录重建（含前像）。"""
    wal_path = tmp_path / "wal.log"
    blobs = FileBlobStore(tmp_path / "blobs")
    log = WalEventLog(wal_path)
    intents = IntentRegistry()
    ws = WorkspaceManager(log, intents, blobs=blobs)
    ws.put("r", "v0")
    v0 = ws.version("r")                     # put 推进版本 → 1
    h = Harness(event_log=log, intents=intents, blobs=blobs, workspace=ws)
    acb = _acb(h, "r")
    acb.base_version["r"] = v0
    assert h.commit_batch(acb, [Write("r", content={"v": 1})]) == Status.OK
    hash_before = ws.content_hash("r")
    assert ws.version("r") == v0 + 1
    log.close()

    # 丢弃实例，用同一 WAL + 同一 blob 目录重建
    log2 = WalEventLog(wal_path)
    ws2 = WorkspaceManager(log2, intents, blobs=blobs)
    assert ws2.version("r") == v0 + 1        # 种子 + 提交 都被重放
    assert ws2.get("r") == {"v": 1}
    assert ws2.content_hash("r") == hash_before
    # D-6：前像也重建（version-1 的内容 = 种子内容）
    assert ws2._versions.get("r", v0) == "v0"


# ---------------------------------------------------------------- T5
@pytest.mark.ms1
def test_ms1_occ_readset_rejects_stale():
    """T5：读集合陈旧 → REJECT_STALE_READ，且 detail.resource 指向被读资源。"""
    h = make_harness()
    v_r = seed(h, "r", "v0")
    v_dep = seed(h, "dep", "d0")
    plan = h.planner.plan("T", [{"task_id": "T1"}, {"task_id": "T2"}])
    h.plans["T"] = plan
    a = admit(h, "T", plan, "T1")
    b = admit(h, "T", plan, "T2")

    a.base_version["r"] = v_r
    a.read_set["dep"] = v_dep                # 读过 dep，但没读 r
    b.base_version["dep"] = v_dep
    assert h.commit_batch(b, [Write("dep", content="B")]) == Status.OK
    assert h.workspace.version("dep") == v_dep + 1

    r = h.commit_batch(a, [Write("r", content="A")])
    assert r == Status.REJECT_STALE_READ     # base(r) 通过 → 隔离到读集合检查
    assert r.detail["resource"] == "dep"
    assert h.workspace.version("r") == v_r
    assert h.workspace.get("r") == "v0"


# ---------------------------------------------------------------- T5b
@pytest.mark.ms1
def test_ms1_lease_expiry_uses_injected_clock():
    """T5b：租约过期由注入时钟判定（半开区间），renew 检查过期并续期。"""
    clock = ManualClock(1_000_000)
    h = make_harness(clock=clock)
    seed(h, "r", "v0")
    acb = _acb(h, "r")
    lease = h.intents.reserve("r", acb.id, 0, 1000)
    assert lease.expires_at == 1_001_000

    clock.advance(999)
    assert h.intents.is_valid(lease) is True
    clock.advance(1)
    assert h.intents.is_valid(lease) is False   # 半开区间：now == expires_at 即失效
    assert h.intents.renew(lease) is False
    assert h.workspace.prepare(acb, Write("r"), lease, Mode.COMMIT) == Status.REJECT_STALE

    # 对照：未过期时 renew 成功并续期
    lease2 = h.intents.reserve("r", acb.id, 0, 1000)
    clock.advance(500)
    assert h.intents.renew(lease2) is True
    assert lease2.expires_at == clock.now_ms() + 1000


# ---------------------------------------------------------------- T7
@pytest.mark.ms1
@pytest.mark.serial
def test_ms1_lock_order_no_deadlock():
    """T7：每资源锁按 LOCK_KEY 全序获取 → 反向顺序不死锁。

    反假通过：两线程都结束、无异常、结果恰为 {OK, CONFLICT}、
    两资源版本各 v0+1、lock_for("a") 与 lock_for("b") 不是同一把锁（证明是每资源锁而非单锁）。
    """
    h = make_harness()
    h.lock_timeout_s = 30.0                  # 让真死锁表现为「线程未结束」而非超时返回
    v_a = seed(h, "a", 0)
    v_b = seed(h, "b", 0)
    errors, results = [], []
    barrier = threading.Barrier(2)

    def worker(order):
        try:
            acb = _acb(h, order[0])
            acb.base_version.update({"a": v_a, "b": v_b})
            barrier.wait(timeout=5)
            results.append(h.commit_batch(
                acb, [Write(order[0], content=1), Write(order[1], content=1)]))
        except BaseException as e:  # noqa: BLE001  # 线程内捕获一切，交由主线程断言
            errors.append(e)

    ts = [threading.Thread(target=worker, args=(("a", "b"),)),
          threading.Thread(target=worker, args=(("b", "a"),))]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=5)

    assert all(not t.is_alive() for t in ts), "线程未在期限内结束 → 疑似死锁"
    assert not errors
    # OCC 语义：两个线程用同一 base_version，必然一个 OK 一个 CONFLICT
    assert sorted(r.status.value for r in results) == ["CONFLICT", "OK"], results
    assert h.workspace.version("a") == v_a + 1 and h.workspace.version("b") == v_b + 1
    assert h.workspace.lock_for("a") is not h.workspace.lock_for("b")   # 每资源一把锁


# ---------------------------------------------------------------- 回归
@pytest.mark.ms1
def test_ms1_none_content_rejected():
    """None 内容被拒（INVALID_INPUT），且不推进版本、不改内容。"""
    h = make_harness()
    seed(h, "r", "v0")
    acb = _acb(h, "r")
    r = h.commit_batch(acb, [Write("r", content=None)])
    assert r == Status.INVALID_INPUT
    assert r.detail["reason"] == "none_content"
    assert h.workspace.get("r") == "v0"


@pytest.mark.ms1
def test_ms1_duplicate_resource_rejected():
    """同批重复 resource 被拒（INVALID_INPUT），不产生任何写入。"""
    h = make_harness()
    v0 = seed(h, "r", "v0")
    acb = _acb(h, "r")
    acb.base_version["r"] = v0
    r = h.commit_batch(acb, [Write("r", content="a"), Write("r", content="b")])
    assert r == Status.INVALID_INPUT
    assert r.detail["reason"] == "duplicate_resource"
    assert h.workspace.get("r") == "v0"


@pytest.mark.ms1
def test_ms1_bytes_roundtrip_preserves_type():
    """bytes 内容往返仍是 bytes（魔数前缀防被 json.loads 误解码成 dict）。"""
    h = make_harness()
    payload = b'{"a":1}'
    h.workspace.put("b", payload)
    out = h.workspace.get("b")
    assert out == payload
    assert isinstance(out, bytes)


@pytest.mark.ms1
def test_ms1_empty_batch_noop():
    """空批返回 noop（OK + noop=True）。"""
    h = make_harness()
    acb = _acb(h, "r")
    r = h.commit_batch(acb, [])
    assert r == Status.OK
    assert r.detail["noop"] is True


@pytest.mark.ms1
def test_ms1_content_hash_missing_is_empty():
    """content_hash 对不存在的资源返回空串（而非 None/异常）。"""
    h = make_harness()
    assert h.workspace.content_hash("nope") == ""


@pytest.mark.ms1
def test_ms1_readset_lock_contention_upstream_failed():
    """读集合资源被他人持锁 → 提交超时返回 UPSTREAM_FAILED（不写入、不死锁）。"""
    h = make_harness()
    h.lock_timeout_s = 0.1
    v_r = seed(h, "r", "v0")
    v_dep = seed(h, "dep", "d0")
    acb = _acb(h, "r")
    acb.base_version["r"] = v_r
    acb.read_set["dep"] = v_dep

    results: list = []
    lock = h.workspace.lock_for("dep")
    lock.acquire()
    t = threading.Thread(target=lambda: results.append(
        h.commit_batch(acb, [Write("r", content="A")])))
    t.start()
    t.join(timeout=5)
    lock.release()

    assert not t.is_alive()
    assert results and results[0] == Status.UPSTREAM_FAILED
    assert h.workspace.get("r") == "v0"
