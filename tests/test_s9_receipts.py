"""S9b 收据链测试（`docs/s9-persistence-and-receipts.md` v1.1 §2.3）。

判据：
- **S9-3** 同帧：`WalEventLog` 的同一帧内既有 `RESOURCE_COMMITTED` 也有
  `RECEIPT_ISSUED`；负对照把收据拆到第二次 `append_batch` → 两帧 → 断言失败。
- **S9-4** 去重：`append_batch` 成功、投影前抛 → 重试不新增任何收据/提交事件。
- **S9-5** 并发：同 scope 不同资源 + barrier 在「读完 head、append 前」；
  正对照（不同 scope 必须能并行完成，证明不是全局锁）；
  负对照（`nullcontext()` 替 scope 锁 → 链分叉 → verify False）。
- 未签名三态：无 auth_secret → `verify_chain_detailed` 返回 `None` 而非 `False`。
"""

from __future__ import annotations

import copy
import threading
from contextlib import nullcontext

import pytest

import harness.kernel.commit as commit_module
from harness import ACB, EventKind, Harness, Status, Write
from harness.event_log import WalEventLog
from harness.runtime import CrashInjected
from harness.spec.receipt import (
    build_receipt,
    receipt_hash,
    verify_chain,
    verify_chain_detailed,
)


# ---------------------------------------------------------------- helpers
def _acb(h: Harness, run_id: str, subgoal: str, task_id: str = "T-S9") -> ACB:
    """构造未准入的 ACB（收据链不需要预算/能力，只需 run/子目标作用域）。"""
    return ACB(id=f"A-{run_id}-{subgoal}", run_id=run_id,
               task_id=task_id, logical_subgoal_id=subgoal)


def _staged(h: Harness, acb: ACB, resource: str, content=None) -> None:
    h.workspace.stage(acb, resource, {"v": 1} if content is None else content)


def _count(h: Harness, kind: EventKind) -> int:
    return len([e for e in h.event_log.all() if e.kind == kind])


def _same_frame(wal: WalEventLog) -> bool:
    """结构判据：存在某一帧同时包含提交事件与收据事件。"""
    for frame in wal._wal.frames():
        kinds = {rec["kind"] for rec in frame}
        if "RESOURCE_COMMITTED" in kinds and "RECEIPT_ISSUED" in kinds:
            return True
    return False


# ================================================================ S9-3 同帧
def test_s9_3_receipt_in_same_wal_frame(tmp_path):
    """S9-3：`WalEventLog` 同一帧内同时含 `RESOURCE_COMMITTED` + `RECEIPT_ISSUED`。"""
    wal = WalEventLog(str(tmp_path / "wal.log"))
    h = Harness(event_log=wal)
    acb = _acb(h, "R-S9-3", "S-S9-3")
    _staged(h, acb, "r")

    assert h.commit_batch(acb, [Write("r")]) == Status.OK
    assert _same_frame(wal) is True

    frames = wal._wal.frames()
    assert len(frames) == 1                     # 单次提交 → 单帧
    kinds = [rec["kind"] for rec in frames[0]]
    assert kinds == ["RESOURCE_COMMITTED", "RECEIPT_ISSUED"]

    receipt_rec = frames[0][1]
    payload = receipt_rec["payload"]
    assert receipt_rec["subject"] == payload["receipt_id"]   # subject 不是 resource
    assert payload["resource"] == "r"                        # resource 在 payload
    assert receipt_rec["producer"] == acb.id
    assert receipt_rec["idempotency_key"] == f"receipt:{payload['receipt_id']}"
    wal.close()


def test_s9_3_negative_control_split_append_is_two_frames(tmp_path, monkeypatch):
    """S9-3 负对照：把收据拆到第二次 `append_batch` → 两帧 → 同一帧判据为 False。"""
    wal = WalEventLog(str(tmp_path / "wal2.log"))
    orig_append_batch = wal.append_batch

    def split_append(drafts, notify=True):
        if len(drafts) > 1:
            return (orig_append_batch(drafts[:1], notify=notify)
                    + orig_append_batch(drafts[1:], notify=notify))
        return orig_append_batch(drafts, notify=notify)

    monkeypatch.setattr(wal, "append_batch", split_append)
    h = Harness(event_log=wal)
    acb = _acb(h, "R-S9-3N", "S-S9-3N")
    _staged(h, acb, "r")

    assert h.commit_batch(acb, [Write("r")]) == Status.OK
    assert len(wal._wal.frames()) == 2
    assert _same_frame(wal) is False            # 判据本身能把「两帧」判红
    wal.close()


# ================================================================ S9-4 去重
def test_s9_4_retry_after_append_before_projection(monkeypatch):
    """S9-4：`append_batch` 成功后、投影前抛 → 重试不新增提交/收据事件。"""
    h = Harness()
    acb = _acb(h, "R-S9-4", "S-S9-4")
    _staged(h, acb, "r")

    orig_apply = h.workspace._apply_locked
    calls = {"n": 0}

    def boom(*args, **kwargs):
        if calls["n"] == 0:
            calls["n"] += 1
            raise CrashInjected(0, 0)
        return orig_apply(*args, **kwargs)

    monkeypatch.setattr(h.workspace, "_apply_locked", boom)

    with pytest.raises(CrashInjected):
        h.commit_batch(acb, [Write("r")])

    commits = _count(h, EventKind.RESOURCE_COMMITTED)
    receipts = _count(h, EventKind.RECEIPT_ISSUED)
    assert (commits, receipts) == (1, 1)        # 已提交，但投影未发生
    assert h.workspace.version("r") == 0

    st = h.commit_batch(acb, [Write("r")])      # 重试：commit_key 恒等 → 短路
    assert st == Status.OK
    assert st.detail.get("dedup") is True
    assert _count(h, EventKind.RESOURCE_COMMITTED) == commits
    assert _count(h, EventKind.RECEIPT_ISSUED) == receipts


# ================================================================ S9-5 并发
def _commit_pair(h: Harness, pairs, monkeypatch, barrier):
    """两线程并发 commit；`build_receipt` 内 `barrier.wait()` 作为「读完 head」同步点。"""
    orig_build = commit_module.build_receipt

    def build_with_barrier(**kwargs):
        barrier.wait()                          # 位于 head 读取之后、append 之前
        return orig_build(**kwargs)

    monkeypatch.setattr(commit_module, "build_receipt", build_with_barrier)

    results: dict = {}
    errors: list = []

    def worker(name, acb, resource):
        try:
            results[name] = h.commit_batch(acb, [Write(resource)])
        except BaseException as exc:            # noqa: BLE001  # 记录后由断言暴露
            errors.append((name, exc))

    threads = [threading.Thread(target=worker, args=(name, acb, resource))
               for name, acb, resource in pairs]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads), "线程未退出（疑似死锁）"
    return results, errors


def test_s9_5_positive_control_different_scopes_run_in_parallel(monkeypatch):
    """S9-5 正对照：不同 scope 的两线程必须**同时**到达 barrier（不是全局锁）。"""
    h = Harness()
    a = _acb(h, "R-S9-5A", "S-A")
    b = _acb(h, "R-S9-5B", "S-B")
    _staged(h, a, "ra")
    _staged(h, b, "rb")

    barrier = threading.Barrier(2, timeout=5)   # 若被全局锁串行化 → BrokenBarrierError
    results, errors = _commit_pair(
        h, [("a", a, "ra"), ("b", b, "rb")], monkeypatch, barrier)

    assert errors == []
    assert results["a"] == Status.OK and results["b"] == Status.OK
    assert h.verify_receipts("R-S9-5A", "S-A") is None      # 未签名 → 三态 None
    assert h.verify_receipts("R-S9-5B", "S-B") is None


def test_s9_5_negative_control_nullcontext_forks_chain(monkeypatch):
    """S9-5 负对照：scope 锁换成 `nullcontext()` → barrier 到达 → 链分叉 → verify False。"""
    h = Harness()
    monkeypatch.setattr(h.receipts, "scope_lock", lambda scope: nullcontext())

    a = _acb(h, "R-S9-5N", "S-N", task_id="T-A")
    b = _acb(h, "R-S9-5N", "S-N", task_id="T-B")
    _staged(h, a, "r1")
    _staged(h, b, "r2")

    barrier = threading.Barrier(2, timeout=5)
    results, errors = _commit_pair(
        h, [("a", a, "r1"), ("b", b, "r2")], monkeypatch, barrier)

    assert errors == []
    assert results["a"] == Status.OK and results["b"] == Status.OK
    receipts = h.receipts_of("R-S9-5N", "S-N")
    assert len(receipts) == 2
    assert receipts[0].prev_receipt_hash == receipts[1].prev_receipt_hash  # 同一 head → 分叉
    assert verify_chain(receipts) is False
    assert h.verify_receipts("R-S9-5N", "S-N") is False

    # 正对照：同一批收据在 scope 锁生效时不会分叉（见上一个用例）


# ================================================================ 未签名三态
def test_s9_unsigned_returns_none_not_false():
    """无 auth_secret → `verify_chain_detailed` 返回 `None`；`verify_chain` 只看链连续。"""
    h = Harness()                               # 未注入 secret
    acb = _acb(h, "R-S9-U", "S-U")
    _staged(h, acb, "r")
    assert h.commit_batch(acb, [Write("r")]) == Status.OK

    receipts = h.receipts_of("R-S9-U", "S-U")
    assert len(receipts) == 1
    assert receipts[0].signature == ""
    assert receipts[0].signed is False
    assert verify_chain_detailed(receipts) is None
    assert verify_chain(receipts) is True                 # 空 secret → 只校验链连续
    assert verify_chain(receipts, "some-secret") is False  # 有 secret 但未签名 → 不算通过
    assert h.verify_receipts("R-S9-U", "S-U") is None


def test_s9_signed_chain_uses_full_hmac_hex():
    """有 auth_secret → 全量 64 hex HMAC，链验证 `True`；篡改后 `False`。"""
    h = Harness(auth_secret="s3cr3t")
    acb = _acb(h, "R-S9-S", "S-S")
    _staged(h, acb, "r")
    assert h.commit_batch(acb, [Write("r")]) == Status.OK

    receipts = h.receipts_of("R-S9-S", "S-S")
    assert receipts[0].signed is True
    assert len(receipts[0].signature) == 64
    assert verify_chain_detailed(receipts, "s3cr3t") is True
    assert h.verify_receipts("R-S9-S", "S-S") is True

    tampered = copy.deepcopy(receipts[0])
    tampered.version_after += 99                # payload 变 → 签名不再匹配
    assert verify_chain([tampered], "s3cr3t") is False


def test_s9_verify_chain_rejects_empty_and_duplicate():
    """S9-b 加固：空列表 / 重复 `receipt_id` → `False`（v1.0 空列表 fail-open）。"""
    assert verify_chain([]) is False
    assert verify_chain_detailed([]) is False

    r1 = build_receipt(receipt_id="dup", run_id="R", task_id="T",
                       logical_subgoal_id="S", resource="r",
                       version_before=0, version_after=1)
    r2 = build_receipt(receipt_id="dup", run_id="R", task_id="T",
                       logical_subgoal_id="S", resource="r",
                       version_before=1, version_after=2,
                       prev_receipt_hash=receipt_hash(r1))
    assert verify_chain_detailed([r1, r2]) is False
    assert verify_chain([r1, r2]) is False


def test_s9_head_rebuilt_from_event_log(tmp_path):
    """head/id 在新实例 `__init__` 时从 EventLog 重建，且链能继续接上。"""
    wal = WalEventLog(str(tmp_path / "wal3.log"))
    h = Harness(event_log=wal)
    acb = _acb(h, "R-S9-RB", "S-RB")
    _staged(h, acb, "r1")
    assert h.commit_batch(acb, [Write("r1")]) == Status.OK
    scope = ("R-S9-RB", "S-RB")
    head = h.state.receipt_heads[scope]

    h2 = Harness(event_log=wal)                 # 模拟重启：只靠 EventLog 重建
    assert h2.state.receipt_heads[scope] == head
    assert h2.state.receipt_ids == h.state.receipt_ids

    acb2 = _acb(h2, "R-S9-RB", "S-RB", task_id="T-S9-2")
    _staged(h2, acb2, "r2")
    assert h2.commit_batch(acb2, [Write("r2")]) == Status.OK
    receipts = h2.receipts_of("R-S9-RB", "S-RB")
    assert len(receipts) == 2
    assert receipts[-1].prev_receipt_hash == head       # 链在新实例上续接
    assert h2.verify_receipts("R-S9-RB", "S-RB") is None
    wal.close()


def test_s9_head_anchor_matches_task_completed():
    """head 锚点：TASK_COMPLETED 的 `receipt_head`/`receipt_count` 与链尾一致。"""
    h = Harness(auth_secret="s3cr3t")
    acb = _acb(h, "R-S9-A", "S-A", task_id="T-A")
    h.plans["R-S9-A"] = {"run_id": "R-S9-A", "tasks": {}, "edges": []}
    _staged(h, acb, "r")
    assert h.commit_batch(acb, [Write("r")]) == Status.OK

    h.runtime.run(acb, [])
    completed = next(e for e in h.event_log.all()
                     if e.kind == EventKind.TASK_COMPLETED)
    receipts = h.receipts_of("R-S9-A", "S-A")
    assert completed.payload["run_id"] == "R-S9-A"
    assert completed.payload["receipt_count"] == len(receipts)
    assert completed.payload["receipt_head"] == receipt_hash(receipts[-1])
    assert h.plans["R-S9-A"]["receipt_heads"]["S-A"] == completed.payload["receipt_head"]
