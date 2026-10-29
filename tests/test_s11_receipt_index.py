"""S11b：K-44 收据增量索引 + K-46 fencing 绑 WAL（`docs/s11-egress-enforcement.md` v1.1 §3）。

| 用例 | 判据 |
|---|---|
| S11-2 | 枚举计数：N 次 commit 的总枚举量 ≤ N+K（线性上界） |
| S11-2 负对照 | 全量实现 ≥ N(N-1)/2（二次下界）→ 必超线性上界 |
| S11-2b | `_rescan_from_disk` 同步重置索引游标；跨实例新增帧可被看到 |
| S11-3 | 两实例共享同一 WAL（不同 checkpoint root）→ bump 后旧实例 append 被拒（不写入、不 ack） |
| S11-3 负对照 | 旧设计（各读自己 store.json）→ 互不 fence（append 成功） |
"""

from __future__ import annotations

import os

import pytest

from harness import ACB, EventKind, Harness, Status, Write
from harness.event_log import WalEventLog
from harness.storage._lock import FencingRejected
from harness.kernel.receipts import ReceiptChain
from harness.storage.wal import Wal

N = 8
K = 2


def _harness(tmp_path, name: str = "cp") -> tuple[Harness, WalEventLog]:
    log = WalEventLog(str(tmp_path / "wal.log"))
    h = Harness(run_limits=1000, event_log=log,
                checkpoint_root=str(tmp_path / name),
                auth_secret="s11-receipt-secret")
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h, log


def _commit_n(h: Harness, acb: ACB, n: int = N) -> None:
    for i in range(n):
        h.workspace.stage(acb, "r", {"v": i + 1})
        st = h.commit_batch(acb, [Write("r")])
        assert st == Status.OK, f"第 {i + 1} 次提交失败：{st}"


def _count_records(monkeypatch) -> list[int]:
    enumerated: list[int] = []
    orig = Wal.records

    def counting(self):
        out = orig(self)
        enumerated.append(len(out))
        return out

    monkeypatch.setattr(Wal, "records", counting)
    return enumerated


# ================================================================ S11-2
def test_s11_2_receipt_enumeration_is_linear(tmp_path, monkeypatch):
    """正对照：增量索引 → 不调 `Wal.records()`，且索引结果 == 全量扫描结果。

    终审 9-1：旧断言 `sum(enumerated)=0 <= N+K` 恒真（增量路径从不调
    `Wal.records`）→ 把 `receipts_from` 改成返回 `[]` 仍通过。现补：
    ① 索引确实解析了新增帧（`> 0`，非恒真）；② 索引结果与全量扫描**逐条相等**。
    """
    h, log = _harness(tmp_path)
    acb = ACB(id="A-S11-2", run_id="R-S11-2", task_id="T1",
              logical_subgoal_id="S1")
    full_scan_calls = _count_records(monkeypatch)
    indexed_records: list[int] = []
    orig_receipts = Wal.receipts_from

    def counting(self, watermark: int = 0):
        before = self._indexed_frame
        out = orig_receipts(self, watermark)
        after = self._indexed_frame
        indexed_records.append(
            sum(len(self._frames[i]) for i in range(before, after)))
        return out

    monkeypatch.setattr(Wal, "receipts_from", counting)

    _commit_n(h, acb)

    assert sum(full_scan_calls) == 0, "增量路径不得调用 Wal.records()（全量枚举）"
    total = sum(indexed_records)
    assert total > 0, "增量索引未解析任何新增帧（receipts_from 可能被改成返回 []）"
    assert total <= 2 * N + K, \
        f"增量索引解析量 {total} 超线性上界（每提交 1 帧 2 条记录）"

    # 正确性：增量不等于漏扫；索引结果必须与全量扫描逐条相等
    wal = log._wal
    indexed = wal.receipts_from(0)
    full = sorted(
        ((r.get("seq", 0), r.get("payload")) for r in wal.records()
         if r.get("kind") == EventKind.RECEIPT_ISSUED.value),
        key=lambda rp: rp[0])
    assert len(indexed) == N, f"索引结果应含 {N} 条收据，实际 {len(indexed)}"
    assert indexed == full, "增量索引结果与全量扫描不等价（漏扫/错序）"
    assert h.verify_receipts("R-S11-2", "S1") is True
    assert len(h.receipts_of("R-S11-2", "S1")) == N


def test_s11_2_negative_control_full_scan_is_quadratic(tmp_path, monkeypatch):
    """负对照：全量实现每次枚举全部记录 → 总枚举量 ≥ N(N-1)/2，必超线性上界。"""
    h, _log = _harness(tmp_path)
    acb = ACB(id="A-S11-2N", run_id="R-S11-2N", task_id="T1",
              logical_subgoal_id="S1")

    def full_scan(self, watermark: int = 0):
        wal = getattr(self.deps.event_log, "_wal", None)
        raw = wal.records()
        recs = [(r.get("seq", 0), r.get("payload")) for r in raw
                if r.get("kind") == EventKind.RECEIPT_ISSUED.value]
        recs.sort(key=lambda rp: rp[0])
        return [rp for rp in recs if rp[0] > watermark]

    monkeypatch.setattr(ReceiptChain, "_receipt_records", full_scan)
    enumerated = _count_records(monkeypatch)

    _commit_n(h, acb)

    total = sum(enumerated)
    assert total >= N * (N - 1) // 2, \
        f"负对照无牙：全量实现总枚举量 {total} < {N * (N - 1) // 2}"
    assert total > N + K, "全量实现必须超出线性上界（否则判据不成立）"
    assert h.verify_receipts("R-S11-2N", "S1") is True


# ================================================================ S11-2b
def test_s11_2_rescan_resets_index_and_sees_new_frames(tmp_path):
    """`_rescan_from_disk` 重建 `_frames` 时必须同步重置索引游标。"""
    h, log = _harness(tmp_path)
    acb = ACB(id="A-S11-2B", run_id="R-S11-2B", task_id="T1",
              logical_subgoal_id="S1")
    _commit_n(h, acb, n=2)

    wal = log._wal
    first = wal.receipts_from(0)
    assert len(first) == 2
    assert wal._indexed_frame == len(wal._frames)

    # 另一个 Wal 实例追加一帧收据（模拟跨进程写）
    other = Wal(str(tmp_path / "wal.log"))
    other.append_frame([{
        "kind": EventKind.RECEIPT_ISSUED.value,
        "subject": "fake-receipt",
        "payload": {"run_id": "R-X", "logical_subgoal_id": "S-X"},
    }])
    other.close()

    seen = wal.receipts_from(0)
    assert len(seen) == 3, "回扫后必须看到别的实例新增的收据帧"

    # 直接回扫：索引游标与内容必须一并重置（否则旧游标指向新列表 → 错位）
    with wal._lock():
        wal._rescan_from_disk()
        assert wal._indexed_frame == 0
        assert wal._receipt_index == []
        assert wal._receipt_index_seq == 0
    assert len(wal.receipts_from(0)) == 3, "重置后重建索引必须仍返回全部收据"


# ================================================================ S11-3
def _two_instances(tmp_path):
    wal_path = str(tmp_path / "wal.log")
    log1 = WalEventLog(wal_path)
    h1 = Harness(event_log=log1, checkpoint_root=str(tmp_path / "cp1"),
                 writer_id="writer-1")
    log2 = WalEventLog(wal_path)
    h2 = Harness(event_log=log2, checkpoint_root=str(tmp_path / "cp2"),
                 writer_id="writer-2")
    return wal_path, log1, h1, log2, h2


def test_s11_3_fencing_bound_to_wal(tmp_path):
    """共享 WAL + 不同 checkpoint root：bump_epoch 后旧实例 append 被拒。"""
    wal_path, log1, h1, _log2, h2 = _two_instances(tmp_path)

    h1.event_log.append(EventKind.TASK_COMPLETED, "s1", {"v": 1})
    h2.bump_epoch("takeover")                    # 只推进 WAL epoch
    assert log1._wal.read_epoch() == 1
    assert h1.intents.get_epoch() == 0, "接管者 bump 不应改写旧实例本地 epoch"

    before = os.path.getsize(wal_path)
    with pytest.raises(FencingRejected):
        h1.event_log.append(EventKind.TASK_COMPLETED, "s2", {"v": 2})
    assert os.path.getsize(wal_path) == before, "被拒的帧不得落盘（不写入）"

    acb = ACB(id="A-S11-3", run_id="R-S11-3", task_id="T1",
              logical_subgoal_id="S1")
    h1.workspace.stage(acb, "r", {"v": 1})
    st = h1.commit_batch(acb, [Write("r")])
    assert st.status is Status.REJECT_STALE
    assert st.detail.get("reason") == "fencing_epoch_rejected"
    assert ("R-S11-3", "S1") not in h1.state.receipt_heads, "被拒的写不得 ack（改 head）"
    assert os.path.getsize(wal_path) == before


def test_s11_3_negative_control_store_epoch_does_not_fence(tmp_path):
    """旧设计：fencing 各读自己 store.json → 共享 WAL 也互不 fence（反例）。"""
    wal_path, log1, h1, _log2, h2 = _two_instances(tmp_path)

    h1.event_log.append(EventKind.TASK_COMPLETED, "s1", {"v": 1})
    h2.bump_epoch("takeover")
    assert log1._wal.read_epoch() == 1

    # 退回旧设计：fencing 锁/读口绑各自 checkpoint store（epoch 仍为 0）
    h1.event_log._fencing_lock = h1.checkpoints._flock
    h1.event_log._fencing_reader = h1.checkpoints.read_epoch
    assert h1.checkpoints.read_epoch() == 0

    before = os.path.getsize(wal_path)
    h1.event_log.append(EventKind.TASK_COMPLETED, "s2", {"v": 2})   # 不抛
    assert os.path.getsize(wal_path) > before, \
        "旧设计下旧实例应能继续写（负对照前提）"


def test_final_6_2_receipts_from_slices_from_watermark(tmp_path):
    """终审 6-2：索引键单调（bisect 前提）+ `seq > watermark` 语义精确。"""
    h, log = _harness(tmp_path)
    acb = ACB(id="A-6-2", run_id="R-6-2", task_id="T1",
              logical_subgoal_id="S1")
    _commit_n(h, acb, n=4)

    wal = log._wal
    all_recs = wal.receipts_from(0)
    assert len(all_recs) == 4
    assert wal._receipt_index_keys == sorted(wal._receipt_index_keys), \
        "索引键必须单调递增，否则 bisect 切片会漏记录"
    watermark = all_recs[1][0]
    assert wal.receipts_from(watermark) == all_recs[2:]
    assert wal.receipts_from(all_recs[-1][0]) == []


# ================================================================ 终审 7-1 / 7-3
def _tokenized(h: Harness) -> None:
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")


def test_final_7_1_clean_restart_restores_epoch(tmp_path):
    """干净重启（lease 已释放、WAL epoch=N）→ 构造期恢复 epoch，首写不得被 fence。"""
    wal_path = str(tmp_path / "wal.log")
    cp = str(tmp_path / "cp")
    log1 = WalEventLog(wal_path)
    h1 = Harness(event_log=log1, checkpoint_root=cp, auth_secret="s")
    _tokenized(h1)
    acb = ACB(id="A-7-1", run_id="R-7-1", task_id="T1", logical_subgoal_id="S1")
    h1.workspace.stage(acb, "r", {"v": 1})
    assert h1.commit_batch(acb, [Write("r")]) == Status.OK
    assert h1.bump_epoch("takeover") == 1
    log1.close()                                   # 写者会话结束 → 释放 lease

    log2 = WalEventLog(wal_path)
    h2 = Harness(event_log=log2, checkpoint_root=cp, auth_secret="s")
    _tokenized(h2)
    assert h2.intents.get_epoch() == 1, "构造期未恢复 durable epoch"

    acb2 = ACB(id="A-7-1b", run_id="R-7-1b", task_id="T2", logical_subgoal_id="S2")
    h2.workspace.stage(acb2, "r2", {"v": 1})
    st = h2.commit_batch(acb2, [Write("r2")])
    assert st == Status.OK, f"干净重启后首写被永久 fence：{st}"


def test_final_7_3_wal_rebuild_rotates_generation(tmp_path):
    """删除 wal.log + .epoch 重建 → generation 轮换，旧写者一律拒绝（不再复活）。"""
    wal_path = str(tmp_path / "wal.log")
    old = Wal(wal_path)
    old.append_frame([{"kind": "x", "subject": "s", "payload": {}}])
    assert old.bump_epoch() == 1
    old_generation = old.generation

    os.unlink(wal_path)
    os.unlink(wal_path + ".epoch")
    new = Wal(wal_path)
    new.append_frame([{"kind": "x", "subject": "s2", "payload": {}}])

    assert new.generation != old_generation, "WAL 重建后 generation 未轮换"
    with pytest.raises(FencingRejected, match="generation"):
        old.append_frame(
            [{"kind": "x", "subject": "old", "payload": {}}],
            fencing_epoch=5, fencing_lock=old.epoch_lock,
            fencing_reader=old.read_epoch)
    # 新 generation 写者不受影响
    new.append_frame([{"kind": "x", "subject": "s3", "payload": {}}])
    assert new.tail == 2


def test_final_7_3_generation_survives_normal_reopen(tmp_path):
    """正常重开（WAL 文件仍在）不得轮换 generation，否则并发实例互相拒绝。"""
    wal_path = str(tmp_path / "wal.log")
    first = Wal(wal_path)
    first.append_frame([{"kind": "x", "subject": "s", "payload": {}}])
    gen = first.generation
    first.close()
    second = Wal(wal_path)
    assert second.generation == gen
    second.append_frame([{"kind": "x", "subject": "s2", "payload": {}}])
    assert second.tail == 2
