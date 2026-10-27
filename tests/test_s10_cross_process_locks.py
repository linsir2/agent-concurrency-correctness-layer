"""S10b：跨进程锁与 fencing（`docs/s10-cross-process-reconciliation.md` v1.1 §3/§5）。

| 用例 | 判据 |
|---|---|
| S10-6 | 跨进程 scope 锁：**文件信号 barrier**（管道阻塞读，非 `threading.Barrier`）；正对照（flock 生效）→ barrier 不超时 + `verify_receipts is True`；负对照（`nullcontext` 替锁）→ barrier 到达 → 链分叉 → `verify is False` |
| S10-7 | WAL seq 不冲突：两进程 barrier 后同时 `append_frame`；seq 集合恰为 `1..2N` + 重开不抛 `WalCorrupt` + 总记录数 == 两进程之和 |
| S10-8 | fencing：旧 epoch 写者 append 被拒（不写入、不 ack）+「ack 后丢写」反例被 append 前复验拦截 |
| S10-10 | 无循环导入：`python3 -c "import harness.storage.wal"` 不抛 `ImportError` |
| S10-11 | 锁序：`LOCK_ORDER` 结构断言 + 持 `store.lock` 取 `receipts.lock` → `LockOrderViolation` |

barrier 为什么不用 `threading.Barrier`：它只在**单进程多线程**间有效；跨进程
必须用文件/管道信号。这里用**管道阻塞读**：子进程 `os.write(arrive_fd)` 通知
父进程后 `os.read(release_fd)` 阻塞，父进程 `select` 收齐全部 arrive 再逐个放行
—— 确定性 rendezvous，不靠 sleep 猜时机（复用 S9-7 marker + Event 阻塞的模式）。

barrier 放置点（S10-6）：
- 正对照放**临界区之前**（发令枪）：flock 会串行化临界区，若 barrier 在锁内，
  后到者被锁挡住 → barrier 必超时 → 断言「不超时」失败。放锁外才能证明两进程
  **同时**抵达提交点（否则串行执行会假绿）。
- 负对照放 `build_receipt` 内（读完 head、append 前）：撤掉锁后两进程都读到
  同一 head → 分叉，且 barrier 到达证明两者确实并发读 head（否则串行不分叉）。
"""

from __future__ import annotations

import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path

import pytest

from harness import ACB, EventKind, Harness, ManualClock, Status, Write
from harness.event_log import WalEventLog
from harness.spec.receipt import GENESIS_HASH, receipt_hash, verify_chain
from harness.storage._lock import (
    LOCK_ORDER,
    RECEIPTS_ORDER,
    STORE_ORDER,
    WAL_ORDER,
    FencingRejected,
    LockOrderViolation,
    _FileLock,
)
from harness.storage.wal import Wal, WalCorrupt

REPO_ROOT = Path(__file__).resolve().parents[1]
CHILD = Path(__file__).resolve().parent / "_s10_child.py"
SECRET = "s10b-cross-process-secret"
RUN_ID = "R-S10-6"
SUBGOAL = "S-S10-6"


# ---------------------------------------------------------------- 跨进程 barrier
class _Barrier:
    """跨进程文件信号 barrier：子进程写 arrive、阻塞读 release；父进程收齐后放行。"""

    def __init__(self, timeout: float = 60.0) -> None:
        self.timeout = timeout
        self._arrive_r: list[int] = []
        self._release_w: list[int] = []

    def spawn(self, cmd_fn, **kw) -> subprocess.Popen:
        """`cmd_fn(arrive_w, release_r) -> cmd`；fd 经 `pass_fds` 继承给子进程。"""
        arrive_r, arrive_w = os.pipe()
        release_r, release_w = os.pipe()
        proc = subprocess.Popen(cmd_fn(arrive_w, release_r),
                                pass_fds=(arrive_w, release_r), **kw)
        os.close(arrive_w)          # 父进程只留读端 / 写端
        os.close(release_r)
        self._arrive_r.append(arrive_r)
        self._release_w.append(release_w)
        return proc

    def wait_all(self) -> int:
        """收齐全部 arrive 后放行；返回**到达数**（< n 即超时）。"""
        n = len(self._arrive_r)
        arrived = 0
        deadline = time.monotonic() + self.timeout
        for r in self._arrive_r:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            ready, _, _ = select.select([r], [], [], remaining)
            if not ready:
                break
            os.read(r, 1)
            arrived += 1
        if arrived == n:
            for w in self._release_w:
                os.write(w, b"G")
        return arrived

    def close(self) -> None:
        for fd in self._arrive_r + self._release_w:
            try:
                os.close(fd)
            except OSError:
                pass
        self._arrive_r.clear()
        self._release_w.clear()


def _child_cmd(*extra: str, arrive_fd: int, release_fd: int) -> list[str]:
    return [sys.executable, str(CHILD), *extra,
            "--arrive-fd", str(arrive_fd), "--release-fd", str(release_fd)]


def _spawn(barrier: _Barrier, *extra: str) -> subprocess.Popen:
    return barrier.spawn(
        lambda a, r: _child_cmd(*extra, arrive_fd=a, release_fd=r),
        cwd=str(REPO_ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1)


def _collect(proc: subprocess.Popen) -> dict:
    out, err = proc.communicate(timeout=120)
    assert proc.returncode == 0, f"子进程失败 rc={proc.returncode}: {err}"
    lines = [line for line in out.splitlines() if line.strip()]
    assert lines, f"子进程未输出 JSON: {err}"
    return json.loads(lines[-1])


def _receipts_reader(wal_path: str) -> Harness:
    """父进程侧重开 WAL 读收据（内存 store，不参与跨进程竞争）。"""
    return Harness(event_log=WalEventLog(wal_path), auth_secret=SECRET)


# ================================================================ S10-6
def test_s10_6_positive_scope_lock_serializes_cross_process(tmp_path):
    """S10-6 正对照：flock 生效 → 两进程同时抵达提交点（barrier 不超时）→ 链不分叉。"""
    root = tmp_path / "cp"
    os.makedirs(root, exist_ok=True)
    barrier = _Barrier()
    procs = [
        _spawn(barrier, "scope-lock", "--root", str(root),
               "--run-id", RUN_ID, "--subgoal", SUBGOAL,
               "--task-id", "T-A", "--acb-id", "A-T-A", "--resource", "r1",
               "--barrier-at", "start"),
        _spawn(barrier, "scope-lock", "--root", str(root),
               "--run-id", RUN_ID, "--subgoal", SUBGOAL,
               "--task-id", "T-B", "--acb-id", "A-T-B", "--resource", "r2",
               "--barrier-at", "start"),
    ]
    arrived = barrier.wait_all()
    assert arrived == 2, \
        f"barrier 超时（到达 {arrived}/2）→ 串行执行会假绿，前提不成立"
    outs = [_collect(p) for p in procs]
    barrier.close()

    assert all(o["status"] == "OK" for o in outs), outs
    assert all(o["staged_ok"] for o in outs), outs

    h = _receipts_reader(str(root / "wal.log"))
    receipts = h.receipts_of(RUN_ID, SUBGOAL)
    assert len(receipts) == 2, f"同 scope 两进程应各产 1 张收据：{len(receipts)}"
    assert receipts[1].prev_receipt_hash == receipt_hash(receipts[0]), \
        "第二张收据应接在第一张之后（未分叉）"
    assert verify_chain(receipts) is True
    assert h.verify_receipts(RUN_ID, SUBGOAL) is True


def test_s10_6_negative_nullcontext_forks_chain(tmp_path):
    """S10-6 负对照：`nullcontext` 替 scope 锁 → 两进程都到达 barrier → 链分叉 → verify False。"""
    root = tmp_path / "cp"
    os.makedirs(root, exist_ok=True)
    barrier = _Barrier()
    procs = [
        _spawn(barrier, "scope-lock", "--root", str(root),
               "--run-id", RUN_ID, "--subgoal", SUBGOAL,
               "--task-id", "T-A", "--acb-id", "A-T-A", "--resource", "r1",
               "--barrier-at", "build_receipt", "--null-context"),
        _spawn(barrier, "scope-lock", "--root", str(root),
               "--run-id", RUN_ID, "--subgoal", SUBGOAL,
               "--task-id", "T-B", "--acb-id", "A-T-B", "--resource", "r2",
               "--barrier-at", "build_receipt", "--null-context"),
    ]
    arrived = barrier.wait_all()
    assert arrived == 2, \
        f"barrier 超时（到达 {arrived}/2）→ 无法证明两进程并发读 head，负对照无牙"
    outs = [_collect(p) for p in procs]
    barrier.close()

    assert all(o["status"] == "OK" for o in outs), outs
    h = _receipts_reader(str(root / "wal.log"))
    receipts = h.receipts_of(RUN_ID, SUBGOAL)
    assert len(receipts) == 2, f"两进程应各产 1 张收据：{len(receipts)}"
    assert receipts[0].prev_receipt_hash == receipts[1].prev_receipt_hash == GENESIS_HASH, \
        "撤锁后两进程应读到同一 head（GENESIS）→ 分叉"
    assert verify_chain(receipts) is False
    assert h.verify_receipts(RUN_ID, SUBGOAL) is False


# ================================================================ S10-7
def test_s10_7_wal_seq_no_conflict(tmp_path):
    """S10-7：两进程 barrier 后同时 `append_frame` → seq 恰为 1..2N，无重复/空洞。"""
    wal_path = str(tmp_path / "wal.log")
    n = 8
    barrier = _Barrier()
    procs = [
        _spawn(barrier, "wal-append", "--wal", wal_path, "--n", str(n), "--tag", tag)
        for tag in ("P1", "P2")
    ]
    arrived = barrier.wait_all()
    assert arrived == 2, \
        f"barrier 超时（到达 {arrived}/2）→ 未强制并发，seq 判据会假绿"
    outs = [_collect(p) for p in procs]
    barrier.close()

    assert all(o["count"] == n for o in outs), outs
    seqs = sorted(seq for o in outs for seq in o["seqs"])
    assert len(seqs) == 2 * n, f"总记录数应 == 两进程之和 {2 * n}：{len(seqs)}"
    assert len(set(seqs)) == len(seqs), f"seq 有重复：{seqs}"
    assert seqs == list(range(1, 2 * n + 1)), f"seq 应为 1..{2 * n}：{seqs}"

    wal = Wal(wal_path)                       # ② 重开不抛 WalCorrupt
    recs = wal.records()
    wal.close()
    assert len(recs) == 2 * n                 # ③ 总记录数 == 两进程写入数之和
    assert [r["seq"] for r in recs] == list(range(1, 2 * n + 1))


def test_s10_7_negative_control_without_rescan_seq_collides(tmp_path):
    """S10-7 负对照：禁用 fstat 回扫（模拟「只信内存 `_seq`」旧 bug）→ seq 冲突。"""
    wal_path = str(tmp_path / "wal.log")
    n = 4
    barrier = _Barrier()
    procs = [
        _spawn(barrier, "wal-append", "--wal", wal_path, "--n", str(n),
               "--tag", tag, "--no-rescan")
        for tag in ("P1", "P2")
    ]
    arrived = barrier.wait_all()
    assert arrived == 2
    outs = [_collect(p) for p in procs]
    barrier.close()

    seqs = [seq for o in outs for seq in o["seqs"]]
    assert len(seqs) != len(set(seqs)), f"禁用回扫后 seq 应冲突（有重复）：{seqs}"
    with pytest.raises(WalCorrupt):
        Wal(wal_path)


# ================================================================ S10-8
def test_s10_8_fencing_rejects_stale_writer_and_preserves_acked_frame(tmp_path):
    """S10-8：旧 epoch 写者 append 被拒（不写入、不 ack）+ 已 ack 帧恢复时不被拒。"""
    root = tmp_path / "cp"
    wal_path = str(tmp_path / "wal.log")
    wal = WalEventLog(wal_path, fsync=True)
    h = Harness(event_log=wal, checkpoint_root=str(root), auth_secret=SECRET)

    # 写者本地 epoch = 4（模拟被接管前的旧写者）
    h.intents.set_epoch(4)
    h.budget.set_epoch(4, emit=False)
    assert h.intents.get_epoch() == 4

    # epoch=4 时 append 成功（store epoch 仍为 4）→ 该帧已 ack
    wal.append(EventKind.TASK_COMPLETED, "s1", {"v": 1})
    assert len(wal._wal.records()) == 1
    assert wal._wal.records()[0]["fencing_epoch"] == 4

    # 监督者 bump 到 5（S11b K-46：epoch 绑 WAL，`wal.log.epoch` 内 RMW；
    # 只 bump 持久 epoch、**不**推进本写者 intents epoch → 模拟被接管的旧写者）
    for _ in range(5):
        wal._wal.bump_epoch()
    assert wal._wal.read_epoch() == 5
    assert h.intents.get_epoch() == 4, "监督者 bump 不应改写旧写者的本地 epoch"

    # ① 旧 epoch 写者再 append → 被拒，且不落盘
    before = len(wal._wal.records())
    with pytest.raises(FencingRejected):
        wal.append(EventKind.TASK_COMPLETED, "s2", {"v": 2})
    assert len(wal._wal.records()) == before, "被拒的帧不得落盘（不写入）"

    # commit_batch 路径：REJECT_STALE + 不 ack（head 不变）
    acb = ACB(id="A-FENCE", run_id="R-FENCE", task_id="T1",
              logical_subgoal_id="S1")
    h.workspace.stage(acb, "r", {"v": 1})
    st = h.commit_batch(acb, [Write("r")])
    assert st.status is Status.REJECT_STALE
    assert st.detail.get("reason") == "fencing_epoch_rejected"
    assert ("R-FENCE", "S1") not in h._receipt_heads, "被拒的写不得 ack（改 head）"
    assert len(wal._wal.records()) == before

    # ② 「ack 后丢写」反例被拦截：已 ack 的 epoch=4 帧在恢复时**不得**被拒绝
    wal.close()
    reopened = WalEventLog(wal_path)          # 不抛 WalCorrupt
    recs = reopened._wal.records()
    reopened.close()
    assert len(recs) == 1, "已 ack 的帧不得在恢复时被丢弃"
    assert recs[0]["fencing_epoch"] == 4


def test_s10_8_negative_control_without_recheck_stale_append_succeeds(tmp_path):
    """S10-8 负对照：撤掉 append 前复验 → 旧 epoch 写者的 append 不再被拒（证明复验是拦截点）。"""
    root = tmp_path / "cp"
    wal_path = str(tmp_path / "wal.log")
    wal = WalEventLog(wal_path, fsync=True)
    h = Harness(event_log=wal, checkpoint_root=str(root), auth_secret=SECRET)

    h.intents.set_epoch(4)
    for _ in range(5):
        wal._wal.bump_epoch()                 # S11b K-46：WAL 侧 epoch
    assert wal._wal.read_epoch() == 5

    # 模拟缺少 append 前复验的旧实现
    wal._fencing_lock = None
    wal._fencing_reader = None
    wal.append(EventKind.TASK_COMPLETED, "s1", {"v": 1})
    assert len(wal._wal.records()) == 1, "撤掉复验后旧 epoch 写者应能写入（负对照前提）"
    wal.close()


# ================================================================ S10-10
@pytest.mark.parametrize("module", [
    "harness.storage.wal",
    "harness.event_log",
    "harness.checkpoint",
    "harness.harness",
])
def test_s10_10_no_circular_import(module):
    """S10-10：`_FileLock` 下沉后无循环导入（子进程独立 import 断言）。"""
    proc = subprocess.run([sys.executable, "-c", f"import {module}"],
                          cwd=str(REPO_ROOT), capture_output=True, text=True,
                          timeout=60)
    assert proc.returncode == 0, f"import {module} 失败：{proc.stderr}"
    assert "ImportError" not in proc.stderr
    assert "circular import" not in proc.stderr.lower()


# ================================================================ S10-11
def test_s10_11_lock_order_constant_is_immutable():
    """S10-11 / S11b §3.2：`LOCK_ORDER` 结构断言（全序写死、不可更改）。"""
    assert LOCK_ORDER == (
        "resource", "budget", "receipts", "wal", "epoch", "store", "eventlog")
    assert isinstance(LOCK_ORDER, tuple), "必须是不可变元组（防被原地改写）"
    with pytest.raises(TypeError):
        LOCK_ORDER[0] = "x"                       # type: ignore[index]
    assert (RECEIPTS_ORDER, WAL_ORDER, STORE_ORDER) == (2, 3, 5)
    assert LOCK_ORDER.index("resource") < LOCK_ORDER.index("budget")
    assert LOCK_ORDER.index("budget") < LOCK_ORDER.index("receipts")
    assert LOCK_ORDER.index("receipts") < LOCK_ORDER.index("wal")
    assert LOCK_ORDER.index("wal") < LOCK_ORDER.index("epoch")
    assert LOCK_ORDER.index("epoch") < LOCK_ORDER.index("store")
    assert LOCK_ORDER.index("store") < LOCK_ORDER.index("eventlog")


def test_s10_11_reverse_acquisition_raises_lock_order_violation(tmp_path):
    """S10-11：持 `store.lock` 取 `receipts.lock`（反向）→ `LockOrderViolation`。"""
    store_lock = _FileLock(tmp_path / "store.lock", order=STORE_ORDER)
    receipts_lock = _FileLock(tmp_path / "receipts.lock", order=RECEIPTS_ORDER)

    with store_lock():
        with pytest.raises(LockOrderViolation):
            with receipts_lock():
                pass

    # 正序（receipts → wal → store）不抛
    wal_lock = _FileLock(tmp_path / "wal.lock", order=WAL_ORDER)
    with receipts_lock():
        with wal_lock():
            with store_lock():
                pass


# ================================================================ 终审 7-2
def test_final_7_2_takeover_bumps_epoch_before_writing_lease(tmp_path, monkeypatch):
    """接管必须**先 bump_epoch 再落租约**（否则旧写者可在窗口内以旧 epoch 写入）。

    判据：bump 执行时，磁盘上的租约仍属于旧写者（w1）。旧实现先写租约再 bump，
    此处会观察到 "w2"。
    """
    clock = ManualClock()
    cp = str(tmp_path / "cp")
    h1 = Harness(checkpoint_root=cp, writer_id="w1", clock=clock, lease_ms=1000)
    assert h1._read_lease()["owner"] == "w1"

    observed: list[str | None] = []
    orig_bump = Harness.bump_epoch

    def spy(self, reason: str = ""):
        if getattr(self, "_writer_id", None) == "w2":
            lease = self._read_lease()
            observed.append(None if lease is None else lease.get("owner"))
        return orig_bump(self, reason)

    monkeypatch.setattr(Harness, "bump_epoch", spy)
    clock.advance(2000)                       # w1 租约过期 → w2 接管
    h2 = Harness(checkpoint_root=cp, writer_id="w2", clock=clock, lease_ms=1000)

    assert observed == ["w1"], \
        f"接管窗口未消除：bump 时租约 owner={observed!r}（期望旧写者 w1）"
    assert h2._writer_lease_ok is True
    lease = h2._read_lease()
    assert lease["owner"] == "w2" and lease["epoch"] == 1
