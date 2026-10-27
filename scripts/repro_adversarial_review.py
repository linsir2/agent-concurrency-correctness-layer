#!/usr/bin/env python3
"""对抗评审清单【高】1–4 的复现脚本（修复前 FAIL，修复后 PASS）。

用法：

    HARNESS_PATH=/tmp/before python3 scripts/repro_adversarial_review.py   # 旧代码
    python3 scripts/repro_adversarial_review.py                            # 当前代码

每条用例独立构造反例，输出 `[PASS]/[FAIL] 条目N: 描述`，非零退出码表示仍有 FAIL。
`HARNESS_PATH` 用于把 `harness` 包指向旧版本，从而给出**同一脚本**的修复前后对比。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = os.environ.get("HARNESS_PATH", str(REPO_ROOT))
sys.path.insert(0, HARNESS_PATH)

from harness import ACB, EventKind, Harness, ManualClock, Status, Write
from harness.budget import BudgetManager, _is_budget_kind
from harness.checkpoint import FileCheckpointStore
from harness.event_log import EventLog
from harness.orchestration import SlotTracker


def _harness_kwargs(**kwargs) -> dict:
    """旧版 Harness 没有 `writer_id` 参数 → 自动降级（同一脚本兼容前后）。"""
    import inspect

    params = inspect.signature(Harness.__init__).parameters
    if "writer_id" not in params:
        kwargs.pop("writer_id", None)
    return kwargs


# ------------------------------------------------------------------ 条目1
def case1_consume_emit_outside_lock() -> None:
    """1 consume 的 CONSUMED 事件在锁外 emit → 重放序 RESERVED→RELEASED→CONSUMED。"""
    log = EventLog()
    budget = BudgetManager(100, event_log=log)
    budget.create_node("B", 100)
    resv = budget.reserve("B", 10)
    assert resv is not None

    emit_enter = threading.Event()
    release_done = threading.Event()
    orig_emit = budget._emit

    def slow_emit(kind, node_id, payload):
        if kind == EventKind.BUDGET_CONSUMED:
            emit_enter.set()
            # 旧代码：锁外 emit → release 可先完成；新代码：锁内 emit → release 阻塞，
            # 这里用超时等待避免死锁（超时后照常 append，顺序仍然正确）。
            release_done.wait(0.3)
        return orig_emit(kind, node_id, payload)

    budget._emit = slow_emit

    def releaser():
        emit_enter.wait(2.0)
        budget.release(resv.reservation_id)
        release_done.set()

    thread = threading.Thread(target=releaser)
    thread.start()
    assert budget.consume(resv.reservation_id, 10) == Status.OK
    thread.join(timeout=5.0)

    kinds = [e.kind for e in log.all() if _is_budget_kind(e.kind)]
    consumed_at = kinds.index(EventKind.BUDGET_CONSUMED)
    released_at = kinds.index(EventKind.BUDGET_RELEASED)
    assert consumed_at < released_at, \
        f"CONSUMED 必须早于 RELEASED（实际事件序 {[k.value for k in kinds]}）"

    replay = BudgetManager(100, event_log=log)
    for ev in log.all():
        if _is_budget_kind(ev.kind):
            assert replay.apply_budget_event(ev), f"重放失败 seq={ev.seq}"
    replay.check_invariants()          # 旧代码：E3 误破 → InvariantError


# ------------------------------------------------------------------ 条目2
def case2_writer_lease_not_on_production_path(tmp: Path) -> None:
    """2 单写者租约只在定义/测试里 → 构造期不 acquire、写入口不拒绝他人。"""
    root = tmp / "cp"
    clock = ManualClock()
    h1 = Harness(**_harness_kwargs(
        run_limits=1000, checkpoint_root=str(root), lease_ms=1000, clock=clock,
        writer_id="writer-1"))
    h2 = Harness(**_harness_kwargs(
        run_limits=1000, checkpoint_root=str(root), lease_ms=1000, clock=clock,
        writer_id="writer-2"))

    lease_file = root / "writer_lease.json"
    assert lease_file.exists(), "构造期未 acquire：durable 模式缺少 writer_lease.json"
    acb = ACB(id="A-LEASE", run_id="R", task_id="T1")
    st = h2.commit_batch(acb, [Write("r", content={"v": 1})])
    assert st == Status.UPSTREAM_FAILED, f"他人持有租约时 commit 仍服务：{st}"
    assert st.detail.get("reason") == "writer_lease_held"
    rec = h2.recover(h1.checkpoints.save(acb))
    assert rec.failure == "WRITER_LEASE_HELD", f"recover 未拒绝服务：{rec.failure}"


# ------------------------------------------------------------------ 条目3
def case3_budget_epoch_not_fenced(tmp: Path) -> None:
    """3 预算写不校验 budget_epoch → 慢写者被接管后仍能 reserve/consume。"""
    root = tmp / "cp"
    h1 = Harness(**_harness_kwargs(
        run_limits=1000, checkpoint_root=str(root), writer_id="writer-1"))
    h1.budget.create_node("B", 100)
    resv = h1.budget.reserve("B", 10)
    assert resv is not None

    h2 = Harness(**_harness_kwargs(
        run_limits=1000, checkpoint_root=str(root), writer_id="writer-2"))
    h2.bump_epoch("takeover")

    assert h1.budget.reserve("B", 5) is None, "旧 budget_epoch 仍可 reserve（超发）"
    result = h1.budget.consume(resv.reservation_id, 1)
    assert result == Status.REJECT_STALE, f"旧 budget_epoch 仍可 consume：{result}"
    assert h1.budget.release(resv.reservation_id) == 0
    assert h1.budget.settle(resv.reservation_id) == 0


# ------------------------------------------------------------------ 条目4
def case4_slot_shared_read_write_not_atomic(tmp: Path) -> None:
    """4 槽位 read 与 write 是两次 flock → 两进程都读 {} → max_concurrent 被击穿。"""
    root = tmp / "cp"
    stores = [FileCheckpointStore(root) for _ in range(2)]
    trackers = [SlotTracker(1, shared=s) for s in stores]
    barrier = threading.Barrier(2)
    results: list[tuple[str, bool]] = []

    for tracker in trackers:
        orig_read = tracker._shared_read

        def slow_read(_orig=orig_read):
            shared = _orig()
            try:
                barrier.wait(timeout=1.0)      # 旧代码：两线程都在读到 {} 后同步
            except threading.BrokenBarrierError:
                pass
            return shared

        tracker._shared_read = slow_read

    def worker(tracker, key):
        results.append((key, tracker.try_acquire(key)))

    threads = [threading.Thread(target=worker, args=(t, k))
               for t, k in zip(trackers, ["A", "B"])]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    winners = [key for key, ok in results if ok]
    shared = stores[0].read_slots()
    assert len(winners) == 1, f"max_concurrent=1 被击穿：winners={winners}"
    assert len(shared) == 1, f"共享槽位残留多占：{shared}"


# ------------------------------------------------------------------ runner
CASES = [
    ("1", "consume 事件锁外 emit / CONSUMED 无 state 守卫", case1_consume_emit_outside_lock),
    ("2", "单写者租约未接入生产路径", case2_writer_lease_not_on_production_path),
    ("3", "预算写未做 budget_epoch fencing", case3_budget_epoch_not_fenced),
    ("4", "槽位共享读-改-写非原子（无 CAS）", case4_slot_shared_read_write_not_atomic),
]


def main(argv: list[str]) -> int:
    wanted = set(argv[1:]) if len(argv) > 1 else None
    failed = []
    print(f"harness = {HARNESS_PATH}")
    for num, desc, fn in CASES:
        if wanted and num not in wanted:
            continue
        try:
            if num in ("2", "3", "4"):
                with tempfile.TemporaryDirectory() as tmp:
                    fn(Path(tmp))
            else:
                fn()
        except BaseException:  # noqa: BLE001  # 复现脚本需报告任意失败（含 KeyboardInterrupt）
            failed.append(num)
            print(f"[FAIL] 条目{num}: {desc}")
            traceback.print_exc()
        else:
            print(f"[PASS] 条目{num}: {desc}")
    print("-" * 72)
    print(f"总计：{len(CASES) if not wanted else len(wanted)} 条，失败 {len(failed)} 条")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
