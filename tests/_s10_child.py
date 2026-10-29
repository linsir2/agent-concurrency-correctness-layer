"""S10b 子进程脚本（`tests/test_s10_cross_process_locks.py`）。

不作为 pytest 用例收集（文件名不以 `test_` 开头）。两种模式：

- `scope-lock`：跨进程 scope 锁（S10-6）。两进程用**同一 durable root** 构建
  `Harness`（`receipts.lock` flock），在 `--barrier-at` 指定点到达**文件信号
  barrier**（写 arrive fd + 阻塞读 release fd；父进程收齐后放行），再提交。
  `--null-context` 把 `_scope_lock` 换成 `nullcontext()`（负对照）。
- `wal-append`：跨进程 WAL seq（S10-7）。两进程在同一 WAL 上 barrier 后各自
  `append_frame`，回报分配的 seq 集合。`--no-rescan` 禁用 fstat 回扫（负对照，
  模拟「只信内存 `_seq`」的旧 bug）。

barrier 说明：跨进程**不能**用 `threading.Barrier`；这里用**管道阻塞读**实现
确定性 rendezvous（子进程 `os.write(arrive_fd)` 后 `os.read(release_fd)` 阻塞，
父进程 `select` 收齐全部 arrive 后向每个 release 管道写一字节放行）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harness import ACB, Harness, Status, Write
from harness.event_log import WalEventLog

SECRET = "s10b-cross-process-secret"
WRITER_ID = "s10-6-shared-writer"


def _emit(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
    sys.stdout.flush()


def _barrier(args: argparse.Namespace) -> None:
    """文件信号 barrier：写 arrive（通知父进程）→ 阻塞读 release（等父进程放行）。"""
    os.write(args.arrive_fd, b"R")
    os.read(args.release_fd, 1)


# ---------------------------------------------------------------- scope-lock
def run_scope_lock(args: argparse.Namespace) -> int:
    from contextlib import nullcontext

    import harness.kernel.commit as harness_module

    root = args.root
    wal = WalEventLog(os.path.join(root, "wal.log"), fsync=True)
    # 两进程用**同一 writer_id**：否则第二个进程的 writer 租约被拒 → 无法提交，
    # 与「scope 锁」无关的租约会污染本用例。
    h = Harness(event_log=wal, checkpoint_root=root, auth_secret=SECRET,
                writer_id=WRITER_ID)
    if args.null_context:
        h.receipts.scope_lock = lambda scope: nullcontext()      # 负对照：撤掉跨进程锁

    if args.barrier_at == "build_receipt":
        # barrier 位于「读完 head 之后、append 之前」：两进程都读到同一 head → 分叉
        orig_build = harness_module.build_receipt

        def build_with_barrier(**kwargs):
            _barrier(args)
            return orig_build(**kwargs)

        harness_module.build_receipt = build_with_barrier

    acb = ACB(id=args.acb_id, run_id=args.run_id, task_id=args.task_id,
              logical_subgoal_id=args.subgoal)
    staged = h.workspace.stage(acb, args.resource, {"v": 1})
    if args.barrier_at == "start":
        _barrier(args)                                    # 发令枪：两进程同时开跑
    st = h.commit_batch(acb, [Write(args.resource)])
    wal.close()
    _emit({"mode": "scope-lock", "status": st.status.value,
           "detail": st.detail, "barrier": args.barrier_at,
           "null_context": bool(args.null_context),
           "staged_ok": staged == Status.OK})
    return 0


# ---------------------------------------------------------------- wal-append
def run_wal_append(args: argparse.Namespace) -> int:
    from harness.storage.wal import Wal

    if args.no_rescan:
        # 负对照：禁用 fstat 回扫 → 两进程都信自己的内存 `_seq`（旧 bug）
        Wal._rescan_from_disk = lambda self: None          # type: ignore[method-assign]

    wal = Wal(args.wal, fsync=True)
    _barrier(args)                                        # 两进程同时进入 append
    assigned: list[int] = []
    for i in range(args.n):
        recs = wal.append_frame([
            {"kind": "TEST", "subject": f"{args.tag}-{i}", "payload": {"i": i}},
        ])
        assigned.extend(rec["seq"] for rec in recs)
    wal.close()
    _emit({"mode": "wal-append", "tag": args.tag, "seqs": assigned,
           "count": len(assigned), "no_rescan": bool(args.no_rescan)})
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)

    sl = sub.add_parser("scope-lock")
    sl.add_argument("--root", required=True)
    sl.add_argument("--run-id", required=True)
    sl.add_argument("--subgoal", required=True)
    sl.add_argument("--task-id", required=True)
    sl.add_argument("--acb-id", required=True)
    sl.add_argument("--resource", required=True)
    sl.add_argument("--arrive-fd", type=int, required=True)
    sl.add_argument("--release-fd", type=int, required=True)
    sl.add_argument("--barrier-at", choices=["start", "build_receipt"],
                    default="start")
    sl.add_argument("--null-context", action="store_true")

    wa = sub.add_parser("wal-append")
    wa.add_argument("--wal", required=True)
    wa.add_argument("--arrive-fd", type=int, required=True)
    wa.add_argument("--release-fd", type=int, required=True)
    wa.add_argument("--n", type=int, required=True)
    wa.add_argument("--tag", required=True)
    wa.add_argument("--no-rescan", action="store_true")

    args = parser.parse_args(argv)
    try:
        if args.mode == "scope-lock":
            return run_scope_lock(args)
        return run_wal_append(args)
    finally:
        for fd in (getattr(args, "arrive_fd", None),
                   getattr(args, "release_fd", None)):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
