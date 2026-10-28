"""S9d 子进程脚本（`tests/test_s9_cross_process.py` / `tests/test_s9_sigkill.py`）。

不作为 pytest 用例收集（文件名不以 `test_` 开头）。两种模式：

- `cross-process`：新进程用**同一套 durable 三件套**（同 WAL / blob root /
  checkpoint root）从 **seq 0 全量重建 workspace**，再用 `latest_ref_for_acb`
  取 ref（不依赖父进程内存里的 `acb.result_ref`）并 `recover(ref, RESUME)`；
  断言结果以单行 JSON 写 stdout，由父进程解析。
- `sigkill`：在 `FileCheckpointStore.save` 的 4 个阶段之一写 marker 到 stdout，
  然后 `threading.Event().wait()` **确定性阻塞**（不靠 sleep），父进程读到
  marker 后再 SIGKILL。

注意（S9 §2.4.7 / §5）：SIGKILL 只证明**原子性**（进程内一次写入要么生效
要么不生效），**不证明掉电持久性** —— SIGKILL 不会丢 page cache，`fsync`
是否真的落盘只能靠掉电/故障盘测试，本阶段不承诺。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import harness.checkpoint as cp_mod
from harness import ACB, Harness, TaskState
from harness.checkpoint import FileCheckpointStore
from harness.event_log import WalEventLog
from harness.storage.blob import FileBlobStore


def _marker(payload: dict) -> None:  # 父进程读到该行才 kill
    sys.stdout.write("MARKER " + json.dumps(payload, sort_keys=True) + "\n")
    sys.stdout.flush()


def _block_forever() -> None:
    """确定性阻塞：父进程收到 marker 后 SIGKILL（**不用 sleep 猜时机**）。"""
    threading.Event().wait()


# ---------------------------------------------------------------- cross-process
def run_cross_process(args: argparse.Namespace) -> int:
    handoff = json.loads(Path(args.handoff).read_text(encoding="utf-8"))

    wal = WalEventLog(args.wal)
    h = Harness(event_log=wal,
                blobs=FileBlobStore(args.blobs),
                checkpoints=FileCheckpointStore(args.checkpoints),
                auth_secret=args.secret)

    # §2.4.9 durable 前置条件：新进程必须**从 seq 0 全量重建** workspace
    # （`recover` 只按 per-ACB `event_cursor` 重放，新进程 manifest 初始为空）。
    # `WorkspaceManager.__init__` 已调用 `_rebuild_from_events()`，此处显式断言。
    assert h.workspace.manifest, "workspace 未从 seq 0 重建（manifest 为空）"

    store = h.checkpoints
    assert store.store_uuid == handoff["store_uuid"], "store_uuid 与父进程不一致"

    # 证明不依赖父进程内存里的 `acb.result_ref`：只从磁盘 latest 指针取 ref
    ref = store.latest_ref_for_acb(handoff["acb_id"])
    assert ref is not None, "latest_ref_for_acb 返回 None"
    assert ref == handoff["ref"], (ref, handoff["ref"])

    acb = h.recover(ref, resume_policy="RESUME")
    resource = handoff["resource"]
    receipts = h.receipts_of(handoff["run_id"])
    # P1-8：recover 必须恢复 subscriptions/materialized → context_view 非空
    #（否则事件全被 `ev.subject in acb.subscriptions` 过滤掉）。
    view = h.context.context_view(acb)
    assert view.get(resource), "跨进程恢复后 context_view 为空（订阅未恢复）"
    result = {
        "manifest_size": len(h.workspace.manifest),
        "ref": ref,
        "acb_id": acb.id,
        "content_hash": h.workspace.content_hash(resource),
        "version": h.workspace.version(resource),
        "state": acb.state.value,
        "generation": int(acb.generation),
        "receipt_count": len(receipts),
        "receipts_nonempty": bool(receipts),
        "verified": h.verify_receipts(handoff["run_id"]),
        "expected_content_hash": handoff["content_hash"],
        "expected_version": handoff["version"],
        "expected_state": handoff["state"],
        "done_state": TaskState.DONE.value,
        "subscriptions_restored": sorted(acb.subscriptions),
        "context_view_nonempty": bool(view.get(resource)),
        "context_view_value": view.get(resource),
    }
    wal.close()
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    sys.stdout.flush()
    return 0


# ---------------------------------------------------------------- SIGKILL
def run_sigkill(args: argparse.Namespace) -> int:
    stage = args.stage
    store = FileCheckpointStore(args.root)
    acb = ACB(id=args.acb_id, run_id="R-SIGKILL",
              task_id="T-SIGKILL", logical_subgoal_id="S-SIGKILL")
    acb.staged = {"r": {"v": 2}}

    orig_write_new = FileCheckpointStore._write_new   # 真实静态方法
    orig_write_all = cp_mod.write_all
    orig_write_pointer = store._write_pointer
    state: dict = {"ref": None, "pointer_calls": 0}

    def write_new(path: Path, data: bytes) -> None:
        state["ref"] = path.name[: -len(".json")]
        orig_write_new(path, data)                   # 走真实 `_write_new`（tmp+link）

    def write_all_half(fd: int, data: bytes) -> None:
        # P0：真实 `_write_new` 内只写一半到 tmp → SIGKILL → 半截 tmp
        orig_write_all(fd, data[: max(1, len(data) // 2)])
        _marker({"stage": "P0", "ref": state["ref"]})
        _block_forever()

    def link_block(tmp, path) -> None:
        # P1：真实 `_write_new` 已写完整 tmp + fsync，link 前阻塞
        _marker({"stage": "P1", "ref": state["ref"]})
        _block_forever()

    def write_pointer(path, ref, acb_, seq, now_ms) -> None:
        state["pointer_calls"] += 1
        if stage == "P2" and state["pointer_calls"] == 1:
            # ref 已 link 完成、latest 指针尚未写 → 阻塞
            _marker({"stage": "P2", "ref": state["ref"]})
            _block_forever()
        orig_write_pointer(path, ref, acb_, seq, now_ms)
        if stage == "P3" and state["pointer_calls"] == 3:
            # 三指针全部落盘 → 阻塞
            _marker({"stage": "P3", "ref": state["ref"]})
            _block_forever()

    store._write_new = write_new                    # type: ignore[method-assign]
    store._write_pointer = write_pointer            # type: ignore[method-assign]
    if stage == "P0":
        cp_mod.write_all = write_all_half           # 注入真实 `_write_new` 内的写
    elif stage == "P1":
        os.link = link_block                        # 注入真实 `_write_new` 内的 link

    store.save(acb)

    # 到达这里说明注入阶段未阻塞（测试前提不成立）
    sys.stderr.write(f"stage {stage} did not block\n")
    return 2


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)

    cp = sub.add_parser("cross-process")
    cp.add_argument("--handoff", required=True)
    cp.add_argument("--wal", required=True)
    cp.add_argument("--blobs", required=True)
    cp.add_argument("--checkpoints", required=True)
    cp.add_argument("--secret", required=True)

    sk = sub.add_parser("sigkill")
    sk.add_argument("--stage", required=True, choices=["P0", "P1", "P2", "P3"])
    sk.add_argument("--root", required=True)
    sk.add_argument("--acb-id", required=True, dest="acb_id")

    args = parser.parse_args(argv)
    if args.mode == "cross-process":
        return run_cross_process(args)
    return run_sigkill(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
