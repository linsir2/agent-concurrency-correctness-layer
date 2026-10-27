"""S9d / **S9-7**：SIGKILL 阶段 barrier（`docs/s9-persistence-and-receipts.md`
v1.1 §2.4 / §3）。

四个确定性阶段（子进程写 marker 到 stdout 后 `threading.Event().wait()` 阻塞，
父进程**读到 marker 才** `SIGKILL` —— 不用 sleep 猜时机）：

| 阶段 | 注入点 | 杀进程时磁盘状态 |
|---|---|---|
| P0 | `_write_new` 改为半写 `<ref>.json.part` 后 | 只有半个 `.part` |
| P1 | `.part` 完整写 + fsync、**未 replace** | `.part` 完整，ref 不存在 |
| P2 | ref replace 完成、**latest 指针未写** | ref 可读，latest 仍指向旧 ref |
| P3 | `latest` 指针写入后 | ref + latest 都完整 |

判据：旧 ref 完整可 `load_strict`；`save_seq` 不回退（新 save 的 seq 严格大于
被杀进程已分配的 seq）；`.part` 不进 ref 命名空间（扫描时被忽略）；P2/P3 新
ref 存在且双 checksum 有效；store 重开不抛。

**边界（必须明确）**：SIGKILL 只证明**原子性**（写入要么生效要么不生效），
**不证明掉电持久性** —— SIGKILL 不丢 page cache，`fsync` 是否真正落盘只能由
掉电/坏盘测试证明，本阶段不承诺（S9 §2.4.7 / §5）。
"""

from __future__ import annotations

import json
import select
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from harness import ACB
from harness.checkpoint import _REF_FILE_RE, CheckpointCorrupt, FileCheckpointStore

REPO_ROOT = Path(__file__).resolve().parents[1]
CHILD = Path(__file__).resolve().parent / "_s9_child.py"


def _seq_of(ref: str) -> int:
    return int(ref.split("-")[2])


def _scan_refs_for_acb(root: Path, acb_id: str) -> tuple[int, str] | None:
    """扫描 refs 目录，返回某 acb_id 的**最大 seq** ref（按文件名 seq）。

    P2 判据：ref 存在但 latest 指针未写时，扫描仍能定位到该 acb 的完整 ref。
    """
    refs_root = root / "refs"
    best: tuple[int, str] | None = None
    for path in refs_root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.endswith((".part", ".corrupt", ".tmp", ".tmp.corrupt")):
            continue
        match = _REF_FILE_RE.match(path.name)
        if not match:
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if isinstance(doc, dict) and doc.get("acb_id") == acb_id:
            seq = int(match.group(1))
            if best is None or seq > best[0]:
                best = (seq, doc.get("ref"))
    return best


def _spawn(root: Path, stage: str, acb_id: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(CHILD), "sigkill",
         "--stage", stage, "--root", str(root), "--acb-id", acb_id],
        cwd=str(REPO_ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1)


def _wait_marker(proc: subprocess.Popen, timeout: float = 60.0) -> dict:
    """确定性同步：等子进程写出 marker；超时/EOF 立即失败（不 sleep 轮询）。"""
    ready, _, _ = select.select([proc.stdout], [], [], timeout)
    assert ready, "子进程未在超时内输出 marker（疑似未到达注入阶段）"
    line = proc.stdout.readline()
    assert line.startswith("MARKER "), f"marker 格式非法：{line!r}"
    return json.loads(line[len("MARKER "):])


def _kill_and_wait(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGKILL)
    rc = proc.wait(timeout=30)
    stderr = proc.stderr.read()
    proc.stdout.close()
    proc.stderr.close()
    assert rc == -signal.SIGKILL, f"子进程未被 SIGKILL 终止：rc={rc} {stderr}"


@pytest.mark.fault
@pytest.mark.parametrize("stage", ["P0", "P1", "P2", "P3"])
def test_s9_7_sigkill_stage_barrier(tmp_path, stage):
    root = tmp_path / "cp"
    setup = FileCheckpointStore(root)
    old_acb = ACB(id="A-OLD", run_id="R-OLD", task_id="T-OLD",
                  logical_subgoal_id="S-OLD")
    old_acb.staged = {"r": {"v": 0}}
    old_ref = setup.save(old_acb)
    old_seq = _seq_of(old_ref)
    store_uuid = setup.store_uuid
    del setup                                   # 父进程实例不再持有状态

    child_acb_id = "A-SIGKILL"
    proc = _spawn(root, stage, child_acb_id)
    marker = _wait_marker(proc)
    _kill_and_wait(proc)

    assert marker["stage"] == stage
    child_ref = marker["ref"]
    child_seq = _seq_of(child_ref)
    assert child_seq == old_seq + 1             # 被杀进程已从磁盘 meta 分配了 seq

    reopened = FileCheckpointStore(root)        # ① store 重开不抛
    assert reopened.store_uuid == store_uuid

    # ② 旧 ref 完整可 load_strict（原子性：新写入未破坏既有 ref）
    assert reopened.load_strict(old_ref)["acb_id"] == old_acb.id

    parts = sorted(p for p in (root / "refs").rglob("*.part") if p.is_file())
    tmps = sorted(p for p in (root / "refs").rglob("*.tmp") if p.is_file())
    assert parts == []                          # 真实 `_write_new` 用 `.tmp`，不用 `.part`

    if stage in ("P0", "P1"):
        # ③ 半写 tmp 被 quarantine 改名 `.corrupt`（隔离，绝不当合法 ref）
        corrupt = sorted(p for p in (root / "refs").rglob("*.tmp.corrupt")
                         if p.is_file())
        assert corrupt, f"{stage} 未留下被隔离的半写 tmp（注入未生效）"
        assert tmps == [], "半写 tmp 不得残留（应被改名 .corrupt）"
        for path in corrupt:
            assert _REF_FILE_RE.match(path.name) is None
        # 扫描只认文件名里的合法 ref → 半截/完整 tmp 都不抬高 next_seq
        assert reopened._scan_max_seq() == old_seq
        assert not reopened._ref_path(child_ref).exists()
        with pytest.raises(CheckpointCorrupt):
            reopened.load_strict(child_ref)
        # latest 悬空（指针未写）且可重建
        assert reopened.latest_ref_for_acb(child_acb_id) is None
        assert reopened.latest_ref_for_acb(old_acb.id) == old_ref
    else:
        assert tmps == []
        assert reopened._scan_max_seq() == child_seq
        # ④ P2/P3：新 ref 存在且双 checksum 有效
        assert reopened.load_strict(child_ref)["acb_id"] == child_acb_id
        if stage == "P2":
            # latest 指针未写 → 扫描仍能定位该 acb 的最大 seq ref 且双 checksum 有效
            scanned = _scan_refs_for_acb(root, child_acb_id)
            assert scanned == (child_seq, child_ref), \
                f"扫描未定位到 child ref：{scanned}"
            assert reopened.load_strict(scanned[1])["acb_id"] == child_acb_id
            # latest 悬空（None）但 ref 可重建；三指针一致（均未写 → None）
            assert reopened.latest_ref_for_acb(child_acb_id) is None
            assert reopened.latest_ref_for_run("R-SIGKILL") is None
            assert (reopened.latest_ref_for_task(
                "R-SIGKILL", "T-SIGKILL", "S-SIGKILL") is None)
            assert reopened.latest_ref_for_acb(old_acb.id) == old_ref
        else:
            # 三指针一致：均指向 child_ref
            assert reopened.latest_ref_for_acb(child_acb_id) == child_ref
            assert reopened.latest_ref_for_run("R-SIGKILL") == child_ref
            assert (reopened.latest_ref_for_task(
                "R-SIGKILL", "T-SIGKILL", "S-SIGKILL") == child_ref)

    # ⑤ save_seq 不回退：新 save 拿到的 seq 严格大于被杀进程已分配的 seq
    fresh = ACB(id="A-FRESH", run_id="R-FRESH", task_id="T-FRESH",
                logical_subgoal_id="S-FRESH")
    fresh.staged = {"r": {"v": 3}}
    fresh_ref = reopened.save(fresh)
    assert _seq_of(fresh_ref) > child_seq
    assert reopened.load_strict(fresh_ref)["acb_id"] == "A-FRESH"
