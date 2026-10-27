"""S9d / **S9-6 强化版**：跨进程恢复（`docs/s9-persistence-and-receipts.md`
v1.1 §2.4.9 / §3）。

判据（全部由**子进程**输出 JSON，父进程解析后断言，避免父进程猜）：

1. 父进程用 durable 三件套（`WalEventLog(fsync=True)` + `FileBlobStore` +
   `FileCheckpointStore`，同 root）+ `auth_secret` 跑一个**真实提交**
   （`dispatch_and_run` → `commit_batch` → 收据链 + checkpoint），写
   `handoff.json {acb_id, ref, store_uuid}`，**关闭 WAL**；
2. 子进程（`tests/_s9_child.py cross-process`）用同参数新 `Harness`：
   - `WorkspaceManager.__init__` 已从 **seq 0** 全量重建 workspace（断言
     `manifest` 非空）；
   - `ref = latest_ref_for_acb(acb_id)`（不依赖父进程内存 `acb.result_ref`）；
   - `recover(ref, resume_policy="RESUME")`；
3. 断言：`content_hash` / `version` 与父进程一致、`receipts_of(run_id)` 非空且
   `verify_receipts` 通过、`acb.state` 合理、`store_uuid` 同一存储。
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from conftest import commit, decision, stage

from harness import EventKind, Harness, TaskState
from harness.checkpoint import FileCheckpointStore
from harness.event_log import WalEventLog
from harness.storage.blob import FileBlobStore

REPO_ROOT = Path(__file__).resolve().parents[1]
CHILD = Path(__file__).resolve().parent / "_s9_child.py"

RUN_ID = "R-S9-6"
RESOURCE = "r"
SECRET = "s9d-cross-process-secret"


def _durable_harness(tmp_path: Path) -> Harness:
    """durable 三件套 + auth_secret（默认 token 仅用于非安全用例夹具）。"""
    h = Harness(run_limits=1000,
                event_log=WalEventLog(str(tmp_path / "wal.log"), fsync=True),
                blobs=FileBlobStore(tmp_path / "blobs"),
                checkpoints=FileCheckpointStore(tmp_path / "cp"),
                auth_secret=SECRET)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def test_s9_6_cross_process_recover(tmp_path):
    """S9-6：父进程关闭 WAL 后，子进程从磁盘全量重建并恢复一致状态。"""
    h = _durable_harness(tmp_path)
    h.workspace.put(RESOURCE, {"v": 0})                 # 种子（也是 WAL 事件）

    plan = h.planner.plan(RUN_ID, [{"task_id": "T1", "contingent": [RESOURCE]}])
    h.plans[RUN_ID] = plan
    acb = h.dispatch_and_run(
        plan, "T1", [decision(1, stage(RESOURCE, {"v": 1}), commit(RESOURCE))])

    assert acb.state is TaskState.DONE
    ref = acb.result_ref
    assert ref, "真实提交路径必须落盘 checkpoint"
    assert ref == h.checkpoints.latest_ref_for_acb(acb.id)

    # 父进程侧事实（作为子进程的对照期望值）
    parent_hash = h.workspace.content_hash(RESOURCE)
    parent_version = h.workspace.version(RESOURCE)
    parent_receipts = h.receipts_of(RUN_ID)
    assert parent_receipts, "生产路径必须产生收据"
    assert h.verify_receipts(RUN_ID) is True

    # P1-8：在最后一次 checkpoint 之后追加一条订阅资源的事件 → 子进程 recover 后
    # context_view 必须能重放出它（证明 subscriptions/materialized 已恢复）。
    h.event_log.append(EventKind.RESOURCE_COMMITTED, RESOURCE,
                       {"content": {"v": 7}, "version": 99})

    handoff = {
        "acb_id": acb.id,
        "ref": ref,
        "store_uuid": h.checkpoints.store_uuid,
        # 以下是子进程的期望值（不是子进程自证）
        "run_id": RUN_ID,
        "resource": RESOURCE,
        "content_hash": parent_hash,
        "version": parent_version,
        "state": acb.state.value,
        "generation": int(acb.generation),
    }
    handoff_path = tmp_path / "handoff.json"
    handoff_path.write_text(json.dumps(handoff), encoding="utf-8")

    h.event_log.close()                                 # 关闭 WAL（子进程唯一写者/读者）

    proc = subprocess.run(
        [sys.executable, str(CHILD), "cross-process",
         "--handoff", str(handoff_path),
         "--wal", str(tmp_path / "wal.log"),
         "--blobs", str(tmp_path / "blobs"),
         "--checkpoints", str(tmp_path / "cp"),
         "--secret", SECRET],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=120,
        check=False)
    assert proc.returncode == 0, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert lines, "子进程未输出 JSON"
    out = json.loads(lines[-1])

    # ① 从 seq 0 全量重建 workspace（不是空 manifest）
    assert out["manifest_size"] >= 1
    # ② ref 来自磁盘 latest 指针
    assert out["ref"] == ref
    assert out["acb_id"] == acb.id
    # ③ workspace 内容与版本和父进程一致
    assert out["content_hash"] == parent_hash
    assert out["version"] == parent_version
    # ④ 收据链可离线验证（signed=True → True，不是 None）
    assert out["receipts_nonempty"] is True
    assert out["receipt_count"] == len(parent_receipts)
    assert out["verified"] is True
    # ⑤ ACB 状态合理：恢复的是父进程落盘的终态，generation 前进
    assert out["state"] == out["expected_state"] == "DONE"
    # P2：generation 精确等于父进程 + 1（不是宽松的 >= 2）
    assert out["generation"] == handoff["generation"] + 1
    # ⑥ P1-8：跨进程恢复后 subscriptions 已恢复 → context_view 非空（事件未被过滤）
    assert out["subscriptions_restored"] == [RESOURCE]
    assert out["context_view_nonempty"] is True
    assert out["context_view_value"] == {"v": 7}
