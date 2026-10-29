"""S24-P1 跨进程重放子进程：固定场景 → WAL / checkpoint / set 编码摘要。

不作为 pytest 用例收集。父进程用不同 `PYTHONHASHSEED` 启动，比较摘要。
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harness import RscSession
from harness.entropy import Deterministic
from harness.event_log import WalEventLog
from harness.harness import Harness
from harness.storage.wal import Wal


def _sha(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree_sha(root: pathlib.Path) -> str:
    want = {"store.json"}
    dirs = {"refs", "latest", "latest_run", "latest_task", "run_acbs"}
    entries = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if rel in want or rel.split("/")[0] in dirs:
            entries.append((rel, _sha(path)))
    return hashlib.sha256(
        json.dumps(entries, separators=(",", ":")).encode("utf-8")).hexdigest()


def main() -> int:
    root = pathlib.Path(os.environ["RSC_CHILD_ROOT"])
    root.mkdir(parents=True, exist_ok=True)
    ent = Deterministic("cross-process")
    wal = WalEventLog(str(root / "wal.log"), fsync=False, clock=ent, entropy=ent)
    h = Harness(event_log=wal, checkpoint_root=str(root), entropy=ent,
                run_limits=10)
    session = RscSession(h, tenant="t", min_budget=1,
                         state_dir=str(root / "idx"))
    tok = session.begin("a", "g", writes=["a.md"])
    session.apply(tok.token, [("a.md", "v1")], idempotency_key="k", agent_id="a")
    session.finish(tok.token)
    wal.close()

    # 集合编码：跨 hash seed 必须逐字节稳定（排序后写入）
    sets_path = root / "sets.log"
    sets = Wal(str(sets_path), fsync=False, entropy=ent)
    sets.append_frame([{"kind": "SET_PROBE",
                        "payload": {"s": {"c", "a", "b"}}}])
    sets.close()
    print(json.dumps({
        "wal_sha256": _sha(root / "wal.log"),
        "checkpoint_tree_sha256": _tree_sha(root),
        "sets_sha256": _sha(sets_path),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
